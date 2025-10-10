package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unsafe"

	flag "github.com/spf13/pflag"
	"github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
	"github.com/sst/opencode/internal/api"
	"github.com/sst/opencode/internal/app"
	"github.com/sst/opencode/internal/debug"
	"github.com/sst/opencode/internal/ipc"
	"github.com/sst/opencode/internal/state"
	"github.com/sst/opencode/internal/util"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

var Version = "dev"

func main() {
	version := Version
	if version != "dev" && !strings.HasPrefix(Version, "v") {
		version = "v" + Version
	}

	var model *string = flag.String("model", "", "model to begin with")
	var prompt *string = flag.String("prompt", "", "prompt to begin with")
	var agent *string = flag.String("agent", "", "agent to begin with")
	var sessionID *string = flag.String("session", "", "session ID")
	flag.Parse()

	url := os.Getenv("OPENCODE_SERVER")
	if url == "" {
		slog.Error("OPENCODE_SERVER environment variable not set")
		os.Exit(1)
	}

	httpClient := opencode.NewClient(
		option.WithBaseURL(url),
	)

	// Fetch required data in parallel
	var agents []opencode.Agent
	var path *opencode.Path
	var project *opencode.Project

	batch := errgroup.Group{}

	batch.Go(func() error {
		result, err := httpClient.Project.Current(context.Background(), opencode.ProjectCurrentParams{})
		if err != nil {
			return err
		}
		project = result
		return nil
	})

	batch.Go(func() error {
		result, err := httpClient.Agent.List(context.Background(), opencode.AgentListParams{})
		if err != nil {
			return err
		}
		agents = *result
		return nil
	})

	batch.Go(func() error {
		result, err := httpClient.Path.Get(context.Background(), opencode.PathGetParams{})
		if err != nil {
			return err
		}
		path = result
		return nil
	})

	err := batch.Wait()
	if err != nil {
		slog.Error("Failed to fetch initial data", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize logging
	apiHandler := util.NewAPILogHandler(ctx, httpClient, "messages", slog.LevelDebug)
	logger := slog.New(apiHandler)
	slog.SetDefault(logger)

	slog.Debug("Messages pane launched")

	// Create app instance
	app_, err := app.New(ctx, version, project, path, agents, httpClient, model, prompt, agent, sessionID)
	if err != nil {
		slog.Error("Failed to create app", "error", err)
		os.Exit(1)
	}

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Start message renderer
	renderer := NewMessageRenderer(app_)

	// Start event streaming
	go func() {
		stream := httpClient.Event.ListStreaming(ctx, opencode.EventListParams{})
		for stream.Next() {
			evt := stream.Current().AsUnion()
			renderer.HandleEvent(evt)
		}
		if err := stream.Err(); err != nil {
			slog.Error("Error streaming events", "error", err)
		}
	}()

	// Start API server (for TUI control bridge)
	go api.Start(ctx, nil, httpClient)

	// Initialize with first available session if none specified
	if app_.Session.ID == "" {
		go func() {
			// Wait a moment for other components to initialize
			time.Sleep(1 * time.Second)
			if err := renderer.loadFirstAvailableSession(); err != nil {
				slog.Warn("Failed to load first available session", "error", err)
			}
		}()
	}

	// Start the renderer
	go renderer.Start(ctx)

	// Start session synchronization monitor
	go renderer.monitorSessionSync(ctx)

	// Start message polling as backup mechanism
	go renderer.startMessagePolling(ctx)

	// Wait for shutdown signal
	select {
	case sig := <-sigChan:
		slog.Info("Messages pane received signal", "signal", sig)
	case <-ctx.Done():
		slog.Info("Messages pane context cancelled")
	}

	cancel()
	slog.Info("Messages pane exited")
}

// MessageRenderer handles message display in the messages pane
type MessageRenderer struct {
	app           *app.App
	lastRender    string
	eventChan     chan interface{}
	ipcClient     *ipc.Client
	stateManager  *state.StateManager
	statusReporter *debug.StatusReporter
}

func NewMessageRenderer(app *app.App) *MessageRenderer {
	// Initialize IPC client
	socketPath := os.Getenv("OPENCODE_IPC_SOCKET")
	var ipcClient *ipc.Client
	if socketPath != "" {
		ipcClient = ipc.NewClient(socketPath)
	}

	statusReporter := debug.NewStatusReporter("messages")
	statusReporter.UpdateIPCStatus(ipcClient != nil, socketPath)

	return &MessageRenderer{
		app:           app,
		eventChan:     make(chan interface{}, 100),
		ipcClient:     ipcClient,
		stateManager:  state.NewStateManager(),
		statusReporter: statusReporter,
	}
}

func (r *MessageRenderer) HandleEvent(evt interface{}) {
	select {
	case r.eventChan <- evt:
	default:
		// Channel full, drop event
		slog.Warn("Event channel full, dropping event")
	}
}

func (r *MessageRenderer) Start(ctx context.Context) {
	// Connect to IPC server if available
	if r.ipcClient != nil {
		err := r.ipcClient.Connect(ctx)
		if err != nil {
			slog.Warn("Failed to connect to IPC server", "error", err)
		} else {
			defer r.ipcClient.Disconnect()
			slog.Info("Messages pane connected to IPC server")

			// Start IPC event listening in background
			go r.listenForEvents(ctx)
		}
	}

	// Load initial messages for current session
	if r.app.Session.ID != "" {
		r.loadMessagesForSession(r.app.Session.ID)
	}

	// Initial render
	r.render()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.eventChan:
			// Event received, trigger render
			r.render()
		case <-ticker.C:
			// Periodic render for animations
			if r.app.HasAnimatingWork() {
				r.render()
			}
		}
	}
}

// listenForEvents listens for IPC events from other panes
func (r *MessageRenderer) listenForEvents(ctx context.Context) {
	if r.ipcClient == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-r.ipcClient.Events():
			switch event.Type {
			case ipc.EventSessionChanged:
				sessionID := r.extractSessionID(event)
				if sessionID != "" && sessionID != r.app.Session.ID {
					slog.Info("Messages pane received session change", "sessionID", sessionID, "previousID", r.app.Session.ID)

					// Update app session
					r.app.Session.ID = sessionID

					// Try to get session title from the event data
					if data, ok := event.Data.(map[string]interface{}); ok {
						if title, exists := data["title"].(string); exists {
							r.app.Session.Title = title
							slog.Info("Updated session title", "title", title)
						}
					}

					// Update status reporter
					if r.statusReporter != nil {
						r.statusReporter.UpdateSessionStatus(sessionID)
					}

					// Load messages for the new session
					r.loadMessagesForSession(sessionID)

					// Trigger render immediately
					r.HandleEvent("session_changed")
				} else {
					if sessionID == "" {
						slog.Error("Failed to extract sessionID from session change event", "eventData", event.Data)
					}
				}
			case ipc.EventSessionCreated:
				slog.Info("Messages pane received session created event")
				// Could refresh session list or handle new session
			case ipc.EventMessageSent:
				// Handle message sent events from input pane
				if data, ok := event.Data.(map[string]interface{}); ok {
					if sessionID, exists := data["session_id"].(string); exists {
						if sessionID == r.app.Session.ID {
							slog.Info("Messages pane received message sent event for current session", "sessionID", sessionID)
							// Refresh messages for current session
							r.loadMessagesForSession(sessionID)
							r.HandleEvent("message_sent")
						}
					}
				}
			case ipc.EventMessageReceived:
				// Handle message received events (from server/assistant responses)
				if data, ok := event.Data.(map[string]interface{}); ok {
					if sessionID, exists := data["session_id"].(string); exists {
						if sessionID == r.app.Session.ID {
							slog.Info("Messages pane received message received event for current session", "sessionID", sessionID)
							// Refresh messages for current session
							r.loadMessagesForSession(sessionID)
							r.HandleEvent("message_received")
						}
					}
				}
			case ipc.EventSessionDeleted:
				sessionID := r.extractSessionID(event)
				if sessionID != "" {
					slog.Info("Messages pane received session deleted event", "sessionID", sessionID)

					// 如果删除的是当前显示的session，清空显示
					if r.app.Session.ID == sessionID {
						r.app.Session.ID = ""
						r.app.Messages = nil
						r.HandleEvent("session_deleted")
					}
				} else {
					slog.Error("Failed to extract sessionID from session deleted event", "eventData", event.Data)
				}
			}
		}
	}
}

// loadMessagesForSession loads messages for the specified session
func (r *MessageRenderer) loadMessagesForSession(sessionID string) {
	if sessionID == "" {
		slog.Warn("Cannot load messages: sessionID is empty")
		r.showLoadingError("SessionID为空，无法加载消息")
		return
	}

	slog.Info("Loading messages for session", "sessionID", sessionID)

	// Clear existing messages immediately to show the switch
	r.app.Messages = nil

	// 添加超时控制
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use the app's existing method to load messages
	messages, err := r.app.ListMessages(ctx, sessionID)
	if err != nil {
		slog.Error("Failed to load messages", "error", err, "sessionID", sessionID)
		r.showLoadingError(fmt.Sprintf("加载session消息失败: %v", err))

		// Fallback: 至少清空当前消息，避免显示错误的历史消息
		r.app.Messages = nil
		return
	}

	// Update app messages
	r.app.Messages = messages
	slog.Info("Successfully loaded messages for session",
		"sessionID", sessionID,
		"messageCount", len(messages),
		"previousSessionID", func() string {
			if r.app.Session != nil {
				return r.app.Session.ID
			}
			return "unknown"
		}())

	// 确保app的session信息是最新的
	if r.app.Session == nil {
		slog.Warn("App session is nil, creating new session object")
		// 这里可以根据需要创建一个基本的session对象
	}

	// 立即触发渲染
	r.HandleEvent("messages_loaded")
}

// showLoadingError 显示加载错误（可以在界面上体现）
func (r *MessageRenderer) showLoadingError(errorMsg string) {
	slog.Error("Message loading error", "error", errorMsg)
	// 这里可以设置一个错误状态，在render中显示
	// 暂时只记录日志
}

// getSessionDisplayName returns a user-friendly session name
func (r *MessageRenderer) getSessionDisplayName() string {
	if r.app.Session.Title != "" {
		return r.app.Session.Title
	}
	if r.app.Session.ID != "" {
		// Show only last 8 characters of session ID for brevity
		if len(r.app.Session.ID) > 8 {
			return "..." + r.app.Session.ID[len(r.app.Session.ID)-8:]
		}
		return r.app.Session.ID
	}
	return "Unknown Session"
}

func (r *MessageRenderer) render() {
	// Clear screen and move cursor to top
	fmt.Print("\033[2J\033[H")

	// Get terminal size
	width, height := getTerminalSize()

	// Render header
	header := r.renderHeader(width)
	if header != "" {
		fmt.Print(header)
		fmt.Print("\n")
		height -= strings.Count(header, "\n") + 1
	}

	// Render messages
	messages := r.renderMessages(width, height-2) // Leave space for borders
	fmt.Print(messages)
}

func (r *MessageRenderer) renderHeader(width int) string {
	if r.app.Session.ID == "" {
		return ""
	}

	// Simple header with session title
	title := r.app.Session.Title
	if len(title) > width-4 {
		title = title[:width-7] + "..."
	}

	header := fmt.Sprintf("┌%s┐", strings.Repeat("─", width-2))
	header += fmt.Sprintf("\n│ %s%s │", title, strings.Repeat(" ", width-4-len(title)))
	header += fmt.Sprintf("\n└%s┘", strings.Repeat("─", width-2))

	return header
}

func (r *MessageRenderer) renderMessages(width, height int) string {
	if r.app.Session.ID == "" {
		return fmt.Sprintf("%s\n%s\n\n%s\n%s",
			"🔍 No active session selected",
			"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
			"📋 Available actions:",
			"   • Select a session from the Sessions pane (left panel)")
	}

	if len(r.app.Messages) == 0 {
		return fmt.Sprintf("%s: %s\n%s\n\n%s\n%s",
			"📝 Session", r.getSessionDisplayName(),
			"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
			"💬 No messages in this session yet...",
			"   Send a message from the Input pane to start the conversation.")
	}

	var lines []string

	for _, message := range r.app.Messages {
		switch msg := message.Info.(type) {
		case opencode.UserMessage:
			lines = append(lines, r.renderUserMessage(msg, message.Parts, width))
		case opencode.AssistantMessage:
			lines = append(lines, r.renderAssistantMessage(msg, message.Parts, width))
		}
		lines = append(lines, "") // Add spacing between messages
	}

	// Take last N lines that fit in height
	if len(lines) > height {
		lines = lines[len(lines)-height:]
	}

	return strings.Join(lines, "\n")
}

func (r *MessageRenderer) renderUserMessage(msg opencode.UserMessage, parts []opencode.PartUnion, width int) string {
	var content strings.Builder

	// User indicator
	content.WriteString("👤 User: ")

	for _, part := range parts {
		if textPart, ok := part.(opencode.TextPart); ok {
			if !textPart.Synthetic && textPart.Text != "" {
				// Word wrap the text
				wrapped := wordWrap(textPart.Text, width-10)
				content.WriteString(wrapped)
			}
		}
	}

	return content.String()
}

func (r *MessageRenderer) renderAssistantMessage(msg opencode.AssistantMessage, parts []opencode.PartUnion, width int) string {
	var content strings.Builder

	// Assistant indicator
	modelName := msg.ModelID
	if modelName == "" {
		modelName = "Assistant"
	}
	content.WriteString(fmt.Sprintf("🤖 %s: ", modelName))

	hasContent := false
	for _, part := range parts {
		if textPart, ok := part.(opencode.TextPart); ok {
			if strings.TrimSpace(textPart.Text) != "" {
				// Word wrap the text
				wrapped := wordWrap(textPart.Text, width-10)
				content.WriteString(wrapped)
				hasContent = true
			}
		} else if toolPart, ok := part.(opencode.ToolPart); ok {
			// Show tool calls if configured
			if toolPart.State.Status == opencode.ToolPartStateStatusCompleted {
				content.WriteString(fmt.Sprintf("\n[Tool: %s]", toolPart.Tool))
				hasContent = true
			}
		}
	}

	if !hasContent && msg.Time.Completed == 0 {
		content.WriteString("Generating...")
	}

	return content.String()
}

func wordWrap(text string, width int) string {
	if width <= 0 {
		return text
	}

	words := strings.Fields(text)
	if len(words) == 0 {
		return text
	}

	var lines []string
	var currentLine strings.Builder

	for _, word := range words {
		if currentLine.Len()+len(word)+1 > width {
			if currentLine.Len() > 0 {
				lines = append(lines, currentLine.String())
				currentLine.Reset()
			}
		}

		if currentLine.Len() > 0 {
			currentLine.WriteString(" ")
		}
		currentLine.WriteString(word)
	}

	if currentLine.Len() > 0 {
		lines = append(lines, currentLine.String())
	}

	return strings.Join(lines, "\n")
}

// extractSessionID 从IPC事件中提取sessionID，支持多种数据格式
func (r *MessageRenderer) extractSessionID(event *ipc.Event) string {
	if event.Data == nil {
		slog.Warn("Event data is nil", "eventType", event.Type)
		return ""
	}

	// 方法1: 尝试直接类型断言为SessionChangedData
	if sessionData, ok := event.Data.(*ipc.SessionChangedData); ok {
		slog.Debug("Successfully extracted sessionID via direct type assertion", "sessionID", sessionData.SessionID)
		return sessionData.SessionID
	}

	// 方法2: 尝试类型断言为SessionChangedData (非指针)
	if sessionData, ok := event.Data.(ipc.SessionChangedData); ok {
		slog.Debug("Successfully extracted sessionID via value type assertion", "sessionID", sessionData.SessionID)
		return sessionData.SessionID
	}

	// 方法3: 尝试作为map[string]interface{}处理
	if data, ok := event.Data.(map[string]interface{}); ok {
		slog.Debug("Event data is map[string]interface{}", "keys", getMapKeys(data))

		// 尝试多种可能的字段名
		possibleKeys := []string{"session_id", "SessionID", "sessionId", "sessionID"}
		for _, key := range possibleKeys {
			if value, exists := data[key]; exists {
				if sessionID, ok := value.(string); ok && sessionID != "" {
					slog.Debug("Successfully extracted sessionID via map access",
						"key", key, "sessionID", sessionID)
					return sessionID
				}
			}
		}

		slog.Warn("No valid sessionID found in map data", "availableKeys", getMapKeys(data))
	}

	// 方法4: 尝试JSON重新解析 (最后手段)
	if jsonBytes, err := json.Marshal(event.Data); err == nil {
		var sessionData ipc.SessionChangedData
		if err := json.Unmarshal(jsonBytes, &sessionData); err == nil && sessionData.SessionID != "" {
			slog.Debug("Successfully extracted sessionID via JSON re-parsing", "sessionID", sessionData.SessionID)
			return sessionData.SessionID
		}
	}

	slog.Warn("All extraction methods failed", "dataType", fmt.Sprintf("%T", event.Data))
	return ""
}

// getMapKeys 获取map的所有键，用于调试
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// loadFirstAvailableSession loads the first available session if no session is currently selected
func (r *MessageRenderer) loadFirstAvailableSession() error {
	if r.app.Session.ID != "" {
		// Already have a session
		return nil
	}

	// Get list of sessions
	sessions, err := r.app.ListSessions(context.Background())
	if err != nil {
		return fmt.Errorf("failed to list sessions: %w", err)
	}

	// Find first non-child session
	for _, session := range sessions {
		if session.ParentID == "" {
			slog.Info("Auto-selecting first available session", "sessionID", session.ID, "title", session.Title)

			// Update app session
			r.app.Session.ID = session.ID
			r.app.Session.Title = session.Title

			// Load messages for this session
			r.loadMessagesForSession(session.ID)

			// Trigger render
			r.HandleEvent("auto_session_selected")

			return nil
		}
	}

	slog.Info("No sessions available to auto-select")
	return nil
}

// monitorSessionSync monitors for session synchronization issues and attempts recovery
func (r *MessageRenderer) monitorSessionSync(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Try fallback state synchronization if IPC isn't working
			if r.ipcClient == nil || r.app.Session.ID == "" {
				if sharedState, err := r.stateManager.GetState(); err == nil {
					if sharedState.CurrentSessionID != "" && sharedState.CurrentSessionID != r.app.Session.ID {
						slog.Info("Fallback sync: found different session in shared state",
							"currentSession", r.app.Session.ID,
							"sharedSession", sharedState.CurrentSessionID,
							"updatedBy", sharedState.UpdatedBy)

						// Update to the session from shared state
						r.app.Session.ID = sharedState.CurrentSessionID
						r.app.Session.Title = sharedState.CurrentTitle
						r.loadMessagesForSession(sharedState.CurrentSessionID)
						r.HandleEvent("fallback_session_sync")
					}
				}
			}

			// Check if we have a session but no messages loaded, which might indicate sync issues
			if r.app.Session.ID != "" && len(r.app.Messages) == 0 {
				slog.Debug("Session sync check: have sessionID but no messages",
					"sessionID", r.app.Session.ID)

				// Try to reload messages
				r.loadMessagesForSession(r.app.Session.ID)
			} else if r.app.Session.ID == "" {
				// No session selected, try to auto-select first available
				slog.Debug("Session sync check: no session selected, attempting auto-select")
				if err := r.loadFirstAvailableSession(); err != nil {
					slog.Debug("Auto-select failed", "error", err)
				}
			}

			// Debug output every sync cycle
			if r.statusReporter != nil {
				slog.Debug("Messages pane sync status",
					"sessionID", r.app.Session.ID,
					"sessionTitle", r.app.Session.Title,
					"messageCount", len(r.app.Messages),
					"ipcConnected", r.ipcClient != nil)
			}
		}
	}
}

// startMessagePolling polls for message updates as a backup mechanism
func (r *MessageRenderer) startMessagePolling(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second) // Poll every 2 seconds
	defer ticker.Stop()

	var lastMessageCount int

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if r.app.Session.ID != "" {
				// Check if message count has changed
				messages, err := r.app.ListMessages(ctx, r.app.Session.ID)
				if err != nil {
					continue // Skip this poll cycle on error
				}

				currentMessageCount := len(messages)
				if currentMessageCount != lastMessageCount {
					slog.Info("Message polling detected message count change",
						"sessionID", r.app.Session.ID,
						"previousCount", lastMessageCount,
						"currentCount", currentMessageCount)

					// Update messages and trigger render
					r.app.Messages = messages
					r.HandleEvent("messages_polled")
					lastMessageCount = currentMessageCount
				}
			} else {
				// Reset counter when no session
				lastMessageCount = 0
			}
		}
	}
}

func getTerminalSize() (width, height int) {
	// Default size if we can't detect
	width, height = 80, 24

	// Try to get actual terminal size using syscall
	ws := &unix.Winsize{}
	if _, _, err := unix.Syscall(unix.SYS_IOCTL, os.Stdout.Fd(), unix.TIOCGWINSZ, uintptr(unsafe.Pointer(ws))); err == 0 {
		width = int(ws.Col)
		height = int(ws.Row)
	}

	return width, height
}