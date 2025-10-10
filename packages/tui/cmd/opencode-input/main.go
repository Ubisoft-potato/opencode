package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/chzyer/readline"
	flag "github.com/spf13/pflag"
	"github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
	"github.com/sst/opencode/internal/app"
	"github.com/sst/opencode/internal/ipc"
	"github.com/sst/opencode/internal/state"
	"github.com/sst/opencode/internal/util"
	"golang.org/x/sync/errgroup"
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
	apiHandler := util.NewAPILogHandler(ctx, httpClient, "input", slog.LevelDebug)
	logger := slog.New(apiHandler)
	slog.SetDefault(logger)

	slog.Debug("Input pane launched")

	// Create app instance
	app_, err := app.New(ctx, version, project, path, agents, httpClient, model, prompt, agent, sessionID)
	if err != nil {
		slog.Error("Failed to create app", "error", err)
		os.Exit(1)
	}

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Start input handler
	inputHandler := NewInputHandler(app_)

	// Handle initial prompt if provided
	if prompt != nil && *prompt != "" {
		err = inputHandler.SendPrompt(*prompt)
		if err != nil {
			slog.Error("Failed to send initial prompt", "error", err)
		}
	}

	// Start the input loop
	go func() {
		err := inputHandler.Start(ctx)
		if err != nil && err != io.EOF {
			slog.Error("Input handler error", "error", err)
		}
		cancel()
	}()

	// Wait for shutdown signal
	select {
	case sig := <-sigChan:
		slog.Info("Input pane received signal", "signal", sig)
	case <-ctx.Done():
		slog.Info("Input pane context cancelled")
	}

	cancel()
	slog.Info("Input pane exited")
}

// InputHandler handles user input and command processing
type InputHandler struct {
	app          *app.App
	rl           *readline.Instance
	history      []string
	ipcClient    *ipc.Client
	stateManager *state.StateManager
}

func NewInputHandler(app *app.App) *InputHandler {
	// Configure readline
	config := &readline.Config{
		Prompt:          "> ",
		HistoryFile:     "/tmp/opencode_history",
		AutoComplete:    newCompleter(app),
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	}

	rl, err := readline.NewEx(config)
	if err != nil {
		slog.Error("Failed to create readline instance", "error", err)
		return &InputHandler{app: app}
	}

	// Initialize IPC client
	socketPath := os.Getenv("OPENCODE_IPC_SOCKET")
	var ipcClient *ipc.Client
	if socketPath != "" {
		ipcClient = ipc.NewClient(socketPath)
	}

	return &InputHandler{
		app:          app,
		rl:           rl,
		history:      make([]string, 0),
		ipcClient:    ipcClient,
		stateManager: state.NewStateManager(),
	}
}

func (h *InputHandler) Start(ctx context.Context) error {
	if h.rl == nil {
		return fmt.Errorf("readline not initialized")
	}
	defer h.rl.Close()

	// Connect to IPC server with enhanced reliability
	if h.ipcClient != nil {
		go h.maintainIPCConnection(ctx)
	}

	// Start IPC event listening in background
	if h.ipcClient != nil {
		go h.listenForEvents(ctx)
	}

	// Start periodic session sync to ensure we don't miss session changes
	go h.startPeriodicSessionSync(ctx)

	// Start session state monitoring for debugging
	go h.startSessionStateMonitoring(ctx)

	// Try to sync with current session from shared state
	h.syncWithSharedSession()

	// Display welcome message with debug info
	h.displayWelcomeMessage()
	h.debugSessionState()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			line, err := h.rl.Readline()
			if err != nil {
				if err == io.EOF || err == readline.ErrInterrupt {
					return err
				}
				slog.Error("Readline error", "error", err)
				continue
			}

			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			// Handle special commands
			switch line {
			case "exit", "quit", "q", ":q":
				return io.EOF
			}

			// Add to history
			h.history = append(h.history, line)

			// Process the input
			err = h.processInput(line)
			if err != nil {
				slog.Error("Failed to process input", "error", err, "input", line)
				fmt.Printf("Error: %v\n", err)
			}
		}
	}
}

func (h *InputHandler) processInput(input string) error {
	if strings.HasPrefix(input, "/") {
		// Handle slash commands
		return h.handleCommand(input[1:])
	}

	// Send as regular prompt
	return h.SendPrompt(input)
}

func (h *InputHandler) handleCommand(command string) error {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return fmt.Errorf("empty command")
	}

	commandName := parts[0]
	_ = parts[1:] // Ignore additional arguments for now

	switch commandName {
	case "new":
		fmt.Println("🚀 Creating new session...")
		err := h.createNewSession()
		if err != nil {
			fmt.Printf("❌ Failed to create session: %v\n", err)
			return err
		}

		// Notify other panes via IPC that a new session was created
		if h.ipcClient != nil && h.app.Session.ID != "" {
			event := ipc.NewEvent(ipc.EventSessionCreated, "input", &ipc.SessionChangedData{
				SessionID: h.app.Session.ID,
				Title:     "New Session",
			})
			if err := h.ipcClient.Send(event); err != nil {
				slog.Warn("Failed to send session created event", "error", err)
			} else {
				fmt.Println("📡 Notified other panes about new session")
			}
		}

		fmt.Println("💡 Session created! Other panes should update automatically.")
		return nil

	case "help":
		h.displayHelp()
		return nil

	case "sessions":
		fmt.Println("📋 Listing sessions (feature coming soon)")
		return nil

	case "models":
		fmt.Println("🤖 Listing models (feature coming soon)")
		return nil

	case "agents":
		fmt.Println("🤖 Listing agents (feature coming soon)")
		return nil

	case "clear":
		// Clear screen
		fmt.Print("\033[2J\033[H")
		h.displayWelcomeMessage()
		return nil

	default:
		fmt.Printf("❌ Unknown command: /%s\n", commandName)
		fmt.Println("💡 Type /help for available commands")
		return nil
	}
}

func (h *InputHandler) SendPrompt(text string) error {
	// Force immediate sync to catch any recent session changes
	if h.syncWithSharedSession() {
		fmt.Printf("🔄 Synced with selected session: %s\n", h.app.Session.Title)
	}

	if h.app.Session.ID == "" {
		fmt.Println("\n🔍 No active session found. Attempting enhanced session discovery...")

		// Enhanced session detection with multiple strategies
		currentTime := time.Now()

		// Strategy 1: Immediate sync attempts with progressive delays
		for attempt := 1; attempt <= 5; attempt++ {
			if h.syncWithSharedSession() {
				fmt.Printf("✅ Found selected session on immediate attempt %d: %s (%s)\n",
					attempt, h.app.Session.Title, h.app.Session.ID)
				fmt.Println("📤 Proceeding to send your message...")
				break
			}

			if attempt <= 2 {
				time.Sleep(50 * time.Millisecond) // Quick retries first
			} else {
				time.Sleep(200 * time.Millisecond) // Longer waits for later attempts
			}
		}

		// Strategy 2: Wait for potential session changes using the new method
		if h.app.Session.ID == "" {
			fmt.Println("🕐 Waiting for session selection (up to 2 seconds)...")

			newState, err := h.stateManager.WaitForStateChange(currentTime, 2*time.Second)
			if err != nil {
				slog.Warn("Failed to wait for state change", "error", err)
			} else if newState.CurrentSessionID != "" {
				h.app.Session.ID = newState.CurrentSessionID
				h.app.Session.Title = newState.CurrentTitle
				fmt.Printf("✅ Detected session selection: %s (%s)\n",
					h.app.Session.Title, h.app.Session.ID)
			}
		}

		// Strategy 3: Final verification and user guidance
		if h.app.Session.ID == "" {
			// Get current shared state for debugging
			if sharedState, err := h.stateManager.GetState(); err == nil {
				fmt.Printf("🔍 Debug: Shared state shows session '%s' updated by '%s' at %s\n",
					sharedState.CurrentSessionID, sharedState.UpdatedBy,
					sharedState.LastUpdated.Format("15:04:05"))
			}

			fmt.Println("\n⚠️  No session is currently selected in any pane.")
			fmt.Println("🔧 Please select a session from the Sessions pane (left panel) first,")
			fmt.Println("   or type '/new' to create a new session manually.")
			fmt.Println("💡 If you just selected a session, please try again in a moment.")
			return fmt.Errorf("no session selected - please choose a session first or use /new")
		}
	}

	// Final verification before sending
	fmt.Printf("📋 Session confirmed: %s (%s)\n", h.app.Session.Title, h.app.Session.ID)

	// Send message to OpenCode via HTTP API
	if err := h.sendMessageToSession(text); err != nil {
		fmt.Printf("❌ Failed to send message: %v\n", err)
		return err
	}

	fmt.Printf("📤 Message sent successfully!\n")
	return nil
}

func (h *InputHandler) displayHelp() {
	fmt.Println("🔧 Available Commands:")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  /new      - Create a new OpenCode session")
	fmt.Println("  /sessions - List all sessions (coming soon)")
	fmt.Println("  /models   - List available models (coming soon)")
	fmt.Println("  /agents   - List available agents (coming soon)")
	fmt.Println("  /clear    - Clear the screen")
	fmt.Println("  /help     - Show this help message")
	fmt.Println("  /quit     - Exit the input pane")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("💬 Or just type a message to chat with OpenCode")
	fmt.Println()
}

func (h *InputHandler) displayWelcomeMessage() {
	fmt.Println("🎯 OpenCode TMux Input Pane")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	if h.app.Session.ID != "" {
		fmt.Printf("✅ Active session: %s\n", h.app.Session.ID)
	} else {
		fmt.Println("⏳ No active session - will create one automatically when you send a message")
	}

	fmt.Println()
	fmt.Println("💬 Start typing to chat with OpenCode")
	fmt.Println("🔧 Commands: /new, /help, /quit")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()
}

type SessionResponse struct {
	ID    string `json:"id"`
	Title string `json:"title,omitempty"`
}

func (h *InputHandler) createNewSession() error {
	// Use HTTP client to create a real session
	url := os.Getenv("OPENCODE_SERVER")
	if url == "" {
		return fmt.Errorf("OPENCODE_SERVER environment variable not set")
	}

	// Create session via HTTP API
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	sessionURL := strings.TrimSuffix(url, "/") + "/session"
	requestBody := strings.NewReader("{}")

	req, err := http.NewRequest("POST", sessionURL, requestBody)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status code %d: %s", resp.StatusCode, string(body))
	}

	// Parse response to get session ID
	var sessionResp SessionResponse
	if err := json.NewDecoder(resp.Body).Decode(&sessionResp); err != nil {
		return fmt.Errorf("failed to parse session response: %w", err)
	}

	// Update the app's session
	h.app.Session.ID = sessionResp.ID
	fmt.Printf("✅ New session created successfully!\n")
	fmt.Printf("🆔 Session ID: %s\n", sessionResp.ID)

	return nil
}

// listenForEvents listens for IPC events from other panes
func (h *InputHandler) listenForEvents(ctx context.Context) {
	if h.ipcClient == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-h.ipcClient.Events():
			switch event.Type {
			case ipc.EventSessionChanged:
				sessionID := h.extractSessionID(event)
				if sessionID != "" && sessionID != h.app.Session.ID {
					slog.Info("Input pane received session change", "sessionID", sessionID, "previousID", h.app.Session.ID)

					h.app.Session.ID = sessionID

					// Try to get session title from the event data
					if data, ok := event.Data.(map[string]interface{}); ok {
						if title, exists := data["title"].(string); exists {
							h.app.Session.Title = title
						}
					}

					fmt.Printf("📡 Session switched to: %s\n", sessionID)

					// Update welcome message to reflect new session
					h.displayWelcomeMessage()
				}
			case ipc.EventSessionCreated:
				fmt.Println("📡 New session created by another pane")
			case ipc.EventSessionDeleted:
				if data, ok := event.Data.(map[string]interface{}); ok {
					if sessionID, exists := data["session_id"].(string); exists {
						fmt.Printf("📡 Session deleted: %s\n", sessionID)

						// 如果删除的是当前活动session，清空当前session
						if h.app.Session.ID == sessionID {
							h.app.Session.ID = ""
							fmt.Println("⚠️  当前活动session已被删除，请选择或创建新的session")
							h.displayWelcomeMessage()
						}
					}
				}
			}
		}
	}
}

// sendMessageToSession sends a message to the current session via HTTP API
func (h *InputHandler) sendMessageToSession(text string) error {
	if h.app.Session.ID == "" {
		return fmt.Errorf("no active session")
	}

	url := os.Getenv("OPENCODE_SERVER")
	if url == "" {
		return fmt.Errorf("OPENCODE_SERVER environment variable not set")
	}

	// Create message via HTTP API
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Create message request body with correct parts format
	messageData := map[string]interface{}{
		"parts": []map[string]interface{}{
			{
				"text": text,
				"type": "text",
			},
		},
	}

	messageBytes, err := json.Marshal(messageData)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	messageURL := strings.TrimSuffix(url, "/") + "/session/" + h.app.Session.ID + "/message"
	req, err := http.NewRequest("POST", messageURL, strings.NewReader(string(messageBytes)))
	if err != nil {
		return fmt.Errorf("failed to create message request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status code %d: %s", resp.StatusCode, string(body))
	}

	// Success! Notify other panes that a message was sent
	h.notifyMessageSent(text)

	return nil
}

// syncWithSharedSession tries to sync with a session selected in other panes
func (h *InputHandler) syncWithSharedSession() bool {
	sharedState, err := h.stateManager.GetState()
	if err != nil {
		slog.Warn("Failed to get shared state during sync", "error", err)
		return false
	}

	// Check if we need to sync
	if sharedState.CurrentSessionID == "" {
		// No session selected in shared state
		return false
	}

	if sharedState.CurrentSessionID == h.app.Session.ID {
		// Already synced with the shared session
		return false
	}

	// Sync with shared session
	slog.Info("Input pane syncing with shared session",
		"currentSession", h.app.Session.ID,
		"sharedSession", sharedState.CurrentSessionID,
		"updatedBy", sharedState.UpdatedBy,
		"lastUpdated", sharedState.LastUpdated.Format("15:04:05"))

	h.app.Session.ID = sharedState.CurrentSessionID
	h.app.Session.Title = sharedState.CurrentTitle

	return true
}

// notifySessionChange notifies other panes about session changes
func (h *InputHandler) notifySessionChange(sessionID, title string) {
	// Update shared state
	if err := h.stateManager.SetState(sessionID, title, "input"); err != nil {
		slog.Error("Failed to update shared state", "error", err, "sessionID", sessionID)
	}

	// Send IPC event if available
	if h.ipcClient != nil {
		event := ipc.NewEvent(ipc.EventSessionChanged, "input", &ipc.SessionChangedData{
			SessionID: sessionID,
			Title:     title,
		})
		if err := h.ipcClient.Send(event); err != nil {
			slog.Warn("Failed to send session change event", "error", err)
		} else {
			slog.Info("Successfully notified other panes of session change", "sessionID", sessionID)
		}
	}
}

// extractSessionID extracts session ID from IPC event data with multiple fallback methods
func (h *InputHandler) extractSessionID(event *ipc.Event) string {
	if event.Data == nil {
		return ""
	}

	// Method 1: Direct type assertion to SessionChangedData
	if sessionData, ok := event.Data.(*ipc.SessionChangedData); ok {
		return sessionData.SessionID
	}

	// Method 2: Value type assertion
	if sessionData, ok := event.Data.(ipc.SessionChangedData); ok {
		return sessionData.SessionID
	}

	// Method 3: Map interface access
	if data, ok := event.Data.(map[string]interface{}); ok {
		possibleKeys := []string{"session_id", "SessionID", "sessionId", "sessionID"}
		for _, key := range possibleKeys {
			if value, exists := data[key]; exists {
				if sessionID, ok := value.(string); ok && sessionID != "" {
					return sessionID
				}
			}
		}
	}

	return ""
}

// notifyMessageSent notifies other panes that a message was sent to the current session
func (h *InputHandler) notifyMessageSent(text string) {
	if h.app.Session.ID == "" {
		return
	}

	// Update shared state to indicate message activity
	if err := h.stateManager.SetState(h.app.Session.ID, h.app.Session.Title, "input-message"); err != nil {
		slog.Error("Failed to update shared state after message sent", "error", err)
	}

	// Send IPC event if available
	if h.ipcClient != nil {
		messageEvent := ipc.NewEvent(ipc.EventMessageSent, "input", &ipc.MessageData{
			SessionID: h.app.Session.ID,
			Text:      text,
			Timestamp: time.Now().Unix(),
		})
		if err := h.ipcClient.Send(messageEvent); err != nil {
			slog.Warn("Failed to send message sent event", "error", err)
		} else {
			slog.Info("Successfully notified other panes of message sent", "sessionID", h.app.Session.ID)
		}
	}
}

// startPeriodicSessionSync runs periodic session synchronization to catch missed events
func (h *InputHandler) startPeriodicSessionSync(ctx context.Context) {
	var lastStateCheck time.Time
	ticker := time.NewTicker(500 * time.Millisecond) // More frequent checks for responsiveness
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Check if state has been updated since our last check
			stateChanged, err := h.stateManager.IsStateNewer(lastStateCheck)
			if err != nil {
				slog.Warn("Failed to check state freshness", "error", err)
				continue
			}

			if !stateChanged && h.app.Session.ID != "" {
				// No state changes and we have a session, skip this cycle
				continue
			}

			// Update our check timestamp
			lastStateCheck = time.Now()

			// Try to sync with current state
			if h.syncWithSharedSession() {
				slog.Info("Periodic sync: synced with shared session",
					"sessionID", h.app.Session.ID,
					"sessionTitle", h.app.Session.Title)
				h.displayWelcomeMessage()
			} else if h.app.Session.ID == "" {
				// Log periodically that we're waiting for session selection
				slog.Debug("Periodic sync: waiting for session selection")
			}
		}
	}
}

// maintainIPCConnection handles IPC connection with auto-reconnection
func (h *InputHandler) maintainIPCConnection(ctx context.Context) {
	connected := false
	retryDelay := 1 * time.Second
	maxRetryDelay := 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			if connected {
				h.ipcClient.Disconnect()
			}
			return
		default:
			if !connected {
				fmt.Printf("🔗 Attempting to connect to IPC server...\n")
				err := h.ipcClient.Connect(ctx)
				if err != nil {
					slog.Warn("Failed to connect to IPC server, retrying", "error", err, "retryDelay", retryDelay)
					fmt.Printf("⚠️  IPC connection failed, retrying in %v...\n", retryDelay)

					time.Sleep(retryDelay)
					// Exponential backoff with max limit
					retryDelay = time.Duration(float64(retryDelay) * 1.5)
					if retryDelay > maxRetryDelay {
						retryDelay = maxRetryDelay
					}
					continue
				}

				connected = true
				retryDelay = 1 * time.Second // Reset retry delay on success
				fmt.Printf("✅ Connected to IPC server successfully\n")
				slog.Info("IPC connection established")

				// Immediately try to sync session state after connection
				if h.syncWithSharedSession() {
					fmt.Printf("🔄 Synced session after IPC connection: %s\n", h.app.Session.ID)
					h.displayWelcomeMessage()
				}
			}

			// Check connection health every 5 seconds
			time.Sleep(5 * time.Second)

			// Simple health check - if we can't access the socket file, mark as disconnected
			socketPath := os.Getenv("OPENCODE_IPC_SOCKET")
			if socketPath != "" {
				if _, err := os.Stat(socketPath); err != nil {
					if connected {
						slog.Warn("IPC socket file disappeared, marking as disconnected", "socketPath", socketPath)
						fmt.Printf("⚠️  IPC connection lost, attempting to reconnect...\n")
						connected = false
						h.ipcClient.Disconnect()
					}
				}
			}
		}
	}
}

// debugSessionState provides detailed debugging information about current session state
func (h *InputHandler) debugSessionState() {
	fmt.Println("\n=== 🔍 Input Panel Session Debug ===")
	fmt.Printf("📋 Current Session ID: %s\n", h.app.Session.ID)
	fmt.Printf("📋 Current Session Title: %s\n", h.app.Session.Title)
	fmt.Printf("🔗 IPC Client Connected: %v\n", h.ipcClient != nil)

	// Check shared state file
	if sharedState, err := h.stateManager.GetState(); err == nil {
		fmt.Printf("📁 Shared State Session ID: %s\n", sharedState.CurrentSessionID)
		fmt.Printf("📁 Shared State Title: %s\n", sharedState.CurrentTitle)
		fmt.Printf("📁 Last Updated By: %s\n", sharedState.UpdatedBy)
		fmt.Printf("📁 Last Updated: %s\n", sharedState.LastUpdated.Format("15:04:05"))
	} else {
		fmt.Printf("❌ Failed to read shared state: %v\n", err)
	}

	// Check IPC socket
	socketPath := os.Getenv("OPENCODE_IPC_SOCKET")
	fmt.Printf("🔌 IPC Socket Path: %s\n", socketPath)
	if socketPath != "" {
		if _, err := os.Stat(socketPath); err == nil {
			fmt.Printf("✅ IPC Socket File Exists\n")
		} else {
			fmt.Printf("❌ IPC Socket File Missing: %v\n", err)
		}
	}

	fmt.Println("============================================")

	// Log to system as well
	slog.Info("Input Panel Session Debug",
		"sessionID", h.app.Session.ID,
		"sessionTitle", h.app.Session.Title,
		"ipcConnected", h.ipcClient != nil,
		"socketPath", socketPath)
}

// startSessionStateMonitoring continuously monitors session state for debugging
func (h *InputHandler) startSessionStateMonitoring(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second) // Debug every 5 seconds
	defer ticker.Stop()

	lastSessionID := h.app.Session.ID

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentSessionID := h.app.Session.ID
			if currentSessionID != lastSessionID {
				fmt.Printf("\n🔄 Session state changed: %s → %s\n", lastSessionID, currentSessionID)
				h.debugSessionState()
				lastSessionID = currentSessionID
			}

			// Log periodic state for debugging
			slog.Debug("Input Panel periodic state check",
				"sessionID", currentSessionID,
				"ipcConnected", h.ipcClient != nil)
		}
	}
}

// Simple completer for basic commands and files
func newCompleter(app *app.App) readline.AutoCompleter {
	return readline.NewPrefixCompleter(
		readline.PcItem("/help"),
		readline.PcItem("/clear"),
		readline.PcItem("/quit"),
		readline.PcItem("/exit"),
		readline.PcItem("/new"),
		readline.PcItem("/sessions"),
		readline.PcItem("/models"),
		readline.PcItem("/agents"),
	)
}