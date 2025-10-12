package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea/v2"
	"github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
	"github.com/sst/opencode/internal/ipc"
	"github.com/sst/opencode/internal/state"
	"github.com/sst/opencode/internal/types"
	"github.com/sst/opencode/internal/styles"
	"github.com/sst/opencode/internal/theme"
)

// MessagesPanel manages the message history panel
type MessagesPanel struct {
	client           *opencode.Client
	ipcClient        *ipc.SocketClient
	messages         []types.MessageInfo
	currentSessionID string
	scrollOffset     int
	width            int
	height           int
	ctx              context.Context
	cancel           context.CancelFunc
	autoScroll       bool
	showTimestamps   bool
	isStreaming      bool
	currentMessage   *types.MessageInfo
}

// NewMessagesPanel creates a new messages panel
func NewMessagesPanel(httpClient *opencode.Client, socketPath string) *MessagesPanel {
	ctx, cancel := context.WithCancel(context.Background())

	panel := &MessagesPanel{
		client:         httpClient,
		ipcClient:      ipc.NewSocketClient(socketPath, "messages-panel", "messages"),
		messages:       make([]types.MessageInfo, 0),
		scrollOffset:   0,
		autoScroll:     true,
		showTimestamps: false,
		ctx:            ctx,
		cancel:         cancel,
	}

	// Register event handlers
	panel.ipcClient.RegisterEventHandler(state.EventMessageAdded, panel.handleMessageAdded)
	panel.ipcClient.RegisterEventHandler(state.EventMessageUpdated, panel.handleMessageUpdated)
	panel.ipcClient.RegisterEventHandler(state.EventMessageDeleted, panel.handleMessageDeleted)
	panel.ipcClient.RegisterEventHandler(state.EventSessionChanged, panel.handleSessionChanged)
	panel.ipcClient.RegisterEventHandler(state.EventStateSync, panel.handleStateSync)

	return panel
}

// Init initializes the panel
func (p MessagesPanel) Init() tea.Cmd {
	var cmds []tea.Cmd

	// Connect to IPC server
	cmds = append(cmds, func() tea.Msg {
		if err := p.ipcClient.Connect(); err != nil {
			return ErrorMsg{Error: fmt.Errorf("failed to connect to IPC: %w", err)}
		}
		return ConnectedMsg{}
	})

	// Request initial state
	cmds = append(cmds, func() tea.Msg {
		time.Sleep(100 * time.Millisecond) // Wait for connection
		if currentState, err := p.ipcClient.RequestState(); err == nil {
			return StateLoadedMsg{State: currentState}
		}else{
     		  log.Printf("Sessions panel initial state err:%v",err)
    }
		return ErrorMsg{Error: fmt.Errorf("failed to load state")}
	})

	// Start streaming updates
	cmds = append(cmds, p.startEventStream())

	return tea.Batch(cmds...)
}

// Update handles messages and updates the panel state
func (p MessagesPanel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		p.width = msg.Width
		p.height = msg.Height
		return p, nil

	case tea.KeyMsg:
		return p.handleKeyPress(msg)

	case ConnectedMsg:
		log.Printf("Messages panel connected to IPC")
		return p, nil

	case StateLoadedMsg:
		p.currentSessionID = msg.State.CurrentSessionID
		p.messages = p.filterMessagesForSession(msg.State.Messages, p.currentSessionID)
		if p.autoScroll {
			p.scrollToBottom()
		}
		return p, nil

	case ErrorMsg:
		log.Printf("Messages panel error: %v", msg.Error)
		return p, nil

	case MessageEventMsg:
		return p.handleMessageEvent(msg.Event)

	case StreamingUpdateMsg:
		return p.handleStreamingUpdate(msg)

	default:
		return p, nil
	}
}

// View renders the messages panel
func (p MessagesPanel) View() string {
	if len(p.messages) == 0 {
		return p.renderEmptyState()
	}

	return p.renderMessages()
}

// handleKeyPress processes keyboard input
func (p MessagesPanel) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		return p, tea.Quit

	case "up", "k":
		if p.scrollOffset > 0 {
			p.scrollOffset--
		}
		p.autoScroll = false

	case "down", "j":
		maxScroll := p.calculateMaxScroll()
		if p.scrollOffset < maxScroll {
			p.scrollOffset++
		}
		// Re-enable auto scroll if at bottom
		if p.scrollOffset >= maxScroll {
			p.autoScroll = true
		}

	case "page_up":
		p.scrollOffset = max(0, p.scrollOffset-p.height/2)
		p.autoScroll = false

	case "page_down":
		maxScroll := p.calculateMaxScroll()
		p.scrollOffset = min(maxScroll, p.scrollOffset+p.height/2)
		if p.scrollOffset >= maxScroll {
			p.autoScroll = true
		}

	case "home":
		p.scrollOffset = 0
		p.autoScroll = false

	case "end":
		p.scrollToBottom()
		p.autoScroll = true

	case "t":
		p.showTimestamps = !p.showTimestamps

	case "a":
		p.autoScroll = !p.autoScroll
		if p.autoScroll {
			p.scrollToBottom()
		}

	case "r":
		return p, p.refreshMessages()
	}

	return p, nil
}

// refreshMessages refreshes messages from the API
func (p MessagesPanel) refreshMessages() tea.Cmd {
	if p.currentSessionID == "" {
		return nil
	}

	return func() tea.Msg {
		messages, err := p.client.Session.Messages(p.ctx, p.currentSessionID, opencode.SessionMessagesParams{})
		if err != nil {
			return ErrorMsg{Error: fmt.Errorf("failed to refresh messages: %w", err)}
		}

		// Convert to state format
		messageInfos := make([]types.MessageInfo, 0)
		for _, message := range *messages {
			var messageType string
			var content string

			switch msg := message.Info.AsUnion().(type) {
			case opencode.UserMessage:
				messageType = "user"
				var contentParts []string
				for _, part := range message.Parts {
					if textPart, ok := part.AsUnion().(opencode.TextPart); ok {
						contentParts = append(contentParts, textPart.Text)
					}
				}
				content = strings.Join(contentParts, "\n")
			case opencode.AssistantMessage:
				messageType = "assistant"
				// Extract content from parts if available
				if len(message.Parts) > 0 {
					var contentParts []string
					for _, part := range message.Parts {
						if textPart, ok := part.AsUnion().(opencode.TextPart); ok {
							contentParts = append(contentParts, textPart.Text)
						}
					}
					content = strings.Join(contentParts, "\n")
				}
			default:
				messageType = "system"
				content = fmt.Sprintf("Unknown message type: %T", msg)
			}

			messageInfo := types.MessageInfo{
				ID:        message.Info.ID,
				SessionID: p.currentSessionID,
				Type:      messageType,
				Content:   content,
				Timestamp: time.Now(), // You might want to extract actual timestamp
				Status:    "completed",
			}

			messageInfos = append(messageInfos, messageInfo)
		}

		return MessagesRefreshedMsg{Messages: messageInfos}
	}
}

// startEventStream starts listening for streaming events
func (p MessagesPanel) startEventStream() tea.Cmd {
	return func() tea.Msg {
		// This would normally set up streaming from the OpenCode API
		// For now, we'll just return a placeholder
		return StreamingStartedMsg{}
	}
}

// Event handlers

func (p *MessagesPanel) handleMessageAdded(event state.StateEvent) error {
	if payload, ok := event.Data.(state.MessageAddPayload); ok {
		if payload.Message.SessionID == p.currentSessionID {
			p.messages = append(p.messages, payload.Message)
			if p.autoScroll {
				p.scrollToBottom()
			}
		}
		log.Printf("Message added: %s", payload.Message.ID)
	}
	return nil
}

func (p *MessagesPanel) handleMessageUpdated(event state.StateEvent) error {
	if payload, ok := event.Data.(state.MessageUpdatePayload); ok {
		for i, message := range p.messages {
			if message.ID == payload.MessageID {
				if payload.Content != "" {
					p.messages[i].Content = payload.Content
				}
				if payload.Status != "" {
					p.messages[i].Status = payload.Status
				}
				p.messages[i].Timestamp = time.Now()
				break
			}
		}
		log.Printf("Message updated: %s", payload.MessageID)
	}
	return nil
}

func (p *MessagesPanel) handleMessageDeleted(event state.StateEvent) error {
	if payload, ok := event.Data.(state.MessageDeletePayload); ok {
		for i, message := range p.messages {
			if message.ID == payload.MessageID {
				p.messages = append(p.messages[:i], p.messages[i+1:]...)
				break
			}
		}
		log.Printf("Message deleted: %s", payload.MessageID)
	}
	return nil
}

func (p *MessagesPanel) handleSessionChanged(event state.StateEvent) error {
	if payload, ok := event.Data.(state.SessionChangePayload); ok {
		p.currentSessionID = payload.SessionID
		// Filter messages for new session
		// In a real implementation, you'd load messages for the new session
		p.messages = make([]types.MessageInfo, 0)
		p.scrollOffset = 0
		log.Printf("Session changed: %s", payload.SessionID)
	}
	return nil
}

func (p *MessagesPanel) handleStateSync(event state.StateEvent) error {
	if payload, ok := event.Data.(types.StateSyncPayload); ok {
		p.currentSessionID = payload.State.CurrentSessionID
		p.messages = p.filterMessagesForSession(payload.State.Messages, p.currentSessionID)
		if p.autoScroll {
			p.scrollToBottom()
		}
		log.Printf("State synchronized")
	}
	return nil
}

func (p *MessagesPanel) handleMessageEvent(event state.StateEvent) (MessagesPanel, tea.Cmd) {
	switch event.Type {
	case state.EventMessageAdded:
		p.handleMessageAdded(event)
	case state.EventMessageUpdated:
		p.handleMessageUpdated(event)
	case state.EventMessageDeleted:
		p.handleMessageDeleted(event)
	case state.EventSessionChanged:
		p.handleSessionChanged(event)
	case state.EventStateSync:
		p.handleStateSync(event)
	}
	return *p, nil
}

func (p *MessagesPanel) handleStreamingUpdate(msg StreamingUpdateMsg) (MessagesPanel, tea.Cmd) {
	if msg.MessageID != "" {
		// Update existing message with streaming content
		for i, message := range p.messages {
			if message.ID == msg.MessageID {
				p.messages[i].Content = msg.Content
				p.messages[i].Status = msg.Status
				break
			}
		}
	}
	return *p, nil
}

// filterMessagesForSession filters messages for a specific session
func (p *MessagesPanel) filterMessagesForSession(messages []types.MessageInfo, sessionID string) []types.MessageInfo {
	filtered := make([]types.MessageInfo, 0)
	for _, message := range messages {
		if message.SessionID == sessionID {
			filtered = append(filtered, message)
		}
	}
	return filtered
}

// calculateMaxScroll calculates the maximum scroll offset
func (p *MessagesPanel) calculateMaxScroll() int {
	visibleLines := p.height - 2 // Account for header
	totalLines := len(p.messages) * 3 // Approximate lines per message
	return max(0, totalLines-visibleLines)
}

// scrollToBottom scrolls to the bottom of the messages
func (p *MessagesPanel) scrollToBottom() {
	p.scrollOffset = p.calculateMaxScroll()
}

// renderEmptyState renders the empty messages state
func (p MessagesPanel) renderEmptyState() string {
	t := theme.CurrentTheme()
	style := styles.NewStyle().
		Foreground(t.TextMuted()).
		Align(styles.Center).
		Width(p.width).
		Height(p.height)

	if p.currentSessionID == "" {
		return style.Render("No session selected\n\nSelect a session to view messages")
	}

	return style.Render("No messages in this session\n\nStart a conversation in the input panel")
}

// renderMessages renders the list of messages
func (p MessagesPanel) renderMessages() string {
	t := theme.CurrentTheme()

	var content string

	// Header
	header := "Messages"
	if p.currentSessionID != "" {
		header += fmt.Sprintf(" - Session %s", p.currentSessionID[:8])
	}
	if p.isStreaming {
		header += " [STREAMING]"
	}

	content += styles.NewStyle().
		Foreground(t.Primary()).
		Bold(true).
		Render(header) + "\n\n"

	// Calculate visible messages based on scroll
	visibleMessages := p.calculateVisibleMessages()

	for _, message := range visibleMessages {
		content += p.renderMessage(message) + "\n"
	}

	// Footer with scroll indicator
	if p.calculateMaxScroll() > 0 {
		scrollIndicator := fmt.Sprintf("[%d/%d]", p.scrollOffset, p.calculateMaxScroll())
		if !p.autoScroll {
			scrollIndicator += " [MANUAL]"
		}

		content += "\n" + styles.NewStyle().
			Foreground(t.TextMuted()).
			Align(styles.Right).
			Width(p.width).
			Render(scrollIndicator)
	}

	// Help text
	content += "\n" + styles.NewStyle().
		Foreground(t.TextMuted()).
		Render("↑/k up • ↓/j down • PgUp/PgDn page • Home/End • t timestamps • a auto-scroll • r refresh • q quit")

	return content
}

// calculateVisibleMessages returns messages visible in the current scroll view
func (p *MessagesPanel) calculateVisibleMessages() []types.MessageInfo {
	if len(p.messages) == 0 {
		return []types.MessageInfo{}
	}

	// Simple implementation - show all messages for now
	// In a more sophisticated implementation, you'd calculate based on actual rendered height
	return p.messages
}

// renderMessage renders a single message
func (p MessagesPanel) renderMessage(message types.MessageInfo) string {
	t := theme.CurrentTheme()

	var style styles.Style
	var prefix string

	switch message.Type {
	case "user":
		style = styles.NewStyle().Foreground(t.Success())
		prefix = "You: "
	case "assistant":
		style = styles.NewStyle().Foreground(t.Info())
		prefix = "Assistant: "
	case "system":
		style = styles.NewStyle().Foreground(t.Warning())
		prefix = "System: "
	default:
		style = styles.NewStyle().Foreground(t.Text())
		prefix = fmt.Sprintf("%s: ", message.Type)
	}

	content := prefix + message.Content

	// Add timestamp if enabled
	if p.showTimestamps {
		timestamp := message.Timestamp.Format("15:04:05")
		content = fmt.Sprintf("[%s] %s", timestamp, content)
	}

	// Add status indicator for pending messages
	if message.Status == "pending" {
		content += " ⏳"
	} else if message.Status == "error" {
		content += " ❌"
	}

	// Word wrap content to fit width
	if p.width > 0 {
		content = p.wordWrap(content, p.width-2)
	}

	return style.Render(content)
}

// wordWrap wraps text to fit within specified width
func (p MessagesPanel) wordWrap(text string, width int) string {
	if width <= 0 {
		return text
	}

	words := strings.Fields(text)
	if len(words) == 0 {
		return text
	}

	var lines []string
	var currentLine string

	for _, word := range words {
		if len(currentLine)+len(word)+1 <= width {
			if currentLine == "" {
				currentLine = word
			} else {
				currentLine += " " + word
			}
		} else {
			if currentLine != "" {
				lines = append(lines, currentLine)
			}
			currentLine = word
		}
	}

	if currentLine != "" {
		lines = append(lines, currentLine)
	}

	return strings.Join(lines, "\n")
}

// Message types
type ConnectedMsg struct{}

type StateLoadedMsg struct {
	State *state.SharedApplicationState
}

type ErrorMsg struct {
	Error error
}

type MessageEventMsg struct {
	Event state.StateEvent
}

type MessagesRefreshedMsg struct {
	Messages []types.MessageInfo
}

type StreamingStartedMsg struct{}

type StreamingUpdateMsg struct {
	MessageID string
	Content   string
	Status    string
}

// Utility functions
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	// Configure logging to file
	logFileHomeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("Failed to get home directory: %v", err)
	}
	logDir := filepath.Join(logFileHomeDir, ".opencode")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("Failed to create log directory: %v", err)
	}
	logPath := filepath.Join(logDir, "messages.log")
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	log.SetOutput(logFile)

	// Get environment variables
	serverURL := os.Getenv("OPENCODE_SERVER")
	if serverURL == "" {
		log.Fatal("OPENCODE_SERVER environment variable not set")
	}

	socketPath := os.Getenv("OPENCODE_SOCKET")
	if socketPath == "" {
		// Default socket path
		homeDir, _ := os.UserHomeDir()
		socketPath = filepath.Join(homeDir, ".opencode", "ipc.sock")
	}

	// Create HTTP client
	httpClient := opencode.NewClient(option.WithBaseURL(serverURL))

	// Initialize theme
	if err := theme.LoadThemesFromJSON(); err != nil {
		log.Fatal("Failed to load themes:", err)
	}
	if err := theme.SetTheme("opencode"); err != nil {
		log.Fatal("Failed to set theme:", err)
	}

	// Create and run panel
	panel := NewMessagesPanel(httpClient, socketPath)

	program := tea.NewProgram(
		panel,
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	// Handle signals
	_, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		log.Printf("Received signal, shutting down messages panel")
		cancel()
		program.Quit()
	}()

	// Run the program
	if _, err := program.Run(); err != nil {
		log.Printf("Messages panel error: %v", err)
		os.Exit(1)
	}

	// Cleanup
	panel.ipcClient.Disconnect()
	cancel()
}
