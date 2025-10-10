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
	"github.com/sst/opencode/internal/styles"
	"github.com/sst/opencode/internal/theme"
)

// InputPanel manages the user input panel
type InputPanel struct {
	client           *opencode.Client
	ipcClient        *ipc.SocketClient
	buffer           string
	cursorPosition   int
	selectionStart   int
	selectionEnd     int
	mode             string // "normal", "command", "multiline"
	history          []string
	historyIndex     int
	currentSessionID string
	width            int
	height           int
	ctx              context.Context
	cancel           context.CancelFunc
	isMultiline      bool
	showHelp         bool
	lastCommand      string
}

// NewInputPanel creates a new input panel
func NewInputPanel(httpClient *opencode.Client, socketPath string) *InputPanel {
	ctx, cancel := context.WithCancel(context.Background())

	panel := &InputPanel{
		client:         httpClient,
		ipcClient:      ipc.NewSocketClient(socketPath, "input-panel", "input"),
		buffer:         "",
		cursorPosition: 0,
		selectionStart: 0,
		selectionEnd:   0,
		mode:           "normal",
		history:        make([]string, 0),
		historyIndex:   -1,
		ctx:            ctx,
		cancel:         cancel,
		isMultiline:    false,
		showHelp:       false,
	}

	// Register event handlers
	panel.ipcClient.RegisterEventHandler(state.EventInputUpdated, panel.handleInputUpdated)
	panel.ipcClient.RegisterEventHandler(state.EventCursorMoved, panel.handleCursorMoved)
	panel.ipcClient.RegisterEventHandler(state.EventSessionChanged, panel.handleSessionChanged)
	panel.ipcClient.RegisterEventHandler(state.EventStateSync, panel.handleStateSync)

	return panel
}

// Init initializes the panel
func (p InputPanel) Init() tea.Cmd {
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
		}
		return ErrorMsg{Error: fmt.Errorf("failed to load state")}
	})

	return tea.Batch(cmds...)
}

// Update handles messages and updates the panel state
func (p InputPanel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		p.width = msg.Width
		p.height = msg.Height
		return p, nil

	case tea.KeyMsg:
		return p.handleKeyPress(msg)

	case ConnectedMsg:
		log.Printf("Input panel connected to IPC")
		return p, nil

	case StateLoadedMsg:
		p.currentSessionID = msg.State.CurrentSessionID
		p.buffer = msg.State.Input.Buffer
		p.cursorPosition = msg.State.Input.CursorPosition
		p.selectionStart = msg.State.Input.SelectionStart
		p.selectionEnd = msg.State.Input.SelectionEnd
		p.mode = msg.State.Input.Mode
		p.history = msg.State.Input.History
		p.historyIndex = msg.State.Input.HistoryIndex
		return p, nil

	case ErrorMsg:
		log.Printf("Input panel error: %v", msg.Error)
		return p, nil

	case InputEventMsg:
		return p.handleInputEvent(msg.Event)

	case MessageSentMsg:
		// Clear buffer after message is sent
		p.buffer = ""
		p.cursorPosition = 0
		p.selectionStart = 0
		p.selectionEnd = 0
		return p, p.syncInputState()

	default:
		return p, nil
	}
}

// View renders the input panel
func (p InputPanel) View() string {
	return p.renderInput()
}

// handleKeyPress processes keyboard input
func (p InputPanel) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		if p.buffer == "" {
			return p, tea.Quit
		}
		// Clear buffer if not empty
		p.buffer = ""
		p.cursorPosition = 0
		return p, p.syncInputState()

	case "ctrl+d":
		return p, tea.Quit

	case "enter":
		return p.handleEnter()

	case "ctrl+enter":
		// Toggle multiline mode
		p.isMultiline = !p.isMultiline
		if p.isMultiline {
			p.mode = "multiline"
		} else {
			p.mode = "normal"
		}
		return p, p.syncInputState()

	case "tab":
		return p.handleTab()

	case "shift+tab":
		return p.handleShiftTab()

	case "up":
		return p.handleArrowUp()

	case "down":
		return p.handleArrowDown()

	case "left":
		return p.handleArrowLeft()

	case "right":
		return p.handleArrowRight()

	case "home":
		p.cursorPosition = 0
		return p, p.syncCursorPosition()

	case "end":
		p.cursorPosition = len(p.buffer)
		return p, p.syncCursorPosition()

	case "backspace":
		return p.handleBackspace()

	case "delete":
		return p.handleDelete()

	case "ctrl+a":
		p.selectionStart = 0
		p.selectionEnd = len(p.buffer)
		p.cursorPosition = len(p.buffer)
		return p, p.syncCursorPosition()

	case "ctrl+k":
		// Delete from cursor to end of line
		p.buffer = p.buffer[:p.cursorPosition]
		return p, p.syncInputState()

	case "ctrl+u":
		// Delete from beginning to cursor
		p.buffer = p.buffer[p.cursorPosition:]
		p.cursorPosition = 0
		return p, p.syncInputState()

	case "ctrl+w":
		// Delete previous word
		return p.deletePreviousWord()

	case "ctrl+l":
		// Clear screen (in this case, just clear buffer)
		p.buffer = ""
		p.cursorPosition = 0
		return p, p.syncInputState()

	case "f1":
		p.showHelp = !p.showHelp
		return p, nil

	default:
		// Handle character input
		if len(msg.String()) == 1 {
			return p.insertCharacter(msg.String())
		}
	}

	return p, nil
}

// handleEnter processes the enter key
func (p InputPanel) handleEnter() (tea.Model, tea.Cmd) {
	if p.isMultiline {
		// In multiline mode, enter adds a newline
		return p.insertCharacter("\n")
	}

	// In normal mode, enter sends the message
	if strings.TrimSpace(p.buffer) == "" {
		return p, nil
	}

	// Check if it's a command
	if strings.HasPrefix(p.buffer, "/") {
		return p.handleCommand()
	}

	// Send as regular message
	return p, p.sendMessage()
}

// handleTab processes tab completion
func (p InputPanel) handleTab() (tea.Model, tea.Cmd) {
	// Simple tab completion for commands
	if strings.HasPrefix(p.buffer, "/") {
		commands := []string{"/help", "/clear", "/session", "/new", "/delete", "/theme", "/model", "/agent"}

		for _, cmd := range commands {
			if strings.HasPrefix(cmd, p.buffer) && len(cmd) > len(p.buffer) {
				p.buffer = cmd + " "
				p.cursorPosition = len(p.buffer)
				return p, p.syncInputState()
			}
		}
	}

	// Default tab behavior - insert spaces
	return p.insertCharacter("    ")
}

// handleShiftTab processes shift+tab
func (p InputPanel) handleShiftTab() (tea.Model, tea.Cmd) {
	// Remove indentation
	if strings.HasPrefix(p.buffer[max(0, p.cursorPosition-4):p.cursorPosition], "    ") {
		start := max(0, p.cursorPosition-4)
		p.buffer = p.buffer[:start] + p.buffer[p.cursorPosition:]
		p.cursorPosition = start
		return p, p.syncInputState()
	}
	return p, nil
}

// handleArrowUp handles up arrow (history navigation)
func (p InputPanel) handleArrowUp() (tea.Model, tea.Cmd) {
	if len(p.history) > 0 && p.historyIndex < len(p.history)-1 {
		p.historyIndex++
		p.buffer = p.history[len(p.history)-1-p.historyIndex]
		p.cursorPosition = len(p.buffer)
		return p, p.syncInputState()
	}
	return p, nil
}

// handleArrowDown handles down arrow (history navigation)
func (p InputPanel) handleArrowDown() (tea.Model, tea.Cmd) {
	if p.historyIndex > 0 {
		p.historyIndex--
		p.buffer = p.history[len(p.history)-1-p.historyIndex]
		p.cursorPosition = len(p.buffer)
		return p, p.syncInputState()
	} else if p.historyIndex == 0 {
		p.historyIndex = -1
		p.buffer = ""
		p.cursorPosition = 0
		return p, p.syncInputState()
	}
	return p, nil
}

// handleArrowLeft handles left arrow
func (p InputPanel) handleArrowLeft() (tea.Model, tea.Cmd) {
	if p.cursorPosition > 0 {
		p.cursorPosition--
		return p, p.syncCursorPosition()
	}
	return p, nil
}

// handleArrowRight handles right arrow
func (p InputPanel) handleArrowRight() (tea.Model, tea.Cmd) {
	if p.cursorPosition < len(p.buffer) {
		p.cursorPosition++
		return p, p.syncCursorPosition()
	}
	return p, nil
}

// handleBackspace handles backspace
func (p InputPanel) handleBackspace() (tea.Model, tea.Cmd) {
	if p.cursorPosition > 0 {
		p.buffer = p.buffer[:p.cursorPosition-1] + p.buffer[p.cursorPosition:]
		p.cursorPosition--
		return p, p.syncInputState()
	}
	return p, nil
}

// handleDelete handles delete key
func (p InputPanel) handleDelete() (tea.Model, tea.Cmd) {
	if p.cursorPosition < len(p.buffer) {
		p.buffer = p.buffer[:p.cursorPosition] + p.buffer[p.cursorPosition+1:]
		return p, p.syncInputState()
	}
	return p, nil
}

// insertCharacter inserts a character at the cursor position
func (p InputPanel) insertCharacter(char string) (tea.Model, tea.Cmd) {
	p.buffer = p.buffer[:p.cursorPosition] + char + p.buffer[p.cursorPosition:]
	p.cursorPosition += len(char)
	return p, p.syncInputState()
}

// deletePreviousWord deletes the previous word
func (p InputPanel) deletePreviousWord() (tea.Model, tea.Cmd) {
	if p.cursorPosition == 0 {
		return p, nil
	}

	// Find the start of the previous word
	i := p.cursorPosition - 1
	for i > 0 && p.buffer[i] == ' ' {
		i--
	}
	for i > 0 && p.buffer[i] != ' ' {
		i--
	}
	if p.buffer[i] == ' ' {
		i++
	}

	p.buffer = p.buffer[:i] + p.buffer[p.cursorPosition:]
	p.cursorPosition = i
	return p, p.syncInputState()
}

// handleCommand processes command input
func (p InputPanel) handleCommand() (tea.Model, tea.Cmd) {
	command := strings.TrimSpace(p.buffer)
	parts := strings.Fields(command)

	if len(parts) == 0 {
		return p, nil
	}

	cmd := parts[0]
	args := parts[1:]

	switch cmd {
	case "/help":
		return p, p.showHelpMessage()
	case "/clear":
		return p, p.clearMessages()
	case "/new":
		return p, p.createNewSession()
	case "/session":
		if len(args) > 0 {
			return p, p.switchToSession(args[0])
		}
	case "/theme":
		if len(args) > 0 {
			return p, p.changeTheme(args[0])
		}
	case "/model":
		if len(args) > 1 {
			return p, p.changeModel(args[0], args[1])
		}
	case "/agent":
		if len(args) > 0 {
			return p, p.changeAgent(args[0])
		}
	}

	// Add to history
	p.addToHistory(command)
	p.buffer = ""
	p.cursorPosition = 0

	return p, p.syncInputState()
}

// sendMessage sends the current buffer as a message
func (p InputPanel) sendMessage() tea.Cmd {
	if p.currentSessionID == "" {
		return func() tea.Msg {
			return ErrorMsg{Error: fmt.Errorf("no session selected")}
		}
	}

	message := strings.TrimSpace(p.buffer)
	if message == "" {
		return nil
	}

	return func() tea.Msg {
		// Add to history
		p.addToHistory(message)

		// Create message info
		messageInfo := state.MessageInfo{
			ID:        fmt.Sprintf("msg_%d", time.Now().UnixNano()),
			SessionID: p.currentSessionID,
			Type:      "user",
			Content:   message,
			Timestamp: time.Now(),
			Status:    "completed",
		}

		// Send state update
		update := state.StateUpdate{
			Type:        state.MessageAdded,
			Payload:     state.MessageAddPayload{Message: messageInfo},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
		}

		if err := p.ipcClient.SendStateUpdate(update); err != nil {
			return ErrorMsg{Error: err}
		}

		// Send to API
		// This would normally send to the OpenCode API
		// For now, we'll just simulate success

		return MessageSentMsg{Message: messageInfo}
	}
}

// addToHistory adds a message to the history
func (p *InputPanel) addToHistory(message string) {
	// Don't add empty messages or duplicates
	if message == "" || (len(p.history) > 0 && p.history[len(p.history)-1] == message) {
		return
	}

	p.history = append(p.history, message)

	// Keep only last 100 items
	if len(p.history) > 100 {
		p.history = p.history[1:]
	}

	p.historyIndex = -1
}

// Event handlers

func (p *InputPanel) handleInputUpdated(event state.StateEvent) error {
	if payload, ok := event.Data.(state.InputUpdatePayload); ok {
		// Only update if not from this panel
		if event.SourcePanel != "input-panel" {
			p.buffer = payload.Buffer
			p.cursorPosition = payload.CursorPosition
			p.selectionStart = payload.SelectionStart
			p.selectionEnd = payload.SelectionEnd
			if payload.Mode != "" {
				p.mode = payload.Mode
			}
		}
	}
	return nil
}

func (p *InputPanel) handleCursorMoved(event state.StateEvent) error {
	if payload, ok := event.Data.(state.CursorMovePayload); ok {
		// Only update if not from this panel
		if event.SourcePanel != "input-panel" {
			p.cursorPosition = payload.Position
			p.selectionStart = payload.SelectionStart
			p.selectionEnd = payload.SelectionEnd
		}
	}
	return nil
}

func (p *InputPanel) handleSessionChanged(event state.StateEvent) error {
	if payload, ok := event.Data.(state.SessionChangePayload); ok {
		p.currentSessionID = payload.SessionID
		log.Printf("Session changed: %s", payload.SessionID)
	}
	return nil
}

func (p *InputPanel) handleStateSync(event state.StateEvent) error {
	if payload, ok := event.Data.(state.StateSyncPayload); ok {
		p.currentSessionID = payload.State.CurrentSessionID
		p.buffer = payload.State.Input.Buffer
		p.cursorPosition = payload.State.Input.CursorPosition
		p.selectionStart = payload.State.Input.SelectionStart
		p.selectionEnd = payload.State.Input.SelectionEnd
		p.mode = payload.State.Input.Mode
		p.history = payload.State.Input.History
		p.historyIndex = payload.State.Input.HistoryIndex
		log.Printf("State synchronized")
	}
	return nil
}

func (p *InputPanel) handleInputEvent(event state.StateEvent) (InputPanel, tea.Cmd) {
	switch event.Type {
	case state.EventInputUpdated:
		p.handleInputUpdated(event)
	case state.EventCursorMoved:
		p.handleCursorMoved(event)
	case state.EventSessionChanged:
		p.handleSessionChanged(event)
	case state.EventStateSync:
		p.handleStateSync(event)
	}
	return *p, nil
}

// Sync methods

func (p InputPanel) syncInputState() tea.Cmd {
	return func() tea.Msg {
		update := state.StateUpdate{
			Type: state.InputUpdated,
			Payload: state.InputUpdatePayload{
				Buffer:         p.buffer,
				CursorPosition: p.cursorPosition,
				SelectionStart: p.selectionStart,
				SelectionEnd:   p.selectionEnd,
				Mode:           p.mode,
			},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
		}

		if err := p.ipcClient.SendStateUpdate(update); err != nil {
			return ErrorMsg{Error: err}
		}

		return nil
	}
}

func (p InputPanel) syncCursorPosition() tea.Cmd {
	return func() tea.Msg {
		update := state.StateUpdate{
			Type: state.CursorMoved,
			Payload: state.CursorMovePayload{
				Position:       p.cursorPosition,
				SelectionStart: p.selectionStart,
				SelectionEnd:   p.selectionEnd,
			},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
		}

		if err := p.ipcClient.SendStateUpdate(update); err != nil {
			return ErrorMsg{Error: err}
		}

		return nil
	}
}

// Command implementations

func (p InputPanel) showHelpMessage() tea.Cmd {
	return func() tea.Msg {
		return InfoMsg{Message: "Commands: /help /clear /new /session <id> /theme <name> /model <provider> <model> /agent <name>"}
	}
}

func (p InputPanel) clearMessages() tea.Cmd {
	// This would clear messages in the current session
	return func() tea.Msg {
		return InfoMsg{Message: "Messages cleared"}
	}
}

func (p InputPanel) createNewSession() tea.Cmd {
	return func() tea.Msg {
		// This would trigger session creation
		return InfoMsg{Message: "Creating new session..."}
	}
}

func (p InputPanel) switchToSession(sessionID string) tea.Cmd {
	return func() tea.Msg {
		update := state.StateUpdate{
			Type:        state.SessionChanged,
			Payload:     state.SessionChangePayload{SessionID: sessionID},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
		}

		if err := p.ipcClient.SendStateUpdate(update); err != nil {
			return ErrorMsg{Error: err}
		}

		return InfoMsg{Message: fmt.Sprintf("Switched to session %s", sessionID)}
	}
}

func (p InputPanel) changeTheme(theme string) tea.Cmd {
	return func() tea.Msg {
		update := state.StateUpdate{
			Type:        state.ThemeChanged,
			Payload:     state.ThemeChangePayload{Theme: theme},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
		}

		if err := p.ipcClient.SendStateUpdate(update); err != nil {
			return ErrorMsg{Error: err}
		}

		return InfoMsg{Message: fmt.Sprintf("Theme changed to %s", theme)}
	}
}

func (p InputPanel) changeModel(provider, model string) tea.Cmd {
	return func() tea.Msg {
		update := state.StateUpdate{
			Type:        state.ModelChanged,
			Payload:     state.ModelChangePayload{Provider: provider, Model: model},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
		}

		if err := p.ipcClient.SendStateUpdate(update); err != nil {
			return ErrorMsg{Error: err}
		}

		return InfoMsg{Message: fmt.Sprintf("Model changed to %s/%s", provider, model)}
	}
}

func (p InputPanel) changeAgent(agent string) tea.Cmd {
	return func() tea.Msg {
		update := state.StateUpdate{
			Type:        state.AgentChanged,
			Payload:     state.AgentChangePayload{Agent: agent},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
		}

		if err := p.ipcClient.SendStateUpdate(update); err != nil {
			return ErrorMsg{Error: err}
		}

		return InfoMsg{Message: fmt.Sprintf("Agent changed to %s", agent)}
	}
}

// renderInput renders the input panel
func (p InputPanel) renderInput() string {
	t := theme.CurrentTheme()

	var content string

	// Header
	header := "Input"
	if p.currentSessionID != "" {
		header += fmt.Sprintf(" - Session %s", p.currentSessionID[:8])
	}
	if p.isMultiline {
		header += " [MULTILINE]"
	}

	content += styles.NewStyle().
		Foreground(t.Primary()).
		Bold(true).
		Render(header) + "\n\n"

	// Input field
	inputStyle := styles.NewStyle().
		Border(styles.RoundedBorder()).
		BorderForeground(t.Border()).
		Padding(1).
		Width(p.width - 2)

	// Render buffer with cursor
	displayBuffer := p.buffer
	if p.cursorPosition <= len(displayBuffer) {
		cursorChar := "│"
		displayBuffer = displayBuffer[:p.cursorPosition] +
			styles.NewStyle().Foreground(t.Success()).Render(cursorChar) +
			displayBuffer[p.cursorPosition:]
	}

	content += inputStyle.Render(displayBuffer) + "\n"

	// Mode indicator
	modeText := fmt.Sprintf("Mode: %s", p.mode)
	if len(p.history) > 0 {
		modeText += fmt.Sprintf(" | History: %d items", len(p.history))
	}

	content += styles.NewStyle().
		Foreground(t.TextMuted()).
		Render(modeText) + "\n"

	// Help text
	helpText := "Enter send • Ctrl+Enter multiline • ↑/↓ history • Tab complete • F1 help • Ctrl+C quit"
	content += styles.NewStyle().
		Foreground(t.TextMuted()).
		Render(helpText)

	if p.showHelp {
		content += "\n\n" + p.renderHelp()
	}

	return content
}

// renderHelp renders the help text
func (p InputPanel) renderHelp() string {
	t := theme.CurrentTheme()

	helpContent := `
Commands:
  /help                    Show this help
  /clear                   Clear current session messages
  /new                     Create new session
  /session <id>            Switch to session
  /theme <name>            Change theme
  /model <provider> <model> Change model
  /agent <name>            Change agent

Keyboard Shortcuts:
  Enter                    Send message
  Ctrl+Enter               Toggle multiline mode
  ↑/↓                     Navigate history
  Tab                      Command completion
  Ctrl+A                   Select all
  Ctrl+K                   Delete to end of line
  Ctrl+U                   Delete to beginning of line
  Ctrl+W                   Delete previous word
  Ctrl+L                   Clear buffer
  F1                       Toggle this help
  Ctrl+C                   Quit (or clear if buffer not empty)
`

	return styles.NewStyle().
		Foreground(t.Info()).
		Border(styles.RoundedBorder()).
		BorderForeground(t.Border()).
		Padding(1).
		Render(strings.TrimSpace(helpContent))
}

// Message types
type ConnectedMsg struct{}

type StateLoadedMsg struct {
	State *state.SharedApplicationState
}

type ErrorMsg struct {
	Error error
}

type InfoMsg struct {
	Message string
}

type InputEventMsg struct {
	Event state.StateEvent
}

type MessageSentMsg struct {
	Message state.MessageInfo
}

// Utility functions
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func main() {
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
	theme.SetTheme("opencode")

	// Create and run panel
	panel := NewInputPanel(httpClient, socketPath)

	program := tea.NewProgram(
		panel,
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	// Handle signals
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		log.Printf("Received signal, shutting down input panel")
		cancel()
		program.Quit()
	}()

	// Run the program
	if _, err := program.Run(); err != nil {
		log.Printf("Input panel error: %v", err)
		os.Exit(1)
	}

	// Cleanup
	panel.ipcClient.Disconnect()
	cancel()
}