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
	"github.com/sst/opencode/internal/types"
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
	version          int64
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
func (p *InputPanel) Init() tea.Cmd {
	var cmds []tea.Cmd

	// Connect to IPC server with retry
	cmds = append(cmds, func() tea.Msg {
		socketPath := os.Getenv("OPENCODE_SOCKET")
		log.Printf("[INPUT] Attempting to connect to IPC server at %s", socketPath)

		// Try to connect with retries
		var lastErr error
		for attempt := 1; attempt <= 3; attempt++ {
			if err := p.ipcClient.Connect(); err != nil {
				lastErr = err
				log.Printf("[INPUT] Connection attempt %d failed: %v", attempt, err)
				if attempt < 3 {
					time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
				}
				continue
			}
			log.Printf("[INPUT] Successfully connected to IPC server on attempt %d", attempt)
			return ConnectedMsg{}
		}

		return ErrorMsg{Error: fmt.Errorf("failed to connect to IPC after 3 attempts: %w", lastErr)}
	})

	// Request initial state with better error handling
	cmds = append(cmds, func() tea.Msg {
		// Wait longer for connection to establish
		time.Sleep(500 * time.Millisecond)

		log.Printf("[INPUT] Requesting initial state from IPC server")
		if currentState, err := p.ipcClient.RequestState(); err == nil {
			log.Printf("[INPUT] Successfully loaded initial state")
			return StateLoadedMsg{State: currentState}
		} else {
			log.Printf("[INPUT] Failed to load initial state: %v", err)
			// Don't treat this as fatal - continue with empty state
			return StateLoadedMsg{State: nil}
		}
	})

	return tea.Batch(cmds...)
}

// Update handles messages and updates the panel state
func (p *InputPanel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
		if msg.State != nil {
			log.Printf("[INPUT] Loading state from IPC server, version %d", msg.State.Version.Version)
			p.currentSessionID = msg.State.CurrentSessionID
			p.buffer = msg.State.Input.Buffer
			p.cursorPosition = msg.State.Input.CursorPosition
			p.selectionStart = msg.State.Input.SelectionStart
			p.selectionEnd = msg.State.Input.SelectionEnd
			p.mode = msg.State.Input.Mode
			p.history = msg.State.Input.History
			p.historyIndex = msg.State.Input.HistoryIndex
			p.version = msg.State.Version.Version
		} else {
			log.Printf("[INPUT] No state available, using defaults")
			// Initialize with default values
			p.currentSessionID = ""
			p.buffer = ""
			p.cursorPosition = 0
			p.selectionStart = 0
			p.selectionEnd = 0
			p.mode = "normal"
			p.history = make([]string, 0)
			p.historyIndex = -1
		}
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
func (p *InputPanel) View() string {
	return p.renderInput()
}

// handleKeyPress processes keyboard input
func (p *InputPanel) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
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
		keyStr := msg.String()
		log.Printf("[INPUT] Key pressed: %q (length: %d, runes: %d)", keyStr, len(keyStr), len([]rune(keyStr)))

		// Check if it's a printable character (including space and Unicode)
		runes := []rune(keyStr)
		if len(runes) == 1 {
			char := runes[0]
			// Allow printable characters including space (32) and Unicode characters
			if char >= 32 || char == 9 { // 32 = space, 9 = tab
				log.Printf("[INPUT] Inserting character: %q (Unicode: U+%04X)", keyStr, char)
				return p.insertCharacter(keyStr)
			} else {
				log.Printf("[INPUT] Ignoring non-printable character: %q (Unicode: U+%04X)", keyStr, char)
			}
		} else if len(runes) > 1 {
			log.Printf("[INPUT] Multi-character key sequence ignored: %q", keyStr)
		}
	}

	return p, nil
}

// handleEnter processes the enter key
func (p *InputPanel) handleEnter() (tea.Model, tea.Cmd) {
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
func (p *InputPanel) handleTab() (tea.Model, tea.Cmd) {
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
func (p *InputPanel) handleShiftTab() (tea.Model, tea.Cmd) {
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
func (p *InputPanel) handleArrowUp() (tea.Model, tea.Cmd) {
	if len(p.history) > 0 && p.historyIndex < len(p.history)-1 {
		p.historyIndex++
		p.buffer = p.history[len(p.history)-1-p.historyIndex]
		p.cursorPosition = len(p.buffer)
		return p, p.syncInputState()
	}
	return p, nil
}

// handleArrowDown handles down arrow (history navigation)
func (p *InputPanel) handleArrowDown() (tea.Model, tea.Cmd) {
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
func (p *InputPanel) handleArrowLeft() (tea.Model, tea.Cmd) {
	if p.cursorPosition > 0 {
		p.cursorPosition--
		return p, p.syncCursorPosition()
	}
	return p, nil
}

// handleArrowRight handles right arrow
func (p *InputPanel) handleArrowRight() (tea.Model, tea.Cmd) {
	if p.cursorPosition < len(p.buffer) {
		p.cursorPosition++
		return p, p.syncCursorPosition()
	}
	return p, nil
}

// handleBackspace handles backspace
func (p *InputPanel) handleBackspace() (tea.Model, tea.Cmd) {
	if p.cursorPosition > 0 {
		p.buffer = p.buffer[:p.cursorPosition-1] + p.buffer[p.cursorPosition:]
		p.cursorPosition--
		return p, p.syncInputState()
	}
	return p, nil
}

// handleDelete handles delete key
func (p *InputPanel) handleDelete() (tea.Model, tea.Cmd) {
	if p.cursorPosition < len(p.buffer) {
		p.buffer = p.buffer[:p.cursorPosition] + p.buffer[p.cursorPosition+1:]
		return p, p.syncInputState()
	}
	return p, nil
}

// insertCharacter inserts a character at the cursor position
func (p *InputPanel) insertCharacter(char string) (tea.Model, tea.Cmd) {
	p.buffer = p.buffer[:p.cursorPosition] + char + p.buffer[p.cursorPosition:]
	p.cursorPosition += len(char)
	return p, p.syncInputState()
}

// deletePreviousWord deletes the previous word
func (p *InputPanel) deletePreviousWord() (tea.Model, tea.Cmd) {
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
func (p *InputPanel) handleCommand() (tea.Model, tea.Cmd) {
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
	case "/delete":
		if len(args) > 0 {
			return p, p.deleteSession(args[0])
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
func (p *InputPanel) sendMessage() tea.Cmd {
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
		messageInfo := types.MessageInfo{
			ID:        fmt.Sprintf("msg_%d", time.Now().UnixNano()),
			SessionID: p.currentSessionID,
			Type:      "user",
			Content:   message,
			Timestamp: time.Now(),
			Status:    "completed",
		}

		// Send state update
		update := types.StateUpdate{
			Type:            types.MessageAdded,
			Payload:         types.MessageAddPayload{Message: messageInfo},
			SourcePanel:     "input-panel",
			Timestamp:       time.Now(),
			ExpectedVersion: p.version,
		}

		if newVersion, err := p.ipcClient.SendStateUpdateAndWait(update); err != nil {
			return ErrorMsg{Error: err}
		} else {
			p.version = newVersion
		}

		// Send to OpenCode API
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		response, err := p.client.Session.Prompt(ctx, p.currentSessionID, opencode.SessionPromptParams{
			Parts: opencode.F([]opencode.SessionPromptParamsPartUnion{
				opencode.TextPartInputParam{
					Text: opencode.F(message),
					Type: opencode.F(opencode.TextPartInputTypeText),
				},
			}),
		})

		if err != nil {
			log.Printf("[INPUT] Failed to send message to OpenCode API: %v", err)
			return ErrorMsg{Error: fmt.Errorf("failed to send message to OpenCode: %w", err)}
		}

		log.Printf("[INPUT] Successfully sent message to OpenCode API, response received")

		// Create assistant message from response
		if response != nil {
			// Check for errors in the response
			if response.Info.Error.Name != "" {
				log.Printf("[INPUT] Assistant message error: %s - %v", response.Info.Error.Name, response.Info.Error.Data)
				return ErrorMsg{Error: fmt.Errorf("assistant error: %s", response.Info.Error.Name)}
			}

			assistantMessage := types.MessageInfo{
				ID:        response.Info.ID, // Use the actual message ID from response
				SessionID: p.currentSessionID,
				Type:      "assistant",
				Content:   "", // Will be populated from response parts
				Timestamp: time.Now(),
				Status:    "completed",
			}

			// Extract content from response parts
			if len(response.Parts) > 0 {
				var contentParts []string
				for _, part := range response.Parts {
					// Filter for text parts and skip synthetic parts
					if part.Type == opencode.PartTypeText && !part.Synthetic {
						if strings.TrimSpace(part.Text) != "" {
							contentParts = append(contentParts, part.Text)
						}
					}
				}
				assistantMessage.Content = strings.Join(contentParts, "\n")
			}

			// Send assistant message state update
			assistantUpdate := types.StateUpdate{
				Type:            types.MessageAdded,
				Payload:         types.MessageAddPayload{Message: assistantMessage},
				SourcePanel:     "input-panel",
				Timestamp:       time.Now(),
				ExpectedVersion: p.version,
			}

			if newVersion, err := p.ipcClient.SendStateUpdateAndWait(assistantUpdate); err != nil {
				log.Printf("[INPUT] Failed to send assistant message state update: %v", err)
			} else {
				p.version = newVersion
				log.Printf("[INPUT] Successfully added assistant response to state")
			}
		}

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

func (p *InputPanel) handleInputUpdated(event types.StateEvent) error {
	if payload, ok := event.Data.(types.InputUpdatePayload); ok {
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

func (p *InputPanel) handleCursorMoved(event types.StateEvent) error {
	if payload, ok := event.Data.(types.CursorMovePayload); ok {
		// Only update if not from this panel
		if event.SourcePanel != "input-panel" {
			p.cursorPosition = payload.Position
			p.selectionStart = payload.SelectionStart
			p.selectionEnd = payload.SelectionEnd
		}
	}
	return nil
}

func (p *InputPanel) handleSessionChanged(event types.StateEvent) error {
	if payload, ok := event.Data.(types.SessionChangePayload); ok {
		p.currentSessionID = payload.SessionID
		log.Printf("Session changed: %s", payload.SessionID)
	}
	return nil
}

func (p *InputPanel) handleStateSync(event types.StateEvent) error {
	if payload, ok := event.Data.(types.StateSyncPayload); ok {
		p.currentSessionID = payload.State.CurrentSessionID
		p.buffer = payload.State.Input.Buffer
		p.cursorPosition = payload.State.Input.CursorPosition
		p.selectionStart = payload.State.Input.SelectionStart
		p.selectionEnd = payload.State.Input.SelectionEnd
		p.mode = payload.State.Input.Mode
		p.history = payload.State.Input.History
		p.historyIndex = payload.State.Input.HistoryIndex
		p.version = payload.State.Version.Version
		log.Printf("State synchronized, version set to %d", p.version)
	}
	return nil
}

func (p *InputPanel) handleInputEvent(event types.StateEvent) (tea.Model, tea.Cmd) {
	switch event.Type {
	case types.EventInputUpdated:
		p.handleInputUpdated(event)
	case types.EventCursorMoved:
		p.handleCursorMoved(event)
	case types.EventSessionChanged:
		p.handleSessionChanged(event)
	case types.EventStateSync:
		p.handleStateSync(event)
	}
	return p, nil
}
// Sync methods

func (p *InputPanel) syncInputState() tea.Cmd {
	return func() tea.Msg {
		update := types.StateUpdate{
			Type: types.InputUpdated,
			Payload: types.InputUpdatePayload{
				Buffer:         p.buffer,
				CursorPosition: p.cursorPosition,
				SelectionStart: p.selectionStart,
				SelectionEnd:   p.selectionEnd,
				Mode:           p.mode,
			},
			SourcePanel:     "input-panel",
			Timestamp:       time.Now(),
			ExpectedVersion: p.version,
		}

		if newVersion, err := p.ipcClient.SendStateUpdateAndWait(update); err != nil {
			return ErrorMsg{Error: err}
		} else {
			p.version = newVersion
		}

		return nil
	}
}

func (p *InputPanel) syncCursorPosition() tea.Cmd {
	return func() tea.Msg {
		update := types.StateUpdate{
			Type: types.CursorMoved,
			Payload: types.CursorMovePayload{
				Position:       p.cursorPosition,
				SelectionStart: p.selectionStart,
				SelectionEnd:   p.selectionEnd,
			},
			SourcePanel:     "input-panel",
			Timestamp:       time.Now(),
			ExpectedVersion: p.version,
		}

		if newVersion, err := p.ipcClient.SendStateUpdateAndWait(update); err != nil {
			return ErrorMsg{Error: err}
		} else {
			p.version = newVersion
		}

		return nil
	}
}

// Command implementations

func (p *InputPanel) showHelpMessage() tea.Cmd {
	return func() tea.Msg {
		return InfoMsg{Message: "Commands: /help /clear /new /session <id> /delete <id> /theme <name> /model <provider> <model> /agent <name>"}
	}
}

func (p *InputPanel) clearMessages() tea.Cmd {
	// This would clear messages in the current session
	return func() tea.Msg {
		return InfoMsg{Message: "Messages cleared"}
	}
}

func (p *InputPanel) createNewSession() tea.Cmd {
	return func() tea.Msg {
		// Generate a default title with timestamp
		title := fmt.Sprintf("New Session %s", time.Now().Format("15:04:05"))

		// Create session on OpenCode server first
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		session, err := p.client.Session.New(ctx, opencode.SessionNewParams{
			Directory: opencode.F("/Users/hhx/work/upwork/opencode"), // Use project root directory
			Title:     opencode.F(title),
		})

		if err != nil {
			log.Printf("[INPUT] Failed to create session on OpenCode server: %v", err)
			return ErrorMsg{Error: fmt.Errorf("failed to create session: %w", err)}
		}

		log.Printf("[INPUT] Successfully created session on OpenCode server: %s", session.ID)

		// Now create local state update with the server-assigned session ID
		update := types.StateUpdate{
			Type: types.SessionAdded,
			Payload: types.SessionAddPayload{
				Session: types.SessionInfo{
					ID:           session.ID, // Use server-assigned ID
					Title:        session.Title,
					CreatedAt:    time.Now(),
					UpdatedAt:    time.Now(),
					MessageCount: 0,
					IsActive:     true,
				},
			},
			SourcePanel:     "input-panel",
			Timestamp:       time.Now(),
			ExpectedVersion: p.version,
		}

		// Send the update via IPC
		if newVersion, err := p.ipcClient.SendStateUpdateAndWait(update); err != nil {
			log.Printf("[INPUT] Failed to send session state update: %v", err)
			return ErrorMsg{Error: err}
		} else {
			p.version = newVersion
		}

		return InfoMsg{Message: fmt.Sprintf("Created new session: %s (ID: %s)", session.Title, session.ID)}
	}
}

func (p *InputPanel) switchToSession(sessionID string) tea.Cmd {
	return func() tea.Msg {
		update := types.StateUpdate{
			Type:            types.SessionChanged,
			Payload:         types.SessionChangePayload{SessionID: sessionID},
			SourcePanel:     "input-panel",
			Timestamp:       time.Now(),
			ExpectedVersion: p.version,
		}

		if newVersion, err := p.ipcClient.SendStateUpdateAndWait(update); err != nil {
			return ErrorMsg{Error: err}
		} else {
			p.version = newVersion
		}

		return InfoMsg{Message: fmt.Sprintf("Switched to session %s", sessionID)}
	}
}

func (p *InputPanel) deleteSession(sessionID string) tea.Cmd {
	return func() tea.Msg {
		// Delete session on OpenCode server first
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		result, err := p.client.Session.Delete(ctx, sessionID, opencode.SessionDeleteParams{
			Directory: opencode.F("/Users/hhx/work/upwork/opencode"), // Use project root directory
		})

		if err != nil {
			log.Printf("[INPUT] Failed to delete session on OpenCode server: %v", err)
			return ErrorMsg{Error: fmt.Errorf("failed to delete session: %w", err)}
		}

		if result == nil || !*result {
			log.Printf("[INPUT] Session deletion returned false or nil result")
			return ErrorMsg{Error: fmt.Errorf("session deletion failed on server")}
		}

		log.Printf("[INPUT] Successfully deleted session on OpenCode server: %s", sessionID)

		// Now update local state
		update := types.StateUpdate{
			Type:            types.SessionDeleted,
			Payload:         types.SessionDeletePayload{SessionID: sessionID},
			SourcePanel:     "input-panel",
			Timestamp:       time.Now(),
			ExpectedVersion: p.version,
		}

		if newVersion, err := p.ipcClient.SendStateUpdateAndWait(update); err != nil {
			log.Printf("[INPUT] Failed to send session delete state update: %v", err)
			return ErrorMsg{Error: err}
		} else {
			p.version = newVersion
		}

		return InfoMsg{Message: fmt.Sprintf("Deleted session %s", sessionID)}
	}
}

func (p *InputPanel) changeTheme(theme string) tea.Cmd {
	return func() tea.Msg {
		update := types.StateUpdate{
			Type:            types.ThemeChanged,
			Payload:         types.ThemeChangePayload{Theme: theme},
			SourcePanel:     "input-panel",
			Timestamp:       time.Now(),
			ExpectedVersion: p.version,
		}

		if newVersion, err := p.ipcClient.SendStateUpdateAndWait(update); err != nil {
			return ErrorMsg{Error: err}
		} else {
			p.version = newVersion
		}

		return InfoMsg{Message: fmt.Sprintf("Theme changed to %s", theme)}
	}
}

func (p *InputPanel) changeModel(provider, model string) tea.Cmd {
	return func() tea.Msg {
		update := types.StateUpdate{
			Type:            types.ModelChanged,
			Payload:         types.ModelChangePayload{Provider: provider, Model: model},
			SourcePanel:     "input-panel",
			Timestamp:       time.Now(),
			ExpectedVersion: p.version,
		}

		if newVersion, err := p.ipcClient.SendStateUpdateAndWait(update); err != nil {
			return ErrorMsg{Error: err}
		} else {
			p.version = newVersion
		}

		return InfoMsg{Message: fmt.Sprintf("Model changed to %s/%s", provider, model)}
	}
}

func (p *InputPanel) changeAgent(agent string) tea.Cmd {
	return func() tea.Msg {
		update := types.StateUpdate{
			Type:            types.AgentChanged,
			Payload:         types.AgentChangePayload{Agent: agent},
			SourcePanel:     "input-panel",
			Timestamp:       time.Now(),
			ExpectedVersion: p.version,
		}

		if newVersion, err := p.ipcClient.SendStateUpdateAndWait(update); err != nil {
			return ErrorMsg{Error: err}
		} else {
			p.version = newVersion
		}

		return InfoMsg{Message: fmt.Sprintf("Agent changed to %s", agent)}
	}
}

// renderInput renders the input panel
func (p *InputPanel) renderInput() string {
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
		Border(styles.RoundedBorder).
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
func (p *InputPanel) renderHelp() string {
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
		Border(styles.RoundedBorder).
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
	Event types.StateEvent
}

type MessageSentMsg struct {
	Message types.MessageInfo
}

// Utility functions
func max(a, b int) int {
	if a > b {
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
	logPath := filepath.Join(logDir, "input.log")
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

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
	panel := NewInputPanel(httpClient, socketPath)

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
