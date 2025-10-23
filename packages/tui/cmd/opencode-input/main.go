package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
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
	client              *opencode.Client
	ipcClient           *ipc.SocketClient
	buffer              string
	cursorPosition      int
	selectionStart      int
	selectionEnd        int
	mode                string // "normal", "command", "multiline"
	history             []string
	historyIndex        int
	currentSessionID    string
	currentSessionTitle string // Add field to store session title
	width               int
	height              int
	ctx                 context.Context
	cancel              context.CancelFunc
	isMultiline         bool
	showHelp            bool
	lastCommand         string
	version             int64
	cachedState         *state.SharedApplicationState // Cache the state locally
	program             *tea.Program                  // Reference to the program for triggering updates
	// Scroll state for help content
	helpScrollOffset int      // Current scroll position in help content
	helpScrollMode   bool     // Whether we're in help scroll mode
	helpLines        []string // Cached help lines for scrolling
	// Theme switching state
	comfortableThemes []string // List of comfortable themes for cycling
	currentThemeIndex int      // Current index in comfortable themes list
}

// NewInputPanel creates a new input panel
func NewInputPanel(httpClient *opencode.Client, socketPath string) *InputPanel {
	ctx, cancel := context.WithCancel(context.Background())

	// Define comfortable themes for cycling
	comfortableThemes := []string{"dracula", "gruvbox", "tokyonight", "catppuccin", "nord", "rosepine"}

	// Find current theme index
	currentTheme := theme.CurrentThemeName()
	currentIndex := 0
	for i, themeName := range comfortableThemes {
		if themeName == currentTheme {
			currentIndex = i
			break
		}
	}

	panel := &InputPanel{
		client:            httpClient,
		ipcClient:         ipc.NewSocketClient(socketPath, "input-panel", "input"),
		buffer:            "",
		cursorPosition:    0,
		selectionStart:    0,
		selectionEnd:      0,
		mode:              "normal",
		history:           make([]string, 0),
		historyIndex:      -1,
		ctx:               ctx,
		cancel:            cancel,
		isMultiline:       false,
		showHelp:          false,
		helpScrollOffset:  0,
		helpScrollMode:    false,
		helpLines:         make([]string, 0),
		comfortableThemes: comfortableThemes,
		currentThemeIndex: currentIndex,
	}

	// Register event handlers
	panel.ipcClient.RegisterEventHandler(state.EventInputUpdated, panel.handleInputUpdated)
	panel.ipcClient.RegisterEventHandler(state.EventCursorMoved, panel.handleCursorMoved)
	panel.ipcClient.RegisterEventHandler(state.EventSessionChanged, panel.handleSessionChanged)
	panel.ipcClient.RegisterEventHandler(state.EventStateSync, panel.handleStateSync)
	// Wildcard handler for diagnostics: log all incoming events
	panel.ipcClient.RegisterEventHandler(types.StateEventType("*"), panel.handleAnyEvent)

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

	// Request initial state and preload all sessions
	cmds = append(cmds, func() tea.Msg {
		// Wait longer for connection to establish
		time.Sleep(500 * time.Millisecond)

		log.Printf("[INPUT] Requesting initial state and preloading sessions")
		if currentState, err := p.ipcClient.RequestState(); err == nil {
			log.Printf("[INPUT] Successfully loaded initial state with %d sessions", len(currentState.Sessions))

			// Preload all session information
			sessionCount := len(currentState.Sessions)
			log.Printf("[INPUT] Preloaded %d sessions for instant access", sessionCount)

			return StateLoadedMsg{State: currentState}
		} else {
			log.Printf("[INPUT] Failed to load initial state: %v", err)
			// Don't treat this as fatal - continue with empty state
			return StateLoadedMsg{State: nil}
		}
	})

	// Add preload completion notification
	cmds = append(cmds, func() tea.Msg {
		time.Sleep(600 * time.Millisecond) // Wait for state to be loaded
		if p.cachedState != nil {
			return PreloadCompletedMsg{SessionCount: len(p.cachedState.Sessions)}
		}
		return PreloadCompletedMsg{SessionCount: 0}
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
			// Cache the state locally
			p.cachedState = msg.State

			p.currentSessionID = msg.State.CurrentSessionID

			// Get session title for the current session
			if sessionInfo, found := msg.State.GetSessionByID(p.currentSessionID); found {
				p.currentSessionTitle = sessionInfo.Title
			} else {
				p.currentSessionTitle = ""
			}

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
			p.currentSessionTitle = ""
			p.buffer = ""
			p.cursorPosition = 0
			p.selectionStart = 0
			p.selectionEnd = 0
			p.mode = "normal"
			p.history = make([]string, 0)
			p.historyIndex = -1
		}
		return p, nil

	case PreloadCompletedMsg:
		log.Printf("[INPUT] Preload completed with %d sessions", msg.SessionCount)
		return p, nil

	case TitleUpdatedMsg:
		if msg.SessionID == p.currentSessionID {
			p.currentSessionTitle = msg.Title
			log.Printf("[INPUT] Title updated for session %s: %s", msg.SessionID, msg.Title)
		}
		return p, nil

	case SessionSyncMsg:
		log.Printf("[INPUT] Session sync received with %d sessions", len(msg.Sessions))
		// Update cached state if we have one
		if p.cachedState != nil {
			p.cachedState.Sessions = msg.Sessions
			p.cachedState.CurrentSessionID = msg.CurrentSessionID
			if msg.Version > p.version {
				p.version = msg.Version
			}
		}
		// Update current session info if it changed
		if msg.CurrentSessionID != p.currentSessionID {
			p.currentSessionID = msg.CurrentSessionID
			// Find and update session title
			for _, session := range msg.Sessions {
				if session.ID == p.currentSessionID {
					p.currentSessionTitle = session.Title
					break
				}
			}
		}
		return p, nil

	case SessionUpdatedMsg:
		log.Printf("[INPUT] Session updated: %s", msg.Session.ID)
		// Update cached state if this is our current session
		if msg.Session.ID == p.currentSessionID && msg.Session.Title != p.currentSessionTitle {
			p.currentSessionTitle = msg.Session.Title
			log.Printf("[INPUT] Current session title updated to: %s", p.currentSessionTitle)
		}
		// Update in cached sessions list
		if p.cachedState != nil {
			for i, session := range p.cachedState.Sessions {
				if session.ID == msg.Session.ID {
					p.cachedState.Sessions[i] = msg.Session
					break
				}
			}
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

	case tea.ClipboardMsg:
		// Handle clipboard paste
		text := string(msg)
		log.Printf("[INPUT] Clipboard paste received: %q", text)
		return p.insertCharacter(text)

	case ClipboardReadMsg:
		if msg.Error != nil {
			log.Printf("[INPUT] Clipboard read error: %v", msg.Error)
			return p, nil
		}

		log.Printf("[INPUT] Clipboard content received: %q (length: %d)", msg.Content, len(msg.Content))

		// Insert clipboard content at cursor position
		if p.cursorPosition <= len(p.buffer) {
			p.buffer = p.buffer[:p.cursorPosition] + msg.Content + p.buffer[p.cursorPosition:]
			p.cursorPosition += len(msg.Content)
		}

		// Sync the updated state
		return p, p.syncInputState()

	default:
		return p, nil
	}
}

// expectedVersion returns a safe ExpectedVersion for updates.
// Prefer the client's server-known version; if unavailable (0), fall back to local panel version;
// and as a last resort use 1 (initial state version).
func (p *InputPanel) expectedVersion() int64 {
	v := p.ipcClient.GetCurrentVersion()
	if v <= 0 {
		if p.version > 0 {
			return p.version
		}
		return 1
	}
	return v
}

// sendUpdateWithRetry sends a state update with optimistic concurrency and resolves
// version conflicts by refreshing the latest state version and retrying once.
func (p *InputPanel) sendUpdateWithRetry(update types.StateUpdate) (int64, error) {
	// First attempt with our best-known version
	update.ExpectedVersion = p.expectedVersion()
	newVersion, err := p.ipcClient.SendStateUpdateAndWait(update)
	if err == nil {
		p.version = newVersion
		return newVersion, nil
	}

	// If we hit a version conflict, refresh the version and retry once
	if strings.Contains(err.Error(), "version conflict") {
		// Try to refresh current version via state request
		if currentState, reqErr := p.ipcClient.RequestState(); reqErr == nil && currentState != nil {
			p.version = currentState.Version.Version
		}

		update.ExpectedVersion = p.expectedVersion()
		if newVersion2, err2 := p.ipcClient.SendStateUpdateAndWait(update); err2 == nil {
			p.version = newVersion2
			return newVersion2, nil
		} else {
			return 0, err2
		}
	}

	// Non-conflict error, propagate
	return 0, err
}

// View renders the input panel
func (p *InputPanel) View() string {
	return p.renderInput()
}

// handleKeyPress processes keyboard input
func (p *InputPanel) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Log all keyboard input for debugging
	log.Printf("[INPUT] Key pressed: %q", msg.String())

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
		// If in help scroll mode, scroll up in help content
		if p.helpScrollMode && p.showHelp {
			return p.handleHelpScrollUp()
		}
		return p.handleArrowUp()

	case "down":
		// If in help scroll mode, scroll down in help content
		if p.helpScrollMode && p.showHelp {
			return p.handleHelpScrollDown()
		}
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

	case "ctrl+v", "cmd+v", "ctrl+shift+v", "f2":
		// Handle paste - request clipboard content (multiple key combinations for compatibility)
		// F2 is added as an alternative paste key that VS Code won't intercept
		log.Printf("[INPUT] Paste key detected: %q", msg.String())
		log.Printf("[INPUT] Attempting to read clipboard...")
		return p, p.readClipboard()

	case "ctrl+t":
		// Cycle through comfortable themes
		return p.cycleTheme()

	case "f1":
		p.showHelp = !p.showHelp
		// Reset scroll state when toggling help
		if p.showHelp {
			p.helpScrollOffset = 0
			p.helpScrollMode = true
			p.cacheHelpLines()
		} else {
			p.helpScrollMode = false
		}
		return p, nil

	case "pgup", "page_up":
		if p.showHelp && p.helpScrollMode {
			return p.handleHelpPageUp()
		}
		return p, nil

	case "pgdn", "page_down":
		if p.showHelp && p.helpScrollMode {
			return p.handleHelpPageDown()
		}
		return p, nil

	case "esc":
		// Exit help scroll mode
		if p.helpScrollMode {
			p.helpScrollMode = false
			p.helpScrollOffset = 0
		}
		return p, nil

	case "space":
		log.Printf("[INPUT] Space key detected, inserting space character")
		return p.insertCharacter(" ")

	default:
		// Handle character input
		keyStr := msg.String()
		log.Printf("[INPUT] Key pressed: %q (length: %d, runes: %d)", keyStr, len(keyStr), len([]rune(keyStr)))

		// Special handling for space key (fallback)
		if keyStr == " " {
			log.Printf("[INPUT] Space key detected, inserting space character")
			return p.insertCharacter(" ")
		}

		// Check if it's a printable character (including Unicode)
		runes := []rune(keyStr)
		if len(runes) == 1 {
			char := runes[0]
			// Allow printable characters including space (32), tab (9) and Unicode characters
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

	// Add to history first
	p.addToHistory(command)

	// Clear buffer and cursor position immediately for all commands
	p.buffer = ""
	p.cursorPosition = 0

	var cmdToExecute tea.Cmd

	switch cmd {
	case "/help":
		// 显示内置帮助视图，而不是发送无处渲染的 InfoMsg
		p.showHelp = true
	case "/clear":
		cmdToExecute = p.clearMessages()
	case "/new":
		cmdToExecute = p.createNewSession()
	case "/session":
		if len(args) > 0 {
			cmdToExecute = p.switchToSession(args[0])
		}
	case "/delete":
		if len(args) > 0 {
			cmdToExecute = p.deleteSession(args[0])
		}
	case "/theme":
		if len(args) > 0 {
			cmdToExecute = p.changeTheme(args[0])
		}
	case "/model":
		if len(args) > 1 {
			cmdToExecute = p.changeModel(args[0], args[1])
		}
	case "/agent":
		if len(args) > 0 {
			cmdToExecute = p.changeAgent(args[0])
		}
	}

	// Combine input state sync with the command execution
	if cmdToExecute != nil {
		return p, tea.Batch(p.syncInputState(), cmdToExecute)
	}

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

		// Perform API call in background so UI remains responsive
		go func(sessionID, userMsg string) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			response, err := p.client.Session.Prompt(ctx, sessionID, opencode.SessionPromptParams{
				Parts: opencode.F([]opencode.SessionPromptParamsPartUnion{
					opencode.TextPartInputParam{
						Text: opencode.F(userMsg),
						Type: opencode.F(opencode.TextPartInputTypeText),
					},
				}),
			})

			if err != nil {
				log.Printf("[INPUT] Failed to send message to OpenCode API: %v", err)
				return
			}

			log.Printf("[INPUT] Successfully sent message to OpenCode API, response received")

			// Create assistant message from response
			if response != nil {
				// Check for errors in the response
				if response.Info.Error.Name != "" {
					log.Printf("[INPUT] Assistant message error: %s - %v", response.Info.Error.Name, response.Info.Error.Data)
					return
				}

				// Do not add assistant message from input panel.
				// The SSE orchestrator will add and stream-update messages to avoid duplicates.
			}
		}(p.currentSessionID, message)

		// Immediately clear input buffer in UI
		return MessageSentMsg{}
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
	if payloadMap, ok := event.Data.(map[string]interface{}); ok {
		var payload types.InputUpdatePayload
		if err := decodePayload(payloadMap, &payload); err == nil {
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
			// Always sync local version to event version to avoid conflicts
			p.version = event.Version
		}
	}
	return nil
}

func (p *InputPanel) handleCursorMoved(event types.StateEvent) error {
	if payloadMap, ok := event.Data.(map[string]interface{}); ok {
		var payload types.CursorMovePayload
		if err := decodePayload(payloadMap, &payload); err == nil {
			// Only update if not from this panel
			if event.SourcePanel != "input-panel" {
				p.cursorPosition = payload.Position
				p.selectionStart = payload.SelectionStart
				p.selectionEnd = payload.SelectionEnd
			}
			// Always sync local version to event version to avoid conflicts
			p.version = event.Version
		}
	}
	return nil
}

func (p *InputPanel) handleSessionChanged(event types.StateEvent) error {
	if payloadMap, ok := event.Data.(map[string]interface{}); ok {
		var payload types.SessionChangePayload
		if err := decodePayload(payloadMap, &payload); err == nil {
			oldSessionID := p.currentSessionID
			p.currentSessionID = payload.SessionID

			// Try to find session title from cached sessions first
			if sessionInfo, found := p.getSessionInfo(p.currentSessionID); found {
				p.currentSessionTitle = sessionInfo.Title
				log.Printf("[INPUT] Session changed from %s to %s, title updated to '%s' (from cache), version=%d", oldSessionID, p.currentSessionID, p.currentSessionTitle, event.Version)
				// Trigger immediate UI update
				if p.program != nil {
					go func() {
						p.program.Send(TitleUpdatedMsg{SessionID: p.currentSessionID, Title: sessionInfo.Title})
					}()
				}
			} else {
				// If not found in cache, use a fallback title and update asynchronously
				p.currentSessionTitle = fmt.Sprintf("Session %s", p.currentSessionID[:8])
				log.Printf("[INPUT] Session changed from %s to %s, using fallback title '%s', version=%d", oldSessionID, p.currentSessionID, p.currentSessionTitle, event.Version)

				// Trigger async state request to update cache and title
				go func() {
					if currentState, err := p.ipcClient.RequestState(); err == nil {
						p.cachedState = currentState // Update cache
						if sessionInfo, found := currentState.GetSessionByID(p.currentSessionID); found {
							// Update title in background and trigger UI update
							p.currentSessionTitle = sessionInfo.Title
							log.Printf("[INPUT] Async title update: session %s title set to '%s'", p.currentSessionID, p.currentSessionTitle)
							if p.program != nil {
								p.program.Send(TitleUpdatedMsg{SessionID: p.currentSessionID, Title: sessionInfo.Title})
							}
						}
					} else {
						log.Printf("[INPUT] Async state request failed: %v", err)
						// Keep the fallback title if async request fails
					}
				}()
			}

			// Sync local version to event version to avoid conflicts
			p.version = event.Version
		}
	}
	return nil
}

func (p *InputPanel) handleStateSync(event types.StateEvent) error {
	if payloadMap, ok := event.Data.(map[string]interface{}); ok {
		var payload types.StateSyncPayload
		if err := decodePayload(payloadMap, &payload); err == nil {
			// Update cached state with smart invalidation
			if p.cachedState == nil || payload.State.Version.Version > p.version {
				p.cachedState = payload.State
				p.version = payload.State.Version.Version
				log.Printf("[INPUT] Cache updated to version %d", p.version)

				p.currentSessionID = payload.State.CurrentSessionID
				p.buffer = payload.State.Input.Buffer
				p.cursorPosition = payload.State.Input.CursorPosition
				p.selectionStart = payload.State.Input.SelectionStart
				p.selectionEnd = payload.State.Input.SelectionEnd
				p.mode = payload.State.Input.Mode
				p.history = payload.State.Input.History
				p.historyIndex = payload.State.Input.HistoryIndex

				// Update session title when we receive state sync
				if sessionInfo, found := payload.State.GetSessionByID(p.currentSessionID); found {
					oldTitle := p.currentSessionTitle
					p.currentSessionTitle = sessionInfo.Title
					log.Printf("[INPUT] Session title updated from '%s' to '%s' via state sync", oldTitle, p.currentSessionTitle)

					// Trigger UI update if title changed
					if oldTitle != p.currentSessionTitle && p.program != nil {
						go func() {
							p.program.Send(TitleUpdatedMsg{SessionID: p.currentSessionID, Title: p.currentSessionTitle})
						}()
					}
				} else {
					// Session not found, clear title
					if p.currentSessionTitle != "" {
						p.currentSessionTitle = ""
						log.Printf("[INPUT] Session %s not found in state sync, title cleared", p.currentSessionID)

						if p.program != nil {
							go func() {
								p.program.Send(TitleUpdatedMsg{SessionID: p.currentSessionID, Title: ""})
							}()
						}
					}
				}

				log.Printf("[INPUT] State synchronized, version=%d", p.version)
			} else {
				log.Printf("[INPUT] Ignoring state sync with older/same version %d (current: %d)", payload.State.Version.Version, p.version)
			}
		}
	}
	return nil
}

// handleAnyEvent logs any received event for diagnostics
func (p *InputPanel) handleAnyEvent(event types.StateEvent) error {
	log.Printf("[INPUT] v%v Received event type: %s from %s", event.Version, event.Type, event.SourcePanel)
	// Keep local version in sync with server event version
	p.version = event.Version
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
		// Return a command to trigger UI re-render immediately after session change
		return p, tea.Batch(tea.Tick(time.Millisecond*10, func(t time.Time) tea.Msg {
			return tea.WindowSizeMsg{}
		}))
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
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
			// ExpectedVersion will be set by sendUpdateWithRetry
		}
		if newVersion, err := p.sendUpdateWithRetry(update); err != nil {
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
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
			// ExpectedVersion will be set by sendUpdateWithRetry
		}
		if newVersion, err := p.sendUpdateWithRetry(update); err != nil {
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
			Title: opencode.F(title),
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
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
			// ExpectedVersion will be set by sendUpdateWithRetry
		}

		// Send the update via IPC
		if newVersion, err := p.sendUpdateWithRetry(update); err != nil {
			log.Printf("[INPUT] Failed to send session state update: %v", err)
			return ErrorMsg{Error: err}
		} else {
			p.version = newVersion
		}

		// Broadcast session change so other panels switch, and update locally
		change := types.StateUpdate{
			Type:        types.SessionChanged,
			Payload:     types.SessionChangePayload{SessionID: session.ID},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
		}
		if newVersion, err := p.sendUpdateWithRetry(change); err != nil {
			log.Printf("[INPUT] Failed to broadcast session change: %v", err)
		} else {
			p.version = newVersion
		}
		p.currentSessionID = session.ID

		return InfoMsg{Message: fmt.Sprintf("Created new session: %s (ID: %s)", session.Title, session.ID)}
	}
}

func (p *InputPanel) switchToSession(sessionID string) tea.Cmd {
	return func() tea.Msg {
		update := types.StateUpdate{
			Type:        types.SessionChanged,
			Payload:     types.SessionChangePayload{SessionID: sessionID},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
			// ExpectedVersion will be set by sendUpdateWithRetry
		}
		if newVersion, err := p.sendUpdateWithRetry(update); err != nil {
			return ErrorMsg{Error: err}
		} else {
			p.version = newVersion
		}

		// Update local header immediately
		p.currentSessionID = sessionID

		return InfoMsg{Message: fmt.Sprintf("Switched to session %s", sessionID)}
	}
}

func (p *InputPanel) deleteSession(sessionID string) tea.Cmd {
	return func() tea.Msg {
		// Delete session on OpenCode server first
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		result, err := p.client.Session.Delete(ctx, sessionID, opencode.SessionDeleteParams{})

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
			Type:        types.SessionDeleted,
			Payload:     types.SessionDeletePayload{SessionID: sessionID},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
			// ExpectedVersion will be set by sendUpdateWithRetry
		}
		if newVersion, err := p.sendUpdateWithRetry(update); err != nil {
			log.Printf("[INPUT] Failed to send session delete state update: %v", err)
			return ErrorMsg{Error: err}
		} else {
			p.version = newVersion
		}

		if p.currentSessionID == sessionID {
			p.currentSessionID = ""
		}

		return InfoMsg{Message: fmt.Sprintf("Deleted session %s", sessionID)}
	}
}

func (p *InputPanel) changeTheme(theme string) tea.Cmd {
	return func() tea.Msg {
		update := types.StateUpdate{
			Type:        types.ThemeChanged,
			Payload:     types.ThemeChangePayload{Theme: theme},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
			// ExpectedVersion will be set by sendUpdateWithRetry
		}
		if newVersion, err := p.sendUpdateWithRetry(update); err != nil {
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
			Type:        types.ModelChanged,
			Payload:     types.ModelChangePayload{Provider: provider, Model: model},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
			// ExpectedVersion will be set by sendUpdateWithRetry
		}
		if newVersion, err := p.sendUpdateWithRetry(update); err != nil {
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
			Type:        types.AgentChanged,
			Payload:     types.AgentChangePayload{Agent: agent},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
			// ExpectedVersion will be set by sendUpdateWithRetry
		}
		if newVersion, err := p.sendUpdateWithRetry(update); err != nil {
			return ErrorMsg{Error: err}
		} else {
			p.version = newVersion
		}

		return InfoMsg{Message: fmt.Sprintf("Agent changed to %s", agent)}
	}
}

// cycleTheme cycles through comfortable themes
func (p *InputPanel) cycleTheme() (tea.Model, tea.Cmd) {
	// Move to next theme in the list
	p.currentThemeIndex = (p.currentThemeIndex + 1) % len(p.comfortableThemes)
	nextTheme := p.comfortableThemes[p.currentThemeIndex]

	// Apply the theme locally first
	if err := theme.SetTheme(nextTheme); err != nil {
		log.Printf("[INPUT] Failed to set theme locally: %v", err)
		return p, func() tea.Msg {
			return ErrorMsg{Error: fmt.Errorf("Failed to switch theme: %v", err)}
		}
	}

	// Send theme change to other panels
	return p, p.changeTheme(nextTheme)
}

// cacheHelpLines caches the full help content lines for scrolling
func (p *InputPanel) cacheHelpLines() {
	fullHelpLines := []string{
		"Commands:",
		"  /help                    Show this help",
		"  /clear                   Clear current session messages",
		"  /new                     Create new session",
		"  /session <id>            Switch to session",
		"  /theme <name>            Change theme",
		"  /model <provider> <model> Change model",
		"  /agent <name>            Change agent",
		"",
		"Keyboard Shortcuts:",
		"  Enter                    Send message",
		"  Ctrl+Enter               Toggle multiline mode",
		"  ↑/↓                     Navigate history",
		"  Tab                      Command completion",
		"  Ctrl+A                   Select all",
		"  Ctrl+K                   Delete to end of line",
		"  Ctrl+U                   Delete to beginning of line",
		"  Ctrl+W                   Delete previous word",
		"  Ctrl+L                   Clear buffer",
		"  Ctrl+T                   Cycle through comfortable themes",
		"  F1                       Toggle this help",
		"  Page Up/Down             Scroll help content",
		"  Esc                      Exit help scroll mode",
		"  Ctrl+C                   Quit (or clear if buffer not empty)",
	}
	p.helpLines = fullHelpLines
}

// handleHelpScrollUp scrolls up in help content
func (p *InputPanel) handleHelpScrollUp() (tea.Model, tea.Cmd) {
	if p.helpScrollOffset > 0 {
		p.helpScrollOffset--
	}
	return p, nil
}

// handleHelpScrollDown scrolls down in help content
func (p *InputPanel) handleHelpScrollDown() (tea.Model, tea.Cmd) {
	maxOffset := len(p.helpLines) - 1
	if p.helpScrollOffset < maxOffset {
		p.helpScrollOffset++
	}
	return p, nil
}

// handleHelpPageUp scrolls up by page in help content
func (p *InputPanel) handleHelpPageUp() (tea.Model, tea.Cmd) {
	pageSize := max(1, p.height/4) // Scroll by quarter of screen height
	p.helpScrollOffset = max(0, p.helpScrollOffset-pageSize)
	return p, nil
}

// handleHelpPageDown scrolls down by page in help content
func (p *InputPanel) handleHelpPageDown() (tea.Model, tea.Cmd) {
	pageSize := max(1, p.height/4) // Scroll by quarter of screen height
	maxOffset := len(p.helpLines) - 1
	p.helpScrollOffset = min(maxOffset, p.helpScrollOffset+pageSize)
	return p, nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// renderInput renders the input panel with dynamic height management
func (p *InputPanel) renderInput() string {
	t := theme.CurrentTheme()

	var content string

	// Calculate available height (reserve some space for margins)
	availableHeight := p.height - 2
	if availableHeight < 10 {
		availableHeight = 10 // Minimum height
	}

	// Header
	header := "Input"
	if p.currentSessionID != "" {
		if p.currentSessionTitle != "" {
			header += fmt.Sprintf(" - %s", p.currentSessionTitle)
		} else {
			header += fmt.Sprintf(" - Session %s", p.currentSessionID[:8])
		}
	}
	if p.isMultiline {
		header += " [MULTILINE]"
	}

	// Debug log to track header rendering
	log.Printf("[INPUT] Rendering header: %s (currentSessionID: %s, currentSessionTitle: %s)", header, p.currentSessionID, p.currentSessionTitle)

	headerContent := styles.NewStyle().
		Foreground(t.Primary()).
		Bold(true).
		Render(header) + "\n\n"

	content += headerContent
	usedLines := 3 // Header + spacing

	// Input field with dynamic height
	inputStyle := styles.NewStyle().
		Border(styles.RoundedBorder).
		BorderForeground(t.Border()).
		Padding(1).
		Width(p.width - 2)

	// Calculate input field height based on content
	inputLines := strings.Count(p.buffer, "\n") + 1
	maxInputHeight := availableHeight - usedLines - 4 // Reserve space for other elements
	if maxInputHeight < 3 {
		maxInputHeight = 3
	}

	// Limit input height if needed
	if inputLines > maxInputHeight {
		inputStyle = inputStyle.Height(maxInputHeight)
	}

	// Render buffer with cursor
	displayBuffer := p.buffer
	if p.cursorPosition <= len(displayBuffer) {
		cursorChar := "│"
		displayBuffer = displayBuffer[:p.cursorPosition] +
			styles.NewStyle().Foreground(t.Success()).Render(cursorChar) +
			displayBuffer[p.cursorPosition:]
	}

	inputContent := inputStyle.Render(displayBuffer) + "\n"
	content += inputContent
	usedLines += max(inputLines+2, 4) // Input field + border

	// Mode indicator
	modeText := fmt.Sprintf("Mode: %s", p.mode)
	if len(p.history) > 0 {
		modeText += fmt.Sprintf(" | History: %d items", len(p.history))
	}

	modeContent := styles.NewStyle().
		Foreground(t.TextMuted()).
		Render(modeText) + "\n"

	content += modeContent
	usedLines += 1

	// Help text
	helpText := "Enter send • Ctrl+Enter multiline • ↑/↓ history • Tab complete • F1 help • Ctrl+C quit"
	helpContent := styles.NewStyle().
		Foreground(t.TextMuted()).
		Render(helpText)

	content += helpContent
	usedLines += 1

	// Show help if requested and there's space
	if p.showHelp {
		remainingLines := availableHeight - usedLines
		if remainingLines > 5 {
			content += "\n\n" + p.renderHelpCompact(remainingLines-2)
		} else {
			// Not enough space for help, show a hint
			content += "\n" + styles.NewStyle().
				Foreground(t.Warning()).
				Render("(Help available - resize window or press F1 to toggle)")
		}
	}

	return content
}

// renderHelpCompact renders a compact version of help text that fits in available space
func (p *InputPanel) renderHelpCompact(maxLines int) string {
	t := theme.CurrentTheme()

	// If we're in scroll mode and have cached lines, use them
	if p.helpScrollMode && len(p.helpLines) > 0 {
		return p.renderScrollableHelp(maxLines)
	}

	// Full help content
	fullHelpLines := []string{
		"Commands:",
		"  /help                    Show this help",
		"  /clear                   Clear current session messages",
		"  /new                     Create new session",
		"  /session <id>            Switch to session",
		"  /theme <name>            Change theme",
		"  /model <provider> <model> Change model",
		"  /agent <name>            Change agent",
		"",
		"Keyboard Shortcuts:",
		"  Enter                    Send message",
		"  Ctrl+Enter               Toggle multiline mode",
		"  ↑/↓                     Navigate history",
		"  Tab                      Command completion",
		"  Ctrl+A                   Select all",
		"  Ctrl+K                   Delete to end of line",
		"  Ctrl+U                   Delete to beginning of line",
		"  Ctrl+W                   Delete previous word",
		"  Ctrl+L                   Clear buffer",
		"  F1                       Toggle this help",
		"  Ctrl+C                   Quit (or clear if buffer not empty)",
	}

	// If we have enough space, show full help
	if maxLines >= len(fullHelpLines)+2 {
		return p.renderHelp()
	}

	// Otherwise, show a truncated version
	var helpLines []string

	// Always show commands section if we have space
	if maxLines >= 10 {
		helpLines = append(helpLines, fullHelpLines[0:8]...)
		if maxLines >= 15 {
			helpLines = append(helpLines, "")
			helpLines = append(helpLines, "Key Shortcuts: Enter=send, Ctrl+Enter=multiline, ↑/↓=history, F1=help")
		}
	} else {
		// Very limited space - show essential info only
		helpLines = []string{
			"Essential Commands:",
			"  /help /clear /new /session <id>",
			"Keys: Enter=send, Ctrl+Enter=multiline, F1=toggle help",
		}
	}

	// Add scroll hint if we have cached help lines (indicating scrollable content)
	if len(p.helpLines) > 0 {
		helpLines = append(helpLines, "... (↑/↓ to scroll, Esc to exit scroll mode)")
	} else {
		// Add truncation indicator if needed
		if len(helpLines) < len(fullHelpLines) {
			helpLines = append(helpLines, "... (resize window for full help)")
		}
	}

	helpContent := strings.Join(helpLines, "\n")

	return styles.NewStyle().
		Foreground(t.Info()).
		Border(styles.RoundedBorder).
		BorderForeground(t.Border()).
		Padding(1).
		Render(helpContent)
}

// renderScrollableHelp renders help content with scrolling support
func (p *InputPanel) renderScrollableHelp(maxLines int) string {
	t := theme.CurrentTheme()

	if len(p.helpLines) == 0 {
		return ""
	}

	// Calculate how many lines we can show (accounting for border and padding)
	availableLines := maxLines - 4 // 2 for border, 2 for padding
	if availableLines <= 0 {
		availableLines = 1
	}

	// Calculate the visible range
	startLine := p.helpScrollOffset
	endLine := min(startLine+availableLines, len(p.helpLines))

	// Ensure we don't go beyond bounds
	if startLine >= len(p.helpLines) {
		startLine = max(0, len(p.helpLines)-availableLines)
		p.helpScrollOffset = startLine
	}

	if endLine <= startLine {
		endLine = startLine + 1
	}

	// Get the visible lines
	visibleLines := p.helpLines[startLine:endLine]

	// Add scroll indicators
	var scrollInfo []string
	if startLine > 0 {
		scrollInfo = append(scrollInfo, "▲ More content above")
	}

	scrollInfo = append(scrollInfo, visibleLines...)

	if endLine < len(p.helpLines) {
		scrollInfo = append(scrollInfo, "▼ More content below")
	}

	// Add navigation hint at the bottom
	scrollInfo = append(scrollInfo, "")
	scrollInfo = append(scrollInfo, fmt.Sprintf("Scroll: ↑/↓ lines, PgUp/PgDn pages, Esc to exit (%d/%d)",
		startLine+1, len(p.helpLines)))

	helpContent := strings.Join(scrollInfo, "\n")

	return styles.NewStyle().
		Foreground(t.Info()).
		Border(styles.RoundedBorder).
		BorderForeground(t.Border()).
		Padding(1).
		Render(helpContent)
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

type TitleUpdatedMsg struct {
	SessionID string
	Title     string
}

type PreloadCompletedMsg struct {
	SessionCount int
}

type SessionSyncMsg struct {
	Sessions         []types.SessionInfo
	CurrentSessionID string
	Version          int64
}

type SessionUpdatedMsg struct {
	Session types.SessionInfo
}

type ClipboardReadMsg struct {
	Content string
	Error   error
}

func (p *InputPanel) readClipboard() tea.Cmd {
	return func() tea.Msg {
		log.Printf("[INPUT] Attempting to read clipboard using pbpaste...")

		cmd := exec.Command("pbpaste")
		output, err := cmd.Output()

		if err != nil {
			log.Printf("[INPUT] Failed to read clipboard: %v", err)
			return ClipboardReadMsg{
				Content: "",
				Error:   err,
			}
		}

		content := string(output)
		log.Printf("[INPUT] Clipboard content read successfully: %q (length: %d)", content, len(content))

		return ClipboardReadMsg{
			Content: content,
			Error:   nil,
		}
	}
}

// Utility functions
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// getSessionInfo retrieves session information from the cached state
func (p *InputPanel) getSessionInfo(sessionID string) (types.SessionInfo, bool) {
	// Use cached state if available
	if p.cachedState != nil {
		return p.cachedState.GetSessionByID(sessionID)
	}

	// Fallback to requesting state only if no cache is available
	if currentState, err := p.ipcClient.RequestState(); err == nil {
		p.cachedState = currentState // Cache the result
		return currentState.GetSessionByID(sessionID)
	} else {
		log.Printf("[INPUT] Failed to request session info: %v", err)
		return types.SessionInfo{}, false
	}
}

// decodePayload converts an event payload map back into the target struct type.
func decodePayload(data interface{}, target interface{}) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal payload map: %w", err)
	}
	if err := json.Unmarshal(bytes, target); err != nil {
		return fmt.Errorf("failed to unmarshal payload into target struct: %w", err)
	}
	return nil
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

	// Set program reference in panel for UI updates
	panel.program = program

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
