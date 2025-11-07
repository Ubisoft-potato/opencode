package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
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

// ModelInfo represents a model available for selection
type ModelInfo struct {
	Provider string
	Model    string
	Name     string
}

// updateAgentScrollOffset updates the scroll offset for agent list to keep selected item visible
func (p *InputPanel) updateAgentScrollOffset() {
	base, visible := p.agentListCapacities()
	if p.agentSelectedIdx < p.agentScrollOffset {
		p.agentScrollOffset = p.agentSelectedIdx
	}
	if visible < 1 {
		visible = 1
	}
	if p.agentSelectedIdx >= p.agentScrollOffset+visible {
		p.agentScrollOffset = p.agentSelectedIdx - visible + 1
	}

	if p.agentScrollOffset < 0 {
		p.agentScrollOffset = 0
	}

	maxOffset := len(p.availableAgents) - base
	if maxOffset < 0 {
		maxOffset = 0
	}
	if p.agentScrollOffset > maxOffset {
		p.agentScrollOffset = maxOffset
	}
}

func (p *InputPanel) agentListCapacities() (int, int) {
	dialogHeight := min(p.height-4, 20)
	base := dialogHeight - 7
	if p.agentSearchQuery != "" {
		base = dialogHeight - 6
	}
	if base < 1 {
		base = 1
	}

	visible := base
	if p.agentScrollOffset > 0 && visible > 1 {
		visible--
	}

	return base, visible
}

// filterAgents filters available agents based on search query
func (p *InputPanel) filterAgents() {
	if len(p.allAgents) == 0 {
		p.availableAgents = nil
		p.agentSelectedIdx = 0
		return
	}

	agents := make([]AgentInfo, 0, len(p.allAgents))
	if p.agentSearchQuery != "" {
		query := strings.ToLower(p.agentSearchQuery)
		for _, agent := range p.allAgents {
			name := strings.ToLower(agent.Name)
			if strings.Contains(name, query) {
				agents = append(agents, agent)
				continue
			}
			desc := strings.ToLower(agent.Description)
			if strings.Contains(desc, query) {
				agents = append(agents, agent)
			}
		}
	}
	if p.agentSearchQuery == "" {
		agents = append(agents, p.allAgents...)
	}

	p.availableAgents = agents
	if p.agentSelectedIdx >= len(p.availableAgents) {
		p.agentSelectedIdx = 0
	}
}

type AgentInfo struct {
	Name        string
	Description string
}

type InputPanel struct {
	client              *opencode.Client
	ipcClient           *ipc.SocketClient
	buffer              string
	cursorPosition      int
	selectionStart      int
	selectionEnd        int
	mode                string // "normal", "command", "multiline", "model_select", "agent_select"
	history             []string
	historyIndex        int
	currentSessionTitle string // Add field to store session title
	currentSessionID    string // Add field to store current session ID
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
	// Model selection state
	showModelDialog   bool        // Whether model selection dialog is visible
	modelSearchQuery  string      // Current search query in model dialog
	modelSelectedIdx  int         // Currently selected model index
	modelScrollOffset int         // Current scroll offset in model list
	availableModels   []ModelInfo // Available models for selection
	allModels         []ModelInfo // Cached models fetched from API
	// Agent selection state
	showAgentDialog   bool        // Whether agent selection dialog is visible
	agentSearchQuery  string      // Current search query in agent dialog
	agentSelectedIdx  int         // Currently selected agent index
	agentScrollOffset int         // Current scroll offset in agent list
	availableAgents   []AgentInfo // Available agents for selection
	allAgents         []AgentInfo // Cached agents fetched from API
	// Command completion dialog state
	showCompletionDialog   bool     // Whether command completion dialog is visible
	completionCommands     []string // Available commands for completion
	completionSelectedIdx  int      // Currently selected command index
	completionScrollOffset int      // Current scroll offset in completion list
	// Current model information
	currentProvider string // Current selected provider
	currentModel    string // Current selected model
}

var completionSuggestions = []string{
	"new",
	"models",
	"agents",
	"clear",
	"agent",
	// "share",
	// "unshare",
	"compact",
}

// NewInputPanel creates a new input panel
func NewInputPanel(httpClient *opencode.Client, socketPath string, comfortableThemes []string, currentTheme string) *InputPanel {
	ctx, cancel := context.WithCancel(context.Background())

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
	panel.ipcClient.RegisterEventHandler(types.EventUIActionTriggered, panel.handleUIActionTriggered)
	// Wildcard handler for diagnostics: log all incoming events
	panel.ipcClient.RegisterEventHandler(types.StateEventType("*"), panel.handleAnyEvent)

	return panel
}

// Init initializes the InputPanel and returns initial commands
func (p *InputPanel) Init() tea.Cmd {
	var cmds []tea.Cmd

	// Connect to IPC server
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

			// Update current model information
			p.currentProvider = msg.State.Provider
			p.currentModel = msg.State.Model
			log.Printf("[INPUT] Model info loaded: Provider='%s', Model='%s'", p.currentProvider, p.currentModel)
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
			p.currentProvider = ""
			p.currentModel = ""
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

	case InfoMsg:
		log.Printf("Input panel info: %s", msg.Message)
		// If this is a model change message, trigger a state request to update UI
		if strings.Contains(msg.Message, "Model changed to") {
			return p, func() tea.Msg {
				// Request fresh state from server to get updated model information
				if p.ipcClient != nil {
					if currentState, err := p.ipcClient.RequestState(); err == nil {
						return StateLoadedMsg{State: currentState}
					} else {
						log.Printf("[INPUT] Failed to request state after model change: %v", err)
					}
				}
				return nil
			}
		}
		return p, nil

	case ModelsLoadedMsg:
		p.allModels = msg.Models
		p.modelSelectedIdx = 0
		p.modelScrollOffset = 0
		p.filterModels()
		p.updateModelScrollOffset()
		log.Printf("[MODEL_DIALOG] Models loaded: %d available", len(p.availableModels))
		return p, nil

	case AgentsLoadedMsg:
		p.allAgents = msg.Agents
		p.agentSelectedIdx = 0
		p.agentScrollOffset = 0
		p.filterAgents()
		p.updateAgentScrollOffset()
		log.Printf("[AGENT_DIALOG] Agents loaded: %d available", len(p.availableAgents))
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
	if p.showModelDialog {
		return p.renderModelDialog()
	}
	if p.showAgentDialog {
		return p.renderAgentDialog()
	}
	if p.showCompletionDialog {
		return p.renderCompletionDialog()
	}
	return p.renderInput()
}

// handleKeyPress processes keyboard input
func (p *InputPanel) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Log all keyboard input for debugging
	log.Printf("[INPUT] Key pressed: %q", msg.String())

	// Handle command completion dialog keys first
	if p.showCompletionDialog {
		return p.handleCompletionDialogKeys(msg)
	}

	// Handle model selection dialog keys
	if p.showModelDialog && p.mode == "model_select" {
		return p.handleModelDialogKeys(msg)
	}

	// Handle agent selection dialog keys
	if p.showAgentDialog && p.mode == "agent_select" {
		return p.handleAgentDialogKeys(msg)
	}

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

// handleCompletionDialogKeys handles keyboard input when command completion dialog is active
func (p *InputPanel) handleCompletionDialogKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	keyStr := msg.String()

	switch keyStr {
	case "escape", "esc", "ctrl+c":
		// Close completion dialog
		p.showCompletionDialog = false
		return p, nil

	case "enter":
		// Select current command
		if p.completionSelectedIdx < len(p.completionCommands) {
			selectedCommand := p.completionCommands[p.completionSelectedIdx]
			p.showCompletionDialog = false
			// Replace the "/" with the selected command
			p.buffer = "/" + selectedCommand
			p.cursorPosition = len(p.buffer)
			return p, p.syncInputState()
		}
		return p, nil

	case "up":
		// Move selection up
		if p.completionSelectedIdx > 0 {
			p.completionSelectedIdx--
			p.updateCompletionScrollOffset()
		}
		return p, nil

	case "down":
		// Move selection down
		if p.completionSelectedIdx < len(p.completionCommands)-1 {
			p.completionSelectedIdx++
			p.updateCompletionScrollOffset()
		}
		return p, nil

	case "tab":
		// Tab also selects the current command
		if p.completionSelectedIdx < len(p.completionCommands) {
			selectedCommand := p.completionCommands[p.completionSelectedIdx]
			p.showCompletionDialog = false
			// Replace the "/" with the selected command
			p.buffer = "/" + selectedCommand
			p.cursorPosition = len(p.buffer)
			return p, p.syncInputState()
		}
		return p, nil

	case "backspace":
		return p.handleBackspace()

	case "delete":
		return p.handleDelete()

	default:
		runes := []rune(keyStr)
		if len(runes) == 1 {
			char := runes[0]
			if char >= 32 || char == 9 {
				return p.insertCharacter(keyStr)
			}
		}
		// For other keys, close the dialog and handle normally
		p.showCompletionDialog = false
		// Re-handle the key press normally
		return p.handleKeyPress(msg)
	}
}

// handleAgentDialogKeys handles keyboard input when agent selection dialog is active
func (p *InputPanel) handleAgentDialogKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	keyStr := msg.String()
	log.Printf("[AGENT_DIALOG] Key pressed: %q, availableAgents count: %d, selectedIdx: %d, scrollOffset: %d", keyStr, len(p.availableAgents), p.agentSelectedIdx, p.agentScrollOffset)

	switch keyStr {
	case "escape", "esc", "ctrl+c":
		// Close agent dialog
		p.showAgentDialog = false
		p.mode = "normal"
		return p, nil

	case "enter":
		// Select current agent
		if p.agentSelectedIdx < len(p.availableAgents) {
			selectedAgent := p.availableAgents[p.agentSelectedIdx]
			p.showAgentDialog = false
			p.mode = "normal"
			return p, p.changeAgent(selectedAgent.Name)
		}
		return p, nil

	case "up":
		// Move selection up
		if p.agentSelectedIdx > 0 {
			p.agentSelectedIdx--
			p.updateAgentScrollOffset()
		}
		return p, nil

	case "down":
		// Move selection down
		if p.agentSelectedIdx < len(p.availableAgents)-1 {
			p.agentSelectedIdx++
			p.updateAgentScrollOffset()
		}
		return p, nil

	case "page_up", "ctrl+u":
		// Page up in agent list
		dialogHeight := min(p.height-4, 20)
		maxAgents := dialogHeight - 8
		if p.agentSearchQuery != "" {
			maxAgents = dialogHeight - 5
		}

		p.agentSelectedIdx = max(0, p.agentSelectedIdx-maxAgents)
		p.updateAgentScrollOffset()
		return p, nil

	case "page_down", "ctrl+d":
		// Page down in agent list
		dialogHeight := min(p.height-4, 20)
		maxAgents := dialogHeight - 8
		if p.agentSearchQuery != "" {
			maxAgents = dialogHeight - 5
		}

		p.agentSelectedIdx = min(len(p.availableAgents)-1, p.agentSelectedIdx+maxAgents)
		p.updateAgentScrollOffset()
		return p, nil

	case "home", "ctrl+a":
		// Go to first agent
		p.agentSelectedIdx = 0
		p.agentScrollOffset = 0
		return p, nil

	case "end", "ctrl+e":
		// Go to last agent
		p.agentSelectedIdx = len(p.availableAgents) - 1
		p.updateAgentScrollOffset()
		return p, nil

	default:
		// Handle search input
		if len(msg.String()) == 1 {
			p.agentSearchQuery += msg.String()
			p.filterAgents()
			p.agentSelectedIdx = 0
			p.agentScrollOffset = 0
			return p, nil
		}

		// Handle backspace in search
		if msg.String() == "backspace" && len(p.agentSearchQuery) > 0 {
			p.agentSearchQuery = p.agentSearchQuery[:len(p.agentSearchQuery)-1]
			p.filterAgents()
			p.agentSelectedIdx = 0
			p.agentScrollOffset = 0
			return p, nil
		}

		// Special handling for ESC key variants that might not match the case above
		if strings.Contains(strings.ToLower(keyStr), "esc") {
			log.Printf("[AGENT_DIALOG] ESC variant detected - closing dialog")
			p.showAgentDialog = false
			p.mode = "normal"
			log.Printf("[AGENT_DIALOG] After ESC variant - showAgentDialog: %v, mode: %s", p.showAgentDialog, p.mode)
			return p, nil
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

// updateCompletionScrollOffset updates the scroll offset to keep the selected item visible
func (p *InputPanel) updateCompletionScrollOffset() {
	if len(p.completionCommands) == 0 {
		return
	}

	// Calculate dialog dimensions
	dialogHeight := min(p.height-4, 15) // Smaller height for completion dialog
	maxCommands := dialogHeight - 4     // Reserve space for header and borders

	// If all commands fit, no scrolling needed
	if len(p.completionCommands) <= maxCommands {
		p.completionScrollOffset = 0
		return
	}

	// Ensure selected item is visible
	if p.completionSelectedIdx < p.completionScrollOffset {
		// Selected item is above visible area, scroll up
		p.completionScrollOffset = p.completionSelectedIdx
	} else if p.completionSelectedIdx >= p.completionScrollOffset+maxCommands {
		// Selected item is below visible area, scroll down
		p.completionScrollOffset = p.completionSelectedIdx - maxCommands + 1
	}

	// Ensure scroll offset is within bounds
	maxScrollOffset := len(p.completionCommands) - maxCommands
	if p.completionScrollOffset > maxScrollOffset {
		p.completionScrollOffset = maxScrollOffset
	}
	if p.completionScrollOffset < 0 {
		p.completionScrollOffset = 0
	}
}

// handleTab processes tab completion
func (p *InputPanel) handleTab() (tea.Model, tea.Cmd) {
	if strings.HasPrefix(p.buffer, "/") {
		commands := []string{"/help", "/clear", "/session", "/new", "/delete", "/theme", "/model", "/models", "/agent", "/agents"}

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
	if p.cursorPosition == 0 {
		return p, nil
	}

	p.buffer = p.buffer[:p.cursorPosition-1] + p.buffer[p.cursorPosition:]
	p.cursorPosition--
	p.refreshCompletionCommands()
	return p, p.syncInputState()
}

// handleDelete handles delete key
func (p *InputPanel) handleDelete() (tea.Model, tea.Cmd) {
	if p.cursorPosition >= len(p.buffer) {
		return p, nil
	}

	p.buffer = p.buffer[:p.cursorPosition] + p.buffer[p.cursorPosition+1:]
	p.refreshCompletionCommands()
	return p, p.syncInputState()
}

// insertCharacter inserts a character at the cursor position
func (p *InputPanel) insertCharacter(char string) (tea.Model, tea.Cmd) {
	p.buffer = p.buffer[:p.cursorPosition] + char + p.buffer[p.cursorPosition:]
	p.cursorPosition += len(char)

	// Show completion dialog when "/" is typed at the beginning of the buffer
	if char == "/" && p.cursorPosition == 1 {
		p.showCompletionDialog = true
		p.completionSelectedIdx = 0
		p.completionScrollOffset = 0
	}

	p.refreshCompletionCommands()

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
	p.refreshCompletionCommands()
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
		p.showHelp = true
	case "/models":
		cmdToExecute = p.openModelDialog()
	case "/agents":
		cmdToExecute = p.openAgentDialog()
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
	case "/compact":
		cmdToExecute = p.compactCurrentSession()
	}
	// Combine input state sync with the command execution
	if cmdToExecute != nil {
		return p, tea.Batch(p.syncInputState(), cmdToExecute)
	}

	return p, p.syncInputState()
}

func (p *InputPanel) openModelDialog() tea.Cmd {
	p.showModelDialog = true
	p.mode = "model_select"
	p.modelSearchQuery = ""
	p.modelSelectedIdx = 0
	p.modelScrollOffset = 0
	p.availableModels = nil

	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		resp, err := p.client.App.Providers(ctx, opencode.AppProvidersParams{})
		if err != nil {
			return ErrorMsg{Error: fmt.Errorf("failed to load models: %w", err)}
		}

		if resp == nil {
			return ErrorMsg{Error: errors.New("no models available")}
		}

		models := make([]ModelInfo, 0, len(resp.Providers))
		seen := map[string]struct{}{}

		for _, provider := range resp.Providers {
			for id, model := range provider.Models {
				key := provider.ID + "/" + id
				if _, ok := seen[key]; ok {
					continue
				}
				name := strings.TrimSpace(model.Name)
				if name == "" {
					name = id
				}
				models = append(models, ModelInfo{
					Provider: provider.ID,
					Model:    id,
					Name:     name,
				})
				seen[key] = struct{}{}
			}
		}

		for providerID, modelID := range resp.Default {
			if providerID == "" || modelID == "" {
				continue
			}
			key := providerID + "/" + modelID
			if _, ok := seen[key]; ok {
				continue
			}
			models = append(models, ModelInfo{
				Provider: providerID,
				Model:    modelID,
				Name:     modelID,
			})
			seen[key] = struct{}{}
		}

		if len(models) == 0 {
			return ErrorMsg{Error: errors.New("no models available")}
		}

		sort.Slice(models, func(i, j int) bool {
			if models[i].Provider == models[j].Provider {
				if models[i].Name == models[j].Name {
					return models[i].Model < models[j].Model
				}
				return models[i].Name < models[j].Name
			}
			return models[i].Provider < models[j].Provider
		})

		log.Printf("[MODEL_DIALOG] Loaded %d models from API", len(models))
		return ModelsLoadedMsg{Models: models}
	}
}

func (p *InputPanel) openAgentDialog() tea.Cmd {
	p.showAgentDialog = true
	p.mode = "agent_select"
	p.agentSearchQuery = ""
	p.agentSelectedIdx = 0
	p.agentScrollOffset = 0
	p.availableAgents = nil

	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		res, err := p.client.Agent.List(ctx, opencode.AgentListParams{})
		if err != nil {
			return ErrorMsg{Error: fmt.Errorf("failed to load agents: %w", err)}
		}

		if res == nil {
			return ErrorMsg{Error: errors.New("no agents available")}
		}

		agents := make([]AgentInfo, 0, len(*res))
		for _, agent := range *res {
			desc := strings.TrimSpace(agent.Description)
			if desc == "" {
				desc = string(agent.Mode)
			}
			agents = append(agents, AgentInfo{
				Name:        agent.Name,
				Description: desc,
			})
		}

		if len(agents) == 0 {
			return ErrorMsg{Error: errors.New("no agents available")}
		}

		sort.Slice(agents, func(i, j int) bool {
			left := strings.ToLower(agents[i].Name)
			right := strings.ToLower(agents[j].Name)
			if left == right {
				return strings.ToLower(agents[i].Description) < strings.ToLower(agents[j].Description)
			}
			return left < right
		})

		log.Printf("[AGENT_DIALOG] Loaded %d agents from API", len(agents))
		return AgentsLoadedMsg{Agents: agents}
	}
}

// sendMessage sends the current buffer as a message
func (p *InputPanel) sendMessage() tea.Cmd {
	message := strings.TrimSpace(p.buffer)
	if message == "" {
		return nil
	}

	if p.currentSessionID != "" {
		return p.makeSendCommand(message, p.currentSessionID)
	}

	if p.cachedState != nil && len(p.cachedState.Sessions) > 0 {
		return func() tea.Msg {
			return ErrorMsg{Error: fmt.Errorf("no session selected")}
		}
	}

	return p.createSessionAndSend(message)
}

func (p *InputPanel) makeSendCommand(message, sessionID string) tea.Cmd {
	return func() tea.Msg {
		p.addToHistory(message)

		go func(session, userMsg string) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			response, err := p.client.Session.Prompt(ctx, session, opencode.SessionPromptParams{
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

			if response == nil {
				return
			}

			if response.Info.Error.Name != "" {
				log.Printf("[INPUT] Assistant message error: %s - %v", response.Info.Error.Name, response.Info.Error.Data)
			}
		}(sessionID, message)

		return MessageSentMsg{}
	}
}

func (p *InputPanel) createSessionAndSend(message string) tea.Cmd {
	return func() tea.Msg {
		title := fmt.Sprintf("New Session %s", time.Now().Format("15:04:05"))

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		session, err := p.client.Session.New(ctx, opencode.SessionNewParams{
			Title: opencode.F(title),
		})
		if err != nil {
			log.Printf("[INPUT] Failed to create session before sending message: %v", err)
			return ErrorMsg{Error: fmt.Errorf("failed to create session: %w", err)}
		}

		info := types.SessionInfo{
			ID:           session.ID,
			Title:        session.Title,
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
			MessageCount: 0,
			IsActive:     true,
		}

		update := types.StateUpdate{
			Type: types.SessionAdded,
			Payload: types.SessionAddPayload{
				Session: info,
			},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
		}

		_, err = p.sendUpdateWithRetry(update)
		if err != nil {
			log.Printf("[INPUT] Failed to send session state update: %v", err)
			return ErrorMsg{Error: err}
		}

		if p.cachedState != nil {
			p.cachedState.Sessions = append(p.cachedState.Sessions, info)
			p.cachedState.CurrentSessionID = session.ID
		}

		change := types.StateUpdate{
			Type:        types.SessionChanged,
			Payload:     types.SessionChangePayload{SessionID: session.ID},
			SourcePanel: "input-panel",
			Timestamp:   time.Now(),
		}

		_, err = p.sendUpdateWithRetry(change)
		if err != nil {
			log.Printf("[INPUT] Failed to broadcast session change: %v", err)
		}

		p.currentSessionID = session.ID
		p.currentSessionTitle = session.Title

		return p.makeSendCommand(message, session.ID)()
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

				// Update current model information
				p.currentProvider = payload.State.Provider
				p.currentModel = payload.State.Model
				log.Printf("[INPUT] Model info updated: Provider='%s', Model='%s'", p.currentProvider, p.currentModel)

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

// handleUIActionTriggered handles UI action triggered events
func (p *InputPanel) handleUIActionTriggered(event types.StateEvent) error {
	log.Printf("[INPUT] Received UI action triggered event: %+v", event)

	// Extract action from the event payload
	if payloadMap, ok := event.Data.(map[string]interface{}); ok {
		if actionRaw, exists := payloadMap["action"]; exists {
			if action, ok := actionRaw.(string); ok {
				log.Printf("[INPUT] UI action: %s", action)

				// Handle open_models action by showing model selection dialog
				if action == "open_models" {
					cmd := p.openModelDialog()
					if p.program != nil {
						p.program.Send(InfoMsg{Message: "Model selection dialog opened"})
					}
					if cmd != nil {
						go func() {
							msg := cmd()
							if msg == nil {
								return
							}
							if p.program != nil {
								p.program.Send(msg)
								return
							}
							log.Printf("[MODEL_DIALOG] Dropped message because program is nil: %#v", msg)
						}()
					}
				}

				// Handle open_agents action by showing agent selection dialog
				if action == "open_agents" {
					cmd := p.openAgentDialog()
					if p.program != nil {
						p.program.Send(InfoMsg{Message: "Agent selection dialog opened"})
					}
					if cmd != nil {
						go func() {
							msg := cmd()
							if msg == nil {
								return
							}
							if p.program != nil {
								p.program.Send(msg)
								return
							}
							log.Printf("[AGENT_DIALOG] Dropped message because program is nil: %#v", msg)
						}()
					}
				}

				return nil
			}
		}
	}

	log.Printf("[INPUT] Failed to extract action from UI action event payload")
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
	return func() tea.Msg {
		if p.currentSessionID == "" {
			log.Printf("[INPUT] No session selected, cannot clear messages")
			return ErrorMsg{Error: fmt.Errorf("no session selected")}
		}

		log.Printf("[INPUT] Clearing messages for session %s", p.currentSessionID)

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// First, delete messages from backend
		response, err := p.client.Session.ClearMessages(ctx, p.currentSessionID)
		if err != nil {
			log.Printf("[INPUT] Failed to clear messages from backend: %v", err)
			return ErrorMsg{Error: fmt.Errorf("failed to clear messages: %w", err)}
		}

		cleared := int64(0)
		if response != nil {
			cleared = response.Count
		}

		log.Printf("[INPUT] Successfully cleared %d messages from backend for session %s", cleared, p.currentSessionID)

		if err := purgeLocalSessionMessages(p.currentSessionID); err != nil {
			log.Printf("[INPUT] Failed to purge local messages for session %s: %v", p.currentSessionID, err)
		}

		// Then, send clear messages request via IPC to update local state
		if err := p.ipcClient.SendClearSessionMessages(p.currentSessionID); err != nil {
			log.Printf("[INPUT] Failed to sync clear messages state: %v", err)
			return ErrorMsg{Error: fmt.Errorf("failed to sync state: %w", err)}
		}

		log.Printf("[INPUT] Successfully cleared messages for session %s", p.currentSessionID)

		// Ensure we have the latest state after clearing
		sessionID := p.currentSessionID
		if currentState, err := p.ipcClient.RequestState(); err == nil && currentState != nil {
			p.cachedState = currentState
			if currentState.CurrentSessionID != "" {
				p.currentSessionID = currentState.CurrentSessionID
			} else {
				p.currentSessionID = sessionID
			}
		} else {
			p.currentSessionID = sessionID
		}

		// Reset input-related UI state
		p.buffer = ""
		p.cursorPosition = 0
		p.selectionStart = 0
		p.selectionEnd = 0
		p.mode = "normal"
		p.showCompletionDialog = false
		p.completionCommands = nil
		p.completionSelectedIdx = 0
		p.completionScrollOffset = 0

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

func (p *InputPanel) compactCurrentSession() tea.Cmd {
	sessionID := p.currentSessionID
	if sessionID == "" {
		return func() tea.Msg {
			return ErrorMsg{Error: fmt.Errorf("no session selected")}
		}
	}

	provider, model, err := p.resolveProviderAndModel()
	if err != nil {
		return func() tea.Msg {
			return ErrorMsg{Error: err}
		}
	}

	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, apiErr := p.client.Session.Summarize(
			ctx,
			sessionID,
			opencode.SessionSummarizeParams{
				ProviderID: opencode.F(provider),
				ModelID:    opencode.F(model),
			},
		)
		if apiErr != nil {
			return ErrorMsg{Error: fmt.Errorf("failed to compact session: %w", apiErr)}
		}

		return InfoMsg{Message: "Session compact request sent"}
	}
}

func (p *InputPanel) resolveProviderAndModel() (string, string, error) {
	if p.cachedState != nil && p.cachedState.Provider != "" && p.cachedState.Model != "" {
		return p.cachedState.Provider, p.cachedState.Model, nil
	}

	state, err := p.ipcClient.RequestState()
	if err != nil {
		return "", "", fmt.Errorf("failed to fetch application state: %w", err)
	}

	p.cachedState = state

	if provider := strings.TrimSpace(state.Provider); provider != "" && strings.TrimSpace(state.Model) != "" {
		return state.Provider, state.Model, nil
	}

	provider, model, err := p.determineProviderAndModel(state)
	if err != nil {
		return "", "", err
	}

	p.cachedState.Provider = provider
	p.cachedState.Model = model
	if p.cachedState.AgentModel == nil {
		p.cachedState.AgentModel = map[string]string{}
	}
	if state.Agent != "" {
		p.cachedState.AgentModel[state.Agent] = provider + "/" + model
	}

	return provider, model, nil
}

func (p *InputPanel) determineProviderAndModel(state *types.SharedApplicationState) (string, string, error) {
	if state.AgentModel != nil {
		if entry, ok := state.AgentModel[state.Agent]; ok && entry != "" {
			if provider, model := splitProviderModel(entry); provider != "" && model != "" {
				return provider, model, nil
			}
		}
	}

	if provider, model, err := p.resolveProviderFromAgents(state); err == nil && provider != "" && model != "" {
		return provider, model, nil
	}

	return p.resolveProviderFromProviders()
}

func (p *InputPanel) resolveProviderFromAgents(state *types.SharedApplicationState) (string, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	agents, err := p.client.Agent.List(ctx, opencode.AgentListParams{})
	if err != nil {
		return "", "", fmt.Errorf("failed to load agents: %w", err)
	}

	if agents == nil || len(*agents) == 0 {
		return "", "", fmt.Errorf("no agents available")
	}

	targetName := state.Agent
	var selected opencode.Agent
	found := false

	if targetName != "" {
		for _, agent := range *agents {
			if strings.EqualFold(agent.Name, targetName) {
				selected = agent
				found = true
				break
			}
		}
	}

	if !found {
		// Prefer first primary agent
		for _, agent := range *agents {
			if agent.Mode != opencode.AgentModeSubagent {
				selected = agent
				found = true
				break
			}
		}
	}

	if !found {
		selected = (*agents)[0]
	}

	provider := selected.Model.ProviderID
	model := selected.Model.ModelID

	if provider == "" || model == "" {
		if entry, ok := state.AgentModel[selected.Name]; ok && entry != "" {
			provider, model = splitProviderModel(entry)
		}
	}

	if provider == "" || model == "" {
		return "", "", fmt.Errorf("agent does not specify provider/model")
	}

	return provider, model, nil
}

func (p *InputPanel) resolveProviderFromProviders() (string, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := p.client.App.Providers(ctx, opencode.AppProvidersParams{})
	if err != nil {
		return "", "", fmt.Errorf("failed to load providers: %w", err)
	}

	for providerID, modelID := range resp.Default {
		if providerID != "" && modelID != "" {
			return providerID, modelID, nil
		}
	}

	for _, provider := range resp.Providers {
		for modelID := range provider.Models {
			if modelID != "" {
				return provider.ID, modelID, nil
			}
		}
	}

	return "", "", fmt.Errorf("no providers or models available")
}

func splitProviderModel(entry string) (string, string) {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return "", ""
	}
	if strings.Contains(entry, "/") {
		parts := strings.SplitN(entry, "/", 2)
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	return "", strings.TrimSpace(entry)
}

// cycleTheme cycles through comfortable themes
// handleModelDialogKeys handles keyboard input when model selection dialog is active
func (p *InputPanel) handleModelDialogKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	keyStr := msg.String()

	switch keyStr {
	case "escape", "esc", "ctrl+c":
		// Close model dialog
		p.showModelDialog = false
		p.mode = "normal"
		return p, nil

	case "enter":
		// Select current model
		if p.modelSelectedIdx < len(p.availableModels) {
			selectedModel := p.availableModels[p.modelSelectedIdx]
			p.showModelDialog = false
			p.mode = "normal"
			return p, p.changeModel(selectedModel.Provider, selectedModel.Model)
		}
		return p, nil

	case "up":
		// Move selection up
		if p.modelSelectedIdx > 0 {
			p.modelSelectedIdx--
			p.updateModelScrollOffset()
		}
		return p, nil

	case "down":
		// Move selection down
		if p.modelSelectedIdx < len(p.availableModels)-1 {
			p.modelSelectedIdx++
			p.updateModelScrollOffset()
		}
		return p, nil

	case "page_up", "ctrl+u":
		// Page up in model list
		dialogHeight := min(p.height-4, 20)
		maxModels := dialogHeight - 8
		if p.modelSearchQuery != "" {
			maxModels = dialogHeight - 5
		}

		p.modelSelectedIdx = max(0, p.modelSelectedIdx-maxModels)
		p.updateModelScrollOffset()
		return p, nil

	case "page_down", "ctrl+d":
		// Page down in model list
		dialogHeight := min(p.height-4, 20)
		maxModels := dialogHeight - 8
		if p.modelSearchQuery != "" {
			maxModels = dialogHeight - 5
		}

		p.modelSelectedIdx = min(len(p.availableModels)-1, p.modelSelectedIdx+maxModels)
		p.updateModelScrollOffset()
		return p, nil

	case "home", "ctrl+a":
		// Go to first model
		p.modelSelectedIdx = 0
		p.modelScrollOffset = 0
		return p, nil

	case "end", "ctrl+e":
		// Go to last model
		p.modelSelectedIdx = len(p.availableModels) - 1
		p.updateModelScrollOffset()
		return p, nil

	default:
		// Handle search input
		if len(msg.String()) == 1 {
			p.modelSearchQuery += msg.String()
			p.filterModels()
			p.modelSelectedIdx = 0
			p.modelScrollOffset = 0
			return p, nil
		}

		// Handle backspace in search
		if msg.String() == "backspace" && len(p.modelSearchQuery) > 0 {
			p.modelSearchQuery = p.modelSearchQuery[:len(p.modelSearchQuery)-1]
			p.filterModels()
			p.modelSelectedIdx = 0
			p.modelScrollOffset = 0
			return p, nil
		}

		// Special handling for ESC key variants that might not match the case above
		if strings.Contains(strings.ToLower(keyStr), "esc") {
			log.Printf("[MODEL_DIALOG] ESC variant detected - closing dialog")
			p.showModelDialog = false
			p.mode = "normal"
			log.Printf("[MODEL_DIALOG] After ESC variant - showModelDialog: %v, mode: %s", p.showModelDialog, p.mode)
			return p, nil
		}

		log.Printf("[MODEL_DIALOG] Unhandled key: %q", keyStr)
	}

	return p, nil
}

// updateModelScrollOffset adjusts scroll offset to keep selected item visible
func (p *InputPanel) updateModelScrollOffset() {
	dialogHeight := min(p.height-4, 20)
	maxModels := dialogHeight - 8 // Reserve space for header, search, etc.
	if p.modelSearchQuery != "" {
		maxModels = dialogHeight - 5 // Less space needed when no recent section
	}

	// Ensure selected item is visible
	if p.modelSelectedIdx < p.modelScrollOffset {
		p.modelScrollOffset = p.modelSelectedIdx
	} else if p.modelSelectedIdx >= p.modelScrollOffset+maxModels {
		p.modelScrollOffset = p.modelSelectedIdx - maxModels + 1
	}

	// Ensure scroll offset doesn't go negative
	if p.modelScrollOffset < 0 {
		p.modelScrollOffset = 0
	}

	// Ensure scroll offset doesn't exceed available models
	maxOffset := len(p.availableModels) - maxModels
	if maxOffset < 0 {
		maxOffset = 0
	}
	if p.modelScrollOffset > maxOffset {
		p.modelScrollOffset = maxOffset
	}
}

// filterModels filters available models based on search query
func (p *InputPanel) filterModels() {
	if len(p.allModels) == 0 {
		p.availableModels = nil
		p.modelSelectedIdx = 0
		return
	}

	models := make([]ModelInfo, 0, len(p.allModels))
	if p.modelSearchQuery != "" {
		query := strings.ToLower(p.modelSearchQuery)
		for _, model := range p.allModels {
			name := strings.ToLower(strings.TrimSpace(model.Name))
			if name == "" {
				name = strings.ToLower(model.Model)
			}
			if strings.Contains(name, query) {
				models = append(models, model)
				continue
			}
			if strings.Contains(strings.ToLower(model.Model), query) {
				models = append(models, model)
				continue
			}
			if strings.Contains(strings.ToLower(model.Provider), query) {
				models = append(models, model)
			}
		}
	}
	if p.modelSearchQuery == "" {
		models = append(models, p.allModels...)
	}

	p.availableModels = models
	if p.modelSelectedIdx >= len(p.availableModels) {
		p.modelSelectedIdx = 0
	}
}

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

	// Mode indicator with current model information
	modeText := fmt.Sprintf("Mode: %s", p.mode)
	if p.currentProvider != "" && p.currentModel != "" {
		modeText += fmt.Sprintf(" | Model: %s/%s", p.currentProvider, p.currentModel)
	}
	if len(p.history) > 0 {
		modeText += fmt.Sprintf(" | History: %d items", len(p.history))
	}
	log.Printf("[INPUT] Rendering mode text: '%s' (Provider='%s', Model='%s')", modeText, p.currentProvider, p.currentModel)

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

func (p *InputPanel) renderModelDialog() string {
	var result strings.Builder

	// Calculate dialog dimensions
	dialogWidth := min(p.width-4, 60)   // Leave margin and max width
	dialogHeight := min(p.height-4, 20) // Leave margin and max height

	// Top border
	result.WriteString("┌" + strings.Repeat("─", dialogWidth-2) + "┐\n")

	// Title line with close hint
	title := " Select Model "
	closeHint := " esc "
	padding := dialogWidth - len(title) - len(closeHint) - 2
	if padding < 0 {
		padding = 0
	}
	result.WriteString("│" + title + strings.Repeat(" ", padding) + closeHint + "│\n")

	// Separator
	result.WriteString("├" + strings.Repeat("─", dialogWidth-2) + "┤\n")

	// Search box
	searchPrompt := " 🔍 Search models..."
	if p.modelSearchQuery != "" {
		searchPrompt = " 🔍 " + p.modelSearchQuery
	}
	searchPadding := dialogWidth - len(searchPrompt) - 2
	if searchPadding < 0 {
		searchPadding = 0
	}
	result.WriteString("│" + searchPrompt + strings.Repeat(" ", searchPadding) + "│\n")

	// Empty line
	result.WriteString("│" + strings.Repeat(" ", dialogWidth-2) + "│\n")

	// Recent section (if no search query)
	if p.modelSearchQuery == "" {
		recentTitle := " Recent"
		recentPadding := dialogWidth - len(recentTitle) - 2
		if recentPadding < 0 {
			recentPadding = 0
		}
		result.WriteString("│" + recentTitle + strings.Repeat(" ", recentPadding) + "│\n")

		// Recent model
		recentModel := "   Grok Code Fast 1  OpenCode Zen"
		if len(recentModel) > dialogWidth-2 {
			recentModel = recentModel[:dialogWidth-5] + "..."
		}
		recentPadding = dialogWidth - len(recentModel) - 2
		if recentPadding < 0 {
			recentPadding = 0
		}
		result.WriteString("│" + recentModel + strings.Repeat(" ", recentPadding) + "│\n")

		// Empty line
		result.WriteString("│" + strings.Repeat(" ", dialogWidth-2) + "│\n")

		// Provider section
		providerTitle := " OpenCode Zen"
		providerPadding := dialogWidth - len(providerTitle) - 2
		if providerPadding < 0 {
			providerPadding = 0
		}
		result.WriteString("│" + providerTitle + strings.Repeat(" ", providerPadding) + "│\n")
	}

	// Calculate available space for models
	maxModels := dialogHeight - 8 // Reserve space for header, search, etc.
	if p.modelSearchQuery != "" {
		maxModels = dialogHeight - 5 // Less space needed when no recent section
	}

	// Calculate scroll indicators
	totalModels := len(p.availableModels)
	showScrollUp := p.modelScrollOffset > 0
	showScrollDown := p.modelScrollOffset+maxModels < totalModels

	// Add scroll up indicator if needed
	if showScrollUp {
		scrollUpLine := strings.Repeat(" ", (dialogWidth-3)/2) + "↑" + strings.Repeat(" ", (dialogWidth-3)/2)
		if len(scrollUpLine) > dialogWidth-2 {
			scrollUpLine = scrollUpLine[:dialogWidth-2]
		}
		scrollUpPadding := dialogWidth - len(scrollUpLine) - 2
		if scrollUpPadding < 0 {
			scrollUpPadding = 0
		}
		result.WriteString("│" + scrollUpLine + strings.Repeat(" ", scrollUpPadding) + "│\n")
		maxModels-- // Reduce available space for models
	}

	// Render visible models with scroll offset
	visibleStart := p.modelScrollOffset
	visibleEnd := min(visibleStart+maxModels, totalModels)

	for i := visibleStart; i < visibleEnd; i++ {
		model := p.availableModels[i]

		prefix := "   "
		if i == p.modelSelectedIdx {
			prefix = " ▶ " // Selection indicator with proper spacing
		}

		modelLine := prefix + model.Name + "  " + model.Provider
		if len(modelLine) > dialogWidth-2 {
			modelLine = modelLine[:dialogWidth-5] + "..."
		}

		modelPadding := dialogWidth - len(modelLine) - 2
		if modelPadding < 0 {
			modelPadding = 0
		}
		result.WriteString("│" + modelLine + strings.Repeat(" ", modelPadding) + "│\n")

		// Add underline for selected model on the next line
		if i == p.modelSelectedIdx {
			// Create underline for the model name part only
			nameLength := len(model.Name)
			prefixLength := 4                            // Length of " ▶ "
			if nameLength > dialogWidth-prefixLength-6 { // Account for prefix, provider and padding
				nameLength = dialogWidth - prefixLength - 6
			}
			underline := strings.Repeat("─", nameLength)
			underlineLine := strings.Repeat(" ", prefixLength) + underline
			underlinePadding := dialogWidth - len(underlineLine) - 2
			if underlinePadding < 0 {
				underlinePadding = 0
			}
			result.WriteString("│" + underlineLine + strings.Repeat(" ", underlinePadding) + "│\n")
		}
	}

	// Add scroll down indicator if needed
	if showScrollDown {
		scrollDownLine := strings.Repeat(" ", (dialogWidth-3)/2) + "↓" + strings.Repeat(" ", (dialogWidth-3)/2)
		if len(scrollDownLine) > dialogWidth-2 {
			scrollDownLine = scrollDownLine[:dialogWidth-2]
		}
		scrollDownPadding := dialogWidth - len(scrollDownLine) - 2
		if scrollDownPadding < 0 {
			scrollDownPadding = 0
		}
		result.WriteString("│" + scrollDownLine + strings.Repeat(" ", scrollDownPadding) + "│\n")
		maxModels-- // Account for scroll indicator space
	}

	// Fill remaining space if needed
	currentLines := 5 // header lines
	if p.modelSearchQuery == "" {
		currentLines += 4 // recent section lines
	}
	if showScrollUp {
		currentLines++
	}
	currentLines += (visibleEnd - visibleStart) // visible models
	if showScrollDown {
		currentLines++
	}

	for currentLines < dialogHeight-1 {
		result.WriteString("│" + strings.Repeat(" ", dialogWidth-2) + "│\n")
		currentLines++
	}

	// Bottom border
	result.WriteString("└" + strings.Repeat("─", dialogWidth-2) + "┘")

	// Add scroll position indicator in bottom right corner if there are many models
	if totalModels > maxModels {
		scrollInfo := fmt.Sprintf(" %d/%d ", p.modelSelectedIdx+1, totalModels)
		bottomLine := result.String()
		lines := strings.Split(bottomLine, "\n")
		if len(lines) > 0 {
			lastLine := lines[len(lines)-1]
			if len(lastLine) >= len(scrollInfo)+1 {
				// Replace part of bottom border with scroll info
				newLastLine := lastLine[:len(lastLine)-len(scrollInfo)-1] + scrollInfo + "┘"
				lines[len(lines)-1] = newLastLine
				result.Reset()
				result.WriteString(strings.Join(lines, "\n"))
			}
		}
	}

	return result.String()
}

func (p *InputPanel) refreshCompletionCommands() {
	if !p.showCompletionDialog {
		return
	}

	if !strings.HasPrefix(p.buffer, "/") {
		p.showCompletionDialog = false
		p.completionCommands = nil
		p.completionSelectedIdx = 0
		p.completionScrollOffset = 0
		return
	}

	word := strings.TrimPrefix(p.buffer, "/")
	if idx := strings.Index(word, " "); idx >= 0 {
		word = word[:idx]
	}

	filtered := make([]string, 0, len(completionSuggestions))
	for _, cmd := range completionSuggestions {
		if word == "" || strings.HasPrefix(cmd, word) {
			filtered = append(filtered, cmd)
		}
	}

	p.completionCommands = filtered
	if len(filtered) == 0 {
		p.completionSelectedIdx = 0
		p.completionScrollOffset = 0
		return
	}

	if p.completionSelectedIdx >= len(filtered) {
		p.completionSelectedIdx = len(filtered) - 1
	}

	if p.completionSelectedIdx < 0 {
		p.completionSelectedIdx = 0
	}

	if p.completionScrollOffset >= len(filtered) {
		p.completionScrollOffset = 0
	}

	p.updateCompletionScrollOffset()
}

func (p *InputPanel) renderAgentDialog() string {
	var result strings.Builder

	log.Printf("[AGENT_DIALOG] Rendering agent dialog - availableAgents count: %d, selectedIdx: %d, scrollOffset: %d, mode: %s", len(p.availableAgents), p.agentSelectedIdx, p.agentScrollOffset, p.mode)

	// Calculate dialog dimensions
	dialogWidth := min(p.width-4, 60)   // Leave margin and max width
	dialogHeight := min(p.height-4, 20) // Leave margin and max height

	// Top border
	result.WriteString("┌" + strings.Repeat("─", dialogWidth-2) + "┐\n")

	// Title line with close hint
	title := " Select Agent "
	closeHint := " esc "
	padding := dialogWidth - len(title) - len(closeHint) - 2
	if padding < 0 {
		padding = 0
	}
	result.WriteString("│" + title + strings.Repeat(" ", padding) + closeHint + "│\n")

	// Separator
	result.WriteString("├" + strings.Repeat("─", dialogWidth-2) + "┤\n")

	// Search box
	searchPrompt := " 🔍 Search agents..."
	if p.agentSearchQuery != "" {
		searchPrompt = " 🔍 " + p.agentSearchQuery
	}
	searchPadding := dialogWidth - len(searchPrompt) - 2
	if searchPadding < 0 {
		searchPadding = 0
	}
	result.WriteString("│" + searchPrompt + strings.Repeat(" ", searchPadding) + "│\n")

	// Empty line
	result.WriteString("│" + strings.Repeat(" ", dialogWidth-2) + "│\n")

	// Available agents section
	agentsTitle := " Available Agents"
	agentsPadding := dialogWidth - len(agentsTitle) - 2
	if agentsPadding < 0 {
		agentsPadding = 0
	}
	result.WriteString("│" + agentsTitle + strings.Repeat(" ", agentsPadding) + "│\n")

	// Calculate available space for agents
	maxAgents := dialogHeight - 7 // Reserve space for header, search, etc.
	if p.agentSearchQuery != "" {
		maxAgents = dialogHeight - 6 // Less space needed when no recent section
	}

	// Calculate scroll indicators
	totalAgents := len(p.availableAgents)
	showScrollUp := p.agentScrollOffset > 0
	showScrollDown := p.agentScrollOffset+maxAgents < totalAgents

	// Add scroll up indicator if needed
	if showScrollUp {
		scrollUpLine := strings.Repeat(" ", (dialogWidth-3)/2) + "↑" + strings.Repeat(" ", (dialogWidth-3)/2)
		if len(scrollUpLine) > dialogWidth-2 {
			scrollUpLine = scrollUpLine[:dialogWidth-2]
		}
		scrollUpPadding := dialogWidth - len(scrollUpLine) - 2
		if scrollUpPadding < 0 {
			scrollUpPadding = 0
		}
		result.WriteString("│" + scrollUpLine + strings.Repeat(" ", scrollUpPadding) + "│\n")
		maxAgents-- // Reduce available space for agents
	}

	// Render visible agents with scroll offset
	visibleStart := p.agentScrollOffset
	visibleEnd := min(visibleStart+maxAgents, totalAgents)

	for i := visibleStart; i < visibleEnd; i++ {
		agent := p.availableAgents[i]

		prefix := "   "
		if i == p.agentSelectedIdx {
			prefix = " ▶ " // Selection indicator with proper spacing
		}

		agentLine := prefix + agent.Name + "  " + agent.Description
		if len(agentLine) > dialogWidth-2 {
			agentLine = agentLine[:dialogWidth-5] + "..."
		}

		agentPadding := dialogWidth - len(agentLine) - 2
		if agentPadding < 0 {
			agentPadding = 0
		}
		result.WriteString("│" + agentLine + strings.Repeat(" ", agentPadding) + "│\n")

		// Add underline for selected agent on the next line
		if i == p.agentSelectedIdx {
			// Create underline for the agent name part only
			nameLength := len(agent.Name)
			prefixLength := 4                            // Length of " ▶ "
			if nameLength > dialogWidth-prefixLength-6 { // Account for prefix, description and padding
				nameLength = dialogWidth - prefixLength - 6
			}
			underline := strings.Repeat("─", nameLength)
			underlineLine := strings.Repeat(" ", prefixLength) + underline
			underlinePadding := dialogWidth - len(underlineLine) - 2
			if underlinePadding < 0 {
				underlinePadding = 0
			}
			result.WriteString("│" + underlineLine + strings.Repeat(" ", underlinePadding) + "│\n")
		}
	}

	// Add scroll down indicator if needed
	if showScrollDown {
		scrollDownLine := strings.Repeat(" ", (dialogWidth-3)/2) + "↓" + strings.Repeat(" ", (dialogWidth-3)/2)
		if len(scrollDownLine) > dialogWidth-2 {
			scrollDownLine = scrollDownLine[:dialogWidth-2]
		}
		scrollDownPadding := dialogWidth - len(scrollDownLine) - 2
		if scrollDownPadding < 0 {
			scrollDownPadding = 0
		}
		result.WriteString("│" + scrollDownLine + strings.Repeat(" ", scrollDownPadding) + "│\n")
		maxAgents-- // Account for scroll indicator space
	}

	// Fill remaining space if needed
	currentLines := 6 // header lines
	if showScrollUp {
		currentLines++
	}
	currentLines += (visibleEnd - visibleStart) // visible agents
	if showScrollDown {
		currentLines++
	}

	for currentLines < dialogHeight-1 {
		result.WriteString("│" + strings.Repeat(" ", dialogWidth-2) + "│\n")
		currentLines++
	}

	// Bottom border
	result.WriteString("└" + strings.Repeat("─", dialogWidth-2) + "┘")

	// Add scroll position indicator in bottom right corner if there are many agents
	if totalAgents > maxAgents {
		scrollInfo := fmt.Sprintf(" %d/%d ", p.agentSelectedIdx+1, totalAgents)
		bottomLine := result.String()
		lines := strings.Split(bottomLine, "\n")
		if len(lines) > 0 {
			lastLine := lines[len(lines)-1]
			if len(lastLine) >= len(scrollInfo)+1 {
				// Replace part of bottom border with scroll info
				newLastLine := lastLine[:len(lastLine)-len(scrollInfo)-1] + scrollInfo + "┘"
				lines[len(lines)-1] = newLastLine
				result.Reset()
				result.WriteString(strings.Join(lines, "\n"))
			}
		}
	}

	return result.String()
}

func (p *InputPanel) renderCompletionDialog() string {
	var result strings.Builder

	// Calculate dialog dimensions
	dialogWidth := min(p.width-4, 40)   // Smaller width for completion dialog
	dialogHeight := min(p.height-4, 15) // Smaller height for completion dialog

	// Top border
	result.WriteString("┌" + strings.Repeat("─", dialogWidth-2) + "┐\n")

	// Title line with close hint
	title := " Command Completion "
	closeHint := " esc "
	padding := dialogWidth - len(title) - len(closeHint) - 2
	if padding < 0 {
		padding = 0
	}
	result.WriteString("│" + title + strings.Repeat(" ", padding) + closeHint + "│\n")

	// Separator
	result.WriteString("├" + strings.Repeat("─", dialogWidth-2) + "┤\n")

	// Available commands section
	maxCommands := dialogHeight - 4 // Reserve space for header and borders

	// Calculate visible range
	totalCommands := len(p.completionCommands)
	visibleStart := p.completionScrollOffset
	visibleEnd := min(visibleStart+maxCommands, totalCommands)

	// Show scroll up indicator if needed
	if visibleStart > 0 {
		scrollUpLine := strings.Repeat(" ", (dialogWidth-3)/2) + "↑" + strings.Repeat(" ", (dialogWidth-3)/2)
		if len(scrollUpLine) > dialogWidth-2 {
			scrollUpLine = scrollUpLine[:dialogWidth-2]
		}
		scrollUpPadding := dialogWidth - len(scrollUpLine) - 2
		if scrollUpPadding < 0 {
			scrollUpPadding = 0
		}
		result.WriteString("│" + scrollUpLine + strings.Repeat(" ", scrollUpPadding) + "│\n")
		maxCommands-- // Account for scroll indicator space
	}

	// Show visible commands
	for i := visibleStart; i < visibleEnd; i++ {
		if i >= len(p.completionCommands) {
			break
		}

		command := p.completionCommands[i]

		// Highlight selected command
		prefix := " "
		if i == p.completionSelectedIdx {
			prefix = "▶"
		}

		commandLine := prefix + " " + command
		commandPadding := dialogWidth - len(commandLine) - 2
		if commandPadding < 0 {
			commandPadding = 0
		}
		result.WriteString("│" + commandLine + strings.Repeat(" ", commandPadding) + "│\n")
	}

	// Show scroll down indicator if needed
	if visibleEnd < totalCommands {
		scrollDownLine := strings.Repeat(" ", (dialogWidth-3)/2) + "↓" + strings.Repeat(" ", (dialogWidth-3)/2)
		if len(scrollDownLine) > dialogWidth-2 {
			scrollDownLine = scrollDownLine[:dialogWidth-2]
		}
		scrollDownPadding := dialogWidth - len(scrollDownLine) - 2
		if scrollDownPadding < 0 {
			scrollDownPadding = 0
		}
		result.WriteString("│" + scrollDownLine + strings.Repeat(" ", scrollDownPadding) + "│\n")
		maxCommands-- // Account for scroll indicator space
	}

	// Fill remaining space if needed
	currentLines := 3 // header lines
	if visibleStart > 0 {
		currentLines++ // scroll up indicator
	}
	currentLines += (visibleEnd - visibleStart) // visible commands
	if visibleEnd < totalCommands {
		currentLines++ // scroll down indicator
	}

	for currentLines < dialogHeight-1 {
		result.WriteString("│" + strings.Repeat(" ", dialogWidth-2) + "│\n")
		currentLines++
	}

	// Bottom border
	result.WriteString("└" + strings.Repeat("─", dialogWidth-2) + "┘")

	// Add scroll position indicator in bottom right corner if there are many commands
	if totalCommands > maxCommands {
		scrollInfo := fmt.Sprintf(" %d/%d ", p.completionSelectedIdx+1, totalCommands)
		bottomLine := result.String()
		lines := strings.Split(bottomLine, "\n")
		if len(lines) > 0 {
			lastLine := lines[len(lines)-1]
			if len(lastLine) >= len(scrollInfo)+1 {
				// Replace part of bottom border with scroll info
				newLastLine := lastLine[:len(lastLine)-len(scrollInfo)-1] + scrollInfo + "┘"
				lines[len(lines)-1] = newLastLine
				result.Reset()
				result.WriteString(strings.Join(lines, "\n"))
			}
		}
	}

	return result.String()
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

type ModelsLoadedMsg struct {
	Models []ModelInfo
}

type AgentsLoadedMsg struct {
	Agents []AgentInfo
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

func purgeLocalSessionMessages(sessionID string) error {
	root, err := resolveStorageRoot()
	if err != nil {
		return err
	}

	sessionDir := filepath.Join(root, "message", sessionID)
	entries, err := os.ReadDir(sessionDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
		if name == "" {
			continue
		}
		partDir := filepath.Join(root, "part", name)
		if removeErr := os.RemoveAll(partDir); removeErr != nil {
			log.Printf("[INPUT] Failed to remove message part directory %s: %v", partDir, removeErr)
		}
	}

	return os.RemoveAll(sessionDir)
}

func resolveStorageRoot() (string, error) {
	if base := os.Getenv("XDG_DATA_HOME"); base != "" {
		return filepath.Join(base, "opencode", "storage"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".local", "share", "opencode", "storage"), nil
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
		log.Printf("[INPUT] Clipboard content received successfully: %q (length: %d)", content, len(content))

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
	comfortableThemes := []string{"opencode", "dark", "light"} // Default comfortable themes
	currentTheme := "opencode"                                 // Default theme
	panel := NewInputPanel(httpClient, socketPath, comfortableThemes, currentTheme)

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
