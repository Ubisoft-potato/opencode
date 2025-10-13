package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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

// SessionsPanel manages the sessions list panel
type SessionsPanel struct {
	client       *opencode.Client
	ipcClient    *ipc.SocketClient
	syncManager  *state.PanelSyncManager
	sessions     []types.SessionInfo
	currentIndex int
	currentSessionID string
	width        int
	height       int
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewSessionsPanel creates a new sessions panel
func NewSessionsPanel(httpClient *opencode.Client, socketPath string) *SessionsPanel {
	ctx, cancel := context.WithCancel(context.Background())

	panel := &SessionsPanel{
		client:      httpClient,
		ipcClient:   ipc.NewSocketClient(socketPath, "sessions-panel", "sessions"),
		sessions:    make([]types.SessionInfo, 0),
		currentIndex: 0,
		ctx:         ctx,
		cancel:      cancel,
	}

	// Register event handlers
	panel.ipcClient.RegisterEventHandler(state.EventSessionAdded, panel.handleSessionAdded)
	panel.ipcClient.RegisterEventHandler(state.EventSessionDeleted, panel.handleSessionDeleted)
	panel.ipcClient.RegisterEventHandler(state.EventSessionUpdated, panel.handleSessionUpdated)
	panel.ipcClient.RegisterEventHandler(state.EventSessionChanged, panel.handleSessionChanged)
	panel.ipcClient.RegisterEventHandler(state.EventStateSync, panel.handleStateSync)

	return panel
}

// Init initializes the panel
func (p SessionsPanel) Init() tea.Cmd {
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

	return tea.Batch(cmds...)
}

// Update handles messages and updates the panel state
func (p SessionsPanel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		p.width = msg.Width
		p.height = msg.Height
		return p, nil

	case tea.KeyMsg:
		return p.handleKeyPress(msg)

	case ConnectedMsg:
		log.Printf("Sessions panel connected to IPC")
		return p, nil

	case StateLoadedMsg:
		p.sessions = msg.State.Sessions
		p.currentSessionID = msg.State.CurrentSessionID
		p.updateCurrentIndex()
		return p, nil

	case ErrorMsg:
		log.Printf("Sessions panel error: %v", msg.Error)
		return p, nil

	case SessionEventMsg:
		return p.handleSessionEvent(msg.Event)

	default:
		return p, nil
	}
}

// View renders the sessions panel
func (p SessionsPanel) View() string {
	if len(p.sessions) == 0 {
		return p.renderEmptyState()
	}

	return p.renderSessionsList()
}

// handleKeyPress processes keyboard input
func (p SessionsPanel) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		return p, tea.Quit

	case "up", "k":
		if p.currentIndex > 0 {
			p.currentIndex--
			return p, p.selectCurrentSession()
		}

	case "down", "j":
		if p.currentIndex < len(p.sessions)-1 {
			p.currentIndex++
			return p, p.selectCurrentSession()
		}

	case "enter":
		return p, p.selectCurrentSession()

	case "n":
		return p, p.createNewSession()

	case "d":
		if len(p.sessions) > 0 {
			return p, p.deleteCurrentSession()
		}

	case "r":
		return p, p.refreshSessions()
	}

	return p, nil
}

// selectCurrentSession sends a session selection update
func (p SessionsPanel) selectCurrentSession() tea.Cmd {
	if p.currentIndex >= 0 && p.currentIndex < len(p.sessions) {
		session := p.sessions[p.currentIndex]
		log.Printf("[SESSIONS] Selecting session: %s (index: %d)", session.ID, p.currentIndex)

		return func() tea.Msg {
			update := state.StateUpdate{
				Type:        state.SessionChanged,
				Payload:     state.SessionChangePayload{SessionID: session.ID},
				SourcePanel: "sessions-panel",
				Timestamp:   time.Now(),
			}

			if err := p.ipcClient.SendStateUpdateAndWait(update); err != nil {
				log.Printf("[SESSIONS] Failed to send SessionChanged event: %v", err)
				return ErrorMsg{Error: err}
			}

			log.Printf("[SESSIONS] Successfully sent and acknowledged SessionChanged event")
			return SessionSelectedMsg{SessionID: session.ID}
		}
	}
	log.Printf("[SESSIONS] Cannot select session - invalid index: %d (total: %d)", p.currentIndex, len(p.sessions))
	return nil
}

// createNewSession creates a new session
func (p SessionsPanel) createNewSession() tea.Cmd {
	return func() tea.Msg {
		// Create session via API
		session, err := p.client.Session.New(p.ctx, opencode.SessionNewParams{})
		if err != nil {
			return ErrorMsg{Error: fmt.Errorf("failed to create session: %w", err)}
		}

		// Convert to state format
		sessionInfo := types.SessionInfo{
			ID:           session.ID,
			Title:        session.Title,
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
			MessageCount: 0,
			IsActive:     true,
		}

		        // Send update
		        update := state.StateUpdate{
		            Type:        state.SessionAdded,
		            Payload:     state.SessionAddPayload{Session: sessionInfo},
		            SourcePanel: "sessions-panel",
		            Timestamp:   time.Now(),
		        }
		
		        if err := p.ipcClient.SendStateUpdateAndWait(update); err != nil {			return ErrorMsg{Error: err}
		}

		return SessionCreatedMsg{Session: sessionInfo}
	}
}

// deleteCurrentSession deletes the currently selected session
func (p SessionsPanel) deleteCurrentSession() tea.Cmd {
	if p.currentIndex >= 0 && p.currentIndex < len(p.sessions) {
		sessionID := p.sessions[p.currentIndex].ID
		return func() tea.Msg {
			// Delete via API
			if _, err := p.client.Session.Delete(p.ctx, sessionID, opencode.SessionDeleteParams{}); err != nil {
				return ErrorMsg{Error: fmt.Errorf("failed to delete session: %w", err)}
			}

			// Send update
			update := state.StateUpdate{
				Type:        state.SessionDeleted,
				Payload:     state.SessionDeletePayload{SessionID: sessionID},
				SourcePanel: "sessions-panel",
				Timestamp:   time.Now(),
			}

			if err := p.ipcClient.SendStateUpdateAndWait(update); err != nil {
				return ErrorMsg{Error: err}
			}

			return SessionDeletedMsg{SessionID: sessionID}
		}
	}
	return nil
}

// refreshSessions refreshes the sessions list from the API
func (p SessionsPanel) refreshSessions() tea.Cmd {
	return func() tea.Msg {
		sessions, err := p.client.Session.List(p.ctx, opencode.SessionListParams{})
		if err != nil {
			return ErrorMsg{Error: fmt.Errorf("failed to refresh sessions: %w", err)}
		}

		// Convert to state format
		sessionInfos := make([]types.SessionInfo, len(*sessions))
		for i, session := range *sessions {
			sessionInfos[i] = types.SessionInfo{
				ID:           session.ID,
				Title:        session.Title,
				CreatedAt:    time.Unix(int64(session.Time.Created), 0),
				UpdatedAt:    time.Unix(int64(session.Time.Updated), 0),
				MessageCount: 0,
				IsActive:     true,
			}
		}

		return SessionsRefreshedMsg{Sessions: sessionInfos}
	}
}

// Event handlers

func (p *SessionsPanel) handleSessionAdded(event state.StateEvent) error {
	if payload, ok := event.Data.(state.SessionAddPayload); ok {
		p.sessions = append(p.sessions, payload.Session)
		log.Printf("Session added: %s", payload.Session.ID)
	}
	return nil
}

func (p *SessionsPanel) handleSessionDeleted(event state.StateEvent) error {
	if payload, ok := event.Data.(state.SessionDeletePayload); ok {
		for i, session := range p.sessions {
			if session.ID == payload.SessionID {
				p.sessions = append(p.sessions[:i], p.sessions[i+1:]...)
				if p.currentIndex >= len(p.sessions) && len(p.sessions) > 0 {
					p.currentIndex = len(p.sessions) - 1
				}
				break
			}
		}
		log.Printf("Session deleted: %s", payload.SessionID)
	}
	return nil
}

func (p *SessionsPanel) handleSessionUpdated(event state.StateEvent) error {
	if payload, ok := event.Data.(state.SessionUpdatePayload); ok {
		for i, session := range p.sessions {
			if session.ID == payload.SessionID {
				if payload.Title != "" {
					p.sessions[i].Title = payload.Title
				}
				p.sessions[i].IsActive = payload.IsActive
				p.sessions[i].UpdatedAt = time.Now()
				break
			}
		}
		log.Printf("Session updated: %s", payload.SessionID)
	}
	return nil
}

func (p *SessionsPanel) handleSessionChanged(event state.StateEvent) error {
	if payload, ok := event.Data.(state.SessionChangePayload); ok {
		p.currentSessionID = payload.SessionID
		p.updateCurrentIndex()
		log.Printf("Session changed: %s", payload.SessionID)
	}
	return nil
}

func (p *SessionsPanel) handleStateSync(event state.StateEvent) error {
	if payload, ok := event.Data.(types.StateSyncPayload); ok {
		p.sessions = payload.State.Sessions
		p.currentSessionID = payload.State.CurrentSessionID
		p.updateCurrentIndex()
		log.Printf("State synchronized")
	}
	return nil
}

func (p *SessionsPanel) handleSessionEvent(event state.StateEvent) (SessionsPanel, tea.Cmd) {
	// Handle event processing here
	switch event.Type {
	case state.EventSessionAdded:
		p.handleSessionAdded(event)
	case state.EventSessionDeleted:
		p.handleSessionDeleted(event)
	case state.EventSessionUpdated:
		p.handleSessionUpdated(event)
	case state.EventSessionChanged:
		p.handleSessionChanged(event)
	case state.EventStateSync:
		p.handleStateSync(event)
	}
	return *p, nil
}

// updateCurrentIndex updates the current index based on current session ID
func (p *SessionsPanel) updateCurrentIndex() {
	for i, session := range p.sessions {
		if session.ID == p.currentSessionID {
			p.currentIndex = i
			return
		}
	}
	// If current session not found, select first session
	if len(p.sessions) > 0 {
		p.currentIndex = 0
	}
}

// renderEmptyState renders the empty sessions state
func (p SessionsPanel) renderEmptyState() string {
	t := theme.CurrentTheme()
	style := styles.NewStyle().
		Foreground(t.TextMuted()).
		Align(styles.Center).
		Width(p.width).
		Height(p.height)

	return style.Render("No sessions\n\nPress 'n' to create a new session")
}

// renderSessionsList renders the list of sessions
func (p SessionsPanel) renderSessionsList() string {
	t := theme.CurrentTheme()

	var content string
	content += styles.NewStyle().
		Foreground(t.Primary()).
		Bold(true).
		Render("Sessions") + "\n\n"

	for i, session := range p.sessions {
		isSelected := i == p.currentIndex
		isCurrent := session.ID == p.currentSessionID

		var style styles.Style
		if isSelected {
			style = styles.NewStyle().
				Background(t.BackgroundElement()).
				Foreground(t.Text()).
				Padding(0, 1)
		} else {
			style = styles.NewStyle().
				Foreground(t.Text()).
				Padding(0, 1)
		}

		var indicator string
		if isCurrent {
			indicator = "● "
		} else {
			indicator = "  "
		}

		title := session.Title
		if title == "" {
			title = fmt.Sprintf("Session %s", session.ID[:8])
		}

		sessionLine := fmt.Sprintf("%s%s (%d msgs)", indicator, title, session.MessageCount)
		content += style.Render(sessionLine) + "\n"
	}

	// Add help text
	content += "\n" + styles.NewStyle().
		Foreground(t.TextMuted()).
		Render("↑/k up • ↓/j down • enter select • n new • d delete • r refresh • q quit")

	return content
}

// Message types
type ConnectedMsg struct{}

type StateLoadedMsg struct {
	State *state.SharedApplicationState
}

type ErrorMsg struct {
	Error error
}

type SessionEventMsg struct {
	Event state.StateEvent
}

type SessionSelectedMsg struct {
	SessionID string
}

type SessionCreatedMsg struct {
	Session types.SessionInfo
}

type SessionDeletedMsg struct {
	SessionID string
}

type SessionsRefreshedMsg struct {
	Sessions []types.SessionInfo
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
	logPath := filepath.Join(logDir, "sessions.log")
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
	panel := NewSessionsPanel(httpClient, socketPath)

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
		log.Printf("Received signal, shutting down sessions panel")
		cancel()
		program.Quit()
	}()

	// Run the program
	if _, err := program.Run(); err != nil {
		log.Printf("Sessions panel error: %v", err)
		os.Exit(1)
	}

	// Cleanup
	panel.ipcClient.Disconnect()
	cancel()
}
