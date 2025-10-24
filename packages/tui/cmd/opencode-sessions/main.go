package main

import (
	"context"
	"encoding/json"
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
	"github.com/sst/opencode/internal/styles"
	"github.com/sst/opencode/internal/theme"
	"github.com/sst/opencode/internal/types"
)

// SessionsPanel manages the sessions list panel
type SessionsPanel struct {
	client           *opencode.Client
	ipcClient        *ipc.SocketClient
	sessions         []types.SessionInfo
	currentIndex     int
	currentSessionID string
	width            int
	height           int
	ctx              context.Context
	cancel           context.CancelFunc
	version          int64 // Store the state version directly in the model
	eventsChan       chan types.StateEvent
	program          *tea.Program // Reference to the program for triggering updates
	scrollOffset     int          // Track scroll position for viewport
}

// NewSessionsPanel creates a new sessions panel
func NewSessionsPanel(httpClient *opencode.Client, socketPath string) *SessionsPanel {
	ctx, cancel := context.WithCancel(context.Background())

	panel := &SessionsPanel{
		client:       httpClient,
		ipcClient:    ipc.NewSocketClient(socketPath, "sessions-panel", "sessions"),
		sessions:     make([]types.SessionInfo, 0),
		currentIndex: 0,
		ctx:          ctx,
		cancel:       cancel,
		eventsChan:   make(chan types.StateEvent, 64),
	}

	// Register event handlers
	// Bridge IPC session events into Bubble Tea loop to force immediate UI refresh
	panel.ipcClient.RegisterEventHandler(types.EventSessionAdded, panel.forwardSessionEventToUI)
	panel.ipcClient.RegisterEventHandler(types.EventSessionDeleted, panel.forwardSessionEventToUI)
	panel.ipcClient.RegisterEventHandler(types.EventSessionUpdated, panel.forwardSessionEventToUI)
	panel.ipcClient.RegisterEventHandler(types.EventSessionChanged, panel.forwardSessionEventToUI)
	panel.ipcClient.RegisterEventHandler(types.EventStateSync, panel.forwardSessionEventToUI)
	panel.ipcClient.RegisterEventHandler(types.EventThemeChanged, panel.forwardSessionEventToUI)
	panel.ipcClient.RegisterEventHandler(types.EventUIActionTriggered, panel.forwardSessionEventToUI)

	return panel
}

// Init initializes the panel
func (p *SessionsPanel) Init() tea.Cmd {
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
			log.Printf("[SESSIONS] ipcClient in Init: %+v", p.ipcClient)
			return StateLoadedMsg{State: currentState}
		} else {
			log.Printf("Sessions panel initial state err:%v", err)
		}
		return ErrorMsg{Error: fmt.Errorf("failed to load state")}
	})

	// Subscribe to IPC events bridged via eventsChan
	cmds = append(cmds, p.subscribeSessionEvents())
	return tea.Batch(cmds...)

}

// expectedVersion returns a safe ExpectedVersion for updates.
// Prefer the client's server-known version; if unavailable (0), fall back to local panel version;
// and as a last resort use 1 (initial state version).
func (p *SessionsPanel) expectedVersion() int64 {
	v := p.ipcClient.GetCurrentVersion()
	if v <= 0 {
		if p.version > 0 {
			return p.version
		}
		return 1
	}
	return v
}

// Update handles messages and updates the panel state
func (p *SessionsPanel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
		p.version = msg.State.Version.Version // Explicitly store the version in the model state
		log.Printf("[SESSIONS] Stored version %d in model state", p.version)
		p.updateCurrentIndex()
		return p, nil

	case ErrorMsg:
		log.Printf("Sessions panel error: %v", msg.Error)
		return p, nil

	case SessionSyncMsg:
		log.Printf("[SESSIONS] Received session sync with %d sessions", len(msg.Sessions))
		return p, nil

	case SessionUpdatedMsg:
		log.Printf("[SESSIONS] Session updated: %s", msg.Session.ID)
		return p, nil

	case SessionDeletedMsg:
		log.Printf("Session deleted: %s", msg.SessionID)
		// UI will be refreshed automatically since the session was already removed
		// from p.sessions in handleSessionDeleted via the SessionEventMsg flow
		return p, nil

	case SessionCreatedMsg:
		log.Printf("Session created: %s", msg.Session.ID)
		// UI will be refreshed automatically since the session was already added
		// to p.sessions in handleSessionAdded via the SessionEventMsg flow
		return p, nil

	case SessionSelectedMsg:
		log.Printf("Session selected: %s", msg.SessionID)
		// The session change has already been processed, just trigger UI refresh
		return p, nil

	case SessionEventMsg:
		_, cmd := p.handleSessionEvent(msg.Event)
		return p, tea.Batch(cmd, p.subscribeSessionEvents())

	case ThemeChangedMsg:
		log.Printf("[SESSIONS] Applying theme change: %s", msg.Theme)
		// Apply the theme change immediately
		if err := theme.SetTheme(msg.Theme); err != nil {
			log.Printf("[SESSIONS] Failed to apply theme %s: %v", msg.Theme, err)
		} else {
			log.Printf("[SESSIONS] Successfully applied theme: %s", msg.Theme)
		}
		return p, nil

	default:
		return p, nil
	}
}

// View renders the sessions panel
func (p *SessionsPanel) View() string {
	if len(p.sessions) == 0 {
		return p.renderEmptyState()
	}

	return p.renderSessionsList()
}

// handleKeyPress processes keyboard input
func (p *SessionsPanel) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		return p, tea.Quit

	case "up", "k":
		if p.currentIndex > 0 {
			p.currentIndex--
			p.updateScrollOffset()
			return p, p.selectCurrentSession()
		}

	case "down", "j":
		if p.currentIndex < len(p.sessions)-1 {
			p.currentIndex++
			p.updateScrollOffset()
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
func (p *SessionsPanel) selectCurrentSession() tea.Cmd {
	if p.currentIndex >= 0 && p.currentIndex < len(p.sessions) {
		session := p.sessions[p.currentIndex]
		log.Printf("[SESSIONS] Selecting session: %s (index: %d)", session.ID, p.currentIndex)

		// Immediately update the local currentSessionID for instant UI feedback
		p.currentSessionID = session.ID

		return func() tea.Msg {
			versionToSend := p.expectedVersion()
			log.Printf("[SESSIONS] Sending update with ExpectedVersion: %d", versionToSend)

			update := types.StateUpdate{
				Type:            types.SessionChanged,
				ExpectedVersion: versionToSend,
				Payload:         types.SessionChangePayload{SessionID: session.ID},
				SourcePanel:     "sessions-panel",
				Timestamp:       time.Now(),
			}

			newVersion, err := p.ipcClient.SendStateUpdateAndWait(update)
			if err != nil {
				log.Printf("[SESSIONS] Failed to send SessionChanged event: %v", err)
				return ErrorMsg{Error: err}
			}
			p.version = newVersion // IMPORTANT: Update model version from response

			log.Printf("[SESSIONS] Successfully sent. New version is %d", p.version)
			return SessionSelectedMsg{SessionID: session.ID}
		}
	}
	log.Printf("[SESSIONS] Cannot select session - invalid index: %d (total: %d)", p.currentIndex, len(p.sessions))
	return nil
}

// createNewSession creates a new session
func (p *SessionsPanel) createNewSession() tea.Cmd {
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
		update := types.StateUpdate{
			Type:            types.SessionAdded,
			ExpectedVersion: p.expectedVersion(),
			Payload:         types.SessionAddPayload{Session: sessionInfo},
			SourcePanel:     "sessions-panel",
			Timestamp:       time.Now(),
		}

		newVersion, err := p.ipcClient.SendStateUpdateAndWait(update)
		if err != nil {
			return ErrorMsg{Error: err}
		}
		p.version = newVersion

		return SessionCreatedMsg{Session: sessionInfo}
	}
}

// deleteCurrentSession deletes the currently selected session
func (p *SessionsPanel) deleteCurrentSession() tea.Cmd {
	if p.currentIndex >= 0 && p.currentIndex < len(p.sessions) {
		sessionID := p.sessions[p.currentIndex].ID
		return func() tea.Msg {
			// Send update first to remove from shared state
			update := types.StateUpdate{
				Type:            types.SessionDeleted,
				ExpectedVersion: p.expectedVersion(),
				Payload:         types.SessionDeletePayload{SessionID: sessionID},
				SourcePanel:     "sessions-panel",
				Timestamp:       time.Now(),
			}

			newVersion, err := p.ipcClient.SendStateUpdateAndWait(update)
			if err != nil {
				// If state update fails, don't proceed with API deletion
				return ErrorMsg{Error: fmt.Errorf("failed to update state for session deletion: %w", err)}
			}
			p.version = newVersion

			// Delete via API after state update
			if _, err := p.client.Session.Delete(p.ctx, sessionID, opencode.SessionDeleteParams{}); err != nil {
				log.Printf("Warning: API deletion failed for session %s, but state was updated: %v", sessionID, err)
				// Don't return error here since state was already updated
			}

			return SessionDeletedMsg{SessionID: sessionID}
		}
	}
	return nil
}

// refreshSessions refreshes the sessions list from the API
func (p *SessionsPanel) refreshSessions() tea.Cmd {
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

		return SessionSyncMsg{
			Sessions:         sessionInfos,
			CurrentSessionID: "",
			Version:          1,
		}
	}
}

// Event handlers

func (p *SessionsPanel) handleSessionAdded(event types.StateEvent) error {
	if payload, ok := event.Data.(map[string]interface{}); ok {
		var sessionAddPayload types.SessionAddPayload
		if err := decodePayload(payload, &sessionAddPayload); err == nil {
			p.sessions = append(p.sessions, sessionAddPayload.Session)
			p.version = event.Version
			log.Printf("Session added: %s, version updated to %d", sessionAddPayload.Session.ID, p.version)

			// Trigger immediate UI update
			if p.program != nil {
				go func() {
					p.program.Send(SessionCreatedMsg{Session: sessionAddPayload.Session})
				}()
			}
		}
	}
	return nil
}

func (p *SessionsPanel) handleSessionDeleted(event types.StateEvent) error {
	if payload, ok := event.Data.(map[string]interface{}); ok {
		var sessionDeletePayload types.SessionDeletePayload
		if err := decodePayload(payload, &sessionDeletePayload); err == nil {
			for i, session := range p.sessions {
				if session.ID == sessionDeletePayload.SessionID {
					p.sessions = append(p.sessions[:i], p.sessions[i+1:]...)
					if p.currentIndex >= len(p.sessions) && len(p.sessions) > 0 {
						p.currentIndex = len(p.sessions) - 1
					}
					break
				}
			}
			p.version = event.Version
			log.Printf("Session deleted: %s, version updated to %d", sessionDeletePayload.SessionID, p.version)

			// Trigger immediate UI update
			if p.program != nil {
				go func() {
					p.program.Send(SessionDeletedMsg{SessionID: sessionDeletePayload.SessionID})
				}()
			}
		}
	}
	return nil
}

func (p *SessionsPanel) handleSessionUpdated(event types.StateEvent) error {
	if payload, ok := event.Data.(map[string]interface{}); ok {
		var sessionUpdatePayload types.SessionUpdatePayload
		if err := decodePayload(payload, &sessionUpdatePayload); err == nil {
			var updatedSession types.SessionInfo
			for i, session := range p.sessions {
				if session.ID == sessionUpdatePayload.SessionID {
					if sessionUpdatePayload.Title != "" {
						p.sessions[i].Title = sessionUpdatePayload.Title
					}
					p.sessions[i].IsActive = sessionUpdatePayload.IsActive
					p.sessions[i].UpdatedAt = time.Now()
					updatedSession = p.sessions[i]
					break
				}
			}
			p.version = event.Version
			log.Printf("Session updated: %s, version updated to %d", sessionUpdatePayload.SessionID, p.version)

			// Trigger immediate UI update
			if p.program != nil {
				go func() {
					p.program.Send(SessionUpdatedMsg{Session: updatedSession})
				}()
			}
		}
	}
	return nil
}

func (p *SessionsPanel) handleSessionChanged(event types.StateEvent) error {
	if payload, ok := event.Data.(map[string]interface{}); ok {
		var sessionChangePayload types.SessionChangePayload
		if err := decodePayload(payload, &sessionChangePayload); err == nil {
			oldSessionID := p.currentSessionID
			p.currentSessionID = sessionChangePayload.SessionID
			p.version = event.Version
			p.updateCurrentIndex()
			log.Printf("Session changed from %s to %s, version updated to %d", oldSessionID, sessionChangePayload.SessionID, p.version)

			// Trigger immediate UI update
			if p.program != nil {
				go func() {
					p.program.Send(SessionSelectedMsg{SessionID: sessionChangePayload.SessionID})
				}()
			}
		}
	}
	return nil
}

func (p *SessionsPanel) handleStateSync(event types.StateEvent) error {
	if payload, ok := event.Data.(types.StateSyncPayload); ok {
		// Smart cache invalidation - only update if version is newer
		if payload.State.Version.Version > p.version {
			oldVersion := p.version
			p.sessions = payload.State.Sessions
			p.currentSessionID = payload.State.CurrentSessionID
			p.version = payload.State.Version.Version
			p.updateCurrentIndex()
			log.Printf("State synchronized from version %d to %d", oldVersion, p.version)

			// Trigger immediate UI update for state sync
			if p.program != nil {
				go func() {
					p.program.Send(SessionSyncMsg{
						Sessions:         payload.State.Sessions,
						CurrentSessionID: payload.State.CurrentSessionID,
						Version:          payload.State.Version.Version,
					})
				}()
			}
		} else {
			log.Printf("Ignoring state sync with older/same version %d (current: %d)", payload.State.Version.Version, p.version)
		}
	}
	return nil
}

func (p *SessionsPanel) handleThemeChanged(event types.StateEvent) error {
	var payload types.ThemeChangePayload
	if err := decodePayload(event.Data, &payload); err != nil {
		log.Printf("[SESSIONS] Failed to decode theme change payload: %v", err)
		return err
	}

	log.Printf("[SESSIONS] Theme changed to: %s", payload.Theme)

	// Apply the theme change immediately
	if err := theme.SetTheme(payload.Theme); err != nil {
		log.Printf("[SESSIONS] Failed to set theme %s: %v", payload.Theme, err)
		return err
	}

	log.Printf("[SESSIONS] Successfully applied theme: %s", payload.Theme)

	// Trigger immediate UI update for theme change
	if p.program != nil {
		go func() {
			p.program.Send(ThemeChangedMsg{
				Theme: payload.Theme,
			})
		}()
	}
	return nil
}

func (p *SessionsPanel) handleUIActionTriggered(event types.StateEvent) error {
	log.Printf("[SESSIONS] Received UI action triggered event: %+v", event)

	// Extract action from the event payload
	if payloadMap, ok := event.Data.(map[string]interface{}); ok {
		if actionRaw, exists := payloadMap["action"]; exists {
			if action, ok := actionRaw.(string); ok {
				log.Printf("[SESSIONS] UI action: %s", action)
				// Sessions panel doesn't need to handle modal dialogs directly
				// These are typically handled by other panels or the main TUI
				// Just log for now
				return nil
			}
		}
	}

	log.Printf("[SESSIONS] Failed to extract action from UI action event payload")
	return nil
}

func (p *SessionsPanel) handleSessionEvent(event types.StateEvent) (tea.Model, tea.Cmd) {
	// Handle event processing here
	switch event.Type {
	case types.EventSessionAdded:
		p.handleSessionAdded(event)
	case types.EventSessionDeleted:
		p.handleSessionDeleted(event)
	case types.EventSessionUpdated:
		p.handleSessionUpdated(event)
	case types.EventSessionChanged:
		p.handleSessionChanged(event)
	case types.EventStateSync:
		p.handleStateSync(event)
	case types.EventThemeChanged:
		p.handleThemeChanged(event)
	case types.EventUIActionTriggered:
		p.handleUIActionTriggered(event)
	}
	return p, nil
}

// forwardSessionEventToUI bridges IPC session events into Bubble Tea by pushing into eventsChan
func (p *SessionsPanel) forwardSessionEventToUI(event types.StateEvent) error {
	select {
	case p.eventsChan <- event:
	default:
		log.Printf("sessions: events channel full, dropping event %s", event.Type)
	}
	return nil
}

// subscribeSessionEvents returns a command that waits for the next IPC event and emits it as a SessionEventMsg
func (p *SessionsPanel) subscribeSessionEvents() tea.Cmd {
	return func() tea.Msg {
		evt := <-p.eventsChan
		return SessionEventMsg{Event: evt}
	}
}

// updateCurrentIndex updates the current index based on current session ID
func (p *SessionsPanel) updateCurrentIndex() {
	for i, session := range p.sessions {
		if session.ID == p.currentSessionID {
			p.currentIndex = i
			p.updateScrollOffset()
			return
		}
	}
	// If current session not found, select first session
	if len(p.sessions) > 0 {
		p.currentIndex = 0
		p.scrollOffset = 0
	}
}

// updateScrollOffset adjusts scroll offset to keep current selection visible
func (p *SessionsPanel) updateScrollOffset() {
	visibleLines := p.height - 4
	if visibleLines < 1 {
		visibleLines = 1
	}

	// If current index is above the viewport, scroll up
	if p.currentIndex < p.scrollOffset {
		p.scrollOffset = p.currentIndex
	}

	// If current index is below the viewport, scroll down
	if p.currentIndex >= p.scrollOffset+visibleLines {
		p.scrollOffset = p.currentIndex - visibleLines + 1
	}

	// Ensure scroll offset is within bounds
	if p.scrollOffset < 0 {
		p.scrollOffset = 0
	}
	maxScroll := len(p.sessions) - visibleLines
	if maxScroll < 0 {
		maxScroll = 0
	}
	if p.scrollOffset > maxScroll {
		p.scrollOffset = maxScroll
	}
}

// calculateViewport returns the start and end indices for visible sessions
func (p *SessionsPanel) calculateViewport(visibleLines int) (startIdx, endIdx int) {
	if len(p.sessions) == 0 {
		return 0, 0
	}

	startIdx = p.scrollOffset
	endIdx = startIdx + visibleLines

	if endIdx > len(p.sessions) {
		endIdx = len(p.sessions)
	}

	return startIdx, endIdx
}

// renderEmptyState renders the empty sessions state
func (p *SessionsPanel) renderEmptyState() string {
	t := theme.CurrentTheme()
	style := styles.NewStyle().
		Foreground(t.TextMuted()).
		Align(styles.Center).
		Width(p.width).
		Height(p.height)

	return style.Render("No sessions\n\nPress 'n' to create a new session")
}

// renderSessionsList renders the list of sessions
func (p *SessionsPanel) renderSessionsList() string {
	t := theme.CurrentTheme()

	var content string
	content += styles.NewStyle().
		Foreground(t.Primary()).
		Bold(true).
		Render("Sessions") + "\n\n"

	// Calculate visible area: total height - header (2 lines) - help text (2 lines) - margins
	visibleLines := p.height - 4
	if visibleLines < 1 {
		visibleLines = 1
	}

	// Calculate viewport
	startIdx, endIdx := p.calculateViewport(visibleLines)

	// Render only visible sessions
	for i := startIdx; i < endIdx; i++ {
		session := p.sessions[i]
		isSelected := i == p.currentIndex
		isCurrent := session.ID == p.currentSessionID

		var style styles.Style
		var prefix string

		if isSelected {
			style = styles.NewStyle().
				Background(t.Primary()).
				Foreground(t.Background()).
				Bold(true).
				Padding(0, 1)
			prefix = ""
		} else {
			style = styles.NewStyle().
				Foreground(t.TextMuted()).
				Padding(0, 1)
			prefix = ""
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

		sessionLine := fmt.Sprintf("%s%s%s (%d msgs)", prefix, indicator, title, session.MessageCount)
		content += style.Render(sessionLine) + "\n"
	}

	// Add scroll indicator if there are more items
	scrollInfo := ""
	if len(p.sessions) > visibleLines {
		scrollInfo = fmt.Sprintf(" [%d-%d/%d]", startIdx+1, endIdx, len(p.sessions))
	}

	// Add help text
	content += "\n" + styles.NewStyle().
		Foreground(t.TextMuted()).
		Render("↑/k up • ↓/j down • enter select • n new • d delete • r refresh • q quit"+scrollInfo)

	return content
}

// Message types
type ConnectedMsg struct{}

type StateLoadedMsg struct {
	State *types.SharedApplicationState
}

type ErrorMsg struct {
	Error error
}

type SessionEventMsg struct {
	Event types.StateEvent
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

type SessionSyncMsg struct {
	Sessions         []types.SessionInfo
	CurrentSessionID string
	Version          int64
}

type SessionUpdatedMsg struct {
	Session types.SessionInfo
}

type ThemeChangedMsg struct {
	Theme string
}

// decodePayload is a helper to convert a map payload from JSON decoding back into a specific struct type.
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

	// Set the program reference in the panel for UI updates
	panel.program = program

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
