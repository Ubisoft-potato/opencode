package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
	tmuxconfig "github.com/sst/opencode/internal/config"
	"github.com/sst/opencode/internal/ipc"
	"github.com/sst/opencode/internal/persistence"
	"github.com/sst/opencode/internal/state"
	"github.com/sst/opencode/internal/theme"
	"github.com/sst/opencode/internal/types"
)

// TmuxOrchestrator manages the tmux session and panels
type TmuxOrchestrator struct {
	sessionName   string
	socketPath    string
	statePath     string
	httpClient    *opencode.Client
	ipcServer     *ipc.SocketServer
	syncManager   *state.PanelSyncManager
	ctx           context.Context
	cancel        context.CancelFunc
	tmuxCommand   string
	isRunning     bool
	serverOnly    bool
	sseClient     *http.Client
	serverURL     string
	config        *tmuxconfig.File
	panes         map[string]string
	reuseExisting bool
}

// NewTmuxOrchestrator creates a new tmux orchestrator
func NewTmuxOrchestrator(sessionName, socketPath, statePath, serverURL string, httpClient *opencode.Client, serverOnly bool, cfg *tmuxconfig.File) *TmuxOrchestrator {
	ctx, cancel := context.WithCancel(context.Background())

	return &TmuxOrchestrator{
		sessionName: sessionName,
		socketPath:  socketPath,
		statePath:   statePath,
		httpClient:  httpClient,
		ctx:         ctx,
		cancel:      cancel,
		tmuxCommand: "tmux",
		serverOnly:  serverOnly,
		sseClient:   &http.Client{Timeout: 0}, // No timeout for SSE connections
		serverURL:   serverURL,
		config:      cfg,
		panes:       map[string]string{},
	}
}

// Initialize sets up the orchestrator and its components
func (orch *TmuxOrchestrator) Initialize() error {
	log.Printf("Initializing tmux orchestrator...")

	// Create directories
	if err := orch.createDirectories(); err != nil {
		return fmt.Errorf("failed to create directories: %w", err)
	}

	// Initialize state management
	if err := orch.initializeStateManagement(); err != nil {
		return fmt.Errorf("failed to initialize state management: %w", err)
	}

	// Load existing sessions from OpenCode server
	if err := orch.loadSessionsFromServer(); err != nil {
		log.Printf("Warning: Failed to load sessions from server: %v", err)
		// Don't fail initialization if session loading fails - it's not critical
	}

	// Start IPC server
	if err := orch.startIPCServer(); err != nil {
		return fmt.Errorf("failed to start IPC server: %w", err)
	}

	// Start API request handler for TUI control
	go orch.startAPIRequestHandler()

	// Start SSE client for real-time updates
	go orch.startSSEClient()

	log.Printf("Tmux orchestrator initialized successfully")
	return nil
}

// Start creates and configures the tmux session with panels
func (orch *TmuxOrchestrator) Start() error {
	log.Printf("Starting tmux session: %s", orch.sessionName)
	if orch.config != nil {
		name := strings.TrimSpace(orch.config.Session.Name)
		if name != "" {
			orch.sessionName = name
			log.Printf("Session name overridden by config: %s", orch.sessionName)
		}
	}

	orch.panes = map[string]string{}

	if err := orch.handleExistingSession(); err != nil {
		return err
	}
	if orch.reuseExisting {
		orch.isRunning = true
		log.Printf("Reusing existing tmux session without reconfiguration")
		return nil
	}

	// Check if tmux is available
	if !orch.isTmuxAvailable() {
		return fmt.Errorf("tmux is not available")
	}

	// Create tmux session
	if err := orch.createTmuxSession(); err != nil {
		return fmt.Errorf("failed to create tmux session: %w", err)
	}

	if orch.serverOnly {
		log.Printf("Server-only mode: skipping panel configuration and applications")
	} else {
		// Configure panels
		if err := orch.configurePanels(); err != nil {
			return fmt.Errorf("failed to configure panels: %w", err)
		}

		// Start panel applications
		if err := orch.startPanelApplications(); err != nil {
			return fmt.Errorf("failed to start panel applications: %w", err)
		}
	}

	orch.isRunning = true
	log.Printf("Tmux session started successfully")
	return nil
}

// Stop gracefully shuts down the tmux session and all components
func (orch *TmuxOrchestrator) Stop() error {
	log.Printf("Stopping tmux orchestrator...")

	orch.isRunning = false

	// Cancel context to signal shutdown
	orch.cancel()

	// Stop sync manager
	if orch.syncManager != nil {
		orch.syncManager.Stop()
	}

	// Stop IPC server
	if orch.ipcServer != nil {
		orch.ipcServer.Stop()
	}

	// Kill tmux session
	if err := orch.killTmuxSession(); err != nil {
		log.Printf("Failed to kill tmux session: %v", err)
	}

	// Cleanup socket file
	if err := os.Remove(orch.socketPath); err != nil && !os.IsNotExist(err) {
		log.Printf("Failed to remove socket file: %v", err)
	}

	log.Printf("Tmux orchestrator stopped")
	return nil
}

// createDirectories creates necessary directories
func (orch *TmuxOrchestrator) createDirectories() error {
	dirs := []string{
		filepath.Dir(orch.socketPath),
		filepath.Dir(orch.statePath),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	return nil
}

// initializeStateManagement sets up state management components
func (orch *TmuxOrchestrator) initializeStateManagement() error {
	// Create shared state
	sharedState := types.NewSharedApplicationState()

	// Create file manager
	fileManagerConfig := persistence.DefaultFileManagerConfig(orch.statePath)
	fileManager := persistence.NewFileManager(fileManagerConfig)

	// Create event bus
	eventBus := state.NewEventBus(1000)

	// Create conflict resolver
	conflictResolver := state.DefaultConflictResolver()

	// Create sync manager
	syncManagerConfig := state.DefaultSyncManagerConfig()
	orch.syncManager = state.NewPanelSyncManager(sharedState, fileManager, eventBus, conflictResolver, syncManagerConfig)

	// Create event channel for local state changes
	eventChan := make(chan types.StateEvent, 100)
	eventBus.Subscribe("tmux-orchestrator", "orchestrator", eventChan)

	// Start goroutine to handle events
	go orch.handleEvents(eventChan)

	// Initialize sync manager
	if err := orch.syncManager.Initialize(); err != nil {
		return err
	}

	// Verify initialization
	if orch.syncManager == nil {
		return fmt.Errorf("sync manager is nil after initialization")
	}

	testState := orch.syncManager.GetState()
	if testState == nil {
		return fmt.Errorf("state manager returns nil state after initialization")
	}

	log.Printf("State management initialized successfully, initial version: %d", testState.Version.Version)
	log.Printf("State details - SessionID: %s, Theme: %s, UpdateCount: %d",
		testState.CurrentSessionID, testState.Theme, testState.UpdateCount)

	return nil
}

// handleLocalSessionChanged handles local session change events from panels
func (orch *TmuxOrchestrator) handleEvents(eventChan chan types.StateEvent) {
	for event := range eventChan {
		switch event.Type {
		case types.EventSessionChanged:
			if err := orch.handleLocalSessionChanged(event); err != nil {
				log.Printf("Error handling local session change: %v", err)
			}
		case types.EventThemeChanged:
			if err := orch.handleThemeChanged(event); err != nil {
				log.Printf("Error handling theme change: %v", err)
			}
		default:
			// Handle other event types if needed
			log.Printf("Received event: %s from panel %s", event.Type, event.SourcePanel)
		}
	}
}

func (orch *TmuxOrchestrator) handleLocalSessionChanged(event types.StateEvent) error {
	log.Printf("[TMUX] Handling local session change event: %+v", event)

	// Extract session ID from the event payload
	if payloadMap, ok := event.Data.(map[string]interface{}); ok {
		var payload types.SessionChangePayload
		if sessionIDRaw, exists := payloadMap["session_id"]; exists {
			if sessionID, ok := sessionIDRaw.(string); ok {
				payload.SessionID = sessionID
				log.Printf("[TMUX] Session changed to: %s", payload.SessionID)

				// The state has already been updated by the sync manager
				// We just need to log this for debugging purposes
				return nil
			}
		}
	}

	log.Printf("[TMUX] Failed to extract session ID from event payload")
	return nil
}

func (orch *TmuxOrchestrator) handleThemeChanged(event types.StateEvent) error {
	log.Printf("[TMUX] Handling theme change event: %+v", event)

	// Extract theme name from the event payload
	if payloadMap, ok := event.Data.(map[string]interface{}); ok {
		var payload types.ThemeChangePayload
		if themeRaw, exists := payloadMap["theme"]; exists {
			if theme, ok := themeRaw.(string); ok {
				payload.Theme = theme
				log.Printf("[TMUX] Theme changed to: %s", payload.Theme)

				// Apply the theme globally to all panels
				return orch.applyGlobalTheme(payload.Theme)
			}
		}
	}

	log.Printf("[TMUX] Failed to extract theme from event payload")
	return nil
}

// applyGlobalTheme applies the theme to all connected panels
func (orch *TmuxOrchestrator) applyGlobalTheme(themeName string) error {
	log.Printf("[TMUX] Applying global theme: %s", themeName)

	// Actually set the theme in the theme manager
	if err := theme.SetTheme(themeName); err != nil {
		log.Printf("[TMUX] Error setting theme: %v", err)
		return err
	}

	// The theme has been updated in the theme manager
	// All connected panels will receive the theme change through the state sync mechanism

	if orch.ipcServer != nil {
		connections := orch.ipcServer.GetConnections()
		log.Printf("[TMUX] Theme applied to %d connected panels", len(connections))
		for _, conn := range connections {
			log.Printf("[TMUX] - Panel %s (%s) received theme update", conn.PanelID, conn.PanelType)
		}
	}

	return nil
}

// startIPCServer starts the IPC server for panel communication
func (orch *TmuxOrchestrator) startIPCServer() error {
	// Create IPC server
	orch.ipcServer = ipc.NewSocketServer(
		orch.socketPath,
		orch.syncManager.GetEventBus(),
		orch.syncManager,
	)

	// Start server
	if err := orch.ipcServer.Start(); err != nil {
		return err
	}

	return nil
}

// isTmuxAvailable checks if tmux is available on the system
func (orch *TmuxOrchestrator) isTmuxAvailable() bool {
	_, err := exec.LookPath(orch.tmuxCommand)
	return err == nil
}

// createTmuxSession creates a new tmux session
func (orch *TmuxOrchestrator) createTmuxSession() error {
	// Kill existing session if it exists
	orch.killTmuxSession()

	// Create new session
	cmd := exec.CommandContext(orch.ctx, orch.tmuxCommand, "new-session", "-d", "-s", orch.sessionName)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to create tmux session: %w", err)
	}

	return nil
}

// configurePanels configures the tmux panel layout
func (orch *TmuxOrchestrator) configurePanels() error {
	if orch.config != nil {
		return orch.configurePanelsFromConfig()
	}
	return orch.configureDefaultPanels()
}

func (orch *TmuxOrchestrator) configureDefaultPanels() error {
	sessionTarget := orch.sessionName + ":0"

	// Split window horizontally (sessions + messages)
	cmd := exec.CommandContext(orch.ctx, orch.tmuxCommand, "split-window", "-h", "-t", sessionTarget)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to split window horizontally: %w", err)
	}

	// Split the bottom pane for input (full width)
	cmd = exec.CommandContext(orch.ctx, orch.tmuxCommand, "split-window", "-v", "-t", sessionTarget+".1")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to split window vertically: %w", err)
	}

	// Adjust pane sizes
	// Sessions panel: 20% width
	cmd = exec.CommandContext(orch.ctx, orch.tmuxCommand, "resize-pane", "-t", sessionTarget+".0", "-x", "20%")
	if err := cmd.Run(); err != nil {
		log.Printf("Warning: failed to resize sessions pane: %v", err)
	}

	// Input panel: 20% height
	cmd = exec.CommandContext(orch.ctx, orch.tmuxCommand, "resize-pane", "-t", sessionTarget+".2", "-y", "20%")
	if err := cmd.Run(); err != nil {
		log.Printf("Warning: failed to resize input pane: %v", err)
	}

	orch.panes = map[string]string{
		"sessions": sessionTarget + ".0",
		"messages": sessionTarget + ".1",
		"input":    sessionTarget + ".2",
	}

	return nil
}

func (orch *TmuxOrchestrator) handleExistingSession() error {
	cmd := exec.Command(orch.tmuxCommand, "has-session", "-t", orch.sessionName)
	if err := cmd.Run(); err != nil {
		return nil
	}

	if !isTerminal() {
		log.Printf("Existing tmux session detected but stdin is not a terminal, creating new session")
		return orch.killTmuxSession()
	}

	fmt.Printf("检测到已有 tmux 会话: %s\n", orch.sessionName)
	fmt.Printf("选择操作: [r] 复用现有会话 (默认) / [n] 新建并覆盖 / [q] 退出: ")

	reader := bufio.NewReader(os.Stdin)

	for {
		line, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("读取用户输入失败: %w", err)
		}
		choice := strings.ToLower(strings.TrimSpace(line))
		if choice == "" {
			choice = "r"
		}

		switch choice {
		case "r", "reuse", "y":
			orch.reuseExisting = true
			return nil
		case "n", "new":
			log.Printf("User requested new tmux session, killing existing session: %s", orch.sessionName)
			return orch.killTmuxSession()
		case "q", "quit":
			return fmt.Errorf("用户取消启动")
		default:
			fmt.Printf("输入无效，请输入 r / n / q: ")
		}
	}
}

func (orch *TmuxOrchestrator) configurePanelsFromConfig() error {
	sessionTarget := orch.sessionName + ":0"
	rootPane, err := orch.resolvePaneID(sessionTarget)
	if err != nil {
		return fmt.Errorf("failed to resolve root pane id: %w", err)
	}
	panes := map[string]string{
		"root": rootPane,
	}

	for _, split := range orch.config.Splits {
		target, ok := panes[split.Target]
		if !ok {
			return fmt.Errorf("unknown split target: %s", split.Target)
		}

		if len(split.Panels) != 2 {
			return fmt.Errorf("split %s must define exactly two panels", split.Target)
		}

		args := []string{"split-window", "-P", "-F", "#{pane_id}", "-t", target}
		typ := strings.ToLower(strings.TrimSpace(split.Type))
		if typ == "horizontal" {
			args = append(args, "-h")
		}
		if typ != "horizontal" {
			args = append(args, "-v")
		}

		cmd := exec.CommandContext(orch.ctx, orch.tmuxCommand, args...)
		out, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("failed to split pane %s (%s): %w", split.Target, split.Type, err)
		}

		newPane := strings.TrimSpace(string(out))
		first := split.Panels[0]
		second := split.Panels[1]

		panes[first] = target
		panes[second] = newPane

		if split.Ratio != "" {
			firstPct, _, ok := orch.config.RatioPercents(split.Ratio)
			if ok {
				if typ == "horizontal" {
					scale := fmt.Sprintf("%d%%", firstPct)
					sizeCmd := exec.CommandContext(orch.ctx, orch.tmuxCommand, "resize-pane", "-t", panes[first], "-x", scale)
					if err := sizeCmd.Run(); err != nil {
						log.Printf("Failed to apply horizontal ratio for %s: %v", first, err)
					}
				}
				if typ != "horizontal" {
					scale := fmt.Sprintf("%d%%", firstPct)
					sizeCmd := exec.CommandContext(orch.ctx, orch.tmuxCommand, "resize-pane", "-t", panes[first], "-y", scale)
					if err := sizeCmd.Run(); err != nil {
						log.Printf("Failed to apply vertical ratio for %s: %v", first, err)
					}
				}
			}
		}
	}

	for _, panel := range orch.config.Panels {
		target, ok := panes[panel.ID]
		if !ok {
			continue
		}

		width := strings.TrimSpace(panel.Width)
		if width != "" {
			cmd := exec.CommandContext(orch.ctx, orch.tmuxCommand, "resize-pane", "-t", target, "-x", width)
			if err := cmd.Run(); err != nil {
				log.Printf("Failed to apply width for %s: %v", panel.ID, err)
			}
		}

		height := strings.TrimSpace(panel.Height)
		if height != "" {
			cmd := exec.CommandContext(orch.ctx, orch.tmuxCommand, "resize-pane", "-t", target, "-y", height)
			if err := cmd.Run(); err != nil {
				log.Printf("Failed to apply height for %s: %v", panel.ID, err)
			}
		}
	}

	orch.panes = panes
	return nil
}

func (orch *TmuxOrchestrator) resolvePaneID(target string) (string, error) {
	cmd := exec.CommandContext(orch.ctx, orch.tmuxCommand, "display-message", "-p", "-t", target, "#{pane_id}")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// startPanelApplications starts the applications in each panel
func (orch *TmuxOrchestrator) startPanelApplications() error {
	if orch.config != nil {
		return orch.startConfigPanelApplications()
	}
	return orch.startDefaultPanelApplications()
}

func (orch *TmuxOrchestrator) startDefaultPanelApplications() error {
	sessionTarget := orch.sessionName + ":0"

	// Set environment variables for all panels
	envVars := map[string]string{
		"OPENCODE_SERVER": os.Getenv("OPENCODE_SERVER"),
		"OPENCODE_SOCKET": orch.socketPath,
	}

	log.Printf("Starting panel applications with IPC socket: %s", orch.socketPath)

	// Wait a moment for IPC server to be fully ready
	time.Sleep(1 * time.Second)

	// Define panel configurations
	panels := []struct {
		pane string
		name string
		desc string
	}{
		{sessionTarget + ".0", "opencode-sessions", "sessions panel"},
		{sessionTarget + ".1", "opencode-messages", "messages panel"},
		{sessionTarget + ".2", "opencode-input", "input panel"},
	}

	// Start each panel with error recovery
	for i, panel := range panels {
		log.Printf("Starting %s (%d/3)...", panel.desc, i+1)

		if err := orch.startPanelApp(panel.pane, panel.name, envVars); err != nil {
			log.Printf("Failed to start %s: %v", panel.desc, err)
			// Don't fail completely - continue with other panels
			continue
		}

		// Give each panel time to start before starting the next
		time.Sleep(500 * time.Millisecond)
		log.Printf("✅ %s started successfully", panel.desc)
	}

	// Verify at least one panel is running
	if orch.verifyPanelsRunning() {
		log.Printf("Panel startup completed - at least one panel is running")
		return nil
	} else {
		return fmt.Errorf("no panels could be started successfully")
	}
}

func (orch *TmuxOrchestrator) startConfigPanelApplications() error {
	envVars := map[string]string{
		"OPENCODE_SERVER": os.Getenv("OPENCODE_SERVER"),
		"OPENCODE_SOCKET": orch.socketPath,
	}

	log.Printf("Starting panel applications with IPC socket: %s", orch.socketPath)
	time.Sleep(1 * time.Second)

	success := 0
	for idx, panel := range orch.config.Panels {
		target, ok := orch.panes[panel.ID]
		if !ok {
			log.Printf("Pane target for %s not found; skipping", panel.ID)
			continue
		}

		appName, err := resolvePanelAppName(panel)
		if err != nil {
			log.Printf("Failed to resolve panel %s: %v", panel.ID, err)
			continue
		}

		log.Printf("Starting %s panel (%d/%d)...", panel.ID, idx+1, len(orch.config.Panels))
		if err := orch.startPanelApp(target, appName, envVars); err != nil {
			log.Printf("Failed to start %s panel: %v", panel.ID, err)
			continue
		}

		time.Sleep(500 * time.Millisecond)
		log.Printf("✅ %s panel started successfully", panel.ID)
		success++
	}

	if success == 0 {
		return fmt.Errorf("no panels could be started successfully")
	}

	if orch.verifyPanelsRunning() {
		log.Printf("Panel startup completed - %d panels requested", success)
		return nil
	}

	return fmt.Errorf("panels failed health check after startup")
}

func resolvePanelAppName(panel tmuxconfig.Panel) (string, error) {
	custom := strings.TrimSpace(panel.Command)
	if custom != "" {
		return custom, nil
	}

	key := strings.ToLower(strings.TrimSpace(panel.Type))
	switch key {
	case "sessions":
		return "opencode-sessions", nil
	case "messages":
		return "opencode-messages", nil
	case "input":
		return "opencode-input", nil
	}

	return "", fmt.Errorf("unsupported panel type: %s", panel.Type)
}

// startPanelApp starts an application in a specific tmux pane
func (orch *TmuxOrchestrator) startPanelApp(paneTarget, appName string, envVars map[string]string) error {
	// Build command with environment variables
	var envCmd string
	for key, value := range envVars {
		if value != "" {
			envCmd += fmt.Sprintf("export %s='%s'; ", key, value)
		}
	}

	run := strings.TrimSpace(appName)
	binaryPath, err := orch.getBinaryPath(appName)
	if err == nil {
		run = binaryPath
	}
	if err != nil {
		log.Printf("[DEBUG] Using raw command for %s: %v", appName, err)
	}
	if run == "" {
		return fmt.Errorf("no command for panel %s", appName)
	}

	command := fmt.Sprintf("%s%s; sleep 600", envCmd, run)

	log.Printf("[DEBUG] Sending command to pane %s: %s", paneTarget, command)

	// Send command to pane
	cmd := exec.CommandContext(orch.ctx, orch.tmuxCommand, "send-keys", "-t", paneTarget, command, "Enter")
	if err := cmd.Run(); err != nil {
		log.Printf("[DEBUG] Error sending command to pane %s: %v", paneTarget, err)
		time.Sleep(500 * time.Millisecond)
		return fmt.Errorf("failed to send command to pane %s: %w", paneTarget, err)
	}

	log.Printf("[DEBUG] Successfully sent command to pane %s", paneTarget)

	// Give the application time to start
	time.Sleep(500 * time.Millisecond)

	return nil
}

// getBinaryPath returns the correct binary path for a panel application
func (orch *TmuxOrchestrator) getBinaryPath(appName string) (string, error) {
	// Get the directory where the current executable is located
	execPath, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("failed to get executable path: %w", err)
	}

	// Get the cmd directory (parent of opencode-tmux)
	execDir := filepath.Dir(execPath)
	cmdDir := filepath.Dir(execDir)
	if filepath.Base(execDir) == "dist" {
		cmdDir = filepath.Dir(cmdDir)
	}

	// Map app names to their binary paths
	var binaryName string
	switch appName {
	case "opencode-sessions":
		binaryName = filepath.Join(cmdDir, "opencode-sessions", "dist", "sessions-pane")
	case "opencode-messages":
		binaryName = filepath.Join(cmdDir, "opencode-messages", "dist", "messages-pane")
	case "opencode-input":
		binaryName = filepath.Join(cmdDir, "opencode-input", "dist", "input-pane")
	default:
		return "", fmt.Errorf("unknown app name: %s", appName)
	}

	// Check if binary exists
	if _, err := os.Stat(binaryName); os.IsNotExist(err) {
		return "", fmt.Errorf("binary not found: %s", binaryName)
	}

	log.Printf("[DEBUG] Resolved binary path for %s: %s", appName, binaryName)
	return binaryName, nil
}

// verifyPanelsRunning checks if panel applications are running
func (orch *TmuxOrchestrator) verifyPanelsRunning() bool {
	targets := []string{}

	if orch.config != nil {
		seen := map[string]struct{}{}
		for _, panel := range orch.config.Panels {
			paneTarget, ok := orch.panes[panel.ID]
			if !ok {
				continue
			}
			if _, exists := seen[paneTarget]; exists {
				continue
			}
			targets = append(targets, paneTarget)
			seen[paneTarget] = struct{}{}
		}
	}

	if len(targets) == 0 {
		sessionTarget := orch.sessionName + ":0"
		targets = []string{
			sessionTarget + ".0",
			sessionTarget + ".1",
			sessionTarget + ".2",
		}
	}

	panelsRunning := 0
	for idx, paneTarget := range targets {
		cmd := exec.Command(orch.tmuxCommand, "list-panes", "-t", paneTarget, "-F", "#{pane_pid}")
		output, err := cmd.Output()
		if err == nil && len(output) > 0 {
			panelsRunning++
			log.Printf("[DEBUG] Pane %s is active (PID: %s)", paneTarget, strings.TrimSpace(string(output)))
			continue
		}
		log.Printf("[DEBUG] Pane %s (index %d) is not active or has no process", paneTarget, idx)
	}

	log.Printf("Panel verification: %d/%d panels are running", panelsRunning, len(targets))
	return panelsRunning > 0
}

// killTmuxSession kills the tmux session if it exists
func (orch *TmuxOrchestrator) killTmuxSession() error {
	cmd := exec.Command(orch.tmuxCommand, "kill-session", "-t", orch.sessionName)
	cmd.Run() // Ignore errors as session might not exist
	return nil
}

// attachToSession attaches to the tmux session
func (orch *TmuxOrchestrator) attachToSession() error {
	if !orch.isRunning {
		return fmt.Errorf("session is not running")
	}

	cmd := exec.Command(orch.tmuxCommand, "attach-session", "-t", orch.sessionName)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// waitForShutdown waits for shutdown signals
func (orch *TmuxOrchestrator) waitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	<-sigChan
	log.Printf("Received shutdown signal")
}

// monitorHealth monitors the health of the system
func (orch *TmuxOrchestrator) monitorHealth() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-orch.ctx.Done():
			return
		case <-ticker.C:
			orch.performHealthCheck()
		}
	}
}

// performHealthCheck checks the health of all components
func (orch *TmuxOrchestrator) performHealthCheck() {
	// Check sync manager health
	if orch.syncManager != nil && !orch.syncManager.IsHealthy() {
		log.Printf("Warning: Sync manager is not healthy")
	}

	// Check IPC server health
	if orch.ipcServer != nil && !orch.ipcServer.IsRunning() {
		log.Printf("Warning: IPC server is not running")
	}

	// Check tmux session
	if orch.isRunning && !orch.isTmuxSessionRunning() {
		log.Printf("Warning: Tmux session is not running")
	}
}

// isTmuxSessionRunning checks if the tmux session is still running
func (orch *TmuxOrchestrator) isTmuxSessionRunning() bool {
	cmd := exec.Command(orch.tmuxCommand, "has-session", "-t", orch.sessionName)
	return cmd.Run() == nil
}

// printStatus prints the current status of the orchestrator
func (orch *TmuxOrchestrator) printStatus() {
	fmt.Printf("OpenCode Tmux Orchestrator Status:\n")
	fmt.Printf("  Session Name: %s\n", orch.sessionName)
	fmt.Printf("  Socket Path: %s\n", orch.socketPath)
	fmt.Printf("  State Path: %s\n", orch.statePath)
	fmt.Printf("  Running: %v\n", orch.isRunning)

	if orch.ipcServer != nil {
		fmt.Printf("  IPC Server: %v\n", orch.ipcServer.IsRunning())
		connections := orch.ipcServer.GetConnections()
		fmt.Printf("  Connected Panels: %d\n", len(connections))
		for _, conn := range connections {
			fmt.Printf("    - %s (%s)\n", conn.PanelID, conn.PanelType)
		}
	}

	if orch.syncManager != nil {
		metrics := orch.syncManager.GetMetrics()
		fmt.Printf("  State Updates: %d (%.1f%% success)\n",
			metrics.TotalUpdates, metrics.GetSuccessRate())
		fmt.Printf("  State Saves: %d (%.1f%% success)\n",
			metrics.TotalSaves, metrics.GetSaveSuccessRate())
	}
}

func main() {
	// 设置日志输出到文件
	// Configure logging to file
	logFileHomeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("Failed to get home directory: %v", err)
	}
	logDir := filepath.Join(logFileHomeDir, ".opencode")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("Failed to create log directory: %v", err)
	}
	logPath := filepath.Join(logDir, "tmux.log")
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	} else {
		log.SetOutput(logFile)
		log.SetFlags(log.LstdFlags | log.Lshortfile)
		defer logFile.Close()
	}

	// Parse command line arguments
	var serverOnly bool
	flag.BoolVar(&serverOnly, "server-only", false, "Only start IPC server without panels")
	flag.Parse()

	sessionName := "opencode"
	sessionOverride := false
	if flag.NArg() > 0 {
		sessionName = flag.Arg(0)
		sessionOverride = true
	}

	// Get configuration from environment
	serverURL := os.Getenv("OPENCODE_SERVER")
	if serverURL == "" {
		log.Fatal("OPENCODE_SERVER environment variable not set")
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal("Failed to get home directory:", err)
	}

	configPath := os.Getenv("OPENCODE_TMUX_CONFIG")
	if configPath == "" {
		configPath = filepath.Join(homeDir, ".opencode", "tmux.yaml")
	}

	cfg, err := tmuxconfig.Load(configPath)
	if err != nil {
		log.Fatalf("Failed to load tmux config: %v", err)
	}
	if !sessionOverride {
		name := strings.TrimSpace(cfg.Session.Name)
		if name != "" {
			sessionName = name
		}
	}

	socketPath := os.Getenv("OPENCODE_SOCKET")
	if socketPath == "" {
		socketPath = filepath.Join(homeDir, ".opencode", "ipc.sock")
	}

	statePath := os.Getenv("OPENCODE_STATE")
	if statePath == "" {
		statePath = filepath.Join(homeDir, ".opencode", "state.json")
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

	// Create orchestrator
	orchestrator := NewTmuxOrchestrator(sessionName, socketPath, statePath, serverURL, httpClient, serverOnly, cfg)

	if serverOnly {
		log.Printf("Starting in server-only mode - IPC server only, no panels")
	}

	// Initialize
	if err := orchestrator.Initialize(); err != nil {
		log.Fatal("Failed to initialize orchestrator:", err)
	}

	// Start tmux session
	if err := orchestrator.Start(); err != nil {
		log.Fatal("Failed to start tmux session:", err)
	}

	// Start health monitoring
	go orchestrator.monitorHealth()

	// Print status
	orchestrator.printStatus()

	// Attach to session if stdin is a terminal and not in server-only mode
	if isTerminal() && !serverOnly {
		log.Printf("Attaching to tmux session...")
		if err := orchestrator.attachToSession(); err != nil {
			log.Printf("Failed to attach to session: %v", err)
		}
	} else {
		// Wait for shutdown signal (for server-only mode or non-terminal)
		if serverOnly {
			log.Printf("Server-only mode: waiting for shutdown signal...")
		}
		orchestrator.waitForShutdown()
	}

	// Cleanup
	if err := orchestrator.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
}

// startSSEClient starts the Server-Sent Events client for real-time updates
func (orch *TmuxOrchestrator) startSSEClient() error {
	// Replace raw SSE parsing with typed SDK streaming
	log.Printf("Starting typed event stream client via SDK")

	go func() {
		for {
			select {
			case <-orch.ctx.Done():
				log.Printf("Event stream stopping due to context cancellation")
				return
			default:
				// Open a new stream filtered by project directory
				stream := orch.httpClient.Event.ListStreaming(
					orch.ctx,
					opencode.EventListParams{},
				)

				for stream.Next() {
					evt := stream.Current()
					orch.handleTypedEvent(evt)
				}

				// Capture error, close, and backoff before retrying
				if err := stream.Err(); err != nil {
					log.Printf("Event stream error: %v", err)
				}
				_ = stream.Close()
				log.Printf("Event stream closed; retrying in 5 seconds...")
				time.Sleep(5 * time.Second)
			}
		}
	}()

	return nil
}

// handleTypedEvent processes typed SDK events
func (orch *TmuxOrchestrator) handleTypedEvent(evt opencode.EventListResponse) {
	switch evt.Type {
	case opencode.EventListResponseTypeMessageUpdated:
		// Cast to typed union variant
		uni := evt.AsUnion()
		if v, ok := uni.(opencode.EventListResponseEventMessageUpdated); ok {
			info := v.Properties.Info

			// Create or refresh local message metadata; content will be built by parts
			msg := types.MessageInfo{
				ID:        info.ID,
				SessionID: info.SessionID,
				Type:      string(info.Role),
				Content:   "",
				Timestamp: time.Now(),
				// User messages are immediately completed; assistant starts pending
				Status: func() string {
					if info.Role == opencode.MessageRoleUser {
						return "completed"
					}
					return "pending"
				}(),
			}
			// Avoid duplicate additions if multiple message.updated events arrive for same ID
			exists := false
			currentStatus := ""
			st := orch.syncManager.GetState()
			for _, m := range st.Messages {
				if m.ID == msg.ID {
					exists = true
					currentStatus = m.Status
					break
				}
			}
			if exists {
				// Only update status if the message is not already completed
				// This prevents message.updated events from overwriting the "completed" status
				// that was set by step-finish events
				if currentStatus != "completed" {
					desiredStatus := "pending"
					if info.Role == opencode.MessageRoleUser {
						desiredStatus = "completed"
					}
					if err := orch.syncManager.UpdateMessage(msg.ID, "", desiredStatus, "sse"); err != nil {
						log.Printf("[SSE] Failed to refresh message status for %s: %v", msg.ID, err)
					} else {
						log.Printf("[SSE] Message metadata exists; status refreshed: %s -> %s", msg.ID, desiredStatus)
					}
				} else {
					log.Printf("[SSE] Message metadata exists but already completed; skipping status update: %s", msg.ID)
				}
			} else {
				if err := orch.syncManager.AddMessage(msg, "sse"); err != nil {
					log.Printf("[SSE] Failed to add message: %v", err)
				} else {
					log.Printf("[SSE] Message metadata added: %s", msg.ID)
				}
			}
		} else {
			log.Printf("[SSE] Unexpected union type for message.updated")
		}

	case opencode.EventListResponseTypeMessagePartUpdated:
		uni := evt.AsUnion()
		if v, ok := uni.(opencode.EventListResponseEventMessagePartUpdated); ok {
			part := v.Properties.Part
			// Skip reasoning/analysis parts from streaming into visible assistant content
			if strings.EqualFold(string(part.Type), "reasoning") || strings.EqualFold(string(part.Type), "thinking") || strings.EqualFold(string(part.Type), "analysis") {
				log.Printf("[SSE] part.skipped id=%s type=%s len=%d (reasoning/thinking)", part.MessageID, part.Type, len(part.Text))
				return
			}
			// Mark message as completed when step-finish part arrives
			if part.Type == opencode.PartTypeStepFinish {
				if err := orch.syncManager.UpdateMessage(part.MessageID, "", "completed", "sse"); err != nil {
					log.Printf("[SSE] Failed to mark completed for message %s: %v", part.MessageID, err)
				} else {
					log.Printf("[SSE] message.completed id=%s", part.MessageID)
				}
				return
			}
			// Append text to message content
			messageID := part.MessageID
			appended := part.Text
			if appended == "" {
				// nothing to append
				return
			}

			// Get current content for the message and append
			st := orch.syncManager.GetState()
			cur := ""
			exists := false
			for _, m := range st.Messages {
				if m.ID == messageID {
					cur = m.Content
					exists = true
					break
				}
			}
			// If message doesn't exist yet (part arrived before metadata), create it
			if !exists {
				placeholder := types.MessageInfo{
					ID:        messageID,
					SessionID: part.SessionID,
					Type:      "assistant",
					Content:   "",
					Timestamp: time.Now(),
					Status:    "pending",
				}
				if err := orch.syncManager.AddMessage(placeholder, "sse"); err != nil {
					log.Printf("[SSE] Failed to create placeholder message %s: %v", messageID, err)
				}
			}
			// Log diagnostic info before merging
			prefixReplace := strings.HasPrefix(appended, cur)
			// Compute overlap length (suffix of current vs prefix of appended)
			max := len(cur)
			if len(appended) < max {
				max = len(appended)
			}
			overlap := 0
			for i := 1; i <= max; i++ {
				if strings.HasSuffix(cur, appended[:i]) {
					overlap = i
				}
			}
			log.Printf("[SSE] part.updated id=%s type=%s cur_len=%d app_len=%d prefix_replace=%t overlap=%d app_preview=%.80q",
				messageID, part.Type, len(cur), len(appended), prefixReplace, overlap, appended)

			// Merge streaming text intelligently to avoid duplicated content
			newContent := mergeStreamingText(cur, appended)
			log.Printf("[SSE] part.merge   id=%s new_len=%d new_preview=%.80q", messageID, len(newContent), newContent)

			if err := orch.syncManager.UpdateMessage(messageID, newContent, "", "sse"); err != nil {
				log.Printf("[SSE] Failed to append part to message %s: %v", messageID, err)
			}
		} else {
			log.Printf("[SSE] Unexpected union type for message.part.updated")
		}

	case opencode.EventListResponseTypeMessageRemoved:
		uni := evt.AsUnion()
		if v, ok := uni.(opencode.EventListResponseEventMessageRemoved); ok {
			// Construct a deletion update with optimistic version check
			upd := types.StateUpdate{
				ID:              fmt.Sprintf("del_%s_%d", v.Properties.MessageID, time.Now().UnixNano()),
				Type:            types.MessageDeleted,
				ExpectedVersion: orch.syncManager.GetState().GetCurrentVersion(),
				Payload:         types.MessageDeletePayload{MessageID: v.Properties.MessageID},
				SourcePanel:     "sse",
				Timestamp:       time.Now(),
			}
			if err := orch.syncManager.UpdateWithVersionCheck(upd); err != nil {
				log.Printf("[SSE] Failed to delete message %s: %v", v.Properties.MessageID, err)
			}
		} else {
			log.Printf("[SSE] Unexpected union type for message.removed")
		}

	case opencode.EventListResponseTypeSessionDeleted:
		uni := evt.AsUnion()
		if v, ok := uni.(opencode.EventListResponseEventSessionDeleted); ok {
			// Construct a session deletion update
			upd := types.StateUpdate{
				ID:              fmt.Sprintf("del_session_%s_%d", v.Properties.Info.ID, time.Now().UnixNano()),
				Type:            types.SessionDeleted,
				ExpectedVersion: orch.syncManager.GetState().GetCurrentVersion(),
				Payload:         types.SessionDeletePayload{SessionID: v.Properties.Info.ID},
				SourcePanel:     "sse",
				Timestamp:       time.Now(),
			}
			// Apply the update without version check to make deletion idempotent
			if err := orch.syncManager.UpdateWithVersionCheck(upd); err != nil {
				log.Printf("[SSE] Failed to delete session %s: %v", v.Properties.Info.ID, err)
			} else {
				log.Printf("[SSE] Session deleted from state: %s", v.Properties.Info.ID)
			}
		} else {
			log.Printf("[SSE] Unexpected union type for session.deleted")
		}

	default:
		// Log unhandled event types for future mapping
		log.Printf("[SSE] Unhandled event type: %s", string(evt.Type))
	}
}

// handleSSEEvent processes incoming SSE events
func (orch *TmuxOrchestrator) handleSSEEvent(data string) {
	log.Printf("[SSE] Received event: %s", data)

	type envelope struct {
		Type       string          `json:"type"`
		Properties json.RawMessage `json:"properties"`
	}

	var env envelope
	if err := json.Unmarshal([]byte(data), &env); err != nil {
		log.Printf("[SSE] Failed to decode event envelope: %v", err)
		return
	}

	switch env.Type {
	case "session.idle":
		// properties: { sessionID: string }
		type sessionIdleProps struct {
			SessionID string `json:"sessionID"`
		}
		var props sessionIdleProps
		if err := json.Unmarshal(env.Properties, &props); err != nil {
			log.Printf("[SSE] Failed to decode session.idle properties: %v", err)
			return
		}
		log.Printf("[SSE] Session %s is now idle", props.SessionID)
		// For now, just log the event. Could be used for UI state updates in the future.

	case "session.updated":
		// properties: { info: Session }
		type sessionUpdatedProps struct {
			Info opencode.Session `json:"info"`
		}
		var props sessionUpdatedProps
		if err := json.Unmarshal(env.Properties, &props); err != nil {
			log.Printf("[SSE] Failed to decode session.updated properties: %v", err)
			return
		}

		// Update session in state
		sessionInfo := types.SessionInfo{
			ID:           props.Info.ID,
			Title:        props.Info.Title,
			CreatedAt:    parseServerTime(props.Info.Time.Created),
			UpdatedAt:    parseServerTime(props.Info.Time.Updated),
			MessageCount: 0, // Will be updated by message events
			IsActive:     true,
		}

		if err := orch.syncManager.UpdateSession(sessionInfo.ID, sessionInfo.Title, sessionInfo.IsActive, "sse"); err != nil {
			log.Printf("[SSE] Failed to update session in state: %v", err)
			return
		}
		log.Printf("[SSE] Session updated in state: %s", sessionInfo.ID)

	case "message.updated":
		// properties: { info: Message }
		type messageUpdatedProps struct {
			Info opencode.Message `json:"info"`
		}
		var props messageUpdatedProps
		if err := json.Unmarshal(env.Properties, &props); err != nil {
			log.Printf("[SSE] Failed to decode message.updated properties: %v", err)
			return
		}

		// Map to local MessageInfo and add to state
		msgType := string(props.Info.Role)
		message := types.MessageInfo{
			ID:        props.Info.ID,
			SessionID: props.Info.SessionID,
			Type:      msgType,
			Content:   "",
			Timestamp: time.Now(),
			Status:    "pending",
		}

		if err := orch.syncManager.AddMessage(message, "sse"); err != nil {
			log.Printf("[SSE] Failed to add message to state: %v", err)
			return
		}
		log.Printf("[SSE] Message added to state: %s", message.ID)

	case "message.part.updated":
		// properties: { part: { messageID, text, type, ... } }
		type partProps struct {
			Part struct {
				MessageID string `json:"messageID"`
				Text      string `json:"text"`
				Type      string `json:"type"`
			} `json:"part"`
		}
		var props partProps
		if err := json.Unmarshal(env.Properties, &props); err != nil {
			log.Printf("[SSE] Failed to decode message.part.updated properties: %v", err)
			return
		}

		// Update message content; parts aggregation not required for panel
		if err := orch.syncManager.UpdateMessage(props.Part.MessageID, props.Part.Text, "", "sse"); err != nil {
			log.Printf("[SSE] Failed to update message content: %v", err)
			return
		}
		log.Printf("[SSE] Message content updated: %s (%d chars)", props.Part.MessageID, len(props.Part.Text))

	case "message.removed":
		// properties: { messageID, sessionID }
		type messageRemovedProps struct {
			MessageID string `json:"messageID"`
			SessionID string `json:"sessionID"`
		}
		var props messageRemovedProps
		if err := json.Unmarshal(env.Properties, &props); err != nil {
			log.Printf("[SSE] Failed to decode message.removed properties: %v", err)
			return
		}

		// Construct a state update for deletion and apply via manager
		update := types.StateUpdate{
			ID:              time.Now().Format("20060102150405.000000"),
			Type:            types.MessageDeleted,
			ExpectedVersion: orch.syncManager.GetState().GetCurrentVersion(),
			Payload:         types.MessageDeletePayload{MessageID: props.MessageID},
			SourcePanel:     "sse",
			Timestamp:       time.Now(),
		}
		if err := orch.syncManager.UpdateWithVersionCheck(update); err != nil {
			log.Printf("[SSE] Failed to delete message in state: %v", err)
			return
		}
		log.Printf("[SSE] Message deleted from state: %s", props.MessageID)

	default:
		// For other event types, just log for now
		log.Printf("[SSE] Unhandled event type: %s", env.Type)
	}
}

// mergeStreamingText merges an incoming streaming chunk with the current text,
// avoiding duplicated content when updates send the full text each time.
// Strategy:
//   - If the new chunk starts with the current text, use the new chunk (replacement).
//   - Else, find the longest overlap where the end of current matches the start of new,
//     and append only the non-overlapping suffix.
func mergeStreamingText(current, incoming string) string {
	if incoming == "" {
		return current
	}
	if current == "" {
		return incoming
	}
	// If server sends full text repeatedly, incoming will have current as prefix
	if strings.HasPrefix(incoming, current) {
		return incoming
	}
	// Compute maximal overlap between suffix of current and prefix of incoming
	max := len(current)
	if len(incoming) < max {
		max = len(incoming)
	}
	overlap := 0
	for i := 1; i <= max; i++ {
		if strings.HasSuffix(current, incoming[:i]) {
			overlap = i
		}
	}
	return current + incoming[overlap:]
}

// loadSessionsFromServer loads existing sessions from OpenCode server into local state
func (orch *TmuxOrchestrator) loadSessionsFromServer() error {
	log.Printf("Loading existing sessions from OpenCode server...")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Get sessions from server
	sessions, err := orch.httpClient.Session.List(ctx, opencode.SessionListParams{})

	if err != nil {
		return fmt.Errorf("failed to list sessions from server: %w", err)
	}

	if sessions == nil || len(*sessions) == 0 {
		log.Printf("No existing sessions found on server")
		return nil
	}

	log.Printf("Found %d existing sessions on server", len(*sessions))

	// Convert server sessions to local session format and add to state
	for _, serverSession := range *sessions {
		sessionInfo := state.SessionInfo{
			ID:           serverSession.ID,
			Title:        serverSession.Title,
			CreatedAt:    parseServerTime(serverSession.Time.Created),
			UpdatedAt:    parseServerTime(serverSession.Time.Updated),
			MessageCount: 0, // We'd need to call message endpoint to get count
			IsActive:     true,
		}

		// Add the session through the sync manager
		if err := orch.syncManager.AddSession(sessionInfo, "server-sync"); err != nil {
			log.Printf("Warning: Failed to add session %s to local state: %v", serverSession.ID, err)
			continue
		}

		log.Printf("Loaded session: %s (%s)", sessionInfo.Title, sessionInfo.ID)
	}

	// After loading sessions, eagerly load message history for each session so
	// the messages panel shows content on startup and session counts are correct.
	for _, serverSession := range *sessions {
		sid := serverSession.ID
		msgs, err := orch.httpClient.Session.Messages(ctx, sid, opencode.SessionMessagesParams{})
		if err != nil {
			log.Printf("Warning: Failed to load messages for session %s: %v", sid, err)
			continue
		}
		if msgs == nil || len(*msgs) == 0 {
			log.Printf("No messages found for session %s", sid)
			continue
		}

		for _, m := range *msgs {
			var messageType string
			var contentParts []string

			switch info := m.Info.AsUnion().(type) {
			case opencode.UserMessage:
				messageType = "user"
			case opencode.AssistantMessage:
				messageType = "assistant"
				_ = info // suppress unused in switch
			default:
				messageType = "system"
			}

			for _, part := range m.Parts {
				if textPart, ok := part.AsUnion().(opencode.TextPart); ok {
					contentParts = append(contentParts, textPart.Text)
				}
			}

			mi := types.MessageInfo{
				ID:        m.Info.ID,
				SessionID: sid,
				Type:      messageType,
				Content:   strings.Join(contentParts, "\n"),
				Timestamp: time.Now(),
				Status:    "completed",
			}

			if err := orch.syncManager.AddMessage(mi, "server-sync"); err != nil {
				log.Printf("Warning: Failed to add message %s for session %s: %v", mi.ID, sid, err)
			}
		}
	}

	// If no current session selected, choose the first one for consistency
	st := orch.syncManager.GetState()
	if len(st.Sessions) > 0 {
		ensureValid := false
		if st.CurrentSessionID == "" {
			ensureValid = true
		} else {
			// Verify current selection exists in loaded sessions
			found := false
			for _, s := range st.Sessions {
				if s.ID == st.CurrentSessionID {
					found = true
					break
				}
			}
			ensureValid = !found
		}

		if ensureValid {
			first := st.Sessions[0].ID
			if err := orch.syncManager.UpdateSessionSelection(first, "server-sync"); err != nil {
				log.Printf("Warning: failed to set valid session selection: %v", err)
			} else {
				log.Printf("Selected valid session: %s", first)
			}
		}
	}
	if st.CurrentSessionID == "" && len(st.Sessions) > 0 {
		first := st.Sessions[0].ID
		if err := orch.syncManager.UpdateSessionSelection(first, "server-sync"); err != nil {
			log.Printf("Warning: failed to set default session selection: %v", err)
		} else {
			log.Printf("Default session selected: %s", first)
		}
	}

	log.Printf("Successfully loaded %d sessions from server", len(*sessions))
	return nil
}

// parseServerTime safely converts a server timestamp to a time.Time object.
// It returns a zero-value time if the timestamp is not positive.
func parseServerTime(timestamp float64) time.Time {
	if timestamp <= 0 {
		return time.Time{}
	}
	// The timestamp from the server is in milliseconds, so use UnixMilli.
	return time.UnixMilli(int64(timestamp))
}

// isTerminal checks if stdin is a terminal
func isTerminal() bool {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) != 0
}

// startAPIRequestHandler starts the API request handler for TUI control
func (orch *TmuxOrchestrator) startAPIRequestHandler() {
	log.Printf("Starting API request handler...")

	for {
		select {
		case <-orch.ctx.Done():
			log.Printf("API request handler shutting down")
			return
		default:
			var req struct {
				Path string          `json:"path"`
				Body json.RawMessage `json:"body"`
			}

			ctx, cancel := context.WithTimeout(orch.ctx, 5*time.Second)
			err := orch.httpClient.Get(ctx, "/tui/control/next", nil, &req)
			cancel()

			if err != nil {
				// Log error but continue - this is expected when no requests are pending
				time.Sleep(1 * time.Second)
				continue
			}

			log.Printf("Received API request: %s", req.Path)

			// Handle the API request
			response := orch.handleAPIRequest(req.Path, req.Body)

			// Send response back to server
			ctx, cancel = context.WithTimeout(orch.ctx, 5*time.Second)
			err = orch.httpClient.Post(ctx, "/tui/control/response", response, nil)
			cancel()

			if err != nil {
				log.Printf("Failed to send API response: %v", err)
			}
		}
	}
}

// handleAPIRequest handles incoming API requests
func (orch *TmuxOrchestrator) handleAPIRequest(path string, body json.RawMessage) interface{} {
	log.Printf("Handling API request: %s", path)

	switch path {
	case "/tui/open-models":
		return orch.handleOpenModelsRequest(body)
	case "/tui/open-sessions":
		return orch.handleOpenSessionsRequest(body)
	case "/tui/open-themes":
		return orch.handleOpenThemesRequest(body)
	case "/tui/open-help":
		return orch.handleOpenHelpRequest(body)
	case "/tui/open-agents":
		return orch.handleOpenAgentsRequest(body)
	default:
		log.Printf("Unknown API request path: %s", path)
		return map[string]interface{}{
			"success": false,
			"error":   "unknown request path",
		}
	}
}

// handleOpenModelsRequest handles the /tui/open-models request
func (orch *TmuxOrchestrator) handleOpenModelsRequest(body json.RawMessage) interface{} {
	log.Printf("Handling open models request")

	// Create a state update to trigger model dialog opening in all connected panels
	update := types.StateUpdate{
		ID:              fmt.Sprintf("open_models_%d", time.Now().UnixNano()),
		Type:            types.UIActionTriggered,
		ExpectedVersion: orch.syncManager.GetState().GetCurrentVersion(),
		Payload: types.UIActionPayload{
			Action: "open_models",
		},
		SourcePanel: "tmux-orchestrator",
		Timestamp:   time.Now(),
	}

	// Apply the update through sync manager
	if err := orch.syncManager.UpdateWithVersionCheck(update); err != nil {
		log.Printf("Failed to trigger open models action: %v", err)
		return map[string]interface{}{
			"success": false,
			"error":   "failed to trigger model dialog",
		}
	}

	log.Printf("Successfully triggered open models action")
	return true
}

// handleOpenAgentsRequest handles the /tui/open-agents request
func (orch *TmuxOrchestrator) handleOpenAgentsRequest(body json.RawMessage) interface{} {
	log.Printf("Handling open agents request")

	// Create a state update to trigger agent dialog opening
	update := types.StateUpdate{
		ID:              fmt.Sprintf("open_agents_%d", time.Now().UnixNano()),
		Type:            types.UIActionTriggered,
		ExpectedVersion: orch.syncManager.GetState().GetCurrentVersion(),
		Payload: types.UIActionPayload{
			Action: "open_agents",
		},
		SourcePanel: "tmux-orchestrator",
		Timestamp:   time.Now(),
	}

	if err := orch.syncManager.UpdateWithVersionCheck(update); err != nil {
		log.Printf("Failed to trigger open agents action: %v", err)
		return map[string]interface{}{
			"success": false,
			"error":   "failed to trigger agent dialog",
		}
	}

	return true
}

// handleOpenSessionsRequest handles the /tui/open-sessions request
func (orch *TmuxOrchestrator) handleOpenSessionsRequest(body json.RawMessage) interface{} {
	log.Printf("Handling open sessions request")

	// Create a state update to trigger session dialog opening
	update := types.StateUpdate{
		ID:              fmt.Sprintf("open_sessions_%d", time.Now().UnixNano()),
		Type:            types.UIActionTriggered,
		ExpectedVersion: orch.syncManager.GetState().GetCurrentVersion(),
		Payload: types.UIActionPayload{
			Action: "open_sessions",
		},
		SourcePanel: "tmux-orchestrator",
		Timestamp:   time.Now(),
	}

	if err := orch.syncManager.UpdateWithVersionCheck(update); err != nil {
		log.Printf("Failed to trigger open sessions action: %v", err)
		return map[string]interface{}{
			"success": false,
			"error":   "failed to trigger session dialog",
		}
	}

	return true
}

// handleOpenThemesRequest handles the /tui/open-themes request
func (orch *TmuxOrchestrator) handleOpenThemesRequest(body json.RawMessage) interface{} {
	log.Printf("Handling open themes request")

	// Create a state update to trigger theme dialog opening
	update := types.StateUpdate{
		ID:              fmt.Sprintf("open_themes_%d", time.Now().UnixNano()),
		Type:            types.UIActionTriggered,
		ExpectedVersion: orch.syncManager.GetState().GetCurrentVersion(),
		Payload: types.UIActionPayload{
			Action: "open_themes",
		},
		SourcePanel: "tmux-orchestrator",
		Timestamp:   time.Now(),
	}

	if err := orch.syncManager.UpdateWithVersionCheck(update); err != nil {
		log.Printf("Failed to trigger open themes action: %v", err)
		return map[string]interface{}{
			"success": false,
			"error":   "failed to trigger theme dialog",
		}
	}

	return true
}

// handleOpenHelpRequest handles the /tui/open-help request
func (orch *TmuxOrchestrator) handleOpenHelpRequest(body json.RawMessage) interface{} {
	log.Printf("Handling open help request")

	// Create a state update to trigger help dialog opening
	update := types.StateUpdate{
		ID:              fmt.Sprintf("open_help_%d", time.Now().UnixNano()),
		Type:            types.UIActionTriggered,
		ExpectedVersion: orch.syncManager.GetState().GetCurrentVersion(),
		Payload: types.UIActionPayload{
			Action: "open_help",
		},
		SourcePanel: "tmux-orchestrator",
		Timestamp:   time.Now(),
	}

	if err := orch.syncManager.UpdateWithVersionCheck(update); err != nil {
		log.Printf("Failed to trigger open help action: %v", err)
		return map[string]interface{}{
			"success": false,
			"error":   "failed to trigger help dialog",
		}
	}

	return true
}
