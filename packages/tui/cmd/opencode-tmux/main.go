package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
	"github.com/sst/opencode/internal/ipc"
	"github.com/sst/opencode/internal/persistence"
	"github.com/sst/opencode/internal/state"
	"github.com/sst/opencode/internal/types"
)

// TmuxOrchestrator manages the tmux session and panels
type TmuxOrchestrator struct {
	sessionName    string
	socketPath     string
	statePath      string
	httpClient     *opencode.Client
	ipcServer      *ipc.SocketServer
	syncManager    *state.PanelSyncManager
	ctx            context.Context
	cancel         context.CancelFunc
	tmuxCommand    string
	isRunning      bool
	serverOnly     bool
}

// NewTmuxOrchestrator creates a new tmux orchestrator
func NewTmuxOrchestrator(sessionName, socketPath, statePath string, httpClient *opencode.Client, serverOnly bool) *TmuxOrchestrator {
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

	// Start IPC server
	if err := orch.startIPCServer(); err != nil {
		return fmt.Errorf("failed to start IPC server: %w", err)
	}

	log.Printf("Tmux orchestrator initialized successfully")
	return nil
}

// Start creates and configures the tmux session with panels
func (orch *TmuxOrchestrator) Start() error {
	log.Printf("Starting tmux session: %s", orch.sessionName)

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

	return nil
}

// startPanelApplications starts the applications in each panel
func (orch *TmuxOrchestrator) startPanelApplications() error {
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

// startPanelApp starts an application in a specific tmux pane
func (orch *TmuxOrchestrator) startPanelApp(paneTarget, appName string, envVars map[string]string) error {
	// Build command with environment variables
	var envCmd string
	for key, value := range envVars {
		if value != "" {
			envCmd += fmt.Sprintf("export %s='%s'; ", key, value)
		}
	}

	// Build correct binary path based on app name
	binaryPath, err := orch.getBinaryPath(appName)
	if err != nil {
		return fmt.Errorf("failed to get binary path for %s: %w", appName, err)
	}

	command := fmt.Sprintf("%s%s; sleep 600", envCmd, binaryPath)

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
	sessionTarget := orch.sessionName + ":0"

	panelsRunning := 0
	totalPanels := 3

	for i := 0; i < totalPanels; i++ {
		paneTarget := fmt.Sprintf("%s.%d", sessionTarget, i)

		// Check if pane exists and is active
		cmd := exec.Command(orch.tmuxCommand, "list-panes", "-t", paneTarget, "-F", "#{pane_pid}")
		if output, err := cmd.Output(); err == nil && len(output) > 0 {
			panelsRunning++
			log.Printf("[DEBUG] Pane %d is active (PID: %s)", i, string(output)[:len(output)-1])
		} else {
			log.Printf("[DEBUG] Pane %d is not active or has no process", i)
		}
	}

	log.Printf("Panel verification: %d/%d panels are running", panelsRunning, totalPanels)
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
	logFile, err := os.OpenFile("/Users/hhx/.opencode/tmux.log",
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		log.SetOutput(logFile)
		defer logFile.Close()
	}

	// Parse command line arguments
	var serverOnly bool
	flag.BoolVar(&serverOnly, "server-only", false, "Only start IPC server without panels")
	flag.Parse()

	sessionName := "opencode"
	if flag.NArg() > 0 {
		sessionName = flag.Arg(0)
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

	// Create orchestrator
	orchestrator := NewTmuxOrchestrator(sessionName, socketPath, statePath, httpClient, serverOnly)

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

// isTerminal checks if stdin is a terminal
func isTerminal() bool {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) != 0
}