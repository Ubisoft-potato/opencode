package main

import (
	"context"
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
}

// NewTmuxOrchestrator creates a new tmux orchestrator
func NewTmuxOrchestrator(sessionName, socketPath, statePath string, httpClient *opencode.Client) *TmuxOrchestrator {
	ctx, cancel := context.WithCancel(context.Background())

	return &TmuxOrchestrator{
		sessionName: sessionName,
		socketPath:  socketPath,
		statePath:   statePath,
		httpClient:  httpClient,
		ctx:         ctx,
		cancel:      cancel,
		tmuxCommand: "tmux",
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

	// Configure panels
	if err := orch.configurePanels(); err != nil {
		return fmt.Errorf("failed to configure panels: %w", err)
	}

	// Start panel applications
	if err := orch.startPanelApplications(); err != nil {
		return fmt.Errorf("failed to start panel applications: %w", err)
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

	// Start sessions panel
	if err := orch.startPanelApp(sessionTarget+".0", "opencode-sessions", envVars); err != nil {
		return fmt.Errorf("failed to start sessions panel: %w", err)
	}

	// Start messages panel
	if err := orch.startPanelApp(sessionTarget+".1", "opencode-messages", envVars); err != nil {
		return fmt.Errorf("failed to start messages panel: %w", err)
	}

	// Start input panel
	if err := orch.startPanelApp(sessionTarget+".2", "opencode-input", envVars); err != nil {
		return fmt.Errorf("failed to start input panel: %w", err)
	}

	return nil
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

	command := fmt.Sprintf("%s%s", envCmd, appName)

	// Send command to pane
	cmd := exec.CommandContext(orch.ctx, orch.tmuxCommand, "send-keys", "-t", paneTarget, command, "Enter")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to send command to pane %s: %w", paneTarget, err)
	}

	// Give the application time to start
	time.Sleep(500 * time.Millisecond)

	return nil
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
	// Parse command line arguments
	sessionName := "opencode"
	if len(os.Args) > 1 {
		sessionName = os.Args[1]
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
	orchestrator := NewTmuxOrchestrator(sessionName, socketPath, statePath, httpClient)

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

	// Attach to session if stdin is a terminal
	if isTerminal() {
		log.Printf("Attaching to tmux session...")
		if err := orchestrator.attachToSession(); err != nil {
			log.Printf("Failed to attach to session: %v", err)
		}
	} else {
		// Wait for shutdown signal
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