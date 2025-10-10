package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	flag "github.com/spf13/pflag"
	"github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
	"github.com/sst/opencode/internal/ipc"
	"github.com/sst/opencode/internal/util"
)

var Version = "dev"

func main() {
	version := Version
	if version != "dev" && !strings.HasPrefix(Version, "v") {
		version = "v" + Version
	}

	var model *string = flag.String("model", "", "model to begin with")
	var prompt *string = flag.String("prompt", "", "prompt to begin with")
	var agent *string = flag.String("agent", "", "agent to begin with")
	var sessionID *string = flag.String("session", "", "session ID")
	flag.Parse()

	url := os.Getenv("OPENCODE_SERVER")
	if url == "" {
		slog.Error("OPENCODE_SERVER environment variable not set")
		os.Exit(1)
	}

	httpClient := opencode.NewClient(
		option.WithBaseURL(url),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize logging
	apiHandler := util.NewAPILogHandler(ctx, httpClient, "tmux", slog.LevelDebug)
	logger := slog.New(apiHandler)
	slog.SetDefault(logger)

	slog.Debug("TMux TUI launched")

	// Verify server connectivity before proceeding
	slog.Info("Verifying OpenCode server connectivity", "url", url)
	if err := verifyServerConnection(url); err != nil {
		slog.Error("Failed to connect to OpenCode server", "error", err, "url", url)
		slog.Error("Please ensure that:")
		slog.Error("  1. The OpenCode server is running")
		slog.Error("  2. The server URL is correct: %s", url)
		slog.Error("  3. No firewall is blocking the connection")
		slog.Error("  4. The server has finished initializing")
		os.Exit(1)
	}
	slog.Info("✅ Server connectivity verified successfully")

	// Start IPC server
	socketPath := ipc.SocketPath()
	slog.Info("Starting IPC server", "socketPath", socketPath)
	ipcServer := ipc.NewServer(socketPath)
	err := ipcServer.Start(ctx)
	if err != nil {
		slog.Error("Failed to start IPC server", "error", err, "socketPath", socketPath)
		slog.Error("This might be due to:")
		slog.Error("  1. Permission issues with socket directory")
		slog.Error("  2. Socket file already exists from previous run")
		slog.Error("  3. System resource limitations")
		os.Exit(1)
	}
	defer ipcServer.Stop()
	slog.Info("✅ IPC server started successfully")

	// Set environment variable for panes to find the IPC socket
	os.Setenv("OPENCODE_IPC_SOCKET", socketPath)

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Create tmux session
	sessionName := fmt.Sprintf("opencode-%d", os.Getpid())

	// Check if tmux is available
	slog.Info("Checking tmux availability")
	if _, err := exec.LookPath("tmux"); err != nil {
		slog.Error("tmux not found in PATH", "error", err)
		slog.Error("Please install tmux:")
		slog.Error("  macOS: brew install tmux")
		slog.Error("  Ubuntu/Debian: sudo apt-get install tmux")
		slog.Error("  CentOS/RHEL: sudo yum install tmux")
		os.Exit(1)
	}
	slog.Info("✅ tmux found and available")

	// Kill any existing session with the same name
	slog.Info("Cleaning up any existing tmux session", "sessionName", sessionName)
	exec.Command("tmux", "kill-session", "-t", sessionName).Run()

	// Create new tmux session with custom layout
	slog.Info("Creating tmux session with OpenCode panes", "sessionName", sessionName)
	err = createTmuxSession(sessionName, model, prompt, agent, sessionID)
	if err != nil {
		slog.Error("Failed to create tmux session", "error", err, "sessionName", sessionName)
		slog.Error("This might be due to:")
		slog.Error("  1. Missing pane binaries")
		slog.Error("  2. tmux configuration issues")
		slog.Error("  3. Insufficient system resources")
		os.Exit(1)
	}
	slog.Info("✅ tmux session created successfully")

	// Create a default OpenCode session for better user experience
	slog.Info("Creating default OpenCode session for immediate use")
	if err := createDefaultSession(url); err != nil {
		slog.Warn("Failed to create default session, user will need to create one manually", "error", err)
	} else {
		slog.Info("✅ Default OpenCode session created")
	}

	// Handle signals
	go func() {
		sig := <-sigChan
		slog.Info("Received signal, shutting down gracefully", "signal", sig)
		cleanup(sessionName)
		cancel()
	}()

	// Attach to the tmux session (blocking)
	attachCmd := exec.CommandContext(ctx, "tmux", "attach-session", "-t", sessionName)
	attachCmd.Stdin = os.Stdin
	attachCmd.Stdout = os.Stdout
	attachCmd.Stderr = os.Stderr

	err = attachCmd.Run()
	if err != nil {
		slog.Error("Failed to attach to tmux session", "error", err)
	}

	cleanup(sessionName)
	slog.Info("TMux TUI exited")
}

func createTmuxSession(sessionName string, model, prompt, agent, sessionID *string) error {
	// Get the directory containing the tmux binary
	currentDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current directory: %w", err)
	}

	// Build paths to the pane binaries - use compiled binaries from dist directories
	messagesPath := filepath.Join(currentDir, "packages", "tui", "cmd", "opencode-messages", "dist", "messages-pane")
	inputPath := filepath.Join(currentDir, "packages", "tui", "cmd", "opencode-input", "dist", "input-pane")
	sessionsPath := filepath.Join(currentDir, "packages", "tui", "cmd", "opencode-sessions", "dist", "sessions-pane")

	// Verify all binaries exist
	for name, path := range map[string]string{
		"messages": messagesPath,
		"input":    inputPath,
		"sessions": sessionsPath,
	} {
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("pane binary %s not found at %s: %w", name, path, err)
		}
	}

	// Create new tmux session in detached mode
	createCmd := exec.Command("tmux", "new-session", "-d", "-s", sessionName)
	if err := createCmd.Run(); err != nil {
		return fmt.Errorf("failed to create tmux session: %w", err)
	}

	// Split horizontally for top section (sessions + messages)
	splitCmd := exec.Command("tmux", "split-window", "-h", "-t", sessionName+":0")
	if err := splitCmd.Run(); err != nil {
		return fmt.Errorf("failed to split window horizontally: %w", err)
	}

	// Split vertically for bottom section (input)
	splitBottomCmd := exec.Command("tmux", "split-window", "-v", "-t", sessionName+":0.1")
	if err := splitBottomCmd.Run(); err != nil {
		return fmt.Errorf("failed to split window vertically: %w", err)
	}

	// Resize panes: sessions pane (20%), messages pane (80%), input pane (full width, bottom)
	// Set sessions pane to 20% width
	resizeSessionsCmd := exec.Command("tmux", "resize-pane", "-t", sessionName+":0.0", "-x", "20%")
	if err := resizeSessionsCmd.Run(); err != nil {
		return fmt.Errorf("failed to resize sessions pane: %w", err)
	}

	// Set input pane to 20% height
	resizeInputCmd := exec.Command("tmux", "resize-pane", "-t", sessionName+":0.2", "-y", "20%")
	if err := resizeInputCmd.Run(); err != nil {
		return fmt.Errorf("failed to resize input pane: %w", err)
	}

	// Prepare command arguments
	args := []string{}
	if model != nil && *model != "" {
		args = append(args, "--model", *model)
	}
	if prompt != nil && *prompt != "" {
		args = append(args, "--prompt", *prompt)
	}
	if agent != nil && *agent != "" {
		args = append(args, "--agent", *agent)
	}
	if sessionID != nil && *sessionID != "" {
		args = append(args, "--session", *sessionID)
	}

	// Start sessions pane (left pane - pane 0)
	sessionsCmd := fmt.Sprintf("%s %s", sessionsPath, strings.Join(args, " "))
	startSessionsCmd := exec.Command("tmux", "send-keys", "-t", sessionName+":0.0", sessionsCmd, "Enter")
	if err := startSessionsCmd.Run(); err != nil {
		return fmt.Errorf("failed to start sessions pane: %w", err)
	}

	// Start messages pane (right pane - pane 1)
	messagesCmd := fmt.Sprintf("%s %s", messagesPath, strings.Join(args, " "))
	startMessagesCmd := exec.Command("tmux", "send-keys", "-t", sessionName+":0.1", messagesCmd, "Enter")
	if err := startMessagesCmd.Run(); err != nil {
		return fmt.Errorf("failed to start messages pane: %w", err)
	}

	// Start input pane (bottom pane - pane 2)
	inputCmd := fmt.Sprintf("%s %s", inputPath, strings.Join(args, " "))
	startInputCmd := exec.Command("tmux", "send-keys", "-t", sessionName+":0.2", inputCmd, "Enter")
	if err := startInputCmd.Run(); err != nil {
		return fmt.Errorf("failed to start input pane: %w", err)
	}

	// Set focus to input pane
	focusCmd := exec.Command("tmux", "select-pane", "-t", sessionName+":0.2")
	if err := focusCmd.Run(); err != nil {
		return fmt.Errorf("failed to focus input pane: %w", err)
	}

	// Wait a moment for panes to initialize
	time.Sleep(500 * time.Millisecond)

	return nil
}

func cleanup(sessionName string) {
	// Kill the tmux session
	killCmd := exec.Command("tmux", "kill-session", "-t", sessionName)
	if err := killCmd.Run(); err != nil {
		slog.Debug("Failed to kill tmux session (may already be dead)", "error", err)
	}
}

func verifyServerConnection(serverURL string) error {
	// Simple HTTP GET to check if server is responding
	// Use /config endpoint as it's available and indicates server readiness
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	maxAttempts := 20 // Try for up to 20 times
	retryDelay := 500 * time.Millisecond

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		configURL := strings.TrimSuffix(serverURL, "/") + "/config"
		slog.Debug("Attempting server connection", "attempt", attempt, "maxAttempts", maxAttempts, "url", configURL)

		resp, err := client.Get(configURL)
		if err != nil {
			slog.Debug("Connection attempt failed", "attempt", attempt, "error", err)
			if attempt < maxAttempts {
				time.Sleep(retryDelay)
				continue
			}
			return fmt.Errorf("failed to connect to server after %d attempts: %w", maxAttempts, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			slog.Debug("Server connection successful", "attempt", attempt, "statusCode", resp.StatusCode)
			return nil
		}

		slog.Debug("Server returned non-success status", "attempt", attempt, "statusCode", resp.StatusCode)

		if attempt < maxAttempts {
			time.Sleep(retryDelay)
			continue
		}

		return fmt.Errorf("server returned status code %d after %d attempts", resp.StatusCode, maxAttempts)
	}

	return fmt.Errorf("unexpected end of retry loop")
}

func createDefaultSession(serverURL string) error {
	// Create a simple HTTP client for the API call
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Create session request with proper JSON body
	sessionURL := strings.TrimSuffix(serverURL, "/") + "/session"

	// Create a basic session request body
	requestBody := strings.NewReader("{}")

	req, err := http.NewRequest("POST", sessionURL, requestBody)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		slog.Debug("Default session created successfully", "statusCode", resp.StatusCode)
		return nil
	}

	// Read response body for better error information
	body, _ := io.ReadAll(resp.Body)
	slog.Debug("Session creation failed", "statusCode", resp.StatusCode, "response", string(body))

	return fmt.Errorf("server returned status code: %d", resp.StatusCode)
}