package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/chzyer/readline"
	flag "github.com/spf13/pflag"
	"github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
	"github.com/sst/opencode/internal/app"
	"github.com/sst/opencode/internal/ipc"
	"github.com/sst/opencode/internal/state"
	"github.com/sst/opencode/internal/util"
	"golang.org/x/sync/errgroup"
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

	// Fetch required data in parallel
	var agents []opencode.Agent
	var path *opencode.Path
	var project *opencode.Project

	batch := errgroup.Group{}

	batch.Go(func() error {
		result, err := httpClient.Project.Current(context.Background(), opencode.ProjectCurrentParams{})
		if err != nil {
			return err
		}
		project = result
		return nil
	})

	batch.Go(func() error {
		result, err := httpClient.Agent.List(context.Background(), opencode.AgentListParams{})
		if err != nil {
			return err
		}
		agents = *result
		return nil
	})

	batch.Go(func() error {
		result, err := httpClient.Path.Get(context.Background(), opencode.PathGetParams{})
		if err != nil {
			return err
		}
		path = result
		return nil
	})

	err := batch.Wait()
	if err != nil {
		slog.Error("Failed to fetch initial data", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize logging
	apiHandler := util.NewAPILogHandler(ctx, httpClient, "input", slog.LevelDebug)
	logger := slog.New(apiHandler)
	slog.SetDefault(logger)

	slog.Debug("Input pane launched")

	// Create app instance
	app_, err := app.New(ctx, version, project, path, agents, httpClient, model, prompt, agent, sessionID)
	if err != nil {
		slog.Error("Failed to create app", "error", err)
		os.Exit(1)
	}

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Start input handler
	inputHandler := NewInputHandler(app_)

	// Handle initial prompt if provided
	if prompt != nil && *prompt != "" {
		err = inputHandler.SendPrompt(*prompt)
		if err != nil {
			slog.Error("Failed to send initial prompt", "error", err)
		}
	}

	// Start the input loop
	go func() {
		err := inputHandler.Start(ctx)
		if err != nil && err != io.EOF {
			slog.Error("Input handler error", "error", err)
		}
		cancel()
	}()

	// Wait for shutdown signal
	select {
	case sig := <-sigChan:
		slog.Info("Input pane received signal", "signal", sig)
	case <-ctx.Done():
		slog.Info("Input pane context cancelled")
	}

	cancel()
	slog.Info("Input pane exited")
}

// InputHandler handles user input and command processing
type InputHandler struct {
	app          *app.App
	rl           *readline.Instance
	history      []string
	ipcClient    *ipc.Client
	stateManager *state.StateManager
}

func NewInputHandler(app *app.App) *InputHandler {
	// Configure readline
	config := &readline.Config{
		Prompt:          "> ",
		HistoryFile:     "/tmp/opencode_history",
		AutoComplete:    newCompleter(app),
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	}

	rl, err := readline.NewEx(config)
	if err != nil {
		slog.Error("Failed to create readline instance", "error", err)
		return &InputHandler{app: app}
	}

	// Initialize IPC client
	socketPath := os.Getenv("OPENCODE_IPC_SOCKET")
	var ipcClient *ipc.Client
	if socketPath != "" {
		ipcClient = ipc.NewClient(socketPath)
	}

	return &InputHandler{
		app:          app,
		rl:           rl,
		history:      make([]string, 0),
		ipcClient:    ipcClient,
		stateManager: state.NewStateManager(),
	}
}

func (h *InputHandler) Start(ctx context.Context) error {
	if h.rl == nil {
		return fmt.Errorf("readline not initialized")
	}
	defer h.rl.Close()

	// Connect to IPC server if available
	if h.ipcClient != nil {
		err := h.ipcClient.Connect(ctx)
		if err != nil {
			slog.Warn("Failed to connect to IPC server", "error", err)
		} else {
			defer h.ipcClient.Disconnect()
			slog.Info("Connected to IPC server")
		}
	}

	// Start IPC event listening in background
	if h.ipcClient != nil {
		go h.listenForEvents(ctx)
	}

	// Try to sync with current session from shared state
	h.syncWithSharedSession()

	// Display welcome message
	h.displayWelcomeMessage()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			line, err := h.rl.Readline()
			if err != nil {
				if err == io.EOF || err == readline.ErrInterrupt {
					return err
				}
				slog.Error("Readline error", "error", err)
				continue
			}

			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			// Handle special commands
			switch line {
			case "exit", "quit", "q", ":q":
				return io.EOF
			}

			// Add to history
			h.history = append(h.history, line)

			// Process the input
			err = h.processInput(line)
			if err != nil {
				slog.Error("Failed to process input", "error", err, "input", line)
				fmt.Printf("Error: %v\n", err)
			}
		}
	}
}

func (h *InputHandler) processInput(input string) error {
	if strings.HasPrefix(input, "/") {
		// Handle slash commands
		return h.handleCommand(input[1:])
	}

	// Send as regular prompt
	return h.SendPrompt(input)
}

func (h *InputHandler) handleCommand(command string) error {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return fmt.Errorf("empty command")
	}

	commandName := parts[0]
	_ = parts[1:] // Ignore additional arguments for now

	switch commandName {
	case "new":
		fmt.Println("🚀 Creating new session...")
		err := h.createNewSession()
		if err != nil {
			fmt.Printf("❌ Failed to create session: %v\n", err)
			return err
		}

		// Notify other panes via IPC that a new session was created
		if h.ipcClient != nil && h.app.Session.ID != "" {
			event := ipc.NewEvent(ipc.EventSessionCreated, "input", &ipc.SessionChangedData{
				SessionID: h.app.Session.ID,
				Title:     "New Session",
			})
			if err := h.ipcClient.Send(event); err != nil {
				slog.Warn("Failed to send session created event", "error", err)
			} else {
				fmt.Println("📡 Notified other panes about new session")
			}
		}

		fmt.Println("💡 Session created! Other panes should update automatically.")
		return nil

	case "help":
		h.displayHelp()
		return nil

	case "sessions":
		fmt.Println("📋 Listing sessions (feature coming soon)")
		return nil

	case "models":
		fmt.Println("🤖 Listing models (feature coming soon)")
		return nil

	case "agents":
		fmt.Println("🤖 Listing agents (feature coming soon)")
		return nil

	case "clear":
		// Clear screen
		fmt.Print("\033[2J\033[H")
		h.displayWelcomeMessage()
		return nil

	default:
		fmt.Printf("❌ Unknown command: /%s\n", commandName)
		fmt.Println("💡 Type /help for available commands")
		return nil
	}
}

func (h *InputHandler) SendPrompt(text string) error {
	if h.app.Session.ID == "" {
		// FIRST: Try to sync with existing selected session from other panes
		fmt.Println("\n🔍 No active session found. Checking if another pane has selected a session...")

		if h.syncWithSharedSession() {
			fmt.Printf("✅ Found selected session: %s\n", h.app.Session.ID)
			fmt.Println("📤 Sending your message to the selected session...")
		} else {
			// ONLY create new session if no session is selected anywhere
			fmt.Println("🚀 No session selected anywhere. Creating a new session...")

			err := h.createNewSession()
			if err != nil {
				fmt.Printf("❌ Failed to create new session: %v\n", err)
				fmt.Println("💡 Try typing '/new' to create a session manually, or check if the server is running.")
				return fmt.Errorf("no active session and failed to create one: %w", err)
			}

			fmt.Println("✅ New session created! Sending your message...")

			// Notify other panes about the new session via both IPC and shared state
			h.notifySessionChange(h.app.Session.ID, "New Session")
		}
	}

	// Send message to OpenCode via HTTP API
	if err := h.sendMessageToSession(text); err != nil {
		fmt.Printf("❌ Failed to send message: %v\n", err)
		return err
	}

	fmt.Printf("📤 Message sent: %s\n", text)
	return nil
}

func (h *InputHandler) displayHelp() {
	fmt.Println("🔧 Available Commands:")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  /new      - Create a new OpenCode session")
	fmt.Println("  /sessions - List all sessions (coming soon)")
	fmt.Println("  /models   - List available models (coming soon)")
	fmt.Println("  /agents   - List available agents (coming soon)")
	fmt.Println("  /clear    - Clear the screen")
	fmt.Println("  /help     - Show this help message")
	fmt.Println("  /quit     - Exit the input pane")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("💬 Or just type a message to chat with OpenCode")
	fmt.Println()
}

func (h *InputHandler) displayWelcomeMessage() {
	fmt.Println("🎯 OpenCode TMux Input Pane")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	if h.app.Session.ID != "" {
		fmt.Printf("✅ Active session: %s\n", h.app.Session.ID)
	} else {
		fmt.Println("⏳ No active session - will create one automatically when you send a message")
	}

	fmt.Println()
	fmt.Println("💬 Start typing to chat with OpenCode")
	fmt.Println("🔧 Commands: /new, /help, /quit")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()
}

type SessionResponse struct {
	ID    string `json:"id"`
	Title string `json:"title,omitempty"`
}

func (h *InputHandler) createNewSession() error {
	// Use HTTP client to create a real session
	url := os.Getenv("OPENCODE_SERVER")
	if url == "" {
		return fmt.Errorf("OPENCODE_SERVER environment variable not set")
	}

	// Create session via HTTP API
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	sessionURL := strings.TrimSuffix(url, "/") + "/session"
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

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status code %d: %s", resp.StatusCode, string(body))
	}

	// Parse response to get session ID
	var sessionResp SessionResponse
	if err := json.NewDecoder(resp.Body).Decode(&sessionResp); err != nil {
		return fmt.Errorf("failed to parse session response: %w", err)
	}

	// Update the app's session
	h.app.Session.ID = sessionResp.ID
	fmt.Printf("✅ New session created successfully!\n")
	fmt.Printf("🆔 Session ID: %s\n", sessionResp.ID)

	return nil
}

// listenForEvents listens for IPC events from other panes
func (h *InputHandler) listenForEvents(ctx context.Context) {
	if h.ipcClient == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-h.ipcClient.Events():
			switch event.Type {
			case ipc.EventSessionChanged:
				sessionID := h.extractSessionID(event)
				if sessionID != "" && sessionID != h.app.Session.ID {
					slog.Info("Input pane received session change", "sessionID", sessionID, "previousID", h.app.Session.ID)

					h.app.Session.ID = sessionID

					// Try to get session title from the event data
					if data, ok := event.Data.(map[string]interface{}); ok {
						if title, exists := data["title"].(string); exists {
							h.app.Session.Title = title
						}
					}

					fmt.Printf("📡 Session switched to: %s\n", sessionID)

					// Update welcome message to reflect new session
					h.displayWelcomeMessage()
				}
			case ipc.EventSessionCreated:
				fmt.Println("📡 New session created by another pane")
			case ipc.EventSessionDeleted:
				if data, ok := event.Data.(map[string]interface{}); ok {
					if sessionID, exists := data["session_id"].(string); exists {
						fmt.Printf("📡 Session deleted: %s\n", sessionID)

						// 如果删除的是当前活动session，清空当前session
						if h.app.Session.ID == sessionID {
							h.app.Session.ID = ""
							fmt.Println("⚠️  当前活动session已被删除，请选择或创建新的session")
							h.displayWelcomeMessage()
						}
					}
				}
			}
		}
	}
}

// sendMessageToSession sends a message to the current session via HTTP API
func (h *InputHandler) sendMessageToSession(text string) error {
	if h.app.Session.ID == "" {
		return fmt.Errorf("no active session")
	}

	url := os.Getenv("OPENCODE_SERVER")
	if url == "" {
		return fmt.Errorf("OPENCODE_SERVER environment variable not set")
	}

	// Create message via HTTP API
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Create message request body with correct parts format
	messageData := map[string]interface{}{
		"parts": []map[string]interface{}{
			{
				"text": text,
				"type": "text",
			},
		},
	}

	messageBytes, err := json.Marshal(messageData)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	messageURL := strings.TrimSuffix(url, "/") + "/session/" + h.app.Session.ID + "/message"
	req, err := http.NewRequest("POST", messageURL, strings.NewReader(string(messageBytes)))
	if err != nil {
		return fmt.Errorf("failed to create message request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status code %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// syncWithSharedSession tries to sync with a session selected in other panes
func (h *InputHandler) syncWithSharedSession() bool {
	if sharedState, err := h.stateManager.GetState(); err == nil {
		if sharedState.CurrentSessionID != "" && sharedState.CurrentSessionID != h.app.Session.ID {
			slog.Info("Input pane syncing with shared session",
				"currentSession", h.app.Session.ID,
				"sharedSession", sharedState.CurrentSessionID,
				"updatedBy", sharedState.UpdatedBy)

			h.app.Session.ID = sharedState.CurrentSessionID
			h.app.Session.Title = sharedState.CurrentTitle
			return true
		}
	}
	return false
}

// notifySessionChange notifies other panes about session changes
func (h *InputHandler) notifySessionChange(sessionID, title string) {
	// Update shared state
	if err := h.stateManager.SetState(sessionID, title, "input"); err != nil {
		slog.Error("Failed to update shared state", "error", err, "sessionID", sessionID)
	}

	// Send IPC event if available
	if h.ipcClient != nil {
		event := ipc.NewEvent(ipc.EventSessionChanged, "input", &ipc.SessionChangedData{
			SessionID: sessionID,
			Title:     title,
		})
		if err := h.ipcClient.Send(event); err != nil {
			slog.Warn("Failed to send session change event", "error", err)
		} else {
			slog.Info("Successfully notified other panes of session change", "sessionID", sessionID)
		}
	}
}

// extractSessionID extracts session ID from IPC event data with multiple fallback methods
func (h *InputHandler) extractSessionID(event *ipc.Event) string {
	if event.Data == nil {
		return ""
	}

	// Method 1: Direct type assertion to SessionChangedData
	if sessionData, ok := event.Data.(*ipc.SessionChangedData); ok {
		return sessionData.SessionID
	}

	// Method 2: Value type assertion
	if sessionData, ok := event.Data.(ipc.SessionChangedData); ok {
		return sessionData.SessionID
	}

	// Method 3: Map interface access
	if data, ok := event.Data.(map[string]interface{}); ok {
		possibleKeys := []string{"session_id", "SessionID", "sessionId", "sessionID"}
		for _, key := range possibleKeys {
			if value, exists := data[key]; exists {
				if sessionID, ok := value.(string); ok && sessionID != "" {
					return sessionID
				}
			}
		}
	}

	return ""
}

// Simple completer for basic commands and files
func newCompleter(app *app.App) readline.AutoCompleter {
	return readline.NewPrefixCompleter(
		readline.PcItem("/help"),
		readline.PcItem("/clear"),
		readline.PcItem("/quit"),
		readline.PcItem("/exit"),
		readline.PcItem("/new"),
		readline.PcItem("/sessions"),
		readline.PcItem("/models"),
		readline.PcItem("/agents"),
	)
}