package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/eiannone/keyboard"
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
	apiHandler := util.NewAPILogHandler(ctx, httpClient, "sessions", slog.LevelDebug)
	logger := slog.New(apiHandler)
	slog.SetDefault(logger)

	slog.Debug("Sessions pane launched")

	// Create app instance
	app_, err := app.New(ctx, version, project, path, agents, httpClient, model, prompt, agent, sessionID)
	if err != nil {
		slog.Error("Failed to create app", "error", err)
		os.Exit(1)
	}

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Start sessions browser
	browser := NewSessionsBrowser(app_)

	// Start the browser
	go func() {
		err := browser.Start(ctx)
		if err != nil {
			slog.Error("Sessions browser error", "error", err)
		}
		cancel()
	}()

	// Wait for shutdown signal
	select {
	case sig := <-sigChan:
		slog.Info("Sessions pane received signal", "signal", sig)
	case <-ctx.Done():
		slog.Info("Sessions pane context cancelled")
	}

	cancel()
	slog.Info("Sessions pane exited")
}

// SessionsBrowser handles session listing and management
type SessionsBrowser struct {
	app              *app.App
	sessions         []opencode.Session
	selectedIndex    int
	lastRender       string
	refreshInterval  time.Duration
	ipcClient        *ipc.Client
	stateManager     *state.StateManager
	keyboardEnabled  bool
	confirmationMode bool
	keyboardErrors   int
	lastKeyboardError time.Time
	maxRetries       int
	exitRequested    bool
}

func NewSessionsBrowser(app *app.App) *SessionsBrowser {
	// Initialize IPC client
	socketPath := os.Getenv("OPENCODE_IPC_SOCKET")
	var ipcClient *ipc.Client
	if socketPath != "" {
		ipcClient = ipc.NewClient(socketPath)
	}

	return &SessionsBrowser{
		app:             app,
		sessions:        make([]opencode.Session, 0),
		selectedIndex:   0,
		refreshInterval: 2 * time.Second,
		ipcClient:       ipcClient,
		stateManager:    state.NewStateManager(),
		maxRetries:      5,
		exitRequested:   false,
	}
}

func (b *SessionsBrowser) Start(ctx context.Context) error {
	// Connect to IPC server if available
	if b.ipcClient != nil {
		err := b.ipcClient.Connect(ctx)
		if err != nil {
			slog.Warn("Failed to connect to IPC server", "error", err)
		} else {
			defer b.ipcClient.Disconnect()
			slog.Info("Connected to IPC server")
		}
	}

	// Initial load
	err := b.refreshSessions()
	if err != nil {
		slog.Error("Failed to load initial sessions", "error", err)
	}

	// Start input handling in background
	go b.handleInput(ctx)

	// Periodic refresh
	ticker := time.NewTicker(b.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Refresh sessions periodically
			err := b.refreshSessions()
			if err != nil {
				slog.Warn("Failed to refresh sessions", "error", err)
			}
			b.render()
		}
	}
}

func (b *SessionsBrowser) refreshSessions() error {
	sessions, err := b.app.ListSessions(context.Background())
	if err != nil {
		return err
	}

	// Filter out child sessions
	var filteredSessions []opencode.Session
	for _, session := range sessions {
		if session.ParentID == "" {
			filteredSessions = append(filteredSessions, session)
		}
	}

	b.sessions = filteredSessions

	// Adjust selected index if needed
	if b.selectedIndex >= len(b.sessions) {
		b.selectedIndex = len(b.sessions) - 1
	}
	if b.selectedIndex < 0 && len(b.sessions) > 0 {
		b.selectedIndex = 0
	}

	return nil
}

func (b *SessionsBrowser) handleInput(ctx context.Context) {
	// Initialize keyboard with retry logic
	if err := b.initializeKeyboard(); err != nil {
		slog.Error("Failed to initialize keyboard after retries", "error", err)
		return
	}
	defer b.closeKeyboard()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Input handler context cancelled")
			return
		default:
			// Check if exit was requested
			if b.exitRequested {
				slog.Info("Exit requested, terminating input handler")
				return
			}

			char, key, err := b.getKeyWithRetry()
			if err != nil {
				if b.keyboardErrors > b.maxRetries {
					slog.Error("Too many keyboard errors, exiting", "errors", b.keyboardErrors)
					return
				}
				continue
			}

			// 处理确认模式的输入
			if b.confirmationMode {
				if err := b.handleConfirmationInput(char, key); err != nil {
					slog.Debug("Confirmation input handled", "error", err)
				}
				continue
			}

			// 处理普通模式的输入
			if err := b.handleNormalInput(char, key); err != nil {
				slog.Error("Error handling normal input", "error", err)
			}

			// 重新渲染（除非在确认模式）
			if !b.confirmationMode {
				b.render()
			}
		}
	}
}

// handleNormalInput 处理普通模式下的键盘输入
func (b *SessionsBrowser) handleNormalInput(char rune, key keyboard.Key) error {
	slog.Debug("Key pressed", "char", string(char), "key", key)

	switch {
	case char == 'j' || char == 'J' || key == keyboard.KeyArrowDown:
		b.moveDown()
		slog.Debug("Move down", "selectedIndex", b.selectedIndex)
	case char == 'k' || char == 'K' || key == keyboard.KeyArrowUp:
		b.moveUp()
		slog.Debug("Move up", "selectedIndex", b.selectedIndex)
	case key == keyboard.KeyEnter:
		b.selectCurrentSession()
	case char == 'n' || char == 'N':
		b.createNewSession()
	case char == 'd' || char == 'D':
		slog.Info("Delete key pressed, starting deletion process")
		b.deleteCurrentSession()
	case char == 'r' || char == 'R':
		slog.Info("Refresh key pressed")
		if err := b.refreshSessions(); err != nil {
			slog.Error("Failed to refresh sessions", "error", err)
		}
	case key == keyboard.KeyCtrlC || key == keyboard.KeyEsc:
		slog.Info("Exit key pressed, requesting safe exit")
		b.requestExit()
	}

	return nil
}

// handleConfirmationInput 处理确认模式下的键盘输入
func (b *SessionsBrowser) handleConfirmationInput(char rune, key keyboard.Key) error {
	slog.Debug("Confirmation input", "char", string(char), "key", key)

	switch {
	case char == 'y' || char == 'Y':
		slog.Info("User confirmed deletion")
		return fmt.Errorf("CONFIRMED") // 使用错误来传递确认状态
	case char == 'n' || char == 'N' || key == keyboard.KeyEsc || key == keyboard.KeyCtrlC:
		slog.Info("User cancelled deletion")
		return fmt.Errorf("CANCELLED") // 使用错误来传递取消状态
	case key == keyboard.KeyEnter:
		// 回车键默认取消
		slog.Info("User pressed enter, defaulting to cancel")
		return fmt.Errorf("CANCELLED")
	}

	return nil
}

func (b *SessionsBrowser) moveDown() {
	if b.selectedIndex < len(b.sessions)-1 {
		b.selectedIndex++
	}
}

func (b *SessionsBrowser) moveUp() {
	if b.selectedIndex > 0 {
		b.selectedIndex--
	}
}

func (b *SessionsBrowser) selectCurrentSession() {
	// 验证选中索引
	if b.selectedIndex < 0 || b.selectedIndex >= len(b.sessions) {
		slog.Warn("Invalid session index for selection",
			"selectedIndex", b.selectedIndex,
			"totalSessions", len(b.sessions))
		b.showError("无效的session选择")
		return
	}

	if len(b.sessions) == 0 {
		slog.Warn("No sessions available for selection")
		b.showError("没有可选择的session")
		return
	}

	session := b.sessions[b.selectedIndex]
	if session.ID == "" {
		slog.Warn("Selected session has empty ID", "selectedIndex", b.selectedIndex)
		b.showError("选中的session ID无效")
		return
	}

	slog.Info("Switching to session",
		"title", session.Title,
		"id", session.ID,
		"selectedIndex", b.selectedIndex,
		"totalSessions", len(b.sessions))

	// 检查是否已经是当前session
	if b.app.Session.ID == session.ID {
		slog.Info("Already on the selected session", "sessionID", session.ID)
		fmt.Printf("⚠️  Already in session: %s\n", session.Title)
		time.Sleep(1 * time.Second)
		b.render()
		return
	}

	// Update app's current session
	previousSessionID := b.app.Session.ID
	b.app.Session.ID = session.ID
	b.app.Session.Title = session.Title

	slog.Info("Session selection updated locally",
		"sessionID", session.ID,
		"title", session.Title,
		"previousSessionID", previousSessionID)

	// Notify other panes via IPC about session change with retry logic
	if b.ipcClient != nil {
		sessionData := &ipc.SessionChangedData{
			SessionID: session.ID,
			Title:     session.Title,
		}
		event := ipc.NewEvent(ipc.EventSessionChanged, "sessions", sessionData)

		slog.Info("Sending session change event",
			"sessionID", sessionData.SessionID,
			"title", sessionData.Title,
			"previousSessionID", previousSessionID,
			"eventType", event.Type,
			"dataType", fmt.Sprintf("%T", event.Data))

		// Also log the raw event data for debugging
		slog.Debug("Session change event details", "eventData", sessionData)

		// Try sending with retry logic
		maxRetries := 3
		var lastErr error
		success := false

		for attempt := 1; attempt <= maxRetries; attempt++ {
			if err := b.ipcClient.Send(event); err != nil {
				lastErr = err
				slog.Warn("Failed to send session changed event",
					"attempt", attempt,
					"error", err,
					"sessionID", sessionData.SessionID)

				if attempt < maxRetries {
					time.Sleep(time.Duration(attempt*100) * time.Millisecond) // Exponential backoff
					continue
				}
			} else {
				success = true
				slog.Info("Successfully sent session changed event",
					"sessionID", sessionData.SessionID,
					"title", sessionData.Title,
					"attempt", attempt)
				break
			}
		}

		if !success {
			slog.Error("Failed to send session changed event after all retries",
				"error", lastErr,
				"maxRetries", maxRetries,
				"sessionID", sessionData.SessionID,
				"eventData", sessionData)
			fmt.Printf("⚠️  The session switch was successful, but the notification to other panels failed\n")
		}
	} else {
		slog.Warn("IPC client is nil, cannot send session change event",
			"sessionID", session.ID)
		fmt.Printf("⚠️  IPC connection unavailable, file synchronization alternative is used\n")
	}

	// Fallback: write to shared state file
	if err := b.stateManager.SetState(session.ID, session.Title, "sessions"); err != nil {
		slog.Error("Failed to update shared state as fallback", "error", err, "sessionID", session.ID)
	} else {
		slog.Info("Successfully updated shared state as fallback", "sessionID", session.ID)
	}

	fmt.Printf("✅ Switched to session: %s\n", session.Title)

	// Brief pause to show the success message
	time.Sleep(800 * time.Millisecond)
}

func (b *SessionsBrowser) createNewSession() {
	// For now, just log the action - would need proper integration
	slog.Info("Would create new session")
}

func (b *SessionsBrowser) deleteCurrentSession() {
	// 验证输入
	if b.selectedIndex < 0 || b.selectedIndex >= len(b.sessions) {
		slog.Warn("Invalid session index for deletion", "selectedIndex", b.selectedIndex, "sessionsCount", len(b.sessions))
		b.showError("No valid session selected")
		return
	}

	if len(b.sessions) == 0 {
		slog.Warn("No sessions available for deletion")
		b.showError("No erasable sessions")
		return
	}

	session := b.sessions[b.selectedIndex]
	slog.Info("Starting session deletion process", "sessionID", session.ID, "title", session.Title)

	// 显示确认对话框并等待用户输入
	confirmed := b.confirmDeletion(session)
	if !confirmed {
		slog.Info("Session deletion cancelled by user")
		b.render() // 重新渲染正常界面
		return
	}

	// 执行删除操作
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	slog.Info("Executing session deletion", "sessionID", session.ID)
	err := b.app.DeleteSession(ctx, session.ID)
	if err != nil {
		slog.Error("Failed to delete session", "error", err, "sessionID", session.ID)
		b.showError(fmt.Sprintf("删除session失败: %v", err))
		return
	}

	slog.Info("Session deleted successfully", "title", session.Title, "id", session.ID)

	// 从本地列表中移除session
	b.removeSessionFromList(b.selectedIndex)

	// 调整选中索引
	if b.selectedIndex >= len(b.sessions) && len(b.sessions) > 0 {
		b.selectedIndex = len(b.sessions) - 1
	}

	// 通过IPC通知其他面板session已删除
	b.notifySessionDeleted(session.ID)

	// 显示成功消息
	b.showSuccess(fmt.Sprintf("Session '%s' 已成功删除", session.Title))

	// 立即刷新显示
	b.render()
}

func (b *SessionsBrowser) render() {
	// 不在确认模式下才渲染主界面
	if b.confirmationMode {
		return
	}

	// Clear screen and move cursor to top
	fmt.Print("\033[2J\033[H")

	// Get terminal size
	width, height := getTerminalSize()

	// Enhanced title with status
	status := ""
	if b.keyboardEnabled {
		status = " [keyboard ready]"
	} else {
		status = " [Keyboard not ready]"
	}
	title := fmt.Sprintf("Sessions%s (%d个)", status, len(b.sessions))

	fmt.Printf("┌%s┐\n", strings.Repeat("─", width-2))
	fmt.Printf("│ %-*s │\n", width-4, title)
	fmt.Printf("├%s┤\n", strings.Repeat("─", width-2))

	// Available height for sessions list (留空间给调试信息)
	availableHeight := height - 9 // Leave space for title, borders, help, and 2 debug lines

	// Render sessions
	if len(b.sessions) == 0 {
		fmt.Printf("│ %-*s │\n", width-4, "没有可用的session")
		fmt.Printf("│ %-*s │\n", width-4, "按 'n' 创建新session")
		fmt.Printf("│ %-*s │\n", width-4, "")
	} else {
		startIdx := 0
		endIdx := len(b.sessions)

		// If we have more sessions than space, show a window around selected
		if len(b.sessions) > availableHeight {
			startIdx = b.selectedIndex - availableHeight/2
			if startIdx < 0 {
				startIdx = 0
			}
			endIdx = startIdx + availableHeight
			if endIdx > len(b.sessions) {
				endIdx = len(b.sessions)
				startIdx = endIdx - availableHeight
				if startIdx < 0 {
					startIdx = 0
				}
			}
		}

		for i := startIdx; i < endIdx; i++ {
			session := b.sessions[i]
			indicator := " "
			if i == b.selectedIndex {
				indicator = "▶"
			}

			// Mark current session
			current := ""
			if b.app.Session != nil && b.app.Session.ID == session.ID {
				current = " (当前)"
			}

			// Show session index for debugging
			displayTitle := fmt.Sprintf("[%d] %s%s", i, session.Title, current)
			maxTitleLen := width - 8
			if len(displayTitle) > maxTitleLen {
				displayTitle = displayTitle[:maxTitleLen-3] + "..."
			}

			fmt.Printf("│%s %-*s │\n", indicator, width-5, displayTitle)
		}

		// Fill remaining space
		for i := endIdx - startIdx; i < availableHeight; i++ {
			fmt.Printf("│ %-*s │\n", width-4, "")
		}
	}

	// Enhanced debug info
	fmt.Printf("├%s┤\n", strings.Repeat("─", width-2))

	// Status line 1: basic info
	debugInfo1 := fmt.Sprintf("状态: 选中=%d/%d | 键盘=%v | 模式=%s",
		b.selectedIndex,
		func() int {
			if len(b.sessions) == 0 { return 0 }
			return len(b.sessions) - 1
		}(),
		b.keyboardEnabled,
		func() string {
			if b.confirmationMode { return "确认" }
			if b.exitRequested { return "退出中" }
			return "正常"
		}())
	if len(debugInfo1) > width-4 {
		debugInfo1 = debugInfo1[:width-7] + "..."
	}
	fmt.Printf("│ %-*s │\n", width-4, debugInfo1)

	// Status line 2: error info
	debugInfo2 := fmt.Sprintf("错误: 键盘错误=%d/%d | 上次错误=%s",
		b.keyboardErrors, b.maxRetries,
		func() string {
			if b.lastKeyboardError.IsZero() {
				return "无"
			}
			return fmt.Sprintf("%.1f秒前", time.Since(b.lastKeyboardError).Seconds())
		}())
	if len(debugInfo2) > width-4 {
		debugInfo2 = debugInfo2[:width-7] + "..."
	}
	fmt.Printf("│ %-*s │\n", width-4, debugInfo2)

	// Help text
	help := "j/k: 移动 | Enter: 选择 | n: 新建 | d: 删除 | r: 刷新 | ESC: 退出"
	if len(help) > width-4 {
		help = help[:width-7] + "..."
	}
	fmt.Printf("│ %-*s │\n", width-4, help)
	fmt.Printf("└%s┘\n", strings.Repeat("─", width-2))

	// 定期输出状态监控
	slog.Debug("Rendered sessions browser",
		"keyboardEnabled", b.keyboardEnabled,
		"confirmationMode", b.confirmationMode,
		"keyboardErrors", b.keyboardErrors,
		"exitRequested", b.exitRequested,
		"selectedIndex", b.selectedIndex,
		"sessionsCount", len(b.sessions))

	// 检查异常状态并记录警告
	if b.keyboardErrors > 0 {
		slog.Warn("Keyboard errors detected", "errorCount", b.keyboardErrors, "maxRetries", b.maxRetries)
	}
	if b.exitRequested {
		slog.Info("Exit has been requested - system will terminate soon")
	}
}

func getTerminalSize() (width, height int) {
	// Default size if we can't detect
	width, height = 30, 24

	// Try to get actual terminal size using ANSI escape sequences
	// This is a simple fallback - in production you'd use a proper terminal library
	if cols := os.Getenv("COLUMNS"); cols != "" {
		if w, err := strconv.Atoi(cols); err == nil {
			width = w
		}
	}
	if rows := os.Getenv("LINES"); rows != "" {
		if h, err := strconv.Atoi(rows); err == nil {
			height = h
		}
	}

	return width, height
}

// confirmDeletion 显示确认对话框并使用键盘输入处理
func (b *SessionsBrowser) confirmDeletion(session opencode.Session) bool {
	// 安全切换到确认模式
	b.enterConfirmationMode()
	defer b.exitConfirmationMode()

	slog.Info("Entering confirmation mode for session deletion")

	// 显示确认信息
	fmt.Print("\033[2J\033[H") // 清屏
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("                    确认删除Session")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()
	fmt.Printf("您确定要删除以下session吗？\n")
	fmt.Printf("标题: %s\n", session.Title)
	fmt.Printf("ID:   %s\n", session.ID)
	fmt.Println()
	fmt.Println("⚠️  警告: 此操作不可撤销，将永久删除session及其所有消息！")
	fmt.Println()
	fmt.Println("按 Y 确认删除，按 N 或 ESC 取消")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// 等待用户输入 - 循环直到得到明确的答案
	for {
		if !b.keyboardEnabled {
			slog.Error("Keyboard not enabled, cannot get confirmation")
			return false
		}

		char, key, err := keyboard.GetKey()
		if err != nil {
			slog.Error("Failed to get confirmation key", "error", err)
			return false
		}

		slog.Debug("Confirmation key pressed", "char", string(char), "key", key)

		switch {
		case char == 'y' || char == 'Y':
			fmt.Println("\n✅ 确认删除")
			slog.Info("User confirmed deletion")
			time.Sleep(500 * time.Millisecond) // 短暂停顿让用户看到确认
			return true
		case char == 'n' || char == 'N' || key == keyboard.KeyEsc:
			fmt.Println("\n❌ 取消删除")
			slog.Info("User cancelled deletion")
			time.Sleep(500 * time.Millisecond) // 短暂停顿让用户看到取消
			return false
		case key == keyboard.KeyCtrlC:
			fmt.Println("\n⚠️  Ctrl+C pressed - 取消删除并请求退出")
			slog.Info("User pressed Ctrl+C during confirmation - cancelling and requesting exit")
			time.Sleep(500 * time.Millisecond)
			b.requestExit() // 请求安全退出
			return false
		case key == keyboard.KeyEnter:
			// 回车键默认取消
			fmt.Println("\n❌ 默认取消删除")
			slog.Info("User pressed enter, defaulting to cancel")
			time.Sleep(500 * time.Millisecond)
			return false
		default:
			// 忽略其他按键，继续等待
			continue
		}
	}
}

// showError 显示错误信息并等待用户确认
func (b *SessionsBrowser) showError(message string) {
	slog.Error("Showing error to user", "message", message)
	fmt.Print("\033[2J\033[H") // 清屏
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("                       错误")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()
	fmt.Printf("❌ %s\n", message)
	fmt.Println()
	fmt.Println("按任意键继续...")

	b.waitForAnyKey()
}

// showSuccess 显示成功信息
func (b *SessionsBrowser) showSuccess(message string) {
	slog.Info("Showing success to user", "message", message)
	fmt.Print("\033[2J\033[H") // 清屏
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("                       成功")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()
	fmt.Printf("✅ %s\n", message)
	fmt.Println()
	fmt.Println("按任意键继续...")

	b.waitForAnyKey()
}

// waitForAnyKey 等待用户按任意键
func (b *SessionsBrowser) waitForAnyKey() {
	if !b.keyboardEnabled {
		slog.Warn("Keyboard not enabled, cannot wait for key")
		time.Sleep(2 * time.Second) // 2秒后自动继续
		return
	}

	for {
		_, _, err := keyboard.GetKey()
		if err != nil {
			slog.Error("Failed to get key while waiting", "error", err)
			time.Sleep(2 * time.Second) // 出错时等待2秒
			return
		}
		// 任意按键都会继续
		return
	}
}

// removeSessionFromList 从本地列表中移除session
func (b *SessionsBrowser) removeSessionFromList(index int) {
	if index < 0 || index >= len(b.sessions) {
		return
	}

	// 从切片中移除元素
	b.sessions = append(b.sessions[:index], b.sessions[index+1:]...)
}

// notifySessionDeleted 通过IPC通知其他面板session已删除
func (b *SessionsBrowser) notifySessionDeleted(sessionID string) {
	if b.ipcClient == nil {
		slog.Warn("IPC client is nil, cannot send session deleted event", "sessionID", sessionID)
		return
	}

	// 创建session删除事件
	sessionData := &ipc.SessionChangedData{
		SessionID: sessionID,
	}
	event := ipc.NewEvent(ipc.EventSessionDeleted, "sessions", sessionData)

	slog.Info("Sending session deleted event",
		"sessionID", sessionData.SessionID,
		"eventType", event.Type,
		"dataType", fmt.Sprintf("%T", event.Data))

	if err := b.ipcClient.Send(event); err != nil {
		slog.Error("Failed to send session deleted event",
			"error", err,
			"sessionID", sessionID,
			"eventData", sessionData)
	} else {
		slog.Info("Successfully sent session deleted event", "sessionID", sessionID)
	}
}

// initializeKeyboard 初始化键盘输入，带重试逻辑
func (b *SessionsBrowser) initializeKeyboard() error {
	// Check if we're in a tmux environment and have proper stdin
	if !isStdinTerminal() {
		slog.Warn("Not a proper terminal environment, using basic input mode")
		b.keyboardEnabled = false
		return fmt.Errorf("not a terminal environment")
	}

	var lastErr error

	for attempt := 1; attempt <= b.maxRetries; attempt++ {
		// For tmux, we need to be more careful about keyboard initialization
		if isTmuxEnvironment() {
			slog.Debug("Tmux environment detected, using tmux-specific initialization", "attempt", attempt)
			// Add a small delay for tmux pane stabilization
			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
		}

		if err := keyboard.Open(); err != nil {
			lastErr = err
			slog.Warn("Keyboard initialization failed", "attempt", attempt, "error", err, "isTmux", isTmuxEnvironment())
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond) // 递增延迟
			continue
		}

		b.keyboardEnabled = true
		b.keyboardErrors = 0
		slog.Info("Keyboard input initialized successfully", "attempt", attempt, "isTmux", isTmuxEnvironment())
		return nil
	}

	slog.Error("Failed to initialize keyboard after all attempts", "maxAttempts", b.maxRetries, "lastError", lastErr, "isTmux", isTmuxEnvironment())
	return fmt.Errorf("failed to initialize keyboard after %d attempts: %w", b.maxRetries, lastErr)
}

// closeKeyboard 安全关闭键盘输入
func (b *SessionsBrowser) closeKeyboard() {
	if b.keyboardEnabled {
		keyboard.Close()
		b.keyboardEnabled = false
		slog.Info("Keyboard input closed")
	}
}

// getKeyWithRetry 获取键盘输入，带错误恢复
func (b *SessionsBrowser) getKeyWithRetry() (rune, keyboard.Key, error) {
	char, key, err := keyboard.GetKey()
	if err != nil {
		b.keyboardErrors++
		b.lastKeyboardError = time.Now()

		slog.Warn("Keyboard error occurred",
			"error", err,
			"errorCount", b.keyboardErrors,
			"maxRetries", b.maxRetries)

		// 如果错误次数还在可接受范围内，尝试重新初始化键盘
		if b.keyboardErrors <= b.maxRetries {
			slog.Info("Attempting to reinitialize keyboard", "attempt", b.keyboardErrors)

			// 关闭并重新打开键盘
			b.closeKeyboard()
			time.Sleep(500 * time.Millisecond) // 等待一下

			if reinitErr := b.initializeKeyboard(); reinitErr != nil {
				slog.Error("Failed to reinitialize keyboard", "error", reinitErr)
				return 0, 0, fmt.Errorf("keyboard reinitialization failed: %w", reinitErr)
			}

			// 重新尝试获取键盘输入
			return keyboard.GetKey()
		}

		return 0, 0, err
	}

	// 成功获取输入，重置错误计数
	if b.keyboardErrors > 0 {
		slog.Info("Keyboard recovered", "previousErrors", b.keyboardErrors)
		b.keyboardErrors = 0
	}

	return char, key, nil
}

// requestExit 请求安全退出
func (b *SessionsBrowser) requestExit() {
	slog.Info("Exit requested by user")
	b.exitRequested = true
}

// enterConfirmationMode 安全进入确认模式
func (b *SessionsBrowser) enterConfirmationMode() {
	if b.confirmationMode {
		slog.Warn("Already in confirmation mode")
		return
	}
	b.confirmationMode = true
	slog.Info("Entered confirmation mode")
}

// exitConfirmationMode 安全退出确认模式
func (b *SessionsBrowser) exitConfirmationMode() {
	if !b.confirmationMode {
		slog.Warn("Not in confirmation mode")
		return
	}
	b.confirmationMode = false
	slog.Info("Exited confirmation mode")
}

// getSystemStatus 获取系统状态信息
func (b *SessionsBrowser) getSystemStatus() map[string]interface{} {
	return map[string]interface{}{
		"keyboardEnabled":    b.keyboardEnabled,
		"confirmationMode":   b.confirmationMode,
		"keyboardErrors":     b.keyboardErrors,
		"lastKeyboardError":  b.lastKeyboardError,
		"exitRequested":      b.exitRequested,
		"selectedIndex":      b.selectedIndex,
		"sessionsCount":      len(b.sessions),
		"maxRetries":         b.maxRetries,
	}
}

// isStdinTerminal checks if stdin is a terminal
func isStdinTerminal() bool {
	if os.Getenv("OPENCODE_FORCE_TERMINAL") == "true" {
		return true
	}

	// Check if stdin is a terminal
	stat, err := os.Stdin.Stat()
	if err != nil {
		return false
	}

	// Check if it's a character device (terminal)
	return (stat.Mode() & os.ModeCharDevice) != 0
}

// isTmuxEnvironment checks if we're running inside tmux
func isTmuxEnvironment() bool {
	return os.Getenv("TMUX") != "" || os.Getenv("TMUX_PANE") != ""
}
