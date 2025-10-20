package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea/v2"
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/charmbracelet/lipgloss/v2/compat"
	"github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
	"github.com/sst/opencode/internal/ipc"
	"github.com/sst/opencode/internal/state"
	"github.com/sst/opencode/internal/types"
	"github.com/sst/opencode/internal/styles"
	"github.com/sst/opencode/internal/theme"
	"github.com/sst/opencode/internal/util"
)

// RenderedLine represents a single rendered line with metadata
type RenderedLine struct {
	Content     string    `json:"content"`
	MessageID   string    `json:"message_id"`
	MessageType string    `json:"message_type"`
	LineIndex   int       `json:"line_index"`
	IsFirstLine bool      `json:"is_first_line"`
	IsLastLine  bool      `json:"is_last_line"`
}

// MessageRenderCache caches rendered message content
type MessageRenderCache struct {
	ContentHash   string         `json:"content_hash"`
	RenderedLines []RenderedLine `json:"rendered_lines"`
	Height        int            `json:"height"`
	Width         int            `json:"width"`
	Mode          string         `json:"mode"` // "markdown" or "plain"
	CreatedAt     time.Time      `json:"created_at"`
}

// SessionViewState stores view state for each session
type SessionViewState struct {
	ScrollOffset        int       `json:"scroll_offset"`
	AutoScroll          bool      `json:"auto_scroll"`
	LastViewTime        time.Time `json:"last_view_time"`
	LastViewedMessageID string    `json:"last_viewed_message_id"`
	TotalLines          int       `json:"total_lines"`
}

// LineBasedRenderer manages line-based rendering and caching
type LineBasedRenderer struct {
	renderedLines   []RenderedLine                 `json:"rendered_lines"`
	lineToMessage   map[int]string                 `json:"line_to_message"`
	totalLines      int                            `json:"total_lines"`
	renderCache     map[string]*MessageRenderCache `json:"render_cache"`
	sessionStates   map[string]*SessionViewState   `json:"session_states"`
	lastRenderWidth int                            `json:"last_render_width"`
	cacheHits       int64                          `json:"cache_hits"`
	cacheMisses     int64                          `json:"cache_misses"`
}

// MessagesPanel manages the message history panel
type MessagesPanel struct {
	client           *opencode.Client
	ipcClient        *ipc.SocketClient
	messages         []types.MessageInfo
	currentSessionID string
	scrollOffset     int
	width            int
	height           int
	ctx              context.Context
	cancel           context.CancelFunc
	autoScroll       bool
	showTimestamps   bool
	isStreaming      bool
	currentMessage   *types.MessageInfo
	version          int64
	eventsChan       chan state.StateEvent
	markdownMode     bool // true for markdown rendering, false for plain text
	lineRenderer     *LineBasedRenderer
}

// NewMessagesPanel creates a new messages panel
func NewMessagesPanel(httpClient *opencode.Client, socketPath string) *MessagesPanel {
	ctx, cancel := context.WithCancel(context.Background())

	panel := &MessagesPanel{
		client:         httpClient,
		ipcClient:      ipc.NewSocketClient(socketPath, "messages-panel", "messages"),
		messages:       make([]types.MessageInfo, 0),
		scrollOffset:   0,
		autoScroll:     true,
		showTimestamps: false,
		markdownMode:   true, // Default to markdown mode
		ctx:            ctx,
		cancel:         cancel,
		eventsChan:     make(chan state.StateEvent, 64),
		lineRenderer: &LineBasedRenderer{
			renderedLines:   make([]RenderedLine, 0),
			lineToMessage:   make(map[int]string),
			totalLines:      0,
			renderCache:     make(map[string]*MessageRenderCache),
			sessionStates:   make(map[string]*SessionViewState),
			lastRenderWidth: 0,
			cacheHits:       0,
			cacheMisses:     0,
		},
	}

    // Register event handlers (bridge IPC events into Bubble Tea loop)
    panel.ipcClient.RegisterEventHandler(state.EventMessageAdded, panel.forwardEventToUI)
    panel.ipcClient.RegisterEventHandler(state.EventMessageUpdated, panel.forwardEventToUI)
    panel.ipcClient.RegisterEventHandler(state.EventMessageDeleted, panel.forwardEventToUI)
    panel.ipcClient.RegisterEventHandler(state.EventSessionChanged, panel.forwardEventToUI)
    panel.ipcClient.RegisterEventHandler(state.EventStateSync, panel.forwardEventToUI)
    panel.ipcClient.RegisterEventHandler(types.EventThemeChanged, panel.forwardEventToUI)

    // Wildcard handler to log receipt of any event type for diagnostics
    panel.ipcClient.RegisterEventHandler(types.StateEventType("*"), panel.handleAnyEvent)

	return panel
}

// Init initializes the panel
func (p *MessagesPanel) Init() tea.Cmd {
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

    // Subscribe to IPC events bridged via eventsChan
    cmds = append(cmds, p.subscribeEvents())

	return tea.Batch(cmds...)
}

// Update handles messages and updates the panel state
func (p *MessagesPanel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		p.width = msg.Width
		p.height = msg.Height
		return p, nil

	case tea.KeyMsg:
		return p.handleKeyPress(msg)

	case ConnectedMsg:
		log.Printf("Messages panel connected to IPC")
		return p, nil

	case StateLoadedMsg:
		p.currentSessionID = msg.State.CurrentSessionID
		p.messages = p.filterMessagesForSession(msg.State.Messages, p.currentSessionID)
		if p.autoScroll {
			p.scrollToBottom()
		}
		return p, nil

	case ErrorMsg:
		log.Printf("Messages panel error: %v", msg.Error)
		return p, nil

    case MessageEventMsg:
        // Handle event and continue listening for more
        _, cmd := p.handleMessageEvent(msg.Event)
        return p, tea.Batch(cmd, p.subscribeEvents())

	case StreamingUpdateMsg:
		return p.handleStreamingUpdate(msg)

	default:
		return p, nil
	}
}

// View renders the messages panel
func (p *MessagesPanel) View() string {
	if len(p.messages) == 0 {
		return p.renderEmptyState()
	}

	return p.renderMessages()
}

// handleKeyPress processes keyboard input
func (p *MessagesPanel) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		return p, tea.Quit

	case "up", "k":
		if len(p.messages) == 0 {
			break
		}
		maxScroll := p.calculateMaxScroll()
		if maxScroll == 0 {
			// All lines fit, no scrolling needed
			break
		}

		if p.scrollOffset > 0 {
			p.scrollOffset = max(0, p.scrollOffset-1) // Single line scroll
			log.Printf("[MESSAGES] Scrolled up to offset %d", p.scrollOffset)
		}
		p.autoScroll = false

	case "down", "j":
		if len(p.messages) == 0 {
			break
		}
		maxScroll := p.calculateMaxScroll()
		if maxScroll == 0 {
			// All lines fit, no scrolling needed
			break
		}

		if p.scrollOffset < maxScroll {
			p.scrollOffset = min(maxScroll, p.scrollOffset+1) // Single line scroll
			log.Printf("[MESSAGES] Scrolled down to offset %d", p.scrollOffset)
		}

		// Re-enable auto scroll if at bottom
		if p.scrollOffset >= maxScroll {
			p.autoScroll = true
		}

	case "page_up":
		if len(p.messages) == 0 {
			break
		}
		maxScroll := p.calculateMaxScroll()
		if maxScroll == 0 {
			break
		}

		// Calculate available height for messages (excluding header and footer)
		availableHeight := p.height - 6
		if availableHeight <= 0 {
			break
		}

		// Page up by half the available height
		pageSize := max(1, availableHeight/2)
		p.scrollOffset = max(0, p.scrollOffset-pageSize)
		log.Printf("[MESSAGES] Page up to offset %d (page size=%d)", p.scrollOffset, pageSize)
		p.autoScroll = false

	case "page_down":
		if len(p.messages) == 0 {
			break
		}
		maxScroll := p.calculateMaxScroll()
		if maxScroll == 0 {
			break
		}

		// Calculate available height for messages (excluding header and footer)
		availableHeight := p.height - 6
		if availableHeight <= 0 {
			break
		}

		// Page down by half the available height
		pageSize := max(1, availableHeight/2)
		p.scrollOffset = min(maxScroll, p.scrollOffset+pageSize)
		log.Printf("[MESSAGES] Page down to offset %d (page size=%d)", p.scrollOffset, pageSize)

		if p.scrollOffset >= maxScroll {
			p.autoScroll = true
		}

	case "home":
		p.scrollOffset = 0
		p.autoScroll = false

	case "end":
		p.scrollToBottom()
		p.autoScroll = true

	case "t":
		p.showTimestamps = !p.showTimestamps
		log.Printf("[MESSAGES] Timestamps %s", map[bool]string{true: "enabled", false: "disabled"}[p.showTimestamps])

		// Rebuild rendered lines with timestamp change
		if len(p.messages) > 0 {
			mode := "plain"
			if p.markdownMode {
				mode = "markdown"
			}
			p.lineRenderer.rebuildRenderedLines(p.messages, p.width, mode, p.showTimestamps)

			// Maintain scroll position after timestamp toggle
			maxScroll := p.calculateMaxScroll()
			if p.scrollOffset > maxScroll {
				p.scrollOffset = maxScroll
			}
			log.Printf("[MESSAGES] Rebuilt lines for timestamp toggle, adjusted scroll to %d", p.scrollOffset)
		}

	case "a":
		p.autoScroll = !p.autoScroll
		if p.autoScroll {
			p.scrollToBottom()
		}

	case "r":
		return p, p.refreshMessages()

	case "m":
		p.markdownMode = !p.markdownMode
		log.Printf("[MESSAGES] Switched to %s mode", map[bool]string{true: "markdown", false: "plain"}[p.markdownMode])

		// Rebuild rendered lines with new mode
		if len(p.messages) > 0 {
			mode := "plain"
			if p.markdownMode {
				mode = "markdown"
			}
			p.lineRenderer.rebuildRenderedLines(p.messages, p.width, mode, p.showTimestamps)

			// Maintain scroll position after mode switch
			maxScroll := p.calculateMaxScroll()
			if p.scrollOffset > maxScroll {
				p.scrollOffset = maxScroll
			}
			log.Printf("[MESSAGES] Rebuilt lines for mode switch, adjusted scroll to %d", p.scrollOffset)
		}
	}

	return p, nil
}

// refreshMessages refreshes messages from the API
func (p *MessagesPanel) refreshMessages() tea.Cmd {
	if p.currentSessionID == "" {
		return nil
	}

	return func() tea.Msg {
		messages, err := p.client.Session.Messages(p.ctx, p.currentSessionID, opencode.SessionMessagesParams{})
		if err != nil {
			return ErrorMsg{Error: fmt.Errorf("failed to refresh messages: %w", err)}
		}

		// Convert to state format
		messageInfos := make([]types.MessageInfo, 0)
		for _, message := range *messages {
			var messageType string
			var content string

			switch msg := message.Info.AsUnion().(type) {
			case opencode.UserMessage:
				messageType = "user"
				var contentParts []string
				for _, part := range message.Parts {
					if textPart, ok := part.AsUnion().(opencode.TextPart); ok {
						contentParts = append(contentParts, textPart.Text)
					}
				}
				content = strings.Join(contentParts, "\n")
			case opencode.AssistantMessage:
				messageType = "assistant"
				// Extract content from parts if available
				if len(message.Parts) > 0 {
					var contentParts []string
					for _, part := range message.Parts {
						if textPart, ok := part.AsUnion().(opencode.TextPart); ok {
							contentParts = append(contentParts, textPart.Text)
						}
					}
					content = strings.Join(contentParts, "\n")
				}
			default:
				messageType = "system"
				content = fmt.Sprintf("Unknown message type: %T", msg)
			}

			messageInfo := types.MessageInfo{
				ID:        message.Info.ID,
				SessionID: p.currentSessionID,
				Type:      messageType,
				Content:   content,
				Timestamp: time.Now(), // You might want to extract actual timestamp
				Status:    "completed",
			}

			messageInfos = append(messageInfos, messageInfo)
		}

		return MessagesRefreshedMsg{Messages: messageInfos}
	}
}

// startEventStream starts listening for streaming events
func (p *MessagesPanel) startEventStream() tea.Cmd {
	return func() tea.Msg {
		// This would normally set up streaming from the OpenCode API
		// For now, we'll just return a placeholder
		return StreamingStartedMsg{}
	}
}

// Event handlers

func (p *MessagesPanel) handleMessageAdded(event state.StateEvent) error {
    p.version = event.Version
    if payloadMap, ok := event.Data.(map[string]interface{}); ok {
        var payload types.MessageAddPayload
        if err := decodePayload(payloadMap, &payload); err == nil {
            if payload.Message.SessionID == p.currentSessionID {
                p.messages = append(p.messages, payload.Message)

                // Rebuild rendered lines with the new message
                mode := "plain"
                if p.markdownMode {
                    mode = "markdown"
                }
                p.lineRenderer.rebuildRenderedLines(p.messages, p.width, mode, p.showTimestamps)

                // Auto-scroll to bottom if enabled
                if p.autoScroll {
                    p.scrollToBottom()
                    log.Printf("[MESSAGES] Auto-scrolled to bottom for new message")
                }

                // Clean up cache periodically
                p.lineRenderer.cleanupCache()
            }
            log.Printf("[MESSAGES] v%v Message added: %s (session=%s)", event.Version, payload.Message.ID, payload.Message.SessionID)
        }
    }
    return nil
}

func (p *MessagesPanel) handleMessageUpdated(event state.StateEvent) error {
    p.version = event.Version
    if payloadMap, ok := event.Data.(map[string]interface{}); ok {
        var payload types.MessageUpdatePayload
        if err := decodePayload(payloadMap, &payload); err == nil {
            messageUpdated := false
            for i, message := range p.messages {
                if message.ID == payload.MessageID {
                    if payload.Content != "" {
                        p.messages[i].Content = payload.Content
                        messageUpdated = true
                    }
                    if payload.Status != "" {
                        p.messages[i].Status = payload.Status
                        messageUpdated = true
                    }
                    p.messages[i].Timestamp = time.Now()
                    break
                }
            }

            // Rebuild rendered lines if message content changed
            if messageUpdated {
                mode := "plain"
                if p.markdownMode {
                    mode = "markdown"
                }
                p.lineRenderer.rebuildRenderedLines(p.messages, p.width, mode, p.showTimestamps)

                // Auto-scroll to bottom if enabled and this is a streaming update
                if p.autoScroll && payload.Status == "pending" {
                    p.scrollToBottom()
                    log.Printf("[MESSAGES] Auto-scrolled for streaming update")
                }
            }

            log.Printf("[MESSAGES] v%v Message updated: %s (content changed: %t)", event.Version, payload.MessageID, messageUpdated)
        }
    }
    return nil
}

func (p *MessagesPanel) handleMessageDeleted(event state.StateEvent) error {
    p.version = event.Version
    if payloadMap, ok := event.Data.(map[string]interface{}); ok {
        var payload types.MessageDeletePayload
        if err := decodePayload(payloadMap, &payload); err == nil {
            for i, message := range p.messages {
                if message.ID == payload.MessageID {
                    p.messages = append(p.messages[:i], p.messages[i+1:]...)
                    break
                }
            }
            log.Printf("[MESSAGES] v%v Message deleted: %s", event.Version, payload.MessageID)
        }
    }
    return nil
}

func (p *MessagesPanel) handleSessionChanged(event state.StateEvent) error {
    p.version = event.Version
    if payloadMap, ok := event.Data.(map[string]interface{}); ok {
        var payload types.SessionChangePayload
        if err := decodePayload(payloadMap, &payload); err == nil {
            log.Printf("[MESSAGES] v%v Session changed from %s to: %s", event.Version, p.currentSessionID, payload.SessionID)

            // Save current session state before switching
            if p.currentSessionID != "" && len(p.messages) > 0 {
                lastMessageID := ""
                if len(p.messages) > 0 {
                    lastMessageID = p.messages[len(p.messages)-1].ID
                }
                p.lineRenderer.saveSessionState(p.currentSessionID, p.scrollOffset, p.autoScroll, lastMessageID)
                log.Printf("[MESSAGES] Saved state for session %s", p.currentSessionID)
            }

            // Switch to new session
            oldSessionID := p.currentSessionID
            p.currentSessionID = payload.SessionID

            // Try to restore state for the new session
            sessionState := p.lineRenderer.getSessionState(p.currentSessionID)
            if sessionState != nil && oldSessionID != p.currentSessionID {
                log.Printf("[MESSAGES] Restoring state for session %s: offset=%d, autoScroll=%t",
                    p.currentSessionID, sessionState.ScrollOffset, sessionState.AutoScroll)
            }

            // Fetch messages for the new session
            messages, err := p.client.Session.Messages(p.ctx, p.currentSessionID, opencode.SessionMessagesParams{})
            if err != nil {
                log.Printf("[MESSAGES] Error fetching messages for session %s: %v", p.currentSessionID, err)
                p.messages = make([]types.MessageInfo, 0) // Clear messages on error
                // Reset to default state
                p.scrollOffset = 0
                p.autoScroll = true
                return nil
            }

            // Convert to state format
            messageInfos := make([]types.MessageInfo, 0)
            if messages != nil {
                for _, message := range *messages {
                    var messageType string
                    var content string

                    switch msg := message.Info.AsUnion().(type) {
                    case opencode.UserMessage:
                        messageType = "user"
                        var contentParts []string
                        for _, part := range message.Parts {
                            if textPart, ok := part.AsUnion().(opencode.TextPart); ok {
                                contentParts = append(contentParts, textPart.Text)
                            }
                        }
                        content = strings.Join(contentParts, "\n")
                    case opencode.AssistantMessage:
                        messageType = "assistant"
                        if len(message.Parts) > 0 {
                            var contentParts []string
                            for _, part := range message.Parts {
                                if textPart, ok := part.AsUnion().(opencode.TextPart); ok {
                                    contentParts = append(contentParts, textPart.Text)
                                }
                            }
                            content = strings.Join(contentParts, "\n")
                        }
                    default:
                        messageType = "system"
                        content = fmt.Sprintf("Unknown message type: %T", msg)
                    }

                    messageInfo := types.MessageInfo{
                        ID:        message.Info.ID,
                        SessionID: p.currentSessionID,
                        Type:      messageType,
                        Content:   content,
                        Timestamp: time.Now(), // Use time.Now() for safety, like in refreshMessages
                        Status:    "completed",
                    }
                    messageInfos = append(messageInfos, messageInfo)
                }
            }

            log.Printf("[MESSAGES] v%v Fetched %d messages for session %s", event.Version, len(messageInfos), p.currentSessionID)
            p.messages = messageInfos

            // Rebuild rendered lines for the new messages
            mode := "plain"
            if p.markdownMode {
                mode = "markdown"
            }
            p.lineRenderer.rebuildRenderedLines(p.messages, p.width, mode, p.showTimestamps)

            // Restore session state or use defaults
            sessionState = p.lineRenderer.getSessionState(p.currentSessionID)
            if sessionState != nil && len(p.messages) > 0 {
                // Check if there are new messages since last view
                hasNewMessages := false
                if sessionState.LastViewedMessageID != "" && len(p.messages) > 0 {
                    lastMessage := p.messages[len(p.messages)-1]
                    if lastMessage.ID != sessionState.LastViewedMessageID {
                        hasNewMessages = true
                        log.Printf("[MESSAGES] New messages detected since last view")
                    }
                }

                // Restore scroll position and auto-scroll state
                p.autoScroll = sessionState.AutoScroll

                // If user was at bottom and there are new messages, stay at bottom
                if sessionState.AutoScroll && hasNewMessages {
                    p.scrollToBottom()
                    log.Printf("[MESSAGES] Auto-scrolled to bottom due to new messages")
                } else {
                    // Restore previous scroll position, but validate it
                    maxScroll := p.calculateMaxScroll()
                    p.scrollOffset = min(sessionState.ScrollOffset, maxScroll)
                    log.Printf("[MESSAGES] Restored scroll offset to %d (max=%d)", p.scrollOffset, maxScroll)
                }
            } else {
                // Default behavior for new sessions
                p.autoScroll = true
                p.scrollToBottom()
                log.Printf("[MESSAGES] Using default state for new session")
            }
        }
    }
    return nil
}

func (p *MessagesPanel) handleStateSync(event state.StateEvent) error {
    p.version = event.Version
    if payloadMap, ok := event.Data.(map[string]interface{}); ok {
        var payload types.StateSyncPayload
        if err := decodePayload(payloadMap, &payload); err == nil {
            p.currentSessionID = payload.State.CurrentSessionID
            p.messages = p.filterMessagesForSession(payload.State.Messages, p.currentSessionID)
            if p.autoScroll {
                p.scrollToBottom()
            }
            log.Printf("[MESSAGES] v%v State synchronized", event.Version)
        }
    }
    return nil
}

func (p *MessagesPanel) handleThemeChanged(event state.StateEvent) error {
	var payload types.ThemeChangePayload
	if err := decodePayload(event.Data.(map[string]interface{}), &payload); err != nil {
		log.Printf("[MESSAGES] Failed to decode theme change payload: %v", err)
		return err
	}
	
	log.Printf("[MESSAGES] Theme changed to: %s", payload.Theme)
	
	// Apply the theme change immediately
	if err := theme.SetTheme(payload.Theme); err != nil {
		log.Printf("[MESSAGES] Failed to set theme %s: %v", payload.Theme, err)
		return err
	}
	
	log.Printf("[MESSAGES] Successfully applied theme: %s", payload.Theme)
	return nil
}

// handleAnyEvent logs any received event for diagnostics
func (p *MessagesPanel) handleAnyEvent(event state.StateEvent) error {
    p.version = event.Version
    log.Printf("[MESSAGES] v%v Received event type: %s from %s", event.Version, event.Type, event.SourcePanel)
    return nil
}

// decodePayload converts a generic map payload into a concrete struct
func decodePayload[T any](data map[string]interface{}, out *T) error {
    b, err := json.Marshal(data)
    if err != nil {
        return fmt.Errorf("marshal payload: %w", err)
    }
    if err := json.Unmarshal(b, out); err != nil {
        return fmt.Errorf("unmarshal payload: %w", err)
    }
    return nil
}

func (p *MessagesPanel) handleMessageEvent(event state.StateEvent) (tea.Model, tea.Cmd) {
    switch event.Type {
    case state.EventMessageAdded:
        p.handleMessageAdded(event)
    case state.EventMessageUpdated:
        p.handleMessageUpdated(event)
    case state.EventMessageDeleted:
        p.handleMessageDeleted(event)
    case state.EventSessionChanged:
        p.handleSessionChanged(event)
    case state.EventStateSync:
        p.handleStateSync(event)
    case types.EventThemeChanged:
        p.handleThemeChanged(event)
    }
    return p, nil
}

// forwardEventToUI bridges IPC events into Bubble Tea by pushing into eventsChan
func (p *MessagesPanel) forwardEventToUI(event state.StateEvent) error {
    select {
    case p.eventsChan <- event:
    default:
        // Drop if channel is full to avoid blocking
        log.Printf("messages: events channel full, dropping event %s", event.Type)
    }
    return nil
}

// subscribeEvents returns a command that waits for the next IPC event and emits it as a MessageEventMsg
func (p *MessagesPanel) subscribeEvents() tea.Cmd {
    return func() tea.Msg {
        evt := <-p.eventsChan
        return MessageEventMsg{Event: evt}
    }
}

func (p *MessagesPanel) handleStreamingUpdate(msg StreamingUpdateMsg) (tea.Model, tea.Cmd) {
	if msg.MessageID != "" {
		// Update existing message with streaming content
		for i, message := range p.messages {
			if message.ID == msg.MessageID {
				p.messages[i].Content = msg.Content
				p.messages[i].Status = msg.Status
				break
			}
		}
	}
    return p, nil
}

// filterMessagesForSession filters messages for a specific session
func (p *MessagesPanel) filterMessagesForSession(messages []types.MessageInfo, sessionID string) []types.MessageInfo {
	filtered := make([]types.MessageInfo, 0)
	for _, message := range messages {
		if message.SessionID == sessionID {
			filtered = append(filtered, message)
		}
	}
	return filtered
}

// calculateMaxScroll calculates the maximum scroll offset using line-based calculation
func (p *MessagesPanel) calculateMaxScroll() int {
	// Calculate available height for messages (excluding header and footer)
	availableHeight := p.height - 6
	if availableHeight <= 0 {
		log.Printf("[MESSAGES] No available height for scroll calculation (height=%d)", p.height)
		return 0
	}

	// Use the line renderer to calculate max scroll offset
	maxScroll := p.lineRenderer.calculateMaxScrollOffset(availableHeight)

	log.Printf("[MESSAGES] Calculated max scroll: %d (total lines=%d, available height=%d)",
		maxScroll, p.lineRenderer.totalLines, availableHeight)

	return maxScroll
}

// scrollToBottom scrolls to the bottom of the messages
func (p *MessagesPanel) scrollToBottom() {
	p.scrollOffset = p.calculateMaxScroll()
}

// renderEmptyState renders the empty messages state
func (p *MessagesPanel) renderEmptyState() string {
	t := theme.CurrentTheme()
	style := styles.NewStyle().
		Foreground(t.TextMuted()).
		Align(styles.Center).
		Width(p.width).
		Height(p.height)

	if p.currentSessionID == "" {
		return style.Render("No session selected\n\nSelect a session to view messages")
	}

	return style.Render("No messages in this session\n\nStart a conversation in the input panel")
}

// renderMessages renders the list of messages using line-based rendering
func (p *MessagesPanel) renderMessages() string {
	t := theme.CurrentTheme()
	log.Printf("[MESSAGES] Starting renderMessages (width=%d, height=%d, offset=%d)", p.width, p.height, p.scrollOffset)

	var content string

	// Header
	header := "Messages"
	if p.currentSessionID != "" {
		header += fmt.Sprintf(" - Session %s", p.currentSessionID[:8])
	}
	if p.isStreaming {
		header += " [STREAMING]"
	}

	// Add cache stats to header in debug mode
	hits, misses, hitRate := p.lineRenderer.getCacheStats()
	if hits+misses > 0 {
		header += fmt.Sprintf(" [Cache: %.1f%%]", hitRate)
	}

	content += styles.NewStyle().
		Foreground(t.Primary()).
		Bold(true).
		Render(header) + "\n\n"

	// Calculate visible lines using line-based rendering
	visibleLines := p.calculateVisibleLines()

	log.Printf("[MESSAGES] Rendering %d visible lines", len(visibleLines))

	// Render visible lines directly
	for _, line := range visibleLines {
		// Apply styling based on message type
		var lineStyle styles.Style
		switch line.MessageType {
		case "user":
			lineStyle = styles.NewStyle().Foreground(t.Text())
		case "assistant":
			lineStyle = styles.NewStyle().Foreground(t.Text())
		case "system":
			lineStyle = styles.NewStyle().Foreground(t.TextMuted())
		default:
			lineStyle = styles.NewStyle().Foreground(t.Text())
		}

		content += lineStyle.Render(line.Content) + "\n"
	}

	// Footer with scroll indicator
	maxScroll := p.calculateMaxScroll()
	if maxScroll > 0 {
		currentLine := p.scrollOffset + 1
		totalLines := p.lineRenderer.totalLines
		availableHeight := p.height - 6
		endLine := min(currentLine+availableHeight-1, totalLines)

		var scrollIndicator string
		if availableHeight == 1 {
			scrollIndicator = fmt.Sprintf("[Line %d/%d]", currentLine, totalLines)
		} else {
			scrollIndicator = fmt.Sprintf("[Lines %d-%d/%d]", currentLine, endLine, totalLines)
		}

		if !p.autoScroll {
			scrollIndicator += " [MANUAL]"
		}

		content += "\n" + styles.NewStyle().
			Foreground(t.TextMuted()).
			Align(styles.Right).
			Width(p.width).
			Render(scrollIndicator)
	}

	// Help text
	modeText := "plain"
	if p.markdownMode {
		modeText = "markdown"
	}
	content += "\n" + styles.NewStyle().
		Foreground(t.TextMuted()).
		Render(fmt.Sprintf("↑/k up • ↓/j down • PgUp/PgDn page • Home/End • t timestamps • a auto-scroll • m mode(%s) • r refresh • q quit", modeText))

	log.Printf("[MESSAGES] Completed renderMessages")
	return content
}

// calculateVisibleLines returns lines visible in the current scroll view using line-based rendering
func (p *MessagesPanel) calculateVisibleLines() []RenderedLine {
	if len(p.messages) == 0 {
		log.Printf("[MESSAGES] No messages to display")
		return []RenderedLine{}
	}

	// Calculate available height for messages (excluding header and footer)
	// Account for: header (2 lines), scroll indicator (2 lines), help text (2 lines)
	availableHeight := p.height - 6
	if availableHeight <= 0 {
		log.Printf("[MESSAGES] No available height for messages (height=%d)", p.height)
		return []RenderedLine{}
	}

	// Check if we need to rebuild rendered lines (width change or first time)
	mode := "plain"
	if p.markdownMode {
		mode = "markdown"
	}

	if p.lineRenderer.lastRenderWidth != p.width || len(p.lineRenderer.renderedLines) == 0 {
		log.Printf("[MESSAGES] Rebuilding lines due to width change: %d -> %d", p.lineRenderer.lastRenderWidth, p.width)
		p.lineRenderer.rebuildRenderedLines(p.messages, p.width, mode, p.showTimestamps)
	}

	// Get visible lines using the line-based renderer
	visibleLines := p.lineRenderer.getVisibleLines(p.scrollOffset, availableHeight)

	log.Printf("[MESSAGES] Calculated %d visible lines (offset=%d, height=%d, total=%d)",
		len(visibleLines), p.scrollOffset, availableHeight, p.lineRenderer.totalLines)

	return visibleLines
}

// calculateMessageHeight calculates the actual height needed to render a message
func (p *MessagesPanel) calculateMessageHeight(message types.MessageInfo) int {
	// Start with base height for borders and padding
	height := 3 // Top border, bottom border, and padding
	
	var content string
	prefix := ""
	
	// Determine prefix based on message type
	switch message.Type {
	case "user":
		prefix = "🧑 You: "
	case "assistant":
		prefix = "🤖 Assistant: "
	case "system":
		prefix = "⚙️ System: "
	default:
		prefix = fmt.Sprintf("%s: ", message.Type)
	}
	
	// Calculate content with prefix
	if p.markdownMode {
		// For markdown mode, use util.ToMarkdown to get accurate line count
		backgroundColor := compat.AdaptiveColor{
			Light: lipgloss.NoColor{},
			Dark:  lipgloss.NoColor{},
		}
		renderedContent := util.ToMarkdown(message.Content, p.width-len(prefix)-4, backgroundColor)
		lines := strings.Split(renderedContent, "\n")
		height += len(lines)
	} else {
		// For plain text mode, calculate wrapped lines
		content = prefix + message.Content
		if p.width > 4 { // Account for padding and borders
			wrappedContent := p.wordWrap(content, p.width-4)
			lines := strings.Split(wrappedContent, "\n")
			height += len(lines)
		} else {
			height += 1 // At least one line
		}
	}
	
	// Add extra line for timestamp if enabled
	if p.showTimestamps {
		// Timestamp is added to first line, so no extra height needed
	}
	
	// Add extra space for status indicators
	if message.Status == "pending" || message.Status == "error" {
		// Status indicators are added to last line, so no extra height needed
	}
	
	return height
}

// renderMessage renders a single message
func (p *MessagesPanel) renderMessage(message types.MessageInfo) string {
	t := theme.CurrentTheme()

	var style styles.Style
	var prefix string

	switch message.Type {
	case "user":
		// User messages: border box with theme text color for proper visibility
		style = styles.NewStyle().
			Foreground(t.Text()).
			Padding(1, 2).
			MarginBottom(1).
			BorderStyle(styles.RoundedBorder).
			BorderTop(true).
			BorderBottom(true).
			BorderLeft(true).
			BorderRight(true).
			BorderForeground(t.Success())
		prefix = "🧑 You: "
	case "assistant":
		// Assistant messages: theme text color with no background
		style = styles.NewStyle().
			Foreground(t.Text()).
			Padding(1, 2).
			MarginBottom(1).
			BorderStyle(styles.RoundedBorder).
			BorderTop(true).
			BorderBottom(true).
			BorderLeft(true).
			BorderRight(true).
			BorderForeground(t.Info())
		prefix = "🤖 Assistant: "
	case "system":
		// System messages: default text color with no background
		style = styles.NewStyle().
			Padding(1, 2).
			MarginBottom(1).
			BorderStyle(styles.RoundedBorder).
			BorderTop(true).
			BorderBottom(true).
			BorderLeft(true).
			BorderRight(true).
			BorderForeground(t.Warning())
		prefix = "⚙️ System: "
	default:
		style = styles.NewStyle().
			Padding(1, 2).
			MarginBottom(1).
			BorderStyle(styles.RoundedBorder).
			BorderTop(true).
			BorderBottom(true).
			BorderLeft(true).
			BorderRight(true).
			BorderForeground(t.BorderSubtle())
		prefix = fmt.Sprintf("%s: ", message.Type)
	}

	var content string
	
	// Render content based on mode
	if p.markdownMode {
		// Use transparent background for code blocks
		backgroundColor := compat.AdaptiveColor{
			Light: lipgloss.NoColor{},
			Dark:  lipgloss.NoColor{},
		}
		
		// Render the message content as markdown
		renderedContent := util.ToMarkdown(message.Content, p.width-len(prefix)-4, backgroundColor)
		
		// Add prefix to each line of the rendered content
		lines := strings.Split(renderedContent, "\n")
		for i, line := range lines {
			if i == 0 {
				lines[i] = prefix + line
			} else {
				// Indent continuation lines to align with content
				lines[i] = strings.Repeat(" ", len(prefix)) + line
			}
		}
		content = strings.Join(lines, "\n")
	} else {
		// Plain text mode
		content = prefix + message.Content
		
		// Word wrap content to fit width
		if p.width > 0 {
			content = p.wordWrap(content, p.width-2)
		}
	}

	// Add timestamp if enabled
	if p.showTimestamps {
		timestamp := message.Timestamp.Format("15:04:05")
		if p.markdownMode {
			// For markdown mode, add timestamp to the first line only
			lines := strings.Split(content, "\n")
			if len(lines) > 0 {
				lines[0] = fmt.Sprintf("[%s] %s", timestamp, lines[0])
				content = strings.Join(lines, "\n")
			}
		} else {
			content = fmt.Sprintf("[%s] %s", timestamp, content)
		}
	}

	// Add status indicator for pending messages
	if message.Status == "pending" {
		if p.markdownMode {
			lines := strings.Split(content, "\n")
			if len(lines) > 0 {
				lines[len(lines)-1] += " ⏳"
				content = strings.Join(lines, "\n")
			}
		} else {
			content += " ⏳"
		}
	} else if message.Status == "error" {
		if p.markdownMode {
			lines := strings.Split(content, "\n")
			if len(lines) > 0 {
				lines[len(lines)-1] += " ❌"
				content = strings.Join(lines, "\n")
			}
		} else {
			content += " ❌"
		}
	}

	return style.Render(content)
}

// wordWrap wraps text to fit within specified width
func (p *MessagesPanel) wordWrap(text string, width int) string {
	if width <= 0 {
		return text
	}

	words := strings.Fields(text)
	if len(words) == 0 {
		return text
	}

	var lines []string
	var currentLine string

	for _, word := range words {
		if len(currentLine)+len(word)+1 <= width {
			if currentLine == "" {
				currentLine = word
			} else {
				currentLine += " " + word
			}
		} else {
			if currentLine != "" {
				lines = append(lines, currentLine)
			}
			currentLine = word
		}
	}

	if currentLine != "" {
		lines = append(lines, currentLine)
	}

	return strings.Join(lines, "\n")
}

// Message types
type ConnectedMsg struct{}

type StateLoadedMsg struct {
	State *state.SharedApplicationState
}

type ErrorMsg struct {
	Error error
}

type MessageEventMsg struct {
	Event state.StateEvent
}

type MessagesRefreshedMsg struct {
	Messages []types.MessageInfo
}

type StreamingStartedMsg struct{}

type StreamingUpdateMsg struct {
	MessageID string
	Content   string
	Status    string
}

// Utility functions
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// LineBasedRenderer methods

// generateContentHash creates a hash for caching purposes
func (lr *LineBasedRenderer) generateContentHash(message types.MessageInfo, width int, mode string, showTimestamps bool) string {
	content := fmt.Sprintf("%s|%s|%d|%s|%t|%s", message.ID, message.Content, width, mode, showTimestamps, message.Type)
	hash := md5.Sum([]byte(content))
	return hex.EncodeToString(hash[:])
}

// renderMessageToLines converts a message to a list of rendered lines
func (lr *LineBasedRenderer) renderMessageToLines(message types.MessageInfo, width int, mode string, showTimestamps bool) []RenderedLine {
	log.Printf("[RENDERER] Rendering message %s (type=%s, mode=%s, width=%d)", message.ID, message.Type, mode, width)

	contentHash := lr.generateContentHash(message, width, mode, showTimestamps)

	// Check cache first
	if cached, exists := lr.renderCache[contentHash]; exists {
		lr.cacheHits++
		log.Printf("[RENDERER] Cache hit for message %s (hash=%s)", message.ID, contentHash[:8])
		return cached.RenderedLines
	}

	lr.cacheMisses++
	log.Printf("[RENDERER] Cache miss for message %s (hash=%s)", message.ID, contentHash[:8])

	var lines []RenderedLine
	var renderedContent string

	// Determine prefix based on message type
	var prefix string
	switch message.Type {
	case "user":
		prefix = "🧑 You: "
	case "assistant":
		prefix = "🤖 Assistant: "
	case "system":
		prefix = "⚙️ System: "
	default:
		prefix = fmt.Sprintf("%s: ", message.Type)
	}

	if mode == "markdown" {
		// Use transparent background for code blocks
		backgroundColor := compat.AdaptiveColor{
			Light: lipgloss.NoColor{},
			Dark:  lipgloss.NoColor{},
		}

		// Render the message content as markdown
		renderedContent = util.ToMarkdown(message.Content, width-len(prefix)-4, backgroundColor)

		// Split into lines and add prefix
		contentLines := strings.Split(renderedContent, "\n")
		for i, line := range contentLines {
			var finalLine string
			if i == 0 {
				finalLine = prefix + line
			} else {
				// Indent continuation lines to align with content
				finalLine = strings.Repeat(" ", len(prefix)) + line
			}

			// Add timestamp if enabled and this is the first line
			if showTimestamps && i == 0 {
				timestamp := message.Timestamp.Format("15:04:05")
				finalLine = fmt.Sprintf("[%s] %s", timestamp, finalLine)
			}

			lines = append(lines, RenderedLine{
				Content:     finalLine,
				MessageID:   message.ID,
				MessageType: message.Type,
				LineIndex:   i,
				IsFirstLine: i == 0,
				IsLastLine:  i == len(contentLines)-1,
			})
		}
	} else {
		// Plain text mode
		content := prefix + message.Content

		// Add timestamp if enabled
		if showTimestamps {
			timestamp := message.Timestamp.Format("15:04:05")
			content = fmt.Sprintf("[%s] %s", timestamp, content)
		}

		// Word wrap content to fit width
		if width > 4 {
			content = lr.wordWrap(content, width-4)
		}

		contentLines := strings.Split(content, "\n")
		for i, line := range contentLines {
			lines = append(lines, RenderedLine{
				Content:     line,
				MessageID:   message.ID,
				MessageType: message.Type,
				LineIndex:   i,
				IsFirstLine: i == 0,
				IsLastLine:  i == len(contentLines)-1,
			})
		}
	}

	// Add status indicator for pending messages on the last line
	if len(lines) > 0 {
		lastIndex := len(lines) - 1
		if message.Status == "pending" {
			lines[lastIndex].Content += " ⏳"
		} else if message.Status == "error" {
			lines[lastIndex].Content += " ❌"
		}
	}

	// Cache the result
	lr.renderCache[contentHash] = &MessageRenderCache{
		ContentHash:   contentHash,
		RenderedLines: lines,
		Height:        len(lines),
		Width:         width,
		Mode:          mode,
		CreatedAt:     time.Now(),
	}

	log.Printf("[RENDERER] Rendered message %s into %d lines", message.ID, len(lines))
	return lines
}

// wordWrap wraps text to fit within specified width (helper method)
func (lr *LineBasedRenderer) wordWrap(text string, width int) string {
	if width <= 0 {
		return text
	}

	words := strings.Fields(text)
	if len(words) == 0 {
		return text
	}

	var lines []string
	var currentLine string

	for _, word := range words {
		if len(currentLine)+len(word)+1 <= width {
			if currentLine == "" {
				currentLine = word
			} else {
				currentLine += " " + word
			}
		} else {
			if currentLine != "" {
				lines = append(lines, currentLine)
			}
			currentLine = word
		}
	}

	if currentLine != "" {
		lines = append(lines, currentLine)
	}

	return strings.Join(lines, "\n")
}

// rebuildRenderedLines rebuilds the complete rendered lines list from messages
func (lr *LineBasedRenderer) rebuildRenderedLines(messages []types.MessageInfo, width int, mode string, showTimestamps bool) {
	log.Printf("[RENDERER] Rebuilding rendered lines for %d messages (width=%d, mode=%s)", len(messages), width, mode)

	lr.renderedLines = make([]RenderedLine, 0)
	lr.lineToMessage = make(map[int]string)
	lr.totalLines = 0

	lineIndex := 0
	for _, message := range messages {
		messageLines := lr.renderMessageToLines(message, width, mode, showTimestamps)

		// Update line indices and add to global list
		for i, line := range messageLines {
			line.LineIndex = lineIndex + i
			lr.renderedLines = append(lr.renderedLines, line)
			lr.lineToMessage[lineIndex+i] = message.ID
		}

		lineIndex += len(messageLines)
	}

	lr.totalLines = lineIndex
	lr.lastRenderWidth = width

	log.Printf("[RENDERER] Rebuilt %d total lines from %d messages", lr.totalLines, len(messages))
}

// getVisibleLines returns the lines that should be visible based on scroll offset and available height
func (lr *LineBasedRenderer) getVisibleLines(scrollOffset int, availableHeight int) []RenderedLine {
	if len(lr.renderedLines) == 0 || availableHeight <= 0 {
		return []RenderedLine{}
	}

	startLine := scrollOffset
	if startLine < 0 {
		startLine = 0
	}
	if startLine >= lr.totalLines {
		startLine = lr.totalLines - 1
	}

	endLine := startLine + availableHeight
	if endLine > lr.totalLines {
		endLine = lr.totalLines
	}

	log.Printf("[RENDERER] Getting visible lines: offset=%d, height=%d, start=%d, end=%d, total=%d",
		scrollOffset, availableHeight, startLine, endLine, lr.totalLines)

	if startLine >= endLine {
		return []RenderedLine{}
	}

	return lr.renderedLines[startLine:endLine]
}

// calculateMaxScrollOffset calculates the maximum scroll offset for the current content
func (lr *LineBasedRenderer) calculateMaxScrollOffset(availableHeight int) int {
	if lr.totalLines <= availableHeight {
		return 0
	}
	return lr.totalLines - availableHeight
}

// getSessionState gets or creates session view state
func (lr *LineBasedRenderer) getSessionState(sessionID string) *SessionViewState {
	if state, exists := lr.sessionStates[sessionID]; exists {
		return state
	}

	// Create new session state
	state := &SessionViewState{
		ScrollOffset:        0,
		AutoScroll:          true,
		LastViewTime:        time.Now(),
		LastViewedMessageID: "",
		TotalLines:          0,
	}
	lr.sessionStates[sessionID] = state

	log.Printf("[RENDERER] Created new session state for %s", sessionID)
	return state
}

// saveSessionState updates the session state
func (lr *LineBasedRenderer) saveSessionState(sessionID string, scrollOffset int, autoScroll bool, lastMessageID string) {
	state := lr.getSessionState(sessionID)
	state.ScrollOffset = scrollOffset
	state.AutoScroll = autoScroll
	state.LastViewTime = time.Now()
	state.LastViewedMessageID = lastMessageID
	state.TotalLines = lr.totalLines

	log.Printf("[RENDERER] Saved session state for %s: offset=%d, autoScroll=%t, lines=%d",
		sessionID, scrollOffset, autoScroll, lr.totalLines)
}

// cleanupCache removes old cache entries to prevent memory growth
func (lr *LineBasedRenderer) cleanupCache() {
	if len(lr.renderCache) < 100 {
		return
	}

	now := time.Now()
	maxAge := 10 * time.Minute

	for hash, cache := range lr.renderCache {
		if now.Sub(cache.CreatedAt) > maxAge {
			delete(lr.renderCache, hash)
		}
	}

	log.Printf("[RENDERER] Cleaned up cache, now %d entries", len(lr.renderCache))
}

// getCacheStats returns cache performance statistics
func (lr *LineBasedRenderer) getCacheStats() (hits int64, misses int64, hitRate float64) {
	total := lr.cacheHits + lr.cacheMisses
	if total == 0 {
		return lr.cacheHits, lr.cacheMisses, 0.0
	}
	hitRate = float64(lr.cacheHits) / float64(total) * 100.0
	return lr.cacheHits, lr.cacheMisses, hitRate
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
	logPath := filepath.Join(logDir, "messages.log")
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
	panel := NewMessagesPanel(httpClient, socketPath)

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
		log.Printf("Received signal, shutting down messages panel")
		cancel()
		program.Quit()
	}()

	// Run the program
	if _, err := program.Run(); err != nil {
		log.Printf("Messages panel error: %v", err)
		os.Exit(1)
	}

	// Cleanup
	panel.ipcClient.Disconnect()
	cancel()
}
