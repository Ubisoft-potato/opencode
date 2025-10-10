package ipc

import "time"

// EventType represents the type of IPC event
type EventType string

const (
	// Session events
	EventSessionChanged EventType = "session_changed"
	EventSessionCreated EventType = "session_created"
	EventSessionDeleted EventType = "session_deleted"

	// Message events
	EventMessageSent     EventType = "message_sent"
	EventMessageReceived EventType = "message_received"

	// Focus events
	EventPaneFocused   EventType = "pane_focused"
	EventPaneBlurred   EventType = "pane_blurred"

	// Coordination events
	EventRefreshAll EventType = "refresh_all"
	EventShutdown   EventType = "shutdown"
)

// Event represents an IPC event between panes
type Event struct {
	Type      EventType   `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Source    string      `json:"source"`
	Data      interface{} `json:"data,omitempty"`
}

// SessionChangedData contains data for session change events
type SessionChangedData struct {
	SessionID string `json:"session_id"`
	Title     string `json:"title"`
}

// MessageData contains data for message events
type MessageData struct {
	SessionID string `json:"session_id"`
	MessageID string `json:"message_id"`
	Text      string `json:"text"`
}

// PaneFocusData contains data for pane focus events
type PaneFocusData struct {
	PaneID string `json:"pane_id"`
}

// NewEvent creates a new IPC event
func NewEvent(eventType EventType, source string, data interface{}) *Event {
	return &Event{
		Type:      eventType,
		Timestamp: time.Now(),
		Source:    source,
		Data:      data,
	}
}