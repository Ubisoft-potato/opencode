package types

import (
	"time"

	"github.com/sst/opencode-sdk-go"
)

// UpdateType defines the different types of state updates
type UpdateType string

const (
	SessionChanged    UpdateType = "session_changed"
	SessionAdded      UpdateType = "session_added"
	SessionDeleted    UpdateType = "session_deleted"
	SessionUpdated    UpdateType = "session_updated"
	MessageAdded      UpdateType = "message_added"
	MessageUpdated    UpdateType = "message_updated"
	MessageDeleted    UpdateType = "message_deleted"
	MessagesCleared   UpdateType = "messages_cleared"
	InputUpdated      UpdateType = "input_updated"
	CursorMoved       UpdateType = "cursor_moved"
	ThemeChanged      UpdateType = "theme_changed"
	ModelChanged      UpdateType = "model_changed"
	AgentChanged      UpdateType = "agent_changed"
	UIActionTriggered UpdateType = "ui_action_triggered"
)

// StateUpdate represents an atomic state change operation
type StateUpdate struct {
	ID              string      `json:"id"`
	Type            UpdateType  `json:"type"`
	ExpectedVersion int64       `json:"expected_version"`
	Payload         interface{} `json:"payload"`
	SourcePanel     string      `json:"source_panel"`
	Timestamp       time.Time   `json:"timestamp"`
}

// Update payload structures for different types of updates

// SessionChangePayload represents a session selection change
type SessionChangePayload struct {
	SessionID string `json:"session_id"`
}

// SessionAddPayload represents adding a new session
type SessionAddPayload struct {
	Session SessionInfo `json:"session"`
}

// SessionUpdatePayload represents updating session metadata
type SessionUpdatePayload struct {
	SessionID string `json:"session_id"`
	Title     string `json:"title,omitempty"`
	IsActive  bool   `json:"is_active,omitempty"`
}

// SessionDeletePayload represents deleting a session
type SessionDeletePayload struct {
	SessionID string `json:"session_id"`
}

// MessageAddPayload represents adding a new message
type MessageAddPayload struct {
	Message MessageInfo `json:"message"`
}

// MessageUpdatePayload represents updating a message
type MessageUpdatePayload struct {
	MessageID string               `json:"message_id"`
	Content   string               `json:"content,omitempty"`
	Status    string               `json:"status,omitempty"`
	Parts     []opencode.PartUnion `json:"parts,omitempty"`
}

// MessageDeletePayload represents deleting a message
type MessageDeletePayload struct {
	MessageID string `json:"message_id"`
}

// MessagesClearPayload represents clearing all messages in a session
type MessagesClearPayload struct {
	SessionID string `json:"session_id"`
}

// InputUpdatePayload represents input buffer changes
type InputUpdatePayload struct {
	Buffer         string `json:"buffer,omitempty"`
	CursorPosition int    `json:"cursor_position"`
	SelectionStart int    `json:"selection_start"`
	SelectionEnd   int    `json:"selection_end"`
	Mode           string `json:"mode,omitempty"`
}

// CursorMovePayload represents cursor position changes
type CursorMovePayload struct {
	Position       int `json:"position"`
	SelectionStart int `json:"selection_start"`
	SelectionEnd   int `json:"selection_end"`
}

// ThemeChangePayload represents theme changes
type ThemeChangePayload struct {
	Theme string `json:"theme"`
}

// ModelChangePayload represents model selection changes
type ModelChangePayload struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

// AgentChangePayload represents agent selection changes
type AgentChangePayload struct {
	Agent string `json:"agent"`
}

// UIActionPayload represents UI action triggers
type UIActionPayload struct {
	Action string                 `json:"action"`
	Data   map[string]interface{} `json:"data,omitempty"`
}

// Event payload structures

// PanelConnectionPayload represents panel connection/disconnection events
type PanelConnectionPayload struct {
	PanelID   string `json:"panel_id"`
	PanelType string `json:"panel_type"`
}

// StateSyncPayload represents full state synchronization events
type StateSyncPayload struct {
	State *SharedApplicationState `json:"state"`
}
