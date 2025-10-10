package types

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/sst/opencode-sdk-go"
)

// StateVersion provides optimistic concurrency control for state updates
type StateVersion struct {
	Version   int64     `json:"version"`
	Timestamp time.Time `json:"timestamp"`
	Source    string    `json:"source"` // panel identifier
}

// SessionInfo represents session data shared across panels
type SessionInfo struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	MessageCount int      `json:"message_count"`
	IsActive    bool      `json:"is_active"`
}

// MessageInfo represents message data for cross-panel synchronization
type MessageInfo struct {
	ID        string                `json:"id"`
	SessionID string               `json:"session_id"`
	Type      string               `json:"type"` // "user", "assistant", "system"
	Content   string               `json:"content"`
	Timestamp time.Time            `json:"timestamp"`
	Status    string               `json:"status"` // "pending", "completed", "error"
	Parts     []opencode.PartUnion `json:"parts,omitempty"`
}

// InputState represents the current input panel state
type InputState struct {
	Buffer         string `json:"buffer"`
	CursorPosition int    `json:"cursor_position"`
	SelectionStart int    `json:"selection_start"`
	SelectionEnd   int    `json:"selection_end"`
	Mode           string `json:"mode"` // "normal", "command", "multiline"
	History        []string `json:"history"`
	HistoryIndex   int    `json:"history_index"`
}

// SharedApplicationState represents the global state shared across all panels
type SharedApplicationState struct {
	// Version control for optimistic locking
	Version StateVersion `json:"version"`

	// Session management state
	Sessions         []SessionInfo `json:"sessions"`
	CurrentSessionID string        `json:"current_session_id"`

	// Message state
	Messages       []MessageInfo `json:"messages"`
	CurrentMessage *MessageInfo  `json:"current_message,omitempty"`

	// Input state
	Input InputState `json:"input"`

	// Application state
	Theme           string            `json:"theme"`
	Provider        string            `json:"provider"`
	Model           string            `json:"model"`
	Agent           string            `json:"agent"`
	AgentModel      map[string]string `json:"agent_model"`

	// Synchronization metadata
	LastUpdate   time.Time `json:"last_update"`
	UpdateCount  int64     `json:"update_count"`

	// Runtime synchronization primitives (not serialized)
	mutex        sync.RWMutex `json:"-"`
	subscribers  map[string]chan StateEvent `json:"-"`
	subMutex     sync.RWMutex `json:"-"`
}

// NewSharedApplicationState creates a new shared state with default values
func NewSharedApplicationState() *SharedApplicationState {
	return &SharedApplicationState{
		Version: StateVersion{
			Version:   1,
			Timestamp: time.Now(),
			Source:    "init",
		},
		Sessions:         make([]SessionInfo, 0),
		CurrentSessionID: "",
		Messages:         make([]MessageInfo, 0),
		Input: InputState{
			Buffer:         "",
			CursorPosition: 0,
			Mode:           "normal",
			History:        make([]string, 0),
			HistoryIndex:   -1,
		},
		Theme:           "opencode",
		AgentModel:      make(map[string]string),
		LastUpdate:      time.Now(),
		UpdateCount:     0,
		subscribers:     make(map[string]chan StateEvent),
	}
}

// GetCurrentVersion returns the current state version (thread-safe)
func (s *SharedApplicationState) GetCurrentVersion() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.Version.Version
}

// GetCurrentSessionID returns the current session ID (thread-safe)
func (s *SharedApplicationState) GetCurrentSessionID() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.CurrentSessionID
}

// GetSessions returns a copy of all sessions (thread-safe)
func (s *SharedApplicationState) GetSessions() []SessionInfo {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	sessions := make([]SessionInfo, len(s.Sessions))
	copy(sessions, s.Sessions)
	return sessions
}

// GetMessages returns a copy of messages for the current session (thread-safe)
func (s *SharedApplicationState) GetMessages() []MessageInfo {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	messages := make([]MessageInfo, 0)
	for _, msg := range s.Messages {
		if msg.SessionID == s.CurrentSessionID {
			messages = append(messages, msg)
		}
	}
	return messages
}

// GetInputState returns a copy of the input state (thread-safe)
func (s *SharedApplicationState) GetInputState() InputState {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Create a deep copy of the input state
	history := make([]string, len(s.Input.History))
	copy(history, s.Input.History)

	return InputState{
		Buffer:         s.Input.Buffer,
		CursorPosition: s.Input.CursorPosition,
		SelectionStart: s.Input.SelectionStart,
		SelectionEnd:   s.Input.SelectionEnd,
		Mode:           s.Input.Mode,
		History:        history,
		HistoryIndex:   s.Input.HistoryIndex,
	}
}

// Clone creates a deep copy of the shared state for serialization
func (s *SharedApplicationState) Clone() *SharedApplicationState {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	clone := &SharedApplicationState{
		Version:          s.Version,
		CurrentSessionID: s.CurrentSessionID,
		Theme:           s.Theme,
		Provider:        s.Provider,
		Model:           s.Model,
		Agent:           s.Agent,
		LastUpdate:      s.LastUpdate,
		UpdateCount:     s.UpdateCount,
	}

	// Deep copy sessions
	clone.Sessions = make([]SessionInfo, len(s.Sessions))
	copy(clone.Sessions, s.Sessions)

	// Deep copy messages
	clone.Messages = make([]MessageInfo, len(s.Messages))
	copy(clone.Messages, s.Messages)

	// Deep copy current message if exists
	if s.CurrentMessage != nil {
		msg := *s.CurrentMessage
		clone.CurrentMessage = &msg
	}

	// Deep copy input state
	clone.Input = InputState{
		Buffer:         s.Input.Buffer,
		CursorPosition: s.Input.CursorPosition,
		SelectionStart: s.Input.SelectionStart,
		SelectionEnd:   s.Input.SelectionEnd,
		Mode:           s.Input.Mode,
		HistoryIndex:   s.Input.HistoryIndex,
	}
	clone.Input.History = make([]string, len(s.Input.History))
	copy(clone.Input.History, s.Input.History)

	// Deep copy agent model map
	clone.AgentModel = make(map[string]string)
	for k, v := range s.AgentModel {
		clone.AgentModel[k] = v
	}

	// Initialize runtime fields
	clone.subscribers = make(map[string]chan StateEvent)

	return clone
}

// MarshalJSON customizes JSON serialization to exclude runtime fields
func (s *SharedApplicationState) MarshalJSON() ([]byte, error) {
	// Create a clone without runtime fields for serialization
	clone := s.Clone()

	// Use an anonymous struct to avoid infinite recursion
	type Alias SharedApplicationState
	return json.Marshal((*Alias)(clone))
}

// StateEvent represents a state change notification
type StateEvent struct {
	ID          string         `json:"id"`
	Type        StateEventType `json:"type"`
	Data        interface{}    `json:"data"`
	Version     int64         `json:"version"`
	SourcePanel string        `json:"source_panel"`
	Timestamp   time.Time     `json:"timestamp"`
}

// StateEventType defines the different types of state change events
type StateEventType string

const (
	EventSessionChanged  StateEventType = "session_changed"
	EventSessionAdded    StateEventType = "session_added"
	EventSessionDeleted  StateEventType = "session_deleted"
	EventSessionUpdated  StateEventType = "session_updated"
	EventMessageAdded    StateEventType = "message_added"
	EventMessageUpdated  StateEventType = "message_updated"
	EventMessageDeleted  StateEventType = "message_deleted"
	EventInputUpdated    StateEventType = "input_updated"
	EventCursorMoved     StateEventType = "cursor_moved"
	EventThemeChanged    StateEventType = "theme_changed"
	EventModelChanged    StateEventType = "model_changed"
	EventAgentChanged    StateEventType = "agent_changed"
	EventStateSync       StateEventType = "state_sync"
	EventPanelConnected  StateEventType = "panel_connected"
	EventPanelDisconnected StateEventType = "panel_disconnected"
)