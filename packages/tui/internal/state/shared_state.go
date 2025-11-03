package state

import (
	"github.com/sst/opencode/internal/types"
)

// Re-export types for backward compatibility
type SharedApplicationState = types.SharedApplicationState
type StateVersion = types.StateVersion
type SessionInfo = types.SessionInfo
type MessageInfo = types.MessageInfo
type InputState = types.InputState
type StateEvent = types.StateEvent
type StateEventType = types.StateEventType

// Re-export constructor
func NewSharedApplicationState() *SharedApplicationState {
	return types.NewSharedApplicationState()
}

// Re-export constants
const (
	EventSessionChanged    = types.EventSessionChanged
	EventSessionAdded      = types.EventSessionAdded
	EventSessionDeleted    = types.EventSessionDeleted
	EventSessionUpdated    = types.EventSessionUpdated
	EventMessageAdded      = types.EventMessageAdded
	EventMessageUpdated    = types.EventMessageUpdated
	EventMessageDeleted    = types.EventMessageDeleted
	EventMessagesCleared   = types.EventMessagesCleared
	EventInputUpdated      = types.EventInputUpdated
	EventCursorMoved       = types.EventCursorMoved
	EventThemeChanged      = types.EventThemeChanged
	EventModelChanged      = types.EventModelChanged
	EventAgentChanged      = types.EventAgentChanged
	EventUIActionTriggered = types.EventUIActionTriggered
	EventStateSync         = types.EventStateSync
	EventPanelConnected    = types.EventPanelConnected
	EventPanelDisconnected = types.EventPanelDisconnected
)
