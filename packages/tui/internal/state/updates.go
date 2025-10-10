package state

import (
	"github.com/sst/opencode/internal/types"
)

// Re-export types for backward compatibility
type UpdateType = types.UpdateType
type StateUpdate = types.StateUpdate

// Re-export payload types
type SessionChangePayload = types.SessionChangePayload
type SessionAddPayload = types.SessionAddPayload
type SessionUpdatePayload = types.SessionUpdatePayload
type SessionDeletePayload = types.SessionDeletePayload
type MessageAddPayload = types.MessageAddPayload
type MessageUpdatePayload = types.MessageUpdatePayload
type MessageDeletePayload = types.MessageDeletePayload
type InputUpdatePayload = types.InputUpdatePayload
type CursorMovePayload = types.CursorMovePayload
type ThemeChangePayload = types.ThemeChangePayload
type ModelChangePayload = types.ModelChangePayload
type AgentChangePayload = types.AgentChangePayload

// Re-export constants
const (
	SessionChanged = types.SessionChanged
	SessionAdded   = types.SessionAdded
	SessionDeleted = types.SessionDeleted
	SessionUpdated = types.SessionUpdated
	MessageAdded   = types.MessageAdded
	MessageUpdated = types.MessageUpdated
	MessageDeleted = types.MessageDeleted
	InputUpdated   = types.InputUpdated
	CursorMoved    = types.CursorMoved
	ThemeChanged   = types.ThemeChanged
	ModelChanged   = types.ModelChanged
	AgentChanged   = types.AgentChanged
)