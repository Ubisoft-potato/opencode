package ipc

import (
	"encoding/json"
	"reflect"
	"time"
)

// IPCMessage represents a message exchanged between server and clients
type IPCMessage struct {
	Type      string      `json:"type"`
	RequestID string      `json:"request_id,omitempty"`
	Data      interface{} `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
}

// HandshakeMessage is sent by clients to initiate connection
type HandshakeMessage struct {
	Type      string    `json:"type"`      // Always "handshake"
	PanelID   string    `json:"panel_id"`  // Unique panel identifier
	PanelType string    `json:"panel_type"` // "sessions", "messages", "input"
	Version   string    `json:"version"`   // Protocol version
	Timestamp time.Time `json:"timestamp"`
}

// HandshakeResponse is sent by server in response to handshake
type HandshakeResponse struct {
	Type         string    `json:"type"`          // Always "handshake_response"
	Success      bool      `json:"success"`
	ConnectionID string    `json:"connection_id"` // Server-assigned connection ID
	ServerTime   time.Time `json:"server_time"`
	Error        string    `json:"error,omitempty"`
}

// Message type constants
const (
	MessageTypeHandshake         = "handshake"
	MessageTypeHandshakeResponse = "handshake_response"
	MessageTypeStateUpdate       = "state_update"
	MessageTypeStateUpdateResponse = "state_update_response"
	MessageTypeStateRequest      = "state_request"
	MessageTypeStateResponse     = "state_response"
	MessageTypeStateEvent        = "state_event"
	MessageTypePing              = "ping"
	MessageTypePong              = "pong"
	MessageTypeError             = "error"
	MessageTypeSubscribe         = "subscribe"
	MessageTypeUnsubscribe       = "unsubscribe"
	MessageTypeHeartbeat         = "heartbeat"
)

// SubscribeMessage allows clients to subscribe to specific event types
type SubscribeMessage struct {
	EventTypes []string `json:"event_types"` // List of event types to subscribe to
	PanelID    string   `json:"panel_id"`
}

// UnsubscribeMessage allows clients to unsubscribe from event types
type UnsubscribeMessage struct {
	EventTypes []string `json:"event_types"` // List of event types to unsubscribe from
	PanelID    string   `json:"panel_id"`
}

// HeartbeatMessage is used for connection health monitoring
type HeartbeatMessage struct {
	PanelID   string    `json:"panel_id"`
	Timestamp time.Time `json:"timestamp"`
	Sequence  int64     `json:"sequence"` // Incrementing sequence number
}

// ErrorMessage represents error responses
type ErrorMessage struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

// Well-known error codes
const (
	ErrorCodeInvalidMessage    = "INVALID_MESSAGE"
	ErrorCodeAuthFailed        = "AUTH_FAILED"
	ErrorCodeVersionConflict   = "VERSION_CONFLICT"
	ErrorCodeStateNotFound     = "STATE_NOT_FOUND"
	ErrorCodeInternalError     = "INTERNAL_ERROR"
	ErrorCodeConnectionClosed  = "CONNECTION_CLOSED"
	ErrorCodeTimeout           = "TIMEOUT"
	ErrorCodeTooManyRetries    = "TOO_MANY_RETRIES"
)

// MessageValidator provides validation for IPC messages
type MessageValidator struct{}

// NewMessageValidator creates a new message validator
func NewMessageValidator() *MessageValidator {
	return &MessageValidator{}
}

// ValidateHandshake validates a handshake message
func (v *MessageValidator) ValidateHandshake(msg HandshakeMessage) error {
	if msg.Type != MessageTypeHandshake {
		return &ValidationError{
			Field:   "type",
			Message: "must be 'handshake'",
		}
	}

	if msg.PanelID == "" {
		return &ValidationError{
			Field:   "panel_id",
			Message: "cannot be empty",
		}
	}

	if msg.PanelType == "" {
		return &ValidationError{
			Field:   "panel_type",
			Message: "cannot be empty",
		}
	}

	// Validate panel type is one of the expected values
	validPanelTypes := map[string]bool{
		"sessions": true,
		"messages": true,
		"input":    true,
	}

	if !validPanelTypes[msg.PanelType] {
		return &ValidationError{
			Field:   "panel_type",
			Message: "must be one of: sessions, messages, input",
		}
	}

	if msg.Version == "" {
		return &ValidationError{
			Field:   "version",
			Message: "cannot be empty",
		}
	}

	return nil
}

// ValidateIPCMessage validates a generic IPC message
func (v *MessageValidator) ValidateIPCMessage(msg IPCMessage) error {
	if msg.Type == "" {
		return &ValidationError{
			Field:   "type",
			Message: "cannot be empty",
		}
	}

	// Validate known message types
	validTypes := map[string]bool{
		MessageTypeHandshake:           true,
		MessageTypeHandshakeResponse:   true,
		MessageTypeStateUpdate:         true,
		MessageTypeStateUpdateResponse: true,
		MessageTypeStateRequest:        true,
		MessageTypeStateResponse:       true,
		MessageTypeStateEvent:          true,
		MessageTypePing:                true,
		MessageTypePong:                true,
		MessageTypeError:               true,
		MessageTypeSubscribe:           true,
		MessageTypeUnsubscribe:         true,
		MessageTypeHeartbeat:           true,
	}

	if !validTypes[msg.Type] {
		return &ValidationError{
			Field:   "type",
			Message: "unknown message type",
		}
	}

	if msg.Timestamp.IsZero() {
		return &ValidationError{
			Field:   "timestamp",
			Message: "cannot be zero",
		}
	}

	return nil
}

// ValidationError represents a message validation error
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e *ValidationError) Error() string {
	return e.Field + ": " + e.Message
}

// MessageSerializer handles message serialization/deserialization
type MessageSerializer struct{}

// NewMessageSerializer creates a new message serializer
func NewMessageSerializer() *MessageSerializer {
	return &MessageSerializer{}
}

// Serialize converts a message to JSON bytes
func (s *MessageSerializer) Serialize(message interface{}) ([]byte, error) {
	return json.Marshal(message)
}

// Deserialize converts JSON bytes to a message
func (s *MessageSerializer) Deserialize(data []byte, message interface{}) error {
	return json.Unmarshal(data, message)
}

// SerializeIPCMessage serializes an IPC message with type information
func (s *MessageSerializer) SerializeIPCMessage(msgType string, data interface{}) ([]byte, error) {
	message := IPCMessage{
		Type:      msgType,
		Data:      data,
		Timestamp: time.Now(),
	}
	return json.Marshal(message)
}

// DeserializeIPCMessage deserializes an IPC message
func (s *MessageSerializer) DeserializeIPCMessage(data []byte) (*IPCMessage, error) {
	var message IPCMessage
	if err := json.Unmarshal(data, &message); err != nil {
		return nil, err
	}
	return &message, nil
}

// MessageRouter routes messages based on type
type MessageRouter struct {
	handlers map[string]MessageHandler
}

// MessageHandler defines the interface for message handlers
type MessageHandler interface {
	HandleMessage(message IPCMessage) error
}

// NewMessageRouter creates a new message router
func NewMessageRouter() *MessageRouter {
	return &MessageRouter{
		handlers: make(map[string]MessageHandler),
	}
}

// RegisterHandler registers a handler for a specific message type
func (r *MessageRouter) RegisterHandler(messageType string, handler MessageHandler) {
	r.handlers[messageType] = handler
}

// RouteMessage routes a message to the appropriate handler
func (r *MessageRouter) RouteMessage(message IPCMessage) error {
	handler, exists := r.handlers[message.Type]
	if !exists {
		return &RoutingError{
			MessageType: message.Type,
			Message:     "no handler registered",
		}
	}

	return handler.HandleMessage(message)
}

// RoutingError represents a message routing error
type RoutingError struct {
	MessageType string `json:"message_type"`
	Message     string `json:"message"`
}

func (e *RoutingError) Error() string {
	return "routing error for " + e.MessageType + ": " + e.Message
}

// Utility functions for type conversion and data handling

// mapToStruct converts a map[string]interface{} to a struct using JSON marshaling
func mapToStruct(data interface{}, target interface{}) error {
	// Convert to JSON and back to handle type conversion
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return json.Unmarshal(jsonData, target)
}

// structToMap converts a struct to map[string]interface{} using JSON marshaling
func structToMap(data interface{}) (map[string]interface{}, error) {
	var result map[string]interface{}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(jsonData, &result)
	return result, err
}

// isValidPanelType checks if a panel type is valid
func isValidPanelType(panelType string) bool {
	validTypes := []string{"sessions", "messages", "input"}
	for _, valid := range validTypes {
		if panelType == valid {
			return true
		}
	}
	return false
}

// createErrorMessage creates a standardized error message
func createErrorMessage(code, message, details string) ErrorMessage {
	return ErrorMessage{
		Code:    code,
		Message: message,
		Details: details,
	}
}

// createIPCMessage creates a standardized IPC message
func createIPCMessage(msgType string, data interface{}) IPCMessage {
	return IPCMessage{
		Type:      msgType,
		Data:      data,
		Timestamp: time.Now(),
	}
}

// MessageStats tracks statistics about message processing
type MessageStats struct {
	TotalMessages   int64            `json:"total_messages"`
	MessagesByType  map[string]int64 `json:"messages_by_type"`
	ErrorCount      int64            `json:"error_count"`
	LastMessageTime time.Time        `json:"last_message_time"`
}

// NewMessageStats creates a new message statistics tracker
func NewMessageStats() *MessageStats {
	return &MessageStats{
		MessagesByType: make(map[string]int64),
	}
}

// RecordMessage records statistics for a processed message
func (s *MessageStats) RecordMessage(messageType string, success bool) {
	s.TotalMessages++
	s.MessagesByType[messageType]++
	s.LastMessageTime = time.Now()

	if !success {
		s.ErrorCount++
	}
}

// GetMessageRate returns messages per second over the last period
func (s *MessageStats) GetMessageRate(period time.Duration) float64 {
	if time.Since(s.LastMessageTime) > period {
		return 0
	}

	return float64(s.TotalMessages) / period.Seconds()
}

// TypeSafeMessage provides type-safe message creation
type TypeSafeMessage struct {
	validator  *MessageValidator
	serializer *MessageSerializer
}

// NewTypeSafeMessage creates a new type-safe message helper
func NewTypeSafeMessage() *TypeSafeMessage {
	return &TypeSafeMessage{
		validator:  NewMessageValidator(),
		serializer: NewMessageSerializer(),
	}
}

// CreateHandshake creates a validated handshake message
func (t *TypeSafeMessage) CreateHandshake(panelID, panelType, version string) (HandshakeMessage, error) {
	msg := HandshakeMessage{
		Type:      MessageTypeHandshake,
		PanelID:   panelID,
		PanelType: panelType,
		Version:   version,
		Timestamp: time.Now(),
	}

	if err := t.validator.ValidateHandshake(msg); err != nil {
		return HandshakeMessage{}, err
	}

	return msg, nil
}

// CreateError creates a standardized error message
func (t *TypeSafeMessage) CreateError(code, message, details string) IPCMessage {
	errorMsg := createErrorMessage(code, message, details)
	return createIPCMessage(MessageTypeError, errorMsg)
}

// IsStructType checks if an interface{} value is a struct
func isStructType(v interface{}) bool {
	return reflect.TypeOf(v).Kind() == reflect.Struct
}