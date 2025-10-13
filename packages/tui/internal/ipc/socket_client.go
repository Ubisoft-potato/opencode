package ipc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/sst/opencode/internal/types"
)

// SocketClient manages Unix Domain Socket client for panel communication
type SocketClient struct {
	socketPath         string
	panelID            string
	panelType          string
	conn               net.Conn
	encoder            *json.Encoder
	decoder            *json.Decoder
	connectionID       string
	isConnected        bool
	connectionMux      sync.RWMutex
	eventHandlers      map[types.StateEventType][]EventHandler
	handlerMux         sync.RWMutex
	ctx                context.Context
	cancel             context.CancelFunc
	reconnectDelay     time.Duration
	maxReconnects      int
	reconnectCount     int
	lastPingTime       time.Time
	pingInterval       time.Duration
	pendingRequests    map[string]chan IPCMessage // Maps requestID to a response channel
	pendingRequestsMux sync.Mutex                 // Mutex for pendingRequests map
	currentVersion     int64                        // Track current state version
	versionMux         sync.RWMutex                 // Mutex for version access
}

// EventHandler defines the signature for event handling functions
type EventHandler func(event types.StateEvent) error

// NewSocketClient creates a new Unix Domain Socket client
func NewSocketClient(socketPath, panelID, panelType string) *SocketClient {
	ctx, cancel := context.WithCancel(context.Background())

	return &SocketClient{
		socketPath:      socketPath,
		panelID:         panelID,
		panelType:       panelType,
		eventHandlers:   make(map[types.StateEventType][]EventHandler),
		pendingRequests: make(map[string]chan IPCMessage),
		ctx:             ctx,
		cancel:          cancel,
		reconnectDelay:  5 * time.Second,
		maxReconnects:   10,
		pingInterval:    10 * time.Second,
	}
}

// Connect establishes a connection to the IPC server
func (client *SocketClient) Connect() error {
	client.connectionMux.Lock()
	defer client.connectionMux.Unlock()

	if client.isConnected {
		return fmt.Errorf("client is already connected")
	}

	// Establish Unix Domain Socket connection
	conn, err := net.Dial("unix", client.socketPath)
	if err != nil {
		return fmt.Errorf("failed to connect to IPC server: %w", err)
	}

	client.conn = conn
	client.encoder = json.NewEncoder(conn)
	client.decoder = json.NewDecoder(conn)

	// Perform handshake
	if err := client.performHandshake(); err != nil {
		conn.Close()
		return fmt.Errorf("handshake failed: %w", err)
	}

	client.isConnected = true
	client.reconnectCount = 0

	log.Printf("Panel %s (%s) connected to IPC server", client.panelID, client.panelType)

	// Start message handling and ping goroutines
	go client.handleMessages()
	go client.pingLoop()

	return nil
}

// Disconnect closes the connection to the IPC server
func (client *SocketClient) Disconnect() error {
	client.connectionMux.Lock()
	defer client.connectionMux.Unlock()

	if !client.isConnected {
		return nil
	}

	log.Printf("Panel %s (%s) disconnecting from IPC server", client.panelID, client.panelType)

	// Cancel context to signal shutdown
	client.cancel()

	// Close connection
	if client.conn != nil {
		client.conn.Close()
	}

	client.isConnected = false
	client.connectionID = ""

	return nil
}

// performHandshake exchanges handshake messages with the server
func (client *SocketClient) performHandshake() error {
	handshake := HandshakeMessage{
		Type:      "handshake",
		PanelID:   client.panelID,
		PanelType: client.panelType,
		Version:   "1.0",
		Timestamp: time.Now(),
	}

	if err := client.encoder.Encode(handshake); err != nil {
		return fmt.Errorf("failed to send handshake: %w", err)
	}

	var response HandshakeResponse
	if err := client.decoder.Decode(&response); err != nil {
		return fmt.Errorf("failed to receive handshake response: %w", err)
	}

	if !response.Success {
		return fmt.Errorf("handshake rejected: %s", response.Error)
	}

	client.connectionID = response.ConnectionID
	log.Printf("Handshake successful, connection ID: %s", client.connectionID)

	return nil
}

// sendRequestAndWait is the new core function for synchronous request-response calls.
func (client *SocketClient) sendRequestAndWait(message *IPCMessage, timeout time.Duration) (*IPCMessage, error) {
	client.connectionMux.RLock()
	if !client.isConnected {
		client.connectionMux.RUnlock()
		return nil, fmt.Errorf("client is not connected")
	}
	client.connectionMux.RUnlock()

	// Generate a unique request ID
	requestID := uuid.New().String()
	message.RequestID = requestID

	// Create a response channel for this specific request
	respChan := make(chan IPCMessage, 1)

	// Register the request
	client.pendingRequestsMux.Lock()
	client.pendingRequests[requestID] = respChan
	client.pendingRequestsMux.Unlock()

	// Ensure cleanup of the pending request
	defer func() {
		client.pendingRequestsMux.Lock()
		delete(client.pendingRequests, requestID)
		close(respChan)
		client.pendingRequestsMux.Unlock()
	}()

	// Send the request
	if err := client.encoder.Encode(message); err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Wait for the response or timeout
	select {
	case response, ok := <-respChan:
		if !ok {
			return nil, fmt.Errorf("response channel closed unexpectedly for request %s", requestID)
		}
		return &response, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for response for request %s", requestID)
	}
}

// RequestState requests the current state from the server using the new sync mechanism.
func (c *SocketClient) RequestState() (*types.SharedApplicationState, error) {
	log.Printf("[CLIENT] Requesting initial state from panel %s", c.panelID)

	message := IPCMessage{
		Type:      "state_request",
		Timestamp: time.Now(),
	}

	response, err := c.sendRequestAndWait(&message, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to get state response: %w", err)
	}

	if response.Type != "state_response" {
		return nil, fmt.Errorf("unexpected response type: expected 'state_response', got '%s'", response.Type)
	}

	if response.Data == nil {
		return nil, fmt.Errorf("received nil state data")
	}

	var stateData types.SharedApplicationState
	if err := mapToStruct(response.Data, &stateData); err != nil {
		return nil, fmt.Errorf("failed to map response data to state: %w", err)
	}

	c.setCurrentVersion(stateData.Version.Version)
	log.Printf("[CLIENT] Successfully received and decoded state version: %d", stateData.Version.Version)
	return &stateData, nil
}

// SendStateUpdateAndWait sends a state update and waits for a confirmation response.
func (client *SocketClient) SendStateUpdateAndWait(update types.StateUpdate) error {
	message := IPCMessage{
		Type:      "state_update",
		Data:      update,
		Timestamp: time.Now(),
	}

	response, err := client.sendRequestAndWait(&message, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to get state update response: %w", err)
	}

	if response.Type != "state_update_response" {
		return fmt.Errorf("unexpected response type: expected 'state_update_response', got '%s'", response.Type)
	}

	if responseData, ok := response.Data.(map[string]interface{}); ok {
		if success, ok := responseData["success"].(bool); ok && success {
			if version, ok := responseData["version"].(float64); ok {
				client.setCurrentVersion(int64(version))
			}
			return nil // Success
		}
		if errorMsg, ok := responseData["error"].(string); ok {
			return fmt.Errorf("state update failed on server: %s", errorMsg)
		}
	}

	return fmt.Errorf("invalid state update response format")
}

// RegisterEventHandler registers a handler for specific event types
func (client *SocketClient) RegisterEventHandler(eventType types.StateEventType, handler EventHandler) {
	client.handlerMux.Lock()
	defer client.handlerMux.Unlock()

	if client.eventHandlers[eventType] == nil {
		client.eventHandlers[eventType] = make([]EventHandler, 0)
	}
	client.eventHandlers[eventType] = append(client.eventHandlers[eventType], handler)
	log.Printf("Registered event handler for %s", eventType)
}

// handleMessages processes incoming messages from the server
func (client *SocketClient) handleMessages() {
	for {
		if client.ctx.Err() != nil {
			return
		}

		var message IPCMessage
		err := client.decoder.Decode(&message)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || isConnectionError(err) {
				log.Printf("Connection closed, triggering reconnect: %v", err)
				client.handleConnectionError(err)
				return // Exit this handler, a new one will be started on reconnect
			}
			log.Printf("Error decoding message: %v", err)
			continue
		}

		client.processMessage(message)
	}
}

// processMessage dispatches incoming messages.
func (client *SocketClient) processMessage(message IPCMessage) {
	// If the message has a RequestID, it's a response to a specific request.
	if message.RequestID != "" {
		client.pendingRequestsMux.Lock()
		if respChan, ok := client.pendingRequests[message.RequestID]; ok {
			// We don't delete from the map here; the waiting function's defer does that.
			client.pendingRequestsMux.Unlock()
			select {
			case respChan <- message:
				// Response sent to the waiting goroutine
			default:
				log.Printf("Warning: could not send response for request %s, channel may be full or closed", message.RequestID)
			}
			return
		}
		client.pendingRequestsMux.Unlock()
		log.Printf("Warning: received response for unknown or timed-out request ID: %s", message.RequestID)
		return
	}

	// Otherwise, it's a broadcast event.
	switch message.Type {
	case "state_event":
		client.handleStateEvent(message)
	case "pong":
		client.handlePong(message)
	case "error":
		client.handleError(message)
	default:
		log.Printf("Unknown broadcast message type: %s", message.Type)
	}
}

// handleStateEvent processes state events from the server
func (client *SocketClient) handleStateEvent(message IPCMessage) {
	var event types.StateEvent
	if err := mapToStruct(message.Data, &event); err != nil {
		log.Printf("Failed to decode state event: %v", err)
		return
	}

	client.setCurrentVersion(event.Version)

	client.handlerMux.RLock()
	defer client.handlerMux.RUnlock()

	handlers := client.eventHandlers[event.Type]
	for _, handler := range handlers {
		if err := handler(event); err != nil {
			log.Printf("Event handler error for %s: %v", event.Type, err)
		}
	}

	wildcardHandlers := client.eventHandlers["*"]
	for _, handler := range wildcardHandlers {
		if err := handler(event); err != nil {
			log.Printf("Wildcard event handler error for %s: %v", event.Type, err)
		}
	}
}

// handlePong processes pong responses
func (client *SocketClient) handlePong(message IPCMessage) {
	client.lastPingTime = time.Now()
}

// handleError processes error messages from the server
func (client *SocketClient) handleError(message IPCMessage) {
	if errorData, ok := message.Data.(map[string]interface{}); ok {
		if errorMsg, ok := errorData["error"].(string); ok {
			log.Printf("Server error: %s", errorMsg)
		}
	}
}

// pingLoop sends periodic ping messages to maintain connection
func (client *SocketClient) pingLoop() {
	ticker := time.NewTicker(client.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-client.ctx.Done():
			return
		case <-ticker.C:
			client.sendPing()
		}
	}
}

// sendPing sends a ping message to the server
func (client *SocketClient) sendPing() {
	client.connectionMux.RLock()
	if !client.isConnected {
		client.connectionMux.RUnlock()
		return
	}
	client.connectionMux.RUnlock()

	message := IPCMessage{
		Type:      "ping",
		Timestamp: time.Now(),
	}

	if err := client.encoder.Encode(message); err != nil {
		log.Printf("Failed to send ping: %v", err)
		client.handleConnectionError(err)
	}
}

// handleConnectionError handles connection errors and attempts reconnection
func (client *SocketClient) handleConnectionError(err error) {
	client.connectionMux.Lock()
	if !client.isConnected {
		client.connectionMux.Unlock()
		return // Already handling a disconnect/reconnect
	}
	client.isConnected = false
	if client.conn != nil {
		client.conn.Close()
	}
	client.connectionMux.Unlock()

	log.Printf("Connection error: %v", err)

	if client.reconnectCount < client.maxReconnects {
		client.reconnectCount++
		log.Printf("Attempting reconnection %d/%d in %v", client.reconnectCount, client.maxReconnects, client.reconnectDelay)
		time.Sleep(client.reconnectDelay)
		if err := client.Connect(); err != nil {
			log.Printf("Reconnection failed: %v", err)
		}
	} else {
		log.Printf("Maximum reconnection attempts exceeded")
		client.cancel() // Stop all operations
	}
}

// IsConnected returns true if the client is currently connected
func (client *SocketClient) IsConnected() bool {
	client.connectionMux.RLock()
	defer client.connectionMux.RUnlock()
	return client.isConnected
}

func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || strings.Contains(err.Error(), "broken pipe") {
		return true
	}
	if netErr, ok := err.(net.Error); ok {
		return !netErr.Timeout()
	}
	return false
}

func (client *SocketClient) setCurrentVersion(version int64) {
	client.versionMux.Lock()
	defer client.versionMux.Unlock()
	client.currentVersion = version
}
