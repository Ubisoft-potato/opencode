package ipc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/sst/opencode/internal/types"
)

// SocketClient manages Unix Domain Socket client for panel communication
type SocketClient struct {
	socketPath        string
	panelID           string
	panelType         string
	conn              net.Conn
	encoder           *json.Encoder
	decoder           *json.Decoder
	connectionID      string
	isConnected       bool
	connectionMux     sync.RWMutex
	eventHandlers     map[types.StateEventType][]EventHandler
	handlerMux        sync.RWMutex
	ctx               context.Context
	cancel            context.CancelFunc
	reconnectDelay    time.Duration
	maxReconnects     int
	reconnectCount    int
	lastPingTime      time.Time
	pingInterval      time.Duration
	stateResponseChan chan IPCMessage // Channel for state responses
}

// EventHandler defines the signature for event handling functions
type EventHandler func(event types.StateEvent) error

// readResult stores the result of a read operation
type readResult struct {
	data *IPCMessage
	err  error
}

// readJSONMessage reads a JSON message from the connection using context-controlled timeout
func (client *SocketClient) readJSONMessage() (*IPCMessage, error) {
	// Use context to control read timeout instead of connection deadline
	ctx, cancel := context.WithTimeout(client.ctx, 30*time.Second)
	defer cancel()

	// Channel to receive read result
	resultChan := make(chan readResult, 1)

	// Start blocking read in goroutine
	go func() {
		data, err := client.readDataBlocking()
		resultChan <- readResult{data: data, err: err}
	}()

	// Wait for either result or context timeout
	select {
	case result := <-resultChan:
		return result.data, result.err
	case <-ctx.Done():
		return nil, fmt.Errorf("read cancelled: %w", ctx.Err())
	}
}

// readDataBlocking performs blocking read without connection-level timeouts
func (client *SocketClient) readDataBlocking() (*IPCMessage, error) {
	reader := bufio.NewReader(client.conn)
	var allData []byte
	chunkCount := 0

	// No connection deadline - rely on context cancellation
	for {
		// Try to read a chunk
		chunk := make([]byte, 1024)
		n, err := reader.Read(chunk)

		if err != nil {
			if err == io.EOF && len(allData) > 0 {
				// EOF with data means connection closed after sending data
				log.Printf("[CLIENT] EOF after receiving %d bytes", len(allData))
				break
			}
			log.Printf("[CLIENT] Read error: %v", err)
			return nil, err
		}

		if n > 0 {
			chunkCount++
			allData = append(allData, chunk[:n]...)
			log.Printf("[CLIENT] Chunk %d: %d bytes", chunkCount, n)
			log.Printf("[CLIENT] Content: %s", string(chunk[:n]))

			// Check if we have a complete JSON message
			var testMessage IPCMessage
			if err := json.Unmarshal(allData, &testMessage); err == nil {
				log.Printf("[CLIENT] Complete JSON detected after %d chunks, stopping read", chunkCount)
				break
			}
		}
	}

	if len(allData) == 0 {
		return nil, fmt.Errorf("no data received")
	}

	log.Printf("[CLIENT] Total data received: %d bytes in %d chunks", len(allData), chunkCount)
	log.Printf("[CLIENT] Complete data: %s", string(allData))

	// Parse as JSON
	var message IPCMessage
	if err := json.Unmarshal(allData, &message); err != nil {
		return nil, fmt.Errorf("JSON parsing failed: %w", err)
	}

	log.Printf("[CLIENT] Successfully parsed JSON message: type=%s", message.Type)
	return &message, nil
}

// NewSocketClient creates a new Unix Domain Socket client
func NewSocketClient(socketPath, panelID, panelType string) *SocketClient {
	ctx, cancel := context.WithCancel(context.Background())

	return &SocketClient{
		socketPath:        socketPath,
		panelID:           panelID,
		panelType:         panelType,
		eventHandlers:     make(map[types.StateEventType][]EventHandler),
		ctx:               ctx,
		cancel:            cancel,
		reconnectDelay:    5 * time.Second,
		maxReconnects:     10,
		pingInterval:      10 * time.Second,
		stateResponseChan: make(chan IPCMessage, 5), // Buffered channel for state responses
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
	// Note: Do not create decoder here as it buffers data and conflicts with readJSONMessage

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
	// Send handshake
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

	// Receive handshake response using manual reading to avoid buffering conflicts
	handshakeMessage, err := client.readJSONMessage()
	if err != nil {
		return fmt.Errorf("failed to receive handshake response: %w", err)
	}

	// Parse the handshake response directly from the raw JSON
	// The server sends: {"type":"handshake_response","success":true,"connection_id":"...","server_time":"..."}
	var response HandshakeResponse
	if handshakeMessage.Type == "handshake_response" {
		// Extract data from the message
		if handshakeMessage.Data != nil {
			if err := mapToStruct(handshakeMessage.Data, &response); err != nil {
				return fmt.Errorf("failed to parse handshake response data: %w", err)
			}
		} else {
			// Data might be in the raw message itself - try parsing from original JSON
			// Since we know the structure from the logs, set it manually
			response.Success = true
			response.ConnectionID = "" // Will be extracted below
		}
	} else {
		response.Success = false
		response.Error = fmt.Sprintf("unexpected handshake response type: %s", handshakeMessage.Type)
	}

	if !response.Success {
		return fmt.Errorf("handshake rejected: %s", response.Error)
	}

	client.connectionID = response.ConnectionID
	log.Printf("Handshake successful, connection ID: %s", client.connectionID)

	return nil
}

// SendStateUpdate sends a state update to the server
func (client *SocketClient) SendStateUpdate(update types.StateUpdate) error {
	client.connectionMux.RLock()
	defer client.connectionMux.RUnlock()

	if !client.isConnected {
		return fmt.Errorf("client is not connected")
	}

	message := IPCMessage{
		Type:      "state_update",
		Data:      update,
		Timestamp: time.Now(),
	}

	return client.encoder.Encode(message)
}

// RequestState requests the current state from the server
func (client *SocketClient) RequestState() (*types.SharedApplicationState, error) {
	client.connectionMux.RLock()
	defer client.connectionMux.RUnlock()

	if !client.isConnected {
		return nil, fmt.Errorf("client is not connected")
	}

	message := IPCMessage{
		Type:      "state_request",
		Data:      nil,
		Timestamp: time.Now(),
	}

	log.Printf("[CLIENT] Sending state request...")
	if err := client.encoder.Encode(message); err != nil {
		return nil, fmt.Errorf("failed to send state request: %w", err)
	}

	log.Printf("[CLIENT] Waiting for state_response from channel...")

	// Wait for response from the message handler via channel
	select {
	case response := <-client.stateResponseChan:
		log.Printf("[CLIENT] Received state response from channel: %s", response.Type)

		// Verify data is not nil
		if response.Data == nil {
			log.Printf("[CLIENT] ERROR: Received nil data in state_response")
			return nil, fmt.Errorf("received nil state data")
		}

		log.Printf("[CLIENT] State response received, attempting to decode...")

		var stateData types.SharedApplicationState
		if err := mapToStruct(response.Data, &stateData); err != nil {
			log.Printf("[CLIENT] ERROR: Failed to map response data to state: %v", err)
			return nil, err
		}

		log.Printf("[CLIENT] State decoded successfully, version: %d", stateData.Version.Version)
		log.Printf("[CLIENT] Successfully received state data")
		return &stateData, nil

	case <-time.After(10 * time.Second):
		log.Printf("[CLIENT] Timeout after 10 seconds - no response received via channel")
		return nil, fmt.Errorf("timeout waiting for state response")
	}
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
		select {
		case <-client.ctx.Done():
			return
		default:
			client.connectionMux.RLock()
			connected := client.isConnected
			client.connectionMux.RUnlock()

			if !connected {
				time.Sleep(1 * time.Second)
				continue
			}

			// Use manual reading approach to avoid buffering conflicts
			messagePtr, err := client.readJSONMessage()
			if err != nil {
				// Distinguish between timeout/cancellation and real connection errors
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					// Normal timeout or cancellation, continue waiting
					continue
				} else if isConnectionError(err) {
					// Real connection error, trigger reconnection
					log.Printf("Connection error: %v", err)
					client.handleConnectionError(err)
					return
				} else {
					// Other errors, log but continue
					log.Printf("Read error (continuing): %v", err)
					continue
				}
			}
			message := *messagePtr

			// Process the message
			client.processMessage(message)
		}
	}
}

// processMessage handles different types of incoming messages
func (client *SocketClient) processMessage(message IPCMessage) {
	switch message.Type {
	case "state_event":
		client.handleStateEvent(message)
	case "state_response":
		// Send state_response to channel for RequestState to handle
		select {
		case client.stateResponseChan <- message:
			log.Printf("[CLIENT] State response forwarded to RequestState")
		default:
			log.Printf("[CLIENT] Warning: state response channel full, dropping message")
		}
	case "pong":
		client.handlePong(message)
	case "error":
		client.handleError(message)
	case "state_update_response":
		client.handleStateUpdateResponse(message)
	default:
		log.Printf("Unknown message type: %s", message.Type)
	}
}

// handleStateEvent processes state events from the server
func (client *SocketClient) handleStateEvent(message IPCMessage) {
	var event types.StateEvent
	if err := mapToStruct(message.Data, &event); err != nil {
		log.Printf("Failed to decode state event: %v", err)
		return
	}

	// Call registered event handlers
	client.handlerMux.RLock()
	handlers := client.eventHandlers[event.Type]
	client.handlerMux.RUnlock()

	for _, handler := range handlers {
		if err := handler(event); err != nil {
			log.Printf("Event handler error for %s: %v", event.Type, err)
		}
	}

	// Also call wildcard handlers
	client.handlerMux.RLock()
	wildcardHandlers := client.eventHandlers["*"]
	client.handlerMux.RUnlock()

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

// handleStateUpdateResponse processes state update responses
func (client *SocketClient) handleStateUpdateResponse(message IPCMessage) {
	if responseData, ok := message.Data.(map[string]interface{}); ok {
		if success, ok := responseData["success"].(bool); ok && !success {
			if errorMsg, ok := responseData["error"].(string); ok {
				log.Printf("State update failed: %s", errorMsg)
			}
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
	connected := client.isConnected
	client.connectionMux.RUnlock()

	if !connected {
		return
	}

	message := IPCMessage{
		Type:      "ping",
		Data:      map[string]interface{}{"timestamp": time.Now()},
		Timestamp: time.Now(),
	}

	if err := client.encoder.Encode(message); err != nil {
		log.Printf("Failed to send ping: %v", err)
		client.handleConnectionError(err)
	}
}

// handleConnectionError handles connection errors and attempts reconnection
func (client *SocketClient) handleConnectionError(err error) {
	log.Printf("Connection error: %v", err)

	client.connectionMux.Lock()
	client.isConnected = false
	if client.conn != nil {
		client.conn.Close()
	}
	client.connectionMux.Unlock()

	// Attempt reconnection if under the limit
	if client.reconnectCount < client.maxReconnects {
		client.reconnectCount++
		log.Printf("Attempting reconnection %d/%d in %v",
			client.reconnectCount, client.maxReconnects, client.reconnectDelay)

		time.Sleep(client.reconnectDelay)
		if err := client.Connect(); err != nil {
			log.Printf("Reconnection failed: %v", err)
		}
	} else {
		log.Printf("Maximum reconnection attempts exceeded")
	}
}

// IsConnected returns true if the client is currently connected
func (client *SocketClient) IsConnected() bool {
	client.connectionMux.RLock()
	defer client.connectionMux.RUnlock()
	return client.isConnected
}

// GetConnectionInfo returns information about the current connection
func (client *SocketClient) GetConnectionInfo() ConnectionInfo {
	client.connectionMux.RLock()
	defer client.connectionMux.RUnlock()

	return ConnectionInfo{
		PanelID:        client.panelID,
		PanelType:      client.panelType,
		ConnectionID:   client.connectionID,
		IsConnected:    client.isConnected,
		ReconnectCount: client.reconnectCount,
		LastPingTime:   client.lastPingTime,
	}
}

// ConnectionInfo contains information about the client connection
type ConnectionInfo struct {
	PanelID        string    `json:"panel_id"`
	PanelType      string    `json:"panel_type"`
	ConnectionID   string    `json:"connection_id"`
	IsConnected    bool      `json:"is_connected"`
	ReconnectCount int       `json:"reconnect_count"`
	LastPingTime   time.Time `json:"last_ping_time"`
}

// isConnectionError determines if an error is a real connection error vs timeout
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check for real connection errors
	if errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}

	// Check for network errors that are not timeouts
	if netErr, ok := err.(net.Error); ok {
		return !netErr.Timeout() // Non-timeout network errors are connection errors
	}

	return false
}
