package ipc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/sst/opencode/internal/types"
)

// SocketClient manages Unix Domain Socket client for panel communication
type SocketClient struct {
	socketPath      string
	panelID         string
	panelType       string
	conn            net.Conn
	encoder         *json.Encoder
	decoder         *json.Decoder
	connectionID    string
	isConnected     bool
	connectionMux   sync.RWMutex
	eventHandlers   map[types.StateEventType][]EventHandler
	handlerMux      sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
	reconnectDelay  time.Duration
	maxReconnects   int
	reconnectCount  int
	lastPingTime    time.Time
	pingInterval    time.Duration
}

// EventHandler defines the signature for event handling functions
type EventHandler func(event types.StateEvent) error

// NewSocketClient creates a new Unix Domain Socket client
func NewSocketClient(socketPath, panelID, panelType string) *SocketClient {
	ctx, cancel := context.WithCancel(context.Background())

	return &SocketClient{
		socketPath:     socketPath,
		panelID:        panelID,
		panelType:      panelType,
		eventHandlers:  make(map[types.StateEventType][]EventHandler),
		ctx:            ctx,
		cancel:         cancel,
		reconnectDelay: 5 * time.Second,
		maxReconnects:  10,
		pingInterval:   30 * time.Second,
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

	// Receive handshake response
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

	if err := client.encoder.Encode(message); err != nil {
		return nil, fmt.Errorf("failed to send state request: %w", err)
	}

	// Wait for response (with timeout)
	responseChan := make(chan *types.SharedApplicationState, 1)
	errorChan := make(chan error, 1)

	// This is a simplified approach - in practice you'd want a more sophisticated
	// request/response correlation mechanism
	go func() {
		for {
			var response IPCMessage
			if err := client.decoder.Decode(&response); err != nil {
				errorChan <- err
				return
			}

			if response.Type == "state_response" {
				var stateData types.SharedApplicationState
				if err := mapToStruct(response.Data, &stateData); err != nil {
					errorChan <- err
					return
				}
				responseChan <- &stateData
				return
			}
		}
	}()

	// Wait for response with timeout
	select {
	case stateData := <-responseChan:
		return stateData, nil
	case err := <-errorChan:
		return nil, err
	case <-time.After(10 * time.Second):
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

			// Set read deadline to allow periodic context checking
			client.conn.SetReadDeadline(time.Now().Add(5 * time.Second))

			var message IPCMessage
			err := client.decoder.Decode(&message)
			if err != nil {
				// Check if it's a timeout
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				log.Printf("Error reading message: %v", err)
				client.handleConnectionError(err)
				return
			}

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