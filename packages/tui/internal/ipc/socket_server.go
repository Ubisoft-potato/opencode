package ipc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sst/opencode/internal/interfaces"
	"github.com/sst/opencode/internal/state"
	"github.com/sst/opencode/internal/types"
)

// SocketServer manages Unix Domain Socket server for inter-panel communication
type SocketServer struct {
	socketPath      string
	listener        net.Listener
	connections     map[string]*ClientConnection
	connectionsMux  sync.RWMutex
	eventBus        interfaces.EventBus
	stateManager    interfaces.StateManager
	ctx             context.Context
	cancel          context.CancelFunc
	isRunning       bool
	runningMux      sync.RWMutex
}

// ClientConnection represents a connected panel client
type ClientConnection struct {
	ID           string        `json:"id"`
	PanelType    string        `json:"panel_type"`
	PanelID      string        `json:"panel_id"`
	Conn         net.Conn      `json:"-"`
	ConnectedAt  time.Time     `json:"connected_at"`
	LastSeen     time.Time     `json:"last_seen"`
	MessageCount int64         `json:"message_count"`
	encoder      *json.Encoder `json:"-"`
	decoder      *json.Decoder `json:"-"`
	sendMutex    sync.Mutex    // To synchronize writes to the connection
}

// send safely writes a message to the client connection.
func (cc *ClientConnection) send(message IPCMessage) error {
	cc.sendMutex.Lock()
	defer cc.sendMutex.Unlock()

	if cc.Conn == nil {
		return fmt.Errorf("cannot send message to nil connection for panel %s", cc.PanelID)
	}

	// Set a write deadline to prevent indefinite blocking.
	cc.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	defer cc.Conn.SetWriteDeadline(time.Time{})

	return cc.encoder.Encode(message)
}

// NewSocketServer creates a new Unix Domain Socket server
func NewSocketServer(socketPath string, eventBus interfaces.EventBus, stateManager interfaces.StateManager) *SocketServer {
	ctx, cancel := context.WithCancel(context.Background())

	return &SocketServer{
		socketPath:   socketPath,
		connections:  make(map[string]*ClientConnection),
		eventBus:     eventBus,
		stateManager: stateManager,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Start begins listening for client connections
func (server *SocketServer) Start() error {
	server.runningMux.Lock()
	defer server.runningMux.Unlock()

	if server.isRunning {
		return fmt.Errorf("server is already running")
	}

	// Remove existing socket file if it exists
	if err := server.cleanupSocket(); err != nil {
		return fmt.Errorf("failed to cleanup existing socket: %w", err)
	}

	// Create directory for socket if it doesn't exist
	socketDir := filepath.Dir(server.socketPath)
	if err := os.MkdirAll(socketDir, 0755); err != nil {
		return fmt.Errorf("failed to create socket directory: %w", err)
	}

	// Start listening on Unix Domain Socket
	listener, err := net.Listen("unix", server.socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on socket: %w", err)
	}

	server.listener = listener
	server.isRunning = true

	log.Printf("IPC server started listening on %s", server.socketPath)

	// Start accepting connections in a separate goroutine
	go server.acceptConnections()

	return nil
}

// Stop gracefully shuts down the server
func (server *SocketServer) Stop() error {
	server.runningMux.Lock()
	defer server.runningMux.Unlock()

	if !server.isRunning {
		return nil
	}

	log.Printf("Stopping IPC server")

	// Cancel context to signal shutdown
	server.cancel()

	// Close all client connections
	server.connectionsMux.Lock()
	for _, conn := range server.connections {
		conn.Conn.Close()
	}
	server.connections = make(map[string]*ClientConnection)
	server.connectionsMux.Unlock()

	// Close listener
	if server.listener != nil {
		server.listener.Close()
	}

	// Cleanup socket file
	server.cleanupSocket()

	server.isRunning = false
	log.Printf("IPC server stopped")

	return nil
}

// acceptConnections handles incoming client connections
func (server *SocketServer) acceptConnections() {
	for {
		select {
		case <-server.ctx.Done():
			return
		default:
			// Set a timeout for Accept to allow periodic context checking
			if unixListener, ok := server.listener.(*net.UnixListener); ok {
				unixListener.SetDeadline(time.Now().Add(1 * time.Second))
			}

			conn, err := server.listener.Accept()
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue // This is expected, just loop again
				}
				if server.ctx.Err() != nil {
					return // Server is stopping
				}
				log.Printf("Error accepting connection: %v", err)
				continue
			}

			// Handle new connection in a separate goroutine
			go server.handleConnection(conn)
		}
	}
}

// handleConnection processes a new client connection
func (server *SocketServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Set initial deadline for handshake
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	// Wait for handshake message
	var handshake HandshakeMessage
	if err := decoder.Decode(&handshake); err != nil {
		log.Printf("Failed to decode handshake: %v", err)
		return
	}

	// Validate handshake
	if handshake.Type != "handshake" || handshake.PanelID == "" || handshake.PanelType == "" {
		log.Printf("Invalid handshake: %+v", handshake)
		// Note: Cannot use the safe send method here as clientConn is not yet created
		// and this is a raw response, not an IPCMessage.
		encoder.Encode(HandshakeResponse{Success: false, Error: "Invalid handshake"})
		return
	}

	// Create client connection object
	clientConn := &ClientConnection{
		ID:          fmt.Sprintf("%s-%d", handshake.PanelID, time.Now().UnixNano()),
		PanelType:   handshake.PanelType,
		PanelID:     handshake.PanelID,
		Conn:        conn,
		ConnectedAt: time.Now(),
		LastSeen:    time.Now(),
		encoder:     encoder,
		decoder:     decoder,
	}

	// Send handshake response (raw, not wrapped in IPCMessage)
	handshakeResponse := HandshakeResponse{
		Type:         "handshake_response",
		Success:      true,
		ConnectionID: clientConn.ID,
		ServerTime:   time.Now(),
	}
	if err := encoder.Encode(handshakeResponse); err != nil {
		log.Printf("Failed to send handshake response: %v", err)
		return
	}

	// Register connection
	server.connectionsMux.Lock()
	server.connections[clientConn.ID] = clientConn
	server.connectionsMux.Unlock()
	log.Printf("Panel %s (%s) connected with ID %s", clientConn.PanelID, clientConn.PanelType, clientConn.ID)

	// Subscribe to event bus
	eventChan := make(chan types.StateEvent, 100)
	server.eventBus.Subscribe(clientConn.PanelID, clientConn.PanelType, eventChan)

	// Start event forwarding goroutine
	go server.forwardEvents(clientConn, eventChan)

	// Remove read deadline for normal operation
	conn.SetReadDeadline(time.Time{})

	// Handle messages from this client in a blocking loop
	server.handleClientMessages(clientConn)

	// Cleanup on disconnect
	server.connectionsMux.Lock()
	delete(server.connections, clientConn.ID)
	server.connectionsMux.Unlock()

	server.eventBus.Unsubscribe(clientConn.PanelID)
	log.Printf("Panel %s (%s) disconnected", clientConn.PanelID, clientConn.PanelType)
}

// handleClientMessages processes incoming messages from a client
func (server *SocketServer) handleClientMessages(clientConn *ClientConnection) {
	for {
		select {
		case <-server.ctx.Done():
			return
		default:
			// Set a read timeout to allow periodic context checking
			clientConn.Conn.SetReadDeadline(time.Now().Add(15 * time.Second)) // Increased timeout

			var message IPCMessage
			err := clientConn.decoder.Decode(&message)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// This is an expected timeout when client is idle, not an error.
					// We can add a verbose log here if needed for debugging.
					// log.Printf("[SERVER] Read timeout for client %s. Looping.", clientConn.ID)
					continue
				}
				log.Printf("Error reading from client %s: %v", clientConn.ID, err)
				return // Real error or EOF, close connection
			}

			clientConn.LastSeen = time.Now()
			clientConn.MessageCount++

			// Process the message in a new goroutine to avoid blocking the read loop
			go server.processClientMessage(clientConn, message)
		}
	}
}

// processClientMessage handles a message from a client
func (server *SocketServer) processClientMessage(clientConn *ClientConnection, message IPCMessage) {
	log.Printf("[SERVER] Received message of type '%s' from client %s (%s)", message.Type, clientConn.PanelID, clientConn.ID)
	switch message.Type {
	case "state_update":
		server.handleStateUpdate(clientConn, message)
	case "state_request":
		server.handleStateRequest(clientConn, message)
	case "ping":
		server.handlePing(clientConn, message)
	default:
		log.Printf("Unknown message type from client %s: %s", clientConn.ID, message.Type)
		server.sendError(clientConn, "unknown message type")
	}
}

// handleStateUpdate processes a state update from a client
func (server *SocketServer) handleStateUpdate(clientConn *ClientConnection, message IPCMessage) {
	var update types.StateUpdate
	if err := mapToStruct(message.Data, &update); err != nil {
		log.Printf("Failed to decode state update: %v", err)
		server.sendError(clientConn, "invalid state update")
		return
	}

	update.SourcePanel = clientConn.PanelID

	err := server.stateManager.UpdateWithVersionCheck(update)
	if err != nil {
		log.Printf("Failed to apply state update: %v", err)
		server.sendErrorMessage(clientConn, "state_update_error", err.Error(), message.RequestID)
		return
	}

	event := state.CreateEventFromUpdate(update, server.stateManager.GetState().GetCurrentVersion())
	server.eventBus.Broadcast(event)

	response := IPCMessage{
		Type:      "state_update_response",
		RequestID: message.RequestID,
		Data: map[string]interface{}{
			"success": true,
			"version": server.stateManager.GetState().GetCurrentVersion(),
		},
		Timestamp: time.Now(),
	}
	if err := clientConn.send(response); err != nil {
		log.Printf("Failed to send state update success response: %v", err)
	}
}

// handleStateRequest processes a state request from a client
func (server *SocketServer) handleStateRequest(clientConn *ClientConnection, message IPCMessage) {
	currentState := server.stateManager.GetState()
	if currentState == nil {
		server.sendError(clientConn, "state not available")
		return
	}

	response := IPCMessage{
		Type:      "state_response",
		RequestID: message.RequestID,
		Data:      currentState.Clone(),
		Timestamp: time.Now(),
	}
	if err := clientConn.send(response); err != nil {
		log.Printf("Failed to send state response: %v", err)
	}
}

// handlePing processes a ping message from a client
func (server *SocketServer) handlePing(clientConn *ClientConnection, message IPCMessage) {
	response := IPCMessage{
		Type:      "pong",
		RequestID: message.RequestID,
		Timestamp: time.Now(),
	}
	if err := clientConn.send(response); err != nil {
		log.Printf("Failed to send pong: %v", err)
	}
}

// forwardEvents forwards state events to a client
func (server *SocketServer) forwardEvents(clientConn *ClientConnection, eventChan chan types.StateEvent) {
	for event := range eventChan {
		message := IPCMessage{
			Type:      "state_event",
			Data:      event,
			Timestamp: time.Now(),
		}
		if err := clientConn.send(message); err != nil {
			log.Printf("Failed to forward event to client %s: %v", clientConn.ID, err)
			return // Stop forwarding if sending fails
		}
	}
}

// sendError sends a generic error message to a client.
func (server *SocketServer) sendError(clientConn *ClientConnection, errorMsg string) {
	response := IPCMessage{
		Type:      "error",
		Data:      map[string]interface{}{"error": errorMsg},
		Timestamp: time.Now(),
	}
	if err := clientConn.send(response); err != nil {
		log.Printf("Failed to send error message: %v", err)
	}
}

// sendErrorMessage sends a structured error message to a client.
func (server *SocketServer) sendErrorMessage(clientConn *ClientConnection, messageType, errorMsg, requestID string) {
	response := IPCMessage{
		Type:      messageType,
		RequestID: requestID,
		Data: map[string]interface{}{
			"success": false,
			"error":   errorMsg,
		},
		Timestamp: time.Now(),
	}
	if err := clientConn.send(response); err != nil {
		log.Printf("Failed to send structured error message: %v", err)
	}
}

// cleanupSocket removes the socket file if it exists
func (server *SocketServer) cleanupSocket() error {
	if _, err := os.Stat(server.socketPath); err == nil {
		return os.Remove(server.socketPath)
	}
	return nil
}

// GetConnections returns information about all active connections
func (server *SocketServer) GetConnections() map[string]*ClientConnection {
	server.connectionsMux.RLock()
	defer server.connectionsMux.RUnlock()

	connections := make(map[string]*ClientConnection)
	for id, conn := range server.connections {
		// Create a copy without the connection object
		connections[id] = &ClientConnection{
			ID:           conn.ID,
			PanelType:    conn.PanelType,
			PanelID:      conn.PanelID,
			ConnectedAt:  conn.ConnectedAt,
			LastSeen:     conn.LastSeen,
			MessageCount: conn.MessageCount,
		}
	}
	return connections
}

// IsRunning returns true if the server is currently running
func (server *SocketServer) IsRunning() bool {
	server.runningMux.RLock()
	defer server.runningMux.RUnlock()
	return server.isRunning
}