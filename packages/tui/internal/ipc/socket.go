package ipc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SocketPath returns the path for the IPC socket
func SocketPath() string {
	tmpDir := os.TempDir()
	return filepath.Join(tmpDir, fmt.Sprintf("opencode-ipc-%d.sock", os.Getpid()))
}

// Server represents an IPC server
type Server struct {
	socketPath string
	listener   net.Listener
	clients    map[string]net.Conn
	clientsMux sync.RWMutex
	eventChan  chan *Event
	done       chan struct{}
}

// NewServer creates a new IPC server
func NewServer(socketPath string) *Server {
	return &Server{
		socketPath: socketPath,
		clients:    make(map[string]net.Conn),
		eventChan:  make(chan *Event, 100),
		done:       make(chan struct{}),
	}
}

// Start starts the IPC server
func (s *Server) Start(ctx context.Context) error {
	// Remove existing socket file
	os.Remove(s.socketPath)

	listener, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on socket: %w", err)
	}
	s.listener = listener

	// Start event broadcaster
	go s.broadcastEvents(ctx)

	// Accept connections
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			go s.handleConnection(ctx, conn)
		}
	}()

	return nil
}

// Stop stops the IPC server
func (s *Server) Stop() {
	close(s.done)
	if s.listener != nil {
		s.listener.Close()
	}
	os.Remove(s.socketPath)
}

// Broadcast sends an event to all connected clients
func (s *Server) Broadcast(event *Event) {
	select {
	case s.eventChan <- event:
	default:
		// Channel full, drop event
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	clientID := conn.RemoteAddr().String()
	s.clientsMux.Lock()
	s.clients[clientID] = conn
	s.clientsMux.Unlock()

	defer func() {
		s.clientsMux.Lock()
		delete(s.clients, clientID)
		s.clientsMux.Unlock()
	}()

	// Read events from client
	decoder := json.NewDecoder(conn)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			var event Event
			if err := decoder.Decode(&event); err != nil {
				return
			}
			// Broadcast received event to other clients
			s.Broadcast(&event)
		}
	}
}

func (s *Server) broadcastEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-s.eventChan:
			s.clientsMux.RLock()
			for _, conn := range s.clients {
				encoder := json.NewEncoder(conn)
				encoder.Encode(event)
			}
			s.clientsMux.RUnlock()
		}
	}
}

// Client represents an IPC client
type Client struct {
	socketPath string
	conn       net.Conn
	eventChan  chan *Event
	done       chan struct{}
}

// NewClient creates a new IPC client
func NewClient(socketPath string) *Client {
	return &Client{
		socketPath: socketPath,
		eventChan:  make(chan *Event, 100),
		done:       make(chan struct{}),
	}
}

// Connect connects to the IPC server with retry logic and verification
func (c *Client) Connect(ctx context.Context) error {
	maxRetries := 10
	retryDelay := 500 * time.Millisecond

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Check if socket file exists
		if _, err := os.Stat(c.socketPath); os.IsNotExist(err) {
			if attempt == 1 {
				fmt.Printf("🔍 IPC socket not found at %s, waiting for server...\n", c.socketPath)
			}
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			return fmt.Errorf("IPC socket file does not exist after %d attempts: %s", maxRetries, c.socketPath)
		}

		conn, err := net.Dial("unix", c.socketPath)
		if err != nil {
			fmt.Printf("⚠️  IPC connection attempt %d/%d failed: %v\n", attempt, maxRetries, err)
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			return fmt.Errorf("failed to connect to socket after %d attempts: %w", maxRetries, err)
		}

		c.conn = conn
		fmt.Printf("✅ IPC connected successfully on attempt %d\n", attempt)

		// Start reading events
		go c.readEvents(ctx)

		// Send a ping to verify connection
		if err := c.sendPing(); err != nil {
			fmt.Printf("⚠️  IPC ping failed: %v\n", err)
		} else {
			fmt.Printf("🔄 IPC ping successful\n")
		}

		return nil
	}

	return fmt.Errorf("failed to connect after %d attempts", maxRetries)
}

// Disconnect disconnects from the IPC server
func (c *Client) Disconnect() {
	close(c.done)
	if c.conn != nil {
		c.conn.Close()
	}
}

// Send sends an event to the server
func (c *Client) Send(event *Event) error {
	if c.conn == nil {
		return fmt.Errorf("not connected")
	}

	encoder := json.NewEncoder(c.conn)
	return encoder.Encode(event)
}

// Events returns the channel for receiving events
func (c *Client) Events() <-chan *Event {
	return c.eventChan
}

// sendPing sends a ping event to verify connection
func (c *Client) sendPing() error {
	pingEvent := NewEvent("ping", "client", map[string]interface{}{
		"timestamp": time.Now().Unix(),
		"client_id": fmt.Sprintf("client_%d", os.Getpid()),
	})
	return c.Send(pingEvent)
}

func (c *Client) readEvents(ctx context.Context) {
	if c.conn == nil {
		return
	}

	decoder := json.NewDecoder(c.conn)
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		default:
			var event Event
			if err := decoder.Decode(&event); err != nil {
				return
			}
			select {
			case c.eventChan <- &event:
			default:
				// Channel full, drop event
			}
		}
	}
}