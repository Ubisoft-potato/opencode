package state

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// SharedState represents the shared state between tmux panes
type SharedState struct {
	CurrentSessionID string    `json:"current_session_id"`
	CurrentTitle     string    `json:"current_title"`
	LastUpdated      time.Time `json:"last_updated"`
	UpdatedBy        string    `json:"updated_by"`
}

// StateManager manages shared state between panes
type StateManager struct {
	statePath string
	mutex     sync.RWMutex
}

// NewStateManager creates a new state manager
func NewStateManager() *StateManager {
	tmpDir := os.TempDir()
	// Use TMUX session ID if available for shared state, otherwise use a fixed name
	tmuxSession := os.Getenv("TMUX")
	var statePath string
	if tmuxSession != "" {
		// Extract session info and create consistent path for all panes in same tmux session
		statePath = filepath.Join(tmpDir, "opencode-shared-state.json")
	} else {
		statePath = filepath.Join(tmpDir, "opencode-shared-state.json")
	}

	return &StateManager{
		statePath: statePath,
	}
}

// GetState reads the current shared state with file locking
func (sm *StateManager) GetState() (*SharedState, error) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	// Open file for reading with shared lock
	file, err := os.OpenFile(sm.statePath, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			// Return default state if file doesn't exist
			return &SharedState{
				LastUpdated: time.Now(),
			}, nil
		}
		return nil, fmt.Errorf("failed to open state file: %w", err)
	}
	defer file.Close()

	// Apply shared lock for reading
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_SH); err != nil {
		return nil, fmt.Errorf("failed to acquire read lock: %w", err)
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)

	// Get file info to check if it's empty
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	if stat.Size() == 0 {
		// Return default state if file is empty
		return &SharedState{
			LastUpdated: time.Now(),
		}, nil
	}

	data, err := os.ReadFile(sm.statePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read state file: %w", err)
	}

	// Handle empty or corrupted files gracefully
	if len(data) == 0 {
		return &SharedState{
			LastUpdated: time.Now(),
		}, nil
	}

	var state SharedState
	if err := json.Unmarshal(data, &state); err != nil {
		// If JSON is corrupted, return default state instead of failing
		slog.Warn("Failed to unmarshal state, returning default", "error", err)
		return &SharedState{
			LastUpdated: time.Now(),
		}, nil
	}

	return &state, nil
}

// SetState updates the shared state with file locking
func (sm *StateManager) SetState(sessionID, title, updatedBy string) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	// Open file for writing with exclusive lock
	file, err := os.OpenFile(sm.statePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open state file for writing: %w", err)
	}
	defer file.Close()

	// Apply exclusive lock for writing
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("failed to acquire write lock: %w", err)
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)

	state := SharedState{
		CurrentSessionID: sessionID,
		CurrentTitle:     title,
		LastUpdated:      time.Now(),
		UpdatedBy:        updatedBy,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("failed to write state to file: %w", err)
	}

	// Ensure data is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync state file: %w", err)
	}

	return nil
}

// Cleanup removes the state file
func (sm *StateManager) Cleanup() {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	os.Remove(sm.statePath)
}

// GetStatePath returns the path to the state file
func (sm *StateManager) GetStatePath() string {
	return sm.statePath
}

// IsStateNewer checks if the shared state is newer than the given timestamp
func (sm *StateManager) IsStateNewer(since time.Time) (bool, error) {
	state, err := sm.GetState()
	if err != nil {
		return false, err
	}

	return state.LastUpdated.After(since), nil
}

// WaitForStateChange blocks until the state changes or timeout
func (sm *StateManager) WaitForStateChange(lastUpdate time.Time, timeout time.Duration) (*SharedState, error) {
	start := time.Now()

	for time.Since(start) < timeout {
		state, err := sm.GetState()
		if err != nil {
			return nil, err
		}

		if state.LastUpdated.After(lastUpdate) {
			return state, nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	// Return current state even if it hasn't changed
	return sm.GetState()
}