package state

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sst/opencode/internal/interfaces"
	"github.com/sst/opencode/internal/types"
)

// PanelSyncManager coordinates state updates across panels with persistence
type PanelSyncManager struct {
	state            *types.SharedApplicationState
	eventBus         interfaces.EventBus
	repository       interfaces.StateRepository
	conflictResolver interfaces.ConflictResolver
	ctx              context.Context
	cancel           context.CancelFunc
	syncMutex        sync.RWMutex
	autoSaveEnabled  bool
	autoSaveInterval time.Duration
	lastSaveTime     time.Time
	saveQueue        chan saveRequest
	metrics          *SyncMetrics
}

// saveRequest represents a queued save operation
type saveRequest struct {
	state    *types.SharedApplicationState
	callback chan error
}

// SyncManagerConfig contains configuration for the sync manager
type SyncManagerConfig struct {
	AutoSaveEnabled  bool          `json:"auto_save_enabled"`
	AutoSaveInterval time.Duration `json:"auto_save_interval"`
	EventHistorySize int           `json:"event_history_size"`
	SaveQueueSize    int           `json:"save_queue_size"`
}

// DefaultSyncManagerConfig returns default configuration
func DefaultSyncManagerConfig() SyncManagerConfig {
	return SyncManagerConfig{
		AutoSaveEnabled:  true,
		AutoSaveInterval: 5 * time.Second,
		EventHistorySize: 1000,
		SaveQueueSize:    100,
	}
}

// NewPanelSyncManager creates a new panel synchronization manager
func NewPanelSyncManager(
	sharedState *types.SharedApplicationState,
	repository interfaces.StateRepository,
	eventBus interfaces.EventBus,
	conflictResolver interfaces.ConflictResolver,
	config SyncManagerConfig,
) *PanelSyncManager {
	ctx, cancel := context.WithCancel(context.Background())

	manager := &PanelSyncManager{
		state:            sharedState,
		eventBus:         eventBus,
		repository:       repository,
		conflictResolver: conflictResolver,
		ctx:              ctx,
		cancel:           cancel,
		autoSaveEnabled:  config.AutoSaveEnabled,
		autoSaveInterval: config.AutoSaveInterval,
		saveQueue:        make(chan saveRequest, config.SaveQueueSize),
		metrics:          NewSyncMetrics(),
	}

	// Start background workers
	go manager.autoSaveWorker()
	go manager.saveWorker()

	return manager
}

// Initialize loads state from persistence and starts the manager
func (manager *PanelSyncManager) Initialize() error {
	// Initialize repository
	if err := manager.repository.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize repository: %w", err)
	}

	// Try to load existing state
	if loadedState, err := manager.repository.LoadStateAtomic(); err == nil {
		manager.syncMutex.Lock()
		manager.state = loadedState
		manager.syncMutex.Unlock()
		log.Printf("Loaded existing state with version %d", loadedState.Version.Version)
	} else {
		// Create new state if load failed
		log.Printf("Failed to load state, creating new: %v", err)
		manager.syncMutex.Lock()
		manager.state = types.NewSharedApplicationState()
		manager.syncMutex.Unlock()

		// Save initial state
		if err := manager.saveStateSync(); err != nil {
			log.Printf("Failed to save initial state: %v", err)
		}
	}

	manager.metrics.RecordInitialization(true)
	return nil
}

// Stop gracefully shuts down the sync manager
func (manager *PanelSyncManager) Stop() error {
	log.Printf("Stopping panel sync manager")

	// Cancel context to signal shutdown
	manager.cancel()

	// Save current state before shutdown
	if err := manager.saveStateSync(); err != nil {
		log.Printf("Failed to save state during shutdown: %v", err)
	}

	// Close save queue
	close(manager.saveQueue)

	log.Printf("Panel sync manager stopped")
	return nil
}

// decodePayload is a helper to convert a map payload from JSON decoding back into a specific struct type.
func decodePayload(data interface{}, target interface{}) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal payload map: %w", err)
	}
	if err := json.Unmarshal(bytes, target); err != nil {
		return fmt.Errorf("failed to unmarshal payload into target struct: %w", err)
	}
	return nil
}

// UpdateSessionSelection handles session changes from Sessions panel
func (manager *PanelSyncManager) UpdateSessionSelection(sessionID string, panelID string) error {
	update := types.StateUpdate{
		ID:              generateUpdateID(),
		Type:            types.SessionChanged,
		ExpectedVersion: manager.state.GetCurrentVersion(),
		Payload:         types.SessionChangePayload{SessionID: sessionID},
		SourcePanel:     panelID,
		Timestamp:       time.Now(),
	}

	return manager.applyUpdateWithEvents(update)
}

// AddSession handles adding a new session
func (manager *PanelSyncManager) AddSession(session types.SessionInfo, panelID string) error {
	update := types.StateUpdate{
		ID:              generateUpdateID(),
		Type:            types.SessionAdded,
		ExpectedVersion: manager.state.GetCurrentVersion(),
		Payload:         types.SessionAddPayload{Session: session},
		SourcePanel:     panelID,
		Timestamp:       time.Now(),
	}

	return manager.applyUpdateWithEvents(update)
}

// UpdateSession handles updating session metadata
func (manager *PanelSyncManager) UpdateSession(sessionID, title string, isActive bool, panelID string) error {
	update := types.StateUpdate{
		ID:              generateUpdateID(),
		Type:            types.SessionUpdated,
		ExpectedVersion: manager.state.GetCurrentVersion(),
		Payload:         types.SessionUpdatePayload{SessionID: sessionID, Title: title, IsActive: isActive},
		SourcePanel:     panelID,
		Timestamp:       time.Now(),
	}

	return manager.applyUpdateWithEvents(update)
}

// DeleteSession handles session deletion
func (manager *PanelSyncManager) DeleteSession(sessionID string, panelID string) error {
	update := types.StateUpdate{
		ID:              generateUpdateID(),
		Type:            types.SessionDeleted,
		ExpectedVersion: manager.state.GetCurrentVersion(),
		Payload:         types.SessionDeletePayload{SessionID: sessionID},
		SourcePanel:     panelID,
		Timestamp:       time.Now(),
	}

	return manager.applyUpdateWithEvents(update)
}

// AddMessage handles new messages from Messages panel
func (manager *PanelSyncManager) AddMessage(message types.MessageInfo, panelID string) error {
	update := types.StateUpdate{
		ID:              generateUpdateID(),
		Type:            types.MessageAdded,
		ExpectedVersion: manager.state.GetCurrentVersion(),
		Payload:         types.MessageAddPayload{Message: message},
		SourcePanel:     panelID,
		Timestamp:       time.Now(),
	}

	return manager.applyUpdateWithEvents(update)
}

// UpdateMessage handles message updates
func (manager *PanelSyncManager) UpdateMessage(messageID, content, status string, panelID string) error {
	update := types.StateUpdate{
		ID:              generateUpdateID(),
		Type:            types.MessageUpdated,
		ExpectedVersion: manager.state.GetCurrentVersion(),
		Payload:         types.MessageUpdatePayload{MessageID: messageID, Content: content, Status: status},
		SourcePanel:     panelID,
		Timestamp:       time.Now(),
	}

	return manager.applyUpdateWithEvents(update)
}

// UpdateInputBuffer handles input changes from Input panel
func (manager *PanelSyncManager) UpdateInputBuffer(buffer string, cursorPos, selStart, selEnd int, mode, panelID string) error {
	update := types.StateUpdate{
		ID:              generateUpdateID(),
		Type:            types.InputUpdated,
		ExpectedVersion: manager.state.GetCurrentVersion(),
		Payload:         types.InputUpdatePayload{
			Buffer:         buffer,
			CursorPosition: cursorPos,
			SelectionStart: selStart,
			SelectionEnd:   selEnd,
			Mode:           mode,
		},
		SourcePanel: panelID,
		Timestamp:   time.Now(),
	}

	return manager.applyUpdateWithEvents(update)
}

// MoveCursor handles cursor movement from Input panel
func (manager *PanelSyncManager) MoveCursor(position, selStart, selEnd int, panelID string) error {
	update := types.StateUpdate{
		ID:              generateUpdateID(),
		Type:            types.CursorMoved,
		ExpectedVersion: manager.state.GetCurrentVersion(),
		Payload:         types.CursorMovePayload{Position: position, SelectionStart: selStart, SelectionEnd: selEnd},
		SourcePanel:     panelID,
		Timestamp:       time.Now(),
	}

	return manager.applyUpdateWithEvents(update)
}

// ChangeTheme handles theme changes
func (manager *PanelSyncManager) ChangeTheme(theme string, panelID string) error {
	update := types.StateUpdate{
		ID:              generateUpdateID(),
		Type:            types.ThemeChanged,
		ExpectedVersion: manager.state.GetCurrentVersion(),
		Payload:         types.ThemeChangePayload{Theme: theme},
		SourcePanel:     panelID,
		Timestamp:       time.Now(),
	}

	return manager.applyUpdateWithEvents(update)
}

// ChangeModel handles model selection changes
func (manager *PanelSyncManager) ChangeModel(provider, model string, panelID string) error {
	update := types.StateUpdate{
		ID:              generateUpdateID(),
		Type:            types.ModelChanged,
		ExpectedVersion: manager.state.GetCurrentVersion(),
		Payload:         types.ModelChangePayload{Provider: provider, Model: model},
		SourcePanel:     panelID,
		Timestamp:       time.Now(),
	}

	return manager.applyUpdateWithEvents(update)
}

// ChangeAgent handles agent selection changes
func (manager *PanelSyncManager) ChangeAgent(agent string, panelID string) error {
	update := types.StateUpdate{
		ID:              generateUpdateID(),
		Type:            types.AgentChanged,
		ExpectedVersion: manager.state.GetCurrentVersion(),
		Payload:         types.AgentChangePayload{Agent: agent},
		SourcePanel:     panelID,
		Timestamp:       time.Now(),
	}

	return manager.applyUpdateWithEvents(update)
}

// applyUpdateWithEvents applies an update and broadcasts events
func (manager *PanelSyncManager) applyUpdateWithEvents(update types.StateUpdate) error {
	// Apply update with conflict resolution
	result := manager.conflictResolver.ResolveConflict(manager, update)
	if !result.Success {
		manager.metrics.RecordUpdate(update.Type, false, result.TimeTaken)
		return result.Error
	}

	manager.metrics.RecordUpdate(update.Type, true, result.TimeTaken)

	// Create and broadcast event
	event := CreateEventFromUpdate(update, manager.state.GetCurrentVersion())
	manager.eventBus.Broadcast(event)

	// Queue save operation if auto-save is enabled
	if manager.autoSaveEnabled {
		select {
		case manager.saveQueue <- saveRequest{state: manager.state.Clone(), callback: nil}:
			// Save queued successfully
		default:
			// Save queue full, log warning
			log.Printf("Save queue full, skipping auto-save for update %s", update.Type)
		}
	}

	return nil
}

// UpdateWithVersionCheck applies a state update with optimistic locking
func (manager *PanelSyncManager) UpdateWithVersionCheck(update types.StateUpdate) error {
    // Acquire lock to perform version check and apply updates atomically.
    // IMPORTANT: Do NOT hold the lock while invoking the conflict resolver,
    // which calls UpdateWithVersionCheck again and would deadlock.
    manager.syncMutex.Lock()

    // Check for version conflicts (optimistic locking)
    if manager.state.Version.Version != update.ExpectedVersion {
        // Release lock before invoking resolver to avoid re-entrant deadlock.
        manager.syncMutex.Unlock()

        // Attempt to resolve the conflict using the configured resolver
        if manager.conflictResolver != nil {
            result := manager.conflictResolver.ResolveConflict(manager, update)
            if result != nil && result.Success {
                // Conflict resolved and update applied within resolver path
                // Return early to avoid double application/broadcast
                return nil
            }
            // If resolver did not succeed, fall through to original error
        }

        return fmt.Errorf("version conflict: expected %d, current %d",
            update.ExpectedVersion, manager.GetState().GetCurrentVersion())
    }

    // No conflict: ensure we unlock on all return paths below
    defer manager.syncMutex.Unlock()

	// Apply the update based on its type
	switch update.Type {
	case types.SessionAdded:
		var payload types.SessionAddPayload
		if err := decodePayload(update.Payload, &payload); err != nil {
			return err
		}
		manager.state.AddSession(payload.Session)

	case types.SessionChanged:
		var payload types.SessionChangePayload
		if err := decodePayload(update.Payload, &payload); err != nil {
			return err
		}
		if !manager.state.SetCurrentSession(payload.SessionID) {
			// Log warning instead of error if session does not exist yet
			log.Printf("Warning: session %s not found during SessionChanged update", payload.SessionID)
		}

	case types.SessionDeleted:
		var payload types.SessionDeletePayload
		if err := decodePayload(update.Payload, &payload); err != nil {
			return err
		}
		if !manager.state.RemoveSession(payload.SessionID) {
			return fmt.Errorf("session %s not found for deletion", payload.SessionID)
		}

	case types.MessageAdded:
		var payload types.MessageAddPayload
		if err := decodePayload(update.Payload, &payload); err != nil {
			return err
		}
		// Deduplicate by Message.ID: if exists, update instead of append
		existingIdx := -1
		for i := range manager.state.Messages {
			if manager.state.Messages[i].ID == payload.Message.ID {
				existingIdx = i
				break
			}
		}
		if existingIdx >= 0 {
			// Update existing message fields conservatively
			if payload.Message.Content != "" {
				manager.state.Messages[existingIdx].Content = payload.Message.Content
			}
			if payload.Message.Status != "" {
				manager.state.Messages[existingIdx].Status = payload.Message.Status
			}
			if payload.Message.Parts != nil {
				manager.state.Messages[existingIdx].Parts = payload.Message.Parts
			}
			// Keep session and type as-is; refresh timestamp
			manager.state.Messages[existingIdx].Timestamp = time.Now()
			// Do not bump session message count for duplicate add
			msg := manager.state.Messages[existingIdx]
			manager.state.CurrentMessage = &msg
		} else {
			// Append new message to state
			manager.state.Messages = append(manager.state.Messages, payload.Message)
			// Update session message count if session exists
			for i := range manager.state.Sessions {
				if manager.state.Sessions[i].ID == payload.Message.SessionID {
					manager.state.Sessions[i].MessageCount++
					break
				}
			}
			// Set current message pointer
			msg := payload.Message
			manager.state.CurrentMessage = &msg
		}

	case types.MessageUpdated:
		var payload types.MessageUpdatePayload
		if err := decodePayload(update.Payload, &payload); err != nil {
			return err
		}
		for i := range manager.state.Messages {
			if manager.state.Messages[i].ID == payload.MessageID {
				if payload.Content != "" {
					manager.state.Messages[i].Content = payload.Content
				}
				if payload.Status != "" {
					manager.state.Messages[i].Status = payload.Status
				}
				if payload.Parts != nil {
					manager.state.Messages[i].Parts = payload.Parts
				}
				break
			}
		}

	case types.MessageDeleted:
		var payload types.MessageDeletePayload
		if err := decodePayload(update.Payload, &payload); err != nil {
			return err
		}
		// Find message and remove it; adjust session count
		for i := range manager.state.Messages {
			if manager.state.Messages[i].ID == payload.MessageID {
				// adjust session count
				sid := manager.state.Messages[i].SessionID
				for j := range manager.state.Sessions {
					if manager.state.Sessions[j].ID == sid && manager.state.Sessions[j].MessageCount > 0 {
						manager.state.Sessions[j].MessageCount--
						break
					}
				}
				// remove message
				manager.state.Messages = append(manager.state.Messages[:i], manager.state.Messages[i+1:]...)
				break
			}
		}

	case types.InputUpdated:
		var payload types.InputUpdatePayload
		if err := decodePayload(update.Payload, &payload); err != nil {
			return err
		}
		manager.state.Input.Buffer = payload.Buffer
		manager.state.Input.CursorPosition = payload.CursorPosition
		manager.state.Input.SelectionStart = payload.SelectionStart
		manager.state.Input.SelectionEnd = payload.SelectionEnd
		if payload.Mode != "" {
			manager.state.Input.Mode = payload.Mode
		}

	case types.CursorMoved:
		var payload types.CursorMovePayload
		if err := decodePayload(update.Payload, &payload); err != nil {
			return err
		}
		manager.state.Input.CursorPosition = payload.Position
		manager.state.Input.SelectionStart = payload.SelectionStart
		manager.state.Input.SelectionEnd = payload.SelectionEnd

	case types.ThemeChanged:
		var payload types.ThemeChangePayload
		if err := decodePayload(update.Payload, &payload); err != nil {
			return err
		}
		manager.state.Theme = payload.Theme

	case types.ModelChanged:
		var payload types.ModelChangePayload
		if err := decodePayload(update.Payload, &payload); err != nil {
			return err
		}
		manager.state.Provider = payload.Provider
		manager.state.Model = payload.Model

	case types.AgentChanged:
		var payload types.AgentChangePayload
		if err := decodePayload(update.Payload, &payload); err != nil {
			return err
		}
		manager.state.Agent = payload.Agent

	default:
		log.Printf("Warning: unhandled update type in UpdateWithVersionCheck: %s. Bumping version only.", update.Type)
	}

	// Increment version and update timestamps for any successful change
	manager.state.Version.Version++
	manager.state.Version.Timestamp = time.Now()
	manager.state.Version.Source = update.SourcePanel
	manager.state.LastUpdate = time.Now()
	manager.state.UpdateCount++

    // Create and broadcast event
    event := CreateEventFromUpdate(update, manager.state.Version.Version)
    manager.eventBus.Broadcast(event)

    return nil
}


// GetState returns a copy of the current state
func (manager *PanelSyncManager) GetState() *types.SharedApplicationState {
	manager.syncMutex.RLock()
	defer manager.syncMutex.RUnlock()
	return manager.state.Clone()
}

// GetEventBus returns the event bus for subscribing to events
func (manager *PanelSyncManager) GetEventBus() interfaces.EventBus {
	return manager.eventBus
}

// SaveStateSync saves the state synchronously
func (manager *PanelSyncManager) SaveStateSync() error {
	return manager.saveStateSync()
}

// saveStateSync performs synchronous state saving
func (manager *PanelSyncManager) saveStateSync() error {
	manager.syncMutex.RLock()
	stateClone := manager.state.Clone()
	manager.syncMutex.RUnlock()

	startTime := time.Now()
	err := manager.repository.SaveStateAtomic(stateClone)
	duration := time.Since(startTime)

	if err == nil {
		manager.lastSaveTime = time.Now()
		manager.metrics.RecordSave(true, duration)
	} else {
		manager.metrics.RecordSave(false, duration)
	}

	return err
}

// autoSaveWorker performs periodic auto-saves
func (manager *PanelSyncManager) autoSaveWorker() {
	if !manager.autoSaveEnabled {
		return
	}

	ticker := time.NewTicker(manager.autoSaveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-manager.ctx.Done():
			return
		case <-ticker.C:
			// Check if state has been modified since last save
			if time.Since(manager.lastSaveTime) >= manager.autoSaveInterval {
				if err := manager.saveStateSync(); err != nil {
					log.Printf("Auto-save failed: %v", err)
				}
			}
		}
	}
}

// saveWorker processes queued save operations
func (manager *PanelSyncManager) saveWorker() {
	for {
		select {
		case <-manager.ctx.Done():
			return
		case req, ok := <-manager.saveQueue:
			if !ok {
				return
			}

			// Process save request
			err := manager.repository.SaveStateAtomic(req.state)
			if err == nil {
				manager.lastSaveTime = time.Now()
			}

			// Send response if callback provided
			if req.callback != nil {
				req.callback <- err
			}
		}
	}
}

// ForceFullSync forces a full state synchronization
func (manager *PanelSyncManager) ForceFullSync() error {
	// Save current state
	if err := manager.saveStateSync(); err != nil {
		return fmt.Errorf("failed to save state: %w", err)
	}

	// Broadcast full state sync event
	event := types.StateEvent{
		ID:          generateEventID(),
		Type:        types.EventStateSync,
		Data:        types.StateSyncPayload{State: manager.state},
		Version:     manager.state.GetCurrentVersion(),
		SourcePanel: "system",
		Timestamp:   time.Now(),
	}

	manager.eventBus.Broadcast(event)
	return nil
}

// GetMetrics returns sync manager metrics
func (manager *PanelSyncManager) GetMetrics() interfaces.StateManagerMetrics {
	m := manager.metrics
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return interfaces.StateManagerMetrics{
		TotalUpdates:         m.TotalUpdates,
		SuccessfulUpdates:    m.SuccessfulUpdates,
		FailedUpdates:        m.FailedUpdates,
		UpdatesByType:        m.UpdatesByType,
		TotalSaves:           m.TotalSaves,
		SuccessfulSaves:      m.SuccessfulSaves,
		FailedSaves:          m.FailedSaves,
		AverageUpdateLatency: m.AverageUpdateLatency,
		AverageSaveLatency:   m.AverageSaveLatency,
		LastUpdateTime:       m.LastUpdateTime,
		LastSaveTime:         m.LastSaveTime,
	}
}

// GetConflictStatistics returns conflict resolution statistics
func (manager *PanelSyncManager) GetConflictStatistics() interfaces.ConflictStatistics {
	return manager.conflictResolver.GetStatistics()
}

// IsHealthy returns true if the sync manager is operating normally
func (manager *PanelSyncManager) IsHealthy() bool {
	// Check if conflict resolver is healthy
	if !manager.conflictResolver.IsHealthy() {
		return false
	}

	// Check if save queue is not full
	if len(manager.saveQueue) >= cap(manager.saveQueue) {
		return false
	}

	// Check if recent operations have been successful
	return manager.metrics.IsHealthy()
}

// generateUpdateID creates a unique identifier for state updates
func generateUpdateID() string {
	return fmt.Sprintf("update_%d_%d", time.Now().UnixNano(), time.Now().Unix())
}

// SyncMetrics tracks synchronization performance metrics
type SyncMetrics struct {
	mutex                sync.RWMutex
	TotalUpdates         int64                    `json:"total_updates"`
	SuccessfulUpdates    int64                    `json:"successful_updates"`
	FailedUpdates        int64                    `json:"failed_updates"`
	UpdatesByType        map[types.UpdateType]int64     `json:"updates_by_type"`
	TotalSaves           int64                    `json:"total_saves"`
	SuccessfulSaves      int64                    `json:"successful_saves"`
	FailedSaves          int64                    `json:"failed_saves"`
	AverageUpdateLatency time.Duration           `json:"average_update_latency"`
	AverageSaveLatency   time.Duration           `json:"average_save_latency"`
	LastUpdateTime       time.Time               `json:"last_update_time"`
	LastSaveTime         time.Time               `json:"last_save_time"`
	InitializationTime   time.Time               `json:"initialization_time"`
	IsInitialized        bool                    `json:"is_initialized"`
}

// NewSyncMetrics creates a new sync metrics tracker
func NewSyncMetrics() *SyncMetrics {
	return &SyncMetrics{
		UpdatesByType: make(map[types.UpdateType]int64),
	}
}

// RecordUpdate records statistics for a state update
func (m *SyncMetrics) RecordUpdate(updateType types.UpdateType, success bool, duration time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.TotalUpdates++
	m.UpdatesByType[updateType]++
	m.LastUpdateTime = time.Now()

	if success {
		m.SuccessfulUpdates++
	} else {
		m.FailedUpdates++
	}

	// Update average latency (simple moving average)
	if m.TotalUpdates == 1 {
		m.AverageUpdateLatency = duration
	} else {
		m.AverageUpdateLatency = (m.AverageUpdateLatency + duration) / 2
	}
}

// RecordSave records statistics for a save operation
func (m *SyncMetrics) RecordSave(success bool, duration time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.TotalSaves++
	m.LastSaveTime = time.Now()

	if success {
		m.SuccessfulSaves++
	} else {
		m.FailedSaves++
	}

	// Update average latency
	if m.TotalSaves == 1 {
		m.AverageSaveLatency = duration
	} else {
		m.AverageSaveLatency = (m.AverageSaveLatency + duration) / 2
	}
}

// RecordInitialization records initialization status
func (m *SyncMetrics) RecordInitialization(success bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.IsInitialized = success
	m.InitializationTime = time.Now()
}

// GetSuccessRate returns the success rate for updates
func (m *SyncMetrics) GetSuccessRate() float64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.TotalUpdates == 0 {
		return 100.0
	}

	return float64(m.SuccessfulUpdates) / float64(m.TotalUpdates) * 100.0
}

// GetSaveSuccessRate returns the success rate for saves
func (m *SyncMetrics) GetSaveSuccessRate() float64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.TotalSaves == 0 {
		return 100.0
	}

	return float64(m.SuccessfulSaves) / float64(m.TotalSaves) * 100.0
}

// IsHealthy returns true if metrics indicate healthy operation
func (m *SyncMetrics) IsHealthy() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// Consider healthy if:
	// - Initialized successfully
	// - Update success rate > 90%
	// - Save success rate > 95%
	// - Recent activity (within last 5 minutes)

	if !m.IsInitialized {
		return false
	}

	updateSuccessRate := m.GetSuccessRate()
	saveSuccessRate := m.GetSaveSuccessRate()

	if updateSuccessRate < 90.0 || saveSuccessRate < 95.0 {
		return false
	}

	// Check for recent activity
	if time.Since(m.LastUpdateTime) > 5*time.Minute && m.TotalUpdates > 0 {
		return false
	}

	return true
}