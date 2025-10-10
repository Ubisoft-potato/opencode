package recovery

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sst/opencode/internal/persistence"
	"github.com/sst/opencode/internal/state"
)

// RecoveryManager handles system failures and state recovery
type RecoveryManager struct {
	stateManager     *state.PanelSyncManager
	fileManager      *persistence.FileManager
	backupManager    *BackupManager
	healthMonitor    *HealthMonitor
	config           RecoveryConfig
	ctx              context.Context
	cancel           context.CancelFunc
	mutex            sync.RWMutex
	recoveryAttempts map[string]int
	lastRecoveryTime time.Time
	isRecovering     bool
}

// RecoveryConfig contains configuration for recovery operations
type RecoveryConfig struct {
	MaxRecoveryAttempts    int           `json:"max_recovery_attempts"`
	RecoveryRetryDelay     time.Duration `json:"recovery_retry_delay"`
	BackupInterval         time.Duration `json:"backup_interval"`
	HealthCheckInterval    time.Duration `json:"health_check_interval"`
	StateCorruptionTimeout time.Duration `json:"state_corruption_timeout"`
	AutoRecoveryEnabled    bool          `json:"auto_recovery_enabled"`
	BackupRetentionDays    int           `json:"backup_retention_days"`
}

// DefaultRecoveryConfig returns default recovery configuration
func DefaultRecoveryConfig() RecoveryConfig {
	return RecoveryConfig{
		MaxRecoveryAttempts:    3,
		RecoveryRetryDelay:     5 * time.Second,
		BackupInterval:         10 * time.Minute,
		HealthCheckInterval:    30 * time.Second,
		StateCorruptionTimeout: 30 * time.Second,
		AutoRecoveryEnabled:    true,
		BackupRetentionDays:    7,
	}
}

// NewRecoveryManager creates a new recovery manager
func NewRecoveryManager(
	stateManager *state.PanelSyncManager,
	fileManager *persistence.FileManager,
	config RecoveryConfig,
) *RecoveryManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &RecoveryManager{
		stateManager:     stateManager,
		fileManager:      fileManager,
		config:           config,
		ctx:              ctx,
		cancel:           cancel,
		recoveryAttempts: make(map[string]int),
		healthMonitor:    NewHealthMonitor(config.HealthCheckInterval),
		backupManager:    NewBackupManager(fileManager, config.BackupInterval, config.BackupRetentionDays),
	}
}

// Start begins the recovery manager operations
func (rm *RecoveryManager) Start() error {
	log.Printf("Starting recovery manager")

	// Start backup manager
	if err := rm.backupManager.Start(rm.ctx); err != nil {
		return fmt.Errorf("failed to start backup manager: %w", err)
	}

	// Start health monitor
	if err := rm.healthMonitor.Start(rm.ctx, rm); err != nil {
		return fmt.Errorf("failed to start health monitor: %w", err)
	}

	// Start recovery worker
	go rm.recoveryWorker()

	log.Printf("Recovery manager started successfully")
	return nil
}

// Stop gracefully shuts down the recovery manager
func (rm *RecoveryManager) Stop() error {
	log.Printf("Stopping recovery manager")

	// Cancel context
	rm.cancel()

	// Stop backup manager
	if err := rm.backupManager.Stop(); err != nil {
		log.Printf("Error stopping backup manager: %v", err)
	}

	// Stop health monitor
	if err := rm.healthMonitor.Stop(); err != nil {
		log.Printf("Error stopping health monitor: %v", err)
	}

	log.Printf("Recovery manager stopped")
	return nil
}

// RecoverFromFailure attempts to recover from a system failure
func (rm *RecoveryManager) RecoverFromFailure(failureType FailureType, context string) error {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	if rm.isRecovering {
		return fmt.Errorf("recovery already in progress")
	}

	rm.isRecovering = true
	defer func() { rm.isRecovering = false }()

	failureKey := fmt.Sprintf("%s_%s", failureType, context)
	attempts := rm.recoveryAttempts[failureKey]

	if attempts >= rm.config.MaxRecoveryAttempts {
		return &MaxRecoveryAttemptsExceededError{
			FailureType: failureType,
			Context:     context,
			Attempts:    attempts,
		}
	}

	rm.recoveryAttempts[failureKey]++
	rm.lastRecoveryTime = time.Now()

	log.Printf("Attempting recovery for %s (attempt %d/%d)",
		failureKey, attempts+1, rm.config.MaxRecoveryAttempts)

	var err error
	switch failureType {
	case StateCorruption:
		err = rm.recoverFromStateCorruption(context)
	case PanelCrash:
		err = rm.recoverFromPanelCrash(context)
	case IPCFailure:
		err = rm.recoverFromIPCFailure(context)
	case FileSystemError:
		err = rm.recoverFromFileSystemError(context)
	case NetworkError:
		err = rm.recoverFromNetworkError(context)
	default:
		err = rm.recoverFromGenericFailure(context)
	}

	if err == nil {
		// Reset recovery attempts on success
		delete(rm.recoveryAttempts, failureKey)
		log.Printf("Recovery successful for %s", failureKey)
	} else {
		log.Printf("Recovery failed for %s: %v", failureKey, err)

		// Wait before next attempt if we haven't exceeded max attempts
		if rm.recoveryAttempts[failureKey] < rm.config.MaxRecoveryAttempts {
			time.Sleep(rm.config.RecoveryRetryDelay)
		}
	}

	return err
}

// recoverFromStateCorruption attempts to recover from state corruption
func (rm *RecoveryManager) recoverFromStateCorruption(context string) error {
	log.Printf("Recovering from state corruption: %s", context)

	// Step 1: Try to load from backup
	if state, err := rm.loadFromLatestBackup(); err == nil {
		return rm.restoreState(state)
	}

	// Step 2: Try to reconstruct state from panels
	if state, err := rm.reconstructStateFromPanels(); err == nil {
		return rm.restoreState(state)
	}

	// Step 3: Initialize with default state
	log.Printf("All recovery methods failed, initializing default state")
	defaultState := state.NewSharedApplicationState()
	return rm.restoreState(defaultState)
}

// recoverFromPanelCrash attempts to recover from panel crashes
func (rm *RecoveryManager) recoverFromPanelCrash(panelID string) error {
	log.Printf("Recovering from panel crash: %s", panelID)

	// Get current state
	currentState := rm.stateManager.GetState()

	// Send state sync event to remaining panels
	event := state.StateEvent{
		ID:          "recovery_" + time.Now().Format("20060102150405"),
		Type:        state.EventStateSync,
		Data:        state.StateSyncPayload{State: currentState},
		Version:     currentState.Version.Version,
		SourcePanel: "recovery",
		Timestamp:   time.Now(),
	}

	rm.stateManager.GetEventBus().Broadcast(event)

	// Save current state to ensure persistence
	return rm.stateManager.SaveStateSync()
}

// recoverFromIPCFailure attempts to recover from IPC communication failures
func (rm *RecoveryManager) recoverFromIPCFailure(context string) error {
	log.Printf("Recovering from IPC failure: %s", context)

	// Force a full state synchronization
	return rm.stateManager.ForceFullSync()
}

// recoverFromFileSystemError attempts to recover from file system errors
func (rm *RecoveryManager) recoverFromFileSystemError(context string) error {
	log.Printf("Recovering from file system error: %s", context)

	// Check disk space
	if err := rm.checkDiskSpace(); err != nil {
		return fmt.Errorf("insufficient disk space: %w", err)
	}

	// Try to create backup directory if it doesn't exist
	backupDir := filepath.Dir(rm.fileManager.GetStats().BackupPath)
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Force state save to verify file system is working
	return rm.stateManager.SaveStateSync()
}

// recoverFromNetworkError attempts to recover from network errors
func (rm *RecoveryManager) recoverFromNetworkError(context string) error {
	log.Printf("Recovering from network error: %s", context)

	// For now, just log the error
	// In a real implementation, you might try to reconnect to services
	log.Printf("Network error recovery not yet implemented")
	return nil
}

// recoverFromGenericFailure attempts generic recovery procedures
func (rm *RecoveryManager) recoverFromGenericFailure(context string) error {
	log.Printf("Recovering from generic failure: %s", context)

	// Perform basic health checks and recovery
	if !rm.stateManager.IsHealthy() {
		return rm.stateManager.ForceFullSync()
	}

	return nil
}

// loadFromLatestBackup loads state from the most recent backup
func (rm *RecoveryManager) loadFromLatestBackup() (*state.SharedApplicationState, error) {
	backups, err := rm.backupManager.ListBackups()
	if err != nil {
		return nil, err
	}

	if len(backups) == 0 {
		return nil, fmt.Errorf("no backups available")
	}

	// Try to load from the most recent backup
	for _, backup := range backups {
		if loadedState, err := rm.backupManager.LoadBackup(backup.Path); err == nil {
			log.Printf("Successfully loaded state from backup: %s", backup.Path)
			return loadedState, nil
		}
	}

	return nil, fmt.Errorf("failed to load from any backup")
}

// reconstructStateFromPanels attempts to reconstruct state from connected panels
func (rm *RecoveryManager) reconstructStateFromPanels() (*state.SharedApplicationState, error) {
	// This is a placeholder implementation
	// In a real scenario, you would query connected panels for their state
	log.Printf("State reconstruction from panels not yet implemented")
	return nil, fmt.Errorf("state reconstruction not implemented")
}

// restoreState restores the given state to the system
func (rm *RecoveryManager) restoreState(restoredState *state.SharedApplicationState) error {
	// Save the restored state
	if err := rm.fileManager.SaveStateAtomic(restoredState); err != nil {
		return fmt.Errorf("failed to save restored state: %w", err)
	}

	// Force full synchronization
	if err := rm.stateManager.ForceFullSync(); err != nil {
		return fmt.Errorf("failed to synchronize restored state: %w", err)
	}

	log.Printf("State successfully restored")
	return nil
}

// checkDiskSpace checks if there's sufficient disk space
func (rm *RecoveryManager) checkDiskSpace() error {
	stats := rm.fileManager.GetStats()

	// Get file system info for the state path
	// This is a simplified check - in production you'd use syscall.Statfs
	if _, err := os.Stat(filepath.Dir(stats.StatePath)); err != nil {
		return fmt.Errorf("cannot access state directory: %w", err)
	}

	return nil
}

// recoveryWorker runs the recovery worker loop
func (rm *RecoveryManager) recoveryWorker() {
	ticker := time.NewTicker(rm.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rm.ctx.Done():
			return
		case <-ticker.C:
			if rm.config.AutoRecoveryEnabled {
				rm.performAutoRecovery()
			}
		}
	}
}

// performAutoRecovery performs automatic recovery checks
func (rm *RecoveryManager) performAutoRecovery() {
	// Check if state manager is healthy
	if !rm.stateManager.IsHealthy() {
		log.Printf("State manager unhealthy, attempting recovery")
		if err := rm.RecoverFromFailure(StateCorruption, "auto_health_check"); err != nil {
			log.Printf("Auto recovery failed: %v", err)
		}
	}

	// Check for stale state
	if rm.isStateStale() {
		log.Printf("State appears stale, forcing sync")
		if err := rm.stateManager.ForceFullSync(); err != nil {
			log.Printf("Failed to sync stale state: %v", err)
		}
	}
}

// isStateStale checks if the state appears to be stale
func (rm *RecoveryManager) isStateStale() bool {
	currentState := rm.stateManager.GetState()
	return time.Since(currentState.LastUpdate) > rm.config.StateCorruptionTimeout
}

// GetRecoveryStatistics returns recovery operation statistics
func (rm *RecoveryManager) GetRecoveryStatistics() RecoveryStatistics {
	rm.mutex.RLock()
	defer rm.mutex.RUnlock()

	totalAttempts := 0
	for _, attempts := range rm.recoveryAttempts {
		totalAttempts += attempts
	}

	return RecoveryStatistics{
		TotalRecoveryAttempts: totalAttempts,
		ActiveRecoveryTypes:   len(rm.recoveryAttempts),
		LastRecoveryTime:      rm.lastRecoveryTime,
		IsRecovering:         rm.isRecovering,
		BackupStatistics:     rm.backupManager.GetStatistics(),
		HealthStatistics:     rm.healthMonitor.GetStatistics(),
	}
}

// FailureType represents different types of system failures
type FailureType string

const (
	StateCorruption   FailureType = "state_corruption"
	PanelCrash        FailureType = "panel_crash"
	IPCFailure        FailureType = "ipc_failure"
	FileSystemError   FailureType = "filesystem_error"
	NetworkError      FailureType = "network_error"
	GenericFailure    FailureType = "generic_failure"
)

// RecoveryStatistics contains statistics about recovery operations
type RecoveryStatistics struct {
	TotalRecoveryAttempts int                  `json:"total_recovery_attempts"`
	ActiveRecoveryTypes   int                  `json:"active_recovery_types"`
	LastRecoveryTime      time.Time            `json:"last_recovery_time"`
	IsRecovering         bool                 `json:"is_recovering"`
	BackupStatistics     BackupStatistics     `json:"backup_statistics"`
	HealthStatistics     HealthStatistics     `json:"health_statistics"`
}

// Error types

// MaxRecoveryAttemptsExceededError indicates that recovery attempts have been exhausted
type MaxRecoveryAttemptsExceededError struct {
	FailureType FailureType `json:"failure_type"`
	Context     string      `json:"context"`
	Attempts    int         `json:"attempts"`
}

func (e *MaxRecoveryAttemptsExceededError) Error() string {
	return fmt.Sprintf("max recovery attempts exceeded for %s (%s): %d attempts",
		e.FailureType, e.Context, e.Attempts)
}

// RecoveryFailedError indicates that a recovery operation failed
type RecoveryFailedError struct {
	FailureType FailureType `json:"failure_type"`
	Context     string      `json:"context"`
	Reason      string      `json:"reason"`
}

func (e *RecoveryFailedError) Error() string {
	return fmt.Sprintf("recovery failed for %s (%s): %s",
		e.FailureType, e.Context, e.Reason)
}

// RecoveryManager interface for external components
type RecoveryHandler interface {
	RecoverFromFailure(failureType FailureType, context string) error
	GetRecoveryStatistics() RecoveryStatistics
}