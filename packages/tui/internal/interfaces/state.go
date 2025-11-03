package interfaces

import (
	"time"

	"github.com/sst/opencode/internal/types"
)

// StateRepository defines the interface for state persistence operations
type StateRepository interface {
	// SaveStateAtomic saves state to persistent storage using atomic operations
	SaveStateAtomic(state *types.SharedApplicationState) error

	// LoadStateAtomic loads state from persistent storage with integrity checks
	LoadStateAtomic() (*types.SharedApplicationState, error)

	// GetStats returns repository statistics and health information
	GetStats() RepositoryStats

	// Initialize sets up the repository and creates necessary resources
	Initialize() error
}

// StateSerializer defines the interface for state serialization/deserialization
type StateSerializer interface {
	// Serialize converts state to bytes for storage or transmission
	Serialize(state *types.SharedApplicationState) ([]byte, error)

	// Deserialize converts bytes back to state object
	Deserialize(data []byte) (*types.SharedApplicationState, error)

	// Validate checks if serialized data is valid and uncorrupted
	Validate(data []byte) error
}

// StateManager defines the interface for state management operations
type StateManager interface {
	// GetState returns a copy of the current state
	GetState() *types.SharedApplicationState

	// UpdateWithVersionCheck applies a state update with optimistic locking
	UpdateWithVersionCheck(update types.StateUpdate) error

	// ForceFullSync forces a complete state synchronization
	ForceFullSync() error

	// SaveStateSync saves the state synchronously
	SaveStateSync() error

	// IsHealthy returns true if the state manager is operating normally
	IsHealthy() bool

	// GetMetrics returns state management performance metrics
	GetMetrics() StateManagerMetrics

	// ClearSessionMessages clears all messages for a given session
	ClearSessionMessages(sessionID string, panelID string) error
}

// EventBus defines the interface for event distribution
type EventBus interface {
	// Subscribe registers a panel for state change notifications
	Subscribe(panelID, panelType string, eventChan chan types.StateEvent)

	// Unsubscribe removes a panel from event notifications
	Unsubscribe(panelID string)

	// Broadcast sends events to all registered panels except the source
	Broadcast(event types.StateEvent)

	// BroadcastToPanel sends an event specifically to one panel
	BroadcastToPanel(event types.StateEvent, targetPanel string)

	// GetSubscribers returns information about all current subscribers
	GetSubscribers() map[string]SubscriberInfo

	// GetEventHistory returns recent events from the history buffer
	GetEventHistory(maxEvents int) []types.StateEvent
}

// ConflictResolver defines the interface for resolving state conflicts
type ConflictResolver interface {
	// ResolveConflict attempts to resolve a state update conflict
	ResolveConflict(state StateManager, update types.StateUpdate) *ConflictResolutionResult

	// GetStatistics returns conflict resolution statistics
	GetStatistics() ConflictStatistics

	// UpdateConflictStrategy changes the conflict resolution strategy
	UpdateConflictStrategy(strategy ConflictStrategy)

	// IsHealthy returns true if the conflict resolver is performing well
	IsHealthy() bool
}

// BackupManager defines the interface for backup operations
type BackupManager interface {
	// CreateBackup creates a backup of the current state
	CreateBackup() (*BackupInfo, error)

	// LoadBackup loads state from a backup file
	LoadBackup(backupPath string) (*types.SharedApplicationState, error)

	// ListBackups returns a list of all available backups
	ListBackups() ([]BackupInfo, error)

	// DeleteBackup deletes a specific backup file
	DeleteBackup(backupPath string) error

	// GetLatestBackup returns information about the most recent backup
	GetLatestBackup() (*BackupInfo, error)

	// GetStatistics returns backup operation statistics
	GetStatistics() BackupStatistics

	// Start begins automatic backup operations
	Start() error

	// Stop gracefully shuts down the backup manager
	Stop() error
}

// HealthMonitor defines the interface for system health monitoring
type HealthMonitor interface {
	// RegisterHealthCheck registers a new health check
	RegisterHealthCheck(check HealthCheck)

	// UnregisterHealthCheck removes a health check
	UnregisterHealthCheck(name string)

	// GetHealthStatus returns the current health status
	GetHealthStatus() HealthStatus

	// GetStatistics returns health monitoring statistics
	GetStatistics() HealthStatistics

	// Start begins health monitoring
	Start() error

	// Stop gracefully shuts down the health monitor
	Stop() error
}

// RecoveryManager defines the interface for failure recovery
type RecoveryManager interface {
	// RecoverFromFailure attempts to recover from a system failure
	RecoverFromFailure(failureType FailureType, context string) error

	// GetRecoveryStatistics returns recovery operation statistics
	GetRecoveryStatistics() RecoveryStatistics

	// Start begins the recovery manager operations
	Start() error

	// Stop gracefully shuts down the recovery manager
	Stop() error
}

// Supporting types and structures

// RepositoryStats contains statistics about repository operations
type RepositoryStats struct {
	StatePath  string    `json:"state_path"`
	FileSize   int64     `json:"file_size"`
	ModTime    time.Time `json:"mod_time"`
	IsLocked   bool      `json:"is_locked"`
	BackupPath string    `json:"backup_path"`
}

// StateManagerMetrics contains performance metrics for state management
type StateManagerMetrics struct {
	TotalUpdates         int64                      `json:"total_updates"`
	SuccessfulUpdates    int64                      `json:"successful_updates"`
	FailedUpdates        int64                      `json:"failed_updates"`
	UpdatesByType        map[types.UpdateType]int64 `json:"updates_by_type"`
	TotalSaves           int64                      `json:"total_saves"`
	SuccessfulSaves      int64                      `json:"successful_saves"`
	FailedSaves          int64                      `json:"failed_saves"`
	AverageUpdateLatency time.Duration              `json:"average_update_latency"`
	AverageSaveLatency   time.Duration              `json:"average_save_latency"`
	LastUpdateTime       time.Time                  `json:"last_update_time"`
	LastSaveTime         time.Time                  `json:"last_save_time"`
}

// GetSuccessRate returns the success rate for updates
func (m *StateManagerMetrics) GetSuccessRate() float64 {
	if m.TotalUpdates == 0 {
		return 100.0
	}
	return float64(m.SuccessfulUpdates) / float64(m.TotalUpdates) * 100.0
}

// GetSaveSuccessRate returns the success rate for saves
func (m *StateManagerMetrics) GetSaveSuccessRate() float64 {
	if m.TotalSaves == 0 {
		return 100.0
	}
	return float64(m.SuccessfulSaves) / float64(m.TotalSaves) * 100.0
}

// SubscriberInfo contains metadata about event subscribers
type SubscriberInfo struct {
	PanelID     string    `json:"panel_id"`
	PanelType   string    `json:"panel_type"`
	ConnectedAt time.Time `json:"connected_at"`
	LastEventAt time.Time `json:"last_event_at"`
	EventCount  int64     `json:"event_count"`
}

// ConflictResolutionResult represents the outcome of conflict resolution
type ConflictResolutionResult struct {
	Success      bool             `json:"success"`
	Attempts     int              `json:"attempts"`
	Strategy     ConflictStrategy `json:"strategy"`
	FinalVersion int64            `json:"final_version"`
	TimeTaken    time.Duration    `json:"time_taken"`
	Error        error            `json:"error,omitempty"`
}

// ConflictStrategy defines how to resolve state conflicts
type ConflictStrategy string

const (
	// LastWriteWins uses timestamp to determine winner
	LastWriteWins ConflictStrategy = "last_write_wins"
	// VersionBased uses version numbers for conflict resolution
	VersionBased ConflictStrategy = "version_based"
	// ManualResolve requires manual intervention
	ManualResolve ConflictStrategy = "manual_resolve"
)

// ConflictStatistics provides metrics about conflict resolution performance
type ConflictStatistics struct {
	TotalAttempts int64            `json:"total_attempts"`
	SuccessCount  int64            `json:"success_count"`
	ConflictCount int64            `json:"conflict_count"`
	RetryCount    int64            `json:"retry_count"`
	SuccessRate   float64          `json:"success_rate"`
	Strategy      ConflictStrategy `json:"strategy"`
}

// BackupInfo contains information about a backup file
type BackupInfo struct {
	Path         string    `json:"path"`
	Timestamp    time.Time `json:"timestamp"`
	Size         int64     `json:"size"`
	StateVersion int64     `json:"state_version"`
	IsValid      bool      `json:"is_valid"`
}

// BackupStatistics contains backup operation statistics
type BackupStatistics struct {
	TotalBackups      int64     `json:"total_backups"`
	SuccessfulBackups int64     `json:"successful_backups"`
	FailedBackups     int64     `json:"failed_backups"`
	LastBackupTime    time.Time `json:"last_backup_time"`
	TotalBackupSize   int64     `json:"total_backup_size"`
	AverageBackupSize int64     `json:"average_backup_size"`
}

// HealthCheck represents a health check function
type HealthCheck struct {
	Name        string                   `json:"name"`
	Description string                   `json:"description"`
	CheckFunc   func() HealthCheckResult `json:"-"`
	Interval    time.Duration            `json:"interval"`
	LastCheck   time.Time                `json:"last_check"`
	LastResult  HealthCheckResult        `json:"last_result"`
	Enabled     bool                     `json:"enabled"`
}

// HealthCheckResult represents the result of a health check
type HealthCheckResult struct {
	Healthy   bool                   `json:"healthy"`
	Message   string                 `json:"message"`
	Duration  time.Duration          `json:"duration"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// HealthStatus represents the current health status
type HealthStatus struct {
	OverallHealthy bool                         `json:"overall_healthy"`
	CheckResults   map[string]HealthCheckResult `json:"check_results"`
	Timestamp      time.Time                    `json:"timestamp"`
}

// HealthStatistics contains health monitoring statistics
type HealthStatistics struct {
	TotalChecks         int64                 `json:"total_checks"`
	HealthyChecks       int64                 `json:"healthy_checks"`
	UnhealthyChecks     int64                 `json:"unhealthy_checks"`
	ChecksByName        map[string]CheckStats `json:"checks_by_name"`
	LastCheckTime       time.Time             `json:"last_check_time"`
	AverageCheckTime    time.Duration         `json:"average_check_time"`
	OverallHealthy      bool                  `json:"overall_healthy"`
	AlertsTriggered     int64                 `json:"alerts_triggered"`
	RecoveriesTriggered int64                 `json:"recoveries_triggered"`
}

// CheckStats contains statistics for individual health checks
type CheckStats struct {
	TotalRuns           int64         `json:"total_runs"`
	SuccessfulRuns      int64         `json:"successful_runs"`
	FailedRuns          int64         `json:"failed_runs"`
	ConsecutiveFailures int           `json:"consecutive_failures"`
	LastSuccess         time.Time     `json:"last_success"`
	LastFailure         time.Time     `json:"last_failure"`
	AverageRunTime      time.Duration `json:"average_run_time"`
	SuccessRate         float64       `json:"success_rate"`
}

// FailureType represents different types of system failures
type FailureType string

const (
	StateCorruption FailureType = "state_corruption"
	PanelCrash      FailureType = "panel_crash"
	IPCFailure      FailureType = "ipc_failure"
	FileSystemError FailureType = "filesystem_error"
	NetworkError    FailureType = "network_error"
	GenericFailure  FailureType = "generic_failure"
)

// RecoveryStatistics contains statistics about recovery operations
type RecoveryStatistics struct {
	TotalRecoveryAttempts int              `json:"total_recovery_attempts"`
	ActiveRecoveryTypes   int              `json:"active_recovery_types"`
	LastRecoveryTime      time.Time        `json:"last_recovery_time"`
	IsRecovering          bool             `json:"is_recovering"`
	BackupStatistics      BackupStatistics `json:"backup_statistics"`
	HealthStatistics      HealthStatistics `json:"health_statistics"`
}
