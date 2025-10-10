package recovery

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sst/opencode/internal/persistence"
	"github.com/sst/opencode/internal/state"
)

// BackupManager handles automatic backup creation and management
type BackupManager struct {
	fileManager      *persistence.FileManager
	backupInterval   time.Duration
	retentionDays    int
	backupDirectory  string
	ctx              context.Context
	cancel           context.CancelFunc
	mutex            sync.RWMutex
	statistics       BackupStatistics
	isRunning        bool
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
	TotalBackups       int64     `json:"total_backups"`
	SuccessfulBackups  int64     `json:"successful_backups"`
	FailedBackups      int64     `json:"failed_backups"`
	LastBackupTime     time.Time `json:"last_backup_time"`
	LastCleanupTime    time.Time `json:"last_cleanup_time"`
	TotalBackupSize    int64     `json:"total_backup_size"`
	OldestBackupTime   time.Time `json:"oldest_backup_time"`
	BackupsDeleted     int64     `json:"backups_deleted"`
	AverageBackupSize  int64     `json:"average_backup_size"`
}

// NewBackupManager creates a new backup manager
func NewBackupManager(fileManager *persistence.FileManager, backupInterval time.Duration, retentionDays int) *BackupManager {
	stats := fileManager.GetStats()
	backupDirectory := filepath.Dir(stats.BackupPath)

	return &BackupManager{
		fileManager:     fileManager,
		backupInterval:  backupInterval,
		retentionDays:   retentionDays,
		backupDirectory: backupDirectory,
		statistics:      BackupStatistics{},
	}
}

// Start begins automatic backup operations
func (bm *BackupManager) Start(ctx context.Context) error {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	if bm.isRunning {
		return fmt.Errorf("backup manager is already running")
	}

	bm.ctx, bm.cancel = context.WithCancel(ctx)
	bm.isRunning = true

	// Create backup directory if it doesn't exist
	if err := os.MkdirAll(bm.backupDirectory, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Start backup worker
	go bm.backupWorker()

	// Start cleanup worker
	go bm.cleanupWorker()

	log.Printf("Backup manager started (interval: %v, retention: %d days)",
		bm.backupInterval, bm.retentionDays)
	return nil
}

// Stop gracefully shuts down the backup manager
func (bm *BackupManager) Stop() error {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	if !bm.isRunning {
		return nil
	}

	if bm.cancel != nil {
		bm.cancel()
	}

	bm.isRunning = false
	log.Printf("Backup manager stopped")
	return nil
}

// CreateBackup creates a backup of the current state
func (bm *BackupManager) CreateBackup() (*BackupInfo, error) {
	startTime := time.Now()

	// Load current state
	currentState, err := bm.fileManager.LoadStateAtomic()
	if err != nil {
		bm.recordBackupAttempt(false)
		return nil, fmt.Errorf("failed to load current state: %w", err)
	}

	// Generate backup filename
	timestamp := startTime.Format("20060102_150405")
	filename := fmt.Sprintf("state_backup_%s_v%d.json", timestamp, currentState.Version.Version)
	backupPath := filepath.Join(bm.backupDirectory, filename)

	// Create backup file
	tempPath := backupPath + ".tmp"
	backupFile, err := os.Create(tempPath)
	if err != nil {
		bm.recordBackupAttempt(false)
		return nil, fmt.Errorf("failed to create backup file: %w", err)
	}
	defer backupFile.Close()

	// Write state to backup file
	tempFileManager := persistence.NewFileManager(persistence.FileManagerConfig{
		StatePath: tempPath,
	})

	if err := tempFileManager.SaveStateAtomic(currentState); err != nil {
		os.Remove(tempPath)
		bm.recordBackupAttempt(false)
		return nil, fmt.Errorf("failed to write backup: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, backupPath); err != nil {
		os.Remove(tempPath)
		bm.recordBackupAttempt(false)
		return nil, fmt.Errorf("failed to finalize backup: %w", err)
	}

	// Get file info
	fileInfo, err := os.Stat(backupPath)
	if err != nil {
		bm.recordBackupAttempt(false)
		return nil, fmt.Errorf("failed to stat backup file: %w", err)
	}

	backupInfo := &BackupInfo{
		Path:         backupPath,
		Timestamp:    startTime,
		Size:         fileInfo.Size(),
		StateVersion: currentState.Version.Version,
		IsValid:      true,
	}

	bm.recordBackupAttempt(true)
	bm.updateBackupStatistics(backupInfo)

	log.Printf("Created backup: %s (size: %d bytes, version: %d)",
		filename, backupInfo.Size, backupInfo.StateVersion)

	return backupInfo, nil
}

// LoadBackup loads state from a backup file
func (bm *BackupManager) LoadBackup(backupPath string) (*state.SharedApplicationState, error) {
	// Verify backup file exists
	if _, err := os.Stat(backupPath); err != nil {
		return nil, fmt.Errorf("backup file not found: %w", err)
	}

	// Create temporary file manager for the backup
	tempFileManager := persistence.NewFileManager(persistence.FileManagerConfig{
		StatePath: backupPath,
	})

	// Load state from backup
	loadedState, err := tempFileManager.LoadStateAtomic()
	if err != nil {
		return nil, fmt.Errorf("failed to load backup: %w", err)
	}

	log.Printf("Loaded state from backup: %s (version: %d)",
		filepath.Base(backupPath), loadedState.Version.Version)

	return loadedState, nil
}

// ListBackups returns a list of all available backups
func (bm *BackupManager) ListBackups() ([]BackupInfo, error) {
	files, err := os.ReadDir(bm.backupDirectory)
	if err != nil {
		return nil, fmt.Errorf("failed to read backup directory: %w", err)
	}

	var backups []BackupInfo
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), "state_backup_") && strings.HasSuffix(file.Name(), ".json") {
			backupPath := filepath.Join(bm.backupDirectory, file.Name())
			if backupInfo := bm.parseBackupInfo(backupPath); backupInfo != nil {
				backups = append(backups, *backupInfo)
			}
		}
	}

	// Sort by timestamp (newest first)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].Timestamp.After(backups[j].Timestamp)
	})

	return backups, nil
}

// DeleteBackup deletes a specific backup file
func (bm *BackupManager) DeleteBackup(backupPath string) error {
	if err := os.Remove(backupPath); err != nil {
		return fmt.Errorf("failed to delete backup: %w", err)
	}

	bm.mutex.Lock()
	bm.statistics.BackupsDeleted++
	bm.mutex.Unlock()

	log.Printf("Deleted backup: %s", filepath.Base(backupPath))
	return nil
}

// GetLatestBackup returns information about the most recent backup
func (bm *BackupManager) GetLatestBackup() (*BackupInfo, error) {
	backups, err := bm.ListBackups()
	if err != nil {
		return nil, err
	}

	if len(backups) == 0 {
		return nil, fmt.Errorf("no backups available")
	}

	return &backups[0], nil
}

// GetStatistics returns backup operation statistics
func (bm *BackupManager) GetStatistics() BackupStatistics {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()

	// Create a copy to avoid race conditions
	stats := bm.statistics

	// Update calculated fields
	if stats.TotalBackups > 0 {
		stats.AverageBackupSize = stats.TotalBackupSize / stats.TotalBackups
	}

	return stats
}

// backupWorker runs the automatic backup creation loop
func (bm *BackupManager) backupWorker() {
	ticker := time.NewTicker(bm.backupInterval)
	defer ticker.Stop()

	// Create initial backup
	if _, err := bm.CreateBackup(); err != nil {
		log.Printf("Failed to create initial backup: %v", err)
	}

	for {
		select {
		case <-bm.ctx.Done():
			return
		case <-ticker.C:
			if _, err := bm.CreateBackup(); err != nil {
				log.Printf("Failed to create scheduled backup: %v", err)
			}
		}
	}
}

// cleanupWorker runs the backup cleanup loop
func (bm *BackupManager) cleanupWorker() {
	// Run cleanup every hour
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	// Run initial cleanup
	bm.cleanupOldBackups()

	for {
		select {
		case <-bm.ctx.Done():
			return
		case <-ticker.C:
			bm.cleanupOldBackups()
		}
	}
}

// cleanupOldBackups removes backups older than the retention period
func (bm *BackupManager) cleanupOldBackups() {
	if bm.retentionDays <= 0 {
		return // Retention disabled
	}

	cutoffTime := time.Now().AddDate(0, 0, -bm.retentionDays)

	backups, err := bm.ListBackups()
	if err != nil {
		log.Printf("Failed to list backups for cleanup: %v", err)
		return
	}

	deletedCount := 0
	for _, backup := range backups {
		if backup.Timestamp.Before(cutoffTime) {
			if err := bm.DeleteBackup(backup.Path); err != nil {
				log.Printf("Failed to delete old backup %s: %v", backup.Path, err)
			} else {
				deletedCount++
			}
		}
	}

	if deletedCount > 0 {
		log.Printf("Cleaned up %d old backups", deletedCount)
	}

	bm.mutex.Lock()
	bm.statistics.LastCleanupTime = time.Now()
	bm.mutex.Unlock()
}

// parseBackupInfo extracts information from a backup file
func (bm *BackupManager) parseBackupInfo(backupPath string) *BackupInfo {
	fileInfo, err := os.Stat(backupPath)
	if err != nil {
		return nil
	}

	// Parse timestamp from filename
	filename := filepath.Base(backupPath)
	// Format: state_backup_20060102_150405_v123.json
	parts := strings.Split(filename, "_")
	if len(parts) < 4 {
		return nil
	}

	timestampStr := parts[2] + "_" + strings.Split(parts[3], "_")[0]
	timestamp, err := time.Parse("20060102_150405", timestampStr)
	if err != nil {
		// Fallback to file modification time
		timestamp = fileInfo.ModTime()
	}

	// Try to parse version from filename
	var stateVersion int64 = 0
	if strings.Contains(filename, "_v") {
		versionPart := strings.Split(filename, "_v")[1]
		versionPart = strings.Split(versionPart, ".")[0]
		fmt.Sscanf(versionPart, "%d", &stateVersion)
	}

	// Validate backup by trying to load it
	isValid := bm.validateBackup(backupPath)

	return &BackupInfo{
		Path:         backupPath,
		Timestamp:    timestamp,
		Size:         fileInfo.Size(),
		StateVersion: stateVersion,
		IsValid:      isValid,
	}
}

// validateBackup checks if a backup file is valid
func (bm *BackupManager) validateBackup(backupPath string) bool {
	tempFileManager := persistence.NewFileManager(persistence.FileManagerConfig{
		StatePath: backupPath,
	})

	_, err := tempFileManager.LoadStateAtomic()
	return err == nil
}

// recordBackupAttempt records statistics for a backup attempt
func (bm *BackupManager) recordBackupAttempt(success bool) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	bm.statistics.TotalBackups++
	if success {
		bm.statistics.SuccessfulBackups++
		bm.statistics.LastBackupTime = time.Now()
	} else {
		bm.statistics.FailedBackups++
	}
}

// updateBackupStatistics updates statistics after a successful backup
func (bm *BackupManager) updateBackupStatistics(backupInfo *BackupInfo) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	bm.statistics.TotalBackupSize += backupInfo.Size

	// Update oldest backup time
	if bm.statistics.OldestBackupTime.IsZero() || backupInfo.Timestamp.Before(bm.statistics.OldestBackupTime) {
		bm.statistics.OldestBackupTime = backupInfo.Timestamp
	}
}

// IsRunning returns true if the backup manager is currently running
func (bm *BackupManager) IsRunning() bool {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	return bm.isRunning
}

// ForceBackup forces an immediate backup creation
func (bm *BackupManager) ForceBackup() (*BackupInfo, error) {
	if !bm.isRunning {
		return nil, fmt.Errorf("backup manager is not running")
	}

	return bm.CreateBackup()
}

// GetBackupHealth returns the health status of the backup system
func (bm *BackupManager) GetBackupHealth() BackupHealth {
	stats := bm.GetStatistics()
	backups, err := bm.ListBackups()

	health := BackupHealth{
		IsHealthy:        true,
		Issues:           make([]string, 0),
		LastBackupAge:    time.Since(stats.LastBackupTime),
		BackupCount:      len(backups),
		TotalBackupSize:  stats.TotalBackupSize,
		SuccessRate:      0,
	}

	if stats.TotalBackups > 0 {
		health.SuccessRate = float64(stats.SuccessfulBackups) / float64(stats.TotalBackups) * 100
	}

	// Check for issues
	if len(backups) == 0 {
		health.IsHealthy = false
		health.Issues = append(health.Issues, "No backups available")
	}

	if health.LastBackupAge > 2*bm.backupInterval {
		health.IsHealthy = false
		health.Issues = append(health.Issues, "Last backup is too old")
	}

	if health.SuccessRate < 90 && stats.TotalBackups > 5 {
		health.IsHealthy = false
		health.Issues = append(health.Issues, "Backup success rate too low")
	}

	// Check for invalid backups
	invalidCount := 0
	for _, backup := range backups {
		if !backup.IsValid {
			invalidCount++
		}
	}

	if invalidCount > 0 {
		health.Issues = append(health.Issues, fmt.Sprintf("%d invalid backups found", invalidCount))
		if invalidCount > len(backups)/2 {
			health.IsHealthy = false
		}
	}

	return health
}

// BackupHealth represents the health status of the backup system
type BackupHealth struct {
	IsHealthy       bool          `json:"is_healthy"`
	Issues          []string      `json:"issues"`
	LastBackupAge   time.Duration `json:"last_backup_age"`
	BackupCount     int           `json:"backup_count"`
	TotalBackupSize int64         `json:"total_backup_size"`
	SuccessRate     float64       `json:"success_rate"`
}