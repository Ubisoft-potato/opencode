package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/sst/opencode/internal/interfaces"
	"github.com/sst/opencode/internal/types"
)

// FileManager handles atomic file operations for state persistence
// Implements the interfaces.StateRepository interface
type FileManager struct {
	statePath          string
	lockPath           string
	backupPath         string
	tempDir            string
	lockTimeout        time.Duration
	fileLock           *os.File
	lockMutex          sync.Mutex
	compressionEnabled bool
	backupRotation     int
}

// FileManagerConfig contains configuration for file manager
type FileManagerConfig struct {
	StatePath          string        `json:"state_path"`
	LockTimeout        time.Duration `json:"lock_timeout"`
	CompressionEnabled bool          `json:"compression_enabled"`
	BackupRotation     int           `json:"backup_rotation"`
	TempDir            string        `json:"temp_dir"`
}

// DefaultFileManagerConfig returns default configuration
func DefaultFileManagerConfig(statePath string) FileManagerConfig {
	dir := filepath.Dir(statePath)
	return FileManagerConfig{
		StatePath:          statePath,
		LockTimeout:        30 * time.Second,
		CompressionEnabled: false,
		BackupRotation:     5,
		TempDir:            filepath.Join(dir, "tmp"),
	}
}

// NewFileManager creates a new file manager with specified configuration
func NewFileManager(config FileManagerConfig) *FileManager {
	return &FileManager{
		statePath:          config.StatePath,
		lockPath:           config.StatePath + ".lock",
		backupPath:         config.StatePath + ".backup",
		tempDir:            config.TempDir,
		lockTimeout:        config.LockTimeout,
		compressionEnabled: config.CompressionEnabled,
		backupRotation:     config.BackupRotation,
	}
}

// Initialize sets up the file manager and creates necessary directories
func (fm *FileManager) Initialize() error {
	// Create directories if they don't exist
	dirs := []string{
		filepath.Dir(fm.statePath),
		fm.tempDir,
		filepath.Dir(fm.backupPath),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return nil
}

// SaveStateAtomic saves state to file using atomic operations
func (fm *FileManager) SaveStateAtomic(state *types.SharedApplicationState) error {
	// Acquire file lock
	if err := fm.acquireFileLock(); err != nil {
		return fmt.Errorf("failed to acquire file lock: %w", err)
	}
	defer fm.releaseFileLock()

	// Create temporary file
	tempFile, err := fm.createTempFile()
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	tempPath := tempFile.Name()
	defer func() {
		tempFile.Close()
		os.Remove(tempPath) // Clean up on any error
	}()

	// Serialize and write state
	if err := fm.writeStateToFile(state, tempFile); err != nil {
		return fmt.Errorf("failed to write state: %w", err)
	}

	// Sync to disk
	if err := tempFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	// Close temp file before rename
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Backup existing file if it exists
	if err := fm.backupExistingFile(); err != nil {
		return fmt.Errorf("failed to backup existing file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, fm.statePath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// LoadStateAtomic loads state from file with integrity checks
func (fm *FileManager) LoadStateAtomic() (*types.SharedApplicationState, error) {
	// Acquire file lock
	if err := fm.acquireFileLock(); err != nil {
		return nil, fmt.Errorf("failed to acquire file lock: %w", err)
	}
	defer fm.releaseFileLock()

	// Check if state file exists
	if _, err := os.Stat(fm.statePath); os.IsNotExist(err) {
		return nil, &FileNotFoundError{Path: fm.statePath}
	}

	// Open and read state file
	file, err := os.Open(fm.statePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open state file: %w", err)
	}
	defer file.Close()

	// Verify file integrity
	if err := fm.verifyFileIntegrity(file); err != nil {
		// Try to load from backup
		return fm.loadFromBackup()
	}

	// Decode state
	var state types.SharedApplicationState
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&state); err != nil {
		// Try to load from backup on decode error
		return fm.loadFromBackup()
	}

	// Validate state structure
	if err := fm.validateState(&state); err != nil {
		return nil, fmt.Errorf("state validation failed: %w", err)
	}

	return &state, nil
}

// acquireFileLock acquires an exclusive file lock
func (fm *FileManager) acquireFileLock() error {
	fm.lockMutex.Lock()
	defer fm.lockMutex.Unlock()

	if fm.fileLock != nil {
		deadline := time.Now().Add(fm.lockTimeout)
		for fm.fileLock != nil && time.Now().Before(deadline) {
			fm.lockMutex.Unlock()
			time.Sleep(50 * time.Millisecond)
			fm.lockMutex.Lock()
		}
		if fm.fileLock != nil {
			return fmt.Errorf("lock already held")
		}
	}

	// Retry loop for stale lock handling
	for attempts := 0; attempts < 3; attempts++ {
		// Create lock file
		lockFile, err := os.OpenFile(fm.lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			if os.IsExist(err) {
				// Lock file exists, check if it's stale
				if err := fm.handleStaleLock(); err != nil {
					return err
				}
				// If handleStaleLock returned nil, the stale lock was removed, retry
				continue
			}
			return fmt.Errorf("failed to create lock file: %w", err)
		}

		// Write process ID to lock file
		if _, err := lockFile.WriteString(fmt.Sprintf("%d", os.Getpid())); err != nil {
			lockFile.Close()
			os.Remove(fm.lockPath)
			return fmt.Errorf("failed to write to lock file: %w", err)
		}

		// Apply exclusive lock using flock
		if err := fm.flockFile(lockFile); err != nil {
			lockFile.Close()
			os.Remove(fm.lockPath)
			return fmt.Errorf("failed to flock file: %w", err)
		}

		fm.fileLock = lockFile
		return nil
	}

	return fmt.Errorf("failed to acquire lock after multiple attempts")
}

// releaseFileLock releases the file lock
func (fm *FileManager) releaseFileLock() error {
	fm.lockMutex.Lock()
	defer fm.lockMutex.Unlock()

	if fm.fileLock == nil {
		return nil
	}

	// Close file (releases flock)
	if err := fm.fileLock.Close(); err != nil {
		return fmt.Errorf("failed to close lock file: %w", err)
	}

	// Remove lock file
	if err := os.Remove(fm.lockPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove lock file: %w", err)
	}

	fm.fileLock = nil
	return nil
}

// handleStaleLock checks if a lock file is stale and removes it if so
func (fm *FileManager) handleStaleLock() error {
	// Check lock file age
	stat, err := os.Stat(fm.lockPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// If lock file is older than timeout, consider it stale
	if time.Since(stat.ModTime()) > fm.lockTimeout {
		if err := os.Remove(fm.lockPath); err != nil {
			return fmt.Errorf("failed to remove stale lock: %w", err)
		}
		// Don't recursively call acquireFileLock as it would cause deadlock
		// Instead, return nil to indicate the stale lock was removed
		// The caller should retry the lock acquisition
		return nil
	}

	return &LockTimeoutError{Path: fm.lockPath, Timeout: fm.lockTimeout}
}

// flockFile applies an exclusive lock to a file
func (fm *FileManager) flockFile(file *os.File) error {
	// Use Unix flock system call
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

// createTempFile creates a temporary file for atomic writes
func (fm *FileManager) createTempFile() (*os.File, error) {
	// Ensure temp directory exists
	if err := os.MkdirAll(fm.tempDir, 0755); err != nil {
		return nil, err
	}

	// Create temporary file with unique name
	pattern := fmt.Sprintf("state_%d_*.tmp", os.Getpid())
	return os.CreateTemp(fm.tempDir, pattern)
}

// writeStateToFile writes state data to a file
func (fm *FileManager) writeStateToFile(state *types.SharedApplicationState, file *os.File) error {
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // Pretty print for debugging

	// Add metadata header
	metadata := StateMetadata{
		Version:   "1.0",
		Timestamp: time.Now(),
		Checksum:  "", // Will be calculated after serialization
	}

	// Write metadata first
	if err := encoder.Encode(metadata); err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}

	// Write state data
	if err := encoder.Encode(state); err != nil {
		return fmt.Errorf("failed to encode state: %w", err)
	}

	return nil
}

// verifyFileIntegrity checks file integrity and structure
func (fm *FileManager) verifyFileIntegrity(file *os.File) error {
	// Seek to beginning
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}

	// Read and verify metadata
	decoder := json.NewDecoder(file)
	var metadata StateMetadata
	if err := decoder.Decode(&metadata); err != nil {
		return &CorruptionError{Path: fm.statePath, Reason: "invalid metadata"}
	}

	// Basic metadata validation
	if metadata.Version == "" {
		return &CorruptionError{Path: fm.statePath, Reason: "missing version"}
	}

	// Reset file position for subsequent reads
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}

	return nil
}

// validateState performs basic state validation
func (fm *FileManager) validateState(state *types.SharedApplicationState) error {
	if state.Version.Version <= 0 {
		return &ValidationError{Field: "version", Message: "must be positive"}
	}

	if state.Version.Timestamp.IsZero() {
		return &ValidationError{Field: "timestamp", Message: "cannot be zero"}
	}

	// Validate sessions have unique IDs
	sessionIDs := make(map[string]bool)
	for _, session := range state.Sessions {
		if session.ID == "" {
			return &ValidationError{Field: "session.id", Message: "cannot be empty"}
		}
		if sessionIDs[session.ID] {
			return &ValidationError{Field: "session.id", Message: "duplicate session ID"}
		}
		sessionIDs[session.ID] = true
	}

	// Validate current session exists if set
	if state.CurrentSessionID != "" {
		if !sessionIDs[state.CurrentSessionID] {
			return &ValidationError{Field: "current_session_id", Message: "references non-existent session"}
		}
	}

	return nil
}

// backupExistingFile creates a backup of the existing state file
func (fm *FileManager) backupExistingFile() error {
	// Check if state file exists
	if _, err := os.Stat(fm.statePath); os.IsNotExist(err) {
		return nil // No file to backup
	}

	// Rotate existing backups
	if err := fm.rotateBackups(); err != nil {
		return err
	}

	// Copy current file to backup
	return fm.copyFile(fm.statePath, fm.backupPath)
}

// rotateBackups manages backup file rotation
func (fm *FileManager) rotateBackups() error {
	// If backup rotation is disabled, just remove the old backup
	if fm.backupRotation <= 1 {
		if _, err := os.Stat(fm.backupPath); err == nil {
			return os.Remove(fm.backupPath)
		}
		return nil
	}

	// Rotate numbered backups
	for i := fm.backupRotation - 1; i > 0; i-- {
		oldPath := fmt.Sprintf("%s.%d", fm.backupPath, i)
		newPath := fmt.Sprintf("%s.%d", fm.backupPath, i+1)

		if _, err := os.Stat(oldPath); err == nil {
			if i == fm.backupRotation-1 {
				// Remove oldest backup
				os.Remove(newPath)
			}
			os.Rename(oldPath, newPath)
		}
	}

	// Move current backup to .1
	if _, err := os.Stat(fm.backupPath); err == nil {
		return os.Rename(fm.backupPath, fm.backupPath+".1")
	}

	return nil
}

// loadFromBackup attempts to load state from backup file
func (fm *FileManager) loadFromBackup() (*types.SharedApplicationState, error) {
	backupPaths := []string{fm.backupPath}

	// Add numbered backups
	for i := 1; i <= fm.backupRotation; i++ {
		backupPaths = append(backupPaths, fmt.Sprintf("%s.%d", fm.backupPath, i))
	}

	for _, backupPath := range backupPaths {
		if _, err := os.Stat(backupPath); os.IsNotExist(err) {
			continue
		}

		file, err := os.Open(backupPath)
		if err != nil {
			continue
		}

		var state types.SharedApplicationState
		decoder := json.NewDecoder(file)

		// Skip metadata
		var metadata StateMetadata
		decoder.Decode(&metadata)

		if err := decoder.Decode(&state); err != nil {
			file.Close()
			continue
		}

		file.Close()

		// Validate backup state
		if err := fm.validateState(&state); err != nil {
			continue
		}

		// Successfully loaded from backup
		return &state, nil
	}

	return nil, &BackupNotFoundError{Paths: backupPaths}
}

// copyFile copies a file from src to dst
func (fm *FileManager) copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	// Copy file contents
	if _, err := dstFile.ReadFrom(srcFile); err != nil {
		return err
	}

	// Sync to disk
	return dstFile.Sync()
}

// GetStats returns file manager statistics
func (fm *FileManager) GetStats() interfaces.RepositoryStats {
	stats := interfaces.RepositoryStats{
		StatePath:  fm.statePath,
		IsLocked:   fm.fileLock != nil,
		BackupPath: fm.backupPath,
	}

	// Get file info if exists
	if stat, err := os.Stat(fm.statePath); err == nil {
		stats.FileSize = stat.Size()
		stats.ModTime = stat.ModTime()
	}

	return stats
}

// StateMetadata contains metadata about the state file
type StateMetadata struct {
	Version   string    `json:"version"`
	Timestamp time.Time `json:"timestamp"`
	Checksum  string    `json:"checksum"`
}

// Error types

// FileNotFoundError indicates the state file doesn't exist
type FileNotFoundError struct {
	Path string `json:"path"`
}

func (e *FileNotFoundError) Error() string {
	return fmt.Sprintf("state file not found: %s", e.Path)
}

// LockTimeoutError indicates lock acquisition timed out
type LockTimeoutError struct {
	Path    string        `json:"path"`
	Timeout time.Duration `json:"timeout"`
}

func (e *LockTimeoutError) Error() string {
	return fmt.Sprintf("lock timeout for %s after %v", e.Path, e.Timeout)
}

// CorruptionError indicates file corruption
type CorruptionError struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("file corruption in %s: %s", e.Path, e.Reason)
}

// ValidationError indicates state validation failure
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error in %s: %s", e.Field, e.Message)
}

// BackupNotFoundError indicates no valid backup was found
type BackupNotFoundError struct {
	Paths []string `json:"paths"`
}

func (e *BackupNotFoundError) Error() string {
	return fmt.Sprintf("no valid backup found in paths: %v", e.Paths)
}
