package state

import (
	"fmt"
	"log"
	"math"
	"time"

	"github.com/sst/opencode/internal/interfaces"
	"github.com/sst/opencode/internal/types"
)

// ConflictResolver handles state synchronization conflicts using various strategies
type ConflictResolver struct {
	maxRetries        int
	baseBackoffMs     int
	maxBackoffMs      int
	conflictStrategy  interfaces.ConflictStrategy
	retryCount        int64
	successCount      int64
	conflictCount     int64
}

// NewConflictResolver creates a new conflict resolver with specified parameters
func NewConflictResolver(maxRetries, baseBackoffMs, maxBackoffMs int, strategy interfaces.ConflictStrategy) *ConflictResolver {
	return &ConflictResolver{
		maxRetries:       maxRetries,
		baseBackoffMs:    baseBackoffMs,
		maxBackoffMs:     maxBackoffMs,
		conflictStrategy: strategy,
	}
}

// DefaultConflictResolver creates a resolver with sensible defaults
func DefaultConflictResolver() *ConflictResolver {
	return NewConflictResolver(
		5,                              // max 5 retries
		10,                             // 10ms base backoff
		1000,                           // 1s max backoff
		interfaces.LastWriteWins,       // use timestamp-based resolution
	)
}

// ResolveConflict attempts to resolve a state update conflict
func (resolver *ConflictResolver) ResolveConflict(
	stateManager interfaces.StateManager,
	update types.StateUpdate,
) *interfaces.ConflictResolutionResult {
	startTime := time.Now()
	result := &interfaces.ConflictResolutionResult{
		Strategy: resolver.conflictStrategy,
	}

	for attempt := 0; attempt < resolver.maxRetries; attempt++ {
		result.Attempts = attempt + 1

		// Get the latest state version before attempting update
		state := stateManager.GetState()
		currentVersion := state.GetCurrentVersion()

		// Create a new update with current version expectations
		updatedUpdate := update
		updatedUpdate.ExpectedVersion = currentVersion
		updatedUpdate.Timestamp = time.Now()

		// Attempt the update
		err := stateManager.UpdateWithVersionCheck(updatedUpdate)
		if err == nil {
			// Success!
			resolver.successCount++
			result.Success = true
			result.FinalVersion = stateManager.GetState().GetCurrentVersion()
			result.TimeTaken = time.Since(startTime)
			return result
		}

		// Check if it's a version conflict
		if isVersionConflict(err) {
			resolver.conflictCount++

			log.Printf("Conflict detected (attempt %d/%d): %v",
				attempt+1, resolver.maxRetries, err)

			// Apply conflict resolution strategy
			if resolved := resolver.applyConflictStrategy(stateManager, update, err); resolved {
				// Try the update again immediately if strategy resolved it
				continue
			}

			// If strategy didn't resolve it, wait before retrying
			if attempt < resolver.maxRetries-1 {
				backoffDuration := resolver.calculateBackoff(attempt)
				log.Printf("Backing off for %v before retry", backoffDuration)
				time.Sleep(backoffDuration)
			}
			continue
		}

		// Non-conflict error, return immediately
		result.Error = err
		result.TimeTaken = time.Since(startTime)
		resolver.retryCount++
		return result
	}

	// Max retries exceeded
	resolver.retryCount++
	result.Error = fmt.Errorf("max retries exceeded (%d) for update type %s", resolver.maxRetries, update.Type)
	result.TimeTaken = time.Since(startTime)
	return result
}

// applyConflictStrategy applies the configured conflict resolution strategy
func (resolver *ConflictResolver) applyConflictStrategy(
	stateManager interfaces.StateManager,
	update types.StateUpdate,
	conflictErr error,
) bool {
	switch resolver.conflictStrategy {
	case interfaces.LastWriteWins:
		return resolver.resolveByTimestamp(stateManager, update, conflictErr)
	case interfaces.VersionBased:
		return resolver.resolveByVersion(stateManager, update, conflictErr)
	case interfaces.ManualResolve:
		return resolver.resolveManually(stateManager, update, conflictErr)
	default:
		log.Printf("Unknown conflict strategy: %s", resolver.conflictStrategy)
		return false
	}
}

// resolveByTimestamp implements last-write-wins conflict resolution
func (resolver *ConflictResolver) resolveByTimestamp(
	stateManager interfaces.StateManager,
	update types.StateUpdate,
	conflictErr error,
) bool {
	state := stateManager.GetState()

	// Get the current state timestamp
	currentTimestamp := state.Version.Timestamp
	updateTimestamp := update.Timestamp

	// If update is newer, force apply it
	if updateTimestamp.After(currentTimestamp) {
		log.Printf("Applying newer update (timestamp %v > %v)",
			updateTimestamp, currentTimestamp)

		// Force update by using current version
		update.ExpectedVersion = state.GetCurrentVersion()
		return true
	}

	// If update is older, skip it
	log.Printf("Skipping older update (timestamp %v <= %v)",
		updateTimestamp, currentTimestamp)
	return false
}

// resolveByVersion implements version-based conflict resolution
func (resolver *ConflictResolver) resolveByVersion(
	stateManager interfaces.StateManager,
	update types.StateUpdate,
	conflictErr error,
) bool {
	// In version-based resolution, we typically just retry with the current version
	// The actual resolution logic would depend on the specific application needs

	state := stateManager.GetState()
	log.Printf("Using version-based resolution: current=%d, expected=%d",
		state.Version.Version, update.ExpectedVersion)

	// Simply retry with current version (exponential backoff will handle timing)
	return false
}

// resolveManually implements manual conflict resolution
func (resolver *ConflictResolver) resolveManually(
	stateManager interfaces.StateManager,
	update types.StateUpdate,
	conflictErr error,
) bool {
	// In a real implementation, this would trigger some kind of user intervention
	// For now, we just log and let the retry mechanism handle it

	log.Printf("Manual conflict resolution required for update %s", update.Type)
	log.Printf("Conflict error: %v", conflictErr)

	// Return false to continue with retry logic
	return false
}

// calculateBackoff computes the backoff duration for retry attempts
func (resolver *ConflictResolver) calculateBackoff(attempt int) time.Duration {
	// Exponential backoff with jitter
	backoffMs := resolver.baseBackoffMs * int(math.Pow(2, float64(attempt)))

	// Cap at maximum backoff
	if backoffMs > resolver.maxBackoffMs {
		backoffMs = resolver.maxBackoffMs
	}

	// Add some jitter to avoid thundering herd
	jitter := time.Duration(backoffMs/4) * time.Millisecond
	baseBackoff := time.Duration(backoffMs) * time.Millisecond

	// Random jitter between -25% and +25%
	jitterOffset := time.Duration((time.Now().UnixNano() % int64(jitter*2)) - int64(jitter))

	return baseBackoff + jitterOffset
}

// GetStatistics returns conflict resolution statistics
func (resolver *ConflictResolver) GetStatistics() interfaces.ConflictStatistics {
	total := resolver.successCount + resolver.retryCount + resolver.conflictCount

	var successRate float64
	if total > 0 {
		successRate = float64(resolver.successCount) / float64(total) * 100
	}

	return interfaces.ConflictStatistics{
		TotalAttempts: total,
		SuccessCount:  resolver.successCount,
		ConflictCount: resolver.conflictCount,
		RetryCount:    resolver.retryCount,
		SuccessRate:   successRate,
		Strategy:      resolver.conflictStrategy,
	}
}

// UpdateConflictStrategy changes the conflict resolution strategy
func (resolver *ConflictResolver) UpdateConflictStrategy(strategy interfaces.ConflictStrategy) {
	resolver.conflictStrategy = strategy
	log.Printf("Conflict resolution strategy updated to: %s", strategy)
}

// IsHealthy returns true if the conflict resolver is performing well
func (resolver *ConflictResolver) IsHealthy() bool {
	stats := resolver.GetStatistics()

	// Consider healthy if success rate is above 80% and conflict rate is below 20%
	if stats.TotalAttempts < 10 {
		return true // Not enough data to determine health
	}

	conflictRate := float64(stats.ConflictCount) / float64(stats.TotalAttempts) * 100

	return stats.SuccessRate >= 80.0 && conflictRate <= 20.0
}

// isVersionConflict checks if the error is a version conflict
func isVersionConflict(err error) bool {
	// Simple check for version conflict errors
	// In a real implementation, this would check for specific error types
	return err != nil && err.Error() != ""
}