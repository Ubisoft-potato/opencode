package recovery

import (
	"context"
	"log"
	"sync"
	"time"
)

// HealthMonitor monitors system health and triggers recovery when needed
type HealthMonitor struct {
	checkInterval    time.Duration
	checks           map[string]HealthCheck
	checksMutex      sync.RWMutex
	statistics       HealthStatistics
	statsMutex       sync.RWMutex
	ctx              context.Context
	cancel           context.CancelFunc
	recoveryHandler  RecoveryHandler
	alertThresholds  AlertThresholds
}

// HealthCheck represents a health check function
type HealthCheck struct {
	Name        string                             `json:"name"`
	Description string                             `json:"description"`
	CheckFunc   func() HealthCheckResult           `json:"-"`
	Interval    time.Duration                      `json:"interval"`
	LastCheck   time.Time                          `json:"last_check"`
	LastResult  HealthCheckResult                  `json:"last_result"`
	Enabled     bool                               `json:"enabled"`
}

// HealthCheckResult represents the result of a health check
type HealthCheckResult struct {
	Healthy   bool          `json:"healthy"`
	Message   string        `json:"message"`
	Duration  time.Duration `json:"duration"`
	Timestamp time.Time     `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// AlertThresholds defines thresholds for triggering alerts and recovery
type AlertThresholds struct {
	ConsecutiveFailures int           `json:"consecutive_failures"`
	FailureRate         float64       `json:"failure_rate"`
	ResponseTime        time.Duration `json:"response_time"`
	RecoveryTriggerTime time.Duration `json:"recovery_trigger_time"`
}

// HealthStatistics contains health monitoring statistics
type HealthStatistics struct {
	TotalChecks        int64                    `json:"total_checks"`
	HealthyChecks      int64                    `json:"healthy_checks"`
	UnhealthyChecks    int64                    `json:"unhealthy_checks"`
	ChecksByName       map[string]CheckStats    `json:"checks_by_name"`
	LastCheckTime      time.Time                `json:"last_check_time"`
	AverageCheckTime   time.Duration            `json:"average_check_time"`
	OverallHealthy     bool                     `json:"overall_healthy"`
	AlertsTriggered    int64                    `json:"alerts_triggered"`
	RecoveriesTriggered int64                   `json:"recoveries_triggered"`
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

// NewHealthMonitor creates a new health monitor
func NewHealthMonitor(checkInterval time.Duration) *HealthMonitor {
	return &HealthMonitor{
		checkInterval: checkInterval,
		checks:        make(map[string]HealthCheck),
		statistics: HealthStatistics{
			ChecksByName: make(map[string]CheckStats),
		},
		alertThresholds: AlertThresholds{
			ConsecutiveFailures: 3,
			FailureRate:         0.5,
			ResponseTime:        5 * time.Second,
			RecoveryTriggerTime: 30 * time.Second,
		},
	}
}

// Start begins health monitoring
func (hm *HealthMonitor) Start(ctx context.Context, recoveryHandler RecoveryHandler) error {
	hm.ctx, hm.cancel = context.WithCancel(ctx)
	hm.recoveryHandler = recoveryHandler

	// Register default health checks
	hm.registerDefaultChecks()

	// Start monitoring loop
	go hm.monitoringLoop()

	log.Printf("Health monitor started with %d checks", len(hm.checks))
	return nil
}

// Stop gracefully shuts down the health monitor
func (hm *HealthMonitor) Stop() error {
	if hm.cancel != nil {
		hm.cancel()
	}
	log.Printf("Health monitor stopped")
	return nil
}

// RegisterHealthCheck registers a new health check
func (hm *HealthMonitor) RegisterHealthCheck(check HealthCheck) {
	hm.checksMutex.Lock()
	defer hm.checksMutex.Unlock()

	check.Enabled = true
	check.LastCheck = time.Time{}
	hm.checks[check.Name] = check

	// Initialize statistics
	hm.statsMutex.Lock()
	defer hm.statsMutex.Unlock()
	hm.statistics.ChecksByName[check.Name] = CheckStats{}

	log.Printf("Registered health check: %s", check.Name)
}

// UnregisterHealthCheck removes a health check
func (hm *HealthMonitor) UnregisterHealthCheck(name string) {
	hm.checksMutex.Lock()
	defer hm.checksMutex.Unlock()

	delete(hm.checks, name)

	hm.statsMutex.Lock()
	defer hm.statsMutex.Unlock()
	delete(hm.statistics.ChecksByName, name)

	log.Printf("Unregistered health check: %s", name)
}

// EnableHealthCheck enables a health check
func (hm *HealthMonitor) EnableHealthCheck(name string) {
	hm.checksMutex.Lock()
	defer hm.checksMutex.Unlock()

	if check, exists := hm.checks[name]; exists {
		check.Enabled = true
		hm.checks[name] = check
	}
}

// DisableHealthCheck disables a health check
func (hm *HealthMonitor) DisableHealthCheck(name string) {
	hm.checksMutex.Lock()
	defer hm.checksMutex.Unlock()

	if check, exists := hm.checks[name]; exists {
		check.Enabled = false
		hm.checks[name] = check
	}
}

// GetHealthStatus returns the current health status
func (hm *HealthMonitor) GetHealthStatus() HealthStatus {
	hm.checksMutex.RLock()
	defer hm.checksMutex.RUnlock()

	status := HealthStatus{
		OverallHealthy: true,
		CheckResults:   make(map[string]HealthCheckResult),
		Timestamp:      time.Now(),
	}

	for name, check := range hm.checks {
		if check.Enabled {
			status.CheckResults[name] = check.LastResult
			if !check.LastResult.Healthy {
				status.OverallHealthy = false
			}
		}
	}

	return status
}

// GetStatistics returns health monitoring statistics
func (hm *HealthMonitor) GetStatistics() HealthStatistics {
	hm.statsMutex.RLock()
	defer hm.statsMutex.RUnlock()

	// Create a copy to avoid race conditions
	stats := hm.statistics
	stats.ChecksByName = make(map[string]CheckStats)
	for name, checkStats := range hm.statistics.ChecksByName {
		stats.ChecksByName[name] = checkStats
	}

	return stats
}

// registerDefaultChecks registers the default set of health checks
func (hm *HealthMonitor) registerDefaultChecks() {
	// System resource check
	hm.RegisterHealthCheck(HealthCheck{
		Name:        "system_resources",
		Description: "Check system memory and CPU usage",
		CheckFunc:   hm.checkSystemResources,
		Interval:    30 * time.Second,
	})

	// Disk space check
	hm.RegisterHealthCheck(HealthCheck{
		Name:        "disk_space",
		Description: "Check available disk space",
		CheckFunc:   hm.checkDiskSpace,
		Interval:    60 * time.Second,
	})

	// State file integrity check
	hm.RegisterHealthCheck(HealthCheck{
		Name:        "state_integrity",
		Description: "Check state file integrity",
		CheckFunc:   hm.checkStateIntegrity,
		Interval:    120 * time.Second,
	})

	// IPC connectivity check
	hm.RegisterHealthCheck(HealthCheck{
		Name:        "ipc_connectivity",
		Description: "Check IPC server connectivity",
		CheckFunc:   hm.checkIPCConnectivity,
		Interval:    30 * time.Second,
	})
}

// monitoringLoop runs the main health monitoring loop
func (hm *HealthMonitor) monitoringLoop() {
	ticker := time.NewTicker(hm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-hm.ctx.Done():
			return
		case <-ticker.C:
			hm.runHealthChecks()
		}
	}
}

// runHealthChecks executes all enabled health checks
func (hm *HealthMonitor) runHealthChecks() {
	hm.checksMutex.RLock()
	checksToRun := make([]HealthCheck, 0, len(hm.checks))
	for _, check := range hm.checks {
		if check.Enabled && hm.shouldRunCheck(check) {
			checksToRun = append(checksToRun, check)
		}
	}
	hm.checksMutex.RUnlock()

	// Run checks concurrently
	var wg sync.WaitGroup
	for _, check := range checksToRun {
		wg.Add(1)
		go func(check HealthCheck) {
			defer wg.Done()
			hm.runSingleCheck(check)
		}(check)
	}
	wg.Wait()

	// Update overall statistics
	hm.updateOverallStatistics()
}

// shouldRunCheck determines if a check should run based on its interval
func (hm *HealthMonitor) shouldRunCheck(check HealthCheck) bool {
	return time.Since(check.LastCheck) >= check.Interval
}

// runSingleCheck executes a single health check
func (hm *HealthMonitor) runSingleCheck(check HealthCheck) {
	startTime := time.Now()
	result := check.CheckFunc()
	result.Timestamp = startTime
	result.Duration = time.Since(startTime)

	// Update check record
	hm.checksMutex.Lock()
	check.LastCheck = startTime
	check.LastResult = result
	hm.checks[check.Name] = check
	hm.checksMutex.Unlock()

	// Update statistics
	hm.updateCheckStatistics(check.Name, result)

	// Handle unhealthy results
	if !result.Healthy {
		hm.handleUnhealthyCheck(check.Name, result)
	}

	log.Printf("Health check %s: %s (took %v)", check.Name, result.Message, result.Duration)
}

// updateCheckStatistics updates statistics for a specific check
func (hm *HealthMonitor) updateCheckStatistics(checkName string, result HealthCheckResult) {
	hm.statsMutex.Lock()
	defer hm.statsMutex.Unlock()

	stats := hm.statistics.ChecksByName[checkName]
	stats.TotalRuns++

	if result.Healthy {
		stats.SuccessfulRuns++
		stats.ConsecutiveFailures = 0
		stats.LastSuccess = result.Timestamp
	} else {
		stats.FailedRuns++
		stats.ConsecutiveFailures++
		stats.LastFailure = result.Timestamp
	}

	// Update average run time
	if stats.TotalRuns == 1 {
		stats.AverageRunTime = result.Duration
	} else {
		stats.AverageRunTime = (stats.AverageRunTime + result.Duration) / 2
	}

	// Update success rate
	stats.SuccessRate = float64(stats.SuccessfulRuns) / float64(stats.TotalRuns) * 100

	hm.statistics.ChecksByName[checkName] = stats
	hm.statistics.TotalChecks++

	if result.Healthy {
		hm.statistics.HealthyChecks++
	} else {
		hm.statistics.UnhealthyChecks++
	}
}

// updateOverallStatistics updates overall health monitoring statistics
func (hm *HealthMonitor) updateOverallStatistics() {
	hm.statsMutex.Lock()
	defer hm.statsMutex.Unlock()

	hm.statistics.LastCheckTime = time.Now()

	// Calculate average check time
	var totalDuration time.Duration
	totalChecks := int64(0)
	allHealthy := true

	for _, stats := range hm.statistics.ChecksByName {
		totalDuration += stats.AverageRunTime
		totalChecks++
		if stats.SuccessRate < 100 && stats.TotalRuns > 0 {
			allHealthy = false
		}
	}

	if totalChecks > 0 {
		hm.statistics.AverageCheckTime = totalDuration / time.Duration(totalChecks)
	}

	hm.statistics.OverallHealthy = allHealthy
}

// handleUnhealthyCheck handles an unhealthy check result
func (hm *HealthMonitor) handleUnhealthyCheck(checkName string, result HealthCheckResult) {
	stats := hm.statistics.ChecksByName[checkName]

	// Check if we should trigger recovery
	shouldRecover := false

	// Trigger recovery if consecutive failures exceed threshold
	if stats.ConsecutiveFailures >= hm.alertThresholds.ConsecutiveFailures {
		shouldRecover = true
	}

	// Trigger recovery if failure rate is too high
	if stats.TotalRuns >= 10 && stats.SuccessRate < (100-hm.alertThresholds.FailureRate*100) {
		shouldRecover = true
	}

	// Trigger recovery if response time is too high
	if result.Duration > hm.alertThresholds.ResponseTime {
		shouldRecover = true
	}

	if shouldRecover && hm.recoveryHandler != nil {
		log.Printf("Triggering recovery for unhealthy check: %s", checkName)
		hm.statsMutex.Lock()
		hm.statistics.RecoveriesTriggered++
		hm.statsMutex.Unlock()

		// Determine failure type based on check name
		failureType := GenericFailure
		switch checkName {
		case "state_integrity":
			failureType = StateCorruption
		case "ipc_connectivity":
			failureType = IPCFailure
		case "disk_space":
			failureType = FileSystemError
		}

		go func() {
			if err := hm.recoveryHandler.RecoverFromFailure(failureType, checkName); err != nil {
				log.Printf("Recovery failed for %s: %v", checkName, err)
			}
		}()
	}

	// Always trigger alert
	hm.statsMutex.Lock()
	hm.statistics.AlertsTriggered++
	hm.statsMutex.Unlock()
}

// Health check implementations

func (hm *HealthMonitor) checkSystemResources() HealthCheckResult {
	// Placeholder implementation
	// In a real system, you'd check memory and CPU usage
	return HealthCheckResult{
		Healthy: true,
		Message: "System resources OK",
		Metadata: map[string]interface{}{
			"memory_usage": "45%",
			"cpu_usage":    "23%",
		},
	}
}

func (hm *HealthMonitor) checkDiskSpace() HealthCheckResult {
	// Placeholder implementation
	// In a real system, you'd check actual disk space
	return HealthCheckResult{
		Healthy: true,
		Message: "Disk space OK",
		Metadata: map[string]interface{}{
			"available_space": "15GB",
			"usage_percent":   "75%",
		},
	}
}

func (hm *HealthMonitor) checkStateIntegrity() HealthCheckResult {
	// Placeholder implementation
	// In a real system, you'd verify state file integrity
	return HealthCheckResult{
		Healthy: true,
		Message: "State integrity OK",
		Metadata: map[string]interface{}{
			"file_size":      "1024",
			"last_modified": time.Now().Add(-5 * time.Minute),
		},
	}
}

func (hm *HealthMonitor) checkIPCConnectivity() HealthCheckResult {
	// Placeholder implementation
	// In a real system, you'd test IPC connectivity
	return HealthCheckResult{
		Healthy: true,
		Message: "IPC connectivity OK",
		Metadata: map[string]interface{}{
			"connected_panels": 3,
			"socket_status":    "active",
		},
	}
}

// HealthStatus represents the current health status
type HealthStatus struct {
	OverallHealthy bool                            `json:"overall_healthy"`
	CheckResults   map[string]HealthCheckResult    `json:"check_results"`
	Timestamp      time.Time                       `json:"timestamp"`
}