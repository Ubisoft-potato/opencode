package debug

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// PaneStatus represents the status of a tmux pane
type PaneStatus struct {
	PaneName         string            `json:"pane_name"`
	ProcessID        int               `json:"process_id"`
	IPCSocketPath    string            `json:"ipc_socket_path"`
	IPCConnected     bool              `json:"ipc_connected"`
	CurrentSessionID string            `json:"current_session_id"`
	LastUpdate       time.Time         `json:"last_update"`
	Environment      map[string]string `json:"environment"`
	Errors           []string          `json:"errors"`
}

// StatusReporter helps debug tmux pane communication issues
type StatusReporter struct {
	status PaneStatus
}

// NewStatusReporter creates a new status reporter
func NewStatusReporter(paneName string) *StatusReporter {
	return &StatusReporter{
		status: PaneStatus{
			PaneName:    paneName,
			ProcessID:   os.Getpid(),
			LastUpdate:  time.Now(),
			Environment: make(map[string]string),
			Errors:      make([]string, 0),
		},
	}
}

// UpdateIPCStatus updates the IPC connection status
func (sr *StatusReporter) UpdateIPCStatus(connected bool, socketPath string) {
	sr.status.IPCConnected = connected
	sr.status.IPCSocketPath = socketPath
	sr.status.LastUpdate = time.Now()
}

// UpdateSessionStatus updates the current session information
func (sr *StatusReporter) UpdateSessionStatus(sessionID string) {
	sr.status.CurrentSessionID = sessionID
	sr.status.LastUpdate = time.Now()
}

// AddError adds an error to the status report
func (sr *StatusReporter) AddError(err string) {
	sr.status.Errors = append(sr.status.Errors, fmt.Sprintf("%s: %s", time.Now().Format("15:04:05"), err))

	// Keep only last 10 errors
	if len(sr.status.Errors) > 10 {
		sr.status.Errors = sr.status.Errors[len(sr.status.Errors)-10:]
	}

	sr.status.LastUpdate = time.Now()
}

// UpdateEnvironment captures relevant environment variables
func (sr *StatusReporter) UpdateEnvironment() {
	envVars := []string{
		"OPENCODE_IPC_SOCKET",
		"OPENCODE_FORCE_TERMINAL",
		"OPENCODE_SERVER",
		"TMUX",
		"TMUX_PANE",
	}

	for _, env := range envVars {
		sr.status.Environment[env] = os.Getenv(env)
	}

	sr.status.LastUpdate = time.Now()
}

// GetStatus returns the current status
func (sr *StatusReporter) GetStatus() PaneStatus {
	sr.UpdateEnvironment()
	return sr.status
}

// PrintStatus prints the status to stdout for debugging
func (sr *StatusReporter) PrintStatus() {
	status := sr.GetStatus()

	fmt.Printf("\n🔍 Pane Status: %s (PID: %d)\n", status.PaneName, status.ProcessID)
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

	// IPC Status
	if status.IPCConnected {
		fmt.Printf("📡 IPC: ✅ Connected (%s)\n", status.IPCSocketPath)
	} else {
		fmt.Printf("📡 IPC: ❌ Disconnected (%s)\n", status.IPCSocketPath)
	}

	// Session Status
	if status.CurrentSessionID != "" {
		fmt.Printf("📝 Session: %s\n", status.CurrentSessionID)
	} else {
		fmt.Printf("📝 Session: ❌ None selected\n")
	}

	// Environment
	fmt.Printf("🌍 Environment:\n")
	for key, value := range status.Environment {
		if value != "" {
			fmt.Printf("   %s=%s\n", key, value)
		} else {
			fmt.Printf("   %s=❌ Not set\n", key)
		}
	}

	// Recent Errors
	if len(status.Errors) > 0 {
		fmt.Printf("⚠️  Recent Errors:\n")
		for _, err := range status.Errors {
			fmt.Printf("   %s\n", err)
		}
	}

	fmt.Printf("🕒 Last Update: %s\n", status.LastUpdate.Format("15:04:05"))
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
}

// WriteStatusToFile writes the status to a debug file
func (sr *StatusReporter) WriteStatusToFile() error {
	status := sr.GetStatus()

	filename := fmt.Sprintf("/tmp/opencode-debug-%s-%d.json", status.PaneName, status.ProcessID)

	data, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal status: %w", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write status file: %w", err)
	}

	fmt.Printf("📄 Debug status written to: %s\n", filename)
	return nil
}