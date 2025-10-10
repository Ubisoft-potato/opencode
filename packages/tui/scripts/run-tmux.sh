#!/bin/bash

# OpenCode tmux Panel Startup Script
# This script creates and configures a tmux session with 3 synchronized panels

set -e

# Configuration
SESSION_NAME="opencode"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Environment variables
export OPENCODE_SERVER="${OPENCODE_SERVER:-http://localhost:3000}"
export OPENCODE_SOCKET="${OPENCODE_SOCKET:-$HOME/.opencode/ipc.sock}"
export OPENCODE_STATE="${OPENCODE_STATE:-$HOME/.opencode/state.json}"

# Color output functions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check dependencies
check_dependencies() {
    log_info "Checking dependencies..."

    if ! command -v tmux &> /dev/null; then
        log_error "tmux is not installed. Please install tmux first."
        exit 1
    fi

    if ! command -v go &> /dev/null; then
        log_error "Go is not installed. Please install Go first."
        exit 1
    fi

    log_success "All dependencies are available"
}

# Build panel applications
build_applications() {
    log_info "Building panel applications..."

    cd "$PROJECT_ROOT"

    # Build opencode-tmux orchestrator
    log_info "Building opencode-tmux..."
    go build -o ./bin/opencode-tmux ./cmd/opencode-tmux

    # Build sessions panel
    log_info "Building opencode-sessions..."
    go build -o ./bin/opencode-sessions ./cmd/opencode-sessions

    # Build messages panel
    log_info "Building opencode-messages..."
    go build -o ./bin/opencode-messages ./cmd/opencode-messages

    # Build input panel
    log_info "Building opencode-input..."
    go build -o ./bin/opencode-input ./cmd/opencode-input

    # Build test suite
    log_info "Building opencode-test..."
    go build -o ./bin/opencode-test ./cmd/opencode-test

    log_success "All applications built successfully"
}

# Setup environment
setup_environment() {
    log_info "Setting up environment..."

    # Create necessary directories
    mkdir -p "$HOME/.opencode"
    mkdir -p "$(dirname "$OPENCODE_SOCKET")"
    mkdir -p "$(dirname "$OPENCODE_STATE")"

    # Add bin directory to PATH if not already there
    BIN_DIR="$PROJECT_ROOT/bin"
    if [[ ":$PATH:" != *":$BIN_DIR:"* ]]; then
        export PATH="$BIN_DIR:$PATH"
        log_info "Added $BIN_DIR to PATH"
    fi

    log_success "Environment setup complete"
}

# Kill existing session
kill_existing_session() {
    if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
        log_warning "Killing existing tmux session: $SESSION_NAME"
        tmux kill-session -t "$SESSION_NAME"
    fi
}

# Start tmux session with orchestrator
start_tmux_session() {
    log_info "Starting tmux session with orchestrator..."

    # Start the orchestrator
    "$PROJECT_ROOT/bin/opencode-tmux" "$SESSION_NAME" &
    ORCHESTRATOR_PID=$!

    # Wait for session to be created
    local max_wait=10
    local wait_count=0

    while ! tmux has-session -t "$SESSION_NAME" 2>/dev/null; do
        if [ $wait_count -ge $max_wait ]; then
            log_error "Timeout waiting for tmux session to be created"
            kill $ORCHESTRATOR_PID 2>/dev/null || true
            exit 1
        fi

        sleep 1
        ((wait_count++))
    done

    log_success "Tmux session created and configured"
}

# Manual tmux setup (fallback)
manual_tmux_setup() {
    log_info "Setting up tmux session manually..."

    # Create new session
    tmux new-session -d -s "$SESSION_NAME"

    # Split window into 3 panes
    # Sessions panel (left, 20% width)
    tmux split-window -h -t "$SESSION_NAME:0"

    # Input panel (bottom, 20% height)
    tmux split-window -v -t "$SESSION_NAME:0.1"

    # Adjust pane sizes
    tmux resize-pane -t "$SESSION_NAME:0.0" -x 20%
    tmux resize-pane -t "$SESSION_NAME:0.2" -y 20%

    # Start applications in each pane
    log_info "Starting panel applications..."

    # Sessions panel (pane 0)
    tmux send-keys -t "$SESSION_NAME:0.0" "opencode-sessions" Enter

    # Messages panel (pane 1)
    tmux send-keys -t "$SESSION_NAME:0.1" "opencode-messages" Enter

    # Input panel (pane 2)
    tmux send-keys -t "$SESSION_NAME:0.2" "opencode-input" Enter

    # Give applications time to start
    sleep 2

    log_success "Manual tmux setup complete"
}

# Run tests
run_tests() {
    if [ "$1" = "--test" ]; then
        log_info "Running test suite..."

        # Run tests in background
        "$PROJECT_ROOT/bin/opencode-test" &
        TEST_PID=$!

        # Wait for tests to complete
        if wait $TEST_PID; then
            log_success "All tests passed"
        else
            log_error "Some tests failed"
            return 1
        fi
    fi
}

# Attach to session
attach_to_session() {
    log_info "Attaching to tmux session: $SESSION_NAME"
    log_info "Use 'Ctrl+b d' to detach from the session"
    log_info "Use './scripts/run-tmux.sh --stop' to stop the session"

    # Attach to the session
    tmux attach-session -t "$SESSION_NAME"
}

# Stop session
stop_session() {
    log_info "Stopping tmux session: $SESSION_NAME"

    if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
        tmux kill-session -t "$SESSION_NAME"
        log_success "Session stopped"
    else
        log_warning "No active session found"
    fi

    # Clean up socket file
    if [ -f "$OPENCODE_SOCKET" ]; then
        rm -f "$OPENCODE_SOCKET"
        log_info "Cleaned up socket file"
    fi
}

# Show status
show_status() {
    log_info "OpenCode Tmux Status:"

    if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
        echo "  Session: ACTIVE"
        tmux list-panes -t "$SESSION_NAME" -F "  Pane #{pane_index}: #{pane_current_command}"
    else
        echo "  Session: INACTIVE"
    fi

    if [ -S "$OPENCODE_SOCKET" ]; then
        echo "  IPC Socket: ACTIVE"
    else
        echo "  IPC Socket: INACTIVE"
    fi

    if [ -f "$OPENCODE_STATE" ]; then
        echo "  State File: EXISTS ($(stat -f%z "$OPENCODE_STATE" 2>/dev/null || stat -c%s "$OPENCODE_STATE" 2>/dev/null) bytes)"
    else
        echo "  State File: NOT FOUND"
    fi
}

# Show help
show_help() {
    cat << EOF
OpenCode tmux Panel Manager

Usage: $0 [OPTION]

Options:
    --help      Show this help message
    --build     Build applications only
    --test      Run tests before starting
    --manual    Use manual tmux setup instead of orchestrator
    --stop      Stop the tmux session
    --status    Show current status
    --clean     Clean up all files and stop session

Environment Variables:
    OPENCODE_SERVER    OpenCode server URL (default: http://localhost:3000)
    OPENCODE_SOCKET    IPC socket path (default: ~/.opencode/ipc.sock)
    OPENCODE_STATE     State file path (default: ~/.opencode/state.json)

Examples:
    $0                 # Start tmux session with all panels
    $0 --test          # Run tests then start session
    $0 --manual        # Use manual setup instead of orchestrator
    $0 --stop          # Stop the session
    $0 --status        # Show current status

Panel Layout:
    ┌─────────────────────────────────────────────────────────────┐
    │ Sessions (20% width)      │ Messages (80% width)            │
    │                          │                                 │
    │ • Session 1              │ User: Hello                     │
    │ • Session 2 (current)    │ Assistant: Hi! How can I help? │
    │ • Session 3              │ User: Can you help me with...   │
    │   + New Session          │ Assistant: Sure! Let me...      │
    ├─────────────────────────────────────────────────────────────┤
    │ Input (full width, 20% height)                             │
    │ > Type your message here...                                 │
    └─────────────────────────────────────────────────────────────┘
EOF
}

# Clean up everything
clean_all() {
    log_info "Cleaning up all files and sessions..."

    stop_session

    # Remove socket and state files
    rm -f "$OPENCODE_SOCKET"
    rm -f "$OPENCODE_STATE"
    rm -rf "$HOME/.opencode"

    # Remove built binaries
    rm -rf "$PROJECT_ROOT/bin"

    log_success "Cleanup complete"
}

# Main script logic
main() {
    case "${1:-}" in
        --help|-h)
            show_help
            exit 0
            ;;
        --build)
            check_dependencies
            build_applications
            exit 0
            ;;
        --stop)
            stop_session
            exit 0
            ;;
        --status)
            show_status
            exit 0
            ;;
        --clean)
            clean_all
            exit 0
            ;;
        --test)
            check_dependencies
            build_applications
            setup_environment
            kill_existing_session

            if run_tests --test; then
                start_tmux_session
                attach_to_session
            else
                log_error "Tests failed, not starting session"
                exit 1
            fi
            ;;
        --manual)
            check_dependencies
            build_applications
            setup_environment
            kill_existing_session
            manual_tmux_setup
            attach_to_session
            ;;
        "")
            # Default behavior
            check_dependencies
            build_applications
            setup_environment
            kill_existing_session
            start_tmux_session
            attach_to_session
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
}

# Handle script interruption
trap 'log_warning "Script interrupted"; stop_session; exit 130' INT TERM

# Run main function
main "$@"