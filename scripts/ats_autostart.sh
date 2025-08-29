#!/bin/bash
"""
ATS Autostart Script - Automatically start ATS dev and intg environments on WSL startup

This script:
1. Starts ats-dev PostgreSQL database
2. Starts ats-intg PostgreSQL database (if needed)
3. Logs startup activities
4. Runs in background to avoid blocking shell startup
"""

# Configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_FILE="/mnt/d/ats-logs/autostart.log"
PID_FILE="/tmp/ats_autostart.pid"

# Ensure log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to check if service is already running
is_service_running() {
    local service_name="$1"
    docker ps --format "{{.Names}}" | grep -q "^${service_name}$"
}

# Function to start ATS services
start_ats_services() {
    log "🚀 Starting ATS autostart sequence..."
    
    cd "$PROJECT_ROOT" || {
        log "❌ Failed to change to project root: $PROJECT_ROOT"
        return 1
    }
    
    # Start ats-dev environment
    if ! is_service_running "ats-dev-postgres"; then
        log "🔧 Starting ats-dev PostgreSQL..."
        python3 scripts/run_dev.py start --service postgres >> "$LOG_FILE" 2>&1
        if [ $? -eq 0 ]; then
            log "✅ ats-dev PostgreSQL started successfully"
        else
            log "❌ Failed to start ats-dev PostgreSQL"
        fi
    else
        log "✅ ats-dev PostgreSQL already running"
    fi
    
    # Start ats-intg environment (if run_intg.py exists)
    if [ -f "scripts/run_intg.py" ]; then
        if ! is_service_running "ats-intg-postgres"; then
            log "🔧 Starting ats-intg PostgreSQL..."
            python3 scripts/run_intg.py start --service postgres >> "$LOG_FILE" 2>&1
            if [ $? -eq 0 ]; then
                log "✅ ats-intg PostgreSQL started successfully"
            else
                log "❌ Failed to start ats-intg PostgreSQL"
            fi
        else
            log "✅ ats-intg PostgreSQL already running"
        fi
    else
        log "ℹ️  ats-intg script not found, skipping"
    fi
    
    # Show final status
    log "📊 Final ATS services status:"
    python3 scripts/run_dev.py status >> "$LOG_FILE" 2>&1
    
    log "🎉 ATS autostart sequence completed"
}

# Main execution
main() {
    # Check if already running
    if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        log "⚠️  ATS autostart already running (PID: $(cat "$PID_FILE"))"
        exit 0
    fi
    
    # Store our PID
    echo $$ > "$PID_FILE"
    
    # Wait a bit for WSL to fully initialize
    sleep 5
    
    # Start services
    start_ats_services
    
    # Clean up PID file
    rm -f "$PID_FILE"
}

# Run in background if called directly
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main &
fi