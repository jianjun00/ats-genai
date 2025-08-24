#!/bin/bash
# Auto-start ATS-DEV environment when WSL starts
# Add this to your ~/.bashrc or create a systemd service

set -e

# Configuration
ATS_HOME="/home/jianjun/ats-genai"
LOG_FILE="$ATS_HOME/logs/auto_start.log"

# Create logs directory
mkdir -p "$ATS_HOME/logs"

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to check if minikube is healthy
is_minikube_healthy() {
    minikube status >/dev/null 2>&1 && \
    kubectl get nodes >/dev/null 2>&1 && \
    kubectl get namespace ats-dev >/dev/null 2>&1 && \
    kubectl get deployment postgres -n ats-dev >/dev/null 2>&1
}

# Main auto-start function
auto_start_ats_dev() {
    log "🚀 Auto-starting ATS-DEV environment..."
    
    # Change to ATS directory
    cd "$ATS_HOME" || {
        log "❌ Failed to change to ATS directory: $ATS_HOME"
        return 1
    }
    
    # Check if environment is already running
    if is_minikube_healthy; then
        log "✅ ATS-DEV environment is already running and healthy"
        return 0
    fi
    
    # Run the full setup script
    log "🔧 Running full environment setup..."
    if ./scripts/setup/setup_ats_dev_environment.sh >> "$LOG_FILE" 2>&1; then
        log "✅ ATS-DEV environment started successfully"
        
        # Additional health check
        if is_minikube_healthy; then
            log "✅ Health check passed - environment is ready"
            return 0
        else
            log "⚠️  Environment started but health check failed"
            return 1
        fi
    else
        log "❌ Failed to start ATS-DEV environment"
        return 1
    fi
}

# Run auto-start if called directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    auto_start_ats_dev
fi