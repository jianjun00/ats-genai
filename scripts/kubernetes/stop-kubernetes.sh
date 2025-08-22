#!/bin/bash

# Configuration
LOG_FILE="/home/jianjun/ats-genai/logs/kubernetes-startup.log"
CLUSTER_NAME="ats-dev"
NAMESPACE="market-data"
NOTIFY_DESKTOP=true

# Create logs directory if it doesn't exist
mkdir -p "$(dirname "$LOG_FILE")"

# Function for logging
log() {
    local message="$1"
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo "[$timestamp] $message" | tee -a "$LOG_FILE"
    
    # Desktop notification if enabled
    if [[ "$NOTIFY_DESKTOP" == true ]] && command -v notify-send &> /dev/null; then
        notify-send "Kubernetes Shutdown" "$message" --icon=dialog-information
    fi
}

log "Starting Kubernetes shutdown procedure..."

# Check if KinD cluster exists
if kind get clusters 2>/dev/null | grep -q "$CLUSTER_NAME"; then
    log "Stopping KinD cluster '$CLUSTER_NAME'..."
    
    # Save resource usage before shutdown for diagnostics
    log "Saving resource usage information before shutdown..."
    kubectl top nodes --use-protocol-buffers 2>/dev/null | tee -a "$LOG_FILE" || true
    kubectl top pods -A --use-protocol-buffers 2>/dev/null | tee -a "$LOG_FILE" || true
    
    # Delete the cluster
    if kind delete cluster --name "$CLUSTER_NAME"; then
        log "KinD cluster '$CLUSTER_NAME' stopped successfully"
    else
        log "WARNING: Failed to stop KinD cluster '$CLUSTER_NAME' gracefully"
    fi
else
    log "No KinD cluster named '$CLUSTER_NAME' found, nothing to stop"
fi

log "Kubernetes shutdown procedure completed"
exit 0
