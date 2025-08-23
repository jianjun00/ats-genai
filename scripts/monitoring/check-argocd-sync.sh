#!/bin/bash
set -euo pipefail

# ArgoCD Deployment Status Monitor (for private ArgoCD)
# This script runs inside your private network to check ArgoCD sync status

ARGOCD_NAMESPACE="${ARGOCD_NAMESPACE:-argocd}"
APP_NAME="${1:-ats-dev}"

echo "🔍 Checking ArgoCD sync status for application: $APP_NAME"

# Check if ArgoCD CLI is available
if ! command -v argocd > /dev/null 2>&1; then
    echo "❌ ArgoCD CLI not found. Please install it first."
    echo "Download: https://github.com/argoproj/argo-cd/releases/latest/download/argocd-linux-amd64"
    exit 1
fi

# Check application status
APP_STATUS=$(kubectl get application "$APP_NAME" -n "$ARGOCD_NAMESPACE" -o json 2>/dev/null || echo "{}")

if [[ "$APP_STATUS" == "{}" ]]; then
    echo "❌ Application $APP_NAME not found"
    exit 1
fi

# Extract status information
HEALTH=$(echo "$APP_STATUS" | jq -r '.status.health.status // "Unknown"')
SYNC_STATUS=$(echo "$APP_STATUS" | jq -r '.status.sync.status // "Unknown"')
REVISION=$(echo "$APP_STATUS" | jq -r '.status.sync.revision // "Unknown"')

echo "📊 Application Status:"
echo "  Health: $HEALTH"
echo "  Sync: $SYNC_STATUS" 
echo "  Revision: ${REVISION:0:8}"

# Check if sync is in progress
if [[ "$SYNC_STATUS" == "Syncing" ]]; then
    echo "⏳ Sync in progress..."
    
    # Wait for sync to complete (max 5 minutes)
    for i in {1..60}; do
        sleep 5
        CURRENT_STATUS=$(kubectl get application "$APP_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "Unknown")
        
        if [[ "$CURRENT_STATUS" != "Syncing" ]]; then
            SYNC_STATUS="$CURRENT_STATUS"
            break
        fi
        
        echo "  Still syncing... (${i}/60)"
    done
fi

# Final status check
FINAL_HEALTH=$(kubectl get application "$APP_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.health.status}' 2>/dev/null || echo "Unknown")
FINAL_SYNC=$(kubectl get application "$APP_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "Unknown")

echo ""
echo "🏁 Final Status:"
echo "  Health: $FINAL_HEALTH"
echo "  Sync: $FINAL_SYNC"

# Exit codes for automation
if [[ "$FINAL_HEALTH" == "Healthy" && "$FINAL_SYNC" == "Synced" ]]; then
    echo "✅ Deployment successful!"
    exit 0
elif [[ "$FINAL_HEALTH" == "Degraded" ]]; then
    echo "❌ Deployment failed - application is degraded"
    exit 1
elif [[ "$FINAL_SYNC" == "OutOfSync" ]]; then
    echo "⚠️ Application is out of sync"
    exit 2
else
    echo "⚠️ Unknown status - manual investigation needed"
    exit 3
fi