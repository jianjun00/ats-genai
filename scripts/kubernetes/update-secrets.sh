#!/bin/bash

# Script to securely update Kubernetes secrets for data agent
# This script helps replace placeholder values with real credentials

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SECRETS_FILE="${SCRIPT_DIR}/../k8s/data-agent/data-agent-secrets.yaml"
LOG_DIR="${SCRIPT_DIR}/../logs"
LOG_FILE="${LOG_DIR}/secrets-update.log"

# Create logs directory if it doesn't exist
mkdir -p "${LOG_DIR}" 2>/dev/null || true

# Check if we can write to the log directory
if [ ! -w "${LOG_DIR}" ]; then
    echo "Warning: Cannot write to log directory ${LOG_DIR}"
    echo "Logs will only be displayed on screen"
    LOG_FILE="/dev/null"
fi

# Function for logging
log() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] $1"
    # Try to write to log file, but don't fail if we can't
    echo "[$timestamp] $1" >> "${LOG_FILE}" 2>/dev/null || true
}

# Check if secrets file exists
if [ ! -f "${SECRETS_FILE}" ]; then
    log "Error: Secrets file not found at ${SECRETS_FILE}"
    exit 1
fi

log "Starting secrets update process..."

# Prompt for API keys and credentials
read -p "Enter Polygon API Key: " POLYGON_API_KEY
read -p "Enter Tiingo API Key: " TIINGO_API_KEY
read -p "Enter OpenAI API Key: " OPENAI_API_KEY
read -p "Enter Database URL: " DATABASE_URL
read -p "Enter Slack Webhook URL (optional, press Enter to skip): " SLACK_WEBHOOK_URL

# Update the secrets file with provided values
log "Updating secrets file with provided credentials..."

# Create a temporary file with the updated values
TMP_FILE=$(mktemp)
cat > "${TMP_FILE}" << EOF
apiVersion: v1
kind: Secret
metadata:
  name: data-agent-secrets
  namespace: market-data
type: Opaque
stringData:
  polygon-api-key: "${POLYGON_API_KEY}"
  tiingo-api-key: "${TIINGO_API_KEY}"
  openai-api-key: "${OPENAI_API_KEY}"
  database-url: "${DATABASE_URL}"
  slack-webhook-url: "${SLACK_WEBHOOK_URL}"
EOF

# Replace the original file with the temporary file
mv "${TMP_FILE}" "${SECRETS_FILE}"
chmod 600 "${SECRETS_FILE}"  # Restrict permissions for security

log "Secrets file updated successfully."

# Check if Kubernetes cluster is running
if ! kubectl cluster-info &>/dev/null; then
    log "Warning: Kubernetes cluster is not running. Please start the cluster before applying secrets."
    log "You can apply the secrets later using: kubectl apply -f ${SECRETS_FILE}"
    exit 0
fi

# Apply the secrets to the Kubernetes cluster
log "Applying secrets to Kubernetes cluster..."
kubectl apply -f "${SECRETS_FILE}" && log "Secrets applied successfully." || log "Error: Failed to apply secrets."

# Restart the data agent deployment to use the new secrets
log "Restarting data agent deployment to use new secrets..."
kubectl rollout restart deployment/data-agent -n market-data && \
    log "Data agent deployment restarted successfully." || \
    log "Error: Failed to restart data agent deployment."

log "Secrets update process completed."
