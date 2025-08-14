#!/bin/bash
# Script to create secrets for Data Agent deployment
# Usage: ./create-secrets.sh [environment]
# Example: ./create-secrets.sh dev

set -e

# Default to dev environment if not specified
ENV=${1:-dev}
ENV_FILE="/home/jianjun/ats-genai/.env.$ENV"

# Check if environment file exists
if [ ! -f "$ENV_FILE" ]; then
    echo "Error: Environment file $ENV_FILE not found."
    echo "Available environments: dev, test, prod"
    exit 1
fi

# Source the environment file
echo "Using environment file: $ENV_FILE"
source "$ENV_FILE"

# Use namespace from environment file or default to market-data
NAMESPACE=${K8S_NAMESPACE_DATA:-market-data}

# Check if namespace exists, create if it doesn't
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo "Creating namespace $NAMESPACE..."
    kubectl create namespace "$NAMESPACE"
fi

# Get values from environment file
POLYGON_API_KEY=${POLYGON_API_KEY}
TIINGO_API_KEY=${TIINGO_API_KEY}
OPENAI_API_KEY=${OPENAI_API_KEY}
DATABASE_URL=${TSDB_URL}
SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL:-""}

# Create the secret
echo "Creating data-agent-secrets in namespace $NAMESPACE..."

# Build the kubectl command
CMD="kubectl create secret generic data-agent-secrets \
  --namespace $NAMESPACE \
  --from-literal=polygon-api-key=$POLYGON_API_KEY \
  --from-literal=tiingo-api-key=$TIINGO_API_KEY \
  --from-literal=openai-api-key=$OPENAI_API_KEY \
  --from-literal=database-url=$DATABASE_URL"

# Add Slack webhook URL if provided
if [ ! -z "$SLACK_WEBHOOK_URL" ]; then
  CMD="$CMD --from-literal=slack-webhook-url=$SLACK_WEBHOOK_URL"
fi

# Add --dry-run=client -o yaml | kubectl apply -f - to handle existing secrets
CMD="$CMD --dry-run=client -o yaml | kubectl apply -f -"

# Execute the command
eval $CMD

echo "Secret 'data-agent-secrets' created successfully in namespace '$NAMESPACE'!"
echo "You can now apply the ArgoCD application manifest:"
echo "kubectl apply -f argocd-application.yaml -n argocd"

# Make the script executable
chmod +x "$0"
