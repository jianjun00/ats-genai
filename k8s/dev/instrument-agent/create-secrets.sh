#!/bin/bash
# Script to create secrets for Instrument Agent deployment
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

# Use namespace from environment file or default to ats-dev
NAMESPACE=${K8S_NAMESPACE_INSTRUMENT:-ats-dev}

# Check if namespace exists, create if it doesn't
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo "Creating namespace $NAMESPACE..."
    kubectl create namespace "$NAMESPACE"
fi

# Get values from environment file
DB_USER=${DB_USER}
DB_PASSWORD=${DB_PASSWORD}
POLYGON_API_KEY=${POLYGON_API_KEY}

# Create the secret
echo "Creating instrument-agent-secrets in namespace $NAMESPACE..."

# Build the kubectl command
CMD="kubectl create secret generic instrument-agent-secrets \
  --namespace $NAMESPACE \
  --from-literal=DB_USER=$DB_USER \
  --from-literal=DB_PASSWORD=$DB_PASSWORD \
  --from-literal=POLYGON_API_KEY=$POLYGON_API_KEY"

# Add --dry-run=client -o yaml | kubectl apply -f - to handle existing secrets
CMD="$CMD --dry-run=client -o yaml | kubectl apply -f -"

# Execute the command
eval $CMD

echo "Secret 'instrument-agent-secrets' created successfully in namespace '$NAMESPACE'!"
echo "You can now apply the ArgoCD application manifest:"
echo "kubectl apply -f argocd-application.yaml -n argocd"

# Make the script executable
chmod +x "$0"
