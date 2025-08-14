#!/bin/bash
# Script to set up registry authentication using Docker Desktop credentials
# Usage: ./docker-registry-auth.sh <namespace> <registry-url>
# Example: ./docker-registry-auth.sh market-data docker.io

set -e

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <namespace> <registry-url>"
    echo "Example: $0 market-data docker.io"
    exit 1
fi

NAMESPACE=$1
REGISTRY_URL=$2
SECRET_NAME="registry-credentials"

echo "Setting up registry authentication for $REGISTRY_URL in namespace $NAMESPACE"

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
    echo "Error: kubectl is not installed. Please install kubectl first."
    exit 1
fi

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Error: docker is not installed. Please install Docker first."
    exit 1
fi

# Check if namespace exists
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo "Creating namespace $NAMESPACE..."
    kubectl create namespace "$NAMESPACE"
fi

# Get Docker config.json path based on OS
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    # Windows
    CONFIG_PATH="$USERPROFILE/.docker/config.json"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    CONFIG_PATH="$HOME/.docker/config.json"
else
    # Linux
    CONFIG_PATH="$HOME/.docker/config.json"
fi

# Check if Docker config exists
if [ ! -f "$CONFIG_PATH" ]; then
    echo "Error: Docker config not found at $CONFIG_PATH"
    echo "Please log in to Docker using 'docker login' first."
    exit 1
fi

echo "Creating Kubernetes secret with Docker credentials..."
kubectl create secret generic $SECRET_NAME \
    --from-file=.dockerconfigjson="$CONFIG_PATH" \
    --type=kubernetes.io/dockerconfigjson \
    --namespace="$NAMESPACE" \
    --dry-run=client -o yaml | kubectl apply -f -

echo "Registry authentication setup complete!"
echo "You can now update your deployment to use the '$SECRET_NAME' secret."
echo "Example:"
echo "---"
echo "spec:"
echo "  imagePullSecrets:"
echo "  - name: $SECRET_NAME"
echo "  containers:"
echo "  - name: your-container"
echo "    image: $REGISTRY_URL/your-image:latest"
echo "    imagePullPolicy: Always"
echo "---"
