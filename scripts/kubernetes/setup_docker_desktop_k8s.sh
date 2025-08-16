#!/bin/bash
# Script to set up Docker Desktop Kubernetes configuration for WSL
# NOTE: DO NOT USE KIND OR MINIKUBE FOR KUBERNETES SETUP.

# Create .kube directory if it doesn't exist
mkdir -p ~/.kube

# Check if Docker Desktop is running
if ! docker info > /dev/null 2>&1; then
  echo "Docker Desktop is not running. Please start Docker Desktop first."
  exit 1
fi

# Check if Kubernetes is enabled in Docker Desktop
if ! minikube status | grep -q "apiserver: Running"; then
  echo "Kubernetes is not enabled in Docker Desktop."
  echo "Please enable Kubernetes in Docker Desktop settings and try again."
  exit 1
fi

# Check for WSL-specific Docker Desktop Kubernetes config locations
POTENTIAL_CONFIG_PATHS=(
  "~/.kube/config"
  "/mnt/c/Users/$USER/.docker/desktop/kubernetes-admin-conf.yml"
  "/mnt/c/Users/$USER/AppData/Roaming/Docker/kubernetes/config"
  "/mnt/c/Program Files/Docker/Docker/Resources/kubernetes/config"
)

CONFIG_FOUND=false

for CONFIG_PATH in "${POTENTIAL_CONFIG_PATHS[@]}"; do
  EXPANDED_PATH=$(eval echo $CONFIG_PATH)
  if [ -f "$EXPANDED_PATH" ]; then
    echo "Found Kubernetes config at $EXPANDED_PATH"
    cp "$EXPANDED_PATH" ~/.kube/config
    chmod 600 ~/.kube/config
    CONFIG_FOUND=true
    break
  fi
done

if [ "$CONFIG_FOUND" = false ]; then
  echo "Could not find Docker Desktop Kubernetes config file."
  echo "Please make sure Kubernetes is enabled in Docker Desktop settings."
  exit 1
fi

echo "Docker Desktop Kubernetes configuration has been set up."
echo "Testing connection..."

# Test the connection
kubectl get nodes

if [ $? -eq 0 ]; then
  echo "Successfully connected to Docker Desktop Kubernetes cluster!"
else
  echo "Failed to connect to Docker Desktop Kubernetes cluster."
  echo "Please check if Kubernetes is enabled in Docker Desktop settings."
  exit 1
fi

# Create ats-dev namespace if it doesn't exist
if ! kubectl get namespace ats-dev > /dev/null 2>&1; then
  echo "Creating ats-dev namespace..."
  kubectl create namespace ats-dev
  echo "ats-dev namespace created."
else
  echo "ats-dev namespace already exists."
fi
