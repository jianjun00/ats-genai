#!/bin/bash
# Script to set up Kind (Kubernetes IN Docker) for local development
# This replaces the Docker Desktop Kubernetes setup

set -e

echo "Setting up Kind Kubernetes cluster for ATS-GenAI project..."

# Check if Docker is installed and running
if ! docker info > /dev/null 2>&1; then
  echo "Docker is not running. Please start Docker first."
  exit 1
fi

# Check if Kind is installed
if ! command -v kind &> /dev/null; then
  echo "Kind is not installed. Installing Kind..."
  curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.20.0/kind-linux-amd64
  chmod +x ./kind
  sudo mv ./kind /usr/local/bin/kind
  echo "Kind installed successfully."
fi

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
  echo "kubectl is not installed. Installing kubectl..."
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  chmod +x kubectl
  sudo mv kubectl /usr/local/bin/
  echo "kubectl installed successfully."
fi

# Create Kind configuration file
cat > kind-config.yaml << EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: ats-dev
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: InitConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "ingress-ready=true"
  extraPortMappings:
  - containerPort: 80
    hostPort: 80
    protocol: TCP
  - containerPort: 443
    hostPort: 443
    protocol: TCP
EOF

# Create Kind cluster
echo "Creating Kind cluster 'ats-dev'..."
kind create cluster --config kind-config.yaml

# Verify cluster is running
echo "Verifying cluster..."
kubectl cluster-info
kubectl get nodes

# Create ats-dev namespace if it doesn't exist
if ! kubectl get namespace ats-dev > /dev/null 2>&1; then
  echo "Creating ats-dev namespace..."
  kubectl create namespace ats-dev
  echo "ats-dev namespace created."
else
  echo "ats-dev namespace already exists."
fi

# Configure kubectl context
kubectl config use-context kind-ats-dev

echo "Kind Kubernetes setup complete!"
echo "You can now use kubectl with the 'kind-ats-dev' context."
echo "To delete this cluster later, run: kind delete cluster --name ats-dev"
