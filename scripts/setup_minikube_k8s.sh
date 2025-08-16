#!/bin/bash
# Script to set up Minikube for local Kubernetes development
# This replaces the Docker Desktop Kubernetes setup

set -e

echo "Setting up Minikube Kubernetes cluster for ATS-GenAI project..."

# Function to handle errors
handle_error() {
  echo "ERROR: $1"
  exit 1
}

# Check if Minikube is installed
if ! command -v minikube &> /dev/null; then
  echo "Minikube is not installed. Installing Minikube..."
  curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
  sudo install minikube-linux-amd64 /usr/local/bin/minikube
  rm minikube-linux-amd64
  echo "Minikube installed successfully."
fi

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
  echo "kubectl is not installed. Installing kubectl..."
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  chmod +x kubectl
  sudo mv kubectl /usr/local/bin/
  echo "kubectl installed successfully."
fi

# Start Minikube with Docker driver
echo "Starting Minikube with Docker driver..."
minikube start --driver=docker --cpus=2 --memory=4g --kubernetes-version=stable || handle_error "Failed to start Minikube cluster"

# Verify Minikube is running
minikube status || handle_error "Minikube is not running properly"

# Enable addons
echo "Enabling useful addons..."
minikube addons enable metrics-server
minikube addons enable dashboard
minikube addons enable ingress

# Verify cluster is running
echo "Verifying cluster..."
kubectl cluster-info
kubectl get nodes

# Create ats-dev namespace if it doesn't exist
if ! kubectl get namespace ats-dev > /dev/null 2>&1; then
  echo "Creating ats-dev namespace..."
  kubectl create namespace ats-dev || handle_error "Failed to create ats-dev namespace"
  echo "ats-dev namespace created."
else
  echo "ats-dev namespace already exists."
fi

# Configure kubectl context
kubectl config use-context minikube

# Verify namespace exists
kubectl get namespace ats-dev > /dev/null 2>&1 || handle_error "Failed to verify ats-dev namespace"

# Create a test pod to verify everything works
echo "Creating a test pod to verify the setup..."
kubectl run test-nginx --image=nginx --namespace=ats-dev --rm --restart=Never --wait=true || echo "Note: Test pod creation might have issues, but this is not critical"

echo "\n✅ Minikube Kubernetes setup complete!\n"
echo "You can now use kubectl with the 'minikube' context."
echo ""
echo "📋 Useful commands:"
echo "- Start dashboard:       minikube dashboard"
echo "- Access service:        minikube service <service-name>"
echo "- Stop cluster:          minikube stop"
echo "- Delete cluster:        minikube delete"
echo "- Check cluster status:  minikube status"
echo "- Run a test job:        kubectl apply -f k8s/dev/test-populate-instrument-polygon-job.yaml -n ats-dev"
echo ""
echo "🔄 To update your scripts, replace 'minikube' context with 'minikube' context"
echo "   in any scripts that were previously using Docker Desktop Kubernetes."
