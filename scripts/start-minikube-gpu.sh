#!/bin/bash
# Simple wrapper for starting minikube with GPU support
# Usage: ./start-minikube-gpu.sh

echo "🚀 Starting Minikube with automated GPU setup..."
echo "This will take a few minutes on first run to install NVIDIA Container Toolkit"
echo ""

# Run the full setup script
exec "$(dirname "$0")/minikube-gpu-setup.sh"