#!/bin/bash
# Script to start Minikube when WSL starts

# Check if minikube is already running
if minikube status &>/dev/null; then
    echo "Minikube is already running"
    exit 0
fi

# Start minikube
echo "Starting Minikube..."
minikube start

# Check if it started successfully
if minikube status &>/dev/null; then
    echo "Minikube started successfully"
else
    echo "Failed to start Minikube"
    exit 1
fi
