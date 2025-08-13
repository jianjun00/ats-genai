#!/bin/bash

# This script helps Argo CD handle symlinks properly during sync
# It creates a clean copy of the repository without problematic symlinks

set -e

# Create a temporary directory for the clean copy
TEMP_DIR=$(mktemp -d)
echo "Created temporary directory: $TEMP_DIR"

# Get GitHub token from Kubernetes secret
GITHUB_TOKEN=$(kubectl get secret argocd-github-token -n argocd -o jsonpath='{.data.token}' | base64 -d)
echo "Retrieved GitHub token for authentication"

# Clone the repository to the temporary directory using the token
git clone https://x-access-token:${GITHUB_TOKEN}@github.com/jianjun00/ats-genai.git $TEMP_DIR
echo "Cloned repository to temporary directory"

# Remove problematic symlinks
find $TEMP_DIR -type l -delete
echo "Removed symlinks from repository"

# Apply the manifests
kubectl apply -k $TEMP_DIR/k8s/environments/dev
kubectl apply -k $TEMP_DIR/k8s/environments/intg

# Clean up
rm -rf $TEMP_DIR
echo "Cleaned up temporary directory"

echo "Sync completed successfully"
