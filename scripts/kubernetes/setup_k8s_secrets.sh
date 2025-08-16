#!/bin/bash
# Script to set up Kubernetes secrets for ATS environments

# Function to create secrets for a namespace
create_secrets() {
    local namespace=$1
    local secret_suffix=$2
    
    echo "Creating secrets for namespace: $namespace"
    
    # Create db-credentials secret
    kubectl create secret generic db-credentials \
        --from-literal=DB_USER=postgres \
        --from-literal=DB_PASSWORD=dev_password \
        --from-literal=DB_NAME=dev_db \
        -n $namespace
        
    # Create api-keys secret
    kubectl create secret generic api-keys \
        --from-literal=polygon-api-key=wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD \
        -n $namespace
    
    echo "Secrets created successfully in namespace: $namespace"
}

# Check if namespace exists, create if it doesn't
ensure_namespace() {
    local namespace=$1
    
    if kubectl get namespace $namespace &> /dev/null; then
        echo "Namespace $namespace already exists"
    else
        echo "Creating namespace $namespace"
        kubectl create namespace $namespace
    fi
}

# Main script
if [ $# -eq 0 ]; then
    echo "Usage: $0 <namespace> [namespace2 namespace3 ...]"
    echo "Example: $0 ats-dev ats-intg ats-prod"
    exit 1
fi

for namespace in "$@"; do
    ensure_namespace $namespace
    create_secrets $namespace
done

echo "All secrets created successfully!"
