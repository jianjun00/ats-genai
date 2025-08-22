#!/bin/bash
# Script to test Kubernetes secrets for database credentials

set -e

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
NAMESPACE="ats-dev"
ENV_FILE=".env.dev"
SECRET_NAME="db-credentials-dev"

# Function to display usage
function show_usage {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -n, --namespace NAMESPACE   Kubernetes namespace (default: ats-dev)"
    echo "  -e, --env-file FILE         Path to .env file (default: .env.dev)"
    echo "  -s, --secret-name NAME      Name of the Kubernetes secret (default: db-credentials-dev)"
    echo "  -h, --help                  Show this help message"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        -e|--env-file)
            ENV_FILE="$2"
            shift 2
            ;;
        -s|--secret-name)
            SECRET_NAME="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Check if the .env file exists
if [ ! -f "$PROJECT_ROOT/$ENV_FILE" ]; then
    echo "Error: $ENV_FILE file not found in $PROJECT_ROOT"
    exit 1
fi

echo "=== Testing Kubernetes Secrets for Database Credentials ==="
echo "Namespace: $NAMESPACE"
echo "Environment file: $ENV_FILE"
echo "Secret name: $SECRET_NAME"
echo

# Step 1: Generate Kubernetes secret from .env file
echo "Step 1: Generating Kubernetes secret from $ENV_FILE..."
"$SCRIPT_DIR/create_k8s_secrets.sh" --env-file "$PROJECT_ROOT/$ENV_FILE" --output-dir "$PROJECT_ROOT/k8s/secrets"

# Step 2: Apply the secret to Kubernetes
echo -e "\nStep 2: Applying secret to Kubernetes..."
kubectl apply -f "$PROJECT_ROOT/k8s/secrets/$SECRET_NAME.yaml"

# Step 3: Verify that the secret exists
echo -e "\nStep 3: Verifying that the secret exists..."
kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o yaml

# Step 4: Create a test pod that uses the secret
echo -e "\nStep 4: Creating a test pod that uses the secret..."
cat <<EOF > "$PROJECT_ROOT/k8s/secrets/test-pod.yaml"
apiVersion: v1
kind: Pod
metadata:
  name: db-secret-test-pod
  namespace: $NAMESPACE
spec:
  containers:
  - name: db-secret-test
    image: dragonflyer762/ats-genai:dev-latest
    command: ["sh", "-c", "echo 'Testing DB credentials from secret'; echo 'DB_USER=\$DB_USER'; echo 'DB_PASSWORD=\$DB_PASSWORD'; echo 'DB_NAME=\$DB_NAME'; sleep 30"]
    env:
    - name: DB_USER
      valueFrom:
        secretKeyRef:
          name: $SECRET_NAME
          key: DB_USER
    - name: DB_PASSWORD
      valueFrom:
        secretKeyRef:
          name: $SECRET_NAME
          key: DB_PASSWORD
    - name: DB_NAME
      valueFrom:
        secretKeyRef:
          name: $SECRET_NAME
          key: DB_NAME
  restartPolicy: Never
EOF

kubectl apply -f "$PROJECT_ROOT/k8s/secrets/test-pod.yaml"

# Step 5: Wait for the pod to start
echo -e "\nStep 5: Waiting for the pod to start..."
kubectl wait --for=condition=Ready pod/db-secret-test-pod -n "$NAMESPACE" --timeout=60s

# Step 6: Check the pod logs
echo -e "\nStep 6: Checking the pod logs..."
kubectl logs db-secret-test-pod -n "$NAMESPACE"

# Step 7: Clean up
echo -e "\nStep 7: Cleaning up..."
kubectl delete pod db-secret-test-pod -n "$NAMESPACE"

echo -e "\nTest completed successfully!"
