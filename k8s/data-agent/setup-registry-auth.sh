#!/bin/bash
# Script to set up registry authentication for Data Agent deployment
# Usage: ./setup-registry-auth.sh <project_id> <namespace>

set -e

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <project_id> <namespace>"
    echo "Example: $0 ats-genai market-data"
    exit 1
fi

PROJECT_ID=$1
NAMESPACE=$2
SERVICE_ACCOUNT_NAME="registry-access"
KEY_FILE="registry-key.json"

echo "Setting up registry authentication for project $PROJECT_ID in namespace $NAMESPACE"

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "Error: gcloud is not installed. Please install Google Cloud SDK first."
    exit 1
fi

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
    echo "Error: kubectl is not installed. Please install kubectl first."
    exit 1
fi

# Check if namespace exists
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo "Creating namespace $NAMESPACE..."
    kubectl create namespace "$NAMESPACE"
fi

echo "Creating service account for registry access..."
gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
    --display-name="Registry Access Service Account" \
    --project="$PROJECT_ID" || echo "Service account already exists, continuing..."

echo "Granting permissions to service account..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectViewer"

echo "Creating and downloading JSON key..."
gcloud iam service-accounts keys create "$KEY_FILE" \
    --iam-account="$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
    --project="$PROJECT_ID"

echo "Creating Kubernetes secret with JSON key..."
kubectl create secret docker-registry registry-credentials \
    --docker-server=gcr.io \
    --docker-username=_json_key \
    --docker-password="$(cat $KEY_FILE)" \
    --docker-email="admin@$PROJECT_ID.com" \
    --namespace="$NAMESPACE" \
    --dry-run=client -o yaml | kubectl apply -f -

echo "Cleaning up sensitive files..."
rm -f "$KEY_FILE"

echo "Registry authentication setup complete!"
echo "You can now update your deployment to use the 'registry-credentials' secret."
echo "Example:"
echo "---"
echo "spec:"
echo "  imagePullSecrets:"
echo "  - name: registry-credentials"
echo "  containers:"
echo "  - name: data-agent"
echo "    image: gcr.io/$PROJECT_ID/data-agent:latest"
echo "    imagePullPolicy: Always"
echo "---"

# Make the script executable
chmod +x "$0"
