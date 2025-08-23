#!/bin/bash
set -euo pipefail

# ArgoCD Integration Setup Script for GitHub Actions
echo "🚀 Setting up ArgoCD Integration for GitHub Actions"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Configuration
ARGOCD_NAMESPACE="argocd"
SERVICE_ACCOUNT_NAME="github-actions-argocd"
TOKEN_SECRET_NAME="${SERVICE_ACCOUNT_NAME}-token"

# Check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check kubectl
    if ! command -v kubectl > /dev/null 2>&1; then
        print_error "kubectl not found. Please install kubectl."
        exit 1
    fi
    
    # Check ArgoCD namespace
    if ! kubectl get namespace "$ARGOCD_NAMESPACE" > /dev/null 2>&1; then
        print_error "ArgoCD namespace '$ARGOCD_NAMESPACE' not found"
        exit 1
    fi
    
    # Check ArgoCD server pod
    if ! kubectl get pods -n "$ARGOCD_NAMESPACE" -l app.kubernetes.io/name=argocd-server > /dev/null 2>&1; then
        print_error "ArgoCD server not found in namespace '$ARGOCD_NAMESPACE'"
        exit 1
    fi
    
    print_success "Prerequisites check passed"
}

# Get ArgoCD server details
get_argocd_server() {
    print_status "Determining ArgoCD server URL..."
    
    # Check if external service exists
    if kubectl get svc argocd-server-external -n "$ARGOCD_NAMESPACE" > /dev/null 2>&1; then
        # Get NodePort details
        NODE_PORT=$(kubectl get svc argocd-server-external -n "$ARGOCD_NAMESPACE" -o jsonpath='{.spec.ports[?(@.name=="https")].nodePort}')
        NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
        
        if [[ -n "$NODE_PORT" && -n "$NODE_IP" ]]; then
            ARGOCD_SERVER="https://${NODE_IP}:${NODE_PORT}"
            print_success "Found external ArgoCD service: $ARGOCD_SERVER"
        else
            print_warning "External service exists but couldn't determine URL"
        fi
    fi
    
    # Fallback to port-forward URL
    if [[ -z "${ARGOCD_SERVER:-}" ]]; then
        ARGOCD_SERVER="https://localhost:8080"
        print_warning "Using port-forward URL: $ARGOCD_SERVER"
        print_status "You'll need to run: kubectl port-forward svc/argocd-server -n argocd 8080:443"
    fi
    
    echo "ARGOCD_SERVER=${ARGOCD_SERVER}" >> /tmp/argocd-config.env
}

# Create service account for GitHub Actions
create_service_account() {
    print_status "Creating service account for GitHub Actions..."
    
    # Create service account
    kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SERVICE_ACCOUNT_NAME}
  namespace: ${ARGOCD_NAMESPACE}
  annotations:
    description: "Service account for GitHub Actions ArgoCD integration"
EOF
    
    # Create token secret (for Kubernetes 1.24+)
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${TOKEN_SECRET_NAME}
  namespace: ${ARGOCD_NAMESPACE}
  annotations:
    kubernetes.io/service-account.name: ${SERVICE_ACCOUNT_NAME}
    description: "Token for GitHub Actions ArgoCD integration"
type: kubernetes.io/service-account-token
EOF
    
    print_success "Service account created"
}

# Create RBAC permissions
create_rbac() {
    print_status "Creating RBAC permissions..."
    
    # Create ClusterRole with ArgoCD permissions
    kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ${SERVICE_ACCOUNT_NAME}-role
  annotations:
    description: "Role for GitHub Actions ArgoCD integration"
rules:
- apiGroups: ["argoproj.io"]
  resources: ["applications", "appprojects"]
  verbs: ["get", "list", "create", "update", "patch", "delete"]
- apiGroups: [""]
  resources: ["events"]
  verbs: ["create", "patch"]
- apiGroups: [""]
  resources: ["pods", "pods/log"]
  verbs: ["get", "list"]
EOF
    
    # Create ClusterRoleBinding
    kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ${SERVICE_ACCOUNT_NAME}-binding
  annotations:
    description: "Role binding for GitHub Actions ArgoCD integration"
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ${SERVICE_ACCOUNT_NAME}-role
subjects:
- kind: ServiceAccount
  name: ${SERVICE_ACCOUNT_NAME}
  namespace: ${ARGOCD_NAMESPACE}
EOF
    
    print_success "RBAC permissions created"
}

# Wait for token to be created
wait_for_token() {
    print_status "Waiting for token to be generated..."
    
    local retries=30
    local count=0
    
    while [[ $count -lt $retries ]]; do
        if kubectl get secret "$TOKEN_SECRET_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.data.token}' > /dev/null 2>&1; then
            TOKEN_DATA=$(kubectl get secret "$TOKEN_SECRET_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.data.token}')
            if [[ -n "$TOKEN_DATA" ]]; then
                print_success "Token generated successfully"
                return 0
            fi
        fi
        
        echo -n "."
        sleep 2
        ((count++))
    done
    
    print_error "Timeout waiting for token generation"
    return 1
}

# Get the service account token
get_token() {
    print_status "Retrieving service account token..."
    
    # Get token from secret
    TOKEN=$(kubectl get secret "$TOKEN_SECRET_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.data.token}' | base64 -d)
    
    if [[ -z "$TOKEN" ]]; then
        print_error "Failed to retrieve token"
        exit 1
    fi
    
    echo "ARGOCD_TOKEN=${TOKEN}" >> /tmp/argocd-config.env
    print_success "Token retrieved successfully"
}

# Test ArgoCD connection
test_connection() {
    print_status "Testing ArgoCD connection..."
    
    # Source the config
    source /tmp/argocd-config.env
    
    # Test connection
    print_status "Testing API connection to: $ARGOCD_SERVER"
    
    # Test version endpoint (doesn't require authentication)
    if curl -k -s --max-time 10 "$ARGOCD_SERVER/api/version" > /dev/null; then
        print_success "✅ ArgoCD API is accessible"
    else
        print_warning "⚠️ Could not reach ArgoCD API (this is normal if using port-forward)"
        print_status "Make sure to run: kubectl port-forward svc/argocd-server -n argocd 8080:443"
    fi
    
    # Test authenticated endpoint
    print_status "Testing authenticated API call..."
    RESPONSE=$(curl -k -s --max-time 10 \
        -H "Authorization: Bearer $ARGOCD_TOKEN" \
        "$ARGOCD_SERVER/api/v1/applications" || echo "connection_failed")
    
    if [[ "$RESPONSE" == "connection_failed" ]]; then
        print_warning "⚠️ Could not test authenticated connection (may need port-forward)"
    elif echo "$RESPONSE" | grep -q '"items"'; then
        print_success "✅ Authenticated API access working"
    elif echo "$RESPONSE" | grep -q "Unauthorized"; then
        print_error "❌ Token authentication failed"
    else
        print_warning "⚠️ Unexpected response: ${RESPONSE:0:100}..."
    fi
}

# Create ArgoCD Application for ATS
create_ats_application() {
    print_status "Creating ATS ArgoCD application..."
    
    source /tmp/argocd-config.env
    
    # Get repository URL
    REPO_URL=$(git config --get remote.origin.url || echo "https://github.com/AkoloTechnologies/ats-genai.git")
    
    # Create application definition
    cat > /tmp/ats-dev-app.json <<EOF
{
  "apiVersion": "argoproj.io/v1alpha1",
  "kind": "Application",
  "metadata": {
    "name": "ats-dev",
    "namespace": "argocd",
    "finalizers": ["resources-finalizer.argocd.argoproj.io"]
  },
  "spec": {
    "project": "default",
    "source": {
      "repoURL": "$REPO_URL",
      "targetRevision": "main",
      "path": "k8s"
    },
    "destination": {
      "server": "https://kubernetes.default.svc",
      "namespace": "ats-dev"
    },
    "syncPolicy": {
      "automated": {
        "prune": false,
        "selfHeal": true
      },
      "syncOptions": [
        "CreateNamespace=true"
      ]
    }
  }
}
EOF
    
    # Try to create the application
    if curl -k -s --max-time 10 \
        -H "Authorization: Bearer $ARGOCD_TOKEN" \
        -H "Content-Type: application/json" \
        -d @/tmp/ats-dev-app.json \
        "$ARGOCD_SERVER/api/v1/applications" > /tmp/create-response.json; then
        
        if grep -q '"name":"ats-dev"' /tmp/create-response.json; then
            print_success "✅ ATS application created successfully"
        else
            print_warning "⚠️ Application creation response: $(cat /tmp/create-response.json)"
        fi
    else
        print_warning "⚠️ Could not create application (may need port-forward or manual creation)"
    fi
    
    # Clean up temp files
    rm -f /tmp/ats-dev-app.json /tmp/create-response.json
}

# Generate GitHub secrets configuration
generate_github_config() {
    print_status "Generating GitHub secrets configuration..."
    
    source /tmp/argocd-config.env
    
    cat > argocd-github-secrets.txt <<EOF
# GitHub Repository Secrets for ArgoCD Integration
# Add these secrets to your GitHub repository settings:
# Repository Settings > Secrets and Variables > Actions

## Required Secrets

ARGOCD_SERVER=${ARGOCD_SERVER}

ARGOCD_TOKEN=${ARGOCD_TOKEN}

## Optional but Recommended

SLACK_WEBHOOK_URL=https://hooks.slack.com/services/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

# For GitOps manifest updates (use your GitHub token)
GITOPS_TOKEN=<your-github-token>

## Instructions

1. Go to your GitHub repository
2. Navigate to Settings > Secrets and Variables > Actions
3. Click "New repository secret"
4. Add each secret above with the corresponding value

## Testing

After adding secrets, test the workflow with:
git checkout -b test/argocd-integration
echo "# Test ArgoCD integration" >> README.md
git add README.md
git commit -m "test: ArgoCD integration"
git push origin test/argocd-integration

EOF
    
    print_success "GitHub secrets configuration saved to: argocd-github-secrets.txt"
}

# Create ArgoCD CLI configuration
create_cli_config() {
    print_status "Creating ArgoCD CLI configuration..."
    
    source /tmp/argocd-config.env
    
    # Create argocd CLI config directory
    mkdir -p ~/.argocd
    
    cat > ~/.argocd/config <<EOF
contexts:
- name: ats-argocd
  server: ${ARGOCD_SERVER}
  user: github-actions
current-context: ats-argocd
servers:
- server: ${ARGOCD_SERVER}
  insecure: true
users:
- name: github-actions
  auth-token: ${ARGOCD_TOKEN}
EOF
    
    print_success "ArgoCD CLI config created at ~/.argocd/config"
    
    # Test CLI if available
    if command -v argocd > /dev/null 2>&1; then
        print_status "Testing ArgoCD CLI..."
        if argocd app list > /dev/null 2>&1; then
            print_success "✅ ArgoCD CLI working"
        else
            print_warning "⚠️ ArgoCD CLI test failed (may need port-forward)"
        fi
    else
        print_status "ArgoCD CLI not installed. Install with:"
        echo "  curl -sSL -o argocd-linux-amd64 https://github.com/argoproj/argo-cd/releases/latest/download/argocd-linux-amd64"
        echo "  sudo install -m 555 argocd-linux-amd64 /usr/local/bin/argocd"
    fi
}

# Main execution
main() {
    echo "🔧 ArgoCD Integration Setup"
    echo "=========================="
    echo ""
    
    # Clean up any previous runs
    rm -f /tmp/argocd-config.env
    
    check_prerequisites
    echo ""
    
    get_argocd_server
    echo ""
    
    create_service_account
    echo ""
    
    create_rbac
    echo ""
    
    wait_for_token
    echo ""
    
    get_token
    echo ""
    
    test_connection
    echo ""
    
    create_ats_application
    echo ""
    
    generate_github_config
    echo ""
    
    create_cli_config
    echo ""
    
    print_success "🎉 ArgoCD Integration Setup Complete!"
    
    echo ""
    print_status "📋 Next Steps:"
    echo "  1. Review the generated secrets: cat argocd-github-secrets.txt"
    echo "  2. Add secrets to GitHub repository settings"
    echo "  3. Test the integration with a pull request"
    echo "  4. Deploy improved workflow: ./scripts/ci-cd/migrate-workflow.sh"
    echo ""
    
    if [[ "$ARGOCD_SERVER" == "https://localhost:8080" ]]; then
        print_warning "⚠️  Remember to run port-forward for local access:"
        echo "     kubectl port-forward svc/argocd-server -n argocd 8080:443"
    fi
    
    # Clean up
    rm -f /tmp/argocd-config.env
}

# Run main function
main "$@"