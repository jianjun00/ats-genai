#!/bin/bash

# ATS Platform CI/CD and GitOps Setup Script
#
# This script sets up the complete CI/CD pipeline with Argo CD for GitOps deployment
# 
# Prerequisites:
# - Kubernetes cluster with kubectl access
# - GitHub repository with appropriate permissions
# - Docker registry access (GitHub Container Registry)

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="argocd"
ARGOCD_VERSION="v2.8.4"
ATS_NAMESPACE="ats-dev"
GITHUB_ORG="${GITHUB_ORG:-your-org}"
GITHUB_REPO="${GITHUB_REPO:-ats-platform}"
GITOPS_REPO="${GITOPS_REPO:-ats-gitops}"

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed or not in PATH"
        exit 1
    fi
    
    # Check cluster connectivity
    if ! kubectl cluster-info &> /dev/null; then
        log_error "Cannot connect to Kubernetes cluster"
        exit 1
    fi
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check GitHub CLI (optional but recommended)
    if ! command -v gh &> /dev/null; then
        log_warning "GitHub CLI (gh) not found. Some features may not work."
    fi
    
    log_success "Prerequisites check passed"
}

install_argocd() {
    log_info "Installing Argo CD..."
    
    # Create namespace
    kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
    
    # Install Argo CD
    kubectl apply -n "$NAMESPACE" -f "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/install.yaml"
    
    # Wait for Argo CD to be ready
    log_info "Waiting for Argo CD to be ready..."
    kubectl wait --for=condition=available --timeout=600s deployment/argocd-server -n "$NAMESPACE"
    kubectl wait --for=condition=available --timeout=600s deployment/argocd-application-controller -n "$NAMESPACE"
    kubectl wait --for=condition=available --timeout=600s deployment/argocd-repo-server -n "$NAMESPACE"
    
    log_success "Argo CD installed successfully"
}

configure_argocd() {
    log_info "Configuring Argo CD..."
    
    # Install Argo CD CLI
    if ! command -v argocd &> /dev/null; then
        log_info "Installing Argo CD CLI..."
        curl -sSL -o argocd-linux-amd64 https://github.com/argoproj/argo-cd/releases/download/${ARGOCD_VERSION}/argocd-linux-amd64
        sudo install -m 555 argocd-linux-amd64 /usr/local/bin/argocd
        rm argocd-linux-amd64
    fi
    
    # Get initial admin password
    ARGOCD_PASSWORD=$(kubectl -n "$NAMESPACE" get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d)
    
    # Port forward to access Argo CD (in background)
    kubectl port-forward svc/argocd-server -n "$NAMESPACE" 8080:443 &
    PORT_FORWARD_PID=$!
    sleep 10
    
    # Login to Argo CD
    argocd login localhost:8080 --username admin --password "$ARGOCD_PASSWORD" --insecure
    
    # Update admin password
    NEW_PASSWORD=$(openssl rand -base64 32)
    argocd account update-password --account admin --current-password "$ARGOCD_PASSWORD" --new-password "$NEW_PASSWORD"
    
    # Save new password to secret
    kubectl create secret generic argocd-admin-password \
        --from-literal=password="$NEW_PASSWORD" \
        -n "$NAMESPACE" \
        --dry-run=client -o yaml | kubectl apply -f -
    
    # Stop port forward
    kill $PORT_FORWARD_PID
    
    log_success "Argo CD configured successfully"
    log_info "Admin password saved to secret 'argocd-admin-password' in namespace '$NAMESPACE'"
}

setup_gitops_repository() {
    log_info "Setting up GitOps repository..."
    
    if command -v gh &> /dev/null; then
        # Check if GitOps repo exists
        if ! gh repo view "$GITHUB_ORG/$GITOPS_REPO" &> /dev/null; then
            log_info "Creating GitOps repository..."
            gh repo create "$GITHUB_ORG/$GITOPS_REPO" --private --description "GitOps repository for ATS Platform"
        fi
        
        # Clone and setup GitOps repo structure
        if [ ! -d "/tmp/$GITOPS_REPO" ]; then
            git clone "https://github.com/$GITHUB_ORG/$GITOPS_REPO.git" "/tmp/$GITOPS_REPO"
        fi
        
        cd "/tmp/$GITOPS_REPO"
        
        # Create directory structure
        mkdir -p {applications,environments/{dev,staging,prod},base/{rbac,network-policies}}
        
        # Copy application manifests
        cp -r "$(dirname "$0")/../argocd/applications/"* applications/
        cp -r "$(dirname "$0")/../argocd/environments/"* environments/
        
        # Update repository URLs in manifests
        find . -name "*.yaml" -type f -exec sed -i "s|your-org|$GITHUB_ORG|g" {} \;
        find . -name "*.yaml" -type f -exec sed -i "s|ats-gitops|$GITOPS_REPO|g" {} \;
        
        # Commit and push
        git add .
        git commit -m "Initial GitOps repository setup for ATS Platform" || true
        git push
        
        cd - > /dev/null
        
        log_success "GitOps repository configured"
    else
        log_warning "GitHub CLI not available. Please manually create GitOps repository: $GITHUB_ORG/$GITOPS_REPO"
    fi
}

deploy_ats_application() {
    log_info "Deploying ATS application to Argo CD..."
    
    # Create ATS dev namespace
    kubectl create namespace "$ATS_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
    
    # Apply Argo CD application manifests
    kubectl apply -f argocd/applications/ats-dev-app.yaml -n "$NAMESPACE"
    
    # Wait for application to sync
    log_info "Waiting for application to sync..."
    sleep 30
    
    # Port forward to access Argo CD
    kubectl port-forward svc/argocd-server -n "$NAMESPACE" 8080:443 &
    PORT_FORWARD_PID=$!
    sleep 10
    
    # Get admin password
    ARGOCD_PASSWORD=$(kubectl -n "$NAMESPACE" get secret argocd-admin-password -o jsonpath="{.data.password}" | base64 -d)
    
    # Login and sync application
    argocd login localhost:8080 --username admin --password "$ARGOCD_PASSWORD" --insecure
    argocd app sync ats-dev --timeout 600
    argocd app wait ats-dev --timeout 600 --health
    
    # Stop port forward
    kill $PORT_FORWARD_PID
    
    log_success "ATS application deployed successfully"
}

setup_github_secrets() {
    log_info "Setting up GitHub repository secrets..."
    
    if command -v gh &> /dev/null; then
        # Get Argo CD admin password
        ARGOCD_PASSWORD=$(kubectl -n "$NAMESPACE" get secret argocd-admin-password -o jsonpath="{.data.password}" | base64 -d)
        
        # Get Argo CD server URL (external)
        ARGOCD_SERVER=$(kubectl get service argocd-server -n "$NAMESPACE" -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
        if [ -z "$ARGOCD_SERVER" ]; then
            ARGOCD_SERVER=$(kubectl get service argocd-server -n "$NAMESPACE" -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')
        fi
        if [ -z "$ARGOCD_SERVER" ]; then
            ARGOCD_SERVER="argocd.your-domain.com"  # Replace with actual domain
            log_warning "Could not determine Argo CD server external address. Using placeholder: $ARGOCD_SERVER"
        fi
        
        # Get kubeconfig for GitHub Actions
        KUBE_CONFIG=$(kubectl config view --flatten --minify | base64 -w 0)
        
        # Create GitOps token
        GITOPS_TOKEN=$(gh auth token)
        
        # Set GitHub secrets
        gh secret set ARGOCD_SERVER --body "$ARGOCD_SERVER" --repo "$GITHUB_ORG/$GITHUB_REPO"
        gh secret set ARGOCD_USERNAME --body "admin" --repo "$GITHUB_ORG/$GITHUB_REPO"
        gh secret set ARGOCD_PASSWORD --body "$ARGOCD_PASSWORD" --repo "$GITHUB_ORG/$GITHUB_REPO"
        gh secret set KUBE_CONFIG_ATS_DEV --body "$KUBE_CONFIG" --repo "$GITHUB_ORG/$GITHUB_REPO"
        gh secret set GITOPS_TOKEN --body "$GITOPS_TOKEN" --repo "$GITHUB_ORG/$GITHUB_REPO"
        
        log_success "GitHub secrets configured"
    else
        log_warning "GitHub CLI not available. Please manually configure GitHub secrets:"
        echo "  ARGOCD_SERVER: Argo CD server URL"
        echo "  ARGOCD_USERNAME: admin"
        echo "  ARGOCD_PASSWORD: [from secret argocd-admin-password]"
        echo "  KUBE_CONFIG_ATS_DEV: [base64 encoded kubeconfig]"
        echo "  GITOPS_TOKEN: [GitHub token with repo access]"
    fi
}

get_access_info() {
    log_info "Getting access information..."
    
    # Get Argo CD URL
    ARGOCD_SERVER_IP=$(kubectl get service argocd-server -n "$NAMESPACE" -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
    if [ -z "$ARGOCD_SERVER_IP" ]; then
        ARGOCD_SERVER_IP=$(kubectl get service argocd-server -n "$NAMESPACE" -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')
    fi
    
    # Get admin password
    ARGOCD_PASSWORD=$(kubectl -n "$NAMESPACE" get secret argocd-admin-password -o jsonpath="{.data.password}" | base64 -d)
    
    echo ""
    log_success "=== ACCESS INFORMATION ==="
    echo ""
    echo "🔄 Argo CD Dashboard:"
    if [ -n "$ARGOCD_SERVER_IP" ]; then
        echo "   URL: https://$ARGOCD_SERVER_IP"
    else
        echo "   URL: Use port forwarding: kubectl port-forward svc/argocd-server -n $NAMESPACE 8080:443"
        echo "        Then access: https://localhost:8080"
    fi
    echo "   Username: admin"
    echo "   Password: $ARGOCD_PASSWORD"
    echo ""
    echo "🚀 GitHub Actions:"
    echo "   Repository: https://github.com/$GITHUB_ORG/$GITHUB_REPO"
    echo "   Workflow: .github/workflows/ci-pipeline.yml"
    echo ""
    echo "📂 GitOps Repository:"
    echo "   Repository: https://github.com/$GITHUB_ORG/$GITOPS_REPO"
    echo "   Applications: applications/"
    echo "   Environments: environments/"
    echo ""
    echo "🎯 ATS Dev Environment:"
    echo "   Namespace: $ATS_NAMESPACE"
    echo "   Application: ats-dev"
    echo ""
}

show_next_steps() {
    echo ""
    log_info "=== NEXT STEPS ==="
    echo ""
    echo "1. Configure vendor API keys:"
    echo "   kubectl edit secret vendor-api-keys -n $ATS_NAMESPACE"
    echo ""
    echo "2. Update database credentials:"
    echo "   kubectl edit secret database-credentials -n $ATS_NAMESPACE"
    echo ""
    echo "3. Customize application configuration:"
    echo "   Edit files in the GitOps repository: $GITHUB_ORG/$GITOPS_REPO"
    echo ""
    echo "4. Trigger first deployment:"
    echo "   git push to main branch will trigger CI/CD pipeline"
    echo ""
    echo "5. Monitor deployment:"
    echo "   Access Argo CD dashboard to monitor GitOps deployments"
    echo ""
    echo "6. Set up monitoring:"
    echo "   Grafana will be available at the configured ingress"
    echo ""
}

# Main execution
main() {
    echo ""
    log_info "=========================================="
    log_info "ATS Platform CI/CD and GitOps Setup"
    log_info "=========================================="
    echo ""
    
    check_prerequisites
    install_argocd
    configure_argocd
    setup_gitops_repository
    deploy_ats_application
    setup_github_secrets
    
    get_access_info
    show_next_steps
    
    echo ""
    log_success "CI/CD and GitOps setup completed successfully!"
    log_info "Your ATS platform is now ready for continuous deployment."
    echo ""
}

# Handle script arguments
case "${1:-}" in
    --help|-h)
        echo "Usage: $0 [--help|--argocd-only|--secrets-only]"
        echo ""
        echo "Options:"
        echo "  --help, -h           Show this help message"
        echo "  --argocd-only        Install and configure Argo CD only"
        echo "  --secrets-only       Configure GitHub secrets only"
        echo ""
        exit 0
        ;;
    --argocd-only)
        check_prerequisites
        install_argocd
        configure_argocd
        get_access_info
        exit 0
        ;;
    --secrets-only)
        check_prerequisites
        setup_github_secrets
        exit 0
        ;;
    "")
        main
        ;;
    *)
        log_error "Unknown argument: $1"
        echo "Use --help for usage information"
        exit 1
        ;;
esac