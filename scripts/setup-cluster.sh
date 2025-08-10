#!/bin/bash

# ATS GenAI Cluster Setup Script
# This script helps set up Kubernetes clusters for different environments

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
ENVIRONMENTS=("dev" "intg" "prod")
NAMESPACES=("ats-dev" "ats-intg" "ats-prod")

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    print_status "Checking prerequisites..."
    
    local missing_tools=()
    
    if ! command -v kubectl &> /dev/null; then
        missing_tools+=("kubectl")
    fi
    
    if ! command -v docker &> /dev/null; then
        missing_tools+=("docker")
    fi
    
    if ! command -v uv &> /dev/null; then
        missing_tools+=("uv")
    fi
    
    if [ ${#missing_tools[@]} -ne 0 ]; then
        print_error "Missing required tools: ${missing_tools[*]}"
        echo "Please install the missing tools and run this script again."
        exit 1
    fi
    
    print_status "All prerequisites satisfied ✓"
}

create_namespaces() {
    print_status "Creating Kubernetes namespaces..."
    
    for namespace in "${NAMESPACES[@]}"; do
        if kubectl get namespace "$namespace" &> /dev/null; then
            print_warning "Namespace $namespace already exists"
        else
            kubectl create namespace "$namespace"
            print_status "Created namespace: $namespace"
        fi
        
        # Label the namespace
        kubectl label namespace "$namespace" environment="${namespace#ats-}" --overwrite
    done
}

setup_secrets() {
    print_status "Setting up secrets..."
    
    # Check if secrets already exist
    for namespace in "${NAMESPACES[@]}"; do
        if kubectl get secret ats-secrets -n "$namespace" &> /dev/null; then
            print_warning "Secrets already exist in $namespace"
            continue
        fi
        
        print_status "Creating secrets for $namespace..."
        
        # Prompt for secret values
        echo "Please provide the following secrets for $namespace:"
        
        read -p "Database URL: " -s db_url
        echo
        read -p "Tiingo API Key: " -s tiingo_key
        echo
        read -p "Polygon API Key: " -s polygon_key
        echo
        
        # Create secret
        kubectl create secret generic ats-secrets \
            --from-literal=database-url="$db_url" \
            --from-literal=tiingo-api-key="$tiingo_key" \
            --from-literal=polygon-api-key="$polygon_key" \
            -n "$namespace"
        
        print_status "Secrets created for $namespace ✓"
    done
}

deploy_environment() {
    local env=$1
    print_status "Deploying $env environment..."
    
    if [ ! -d "k8s/environments/$env" ]; then
        print_error "Environment directory k8s/environments/$env not found"
        return 1
    fi
    
    # Apply the kustomization
    kubectl apply -k "k8s/environments/$env"
    
    # Wait for deployment to be ready
    kubectl wait --for=condition=available --timeout=300s deployment/ats-api -n "ats-$env"
    
    print_status "$env environment deployed successfully ✓"
}

install_argocd() {
    print_status "Installing Argo CD..."
    
    # Create argocd namespace
    if ! kubectl get namespace argocd &> /dev/null; then
        kubectl create namespace argocd
    fi
    
    # Install Argo CD
    kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
    
    # Wait for Argo CD to be ready
    print_status "Waiting for Argo CD to be ready..."
    kubectl wait --for=condition=available --timeout=600s deployment/argocd-server -n argocd
    
    # Get initial admin password
    local admin_password
    admin_password=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d)
    
    print_status "Argo CD installed successfully ✓"
    print_status "Admin password: $admin_password"
    print_status "Access UI: kubectl port-forward svc/argocd-server -n argocd 8080:443"
}

setup_argocd_apps() {
    print_status "Setting up Argo CD applications..."
    
    # Apply Argo CD applications
    kubectl apply -f k8s/argocd/
    
    print_status "Argo CD applications configured ✓"
}

build_and_push_image() {
    print_status "Building and pushing Docker image..."
    
    # Build image
    docker build -t ghcr.io/jianjun00/ats-genai:latest .
    
    # Push image (requires authentication)
    if docker push ghcr.io/jianjun00/ats-genai:latest; then
        print_status "Image pushed successfully ✓"
    else
        print_warning "Failed to push image. Make sure you're authenticated to GitHub Container Registry"
        print_status "Run: echo \$GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin"
    fi
}

run_health_checks() {
    print_status "Running health checks..."
    
    for namespace in "${NAMESPACES[@]}"; do
        print_status "Checking $namespace..."
        
        # Check if pods are running
        if kubectl get pods -n "$namespace" | grep -q "Running"; then
            print_status "$namespace pods are running ✓"
        else
            print_warning "$namespace pods are not running"
            kubectl get pods -n "$namespace"
        fi
        
        # Check service endpoints
        if kubectl get endpoints -n "$namespace" | grep -q "ats-api-service"; then
            print_status "$namespace service endpoints are ready ✓"
        else
            print_warning "$namespace service endpoints not ready"
        fi
    done
}

show_access_info() {
    print_status "Access Information:"
    echo
    echo "Development Environment:"
    echo "  kubectl port-forward service/ats-api-service 8080:80 -n ats-dev"
    echo "  curl http://localhost:8080/health"
    echo
    echo "Integration Environment:"
    echo "  kubectl port-forward service/ats-api-service 8081:80 -n ats-intg"
    echo "  curl http://localhost:8081/health"
    echo
    echo "Production Environment:"
    echo "  kubectl port-forward service/ats-api-service 8082:80 -n ats-prod"
    echo "  curl http://localhost:8082/health"
    echo
    echo "Argo CD UI:"
    echo "  kubectl port-forward svc/argocd-server -n argocd 8080:443"
    echo "  https://localhost:8080"
}

main() {
    echo "=== ATS GenAI Cluster Setup ==="
    echo
    
    case "${1:-all}" in
        "prereq")
            check_prerequisites
            ;;
        "namespaces")
            create_namespaces
            ;;
        "secrets")
            setup_secrets
            ;;
        "build")
            build_and_push_image
            ;;
        "deploy")
            if [ -z "$2" ]; then
                print_error "Please specify environment: dev, intg, or prod"
                exit 1
            fi
            deploy_environment "$2"
            ;;
        "argocd")
            install_argocd
            setup_argocd_apps
            ;;
        "health")
            run_health_checks
            ;;
        "info")
            show_access_info
            ;;
        "all")
            check_prerequisites
            create_namespaces
            setup_secrets
            build_and_push_image
            for env in "${ENVIRONMENTS[@]}"; do
                deploy_environment "$env"
            done
            install_argocd
            setup_argocd_apps
            run_health_checks
            show_access_info
            ;;
        *)
            echo "Usage: $0 [prereq|namespaces|secrets|build|deploy <env>|argocd|health|info|all]"
            echo
            echo "Commands:"
            echo "  prereq     - Check prerequisites"
            echo "  namespaces - Create Kubernetes namespaces"
            echo "  secrets    - Set up secrets"
            echo "  build      - Build and push Docker image"
            echo "  deploy <env> - Deploy specific environment (dev/intg/prod)"
            echo "  argocd     - Install and configure Argo CD"
            echo "  health     - Run health checks"
            echo "  info       - Show access information"
            echo "  all        - Run complete setup"
            exit 1
            ;;
    esac
}

main "$@"
