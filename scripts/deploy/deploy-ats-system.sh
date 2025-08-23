#!/bin/bash
set -euo pipefail

# ATS System Deployment Script
# This script deploys the complete 3-service ATS system to ats-dev namespace

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="ats-dev"
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/config}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Logging functions
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

# Print banner
print_banner() {
    echo -e "${BLUE}"
    echo "=============================================="
    echo "   ATS 3-Service System Deployment Script"
    echo "=============================================="
    echo -e "${NC}"
    echo "Namespace: ${NAMESPACE}"
    echo "Kubeconfig: ${KUBECONFIG_PATH}"
    echo "Project Root: ${PROJECT_ROOT}"
    echo ""
}

# Check prerequisites
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
    
    # Check required files
    local required_files=(
        "${PROJECT_ROOT}/k8s/minute-service/deployment.yaml"
        "${PROJECT_ROOT}/k8s/eod-service/deployment.yaml"
        "${PROJECT_ROOT}/k8s/analytics-service/deployment.yaml"
        "${PROJECT_ROOT}/k8s/argocd/argo-applications.yaml"
    )
    
    for file in "${required_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            log_error "Required file not found: $file"
            exit 1
        fi
    done
    
    log_success "Prerequisites check passed"
}

# Create namespace if it doesn't exist
create_namespace() {
    log_info "Creating namespace ${NAMESPACE}..."
    
    if kubectl get namespace "${NAMESPACE}" &> /dev/null; then
        log_info "Namespace ${NAMESPACE} already exists"
    else
        kubectl create namespace "${NAMESPACE}"
        kubectl label namespace "${NAMESPACE}" managed-by=ats-deployment
        log_success "Namespace ${NAMESPACE} created"
    fi
}

# Create configuration and secrets
create_configurations() {
    log_info "Creating configurations and secrets..."
    
    # Create service account
    kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ats-service-account
  namespace: ${NAMESPACE}
  labels:
    app: ats-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ats-service-role
rules:
- apiGroups: [""]
  resources: ["pods", "services", "endpoints"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["apps"]
  resources: ["deployments"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ats-service-binding
subjects:
- kind: ServiceAccount
  name: ats-service-account
  namespace: ${NAMESPACE}
roleRef:
  kind: ClusterRole
  name: ats-service-role
  apiGroup: rbac.authorization.k8s.io
EOF
    
    # Create ConfigMaps
    kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: ats-config
  namespace: ${NAMESPACE}
  labels:
    app: ats-system
data:
  environment: "dev"
  log_level: "INFO"
  metrics_enabled: "true"
  tracing_enabled: "true"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: ats-gin-config
  namespace: ${NAMESPACE}
  labels:
    app: ats-system
data:
  app_dev.gin: |
    # ATS Development Configuration
    database.max_connections = 20
    database.connection_timeout = 30
    
    # Rate limiting
    polygon.rate_limit = 12.0  # seconds between requests
    tiingo.rate_limit = 0.2
    fmp.rate_limit = 1.0
    
    # Caching
    cache.ttl = 300
    cache.max_size = 1000
    
    # Analytics
    analytics.batch_size = 1000
    analytics.computation_timeout = 60
    
    # Data collection
    collection.symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA"]
    collection.retry_attempts = 3
    collection.backoff_factor = 2.0
EOF
    
    # Create secrets (placeholders - replace with actual values)
    log_warning "Creating placeholder secrets - UPDATE WITH ACTUAL VALUES"
    
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: ats-db-secret
  namespace: ${NAMESPACE}
  labels:
    app: ats-system
type: Opaque
stringData:
  host: "localhost"
  port: "5433"
  username: "postgres"
  password: "postgres"
  database: "dev_db"
---
apiVersion: v1
kind: Secret
metadata:
  name: ats-api-keys
  namespace: ${NAMESPACE}
  labels:
    app: ats-system
type: Opaque
stringData:
  polygon-api-key: "REPLACE_WITH_ACTUAL_POLYGON_KEY"
  tiingo-api-key: "REPLACE_WITH_ACTUAL_TIINGO_KEY"
  fmp-api-key: "REPLACE_WITH_ACTUAL_FMP_KEY"
EOF
    
    log_warning "🔑 Remember to update secrets with actual API keys:"
    log_warning "   kubectl patch secret ats-api-keys -n ${NAMESPACE} -p '{\"stringData\":{\"polygon-api-key\":\"YOUR_KEY\"}}'"
    log_warning "   kubectl patch secret ats-api-keys -n ${NAMESPACE} -p '{\"stringData\":{\"tiingo-api-key\":\"YOUR_KEY\"}}'"
    log_warning "   kubectl patch secret ats-api-keys -n ${NAMESPACE} -p '{\"stringData\":{\"fmp-api-key\":\"YOUR_KEY\"}}'"
    
    log_success "Configurations and secrets created"
}

# Deploy Redis for caching
deploy_redis() {
    log_info "Deploying Redis for caching..."
    
    kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ats-redis
  namespace: ${NAMESPACE}
  labels:
    app: ats-redis
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ats-redis
  template:
    metadata:
      labels:
        app: ats-redis
    spec:
      containers:
      - name: redis
        image: redis:7-alpine
        ports:
        - containerPort: 6379
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "256Mi"
            cpu: "200m"
        livenessProbe:
          exec:
            command:
            - redis-cli
            - ping
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          exec:
            command:
            - redis-cli
            - ping
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: ats-redis-service
  namespace: ${NAMESPACE}
  labels:
    app: ats-redis
spec:
  type: ClusterIP
  ports:
  - port: 6379
    targetPort: 6379
  selector:
    app: ats-redis
EOF
    
    # Wait for Redis to be ready
    log_info "Waiting for Redis to be ready..."
    kubectl wait --for=condition=ready pod -l app=ats-redis -n "${NAMESPACE}" --timeout=60s
    
    log_success "Redis deployed successfully"
}

# Deploy ATS services
deploy_services() {
    log_info "Deploying ATS services..."
    
    # Deploy services in order
    local services=("minute-service" "eod-service" "analytics-service")
    
    for service in "${services[@]}"; do
        log_info "Deploying ${service}..."
        
        # Update image tags to use local/development images
        sed "s|image: ats/${service}:latest|image: ats/${service}:dev|g" \
            "${PROJECT_ROOT}/k8s/${service}/deployment.yaml" | \
            kubectl apply -f -
        
        log_success "${service} deployment created"
    done
    
    log_info "Waiting for services to be ready..."
    
    # Wait for deployments to be ready
    for service in "${services[@]}"; do
        log_info "Waiting for ats-${service} to be ready..."
        kubectl wait --for=condition=available deployment/ats-${service} -n "${NAMESPACE}" --timeout=300s
        log_success "ats-${service} is ready"
    done
}

# Verify deployment
verify_deployment() {
    log_info "Verifying deployment..."
    
    # Check pod status
    log_info "Pod status:"
    kubectl get pods -n "${NAMESPACE}" -o wide
    
    # Check service status
    log_info "Service status:"
    kubectl get services -n "${NAMESPACE}"
    
    # Check service health endpoints
    local services=(
        "ats-minute-service:8081"
        "ats-eod-service:8082"
        "ats-analytics-service:8080"
    )
    
    log_info "Checking service health endpoints..."
    
    for service_port in "${services[@]}"; do
        local service_name=$(echo "$service_port" | cut -d: -f1)
        local port=$(echo "$service_port" | cut -d: -f2)
        
        log_info "Testing ${service_name}:${port}/health..."
        
        if kubectl run -i --rm --restart=Never test-${service_name}-health \
            --image=curlimages/curl:latest \
            --quiet \
            -- curl -f "http://${service_name}.${NAMESPACE}.svc.cluster.local:${port}/health" \
            > /dev/null 2>&1; then
            log_success "${service_name} health check passed"
        else
            log_error "${service_name} health check failed"
            return 1
        fi
    done
    
    log_success "All health checks passed"
}

# Deploy Argo CD applications (if Argo CD is available)
deploy_argocd_apps() {
    log_info "Checking for Argo CD installation..."
    
    if kubectl get namespace argocd &> /dev/null; then
        log_info "Argo CD found, deploying applications..."
        kubectl apply -f "${PROJECT_ROOT}/k8s/argocd/argo-applications.yaml"
        log_success "Argo CD applications deployed"
    else
        log_warning "Argo CD not found, skipping GitOps application deployment"
        log_info "To install Argo CD, run:"
        log_info "  kubectl create namespace argocd"
        log_info "  kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml"
    fi
}

# Show access information
show_access_info() {
    log_info "Getting service access information..."
    
    echo ""
    echo "=============================================="
    echo "   ATS System Access Information"
    echo "=============================================="
    
    # Get service URLs
    echo "Service Endpoints (within cluster):"
    echo "• Minute Service: http://ats-minute-service.${NAMESPACE}.svc.cluster.local:8081"
    echo "• EOD Service: http://ats-eod-service.${NAMESPACE}.svc.cluster.local:8082"
    echo "• Analytics Service: http://ats-analytics-service.${NAMESPACE}.svc.cluster.local:8080"
    echo ""
    
    # Port forwarding commands
    echo "Port Forwarding Commands (for external access):"
    echo "• kubectl port-forward -n ${NAMESPACE} svc/ats-minute-service 8081:8081"
    echo "• kubectl port-forward -n ${NAMESPACE} svc/ats-eod-service 8082:8082"
    echo "• kubectl port-forward -n ${NAMESPACE} svc/ats-analytics-service 8080:8080"
    echo ""
    
    # Dashboard URLs (after port forwarding)
    echo "Dashboard URLs (after port forwarding):"
    echo "• Analytics Dashboard: http://localhost:8080/dashboard"
    echo "• Minute Service Health: http://localhost:8081/health"
    echo "• EOD Service Health: http://localhost:8082/health"
    echo ""
    
    # Useful kubectl commands
    echo "Useful Commands:"
    echo "• View pods: kubectl get pods -n ${NAMESPACE}"
    echo "• View logs: kubectl logs -f -l app=ats-analytics-service -n ${NAMESPACE}"
    echo "• Shell into pod: kubectl exec -it deployment/ats-analytics-service -n ${NAMESPACE} -- /bin/bash"
    echo ""
}

# Cleanup function
cleanup_on_error() {
    log_error "Deployment failed, cleaning up..."
    kubectl delete namespace "${NAMESPACE}" --ignore-not-found=true
    exit 1
}

# Main deployment function
main() {
    print_banner
    
    # Trap errors and cleanup
    trap cleanup_on_error ERR
    
    check_prerequisites
    create_namespace
    create_configurations
    deploy_redis
    deploy_services
    verify_deployment
    deploy_argocd_apps
    show_access_info
    
    log_success "🎉 ATS System deployment completed successfully!"
    log_info "The system is now running in the ${NAMESPACE} namespace"
    
    echo ""
    echo "Next Steps:"
    echo "1. Update API keys in secrets (see warnings above)"
    echo "2. Set up port forwarding to access services"
    echo "3. Run system tests: ./scripts/test/test-ats-system.sh"
    echo "4. Monitor deployments with: kubectl get pods -n ${NAMESPACE} -w"
}

# Script options
case "${1:-deploy}" in
    "deploy")
        main
        ;;
    "cleanup")
        log_info "Cleaning up ATS system..."
        kubectl delete namespace "${NAMESPACE}" --ignore-not-found=true
        log_success "Cleanup completed"
        ;;
    "status")
        log_info "ATS system status in namespace ${NAMESPACE}:"
        kubectl get all -n "${NAMESPACE}"
        ;;
    "logs")
        service="${2:-analytics-service}"
        log_info "Showing logs for ats-${service}..."
        kubectl logs -f -l app=ats-${service} -n "${NAMESPACE}"
        ;;
    *)
        echo "Usage: $0 [deploy|cleanup|status|logs [service-name]]"
        echo "  deploy: Deploy the complete ATS system (default)"
        echo "  cleanup: Remove the ATS system and namespace"
        echo "  status: Show system status"
        echo "  logs: Show logs for a service (default: analytics-service)"
        exit 1
        ;;
esac