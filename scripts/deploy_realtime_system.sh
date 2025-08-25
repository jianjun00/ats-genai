#!/bin/bash

# Real-Time Market Data Collection System Deployment Script
# 
# Deploys the complete real-time data collection infrastructure including:
# - Database schema (Migration 042)
# - Real-time streaming collector
# - Daily validation CronJobs 
# - Gap detection and backfill services
# - Monitoring and alerting stack

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="ats-dev"
DEPLOYMENT_NAME="realtime-data-collection"
TIMEOUT="600s"

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

check_dependencies() {
    log_info "Checking deployment dependencies..."
    
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
    
    # Check namespace exists
    if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
        log_warning "Namespace $NAMESPACE does not exist, creating..."
        kubectl create namespace "$NAMESPACE"
    fi
    
    log_success "Dependencies check passed"
}

deploy_database_schema() {
    log_info "Deploying database schema (Migration 042)..."
    
    # Check if tables already exist
    if python scripts/run_dev.py query "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '%realtime%'" | grep -q "6"; then
        log_warning "Real-time tables already exist, skipping schema deployment"
        return 0
    fi
    
    # Apply database migration
    if python scripts/run_dev.py migrate realtime-data-collection; then
        log_success "Database schema deployed successfully"
    else
        log_error "Failed to deploy database schema"
        return 1
    fi
}

deploy_secrets() {
    log_info "Deploying API key secrets..."
    
    # Check if secrets exist
    if kubectl get secret vendor-api-keys -n "$NAMESPACE" &> /dev/null; then
        log_warning "Vendor API keys secret already exists"
    else
        log_warning "Creating placeholder vendor API keys secret"
        log_warning "Please update with actual API keys before starting collection"
    fi
    
    # Apply secrets from deployment manifest
    kubectl apply -f k8s/dev/realtime-streaming-deployment.yaml -n "$NAMESPACE"
    
    log_success "Secrets deployed successfully"
}

deploy_streaming_collector() {
    log_info "Deploying real-time streaming collector..."
    
    # Apply streaming collector deployment
    kubectl apply -f k8s/dev/realtime-streaming-deployment.yaml -n "$NAMESPACE"
    
    # Wait for deployment to be ready
    log_info "Waiting for streaming collector to be ready..."
    if kubectl wait --for=condition=available --timeout="$TIMEOUT" deployment/realtime-streaming-collector -n "$NAMESPACE"; then
        log_success "Streaming collector deployed successfully"
    else
        log_error "Streaming collector deployment failed or timed out"
        return 1
    fi
}

deploy_validation_services() {
    log_info "Deploying validation and backfill services..."
    
    # Apply CronJob manifests
    kubectl apply -f k8s/dev/realtime-validation-cronjobs.yaml -n "$NAMESPACE"
    
    # Verify CronJobs are created
    if kubectl get cronjobs -n "$NAMESPACE" | grep -E "(daily-realtime-validation|gap-detection-backfill|weekly-comprehensive-backfill)"; then
        log_success "Validation services deployed successfully"
    else
        log_error "Failed to deploy validation services"
        return 1
    fi
}

deploy_monitoring_stack() {
    log_info "Deploying monitoring and alerting stack..."
    
    # Apply monitoring configurations
    kubectl apply -f k8s/monitoring/prometheus-alerting-rules.yaml -n "$NAMESPACE"
    kubectl apply -f k8s/monitoring/monitoring-stack.yaml -n "$NAMESPACE"
    
    # Wait for monitoring services to be ready
    log_info "Waiting for monitoring services to be ready..."
    
    # Wait for Prometheus
    if kubectl wait --for=condition=available --timeout="$TIMEOUT" deployment/prometheus -n "$NAMESPACE"; then
        log_success "Prometheus deployed successfully"
    else
        log_warning "Prometheus deployment may have issues"
    fi
    
    # Wait for Grafana
    if kubectl wait --for=condition=available --timeout="$TIMEOUT" deployment/grafana -n "$NAMESPACE"; then
        log_success "Grafana deployed successfully"
    else
        log_warning "Grafana deployment may have issues"
    fi
    
    # Wait for AlertManager
    if kubectl wait --for=condition=available --timeout="$TIMEOUT" deployment/alertmanager -n "$NAMESPACE"; then
        log_success "AlertManager deployed successfully"
    else
        log_warning "AlertManager deployment may have issues"
    fi
}

configure_grafana_dashboard() {
    log_info "Configuring Grafana dashboard..."
    
    # Wait for Grafana to be fully ready
    sleep 30
    
    # Get Grafana pod
    GRAFANA_POD=$(kubectl get pods -n "$NAMESPACE" -l app=grafana -o jsonpath='{.items[0].metadata.name}')
    
    if [ -n "$GRAFANA_POD" ]; then
        # Copy dashboard to Grafana pod
        kubectl cp k8s/monitoring/grafana-realtime-dashboard.json "$NAMESPACE/$GRAFANA_POD:/var/lib/grafana/dashboards/"
        
        # Restart Grafana to pick up the dashboard
        kubectl delete pod "$GRAFANA_POD" -n "$NAMESPACE"
        
        log_success "Grafana dashboard configured"
    else
        log_warning "Could not find Grafana pod, dashboard not configured"
    fi
}

verify_deployment() {
    log_info "Verifying deployment..."
    
    # Check all deployments
    echo ""
    log_info "Deployment Status:"
    kubectl get deployments -n "$NAMESPACE" -o wide
    
    echo ""
    log_info "Pod Status:"
    kubectl get pods -n "$NAMESPACE" -o wide
    
    echo ""
    log_info "Service Status:"
    kubectl get services -n "$NAMESPACE" -o wide
    
    echo ""
    log_info "CronJob Status:"
    kubectl get cronjobs -n "$NAMESPACE" -o wide
    
    # Check if streaming collector is healthy
    COLLECTOR_PODS=$(kubectl get pods -n "$NAMESPACE" -l app=realtime-streaming-collector --field-selector=status.phase=Running -o name | wc -l)
    
    if [ "$COLLECTOR_PODS" -gt 0 ]; then
        log_success "Real-time streaming collector is running"
    else
        log_error "Real-time streaming collector is not running"
        return 1
    fi
}

get_access_info() {
    log_info "Getting access information..."
    
    # Get node IP
    NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}')
    if [ -z "$NODE_IP" ]; then
        NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
    fi
    
    echo ""
    log_success "=== ACCESS INFORMATION ==="
    echo ""
    echo "📊 Grafana Dashboard:"
    echo "   URL: http://$NODE_IP:30300"
    echo "   Username: admin"
    echo "   Password: admin123"
    echo ""
    echo "📈 Prometheus:"
    echo "   URL: http://$NODE_IP:30090"
    echo ""
    echo "🚨 AlertManager:"
    echo "   URL: http://$NODE_IP:30093"
    echo ""
    echo "📊 Real-time Metrics:"
    echo "   Metrics endpoint: http://$NODE_IP:9090/metrics"
    echo ""
    
    log_info "=== NEXT STEPS ==="
    echo ""
    echo "1. Update vendor API keys in the secret:"
    echo "   kubectl edit secret vendor-api-keys -n $NAMESPACE"
    echo ""
    echo "2. Configure Slack webhook in AlertManager:"
    echo "   kubectl edit configmap alertmanager-config -n $NAMESPACE"
    echo ""
    echo "3. Import Grafana dashboard:"
    echo "   - Navigate to Grafana UI"
    echo "   - Import dashboard from k8s/monitoring/grafana-realtime-dashboard.json"
    echo ""
    echo "4. Monitor deployment:"
    echo "   kubectl logs -f deployment/realtime-streaming-collector -n $NAMESPACE"
    echo ""
}

show_troubleshooting() {
    echo ""
    log_info "=== TROUBLESHOOTING ==="
    echo ""
    echo "If you encounter issues:"
    echo ""
    echo "1. Check pod logs:"
    echo "   kubectl logs -l app=realtime-streaming-collector -n $NAMESPACE"
    echo ""
    echo "2. Check database connectivity:"
    echo "   python scripts/run_dev.py query \"SELECT 1\""
    echo ""
    echo "3. Verify real-time tables exist:"
    echo "   python scripts/run_dev.py query \"SELECT tablename FROM pg_tables WHERE tablename LIKE '%realtime%'\""
    echo ""
    echo "4. Check resource usage:"
    echo "   kubectl top pods -n $NAMESPACE"
    echo ""
    echo "5. View recent events:"
    echo "   kubectl get events -n $NAMESPACE --sort-by='.lastTimestamp'"
    echo ""
}

# Main deployment flow
main() {
    echo ""
    log_info "========================================"
    log_info "Real-Time Data Collection System Deploy"
    log_info "========================================"
    echo ""
    
    # Pre-deployment checks
    check_dependencies
    
    # Core system deployment
    deploy_database_schema
    deploy_secrets
    deploy_streaming_collector
    deploy_validation_services
    
    # Monitoring and alerting
    deploy_monitoring_stack
    configure_grafana_dashboard
    
    # Post-deployment verification
    verify_deployment
    
    # Provide access information
    get_access_info
    show_troubleshooting
    
    echo ""
    log_success "Real-time data collection system deployed successfully!"
    log_info "Monitor the system using the provided URLs and commands above."
    echo ""
}

# Handle script arguments
case "${1:-}" in
    --help|-h)
        echo "Usage: $0 [--help|--verify-only|--monitoring-only]"
        echo ""
        echo "Options:"
        echo "  --help, -h           Show this help message"
        echo "  --verify-only        Only verify existing deployment"
        echo "  --monitoring-only    Deploy only monitoring stack"
        echo ""
        exit 0
        ;;
    --verify-only)
        check_dependencies
        verify_deployment
        get_access_info
        exit 0
        ;;
    --monitoring-only)
        check_dependencies
        deploy_monitoring_stack
        configure_grafana_dashboard
        get_access_info
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