#!/bin/bash
# Deploy ATS Monitoring Infrastructure
# Usage: ./deploy_monitoring.sh [dev|intg|both]

set -e

ENVIRONMENT=${1:-both}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="${SCRIPT_DIR}/../k8s/monitoring"

echo "🚀 Deploying ATS Monitoring Infrastructure"
echo "Environment: ${ENVIRONMENT}"
echo "================================================"

# Function to deploy to a specific environment
deploy_environment() {
    local env=$1
    local namespace="ats-${env}"
    
    echo ""
    echo "📦 Deploying to ${namespace} namespace..."
    
    # Create namespace
    echo "Creating namespace: ${namespace}"
    kubectl apply -f "${K8S_DIR}/namespaces.yaml"
    
    # Wait for namespace to be ready
    kubectl wait --for=condition=Ready namespace/${namespace} --timeout=30s
    
    # Deploy Prometheus
    echo "Deploying Prometheus..."
    kubectl apply -f "${K8S_DIR}/prometheus-config.yaml"
    kubectl apply -f "${K8S_DIR}/prometheus-deployment.yaml"
    
    # Deploy AlertManager
    echo "Deploying AlertManager..."
    kubectl apply -f "${K8S_DIR}/alertmanager-config.yaml"
    kubectl apply -f "${K8S_DIR}/alertmanager-deployment.yaml"
    
    # Deploy Grafana
    echo "Deploying Grafana..."
    kubectl apply -f "${K8S_DIR}/grafana-config.yaml"
    kubectl apply -f "${K8S_DIR}/grafana-dashboards.yaml"
    kubectl apply -f "${K8S_DIR}/grafana-deployment.yaml"
    
    # Deploy Data Quality Exporter
    echo "Deploying Data Quality Exporter..."
    kubectl apply -f "${K8S_DIR}/data-quality-exporter.yaml"
    
    # Wait for deployments to be ready
    echo "Waiting for deployments to be ready..."
    kubectl wait --for=condition=available --timeout=300s deployment/prometheus -n ${namespace}
    kubectl wait --for=condition=available --timeout=300s deployment/alertmanager -n ${namespace}
    kubectl wait --for=condition=available --timeout=300s deployment/grafana -n ${namespace}
    kubectl wait --for=condition=available --timeout=300s deployment/ats-data-quality-exporter -n ${namespace}
    
    echo "✅ ${env} environment deployed successfully!"
}

# Function to show access information
show_access_info() {
    local env=$1
    local namespace="ats-${env}"
    
    echo ""
    echo "🔗 Access Information for ${env}:"
    echo "================================="
    
    # Get node IP
    NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}')
    if [ -z "$NODE_IP" ]; then
        NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
    fi
    
    if [ "$env" = "dev" ]; then
        echo "Grafana (${env}):     http://${NODE_IP}:30300 (admin/ats-dev-password)"
        echo "Prometheus (${env}):  http://${NODE_IP}:30090"
        echo "AlertManager (${env}): http://${NODE_IP}:30093"
    else
        echo "Grafana (${env}):     http://${NODE_IP}:30301 (admin/ats-intg-password)"
        echo "Prometheus (${env}):  http://${NODE_IP}:30091"
        echo "AlertManager (${env}): http://${NODE_IP}:30094"
    fi
    
    echo ""
    echo "Data Quality Metrics: http://${NODE_IP}:<service-port>/metrics"
    echo "  - Access via port-forward: kubectl port-forward -n ${namespace} service/ats-data-quality-exporter 8080:8080"
}

# Deploy based on environment parameter
case $ENVIRONMENT in
    dev)
        deploy_environment "dev"
        show_access_info "dev"
        ;;
    intg)
        deploy_environment "intg"
        show_access_info "intg"
        ;;
    both)
        deploy_environment "dev"
        deploy_environment "intg"
        show_access_info "dev"
        show_access_info "intg"
        ;;
    *)
        echo "❌ Invalid environment: ${ENVIRONMENT}"
        echo "Usage: $0 [dev|intg|both]"
        exit 1
        ;;
esac

echo ""
echo "🎉 Monitoring deployment completed!"
echo ""
echo "📋 Next Steps:"
echo "1. Configure Slack webhook URLs in AlertManager config"
echo "2. Create Slack channels: #ats-dev-alerts, #ats-intg-alerts, #ats-data-quality"
echo "3. Test alerting by simulating metric thresholds"
echo "4. Import additional Grafana dashboards if needed"
echo ""
echo "🔍 Verify deployment:"
echo "kubectl get pods -n ats-dev"
echo "kubectl get pods -n ats-intg"
echo "kubectl get services -n ats-dev"
echo "kubectl get services -n ats-intg"