#!/bin/bash
set -euo pipefail

# ATS Monitoring and Alerting Setup Script
echo "🔍 Setting up ATS Monitoring and Alerting with Slack Integration"

# Configuration
NAMESPACE="monitoring"
SLACK_WEBHOOK_URL="${SLACK_WEBHOOK_URL:-}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl is not installed"
        exit 1
    fi
    
    # Check if monitoring namespace exists
    if ! kubectl get namespace $NAMESPACE &> /dev/null; then
        print_error "Monitoring namespace '$NAMESPACE' not found"
        print_status "Please install Prometheus/Grafana stack first"
        exit 1
    fi
    
    # Check if Prometheus is running
    if ! kubectl get pods -n $NAMESPACE | grep prometheus &> /dev/null; then
        print_error "Prometheus not found in $NAMESPACE namespace"
        exit 1
    fi
    
    print_success "Prerequisites check passed"
}

# Setup Slack webhook secret
setup_slack_webhook() {
    if [[ -z "$SLACK_WEBHOOK_URL" ]]; then
        print_warning "SLACK_WEBHOOK_URL not provided"
        print_status "Please set SLACK_WEBHOOK_URL environment variable or update the secret manually later"
        print_status "Example: kubectl create secret generic slack-webhook -n monitoring --from-literal=url=https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
        return
    fi
    
    print_status "Setting up Slack webhook secret..."
    
    # Delete existing secret if it exists
    kubectl delete secret slack-webhook -n $NAMESPACE --ignore-not-found=true
    
    # Create new secret
    kubectl create secret generic slack-webhook -n $NAMESPACE \
        --from-literal=url="$SLACK_WEBHOOK_URL"
    
    print_success "Slack webhook secret created"
}

# Deploy alert rules
deploy_alert_rules() {
    print_status "Deploying ATS Prometheus alert rules..."
    
    # Apply the alert rules
    kubectl apply -f k8s/monitoring/ats-alert-rules.yaml
    
    # Wait for rules to be loaded
    sleep 5
    
    # Check if rules are loaded
    if kubectl get prometheusrules -n $NAMESPACE ats-service-alerts &> /dev/null; then
        print_success "Alert rules deployed successfully"
    else
        print_error "Failed to deploy alert rules"
        exit 1
    fi
}

# Update AlertManager configuration
update_alertmanager_config() {
    print_status "Updating AlertManager configuration for Slack notifications..."
    
    # Check if custom config exists
    if [[ -f "k8s/monitoring/slack-integration.yaml" ]]; then
        # Apply the Slack integration configuration
        kubectl apply -f k8s/monitoring/slack-integration.yaml
        
        # Restart AlertManager to pick up new config
        kubectl rollout restart deployment/alertmanager -n $NAMESPACE
        
        # Wait for rollout to complete
        kubectl rollout status deployment/alertmanager -n $NAMESPACE --timeout=60s
        
        print_success "AlertManager configuration updated"
    else
        print_warning "Slack integration config file not found"
        print_status "Using default AlertManager configuration"
    fi
}

# Verify monitoring setup
verify_monitoring_setup() {
    print_status "Verifying monitoring setup..."
    
    # Check Prometheus is running
    if kubectl get pods -n $NAMESPACE | grep prometheus | grep -q Running; then
        print_success "✅ Prometheus is running"
    else
        print_error "❌ Prometheus is not running properly"
        return 1
    fi
    
    # Check AlertManager is running
    if kubectl get pods -n $NAMESPACE | grep alertmanager | grep -q Running; then
        print_success "✅ AlertManager is running"
    else
        print_error "❌ AlertManager is not running properly"
        return 1
    fi
    
    # Check Grafana is running
    if kubectl get pods -n $NAMESPACE | grep grafana | grep -q Running; then
        print_success "✅ Grafana is running"
    else
        print_warning "⚠️ Grafana might not be running properly"
    fi
    
    # Check if alert rules are loaded
    if kubectl get prometheusrules -n $NAMESPACE ats-service-alerts &> /dev/null; then
        print_success "✅ ATS alert rules are loaded"
    else
        print_error "❌ ATS alert rules are not loaded"
        return 1
    fi
    
    print_success "Monitoring setup verification completed"
}

# Test alert functionality
test_alerts() {
    print_status "Testing alert functionality..."
    
    # Get AlertManager pod
    ALERTMANAGER_POD=$(kubectl get pods -n $NAMESPACE -l app.kubernetes.io/name=alertmanager -o jsonpath='{.items[0].metadata.name}')
    
    if [[ -n "$ALERTMANAGER_POD" ]]; then
        print_status "AlertManager pod: $ALERTMANAGER_POD"
        
        # Check AlertManager configuration
        kubectl exec -n $NAMESPACE $ALERTMANAGER_POD -- amtool config show | head -20
        
        print_success "AlertManager configuration check completed"
    else
        print_warning "AlertManager pod not found for testing"
    fi
}

# Display access information
display_access_info() {
    print_status "📊 Access Information:"
    echo ""
    
    # Get service information
    echo "🔍 Monitoring Services:"
    kubectl get services -n $NAMESPACE
    echo ""
    
    # Port forward instructions
    echo "🌐 To access monitoring tools locally:"
    echo "  Prometheus:   kubectl port-forward -n $NAMESPACE svc/prometheus 9090:9090"
    echo "  AlertManager: kubectl port-forward -n $NAMESPACE svc/alertmanager 9093:9093"
    echo "  Grafana:      kubectl port-forward -n $NAMESPACE svc/grafana 3000:3000"
    echo ""
    
    # Alert channels setup
    echo "📢 Slack Channels Setup:"
    echo "  Create these Slack channels for optimal alert routing:"
    echo "  • #ats-critical    - Critical alerts requiring immediate attention"
    echo "  • #ats-warnings    - Warning alerts for monitoring"
    echo "  • #ats-services    - Service-specific alerts"
    echo "  • #ats-deployments - Deployment and CI/CD notifications"
    echo ""
}

# Main execution
main() {
    print_status "Starting ATS Monitoring and Alerting Setup..."
    echo ""
    
    check_prerequisites
    setup_slack_webhook
    deploy_alert_rules
    update_alertmanager_config
    verify_monitoring_setup
    test_alerts
    display_access_info
    
    echo ""
    print_success "🎉 ATS Monitoring and Alerting setup completed successfully!"
    echo ""
    print_status "Next steps:"
    echo "  1. Configure your Slack webhook URL if not already done"
    echo "  2. Create the recommended Slack channels"
    echo "  3. Test alerts by triggering a deployment"
    echo "  4. Access Grafana to view dashboards"
    echo ""
}

# Run main function
main "$@"