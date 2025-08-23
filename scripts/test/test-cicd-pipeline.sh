#!/bin/bash
set -euo pipefail

# ATS CI/CD Pipeline Testing Script
echo "🚀 Testing ATS CI/CD Pipeline End-to-End"

# Configuration
NAMESPACE="ats-dev"
TEST_TIMEOUT=600  # 10 minutes

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

# Test GitHub Actions workflow
test_github_actions() {
    print_status "Testing GitHub Actions workflow trigger..."
    
    # Check if we're in a git repository
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        print_error "Not in a git repository"
        return 1
    fi
    
    # Get current branch
    CURRENT_BRANCH=$(git branch --show-current)
    print_status "Current branch: $CURRENT_BRANCH"
    
    # Check for workflow files
    if [[ ! -f ".github/workflows/ats-ci-cd.yaml" ]]; then
        print_error "Main CI/CD workflow file not found"
        return 1
    fi
    
    if [[ ! -f ".github/workflows/test-ci-trigger.yaml" ]]; then
        print_error "Test trigger workflow file not found"
        return 1
    fi
    
    print_success "GitHub Actions workflow files found"
    
    # Show recent workflow runs (if gh CLI is available)
    if command -v gh &> /dev/null; then
        print_status "Recent GitHub Actions runs:"
        gh run list --limit 5 || print_warning "Could not fetch workflow runs"
    else
        print_warning "GitHub CLI not installed - cannot check workflow run status"
    fi
}

# Test Argo CD deployment
test_argocd_deployment() {
    print_status "Testing Argo CD deployment..."
    
    # Check if Argo CD is running
    if ! kubectl get pods -n argocd | grep -q Running; then
        print_error "Argo CD is not running properly"
        return 1
    fi
    
    print_success "✅ Argo CD is running"
    
    # Check applications
    print_status "Checking Argo CD applications..."
    kubectl get applications -n argocd || print_warning "No applications found"
    
    # Check application sync status
    if kubectl get application ats-dev -n argocd &> /dev/null; then
        SYNC_STATUS=$(kubectl get application ats-dev -n argocd -o jsonpath='{.status.sync.status}')
        HEALTH_STATUS=$(kubectl get application ats-dev -n argocd -o jsonpath='{.status.health.status}')
        
        print_status "ATS Application Status:"
        echo "  Sync Status: $SYNC_STATUS"
        echo "  Health Status: $HEALTH_STATUS"
        
        if [[ "$HEALTH_STATUS" == "Healthy" ]]; then
            print_success "✅ ATS application is healthy"
        else
            print_warning "⚠️ ATS application health: $HEALTH_STATUS"
        fi
    else
        print_warning "ATS application not found in Argo CD"
    fi
}

# Test service deployment
test_service_deployment() {
    print_status "Testing ATS service deployment..."
    
    # Check if namespace exists
    if ! kubectl get namespace $NAMESPACE &> /dev/null; then
        print_error "Namespace $NAMESPACE not found"
        return 1
    fi
    
    # Check pods
    print_status "Checking ATS service pods..."
    kubectl get pods -n $NAMESPACE
    
    # Check services
    print_status "Checking ATS services..."
    kubectl get services -n $NAMESPACE
    
    # Test service health
    local services=("ats-minute-service" "ats-eod-service" "ats-analytics-service")
    local ports=(8081 8082 8080)
    
    for i in "${!services[@]}"; do
        local service="${services[$i]}"
        local port="${ports[$i]}"
        
        print_status "Testing health endpoint for $service..."
        
        # Port forward and test
        kubectl port-forward -n $NAMESPACE service/$service $port:$port &
        local pf_pid=$!
        
        # Wait for port forward to establish
        sleep 3
        
        # Test health endpoint
        if curl -f -s "http://localhost:$port/health" > /dev/null; then
            print_success "✅ $service health check passed"
        else
            print_error "❌ $service health check failed"
        fi
        
        # Clean up port forward
        kill $pf_pid 2>/dev/null || true
        
        # Wait a moment between tests
        sleep 2
    done
}

# Test monitoring and alerting
test_monitoring_alerting() {
    print_status "Testing monitoring and alerting setup..."
    
    # Check monitoring namespace
    if ! kubectl get namespace monitoring &> /dev/null; then
        print_warning "Monitoring namespace not found - monitoring not set up"
        return
    fi
    
    # Check Prometheus
    if kubectl get pods -n monitoring | grep prometheus | grep -q Running; then
        print_success "✅ Prometheus is running"
    else
        print_error "❌ Prometheus is not running"
    fi
    
    # Check AlertManager
    if kubectl get pods -n monitoring | grep alertmanager | grep -q Running; then
        print_success "✅ AlertManager is running"
    else
        print_error "❌ AlertManager is not running"
    fi
    
    # Check Grafana
    if kubectl get pods -n monitoring | grep grafana | grep -q Running; then
        print_success "✅ Grafana is running"
    else
        print_warning "⚠️ Grafana may not be running"
    fi
    
    # Check alert rules
    if kubectl get prometheusrules -n monitoring ats-service-alerts &> /dev/null; then
        print_success "✅ ATS alert rules are configured"
    else
        print_warning "⚠️ ATS alert rules not found"
    fi
    
    # Check Slack webhook secret
    if kubectl get secret slack-webhook -n monitoring &> /dev/null; then
        print_success "✅ Slack webhook secret configured"
    else
        print_warning "⚠️ Slack webhook secret not configured"
    fi
}

# Test end-to-end flow
test_end_to_end_flow() {
    print_status "Testing end-to-end CI/CD flow..."
    
    # Create a test commit to trigger CI/CD
    local test_file="test-pipeline-$(date +%s).txt"
    echo "Test pipeline trigger at $(date)" > "$test_file"
    
    print_status "Creating test commit to trigger pipeline..."
    git add "$test_file"
    git commit -m "test: trigger CI/CD pipeline validation"
    
    print_status "Test commit created. Pipeline should trigger automatically."
    print_status "Monitor the pipeline at: https://github.com/$(git remote get-url origin | sed 's|.git||' | sed 's|.*github.com/||' | sed 's|.*:|')/actions"
    
    # Clean up test file
    git rm "$test_file"
    git commit -m "cleanup: remove pipeline test file"
    
    print_success "End-to-end flow test initiated"
}

# Generate test report
generate_test_report() {
    local report_file="cicd-test-report-$(date +%Y%m%d-%H%M%S).md"
    
    print_status "Generating test report: $report_file"
    
    cat > "$report_file" << EOF
# ATS CI/CD Pipeline Test Report

**Generated:** $(date)
**Test Environment:** $NAMESPACE
**Git Branch:** $(git branch --show-current)
**Git Commit:** $(git rev-parse HEAD)

## Test Results Summary

### GitHub Actions
- Workflow files: $(test -f .github/workflows/ats-ci-cd.yaml && echo "✅ Present" || echo "❌ Missing")
- Test trigger: $(test -f .github/workflows/test-ci-trigger.yaml && echo "✅ Present" || echo "❌ Missing")

### Argo CD
- Service Status: $(kubectl get pods -n argocd | grep -q Running && echo "✅ Running" || echo "❌ Not Running")
- Applications: $(kubectl get applications -n argocd --no-headers 2>/dev/null | wc -l) configured

### Service Deployment
- Namespace: $(kubectl get namespace $NAMESPACE &> /dev/null && echo "✅ Exists" || echo "❌ Missing")
- Services Running: $(kubectl get pods -n $NAMESPACE --no-headers 2>/dev/null | grep Running | wc -l)

### Monitoring & Alerting
- Monitoring Namespace: $(kubectl get namespace monitoring &> /dev/null && echo "✅ Exists" || echo "❌ Missing")
- Prometheus: $(kubectl get pods -n monitoring 2>/dev/null | grep prometheus | grep -q Running && echo "✅ Running" || echo "❌ Not Running")
- AlertManager: $(kubectl get pods -n monitoring 2>/dev/null | grep alertmanager | grep -q Running && echo "✅ Running" || echo "❌ Not Running")
- Alert Rules: $(kubectl get prometheusrules -n monitoring ats-service-alerts &> /dev/null && echo "✅ Configured" || echo "❌ Missing")
- Slack Integration: $(kubectl get secret slack-webhook -n monitoring &> /dev/null && echo "✅ Configured" || echo "❌ Missing")

## Detailed Service Status

\`\`\`
$(kubectl get pods -n $NAMESPACE 2>/dev/null || echo "Namespace not found")
\`\`\`

## Recommendations

1. **If GitHub Actions are not running:**
   - Verify GitHub repository has Actions enabled
   - Check if SLACK_WEBHOOK_URL secret is configured
   - Review workflow file syntax

2. **If services are not healthy:**
   - Check pod logs: \`kubectl logs -n $NAMESPACE deployment/ats-{service}-service\`
   - Verify database connectivity
   - Check resource limits

3. **If monitoring is not working:**
   - Run: \`./scripts/deploy/setup-monitoring-alerts.sh\`
   - Configure Slack webhook URL
   - Verify Prometheus targets

4. **If alerts are not firing:**
   - Check AlertManager configuration
   - Test Slack webhook connectivity
   - Review alert rule expressions

## Next Steps

1. Set up Slack webhook for notifications
2. Configure monitoring dashboards
3. Test alert functionality
4. Document operational procedures

EOF

    print_success "Test report generated: $report_file"
    echo "📄 Review the full report for detailed analysis"
}

# Display final status
display_final_status() {
    echo ""
    print_status "🎯 CI/CD Pipeline Test Summary"
    echo "=========================================="
    
    # Component status
    local components=("GitHub Actions" "Argo CD" "Service Deployment" "Monitoring")
    local status=()
    
    # Check each component
    if [[ -f ".github/workflows/ats-ci-cd.yaml" ]]; then
        status+=("✅")
    else
        status+=("❌")
    fi
    
    if kubectl get pods -n argocd | grep -q Running; then
        status+=("✅")
    else
        status+=("❌")
    fi
    
    if kubectl get pods -n $NAMESPACE | grep -q Running; then
        status+=("✅")
    else
        status+=("❌")
    fi
    
    if kubectl get namespace monitoring &> /dev/null; then
        status+=("✅")
    else
        status+=("⚠️")
    fi
    
    # Display results
    for i in "${!components[@]}"; do
        echo "  ${status[$i]} ${components[$i]}"
    done
    
    echo ""
    print_status "🔗 Quick Links:"
    echo "  • GitHub Actions: https://github.com/$(git remote get-url origin | sed 's|.git||' | sed 's|.*github.com/||' | sed 's|.*:|')/actions"
    echo "  • Argo CD: kubectl port-forward -n argocd svc/argocd-server 8080:80"
    echo "  • Grafana: kubectl port-forward -n monitoring svc/grafana 3000:3000"
    echo ""
}

# Main execution
main() {
    print_status "Starting ATS CI/CD Pipeline Testing..."
    echo ""
    
    # Update TodoWrite status
    print_status "Updating task progress..."
    
    # Run tests
    test_github_actions
    echo ""
    
    test_argocd_deployment
    echo ""
    
    test_service_deployment
    echo ""
    
    test_monitoring_alerting
    echo ""
    
    # Generate report
    generate_test_report
    echo ""
    
    # Display final status
    display_final_status
    
    print_success "🎉 CI/CD Pipeline testing completed!"
    
    # Ask if user wants to trigger end-to-end test
    echo ""
    read -p "Would you like to trigger an end-to-end pipeline test? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        test_end_to_end_flow
    fi
}

# Run main function
main "$@"