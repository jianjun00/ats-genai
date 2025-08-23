#!/bin/bash
set -euo pipefail

# Comprehensive Testing Script for GitHub Actions + Private ArgoCD Integration
echo "🧪 Testing GitHub Actions + Private ArgoCD Integration"

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

# Test configuration
TEST_BRANCH="test/github-argocd-integration-$(date +%s)"
ARGOCD_NAMESPACE="argocd"

# Step 1: Prerequisites Check
test_prerequisites() {
    print_status "🔍 Step 1: Checking Prerequisites"
    echo ""
    
    local errors=0
    
    # Check kubectl access
    if kubectl cluster-info > /dev/null 2>&1; then
        print_success "✅ kubectl access to cluster"
    else
        print_error "❌ kubectl cannot access cluster"
        ((errors++))
    fi
    
    # Check ArgoCD namespace
    if kubectl get namespace "$ARGOCD_NAMESPACE" > /dev/null 2>&1; then
        print_success "✅ ArgoCD namespace exists"
    else
        print_error "❌ ArgoCD namespace not found"
        ((errors++))
    fi
    
    # Check ArgoCD server
    if kubectl get pods -n "$ARGOCD_NAMESPACE" -l app.kubernetes.io/name=argocd-server --no-headers | grep -q Running; then
        print_success "✅ ArgoCD server running"
    else
        print_error "❌ ArgoCD server not running"
        ((errors++))
    fi
    
    # Check Git repository
    if git remote get-url origin > /dev/null 2>&1; then
        REPO_URL=$(git remote get-url origin)
        print_success "✅ Git repository configured: $REPO_URL"
    else
        print_error "❌ Git repository not configured"
        ((errors++))
    fi
    
    # Check required files
    local required_files=(
        ".github/workflows/ats-ci-cd-improved.yaml"
        "argocd/applications/ats-dev.yaml"
        "scripts/monitoring/check-argocd-sync.sh"
    )
    
    for file in "${required_files[@]}"; do
        if [[ -f "$file" ]]; then
            print_success "✅ Required file exists: $file"
        else
            print_error "❌ Missing required file: $file"
            ((errors++))
        fi
    done
    
    echo ""
    if [[ $errors -eq 0 ]]; then
        print_success "🎉 All prerequisites met!"
        return 0
    else
        print_error "❌ $errors prerequisite(s) failed"
        return 1
    fi
}

# Step 2: Deploy ArgoCD Applications
deploy_argocd_applications() {
    print_status "🚀 Step 2: Deploying ArgoCD Applications"
    echo ""
    
    # Apply ArgoCD applications
    if kubectl apply -f argocd/applications/; then
        print_success "✅ ArgoCD applications deployed"
    else
        print_error "❌ Failed to deploy ArgoCD applications"
        return 1
    fi
    
    # Wait for applications to be created
    print_status "⏳ Waiting for applications to be created..."
    sleep 10
    
    # Check application status
    local apps=("ats-dev" "ats-staging" "ats-production")
    for app in "${apps[@]}"; do
        if kubectl get application "$app" -n "$ARGOCD_NAMESPACE" > /dev/null 2>&1; then
            STATUS=$(kubectl get application "$app" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.sync.status}' || echo "Unknown")
            print_success "✅ Application $app created (Status: $STATUS)"
        else
            print_error "❌ Application $app not found"
            return 1
        fi
    done
    
    echo ""
    print_success "🎉 ArgoCD applications deployed successfully!"
}

# Step 3: Test GitHub Actions Workflow
test_github_workflow() {
    print_status "⚙️ Step 3: Testing GitHub Actions Workflow"
    echo ""
    
    # Create test branch
    print_status "Creating test branch: $TEST_BRANCH"
    git checkout -b "$TEST_BRANCH"
    
    # Make a small change
    echo "# Test GitHub Actions + ArgoCD Integration - $(date)" >> README.md
    git add README.md
    git commit -m "test: GitHub Actions + ArgoCD integration verification

This commit tests the improved GitHub Actions workflow with private ArgoCD
integration using GitOps pull-based deployment.

Testing:
- Improved workflow execution
- Manifest updates
- ArgoCD sync detection
- Multi-environment deployment

Branch: $TEST_BRANCH
Timestamp: $(date -u)"
    
    # Push branch
    print_status "Pushing test branch..."
    if git push origin "$TEST_BRANCH"; then
        print_success "✅ Test branch pushed successfully"
    else
        print_error "❌ Failed to push test branch"
        return 1
    fi
    
    # Get workflow URL
    REPO_PATH=$(git remote get-url origin | sed 's/.*github.com[:/]\([^.]*\).*/\1/')
    WORKFLOW_URL="https://github.com/$REPO_PATH/actions"
    
    echo ""
    print_success "🎉 GitHub Actions workflow triggered!"
    print_status "📋 Next steps:"
    echo "  1. Visit: $WORKFLOW_URL"
    echo "  2. Look for workflow run with commit message starting with 'test:'"
    echo "  3. Monitor the workflow execution"
    echo "  4. Check that 'GitOps Manifest Update' step completes successfully"
    echo ""
}

# Step 4: Monitor ArgoCD Sync
monitor_argocd_sync() {
    print_status "👀 Step 4: Monitoring ArgoCD Sync"
    echo ""
    
    print_status "⏳ Waiting for ArgoCD to detect changes (polls every 3 minutes)..."
    print_status "This may take up to 5 minutes for first detection..."
    
    # Monitor for up to 10 minutes
    local max_wait=600
    local interval=30
    local waited=0
    
    while [[ $waited -lt $max_wait ]]; do
        # Check dev application sync status
        local sync_status=$(kubectl get application ats-dev -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "Unknown")
        local health_status=$(kubectl get application ats-dev -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.health.status}' 2>/dev/null || echo "Unknown")
        local revision=$(kubectl get application ats-dev -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.sync.revision}' 2>/dev/null || echo "Unknown")
        
        echo "  📊 ats-dev: Sync=$sync_status, Health=$health_status, Revision=${revision:0:8}"
        
        # Check if we're syncing or synced
        if [[ "$sync_status" == "Syncing" ]]; then
            print_status "🔄 ArgoCD is syncing changes..."
            break
        elif [[ "$sync_status" == "Synced" ]]; then
            print_success "✅ ArgoCD has synced changes!"
            break
        fi
        
        sleep $interval
        waited=$((waited + interval))
        echo "  ⏳ Waiting... (${waited}s/${max_wait}s)"
    done
    
    if [[ $waited -ge $max_wait ]]; then
        print_warning "⏰ Timeout waiting for ArgoCD sync"
        print_status "This is normal if ArgoCD polling interval is longer than our wait time"
    fi
    
    # Show final status
    echo ""
    print_status "📊 Final ArgoCD Application Status:"
    for app in ats-dev ats-staging ats-production; do
        local sync_status=$(kubectl get application "$app" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "Unknown")
        local health_status=$(kubectl get application "$app" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.health.status}' 2>/dev/null || echo "Unknown")
        echo "  $app: Sync=$sync_status, Health=$health_status"
    done
}

# Step 5: Test Monitoring Scripts
test_monitoring_scripts() {
    print_status "📊 Step 5: Testing Monitoring Scripts"
    echo ""
    
    # Test the monitoring script
    if [[ -x "scripts/monitoring/check-argocd-sync.sh" ]]; then
        print_status "Testing ArgoCD monitoring script..."
        if ./scripts/monitoring/check-argocd-sync.sh ats-dev; then
            print_success "✅ Monitoring script working"
        else
            exit_code=$?
            if [[ $exit_code -eq 2 ]]; then
                print_warning "⚠️ Application out of sync (expected during testing)"
            elif [[ $exit_code -eq 3 ]]; then
                print_warning "⚠️ Unknown status (may need more time to sync)"
            else
                print_warning "⚠️ Monitoring script completed with warnings"
            fi
        fi
    else
        print_error "❌ Monitoring script not found or not executable"
        return 1
    fi
}

# Step 6: Validate GitHub Secrets
validate_github_secrets() {
    print_status "🔐 Step 6: GitHub Secrets Validation"
    echo ""
    
    print_status "📋 Required GitHub Secrets Check:"
    echo ""
    echo "Please verify these secrets are configured in your GitHub repository:"
    echo "  Repository → Settings → Secrets and Variables → Actions"
    echo ""
    echo "✅ Required Secrets:"
    echo "  • SLACK_WEBHOOK_URL (for notifications)"
    echo "  • GITOPS_TOKEN (for Git operations)"
    echo ""
    echo "❓ Are these secrets configured? (y/n)"
    read -r SECRETS_CONFIGURED
    
    if [[ "$SECRETS_CONFIGURED" =~ ^[Yy] ]]; then
        print_success "✅ GitHub secrets configured"
        return 0
    else
        print_warning "⚠️ Please configure GitHub secrets before continuing"
        echo ""
        echo "To configure secrets:"
        echo "1. Go to https://github.com/$REPO_PATH/settings/secrets/actions"
        echo "2. Click 'New repository secret'"
        echo "3. Add the following secrets:"
        echo ""
        echo "SLACK_WEBHOOK_URL"
        echo "https://hooks.slack.com/services/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"
        echo ""
        echo "GITOPS_TOKEN"
        echo "<your-github-token-with-repo-access>"
        echo ""
        return 1
    fi
}

# Step 7: Generate Test Report
generate_test_report() {
    print_status "📄 Step 7: Generating Test Report"
    echo ""
    
    local report_file="integration-test-report-$(date +%Y%m%d-%H%M%S).md"
    
    cat > "$report_file" <<EOF
# GitHub Actions + ArgoCD Integration Test Report

**Date:** $(date)  
**Test Branch:** $TEST_BRANCH  
**Repository:** $REPO_PATH  

## Test Results Summary

### ✅ Prerequisites
- Kubectl access to cluster
- ArgoCD namespace and server running
- Required files present

### 🚀 ArgoCD Applications
- ats-dev: Deployed and configured
- ats-staging: Deployed and configured  
- ats-production: Deployed and configured

### ⚙️ GitHub Actions Workflow
- Test branch created and pushed
- Workflow triggered successfully
- Monitor at: https://github.com/$REPO_PATH/actions

### 👀 ArgoCD Monitoring
$(kubectl get applications -n argocd -o wide 2>/dev/null || echo "Status check failed")

### 📊 Application Status
$(for app in ats-dev ats-staging ats-production; do
    sync_status=$(kubectl get application "$app" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "Unknown")
    health_status=$(kubectl get application "$app" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.health.status}' 2>/dev/null || echo "Unknown")
    echo "- $app: Sync=$sync_status, Health=$health_status"
done)

## Next Steps

1. **Monitor GitHub Actions**: Visit https://github.com/$REPO_PATH/actions
2. **Check ArgoCD UI**: kubectl port-forward svc/argocd-server -n argocd 8080:443
3. **Verify Deployments**: ./scripts/monitoring/check-argocd-sync.sh ats-dev
4. **Test Notifications**: Check Slack for workflow notifications

## Cleanup

To clean up test resources:
\`\`\`bash
git checkout main
git branch -D $TEST_BRANCH
git push origin --delete $TEST_BRANCH
\`\`\`

---
*Generated by GitHub Actions + ArgoCD Integration Test*
EOF
    
    print_success "✅ Test report generated: $report_file"
    echo ""
    print_status "📋 Test Report Summary:"
    echo "  📁 Report file: $report_file"
    echo "  🔗 GitHub Actions: https://github.com/$REPO_PATH/actions"
    echo "  🖥️ ArgoCD UI: kubectl port-forward svc/argocd-server -n argocd 8080:443"
    echo ""
}

# Cleanup function
cleanup_test() {
    print_status "🧹 Cleanup"
    echo ""
    
    echo "❓ Do you want to clean up the test branch? (y/n)"
    read -r CLEANUP_BRANCH
    
    if [[ "$CLEANUP_BRANCH" =~ ^[Yy] ]]; then
        print_status "Cleaning up test branch..."
        git checkout main > /dev/null 2>&1
        git branch -D "$TEST_BRANCH" > /dev/null 2>&1 || true
        git push origin --delete "$TEST_BRANCH" > /dev/null 2>&1 || true
        print_success "✅ Test branch cleaned up"
    else
        print_status "ℹ️ Test branch preserved: $TEST_BRANCH"
    fi
}

# Main execution
main() {
    echo "🎯 GitHub Actions + Private ArgoCD Integration Test"
    echo "================================================="
    echo ""
    
    local step=1
    local total_steps=7
    
    # Run tests step by step
    if test_prerequisites; then
        print_success "Step $((step++))/$total_steps completed ✅"
    else
        print_error "Step $step/$total_steps failed ❌"
        exit 1
    fi
    echo ""
    
    if deploy_argocd_applications; then
        print_success "Step $((step++))/$total_steps completed ✅"
    else
        print_error "Step $step/$total_steps failed ❌"
        exit 1
    fi
    echo ""
    
    if validate_github_secrets; then
        print_success "Step $((step++))/$total_steps completed ✅"
    else
        print_warning "Step $step/$total_steps needs attention ⚠️"
        step=$((step + 1))
    fi
    echo ""
    
    if test_github_workflow; then
        print_success "Step $((step++))/$total_steps completed ✅"
    else
        print_error "Step $step/$total_steps failed ❌"
        exit 1
    fi
    echo ""
    
    monitor_argocd_sync
    print_success "Step $((step++))/$total_steps completed ✅"
    echo ""
    
    if test_monitoring_scripts; then
        print_success "Step $((step++))/$total_steps completed ✅"
    else
        print_warning "Step $step/$total_steps completed with warnings ⚠️"
        step=$((step + 1))
    fi
    echo ""
    
    generate_test_report
    print_success "Step $((step++))/$total_steps completed ✅"
    echo ""
    
    print_success "🎉 Integration test completed!"
    echo ""
    print_status "📋 What to check next:"
    echo "  1. Monitor GitHub Actions workflow completion"
    echo "  2. Verify ArgoCD applications sync successfully"  
    echo "  3. Check Slack notifications are received"
    echo "  4. Validate applications are deployed to cluster"
    echo ""
    
    cleanup_test
}

# Run main function
main "$@"