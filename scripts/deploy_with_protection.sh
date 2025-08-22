#!/bin/bash
"""
Protected Deployment Script for Analytics Platform

This script ensures chart visualization functionality is working before deploying.
It prevents deployments that would break the user experience.

Usage: 
    ./scripts/deploy_with_protection.sh

Environment Variables:
    SKIP_CHART_TESTS=1    # Skip chart tests (NOT RECOMMENDED)
    TEST_URL=...          # Override test URL (default: localhost:3000)
"""

set -e  # Exit on any error

# Configuration
TEST_URL=${TEST_URL:-"http://localhost:3000"}
SKIP_CHART_TESTS=${SKIP_CHART_TESTS:-0}

echo "🚀 PROTECTED DEPLOYMENT FOR ANALYTICS PLATFORM"
echo "=============================================="
echo "Target URL: $TEST_URL"
echo "Chart Tests: $([ $SKIP_CHART_TESTS -eq 1 ] && echo 'SKIPPED (NOT RECOMMENDED)' || echo 'ENABLED')"
echo ""

# Step 1: Check if service is running
echo "📋 Step 1: Checking service availability..."
if ! curl -s "$TEST_URL/health" > /dev/null; then
    echo "❌ Service not available at $TEST_URL"
    echo "   Make sure port-forward is running:"
    echo "   kubectl port-forward -n ats-dev service/job-management-fixed-service 3000:5000"
    exit 1
fi
echo "✅ Service is available"

# Step 2: Run chart visualization regression protection
if [ $SKIP_CHART_TESTS -eq 0 ]; then
    echo ""
    echo "📋 Step 2: Running chart visualization regression protection..."
    if ! python scripts/test_chart_visualization_before_deploy.py; then
        echo ""
        echo "❌ DEPLOYMENT BLOCKED!"
        echo "Chart visualization tests failed. This would break user experience."
        echo ""
        echo "Common fixes:"
        echo "1. Ensure Chart.js library is included"
        echo "2. Check that buttons use JavaScript functions (not raw JSON links)"
        echo "3. Verify modal system is intact"
        echo "4. Confirm all JavaScript functions are defined"
        echo ""
        echo "Run the test manually for detailed error information:"
        echo "python scripts/test_chart_visualization_before_deploy.py"
        exit 1
    fi
    echo "✅ Chart visualization regression protection passed"
else
    echo ""
    echo "⚠️  Step 2: SKIPPING chart visualization tests (NOT RECOMMENDED)"
    echo "   This could allow deployments that break charts!"
fi

# Step 3: Deploy to Kubernetes
echo ""
echo "📋 Step 3: Deploying to Kubernetes..."

# Update ConfigMap with latest code
echo "   Updating ConfigMap..."
kubectl delete configmap job-management-fixed-config -n ats-dev --ignore-not-found=true
kubectl create configmap job-management-fixed-config --from-file=unified_analytics_fixed.py -n ats-dev

# Restart pods to pick up changes
echo "   Restarting pods..."
kubectl delete pod -l app=job-management-fixed -n ats-dev

# Wait for pod to be ready
echo "   Waiting for pod to be ready..."
kubectl wait --for=condition=ready pod -l app=job-management-fixed -n ats-dev --timeout=60s

echo "✅ Deployment completed successfully"

# Step 4: Post-deployment verification
echo ""
echo "📋 Step 4: Post-deployment verification..."
sleep 5  # Give service a moment to stabilize

if curl -s "$TEST_URL/health" > /dev/null; then
    echo "✅ Service is responding after deployment"
else
    echo "❌ Service not responding after deployment!"
    echo "   Check pod logs: kubectl logs -l app=job-management-fixed -n ats-dev"
    exit 1
fi

# Final success message
echo ""
echo "🎉 DEPLOYMENT SUCCESSFUL!"
echo "=============================================="
echo "Analytics Platform has been deployed with chart visualization protection."
echo ""
echo "Access the platform at: $TEST_URL"
echo "- Job Management: Interactive table with filtering and sorting"
echo "- Dataset Visualization: Interactive charts with Chart.js"
echo ""
echo "Next steps:"
echo "1. Test the deployment manually in a browser"
echo "2. Verify chart visualizations work correctly"
echo "3. Monitor logs for any issues"
echo ""
echo "Monitoring commands:"
echo "kubectl logs -l app=job-management-fixed -n ats-dev -f"
echo "kubectl get pods -l app=job-management-fixed -n ats-dev"