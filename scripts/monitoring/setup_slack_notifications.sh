#!/bin/bash
#
# Setup Slack Notifications for ATS Job Monitoring
#
# This script sets up Slack webhook integration for Kubernetes job notifications.
#
# Usage:
#     ./scripts/monitoring/setup_slack_notifications.sh --webhook-url "https://hooks.slack.com/services/..."
#     ./scripts/monitoring/setup_slack_notifications.sh --test
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Default values
NAMESPACE="ats-dev"
SECRET_NAME="slack-credentials"
SLACK_CHANNEL="#ats-dev-alerts"

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --webhook-url URL    Slack webhook URL (required for setup)"
    echo "  --channel CHANNEL    Slack channel (default: #ats-dev-alerts)"
    echo "  --test              Send test notification"
    echo "  --deploy            Deploy the notifier to Kubernetes"
    echo "  --status            Check notifier status"
    echo "  --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Setup with webhook URL"
    echo "  $0 --webhook-url 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL'"
    echo ""
    echo "  # Deploy to Kubernetes"
    echo "  $0 --deploy"
    echo ""
    echo "  # Send test notification"
    echo "  $0 --test"
}

create_slack_secret() {
    local webhook_url="$1"
    
    echo "🔐 Creating Slack webhook secret..."
    
    # Delete existing secret if it exists
    kubectl delete secret "${SECRET_NAME}" -n "${NAMESPACE}" 2>/dev/null || true
    
    # Create new secret
    kubectl create secret generic "${SECRET_NAME}" \
        --from-literal=webhook_url="${webhook_url}" \
        -n "${NAMESPACE}"
    
    echo "✅ Slack webhook secret created successfully"
}

deploy_notifier() {
    echo "🚀 Deploying Slack job notifier to Kubernetes..."
    
    # Apply the deployment
    kubectl apply -f "${PROJECT_ROOT}/k8s/slack-job-notifier.yaml"
    
    echo "⏳ Waiting for deployment to be ready..."
    kubectl wait --for=condition=available deployment/slack-job-notifier -n "${NAMESPACE}" --timeout=300s
    
    echo "✅ Slack job notifier deployed successfully!"
    
    # Show deployment status
    kubectl get deployment slack-job-notifier -n "${NAMESPACE}"
    kubectl get pods -l app=slack-job-notifier -n "${NAMESPACE}"
}

test_notification() {
    echo "🧪 Sending test notification..."
    
    # Check if notifier is running
    if ! kubectl get deployment slack-job-notifier -n "${NAMESPACE}" >/dev/null 2>&1; then
        echo "❌ Slack notifier is not deployed. Deploy it first with --deploy"
        exit 1
    fi
    
    # Get notifier pod
    NOTIFIER_POD=$(kubectl get pods -l app=slack-job-notifier -n "${NAMESPACE}" -o jsonpath='{.items[0].metadata.name}')
    
    if [[ -z "${NOTIFIER_POD}" ]]; then
        echo "❌ No notifier pod found"
        exit 1
    fi
    
    echo "📤 Sending test notification via pod: ${NOTIFIER_POD}"
    
    # Send test by running the notifier in test mode
    kubectl exec -n "${NAMESPACE}" "${NOTIFIER_POD}" -- python -c """
import asyncio
import aiohttp
import os
import time

async def send_test():
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not webhook_url:
        print('❌ No webhook URL found')
        return False
    
    payload = {
        'channel': '${SLACK_CHANNEL}',
        'username': 'ATS Job Monitor',
        'icon_emoji': ':robot_face:',
        'attachments': [
            {
                'color': 'good',
                'title': '🧪 Test Notification',
                'text': 'Slack integration is working correctly!',
                'fields': [
                    {'title': 'Test Type', 'value': 'Manual Test', 'short': True},
                    {'title': 'Timestamp', 'value': '$(date)', 'short': True}
                ],
                'footer': 'ATS Kubernetes Cluster',
                'ts': int(time.time())
            }
        ]
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(webhook_url, json=payload) as response:
            if response.status == 200:
                print('✅ Test notification sent successfully!')
                return True
            else:
                print(f'❌ Test notification failed: {response.status}')
                return False

asyncio.run(send_test())
"""
}

check_status() {
    echo "📊 Checking Slack notifier status..."
    
    # Check if secret exists
    if kubectl get secret "${SECRET_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
        echo "✅ Slack webhook secret exists"
    else
        echo "❌ Slack webhook secret missing"
    fi
    
    # Check if deployment exists
    if kubectl get deployment slack-job-notifier -n "${NAMESPACE}" >/dev/null 2>&1; then
        echo "✅ Slack notifier deployment exists"
        
        # Check deployment status
        kubectl get deployment slack-job-notifier -n "${NAMESPACE}" -o wide
        
        # Check pod status
        echo ""
        echo "Pod Status:"
        kubectl get pods -l app=slack-job-notifier -n "${NAMESPACE}" -o wide
        
        # Check recent logs
        NOTIFIER_POD=$(kubectl get pods -l app=slack-job-notifier -n "${NAMESPACE}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
        if [[ -n "${NOTIFIER_POD}" ]]; then
            echo ""
            echo "Recent Logs:"
            kubectl logs "${NOTIFIER_POD}" -n "${NAMESPACE}" --tail=10
        fi
    else
        echo "❌ Slack notifier deployment not found"
    fi
}

# Parse command line arguments
WEBHOOK_URL=""
DEPLOY=false
TEST=false
STATUS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --webhook-url)
            WEBHOOK_URL="$2"
            shift 2
            ;;
        --channel)
            SLACK_CHANNEL="$2"
            shift 2
            ;;
        --deploy)
            DEPLOY=true
            shift
            ;;
        --test)
            TEST=true
            shift
            ;;
        --status)
            STATUS=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Main execution
echo "🔔 ATS Slack Notification Setup"
echo "================================"

if [[ "${STATUS}" == true ]]; then
    check_status
    exit 0
fi

if [[ "${TEST}" == true ]]; then
    test_notification
    exit 0
fi

if [[ "${DEPLOY}" == true ]]; then
    deploy_notifier
    exit 0
fi

if [[ -n "${WEBHOOK_URL}" ]]; then
    create_slack_secret "${WEBHOOK_URL}"
    echo ""
    echo "✅ Slack webhook configured successfully!"
    echo ""
    echo "Next steps:"
    echo "1. Deploy the notifier: $0 --deploy"
    echo "2. Test notifications: $0 --test"
    echo "3. Check status: $0 --status"
    exit 0
fi

echo "❌ No action specified. Use --help for usage information."
usage
exit 1