#!/bin/bash
set -euo pipefail

# Private ArgoCD Integration Setup for GitHub Actions
echo "🔒 Setting up Private ArgoCD Integration for GitHub Actions"

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

# Configuration
ARGOCD_NAMESPACE="argocd"

print_status "📋 Private ArgoCD Integration Strategies"
echo ""
echo "Since your ArgoCD is in a private network, GitHub Actions cannot directly access it."
echo "Here are the recommended integration approaches:"
echo ""

# Strategy 1: GitOps Pull-Based (Recommended)
echo "🎯 Strategy 1: GitOps Pull-Based Integration (RECOMMENDED)"
echo "================================================================"
echo ""
print_success "✅ How it works:"
echo "  1. GitHub Actions updates Kubernetes manifests in your repository"
echo "  2. ArgoCD polls the repository and detects changes"
echo "  3. ArgoCD automatically syncs the changes to your cluster"
echo "  4. No direct API access needed from GitHub Actions"
echo ""

print_status "🔧 Implementation:"
cat << 'EOF'
# This approach modifies the improved workflow to use GitOps without direct API calls

Benefits:
✅ Works with private ArgoCD instances
✅ True GitOps - all changes go through Git
✅ ArgoCD handles the deployment timing
✅ No need to expose ArgoCD API
✅ Automatic drift detection and self-healing

Workflow changes needed:
- Remove direct ArgoCD API calls
- Focus on manifest updates and Git commits
- Use ArgoCD webhooks for notifications (optional)
EOF
echo ""

# Strategy 2: Webhook-Based
echo "🎯 Strategy 2: Webhook-Based Integration"
echo "========================================"
echo ""
print_success "✅ How it works:"
echo "  1. GitHub Actions triggers a webhook to your private network"
echo "  2. A service in your private network receives the webhook"
echo "  3. That service calls ArgoCD API locally"
echo "  4. Results are sent back to GitHub Actions or Slack"
echo ""

# Strategy 3: Self-Hosted Runner
echo "🎯 Strategy 3: Self-Hosted GitHub Runner"
echo "======================================="
echo ""
print_success "✅ How it works:"
echo "  1. Run a GitHub Actions runner inside your private network"
echo "  2. This runner can access your private ArgoCD instance"
echo "  3. Full ArgoCD API access while maintaining security"
echo ""

# Implementation choice
echo "❓ Which strategy would you prefer?"
echo ""
echo "1) GitOps Pull-Based (Recommended - no ArgoCD API needed)"
echo "2) Webhook-Based (Requires webhook service in your network)" 
echo "3) Self-Hosted Runner (Requires runner setup in your network)"
echo "4) Show me all implementations"
echo ""

read -p "Enter your choice (1-4): " STRATEGY_CHOICE

case $STRATEGY_CHOICE in
    1)
        implement_gitops_strategy
        ;;
    2)
        implement_webhook_strategy
        ;;
    3)
        implement_self_hosted_runner
        ;;
    4)
        implement_all_strategies
        ;;
    *)
        print_error "Invalid choice. Defaulting to GitOps strategy."
        implement_gitops_strategy
        ;;
esac

# GitOps Pull-Based Implementation
implement_gitops_strategy() {
    print_status "🎯 Implementing GitOps Pull-Based Integration"
    echo ""
    
    # Create modified workflow for GitOps
    print_status "Creating GitOps-optimized workflow..."
    
    # Modify the improved workflow to remove ArgoCD API calls
    sed '/Enhanced Argo CD Integration/,/ARGOCD_HEALTH=${final_status}/c\
    - name: GitOps Manifest Update (Private ArgoCD Compatible)\
      run: |\
        echo "🔄 Updating manifests for GitOps deployment..."\
        \
        TARGET_ENV="${{ needs.preflight.outputs.target-env }}"\
        VERSION="${{ needs.preflight.outputs.version }}"\
        TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")\
        \
        echo "Environment: ${TARGET_ENV}"\
        echo "Version: ${VERSION}"\
        \
        # Update image tags for each service\
        for service_dir in k8s/*/; do\
          service=$(basename "$service_dir")\
          deployment_file="${service_dir}deployment.yaml"\
          \
          if [[ -f "$deployment_file" && "$service" =~ ^(minute-service|eod-service|analytics-service)$ ]]; then\
            echo "Updating ${service}..."\
            \
            # Update image tag\
            NEW_IMAGE="${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}-${service}:${{ github.ref_name }}-${{ github.sha }}"\
            \
            # Use yq to update the image\
            yq eval "(.spec.template.spec.containers[] | select(.name == \"${service}\") | .image) = \"${NEW_IMAGE}\"" -i "$deployment_file"\
            \
            # Update deployment annotation for rolling restart\
            yq eval ".spec.template.metadata.annotations[\"deployment.kubernetes.io/restartedAt\"] = \"${TIMESTAMP}\"" -i "$deployment_file"\
            yq eval ".spec.template.metadata.annotations[\"deployment.kubernetes.io/version\"] = \"${VERSION}\"" -i "$deployment_file"\
            \
            echo "✅ Updated ${service}: ${NEW_IMAGE}"\
          fi\
        done\
        \
        # Store deployment info for notifications\
        echo "DEPLOYMENT_TARGET=${TARGET_ENV}" >> $GITHUB_ENV\
        echo "DEPLOYMENT_VERSION=${VERSION}" >> $GITHUB_ENV' \
    .github/workflows/ats-ci-cd-improved.yaml > .github/workflows/ats-ci-cd-gitops.yaml
    
    # Create ArgoCD Application configuration
    create_argocd_app_config
    
    # Create monitoring script
    create_deployment_monitor
    
    # Generate configuration
    generate_gitops_config
    
    print_success "✅ GitOps integration setup complete!"
}

# Create ArgoCD Application Config
create_argocd_app_config() {
    print_status "Creating ArgoCD application configuration..."
    
    mkdir -p argocd/applications
    
    # Get repository URL
    REPO_URL=$(git config --get remote.origin.url || echo "https://github.com/AkoloTechnologies/ats-genai.git")
    
    cat > argocd/applications/ats-dev.yaml <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ats-dev
  namespace: argocd
  finalizers:
    - resources-finalizer.argocd.argoproj.io
  annotations:
    # Notifications for deployment status
    notifications.argoproj.io/subscribe.on-deployed.slack: ats-deployments
    notifications.argoproj.io/subscribe.on-health-degraded.slack: ats-alerts
    notifications.argoproj.io/subscribe.on-sync-failed.slack: ats-alerts
spec:
  project: default
  
  # Source repository
  source:
    repoURL: ${REPO_URL}
    targetRevision: main
    path: k8s
    
  # Destination cluster and namespace  
  destination:
    server: https://kubernetes.default.svc
    namespace: ats-dev
    
  # Sync policy
  syncPolicy:
    automated:
      prune: true        # Remove resources not in Git
      selfHeal: true     # Revert manual changes
      allowEmpty: false  # Don't sync empty directories
    
    syncOptions:
      - CreateNamespace=true
      - PrunePropagationPolicy=foreground
      - PruneLast=true
      
    retry:
      limit: 3
      backoff:
        duration: 5s
        factor: 2
        maxDuration: 3m
        
  # Health checks
  ignoreDifferences:
  - group: apps
    kind: Deployment
    jsonPointers:
    - /spec/replicas
    # Ignore replica differences for HPA
    
  # Revision history
  revisionHistoryLimit: 5
EOF

    cat > argocd/applications/ats-staging.yaml <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ats-staging
  namespace: argocd
  finalizers:
    - resources-finalizer.argocd.argoproj.io
spec:
  project: default
  source:
    repoURL: ${REPO_URL}
    targetRevision: develop
    path: k8s
  destination:
    server: https://kubernetes.default.svc
    namespace: ats-staging
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
EOF

    cat > argocd/applications/ats-production.yaml <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ats-production
  namespace: argocd
  finalizers:
    - resources-finalizer.argocd.argoproj.io
spec:
  project: default
  source:
    repoURL: ${REPO_URL}
    targetRevision: main
    path: k8s
  destination:
    server: https://kubernetes.default.svc
    namespace: ats-production
  syncPolicy:
    # Manual sync for production
    syncOptions:
      - CreateNamespace=true
    retry:
      limit: 5
      backoff:
        duration: 5s
        factor: 2
        maxDuration: 10m
EOF
    
    print_success "ArgoCD applications configured for dev, staging, and production"
}

# Create deployment monitoring script
create_deployment_monitor() {
    print_status "Creating deployment monitoring script..."
    
    mkdir -p scripts/monitoring
    
    cat > scripts/monitoring/check-argocd-sync.sh <<'EOF'
#!/bin/bash
set -euo pipefail

# ArgoCD Deployment Status Monitor (for private ArgoCD)
# This script runs inside your private network to check ArgoCD sync status

ARGOCD_NAMESPACE="${ARGOCD_NAMESPACE:-argocd}"
APP_NAME="${1:-ats-dev}"

echo "🔍 Checking ArgoCD sync status for application: $APP_NAME"

# Check if ArgoCD CLI is available
if ! command -v argocd > /dev/null 2>&1; then
    echo "❌ ArgoCD CLI not found. Please install it first."
    echo "Download: https://github.com/argoproj/argo-cd/releases/latest/download/argocd-linux-amd64"
    exit 1
fi

# Check application status
APP_STATUS=$(kubectl get application "$APP_NAME" -n "$ARGOCD_NAMESPACE" -o json 2>/dev/null || echo "{}")

if [[ "$APP_STATUS" == "{}" ]]; then
    echo "❌ Application $APP_NAME not found"
    exit 1
fi

# Extract status information
HEALTH=$(echo "$APP_STATUS" | jq -r '.status.health.status // "Unknown"')
SYNC_STATUS=$(echo "$APP_STATUS" | jq -r '.status.sync.status // "Unknown"')
REVISION=$(echo "$APP_STATUS" | jq -r '.status.sync.revision // "Unknown"')

echo "📊 Application Status:"
echo "  Health: $HEALTH"
echo "  Sync: $SYNC_STATUS" 
echo "  Revision: ${REVISION:0:8}"

# Check if sync is in progress
if [[ "$SYNC_STATUS" == "Syncing" ]]; then
    echo "⏳ Sync in progress..."
    
    # Wait for sync to complete (max 5 minutes)
    for i in {1..60}; do
        sleep 5
        CURRENT_STATUS=$(kubectl get application "$APP_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "Unknown")
        
        if [[ "$CURRENT_STATUS" != "Syncing" ]]; then
            SYNC_STATUS="$CURRENT_STATUS"
            break
        fi
        
        echo "  Still syncing... (${i}/60)"
    done
fi

# Final status check
FINAL_HEALTH=$(kubectl get application "$APP_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.health.status}' 2>/dev/null || echo "Unknown")
FINAL_SYNC=$(kubectl get application "$APP_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "Unknown")

echo ""
echo "🏁 Final Status:"
echo "  Health: $FINAL_HEALTH"
echo "  Sync: $FINAL_SYNC"

# Exit codes for automation
if [[ "$FINAL_HEALTH" == "Healthy" && "$FINAL_SYNC" == "Synced" ]]; then
    echo "✅ Deployment successful!"
    exit 0
elif [[ "$FINAL_HEALTH" == "Degraded" ]]; then
    echo "❌ Deployment failed - application is degraded"
    exit 1
elif [[ "$FINAL_SYNC" == "OutOfSync" ]]; then
    echo "⚠️ Application is out of sync"
    exit 2
else
    echo "⚠️ Unknown status - manual investigation needed"
    exit 3
fi
EOF
    
    chmod +x scripts/monitoring/check-argocd-sync.sh
    print_success "Deployment monitoring script created"
}

# Generate GitOps configuration
generate_gitops_config() {
    print_status "Generating GitOps configuration..."
    
    cat > argocd-gitops-setup.md <<'EOF'
# GitOps Integration with Private ArgoCD

## Overview

This setup enables GitHub Actions to work with your private ArgoCD instance using pure GitOps principles - no direct API access required.

## How It Works

1. **GitHub Actions**: Updates Kubernetes manifests in the repository
2. **ArgoCD Polling**: Detects changes in the Git repository 
3. **Automatic Sync**: ArgoCD syncs changes to your cluster
4. **Monitoring**: Use provided scripts to monitor deployment status

## Setup Instructions

### 1. Deploy ArgoCD Applications

Apply the ArgoCD application configurations to your cluster:

```bash
# Apply all applications
kubectl apply -f argocd/applications/

# Or apply individually
kubectl apply -f argocd/applications/ats-dev.yaml
kubectl apply -f argocd/applications/ats-staging.yaml
kubectl apply -f argocd/applications/ats-production.yaml
```

### 2. Configure GitHub Secrets

Add these secrets to your GitHub repository (only these are needed for GitOps):

```
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/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
GITOPS_TOKEN=<your-github-token-with-repo-access>
```

### 3. Deploy Improved Workflow

```bash
# Use the GitOps-optimized workflow
cp .github/workflows/ats-ci-cd-gitops.yaml .github/workflows/ats-ci-cd.yaml

# Commit and push
git add .github/workflows/ats-ci-cd.yaml argocd/
git commit -m "feat: deploy GitOps-compatible CI/CD workflow"  
git push
```

### 4. Monitor Deployments (Optional)

Run the monitoring script from within your private network:

```bash
# Check dev deployment
./scripts/monitoring/check-argocd-sync.sh ats-dev

# Check staging deployment  
./scripts/monitoring/check-argocd-sync.sh ats-staging
```

## Workflow Features

✅ **No ArgoCD API Access Needed**: Works with private ArgoCD instances
✅ **Pure GitOps**: All changes go through Git commits
✅ **Multi-Environment**: Supports dev, staging, production
✅ **Automatic Sync**: ArgoCD handles deployment timing
✅ **Self-Healing**: ArgoCD reverts manual changes automatically
✅ **Drift Detection**: ArgoCD monitors and corrects configuration drift

## ArgoCD Configuration

The ArgoCD applications are configured with:

- **Automated Sync**: Changes are automatically deployed
- **Self-Healing**: Manual changes are automatically reverted
- **Pruning**: Resources not in Git are automatically removed
- **Retry Logic**: Failed syncs are automatically retried
- **Health Checks**: Application health is continuously monitored

## Monitoring and Notifications

### ArgoCD UI
Access your ArgoCD UI to monitor deployments visually.

### Slack Notifications (Optional)
Configure ArgoCD notifications to send Slack updates:

1. Install ArgoCD notifications controller
2. Configure Slack webhook in ArgoCD
3. Applications will send notifications on sync events

### Manual Status Checks
```bash
# Check application status
kubectl get applications -n argocd

# Get detailed status
kubectl describe application ats-dev -n argocd

# Check sync history
kubectl get application ats-dev -n argocd -o yaml | grep -A 10 "history:"
```

## Troubleshooting

### Sync Issues
```bash
# Force refresh
kubectl patch application ats-dev -n argocd -p '{"metadata":{"annotations":{"argocd.argoproj.io/refresh":"hard"}}}' --type=merge

# Manual sync
argocd app sync ats-dev --force
```

### Rollback
```bash
# Rollback to previous revision
argocd app rollback ats-dev

# Or rollback Git commit and let ArgoCD sync
git revert <commit-hash>
git push
```

## Benefits of This Approach

1. **Security**: No need to expose ArgoCD API to GitHub Actions
2. **Compliance**: All changes are audited through Git history
3. **Reliability**: ArgoCD handles retries and error recovery
4. **Monitoring**: Built-in health checks and status reporting
5. **Flexibility**: Works with any Git repository structure

EOF

    print_success "GitOps setup documentation created: argocd-gitops-setup.md"
}

# Webhook-based implementation
implement_webhook_strategy() {
    print_status "🎯 Implementing Webhook-Based Integration"
    echo ""
    
    print_status "This strategy requires a webhook service in your private network."
    echo "Would you like me to create the webhook service implementation? (y/n)"
    read -p "> " CREATE_WEBHOOK
    
    if [[ "$CREATE_WEBHOOK" =~ ^[Yy] ]]; then
        create_webhook_service
    else
        print_status "Webhook service setup skipped. See documentation for manual setup."
    fi
}

# Self-hosted runner implementation  
implement_self_hosted_runner() {
    print_status "🎯 Implementing Self-Hosted Runner Strategy"
    echo ""
    
    print_status "This strategy requires setting up a GitHub Actions runner in your private network."
    create_self_hosted_runner_guide
}

# Create webhook service
create_webhook_service() {
    print_status "Creating webhook service for private ArgoCD integration..."
    
    mkdir -p webhook-service
    
    # Create webhook service implementation
    cat > webhook-service/argocd-webhook.py <<'EOF'
#!/usr/bin/env python3
"""
ArgoCD Webhook Service for Private Network Integration
Receives webhooks from GitHub Actions and calls private ArgoCD API
"""

import os
import json
import asyncio
import aiohttp
import logging
from aiohttp import web
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
ARGOCD_SERVER = os.getenv('ARGOCD_SERVER', 'https://argocd-server.argocd.svc.cluster.local')
ARGOCD_TOKEN = os.getenv('ARGOCD_TOKEN', '')
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', 'your-webhook-secret')
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL', '')

class ArgocdWebhookHandler:
    def __init__(self):
        self.session = None
        
    async def initialize(self):
        connector = aiohttp.TCPConnector(verify_ssl=False)
        self.session = aiohttp.ClientSession(connector=connector)
        
    async def cleanup(self):
        if self.session:
            await self.session.close()
    
    async def handle_deployment(self, request):
        """Handle deployment webhook from GitHub Actions"""
        try:
            # Verify webhook secret
            auth_header = request.headers.get('Authorization', '')
            if not auth_header.startswith('Bearer ') or auth_header[7:] != WEBHOOK_SECRET:
                return web.Response(status=401, text='Unauthorized')
            
            # Parse request
            data = await request.json()
            app_name = data.get('app_name', 'ats-dev')
            environment = data.get('environment', 'dev')
            version = data.get('version', 'unknown')
            
            logger.info(f"Processing deployment request for {app_name} in {environment}")
            
            # Trigger ArgoCD sync
            result = await self.sync_application(app_name)
            
            # Wait for sync completion
            if result['success']:
                status = await self.wait_for_sync(app_name)
                result.update(status)
            
            # Send notification
            if SLACK_WEBHOOK_URL:
                await self.send_slack_notification(app_name, environment, version, result)
            
            return web.json_response(result)
            
        except Exception as e:
            logger.error(f"Deployment handler error: {e}")
            return web.Response(status=500, text=f'Internal error: {e}')
    
    async def sync_application(self, app_name):
        """Trigger ArgoCD application sync"""
        url = f"{ARGOCD_SERVER}/api/v1/applications/{app_name}/sync"
        headers = {
            'Authorization': f'Bearer {ARGOCD_TOKEN}',
            'Content-Type': 'application/json'
        }
        
        sync_request = {
            'prune': False,
            'dryRun': False,
            'strategy': {'hook': {'force': True}}
        }
        
        try:
            async with self.session.post(url, json=sync_request, headers=headers) as response:
                if response.status == 200:
                    logger.info(f"Successfully triggered sync for {app_name}")
                    return {'success': True, 'message': 'Sync triggered'}
                else:
                    text = await response.text()
                    logger.error(f"Sync failed: {response.status} - {text}")
                    return {'success': False, 'error': f'Sync failed: {text}'}
                    
        except Exception as e:
            logger.error(f"Sync request error: {e}")
            return {'success': False, 'error': str(e)}
    
    async def wait_for_sync(self, app_name, max_wait=300):
        """Wait for application sync to complete"""
        url = f"{ARGOCD_SERVER}/api/v1/applications/{app_name}"
        headers = {'Authorization': f'Bearer {ARGOCD_TOKEN}'}
        
        for _ in range(max_wait // 5):
            try:
                async with self.session.get(url, headers=headers) as response:
                    if response.status == 200:
                        app_data = await response.json()
                        health = app_data.get('status', {}).get('health', {}).get('status')
                        sync_status = app_data.get('status', {}).get('sync', {}).get('status')
                        
                        if sync_status != 'Syncing':
                            return {
                                'health': health,
                                'sync_status': sync_status,
                                'completed': True
                            }
                        
                await asyncio.sleep(5)
                        
            except Exception as e:
                logger.error(f"Status check error: {e}")
                break
        
        return {'health': 'Unknown', 'sync_status': 'Timeout', 'completed': False}
    
    async def send_slack_notification(self, app_name, environment, version, result):
        """Send Slack notification"""
        if result['success'] and result.get('health') == 'Healthy':
            color = 'good'
            status = '✅ Deployment Successful'
        else:
            color = 'danger'  
            status = '❌ Deployment Failed'
        
        message = {
            'attachments': [{
                'color': color,
                'title': f'{status} - {app_name}',
                'fields': [
                    {'title': 'Environment', 'value': environment, 'short': True},
                    {'title': 'Version', 'value': version, 'short': True},
                    {'title': 'Health', 'value': result.get('health', 'Unknown'), 'short': True},
                    {'title': 'Sync Status', 'value': result.get('sync_status', 'Unknown'), 'short': True}
                ],
                'footer': 'Private ArgoCD Webhook',
                'ts': int(datetime.now().timestamp())
            }]
        }
        
        try:
            async with self.session.post(SLACK_WEBHOOK_URL, json=message) as response:
                if response.status == 200:
                    logger.info("Slack notification sent")
                else:
                    logger.error(f"Slack notification failed: {response.status}")
        except Exception as e:
            logger.error(f"Slack notification error: {e}")

async def create_app():
    handler = ArgocdWebhookHandler()
    await handler.initialize()
    
    app = web.Application()
    app['handler'] = handler
    
    # Routes
    app.router.add_post('/webhook/deploy', handler.handle_deployment)
    app.router.add_get('/health', lambda r: web.Response(text='OK'))
    
    # Cleanup handler
    async def cleanup(app):
        await app['handler'].cleanup()
    
    app.on_cleanup.append(cleanup)
    return app

if __name__ == '__main__':
    web.run_app(create_app(), host='0.0.0.0', port=8080)
EOF

    # Create Dockerfile for webhook service
    cat > webhook-service/Dockerfile <<'EOF'
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install aiohttp

# Copy service
COPY argocd-webhook.py .

# Expose port
EXPOSE 8080

# Run service
CMD ["python", "argocd-webhook.py"]
EOF

    # Create Kubernetes deployment
    cat > webhook-service/webhook-deployment.yaml <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: argocd-webhook-service
  namespace: argocd
spec:
  replicas: 2
  selector:
    matchLabels:
      app: argocd-webhook-service
  template:
    metadata:
      labels:
        app: argocd-webhook-service
    spec:
      containers:
      - name: webhook
        image: your-registry/argocd-webhook:latest
        ports:
        - containerPort: 8080
        env:
        - name: ARGOCD_SERVER
          value: "https://argocd-server.argocd.svc.cluster.local"
        - name: ARGOCD_TOKEN
          valueFrom:
            secretKeyRef:
              name: argocd-webhook-secret
              key: token
        - name: WEBHOOK_SECRET
          valueFrom:
            secretKeyRef:
              name: argocd-webhook-secret
              key: webhook-secret
        - name: SLACK_WEBHOOK_URL
          valueFrom:
            secretKeyRef:
              name: argocd-webhook-secret
              key: slack-webhook-url
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5

---
apiVersion: v1
kind: Service
metadata:
  name: argocd-webhook-service
  namespace: argocd
spec:
  selector:
    app: argocd-webhook-service
  ports:
  - port: 80
    targetPort: 8080
  type: ClusterIP

---
apiVersion: v1
kind: Secret
metadata:
  name: argocd-webhook-secret
  namespace: argocd
type: Opaque
stringData:
  token: "your-argocd-token"
  webhook-secret: "your-webhook-secret"  
  slack-webhook-url: "https://hooks.slack.com/services/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"
EOF

    print_success "Webhook service created in webhook-service/ directory"
}

# Create self-hosted runner guide
create_self_hosted_runner_guide() {
    cat > self-hosted-runner-setup.md <<'EOF'
# Self-Hosted GitHub Runner for Private ArgoCD

## Overview

Set up a GitHub Actions runner inside your private network that can access your ArgoCD instance directly.

## Prerequisites

- Kubernetes cluster with ArgoCD
- Node or VM with Docker access inside your private network
- GitHub repository admin access

## Setup Steps

### 1. Create Runner Registration Token

1. Go to your GitHub repository
2. Navigate to Settings > Actions > Runners  
3. Click "New self-hosted runner"
4. Copy the registration token

### 2. Deploy Runner in Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: github-runner
  namespace: github-runners
spec:
  replicas: 1
  selector:
    matchLabels:
      app: github-runner
  template:
    metadata:
      labels:
        app: github-runner
    spec:
      containers:
      - name: runner
        image: sumologic/kubernetes-setup-github-actions-runner:latest
        env:
        - name: GITHUB_TOKEN
          value: "your-registration-token"
        - name: GITHUB_REPOSITORY
          value: "your-org/your-repo"
        - name: RUNNER_NAME
          value: "private-network-runner"
        volumeMounts:
        - name: docker-sock
          mountPath: /var/run/docker.sock
      volumes:
      - name: docker-sock
        hostPath:
          path: /var/run/docker.sock
```

### 3. Configure Workflow

Modify your workflow to use the self-hosted runner:

```yaml
jobs:
  deploy:
    runs-on: [self-hosted, linux]
    # Rest of your job configuration
```

### 4. ArgoCD Access

The self-hosted runner can now access your private ArgoCD:

```yaml
- name: Direct ArgoCD Integration
  run: |
    # Direct API calls work from private network
    curl -H "Authorization: Bearer ${{ secrets.ARGOCD_TOKEN }}" \
      "${ARGOCD_SERVER}/api/v1/applications/ats-dev/sync"
```
EOF
    
    print_success "Self-hosted runner guide created: self-hosted-runner-setup.md"
}

# Implement all strategies
implement_all_strategies() {
    print_status "🎯 Implementing All Integration Strategies"
    echo ""
    
    print_status "Creating comprehensive private ArgoCD integration..."
    
    implement_gitops_strategy
    echo ""
    
    implement_webhook_strategy  
    echo ""
    
    implement_self_hosted_runner
    echo ""
    
    print_success "✅ All integration strategies implemented!"
    print_status "Choose the strategy that best fits your infrastructure and security requirements."
}

# Main execution
main() {
    echo "🔒 Private ArgoCD Integration Setup"
    echo "================================="
    echo ""
    
    print_warning "⚠️  Since your ArgoCD is in a private network, direct API access from GitHub Actions won't work."
    print_status "Let's set up the best integration approach for your environment."
    echo ""
}

# Run main function
main "$@"