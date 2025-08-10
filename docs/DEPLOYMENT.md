# ATS GenAI Deployment Guide

## Prerequisites

### Required Tools
- `kubectl` (Kubernetes CLI)
- `kustomize` (or kubectl with kustomize support)
- `docker` (for local builds)
- `uv` (Python package manager)
- `git` (version control)

### Access Requirements
- Kubernetes cluster access (kubeconfig configured)
- Container registry push access (GitHub Container Registry)
- GitHub repository access with Actions enabled

## Environment Setup

### 1. Prepare Secrets

Before deploying, update the secrets in each environment:

```bash
# Generate base64 encoded secrets
echo -n "postgresql://user:pass@timescaledb:5432/ats_prod" | base64
echo -n "your-tiingo-api-key" | base64
echo -n "your-polygon-api-key" | base64
```

Update the respective secrets files:
- `k8s/environments/dev/secrets.yaml`
- `k8s/environments/intg/secrets.yaml`
- `k8s/environments/prod/secrets.yaml`

### 2. Configure GitHub Secrets

In your GitHub repository settings, add these secrets:

```
GITHUB_TOKEN          # Automatically provided
KUBE_CONFIG_DEV       # Base64 encoded kubeconfig for dev cluster
KUBE_CONFIG_INTG      # Base64 encoded kubeconfig for intg cluster  
KUBE_CONFIG_PROD      # Base64 encoded kubeconfig for prod cluster
```

### 3. Set Up Environment Protection Rules

In GitHub repository settings > Environments:

**Integration Environment:**
- No protection rules (auto-deploy)
- Reviewers: Optional

**Production Environment:**
- Required reviewers: 2+ team members
- Wait timer: 5 minutes
- Restrict to main branch only

## Manual Deployment

### Development Environment

```bash
# Apply development configuration
kubectl apply -k k8s/environments/dev

# Verify deployment
kubectl get pods -n ats-dev
kubectl logs -f deployment/ats-api -n ats-dev

# Port forward for local testing
kubectl port-forward service/ats-api-service 8080:80 -n ats-dev
```

### Integration Environment

```bash
# Apply integration configuration
kubectl apply -k k8s/environments/intg

# Verify deployment
kubectl get pods -n ats-intg
kubectl describe deployment ats-api -n ats-intg

# Check service health
kubectl exec -it deployment/ats-api -n ats-intg -- curl localhost:8080/health
```

### Production Environment

```bash
# Apply production configuration (with caution)
kubectl apply -k k8s/environments/prod

# Verify deployment with zero downtime
kubectl rollout status deployment/ats-api -n ats-prod

# Monitor logs
kubectl logs -f deployment/ats-api -n ats-prod --tail=100
```

## GitOps Deployment with Argo CD

### 1. Install Argo CD

```bash
# Create argocd namespace
kubectl create namespace argocd

# Install Argo CD
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

# Wait for pods to be ready
kubectl wait --for=condition=available --timeout=300s deployment/argocd-server -n argocd

# Get initial admin password
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d
```

### 2. Access Argo CD UI

```bash
# Port forward to access UI
kubectl port-forward svc/argocd-server -n argocd 8080:443

# Login at https://localhost:8080
# Username: admin
# Password: (from previous step)
```

### 3. Deploy Applications

```bash
# Apply Argo CD applications
kubectl apply -f k8s/argocd/ats-intg-app.yaml
kubectl apply -f k8s/argocd/ats-prod-app.yaml

# Verify applications
kubectl get applications -n argocd
```

## Troubleshooting

### Common Issues

**Pod CrashLoopBackOff:**
```bash
# Check pod logs
kubectl logs -f pod/<pod-name> -n <namespace>

# Check pod events
kubectl describe pod <pod-name> -n <namespace>

# Check resource constraints
kubectl top pods -n <namespace>
```

**Database Connection Issues:**
```bash
# Verify secrets
kubectl get secrets -n <namespace>
kubectl describe secret ats-secrets -n <namespace>

# Test database connectivity
kubectl exec -it deployment/ats-api -n <namespace> -- python -c "
import asyncpg
import asyncio
async def test():
    conn = await asyncpg.connect('postgresql://...')
    print('DB connection successful')
asyncio.run(test())
"
```

**Image Pull Errors:**
```bash
# Check image exists
docker pull ghcr.io/jianjun00/ats-genai:latest

# Verify registry credentials
kubectl get secrets -n <namespace>
kubectl describe secret regcred -n <namespace>
```

### Health Check Endpoints

Test application health:
```bash
# Direct pod access
kubectl exec -it deployment/ats-api -n <namespace> -- curl localhost:8080/health

# Through service
kubectl run test-pod --rm -i --tty --image=curlimages/curl -- curl ats-api-service.<namespace>.svc.cluster.local/health
```

### Rolling Updates

```bash
# Update image tag
kubectl set image deployment/ats-api ats-api=ghcr.io/jianjun00/ats-genai:v2025.06.1 -n <namespace>

# Monitor rollout
kubectl rollout status deployment/ats-api -n <namespace>

# Rollback if needed
kubectl rollout undo deployment/ats-api -n <namespace>
```

## Monitoring Deployment Status

### Key Metrics to Monitor

```bash
# Pod status
kubectl get pods -n <namespace> -w

# Resource usage
kubectl top pods -n <namespace>
kubectl top nodes

# Events
kubectl get events -n <namespace> --sort-by='.lastTimestamp'

# Application logs
kubectl logs -f deployment/ats-api -n <namespace> --tail=50
```

### Scaling Operations

```bash
# Scale replicas
kubectl scale deployment ats-api --replicas=5 -n <namespace>

# Horizontal Pod Autoscaler (if configured)
kubectl get hpa -n <namespace>
kubectl describe hpa ats-api-hpa -n <namespace>
```

## Maintenance Procedures

### Database Migrations

```bash
# Run migrations manually
kubectl exec -it deployment/ats-api -n <namespace> -- python src/db/setup_trading_db.py

# Check migration status
kubectl logs deployment/ats-api -n <namespace> | grep migration
```

### Backup Procedures

```bash
# Database backup (if using TimescaleDB)
kubectl exec -it <timescaledb-pod> -n <namespace> -- pg_dump -U <user> <database> > backup.sql

# Configuration backup (already in Git)
git archive --format=tar.gz --output=k8s-config-backup.tar.gz HEAD k8s/
```

This deployment guide provides comprehensive procedures for both manual and automated deployments across all environments.
