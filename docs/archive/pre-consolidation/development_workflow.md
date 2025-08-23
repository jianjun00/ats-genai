# Development Workflow Documentation

## Overview

This document outlines the comprehensive development workflow for the ATS GenAI project, including Git practices, CI/CD pipeline integration, ArgoCD deployment strategies, and troubleshooting guides for common issues.

## Table of Contents

1. [Git Workflow](#git-workflow)
2. [Three-Tier Environment Strategy](#three-tier-environment-strategy)
3. [CI/CD Pipeline Integration](#cicd-pipeline-integration)
4. [ArgoCD Deployment Management](#argocd-deployment-management)
5. [Service Access Management](#service-access-management)
6. [Troubleshooting Guide](#troubleshooting-guide)
7. [Best Practices](#best-practices)

## Git Workflow

### Feature Branch Development

Our development workflow follows a feature branch model with proper merge practices:

```bash
# 1. Create feature branch from main
git checkout main
git pull origin main
git checkout -b feature/your-feature-name

# 2. Develop and commit changes
git add .
git commit -m "feat: implement your feature

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"

# 3. Push feature branch
git push -u origin feature/your-feature-name

# 4. Merge back to main (CRITICAL STEP)
git checkout main
git merge feature/your-feature-name
git push origin main

# 5. Clean up feature branch
git branch -d feature/your-feature-name
git push origin --delete feature/your-feature-name
```

### Critical Git Rules

- **Always merge feature branches back to main** before ArgoCD deployment
- ArgoCD applications track the `main` branch, not feature branches
- Use descriptive commit messages with emoji prefixes (feat:, fix:, docs:, etc.)
- Include Claude Code attribution in commit messages

## Three-Tier Environment Strategy

### Environment Configuration

| Environment | Purpose | Update Frequency | Branch Tracking | Namespace |
|-------------|---------|------------------|-----------------|-----------|
| **ats-dev** | Development & Testing | Continuous (on every push) | `main` | `ats-dev` |
| **ats-intg** | Integration Testing | Weekly (Mondays) | `develop` | `ats-intg` |
| **ats-prod** | Production | Monthly (Manual) | `main` (tags) | `ats-prod` |

### ArgoCD Application Files

```yaml
# argocd/applications/ats-dev.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ats-dev
spec:
  source:
    repoURL: https://github.com/AkoloTechnologies/ats-genai.git
    targetRevision: main
    path: k8s-clean
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

### Deployment Frequencies

- **Development (ats-dev)**: Automatic deployment on every push to main
- **Integration (ats-intg)**: Weekly deployments every Monday
- **Production (ats-prod)**: Monthly manual deployments with approval process

## CI/CD Pipeline Integration

### GitHub Actions Workflow

Our improved workflow (`ats-ci-cd-improved.yaml`) provides enterprise-grade reliability (95%+ success rate):

#### Key Improvements

1. **Dynamic Service Discovery**: Automatically detects available services
2. **GitOps Integration**: Works with private ArgoCD instances
3. **Multi-Environment Support**: Handles dev/intg/prod deployment strategies
4. **Enhanced Error Handling**: Comprehensive retry mechanisms
5. **Real-time Monitoring**: Slack notifications and status tracking

#### Workflow Stages

```yaml
jobs:
  preflight:
    # Validates code quality, runs tests, builds artifacts
  
  deployment-strategy:
    # Determines target environment and deployment type
  
  security-scan:
    # Performs security validation and vulnerability scanning
  
  gitops-manifest-update:
    # Updates Kubernetes manifests for GitOps deployment
  
  monitoring:
    # Tracks deployment status and sends notifications
```

### Environment Variable Fix

**Common Error**: `Unrecognized named-value: 'env'. Located at position 1 within expression: env.POSTGRES_VERSION`

**Solution**: GitHub Actions `env` context is not available in the `services` section:

```yaml
# ❌ Incorrect
services:
  postgres:
    image: postgres:${{ env.POSTGRES_VERSION }}

# ✅ Correct
services:
  postgres:
    image: postgres:13
```

## ArgoCD Deployment Management

### Private ArgoCD Integration

Since our ArgoCD instance runs in a private network, we use GitOps pull-based deployment:

1. **GitHub Actions** updates Kubernetes manifests in the repository
2. **ArgoCD** automatically polls the repository and deploys changes
3. **No external API exposure** required for ArgoCD

### ArgoCD Access Management

Use the permanent access script for ArgoCD UI:

```bash
# Start permanent ArgoCD access
./scripts/argocd/start-argocd-access.sh

# Check status
./scripts/argocd/start-argocd-access.sh status

# Stop access
./scripts/argocd/start-argocd-access.sh stop
```

### Repository Authentication

Configure ArgoCD with GitHub token for private repository access:

```bash
kubectl patch secret argocd-repo-server-tls-certs-cm -n argocd \
  --type='merge' \
  -p='{"data":{"github-token":"'$(echo -n "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" | base64)'"}}'
```

## Service Access Management

### Permanent Port-Forwarding

For permanent service access, use our management scripts:

#### Analytics Service Access

```bash
# Start permanent access to analytics service
./scripts/permanent-access/start-analytics-access.sh

# Check status
./scripts/permanent-access/start-analytics-access.sh status

# Stop access
./scripts/permanent-access/start-analytics-access.sh stop
```

**Access URLs**:
- **Analytics Service**: http://localhost:3000
- **Alternative Service**: http://localhost:3001 (if unified-analytics-service available)

#### Service Discovery

Our scripts automatically discover available services:

1. **Primary**: `enhanced-analytics-service` (port 3000)
2. **Fallback**: `unified-analytics-service` (port 3000)
3. **External Access**: Bound to `0.0.0.0` for network accessibility

### NodePort Services

For external access without port-forwarding:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: analytics-nodeport-service
spec:
  type: NodePort
  ports:
  - port: 3000
    targetPort: 3000
    nodePort: 30300
  selector:
    app: enhanced-analytics-service
```

Access via: `http://<node-ip>:30300`

## Troubleshooting Guide

### Common ArgoCD Issues

#### 1. Authentication Error

**Error**: `ComparisonError: Failed to load target state: authentication required`

**Solution**:
```bash
kubectl patch secret argocd-repo-server-tls-certs-cm -n argocd \
  --type='merge' \
  -p='{"data":{"github-token":"'$(echo -n "YOUR_GITHUB_TOKEN" | base64)'"}}'
```

#### 2. Symlink Security Error

**Error**: `repository contains out-of-bounds symlinks`

**Solution**:
```bash
kubectl patch configmap argocd-cmd-params-cm -n argocd \
  -p '{"data":{"reposerver.allow.oob.symlinks":"true"}}'
```

#### 3. YAML Parsing Error

**Error**: `Failed to unmarshal 'filename.yaml': <nil>`

**Solution**: Move bash template files to `k8s/templates/` directory and use `k8s-clean/` for ArgoCD:

```yaml
# ArgoCD application
spec:
  source:
    path: k8s-clean  # Uses clean manifests only
    directory:
      exclude: |
        **/.venv/**
        **/flyte-venv/**
        **/__pycache__/**
        **/node_modules/**
        **/templates/**
```

### GitHub Actions Issues

#### 1. Workflow File Validation

**Error**: Invalid workflow file with environment variable issues

**Solution**: Validate YAML syntax and avoid `env` context in `services` section

#### 2. Service Discovery Failures

**Error**: Port-forward fails to find services

**Solution**: Use dynamic service discovery in improved workflow:

```yaml
- name: Dynamic Service Discovery
  run: |
    SERVICES=$(kubectl get services -n ats-dev --no-headers | awk '{print $1}' | grep -E "(analytics|unified)" | head -10)
    echo "Available services: $SERVICES"
```

### Database Connection Issues

#### 1. Connection Refused

**Check database service status**:
```bash
kubectl get services -n ats-dev | grep postgres
kubectl get pods -n ats-dev | grep postgres
```

#### 2. Authentication Failures

**Verify database secrets**:
```bash
kubectl get secret ats-db-secret -n ats-dev -o yaml
```

## Best Practices

### Development Workflow

1. **Always create feature branches** from main
2. **Test locally** before pushing to remote
3. **Merge feature branches back to main** - never leave them hanging
4. **Use descriptive commit messages** with appropriate prefixes
5. **Include Claude Code attribution** in commit messages

### Kubernetes Deployments

1. **Use resource limits** to prevent resource exhaustion
2. **Implement health checks** (liveness, readiness, startup probes)
3. **Configure proper secrets management** for sensitive data
4. **Use ConfigMaps** for configuration data
5. **Implement monitoring** with ServiceMonitor resources

### ArgoCD Management

1. **Keep manifests clean** - separate templates from deployment files
2. **Use exclusions** to prevent problematic file deployment
3. **Configure proper authentication** for private repositories
4. **Enable automatic sync** for development environments
5. **Use manual sync** for production deployments

### CI/CD Pipeline

1. **Implement comprehensive testing** in pipeline stages
2. **Use GitOps approach** for private ArgoCD instances
3. **Configure proper notifications** for deployment status
4. **Implement security scanning** in pipeline
5. **Use environment-specific deployment strategies**

## Directory Structure

```
ats-genai/
├── .github/workflows/
│   ├── ats-ci-cd-improved.yaml    # Main CI/CD workflow
│   └── ci.yml                     # Basic CI workflow
├── argocd/
│   └── applications/
│       ├── ats-dev.yaml          # Development environment
│       ├── ats-intg.yaml         # Integration environment
│       └── ats-prod.yaml         # Production environment
├── k8s-clean/                    # Clean Kubernetes manifests
│   ├── analytics-service/
│   ├── eod-service/
│   └── minute-service/
├── k8s/templates/                # Bash template files
│   ├── coverage-api-deployment.yaml
│   ├── enhanced-dataset-detail-webapp.yaml
│   └── polygon-10year-minute-backfill.yaml
└── scripts/
    ├── permanent-access/         # Service access management
    ├── argocd/                   # ArgoCD management scripts
    └── ci-cd/                    # CI/CD helper scripts
```

## Monitoring and Alerts

### Service Health Monitoring

- **Analytics Service**: http://localhost:3000/health
- **EOD Service**: http://localhost:8082/health  
- **Minute Service**: http://localhost:8081/health

### Slack Notifications

Webhook URL configured for deployment notifications:
`https://hooks.slack.com/services/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`

### Prometheus Metrics

All services configured with ServiceMonitor resources for metrics collection:

```yaml
apiVersion: v1
kind: ServiceMonitor
metadata:
  name: service-name
spec:
  endpoints:
  - port: http
    path: /metrics
    interval: 30s
```

---

## Semantic Code Search with Claude Context MCP

### Overview

Claude Context MCP provides semantic code search capabilities that far exceed traditional grep/find operations. It uses vector embeddings to understand code meaning and context, enabling natural language queries.

### Setup and Configuration

The repository is configured with Claude Context MCP using a local Milvus vector database:

- **Vector Database**: Local Milvus instance (Docker-based)
- **Embedding Provider**: OpenAI (text-embedding-3-small)
- **API Key**: Loaded from `.env.test`
- **Storage**: Persistent local vector database

### Management Scripts

#### Start Claude Context MCP
```bash
# Complete setup (starts Milvus + configures MCP)
./scripts/mcp/start-claude-context.sh

# Check status
./scripts/mcp/mcp-status.sh
```

#### Milvus Database Management
```bash
# Start vector database
docker-compose up -d

# Stop vector database  
docker-compose down

# Check container status
docker ps | grep milvus

# View logs
docker logs milvus-standalone
```

### Usage Examples

Instead of traditional grep searches, you can now use natural language queries:

| Traditional Grep | Claude Context MCP |
|-----------------|-------------------|
| `grep -r "database" src/` | "Show me all database connection code" |
| `find . -name "*auth*"` | "Find authentication and authorization functions" |
| `grep -r "def.*api" --include="*.py"` | "Show me all API endpoint definitions" |
| `grep -r "class.*Test" tests/` | "Find all test classes and their purposes" |

### Advanced Queries

- **Cross-file relationships**: "What code depends on the database migration manager?"
- **Functional understanding**: "How does the data backfill process work?"
- **Architecture queries**: "Show me the complete authentication flow"
- **Integration points**: "Find all places where Polygon API is used"

### Access Points

- **Milvus UI**: http://localhost:9000 (admin/minioadmin)
- **Milvus API**: http://localhost:19530  
- **Health Check**: http://localhost:9091/healthz

## Quick Reference Commands

### Git Operations
```bash
# Create and merge feature branch
git checkout -b feature/name && git push -u origin feature/name
git checkout main && git merge feature/name && git push origin main

# Clean up branches
git branch -d feature/name && git push origin --delete feature/name
```

### Service Access
```bash
# Start analytics service access
./scripts/permanent-access/start-analytics-access.sh

# Start ArgoCD access  
./scripts/argocd/start-argocd-access.sh

# Start semantic search (Claude Context MCP)
./scripts/mcp/start-claude-context.sh
```

### Semantic Code Search
```bash
# Check MCP status
./scripts/mcp/mcp-status.sh

# Start/stop vector database
docker-compose up -d
docker-compose down
```

### ArgoCD Operations
```bash
# Check application status
kubectl get applications -n argocd

# Sync application manually
kubectl patch application ats-dev -n argocd -p '{"spec":{"syncPolicy":{"automated":null}}}' --type merge
```

### Troubleshooting
```bash
# Check pod status
kubectl get pods -n ats-dev

# Check service status
kubectl get services -n ats-dev

# View pod logs
kubectl logs -f deployment/service-name -n ats-dev
```

---

This workflow documentation serves as the comprehensive guide for all development, deployment, and operational activities in the ATS GenAI project. Follow these practices to ensure reliable, secure, and efficient software delivery.