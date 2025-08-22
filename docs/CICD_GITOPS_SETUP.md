# CI/CD and GitOps Setup for ATS Platform

This document describes the comprehensive CI/CD pipeline and GitOps deployment setup for the ATS real-time market data platform.

## 🏗️ Architecture Overview

### CI/CD Pipeline (GitHub Actions)
- **Trigger**: Push to `main`, `develop`, or feature branches
- **Testing**: Unit tests, integration tests, security scans
- **Build**: Docker images with multi-arch support
- **Publish**: Images to GitHub Container Registry
- **Deploy**: Update GitOps manifests, trigger Argo CD sync

### GitOps Deployment (Argo CD)
- **Source**: GitOps repository with Kubernetes manifests
- **Target**: Kubernetes clusters (dev, staging, prod)
- **Sync**: Automatic synchronization of desired state
- **Rollback**: Automated rollback on deployment failures

## 🚀 Quick Setup

### Prerequisites
- Kubernetes cluster with kubectl access
- GitHub repository with admin permissions
- Docker registry access (GitHub Container Registry)
- GitHub CLI (optional but recommended)

### One-Command Setup
```bash
# Run the comprehensive setup script
./scripts/setup_cicd_gitops.sh

# Or install Argo CD only
./scripts/setup_cicd_gitops.sh --argocd-only

# Or configure GitHub secrets only  
./scripts/setup_cicd_gitops.sh --secrets-only
```

## 📋 Manual Setup Steps

### 1. Install Argo CD

```bash
# Create Argo CD namespace
kubectl create namespace argocd

# Install Argo CD
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/v2.8.4/manifests/install.yaml

# Wait for deployment
kubectl wait --for=condition=available --timeout=600s deployment/argocd-server -n argocd
```

### 2. Configure Argo CD Access

```bash
# Get initial admin password
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d

# Port forward to access UI
kubectl port-forward svc/argocd-server -n argocd 8080:443

# Access UI at https://localhost:8080
# Username: admin
# Password: [from above command]
```

### 3. Setup GitOps Repository

Create a separate GitOps repository with the following structure:
```
ats-gitops/
├── applications/           # Argo CD Application manifests
│   └── ats-dev-app.yaml
├── environments/           # Environment-specific configurations
│   ├── dev/
│   ├── staging/
│   └── prod/
└── base/                   # Shared base configurations
    ├── rbac/
    └── network-policies/
```

### 4. Configure GitHub Secrets

Add the following secrets to your GitHub repository:

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `ARGOCD_SERVER` | Argo CD server URL | `argocd.your-domain.com` |
| `ARGOCD_USERNAME` | Argo CD username | `admin` |
| `ARGOCD_PASSWORD` | Argo CD password | `[generated-password]` |
| `KUBE_CONFIG_ATS_DEV` | Base64 encoded kubeconfig | `[base64-kubeconfig]` |
| `GITOPS_TOKEN` | GitHub token for GitOps repo | `ghp_[token]` |
| `SLACK_WEBHOOK_URL` | Slack notifications (optional) | `https://hooks.slack.com/...` |

### 5. Deploy ATS Application

```bash
# Apply application manifest
kubectl apply -f argocd/applications/ats-dev-app.yaml -n argocd

# Sync application
argocd app sync ats-dev --timeout 600
argocd app wait ats-dev --timeout 600 --health
```

## 🔄 CI/CD Workflow

### Triggered Events
- **Push to main/develop**: Full pipeline with deployment
- **Pull Request**: Testing and validation only  
- **Manual trigger**: Full pipeline with options

### Pipeline Stages

#### 1. **Unit Tests** (Required for all branches)
```yaml
- Fast unit tests (< 10 minutes)
- Core functionality validation
- Immediate feedback on code quality
```

#### 2. **Integration Tests** (Deployment branches only)
```yaml
- Database integration tests
- Real-time system end-to-end tests
- Multi-vendor API integration
- Performance benchmarks
```

#### 3. **Security & Quality Checks**
```yaml
- Bandit security scanning
- Safety dependency vulnerability checks  
- Ruff code quality analysis
- License compliance verification
```

#### 4. **Build & Push Images**
```yaml
- Multi-architecture Docker builds (amd64, arm64)
- Optimized multi-stage builds
- Automatic image tagging and versioning
- Push to GitHub Container Registry
```

#### 5. **Update GitOps Manifests**
```yaml
- Clone GitOps repository
- Update image tags with new versions
- Commit and push changes
- Trigger Argo CD sync
```

#### 6. **Deploy to Environment**
```yaml
- Argo CD automatic synchronization
- Health checks and validation
- Rollback on deployment failures
- Notification on completion
```

## 🎯 Deployment Environments

### Development (ats-dev)
- **Trigger**: Push to `main`, `develop`, feature branches
- **Auto-sync**: Enabled with pruning
- **Resources**: Lower limits for cost optimization
- **Configuration**: Debug mode, verbose logging

### Staging (ats-staging)
- **Trigger**: Manual promotion from dev
- **Auto-sync**: Enabled without pruning
- **Resources**: Production-like sizing
- **Configuration**: Production settings with staging data

### Production (ats-prod)
- **Trigger**: Manual promotion with approvals
- **Auto-sync**: Disabled (manual sync required)
- **Resources**: Full production sizing
- **Configuration**: Optimized for performance and reliability

## 📊 Monitoring and Observability

### Argo CD Monitoring
- Application health status
- Sync status and history
- Resource utilization
- Deployment metrics

### Application Monitoring
- Prometheus metrics collection
- Grafana dashboards
- Custom alerting rules
- Performance benchmarks

### Notifications
- Slack integration for deployment status
- Email alerts for critical failures
- GitHub commit status updates
- Webhook notifications for external systems

## 🔧 Configuration Management

### Environment-Specific Configurations

**Development:**
```yaml
# kustomization.yaml
configMapGenerator:
  - name: ats-dev-config
    literals:
      - ENVIRONMENT=dev
      - LOG_LEVEL=DEBUG
      - UNIVERSE_SIZE=100

patches:
  - patch: |-
      - op: replace
        path: /spec/replicas
        value: 1
    target:
      kind: Deployment
      name: realtime-streaming-collector
```

**Production:**
```yaml
# kustomization.yaml  
configMapGenerator:
  - name: ats-prod-config
    literals:
      - ENVIRONMENT=prod
      - LOG_LEVEL=INFO
      - UNIVERSE_SIZE=5000

patches:
  - patch: |-
      - op: replace
        path: /spec/replicas
        value: 3
    target:
      kind: Deployment
      name: realtime-streaming-collector
```

### Secret Management
- Kubernetes Secrets for sensitive data
- External Secrets Operator integration (optional)
- Sealed Secrets for GitOps security
- HashiCorp Vault integration (enterprise)

## 🛡️ Security Considerations

### Image Security
- Multi-stage builds with minimal base images
- Non-root user execution
- Security scanning with Trivy
- Signed container images (optional)

### Kubernetes Security
- RBAC with least privilege
- Network policies for traffic isolation
- Pod Security Standards enforcement
- Resource limits and quotas

### GitOps Security
- Separate GitOps repository with restricted access
- Signed commits and protected branches
- Audit logging for all changes
- Automated security policy enforcement

## 🚨 Troubleshooting

### Common Issues

#### 1. Argo CD Sync Failures
```bash
# Check application status
argocd app get ats-dev

# View sync logs
argocd app logs ats-dev

# Manual sync with force
argocd app sync ats-dev --force
```

#### 2. Image Pull Failures
```bash
# Check image exists
docker pull ghcr.io/your-org/ats-platform:sha-abc123

# Verify registry credentials
kubectl get secret -n ats-dev

# Check pod events
kubectl describe pod -n ats-dev -l app=realtime-streaming-collector
```

#### 3. Resource Allocation Issues
```bash
# Check resource usage
kubectl top pods -n ats-dev

# Review resource requests/limits
kubectl describe deployment realtime-streaming-collector -n ats-dev

# Check cluster capacity
kubectl describe nodes
```

### Recovery Procedures

#### Rollback Deployment
```bash
# Rollback to previous version
argocd app rollback ats-dev

# Or rollback to specific revision
argocd app rollback ats-dev --revision 5
```

#### Emergency Procedures
```bash
# Scale down deployment
kubectl scale deployment realtime-streaming-collector --replicas=0 -n ats-dev

# Check application logs
kubectl logs -f deployment/realtime-streaming-collector -n ats-dev

# Restart deployment
kubectl rollout restart deployment/realtime-streaming-collector -n ats-dev
```

## 📈 Best Practices

### GitOps Best Practices
1. **Single Source of Truth**: All configuration in Git
2. **Declarative Manifests**: Use Kustomize for environment differences
3. **Automated Sync**: Enable auto-sync for development environments
4. **Manual Approval**: Require manual sync for production
5. **Audit Trail**: Maintain detailed commit messages and PR reviews

### CI/CD Best Practices
1. **Fast Feedback**: Keep unit tests under 10 minutes
2. **Fail Fast**: Stop pipeline on first failure
3. **Parallel Execution**: Run independent jobs concurrently
4. **Artifact Caching**: Cache dependencies between runs
5. **Security First**: Security checks in every pipeline run

### Deployment Best Practices
1. **Blue-Green Deployments**: Zero-downtime deployments
2. **Canary Releases**: Gradual rollout for risk mitigation
3. **Health Checks**: Comprehensive liveness and readiness probes
4. **Resource Monitoring**: Continuous monitoring of resource usage
5. **Backup Strategy**: Regular backups of critical data

## 🔗 Related Documentation

- [Real-time Data Collection System](./realtime_data_collection_prd.md)
- [Kubernetes Deployment Guide](./KUBERNETES_GUIDE.md)
- [Monitoring and Alerting](./MONITORING.md)
- [Security Guidelines](./SECURITY.md)
- [Development Workflow](./DEVELOPMENT_WORKFLOW.md)

## 📞 Support and Contacts

- **Platform Team**: platform-team@company.com
- **DevOps Team**: devops@company.com
- **Security Team**: security@company.com
- **On-call Support**: oncall@company.com

---

**Note**: This setup provides enterprise-grade CI/CD and GitOps capabilities for the ATS platform. Regular reviews and updates of this configuration are recommended to maintain security and performance standards.