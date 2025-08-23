# Unified CI/CD and GitOps Guide

## 🏗️ Overview

This guide consolidates all CI/CD and GitOps deployment processes for the ATS platform, covering GitHub Actions pipelines, ArgoCD GitOps workflows, and automated testing strategies.

## 📊 CI/CD Architecture

### Pipeline Stages

#### 1. **Continuous Integration (CI)**
- **Trigger**: Push to any branch, PR creation
- **Testing**: Unit tests, integration tests, schema validation
- **Quality**: Code coverage, security scanning, linting
- **Build**: Docker images with multi-arch support
- **Publish**: Images to GitHub Container Registry

#### 2. **Continuous Deployment (CD)**
- **GitOps**: Update Kubernetes manifests in GitOps repository
- **ArgoCD**: Automatic synchronization with Kubernetes clusters
- **Environments**: Progressive deployment (dev → staging → prod)
- **Monitoring**: Health checks and rollback on failure

### Test Classification System

#### Unit Tests (Isolated, Fast)
```bash
# Run unit tests only
PYTHONPATH=src pytest tests/unit/ -v --tb=short

# Tests in this category:
# - Pure function tests
# - Mock-based tests
# - Schema validation tests
# - Business logic tests
```

#### Integration Tests (Real Dependencies)
```bash
# Run integration tests only
PYTHONPATH=src pytest tests/integration/ -v --tb=short

# Tests in this category:
# - Database connectivity tests
# - Kubernetes service tests
# - End-to-end API tests
# - Cross-service communication tests
```

#### System Tests (Full Environment)
```bash
# Run system tests (requires deployed services)
PYTHONPATH=src pytest tests/system/ -v --tb=short

# Tests in this category:
# - Complete workflow tests
# - Performance tests
# - Load tests
# - Deployment verification tests
```

## 🚀 GitOps Workflow (Option 2)

### Development Workflow

#### 1. **Make Code Changes**
```bash
# Follow TDD workflow
git checkout -b PGPT-1234/feature-name
# Write tests, implement feature
git commit -m "feat: description"
git push origin PGPT-1234/feature-name
```

#### 2. **Automated CI Pipeline**
```yaml
# GitHub Actions automatically triggers:
name: CI Pipeline
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - name: Unit Tests
        run: PYTHONPATH=src pytest tests/unit/ -v
      - name: Integration Tests  
        run: PYTHONPATH=src pytest tests/integration/ -v
      - name: Schema Validation
        run: python scripts/validate_schema.py --check-all
      - name: Security Scan
        run: safety check && bandit -r src/
```

#### 3. **Automated Deployment**
```bash
# After merge to main, GitHub Actions:
# 1. Builds Docker images
# 2. Pushes to registry
# 3. Updates GitOps manifests
# 4. Triggers ArgoCD sync
```

#### 4. **ArgoCD Synchronization**
```bash
# ArgoCD automatically:
# 1. Detects manifest changes
# 2. Applies changes to Kubernetes
# 3. Performs rolling updates
# 4. Monitors deployment health
```

### Manual GitOps Operations

#### Deploy Changes
```bash
# Deploy using Option 2 workflow scripts
./scripts/dev_deploy.sh

# Monitor deployment progress
./scripts/monitor_deployment.sh your-service
```

#### Force ArgoCD Sync
```bash
# Force immediate synchronization
./scripts/force_argocd_sync.sh --force

# Check ArgoCD application status
kubectl get applications -n argocd
```

#### Rollback Deployments
```bash
# Multiple rollback strategies available
./scripts/rollback_deployment.sh your-service git      # Git revert + sync
./scripts/rollback_deployment.sh your-service k8s     # Kubernetes rollback
./scripts/rollback_deployment.sh your-service argocd  # ArgoCD rollback
```

## 🔧 Setup and Configuration

### Prerequisites
- Kubernetes cluster with kubectl access
- GitHub repository with admin permissions
- Docker registry access (GitHub Container Registry)
- ArgoCD installed in cluster

### Quick Setup
```bash
# One-command comprehensive setup
./scripts/setup_cicd_gitops.sh

# Or install ArgoCD only
./scripts/setup_cicd_gitops.sh --argocd-only

# Or configure secrets only  
./scripts/setup_cicd_gitops.sh --secrets-only
```

### Manual ArgoCD Installation

#### 1. Install ArgoCD
```bash
# Create ArgoCD namespace
kubectl create namespace argocd

# Install ArgoCD
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/v2.8.4/manifests/install.yaml

# Wait for deployment
kubectl wait --for=condition=available --timeout=600s deployment/argocd-server -n argocd
```

#### 2. Configure ArgoCD Access
```bash
# Get initial admin password
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d

# Port forward to access UI
kubectl port-forward svc/argocd-server -n argocd 8080:443

# Access UI at https://localhost:8080
# Username: admin, Password: [from above command]
```

#### 3. Create ArgoCD Application
```yaml
# k8s/argocd/ats-dev-application.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ats-dev
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/your-org/ats-genai-data
    targetRevision: main
    path: k8s
  destination:
    server: https://kubernetes.default.svc
    namespace: ats-dev
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
    - CreateNamespace=true
```

### GitHub Secrets Configuration

Required secrets in GitHub repository settings:

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `ARGOCD_SERVER` | ArgoCD server URL | `argocd.your-domain.com` |
| `ARGOCD_USERNAME` | ArgoCD username | `admin` |
| `ARGOCD_PASSWORD` | ArgoCD password | `[generated-password]` |
| `KUBE_CONFIG_ATS_DEV` | Base64 encoded kubeconfig | `[base64-kubeconfig]` |
| `GITOPS_TOKEN` | GitHub token for GitOps repo | `ghp_[token]` |
| `SLACK_WEBHOOK_URL` | Slack notifications (optional) | `https://hooks.slack.com/...` |

### Environment Configuration

#### Development Environment
- **Namespace**: `ats-dev`
- **Deployment**: Automatic on push to `main`
- **Testing**: Full integration test suite
- **Access**: Port-forwarding and NodePort services

#### Staging Environment  
- **Namespace**: `ats-staging`
- **Deployment**: Automatic after successful dev deployment
- **Testing**: System and performance tests
- **Access**: LoadBalancer services

#### Production Environment
- **Namespace**: `ats-prod`
- **Deployment**: Manual approval required
- **Testing**: Smoke tests and health checks
- **Access**: Ingress with SSL/TLS

## 🚦 GitHub Actions Workflows

### Core CI/CD Pipeline

```yaml
# .github/workflows/ci-cd.yml
name: CI/CD Pipeline
on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.11, 3.12]
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ matrix.python-version }}
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -r requirements-dev.txt
      
      - name: Schema Validation
        run: python scripts/validate_schema.py --check-all
      
      - name: Unit Tests
        run: PYTHONPATH=src pytest tests/unit/ -v --tb=short --cov
      
      - name: Integration Tests
        run: PYTHONPATH=src pytest tests/integration/ -v --tb=short
      
      - name: Security Scan
        run: |
          safety check
          bandit -r src/ -f json
      
      - name: Code Quality
        run: |
          flake8 src/ tests/
          black --check src/ tests/
          mypy src/

  build-and-deploy:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4
      
      - name: Build Docker Images
        run: |
          docker build -t ghcr.io/${{ github.repository }}/ats-api:${{ github.sha }} .
          docker build -t ghcr.io/${{ github.repository }}/ats-worker:${{ github.sha }} -f Dockerfile.worker .
      
      - name: Push to Registry
        run: |
          echo ${{ secrets.GITHUB_TOKEN }} | docker login ghcr.io -u ${{ github.actor }} --password-stdin
          docker push ghcr.io/${{ github.repository }}/ats-api:${{ github.sha }}
          docker push ghcr.io/${{ github.repository }}/ats-worker:${{ github.sha }}
      
      - name: Update Manifests
        run: |
          # Update image tags in Kubernetes manifests
          sed -i "s|image: ghcr.io/${{ github.repository }}/ats-api:.*|image: ghcr.io/${{ github.repository }}/ats-api:${{ github.sha }}|" k8s/deployments/*.yaml
      
      - name: Trigger ArgoCD Sync
        run: |
          ./scripts/force_argocd_sync.sh --force
        env:
          ARGOCD_SERVER: ${{ secrets.ARGOCD_SERVER }}
          ARGOCD_USERNAME: ${{ secrets.ARGOCD_USERNAME }}
          ARGOCD_PASSWORD: ${{ secrets.ARGOCD_PASSWORD }}
```

### Pull Request Validation

```yaml
# .github/workflows/pr-validation.yml
name: PR Validation
on:
  pull_request:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Validate YAML
        run: |
          find k8s/ -name "*.yaml" -exec yamllint {} \;
      
      - name: Detect Conflicts
        run: python scripts/detect_k8s_conflicts.py k8s/
      
      - name: Schema Anti-patterns
        run: python scripts/check_schema_antipatterns.py src/
      
      - name: Security Review
        run: |
          # Check for secrets in code
          git diff --name-only origin/main | xargs grep -l "password\|secret\|key" || true
```

### Deployment Verification

```yaml
# .github/workflows/deployment-verification.yml
name: Deployment Verification
on:
  deployment_status

jobs:
  verify:
    runs-on: ubuntu-latest
    if: github.event.deployment_status.state == 'success'
    steps:
      - name: Health Check
        run: |
          # Wait for services to be ready
          kubectl wait --for=condition=available --timeout=300s deployment/ats-api -n ats-dev
      
      - name: Smoke Tests
        run: |
          # Test critical endpoints
          curl -f http://NODE_IP:30080/health
          curl -f http://NODE_IP:30081/metrics
      
      - name: Integration Verification
        run: PYTHONPATH=src pytest tests/system/test_deployment_verification.py -v
```

## 📊 Monitoring and Observability

### Deployment Monitoring

```bash
# Comprehensive deployment status
./scripts/deployment_status.sh

# Real-time monitoring
./scripts/monitor_deployment.sh your-service

# ArgoCD application health
kubectl get applications -n argocd -o wide
```

### Metrics and Alerts

#### ArgoCD Metrics
- Application sync status
- Deployment success/failure rates
- Sync duration and frequency
- Resource drift detection

#### CI/CD Metrics
- Pipeline success rates
- Build and test duration
- Deployment frequency
- Lead time for changes

### Logging

```bash
# CI/CD pipeline logs
gh run list --limit 10
gh run view <run-id> --log

# ArgoCD logs
kubectl logs -n argocd -l app.kubernetes.io/name=argocd-server

# Application logs
kubectl logs -f deployment/your-service -n ats-dev
```

## 🚨 Troubleshooting

### Common CI/CD Issues

#### Pipeline Failures
```bash
# Check pipeline status
gh run list --limit 5

# View specific run logs
gh run view <run-id> --log

# Re-run failed jobs
gh run rerun <run-id>
```

#### ArgoCD Sync Issues
```bash
# Check application status
kubectl get application ats-dev -n argocd -o yaml

# Force refresh and sync
./scripts/force_argocd_sync.sh --force

# View ArgoCD events
kubectl get events -n argocd
```

#### Deployment Problems
```bash
# Check deployment rollout
kubectl rollout status deployment/your-service -n ats-dev

# View pod events
kubectl describe pod <pod-name> -n ats-dev

# Rollback if needed
./scripts/rollback_deployment.sh your-service k8s
```

### Recovery Procedures

#### Pipeline Recovery
1. **Identify the failure point** from GitHub Actions logs
2. **Fix the underlying issue** (tests, build, deployment)
3. **Re-run the pipeline** or push a fix
4. **Monitor subsequent runs** for stability

#### ArgoCD Recovery
1. **Check ArgoCD server health** and connectivity
2. **Force application refresh** to detect changes
3. **Manually sync applications** if automatic sync fails
4. **Review and fix** any manifest or configuration issues

#### Deployment Recovery
1. **Use rollback scripts** for quick recovery
2. **Check service health** and external access
3. **Monitor logs** for error patterns
4. **Coordinate with team** for shared service issues

## 📋 Best Practices

### CI/CD Pipeline
- **Fast feedback loops** - Keep pipelines under 10 minutes
- **Parallel execution** - Run tests and builds concurrently
- **Fail fast** - Exit early on critical failures
- **Clear error messages** - Provide actionable failure information

### GitOps
- **Single source of truth** - All configuration in Git
- **Atomic changes** - Deploy related changes together
- **Automated rollbacks** - Implement health-based rollbacks
- **Security scanning** - Scan images and manifests

### Testing Strategy
- **Test pyramid** - More unit tests, fewer system tests
- **Test isolation** - Independent, repeatable tests
- **Production parity** - Test environments match production
- **Performance testing** - Include performance regression tests

---

**This unified CI/CD guide ensures reliable, automated, and secure deployment processes across all ATS platform environments.**