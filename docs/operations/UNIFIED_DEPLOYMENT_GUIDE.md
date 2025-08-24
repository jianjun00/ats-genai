# Unified Deployment Guide

## 🚀 Overview

This guide consolidates all deployment approaches for the ATS platform across development, staging, and production environments. It covers both automated CI/CD deployments and manual deployment procedures.

## 📋 Prerequisites

### Required Tools
- `kubectl` (Kubernetes CLI)
- `kustomize` (or kubectl with kustomize support)
- `docker` (for local builds)
- `uv` (Python package manager)
- `git` (version control)
- `argocd` (for GitOps deployments)

### Access Requirements
- Kubernetes cluster access (kubeconfig configured)
- Container registry push access (GitHub Container Registry)
- GitHub repository access with Actions enabled
- ArgoCD access for GitOps workflows

## 🏗️ Deployment Strategies

### 1. GitOps Deployment (Option 2 - Recommended)

**Zero-downtime deployments with ArgoCD synchronization.**

#### Quick Deployment
```bash
# Deploy changes using Option 2 workflow
./scripts/dev_deploy.sh

# Monitor deployment progress
./scripts/monitor_deployment.sh your-service-name

# Verify deployment status
./scripts/deployment_status.sh
```

#### Safety Checks
```bash
# Run pre-deployment checks
./scripts/pre_deploy_check.sh

# Validate deployment files
./scripts/validate_deployment.sh k8s/**/*.yaml
```

#### Rollback Options
```bash
# Fast Kubernetes rollback (~30 seconds)
./scripts/rollback_deployment.sh your-service k8s

# Safe Git revert + ArgoCD sync (~2 minutes)
./scripts/rollback_deployment.sh your-service git

# ArgoCD rollback
./scripts/rollback_deployment.sh your-service argocd
```

### 2. Manual Kubernetes Deployment

**Direct kubectl deployment for immediate changes.**

#### Development Environment
```bash
# Apply development configuration
kubectl apply -k k8s/environments/dev

# Verify deployment
kubectl get pods -n ats-dev
kubectl logs -f deployment/your-service -n ats-dev

# Check service health
kubectl port-forward service/your-service 8080:80 -n ats-dev
```

#### Integration Environment
```bash
# Apply integration configuration
kubectl apply -k k8s/environments/intg

# Verify deployment
kubectl get pods -n ats-intg
kubectl describe deployment your-service -n ats-intg
```

#### Production Environment
```bash
# Apply production configuration (with caution)
kubectl apply -k k8s/environments/prod

# Verify deployment with zero downtime
kubectl rollout status deployment/your-service -n ats-prod

# Monitor logs
kubectl logs -f deployment/your-service -n ats-prod --tail=100
```

### 3. CI/CD Automated Deployment

**Automated deployments triggered by git events.**

#### GitHub Actions Workflow
- **Development**: Automatic deployment on push to `main` branch
- **Integration**: Automatic deployment on successful dev deployment
- **Production**: Manual approval required, triggered by release tags

#### Pipeline Stages
1. **Build**: Docker image creation and push to registry
2. **Test**: Unit and integration tests
3. **Security**: Security scanning and vulnerability assessment
4. **Deploy**: Environment-specific deployment
5. **Verify**: Health checks and smoke tests

## 🌐 Access Management

### Permanent Access Solution

The platform provides multiple access methods:

#### 1. Local Port-Forwarding (Development)
```bash
# Start permanent access for all services
./scripts/setup/start-permanent-access.sh

# Stop all services
./scripts/setup/stop-permanent-access.sh

# External analytics access
./scripts/setup/start-analytics-external.sh
```

#### 2. NodePort Services (Direct Node Access)
```bash
# Apply NodePort services
kubectl apply -f k8s/permanent-access/nodeport-services.yaml

# Get node IP and access services
kubectl get nodes -o wide
# Access: http://NODE_IP:30081
```

#### 3. LoadBalancer Services (Cloud Native)
```bash
# Apply LoadBalancer services
kubectl apply -f k8s/permanent-access/loadbalancer-services.yaml

# Get external IPs
kubectl get services -n ats-dev
```

#### 4. Ingress Controllers (Production)
```bash
# Apply ingress configuration
kubectl apply -f k8s/permanent-access/ingress-controllers.yaml

# Access via domain names with SSL
# https://ats-analytics.domain.com
```

### Service Access Matrix

| Service | Development | Integration | Production |
|---------|-------------|-------------|------------|
| **ATS Minute Service** | http://localhost:8081 | http://intg.domain.com/minute | https://ats-minute.domain.com |
| **ATS EOD Service** | http://localhost:8082 | http://intg.domain.com/eod | https://ats-eod.domain.com |
| **ATS Analytics** | http://localhost:8080 | http://intg.domain.com/analytics | https://ats-analytics.domain.com |
| **Enhanced Analytics** | http://0.0.0.0:3000 | http://intg.domain.com:30003 | https://analytics.domain.com |
| **Prometheus** | http://localhost:9090 | http://intg.domain.com:30190 | https://prometheus.domain.com |
| **Grafana** | http://localhost:3001 | http://intg.domain.com:30330 | https://grafana.domain.com |

## 🔐 Security and Secrets Management

### Environment Secrets Setup

#### 1. Prepare Secrets
```bash
# Generate base64 encoded secrets
echo -n "postgresql://user:pass@timescaledb:5432/ats_prod" | base64
echo -n "your-tiingo-api-key" | base64
echo -n "your-polygon-api-key" | base64
```

#### 2. Update Secret Files
- `k8s/environments/dev/secrets.yaml`
- `k8s/environments/intg/secrets.yaml`
- `k8s/environments/prod/secrets.yaml`

#### 3. Configure GitHub Secrets
```
GITHUB_TOKEN          # Automatically provided
KUBE_CONFIG_DEV       # Base64 encoded kubeconfig for dev cluster
KUBE_CONFIG_INTG      # Base64 encoded kubeconfig for intg cluster  
KUBE_CONFIG_PROD      # Base64 encoded kubeconfig for prod cluster
```

### Environment Protection Rules

#### Integration Environment
- No protection rules (auto-deploy)
- Reviewers: Optional

#### Production Environment
- Required reviewers: 2+ team members
- Wait timer: 5 minutes
- Restrict to main branch only

## 📊 Monitoring and Health Checks

### Deployment Health Verification

```bash
# Comprehensive deployment status
./scripts/deployment_status.sh

# Service-specific monitoring
./scripts/monitor_deployment.sh your-service

# External access points
./scripts/get_external_access.sh all

# ArgoCD sync status
./scripts/force_argocd_sync.sh
```

### Health Check Endpoints

All services provide health endpoints:
- `/health` - Basic health status
- `/ready` - Readiness probe endpoint
- `/metrics` - Prometheus metrics (where applicable)

### Log Monitoring

```bash
# Service logs
kubectl logs -f deployment/your-service -n ats-dev

# Multiple services
kubectl logs -f -l app=your-app -n ats-dev

# Recent events
kubectl get events -n ats-dev --sort-by='.lastTimestamp'
```

## 🚨 Troubleshooting

### Common Deployment Issues

#### ArgoCD Sync Problems
```bash
# Check ArgoCD application status
kubectl get applications -n argocd

# Force hard refresh and sync
./scripts/force_argocd_sync.sh --force

# View ArgoCD logs
kubectl logs -n argocd -l app.kubernetes.io/name=argocd-server
```

#### Pod Startup Issues
```bash
# Check pod status and events
kubectl describe pod pod-name -n ats-dev

# Check resource constraints
kubectl top pods -n ats-dev

# View container logs
kubectl logs pod-name -c container-name -n ats-dev
```

#### Network Connectivity Issues
```bash
# Test service connectivity
kubectl exec -it pod-name -n ats-dev -- curl service-name:port

# Check service endpoints
kubectl get endpoints service-name -n ats-dev

# Verify DNS resolution
kubectl exec -it pod-name -n ats-dev -- nslookup service-name
```

### Recovery Procedures

#### Rollback Deployment
```bash
# Quick Kubernetes rollback
kubectl rollout undo deployment/your-service -n ats-dev

# Using rollback script with multiple strategies
./scripts/rollback_deployment.sh your-service

# Git-based rollback
git revert HEAD && git push
./scripts/force_argocd_sync.sh --force
```

#### Database Recovery
```bash
# Check database connectivity
run_dev query "SELECT version()"

# Restore from backup (if available)
kubectl exec -n ats-dev postgres -- pg_restore backup.sql

# Reset to known good state
kubectl delete pod postgres -n ats-dev  # Pod will restart
```

## 📚 Best Practices

### Deployment Safety
1. **Always use feature branches** for changes
2. **Run pre-deployment checks** before applying changes
3. **Monitor deployments** during rollout
4. **Test external access** after deployment
5. **Keep rollback procedures ready** for quick recovery

### GitOps Workflow
1. **Use Option 2 workflow** for zero-downtime deployments
2. **Coordinate with team** during shared service changes
3. **Document significant changes** in commit messages
4. **Monitor ArgoCD sync status** after changes

### Security Guidelines
1. **Never commit secrets** to repository
2. **Use environment-specific secret files**
3. **Implement proper RBAC** for cluster access
4. **Regular security scans** of container images
5. **Monitor access logs** for unusual activity

### Performance Optimization
1. **Resource requests and limits** on all containers
2. **Horizontal Pod Autoscaling** for variable loads
3. **Persistent Volume Claims** for data persistence
4. **Network policies** for micro-segmentation
5. **Regular performance testing** of deployments

## 🔄 Maintenance

### Regular Tasks
- **Weekly**: Review and update container images
- **Monthly**: Security patch updates and vulnerability scans
- **Quarterly**: Capacity planning and resource optimization
- **As needed**: Backup verification and disaster recovery testing

### Documentation Updates
- **Update this guide** when deployment procedures change
- **Document new services** in the access matrix
- **Maintain troubleshooting procedures** based on real incidents
- **Review and update security procedures** regularly

---

**This unified guide ensures consistent, reliable deployments across all environments while maintaining security, monitoring, and recovery capabilities.**