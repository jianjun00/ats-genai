# 🚢 ATS Deployment Guide

**Complete deployment strategies, environments, monitoring, and troubleshooting for the ATS platform.**

---

## 🏗️ Deployment Strategies

### 🚀 GitOps Deployment (RECOMMENDED)
**Zero-downtime deployments with ArgoCD synchronization**

#### Quick Deployment
```bash
# Deploy changes using GitOps workflow  
./scripts/dev_deploy.sh

# Monitor deployment progress
./scripts/monitor_deployment.sh your-service-name

# Verify deployment status
./scripts/deployment_status.sh
```

#### Safety Checks (MANDATORY)
```bash
# ALWAYS run pre-deployment checks
./scripts/pre_deploy_check.sh

# Validate deployment files
./scripts/validate_deployment.sh k8s/**/*.yaml

# Check for resource conflicts
python scripts/detect_k8s_conflicts.py k8s/
```

#### Rollback Options
```bash
# Fast Kubernetes rollback (~30 seconds)
./scripts/rollback_deployment.sh service-name k8s

# Safe Git revert + ArgoCD sync (~2 minutes)  
./scripts/rollback_deployment.sh service-name git

# ArgoCD application rollback
./scripts/rollback_deployment.sh service-name argocd
```

### 📋 Manual Kubernetes Deployment
**Direct kubectl for immediate changes**

#### Development Environment
```bash
# Apply dev configuration
kubectl apply -k k8s/environments/dev

# Verify deployment
kubectl get pods -n ats-dev
kubectl logs -f deployment/service-name -n ats-dev

# Test service health
kubectl port-forward service/service-name 8080:80 -n ats-dev
curl http://localhost:8080/health
```

#### Integration Environment  
```bash
# Apply integration configuration
kubectl apply -k k8s/environments/intg

# Check deployment status
kubectl rollout status deployment/service-name -n ats-intg
```

#### Production Environment
```bash
# Production requires approval workflow
# 1. Get approval from team lead
# 2. Schedule maintenance window
# 3. Apply with monitoring

kubectl apply -k k8s/environments/prod
kubectl rollout status deployment/service-name -n ats-prod

# Monitor closely during production deployment
watch kubectl get pods -n ats-prod
```

---

## 🌍 Environment Configuration

### Three-Tier Architecture

| Environment | Purpose | Update Frequency | Auto-Sync | Branch |
|-------------|---------|------------------|-----------|--------|
| **dev** | Development & Testing | Continuous | ✅ Yes | `main` |
| **intg** | Weekly Integration Testing | Weekly | ✅ Yes | `develop` |  
| **prod** | Live Customer System | Monthly | ❌ Manual | `main` |

### Database Environments

#### Development Database (ats-dev)
```bash
# Connection Details:
# Host: localhost
# Port: 5432 (Docker container)
# User: postgres
# Password: dev_password  
# Database: dev_db
# Tables: dev_* prefixed (e.g., dev_daily_prices, dev_instruments)

# Start Database:
python scripts/run_dev.py start --service postgres

# Database Operations:
python scripts/run_dev.py query --query "SELECT version()"
PGPASSWORD=dev_password psql -h localhost -p 5432 -U postgres -d dev_db
```

#### Integration Database (ats-intg)
```bash
# Connection Details:
# Host: localhost
# Port: 5433 (Docker container)
# User: postgres
# Password: intg_password  
# Database: intg_db
# Tables: intg_* prefixed (e.g., intg_daily_prices, intg_instruments)

# Start Database:
python scripts/run_intg.py start --service postgres

# Database Operations:
python scripts/run_intg.py query --query "SELECT version()"
PGPASSWORD=intg_password psql -h localhost -p 5433 -U postgres -d intg_db
```

| Environment | Tables | Purpose | Access Method |
|-------------|--------|---------|---------------|
| **dev** | `dev_*` | Development | Docker PostgreSQL |
| **intg** | `intg_*` | Integration testing | Docker PostgreSQL |
| **prod** | `prod_*` | Production data | Docker PostgreSQL |

### Access Configuration

#### Development Environment
```bash
# Database access (ats-dev)
Host: localhost
Port: 5432
Database: dev_db
User: postgres
Password: dev_password

# Start development environment
python scripts/run_dev.py start --service postgres

# Integration environment
Host: localhost
Port: 5433
Database: intg_db
User: postgres
Password: intg_password

# External access
External IP: 192.168.49.2
NodePort range: 30000-32767
```

#### External Access Testing
```bash
# Get external IP and port (NOT localhost)
kubectl get nodes -o wide
kubectl get service service-name -n ats-dev

# Test actual external URL
curl -s "http://NODE_IP:NODE_PORT/health" | jq

# Get comprehensive access info
./scripts/get_external_access.sh all
./scripts/get_external_access.sh service-name
```

---

## 🔄 Deployment Workflows

### GitOps Option 2: Direct Service Replacement

#### Phase 1: Preparation
```bash
# 1. Start from clean main
git checkout main && git pull origin main

# 2. Run safety checks
./scripts/pre_deploy_check.sh

# 3. Create feature branch
git checkout -b feature/deployment-update
```

#### Phase 2: Make Changes
```bash
# 4. Update deployment files
vim k8s/service/deployment.yaml
vim scripts/k8s-extracted/app.py

# 5. Validate changes locally
./scripts/validate_deployment.sh k8s/service/deployment.yaml
python -m pytest scripts/k8s-extracted/ -v
```

#### Phase 3: Deploy and Test
```bash
# 6. Deploy to ats-dev
./scripts/dev_deploy.sh

# 7. Monitor deployment
./scripts/monitor_deployment.sh service-name

# 8. Test changes
curl http://$(./scripts/get_external_access.sh service-name)/endpoint
```

#### Phase 4: Production Ready
```bash
# 9. Final testing
PYTHONPATH=src pytest tests/system/ -v
./scripts/validate_deployment.sh k8s/**/*.yaml

# 10. Merge to main
git add . && git commit -m "feat: deployment update"
gh pr create --title "feat: deployment update"
gh pr merge --squash
```

### Environment Promotion

#### Dev → Integration
```bash
# Weekly promotion (Mondays 9:00 AM UTC)
# Automated via ArgoCD sync to develop branch

# Manual promotion if needed
kubectl apply -k k8s/environments/intg
./scripts/monitor_deployment.sh service-name
```

#### Integration → Production
```bash
# Monthly promotion (requires approval)
# 1. Schedule maintenance window
# 2. Get stakeholder approval
# 3. Execute with monitoring

./scripts/pre_deploy_check.sh --environment prod
kubectl apply -k k8s/environments/prod --dry-run=server
kubectl apply -k k8s/environments/prod
./scripts/monitor_deployment.sh service-name
```

---

## 📊 Monitoring and Verification

### Deployment Monitoring
```bash
# Real-time deployment monitoring
./scripts/monitor_deployment.sh service-name

# Comprehensive system status
./scripts/deployment_status.sh

# Check service health
kubectl get pods -n ats-dev -l app=service-name
kubectl describe deployment service-name -n ats-dev
```

### Health Checks
```bash
# API health check
curl -s http://external-ip:port/health | jq

# Database connectivity
run_dev query "SELECT 1"

# Service logs
kubectl logs -f deployment/service-name -n ats-dev --tail=100
```

### Performance Verification
```bash
# Load testing (after deployment)
PYTHONPATH=src pytest tests/performance/ -v

# Resource utilization
kubectl top pods -n ats-dev
kubectl top nodes
```

---

## 🆘 Troubleshooting

### Common Deployment Issues

#### "ArgoCD Sync Failing"
```bash
# Check ArgoCD application status
kubectl get applications -n argocd
kubectl describe application ats-dev -n argocd

# Force sync
./scripts/force_argocd_sync.sh

# Check for YAML parsing errors
python -c "
import yaml
with open('k8s/problematic-file.yaml', 'r') as f:
    docs = list(yaml.safe_load_all(f))
print(f'Valid YAML with {len(docs)} documents')
"
```

#### "Deployment Stuck in Pending"
```bash
# Check resource constraints
kubectl describe pod pod-name -n ats-dev
kubectl top nodes

# Check PVC status
kubectl get pvc -n ats-dev
kubectl describe pvc pvc-name -n ats-dev

# Common fixes:
# - Increase resource requests/limits
# - Check storage class availability
# - Verify node capacity
```

#### "Service Unreachable Externally"
```bash
# Check service configuration
kubectl get service service-name -n ats-dev -o wide
kubectl describe service service-name -n ats-dev

# Check NodePort accessibility
kubectl get nodes -o wide
curl -v http://NODE_IP:NODE_PORT/health

# Common fixes:
# - Verify NodePort range (30000-32767)
# - Check firewall rules
# - Confirm service selector matches pod labels
```

#### "Database Connection Failing"
```bash
# Check database service
kubectl get service postgres -n ats-dev
kubectl logs deployment/postgres -n ats-dev

# Test connectivity from pod
kubectl exec -it deployment/service-name -n ats-dev -- nc -zv postgres 5432

# Common fixes:
# - Verify secrets configuration
# - Check network policies
# - Confirm database credentials
```

### ArgoCD-Specific Issues

#### YAML Parsing Errors
**Symptom**: `Failed to unmarshal "filename.yaml"`

**Solution**:
```yaml
# ❌ PROBLEMATIC: Complex embedded strings
data:
  script.py: |
    f.write('''complex string''')  # Breaks YAML

# ✅ CORRECT: Proper string handling  
data:
  script.py: |
    content = '''complex string'''
    f.write(content)
```

#### Resource Conflicts
**Symptom**: `Resource already exists`

**Solution**:
```bash
# Detect conflicts
python scripts/detect_k8s_conflicts.py k8s/

# Force replacement
kubectl replace --force -f k8s/conflicting-resource.yaml
```

### Rollback Procedures

#### Immediate Rollback (Production Issues)
```bash
# 1. Fast Kubernetes rollback
kubectl rollout undo deployment/service-name -n ats-prod

# 2. Verify rollback
kubectl rollout status deployment/service-name -n ats-prod

# 3. Monitor recovery
./scripts/monitor_deployment.sh service-name
```

#### Planned Rollback
```bash
# 1. Use rollback script with options
./scripts/rollback_deployment.sh service-name git

# 2. Coordinate with team
# 3. Monitor system stability
./scripts/deployment_status.sh
```

### Emergency Procedures

#### Production Incident Response
```bash
# 1. Assess impact
kubectl get pods -n ats-prod
./scripts/deployment_status.sh

# 2. Quick rollback if needed
kubectl rollout undo deployment/service-name -n ats-prod

# 3. Gather diagnostics
kubectl logs deployment/service-name -n ats-prod --previous
kubectl describe deployment service-name -n ats-prod

# 4. Coordinate with team
# - Notify stakeholders
# - Document incident
# - Plan fix deployment
```

---

## 🎯 Deployment Success Criteria

### Pre-Deployment Checklist
- [ ] Pre-deployment checks pass (`./scripts/pre_deploy_check.sh`)
- [ ] YAML validation succeeds
- [ ] No resource conflicts detected
- [ ] Team coordination completed
- [ ] Rollback procedure documented

### Post-Deployment Verification
- [ ] All pods running successfully
- [ ] External access tested (not port-forwarding)
- [ ] Database connectivity verified
- [ ] Health checks passing
- [ ] Performance metrics acceptable
- [ ] Integration tests pass
- [ ] Monitoring alerts configured

### Production Deployment Requirements
- [ ] Stakeholder approval obtained
- [ ] Maintenance window scheduled
- [ ] Rollback plan documented
- [ ] Team on standby for monitoring
- [ ] Performance baselines established
- [ ] Customer communication sent (if needed)

---

## 🔧 Operational Scripts

### Available Automation Scripts
```bash
# Deployment workflow
./scripts/pre_deploy_check.sh           # Pre-deployment validation
./scripts/dev_deploy.sh                 # GitOps deployment
./scripts/monitor_deployment.sh         # Real-time monitoring
./scripts/deployment_status.sh          # System status overview

# Rollback procedures
./scripts/rollback_deployment.sh        # Multiple rollback options
./scripts/validate_deployment.sh        # YAML validation

# ArgoCD integration
./scripts/force_argocd_sync.sh          # Force ArgoCD sync
./scripts/get_external_access.sh        # External endpoint discovery

# Resource management
python scripts/detect_k8s_conflicts.py  # Conflict detection
./scripts/monthly_k8s_maintenance.sh    # Maintenance tasks
```

---

**🚀 This deployment guide ensures reliable, monitored deployments across all ATS environments with proper safety checks and rollback procedures.**