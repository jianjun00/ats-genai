# GitOps Development Workflow - Option 2: Direct Service Replacement

## 🚀 Overview

This document outlines the **Direct Service Replacement** development workflow for the ATS platform using ArgoCD GitOps. This approach provides rapid iteration with zero-downtime deployments while maintaining GitOps principles.

## 🎯 Workflow Philosophy

**Core Principle**: Make changes directly to main service deployments in feature branches, letting ArgoCD handle rolling updates automatically.

**Benefits:**
- ✅ Fast iteration cycle (30-60 seconds)
- ✅ Zero-downtime rolling updates
- ✅ GitOps compliance (all changes in Git)
- ✅ Real production-like testing
- ✅ Automatic rollback capabilities

**Trade-offs:**
- ⚠️ Immediate impact on ats-dev environment
- ⚠️ Requires team coordination
- ⚠️ No isolated testing environment

## 🔄 Complete Development Workflow

### Phase 1: Preparation and Safety Checks

```bash
# 1. Start from clean main branch
git checkout main
git pull origin main

# 2. Run pre-development safety check
./scripts/pre_deploy_check.sh

# 3. Create feature branch
git checkout -b feature/my-new-feature
git push -u origin feature/my-new-feature
```

### Phase 2: Development and Testing

```bash
# 4. Make your changes to deployment files
vim k8s/analytics-service/deployment.yaml
# Update: image tags, env vars, resource limits, etc.

# 5. Validate changes locally
./scripts/validate_deployment.sh k8s/analytics-service/deployment.yaml

# 6. Deploy to ats-dev for testing
./scripts/dev_deploy.sh

# 7. Monitor deployment progress
./scripts/monitor_deployment.sh analytics-service

# 8. Test your changes
curl http://$(./scripts/get_external_access.sh analytics-service)/your-endpoint
```

### Phase 3: Iteration and Refinement

```bash
# If changes needed:
# 9. Make additional changes
vim k8s/analytics-service/deployment.yaml

# 10. Quick deploy iteration
git add k8s/analytics-service/deployment.yaml
git commit -m "feat: refine analytics endpoint response format"
./scripts/dev_deploy.sh

# 11. Repeat testing
```

### Phase 4: Production Readiness

```bash
# 12. Final validation
./scripts/production_readiness_check.sh

# 13. Create pull request
gh pr create --title "feat: implement new analytics endpoint" \
  --body "$(./scripts/generate_pr_template.sh)"

# 14. Merge to main (triggers production deployment)
gh pr merge --squash
```

## 🛠️ Development Scripts

### Core Scripts Overview

| Script | Purpose | Usage |
|--------|---------|--------|
| `pre_deploy_check.sh` | Safety checks before development | Run before starting work |
| `dev_deploy.sh` | Deploy changes to ats-dev | Run after each change |
| `monitor_deployment.sh` | Watch deployment progress | Monitor rollout status |
| `rollback_deployment.sh` | Emergency rollback | Quick recovery from issues |
| `validate_deployment.sh` | YAML and logic validation | Validate before deploy |

### Team Coordination Tools

| Tool | Purpose | Integration |
|------|---------|-------------|
| Slack notifications | Deploy announcements | Webhook integration |
| Deployment locks | Prevent conflicts | File-based locking |
| Status dashboard | Current environment state | Web interface |

## ⚡ Quick Reference Commands

### Immediate Actions
```bash
# Quick deploy current changes
./scripts/dev_deploy.sh

# Force immediate ArgoCD sync
./scripts/force_argocd_sync.sh

# Emergency rollback
./scripts/rollback_deployment.sh <service-name>

# Check deployment status
./scripts/deployment_status.sh
```

### Monitoring and Debugging
```bash
# Get service external access
./scripts/get_external_access.sh <service-name>

# Check pod logs
./scripts/get_service_logs.sh <service-name>

# Resource utilization
./scripts/check_resource_usage.sh

# ArgoCD application status
./scripts/argocd_status.sh
```

## 🔒 Safety Measures

### Pre-Deployment Checks
1. **YAML Validation** - Syntax and schema validation
2. **Resource Conflicts** - Check for duplicate resources
3. **ArgoCD Status** - Ensure clean sync state
4. **Team Coordination** - Check for ongoing deployments

### During Deployment
1. **Progress Monitoring** - Real-time rollout status
2. **Health Checks** - Service endpoint validation
3. **Resource Monitoring** - CPU/memory usage
4. **Log Streaming** - Real-time error detection

### Post-Deployment
1. **Smoke Tests** - Basic functionality verification
2. **Performance Check** - Response time validation
3. **Error Rate Monitoring** - Monitor for increased errors
4. **Rollback Readiness** - Keep rollback commands ready

## 🚨 Rollback Procedures

### Immediate Rollback (< 30 seconds)
```bash
# Option 1: Kubernetes rollback (fastest)
kubectl rollout undo deployment/<service-name> -n ats-dev

# Option 2: ArgoCD rollback to previous commit
./scripts/rollback_deployment.sh <service-name> --immediate
```

### Git-based Rollback (1-2 minutes)
```bash
# Revert commit and trigger sync
git revert HEAD
git push origin feature-branch
./scripts/force_argocd_sync.sh
```

## 👥 Team Coordination

### Before Deployment
1. **Check #dev-deployments Slack channel** for ongoing work
2. **Announce your deployment** with estimated duration
3. **Coordinate with other developers** if conflicts expected

### During Deployment
1. **Monitor deployment progress** actively
2. **Be ready for immediate rollback** if issues arise
3. **Communicate status updates** in team channels

### After Deployment
1. **Announce completion** with test endpoint info
2. **Share any issues encountered** for team learning
3. **Update documentation** if workflow improved

## 📊 Deployment Metrics

### Success Criteria
- ✅ Deployment completes in < 2 minutes
- ✅ Zero failed pod restarts
- ✅ Health checks pass within 30 seconds
- ✅ Response time < 500ms baseline
- ✅ Error rate < 1%

### Monitoring Dashboards
- **ArgoCD Application Status**: Real-time sync state
- **Kubernetes Resource Health**: Pod and service status
- **Application Metrics**: Response time, error rate, throughput
- **Team Activity**: Current deployments, recent changes

## 🔧 Environment Configuration

### Required Tools
```bash
# Install required CLI tools
./scripts/install_dev_tools.sh

# Tools installed:
# - kubectl (Kubernetes CLI)
# - argocd (ArgoCD CLI)
# - gh (GitHub CLI)  
# - jq (JSON processor)
# - curl (HTTP client)
```

### Environment Variables
```bash
# Set in your shell profile
export KUBECONFIG=~/.kube/config
export ARGOCD_SERVER=argocd.your-domain.com
export GITHUB_TOKEN=your_github_token
export SLACK_WEBHOOK_URL=your_slack_webhook
```

## 📝 Best Practices

### Development Guidelines
1. **Keep changes small** - Single feature per branch
2. **Test locally first** - Use dry-run and validation
3. **Monitor deployments** - Don't deploy and walk away
4. **Communicate actively** - Keep team informed
5. **Learn from issues** - Document problems and solutions

### Git Workflow
1. **Descriptive commit messages** - Clear change description
2. **Atomic commits** - Single logical change per commit  
3. **Feature branches** - Always branch from main
4. **Clean history** - Squash commits before merge

### ArgoCD Integration
1. **Respect sync policies** - Don't bypass ArgoCD
2. **Monitor sync status** - Ensure clean deployments
3. **Use manual sync sparingly** - For urgent fixes only
4. **Keep manifests clean** - Remove test configurations

## 🎓 Training and Onboarding

### New Developer Checklist
- [ ] Complete GitOps workflow training
- [ ] Install and configure development tools
- [ ] Practice with sample deployment
- [ ] Understand rollback procedures
- [ ] Join #dev-deployments Slack channel

### Advanced Techniques
- **Blue-Green Deployments**: For major changes
- **Canary Releases**: Gradual traffic shifting
- **Feature Flags**: Runtime feature control
- **A/B Testing**: Comparative feature testing

## 📚 Additional Resources

### Documentation References
- [ArgoCD Troubleshooting Guide](../operations/ARGOCD_TROUBLESHOOTING.md)
- [Kubernetes Resource Management](KUBERNETES_RESOURCE_MANAGEMENT.md)
- [Development Setup Guide](../onboarding/DEVELOPMENT_SETUP.md)

### External Resources
- [ArgoCD Best Practices](https://argo-cd.readthedocs.io/en/stable/user-guide/best_practices/)
- [Kubernetes Rolling Updates](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#rolling-update-deployment)
- [GitOps Principles](https://opengitops.dev/)

---

*Last updated: 2025-08-23*  
*Update this document when workflow improvements are made.*