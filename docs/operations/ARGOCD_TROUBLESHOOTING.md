# ArgoCD Troubleshooting Guide

## 🚨 Common ArgoCD Sync Issues and Solutions

This guide documents common ArgoCD deployment issues encountered in the ATS platform and their solutions.

## Issue 1: YAML Parsing Errors in Kubernetes Manifests

### Symptoms
```
ComparisonError: Failed to load target state: failed to generate manifest for source 1 of 1: 
rpc error: code = FailedPrecondition desc = Failed to unmarshal "filename.yaml": <nil>
```

### Root Cause
Complex embedded content (like Python scripts with multi-line strings) inside YAML ConfigMaps can break YAML parsing when not properly escaped.

### Example Problem
```yaml
# ❌ PROBLEMATIC: Triple quotes in YAML literal block
data:
  script.py: |
    def create_config():
        with open('config.py', 'w') as f:
            f.write('''
import os
class Config:
    pass
''')  # This breaks YAML parsing
```

### Solution
```yaml
# ✅ CORRECT: Use proper variable assignment
data:
  script.py: |
    def create_config():
        config_content = '''import os
class Config:
    pass
'''
        with open('config.py', 'w') as f:
            f.write(config_content)
```

### Diagnostic Steps
1. **Validate YAML locally:**
   ```bash
   python -c "
   import yaml
   with open('k8s/problematic-file.yaml', 'r') as f:
       docs = list(yaml.safe_load_all(f))
   print(f'Valid YAML with {len(docs)} documents')
   "
   ```

2. **Test with kubectl:**
   ```bash
   kubectl apply -f k8s/problematic-file.yaml --dry-run=client
   ```

3. **Check ArgoCD application status:**
   ```bash
   kubectl describe application ats-dev -n argocd | grep -A 10 "Operation State"
   ```

## Issue 2: Branch Synchronization Problems

### Symptoms
```
ComparisonError: Failed to load target state: failed to generate manifest for source 1 of 1: 
rpc error: code = Unknown desc = k8s-clean: app path does not exist
```

### Root Cause
ArgoCD is pointing to the wrong Git branch or path that doesn't exist in the repository.

### Solution
1. **Check current branch:**
   ```bash
   git branch -a
   ```

2. **Update ArgoCD to correct branch:**
   ```bash
   kubectl patch application ats-dev -n argocd --type='merge' \
     -p='{"spec":{"source":{"targetRevision":"correct-branch-name"}}}'
   ```

3. **Force refresh ArgoCD cache:**
   ```bash
   kubectl patch application ats-dev -n argocd --type='merge' \
     -p='{"metadata":{"annotations":{"argocd.argoproj.io/refresh":"hard"}}}'
   ```

## Issue 3: Resource Duplication Warnings

### Symptoms
```
Message: Resource /ConfigMap/ats-dev/some-config appeared 2 times among application resources.
Type: RepeatedResourceWarning
```

### Root Cause
Multiple YAML files in the `k8s/` directory define the same resource with identical names and namespaces.

### Solution
1. **Find duplicate resources:**
   ```bash
   # Search for duplicate resource names
   find k8s/ -name "*.yaml" -exec grep -l "name: duplicate-resource-name" {} \;
   ```

2. **Consolidate or rename resources:**
   - Merge duplicate resources into a single file
   - Or rename resources to have unique names

3. **Clean up unused files:**
   ```bash
   git rm k8s/obsolete-deployment.yaml
   ```

## Issue 4: Application Stuck in "Unknown" State

### Symptoms
ArgoCD application shows `SYNC STATUS: Unknown` and doesn't progress.

### Solution
1. **Trigger manual sync:**
   ```bash
   kubectl patch application ats-dev -n argocd --type='merge' \
     -p='{"operation":{"sync":{"revision":"main"}}}'
   ```

2. **Check for pending operations:**
   ```bash
   kubectl get application ats-dev -n argocd -o yaml | grep -A 20 "operation"
   ```

3. **Reset application if needed:**
   ```bash
   # Delete and recreate the application
   kubectl delete application ats-dev -n argocd
   # Then reapply the application manifest
   ```

## Issue 5: Missing Kubernetes Resources

### Symptoms
Services or deployments show as "Missing" in ArgoCD but should exist.

### Solution
1. **Check actual cluster state:**
   ```bash
   kubectl get all -n ats-dev | grep missing-resource
   ```

2. **Compare desired vs actual:**
   ```bash
   # Get ArgoCD's desired state
   kubectl get application ats-dev -n argocd -o yaml
   
   # Compare with actual resources
   kubectl get deployment missing-deployment -n ats-dev -o yaml
   ```

3. **Force sync specific resource:**
   ```bash
   kubectl patch application ats-dev -n argocd --type='merge' \
     -p='{"operation":{"sync":{"resources":[{"kind":"Deployment","name":"missing-deployment"}]}}}'
   ```

## Best Practices for ArgoCD Management

### 1. YAML Validation
Always validate YAML before committing:
```bash
# Add to pre-commit hooks
python -c "
import yaml
import sys
try:
    with open(sys.argv[1], 'r') as f:
        list(yaml.safe_load_all(f))
    print('✅ YAML valid')
except Exception as e:
    print(f'❌ YAML invalid: {e}')
    exit(1)
" k8s/new-deployment.yaml
```

### 2. Consistent Resource Naming
- Use unique, descriptive names for all resources
- Follow naming convention: `{component}-{function}-{environment}`
- Example: `analytics-webapp-dev`, `data-processor-staging`

### 3. Proper Git Workflow
- Always commit YAML fixes before expecting ArgoCD to sync
- Use feature branches for changes, not direct commits to main
- Update ArgoCD target revision when working on feature branches

### 4. Monitoring ArgoCD Health
```bash
# Regular health check commands
kubectl get applications -n argocd
kubectl get pods -n argocd
kubectl logs -n argocd deployment/argocd-server
```

## Troubleshooting Checklist

When ArgoCD sync fails, check these in order:

- [ ] **YAML Syntax:** Validate all YAML files locally
- [ ] **Git Status:** Ensure changes are committed and pushed  
- [ ] **Branch Configuration:** Verify ArgoCD points to correct branch
- [ ] **Resource Names:** Check for duplicate resource names
- [ ] **Namespace Existence:** Ensure target namespace exists
- [ ] **RBAC Permissions:** Verify ArgoCD has necessary permissions
- [ ] **Network Connectivity:** Check if ArgoCD can reach Git repository

## Recovery Commands

Quick commands for common recovery scenarios:

```bash
# Force complete refresh
kubectl patch application ats-dev -n argocd --type='merge' \
  -p='{"metadata":{"annotations":{"argocd.argoproj.io/refresh":"hard"}}}'

# Reset to main branch
kubectl patch application ats-dev -n argocd --type='merge' \
  -p='{"spec":{"source":{"targetRevision":"main"}}}'

# Trigger immediate sync
kubectl patch application ats-dev -n argocd --type='merge' \
  -p='{"operation":{"sync":{"revision":"main"}}}'

# Check sync status
kubectl get application ats-dev -n argocd -w
```

---

## Case Study: Enhanced Webapp Deployment Issue (2025-08-23)

**Problem:** YAML parsing error in `enhanced-webapp-deployment.yaml`
**Error:** `Failed to unmarshal "enhanced-webapp-deployment.yaml": <nil>`

**Root Cause:** Complex Python script with triple-quoted strings embedded in YAML ConfigMap caused parser confusion.

**Solution Applied:**
1. Simplified embedded Python script structure
2. Replaced problematic triple quotes with variable assignment pattern
3. Committed changes to feature branch
4. Updated ArgoCD to use correct branch
5. Triggered manual sync

**Outcome:** ArgoCD sync errors resolved, application successfully deployed

**Lesson Learned:** Keep embedded scripts in Kubernetes ConfigMaps simple, avoid complex multi-line string patterns that can confuse YAML parsers.

---

*This document should be updated whenever new ArgoCD issues are discovered and resolved.*