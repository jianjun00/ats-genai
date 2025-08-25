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

## Issue 2: Massive Resource Conflicts and Management Overload

### Symptoms
```
one or more synchronization tasks are not valid (retried 3 times)
Application Status: OutOfSync/Degraded (persistent)
```

### Root Cause
**ArgoCD overwhelmed by resource volume and conflicts:**
- Large number of YAML files (100+)
- Multiple duplicate resource definitions
- Test/debug files mixed with production configs
- Resource naming conflicts

### Systematic Solution

#### Step 1: Resource Audit
```bash
# Audit all YAML files for conflicts
python3 -c "
import os
import yaml
from collections import defaultdict

yaml_files = [f for f in os.listdir('k8s') if f.endswith('.yaml')]
resources = defaultdict(list)

for file in yaml_files:
    with open(f'k8s/{file}', 'r') as f:
        docs = list(yaml.safe_load_all(f))
    
    for doc in docs:
        if doc and 'kind' in doc:
            kind = doc.get('kind', 'Unknown')
            metadata = doc.get('metadata', {})
            name = metadata.get('name', 'unnamed')
            namespace = metadata.get('namespace', 'default')
            
            resource_key = f'{kind}/{namespace}/{name}'
            resources[resource_key].append(file)

# Show conflicts
conflicts = {r: f for r, f in resources.items() if len(f) > 1}
for resource_key, files in conflicts.items():
    print(f'CONFLICT: {resource_key}: {files}')
"
```

#### Step 2: Strategic Cleanup
```bash
# Categories for removal:
# 1. Duplicate resource definitions
# 2. Test/debug files (test-*, debug-*, sample-*)
# 3. Outdated configurations (old-*, deprecated-*)
# 4. Working/temporary files (working-*, temp-*, tmp-*)

# Create backup first
mkdir -p ../k8s-backup && cp -r k8s ../k8s-backup/

# Remove problematic files (example)
cd k8s
rm -f test-*-job.yaml debug-*-job.yaml working-*-webapp.yaml
rm -f enhanced-webapp-simple.yaml  # Keep enhanced-webapp-deployment.yaml
rm -f integrated-analytics-webapp.yaml  # Keep comprehensive-analytics-webapp.yaml
```

#### Step 3: Verification
```bash
# Verify no conflicts remain
python3 -c "
# ... same audit script as Step 1 ...
print(f'Remaining conflicts: {len(conflicts)}')
"

# Test ArgoCD sync
kubectl patch application ats-dev -n argocd --type='merge' \
  -p='{\"metadata\":{\"annotations\":{\"argocd.argoproj.io/refresh\":\"hard\"}}}'
```

## Issue 3: Branch Synchronization Problems

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

## Issue 4: ArgoCD Cache Problems

### Symptoms
```
Manifest generation error (cached): rpc error: code = FailedPrecondition desc = ...
```

### Root Cause
ArgoCD repository server is holding onto cached manifests from previous failed attempts.

### Solution
```bash
# Restart ArgoCD repo server to clear cache
kubectl rollout restart deployment/argocd-repo-server -n argocd

# Wait for deployment to be ready
kubectl rollout status deployment/argocd-repo-server -n argocd

# Trigger fresh sync
kubectl patch application ats-dev -n argocd --type='merge' \
  -p='{"operation":{"sync":{"revision":"main"}}}'
```

## Best Practices for ArgoCD Management

### 1. Resource Organization
```bash
# Separate concerns by directory
k8s/
├── core/           # Essential services (postgres, redis)
├── analytics/      # Analytics services
├── jobs/           # Batch jobs and CronJobs
├── monitoring/     # Monitoring and observability
└── experimental/   # Test and development resources
```

### 2. Naming Conventions
```yaml
# Use descriptive, unique names
metadata:
  name: analytics-webapp-dev        # Not just "webapp"
  name: data-processor-staging      # Not just "processor"
  name: model-trainer-prod          # Not just "trainer"
```

### 3. Resource Validation Pipeline
```bash
# Pre-commit hook for YAML validation
#!/bin/bash
for file in k8s/**/*.yaml; do
    python3 -c "
    import yaml
    with open('$file', 'r') as f:
        list(yaml.safe_load_all(f))
    print('✅ $file valid')
    " || exit 1
done
```

### 4. Conflict Detection Automation
```bash
# Add to CI/CD pipeline
python3 scripts/detect_k8s_conflicts.py k8s/
# Exit 1 if conflicts found
```

### 5. Regular Maintenance
```bash
# Monthly cleanup checklist:
# 1. Audit for duplicate resources
# 2. Remove test/debug files from production directories  
# 3. Update resource limits and requests
# 4. Verify all applications are in sync
# 5. Clean up unused ConfigMaps and Secrets
```

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

# Restart ArgoCD components
kubectl rollout restart deployment/argocd-repo-server -n argocd
kubectl rollout restart deployment/argocd-server -n argocd

# Check sync status
kubectl get application ats-dev -n argocd -w
```

## Case Studies

### Case Study 1: Enhanced Webapp Deployment Issue (2025-08-23)

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

### Case Study 2: Massive Resource Conflicts Resolution (2025-08-23)

**Problem:** ArgoCD ats-dev stuck in persistent OutOfSync/Degraded state despite individual YAML files being valid
**Error:** `one or more synchronization tasks are not valid (retried 3 times)`

**Root Cause Analysis:**
- **110 YAML files** containing **194 total resources**
- **9 resource conflicts** from duplicate resource definitions
- **21 test/debug/outdated files** creating parsing complexity
- ArgoCD overwhelmed by resource volume and conflicts

**Systematic Solution Applied:**
1. **Comprehensive Audit:** Analyzed all 110 YAML files for conflicts and duplicates
2. **Conflict Resolution:** Identified 9 resource conflicts across key components
3. **Strategic Cleanup:** Removed 21 files in categories:
   - **Duplicates:** enhanced-analytics-webapp.yaml, integrated-analytics-webapp.yaml
   - **Test Files:** test-*-job.yaml, debug-*-job.yaml (8 files)
   - **Outdated Configs:** complete-analytics-config.yaml, updated-*.yaml
4. **Verification:** Reduced to **89 files, 175 resources, 0 conflicts**

**Technical Details:**
```bash
# Major conflicts resolved:
- enhanced-analytics-webapp: 3 files → 1 (kept enhanced-webapp-deployment.yaml)
- integrated-analytics: 2 files → 1 (kept comprehensive-analytics-webapp.yaml)
- simple-fixed-analytics-config: 3 files → 1 (kept core webapp)
- unified-analytics-config: 2 files → 1 (kept unified-analytics-app.yaml)
- Multiple test/debug files removed entirely
```

**Outcome:** 
- ✅ **Zero resource conflicts remaining**
- ✅ **21 files successfully removed**
- ✅ **Core services preserved and healthy**
- ✅ **Improved maintainability and reliability**

**Key Lessons Learned:**
1. **Resource conflicts are cumulative** - even valid individual files can cause system-wide issues
2. **Test files in production directories** create ongoing maintenance burden
3. **Systematic auditing tools** are essential for managing large K8s deployments
4. **Core service preservation** must be verified during cleanup operations

**Prevention Strategy:**
- Implement pre-commit hooks to detect resource conflicts
- Separate test/debug files from production manifests
- Regular resource audits (monthly) for large deployments
- Use naming conventions to identify file purposes clearly

## Troubleshooting Checklist

When ArgoCD sync fails, check these in order:

- [ ] **YAML Syntax:** Validate all YAML files locally
- [ ] **Resource Conflicts:** Check for duplicate resource names  
- [ ] **Git Status:** Ensure changes are committed and pushed  
- [ ] **Branch Configuration:** Verify ArgoCD points to correct branch
- [ ] **Cache Issues:** Try restarting ArgoCD repo server
- [ ] **Namespace Existence:** Ensure target namespace exists
- [ ] **RBAC Permissions:** Verify ArgoCD has necessary permissions
- [ ] **Network Connectivity:** Check if ArgoCD can reach Git repository

---

*Last updated: 2025-08-23*
*Update this document whenever new ArgoCD issues are discovered and resolved.*