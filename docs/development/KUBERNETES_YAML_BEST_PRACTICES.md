# Kubernetes YAML Best Practices for ATS Platform

## 🎯 YAML Syntax and Structure Guidelines

This document provides specific guidelines for writing Kubernetes YAML manifests that work reliably with ArgoCD and avoid common parsing issues.

## 1. Embedded Scripts in ConfigMaps

### ❌ Avoid: Complex Multi-line Strings
```yaml
# PROBLEMATIC: Nested quotes and complex structures
apiVersion: v1
kind: ConfigMap
data:
  script.py: |
    def setup():
        config = '''
import os
class Config:
    def __init__(self):
        pass
'''  # This can break YAML parsing
        write_file(config)
```

### ✅ Preferred: Simple Variable Assignment
```yaml
# CORRECT: Clean variable assignment pattern
apiVersion: v1
kind: ConfigMap
data:
  script.py: |
    def setup():
        config_content = '''import os
class Config:
    def __init__(self):
        pass
'''
        write_file(config_content)
```

### ✅ Best: External Script References
```yaml
# IDEAL: Reference external scripts
apiVersion: v1
kind: ConfigMap
data:
  script.py: |
    #!/usr/bin/env python3
    # Simple script that imports from mounted volumes
    import sys
    sys.path.append('/scripts')
    from config_generator import generate_config
    generate_config()
```

## 2. Resource Naming Conventions

### Standard Format
```
{component}-{function}-{environment}
```

### Examples
```yaml
# ✅ Good naming
metadata:
  name: analytics-webapp-dev
  name: data-processor-staging
  name: model-trainer-prod

# ❌ Avoid generic names
metadata:
  name: webapp
  name: service
  name: app
```

## 3. ConfigMap Organization

### ✅ Single Purpose ConfigMaps
```yaml
# Separate ConfigMaps for different purposes
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: webapp-config
data:
  app.py: |
    # Simple webapp code
    
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: webapp-environment
data:
  DATABASE_URL: "postgres://..."
  API_KEY: "from-secret"
```

### ❌ Avoid: Overly Complex ConfigMaps
```yaml
# DON'T: Mix multiple complex scripts in one ConfigMap
apiVersion: v1
kind: ConfigMap
data:
  setup.py: |
    # 200 lines of complex setup code...
  webapp.py: |
    # 300 lines of webapp code...
  database.py: |
    # 150 lines of database code...
```

## 4. Multi-Document YAML Files

### Proper Document Separation
```yaml
# Document 1: ConfigMap
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: ats-dev
data:
  config.yaml: |
    database:
      host: postgres-simple

---
# Document 2: Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app-deployment
  namespace: ats-dev
spec:
  # deployment spec...

---
# Document 3: Service
apiVersion: v1
kind: Service
metadata:
  name: app-service
  namespace: ats-dev
spec:
  # service spec...
```

## 5. Environment Variable Handling

### ✅ Recommended Pattern
```yaml
env:
- name: ENVIRONMENT
  value: "dev"
- name: DB_HOST
  value: "postgres-simple"
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: db-credentials
      key: password
      optional: false  # Make explicit
```

### ❌ Avoid: Inline Secrets
```yaml
env:
- name: DB_PASSWORD
  value: "hardcoded-password"  # Never do this
```

## 6. Resource Limits and Requests

### Always Specify Both
```yaml
resources:
  requests:
    memory: "512Mi"
    cpu: "250m"
  limits:
    memory: "1Gi"
    cpu: "500m"
```

## 7. Health Checks

### Comprehensive Health Checks
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 3000
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /ready
    port: 3000
  initialDelaySeconds: 5
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 3
```

## 8. Volume Mounts

### Clear Volume Organization
```yaml
volumeMounts:
- name: config-volume
  mountPath: /app/config
  readOnly: true
- name: data-volume
  mountPath: /app/data
- name: tmp-volume
  mountPath: /tmp

volumes:
- name: config-volume
  configMap:
    name: app-config
    defaultMode: 0755
- name: data-volume
  persistentVolumeClaim:
    claimName: app-data-pvc
- name: tmp-volume
  emptyDir: {}
```

## 9. Labels and Annotations

### Consistent Labeling
```yaml
metadata:
  labels:
    app: analytics-webapp
    component: frontend
    environment: dev
    version: v1.2.3
    managed-by: argocd
  annotations:
    deployment.kubernetes.io/revision: "1"
    kubectl.kubernetes.io/last-applied-configuration: |
      # ArgoCD manages this
```

## 10. Validation Checklist

### Pre-Commit Validation
```bash
#!/bin/bash
# validate-yaml.sh

for file in k8s/*.yaml; do
    echo "Validating $file..."
    
    # Check YAML syntax
    python3 -c "
import yaml
import sys
try:
    with open('$file', 'r') as f:
        docs = list(yaml.safe_load_all(f))
    print('✅ YAML syntax valid')
except Exception as e:
    print(f'❌ YAML syntax error: {e}')
    sys.exit(1)
    "
    
    # Check with kubectl
    kubectl apply -f "$file" --dry-run=client --validate=true
    
    if [ $? -eq 0 ]; then
        echo "✅ $file validation passed"
    else
        echo "❌ $file validation failed"
        exit 1
    fi
done

echo "🎉 All YAML files validated successfully"
```

## 11. ArgoCD-Specific Considerations

### Application Definition
```yaml
# argocd-application.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ats-dev
  namespace: argocd
spec:
  destination:
    namespace: ats-dev
    server: https://kubernetes.default.svc
  source:
    path: k8s  # Ensure this path exists
    repoURL: https://github.com/AkoloTechnologies/ats-genai.git
    targetRevision: main  # Keep synchronized with actual branch
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
    - CreateNamespace=true
    - PrunePropagationPolicy=foreground
```

### Resource Exclusions
```yaml
# In ArgoCD Application spec
source:
  directory:
    exclude: |
      **/.venv/**
      **/node_modules/**
      **/__pycache__/**
      **/*.pyc
      **/templates/**  # Exclude Helm templates if not using Helm
```

## 12. Common Anti-Patterns to Avoid

### ❌ Don't: Hardcode Environment-Specific Values
```yaml
# BAD
env:
- name: DATABASE_URL
  value: "postgres://user:pass@prod-db:5432/db"  # Hardcoded prod values
```

### ❌ Don't: Use Generic Resource Names
```yaml
# BAD
metadata:
  name: service  # Too generic
  name: app      # Conflicts likely
```

### ❌ Don't: Mix Environments in Same Namespace
```yaml
# BAD - mixing dev and prod in same manifest
metadata:
  namespace: ats-dev
---
metadata:
  namespace: ats-prod  # Should be separate files
```

### ❌ Don't: Embed Large Scripts
```yaml
# BAD - 500+ lines of Python in YAML
data:
  massive-script.py: |
    # Hundreds of lines...
    # This makes YAML unreadable and error-prone
```

## 13. Testing Your YAML

### Local Testing Commands
```bash
# 1. Validate YAML syntax
yamllint k8s/your-file.yaml

# 2. Validate Kubernetes resources
kubectl apply -f k8s/your-file.yaml --dry-run=client

# 3. Test with real cluster (dry-run)
kubectl apply -f k8s/your-file.yaml --dry-run=server

# 4. Check resource creation
kubectl apply -f k8s/your-file.yaml
kubectl get all -n ats-dev | grep your-resource

# 5. Clean up test resources
kubectl delete -f k8s/your-file.yaml
```

### ArgoCD Testing
```bash
# 1. Check ArgoCD can parse the manifests
kubectl patch application ats-dev -n argocd --type='merge' \
  -p='{"metadata":{"annotations":{"argocd.argoproj.io/refresh":"hard"}}}'

# 2. Monitor sync status
kubectl get application ats-dev -n argocd -w

# 3. Check for parsing errors
kubectl describe application ats-dev -n argocd | grep -A 10 "Message"
```

## 14. Emergency Recovery

### Quick Fix for Broken YAML
```bash
# 1. Identify the problematic file
kubectl describe application ats-dev -n argocd | grep "Failed to unmarshal"

# 2. Validate locally
python3 -c "
import yaml
with open('k8s/problematic-file.yaml', 'r') as f:
    try:
        docs = list(yaml.safe_load_all(f))
        print('YAML is valid')
    except Exception as e:
        print(f'YAML error: {e}')
"

# 3. Fix and test
kubectl apply -f k8s/problematic-file.yaml --dry-run=client

# 4. Commit and push
git add k8s/problematic-file.yaml
git commit -m "fix: resolve YAML syntax error"
git push origin feature-branch

# 5. Update ArgoCD if needed
kubectl patch application ats-dev -n argocd --type='merge' \
  -p='{"spec":{"source":{"targetRevision":"feature-branch"}}}'
```

---

## Summary

- **Keep it simple**: Avoid complex embedded scripts in ConfigMaps
- **Validate early**: Use pre-commit hooks for YAML validation
- **Name consistently**: Use descriptive, environment-specific names
- **Test thoroughly**: Validate both syntax and Kubernetes resource creation
- **Document changes**: Update this guide when new patterns are discovered

Following these practices will prevent most ArgoCD sync issues and make Kubernetes manifests more maintainable.

---

*Last updated: 2025-08-23*
*Update this document when new YAML patterns or issues are discovered.*