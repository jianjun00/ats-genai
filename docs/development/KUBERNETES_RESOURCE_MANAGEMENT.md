# Kubernetes Resource Management Best Practices

## 🎯 Overview

This guide provides comprehensive best practices for managing Kubernetes resources in the ATS platform, based on real-world troubleshooting experience and systematic resource cleanup operations.

## 🚨 Critical Anti-Patterns to Avoid

### Resource Organization Anti-Patterns

❌ **Don't mix test/debug files with production manifests**
```bash
# PROBLEMATIC: All in one directory
k8s/
├── enhanced-webapp-deployment.yaml     # Production
├── test-enhanced-webapp.yaml          # Test file - REMOVE
├── debug-enhanced-webapp.yaml         # Debug file - REMOVE
├── working-enhanced-webapp.yaml       # Working file - REMOVE
```

✅ **Organize by purpose and environment**
```bash
# CORRECT: Separated by purpose
k8s/
├── core/
│   └── enhanced-webapp-deployment.yaml
├── experimental/
│   ├── test-enhanced-webapp.yaml
│   └── debug-enhanced-webapp.yaml
└── working/
    └── working-enhanced-webapp.yaml
```

### Resource Naming Anti-Patterns

❌ **Avoid generic or duplicate names**
```yaml
# PROBLEMATIC: Generic names causing conflicts
metadata:
  name: webapp                    # Too generic
  name: analytics                 # Too generic
  name: enhanced-webapp           # Duplicate across files
```

✅ **Use specific, unique names**
```yaml
# CORRECT: Specific, descriptive names
metadata:
  name: enhanced-analytics-webapp-v2
  name: unified-analytics-dashboard
  name: realtime-data-processor
```

## 📊 Resource Conflict Detection

### Automated Conflict Detection Script

Create `scripts/detect_k8s_conflicts.py`:

```python
#!/usr/bin/env python3
"""
Kubernetes Resource Conflict Detection Tool
Analyzes YAML files to identify duplicate resource definitions
"""

import os
import sys
import yaml
from collections import defaultdict
from pathlib import Path

def analyze_k8s_directory(directory: str):
    """Analyze Kubernetes YAML files for conflicts"""
    yaml_files = list(Path(directory).glob("**/*.yaml"))
    resources = defaultdict(list)
    parsing_errors = []
    
    print(f"🔍 Analyzing {len(yaml_files)} YAML files in {directory}")
    
    for file_path in yaml_files:
        try:
            with open(file_path, 'r') as f:
                docs = list(yaml.safe_load_all(f))
            
            for doc_index, doc in enumerate(docs):
                if not doc or 'kind' not in doc:
                    continue
                
                kind = doc.get('kind', 'Unknown')
                metadata = doc.get('metadata', {})
                name = metadata.get('name', 'unnamed')
                namespace = metadata.get('namespace', 'default')
                
                resource_key = f"{kind}/{namespace}/{name}"
                resources[resource_key].append({
                    'file': str(file_path),
                    'doc_index': doc_index
                })
        
        except yaml.YAMLError as e:
            parsing_errors.append(f"❌ YAML Error in {file_path}: {e}")
        except Exception as e:
            parsing_errors.append(f"❌ Error processing {file_path}: {e}")
    
    # Report parsing errors
    if parsing_errors:
        print(f"\n🚨 {len(parsing_errors)} PARSING ERRORS FOUND:")
        for error in parsing_errors:
            print(f"  {error}")
    
    # Find conflicts
    conflicts = {r: files for r, files in resources.items() if len(files) > 1}
    
    if conflicts:
        print(f"\n🚨 {len(conflicts)} RESOURCE CONFLICTS FOUND:")
        for resource_key, files in conflicts.items():
            print(f"  ❌ {resource_key}:")
            for file_info in files:
                print(f"    - {file_info['file']} (document {file_info['doc_index']})")
    else:
        print(f"\n✅ NO CONFLICTS FOUND")
    
    # Summary
    total_resources = sum(len(files) for files in resources.values())
    unique_resources = len(resources)
    
    print(f"\n📊 SUMMARY:")
    print(f"  Files analyzed: {len(yaml_files)}")
    print(f"  Total resources: {total_resources}")
    print(f"  Unique resources: {unique_resources}")
    print(f"  Conflicts: {len(conflicts)}")
    print(f"  Parsing errors: {len(parsing_errors)}")
    
    return len(conflicts) == 0 and len(parsing_errors) == 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python detect_k8s_conflicts.py <k8s_directory>")
        sys.exit(1)
    
    directory = sys.argv[1]
    if not os.path.exists(directory):
        print(f"❌ Directory {directory} does not exist")
        sys.exit(1)
    
    success = analyze_k8s_directory(directory)
    sys.exit(0 if success else 1)
```

### Usage in CI/CD Pipeline

Add to your `.github/workflows/validate.yml`:

```yaml
- name: Check Kubernetes Resource Conflicts
  run: |
    python scripts/detect_k8s_conflicts.py k8s/
    if [ $? -ne 0 ]; then
      echo "❌ Kubernetes resource conflicts detected"
      exit 1
    fi
```

## 🧹 Resource Cleanup Procedures

### Systematic Cleanup Process

1. **Create Backup**
```bash
# Always backup before cleanup
mkdir -p ../k8s-backup-$(date +%Y%m%d)
cp -r k8s ../k8s-backup-$(date +%Y%m%d)/
```

2. **Analyze Current State**
```bash
# Run conflict detection
python scripts/detect_k8s_conflicts.py k8s/

# Count files by category
echo "Test files: $(find k8s -name 'test-*.yaml' | wc -l)"
echo "Debug files: $(find k8s -name 'debug-*.yaml' | wc -l)"
echo "Working files: $(find k8s -name 'working-*.yaml' | wc -l)"
```

3. **Strategic Removal Categories**

**Category 1: Test/Debug Files**
```bash
# Remove test files (keep in experimental/ if needed)
find k8s -name 'test-*.yaml' -not -path '*/experimental/*' -delete
find k8s -name 'debug-*.yaml' -not -path '*/experimental/*' -delete
```

**Category 2: Outdated/Deprecated Files**
```bash
# Remove clearly outdated files
rm -f k8s/*-old.yaml
rm -f k8s/*-deprecated.yaml
rm -f k8s/*-backup.yaml
```

**Category 3: Duplicate Resources**
```bash
# For each conflict found, keep the most comprehensive version
# Example: enhanced-webapp has 3 files, keep enhanced-webapp-deployment.yaml
rm -f k8s/enhanced-webapp-simple.yaml
rm -f k8s/enhanced-webapp.yaml
# Keep: k8s/enhanced-webapp-deployment.yaml
```

**Category 4: Working/Temporary Files**
```bash
# Remove working files (move to working/ directory if needed)
find k8s -name 'working-*.yaml' -not -path '*/working/*' -delete
find k8s -name 'tmp-*.yaml' -delete
find k8s -name 'temp-*.yaml' -delete
```

4. **Verification**
```bash
# Verify no conflicts remain
python scripts/detect_k8s_conflicts.py k8s/

# Test YAML validity
for file in k8s/*.yaml; do
  echo "Validating $file..."
  kubectl apply -f "$file" --dry-run=client --validate=true
done
```

### Case Study: ATS Platform Cleanup (2025-08-23)

**Initial State:**
- 110 YAML files
- 194 total resources  
- 9 resource conflicts
- Multiple test/debug files mixed with production

**Cleanup Actions:**
```bash
# Removed test files
rm -f k8s/test-*-job.yaml              # 8 files removed
rm -f k8s/debug-*-job.yaml             # 3 files removed

# Resolved duplicates
rm -f k8s/enhanced-analytics-webapp.yaml         # Kept enhanced-webapp-deployment.yaml
rm -f k8s/integrated-analytics-webapp.yaml      # Kept comprehensive-analytics-webapp.yaml
rm -f k8s/enhanced-webapp-simple.yaml           # Duplicate of enhanced-webapp-deployment.yaml

# Removed outdated configs
rm -f k8s/complete-analytics-config.yaml        # Superseded by current configs
rm -f k8s/updated-*.yaml                        # Working files

# Working files cleanup
rm -f k8s/working-*-webapp.yaml                 # 3 files removed
```

**Final State:**
- 89 YAML files (-21 files)
- 175 total resources (-19 resources)
- 0 resource conflicts (✅ Success)
- Clean separation of concerns

**Outcome:**
- ✅ ArgoCD sync issues resolved
- ✅ Improved maintainability
- ✅ Faster deployment cycles
- ✅ Reduced cognitive overhead

## 📁 Directory Organization Best Practices

### Recommended Structure

```bash
k8s/
├── core/                          # Essential services
│   ├── postgres.yaml
│   ├── redis-cache.yaml
│   └── monitoring/
├── analytics/                     # Analytics services
│   ├── enhanced-webapp-deployment.yaml
│   ├── unified-analytics-app.yaml
│   └── comprehensive-analytics-webapp.yaml
├── data/                          # Data processing
│   ├── price-unification-job.yaml
│   ├── enhanced-training-job.yaml
│   └── realtime-collector.yaml
├── jobs/                          # Batch jobs and CronJobs
│   ├── daily-price-job.yaml
│   └── model-training-cronjob.yaml
├── monitoring/                    # Observability
│   ├── prometheus.yaml
│   └── grafana.yaml
├── experimental/                  # Test and development
│   ├── test-*.yaml
│   └── debug-*.yaml
└── working/                       # Temporary files
    └── working-*.yaml
```

### File Naming Conventions

**Components:**
- `<service>-<component>-<environment>.yaml`
- Examples: `analytics-webapp-dev.yaml`, `data-processor-prod.yaml`

**Jobs:**
- `<purpose>-<type>-job.yaml`
- Examples: `price-unification-batch-job.yaml`, `training-cron-job.yaml`

**Services:**
- `<service>-<component>-service.yaml`
- Examples: `analytics-webapp-service.yaml`, `database-primary-service.yaml`

## 🔧 Resource Validation Pipeline

### Pre-Commit Hook

Create `.git/hooks/pre-commit`:

```bash
#!/bin/bash
set -e

echo "🔍 Validating Kubernetes manifests..."

# Check for YAML syntax errors
for file in k8s/**/*.yaml; do
    if ! python3 -c "import yaml; yaml.safe_load_all(open('$file'))" 2>/dev/null; then
        echo "❌ YAML syntax error in $file"
        exit 1
    fi
done

# Check for resource conflicts
if ! python scripts/detect_k8s_conflicts.py k8s/; then
    echo "❌ Resource conflicts detected"
    exit 1
fi

# Validate with kubectl (if available)
if command -v kubectl >/dev/null 2>&1; then
    for file in k8s/**/*.yaml; do
        if ! kubectl apply -f "$file" --dry-run=client --validate=true >/dev/null 2>&1; then
            echo "❌ Kubernetes validation failed for $file"
            exit 1
        fi
    done
fi

echo "✅ All Kubernetes manifests validated successfully"
```

Make it executable:
```bash
chmod +x .git/hooks/pre-commit
```

### GitHub Actions Validation

`.github/workflows/k8s-validation.yml`:

```yaml
name: Kubernetes Manifest Validation

on:
  pull_request:
    paths:
      - 'k8s/**/*.yaml'
  push:
    branches: [ main ]
    paths:
      - 'k8s/**/*.yaml'

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        pip install pyyaml
    
    - name: Validate YAML syntax
      run: |
        for file in k8s/**/*.yaml; do
          echo "Validating $file..."
          python -c "import yaml; list(yaml.safe_load_all(open('$file')))"
        done
    
    - name: Check resource conflicts
      run: |
        python scripts/detect_k8s_conflicts.py k8s/
    
    - name: Setup kubectl
      uses: azure/setup-kubectl@v3
    
    - name: Validate with kubectl
      run: |
        for file in k8s/**/*.yaml; do
          echo "Validating $file with kubectl..."
          kubectl apply -f "$file" --dry-run=client --validate=true
        done
```

## 🎯 Resource Monitoring and Maintenance

### Monthly Maintenance Checklist

```bash
#!/bin/bash
# monthly_k8s_maintenance.sh

echo "📋 Monthly Kubernetes Resource Maintenance"
echo "Date: $(date)"

# 1. Resource conflict audit
echo "1. Checking for resource conflicts..."
python scripts/detect_k8s_conflicts.py k8s/

# 2. File count analysis
echo "2. File count analysis:"
echo "  Total YAML files: $(find k8s -name '*.yaml' | wc -l)"
echo "  Test files: $(find k8s -name 'test-*.yaml' | wc -l)"
echo "  Debug files: $(find k8s -name 'debug-*.yaml' | wc -l)"
echo "  Working files: $(find k8s -name 'working-*.yaml' | wc -l)"

# 3. Resource utilization
echo "3. Resource utilization check:"
kubectl top nodes
kubectl top pods -n ats-dev

# 4. Unused resources detection
echo "4. Checking for unused ConfigMaps and Secrets..."
kubectl get configmaps -n ats-dev --no-headers | while read cm _; do
  if ! grep -r "configMap:\|$cm" k8s/ >/dev/null 2>&1; then
    echo "  Potentially unused ConfigMap: $cm"
  fi
done

# 5. Image updates needed
echo "5. Checking for outdated images..."
grep -r "image:" k8s/ | grep -v "latest" | sort | uniq

echo "✅ Maintenance check complete"
```

## 🚀 Performance Optimization

### Resource Sizing Guidelines

**Small Services** (Analytics dashboards, simple APIs):
```yaml
resources:
  requests:
    memory: "128Mi"
    cpu: "100m"
  limits:
    memory: "256Mi"
    cpu: "200m"
```

**Medium Services** (Data processors, complex webapps):
```yaml
resources:
  requests:
    memory: "256Mi"
    cpu: "200m"
  limits:
    memory: "512Mi"
    cpu: "500m"
```

**Large Services** (ML training, heavy batch jobs):
```yaml
resources:
  requests:
    memory: "1Gi"
    cpu: "500m"
  limits:
    memory: "4Gi"
    cpu: "2"
```

### Health Check Best Practices

**Comprehensive Health Checks:**
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: http
  initialDelaySeconds: 60      # Allow startup time
  periodSeconds: 30           # Check every 30s
  timeoutSeconds: 10          # 10s timeout
  failureThreshold: 3         # Restart after 3 failures

readinessProbe:
  httpGet:
    path: /ready
    port: http
  initialDelaySeconds: 30     # Ready check sooner
  periodSeconds: 10           # Check every 10s
  timeoutSeconds: 5           # Quick timeout
  failureThreshold: 3         # Stop routing after 3 failures
```

## 🔍 Troubleshooting Resource Issues

### Common Issues and Solutions

**Issue: Resource conflicts causing ArgoCD failures**
```bash
# Diagnosis
python scripts/detect_k8s_conflicts.py k8s/

# Solution
# Remove duplicate resources, keep most comprehensive version
# Update resource names to be unique
# Organize files by purpose
```

**Issue: YAML parsing errors**
```bash
# Diagnosis
for file in k8s/*.yaml; do
  python -c "import yaml; list(yaml.safe_load_all(open('$file')))" 2>/dev/null || echo "Error in $file"
done

# Solution
# Fix YAML indentation and syntax
# Remove problematic embedded content
# Validate before committing
```

**Issue: Resource overwhelm (too many files)**
```bash
# Diagnosis
find k8s -name "*.yaml" | wc -l
python scripts/detect_k8s_conflicts.py k8s/

# Solution
# Implement systematic cleanup
# Organize into directories by purpose  
# Remove test/debug files from production directories
```

---

*Last updated: 2025-08-23*  
*Update this document after major resource cleanup operations.*