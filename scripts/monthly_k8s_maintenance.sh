#!/bin/bash
# Monthly Kubernetes Resource Maintenance Script
# Performs systematic maintenance checks and cleanup recommendations

set -e

echo "📋 Monthly Kubernetes Resource Maintenance"
echo "Date: $(date)"
echo "========================================"

K8S_DIR="${1:-k8s}"
NAMESPACE="${2:-ats-dev}"

if [ ! -d "$K8S_DIR" ]; then
    echo "❌ K8s directory '$K8S_DIR' not found"
    exit 1
fi

echo "🔍 Analyzing directory: $K8S_DIR"
echo "🎯 Target namespace: $NAMESPACE"
echo ""

# 1. Resource conflict audit
echo "1️⃣  RESOURCE CONFLICT AUDIT"
echo "----------------------------"
if [ -f "scripts/detect_k8s_conflicts.py" ]; then
    python scripts/detect_k8s_conflicts.py "$K8S_DIR"
else
    echo "⚠️  Conflict detection script not found"
fi
echo ""

# 2. File count analysis
echo "2️⃣  FILE COUNT ANALYSIS"
echo "------------------------"
echo "  📊 Total YAML files: $(find "$K8S_DIR" -name '*.yaml' | wc -l)"
echo "  🧪 Test files: $(find "$K8S_DIR" -name 'test-*.yaml' | wc -l)"
echo "  🐛 Debug files: $(find "$K8S_DIR" -name 'debug-*.yaml' | wc -l)"
echo "  🔧 Working files: $(find "$K8S_DIR" -name 'working-*.yaml' | wc -l)"
echo "  🗑️  Temp files: $(find "$K8S_DIR" -name 'tmp-*.yaml' -o -name 'temp-*.yaml' | wc -l)"
echo "  📜 Old files: $(find "$K8S_DIR" -name '*-old.yaml' -o -name '*-backup.yaml' -o -name '*-deprecated.yaml' | wc -l)"
echo ""

# 3. Directory size analysis
echo "3️⃣  DIRECTORY SIZE ANALYSIS"
echo "-----------------------------"
TOTAL_SIZE=$(du -sh "$K8S_DIR" | cut -f1)
echo "  📁 Total directory size: $TOTAL_SIZE"

echo "  📊 Largest files:"
find "$K8S_DIR" -name '*.yaml' -exec ls -lh {} + | sort -k5 -hr | head -5 | while read -r line; do
    size=$(echo "$line" | awk '{print $5}')
    file=$(echo "$line" | awk '{print $9}')
    echo "    - $file ($size)"
done
echo ""

# 4. Resource utilization (if kubectl is available)
if command -v kubectl >/dev/null 2>&1; then
    echo "4️⃣  CLUSTER RESOURCE UTILIZATION"
    echo "--------------------------------"
    
    echo "  🖥️  Node resource usage:"
    kubectl top nodes 2>/dev/null | head -5 || echo "    ⚠️  Node metrics not available"
    
    echo "  🐳 Pod resource usage in $NAMESPACE:"
    kubectl top pods -n "$NAMESPACE" 2>/dev/null | head -10 || echo "    ⚠️  Pod metrics not available"
    
    echo "  📦 Pod status in $NAMESPACE:"
    kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | awk '{print $3}' | sort | uniq -c || echo "    ⚠️  Cannot get pod status"
else
    echo "4️⃣  CLUSTER RESOURCE UTILIZATION"
    echo "--------------------------------"
    echo "  ⚠️  kubectl not available - skipping cluster checks"
fi
echo ""

# 5. Unused resources detection
echo "5️⃣  UNUSED RESOURCES DETECTION"
echo "-------------------------------"
if command -v kubectl >/dev/null 2>&1; then
    echo "  🗺️  Checking for unused ConfigMaps..."
    kubectl get configmaps -n "$NAMESPACE" --no-headers 2>/dev/null | while read -r cm _; do
        if ! grep -r "configMap:\|$cm" "$K8S_DIR" >/dev/null 2>&1; then
            echo "    ⚠️  Potentially unused ConfigMap: $cm"
        fi
    done
    
    echo "  🔐 Checking for unused Secrets..."
    kubectl get secrets -n "$NAMESPACE" --no-headers 2>/dev/null | while read -r secret type _; do
        if [[ "$type" != "kubernetes.io/service-account-token" ]] && [[ "$secret" != "default-token-"* ]]; then
            if ! grep -r "secretName:\|$secret" "$K8S_DIR" >/dev/null 2>&1; then
                echo "    ⚠️  Potentially unused Secret: $secret"
            fi
        fi
    done
else
    echo "  ⚠️  kubectl not available - skipping unused resource detection"
fi
echo ""

# 6. Image analysis
echo "6️⃣  CONTAINER IMAGE ANALYSIS"
echo "-----------------------------"
echo "  🐳 Images in use:"
grep -r "image:" "$K8S_DIR" | sed 's/.*image: *//g' | sort | uniq -c | sort -nr | head -10

echo "  ⚠️  Images using 'latest' tag (not recommended for production):"
grep -r "image:.*:latest" "$K8S_DIR" | sed 's/.*image: *//g' | sort | uniq | head -5
echo ""

# 7. Security analysis
echo "7️⃣  SECURITY ANALYSIS"
echo "----------------------"
echo "  🔍 Checking for potential security issues..."

echo "  🔓 Resources without resource limits:"
grep -L "resources:" $(find "$K8S_DIR" -name "*.yaml" -exec grep -l "kind: Deployment\|kind: Job\|kind: CronJob" {} \;) 2>/dev/null | head -5

echo "  🌐 Services with NodePort (external exposure):"
grep -r "type: NodePort" "$K8S_DIR" | cut -d: -f1 | sort | uniq | head -5

echo "  🔑 Hardcoded secrets or API keys (potential issues):"
grep -r -i "password:\|api.*key:\|token:" "$K8S_DIR" | grep -v "valueFrom\|secretKeyRef" | wc -l | xargs -I {} echo "    Found {} potential hardcoded secrets"
echo ""

# 8. Maintenance recommendations
echo "8️⃣  MAINTENANCE RECOMMENDATIONS"
echo "--------------------------------"

# Check if cleanup is needed
TEST_COUNT=$(find "$K8S_DIR" -name 'test-*.yaml' | wc -l)
DEBUG_COUNT=$(find "$K8S_DIR" -name 'debug-*.yaml' | wc -l)
WORKING_COUNT=$(find "$K8S_DIR" -name 'working-*.yaml' | wc -l)
TEMP_COUNT=$(find "$K8S_DIR" -name 'tmp-*.yaml' -o -name 'temp-*.yaml' | wc -l)

TOTAL_CLEANUP_CANDIDATES=$((TEST_COUNT + DEBUG_COUNT + WORKING_COUNT + TEMP_COUNT))

if [ "$TOTAL_CLEANUP_CANDIDATES" -gt 0 ]; then
    echo "  🧹 CLEANUP RECOMMENDED: $TOTAL_CLEANUP_CANDIDATES files can be cleaned up"
    echo "    Run: python scripts/k8s_resource_cleanup.py $K8S_DIR --dry-run"
else
    echo "  ✅ No immediate cleanup needed"
fi

# Check if conflicts exist
if [ -f "scripts/detect_k8s_conflicts.py" ]; then
    if ! python scripts/detect_k8s_conflicts.py "$K8S_DIR" >/dev/null 2>&1; then
        echo "  ⚠️  RESOURCE CONFLICTS DETECTED - Run conflict detection for details"
    else
        echo "  ✅ No resource conflicts detected"
    fi
fi

echo "  📝 Consider the following actions:"
echo "    - Review and update resource limits based on actual usage"
echo "    - Update container images to specific versions (avoid 'latest')"
echo "    - Clean up unused ConfigMaps and Secrets"
echo "    - Archive or remove old deployment files"
echo ""

# 9. Next maintenance reminder
echo "9️⃣  NEXT MAINTENANCE"
echo "--------------------"
NEXT_MONTH=$(date -d "next month" +"%Y-%m")
echo "  📅 Schedule next maintenance for: $NEXT_MONTH"
echo "  📋 Add to calendar: Monthly K8s maintenance check"
echo ""

echo "✅ MAINTENANCE CHECK COMPLETE"
echo "================================"
echo "💡 For detailed cleanup: python scripts/k8s_resource_cleanup.py $K8S_DIR --dry-run"
echo "🔍 For conflict analysis: python scripts/detect_k8s_conflicts.py $K8S_DIR"
echo ""