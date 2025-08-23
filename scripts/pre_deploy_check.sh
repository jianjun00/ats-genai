#!/bin/bash
# Pre-Deployment Safety Check Script
# Validates environment readiness before deployment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="${NAMESPACE:-ats-dev}"
ARGOCD_NAMESPACE="${ARGOCD_NAMESPACE:-argocd}"
APPLICATION_NAME="${APPLICATION_NAME:-ats-dev}"

echo -e "${BLUE}🔍 Pre-deployment Safety Checks${NC}"
echo -e "${BLUE}================================${NC}"
echo ""

# Function to print status
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✅ $2${NC}"
    else
        echo -e "${RED}❌ $2${NC}"
        return 1
    fi
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Track overall status
OVERALL_STATUS=0

# Check 1: Validate YAML syntax
echo -e "${BLUE}📝 Validating YAML syntax...${NC}"
YAML_ERRORS=0
for file in k8s/**/*.yaml; do
    if [ -f "$file" ]; then
        if ! python3 -c "import yaml; list(yaml.safe_load_all(open('$file')))" >/dev/null 2>&1; then
            echo -e "${RED}   ❌ YAML syntax error in: $file${NC}"
            YAML_ERRORS=$((YAML_ERRORS + 1))
        fi
    fi
done

if [ $YAML_ERRORS -eq 0 ]; then
    print_status 0 "All YAML files are syntactically valid"
else
    print_status 1 "$YAML_ERRORS YAML syntax errors found"
    OVERALL_STATUS=1
fi
echo ""

# Check 2: Validate with kubectl
echo -e "${BLUE}🔍 Validating Kubernetes manifests...${NC}"
KUBECTL_ERRORS=0
for file in k8s/**/*.yaml; do
    if [ -f "$file" ]; then
        if ! kubectl apply -f "$file" --dry-run=client --validate=true >/dev/null 2>&1; then
            echo -e "${RED}   ❌ Kubectl validation error in: $file${NC}"
            KUBECTL_ERRORS=$((KUBECTL_ERRORS + 1))
        fi
    fi
done

if [ $KUBECTL_ERRORS -eq 0 ]; then
    print_status 0 "All manifests pass kubectl validation"
else
    print_status 1 "$KUBECTL_ERRORS manifest validation errors found"
    OVERALL_STATUS=1
fi
echo ""

# Check 3: Resource conflicts detection
echo -e "${BLUE}🔍 Checking for resource conflicts...${NC}"
if [ -f "scripts/detect_k8s_conflicts.py" ]; then
    CONFLICT_OUTPUT=$(python3 scripts/detect_k8s_conflicts.py k8s/ 2>&1)
    if echo "$CONFLICT_OUTPUT" | grep -q "NO CONFLICTS FOUND"; then
        print_status 0 "No resource conflicts detected"
    else
        print_status 1 "Resource conflicts found"
        echo "$CONFLICT_OUTPUT" | grep -E "(CONFLICTS FOUND|❌)" | head -10
        OVERALL_STATUS=1
    fi
else
    print_warning "Conflict detection script not found, skipping check"
fi
echo ""

# Check 4: ArgoCD application status
echo -e "${BLUE}🔄 Checking ArgoCD application status...${NC}"
if kubectl get namespace "$ARGOCD_NAMESPACE" >/dev/null 2>&1; then
    SYNC_STATUS=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "Unknown")
    HEALTH_STATUS=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.health.status}' 2>/dev/null || echo "Unknown")
    
    case "$SYNC_STATUS" in
        "Synced")
            print_status 0 "ArgoCD sync status: $SYNC_STATUS"
            ;;
        "OutOfSync")
            print_warning "ArgoCD sync status: $SYNC_STATUS (proceeding with caution)"
            ;;
        *)
            print_status 1 "ArgoCD sync status: $SYNC_STATUS"
            OVERALL_STATUS=1
            ;;
    esac
    
    print_info "ArgoCD health status: $HEALTH_STATUS"
else
    print_warning "ArgoCD namespace not found, skipping ArgoCD checks"
fi
echo ""

# Check 5: Kubernetes cluster connectivity
echo -e "${BLUE}🌐 Testing Kubernetes cluster connectivity...${NC}"
if kubectl cluster-info >/dev/null 2>&1; then
    print_status 0 "Kubernetes cluster is accessible"
else
    print_status 1 "Cannot connect to Kubernetes cluster"
    OVERALL_STATUS=1
fi
echo ""

# Check 6: Namespace existence
echo -e "${BLUE}📦 Checking target namespace...${NC}"
if kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
    print_status 0 "Target namespace '$NAMESPACE' exists"
else
    print_status 1 "Target namespace '$NAMESPACE' does not exist"
    OVERALL_STATUS=1
fi
echo ""

# Check 7: Git status
echo -e "${BLUE}📋 Checking Git repository status...${NC}"
if git status >/dev/null 2>&1; then
    BRANCH=$(git branch --show-current)
    print_info "Current branch: $BRANCH"
    
    if [ "$BRANCH" = "main" ]; then
        print_warning "You're on the main branch. Consider using a feature branch for development."
    fi
    
    if git diff --quiet && git diff --staged --quiet; then
        print_status 0 "Working directory is clean"
    else
        print_status 0 "Working directory has changes (normal for development)"
    fi
else
    print_warning "Not in a Git repository or Git not available"
fi
echo ""

# Check 8: Required tools availability
echo -e "${BLUE}🛠️  Checking required tools...${NC}"
TOOLS_MISSING=0

check_tool() {
    if command -v "$1" >/dev/null 2>&1; then
        print_status 0 "$1 is available"
    else
        print_status 1 "$1 is not available"
        TOOLS_MISSING=$((TOOLS_MISSING + 1))
    fi
}

check_tool "kubectl"
check_tool "python3"
check_tool "git"

if [ $TOOLS_MISSING -gt 0 ]; then
    OVERALL_STATUS=1
fi
echo ""

# Check 9: Team coordination check (if Slack integration exists)
echo -e "${BLUE}👥 Team coordination check...${NC}"
LOCK_FILE="/tmp/ats-dev-deployment.lock"
if [ -f "$LOCK_FILE" ]; then
    LOCK_INFO=$(cat "$LOCK_FILE")
    print_status 1 "Deployment lock exists: $LOCK_INFO"
    echo -e "${YELLOW}   Someone else might be deploying. Check #dev-deployments channel.${NC}"
    OVERALL_STATUS=1
else
    print_status 0 "No deployment locks found"
fi
echo ""

# Summary
echo -e "${BLUE}📊 Summary${NC}"
echo -e "${BLUE}========${NC}"

if [ $OVERALL_STATUS -eq 0 ]; then
    echo -e "${GREEN}✅ All safety checks passed! Ready for deployment.${NC}"
    echo ""
    echo -e "${BLUE}Next steps:${NC}"
    echo "1. Make your changes to deployment files"
    echo "2. Run: ./scripts/dev_deploy.sh"
    echo "3. Monitor: ./scripts/monitor_deployment.sh <service-name>"
    echo ""
    exit 0
else
    echo -e "${RED}❌ Some safety checks failed. Please resolve issues before deploying.${NC}"
    echo ""
    echo -e "${BLUE}Common fixes:${NC}"
    echo "• Fix YAML syntax errors in identified files"
    echo "• Resolve resource conflicts using cleanup scripts"
    echo "• Wait for ArgoCD to reach Synced state"
    echo "• Check team coordination in Slack #dev-deployments"
    echo ""
    exit 1
fi