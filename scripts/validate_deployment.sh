#!/bin/bash
# Deployment Validation Script
# Validates specific deployment files before applying

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

if [ $# -eq 0 ]; then
    echo -e "${RED}Usage: $0 <deployment-file.yaml> [<deployment-file2.yaml> ...]${NC}"
    echo ""
    echo "Examples:"
    echo "  $0 k8s/analytics-service/deployment.yaml"
    echo "  $0 k8s/**/*deployment*.yaml"
    exit 1
fi

print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✅ $2${NC}"
    else
        echo -e "${RED}❌ $2${NC}"
        return 1
    fi
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

OVERALL_STATUS=0

echo -e "${BLUE}🔍 Validating Deployment Files${NC}"
echo -e "${BLUE}==============================${NC}"
echo ""

for FILE in "$@"; do
    echo -e "${BLUE}📄 Validating: $FILE${NC}"
    
    # Check if file exists
    if [ ! -f "$FILE" ]; then
        print_status 1 "File does not exist: $FILE"
        OVERALL_STATUS=1
        continue
    fi
    
    # YAML syntax validation
    if python3 -c "import yaml; list(yaml.safe_load_all(open('$FILE')))" >/dev/null 2>&1; then
        print_status 0 "YAML syntax is valid"
    else
        print_status 1 "YAML syntax error"
        OVERALL_STATUS=1
        continue
    fi
    
    # Kubernetes validation
    if kubectl apply -f "$FILE" --dry-run=client --validate=true >/dev/null 2>&1; then
        print_status 0 "Kubernetes validation passed"
    else
        print_status 1 "Kubernetes validation failed"
        echo -e "${RED}   Error details:${NC}"
        kubectl apply -f "$FILE" --dry-run=client --validate=true 2>&1 | head -5 | sed 's/^/     /'
        OVERALL_STATUS=1
        continue
    fi
    
    # Extract resource information
    RESOURCES=$(kubectl apply -f "$FILE" --dry-run=client -o name 2>/dev/null | wc -l)
    print_info "Contains $RESOURCES Kubernetes resource(s)"
    
    # Check for common issues
    echo -e "${BLUE}   🔍 Checking deployment best practices...${NC}"
    
    # Check for resource limits
    if grep -q "resources:" "$FILE"; then
        if grep -q "limits:" "$FILE" && grep -q "requests:" "$FILE"; then
            print_status 0 "Resource limits and requests defined"
        else
            print_warning "Missing resource limits or requests"
        fi
    else
        print_warning "No resource limits defined"
    fi
    
    # Check for health checks
    if grep -q "livenessProbe:\|readinessProbe:" "$FILE"; then
        print_status 0 "Health checks defined"
    else
        print_warning "No health checks (livenessProbe/readinessProbe) defined"
    fi
    
    # Check for image tags (avoid 'latest')
    if grep -q "image:.*:latest" "$FILE"; then
        print_warning "Using 'latest' image tag (not recommended for production)"
    elif grep -q "image:.*:" "$FILE"; then
        print_status 0 "Specific image tag used"
    fi
    
    # Check for security context
    if grep -q "securityContext:" "$FILE"; then
        print_status 0 "Security context defined"
    else
        print_warning "No security context defined"
    fi
    
    # NodePort range validation
    if grep -q "nodePort:" "$FILE"; then
        NODEPORTS=$(grep -o "nodePort: [0-9]*" "$FILE" | grep -o "[0-9]*")
        for PORT in $NODEPORTS; do
            if [ "$PORT" -ge 30000 ] && [ "$PORT" -le 32767 ]; then
                print_status 0 "NodePort $PORT is in valid range (30000-32767)"
            else
                print_status 1 "NodePort $PORT is outside valid range (30000-32767)"
                OVERALL_STATUS=1
            fi
        done
    fi
    
    echo ""
done

# Summary
echo -e "${BLUE}📊 Validation Summary${NC}"
echo -e "${BLUE}====================${NC}"

if [ $OVERALL_STATUS -eq 0 ]; then
    echo -e "${GREEN}✅ All deployment files validated successfully!${NC}"
    echo ""
    echo -e "${BLUE}Ready for deployment:${NC}"
    for FILE in "$@"; do
        echo "  • $FILE"
    done
    echo ""
    exit 0
else
    echo -e "${RED}❌ Validation failed for one or more files.${NC}"
    echo ""
    echo -e "${BLUE}Please fix the issues above before deploying.${NC}"
    echo ""
    exit 1
fi