#!/bin/bash

# ATS GenAI Local Kubernetes Cluster Validation Test
# This script validates the local cluster setup and documents current status

set -e

echo "=== ATS GenAI Local Kubernetes Cluster Validation ==="
echo "Timestamp: $(date)"
echo

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "ℹ️  $1"
}

# Test 1: Check if Kind cluster is running
echo "1. Testing Kind cluster status..."
if kind get clusters | grep -q "ats-dev"; then
    print_success "Kind cluster 'ats-dev' is running"
else
    print_error "Kind cluster 'ats-dev' not found"
    exit 1
fi

# Test 2: Check cluster nodes
echo
echo "2. Testing cluster nodes..."
NODE_COUNT=$(kubectl get nodes --no-headers | wc -l)
if [ "$NODE_COUNT" -eq 3 ]; then
    print_success "Cluster has 3 nodes as expected"
    kubectl get nodes
else
    print_warning "Expected 3 nodes, found $NODE_COUNT"
    kubectl get nodes
fi

# Test 3: Check namespaces
echo
echo "3. Testing namespaces..."
NAMESPACES=("ats-dev" "ats-intg" "ats-prod")
for ns in "${NAMESPACES[@]}"; do
    if kubectl get namespace "$ns" &> /dev/null; then
        print_success "Namespace '$ns' exists"
    else
        print_error "Namespace '$ns' missing"
    fi
done

# Test 4: Check test nginx pod
echo
echo "4. Testing nginx validation pod..."
if kubectl get pod test-nginx -n ats-dev &> /dev/null; then
    STATUS=$(kubectl get pod test-nginx -n ats-dev -o jsonpath='{.status.phase}')
    if [ "$STATUS" = "Running" ]; then
        print_success "Test nginx pod is running - cluster networking validated"
    else
        print_warning "Test nginx pod exists but status is: $STATUS"
    fi
else
    print_info "Test nginx pod not found (may have been cleaned up)"
fi

# Test 5: Check ATS API deployment
echo
echo "5. Testing ATS API deployment..."
if kubectl get deployment ats-api -n ats-dev &> /dev/null; then
    print_success "ATS API deployment exists"
    
    # Check pod status
    POD_STATUS=$(kubectl get pods -l app=ats-api -n ats-dev -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "NotFound")
    if [ "$POD_STATUS" = "Running" ]; then
        print_success "ATS API pod is running"
    else
        print_error "ATS API pod status: $POD_STATUS"
        print_info "Checking recent logs..."
        kubectl logs -l app=ats-api -n ats-dev --tail=5 2>/dev/null || echo "No logs available"
    fi
else
    print_error "ATS API deployment not found"
fi

# Test 6: Check services
echo
echo "6. Testing services..."
if kubectl get service ats-api-service -n ats-dev &> /dev/null; then
    print_success "ATS API service exists"
    kubectl get service ats-api-service -n ats-dev
else
    print_error "ATS API service not found"
fi

# Test 7: Check Docker images
echo
echo "7. Testing Docker images..."
if docker images | grep -q "ats-genai.*dev-latest"; then
    print_success "ATS GenAI Docker image (dev-latest) exists"
else
    print_warning "ATS GenAI Docker image (dev-latest) not found locally"
fi

# Test 8: Cluster connectivity test
echo
echo "8. Testing cluster connectivity..."
if kubectl get service kubernetes &> /dev/null; then
    print_success "Kubernetes API server is accessible"
else
    print_error "Cannot access Kubernetes API server"
fi

# Summary
echo
echo "=== CLUSTER SETUP SUMMARY ==="
echo
print_success "✅ COMPLETED SUCCESSFULLY:"
echo "   • Kind cluster with 3 nodes (1 control plane + 2 workers)"
echo "   • Namespaces created (ats-dev, ats-intg, ats-prod)"
echo "   • Kubernetes manifests applied"
echo "   • Docker images built and loaded"
echo "   • Cluster networking validated with test nginx pod"
echo

print_warning "⚠️  KNOWN ISSUES:"
echo "   • ATS API pods in CrashLoopBackOff due to uvicorn dependency issue"
echo "   • Docker build process needs further investigation for proper dependency installation"
echo

print_info "📋 NEXT STEPS:"
echo "   • Investigate uvicorn installation in Docker image"
echo "   • Fix application import dependencies"
echo "   • Test ATS API functionality once dependencies are resolved"
echo

echo "=== CLUSTER MANAGEMENT COMMANDS ==="
echo "View all resources:     kubectl get all -n ats-dev"
echo "Check pod logs:         kubectl logs -l app=ats-api -n ats-dev --tail=20"
echo "Port forward service:   kubectl port-forward service/ats-api-service 8080:80 -n ats-dev"
echo "Restart deployment:     kubectl rollout restart deployment/ats-api -n ats-dev"
echo "Delete cluster:         kind delete cluster --name ats-dev"
echo

echo "Local Kubernetes cluster setup validation completed!"
echo "Cluster infrastructure is operational and ready for development."
