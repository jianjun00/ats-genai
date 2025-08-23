#!/bin/bash
set -euo pipefail

# Setup Permanent Access for ATS Services
echo "🌐 Setting up permanent access for ATS services"

# Configuration
SETUP_TYPE="${1:-nodeport}"  # nodeport, ingress, or loadbalancer
DOMAIN="${2:-your-domain.com}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl is not installed"
        exit 1
    fi
    
    # Check if services are running
    if ! kubectl get pods -n ats-dev | grep "ats-" | grep -q "Running"; then
        print_error "ATS services are not running in ats-dev namespace"
        exit 1
    fi
    
    print_success "Prerequisites check passed"
}

# Setup NodePort access (recommended for development/local)
setup_nodeport() {
    print_status "Setting up NodePort services for permanent access..."
    
    # Apply NodePort services
    kubectl apply -f k8s/permanent-access/nodeport-services.yaml
    
    # Wait for services to be ready
    sleep 5
    
    print_success "NodePort services deployed successfully!"
    
    # Get node IP
    NODE_IP=$(kubectl get nodes -o wide | awk 'NR==2{print $6}')
    if [[ -z "$NODE_IP" ]]; then
        NODE_IP="localhost"
    fi
    
    echo ""
    print_status "🔗 Permanent Access URLs (NodePort):"
    echo "  • ATS Minute Service:   http://${NODE_IP}:30081/health"
    echo "  • ATS EOD Service:      http://${NODE_IP}:30082/health"
    echo "  • ATS Analytics:        http://${NODE_IP}:30180/health"
    echo "  • Prometheus:           http://${NODE_IP}:30190"
    echo "  • Grafana:              http://${NODE_IP}:30330"
    echo "  • Argo CD:              http://${NODE_IP}:30800"
    echo ""
}

# Setup Ingress access (recommended for production)
setup_ingress() {
    print_status "Setting up Ingress controllers for domain-based access..."
    
    # Check if NGINX Ingress Controller is installed
    if ! kubectl get pods -n ingress-nginx | grep -q "nginx"; then
        print_status "Installing NGINX Ingress Controller..."
        kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.8.2/deploy/static/provider/cloud/deploy.yaml
        
        # Wait for ingress controller to be ready
        kubectl wait --namespace ingress-nginx \
            --for=condition=ready pod \
            --selector=app.kubernetes.io/component=controller \
            --timeout=90s
    fi
    
    # Update domain in ingress files
    sed -i "s/your-domain.com/${DOMAIN}/g" k8s/permanent-access/ingress-controllers.yaml
    
    # Apply ingress rules
    kubectl apply -f k8s/permanent-access/ingress-controllers.yaml
    
    print_success "Ingress controllers deployed successfully!"
    
    echo ""
    print_status "🔗 Permanent Access URLs (Ingress):"
    echo "  • ATS Minute Service:   https://ats-minute.${DOMAIN}/health"
    echo "  • ATS EOD Service:      https://ats-eod.${DOMAIN}/health"
    echo "  • ATS Analytics:        https://ats-analytics.${DOMAIN}/health"
    echo "  • Prometheus:           https://prometheus.${DOMAIN}"
    echo "  • Grafana:              https://grafana.${DOMAIN}"
    echo "  • Argo CD:              https://argocd.${DOMAIN}"
    echo ""
    
    print_warning "Note: Configure DNS records to point these domains to your ingress controller IP"
}

# Setup LoadBalancer access (recommended for cloud production)
setup_loadbalancer() {
    print_status "Setting up LoadBalancer services for cloud access..."
    
    # Apply LoadBalancer services
    kubectl apply -f k8s/permanent-access/loadbalancer-services.yaml
    
    print_status "Waiting for LoadBalancer external IPs to be assigned..."
    
    # Wait for external IPs
    for i in {1..60}; do
        ATS_EXTERNAL_IP=$(kubectl get service ats-services-lb -n ats-dev -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
        MONITORING_EXTERNAL_IP=$(kubectl get service monitoring-lb -n monitoring -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
        
        if [[ -n "$ATS_EXTERNAL_IP" && -n "$MONITORING_EXTERNAL_IP" ]]; then
            break
        fi
        
        echo "Waiting for external IPs... (${i}/60)"
        sleep 5
    done
    
    if [[ -z "$ATS_EXTERNAL_IP" || -z "$MONITORING_EXTERNAL_IP" ]]; then
        print_warning "External IPs not assigned yet. Check cloud provider configuration."
        print_status "Run 'kubectl get services -A' to monitor LoadBalancer status"
    else
        print_success "LoadBalancer services deployed successfully!"
        
        echo ""
        print_status "🔗 Permanent Access URLs (LoadBalancer):"
        echo "  • ATS Services:         http://${ATS_EXTERNAL_IP}/health"
        echo "  • ATS Minute Data:      http://${ATS_EXTERNAL_IP}:8081/health"  
        echo "  • ATS EOD Data:         http://${ATS_EXTERNAL_IP}:8082/health"
        echo "  • Grafana:              http://${MONITORING_EXTERNAL_IP}:3000"
        echo "  • Prometheus:           http://${MONITORING_EXTERNAL_IP}:9090"
        echo ""
    fi
}

# Test permanent access
test_access() {
    print_status "Testing permanent access endpoints..."
    
    case "$SETUP_TYPE" in
        "nodeport")
            NODE_IP=$(kubectl get nodes -o wide | awk 'NR==2{print $6}')
            [[ -z "$NODE_IP" ]] && NODE_IP="localhost"
            
            ENDPOINTS=(
                "http://${NODE_IP}:30081/health"
                "http://${NODE_IP}:30082/health"  
                "http://${NODE_IP}:30180/health"
            )
            ;;
        "ingress")
            ENDPOINTS=(
                "https://ats-minute.${DOMAIN}/health"
                "https://ats-eod.${DOMAIN}/health"
                "https://ats-analytics.${DOMAIN}/health"
            )
            ;;
        "loadbalancer")
            ATS_IP=$(kubectl get service ats-services-lb -n ats-dev -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
            if [[ -n "$ATS_IP" ]]; then
                ENDPOINTS=(
                    "http://${ATS_IP}:8081/health"
                    "http://${ATS_IP}:8082/health"
                    "http://${ATS_IP}/health"
                )
            else
                print_warning "LoadBalancer IPs not yet assigned, skipping tests"
                return
            fi
            ;;
    esac
    
    for endpoint in "${ENDPOINTS[@]}"; do
        if curl -f -s "$endpoint" > /dev/null 2>&1; then
            print_success "✅ $endpoint - Accessible"
        else
            print_warning "⚠️ $endpoint - Not accessible (may need time to propagate)"
        fi
    done
}

# Generate access documentation
generate_access_docs() {
    local doc_file="docs/permanent-access-guide.md"
    
    print_status "Generating access documentation..."
    
    mkdir -p docs
    
    cat > "$doc_file" << EOF
# ATS Services Permanent Access Guide

**Generated:** $(date)
**Setup Type:** $SETUP_TYPE
**Domain:** $DOMAIN

## Overview

This guide provides permanent access methods for ATS services, eliminating the need for port-forwarding.

## Access Methods

### Current Setup: ${SETUP_TYPE^^}

EOF

    case "$SETUP_TYPE" in
        "nodeport")
            NODE_IP=$(kubectl get nodes -o wide | awk 'NR==2{print $6}')
            [[ -z "$NODE_IP" ]] && NODE_IP="localhost"
            
            cat >> "$doc_file" << EOF
#### NodePort Access
- **ATS Minute Service**: http://${NODE_IP}:30081
- **ATS EOD Service**: http://${NODE_IP}:30082
- **ATS Analytics**: http://${NODE_IP}:30180
- **Prometheus**: http://${NODE_IP}:30190
- **Grafana**: http://${NODE_IP}:30330
- **Argo CD**: http://${NODE_IP}:30800

#### Health Check URLs
\`\`\`bash
curl http://${NODE_IP}:30081/health  # Minute Service
curl http://${NODE_IP}:30082/health  # EOD Service
curl http://${NODE_IP}:30180/health  # Analytics Service
\`\`\`
EOF
            ;;
        "ingress")
            cat >> "$doc_file" << EOF
#### Ingress Access (Production URLs)
- **ATS Minute Service**: https://ats-minute.${DOMAIN}
- **ATS EOD Service**: https://ats-eod.${DOMAIN}
- **ATS Analytics**: https://ats-analytics.${DOMAIN}
- **Prometheus**: https://prometheus.${DOMAIN}
- **Grafana**: https://grafana.${DOMAIN}
- **Argo CD**: https://argocd.${DOMAIN}

#### Health Check URLs
\`\`\`bash
curl https://ats-minute.${DOMAIN}/health
curl https://ats-eod.${DOMAIN}/health
curl https://ats-analytics.${DOMAIN}/health
\`\`\`

#### DNS Configuration Required
Point these domains to your ingress controller IP:
\`\`\`
ats-minute.${DOMAIN}    -> <INGRESS_IP>
ats-eod.${DOMAIN}       -> <INGRESS_IP>
ats-analytics.${DOMAIN} -> <INGRESS_IP>
prometheus.${DOMAIN}    -> <INGRESS_IP>
grafana.${DOMAIN}       -> <INGRESS_IP>
argocd.${DOMAIN}        -> <INGRESS_IP>
\`\`\`
EOF
            ;;
        "loadbalancer")
            ATS_IP=$(kubectl get service ats-services-lb -n ats-dev -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "PENDING")
            MONITORING_IP=$(kubectl get service monitoring-lb -n monitoring -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "PENDING")
            
            cat >> "$doc_file" << EOF
#### LoadBalancer Access (Cloud)
- **ATS Services**: http://${ATS_IP}
- **ATS Minute Data**: http://${ATS_IP}:8081
- **ATS EOD Data**: http://${ATS_IP}:8082
- **Grafana**: http://${MONITORING_IP}:3000
- **Prometheus**: http://${MONITORING_IP}:9090

#### Health Check URLs
\`\`\`bash
curl http://${ATS_IP}:8081/health     # Minute Service
curl http://${ATS_IP}:8082/health     # EOD Service  
curl http://${ATS_IP}/health          # Analytics Service
\`\`\`
EOF
            ;;
    esac
    
    cat >> "$doc_file" << EOF

## Management Commands

### Check Service Status
\`\`\`bash
kubectl get services -n ats-dev
kubectl get services -n monitoring
kubectl get ingress -A
\`\`\`

### Monitor Health
\`\`\`bash
# Check all ATS service health
for port in 30081 30082 30180; do
    curl -f http://localhost:\$port/health && echo " ✅ Port \$port OK"
done
\`\`\`

### Troubleshooting
1. **Services not accessible**: Check firewall rules for NodePort ranges (30000-32767)
2. **Ingress not working**: Verify DNS records and ingress controller status
3. **LoadBalancer pending**: Check cloud provider configuration and quotas

### Security Considerations
- NodePort: Ensure firewall rules are properly configured
- Ingress: Use SSL certificates and authentication
- LoadBalancer: Configure security groups and access controls

---
*Generated by ATS Permanent Access Setup*
EOF

    print_success "Documentation generated: $doc_file"
}

# Main execution
main() {
    print_status "Starting permanent access setup for ATS services..."
    echo ""
    
    check_prerequisites
    
    case "$SETUP_TYPE" in
        "nodeport")
            setup_nodeport
            ;;
        "ingress")
            setup_ingress
            ;;
        "loadbalancer")
            setup_loadbalancer
            ;;
        *)
            print_error "Invalid setup type. Use: nodeport, ingress, or loadbalancer"
            exit 1
            ;;
    esac
    
    test_access
    generate_access_docs
    
    echo ""
    print_success "🎉 Permanent access setup completed successfully!"
    echo ""
    print_status "Next steps:"
    echo "  1. Test the access URLs above"
    echo "  2. Configure firewall/security rules as needed"
    echo "  3. Set up monitoring for the new endpoints"
    echo "  4. Update your CI/CD pipelines with permanent URLs"
    echo ""
}

# Show usage if no arguments
if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <setup-type> [domain]"
    echo ""
    echo "Setup Types:"
    echo "  nodeport     - Use NodePort services (recommended for local/dev)"
    echo "  ingress      - Use Ingress controllers (recommended for production)"  
    echo "  loadbalancer - Use LoadBalancer services (recommended for cloud)"
    echo ""
    echo "Examples:"
    echo "  $0 nodeport"
    echo "  $0 ingress ats-prod.company.com"
    echo "  $0 loadbalancer"
    echo ""
    exit 1
fi

# Run main function
main "$@"