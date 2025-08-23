#!/bin/bash
set -euo pipefail

# Start Permanent Access Services for ATS
# This script creates persistent port-forwards that work with minikube
echo "🌐 Starting Permanent Access for ATS Services"

# Configuration
NAMESPACE="ats-dev"
MONITORING_NAMESPACE="monitoring"
ARGOCD_NAMESPACE="argocd"

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

# Check if running in background
BACKGROUND_MODE="${1:-foreground}"

# Function to start a persistent port-forward
start_port_forward() {
    local namespace="$1"
    local service="$2"
    local local_port="$3"
    local remote_port="$4"
    local service_name="$5"
    local address="${6:-127.0.0.1}"  # Default to localhost, allow override
    
    # Kill existing port-forward if running
    pkill -f "kubectl port-forward.*${service}.*${local_port}" 2>/dev/null || true
    
    if [[ "$BACKGROUND_MODE" == "background" ]]; then
        # Start in background
        nohup kubectl port-forward -n "$namespace" --address "$address" "service/$service" "$local_port:$remote_port" \
            > "/tmp/port-forward-${service}.log" 2>&1 &
        local pid=$!
        echo "$pid" > "/tmp/port-forward-${service}.pid"
        if [[ "$address" == "0.0.0.0" ]]; then
            print_success "✅ Started $service_name on http://0.0.0.0:$local_port (External Access) (PID: $pid)"
        else
            print_success "✅ Started $service_name on http://localhost:$local_port (PID: $pid)"
        fi
    else
        # Start in foreground for testing
        kubectl port-forward -n "$namespace" --address "$address" "service/$service" "$local_port:$remote_port" &
        local pid=$!
        echo "$pid" > "/tmp/port-forward-${service}.pid"
        if [[ "$address" == "0.0.0.0" ]]; then
            print_success "✅ Started $service_name on http://0.0.0.0:$local_port (External Access) (PID: $pid)"
        else
            print_success "✅ Started $service_name on http://localhost:$local_port (PID: $pid)"
        fi
    fi
}

# Function to test service health
test_service_health() {
    local port="$1"
    local service_name="$2"
    local endpoint="${3:-/health}"
    
    # Wait for port-forward to be ready
    sleep 2
    
    if curl -f -s "http://localhost:$port$endpoint" > /dev/null 2>&1; then
        print_success "✅ $service_name - Health check passed"
    else
        print_warning "⚠️ $service_name - Health check failed (service may be starting)"
    fi
}

# Main function to start all services
start_permanent_access() {
    print_status "Starting permanent access for ATS services..."
    
    # ATS Services
    print_status "Starting ATS service access..."
    start_port_forward "$NAMESPACE" "ats-minute-service" "8081" "8081" "ATS Minute Service"
    start_port_forward "$NAMESPACE" "ats-eod-service" "8082" "8082" "ATS EOD Service"
    start_port_forward "$NAMESPACE" "ats-analytics-service" "8080" "8080" "ATS Analytics Service"
    
    # Enhanced Analytics Service with External Access (replaces unified analytics)
    if kubectl get service enhanced-analytics-service -n "$NAMESPACE" &> /dev/null; then
        print_status "Starting Enhanced Analytics service with external access..."
        start_port_forward "$NAMESPACE" "enhanced-analytics-service" "3000" "3000" "Enhanced Analytics Service" "0.0.0.0"
    elif kubectl get service unified-analytics-service -n "$NAMESPACE" &> /dev/null; then
        print_status "Starting Unified Analytics service with external access..."
        start_port_forward "$NAMESPACE" "unified-analytics-service" "3000" "3000" "Unified Analytics Service" "0.0.0.0"
    fi
    
    # Monitoring Services (if they exist)
    if kubectl get namespace "$MONITORING_NAMESPACE" &> /dev/null; then
        print_status "Starting monitoring service access..."
        if kubectl get service prometheus -n "$MONITORING_NAMESPACE" &> /dev/null; then
            start_port_forward "$MONITORING_NAMESPACE" "prometheus" "9090" "9090" "Prometheus"
        fi
        if kubectl get service grafana -n "$MONITORING_NAMESPACE" &> /dev/null; then
            start_port_forward "$MONITORING_NAMESPACE" "grafana" "3000" "3000" "Grafana"
        fi
    fi
    
    # Argo CD Service (if it exists)
    if kubectl get namespace "$ARGOCD_NAMESPACE" &> /dev/null; then
        print_status "Starting Argo CD access..."
        if kubectl get service argocd-server -n "$ARGOCD_NAMESPACE" &> /dev/null; then
            start_port_forward "$ARGOCD_NAMESPACE" "argocd-server" "8888" "80" "Argo CD Server"
        fi
    fi
    
    # Wait for all services to be ready
    print_status "Waiting for services to be ready..."
    sleep 5
    
    # Test health endpoints
    print_status "Testing service health..."
    test_service_health "8081" "ATS Minute Service"
    test_service_health "8082" "ATS EOD Service" 
    test_service_health "8080" "ATS Analytics Service"
    
    # Test enhanced/unified analytics if it exists
    if kubectl get service enhanced-analytics-service -n "$NAMESPACE" &> /dev/null; then
        test_service_health "3000" "Enhanced Analytics Service" "/"
    elif kubectl get service unified-analytics-service -n "$NAMESPACE" &> /dev/null; then
        test_service_health "3000" "Unified Analytics Service" "/"
    fi
    
    test_service_health "9090" "Prometheus" "/-/healthy"
    if ! kubectl get service enhanced-analytics-service -n "$NAMESPACE" &> /dev/null && ! kubectl get service unified-analytics-service -n "$NAMESPACE" &> /dev/null; then
        test_service_health "3000" "Grafana" "/api/health"
    fi
    test_service_health "8888" "Argo CD" "/"
    
    echo ""
    print_success "🎉 Permanent access setup completed!"
    
    # Display access information
    display_access_info
}

# Display access information
display_access_info() {
    echo ""
    print_status "🔗 Permanent Access URLs:"
    echo "==========================================="
    echo "  🏦 ATS Services:"
    echo "    • Minute Service:    http://localhost:8081/health"
    echo "    • EOD Service:       http://localhost:8082/health"  
    echo "    • Analytics Service: http://localhost:8080/health"
    
    # Display enhanced/unified analytics if it exists
    if kubectl get service enhanced-analytics-service -n "$NAMESPACE" &> /dev/null; then
        echo ""
        echo "  🌐 Enhanced Analytics (External Access):"
        echo "    • Enhanced Analytics: http://0.0.0.0:3000 (accessible from any IP)"
        echo "    • Local Access:       http://localhost:3000"
    elif kubectl get service unified-analytics-service -n "$NAMESPACE" &> /dev/null; then
        echo ""
        echo "  🌐 Unified Analytics (External Access):"
        echo "    • Unified Analytics: http://0.0.0.0:3000 (accessible from any IP)"
        echo "    • Local Access:      http://localhost:3000"
    fi
    
    echo ""
    echo "  📊 Monitoring:"
    echo "    • Prometheus:        http://localhost:9090"
    if ! kubectl get service enhanced-analytics-service -n "$NAMESPACE" &> /dev/null && ! kubectl get service unified-analytics-service -n "$NAMESPACE" &> /dev/null; then
        echo "    • Grafana:           http://localhost:3000"
    fi
    echo ""
    echo "  🚀 Deployment:"
    echo "    • Argo CD:           http://localhost:8888"
    echo ""
    
    print_status "📋 Management Commands:"
    echo "  • Stop all:           ./scripts/setup/stop-permanent-access.sh"
    echo "  • Check status:       ps aux | grep 'kubectl port-forward'"
    echo "  • View logs:          tail -f /tmp/port-forward-*.log"
    echo ""
    
    if [[ "$BACKGROUND_MODE" == "foreground" ]]; then
        print_warning "Running in foreground mode. Press Ctrl+C to stop all services."
        print_status "To run in background mode, use: $0 background"
        echo ""
        
        # Keep script running in foreground
        print_status "Services are running. Press Ctrl+C to stop..."
        trap 'stop_all_services' INT
        while true; do
            sleep 1
        done
    else
        print_success "All services are running in background mode."
        print_status "Services will continue running until you run the stop script."
    fi
}

# Function to stop all services
stop_all_services() {
    echo ""
    print_status "Stopping all permanent access services..."
    
    # Find and kill all kubectl port-forward processes
    if pgrep -f "kubectl port-forward" > /dev/null; then
        pkill -f "kubectl port-forward"
        print_success "Stopped all port-forward processes"
    fi
    
    # Clean up PID files
    rm -f /tmp/port-forward-*.pid /tmp/port-forward-*.log
    
    print_success "All services stopped."
    exit 0
}

# Show usage
show_usage() {
    echo "Usage: $0 [mode]"
    echo ""
    echo "Modes:"
    echo "  foreground  - Run in foreground (default, press Ctrl+C to stop)"
    echo "  background  - Run in background (use stop-permanent-access.sh to stop)"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run in foreground"
    echo "  $0 background         # Run in background"
    echo ""
}

# Main execution
main() {
    if [[ "${1:-}" == "help" || "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
        show_usage
        exit 0
    fi
    
    # Check prerequisites
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl is not installed"
        exit 1
    fi
    
    # Check if ATS services are running
    if ! kubectl get pods -n "$NAMESPACE" | grep "ats-" | grep -q "Running"; then
        print_error "ATS services are not running in $NAMESPACE namespace"
        exit 1
    fi
    
    start_permanent_access
}

# Run main function
main "$@"