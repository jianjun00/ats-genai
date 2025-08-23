#!/bin/bash
set -euo pipefail

# Start External Analytics Access
# Equivalent to: kubectl port-forward -n ats-dev --address 0.0.0.0 svc/unified-analytics-service 3000:3000
echo "🌐 Starting Analytics Service with External Access"

# Configuration
NAMESPACE="ats-dev"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check which analytics service is available
check_analytics_service() {
    if kubectl get service enhanced-analytics-service -n "$NAMESPACE" &> /dev/null; then
        echo "enhanced-analytics-service"
    elif kubectl get service unified-analytics-service -n "$NAMESPACE" &> /dev/null; then
        echo "unified-analytics-service"  
    else
        echo ""
    fi
}

# Start external analytics access
start_analytics_external() {
    local service_name=$(check_analytics_service)
    
    if [[ -z "$service_name" ]]; then
        print_warning "No analytics service found (enhanced-analytics-service or unified-analytics-service)"
        exit 1
    fi
    
    print_status "Found service: $service_name"
    
    # Kill existing port-forward if running
    pkill -f "kubectl port-forward.*${service_name}.*3000" 2>/dev/null || true
    pkill -f "kubectl port-forward.*3000:3000" 2>/dev/null || true
    
    print_status "Starting external port-forward..."
    
    # Start with external access (equivalent to your command)
    nohup kubectl port-forward -n "$NAMESPACE" --address 0.0.0.0 "service/$service_name" 3000:3000 \
        > "/tmp/analytics-external.log" 2>&1 &
    
    local pid=$!
    echo "$pid" > "/tmp/analytics-external.pid"
    
    print_success "✅ Started $service_name with external access on 0.0.0.0:3000 (PID: $pid)"
    
    # Wait and test
    sleep 3
    
    if curl -f -s "http://localhost:3000" > /dev/null 2>&1; then
        print_success "✅ Analytics service is accessible"
    else
        print_warning "⚠️ Service may still be starting up"
    fi
    
    echo ""
    print_status "🔗 Access URLs:"
    echo "  • External Access:   http://0.0.0.0:3000 (accessible from any IP)"
    echo "  • Local Access:      http://localhost:3000"
    echo "  • From Host Network: http://<YOUR_IP>:3000"
    echo ""
    
    print_status "📋 Management:"
    echo "  • View logs:         tail -f /tmp/analytics-external.log"
    echo "  • Stop service:      kill $pid"
    echo "  • Check process:     ps aux | grep 'kubectl port-forward.*3000'"
    echo ""
    
    print_success "🎉 Analytics external access is now permanent!"
    print_status "Process will continue running in background until manually stopped."
}

# Stop function
stop_analytics_external() {
    print_status "Stopping analytics external access..."
    
    if [[ -f "/tmp/analytics-external.pid" ]]; then
        local pid=$(cat "/tmp/analytics-external.pid")
        if kill "$pid" 2>/dev/null; then
            print_success "Stopped analytics external access (PID: $pid)"
        fi
        rm -f "/tmp/analytics-external.pid"
    fi
    
    # Also kill any matching processes
    pkill -f "kubectl port-forward.*3000:3000" 2>/dev/null || true
    
    rm -f /tmp/analytics-external.log
    print_success "Cleanup completed"
}

# Show status
show_status() {
    print_status "Analytics External Access Status:"
    
    local processes=$(ps aux | grep "kubectl port-forward.*3000" | grep -v grep || echo "")
    if [[ -n "$processes" ]]; then
        echo "$processes"
        echo ""
        print_success "✅ External analytics access is running"
        
        if curl -f -s "http://localhost:3000" > /dev/null 2>&1; then
            print_success "✅ Service is accessible on http://localhost:3000"
        else
            print_warning "⚠️ Service not responding (may be starting)"
        fi
    else
        print_warning "No external analytics access found"
    fi
}

# Main execution
case "${1:-start}" in
    "start")
        start_analytics_external
        ;;
    "stop")
        stop_analytics_external
        ;;
    "status")
        show_status
        ;;
    "restart")
        stop_analytics_external
        sleep 2
        start_analytics_external
        ;;
    *)
        echo "Usage: $0 {start|stop|status|restart}"
        echo ""
        echo "Commands:"
        echo "  start   - Start analytics external access (default)"
        echo "  stop    - Stop analytics external access"
        echo "  status  - Show current status"
        echo "  restart - Restart analytics external access"
        echo ""
        echo "This script provides the equivalent of:"
        echo "kubectl port-forward -n ats-dev --address 0.0.0.0 svc/unified-analytics-service 3000:3000"
        exit 1
        ;;
esac