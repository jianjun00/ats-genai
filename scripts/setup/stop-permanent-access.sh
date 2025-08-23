#!/bin/bash
set -euo pipefail

# Stop Permanent Access Services for ATS
echo "🛑 Stopping Permanent Access for ATS Services"

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

# Function to stop all port-forward services
stop_all_services() {
    print_status "Finding and stopping all port-forward processes..."
    
    # Find all kubectl port-forward processes
    local pids=$(pgrep -f "kubectl port-forward" 2>/dev/null || echo "")
    
    if [[ -n "$pids" ]]; then
        echo "$pids" | while read -r pid; do
            if [[ -n "$pid" ]]; then
                local cmd=$(ps -p "$pid" -o cmd= 2>/dev/null || echo "")
                print_status "Stopping PID $pid: $cmd"
                kill "$pid" 2>/dev/null || true
            fi
        done
        
        # Wait for processes to stop
        sleep 2
        
        # Force kill if still running
        local remaining_pids=$(pgrep -f "kubectl port-forward" 2>/dev/null || echo "")
        if [[ -n "$remaining_pids" ]]; then
            print_warning "Force killing remaining processes..."
            echo "$remaining_pids" | while read -r pid; do
                if [[ -n "$pid" ]]; then
                    kill -9 "$pid" 2>/dev/null || true
                fi
            done
        fi
        
        print_success "All port-forward processes stopped"
    else
        print_warning "No port-forward processes found"
    fi
    
    # Clean up temporary files
    print_status "Cleaning up temporary files..."
    rm -f /tmp/port-forward-*.pid /tmp/port-forward-*.log
    
    print_success "Cleanup completed"
}

# Function to show current status
show_status() {
    print_status "Current port-forward processes:"
    
    local processes=$(ps aux | grep "kubectl port-forward" | grep -v grep || echo "")
    if [[ -n "$processes" ]]; then
        echo "$processes"
        echo ""
        
        print_status "Active port mappings:"
        echo "$processes" | while read -r line; do
            if [[ "$line" =~ ([0-9]+):([0-9]+) ]]; then
                local local_port="${BASH_REMATCH[1]}"
                local remote_port="${BASH_REMATCH[2]}"
                if [[ "$line" =~ service/([a-zA-Z0-9-]+) ]]; then
                    local service="${BASH_REMATCH[1]}"
                    echo "  • $service: localhost:$local_port -> $remote_port"
                fi
            fi
        done
    else
        print_success "No active port-forward processes found"
    fi
    
    echo ""
}

# Function to stop specific service
stop_specific_service() {
    local service_name="$1"
    
    print_status "Stopping port-forward for service: $service_name"
    
    local pids=$(pgrep -f "kubectl port-forward.*$service_name" 2>/dev/null || echo "")
    if [[ -n "$pids" ]]; then
        echo "$pids" | while read -r pid; do
            if [[ -n "$pid" ]]; then
                kill "$pid" 2>/dev/null || true
                print_success "Stopped $service_name (PID: $pid)"
            fi
        done
    else
        print_warning "No port-forward process found for service: $service_name"
    fi
}

# Show usage
show_usage() {
    echo "Usage: $0 [action] [service]"
    echo ""
    echo "Actions:"
    echo "  stop     - Stop all port-forward services (default)"
    echo "  status   - Show current port-forward status"
    echo "  service  - Stop specific service"
    echo ""
    echo "Examples:"
    echo "  $0                           # Stop all services"
    echo "  $0 status                    # Show current status"
    echo "  $0 service ats-minute-service # Stop specific service"
    echo ""
}

# Main execution
main() {
    local action="${1:-stop}"
    
    case "$action" in
        "stop")
            stop_all_services
            ;;
        "status")
            show_status
            ;;
        "service")
            if [[ -n "${2:-}" ]]; then
                stop_specific_service "$2"
            else
                print_error "Service name required for 'service' action"
                show_usage
                exit 1
            fi
            ;;
        "help"|"--help"|"-h")
            show_usage
            ;;
        *)
            print_error "Invalid action: $action"
            show_usage
            exit 1
            ;;
    esac
    
    echo ""
    print_success "🎉 Operation completed!"
}

# Run main function
main "$@"