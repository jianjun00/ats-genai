#!/bin/bash

# ArgoCD Permanent Access Manager
# Provides permanent kubectl port-forward for ArgoCD UI access

set -euo pipefail

# Configuration
ARGOCD_LOCAL_PORT=${ARGOCD_LOCAL_PORT:-8080}
ARGOCD_SERVICE_PORT=${ARGOCD_SERVICE_PORT:-443}
ARGOCD_NAMESPACE=${ARGOCD_NAMESPACE:-argocd}
ARGOCD_SERVICE=${ARGOCD_SERVICE:-argocd-server}
ADDRESS=${ADDRESS:-0.0.0.0}

# PID file location
PID_DIR="/tmp/argocd-access"
PID_FILE="$PID_DIR/port-forward.pid"

# Create PID directory
mkdir -p "$PID_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

# Function to check if ArgoCD service is available
check_argocd_service() {
    print_status "Checking ArgoCD service availability..."
    
    if ! kubectl get svc "$ARGOCD_SERVICE" -n "$ARGOCD_NAMESPACE" &>/dev/null; then
        print_error "ArgoCD service '$ARGOCD_SERVICE' not found in namespace '$ARGOCD_NAMESPACE'"
        print_error "Available services:"
        kubectl get svc -n "$ARGOCD_NAMESPACE" 2>/dev/null || echo "  No services found or namespace doesn't exist"
        return 1
    fi
    
    print_status "✅ ArgoCD service found: $ARGOCD_SERVICE"
    return 0
}

# Function to check if port is already in use
check_port_usage() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        local pid=$(lsof -Pi :$port -sTCP:LISTEN -t)
        local process=$(ps -p $pid -o comm= 2>/dev/null || echo "unknown")
        print_warning "Port $port is already in use by PID $pid ($process)"
        return 1
    fi
    return 0
}

# Function to start port-forward
start_port_forward() {
    print_header "Starting ArgoCD Permanent Access"
    
    # Check if ArgoCD service exists
    if ! check_argocd_service; then
        return 1
    fi
    
    # Check if already running
    if [[ -f "$PID_FILE" ]]; then
        local old_pid=$(cat "$PID_FILE")
        if kill -0 "$old_pid" 2>/dev/null; then
            print_warning "ArgoCD access already running with PID $old_pid"
            print_status "ArgoCD UI: https://localhost:$ARGOCD_LOCAL_PORT"
            return 0
        else
            print_warning "Removing stale PID file"
            rm -f "$PID_FILE"
        fi
    fi
    
    # Check if port is available
    if ! check_port_usage "$ARGOCD_LOCAL_PORT"; then
        print_error "Cannot start ArgoCD access - port $ARGOCD_LOCAL_PORT is in use"
        return 1
    fi
    
    print_status "Starting kubectl port-forward..."
    print_status "Service: $ARGOCD_SERVICE:$ARGOCD_SERVICE_PORT"
    print_status "Local access: https://$ADDRESS:$ARGOCD_LOCAL_PORT"
    
    # Start port-forward in background
    nohup kubectl port-forward \
        --address "$ADDRESS" \
        svc/"$ARGOCD_SERVICE" \
        -n "$ARGOCD_NAMESPACE" \
        "$ARGOCD_LOCAL_PORT:$ARGOCD_SERVICE_PORT" \
        > "$PID_DIR/port-forward.log" 2>&1 &
    
    local pid=$!
    echo "$pid" > "$PID_FILE"
    
    # Wait a moment to check if it started successfully
    sleep 3
    
    if kill -0 "$pid" 2>/dev/null; then
        print_status "✅ ArgoCD access started successfully!"
        print_status "🌐 ArgoCD UI: https://localhost:$ARGOCD_LOCAL_PORT"
        print_status "📁 PID: $pid (saved to $PID_FILE)"
        print_status "📋 Logs: $PID_DIR/port-forward.log"
        
        # Show login instructions
        echo ""
        print_header "ArgoCD Login Instructions"
        print_status "1. Open browser: https://localhost:$ARGOCD_LOCAL_PORT"
        print_status "2. Accept self-signed certificate"
        print_status "3. Get admin password:"
        echo "   kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath=\"{.data.password}\" | base64 -d"
        print_status "4. Login with username: admin"
        
        return 0
    else
        print_error "Failed to start ArgoCD access"
        rm -f "$PID_FILE"
        return 1
    fi
}

# Function to stop port-forward
stop_port_forward() {
    print_header "Stopping ArgoCD Access"
    
    if [[ ! -f "$PID_FILE" ]]; then
        print_warning "No PID file found. ArgoCD access may not be running."
        return 1
    fi
    
    local pid=$(cat "$PID_FILE")
    
    if kill -0 "$pid" 2>/dev/null; then
        print_status "Stopping ArgoCD access (PID: $pid)..."
        kill "$pid"
        sleep 2
        
        if kill -0 "$pid" 2>/dev/null; then
            print_warning "Process still running, force killing..."
            kill -9 "$pid"
        fi
        
        rm -f "$PID_FILE"
        print_status "✅ ArgoCD access stopped"
    else
        print_warning "Process not running, cleaning up PID file"
        rm -f "$PID_FILE"
    fi
}

# Function to show status
show_status() {
    print_header "ArgoCD Access Status"
    
    if [[ -f "$PID_FILE" ]]; then
        local pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            print_status "✅ Running (PID: $pid)"
            print_status "🌐 ArgoCD UI: https://localhost:$ARGOCD_LOCAL_PORT"
            print_status "📋 Logs: $PID_DIR/port-forward.log"
            
            # Show recent logs
            if [[ -f "$PID_DIR/port-forward.log" ]]; then
                echo ""
                print_status "Recent logs:"
                tail -5 "$PID_DIR/port-forward.log" | sed 's/^/  /'
            fi
        else
            print_warning "❌ Not running (stale PID file)"
            rm -f "$PID_FILE"
        fi
    else
        print_warning "❌ Not running"
    fi
    
    # Show port usage
    echo ""
    print_status "Port $ARGOCD_LOCAL_PORT usage:"
    if lsof -Pi :$ARGOCD_LOCAL_PORT -sTCP:LISTEN 2>/dev/null; then
        echo "  Port is in use"
    else
        echo "  Port is available"
    fi
}

# Function to restart port-forward
restart_port_forward() {
    print_header "Restarting ArgoCD Access"
    stop_port_forward
    sleep 2
    start_port_forward
}

# Function to show logs
show_logs() {
    if [[ -f "$PID_DIR/port-forward.log" ]]; then
        print_status "ArgoCD access logs:"
        tail -20 "$PID_DIR/port-forward.log"
    else
        print_warning "No log file found"
    fi
}

# Function to get admin password
get_admin_password() {
    print_header "ArgoCD Admin Password"
    
    if kubectl get secret argocd-initial-admin-secret -n argocd &>/dev/null; then
        local password=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d)
        print_status "Username: admin"
        print_status "Password: $password"
    else
        print_error "Admin secret not found. You may need to reset the password."
        print_status "To reset password:"
        echo "  kubectl -n argocd patch secret argocd-secret -p '{\"stringData\": {\"admin.password\": \"$2a$10$rRyBsGSHK6.uc8fntPwVIuLVHgsAhAX7TcdrqW/RADU0uh7CaChLa\",\"admin.passwordMtime\": \"$(date +%FT%T%Z)\"}}'"
        echo "  Default password after reset: password"
    fi
}

# Main script logic
case "${1:-}" in
    start)
        start_port_forward
        ;;
    stop)
        stop_port_forward
        ;;
    restart)
        restart_port_forward
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    password)
        get_admin_password
        ;;
    *)
        print_header "ArgoCD Permanent Access Manager"
        echo "Usage: $0 {start|stop|restart|status|logs|password}"
        echo ""
        echo "Commands:"
        echo "  start    - Start permanent ArgoCD access"
        echo "  stop     - Stop ArgoCD access"
        echo "  restart  - Restart ArgoCD access"
        echo "  status   - Show current status"
        echo "  logs     - Show recent logs"
        echo "  password - Get admin login password"
        echo ""
        echo "Environment Variables:"
        echo "  ARGOCD_LOCAL_PORT     - Local port (default: 8080)"
        echo "  ARGOCD_SERVICE_PORT   - Service port (default: 443)"
        echo "  ARGOCD_NAMESPACE      - Namespace (default: argocd)"
        echo "  ADDRESS               - Bind address (default: 0.0.0.0)"
        echo ""
        echo "Access URL: https://localhost:$ARGOCD_LOCAL_PORT"
        exit 1
        ;;
esac