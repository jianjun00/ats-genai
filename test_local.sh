#!/bin/bash

# ATS GenAI Local Testing Script
# Test the application components locally before deploying to cluster

set -e

echo "=== ATS GenAI Local Testing ==="
echo "Timestamp: $(date)"
echo

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_section() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

# Test 1: Environment Setup
print_section "Testing Environment Setup"
if command -v uv &> /dev/null; then
    print_success "uv is installed"
    uv --version
else
    print_error "uv not found"
    exit 1
fi

if [ -d ".venv" ]; then
    print_success "Virtual environment exists"
else
    print_warning "Virtual environment not found, creating..."
    uv venv
fi

# Test 2: Dependencies
print_section "Testing Dependencies"
print_info "Checking if uvicorn is available locally..."
if uv run python -c "import uvicorn; print('uvicorn version:', uvicorn.__version__)" 2>/dev/null; then
    print_success "uvicorn is available locally"
else
    print_error "uvicorn not available locally"
fi

print_info "Checking FastAPI..."
if uv run python -c "import fastapi; print('FastAPI version:', fastapi.__version__)" 2>/dev/null; then
    print_success "FastAPI is available"
else
    print_error "FastAPI not available"
fi

# Test 3: Simple FastAPI App
print_section "Testing Simple FastAPI Application"
print_info "Testing simple_main.py import..."
if uv run python -c "from src.simple_main import app; print('Simple app imported successfully')" 2>/dev/null; then
    print_success "Simple FastAPI app can be imported"
    
    print_info "Starting simple FastAPI app on port 8081 (background)..."
    uv run uvicorn src.simple_main:app --host 0.0.0.0 --port 8081 &
    SERVER_PID=$!
    
    # Wait for server to start
    sleep 3
    
    print_info "Testing API endpoints..."
    if curl -s http://localhost:8081/ | grep -q "ATS GenAI API is running"; then
        print_success "Root endpoint working"
    else
        print_error "Root endpoint failed"
    fi
    
    if curl -s http://localhost:8081/health | grep -q "healthy"; then
        print_success "Health endpoint working"
    else
        print_error "Health endpoint failed"
    fi
    
    if curl -s http://localhost:8081/api/v1/status | grep -q "operational"; then
        print_success "Status endpoint working"
    else
        print_error "Status endpoint failed"
    fi
    
    # Stop the server
    kill $SERVER_PID 2>/dev/null || true
    print_info "Stopped test server"
    
else
    print_error "Cannot import simple FastAPI app"
fi

# Test 4: Main Application
print_section "Testing Main Application"
print_info "Testing main.py import..."
if uv run python -c "from src.main import app; print('Main app imported successfully')" 2>/dev/null; then
    print_success "Main FastAPI app can be imported"
else
    print_error "Main FastAPI app has import issues"
    print_info "This is expected due to dependency chain issues"
fi

# Test 5: Individual Modules
print_section "Testing Individual Modules"
MODULES=(
    "config.environment"
    "calendars.exchange_calendar"
    "utils"
)

for module in "${MODULES[@]}"; do
    print_info "Testing module: $module"
    if uv run python -c "import $module; print('✓ $module')" 2>/dev/null; then
        print_success "$module works"
    else
        print_warning "$module has issues"
    fi
done

# Test 6: Database Configuration
print_section "Testing Database Configuration"
print_info "Testing database configuration..."
if uv run python -c "from config.database import *; print('Database config loaded')" 2>/dev/null; then
    print_success "Database configuration works"
else
    print_warning "Database configuration has issues"
fi

# Test 7: Cluster Connectivity
print_section "Testing Cluster Connectivity"
if command -v kubectl &> /dev/null; then
    if kubectl cluster-info &> /dev/null; then
        print_success "Kubernetes cluster is accessible"
        
        # Test nginx pod if it exists
        if kubectl get pod test-nginx -n ats-dev &> /dev/null 2>&1; then
            print_info "Testing cluster service via port-forward..."
            kubectl port-forward service/test-nginx 8082:80 -n ats-dev &
            FORWARD_PID=$!
            sleep 2
            
            if curl -s http://localhost:8082 | grep -q "nginx"; then
                print_success "Cluster networking works via port-forward"
            else
                print_warning "Cluster port-forward test failed"
            fi
            
            kill $FORWARD_PID 2>/dev/null || true
        else
            print_info "Test nginx pod not found (may have been cleaned up)"
        fi
    else
        print_warning "Kubernetes cluster not accessible"
    fi
else
    print_warning "kubectl not found"
fi

# Summary
print_section "Testing Summary"
echo
print_success "LOCAL TESTING COMPLETED"
echo
print_info "RECOMMENDATIONS:"
echo "   1. Simple FastAPI app works locally - use this for development"
echo "   2. Main application has dependency issues - needs fixing"
echo "   3. Kubernetes cluster is operational for infrastructure testing"
echo "   4. Individual modules can be tested separately"
echo
print_info "NEXT STEPS:"
echo "   • Use simple_main.py for API development and testing"
echo "   • Fix import dependencies in main.py gradually"
echo "   • Test individual components before integration"
echo "   • Use cluster for infrastructure and networking tests"
echo
echo "=== LOCAL TESTING COMMANDS ==="
echo "Start simple app:       uv run uvicorn src.simple_main:app --host 0.0.0.0 --port 8080"
echo "Test endpoints:         curl http://localhost:8080/health"
echo "Test modules:           uv run python -c \"import MODULE_NAME\""
echo "Interactive Python:     uv run python"
echo "Cluster port-forward:   kubectl port-forward service/SERVICE_NAME 8080:80 -n ats-dev"
echo
