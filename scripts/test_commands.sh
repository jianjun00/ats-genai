#!/bin/bash
# Test execution commands for local development
# This script provides easy commands to run different test categories

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default Python path
export PYTHONPATH=src

echo_header() {
    echo -e "${BLUE}===================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}===================================================${NC}"
}

echo_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

echo_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

echo_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to run tests with error handling
run_tests() {
    local test_name="$1"
    local cmd="$2"
    
    echo_header "Running $test_name"
    echo "Command: $cmd"
    echo
    
    if eval "$cmd"; then
        echo_success "$test_name completed successfully"
        return 0
    else
        echo_error "$test_name failed"
        return 1
    fi
}

# Main commands
case "${1:-help}" in
    "unit"|"u")
        echo_header "Fast Unit Tests"
        run_tests "Unit Tests" "pytest -m 'unit' --tb=short -v"
        ;;
        
    "integration"|"i")
        echo_header "Integration Tests"
        run_tests "Integration Tests" "pytest -m 'integration and not skip_in_batch' --tb=short --maxfail=3 -v"
        ;;
        
    "gin"|"g")
        echo_header "Gin Configuration Tests (Individual)"
        echo_warning "Running gin tests individually with forked processes"
        run_tests "Gin Tests" "pytest -m 'gin_heavy' --forked --tb=short -v"
        ;;
        
    "database"|"db")
        echo_header "Database Tests"
        run_tests "Database Tests" "pytest -m 'database' --tb=short --maxfail=3 -v"
        ;;
        
    "skip"|"s")
        echo_header "Skip-in-Batch Tests (Individual)"
        echo_warning "Running problematic tests individually"
        run_tests "Skip-in-Batch Tests" "pytest -m 'skip_in_batch' --forked --tb=short -v"
        ;;
        
    "all"|"a")
        echo_header "All Tests (Recommended Order)"
        
        # Run in recommended order with proper isolation
        echo_header "Step 1: Unit Tests"
        if run_tests "Unit Tests" "pytest -m 'unit' --tb=short -v"; then
            echo_success "Unit tests passed, continuing..."
        else
            echo_error "Unit tests failed, stopping"
            exit 1
        fi
        
        echo_header "Step 2: Integration Tests"
        if run_tests "Integration Tests" "pytest -m 'integration and not skip_in_batch' --tb=short --maxfail=3 -v"; then
            echo_success "Integration tests passed, continuing..."
        else
            echo_warning "Integration tests failed, but continuing with gin tests..."
        fi
        
        echo_header "Step 3: Gin Configuration Tests"
        if run_tests "Gin Tests" "pytest -m 'gin_heavy' --forked --tb=short -v"; then
            echo_success "Gin tests passed"
        else
            echo_warning "Gin tests failed"
        fi
        ;;
        
    "individual"|"ind")
        echo_header "All Tests (Individual Execution)"
        echo_warning "Running ALL tests individually - this will take a while"
        run_tests "All Tests Individual" "pytest --forked --tb=short -v"
        ;;
        
    "fast"|"f")
        echo_header "Fast Test Suite"
        echo "Running only fast, reliable tests for quick feedback"
        run_tests "Fast Tests" "pytest -m 'unit and not slow' --tb=short -v"
        ;;
        
    "ci")
        echo_header "CI/CD Simulation"
        echo "Simulating the CI/CD pipeline locally"
        
        # Unit tests
        if run_tests "CI Unit Tests" "pytest -m 'unit' --tb=short --cov=src --cov-report=term-missing -v"; then
            echo_success "CI Unit tests passed"
        else
            echo_error "CI Unit tests failed"
            exit 1
        fi
        
        # Integration tests
        if run_tests "CI Integration Tests" "pytest -m 'integration and not skip_in_batch' --tb=short --maxfail=3 -v"; then
            echo_success "CI Integration tests passed"
        else
            echo_warning "CI Integration tests failed"
        fi
        
        # Gin tests
        if run_tests "CI Gin Tests" "pytest -m 'gin_heavy' --forked --tb=short -v"; then
            echo_success "CI Gin tests passed"
        else
            echo_warning "CI Gin tests failed"
        fi
        ;;
        
    "list"|"l")
        echo_header "Available Test Categories"
        echo "pytest -m 'unit' --collect-only -q | head -20"
        echo "pytest -m 'integration' --collect-only -q | head -20"
        echo "pytest -m 'gin_heavy' --collect-only -q"
        echo "pytest -m 'skip_in_batch' --collect-only -q"
        ;;
        
    "help"|"h"|*)
        echo_header "Test Command Usage"
        echo "Usage: $0 [command]"
        echo
        echo "Available commands:"
        echo "  unit, u          - Run fast unit tests"
        echo "  integration, i   - Run integration tests"
        echo "  gin, g          - Run gin configuration tests (individual)"
        echo "  database, db    - Run database tests"
        echo "  skip, s         - Run skip-in-batch tests (individual)"
        echo "  all, a          - Run all tests in recommended order"
        echo "  individual, ind - Run all tests individually (slow)"
        echo "  fast, f         - Run only fast tests for quick feedback"
        echo "  ci              - Simulate CI/CD pipeline"
        echo "  list, l         - List available test categories"
        echo "  help, h         - Show this help"
        echo
        echo "Examples:"
        echo "  $0 unit              # Quick unit tests"
        echo "  $0 integration       # Integration tests"
        echo "  $0 gin               # Gin config tests"
        echo "  $0 ci                # Full CI simulation"
        echo
        echo "Environment variables:"
        echo "  PYTHONPATH=src (automatically set)"
        echo "  DATABASE_URL (for database tests)"
        ;;
esac