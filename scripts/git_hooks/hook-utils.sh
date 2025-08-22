#!/bin/bash
# Utilities for git hooks

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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

run_tests_with_timeout() {
    local test_name="$1"
    local cmd="$2"
    local timeout_duration="$3"
    
    if timeout "${timeout_duration}" bash -c "$cmd"; then
        return 0
    else
        return 1
    fi
}

run_security_checks() {
    echo_header "Security Checks"
    
    # Check for sensitive patterns
    sensitive_patterns=(
        "password\s*=\s*['\"][^'\"]*['\"]"
        "api_key\s*=\s*['\"][^'\"]*['\"]"
        "secret\s*=\s*['\"][^'\"]*['\"]"
    )
    
    security_issues=0
    for pattern in "${sensitive_patterns[@]}"; do
        if git diff --cached | grep -iE "$pattern" > /dev/null; then
            echo_error "Potential sensitive information: $pattern"
            security_issues=$((security_issues + 1))
        fi
    done
    
    if [ $security_issues -gt 0 ]; then
        echo_error "Security check failed"
        return 1
    fi
    
    echo_success "Security checks passed"
    return 0
}

run_comprehensive_tests_for_main() {
    local branch="$1"
    
    echo_header "Comprehensive Tests for $branch"
    export PYTHONPATH=src
    
    # Core tests
    if ! run_tests_with_timeout "Core Tests" "python -m pytest tests/core/ --tb=short -q" 180; then
        echo_error "Core tests failed - push rejected"
        exit 1
    fi
    
    # Config tests  
    if ! run_tests_with_timeout "Config Tests" "python -m pytest tests/config/ --tb=short -q" 120; then
        echo_error "Config tests failed - push rejected"
        exit 1
    fi
    
    echo_success "All comprehensive tests passed for $branch"
}

run_basic_tests() {
    export PYTHONPATH=src
    
    if ! run_tests_with_timeout "Basic Tests" "python -m pytest tests/core/test_run_context.py::TestRunIdGenerator --tb=short -q" 60; then
        echo_warning "Basic tests failed on feature branch"
    else
        echo_success "Basic tests passed"
    fi
}
