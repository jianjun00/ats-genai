#!/bin/bash
# Pre-commit hook logic
# Shared across all team members

set -e

# Import hook utilities
REPO_ROOT=$(git rev-parse --show-toplevel)
source "$REPO_ROOT/scripts/git_hooks/hook-utils.sh"

echo_header "Pre-commit Hook: Running Tests"

# Check if this is a merge commit (skip tests for merge commits)
if git rev-parse -q --verify MERGE_HEAD > /dev/null; then
    echo_warning "Merge commit detected, skipping pre-commit tests"
    exit 0
fi

# Skip tests if NO_VERIFY environment variable is set
if [ "$NO_VERIFY" = "1" ]; then
    echo_warning "NO_VERIFY=1 set, skipping pre-commit tests"
    exit 0
fi

# Get current branch
BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "Running on branch: $BRANCH"

# Set Python path
export PYTHONPATH=src

# Run fast unit tests (required for all commits)
echo_header "Fast Unit Tests (Required)"
if run_tests_with_timeout "Unit Tests" "python -m pytest tests/core/ tests/config/test_logging_config.py tests/signals/test_indicator.py --tb=short -q" 120; then
    echo_success "Fast unit tests passed"
else
    echo_error "Fast unit tests failed - commit rejected"
    echo ""
    echo "To bypass: git commit --no-verify (not recommended)"
    echo "To fix: ./scripts/test_commands.sh unit"
    exit 1
fi

# Security checks
run_security_checks

echo_success "Pre-commit checks completed"
