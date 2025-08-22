#!/bin/bash
# Setup script for git hooks to ensure all team members have the same hooks
# This script should be run after cloning the repository

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo_error "Not in a git repository"
    exit 1
fi

REPO_ROOT=$(git rev-parse --show-toplevel)
HOOKS_DIR="$REPO_ROOT/.git/hooks"
SHARED_HOOKS_DIR="$REPO_ROOT/scripts/git_hooks"

echo_header "Git Hooks Setup"
echo "Repository root: $REPO_ROOT"
echo "Git hooks directory: $HOOKS_DIR"

# Create shared hooks directory if it doesn't exist
mkdir -p "$SHARED_HOOKS_DIR"

# Function to create/update a hook
setup_hook() {
    local hook_name="$1"
    local hook_source="$2"
    
    echo "Setting up $hook_name hook..."
    
    # Copy the hook
    cp "$hook_source" "$HOOKS_DIR/$hook_name"
    chmod +x "$HOOKS_DIR/$hook_name"
    
    echo_success "$hook_name hook installed"
}

# Check if hooks already exist and prompt for overwrite
hooks_exist=false
for hook in pre-commit pre-push; do
    if [ -f "$HOOKS_DIR/$hook" ]; then
        hooks_exist=true
        break
    fi
done

if [ "$hooks_exist" = true ]; then
    echo_warning "Git hooks already exist"
    echo "This will overwrite existing hooks. Continue? (y/N)"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        echo "Setup cancelled"
        exit 0
    fi
fi

# Create the hooks directory if it doesn't exist
mkdir -p "$HOOKS_DIR"

# Store hooks in shared directory for version control
echo_header "Creating Shared Hook Templates"

# Create pre-commit hook template
cat > "$SHARED_HOOKS_DIR/pre-commit" << 'EOF'
#!/bin/bash
# Pre-commit hook template
# This file is version controlled and shared across the team

# Source the actual pre-commit logic
REPO_ROOT=$(git rev-parse --show-toplevel)
source "$REPO_ROOT/scripts/git_hooks/pre-commit-logic.sh"
EOF

# Create pre-push hook template  
cat > "$SHARED_HOOKS_DIR/pre-push" << 'EOF'
#!/bin/bash
# Pre-push hook template
# This file is version controlled and shared across the team

# Source the actual pre-push logic
REPO_ROOT=$(git rev-parse --show-toplevel)
source "$REPO_ROOT/scripts/git_hooks/pre-push-logic.sh"
EOF

# Create the actual hook logic files
cat > "$SHARED_HOOKS_DIR/pre-commit-logic.sh" << 'EOF'
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
EOF

cat > "$SHARED_HOOKS_DIR/pre-push-logic.sh" << 'EOF'
#!/bin/bash
# Pre-push hook logic
# Shared across all team members

set -e

# Import hook utilities
REPO_ROOT=$(git rev-parse --show-toplevel)
source "$REPO_ROOT/scripts/git_hooks/hook-utils.sh"

# Read stdin for refs being pushed
while read local_ref local_sha remote_ref remote_sha; do
    # Skip if deleting a branch
    if [ "$local_sha" = "0000000000000000000000000000000000000000" ]; then
        continue
    fi
    
    # Extract branch name
    branch=$(echo "$remote_ref" | sed 's|refs/heads/||')
    
    echo_header "Pre-push Hook: Validating push to '$branch'"
    
    # Master/main branch protection
    if [[ "$branch" == "main" || "$branch" == "master" ]]; then
        echo_warning "Pushing to protected branch: $branch"
        
        if [ "$NO_VERIFY" = "1" ]; then
            echo_warning "NO_VERIFY=1 set, bypassing protection (DANGEROUS!)"
            continue
        fi
        
        # Run comprehensive tests for main/master
        run_comprehensive_tests_for_main "$branch"
        
    else
        echo_success "Feature branch '$branch' - basic validation"
        run_basic_tests
    fi
done

echo_success "Pre-push checks completed"
EOF

# Create utilities file
cat > "$SHARED_HOOKS_DIR/hook-utils.sh" << 'EOF'
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
EOF

# Make all shared hooks executable
chmod +x "$SHARED_HOOKS_DIR"/*

# Install the hooks
echo_header "Installing Git Hooks"

setup_hook "pre-commit" "$SHARED_HOOKS_DIR/pre-commit"
setup_hook "pre-push" "$SHARED_HOOKS_DIR/pre-push"

# Test the hooks
echo_header "Testing Hook Installation"

if [ -x "$HOOKS_DIR/pre-commit" ]; then
    echo_success "Pre-commit hook is executable"
else
    echo_error "Pre-commit hook is not executable"
    exit 1
fi

if [ -x "$HOOKS_DIR/pre-push" ]; then
    echo_success "Pre-push hook is executable"
else
    echo_error "Pre-push hook is not executable"
    exit 1
fi

# Add hooks to git tracking (shared templates only)
echo_header "Adding Shared Hooks to Version Control"
git add "$SHARED_HOOKS_DIR/"

echo_header "Git Hooks Setup Complete"
echo_success "All git hooks have been installed successfully"
echo ""
echo "Next steps:"
echo "1. Commit the shared hook templates:"
echo "   git commit -m 'feat(git): add shared git hooks for test enforcement'"
echo ""
echo "2. All team members should run this script after cloning:"
echo "   ./scripts/setup_git_hooks.sh"
echo ""
echo "3. To bypass hooks in emergencies (not recommended):"
echo "   git commit --no-verify"
echo "   git push --no-verify"
echo ""
echo "4. To test the hooks:"
echo "   echo 'test' > test.txt && git add test.txt && git commit -m 'test commit'"
EOF