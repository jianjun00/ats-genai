#!/bin/bash
# Repository Cleanup Automation Script
# This script performs comprehensive repository cleanup operations

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_FILE="/tmp/repo_cleanup_$(date +%Y%m%d_%H%M%S).log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_FILE"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}" | tee -a "$LOG_FILE"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "${RED}❌ $1${NC}" | tee -a "$LOG_FILE"
}

log_info() {
    echo -e "${BLUE}🔍 $1${NC}" | tee -a "$LOG_FILE"
}

# Check if we're in the repository root
check_repository() {
    if [[ ! -f "$REPO_ROOT/.gitignore" ]] || [[ ! -d "$REPO_ROOT/.git" ]]; then
        log_error "Not in repository root directory"
        exit 1
    fi
    log_success "Repository root confirmed: $REPO_ROOT"
}

# Clean Python artifacts
clean_python_artifacts() {
    log_info "Cleaning Python artifacts..."

    cd "$REPO_ROOT"

    # Count before cleanup
    local pyc_count=$(find . -name "*.pyc" -type f | wc -l || echo 0)
    local pycache_count=$(find . -name "__pycache__" -type d | wc -l || echo 0)
    local pyo_count=$(find . -name "*.pyo" -type f | wc -l || echo 0)

    log_info "Found: $pyc_count .pyc files, $pycache_count __pycache__ dirs, $pyo_count .pyo files"

    # Remove Python artifacts
    find . -name "*.pyc" -type f -delete 2>/dev/null || true
    find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    find . -name "*.pyo" -type f -delete 2>/dev/null || true
    find . -name ".pytest_cache" -type d -exec rm -rf {} + 2>/dev/null || true
    find . -name ".mypy_cache" -type d -exec rm -rf {} + 2>/dev/null || true

    log_success "Python artifacts cleaned"
}

# Clean virtual environments
clean_virtual_environments() {
    log_info "Cleaning virtual environments..."

    cd "$REPO_ROOT"

    # Count and remove virtual environments
    local venv_dirs=0
    for pattern in "*-venv" ".venv" "venv" ".atsenv"; do
        if find . -maxdepth 2 -name "$pattern" -type d 2>/dev/null | grep -q .; then
            find . -maxdepth 2 -name "$pattern" -type d -exec rm -rf {} + 2>/dev/null || true
            ((venv_dirs++))
        fi
    done

    if [[ $venv_dirs -gt 0 ]]; then
        log_success "Removed $venv_dirs virtual environment directories"
    else
        log_info "No virtual environments found"
    fi
}

# Clean Node.js dependencies
clean_node_dependencies() {
    log_info "Cleaning Node.js dependencies..."

    cd "$REPO_ROOT"

    # Count and remove node_modules
    local node_dirs=$(find . -name "node_modules" -type d | wc -l || echo 0)

    if [[ $node_dirs -gt 0 ]]; then
        find . -name "node_modules" -type d -exec rm -rf {} + 2>/dev/null || true
        log_success "Removed $node_dirs node_modules directories"
    else
        log_info "No node_modules directories found"
    fi

    # Clean npm/yarn logs
    find . -name "npm-debug.log*" -type f -delete 2>/dev/null || true
    find . -name "yarn-debug.log*" -type f -delete 2>/dev/null || true
    find . -name "yarn-error.log*" -type f -delete 2>/dev/null || true
}

# Clean temporary files
clean_temporary_files() {
    log_info "Cleaning temporary files..."

    cd "$REPO_ROOT"

    # Remove various temporary files
    find . -name "*.tmp" -type f -delete 2>/dev/null || true
    find . -name "*.temp" -type f -delete 2>/dev/null || true
    find . -name "*~" -type f -delete 2>/dev/null || true
    find . -name "*.swp" -type f -delete 2>/dev/null || true
    find . -name "*.swo" -type f -delete 2>/dev/null || true
    find . -name ".DS_Store" -type f -delete 2>/dev/null || true
    find . -name "._*" -type f -delete 2>/dev/null || true

    # Clean up log files older than 7 days
    find . -name "*.log" -type f -mtime +7 -delete 2>/dev/null || true

    log_success "Temporary files cleaned"
}

# Validate repository state
validate_repository_state() {
    log_info "Validating repository state..."

    cd "$REPO_ROOT"

    # Check for remaining artifacts
    local remaining_issues=0

    if find . -name "*.pyc" -type f | grep -q .; then
        log_warning "Still found .pyc files"
        ((remaining_issues++))
    fi

    if find . -name "__pycache__" -type d | grep -q .; then
        log_warning "Still found __pycache__ directories"
        ((remaining_issues++))
    fi

    if find . -name "*-venv" -type d | grep -q .; then
        log_warning "Still found virtual environment directories"
        ((remaining_issues++))
    fi

    if [[ $remaining_issues -eq 0 ]]; then
        log_success "Repository state validation passed"
        return 0
    else
        log_warning "Found $remaining_issues remaining issues"
        return 1
    fi
}

# Run schema validation
run_schema_validation() {
    log_info "Running schema validation..."

    cd "$REPO_ROOT"

    if [[ -f "scripts/validate_schema.py" ]]; then
        if python scripts/validate_schema.py --check-all 2>&1 | tee -a "$LOG_FILE"; then
            log_success "Schema validation passed"
        else
            log_warning "Schema validation found issues"
        fi
    else
        log_warning "Schema validation script not found"
    fi
}

# Check Kubernetes conflicts
check_kubernetes_conflicts() {
    log_info "Checking Kubernetes conflicts..."

    cd "$REPO_ROOT"

    if [[ -f "scripts/detect_k8s_conflicts.py" ]] && [[ -d "k8s" ]]; then
        if python scripts/detect_k8s_conflicts.py k8s/ 2>&1 | tee -a "$LOG_FILE"; then
            log_success "No Kubernetes conflicts found"
        else
            log_warning "Kubernetes conflicts detected"
        fi
    else
        log_info "Kubernetes conflict detection skipped (files not found)"
    fi
}

# Generate cleanup report
generate_cleanup_report() {
    log_info "Generating cleanup report..."

    cd "$REPO_ROOT"

    local report_file="cleanup_report_$(date +%Y%m%d_%H%M%S).md"

    cat > "$report_file" << EOF
# Repository Cleanup Report
Generated: $(date)

## Summary
- **Repository**: $(basename "$REPO_ROOT")
- **Cleanup Date**: $(date)
- **Log File**: $LOG_FILE

## Files Cleaned
- Python artifacts (.pyc, __pycache__, .pyo files)
- Virtual environments (*-venv, .venv, venv directories)
- Node.js dependencies (node_modules directories)
- Temporary files (*.tmp, *.temp, *~, *.swp, .DS_Store)
- Old log files (>7 days)

## Current Repository State
\`\`\`bash
# File counts after cleanup
Python files: $(find . -name "*.py" | wc -l)
Documentation files: $(find docs/ -name "*.md" 2>/dev/null | wc -l || echo 0)
Kubernetes files: $(find k8s/ -name "*.yaml" 2>/dev/null | wc -l || echo 0)

# Size information
Repository size: $(du -sh . 2>/dev/null | cut -f1 || echo "Unknown")
\`\`\`

## Next Steps
1. Review remaining files in the repository
2. Ensure .gitignore is properly configured
3. Set up pre-commit hooks for automatic cleanup
4. Schedule regular cleanup maintenance

## Log Details
See full details in: $LOG_FILE
EOF

    log_success "Cleanup report generated: $report_file"
}

# Main cleanup function
main_cleanup() {
    log_info "Starting repository cleanup..."

    check_repository
    clean_python_artifacts
    clean_virtual_environments
    clean_node_dependencies
    clean_temporary_files

    if validate_repository_state; then
        log_success "Repository cleanup completed successfully"
    else
        log_warning "Repository cleanup completed with warnings"
    fi

    run_schema_validation
    check_kubernetes_conflicts
    generate_cleanup_report

    log_success "Full cleanup process completed"
    log_info "Log file: $LOG_FILE"
}

# Show help
show_help() {
    cat << EOF
Repository Cleanup Script

Usage: $0 [COMMAND]

Commands:
  clean        - Run full cleanup process (default)
  python       - Clean only Python artifacts
  venv         - Clean only virtual environments
  node         - Clean only Node.js dependencies
  temp         - Clean only temporary files
  validate     - Validate repository state
  schema       - Run schema validation
  k8s          - Check Kubernetes conflicts
  report       - Generate cleanup report only
  help         - Show this help

Examples:
  $0                    # Run full cleanup
  $0 clean             # Run full cleanup
  $0 python            # Clean only Python artifacts
  $0 validate          # Validate repository state

Environment Variables:
  LOG_FILE           - Custom log file path
  REPO_ROOT          - Custom repository root path
EOF
}

# Main entry point
case "${1:-clean}" in
    "clean")
        main_cleanup
        ;;
    "python")
        check_repository
        clean_python_artifacts
        validate_repository_state
        ;;
    "venv")
        check_repository
        clean_virtual_environments
        ;;
    "node")
        check_repository
        clean_node_dependencies
        ;;
    "temp")
        check_repository
        clean_temporary_files
        ;;
    "validate")
        check_repository
        validate_repository_state
        ;;
    "schema")
        check_repository
        run_schema_validation
        ;;
    "k8s")
        check_repository
        check_kubernetes_conflicts
        ;;
    "report")
        check_repository
        generate_cleanup_report
        ;;
    "help"|"-h"|"--help")
        show_help
        ;;
    *)
        log_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac