#!/bin/bash
# MASTER CLEANUP SCRIPT
# Orchestrates safe cleanup operations with validation

set -e

echo "=== ATS GenAI Admin Codebase Cleanup ==="
echo "This script will perform automated cleanup operations."
echo "Each step includes safety checks and backups."
echo

# Check if we're in the right directory
if [[ ! -f "CLAUDE.md" ]]; then
    echo "ERROR: Please run this script from the ats-genai-admin root directory"
    exit 1
fi

# Create master backup
MASTER_BACKUP="master_cleanup_backup_$(date +%Y%m%d_%H%M%S)"
echo "Creating master backup: $MASTER_BACKUP"
mkdir -p "$MASTER_BACKUP"

# Function to run tests and check results
run_tests() {
    echo "Running test suite..."
    if python3 scripts/run_dev.py test --test tests/unit/ --quiet; then
        echo "✓ Tests passed"
        return 0
    else
        echo "✗ Tests failed"
        return 1
    fi
}

# Phase 1: Clean unused imports
echo
echo "=== Phase 1: Cleaning unused imports ==="
read -p "Proceed with unused imports cleanup? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if [[ -f "cleanup_unused_imports.sh" ]]; then
        ./cleanup_unused_imports.sh
        
        if run_tests; then
            echo "✓ Phase 1 completed successfully"
        else
            echo "✗ Phase 1 caused test failures - manual review required"
            exit 1
        fi
    else
        echo "cleanup_unused_imports.sh not found"
    fi
fi

# Phase 2: Clean commented code
echo
echo "=== Phase 2: Cleaning large comment blocks ==="
read -p "Proceed with commented code cleanup? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if [[ -f "cleanup_commented_code.sh" ]]; then
        ./cleanup_commented_code.sh
        
        if run_tests; then
            echo "✓ Phase 2 completed successfully"
        else
            echo "✗ Phase 2 caused test failures - manual review required"
            exit 1
        fi
    else
        echo "cleanup_commented_code.sh not found"
    fi
fi

# Phase 3: Analysis of dead functions (manual review required)
echo
echo "=== Phase 3: Dead functions analysis ==="
echo "Review the generated analysis files for manual cleanup:"
echo "- focused_cleanup_analysis.json"
echo "- ORPHANED_FILES_ANALYSIS.md"
echo "- COMPREHENSIVE_DEAD_CODE_ANALYSIS_REPORT.md"

echo
echo "=== Cleanup Summary ==="
echo "Backup created: $MASTER_BACKUP"
echo "Analysis files created for manual review"
echo "Run 'git status' to see all changes"
echo "Run 'git diff' to review specific changes"
echo
echo "Next steps:"
echo "1. Review analysis files"
echo "2. Manually clean dead functions"
echo "3. Archive orphaned files after review"
echo "4. Commit changes: git add . && git commit -m 'chore: automated code cleanup'"
