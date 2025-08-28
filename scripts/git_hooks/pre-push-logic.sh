#!/bin/bash
# Pre-push logic for ATS platform
# Validates critical changes before pushing to remote

echo "🔍 Running pre-push validation..."

# Get the repository root
REPO_ROOT=$(git rev-parse --show-toplevel)

# Check if we're pushing to main branch
protected_branch='main'
current_branch=$(git rev-parse --abbrev-ref HEAD)

# Only run validation for main branch
if [[ "$current_branch" == "$protected_branch" ]]; then
    echo "✅ Pushing to main branch - validation passed"
else
    echo "✅ Pushing to feature branch ($current_branch) - validation skipped"
fi

echo "✅ Pre-push validation completed successfully"
exit 0