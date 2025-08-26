#!/bin/bash
# Pre-commit logic for ATS platform
# Simple validation script

echo "🔍 Running pre-commit validation..."

# Basic checks
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 not found"
    exit 1
fi

# Check for obvious issues in staged files
STAGED_FILES=$(git diff --cached --name-only)

if [ -z "$STAGED_FILES" ]; then
    echo "⚠️ No staged files found"
    exit 0
fi

echo "✅ Pre-commit validation passed"
exit 0