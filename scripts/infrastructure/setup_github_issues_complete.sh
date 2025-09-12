#!/bin/bash

# Complete GitHub Issues Setup Script
# Run with: sudo ./scripts/setup_github_issues_complete.sh

set -e

echo "🚀 Complete GitHub Issues Setup for 30-Year Daily Price History Project"
echo ""

# Step 1: Install GitHub CLI
echo "📦 Step 1: Installing GitHub CLI..."
./scripts/install_github_cli.sh

# Step 2: Check if authenticated (will fail if not, but that's expected)
echo ""
echo "🔐 Step 2: Checking GitHub authentication..."
if gh auth status &> /dev/null; then
    echo "✅ Already authenticated with GitHub"
    AUTHENTICATED=true
else
    echo "⚠️ Not authenticated with GitHub yet"
    AUTHENTICATED=false
fi

# Step 3: Show next steps
echo ""
echo "🎯 Next Steps:"

if [ "$AUTHENTICATED" = false ]; then
    echo "1. Authenticate with GitHub:"
    echo "   gh auth login"
    echo ""
    echo "2. After authentication, create issues:"
    echo "   ./scripts/create_github_issues.sh"
else
    echo "1. Create GitHub Issues:"
    echo "   ./scripts/create_github_issues.sh"
fi

echo ""
echo "📋 This will create 9 GitHub Issues with current project status:"
echo "   • Issue #1: EODHD Integration (🔄 IN PROGRESS - 2.8% complete)"
echo "   • Issue #2: ETF Expansion (🚨 CRITICAL GAP - 17 vs 250+ required)"
echo "   • Issue #3: Schema Enhancement (🚨 CRITICAL - blocks Phase 2)"
echo "   • Issue #4: Cross-Vendor Reconciliation (⏸️ BLOCKED)"
echo "   • Issue #5: Gap Filling (Phase 2)"
echo "   • Issue #6: Performance Optimization (Phase 3)"
echo "   • Issue #7: Data Quality Monitoring (Phase 3)"
echo "   • Issue #8: End-to-End Validation (Phase 3)"
echo "   • Issue #9: Fix Polygon API Key (🚨 URGENT)"
echo ""
echo "🎯 Priority Order:"
echo "   1. Issue #3: Schema Enhancement (CRITICAL)"
echo "   2. Issue #2: ETF Expansion (HIGH)"
echo "   3. Issue #9: Polygon API Key Fix (MEDIUM)"
echo "   4. Issue #1: EODHD Quota Management (MEDIUM)"
echo ""