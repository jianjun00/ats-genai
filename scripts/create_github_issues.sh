#!/bin/bash

# GitHub Issues Creation Script for 30-Year Daily Price History Project
# Run this after installing GitHub CLI and authenticating

set -e

echo "🚀 Creating GitHub Issues for 30-Year Daily Price History Project"

# Check if GitHub CLI is available
if ! command -v gh &> /dev/null; then
    echo "❌ GitHub CLI not found. Please install it first:"
    echo "sudo apt update && sudo apt install gh"
    echo "# OR"
    echo "sudo snap install gh"
    exit 1
fi

# Check if authenticated
if ! gh auth status &> /dev/null; then
    echo "❌ Not authenticated with GitHub. Please run:"
    echo "gh auth login"
    exit 1
fi

echo "✅ GitHub CLI found and authenticated"

# Issue 1: EODHD Integration
echo "📝 Creating Issue #1: EODHD Integration"
gh issue create \
    --title "Phase 1: Enhanced EODHD Integration for Historical Data Backfill" \
    --label "data-infrastructure,phase-1,enhancement" \
    --body-file github-issues/issue-1-eodhd-integration.md

# Issue 2: ETF Expansion
echo "📝 Creating Issue #2: ETF Expansion"
gh issue create \
    --title "Phase 1: Critical ETF Universe Expansion" \
    --label "data-infrastructure,phase-1,enhancement" \
    --body-file github-issues/issue-2-etf-expansion.md

# Issue 3: Schema Enhancement
echo "📝 Creating Issue #3: Schema Enhancement"
gh issue create \
    --title "Phase 1: Enhanced Daily Prices Schema" \
    --label "database,phase-1,schema" \
    --body-file github-issues/issue-3-schema-enhancement.md

# Issue 4: Cross-Vendor Reconciliation
echo "📝 Creating Issue #4: Cross-Vendor Reconciliation"
gh issue create \
    --title "Phase 2: Advanced Cross-Vendor Data Reconciliation Engine" \
    --label "data-quality,phase-2,enhancement" \
    --body-file github-issues/issue-4-cross-vendor-reconciliation.md

# Issue 5: Gap Filling
echo "📝 Creating Issue #5: Gap Filling"
gh issue create \
    --title "Phase 2: Intelligent Gap Filling and Forward-Fill Automation" \
    --label "data-quality,phase-2,automation" \
    --body-file github-issues/issue-5-intelligent-gap-filling.md

# Issue 6: Performance Optimization
echo "📝 Creating Issue #6: Performance Optimization"
gh issue create \
    --title "Phase 3: Query Performance Optimization and TimescaleDB Tuning" \
    --label "performance,phase-3,database" \
    --body-file github-issues/issue-6-performance-optimization.md

# Issue 7: Data Quality Monitoring
echo "📝 Creating Issue #7: Data Quality Monitoring"
gh issue create \
    --title "Phase 3: Real-Time Data Quality Monitoring Dashboard" \
    --label "monitoring,phase-3,dashboard" \
    --body-file github-issues/issue-7-data-quality-monitoring.md

# Issue 8: End-to-End Validation
echo "📝 Creating Issue #8: End-to-End Validation"
gh issue create \
    --title "Phase 3: End-to-End System Validation and Testing" \
    --label "testing,phase-3,validation" \
    --body-file github-issues/issue-8-end-to-end-validation.md

# Issue 9: Polygon API Key Fix
echo "📝 Creating Issue #9: Polygon API Key Fix"
gh issue create \
    --title "URGENT: Fix Polygon API Key for Data Population" \
    --label "urgent,infrastructure,api-keys" \
    --body-file github-issues/issue-9-polygon-api-key.md

echo ""
echo "✅ All GitHub Issues created successfully!"
echo ""
echo "🎯 Next steps:"
echo "1. Review created issues at: https://github.com/AkoloTechnologies/ats-genai/issues"
echo "2. Assign team members to critical issues (#3, #2, #9)"
echo "3. Set up project board for tracking progress"
echo "4. Begin work on Phase 1 critical issues"
echo ""
echo "📋 Priority order:"
echo "  1. Issue #3: Schema Enhancement (CRITICAL)"
echo "  2. Issue #2: ETF Expansion (HIGH)"
echo "  3. Issue #9: Polygon API Key Fix (MEDIUM)"
echo "  4. Issue #1: EODHD Quota Management (MEDIUM)"
echo ""