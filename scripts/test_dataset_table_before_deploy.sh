#!/bin/bash
# Dataset Table Regression Protection Script
#
# CRITICAL: Run this script before any deployment that might affect dataset functionality!
#
# This script protects against regressions in the user-requested feature:
# "let' do the same for dataset dashboard where all training datasets are shown in a table with filter and sort."
#
# Usage: ./scripts/test_dataset_table_before_deploy.sh
# Exit codes: 0 = success, 1 = regression detected

set -e

echo "🔍 Dataset Table Regression Protection Test"
echo "=========================================="
echo ""

# Check if the analytics service is running
echo "📡 Checking if analytics service is accessible..."
if ! curl -s "http://172.25.223.121:3000/health" > /dev/null 2>&1; then
    echo "❌ Analytics service not accessible at http://172.25.223.121:3000"
    echo "💡 Make sure port-forward is running: kubectl port-forward -n ats-dev service/job-management-fixed-service 3000:5000"
    exit 1
fi

echo "✅ Analytics service is accessible"
echo ""

# Run the comprehensive regression test
echo "🧪 Running dataset table functionality tests..."
export PYTHONPATH=src
if python test_dataset_table_regression_protection.py; then
    echo ""
    echo "🎉 SUCCESS: Dataset table functionality is working correctly!"
    echo "✅ Safe to deploy - no regressions detected"
    exit 0
else
    echo ""
    echo "💥 FAILURE: Dataset table functionality is broken!"
    echo "🚨 DO NOT DEPLOY - regressions detected"
    echo "🔧 Fix the issues above before proceeding"
    exit 1
fi