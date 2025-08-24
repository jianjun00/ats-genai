#!/bin/bash

# Analytics Service Test Runner
# Runs comprehensive tests for the analytics service to prevent regression

set -e

echo "🧪 Running Analytics Service Tests"
echo "=================================="

# Set environment variables if not set
export DB_HOST=${DB_HOST:-postgres}
export DB_PORT=${DB_PORT:-5432}
export DB_USER=${DB_USER:-postgres}
export DB_PASSWORD=${DB_PASSWORD:-dev_password}
export DB_NAME=${DB_NAME:-dev_db}
export ANALYTICS_SERVICE_URL=${ANALYTICS_SERVICE_URL:-http://localhost:3001}

echo "📋 Test Configuration:"
echo "  Database: ${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo "  Analytics Service: ${ANALYTICS_SERVICE_URL}"
echo ""

# Check if analytics service is accessible
echo "🔍 Checking analytics service health..."
if curl -s "${ANALYTICS_SERVICE_URL}/health" > /dev/null; then
    echo "✅ Analytics service is accessible"
else
    echo "❌ Analytics service is not accessible at ${ANALYTICS_SERVICE_URL}"
    echo "   Make sure the service is running and port-forwarded:"
    echo "   kubectl port-forward service/ats-analytics-service 3001:3000 -n ats-dev"
    exit 1
fi

# Install test dependencies if needed
echo "📦 Installing test dependencies..."
pip install -q pytest pytest-asyncio httpx asyncpg

echo ""
echo "🧪 Running Database Schema Validation Tests..."
echo "==============================================="
PYTHONPATH=src pytest tests/analytics/test_database_schema_validation.py -v --tb=short

echo ""
echo "🧪 Running Analytics Endpoints Integration Tests..."
echo "=================================================="
PYTHONPATH=src pytest tests/analytics/test_analytics_endpoints.py -v --tb=short

echo ""
echo "🧪 Running Query Performance Tests..."
echo "===================================="
PYTHONPATH=src pytest tests/analytics/test_query_performance.py -v --tb=short -s

echo ""
echo "✅ All Analytics Tests Completed Successfully!"
echo "=============================================="

echo ""
echo "📊 Test Summary:"
echo "  ✅ Database schema validation - Prevents wrong table/column names"
echo "  ✅ Endpoint integration tests - Prevents data inconsistencies"  
echo "  ✅ Query performance tests - Prevents slow queries and timeouts"
echo ""
echo "🛡️ These tests prevent the following issues we previously encountered:"
echo "  • Jobs stats showing data but jobs list empty (wrong column names)"
echo "  • Coverage showing 0 when millions of records exist (wrong timestamp columns)"
echo "  • 'Operation in progress' errors (single connection vs connection pool)"
echo "  • SQL syntax errors in UNION queries"
echo "  • Service crashes due to schema mismatches"
echo ""

# Optional: Run load tests if specified
if [ "$RUN_LOAD_TESTS" = "true" ]; then
    echo "🚀 Running Load Tests..."
    echo "======================="
    
    # Simple load test with curl
    echo "Testing concurrent requests..."
    for i in {1..10}; do
        curl -s "${ANALYTICS_SERVICE_URL}/api/v1/jobs/stats" > /dev/null &
        curl -s "${ANALYTICS_SERVICE_URL}/api/v1/coverage/summary" > /dev/null &
    done
    wait
    
    echo "✅ Load test completed"
fi

echo "🎉 Analytics service testing completed successfully!"