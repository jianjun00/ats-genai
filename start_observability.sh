#!/bin/bash

# ATS Platform Observability Startup Script
# This script starts the complete observability stack for usage tracking and cleanup detection

set -e

echo "🚀 Starting ATS Platform Observability Stack"
echo "============================================"

# Check if Docker network exists
if ! docker network inspect ats-network >/dev/null 2>&1; then
    echo "📡 Creating ats-network..."
    docker network create ats-network
else
    echo "✅ ats-network already exists"
fi

# Start SigNoz stack
echo "📊 Starting SigNoz observability platform..."
docker-compose -f docker-compose.signoz.yml up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 10

# Check service health
echo "🔍 Checking service health..."

# Check SigNoz frontend
if curl -f -s http://localhost:3301/api/v1/version >/dev/null 2>&1; then
    echo "✅ SigNoz Frontend: http://localhost:3301"
else
    echo "⚠️ SigNoz Frontend not ready yet"
fi

# Check ClickHouse
if curl -f -s http://localhost:8124/ping >/dev/null 2>&1; then
    echo "✅ ClickHouse Database: http://localhost:8124"
else
    echo "⚠️ ClickHouse not ready yet"
fi

# Check OTLP Collector
if nc -z localhost 4317 >/dev/null 2>&1; then
    echo "✅ OTLP Collector: localhost:4317"
else
    echo "⚠️ OTLP Collector not ready yet"
fi

echo ""
echo "🎯 Observability Stack Status:"
echo "================================"
echo "📊 SigNoz Dashboard:     http://localhost:3301"
echo "📈 ClickHouse:           http://localhost:8124"
echo "🔄 OTLP Collector:       localhost:4317"
echo "📡 Prometheus Gateway:   http://localhost:9091"
echo ""

# Start ATS instrumentation
echo "🔧 Initializing ATS instrumentation..."
PYTHONPATH=src python3 -c "
from src.observability.instrumentation_setup import setup_ats_instrumentation
from src.observability.code_usage_tracker import get_code_tracker

print('🚀 Setting up ATS instrumentation...')
success = setup_ats_instrumentation(enable_metrics_endpoint=True)

if success:
    print('✅ ATS instrumentation ready')
    print('📊 Metrics endpoint: http://localhost:8000/metrics')
    print('🏥 Health check: http://localhost:8000/health')

    # Keep metrics server running
    import time
    print('📡 Metrics server running... (Press Ctrl+C to stop)')
    try:
        while True:
            time.sleep(60)
            tracker = get_code_tracker()
            stats = tracker.get_usage_stats()
            print(f'📈 Functions called: {stats[\"total_function_calls\"]}, Unique: {stats[\"unique_functions_called\"]}')
    except KeyboardInterrupt:
        print('👋 Shutting down metrics server')
else:
    print('❌ ATS instrumentation setup failed')
" &

METRICS_PID=$!
echo "🎯 ATS Metrics Server PID: $METRICS_PID"

echo ""
echo "✅ ATS Observability Stack Started Successfully!"
echo ""
echo "📋 Quick Commands:"
echo "  View logs:           docker-compose -f docker-compose.signoz.yml logs -f"
echo "  Stop stack:          docker-compose -f docker-compose.signoz.yml down"
echo "  Run cleanup analysis: PYTHONPATH=src python3 -m src.observability.cleanup_detector"
echo "  View metrics:        curl http://localhost:8000/metrics"
echo ""
echo "💡 Let the system run for 24-48 hours to collect meaningful usage data"
echo "   then run cleanup analysis to identify unused code and database tables."
echo ""

# Save PID for cleanup
echo $METRICS_PID > /tmp/ats_metrics_server.pid

echo "🔥 System is now monitoring all function calls and database queries!"
echo "   Press Ctrl+C to stop the metrics server (SigNoz will keep running)"

# Wait for metrics server
wait $METRICS_PID