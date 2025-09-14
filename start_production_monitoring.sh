#!/bin/bash
#
# Production Monitoring Stack Startup Script
# Starts comprehensive monitoring and alerting for ATS Data Quality Agent
#

set -e  # Exit on any error

echo "🚀 Starting ATS Data Quality Agent Production Monitoring Stack"
echo "=" * 70

# Step 1: Validate configuration files
echo "📋 Step 1: Validating monitoring configuration..."

# Check Prometheus config
if [ ! -f "grafana/prometheus.yml" ]; then
    echo "❌ Missing Prometheus configuration: grafana/prometheus.yml"
    exit 1
fi

# Check alert rules
if [ ! -f "grafana/ats_data_quality_agent_alerts.yml" ]; then
    echo "❌ Missing alert rules: grafana/ats_data_quality_agent_alerts.yml"
    exit 1
fi

# Check Grafana dashboard
if [ ! -f "grafana/ats-data-quality-agent-dashboard.json" ]; then
    echo "❌ Missing Grafana dashboard: grafana/ats-data-quality-agent-dashboard.json"
    exit 1
fi

echo "✅ All configuration files validated"

# Step 2: Create monitoring network if it doesn't exist
echo "🔗 Step 2: Setting up monitoring network..."
docker network create ats-monitoring 2>/dev/null || echo "   Network ats-monitoring already exists"
echo "✅ Monitoring network ready"

# Step 3: Start the monitoring stack
echo "🏗️ Step 3: Starting monitoring services..."
docker-compose -f docker-compose.monitoring.yml up -d

# Step 4: Wait for services to be ready
echo "⏳ Step 4: Waiting for services to be ready..."
sleep 10

# Check Prometheus
echo "🔍 Checking Prometheus health..."
MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s http://localhost:9090/-/healthy >/dev/null 2>&1; then
        echo "✅ Prometheus is healthy"
        break
    fi
    echo "   Waiting for Prometheus... (${RETRY_COUNT}/${MAX_RETRIES})"
    sleep 2
    RETRY_COUNT=$((RETRY_COUNT + 1))
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "❌ Prometheus failed to become healthy"
    exit 1
fi

# Check Grafana
echo "🔍 Checking Grafana health..."
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s http://localhost:3030/api/health >/dev/null 2>&1; then
        echo "✅ Grafana is healthy"
        break
    fi
    echo "   Waiting for Grafana... (${RETRY_COUNT}/${MAX_RETRIES})"
    sleep 2
    RETRY_COUNT=$((RETRY_COUNT + 1))
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "❌ Grafana failed to become healthy"
    exit 1
fi

# Check AlertManager
echo "🔍 Checking AlertManager health..."
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s http://localhost:9093/-/healthy >/dev/null 2>&1; then
        echo "✅ AlertManager is healthy"
        break
    fi
    echo "   Waiting for AlertManager... (${RETRY_COUNT}/${MAX_RETRIES})"
    sleep 2
    RETRY_COUNT=$((RETRY_COUNT + 1))
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "❌ AlertManager failed to become healthy"
    exit 1
fi

# Step 5: Test analytics service metrics endpoint
echo "🔍 Step 5: Testing Data Quality Agent metrics..."
if curl -s http://localhost:4000/metrics >/dev/null 2>&1; then
    echo "✅ Analytics service metrics endpoint responding"
else
    echo "⚠️  Analytics service metrics endpoint not accessible (start the service first)"
fi

# Step 6: Display access information
echo ""
echo "🎉 PRODUCTION MONITORING STACK STARTED SUCCESSFULLY"
echo "=" * 70
echo ""
echo "📊 ACCESS POINTS:"
echo "   • Prometheus: http://localhost:9090"
echo "   • Grafana: http://localhost:3030"
echo "     - Username: admin"
echo "     - Password: ats_admin_2024"
echo "   • AlertManager: http://localhost:9093"
echo "   • Metrics: http://localhost:4000/metrics"
echo ""
echo "📈 DASHBOARDS:"
echo "   • Data Quality Agent: http://localhost:3030/d/ats-data-quality-agent"
echo "   • Coverage Monitoring: http://localhost:3030/d/ats-coverage-dashboard"
echo ""
echo "🔔 ALERTING:"
echo "   • 25+ alert rules configured for data quality monitoring"
echo "   • Alerts for: Agent status, Issue detection, Performance, Vendor health"
echo "   • Alert severities: Critical, Warning, Info"
echo ""
echo "📋 NEXT STEPS:"
echo "   1. Start the analytics service: docker-compose -f docker-compose.intg.yml up -d"
echo "   2. Access Grafana to view dashboards: http://localhost:3030"
echo "   3. Configure alert notifications in AlertManager"
echo "   4. Monitor metrics at: http://localhost:4000/metrics"
echo ""
echo "🔧 MANAGEMENT COMMANDS:"
echo "   • View logs: docker-compose -f docker-compose.monitoring.yml logs -f"
echo "   • Stop monitoring: docker-compose -f docker-compose.monitoring.yml down"
echo "   • Restart services: docker-compose -f docker-compose.monitoring.yml restart"
echo ""
echo "✅ Production monitoring and alerting is now active!"