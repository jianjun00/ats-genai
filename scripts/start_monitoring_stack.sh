#!/bin/bash
# Start ATS Coverage Monitoring Stack
# Launches Prometheus, Grafana, and Alertmanager

set -e

cd /home/jianjun/ats-genai-pm

echo "🚀 Starting ATS Data Coverage Monitoring Stack..."

# Start the monitoring stack
docker-compose -f docker-compose.monitoring.yml up -d

echo "⏳ Waiting for services to start..."
sleep 15

# Check service health
echo "🔍 Checking service health..."

# Check Prometheus
if curl -f -s http://localhost:9090/-/healthy > /dev/null 2>&1; then
    echo "✅ Prometheus is healthy (http://localhost:9090)"
else
    echo "❌ Prometheus health check failed"
fi

# Check Grafana
if curl -f -s http://localhost:3000/api/health > /dev/null 2>&1; then
    echo "✅ Grafana is healthy (http://localhost:3000)"
    echo "   📋 Default login: admin / ats_admin_2024"
else
    echo "❌ Grafana health check failed"
fi

# Check Node Exporter
if curl -f -s http://localhost:9100/metrics > /dev/null 2>&1; then
    echo "✅ Node Exporter is healthy (http://localhost:9100)"
else
    echo "❌ Node Exporter health check failed"
fi

# Check Alertmanager
if curl -f -s http://localhost:9093/-/healthy > /dev/null 2>&1; then
    echo "✅ Alertmanager is healthy (http://localhost:9093)"
else
    echo "❌ Alertmanager health check failed"
fi

echo ""
echo "🎯 Access Points:"
echo "  📊 Grafana Dashboard: http://localhost:3000"
echo "  📈 Prometheus: http://localhost:9090"
echo "  🚨 Alertmanager: http://localhost:9093"
echo "  💻 Node Exporter: http://localhost:9100"
echo ""
echo "✅ ATS Coverage Monitoring Stack is running!"
