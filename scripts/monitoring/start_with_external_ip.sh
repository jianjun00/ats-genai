#!/bin/bash
# Quick start script with correct external IP for ATS monitoring

echo "🚀 Starting ATS monitoring with external IP: 10.0.0.79"
echo ""

EXTERNAL_IP=10.0.0.79 $(dirname "$0")/start_external_monitoring.sh

echo ""
echo "🎯 External Access URLs for other machines:"
echo "================================"
echo "📊 Grafana Dashboard:    http://10.0.0.79:3000 (admin/admin)"
echo "📈 Prometheus UI:        http://10.0.0.79:9090"
echo "🔍 PostgreSQL Metrics:   http://10.0.0.79:8001/metrics"
echo "📡 Data Agent Metrics:   http://10.0.0.79:8000/metrics"
echo ""
echo "📝 Next: Run Windows port forwarding as Administrator:"
echo "   PowerShell -ExecutionPolicy Bypass -File scripts\\monitoring\\setup_wsl_port_forwarding.ps1"