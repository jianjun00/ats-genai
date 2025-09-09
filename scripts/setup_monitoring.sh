#!/bin/bash
# Setup monitoring dependencies and services for ATS batch job monitoring

echo "🔧 Setting up ATS batch job monitoring..."

# Install Prometheus client if not already installed
echo "📦 Installing Prometheus client..."
pip install prometheus_client

# Check if Prometheus is available
if command -v prometheus &> /dev/null; then
    echo "✅ Prometheus found"
else
    echo "⚠️  Prometheus not found - install separately if needed"
fi

# Check if Pushgateway is available
if pgrep -x "pushgateway" > /dev/null; then
    echo "✅ Prometheus Pushgateway is running"
else
    echo "⚠️  Prometheus Pushgateway not running"
    echo "   Start with: docker run -d -p 9091:9091 prom/pushgateway"
fi

# Check Grafana connection
if curl -s http://localhost:3000/api/health &> /dev/null; then
    echo "✅ Grafana is accessible"
else
    echo "⚠️  Grafana not accessible on localhost:3000"
fi

echo ""
echo "📋 Monitoring Setup Complete!"
echo ""
echo "🔗 Access Points:"
echo "   Prometheus Metrics: http://localhost:8080/metrics"
echo "   Prometheus Pushgateway: http://localhost:9091"
echo "   Grafana: http://localhost:3000"
echo ""
echo "📊 Dashboard Location:"
echo "   /home/jianjun/ats-genai-data/config/grafana/ats-batch-jobs-dashboard.json"
echo ""
echo "🧪 Test the setup with:"
echo "   python scripts/test_batch_job_metrics.py"
echo ""
echo "🚀 Start monitoring with:"
echo "   python scripts/prometheus_metrics_server.py"