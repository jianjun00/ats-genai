#!/bin/bash

# Setup News Metrics Integration with Existing Prometheus/Grafana

echo "📊 Setting up News Metrics Integration..."

# Check if Prometheus is running
PROMETHEUS_RUNNING=$(docker ps | grep prometheus || echo "")
if [ -z "$PROMETHEUS_RUNNING" ]; then
    echo "⚠️  Prometheus not detected. News metrics will be available at http://localhost:8081/metrics"
    echo "   You can add this as a scrape target to your Prometheus configuration."
else
    echo "✅ Prometheus detected: $PROMETHEUS_RUNNING"
fi

# Check if Grafana is running  
GRAFANA_RUNNING=$(docker ps | grep grafana || echo "")
if [ -z "$GRAFANA_RUNNING" ]; then
    echo "⚠️  Grafana not detected."
else
    echo "✅ Grafana detected: $GRAFANA_RUNNING"
    echo "   📊 Access at: http://10.0.0.79:4002/dashboards"
fi

# Create a simple news dashboard configuration
cat > /tmp/news_dashboard.json << 'EOF'
{
  "dashboard": {
    "id": null,
    "title": "ATS News Ingestion Dashboard",
    "tags": ["ats", "news"],
    "timezone": "browser",
    "panels": [
      {
        "id": 1,
        "title": "News Articles Collected by Vendor",
        "type": "stat",
        "targets": [
          {
            "expr": "ats_news_articles_collected_total",
            "legendFormat": "{{vendor}}"
          }
        ],
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0}
      },
      {
        "id": 2,
        "title": "News Collection Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(ats_news_articles_collected_total[5m])",
            "legendFormat": "{{vendor}} articles/sec"
          }
        ],
        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0}
      },
      {
        "id": 3,
        "title": "API Call Success Rate",
        "type": "graph", 
        "targets": [
          {
            "expr": "rate(ats_news_api_calls_total{status=\"200\"}[5m]) / rate(ats_news_api_calls_total[5m])",
            "legendFormat": "{{vendor}} success rate"
          }
        ],
        "gridPos": {"h": 8, "w": 24, "x": 0, "y": 8}
      }
    ],
    "time": {"from": "now-1h", "to": "now"},
    "refresh": "30s"
  }
}
EOF

echo "📁 Created dashboard configuration at /tmp/news_dashboard.json"
echo ""
echo "🔧 Manual Setup Steps:"
echo ""
echo "1. 📊 **Start News Services:**"
echo "   ./scripts/start_news_ingestion_intg.sh"
echo ""
echo "2. 📈 **Verify Metrics:**" 
echo "   curl http://localhost:8081/metrics | grep ats_news"
echo ""
echo "3. 🎛️  **Add to Grafana** (if available):"
echo "   - Go to: http://10.0.0.79:4002/dashboard/import"
echo "   - Upload: /tmp/news_dashboard.json"
echo "   - Or manually create panels using the metrics from step 2"
echo ""
echo "4. 📋 **Check Database:**"
echo "   python3 scripts/run_intg.py query --query \"SELECT vendor, COUNT(*) FROM intg_realtime_news GROUP BY vendor\""
echo ""
echo "✅ News metrics integration setup complete!"