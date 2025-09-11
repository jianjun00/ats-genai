#!/bin/bash
# Verify News Collection Metrics in SigNoz

echo "🔍 ATS News Collection Metrics Verification"
echo "=========================================="
echo ""

# Check service status
echo "📊 Service Status Check:"
if curl -s -f http://localhost:8082/health >/dev/null; then
    HEALTH_DATA=$(curl -s http://localhost:8082/health)
    SERVICE_NAME=$(echo "$HEALTH_DATA" | grep -o '"service":"[^"]*' | cut -d'"' -f4)
    ARTICLES=$(echo "$HEALTH_DATA" | grep -o '"total_articles":[0-9]*' | cut -d':' -f2)
    LAST_COLLECTION=$(echo "$HEALTH_DATA" | grep -o '"last_collection":"[^"]*' | cut -d'"' -f4)
    
    echo "   ✅ Service: $SERVICE_NAME"
    echo "   ✅ Total Articles: $ARTICLES"
    echo "   ✅ Last Collection: $LAST_COLLECTION"
    echo "   ✅ Health Endpoint: ACTIVE"
else
    echo "   ❌ Service not responding at http://localhost:8082/health"
    echo "   Run: docker logs ats-intg-news-metrics"
    exit 1
fi

echo ""

# Check metrics endpoint
echo "📈 Metrics Endpoint Check:"
if curl -s -f http://localhost:8082/metrics >/dev/null; then
    echo "   ✅ Prometheus metrics: ACTIVE"
    echo "   ✅ Available at: http://localhost:8082/metrics"
else
    echo "   ❌ Metrics endpoint not responding"
fi

echo ""

# Check SigNoz connectivity
echo "📊 SigNoz Integration Check:"
if curl -s -f http://localhost:8080 >/dev/null; then
    echo "   ✅ SigNoz Dashboard: ACTIVE"
    echo "   ✅ Access at: http://localhost:8080"
else
    echo "   ❌ SigNoz not accessible at http://localhost:8080"
fi

# Check OpenTelemetry collector
if docker ps --filter "name=signoz-otel-collector" --filter "status=running" | grep -q .; then
    echo "   ✅ OpenTelemetry Collector: RUNNING"
else
    echo "   ❌ OpenTelemetry Collector not running"
fi

echo ""

# Service logs check
echo "🔍 Recent Service Activity:"
docker logs ats-intg-news-metrics --tail 5 | grep -E "(articles|metrics|INFO)" | head -3

echo ""

# Instructions
echo "📋 Next Steps:"
echo "   1. Open SigNoz Dashboard: http://localhost:8080"
echo "   2. Navigate to 'Services' tab"
echo "   3. Look for service: 'ats-intg-news-collection'"
echo "   4. Click on the service to view metrics and traces"
echo ""

echo "🎯 Available Metrics in SigNoz:"
echo "   - news_articles_fetched_total (Counter)"
echo "   - news_articles_stored_total (Counter)" 
echo "   - news_api_calls_total (Counter)"
echo "   - news_api_response_duration_ms (Histogram)"
echo "   - news_ingestion_cycle_duration_ms (Histogram)"
echo "   - news_data_freshness_minutes (UpDownCounter)"
echo ""

echo "⏱️  Note: Metrics may take 2-3 minutes to appear in SigNoz after service start"

# Manual collection trigger
echo ""
echo "🔧 Manual Operations:"
echo "   Trigger manual collection:"
echo "   curl -X POST -H 'Content-Type: application/json' \\"
echo "        -d '{\"start_date\":\"2025-09-10\", \"end_date\":\"2025-09-11\"}' \\"
echo "        http://localhost:8082/collect"