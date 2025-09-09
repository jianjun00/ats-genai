#!/bin/bash

# ATS News Ingestion Docker Services Startup Script
# Integrates with existing Docker-based INTG environment

set -e

echo "🚀 Starting ATS News Ingestion Services (Docker-based)"

# Ensure Docker network exists
docker network create ats-network 2>/dev/null || echo "✓ ats-network already exists"

# Check if PostgreSQL is running
if ! docker ps | grep -q "ats-intg-postgres"; then
    echo "❌ ATS-INTG PostgreSQL not running. Start with: python scripts/run_intg.py start --service postgres"
    exit 1
fi

# Start News Backfill Service (runs every 6 hours)
echo "📰 Starting News Backfill Service..."
docker run -d \
    --name ats-intg-news-backfill \
    --network ats-network \
    --restart unless-stopped \
    -v /home/jianjun/ats-genai-oncall:/workspace \
    -v /mnt/d/ats-logs/intg:/logs \
    -w /workspace \
    -e PYTHONPATH=/workspace/src \
    -e ENVIRONMENT=intg \
    -e DB_HOST=ats-intg-postgres \
    -e DB_PORT=5432 \
    -e DB_USER=postgres \
    -e DB_PASSWORD=intg_password \
    -e DB_NAME=intg_db \
    -e TIINGO_API_KEY=${TIINGO_API_KEY:-5f40b4f36e171405746304ec0e5a6f3aa9ca77e5} \
    -e POLYGON_API_KEY=${POLYGON_API_KEY:-} \
    -e EODHD_API_KEY=${EODHD_API_KEY:-} \
    -e LOG_LEVEL=INFO \
    dragonflyer762/ats-genai:latest \
    bash -c 'while true; do python3 scripts/multi_vendor_news_backfill.py --vendors tiingo,polygon,eodhd --days 30 >> /logs/news-backfill.log 2>&1; echo "Backfill completed, sleeping 6 hours..."; sleep 21600; done'

# Start Real-Time News Ingestion Service
echo "📡 Starting Real-Time News Ingestion Service..."
docker run -d \
    --name ats-intg-news-realtime \
    --network ats-network \
    --restart unless-stopped \
    -p 8081:8080 \
    -v /home/jianjun/ats-genai-oncall:/workspace \
    -v /mnt/d/ats-logs/intg:/logs \
    -w /workspace \
    -e PYTHONPATH=/workspace/src \
    -e ENVIRONMENT=intg \
    -e DB_HOST=ats-intg-postgres \
    -e DB_PORT=5432 \
    -e DB_USER=postgres \
    -e DB_PASSWORD=intg_password \
    -e DB_NAME=intg_db \
    -e TIINGO_API_KEY=${TIINGO_API_KEY:-5f40b4f36e171405746304ec0e5a6f3aa9ca77e5} \
    -e POLYGON_API_KEY=${POLYGON_API_KEY:-} \
    -e EODHD_API_KEY=${EODHD_API_KEY:-} \
    -e LOG_LEVEL=INFO \
    -e METRICS_PORT=8080 \
    dragonflyer762/ats-genai:latest \
    python3 scripts/realtime_news_ingestion.py --vendors tiingo,polygon,eodhd --interval 300 --daemon

# Start News Health Monitor (runs every 2 hours)
echo "🏥 Starting News Health Monitor..."
docker run -d \
    --name ats-intg-news-monitor \
    --network ats-network \
    --restart unless-stopped \
    -v /home/jianjun/ats-genai-oncall:/workspace \
    -v /mnt/d/ats-logs/intg:/logs \
    -w /workspace \
    -e PYTHONPATH=/workspace/src \
    -e ENVIRONMENT=intg \
    -e DB_HOST=ats-intg-postgres \
    -e DB_PORT=5432 \
    -e DB_USER=postgres \
    -e DB_PASSWORD=intg_password \
    -e DB_NAME=intg_db \
    -e LOG_LEVEL=INFO \
    dragonflyer762/ats-genai:latest \
    bash -c 'while true; do python3 scripts/news_health_monitor.py >> /logs/news-health.log 2>&1; echo "Health check completed, sleeping 2 hours..."; sleep 7200; done'

echo ""
echo "✅ ATS News Ingestion Services Started Successfully!"
echo ""
echo "📊 Service Status:"
echo "   - News Backfill:     docker logs ats-intg-news-backfill --tail 20"
echo "   - Real-time Ingestion: docker logs ats-intg-news-realtime --tail 20"
echo "   - Health Monitor:      docker logs ats-intg-news-monitor --tail 20"
echo ""
echo "📈 Metrics Available:"
echo "   - Real-time Metrics:   curl http://localhost:8081/metrics"
echo "   - Database Query:      python3 scripts/run_intg.py query --query \"SELECT vendor, COUNT(*) FROM intg_realtime_news GROUP BY vendor\""
echo ""
echo "🔧 Management:"
echo "   - Stop all services:   ./scripts/stop_news_ingestion_intg.sh"
echo "   - Manual backfill:     python3 scripts/run_intg.py run --script scripts/multi_vendor_news_backfill.py --days 7"
echo ""