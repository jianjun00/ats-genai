#!/bin/bash

# ATS News Ingestion Docker Services Stop Script

echo "🛑 Stopping ATS News Ingestion Services..."

# Stop and remove news services
docker stop ats-intg-news-backfill ats-intg-news-realtime ats-intg-news-monitor 2>/dev/null || echo "Some services were not running"
docker rm ats-intg-news-backfill ats-intg-news-realtime ats-intg-news-monitor 2>/dev/null || echo "Some containers were already removed"

echo "✅ All news ingestion services stopped and removed"
echo ""
echo "📊 To check remaining containers:"
echo "   docker ps | grep ats-intg"
echo ""
echo "🚀 To restart news services:"
echo "   ./scripts/start_news_ingestion_intg.sh"