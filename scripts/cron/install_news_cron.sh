#!/bin/bash
# Install News Collection Cron Jobs
# Sets up automated daily collection and health monitoring

set -e

ENVIRONMENT="${1:-intg}"
USER="${2:-$(whoami)}"

echo "📅 Installing news collection cron jobs for $ENVIRONMENT environment"
echo "User: $USER"

# Create cron entries
CRON_ENTRIES="
# News Collection - Daily at 8 AM
0 8 * * * ENVIRONMENT=$ENVIRONMENT POLYGON_API_KEY=\"wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD\" /home/jianjun/ats-genai-data/scripts/cron/daily_news_collection.sh >> /mnt/d/ats-logs/$ENVIRONMENT/cron.log 2>&1

# Health Monitoring - Every 4 hours 
0 */4 * * * ENVIRONMENT=$ENVIRONMENT /home/jianjun/ats-genai-data/scripts/cron/news_health_monitor.sh >> /mnt/d/ats-logs/$ENVIRONMENT/health.log 2>&1

# Weekly cleanup - Sundays at 2 AM
0 2 * * 0 find /mnt/d/ats-logs/$ENVIRONMENT -name \"*.log\" -mtime +30 -delete
"

# Backup existing crontab
crontab -l > /tmp/crontab_backup_$(date +%Y%m%d_%H%M%S) 2>/dev/null || echo "No existing crontab"

# Add news collection jobs
echo "Adding cron entries..."
(crontab -l 2>/dev/null | grep -v "News Collection\|Health Monitoring\|Weekly cleanup" || true; echo "$CRON_ENTRIES") | crontab -

echo "✅ Cron jobs installed successfully!"
echo ""
echo "📋 Current cron schedule:"
crontab -l | grep -E "(daily_news_collection|news_health_monitor|Weekly cleanup)" || echo "No news cron jobs found"

echo ""
echo "📂 Log locations:"
echo "  Daily collection: /mnt/d/ats-logs/$ENVIRONMENT/daily_news_YYYYMMDD.log"
echo "  Health monitoring: /mnt/d/ats-logs/$ENVIRONMENT/health_monitor_YYYYMMDD.log"
echo "  Cron execution: /mnt/d/ats-logs/$ENVIRONMENT/cron.log"

echo ""
echo "🔧 Manual testing:"
echo "  Test daily collection: ENVIRONMENT=$ENVIRONMENT POLYGON_API_KEY=\"...\" ./scripts/cron/daily_news_collection.sh"
echo "  Test health monitor: ENVIRONMENT=$ENVIRONMENT ./scripts/cron/news_health_monitor.sh"

echo ""
echo "⚠️  Next steps:"
echo "  1. Set SLACK_WEBHOOK_URL environment variable for alerts"
echo "  2. Monitor first few runs to ensure everything works"
echo "  3. Consider adding to systemd for production reliability"