#!/bin/bash
#
# ATS Events Cron Deployment Script
# Deploys comprehensive event collection cron jobs for ats-intg environment
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="/mnt/d/ats-logs/intg"

echo "🚀 Deploying ATS Events Collection Cron Jobs"
echo "============================================"

# Create log directories
echo "📁 Creating log directories..."
mkdir -p "$LOG_DIR"
mkdir -p "/mnt/d/ats-logs/intg"

# Backup existing crontab
echo "💾 Backing up existing crontab..."
crontab -l > "/tmp/crontab-backup-$(date +%Y%m%d-%H%M%S)" 2>/dev/null || echo "No existing crontab to backup"

# Install new cron jobs
echo "⚙️ Installing events cron jobs..."
crontab "$SCRIPT_DIR/ats-events-crontab"

echo "✅ Events cron jobs installed successfully!"
echo ""

# Verify installation
echo "🔍 Verifying cron installation..."
echo "Active cron jobs for events:"
crontab -l | grep -E "earnings|news|financial|economic|gap|events" | wc -l | xargs echo "- Event-related jobs:"

echo ""
echo "📋 Installed Jobs Summary:"
echo "- 📊 Earnings Events: Daily at 7:00 AM + Weekly backfill"  
echo "- 📰 News Events: Daily at 7:15 AM + Evening at 10:00 PM"
echo "- 💼 Financial Events: Daily sync at 7:30 AM"
echo "- 📈 Economic Indicators: Daily at 7:45 AM"
echo "- ⚡ Gap Events: Daily detection at 8:00 AM"
echo "- 🔍 Health Monitoring: Every 4 hours"
echo "- 🧹 Cleanup: Weekly on Sundays"

echo ""
echo "📊 Testing Event Collection..."

# Test earnings events count
echo "Checking earnings events..."
EARNINGS_COUNT=$(cd /home/jianjun/ats-genai-model && python3 scripts/run_intg.py query --query "SELECT COUNT(*) FROM intg_earnings_events;" | tail -1 | xargs)
echo "- Earnings events in database: $EARNINGS_COUNT"

# Test news events count  
echo "Checking news events..."
NEWS_COUNT=$(cd /home/jianjun/ats-genai-model && python3 scripts/run_intg.py query --query "SELECT COUNT(*) FROM intg_news;" | tail -1 | xargs)
echo "- News events in database: $NEWS_COUNT"

# Test gap events count
echo "Checking gap events..."
GAP_COUNT=$(cd /home/jianjun/ats-genai-model && python3 scripts/run_intg.py query --query "SELECT COUNT(*) FROM intg_gap_events;" | tail -1 | xargs)
echo "- Gap events in database: $GAP_COUNT"

echo ""
echo "🎯 Next Steps:"
echo "1. Monitor logs in $LOG_DIR/"
echo "2. Check ats-intg dashboard at http://localhost:4000"
echo "3. Verify Economic Events and Economic Indicators tabs show data"
echo ""
echo "📈 Dashboard URLs:"
echo "- Economic Events: http://localhost:4000 → Click '📊 Economic Events'"
echo "- Economic Indicators: http://localhost:4000 → Click '📈 Economic Indicators'"
echo ""
echo "✅ Events collection deployment completed!"