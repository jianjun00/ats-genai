#!/bin/bash
# Monitor comprehensive 30-year daily price backfill progress
# Usage: ./scripts/monitor_backfill_progress.sh

echo "==================================================================="
echo "🚀 ATS COMPREHENSIVE 30-YEAR DAILY PRICE BACKFILL MONITOR"
echo "==================================================================="
echo "📅 $(date)"
echo ""

# Check running processes
echo "📊 ACTIVE BACKFILL PROCESSES:"
echo "-------------------------------------------------------------------"
PROCESSES=$(ps aux | grep -E "(tiingo|polygon|eodhd).*backfill" | grep -v grep | wc -l)
if [ $PROCESSES -gt 0 ]; then
    ps aux | grep -E "(tiingo|polygon|eodhd).*backfill" | grep -v grep | awk '{print "✅ " $11 " " $12 " (PID: " $2 ")"}'
    echo "📈 $PROCESSES backfill processes are running"
else
    echo "❌ No backfill processes currently running"
fi
echo ""

# Check latest log activity
echo "📋 LATEST LOG ACTIVITY:"
echo "-------------------------------------------------------------------"
for vendor in tiingo polygon eodhd; do
    LATEST_LOG=$(ls -t /tmp/${vendor}_30year_backfill_*.log 2>/dev/null | head -1)
    if [ -n "$LATEST_LOG" ]; then
        echo "📝 ${vendor^} Latest Log: $(basename $LATEST_LOG)"
        LAST_ACTIVITY=$(stat -c %y "$LATEST_LOG" 2>/dev/null | cut -d' ' -f1-2 | cut -d'.' -f1)
        echo "⏰ Last Activity: $LAST_ACTIVITY"
        
        # Show last few log lines
        echo "🔍 Recent Activity:"
        tail -3 "$LATEST_LOG" 2>/dev/null | sed 's/^/   /'
        echo ""
    else
        echo "❌ No log file found for $vendor"
        echo ""
    fi
done

# Database record counts
echo "📊 CURRENT DATABASE PROGRESS:"
echo "-------------------------------------------------------------------"
python3 scripts/run_dev.py query --query "
SELECT 
    CASE 
        WHEN 'tiingo' = 'tiingo' THEN '🟢 Tiingo'
        WHEN 'polygon' = 'polygon' THEN '🔵 Polygon' 
        WHEN 'eodhd' = 'eodhd' THEN '🟡 EODHD'
    END as vendor,
    TO_CHAR(COUNT(*), 'FM999,999,999') as records,
    TO_CHAR(COUNT(DISTINCT instrument_id), 'FM99,999') as instruments,
    TO_CHAR(MIN(date), 'YYYY-MM-DD') as earliest,
    TO_CHAR(MAX(date), 'YYYY-MM-DD') as latest
FROM (
    SELECT 'tiingo' as source, instrument_id, date FROM dev_daily_prices_tiingo
    UNION ALL
    SELECT 'polygon' as source, instrument_id, date FROM dev_daily_prices_polygon
    UNION ALL  
    SELECT 'eodhd' as source, instrument_id, date FROM dev_daily_prices_eodhd
) combined
GROUP BY source
ORDER BY source;
" 2>/dev/null | grep -E "(Tiingo|Polygon|EODHD)" || echo "❌ Database query failed"

echo ""

# Estimated completion
echo "⏱️  ESTIMATED COMPLETION:"
echo "-------------------------------------------------------------------"
TOTAL_INSTRUMENTS=18331
echo "📋 Total Active Instruments: $(printf "%'d" $TOTAL_INSTRUMENTS)"
echo "📅 30-Year Coverage: 1995-2025 (≈7,800 trading days)"
echo "🎯 Target Records per Vendor: ≈143M records"
echo ""

# Rate limit analysis
echo "🚦 RATE LIMIT ANALYSIS:"
echo "-------------------------------------------------------------------"
echo "🟢 Tiingo: 1000 calls/hour (≈16.7/min) - 1s delays"
echo "🔵 Polygon: 5 calls/minute - 12s delays" 
echo "🟡 EODHD: 20 calls/minute - 3s delays"
echo ""
echo "⏰ Expected Total Time: 15-24 hours per vendor (parallel execution)"
echo ""

# System resources
echo "💻 SYSTEM RESOURCES:"
echo "-------------------------------------------------------------------"
echo "🖥️  CPU & Memory:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" | grep -E "(ats-dev|NAME)" | head -5
echo ""
echo "💾 Storage Usage:"
df -h /mnt/d/ | tail -1 | awk '{print "📁 Data Drive: " $3 " used / " $2 " total (" $5 " full)"}'

# Docker volume usage
POSTGRES_SIZE=$(docker system df -v | grep postgres-data-new | awk '{print $3}' | head -1)
if [ -n "$POSTGRES_SIZE" ]; then
    echo "🗄️  PostgreSQL Volume: $POSTGRES_SIZE"
fi

echo ""
echo "==================================================================="
echo "🔄 Monitor refreshes every 60 seconds. Press Ctrl+C to exit."
echo "📝 Log files: /tmp/{vendor}_30year_backfill_*.log"
echo "🔍 Detailed progress: tail -f /tmp/{vendor}_30year_backfill_*.log"
echo "==================================================================="