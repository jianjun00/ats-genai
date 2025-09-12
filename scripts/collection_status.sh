#!/bin/bash
# ATS Collection Status Quick Check
# Simple bash script for quick status overview

echo "🔍 ATS COLLECTION JOBS STATUS - $(date)"
echo "=================================================================="

# Function to check if process is running and get basic info
check_collection() {
    local name="$1"
    local log_file="$2"
    local pattern="$3"

    echo "📊 $name:"

    if [[ ! -f "$log_file" ]]; then
        echo "   ❌ Log file not found: $log_file"
        return
    fi

    # Check last modification time (active if modified within 5 minutes)
    local last_mod=$(stat -c %Y "$log_file" 2>/dev/null)
    local current_time=$(date +%s)
    local diff=$((current_time - last_mod))

    if [[ $diff -lt 300 ]]; then
        echo "   🟢 ACTIVE (last activity: $((diff/60)) minutes ago)"
    else
        echo "   🔴 INACTIVE (last activity: $((diff/60)) minutes ago)"
    fi

    # Get progress info from last few lines
    local progress=$(tail -20 "$log_file" 2>/dev/null | grep -E "Progress:|events collected|records" | tail -1)
    if [[ -n "$progress" ]]; then
        echo "   📈 $progress"
    fi

    # Get current symbol/activity
    local current=$(tail -10 "$log_file" 2>/dev/null | grep -E "Processing|Collecting" | tail -1 | sed 's/.*- //')
    if [[ -n "$current" ]]; then
        echo "   🎯 $current"
    fi

    # Error count from last 100 lines
    local errors=$(tail -100 "$log_file" 2>/dev/null | grep -c "ERROR\|❌")
    if [[ $errors -gt 0 ]]; then
        echo "   ⚠️  Recent errors: $errors"
    fi

    echo ""
}

echo "💰 PRICE DATA BACKFILLS:"
echo "------------------------------------------------------------------"
check_collection "Polygon 30Y Daily" "/tmp/polygon_30year_daily_backfill.log" "polygon.*daily"
check_collection "Tiingo 30Y Daily" "/tmp/tiingo_30year_backfill.log" "tiingo.*daily"
check_collection "EODHD 30Y Daily" "/tmp/eodhd_30year_backfill.log" "eodhd.*daily"

echo "📅 EVENTS COLLECTION:"
echo "------------------------------------------------------------------"
check_collection "Polygon Earnings" "/tmp/polygon_earnings_fixed.log" "polygon.*earnings"
check_collection "EODHD Events" "/tmp/eodhd_events.log" "eodhd.*events"
check_collection "Tiingo Events" "/tmp/tiingo_events.log" "tiingo.*events"

echo "🔢 MINUTE DATA:"
echo "------------------------------------------------------------------"
check_collection "Polygon Minutes" "/tmp/polygon_minute_backfill.log" "polygon.*minute"

echo "📊 DATABASE SUMMARY:"
echo "------------------------------------------------------------------"

# Database record counts (requires docker/postgresql access)
if command -v psql >/dev/null 2>&1; then
    echo "📈 Price Records:"
    PGPASSWORD=dev_password psql -h localhost -p 5433 -U postgres -d dev_db -t -c "
    SELECT
        'Polygon 30Y: ' || COALESCE(p.cnt, 0) || ' records, ' || COALESCE(p.symbols, 0) || ' symbols' as polygon_30y,
        'Tiingo 30Y: ' || COALESCE(t.cnt, 0) || ' records, ' || COALESCE(t.symbols, 0) || ' symbols' as tiingo_30y,
        'EODHD 30Y: ' || COALESCE(e.cnt, 0) || ' records, ' || COALESCE(e.symbols, 0) || ' symbols' as eodhd_30y
    FROM
        (SELECT COUNT(*) as cnt, COUNT(DISTINCT symbol) as symbols FROM dev_daily_prices_polygon) p
        FULL OUTER JOIN (SELECT COUNT(*) as cnt, COUNT(DISTINCT symbol) as symbols FROM dev_daily_prices_tiingo_30year) t ON true
        FULL OUTER JOIN (SELECT COUNT(*) as cnt, COUNT(DISTINCT symbol) as symbols FROM dev_daily_prices_eodhd_30year) e ON true
    " 2>/dev/null | grep -v "^$" || echo "   ❌ Could not connect to database"

    echo ""
    echo "📅 Event Records:"
    PGPASSWORD=dev_password psql -h localhost -p 5433 -U postgres -d dev_db -t -c "
    SELECT
        vendor || ': ' || COUNT(*) || ' events (' || COUNT(DISTINCT symbol) || ' symbols)'
    FROM dev_financial_events
    GROUP BY vendor
    ORDER BY vendor
    " 2>/dev/null | grep -v "^$" || echo "   ❌ Could not connect to database"
else
    echo "   ℹ️  Install psql for database statistics"
fi

echo ""
echo "🎯 QUICK ACTIONS:"
echo "------------------------------------------------------------------"
echo "Monitor live:     python3 scripts/monitor_all_collections.py"
echo "Detailed status:  python3 scripts/monitor_all_collections.py --summary-only"
echo "Stop all jobs:    docker stop \$(docker ps -q)"
echo "View logs:        tail -f /tmp/polygon_earnings_fixed.log"
echo ""

echo "=================================================================="
echo "✅ Status check complete - $(date)"