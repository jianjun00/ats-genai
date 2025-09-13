#!/bin/bash
# Dashboard Health Check Script
# Ensures dashboard is running and responsive

set -e

# Load environment
cd /home/jianjun/ats-genai-pm
source .env.monitoring 2>/dev/null || true

DASHBOARD_URL="http://${DASHBOARD_HOST:-localhost}:${DASHBOARD_PORT:-8080}/health"
LOG_FILE="/home/jianjun/ats-genai-pm/logs/monitoring/dashboard_health_$(date +%Y%m%d).log"

echo "$(date): Checking dashboard health at $DASHBOARD_URL" >> "$LOG_FILE"

# Check if dashboard is responding
if curl -f -s -m 10 "$DASHBOARD_URL" > /dev/null 2>&1; then
    echo "$(date): ✅ Dashboard is healthy" >> "$LOG_FILE"
else
    echo "$(date): ❌ Dashboard health check failed" >> "$LOG_FILE"
    
    # Try to restart dashboard if it's not running
    if ! pgrep -f "coverage_dashboard_fixed.py" > /dev/null; then
        echo "$(date): 🔄 Starting dashboard..." >> "$LOG_FILE"
        nohup python3 coverage_dashboard_fixed.py >> "$LOG_FILE" 2>&1 &
        sleep 5
        
        # Verify restart
        if curl -f -s -m 10 "$DASHBOARD_URL" > /dev/null 2>&1; then
            echo "$(date): ✅ Dashboard restarted successfully" >> "$LOG_FILE"
        else
            echo "$(date): ❌ Dashboard restart failed" >> "$LOG_FILE"
        fi
    fi
fi
