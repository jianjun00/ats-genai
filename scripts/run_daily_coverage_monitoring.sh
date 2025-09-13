#!/bin/bash
# Daily Coverage Monitoring Script
# Runs comprehensive coverage analysis and alerting

set -e

# Load environment
cd /home/jianjun/ats-genai-pm
source .env.monitoring 2>/dev/null || true

# Set required environment variables
export PYTHONPATH="/home/jianjun/ats-genai-pm/src"
export DB_HOST="${DB_HOST:-localhost}"
export DB_PORT="${DB_PORT:-4432}"
export DB_USER="${DB_USER:-postgres}"
export DB_PASSWORD="${DB_PASSWORD:-intg_password}"
export DB_NAME="${DB_NAME:-intg_db}"

# Logging
LOG_FILE="/home/jianjun/ats-genai-pm/logs/monitoring/daily_monitoring_$(date +%Y%m%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

echo "$(date): Starting daily coverage monitoring" >> "$LOG_FILE"

# Run coverage monitoring
python3 src/monitoring/coverage_monitor.py >> "$LOG_FILE" 2>&1

# Run alert check
python3 src/monitoring/alert_system.py >> "$LOG_FILE" 2>&1

echo "$(date): Daily coverage monitoring completed" >> "$LOG_FILE"
