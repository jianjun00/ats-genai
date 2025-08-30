#!/bin/bash
# ATS-INTG Job Startup Script
# Ensures all jobs are properly initialized and running

set -e

echo "🚀 Starting ATS-INTG Job Infrastructure..."

# Check database connectivity
echo "🔍 Checking database connectivity..."
PGPASSWORD=${DB_PASSWORD} psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -d ${DB_NAME} -c "SELECT version();" > /dev/null
echo "✅ Database connected"

# Fix job infrastructure
echo "🔧 Fixing job infrastructure..."
python3 /workspace/scripts/fix_intg_job_issues.py

# Start monitoring
echo "📊 Starting job monitoring..."
python3 /workspace/scripts/monitor_daily_jobs.py --daemon &

echo "✅ ATS-INTG jobs initialized successfully"
