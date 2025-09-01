#!/bin/bash
# Restart WSL System Monitoring
# This script ensures monitoring is always running

set -e

MONITOR_SCRIPT="simple_wsl_monitor.py"
WORK_DIR="/home/jianjun/ats-genai-data/scripts/monitoring"
LOCK_FILE="/tmp/wsl_monitor.lock"
LOG_FILE="/mnt/d/ats-logs/wsl_monitor.log"

echo "🔧 Restarting WSL System Monitoring..."

# Create logs directory if it doesn't exist
mkdir -p /mnt/d/ats-logs

# Kill any existing monitoring processes
echo "🛑 Stopping existing monitors..."
pkill -f "$MONITOR_SCRIPT" 2>/dev/null || echo "No existing monitors found"

# Remove old lock file
rm -f "$LOCK_FILE"

# Wait a moment for processes to clean up
sleep 2

# Start new monitoring process
echo "🚀 Starting new monitoring process..."
cd "$WORK_DIR"
nohup python3 "$MONITOR_SCRIPT" --hourly > "$LOG_FILE" 2>&1 &
MONITOR_PID=$!

echo "✅ WSL System Monitor started (PID: $MONITOR_PID)"
echo "📋 Log file: $LOG_FILE"
echo "🔍 Check status: ps aux | grep simple_wsl_monitor"

# Send initial test alert to confirm it's working
echo "📱 Sending initial alert to confirm setup..."
python3 "$MONITOR_SCRIPT" --test

echo "✅ WSL monitoring is now active and will send hourly Slack updates!"