#!/bin/bash
# Start frequent monitoring (every 5 minutes) for testing alerts
# Use this when you want to monitor system stress more closely

echo "🚀 Starting frequent WSL system monitoring (every 5 minutes)..."
echo "📱 This will send status updates to Slack every 5 minutes"
echo "💡 Use Ctrl+C to stop"
echo ""
echo "⚠️  This is for testing/debugging - use hourly monitoring for normal operation"
echo ""

cd /home/jianjun/ats-genai-model/scripts/monitoring
python3 simple_wsl_monitor.py --frequent