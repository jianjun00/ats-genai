#!/bin/bash
# ATS Collection Monitoring Cron Job
# Schedule with: */15 * * * * /path/to/cron_collection_monitor.sh

# Set working directory
cd /home/jianjun/ats-genai-admin

# Log file
LOG_FILE="/tmp/collection_monitor_cron.log"
DATE=$(date '+%Y-%m-%d %H:%M:%S')

echo "[$DATE] Starting collection monitor check..." >> $LOG_FILE

# Check if Slack webhook is configured
if [ -z "$SLACK_WEBHOOK_URL" ]; then
    echo "[$DATE] SLACK_WEBHOOK_URL not set, skipping Slack alerts" >> $LOG_FILE
    exit 0
fi

# Run status check and send to Slack
python3 scripts/slack_collection_alerts.py --alert-type status >> $LOG_FILE 2>&1

# Check exit code
if [ $? -eq 0 ]; then
    echo "[$DATE] Successfully sent status to Slack" >> $LOG_FILE
else
    echo "[$DATE] Failed to send status to Slack" >> $LOG_FILE
fi

echo "[$DATE] Collection monitor check completed" >> $LOG_FILE