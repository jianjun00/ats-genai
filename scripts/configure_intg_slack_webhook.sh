#!/bin/bash

# Configure ATS-INTG Slack Webhook for Disk Usage Alerts
# This script helps set up the Slack webhook URL for disk usage monitoring

echo "=== ATS-INTG Slack Webhook Configuration ==="
echo ""
echo "This script will help you configure Slack webhook integration for disk usage alerts."
echo ""

# Check if webhook URL is already configured
CURRENT_WEBHOOK=$(grep "slack_api_url:" /home/jianjun/ats-genai-data/monitoring/alertmanager/alertmanager-intg.yml | cut -d "'" -f 2)

if [[ "$CURRENT_WEBHOOK" == "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK" ]]; then
    echo "⚠️  Slack webhook is not configured yet."
    echo ""
    echo "To set up Slack alerts for disk usage:"
    echo "1. Go to your Slack workspace"
    echo "2. Create a new app at https://api.slack.com/apps"
    echo "3. Enable 'Incoming Webhooks'"
    echo "4. Create webhooks for these channels:"
    echo "   - #ats-intg-disk-alerts (for disk usage warnings)"
    echo "   - #ats-intg-critical (for critical alerts)"
    echo "   - #ats-intg-data-quality (for data quality issues)"
    echo ""
    echo "5. Update the webhook URL in:"
    echo "   monitoring/alertmanager/alertmanager-intg.yml"
    echo ""
    echo "Example webhook URL format:"
    echo "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"
else
    echo "✅ Slack webhook is configured: $CURRENT_WEBHOOK"
fi

echo ""
echo "=== Disk Usage Alert Configuration ==="
echo "The following alerts have been configured:"
echo ""
echo "🟡 Warning Alerts (70% threshold):"
echo "   - C: Drive usage > 70%"
echo "   - D: Drive usage > 70% (ATS Data)"
echo ""
echo "🔴 Critical Alerts (85% threshold):"
echo "   - C: Drive usage > 85%"
echo "   - D: Drive usage > 85% (ATS Data)"
echo ""

echo "=== Testing Configuration ==="
echo "To test the monitoring setup:"
echo ""
echo "1. Start the monitoring stack:"
echo "   docker-compose -f docker-compose.monitoring-intg.yml up -d"
echo ""
echo "2. Access the services:"
echo "   - Grafana: http://localhost:4002 (admin/ats-intg-monitoring-password)"
echo "   - Prometheus: http://localhost:4091"
echo "   - AlertManager: http://localhost:9094"
echo ""
echo "3. View the disk usage dashboard:"
echo "   http://localhost:4002/d/disk-usage-dashboard"
echo ""
echo "4. Check current disk usage:"

echo "   C: Drive:"
df -h /mnt/c 2>/dev/null || echo "   (C: drive not accessible from current location)"

echo "   D: Drive:"
df -h /mnt/d 2>/dev/null || echo "   (D: drive not accessible from current location)"

echo ""
echo "=== Manual Testing ==="
echo "To manually trigger a disk usage alert for testing:"
echo "1. Fill up disk space temporarily (be careful!)"
echo "2. Or modify the threshold in alert_rules-intg.yml to a lower value"
echo "3. Wait 5 minutes for the alert to trigger"
echo "4. Check Slack for the alert message"
echo ""

echo "Configuration complete! 🎉"