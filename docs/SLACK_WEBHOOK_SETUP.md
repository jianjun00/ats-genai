# Slack Webhook Setup for ATS-INTG Alerts

This guide explains how to configure Slack webhooks for ATS-INTG daily data collection and maintenance alerts.

## Overview

The daily data collection and weekly maintenance scripts send automatic notifications to Slack for:
- **Price discrepancies** detected between vendors
- **Data quality issues** (missing data, inconsistencies)  
- **Maintenance operation results**
- **System health alerts**

## Slack Webhook Configuration

### Step 1: Create Slack App and Webhook

1. Go to [Slack API Apps](https://api.slack.com/apps)
2. Click **"Create New App"** → **"From scratch"**
3. Name: `ATS-INTG-Alerts`
4. Select your workspace
5. Go to **"Incoming Webhooks"**
6. Turn on **"Activate Incoming Webhooks"**
7. Click **"Add New Webhook to Workspace"**
8. Select channel: `#ats-alerts` (recommended)
9. Copy the webhook URL (starts with `https://hooks.slack.com/services/...`)

### Step 2: Configure Environment Variable

Add the webhook URL to the ATS-INTG environment:

```bash
# In docker-compose.intg-jobs.yml, add to environment section:
- SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### Step 3: Test Webhook Configuration

Test the webhook from within the ATS-INTG container:

```bash
# Start ATS-INTG scheduler container
docker-compose -f docker-compose.intg-jobs.yml up -d ats-intg-scheduler

# Test webhook
docker exec ats-intg-scheduler bash -c "
curl -X POST -H 'Content-type: application/json' \
  --data '{\"text\":\"🧪 ATS-INTG webhook test - alerts configured successfully!\"}' \
  \$SLACK_WEBHOOK_URL
"
```

## Alert Types and Formats

### Daily Data Collection Alerts

**Price Discrepancy Alert:**
```
🚨 **Daily Price Data Alert**
Found 5 price discrepancies:
• Major: 2
• Moderate: 3

**Major Discrepancies:**
• AAPL 2024-09-01 close: 225.50 vs 226.10 (2.1%)
• TSLA 2024-09-01 high: 245.80 vs 248.20 (3.2%)
```

**Data Collection Summary:**
```
📊 **Daily Collection Complete - 2024-09-01**
✅ Processed 1,000 symbols across 3 vendors
📈 Records: 2,845 inserted, 156 updated
⚠️ 3 minor discrepancies detected
⏱️ Completed in 12.3 minutes
```

### Weekly Maintenance Alerts

**Maintenance Summary:**
```
📅 **Weekly Maintenance Summary - 2024-09-01**

📊 **Data Quality:** 12 passed, 1 failed
🔧 **Maintenance:** 4/4 operations successful
📈 **Data Volume:** 2,845,123 total price records

⚠️ **Recommendations:**
• Address 1 failing data quality metric
• Disk space is running low - consider expanding storage
```

### Health Check Alerts

**System Health Alert:**
```
❌ **ATS-INTG Health Alert**
Daily data collection health check failed:
• Error: Database connection timeout
• Last successful collection: 2024-09-01 03:00
• Action required: Check database connectivity
```

## Alert Configuration

### Alert Thresholds

The scripts use these thresholds for alerts:

**Price Discrepancies:**
- **Minor**: 1-5% difference
- **Moderate**: 5-10% difference  
- **Major**: >10% difference

**Data Quality:**
- **Data Completeness**: <95% triggers alert
- **Data Freshness**: >5 days old triggers alert
- **Price Consistency**: <99% consistent OHLC triggers alert

**System Health:**
- **Failed operations**: Any maintenance operation failure
- **High error rate**: >10% symbols failing collection
- **Disk space**: <20% free space remaining

### Customizing Alert Frequency

Edit the cron schedule in `scripts/intg_startup_manager.py`:

```bash
# Current schedule:
0 3 * * * ...daily_data_refresh.py...        # Daily at 3 AM
0 9,15,21 * * * ...priority symbols...       # Every 6 hours for priority symbols
0 */4 * * * ...health check...               # Every 4 hours health check
0 4 * * 0 ...weekly_maintenance.py...        # Weekly on Sunday at 4 AM
```

## Troubleshooting

### Common Issues

**1. Webhook URL Not Working:**
```bash
# Test webhook manually
curl -X POST -H 'Content-type: application/json' \
  --data '{"text":"Test message"}' \
  https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

**2. Environment Variable Not Set:**
```bash
# Check if webhook URL is available in container
docker exec ats-intg-scheduler env | grep SLACK_WEBHOOK_URL
```

**3. No Alerts Received:**
- Check container logs: `docker logs ats-intg-scheduler`
- Verify cron jobs are running: `docker exec ats-intg-scheduler crontab -l`
- Check alert thresholds in scripts

### Manual Alert Testing

Test alerts without waiting for scheduled jobs:

```bash
# Test daily data collection (single symbol)
docker exec ats-intg-scheduler bash -c "
  cd /workspace && PYTHONPATH=/workspace/src \
  python3 scripts/daily_data_refresh.py --symbols AAPL --vendors tiingo --debug
"

# Test weekly maintenance
docker exec ats-intg-scheduler bash -c "
  cd /workspace && PYTHONPATH=/workspace/src \
  python3 scripts/weekly_maintenance.py --debug
"
```

## Security Considerations

**Webhook URL Security:**
- ✅ Store webhook URL as environment variable (not hardcoded)
- ✅ Use private Slack channels for sensitive alerts
- ✅ Rotate webhook URLs periodically
- ❌ Never commit webhook URLs to version control

**Alert Content:**
- ✅ Include only necessary data in alerts
- ✅ Avoid exposing API keys or passwords
- ✅ Use symbols instead of internal IDs where possible

## Integration with Existing Monitoring

The Slack alerts complement the existing WSL monitoring system:

- **WSL Monitor**: System-level alerts (CPU, memory, disk, process health)
- **ATS-INTG Alerts**: Data-specific alerts (price discrepancies, data quality)
- **Both systems**: Use the same `#ats-alerts` Slack channel for centralized monitoring

## Next Steps

1. **Configure webhook URL** in ATS-INTG environment
2. **Start ATS-INTG scheduler** to begin daily data collection
3. **Monitor alerts** for the first week to tune thresholds
4. **Expand monitoring** to additional symbols or vendors as needed

The system is designed to be "chatty" initially to help identify data quality issues, then can be tuned for production alerting levels.