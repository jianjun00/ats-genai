# Slack Notifications for ATS Job Monitoring

## 🔔 Overview

The ATS Slack notification system provides real-time alerts for Kubernetes job status changes, including:

- **Job Completion** notifications (success/failure)
- **Long-running job progress** updates (every 4 hours)
- **Job start** notifications for critical jobs
- **Error alerts** for failed jobs

## 🚀 Quick Setup

### 1. Create Slack Webhook URL

1. Go to your Slack workspace
2. Navigate to **Apps** → **Incoming Webhooks**
3. Click **Add to Slack** and select your channel (e.g., `#ats-dev-alerts`)
4. Copy the webhook URL (looks like: `https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX`)

### 2. Configure and Deploy

```bash
# Setup webhook secret
./scripts/monitoring/setup_slack_notifications.sh --webhook-url "YOUR_WEBHOOK_URL"

# Deploy to Kubernetes
./scripts/monitoring/setup_slack_notifications.sh --deploy

# Send test notification
./scripts/monitoring/setup_slack_notifications.sh --test
```

### 3. Verify Setup

```bash
# Check status
./scripts/monitoring/setup_slack_notifications.sh --status

# Monitor logs
kubectl logs -l app=slack-job-notifier -n ats-dev --follow
```

## 📋 Configuration Options

### Environment Variables

- **SLACK_WEBHOOK_URL**: Slack webhook URL (required)
- **SLACK_CHANNEL**: Target channel (default: `#ats-dev-alerts`)
- **NOTIFICATION_INTERVAL_HOURS**: Progress update interval (default: 4 hours)

### Monitored Job Patterns

The system monitors jobs matching these patterns:
- `comprehensive-30year-backfill`
- `minute-backfill`
- `price-unification`
- `market-data`
- `model-training`
- `query-` (dev CLI queries)

## 📊 Notification Types

### Job Completion ✅
```
✅ Job Completed Successfully: comprehensive-30year-backfill
Job `comprehensive-30year-backfill` completed successfully.

Namespace: ats-dev
Status: Complete
Started: 2025-08-19 15:06:11 UTC
Completed: 2025-08-29 10:30:45 UTC
Duration: 9 days, 19:24:34
```

### Job Failure ❌
```
❌ Job Failed: market-data-sync
Job `market-data-sync` failed.

Namespace: ats-dev
Status: Failed
Started: 2025-08-21 08:00:00 UTC
Duration: 0:15:30
```

### Progress Update 🔄
```
🔄 Job Progress Update: comprehensive-30year-backfill
Job `comprehensive-30year-backfill` is still running and making progress.

Namespace: ats-dev
Status: Running
Started: 2025-08-19 15:06:11 UTC
Running Time: 2 days, 9:45:12
```

## 🛠️ Management Commands

### Check Current Status
```bash
./scripts/monitoring/setup_slack_notifications.sh --status
```

### Update Webhook URL
```bash
./scripts/monitoring/setup_slack_notifications.sh --webhook-url "NEW_WEBHOOK_URL"
```

### Restart Notifier
```bash
kubectl rollout restart deployment/slack-job-notifier -n ats-dev
```

### View Logs
```bash
kubectl logs -l app=slack-job-notifier -n ats-dev --tail=50
```

### Stop Notifications
```bash
kubectl scale deployment slack-job-notifier --replicas=0 -n ats-dev
```

### Resume Notifications
```bash
kubectl scale deployment slack-job-notifier --replicas=1 -n ats-dev
```

## 🔧 Troubleshooting

### Common Issues

**1. Webhook URL Not Working**
```bash
# Test webhook manually
curl -X POST -H 'Content-type: application/json' \
    --data '{"text":"Test message"}' \
    YOUR_WEBHOOK_URL
```

**2. No Notifications Appearing**
```bash
# Check pod status
kubectl get pods -l app=slack-job-notifier -n ats-dev

# Check logs for errors
kubectl logs -l app=slack-job-notifier -n ats-dev
```

**3. Missing Permissions**
```bash
# Verify service account permissions
kubectl auth can-i get jobs --as=system:serviceaccount:ats-dev:slack-job-notifier
```

### Debug Mode

Enable debug logging:
```bash
kubectl set env deployment/slack-job-notifier LOG_LEVEL=DEBUG -n ats-dev
```

## 📈 Monitoring Coverage

### Current Job Monitoring

The system automatically monitors:

- **Comprehensive 30-year backfill** (currently running)
- **Minute data backfills**
- **Price unification jobs**
- **Model training jobs**
- **Dev CLI queries**

### Custom Job Monitoring

To monitor additional job patterns, update the `monitored_patterns` list in the deployment:

```yaml
env:
- name: MONITORED_PATTERNS
  value: "my-custom-job,another-pattern"
```

## 🔒 Security Notes

- Webhook URLs contain sensitive tokens - store securely in Kubernetes secrets
- Use dedicated Slack channels for different environments (dev/staging/prod)
- Limit webhook permissions to posting messages only
- Regularly rotate webhook URLs for security

## 📱 Example Slack Channel Setup

**Channel: #ats-dev-alerts**
- **Purpose**: ATS development environment job notifications
- **Members**: Development team, DevOps team
- **Retention**: 30 days
- **Integrations**: ATS Job Monitor webhook

**Channel: #ats-prod-alerts**  
- **Purpose**: Production environment critical alerts
- **Members**: On-call engineers, team leads
- **Retention**: 90 days
- **Escalation**: PagerDuty integration for failures

## 🎯 Benefits

- **Real-time awareness** of job status changes
- **Proactive monitoring** of long-running processes
- **Quick failure detection** and response
- **Historical tracking** of job performance
- **Team collaboration** around data operations

The Slack notification system provides comprehensive monitoring of the ATS infrastructure, ensuring the team stays informed about critical data processing operations like the comprehensive 30-year backfill! 🚀