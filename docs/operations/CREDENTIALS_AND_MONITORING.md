# Credentials and Monitoring Configuration

**Document Version:** 1.0  
**Created:** August 23, 2025  
**Last Updated:** August 23, 2025  

---

## 🔐 Slack Integration Credentials

### Primary Webhook URL
**Active Webhook:** `https://hooks.slack.com/services/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`

### Kubernetes Secrets Configuration

#### 1. Monitoring Namespace
```bash
# Primary slack webhook secret (used by alertmanager)
kubectl get secret slack-webhook -n monitoring
# Contains: url (base64 encoded webhook URL)
```

#### 2. ATS-Dev Namespace  
```bash
# Slack credentials for development environment
kubectl get secret slack-credentials -n ats-dev
# Contains: webhook_url (webhook URL for dev alerts)

# Created with:
kubectl create secret generic slack-credentials -n ats-dev \
  --from-literal=webhook_url="https://hooks.slack.com/services/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"
```

#### 3. Database Credentials
```bash
# Development database credentials
kubectl get secret db-credentials-dev -n ats-dev
# Contains: DB_USER=postgres, DB_PASSWORD=dev_password, DB_NAME=dev_db

# Integration database credentials  
kubectl get secret db-credentials-intg -n ats-intg
# Contains: DB_USER=postgres, DB_PASSWORD=postgres, DB_NAME=intg_db

# Production database credentials
kubectl get secret db-credentials-prod -n ats-prod
# Contains: DB_USER=postgres, DB_PASSWORD=prod_password, DB_NAME=prod_db
```

---

## 📊 Active Monitoring Services

### 1. PostgreSQL Slack Monitoring (Multi-Environment)

#### Development Environment (ats-dev)
**Deployment:** `postgres-slack-alerts`  
**Database:** `dev_db` with dev_password
**Alert Channel:** `#ats-dev-alerts`  
**Check Interval:** Every 5 minutes  
**Thresholds:** Long queries >10min, connections >80

```bash
kubectl get pods -n ats-dev -l app=postgres-slack-alerts
kubectl logs -n ats-dev deployment/postgres-slack-alerts
```

#### Integration Environment (ats-intg)
**Deployment:** `postgres-slack-alerts`  
**Database:** `intg_db` with postgres password
**Alert Channel:** `#ats-intg-alerts`  
**Check Interval:** Every 5 minutes  
**Thresholds:** Long queries >10min, connections >50

```bash
kubectl get pods -n ats-intg -l app=postgres-slack-alerts
kubectl logs -n ats-intg deployment/postgres-slack-alerts
```

#### Production Environment (ats-prod)
**Deployment:** `postgres-slack-alerts`  
**Database:** `prod_db` with prod_password
**Alert Channel:** `#ats-prod-alerts`  
**Check Interval:** Every 3 minutes (more frequent)  
**Enhanced Monitoring:** Cache hit ratio, blocked queries, connection usage %
**Thresholds:** Long queries >5min, connections >70% (warning), >90% (critical)

```bash
kubectl get pods -n ats-prod -l app=postgres-slack-alerts
kubectl logs -n ats-prod deployment/postgres-slack-alerts
```

**Common Monitoring Features:**
- Database connectivity and health checks
- Long-running query detection  
- Connection count monitoring
- Table count verification
- Database size tracking
- Startup/shutdown notifications

### 2. General Job Notifier (ats-dev)
**Deployment:** `slack-job-notifier`  
**Purpose:** Kubernetes job completion/failure notifications  
**Alert Channels:** `#ats-dev-alerts`  

```bash
# Check status  
kubectl get pods -n ats-dev -l app=slack-job-notifier
kubectl logs -n ats-dev deployment/slack-job-notifier
```

### 3. Monitoring Infrastructure (monitoring namespace)
**Components:**
- Alertmanager with Slack integration
- Prometheus metrics collection
- Custom alert rules

```bash
# Check monitoring stack
kubectl get all -n monitoring
```

---

## 🚨 Alert Configuration

### PostgreSQL Alerts (Environment-Specific)

#### Development (#ats-dev-alerts)
- ❌ **CRITICAL**: Database connection failures
- ⚠️ **WARNING**: Long-running queries (>10 minutes)  
- ⚠️ **WARNING**: High connection count (>80)
- 📊 **INFO**: Regular health status every 5 minutes

#### Integration (#ats-intg-alerts) 
- ❌ **CRITICAL**: Database connection failures
- ⚠️ **WARNING**: Long-running queries (>10 minutes)
- ⚠️ **WARNING**: High connection count (>50)
- 📊 **INFO**: Integration table count monitoring
- 📊 **INFO**: Database size tracking

#### Production (#ats-prod-alerts) - Enhanced Monitoring
- 🚨 **CRITICAL**: Database connection failures
- 🚨 **CRITICAL**: Connection usage >90%  
- 🚨 **CRITICAL**: >5 blocked queries
- ⚠️ **WARNING**: Connection usage >70%
- ⚠️ **WARNING**: Long-running queries (>5 minutes) - Stricter threshold
- ⚠️ **WARNING**: Cache hit ratio <95% - Higher threshold
- 📊 **INFO**: Enhanced metrics every 3 minutes

**Sample Alert Format:**
```json
{
  "text": ":warning: ATS Dev PostgreSQL Alert",
  "attachments": [{
    "color": "warning",
    "fields": [
      {"title": "Status", "value": "warning"},
      {"title": "Issues", "value": "2 long-running queries (>10min)"},
      {"title": "Connections", "value": "45"},
      {"title": "Timestamp", "value": "2025-08-23 15:30:00"}
    ]
  }]
}
```

### Job Alerts
**Triggers:**
- ✅ Job completions
- ❌ Job failures
- ⚠️ Long-running jobs

---

## 🔧 Setup Instructions

### Adding New Environment Monitoring

#### 1. Create Slack Credentials Secret
```bash
# For new environment (replace <namespace>)
kubectl create secret generic slack-credentials -n <namespace> \
  --from-literal=webhook_url="https://hooks.slack.com/services/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"
```

#### 2. Deploy PostgreSQL Monitoring
```bash
# Copy and modify postgres-slack-alerts.yaml for new environment
# Update namespace, database connection details, and alert channels
kubectl apply -f k8s/<env>/postgres-slack-alerts.yaml
```

#### 3. Verify Setup
```bash
# Check pods are running
kubectl get pods -n <namespace> -l app=postgres-slack-alerts

# Check logs for successful monitoring
kubectl logs -n <namespace> deployment/postgres-slack-alerts

# Test alert by creating a temporary issue (optional)
```

### Updating Webhook URL

#### 1. Update Primary Secret
```bash
# Update monitoring namespace secret
kubectl patch secret slack-webhook -n monitoring \
  --type='json' -p='[{"op": "replace", "path": "/data/url", "value": "'$(echo -n "NEW_WEBHOOK_URL" | base64)'"}]'
```

#### 2. Update Environment Secrets
```bash
# Update each environment
kubectl patch secret slack-credentials -n ats-dev \
  --type='json' -p='[{"op": "replace", "path": "/data/webhook_url", "value": "'$(echo -n "NEW_WEBHOOK_URL" | base64)'"}]'

# Repeat for ats-intg, ats-prod as needed
```

#### 3. Restart Monitoring Services
```bash
kubectl rollout restart deployment postgres-slack-alerts -n ats-dev
kubectl rollout restart deployment slack-job-notifier -n ats-dev
```

---

## 📋 Environment-Specific Details

### Development (ats-dev)
- **Database**: postgres:5432/dev_db (dev_password)
- **Slack Channel**: #ats-dev-alerts  
- **Monitoring**: postgres-slack-alerts, slack-job-notifier
- **Alert Frequency**: Every 5 minutes
- **Services**: 24 total, including unified-analytics on port 30001

### Integration (ats-intg)  
- **Database**: postgres:5432/intg_db (postgres)
- **Slack Channel**: #ats-intg-alerts (if different from dev)
- **Services**: unified-analytics on port 30004
- **Note**: Currently uses same webhook as dev environment

### Production (ats-prod)
- **Database**: postgres:5432/prod_db (prod_password)
- **Slack Channel**: #ats-prod-alerts (critical alerts)
- **Services**: unified-analytics on port 30005 (2 replicas for HA)
- **Note**: Requires separate webhook configuration for production alerts

---

## 🔍 Troubleshooting

### Common Issues

#### 1. Secret Not Found Error
```bash
# Check if secret exists
kubectl get secrets -n <namespace> | grep slack

# Recreate if missing
kubectl create secret generic slack-credentials -n <namespace> \
  --from-literal=webhook_url="WEBHOOK_URL"
```

#### 2. Monitoring Pod Not Starting
```bash
# Check pod status and events
kubectl describe pod -n <namespace> -l app=postgres-slack-alerts

# Check logs for errors
kubectl logs -n <namespace> deployment/postgres-slack-alerts
```

#### 3. No Alerts Received
```bash
# Verify webhook URL is correct
kubectl get secret slack-credentials -n <namespace> -o yaml

# Check monitoring logs
kubectl logs -n <namespace> deployment/postgres-slack-alerts --tail=50

# Test webhook manually (replace with actual URL)
curl -X POST https://hooks.slack.com/services/... \
  -H 'Content-type: application/json' \
  --data '{"text":"Test message from ATS monitoring"}'
```

#### 4. Database Connection Issues
```bash
# Check postgres pod is running
kubectl get pods -n <namespace> -l app=postgres

# Test database connectivity
kubectl exec -n <namespace> deployment/postgres -- \
  psql -U postgres -d <database> -c "SELECT version();"
```

---

## 📚 Related Documentation

- [System Architecture](../architecture/SYSTEM_ARCHITECTURE.md)
- [Deployment Guide](DEPLOYMENT_GUIDE.md)  
- [Monitoring Guide](MONITORING.md)
- [Troubleshooting Guide](TROUBLESHOOTING.md)

---

## 🔄 Maintenance

### Regular Tasks
- **Weekly**: Verify all monitoring services are running
- **Monthly**: Test alert functionality end-to-end
- **Quarterly**: Review and update webhook URLs if needed
- **As Needed**: Add monitoring for new environments

### Alert Testing
```bash
# Create test long-running query to trigger alert
kubectl exec -n ats-dev deployment/postgres -- \
  psql -U postgres -d dev_db -c "SELECT pg_sleep(600);" &

# Monitor for Slack alert within 5 minutes
# Kill test query: kubectl exec -n ats-dev deployment/postgres -- pkill -f pg_sleep
```

---

*This document should be updated whenever new credentials are added or monitoring configurations change.*