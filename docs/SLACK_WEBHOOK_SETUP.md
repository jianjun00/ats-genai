# Environment-Specific Slack Webhook Setup

## Current Issue
- ✅ **Backup notifications are working**
- ❌ **All alerts go to the same channel instead of environment-specific channels**

## Required Setup

You need to create **separate Slack webhooks** for each environment that post to different channels:

### 1. Create Slack Channels (if not already created)
- `#ats-intg` - For integration environment alerts
- `#ats-prod` - For production environment alerts  
- `#ats-dev` - For development environment alerts

### 2. Create Environment-Specific Webhooks

#### For ats-intg environment:
1. Go to https://api.slack.com/apps
2. Select your ATS app (or create one if needed)
3. Navigate to "Incoming Webhooks"
4. Click "Add New Webhook to Workspace"
5. **Select the `#ats-intg` channel**
6. Copy the generated webhook URL

#### For ats-prod environment:
1. Repeat the same process
2. **Select the `#ats-prod` channel**
3. Copy the generated webhook URL

### 3. Update Kubernetes Secrets

Replace the webhook URLs in these files with the actual environment-specific ones:

```bash
# Update ats-intg webhook (should post to #ats-intg)
# Edit k8s/intg/slack-webhook-secret.yaml
webhook-url: "REPLACE_WITH_INTG_WEBHOOK_URL"

# Update ats-prod webhook (should post to #ats-prod)  
# Edit k8s/prod/slack-webhook-secret.yaml
webhook-url: "REPLACE_WITH_PROD_WEBHOOK_URL"

# Apply the changes
kubectl apply -f k8s/intg/slack-webhook-secret.yaml
kubectl apply -f k8s/prod/slack-webhook-secret.yaml
```

### 4. Test Environment-Specific Notifications

```bash
# Test ats-intg (should alert in #ats-intg)
kubectl create job --from=cronjob/ats-intg-database-backup test-intg-alert -n ats-intg

# Test ats-prod (should alert in #ats-prod)
kubectl create job --from=cronjob/ats-prod-database-backup test-prod-alert -n ats-prod
```

## Current Webhook Configuration

**Current State (Temporary):**
- ats-intg: `https://hooks.slack.com/services/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`
- ats-prod: `https://hooks.slack.com/services/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`
- **Issue**: Both use same webhook → both post to same channel

**Required State:**
- ats-intg: `[NEW_WEBHOOK_FOR_INTG_CHANNEL]` → posts to `#ats-intg`
- ats-prod: `[NEW_WEBHOOK_FOR_PROD_CHANNEL]` → posts to `#ats-prod`

## Verification

After setting up the environment-specific webhooks, you should see:
- ✅ ats-intg backup alerts appear in `#ats-intg` channel
- ✅ ats-prod backup alerts appear in `#ats-prod` channel
- ✅ Each alert clearly identifies its environment in the message

## Message Differentiation

The backup job scripts already include environment identification:
- **ats-intg**: "ATS Integration Database Backup" 
- **ats-prod**: "ATS Production Database Backup" with "PRODUCTION 🔴" emphasis

Once the webhooks are properly configured, you'll have complete environment separation for alerts.