# Universal Kubernetes Job Monitor with Flyte Integration

This document describes the enhanced Universal Kubernetes Job Monitor that uses Flyte workflows for scalable notifications about Kubernetes job status changes.

## Overview

The Universal Kubernetes Job Monitor is a service that monitors all Kubernetes jobs across configured namespaces and sends notifications when jobs start, complete, or fail. The enhanced version integrates with Flyte workflows to provide more scalable and reliable notifications through multiple channels:

- Slack notifications
- Email notifications
- Webhook notifications

## Architecture

The system consists of two main components:

1. **Universal Job Monitor**: A Python script running in a Kubernetes pod that continuously monitors job status changes across namespaces.
2. **Flyte Notification Workflow**: A Flyte workflow that handles sending notifications through various channels.

### Workflow

1. The monitor script polls the Kubernetes API for job status changes
2. When a job status change is detected, the monitor invokes the Flyte notification workflow
3. The Flyte workflow sends notifications based on the configured notification types

## Configuration

The Universal Job Monitor can be configured using the following environment variables:

| Environment Variable | Description | Default |
|----------------------|-------------|---------|
| `SLACK_WEBHOOK_URL` | Slack webhook URL for direct notifications | None |
| `SLACK_CHANNEL` | Slack channel to send notifications to | `#ats-dev-alerts` |
| `NOTIFICATION_INTERVAL_HOURS` | How often to send updates for running jobs | `4` |
| `MONITOR_NAMESPACES` | Comma-separated list of namespaces to monitor | `ats-dev,ats-intg,ats-prod,default` |
| `MONITOR_ALL_NAMESPACES` | Whether to monitor all namespaces | `false` |
| `USE_FLYTE` | Whether to use Flyte for notifications | `true` |
| `FLYTE_ENDPOINT` | Flyte service endpoint | `flyte.ats-dev.svc.cluster.local:30081` |
| `FLYTE_PROJECT` | Flyte project name | `ats-monitoring` |
| `FLYTE_DOMAIN` | Flyte domain | `development` |
| `NOTIFICATION_TYPES` | Comma-separated list of notification types | `slack,email,webhook` |
| `EMAIL_RECIPIENTS` | Comma-separated list of email recipients | None |
| `WEBHOOK_URL` | Webhook URL for webhook notifications | None |
| `SMTP_USERNAME` | SMTP username for email notifications | None |
| `SMTP_PASSWORD` | SMTP password for email notifications | None |

## Deployment

The Universal Job Monitor is deployed as a Kubernetes Deployment with the following components:

1. **ConfigMap**: Contains the monitor script and Flyte workflow script
2. **Deployment**: Runs the monitor script in a container
3. **ServiceAccount**: Provides permissions to access the Kubernetes API
4. **ClusterRole/ClusterRoleBinding**: Grants permissions to list and watch jobs across namespaces

### Prerequisites

- Kubernetes cluster with RBAC enabled
- Flyte server accessible from the Kubernetes cluster
- Slack webhook URL (optional)
- SMTP credentials for email notifications (optional)

### Deployment Steps

1. Apply the YAML configuration:

```bash
kubectl apply -f k8s/universal-job-monitor-flyte.yaml
```

2. Verify the deployment:

```bash
kubectl get pods -n ats-dev -l app=universal-job-monitor
```

## Notification Types

### Slack Notifications

Slack notifications include:
- Job name and namespace
- Status (Running, Complete, Failed, Pending)
- Start time, completion time, and duration
- Pod counts (active, succeeded, failed)

### Email Notifications

Email notifications include:
- Job name and namespace
- Status with appropriate emoji
- Start time, completion time, and duration
- Pod counts (active, succeeded, failed)

### Webhook Notifications

Webhook notifications send a JSON payload with:
- Job name and namespace
- Status
- Timestamp
- Start time, completion time, and duration (if available)
- Pod counts (active, succeeded, failed)

## Fallback Mechanism

If Flyte integration is enabled but unavailable (due to network issues, configuration problems, etc.), the monitor will fall back to direct Slack notifications if a Slack webhook URL is configured.

## Troubleshooting

### Common Issues

1. **No notifications are being sent**
   - Check if the monitor pod is running: `kubectl get pods -n ats-dev -l app=universal-job-monitor`
   - Check the logs: `kubectl logs -n ats-dev -l app=universal-job-monitor`
   - Verify that either `USE_FLYTE` is set to `true` or `SLACK_WEBHOOK_URL` is configured

2. **Flyte notifications not working**
   - Check if the Flyte server is accessible from the monitor pod
   - Verify that the Flyte project and domain exist
   - Check the monitor logs for Flyte-related errors

3. **Email notifications not working**
   - Verify that `SMTP_USERNAME` and `SMTP_PASSWORD` are correctly configured
   - Check if the SMTP server is accessible from the monitor pod
   - Verify that `EMAIL_RECIPIENTS` is correctly configured

### Viewing Logs

```bash
kubectl logs -n ats-dev -l app=universal-job-monitor
```

## Development

### Local Testing

To test the monitor script locally:

1. Install dependencies:
```bash
pip install aiohttp flytekit kubernetes
```

2. Set environment variables:
```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/xxx/yyy/zzz"
export USE_FLYTE="false"  # For local testing without Flyte
export MONITOR_NAMESPACES="default"
```

3. Run the script:
```bash
python monitor_all_jobs.py
```

### Testing Flyte Workflow

To test the Flyte notification workflow directly:

```bash
python scripts/flyte/flyte_job_notification_workflow.py \
  --job-name=test-job \
  --namespace=default \
  --status=Running \
  --notification-types=slack \
  --slack-webhook-url="https://hooks.slack.com/services/xxx/yyy/zzz"
```
