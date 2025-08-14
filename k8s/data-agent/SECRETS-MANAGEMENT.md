# Data Agent Secrets Management

This document explains how to securely manage credentials for the data agent deployment in the Kubernetes cluster.

## Overview

The data agent requires several API keys and credentials to function properly:

- **Polygon API Key**: For accessing Polygon market data
- **Tiingo API Key**: For accessing Tiingo market data
- **OpenAI API Key**: For AI-powered analysis features
- **Database URL**: For connecting to the database
- **Slack Webhook URL** (optional): For sending notifications to Slack

These credentials are stored as Kubernetes secrets and injected into the data agent deployment as environment variables.

## Managing Secrets

### Option 1: Using the Update Script (Recommended)

The easiest way to update the secrets is to use the provided script:

```bash
# Run the script
bash /home/jianjun/ats-genai/scripts/update-secrets.sh
```

The script will:
1. Prompt you for each credential
2. Update the secrets file with your provided values
3. Apply the updated secrets to the Kubernetes cluster (if running)
4. Restart the data agent deployment to use the new secrets

### Option 2: Manual Update

If you prefer to update the secrets manually:

1. Edit the secrets file:
   ```bash
   nano /home/jianjun/ats-genai/k8s/data-agent/data-agent-secrets.yaml
   ```

2. Replace the placeholder values with your actual credentials:
   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     name: data-agent-secrets
     namespace: market-data
   type: Opaque
   stringData:
     polygon-api-key: "YOUR_POLYGON_API_KEY"
     tiingo-api-key: "YOUR_TIINGO_API_KEY"
     openai-api-key: "YOUR_OPENAI_API_KEY"
     database-url: "YOUR_DATABASE_URL"
     slack-webhook-url: "YOUR_SLACK_WEBHOOK_URL"
   ```

3. Apply the updated secrets to the Kubernetes cluster:
   ```bash
   kubectl apply -f /home/jianjun/ats-genai/k8s/data-agent/data-agent-secrets.yaml
   ```

4. Restart the data agent deployment:
   ```bash
   kubectl rollout restart deployment/data-agent -n market-data
   ```

## Security Best Practices

1. **Restrict Access**: Keep the secrets file permissions restricted (the update script sets it to 600)
2. **Don't Commit Secrets**: Never commit the secrets file with real credentials to version control
3. **Regular Rotation**: Rotate API keys and credentials regularly
4. **Monitoring**: Monitor for unauthorized access to the secrets

## Verifying Secret Integration

To verify that the data agent is using the secrets correctly:

```bash
# Check if the secrets exist
kubectl get secrets -n market-data

# Check if the data agent pod is running with the updated secrets
kubectl get pods -n market-data

# Check the logs for any credential-related errors
kubectl logs -n market-data deployment/data-agent
```

## Troubleshooting

If you encounter issues with the secrets:

1. Verify that the secret exists in the namespace:
   ```bash
   kubectl get secrets data-agent-secrets -n market-data
   ```

2. Check that the deployment is referencing the correct secret name and keys:
   ```bash
   kubectl describe deployment data-agent -n market-data
   ```

3. Check the data agent logs for any credential-related errors:
   ```bash
   kubectl logs -n market-data deployment/data-agent
   ```
