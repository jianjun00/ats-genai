# Instrument Agent Kubernetes Deployment with ArgoCD

This directory contains Kubernetes manifests for deploying the Instrument Agent using ArgoCD for GitOps-based continuous deployment.

## Components

The deployment includes the following components:

1. **Instrument Agent**
   - CronJob for daily updates (runs after market close)
   - Job template for backfill operations
   - ConfigMap for configuration
   - Secret for sensitive data

## Prerequisites

- Kubernetes cluster with ArgoCD installed
- Namespace `ats-dev` (will be created automatically by ArgoCD)
- Secrets for the Instrument Agent (must be created separately)
- Registry authentication for pulling images

## Setup Instructions

### 1. Registry Authentication

Before deploying, set up registry authentication:

```bash
# Run the setup script with your project ID and namespace
./setup-registry-auth.sh ats-genai ats-dev
```

This script will:
- Create a service account with appropriate permissions
- Generate and download a key for the service account
- Create a Kubernetes secret for registry authentication
- Clean up sensitive files

### 2. Create Secrets

The secrets in this repository contain placeholders and should not be applied directly. Instead, use the automated secret creation script that reads from environment files:

```bash
# Create the instrument-agent-secrets using environment files
./create-secrets.sh [environment]
```

Where `[environment]` is one of `dev`, `test`, or `prod`. If not specified, it defaults to `dev`.

The script will:
- Read values from the corresponding environment file (`.env.dev`, `.env.test`, or `.env.prod`)
- Create the Kubernetes secret in the appropriate namespace
- Display confirmation of successful creation

See the [Secret Management Documentation](../../SECRET_MANAGEMENT.md) for more details.

### 3. Deploy with ArgoCD

Apply the ArgoCD Application manifest:

```bash
kubectl apply -f argocd-application.yaml -n argocd
```

ArgoCD will automatically sync and deploy all resources in this directory to the `ats-dev` namespace.

## Monitoring Deployment

Monitor the deployment status in the ArgoCD UI or using the ArgoCD CLI:

```bash
argocd app get instrument-agent-dev
```

## Troubleshooting

If you encounter issues with the deployment:

1. Check the ArgoCD application status:
   ```bash
   argocd app get instrument-agent-dev
   ```

2. Check the pod logs:
   ```bash
   # For the daily job
   kubectl logs -l app=instrument-agent -n ats-dev
   
   # For the backfill job
   kubectl logs -l job-name=instrument-agent-backfill -n ats-dev
   ```

3. Verify that the secrets exist and are correctly referenced in the jobs:
   ```bash
   kubectl get secrets -n ats-dev
   ```

4. Check if registry authentication is working:
   ```bash
   kubectl get secret registry-credentials -n ats-dev
   ```

## CI/CD Integration

This setup can be integrated with your CI/CD pipeline:

1. On code changes, build and push a new Docker image for the Instrument Agent
2. Update the image tag in the job and cronjob YAML files
3. Commit and push the changes to the repository
4. ArgoCD will automatically detect the changes and update the deployment
