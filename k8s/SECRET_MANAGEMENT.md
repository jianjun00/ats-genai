# Secret Management for ATS-GenAI

This document describes the environment-based secret management approach used for the ATS-GenAI project.

## Overview

The ATS-GenAI project uses environment files (`.env.dev`, `.env.test`, `.env.prod`) as the source of truth for secrets. These files are committed to git for ease of automation and to ensure consistent environment management across development, testing, and production.

## Environment Files

The following environment files are available:

- `.env.dev` - Development environment configuration
- `.env.test` - Testing environment configuration
- `.env.prod` - Production environment configuration

Each environment file contains the following types of configuration:

- Database connection details
- API keys
- Kubernetes namespace configurations
- Project IDs

## Namespace Configuration

Each environment file contains namespace configurations for both agents:

- `K8S_NAMESPACE_DATA` - Namespace for the data-agent
- `K8S_NAMESPACE_INSTRUMENT` - Namespace for the instrument-agent

## Secret Creation

Secret creation is automated using scripts that read from the environment files:

### Data Agent

To create secrets for the data-agent:

```bash
cd /home/jianjun/ats-genai/k8s/data-agent
./create-secrets.sh [environment]
```

Where `[environment]` is one of `dev`, `test`, or `prod`. If not specified, it defaults to `dev`.

### Instrument Agent

To create secrets for the instrument-agent:

```bash
cd /home/jianjun/ats-genai/k8s/dev/instrument-agent
./create-secrets.sh [environment]
```

Where `[environment]` is one of `dev`, `test`, or `prod`. If not specified, it defaults to `dev`.

## Registry Authentication

Registry authentication is also automated using scripts:

### Data Agent

To set up registry authentication for the data-agent:

```bash
cd /home/jianjun/ats-genai/k8s/data-agent
./setup-registry-auth.sh
```

### Instrument Agent

To set up registry authentication for the instrument-agent:

```bash
cd /home/jianjun/ats-genai/k8s/dev/instrument-agent
./setup-registry-auth.sh
```

## ArgoCD Deployment

After creating secrets and setting up registry authentication, you can deploy the applications using ArgoCD:

```bash
kubectl apply -f argocd-application.yaml -n argocd
```

## Adding New Secrets

To add new secrets:

1. Add the secret key-value pair to the appropriate environment files (`.env.dev`, `.env.test`, `.env.prod`)
2. Update the corresponding `create-secrets.sh` script to include the new secret
3. Run the script to update the Kubernetes secret

## Troubleshooting

If you encounter issues with secret creation:

1. Ensure the environment file exists and contains the required values
2. Check that you have the necessary permissions to create secrets in the target namespace
3. Verify that the secret name in the script matches the one expected by the application
