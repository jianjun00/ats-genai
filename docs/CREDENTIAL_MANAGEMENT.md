# Credential Management in ATS

This document describes the credential management approach for the ATS project, focusing on database credentials.

## Overview

The ATS project uses a centralized approach to credential management, with `.env` files as the single source of truth. These environment files are used to:

1. Configure local development environments
2. Generate Kubernetes secrets for deployment environments
3. Ensure consistent credentials across all environments

## Environment Files

Environment-specific configuration files are stored in the project root:

- `.env.dev` - Development environment configuration
- `.env.intg` - Integration environment configuration
- `.env.prod` - Production environment configuration
- `.env.test` - Test environment configuration

These files contain environment variables including database credentials:

```
# Database configuration
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=timescaledb
DB_PORT=5432
DB_NAME=dev_db
```

## Kubernetes Secret Management

### Converting .env Files to Kubernetes Secrets

The project includes tools to convert `.env` files to Kubernetes secrets:

1. `scripts/env_to_k8s_secrets.py` - Python script to convert `.env` files to Kubernetes secret YAML files
2. `scripts/create_k8s_secrets.sh` - Shell script wrapper for easier usage

### Usage

To generate Kubernetes secrets from `.env` files:

```bash
# Generate secrets for a specific environment
./scripts/create_k8s_secrets.sh --env-file .env.dev

# Generate secrets for all environments
./scripts/create_k8s_secrets.sh --all-envs

# Generate and apply secrets to the Kubernetes cluster
./scripts/create_k8s_secrets.sh --all-envs --apply

# Create required namespaces and generate secrets
./scripts/create_k8s_secrets.sh --all-envs --create-ns

# Create namespaces, generate and apply secrets in one command
./scripts/create_k8s_secrets.sh --all-envs --create-ns --apply
```

### Secret Naming Convention

Secrets are named using the pattern: `db-credentials-{environment}`

For example:
- `db-credentials-dev` for development environment
- `db-credentials-intg` for integration environment
- `db-credentials-prod` for production environment

## Kubernetes Job Configuration

Kubernetes jobs and pods are configured to use secrets instead of hardcoded credentials:

```yaml
env:
- name: DB_USER
  valueFrom:
    secretKeyRef:
      name: db-credentials-dev
      key: DB_USER
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: db-credentials-dev
      key: DB_PASSWORD
- name: DB_NAME
  valueFrom:
    secretKeyRef:
      name: db-credentials-dev
      key: DB_NAME
```

## Database Connection Logic

The `Database` class in `src/config/database.py` reads credentials from environment variables:

```python
self.host = db_host or host or 'localhost'
self.port = int(os.getenv("DB_PORT") or port or 5432)
self.user = os.getenv("DB_USER") or user or 'postgres'
self.password = os.getenv("DB_PASSWORD") or password or 'postgres'
```

This approach works seamlessly with both:
- Local development (environment variables from `.env` files)
- Kubernetes deployments (environment variables from secrets)

## Best Practices

1. **Never commit sensitive credentials** to the repository
2. **Always use `.env` files as the single source of truth** for credentials
3. **Update Kubernetes secrets** whenever credentials change:
   ```bash
   ./scripts/create_k8s_secrets.sh --all-envs --apply
   ```
4. **Ensure namespaces exist** before applying secrets:
   ```bash
   ./scripts/create_k8s_secrets.sh --all-envs --create-ns --apply
   ```
5. **Use environment-specific database names** to prevent accidental data manipulation:
   - `dev_db` for development
   - `intg_db` for integration
   - `trading_db` for production

## Troubleshooting

If you encounter database connection issues:

1. Verify that the required namespaces exist:
   ```bash
   kubectl get namespaces | grep ats
   ```
   If they don't exist, create them:
   ```bash
   ./scripts/create_k8s_secrets.sh --all-envs --create-ns
   ```

2. Verify that the correct secrets exist in the Kubernetes cluster:
   ```bash
   kubectl get secrets -n ats-dev
   ```

3. Check that the secret contains the expected keys:
   ```bash
   kubectl describe secret db-credentials-dev -n ats-dev
   ```

4. Verify that the pod/job has the correct environment variables:
   ```bash
   kubectl exec -n ats-dev <pod-name> -- env | grep DB_
   ```

5. Check for connection errors in the pod logs:
   ```bash
   kubectl logs -n ats-dev <pod-name>
   ```
