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

These files contain environment variables including database credentials and API keys:

```
# Database configuration
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=timescaledb
DB_PORT=5432
DB_NAME=dev_db

# Market Data API Keys (CRITICAL: Never hardcode these)
POLYGON_API_KEY=your_polygon_api_key_here
TIINGO_API_KEY=your_tiingo_api_key_here
EODHD_API_KEY=your_eodhd_api_key_here
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

## 🚨 CRITICAL: API Key Security Incident (2025-08-27)

**MAJOR SECURITY VULNERABILITY RESOLVED:**
- **Impact**: Hardcoded API keys found in 18+ files across codebase
- **Exposed Keys**: Polygon (`wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD`), Tiingo, EODHD API keys
- **Risk**: Credential exposure in version control, logs, documentation, and container environments
- **Resolution**: Systematic replacement with environment variable patterns

### ✅ **CORRECT API Key Usage**
```python
# ✅ ALWAYS use environment variables
import os

polygon_api_key = os.getenv('POLYGON_API_KEY')
if not polygon_api_key:
    raise ValueError("POLYGON_API_KEY environment variable is required")

tiingo_api_key = os.getenv('TIINGO_API_KEY')
eodhd_api_key = os.getenv('EODHD_API_KEY')
```

### ❌ **NEVER Do This**
```python
# ❌ NEVER hardcode API keys
polygon_api_key = "wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"  # SECURITY VIOLATION!
tiingo_api_key = "5f40b4f36e171405746304ec0e5a6f3aa9ca77e5"    # SECURITY VIOLATION!

# ❌ NEVER use API keys in:
# - Documentation examples
# - Test files (use test_api_key_placeholder)
# - Configuration templates (use your_api_key_here)
# - Log messages or error outputs
```

### 🔒 **Security Prevention Measures**

**Automated Detection:**
```bash
# Run security regression tests before commit
python3 scripts/run_regression_tests.py --category security --fast

# Scan codebase for hardcoded secrets
python3 tests/regression/test_hardcoded_api_keys_security.py
```

**Environment File Security:**
- `.env.test` - Contains working keys for operations (controlled exception)
- `.env.template` - Uses placeholder values only
- `.env.dev`, `.env.prod` - Use placeholder values, real keys in deployment

**Git Security:**
```bash
# Check git history for leaked secrets
git log --all --full-history -- "**/*.py" "**/*.md" | grep -i "api.*key"

# Use pre-commit hooks to prevent secret commits
pre-commit install
```

## Best Practices

1. **🚨 NEVER commit sensitive credentials** to the repository
2. **🔐 ALWAYS use environment variables** for all API keys and secrets
3. **Always use `.env` files as the single source of truth** for credentials
4. **Use placeholder values** in documentation and templates:
   - `your_polygon_api_key_here`
   - `your_api_key_here`
   - `test_api_key_placeholder` (in tests)
5. **Update Kubernetes secrets** whenever credentials change:
   ```bash
   ./scripts/create_k8s_secrets.sh --all-envs --apply
   ```
6. **Ensure namespaces exist** before applying secrets:
   ```bash
   ./scripts/create_k8s_secrets.sh --all-envs --create-ns --apply
   ```
7. **Use environment-specific database names** to prevent accidental data manipulation:
   - `dev_db` for development
   - `intg_db` for integration
8. **🛡️ Run security tests** before every deployment:
   ```bash
   python3 scripts/run_regression_tests.py --category security
   ```

## 🔍 **Security Verification**

Before deploying or committing code:

1. **API Key Scan:**
   ```bash
   # Search for potential hardcoded keys
   grep -r "sk-" . --exclude-dir=.git
   grep -r "wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD" . --exclude-dir=.git
   ```

2. **Environment Variable Validation:**
   ```bash
   # Check that scripts use os.getenv()
   grep -r "POLYGON_API_KEY" --include="*.py" . | grep -v "os.getenv"
   ```

3. **Regression Test Execution:**
   ```bash
   # Full security regression test suite
   python3 scripts/run_regression_tests.py --category security --integration
   ```
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
