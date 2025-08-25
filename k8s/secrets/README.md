# Kubernetes Secrets for ATS Platform

This directory contains the Kubernetes secret definitions for the ATS platform. **Never commit actual credential values to git.**

## 🔐 Available Secrets

### `api-credentials`
External API keys for data sources.

| Key | Description | Example |
|-----|-------------|---------|
| `polygon-api-key` | Polygon.io API key | `wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD` |
| `tiingo-api-key` | Tiingo API key | `5f40b4f36e171405746304ec0e5a6f3aa9ca77e5` |
| `eodhd-api-key` | EODHD API key | `679d7e11e25c51.17772351` |

### `db-credentials`
Database connection parameters.

| Key | Description | Example |
|-----|-------------|---------|
| `db-host` | Database hostname | `postgres` |
| `db-port` | Database port | `5432` |
| `db-user` | Database username | `postgres` |
| `db-password` | Database password | `dev_password` |
| `db-name` | Database name | `dev_db` |

### `git-credentials`
Git repository access for source code pulling.

| Key | Description | Example |
|-----|-------------|---------|
| `git-token` | GitHub personal access token | `ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx` |
| `git-repo-url` | Repository URL | `https://github.com/AkoloTechnologies/ats-genai.git` |

## 🚀 Deployment

```bash
# Apply all secrets to the cluster
kubectl apply -f k8s/secrets/

# Verify secrets were created
kubectl get secrets -n ats-dev
```

## 🔧 Usage in Jobs

Use `secretKeyRef` to reference secret values in your job YAML files:

```yaml
env:
- name: POLYGON_API_KEY
  valueFrom:
    secretKeyRef:
      name: api-credentials
      key: polygon-api-key
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: db-credentials
      key: db-password
```

## 🚨 Security Best Practices

1. **Base64 Encoding**: Values in secret YAML files are base64 encoded
2. **Environment Separation**: Use different secrets for dev/staging/prod
3. **Access Control**: Limit who can read secrets using RBAC
4. **Rotation**: Update credentials regularly
5. **Monitoring**: Monitor secret access in production environments

## 🔍 Troubleshooting

### View Secret Contents
```bash
# List all secrets
kubectl get secrets -n ats-dev

# View secret details (base64 encoded)
kubectl get secret api-credentials -n ats-dev -o yaml

# Decode a specific value
kubectl get secret api-credentials -n ats-dev -o jsonpath='{.data.polygon-api-key}' | base64 -d
```

### Update Secrets
```bash
# Method 1: Delete and recreate
kubectl delete secret api-credentials -n ats-dev
kubectl create secret generic api-credentials \
  --from-literal=polygon-api-key="new-key-value" \
  -n ats-dev

# Method 2: Patch existing secret
kubectl patch secret api-credentials -n ats-dev \
  -p='{"data":{"polygon-api-key":"'"$(echo -n 'new-key-value' | base64)"'"}}'
```

### Common Issues
- **Secret not found**: Ensure the secret exists in the correct namespace
- **Permission denied**: Check RBAC permissions for the service account
- **Base64 encoding**: Remember to base64 encode values in YAML files
- **Namespace mismatch**: Secrets must be in the same namespace as the pods using them