# ATS GenAI Infrastructure Documentation

## Overview

The ATS GenAI system uses a multi-environment Kubernetes architecture with GitOps deployment via Argo CD. This document covers the complete infrastructure setup and operational procedures.

## Architecture

### Environment Strategy
- **Development (`ats-dev`)**: Local development and feature testing
- **Integration (`ats-intg`)**: Automated testing and validation
- **Production (`ats-prod`)**: Live trading system

### Technology Stack
- **Container Runtime**: Docker with multi-stage builds
- **Orchestration**: Kubernetes with Kustomize
- **GitOps**: Argo CD for automated deployments
- **CI/CD**: GitHub Actions
- **Database**: TimescaleDB (PostgreSQL extension)
- **Package Management**: uv with pyproject.toml

## Directory Structure

```
ats-genai/
├── Dockerfile                      # Multi-stage container build
├── pyproject.toml                  # Python dependencies
├── uv.lock                        # Locked dependencies
├── k8s/
│   ├── base/                      # Base Kubernetes manifests
│   │   ├── deployment.yaml        # API deployment
│   │   ├── service.yaml           # Service definition
│   │   ├── configmap.yaml         # Configuration
│   │   ├── secrets.yaml           # Sensitive data (template)
│   │   └── kustomization.yaml     # Base kustomization
│   ├── environments/
│   │   ├── dev/                   # Development overrides
│   │   ├── intg/                  # Integration overrides
│   │   └── prod/                  # Production overrides
│   └── argocd/
│       ├── ats-intg-app.yaml      # Integration Argo app
│       └── ats-prod-app.yaml      # Production Argo app
├── .github/workflows/
│   └── ci-cd.yaml                 # CI/CD pipeline
└── docs/
    ├── INFRASTRUCTURE.md          # This document
    ├── DEPLOYMENT.md              # Deployment procedures
    └── CLUSTER_SETUP.md           # Cluster setup guide
```

## Container Strategy

### Multi-Stage Dockerfile
- **Builder stage**: Install dependencies with uv
- **Runtime stage**: Minimal Python image with pre-built dependencies
- **Security**: Non-root user (ats:1000)
- **Health checks**: Built-in HTTP health endpoint

### Image Tagging Strategy
- `dev-latest`: Development builds
- `intg-latest`: Integration builds  
- `prod-latest`: Production releases
- `YYYY.WW.patch`: Weekly release tags (e.g., `2025.06.0`)

## Kubernetes Configuration

### Resource Allocation
| Environment | Replicas | CPU Request | Memory Request | CPU Limit | Memory Limit |
|-------------|----------|-------------|----------------|-----------|--------------|
| Dev         | 1        | 100m        | 256Mi          | 250m      | 512Mi        |
| Integration | 2        | 250m        | 512Mi          | 500m      | 1Gi          |
| Production  | 3        | 500m        | 1Gi            | 1000m     | 2Gi          |

### Environment Variables
- `PYTHONPATH=src`: Python module resolution
- `ENVIRONMENT`: Environment identifier (dev/intg/prod)
- `LOG_LEVEL`: Logging verbosity
- `DATABASE_URL`: TimescaleDB connection string (from secret)
- `TIINGO_API_KEY`: Market data API key (from secret)
- `POLYGON_API_KEY`: Market data API key (from secret)

## CI/CD Pipeline

### Trigger Events
- **Push to main**: Run tests and build images
- **Pull requests**: Run tests only
- **Weekly schedule**: Automated release cut (Mondays 9 AM UTC)

### Pipeline Stages
1. **Test**: Run pytest with TimescaleDB service
2. **Build**: Create and push Docker images
3. **Deploy Integration**: Auto-deploy to integration environment
4. **Deploy Production**: Manual approval required

### Weekly Release Process
1. GitHub Actions creates release branch from main
2. Builds and tags image with `YYYY.WW.0` format
3. Auto-deploys to integration for validation
4. Manual approval gate for production deployment
5. Updates Kubernetes manifests with new image tags

## Security Considerations

### Container Security
- Non-root user execution
- Read-only root filesystem where possible
- Resource limits to prevent resource exhaustion
- Health checks for reliable service discovery

### Secrets Management
- Kubernetes secrets for sensitive data
- Base64 encoded values (not encryption)
- Separate secrets per environment
- No secrets in container images or code

### Network Security
- ClusterIP services (internal only)
- Ingress controllers for external access
- Network policies for pod-to-pod communication
- TLS termination at ingress

## Monitoring and Observability

### Health Checks
- **Liveness probe**: `/health` endpoint every 10s
- **Readiness probe**: `/health` endpoint every 5s
- **Startup probe**: 30s initial delay

### Logging
- Structured JSON logging
- Centralized log aggregation
- Environment-specific log levels
- Audit trails for trading operations

## Disaster Recovery

### Backup Strategy
- Database backups via TimescaleDB continuous archiving
- Configuration stored in Git (GitOps)
- Secrets backed up securely outside cluster

### Recovery Procedures
- Database point-in-time recovery
- GitOps rollback via Argo CD
- Multi-AZ deployment for high availability
