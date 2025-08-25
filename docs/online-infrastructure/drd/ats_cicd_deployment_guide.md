# ATS CI/CD Deployment Guide

This guide provides comprehensive instructions for deploying the ATS 3-service system with automated CI/CD using Argo CD and GitHub Actions.

## System Architecture

The ATS system consists of three microservices:

1. **Minute Service** (Port 8081): Real-time 1-minute price data collection
2. **EOD Service** (Port 8082): End-of-day price data collection with enhanced features
3. **Analytics Service** (Port 8080): Unified analytics platform with portfolio analytics

## Prerequisites

### Infrastructure Requirements
- Kubernetes cluster (v1.24+)
- Argo CD installed and configured
- GitHub repository with Actions enabled
- Docker registry (GitHub Container Registry recommended)
- PostgreSQL database (TimescaleDB preferred)
- Redis for caching

### API Keys Required
- Polygon.io API key
- Tiingo API key  
- FMP (Financial Modeling Prep) API key

### Secrets Configuration
Create the following Kubernetes secrets in the `ats-dev` namespace:

```bash
# Database credentials
kubectl create secret generic ats-db-secret \
  --from-literal=host=your-db-host \
  --from-literal=port=5432 \
  --from-literal=username=your-db-user \
  --from-literal=password=your-db-password \
  --from-literal=database=your-db-name \
  -n ats-dev

# API keys
kubectl create secret generic ats-api-keys \
  --from-literal=polygon-api-key=your-polygon-key \
  --from-literal=tiingo-api-key=your-tiingo-key \
  --from-literal=fmp-api-key=your-fmp-key \
  -n ats-dev
```

## Validation Strategy

The deployment uses a comprehensive multi-stage validation approach:

### 1. Pre-Deployment Validation (CI Pipeline)
- **Code Quality**: Black formatting, flake8 linting, mypy type checking
- **Security**: Bandit security scan, Safety dependency check
- **Testing**: Unit tests with coverage, integration tests
- **Container Security**: Image vulnerability scanning, SBOM generation

### 2. Canary Deployment Validation (CD Pipeline)
- **Stage 1 (10% traffic, 5 min)**: Basic health checks, performance validation
- **Stage 2 (25% traffic, 10 min)**: Integration tests, data quality validation  
- **Stage 3 (50% traffic, 15 min)**: Load testing, business logic validation
- **Stage 4 (100% traffic, 5 min)**: Full promotion with continued monitoring

### 3. Continuous Validation (Runtime)
- **Health Monitoring**: HTTP 200 response rate > 95%
- **Performance**: P95 response time < 500ms
- **Resource Usage**: CPU < 80%, Memory < 90%
- **Data Quality**: Vendor data quality score > 85%
- **Dependencies**: Database connectivity, service mesh health

## Automated Rollback Triggers

The system automatically rolls back deployments when:

1. **Immediate Rollback**:
   - Health check failure rate > 5%
   - Memory usage > 90%
   - Database connectivity loss

2. **Gradual Rollback** (with 2-5 minute grace period):
   - P95 response time > 1 second
   - Data quality score < 80%

3. **Pause Deployment** (manual review required):
   - Database connectivity loss
   - Critical infrastructure failures

## Deployment Process

### 1. Code Changes
Push code changes to the `main` branch. The CI/CD pipeline automatically:

1. Runs comprehensive tests and security scans
2. Builds and pushes Docker images
3. Updates Kubernetes manifests
4. Creates a deployment PR for review

### 2. Deployment Approval
Review the auto-generated deployment PR which includes:
- Image tags and build information
- Pre-deployment validation results
- Detailed deployment plan
- Rollback strategy

### 3. Argo CD Sync
Once the PR is merged, Argo CD automatically:
- Syncs the new manifests
- Initiates canary deployment
- Monitors validation metrics
- Promotes or rolls back based on results

## Monitoring and Observability

### Real-Time Monitoring
- **Service Health**: `/health` endpoints on all services
- **Metrics**: Prometheus metrics exposed on `/metrics`
- **Logs**: Structured logging with correlation IDs
- **Tracing**: OpenTelemetry distributed tracing

### Dashboards
- **System Overview**: `http://analytics.ats-dev.your-domain.com/dashboard`
- **Grafana**: Custom dashboards for each service
- **Argo CD**: Deployment status and sync information

### Alerting
- **Slack**: Real-time deployment notifications (#ats-deployments)
- **Email**: Critical failure notifications
- **PagerDuty**: Production incident escalation

## Configuration Management

### Environment Variables
Each service uses environment-specific configuration:

```yaml
# Common environment variables
ENVIRONMENT: dev
PYTHONPATH: /app/src
DB_HOST: <from-secret>
DB_PORT: <from-secret>
DB_USER: <from-secret>  
DB_PASSWORD: <from-secret>
DB_NAME: <from-secret>

# API keys
POLYGON_API_KEY: <from-secret>
TIINGO_API_KEY: <from-secret>
FMP_API_KEY: <from-secret>

# Service-specific
REDIS_URL: redis://ats-redis-service:6379
MINUTE_SERVICE_URL: http://ats-minute-service:8081
EOD_SERVICE_URL: http://ats-eod-service:8082
```

### Gin Configuration
Services use Gin for dependency injection with environment-specific configs:

```python
# config/app_dev.gin
database.host = %DATABASE_HOST
database.port = %DATABASE_PORT
polygon.api_key = %POLYGON_API_KEY
tiingo.api_key = %TIINGO_API_KEY
data_collection.rate_limit = 0.5
analytics.cache_ttl = 300
```

## Troubleshooting

### Common Issues

#### 1. Deployment Stuck in Canary Phase
```bash
# Check Argo Rollout status
kubectl get rollouts -n ats-dev
kubectl describe rollout ats-analytics-service-rollout -n ats-dev

# Check analysis results
kubectl get analysisruns -n ats-dev
kubectl logs -l analysisrun=<analysis-run-name> -n ats-dev
```

#### 2. Database Connection Issues
```bash
# Test database connectivity
kubectl run -it --rm debug --image=postgres:15 --restart=Never -- \
  psql postgresql://user:pass@host:5432/dbname

# Check service endpoints
kubectl get endpoints -n ats-dev
```

#### 3. API Key Issues
```bash
# Verify secrets are created
kubectl get secrets -n ats-dev
kubectl describe secret ats-api-keys -n ats-dev

# Test API connectivity
kubectl run -it --rm debug --image=curlimages/curl --restart=Never -- \
  curl "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2024-01-01/2024-01-01?apikey=YOUR_KEY"
```

### Performance Tuning

#### Resource Allocation
```yaml
# Recommended resource allocation
minute-service:
  requests: { memory: "512Mi", cpu: "250m" }
  limits: { memory: "1Gi", cpu: "500m" }

eod-service:
  requests: { memory: "768Mi", cpu: "300m" }
  limits: { memory: "1.5Gi", cpu: "750m" }

analytics-service:
  requests: { memory: "1Gi", cpu: "500m" }
  limits: { memory: "2Gi", cpu: "1000m" }
```

#### Auto-scaling Configuration
```yaml
# HPA settings for analytics service
minReplicas: 3
maxReplicas: 10
targetCPUUtilization: 70%
targetMemoryUtilization: 80%
```

## Security Best Practices

### 1. Container Security
- Multi-stage Docker builds with minimal base images
- Non-root container execution
- Regular vulnerability scanning
- SBOM generation for supply chain security

### 2. Kubernetes Security
- Service accounts with least privilege
- Network policies for traffic restriction
- Pod security standards enforcement
- Secrets management with external secret operators

### 3. API Security
- API key rotation strategy
- Rate limiting implementation
- Request authentication and authorization
- Audit logging for all API calls

## Backup and Recovery

### Database Backup
```bash
# Automated daily backups
kubectl create cronjob ats-db-backup --schedule="0 2 * * *" \
  --image=postgres:15 -- \
  pg_dump $DATABASE_URL -f /backup/ats_$(date +%Y%m%d).sql
```

### Configuration Backup
```bash
# Export Kubernetes resources
kubectl get all,configmaps,secrets,ingress -n ats-dev -o yaml > ats-backup-$(date +%Y%m%d).yaml
```

### Disaster Recovery
1. **Database Recovery**: Point-in-time recovery from TimescaleDB backups
2. **Service Recovery**: Redeploy from Git using Argo CD
3. **Data Recovery**: Re-run data collection for missing periods

## Performance Benchmarks

### Expected Performance
- **Minute Service**: 1000+ symbols/minute with <100ms latency
- **EOD Service**: 5000+ symbols with 95% data quality
- **Analytics Service**: Sub-second P95 response time, 70%+ cache hit rate

### Load Testing Results
```
# Expected concurrent user support
Minute Service: 100 concurrent API calls
EOD Service: 50 concurrent collections  
Analytics Service: 500 concurrent dashboard users
```

## Support and Maintenance

### Regular Tasks
- **Weekly**: Review deployment metrics and error rates
- **Monthly**: Update dependencies and security patches
- **Quarterly**: Performance review and capacity planning

### Escalation Path
1. **Level 1**: Automated alerts and self-healing
2. **Level 2**: Development team (#ats-dev)
3. **Level 3**: Operations team (#ats-ops)
4. **Level 4**: Architecture team for system-wide issues

## Conclusion

This CI/CD pipeline provides:
- ✅ **Zero-downtime deployments** with canary strategy
- ✅ **Comprehensive validation** at every stage
- ✅ **Automated rollback** with multiple safety nets
- ✅ **Full observability** with metrics, logs, and tracing
- ✅ **Security-first approach** with continuous scanning
- ✅ **Production-ready monitoring** and alerting

The system validates new versions through a rigorous multi-stage process that ensures:
1. Code quality and security before build
2. Progressive traffic rollout with real-time validation
3. Automatic rollback on any quality degradation
4. Comprehensive monitoring and observability

This approach minimizes risk while enabling rapid, confident deployments of the ATS trading system.