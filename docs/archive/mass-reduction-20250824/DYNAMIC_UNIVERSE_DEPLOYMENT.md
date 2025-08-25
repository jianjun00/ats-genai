# Dynamic Modeling Universe - Deployment Guide

This guide covers deploying and managing the dynamic modeling universe system that automatically maintains a trading universe based on market cap and volume criteria.

## Overview

The Dynamic Modeling Universe system:
- **Entry Criteria**: Market cap >$400M AND trading volume >$100M (52-day averages)
- **Grace Period**: 1 week after failing criteria before removal
- **Re-entry Restriction**: 1 year restriction after removal
- **Daily Monitoring**: Automated daily updates via Kubernetes CronJob

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Daily CronJob │───▶│ Universe Engine │───▶│ Database Tables │
│   (6 AM UTC)    │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                               │
                               ▼
                    ┌─────────────────┐
                    │ Reports & Logs  │
                    │                 │
                    └─────────────────┘
```

### Database Schema

The system creates and maintains these tables:

```sql
-- Universe definitions
{env}_universe
├── universe_id (PK)
├── name (unique)
├── description
├── created_at
└── updated_at

-- Current universe memberships  
{env}_universe_membership
├── universe_id (FK)
├── instrument_id (FK)
└── added_at

-- Full tracking history
{env}_universe_tracking
├── id (PK)
├── universe_name
├── instrument_id
├── symbol
├── entry_date
├── last_qualifying_date
├── warning_date (nullable)
├── removal_date (nullable)
├── removal_reason (nullable)
├── avg_market_cap
├── avg_dollar_volume
└── last_update
```

## Deployment Steps

### 1. Prepare Application Code

```bash
# Create ConfigMap with application code
kubectl create configmap dynamic-universe-code \
  --from-file=src/universe/dynamic_modeling_universe.py \
  --from-file=src/config/environment.py \
  --from-file=src/config/__init__.py \
  -n ats-dev --dry-run=client -o yaml | kubectl apply -f -
```

### 2. Deploy Database Secrets

```bash
# Ensure database secrets exist
kubectl get secret postgres-secret -n ats-dev

# If not exists, create it:
kubectl create secret generic postgres-secret \
  --from-literal=username=your_db_user \
  --from-literal=password=your_db_password \
  -n ats-dev
```

### 3. Deploy CronJob

```bash
# Deploy the CronJob and supporting resources
kubectl apply -f k8s/dynamic-modeling-universe-job.yaml

# Verify deployment
kubectl get cronjobs -n ats-dev
kubectl describe cronjob dynamic-modeling-universe-daily -n ats-dev
```

### 4. Initial Manual Run

```bash
# Run initial universe creation manually
kubectl create job --from=cronjob/dynamic-modeling-universe-daily \
  initial-universe-setup-$(date +%Y%m%d) -n ats-dev

# Monitor the job
kubectl logs -f job/initial-universe-setup-$(date +%Y%m%d) -n ats-dev

# Check job status
kubectl get jobs -n ats-dev -l app=dynamic-modeling-universe
```

## Configuration

### Environment Variables

The system uses these environment variables:

```yaml
env:
- name: ENVIRONMENT
  value: "dev"  # or "intg", "prod"
- name: DB_HOST
  value: "postgres"
- name: DB_PORT
  value: "5432"
- name: DB_USER
  valueFrom:
    secretKeyRef:
      name: postgres-secret
      key: username
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: postgres-secret
      key: password
- name: DB_NAME
  value: "dev_db"  # or appropriate database name
```

### System Parameters

Key parameters in the system:

```python
# Configurable in DynamicModelingUniverse class
min_market_cap_millions = 400      # Entry requirement
min_dollar_volume_millions = 100   # Entry requirement
lookback_days = 52                 # ~2.5 months of data
min_trading_days = 40              # Minimum days in lookback
grace_period_days = 7              # 1 week grace period
reentry_restriction_days = 365     # 1 year restriction
```

## Daily Operations

### Monitoring

```bash
# Check CronJob status
kubectl get cronjobs -n ats-dev
kubectl describe cronjob dynamic-modeling-universe-daily -n ats-dev

# View recent job executions
kubectl get jobs -n ats-dev -l app=dynamic-modeling-universe --sort-by=.metadata.creationTimestamp

# Check logs from latest job
kubectl logs -n ats-dev -l app=dynamic-modeling-universe --tail=100

# Monitor for failed jobs
kubectl get jobs -n ats-dev -l app=dynamic-modeling-universe \
  --field-selector=status.failed!=0
```

### Manual Operations

```bash
# Run manual update for specific date
kubectl create job --from=cronjob/dynamic-modeling-universe-daily \
  manual-update-20240815 -n ats-dev

# Generate status report manually
kubectl run universe-report --rm -it --restart=Never \
  --image=python:3.12-slim -n ats-dev -- \
  bash -c "pip install asyncpg && python /app/src/universe/dynamic_modeling_universe.py --report"

# Debug universe state
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c "
    SELECT symbol, entry_date, warning_date, removal_date, avg_market_cap, avg_dollar_volume 
    FROM dev_universe_tracking 
    WHERE universe_name = 'dynamic_modeling_400m_100m' 
    ORDER BY entry_date DESC LIMIT 20;"
```

## Troubleshooting

### Common Issues

#### 1. CronJob Not Running

```bash
# Check CronJob configuration
kubectl describe cronjob dynamic-modeling-universe-daily -n ats-dev

# Check for suspend status
kubectl patch cronjob dynamic-modeling-universe-daily -n ats-dev -p '{"spec":{"suspend":false}}'

# Manually trigger job to test
kubectl create job --from=cronjob/dynamic-modeling-universe-daily test-run -n ats-dev
```

#### 2. Database Connection Issues

```bash
# Test database connectivity
kubectl run db-test --rm -it --restart=Never --image=postgres:15 -n ats-dev -- \
  psql -h postgres -U postgres -d dev_db -c "SELECT version();"

# Check database secrets
kubectl get secret postgres-secret -n ats-dev -o yaml

# Verify table creation
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c "\dt dev_universe*"
```

#### 3. No Qualifying Stocks

```bash
# Check if data exists
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c "
    SELECT COUNT(*) as price_records FROM dev_daily_prices_polygon;
    SELECT COUNT(*) as market_cap_records FROM dev_daily_market_cap;
    SELECT COUNT(*) as instrument_records FROM dev_instrument_xrefs WHERE vendor_id = 3;"

# Test qualification query manually
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c "
    WITH recent_data AS (
      SELECT x.vendor_symbol, COUNT(*) as days,
             AVG(p.close_price * p.volume) / 1000000 as avg_volume_mil
      FROM dev_daily_prices_polygon p
      JOIN dev_instrument_xrefs x ON p.instrument_id = x.instrument_id
      WHERE p.date >= CURRENT_DATE - INTERVAL '60 days'
        AND x.vendor_id = 3
      GROUP BY x.vendor_symbol
    )
    SELECT * FROM recent_data 
    WHERE avg_volume_mil > 100 
    ORDER BY avg_volume_mil DESC LIMIT 10;"
```

#### 4. Performance Issues

```bash
# Check job resource usage
kubectl describe job [job-name] -n ats-dev

# Monitor pod resources during execution
kubectl top pods -n ats-dev -l app=dynamic-modeling-universe

# Check for database query performance
kubectl logs -n ats-dev -l app=dynamic-modeling-universe | grep -i "slow\|timeout\|error"
```

### Log Analysis

```bash
# View comprehensive logs
kubectl logs -n ats-dev -l app=dynamic-modeling-universe --previous --tail=500

# Extract key metrics from logs
kubectl logs -n ats-dev -l app=dynamic-modeling-universe | grep -E "(ADDED|REMOVED|WARNED|ERROR)"

# Monitor for specific patterns
kubectl logs -f -n ats-dev -l app=dynamic-modeling-universe | grep -E "(qualification|grace period|re-entry)"
```

## Maintenance

### Regular Tasks

#### Weekly Review
```bash
# Generate universe health report
kubectl create job --from=cronjob/dynamic-modeling-universe-daily \
  weekly-report-$(date +%Y%m%d) -n ats-dev

# Review universe composition
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c "
    SELECT 
      COUNT(*) as total_stocks,
      COUNT(CASE WHEN warning_date IS NOT NULL THEN 1 END) as warned_stocks,
      AVG(avg_market_cap) as avg_market_cap,
      AVG(avg_dollar_volume) as avg_volume
    FROM dev_universe_tracking 
    WHERE universe_name = 'dynamic_modeling_400m_100m' 
      AND removal_date IS NULL;"
```

#### Monthly Analysis
```bash
# Analyze universe churn
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c "
    SELECT 
      DATE_TRUNC('month', entry_date) as month,
      COUNT(*) as entries,
      COUNT(CASE WHEN removal_date IS NOT NULL THEN 1 END) as removals
    FROM dev_universe_tracking 
    WHERE universe_name = 'dynamic_modeling_400m_100m'
      AND entry_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY DATE_TRUNC('month', entry_date)
    ORDER BY month;"

# Review re-entry patterns
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c "
    SELECT symbol, removal_reason, COUNT(*) as removal_count
    FROM dev_universe_tracking 
    WHERE removal_date IS NOT NULL
    GROUP BY symbol, removal_reason
    HAVING COUNT(*) > 1
    ORDER BY removal_count DESC;"
```

### Configuration Updates

To modify universe criteria:

1. **Update ConfigMap**:
```bash
# Edit the Python file with new parameters
# Then update ConfigMap
kubectl create configmap dynamic-universe-code \
  --from-file=src/universe/dynamic_modeling_universe.py \
  --from-file=src/config/ \
  -n ats-dev --dry-run=client -o yaml | kubectl apply -f -
```

2. **Test Changes**:
```bash
# Run test job with new configuration
kubectl create job --from=cronjob/dynamic-modeling-universe-daily \
  config-test-$(date +%Y%m%d) -n ats-dev
```

3. **Monitor Impact**:
```bash
# Compare before/after universe composition
kubectl logs job/config-test-$(date +%Y%m%d) -n ats-dev
```

## Security Considerations

### Database Access
- Database credentials stored in Kubernetes secrets
- Least-privilege database user with only required permissions
- Network policies to restrict database access

### Resource Limits
```yaml
resources:
  requests:
    memory: "256Mi"
    cpu: "250m"
  limits:
    memory: "512Mi" 
    cpu: "500m"
```

### Audit Trail
- All universe changes logged in database
- Kubernetes job execution history maintained
- Daily summary reports generated and stored

## Monitoring and Alerting

### Key Metrics to Monitor

1. **Universe Size**: Track daily universe composition
2. **Update Success Rate**: Monitor CronJob execution success
3. **Data Freshness**: Ensure market data is current
4. **Performance**: Monitor job execution time
5. **Error Rate**: Track qualification query failures

### Sample Prometheus Metrics

```yaml
# Custom metrics (if implemented)
- universe_total_stocks
- universe_new_additions_daily
- universe_removals_daily
- universe_warnings_active
- universe_update_duration_seconds
- universe_qualification_query_duration_seconds
```

### Alerting Rules

```yaml
# Sample alerting conditions
- alert: UniverseUpdateFailed
  expr: kube_job_status_failed{job_name=~"dynamic-modeling-universe.*"} > 0
  
- alert: UniverseSizeAnomaly  
  expr: abs(universe_total_stocks - universe_total_stocks offset 7d) > 10

- alert: UniverseNoUpdates
  expr: time() - universe_last_update_timestamp > 86400  # 24 hours
```

## Performance Optimization

### Database Optimization

```sql
-- Recommended indexes
CREATE INDEX idx_daily_prices_polygon_date_instrument 
  ON dev_daily_prices_polygon(date, instrument_id);

CREATE INDEX idx_daily_prices_polygon_instrument_date 
  ON dev_daily_prices_polygon(instrument_id, date);

CREATE INDEX idx_universe_tracking_name_removal 
  ON dev_universe_tracking(universe_name, removal_date);

CREATE INDEX idx_universe_tracking_instrument_name 
  ON dev_universe_tracking(instrument_id, universe_name);
```

### Query Optimization

- Use date partitioning for large price tables
- Implement materialized views for frequently accessed metrics
- Consider pre-computing qualification metrics for performance

### Job Optimization

- Tune resource requests/limits based on actual usage
- Consider parallel processing for large universes
- Implement incremental updates for efficiency

This deployment guide provides comprehensive instructions for setting up, monitoring, and maintaining the dynamic modeling universe system in production.