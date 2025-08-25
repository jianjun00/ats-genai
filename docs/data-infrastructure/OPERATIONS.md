# ⚙️ Data Infrastructure Operations

**Data Ops, Pipeline Monitoring, and Troubleshooting Guide**

---

## 🚀 Data Pipeline Operations

### **Daily Operations Checklist**
```bash
#!/bin/bash
# daily_data_ops_checklist.sh

echo "🔍 Daily Data Infrastructure Health Check"

# 1. Check pipeline status
run_dev query "
SELECT vendor, COUNT(*) as records, MAX(date) as latest_date
FROM dev_daily_prices 
WHERE date >= CURRENT_DATE - INTERVAL '2 days'
GROUP BY vendor
ORDER BY vendor;"

# 2. Verify data freshness
./scripts/data_quality/check_data_freshness.sh

# 3. Check storage utilization
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c \
  "SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
   FROM pg_tables 
   WHERE schemaname = 'public' AND tablename LIKE 'dev_%'
   ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC LIMIT 10;"

# 4. Monitor active data jobs
kubectl get jobs -n ats-dev | grep -E "(backfill|data-collection)"

# 5. Check data quality scores
./scripts/data_quality/daily_quality_report.sh

echo "✅ Daily health check completed"
```

### **Pipeline Deployment Procedures**

#### **Real-Time Data Collection**
```bash
# Deploy real-time collectors
kubectl apply -f k8s/data-infrastructure/realtime-collectors/

# Verify deployment
kubectl get pods -n ats-dev -l component=data-collector
kubectl logs -f deployment/polygon-realtime-collector -n ats-dev

# Test data ingestion
run_dev query "
SELECT symbol, COUNT(*) as recent_updates
FROM dev_daily_prices 
WHERE updated_at >= NOW() - INTERVAL '10 minutes'
GROUP BY symbol
ORDER BY recent_updates DESC
LIMIT 10;"
```

#### **Historical Backfill Jobs**
```bash
# Deploy 30-year backfill system
python scripts/backfill/deploy_30year_minute_backfill.py --deploy all

# Monitor backfill progress
python scripts/backfill/deploy_30year_minute_backfill.py --monitor

# Check vendor-specific progress
run_dev query "
SELECT vendor, symbol, MAX(date) as latest_backfill
FROM dev_minute_prices 
GROUP BY vendor, symbol
HAVING MAX(date) < CURRENT_DATE - INTERVAL '30 days'
ORDER BY latest_backfill ASC
LIMIT 20;"
```

---

## 📊 Monitoring & Alerting

### **Key Data Metrics**

#### **Data Freshness Monitoring**
```yaml
data_freshness_alerts:
  daily_prices:
    sla: "< 4 hours after market close"
    critical: "> 8 hours delay"
    
  minute_prices:
    sla: "< 15 minutes during market hours"
    critical: "> 30 minutes delay"
    
  corporate_actions:
    sla: "< 24 hours after announcement"
    critical: "> 48 hours delay"
```

#### **Data Quality Thresholds**
```yaml
quality_thresholds:
  completeness:
    warning: "< 95% expected records"
    critical: "< 90% expected records"
    
  cross_vendor_variance:
    warning: "> 2% price variance"
    critical: "> 5% price variance"
    
  duplicate_rate:
    warning: "> 0.1% duplicates"
    critical: "> 1% duplicates"
```

### **Monitoring Dashboards**

#### **Data Pipeline Health Dashboard**
```bash
# Access Grafana dashboard
kubectl port-forward service/grafana 3000:3000 -n monitoring
# Navigate to: http://localhost:3000/d/data-infrastructure-overview
```

**Key Panels:**
- Ingestion rate by vendor (records/minute)
- Data freshness by asset class
- Quality scores trending over time
- Storage growth and compression ratios
- API rate limit consumption
- Pipeline execution status
- Error rates by data source

#### **Data Quality Dashboard**
**Key Panels:**
- Cross-vendor price variance heat map
- Missing data detection by symbol/date
- Statistical anomaly detection alerts
- Schema validation failure trends
- Data lineage and transformation success rates

---

## 🔧 Troubleshooting Guide

### **Common Data Issues & Solutions**

#### **Missing Daily Prices**
**Symptoms:**
- Gaps in price history for specific symbols
- Quality dashboard shows low completeness

**Diagnosis:**
```bash
# Identify missing data ranges
run_dev query "
WITH date_series AS (
  SELECT generate_series('2024-01-01'::date, CURRENT_DATE, '1 day'::interval) AS date
),
symbol_dates AS (
  SELECT DISTINCT symbol FROM dev_daily_prices WHERE date >= '2024-01-01'
)
SELECT s.symbol, d.date::date as missing_date
FROM symbol_dates s
CROSS JOIN date_series d
LEFT JOIN dev_daily_prices p ON s.symbol = p.symbol AND d.date::date = p.date::date
WHERE p.symbol IS NULL
  AND s.symbol IN ('AAPL', 'MSFT', 'GOOGL')
  AND EXTRACT(dow FROM d.date) NOT IN (0,6)  -- Exclude weekends
ORDER BY s.symbol, d.date
LIMIT 20;"

# Check vendor availability for missing dates
./scripts/data_quality/diagnose_missing_data.sh AAPL 2024-01-15
```

**Solutions:**
```bash
# Trigger targeted backfill for specific symbol/date range
run_dev job historical-backfill \
  --symbols AAPL \
  --start-date 2024-01-10 \
  --end-date 2024-01-20 \
  --vendors polygon,tiingo

# Monitor backfill progress
kubectl logs -f job/historical-backfill-$(date +%Y%m%d) -n ats-dev
```

#### **High Cross-Vendor Price Variance**
**Symptoms:**
- Quality alerts for price variance > 5%
- Reconciliation failures in data quality reports

**Diagnosis:**
```bash
# Find symbols with high variance
run_dev query "
WITH vendor_prices AS (
  SELECT symbol, date, vendor, close,
         AVG(close) OVER (PARTITION BY symbol, date) as avg_close
  FROM dev_daily_prices 
  WHERE date = '2024-01-15'
),
variance_calc AS (
  SELECT symbol, date,
         STDDEV(close) as price_stddev,
         AVG(close) as avg_price,
         COUNT(*) as vendor_count
  FROM dev_daily_prices
  WHERE date = '2024-01-15'
  GROUP BY symbol, date
  HAVING COUNT(*) > 1
)
SELECT symbol, avg_price, price_stddev,
       (price_stddev / avg_price * 100) as variance_pct,
       vendor_count
FROM variance_calc
WHERE (price_stddev / avg_price * 100) > 2.0
ORDER BY variance_pct DESC
LIMIT 20;"

# Investigate specific symbol variance
./scripts/data_quality/investigate_price_variance.sh AAPL 2024-01-15
```

**Solutions:**
```bash
# Review vendor data quality
run_dev query "
SELECT vendor, COUNT(*) as total_records,
       AVG(quality_score) as avg_quality,
       COUNT(*) FILTER (WHERE quality_score < 0.9) as low_quality_count
FROM dev_daily_prices 
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY vendor
ORDER BY avg_quality DESC;"

# Flag problematic vendor data for review
./scripts/data_quality/flag_vendor_issues.sh polygon 2024-01-15

# Recalculate reconciled prices
run_dev job price-reconciliation --date 2024-01-15
```

#### **Data Pipeline Stalled**
**Symptoms:**
- No new data ingestion for > 30 minutes
- Pipeline jobs stuck in pending state

**Diagnosis:**
```bash
# Check pipeline job status
kubectl get jobs -n ats-dev | grep -E "(PENDING|FAILED)"

# Check resource constraints
kubectl describe job data-collection-job -n ats-dev

# Check database connectivity
kubectl exec -it deployment/data-collector -n ats-dev -- \
  python -c "
import psycopg2
try:
    conn = psycopg2.connect('$DB_CONNECTION_STRING')
    print('Database connection: OK')
except Exception as e:
    print(f'Database connection failed: {e}')
"

# Check API rate limits
kubectl logs deployment/polygon-collector -n ats-dev | grep -i "rate limit"
```

**Solutions:**
```bash
# Restart stalled jobs
kubectl delete job data-collection-$(date +%Y%m%d) -n ats-dev
kubectl apply -f k8s/data-infrastructure/daily-collection-job.yaml

# Scale up resources if needed
kubectl patch deployment data-collector -n ats-dev -p \
  '{"spec":{"template":{"spec":{"containers":[{"name":"data-collector","resources":{"limits":{"memory":"4Gi","cpu":"2000m"}}}]}}}}'

# Clear rate limit backoffs
redis-cli DEL "rate_limit:polygon:*"
```

#### **Database Storage Issues**
**Symptoms:**
- Storage utilization > 85%
- Query performance degradation
- Insert operations timing out

**Diagnosis:**
```bash
# Check database storage usage
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c "
SELECT schemaname, tablename,
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
       pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
       pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) as index_size
FROM pg_tables 
WHERE schemaname = 'public' AND tablename LIKE 'dev_%'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 10;"

# Check compression status (TimescaleDB)
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c "
SELECT hypertable_schema, hypertable_name,
       compression_status,
       uncompressed_heap_size,
       compressed_heap_size
FROM timescaledb_information.compressed_hypertable_stats;"
```

**Solutions:**
```bash
# Enable compression for older data
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c "
SELECT add_compression_policy('dev_daily_prices', INTERVAL '30 days');
SELECT add_compression_policy('dev_minute_prices', INTERVAL '7 days');"

# Archive old data
./scripts/data_maintenance/archive_old_data.sh --older-than "2 years"

# Reindex tables if needed
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c "REINDEX TABLE dev_daily_prices;"
```

---

## 🔄 Data Maintenance Operations

### **Regular Maintenance Tasks**

#### **Weekly Maintenance Script**
```bash
#!/bin/bash
# weekly_data_maintenance.sh

echo "🔧 Weekly Data Maintenance Starting..."

# 1. Compression policy execution
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c "
CALL run_job(
  (SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression')
);"

# 2. Vacuum and analyze tables
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c "
VACUUM ANALYZE dev_daily_prices;
VACUUM ANALYZE dev_minute_prices;
UPDATE pg_stat_statements_reset();"

# 3. Data quality audit
./scripts/data_quality/weekly_audit.sh

# 4. Performance statistics collection
./scripts/monitoring/collect_performance_stats.sh

# 5. Backup verification
./scripts/backup/verify_weekly_backup.sh

echo "✅ Weekly maintenance completed"
```

#### **Data Retention Management**
```bash
# Configure retention policies
kubectl exec -it deployment/postgres -n ats-dev -- \
  psql -U postgres -d dev_db -c "
-- Keep minute data for 2 years
SELECT add_retention_policy('dev_minute_prices', INTERVAL '2 years');

-- Keep daily data for 10 years  
SELECT add_retention_policy('dev_daily_prices', INTERVAL '10 years');

-- Archive old data to cold storage before deletion
SELECT add_retention_policy('dev_corporate_actions', INTERVAL '15 years');"
```

### **Data Migration Operations**

#### **Schema Migration Procedures**
```bash
# Test migration in development environment
./scripts/migration/test_schema_migration.sh --dry-run

# Apply schema changes with rollback capability
./scripts/migration/apply_schema_migration.sh \
  --version v2.1.0 \
  --backup-first \
  --rollback-plan migrations/rollback_v2.1.0.sql

# Verify migration success
./scripts/migration/verify_migration.sh --version v2.1.0
```

#### **Data Environment Sync**
```bash
# Sync production data to integration environment
python scripts/data_sync/prod_to_intg_sync.py \
  --date-range "2024-01-01:2024-01-31" \
  --symbols "S&P500" \
  --anonymize-pii

# Verify sync integrity
./scripts/data_sync/verify_sync_integrity.sh dev intg
```

---

## 📊 Performance Optimization

### **Query Performance Tuning**
```sql
-- Identify slow queries
SELECT query, mean_time, calls, total_time
FROM pg_stat_statements 
WHERE query LIKE '%dev_daily_prices%'
ORDER BY mean_time DESC 
LIMIT 10;

-- Optimize common query patterns
CREATE INDEX CONCURRENTLY idx_daily_prices_symbol_date_vendor 
ON dev_daily_prices (symbol, date DESC, vendor);

-- Create materialized views for common aggregations
CREATE MATERIALIZED VIEW mv_daily_portfolio_summary AS
SELECT 
    date,
    COUNT(DISTINCT symbol) as symbol_count,
    SUM(volume * close) as total_dollar_volume,
    AVG(close) as avg_price
FROM dev_daily_prices
WHERE date >= CURRENT_DATE - INTERVAL '1 year'
GROUP BY date;
```

### **Ingestion Performance Optimization**
```python
# Optimize batch sizes based on memory constraints
OPTIMAL_BATCH_SIZES = {
    'daily_prices': 5000,      # ~2MB per batch
    'minute_prices': 10000,    # ~5MB per batch
    'corporate_actions': 1000,  # ~500KB per batch
}

# Connection pool tuning
DB_POOL_CONFIG = {
    'min_size': 5,
    'max_size': 20,
    'max_idle_time': 300,
    'retry_attempts': 3,
    'retry_delay': 1.0
}
```

---

## 💾 Backup & Recovery Operations

### **Daily Backup Procedures**
```bash
# Automated daily backup
kubectl create job --from=cronjob/postgres-backup \
  postgres-backup-$(date +%Y%m%d) -n ats-dev

# Verify backup completion
kubectl logs job/postgres-backup-$(date +%Y%m%d) -n ats-dev

# Test backup integrity
./scripts/backup/test_backup_integrity.sh /backups/postgres_$(date +%Y%m%d).sql
```

### **Disaster Recovery Procedures**
```bash
# Emergency recovery from backup
./scripts/recovery/emergency_recovery.sh \
  --backup-file /backups/postgres_20240115.sql \
  --target-database dev_db_recovery \
  --verify-integrity

# Point-in-time recovery
./scripts/recovery/point_in_time_recovery.sh \
  --target-time "2024-01-15 14:30:00" \
  --backup-base /backups/postgres_20240115.sql
```

---

## 🎯 Operational Runbooks

### **Data Emergency Response**
```bash
#!/bin/bash
# data_emergency_response.sh

ISSUE_TYPE=$1  # missing-data, quality-issue, pipeline-failure

case $ISSUE_TYPE in
  "missing-data")
    echo "🚨 Missing Data Emergency Response"
    ./scripts/emergency/identify_missing_data.sh
    ./scripts/emergency/trigger_emergency_backfill.sh
    ./scripts/emergency/notify_stakeholders.sh "missing-data"
    ;;
    
  "quality-issue") 
    echo "🚨 Data Quality Emergency Response"
    ./scripts/emergency/isolate_bad_data.sh
    ./scripts/emergency/rollback_to_last_good_state.sh
    ./scripts/emergency/investigate_quality_degradation.sh
    ;;
    
  "pipeline-failure")
    echo "🚨 Pipeline Failure Emergency Response"
    ./scripts/emergency/restart_failed_pipelines.sh
    ./scripts/emergency/check_resource_constraints.sh
    ./scripts/emergency/escalate_if_unresolved.sh
    ;;
esac
```

### **Market Data Outage Response**
```bash
#!/bin/bash
# vendor_outage_response.sh

VENDOR=$1  # polygon, tiingo, fmp, alpha_vantage

echo "🔧 Vendor Outage Response for $VENDOR"

# 1. Disable affected data collectors
kubectl scale deployment ${VENDOR}-collector --replicas=0 -n ats-dev

# 2. Switch to backup vendors
./scripts/failover/activate_backup_vendors.sh --exclude $VENDOR

# 3. Monitor alternative sources
./scripts/monitoring/monitor_backup_vendors.sh

# 4. Alert operations team
./scripts/alerts/vendor_outage_alert.sh $VENDOR

# 5. Update status page
./scripts/status/update_vendor_status.sh $VENDOR "OUTAGE"

echo "✅ Vendor outage response completed for $VENDOR"
```

---

**🎯 This operations guide ensures reliable, monitored, and scalable data infrastructure with comprehensive troubleshooting and recovery procedures.**