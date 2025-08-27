# 🆘 Troubleshooting Guide

**Common Issues, Solutions, and Emergency Procedures**

Comprehensive troubleshooting guide covering all common issues, their solutions, and emergency response procedures for the ATS platform.

---

## 🎯 Quick Issue Resolution

### **Emergency Contact Information**
- **Platform Team Lead**: @team-lead (Slack)
- **DevOps Engineer**: @devops-team (Slack)  
- **Database Administrator**: @dba-team (Slack)
- **Emergency Escalation**: #incident-response (Slack)

### **Health Check Commands**
```bash
# Quick system health check
kubectl cluster-info
kubectl get nodes
kubectl get pods -n ats-dev
python scripts/run_dev.py query --query "SELECT 1"

# Service status
kubectl get all -n ats-dev
curl http://external-ip:port/health
```

---

## 🔥 Critical Issues (P0/P1)

### **🚨 Production System Down**

**Symptoms:**
- API endpoints returning 503/504 errors
- No response from services
- Multiple monitoring alerts firing

**Immediate Actions:**
```bash
# 1. Check overall cluster health
kubectl get nodes
kubectl get pods -n ats-prod --sort-by='.status.containerStatuses[0].restartCount'

# 2. Check critical services
kubectl get pods -n ats-prod -l app=analytics-api
kubectl get pods -n ats-prod -l app=postgres
kubectl get pods -n ats-prod -l app=ml-inference-service

# 3. Check recent deployments
kubectl rollout history deployment/analytics-api -n ats-prod

# 4. Emergency rollback if needed
kubectl rollout undo deployment/analytics-api -n ats-prod
kubectl rollout status deployment/analytics-api -n ats-prod
```

**Root Cause Analysis:**
```bash
# Check logs for errors
kubectl logs -n ats-prod deployment/analytics-api --tail=100
kubectl logs -n ats-prod deployment/postgres --tail=50

# Check resource constraints
kubectl top pods -n ats-prod
kubectl describe nodes

# Check network issues
kubectl get services -n ats-prod
kubectl get ingress -n ats-prod
```

### **🚨 Database Connection Lost**

**Symptoms:**
- Applications can't connect to database
- Database timeout errors
- Connection pool exhausted

**Immediate Actions:**
```bash
# 1. Check postgres pod status
kubectl get pods -n ats-dev -l app=postgres
kubectl describe pod -n ats-dev -l app=postgres

# 2. Test database connectivity
kubectl exec -n ats-dev deployment/postgres -- pg_isready -U postgres

# 3. Check connection from app
kubectl exec -n ats-dev deployment/analytics-api -- nc -zv postgres 5432

# 4. Restart postgres if needed
kubectl rollout restart deployment/postgres -n ats-dev
```

**Advanced Debugging:**
```bash
# Check database logs
kubectl logs -n ats-dev deployment/postgres --tail=100

# Check connection limits
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -c "
SELECT setting FROM pg_settings WHERE name = 'max_connections';
SELECT count(*) FROM pg_stat_activity;
"

# Check for long-running queries
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -c "
SELECT pid, now() - pg_stat_activity.query_start AS duration, query 
FROM pg_stat_activity 
WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes';
"
```

### **🚨 Database Schema Compatibility Failures** ⭐ **CRITICAL LESSONS (2025-08-27)**

**Symptoms:**
- Data insertion failures during backfills
- `column "column_name" does not exist` errors  
- Price data not appearing despite successful API calls
- Container logs showing SQL errors

**Root Cause:** Scripts expect different column names than actual database schema.

**Critical Examples Found:**
- Scripts expect `adj_close` → Database has `adjclose` (no underscore)
- Scripts expect `created_at` → Database has `creation_timestamp`
- Scripts expect `dev_training_datasets` → Database has `dev_training_dataset` (singular)

**Immediate Actions:**
```bash
# 1. Validate actual table schema before any database operations
docker exec ats-dev-postgres psql -U postgres -d dev_db -c "\d table_name"

# 2. Check specific column names for price data
docker exec ats-dev-postgres psql -U postgres -d dev_db -c "\d dev_daily_prices_tiingo"
docker exec ats-dev-postgres psql -U postgres -d dev_db -c "\d dev_daily_prices_polygon"

# 3. Compare script expectations vs reality
grep -n "INSERT INTO.*adj_close" scripts/*.py  # Should use 'adjclose' not 'adj_close'
```

**Schema Validation Commands:**
```bash
# Run schema validation before deployment
python scripts/validate_schema.py --check-all

# Run schema compatibility regression tests
python3 scripts/run_regression_tests.py --category schema --integration

# Check all price data table schemas
for table in dev_daily_prices_polygon dev_daily_prices_tiingo dev_daily_prices_eodhd; do
    echo "=== $table ==="
    docker exec ats-dev-postgres psql -U postgres -d dev_db -c "\d $table"
done
```

**Prevention (MANDATORY):**
```bash
# ALWAYS validate schema before writing database code
docker exec ats-dev-postgres psql -U postgres -d dev_db -c "\d target_table"

# Example: Correct Tiingo price insertion
INSERT INTO dev_daily_prices_tiingo 
(date, symbol, open, high, low, close, adjclose, volume, status_id, instrument_id)
--                                   ^^^^^^^^^ Use 'adjclose' not 'adj_close'
```

**Emergency Fixes:**
```bash
# If containers failing with schema errors:
# 1. Stop failing containers
docker stop container_id

# 2. Fix INSERT statements in scripts (use correct column names)
# 3. Restart with corrected scripts

# Check backfill progress
docker exec ats-dev-postgres psql -U postgres -d dev_db -c "
SELECT 
    'Polygon' as vendor,
    COUNT(DISTINCT instrument_id) as instruments_with_data,
    COUNT(*) as total_records
FROM dev_daily_prices_polygon
UNION ALL
SELECT 
    'Tiingo' as vendor,
    COUNT(DISTINCT instrument_id) as instruments_with_data,
    COUNT(*) as total_records
FROM dev_daily_prices_tiingo"
```

### **🚨 ML Model Inference Failures**

**Symptoms:**
- Prediction endpoints returning 500 errors
- Model loading failures
- Inference timeouts

**Immediate Actions:**
```bash
# 1. Check inference service health
kubectl get pods -n ats-dev -l app=ml-inference-service
curl http://ml-inference-service/health

# 2. Check model registry connectivity
kubectl exec -n ats-dev deployment/ml-inference-service -- curl -f http://model-registry:5000/health

# 3. Verify model files
kubectl exec -n ats-dev deployment/ml-inference-service -- ls -la /models/

# 4. Scale service if needed
kubectl scale deployment ml-inference-service --replicas=5 -n ats-dev
```

**Model Loading Debug:**
```bash
# Test model loading manually
kubectl exec -n ats-dev deployment/ml-inference-service -- python -c "
import joblib
import os
models_dir = '/models/'
for model_file in os.listdir(models_dir):
    if model_file.endswith('.pkl'):
        try:
            model = joblib.load(os.path.join(models_dir, model_file))
            print(f'✅ {model_file}: {type(model)}')
        except Exception as e:
            print(f'❌ {model_file}: {e}')
"
```

---

## ⚠️ High Priority Issues (P2)

### **🔶 High API Latency**

**Symptoms:**
- API response times > 500ms
- Client timeouts
- Performance degradation alerts

**Diagnosis & Solutions:**
```bash
# 1. Check current latency
kubectl exec -n ats-dev deployment/analytics-api -- curl -w "@curl-format.txt" -s http://localhost:8000/api/v1/portfolio/recommendations

# 2. Check database performance
python scripts/run_dev.py query --query "
SELECT query, mean_exec_time, calls 
FROM pg_stat_statements 
ORDER BY mean_exec_time DESC 
LIMIT 10;
"

# 3. Check for resource constraints
kubectl top pods -n ats-dev --sort-by=cpu
kubectl top pods -n ats-dev --sort-by=memory

# 4. Scale services if needed
kubectl scale deployment analytics-api --replicas=5 -n ats-dev

# 5. Check cache hit rates
kubectl exec -n ats-dev deployment/redis -- redis-cli info stats | grep cache
```

**Performance Optimization:**
```bash
# Database query optimization
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -d dev_db -c "
-- Analyze query performance
EXPLAIN ANALYZE SELECT * FROM dev_daily_prices 
WHERE symbol = 'AAPL' AND date >= CURRENT_DATE - 30;

-- Update statistics
ANALYZE;

-- Rebuild indexes if needed
REINDEX INDEX CONCURRENTLY idx_daily_prices_symbol_date;
"
```

### **🔶 Data Quality Issues**

**Symptoms:**
- Missing data for recent dates
- Inconsistent prices across vendors
- Data validation failures

**Diagnosis:**
```bash
# 1. Check data completeness
python scripts/run_dev.py query --query "
SELECT date, COUNT(*) as symbol_count
FROM dev_daily_prices 
WHERE date >= CURRENT_DATE - 7
GROUP BY date 
ORDER BY date DESC;
"

# 2. Check vendor data quality
python scripts/run_dev.py query --query "
SELECT vendor, 
       COUNT(*) as total_records,
       COUNT(CASE WHEN close > 0 THEN 1 END) as valid_prices,
       AVG(CASE WHEN close > 0 THEN 1.0 ELSE 0.0 END) as quality_ratio
FROM dev_daily_prices 
WHERE date = CURRENT_DATE - 1
GROUP BY vendor;
"

# 3. Check for anomalies
python scripts/run_dev.py query --query "
SELECT symbol, date, vendor, close
FROM dev_daily_prices 
WHERE date >= CURRENT_DATE - 3
  AND (close <= 0 OR close > 10000)
ORDER BY date DESC, symbol;
"
```

**Data Recovery:**
```bash
# 1. Trigger data collection job
python scripts/run_dev.py deploy --file k8s/emergency-data-collection-job.yaml

# 2. Run data quality validation
python scripts/run_dev.py deploy --file k8s/data-quality-validation-job.yaml

# 3. Cross-vendor reconciliation
python scripts/run_dev.py deploy --file k8s/cross-vendor-reconciliation-job.yaml
```

---

## 🟡 Medium Priority Issues (P3)

### **🔸 Pod Restart Issues**

**Symptoms:**
- Frequent pod restarts
- OOMKilled events
- CrashLoopBackOff status

**Diagnosis:**
```bash
# 1. Check pod events
kubectl describe pod <pod-name> -n ats-dev

# 2. Check resource usage history
kubectl top pod <pod-name> -n ats-dev --containers

# 3. Review application logs
kubectl logs <pod-name> -n ats-dev --previous --tail=100

# 4. Check liveness/readiness probes
kubectl get pod <pod-name> -n ats-dev -o yaml | grep -A 10 livenessProbe
```

**Solutions:**
```bash
# 1. Increase resource limits
kubectl patch deployment analytics-api -n ats-dev -p '{"spec":{"template":{"spec":{"containers":[{"name":"analytics-api","resources":{"limits":{"memory":"4Gi","cpu":"2000m"}}}]}}}}'

# 2. Adjust probe timing
kubectl patch deployment analytics-api -n ats-dev -p '{"spec":{"template":{"spec":{"containers":[{"name":"analytics-api","livenessProbe":{"initialDelaySeconds":60,"periodSeconds":30}}]}}}}'

# 3. Check for memory leaks
kubectl exec -n ats-dev deployment/analytics-api -- python -c "
import psutil
import gc
print(f'Memory usage: {psutil.virtual_memory().used / 1024**3:.2f} GB')
print(f'Garbage collection: {gc.collect()} objects collected')
"
```

### **🔸 Slow Data Ingestion**

**Symptoms:**
- Data collection jobs taking too long
- Missing recent market data
- API rate limit errors

**Diagnosis:**
```bash
# 1. Check running jobs
kubectl get jobs -n ats-dev --sort-by='.status.startTime'

# 2. Check job logs for rate limiting
kubectl logs job/<job-name> -n ats-dev | grep -i "rate\|limit\|throttle"

# 3. Check vendor API status
curl -s https://api.polygon.io/v1/meta/symbols | jq '.status'
curl -s https://api.tiingo.com/api/test | jq '.'
```

**Optimization:**
```bash
# 1. Adjust job parallelism
kubectl patch job data-collection-job -n ats-dev -p '{"spec":{"parallelism":3}}'

# 2. Implement backoff strategy
kubectl set env job/data-collection-job BACKOFF_FACTOR=2 -n ats-dev

# 3. Check API key quotas
kubectl exec -n ats-dev deployment/data-collector -- python -c "
import os
print(f'Polygon API Key: {os.getenv(\"POLYGON_API_KEY\")[:8]}...')
# Make test API call to check quota
"
```

---

## 🟢 Low Priority Issues (P4)

### **🔸 Monitoring Alerts**

**Symptoms:**
- High memory usage warnings
- Disk space warnings
- Non-critical service alerts

**Standard Response:**
```bash
# 1. Acknowledge alert in monitoring system
# 2. Schedule maintenance window for resolution
# 3. Document issue for future prevention

# Memory cleanup
kubectl exec -n ats-dev deployment/analytics-api -- python -c "
import gc
collected = gc.collect()
print(f'Garbage collected: {collected} objects')
"

# Disk cleanup
kubectl exec -n ats-dev deployment/postgres -- du -sh /var/lib/postgresql/data/*
kubectl exec -n ats-dev deployment/postgres -- find /var/log -name '*.log' -mtime +7 -delete
```

---

## 🔧 Debugging Tools & Commands

### **Log Analysis**
```bash
# Search across all pods
kubectl logs -l app=analytics-api -n ats-dev --tail=100 | grep -i error

# Follow logs in real-time
kubectl logs -f deployment/analytics-api -n ats-dev

# Export logs for analysis
kubectl logs deployment/analytics-api -n ats-dev --since=1h > debug.log

# Search for specific patterns
kubectl logs deployment/analytics-api -n ats-dev | grep -E "(ERROR|EXCEPTION|FAILED)"
```

### **Network Debugging**
```bash
# Test service connectivity
kubectl exec -n ats-dev deployment/analytics-api -- nc -zv postgres 5432
kubectl exec -n ats-dev deployment/analytics-api -- nc -zv redis 6379

# Check DNS resolution
kubectl exec -n ats-dev deployment/analytics-api -- nslookup postgres
kubectl exec -n ats-dev deployment/analytics-api -- dig postgres.ats-dev.svc.cluster.local

# Test external connectivity
kubectl exec -n ats-dev deployment/analytics-api -- curl -I https://api.polygon.io
```

### **Performance Profiling**
```bash
# CPU profiling
kubectl exec -n ats-dev deployment/analytics-api -- python -c "
import cProfile
import pstats
# Profile critical functions
"

# Memory profiling
kubectl exec -n ats-dev deployment/analytics-api -- python -c "
import tracemalloc
tracemalloc.start()
# Run memory-intensive operations
current, peak = tracemalloc.get_traced_memory()
print(f'Current: {current / 1024 / 1024:.2f} MB')
print(f'Peak: {peak / 1024 / 1024:.2f} MB')
"
```

---

## 🚨 Emergency Procedures

### **Disaster Recovery Checklist**
```bash
#!/bin/bash
# Emergency disaster recovery procedure

echo "🚨 DISASTER RECOVERY INITIATED"

# 1. Assess damage
kubectl get nodes
kubectl get namespaces
kubectl get pods --all-namespaces | grep -v Running

# 2. Restore from backup
echo "🔄 Restoring database from latest backup..."
LATEST_BACKUP=$(ls -t /backups/*.dump | head -1)
./scripts/recovery/restore-database.sh "$LATEST_BACKUP" dev_db

# 3. Restart critical services
kubectl rollout restart deployment/postgres -n ats-dev
kubectl rollout restart deployment/analytics-api -n ats-dev
kubectl rollout restart deployment/ml-inference-service -n ats-dev

# 4. Verify recovery
kubectl get pods -n ats-dev
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices WHERE date = CURRENT_DATE - 1"

# 5. Notify stakeholders
echo "📧 Disaster recovery completed. System restored."
```

### **Emergency Contacts & Escalation**
1. **Level 1**: Development Team (5 minutes response)
2. **Level 2**: Senior Engineers (10 minutes response)  
3. **Level 3**: Platform Team Lead (15 minutes response)
4. **Level 4**: Engineering Manager (30 minutes response)
5. **Level 5**: CTO/Executive Team (60 minutes response)

---

## 📊 Issue Resolution Metrics

### **Resolution Time Targets**
- **P0 (Critical)**: 15 minutes
- **P1 (High)**: 2 hours
- **P2 (Medium)**: 24 hours  
- **P3 (Low)**: 1 week

### **Common Issue Frequency**
Based on historical data:
1. **Database connectivity** (25% of issues)
2. **Pod resource constraints** (20% of issues)
3. **API performance** (15% of issues)
4. **Data quality problems** (15% of issues)
5. **ML model failures** (10% of issues)
6. **Network connectivity** (10% of issues)
7. **Other** (5% of issues)

---

## 🎯 Prevention Strategies

### **Proactive Monitoring**
- Set up comprehensive alerting for all critical metrics
- Implement automated health checks
- Regular capacity planning reviews
- Performance baseline monitoring

### **Regular Maintenance**
- Weekly database maintenance windows
- Monthly security updates
- Quarterly disaster recovery drills
- Annual infrastructure reviews

---

**🎯 This troubleshooting guide provides comprehensive solutions for all common ATS platform issues with clear escalation procedures and emergency response protocols.**