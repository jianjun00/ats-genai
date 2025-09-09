# Daily Vendor Backfill Deployment Guide

## Overview

This guide explains how to deploy and manage the automated daily vendor data collection system for ATS-INTG.

## Components Created

### 1. Multi-Vendor Data Collector
- **File**: `scripts/multi_vendor_daily_collector.py`
- **Purpose**: Collects data from Tiingo, Polygon, and EODHD APIs
- **Features**: Rate limiting, error handling, schema-aware storage, Slack notifications

### 2. Kubernetes CronJobs
- **File**: `k8s/intg/daily-vendor-backfill-cronjobs.yaml`
- **Jobs**:
  - `daily-multi-vendor-backfill`: Daily at 7:00 AM EST (500 symbols)
  - `daily-tiingo-backfill`: Daily at 8:00 AM EST (200 symbols, fallback)
  - `weekly-comprehensive-vendor-backfill`: Weekly Sunday 3:00 AM EST (30 days)

### 3. Management Interface
- **File**: `scripts/manage_daily_backfill_jobs.py`
- **Commands**: deploy, status, logs, test-run

## Deployment Steps

### Prerequisites
1. Kubernetes cluster accessible via `kubectl`
2. Docker image `ats-genai:latest` available in cluster
3. API keys configured as Kubernetes secrets

### 1. Deploy to Kubernetes
```bash
# Deploy all CronJobs and supporting resources
python3 scripts/manage_daily_backfill_jobs.py deploy

# Check deployment status
python3 scripts/manage_daily_backfill_jobs.py status
```

### 2. Verify Deployment
```bash
# Check CronJobs are created
kubectl get cronjobs -n ats-intg

# Expected output:
# NAME                                 SCHEDULE      SUSPEND   ACTIVE   LAST SCHEDULE   AGE
# daily-multi-vendor-backfill          0 12 * * *    False     0        <none>          1m
# daily-tiingo-backfill               0 13 * * *    False     0        <none>          1m
# weekly-comprehensive-vendor-backfill 0 8 * * 0     False     0        <none>          1m
```

### 3. Test Data Collection
```bash
# Test Tiingo collection
python3 scripts/manage_daily_backfill_jobs.py test-run --vendor tiingo

# Test multi-vendor collection
docker exec ats-intg-analytics python3 scripts/multi_vendor_daily_collector.py --vendors tiingo,polygon --symbols 5 --days 3 --debug
```

## Manual Operations

### Run Immediate Collection
```bash
# Collect last 7 days for all vendors
docker exec ats-intg-analytics python3 scripts/multi_vendor_daily_collector.py --days 7

# Collect specific vendor
docker exec ats-intg-analytics python3 scripts/tiingo_data_collector_intg.py --days 5 --symbols 100
```

### Monitor Jobs
```bash
# View job status
python3 scripts/manage_daily_backfill_jobs.py status

# View job logs
python3 scripts/manage_daily_backfill_jobs.py logs --job daily-multi-vendor-backfill

# Check recent job execution
kubectl get jobs -n ats-intg --sort-by=.metadata.creationTimestamp
```

### Troubleshooting
```bash
# Check failed jobs
kubectl get jobs -n ats-intg --field-selector status.successful!=1

# Debug specific job
kubectl describe job <job-name> -n ats-intg

# Check pod logs
kubectl logs -n ats-intg -l job-name=<cronjob-name> --tail=100
```

## Schedule Overview

| Job | Time (EST) | Frequency | Purpose | Symbols |
|-----|------------|-----------|---------|---------|
| Multi-vendor daily | 7:00 AM | Daily | Current data from all vendors | 500 |
| Tiingo fallback | 8:00 AM | Daily | Ensure Tiingo data is complete | 200 |
| Comprehensive weekly | 3:00 AM Sun | Weekly | 30-day backfill validation | All |

## Configuration

### Environment Variables (in CronJob)
```yaml
# Database
- name: DB_HOST
  value: "ats-intg-postgres"
- name: DB_PASSWORD
  value: "intg_password"
- name: DB_NAME
  value: "intg_db"

# API Keys (from secrets)
- name: TIINGO_API_KEY
  valueFrom:
    secretKeyRef:
      name: vendor-api-keys
      key: tiingo-api-key
```

### API Rate Limits
- **Tiingo**: 1000 requests/hour (1 req/sec)
- **Polygon**: 5 requests/minute (12 sec delay)
- **EODHD**: 20 requests/minute (3 sec delay)

## Monitoring & Alerts

### Slack Notifications
Configure `SLACK_WEBHOOK_URL` secret for notifications:
- Daily collection summaries
- Error alerts when failures exceed thresholds
- Weekly comprehensive backfill reports

### Dashboard Verification
After deployment, verify data appears in Grafana:
- URL: `http://10.0.0.79:4002/d/f9afe708-9be9-4c39-b901-f5c43a0a479f/ats-vendor-monitoring-dashboard-fixed`
- Expected: All three vendors showing recent data
- Tiingo should show data up to current date

## Database Schema Compatibility

The collectors are configured for existing table schemas:

### Tiingo Table (`intg_daily_prices_tiingo`)
- Columns: date, symbol, open, high, low, close, volume, adjusted_close, instrument_id, created_at, updated_at
- No `id` column, uses composite key (instrument_id, date)

### Polygon Table (`intg_daily_prices_polygon`)
- Columns: date, symbol, open, high, low, close, volume, market_cap, instrument_id, created_at, updated_at
- No `adjclose` column, has `market_cap` instead

### EODHD Table (`intg_daily_prices_eodhd`)  
- Columns: id, date, symbol, open, high, low, close, adjusted_close, volume, instrument_id, created_at
- Has `id` column, no `updated_at` column

## Success Metrics

### Immediate (Day 1)
- [x] Tiingo data gap resolved (255 records collected)
- [x] Grafana dashboard shows current Tiingo data
- [x] Multi-vendor collector tested successfully

### Ongoing (Daily)
- [ ] Daily jobs execute successfully  
- [ ] All three vendors collect data without gaps
- [ ] Error rates remain below 5%
- [ ] Grafana dashboard shows consistent data flow

### Weekly Validation
- [ ] Weekly comprehensive job validates 30-day coverage
- [ ] Cross-vendor price discrepancies detected and reported
- [ ] Data quality metrics maintained above 95%

## Rollback Plan

If issues occur:
```bash
# Suspend problematic CronJob
kubectl patch cronjob <job-name> -n ats-intg -p '{"spec":{"suspend":true}}'

# Use manual collection as fallback
docker exec ats-intg-analytics python3 scripts/tiingo_data_collector_intg.py --days 7

# Resume when fixed
kubectl patch cronjob <job-name> -n ats-intg -p '{"spec":{"suspend":false}}'
```

## Files Changed/Added

```
scripts/multi_vendor_daily_collector.py          # Main collector
scripts/manage_daily_backfill_jobs.py           # Management interface  
scripts/tiingo_data_collector_intg.py           # Tiingo-specific (existing fix)
k8s/intg/daily-vendor-backfill-cronjobs.yaml    # Kubernetes CronJobs
DEPLOYMENT_GUIDE_DAILY_BACKFILL.md              # This documentation
```

The system is now ready for production deployment and will ensure continuous, automated data collection from all three vendors.