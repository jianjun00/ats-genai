# Daily Prices Validation System

## Overview

Automated daily validation system for ATS platform that monitors data quality across all vendor daily price feeds. Detects missing prices, abnormal price movements, and data quality issues across EODHD, Tiingo, and Polygon vendors.

## Features

### **Data Quality Metrics**
- **Missing Prices**: Expected vs actual price records for trading days
- **Abnormal Prices**: Detection of data quality issues
  - Negative prices (close, high, low ≤ 0)
  - Zero volume records
  - Price spikes (high/low ratio > 3.0)
- **Vendor Coverage**: Individual analysis per vendor (EODHD, Tiingo, Polygon)
- **Rolling Window**: 90-day analysis window with configurable periods

### **Monitoring & Alerting**
- **Prometheus Metrics**: Real-time metrics export for monitoring
- **Grafana Dashboard**: Visual monitoring in ATS-INTG environment
- **Automated Scheduling**: Daily cron job at 6:45 AM
- **Alerting Thresholds**: Configurable alerts for data quality issues

## Architecture

### **System Components**
```
Daily Prices Tables (INTG DB)
    ↓
ValidationEngine (Python Script)
    ↓
Prometheus Metrics (Push Gateway)
    ↓
Grafana Dashboard (ATS-INTG)
```

### **Database Tables Analyzed**
- `intg_daily_price_eodhd` - EODHD daily price data
- `intg_daily_price_tiingo` - Tiingo daily price data
- `intg_daily_price_polygon` - Polygon daily price data
- `intg_daily_prices` - Unified daily prices (if applicable)

## Usage

### **Automated Daily Run**
```bash
# Runs automatically via cron at 6:45 AM daily
# 45 6 * * * cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/daily_prices_validation.py
```

### **Manual Execution**
```bash
# Standard 90-day validation
PYTHONPATH=src python3 scripts/daily_prices_validation.py

# Debug mode with detailed output
PYTHONPATH=src python3 scripts/daily_prices_validation.py --debug

# Custom analysis period
PYTHONPATH=src python3 scripts/daily_prices_validation.py --days 30 --debug

# Test run without metrics export
PYTHONPATH=src python3 scripts/daily_prices_validation.py --dry-run --debug
```

### **Command Line Options**
- `--days N` - Number of days to analyze (default: 90)
- `--debug` - Enable detailed debug logging
- `--dry-run` - Run validation without exporting metrics
- `--help` - Show usage information

## Prometheus Metrics

### **Metric Names & Labels**
All metrics include `vendor` label (eodhd, tiingo, polygon) and `environment="intg"`:

```prometheus
# Missing prices metrics
ats_daily_prices_missing_count{vendor="tiingo",environment="intg"}
ats_daily_prices_missing_percentage{vendor="tiingo",environment="intg"}
ats_daily_prices_expected_total{vendor="tiingo",environment="intg"}

# Abnormal prices metrics
ats_daily_prices_abnormal_count{vendor="polygon",environment="intg"}
ats_daily_prices_abnormal_percentage{vendor="polygon",environment="intg"}
ats_daily_prices_negative_count{vendor="eodhd",environment="intg"}
ats_daily_prices_zero_volume_count{vendor="eodhd",environment="intg"}
ats_daily_prices_price_spike_count{vendor="eodhd",environment="intg"}

# Validation timestamp
ats_daily_prices_validation_timestamp{environment="intg"}
```

### **Metrics Export**
- **Target**: Prometheus Push Gateway at `http://localhost:9091`
- **Job Name**: `ats-daily-prices-validation`
- **Instance**: `intg`
- **Format**: Prometheus text format with timestamps

## Grafana Dashboard

### **Dashboard URL**
```
http://localhost:4002/d/daily-prices-validation
```

### **Dashboard Panels**
1. **Missing Prices Count** - Current missing price counts by vendor
2. **Missing Prices Percentage** - Missing price percentages with thresholds
3. **Abnormal Prices Count** - Current abnormal price counts by vendor
4. **Abnormal Prices Percentage** - Abnormal price percentages with thresholds
5. **Missing Prices Trend** - 30-day trend line chart
6. **Abnormal Prices Breakdown** - Pie chart of abnormal price types
7. **Expected vs Actual** - Bar chart comparing expected vs actual records
8. **Data Quality Score** - Overall quality gauge per vendor

### **Alert Thresholds**
- **Missing Prices**: Green (0-5%), Yellow (5-10%), Red (>10%)
- **Abnormal Prices**: Green (0-2%), Yellow (2-5%), Red (>5%)
- **Data Quality Score**: Red (<85%), Yellow (85-95%), Green (>95%)

## Installation

### **Grafana Dashboard Setup**
```bash
# Import dashboard configuration
curl -X POST http://admin:admin@localhost:4002/api/dashboards/db \
  -H "Content-Type: application/json" \
  -d @config/grafana/daily-prices-validation-dashboard.json
```

### **Cron Job Installation**
```bash
# Install complete ATS cron configuration (includes validation job)
crontab scripts/cron/ats-complete-crontab

# Or add individual job
crontab -e
# Add: 45 6 * * * cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/daily_prices_validation.py
```

## Validation Logic

### **Missing Prices Detection**
```sql
-- Calculate expected trading days (excludes weekends)
WITH trading_days AS (
    SELECT date_val as trading_date
    FROM generate_series(start_date, end_date, '1 day') AS date_val
    WHERE EXTRACT(DOW FROM date_val) NOT IN (0, 6)  -- Exclude Sat/Sun
),
-- Get active symbols with recent data
active_symbols AS (
    SELECT DISTINCT symbol FROM vendor_table WHERE date >= start_date
)
-- Expected = trading_days * active_symbols
-- Missing = expected - actual
```

### **Abnormal Prices Detection**
```sql
-- Detect various types of abnormal prices
SELECT
    COUNT(*) FILTER (WHERE close <= 0 OR high <= 0 OR low <= 0) as negative_prices,
    COUNT(*) FILTER (WHERE volume = 0) as zero_volume_prices,
    COUNT(*) FILTER (WHERE high / low > 3.0) as price_spike_prices
FROM vendor_daily_prices_table
WHERE date BETWEEN start_date AND end_date
```

### **Data Quality Score Calculation**
```
Quality Score = 100% - Missing% - Abnormal%
```

## Monitoring & Troubleshooting

### **Log Files**
- **Main Log**: `/mnt/d/ats-logs/daily-prices-validation.log`
- **Error Log**: `/mnt/d/ats-logs/daily-prices-validation-error.log`
- **Cron Log**: Check with `journalctl _COMM=cron -f`

### **Common Issues & Solutions**

**High Missing Prices:**
1. Check vendor data sync jobs (DEV → INTG sync at 1:00 AM)
2. Verify vendor API status and rate limits
3. Check database connectivity and table schemas
4. Review vendor-specific backfill job logs

**High Abnormal Prices:**
1. Review data quality from vendor APIs
2. Check for stock splits, dividends not adjusted
3. Verify currency conversion issues for international stocks
4. Check for data feed interruptions or corrupted data

**Metrics Export Failures:**
1. Check Prometheus Push Gateway status: `curl http://localhost:9091/metrics`
2. Verify network connectivity to push gateway
3. Check authentication/authorization for metrics push
4. Review validation script logs for HTTP errors

**Missing Grafana Data:**
1. Verify Prometheus is scraping push gateway metrics
2. Check Grafana data source configuration
3. Confirm dashboard queries match metric names
4. Check Grafana logs for query errors

### **Manual Diagnostics**
```bash
# Check recent validation runs
tail -50 /mnt/d/ats-logs/daily-prices-validation.log

# Test database connectivity
python3 scripts/run_intg.py query --query "SELECT COUNT(*) FROM intg_daily_price_tiingo WHERE date >= CURRENT_DATE - INTERVAL '7 days'"

# Test Prometheus metrics push
curl -X POST http://localhost:9091/metrics/job/test-job/instance/test \
  -H 'Content-Type: text/plain' \
  -d 'test_metric{label="value"} 42'

# Check Grafana dashboard
curl -s http://admin:admin@localhost:4002/api/search | jq '.[] | select(.title | contains("Daily Prices"))'
```

## Sample Output

### **Debug Mode Output**
```
🚀 Starting ATS Daily Prices Validation Job
📅 Analysis period: 7 days
🔧 Initialized DailyPricesValidator for localhost:4432/intg_db
🚀 Starting daily prices validation for past 7 days

📊 Validating tiingo prices for past 7 days...
✅ tiingo: 51 missing, 14 abnormal out of 306 expected prices

📊 Validating polygon prices for past 7 days...
✅ polygon: 30339 missing, 138 abnormal out of 44964 expected prices

✅ Successfully exported 12 metrics to Prometheus

📊 Validation Summary:
   • Total Expected: 45,270 price records
   • Total Missing: 30,390 (67.13%)
   • Total Abnormal: 152 (0.34%)
   • Metrics Export: ✅ Success

📋 Detailed Validation Results:
   📊 TIINGO:
      • Expected: 306 prices
      • Missing: 51 (16.67%)
      • Abnormal: 14 (4.58%)
      • Zero Volume: 13
      • Price Spikes: 1
```

## Integration with ATS Platform

### **Daily Schedule Coordination**
- **1:00 AM**: Database sync (DEV → INTG) populates vendor tables
- **6:30 AM**: Health check validates system status
- **6:45 AM**: **Daily prices validation runs** ← This job
- **Morning**: Fresh metrics available in Grafana dashboard

### **Alerting Integration**
- Grafana alerts can be configured based on validation metrics
- Slack notifications for critical data quality issues
- Integration with existing ATS monitoring infrastructure

---

**Status**: ✅ Production Ready
**Last Updated**: 2025-09-09
**Cron Schedule**: Daily at 6:45 AM EDT
**Monitoring**: ATS-INTG Grafana Dashboard