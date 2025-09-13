# 📊 Daily Prices Quality Dashboard

## Overview

The Daily Prices Quality Dashboard provides comprehensive monitoring of daily prices data quality across all vendors (Polygon, Tiingo, EODHD). It tracks missing prices, bad/invalid prices, and coverage percentages for weekdays only (excluding holidays).

## Dashboard Location

- **File**: `config/dashboards/daily-prices-quality-dashboard.json`
- **SignOz Import**: Copy JSON content and import into SignOz dashboard

## 🎯 Key Metrics Tracked

### Primary Metrics

| Metric | Description | Thresholds |
|--------|-------------|------------|
| **Missing Symbols** | Number of symbols missing daily prices | 🟢 0 / 🟡 10+ / 🔴 100+ |
| **Missing Records** | Total missing daily price records | 🟢 0 / 🟡 1K+ / 🔴 10K+ |
| **Coverage %** | Data coverage percentage | 🔴 <80% / 🟡 80-95% / 🟢 >95% |
| **Bad Symbols** | Symbols with invalid prices | 🟢 0 / 🟡 1+ / 🔴 10+ |
| **Bad Records** | Count of invalid price records | 🟢 0 / 🟡 10+ / 🔴 100+ |

### Data Quality Checks

**Bad Prices Detection:**
- ❌ Invalid prices (≤0, NULL values)
- ❌ Illogical OHLC relationships (high < low, etc.)
- ❌ Extreme price changes (>50% daily change)
- ❌ Negative volume values

## 📋 Dashboard Panels

### Panel 1: 🚨 Missing Daily Prices by Vendor
- **Type**: Stat panel
- **Metric**: `ats_daily_prices_missing_symbols_total`
- **Shows**: Count of symbols missing daily prices per vendor
- **Color coding**: Green (0) → Yellow (10+) → Red (100+)

### Panel 2: 📉 Missing Daily Price Records
- **Type**: Stat panel
- **Metric**: `ats_daily_prices_missing_records_total`
- **Shows**: Total missing daily price records
- **Color coding**: Green (0) → Yellow (1K+) → Red (10K+)

### Panel 3: ✅ Data Coverage Percentage
- **Type**: Stat panel with gauge
- **Metric**: `ats_daily_prices_coverage_percent`
- **Shows**: Coverage percentage per vendor
- **Thresholds**: Red (<80%) → Yellow (80-95%) → Green (>95%)

### Panel 4: 📈 Missing Prices Trend (24h)
- **Type**: Time series graph
- **Shows**: Trend of missing symbols over time
- **Helps**: Identify patterns and deterioration

### Panel 5: 🎯 Coverage Trend (24h)
- **Type**: Time series graph
- **Shows**: Coverage percentage trend with threshold lines
- **Alerts**: Visual indicators when coverage drops below 80% or 95%

### Panel 6: 🚨 Bad Prices Detection
- **Type**: Stat panel
- **Metric**: `ats_daily_prices_bad_symbols_total`
- **Shows**: Symbols with invalid/abnormal prices

### Panel 7: ⚠️ Bad Price Records Count
- **Type**: Stat panel
- **Metric**: `ats_daily_prices_bad_records_total`
- **Shows**: Count of individual bad price records

### Panel 8: 📊 Data Quality Summary Table
- **Type**: Table
- **Shows**: Combined view of all quality metrics by vendor
- **Features**: Color-coded cells based on thresholds

### Panel 9: 🕐 Last Data Update
- **Type**: Stat panel
- **Shows**: Time since last metrics update
- **Alerts**: Yellow (30min+), Red (1hr+)

### Panel 10: 📋 Quality Metrics Info
- **Type**: Text panel
- **Contains**: Documentation and threshold explanations

## 🔧 Setup Instructions

### 1. Generate Metrics Data

```bash
# Generate quality metrics (run every 30 minutes via cron)
python3 scripts/daily_prices_quality_metrics.py --days 90 --push-metrics

# Test with different parameters
python3 scripts/daily_prices_quality_metrics.py --days 30 --environment intg --push-metrics
```

### 2. Import Dashboard to SignOz

1. Copy content from `config/dashboards/daily-prices-quality-dashboard.json`
2. Open SignOz dashboard interface
3. Click "Import Dashboard"
4. Paste JSON content
5. Save dashboard

### 3. Set Up Automated Metrics Collection

```bash
# Add to crontab for automated collection every 30 minutes
*/30 * * * * cd /home/jianjun/ats-genai-model && python3 scripts/daily_prices_quality_metrics.py --days 90 --push-metrics >/dev/null 2>&1
```

## 🎛️ Dashboard Variables

### Template Variables

- **Vendor Filter**: Select specific vendors (polygon, tiingo, eodhd) or all
- **Environment Filter**: Filter by environment (dev, intg) or all

### Time Range Options

- Default: Last 24 hours
- Available: 5m, 15m, 1h, 6h, 12h, 24h, 2d, 7d, 30d
- Refresh rate: 30 seconds

## 🚨 Alerting & Annotations

### Automatic Annotations

1. **Quality Issues**: Triggered when coverage < 80%
2. **Bad Prices Detected**: Triggered when bad records increase

### Recommended Alerts

```yaml
# Coverage Alert
- alert: DailyPricesCoverageLow
  expr: ats_daily_prices_coverage_percent < 80
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Daily prices coverage below 80% for {{ $labels.vendor }}"

# Bad Prices Alert
- alert: BadPricesDetected
  expr: increase(ats_daily_prices_bad_records_total[1h]) > 10
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "Bad prices detected: {{ $value }} bad records in last hour"
```

## 📊 Interpretation Guide

### Healthy Data Signals
- ✅ Coverage: >95% for all vendors
- ✅ Missing symbols: 0 across all vendors
- ✅ Bad prices: 0 invalid records
- ✅ Trends: Stable coverage over time

### Warning Signals
- ⚠️ Coverage: 80-95% (needs investigation)
- ⚠️ Missing symbols: 1-99 symbols missing data
- ⚠️ Increasing trend in missing data

### Critical Issues
- 🚨 Coverage: <80% (immediate action required)
- 🚨 Missing symbols: >100 symbols without data
- 🚨 Bad prices: Any invalid data detected
- 🚨 Degrading trends over time

## 🔍 Troubleshooting

### Low Coverage Issues

1. **Check Data Pipeline**:
   ```bash
   python3 scripts/run_intg.py query --query "SELECT MAX(date), COUNT(*) FROM intg_daily_price_polygon"
   ```

2. **Verify API Keys**:
   ```bash
   python3 scripts/validate_api_keys.py
   ```

3. **Check Service Status**:
   ```bash
   python3 scripts/run_intg.py status
   ```

### Bad Prices Investigation

1. **Query Bad Records**:
   ```sql
   SELECT symbol, date, open, high, low, close, volume
   FROM intg_daily_price_polygon
   WHERE high < low OR open <= 0
   ORDER BY date DESC LIMIT 10;
   ```

2. **Check Data Sources**:
   ```bash
   # Verify if issue is vendor-specific
   python3 scripts/daily_prices_quality_metrics.py --days 7
   ```

## 🔗 Related Resources

- **Script**: `/scripts/daily_prices_quality_metrics.py`
- **Metrics**: Prometheus metrics pushed to localhost:9091
- **Database**: `intg_daily_price_polygon/tiingo/eodhd` tables
- **Operations**: [ATS Operations Guide](OPERATIONS.md)

## 💡 Best Practices

1. **Monitor Daily**: Check dashboard every morning for data quality
2. **Set Alerts**: Configure alerts for coverage < 80% and bad prices > 0
3. **Weekly Review**: Analyze trends weekly to identify patterns
4. **Immediate Action**: Investigate any bad prices immediately
5. **Documentation**: Log any recurring issues and solutions

---

**Dashboard Version**: 1.0
**Last Updated**: 2025-09-11
**Refresh Rate**: 30 seconds
**Data Source**: Prometheus metrics from daily prices quality script