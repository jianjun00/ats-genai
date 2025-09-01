# ATS Operations Guide

## Daily Price Data Backfill Operations

### Overview

The ATS platform supports comprehensive daily price data backfill from multiple vendors with 30-year historical coverage. This guide provides operational procedures for running, monitoring, and troubleshooting daily price backfill operations.

### Supported Vendors

| Vendor | Coverage | Rate Limits | API Key Required | Table Name |
|--------|----------|-------------|------------------|------------|
| **Tiingo** | 1995-present | 1000 calls/hour | TIINGO_API_KEY | `{env}_daily_prices_tiingo` |
| **Polygon** | 2010-present | 5 calls/minute | POLYGON_API_KEY | `{env}_daily_prices_polygon` |
| **EODHD** | 1995-present | 20 calls/minute | EODHD_API_KEY | `{env}_daily_prices_eodhd` |

### Database Tables

**Development Environment (DEV):**
- `dev_daily_prices_tiingo`
- `dev_daily_prices_polygon`
- `dev_daily_prices_eodhd`

**Integration Environment (INTG):**
- `intg_daily_prices_tiingo`
- `intg_daily_prices_polygon`
- `intg_daily_prices_eodhd`

### Prerequisites

1. **Database Environment Running:**
   ```bash
   # Start ATS-DEV environment
   python3 scripts/run_dev.py setup
   
   # Start ATS-INTG environment
   docker-compose -f docker-compose.ats.yml up -d postgres-intg
   ```

2. **API Keys Configuration:**
   ```bash
   export TIINGO_API_KEY=your_tiingo_api_key
   export POLYGON_API_KEY=your_polygon_api_key
   export EODHD_API_KEY=your_eodhd_api_key
   ```

3. **Instrument Population:**
   Ensure instruments are populated before running price backfill:
   ```bash
   # Check instrument counts
   python3 scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_instruments WHERE active = true"
   
   # Populate if needed
   python3 scripts/run_dev.py run --script scripts/run_tiingo_bulk.py
   ```

## Backfill Operations

### Single Vendor Backfill

**Tiingo 30-Year Backfill:**
```bash
# Development environment
python3 scripts/run_dev.py run --script scripts/tiingo_30_year_daily_backfill.py --env '{"TIINGO_API_KEY": "your_key"}'

# Integration environment with specific symbols
TIINGO_API_KEY=your_key python3 scripts/run_dev.py --environment intg run --script scripts/tiingo_30_year_daily_backfill.py --env '{"ENV_TYPE": "intg", "TARGET_SYMBOLS": "AAPL,MSFT,GOOGL"}'

# Full 30-year backfill (background)
nohup TIINGO_API_KEY=your_key python3 scripts/run_dev.py run --script scripts/tiingo_30_year_daily_backfill.py > /tmp/tiingo_30year_backfill.log 2>&1 &
```

**Polygon 30-Year Backfill:**
```bash
# Development environment
python3 scripts/run_dev.py run --script scripts/polygon_30_year_daily_backfill.py --env '{"POLYGON_API_KEY": "your_key"}'

# Integration environment with date range
POLYGON_API_KEY=your_key python3 scripts/run_dev.py --environment intg run --script scripts/polygon_30_year_daily_backfill.py --env '{"ENV_TYPE": "intg", "START_DATE": "2010-06-29"}'

# Full 30-year backfill (background)
nohup POLYGON_API_KEY=your_key python3 scripts/run_dev.py run --script scripts/polygon_30_year_daily_backfill.py > /tmp/polygon_30year_backfill.log 2>&1 &
```

**EODHD 30-Year Backfill:**
```bash
# Development environment
python3 scripts/run_dev.py run --script scripts/eodhd_30_year_daily_backfill.py --env '{"EODHD_API_KEY": "your_key"}'

# Integration environment with limit
EODHD_API_KEY=your_key python3 scripts/run_dev.py --environment intg run --script scripts/eodhd_30_year_daily_backfill.py --env '{"ENV_TYPE": "intg", "LIMIT": "100"}'

# Full 30-year backfill (background)
nohup EODHD_API_KEY=your_key python3 scripts/run_dev.py run --script scripts/eodhd_30_year_daily_backfill.py > /tmp/eodhd_30year_backfill.log 2>&1 &
```

### Multi-Vendor Parallel Backfill

**Complete 3-Vendor Coverage (Production Recommended):**
```bash
# Run all three vendors in parallel for comprehensive coverage
nohup TIINGO_API_KEY=your_tiingo_key python3 scripts/run_dev.py run --script scripts/tiingo_30_year_daily_backfill.py > /tmp/tiingo_30year_backfill.log 2>&1 &

nohup POLYGON_API_KEY=your_polygon_key python3 scripts/run_dev.py run --script scripts/polygon_30_year_daily_backfill.py > /tmp/polygon_30year_backfill.log 2>&1 &

nohup EODHD_API_KEY=your_eodhd_key python3 scripts/run_dev.py run --script scripts/eodhd_30_year_daily_backfill.py > /tmp/eodhd_30year_backfill.log 2>&1 &
```

### Configuration Parameters

**Environment Variables:**
- `ENV_TYPE`: Set to `intg` for integration environment, `dev` for development (auto-detected)
- `TARGET_SYMBOLS`: Comma-separated list of specific symbols to process (e.g., "AAPL,MSFT,GOOGL")
- `START_DATE`: Override start date (YYYY-MM-DD format)
- `END_DATE`: Override end date (YYYY-MM-DD format, defaults to today)
- `LIMIT`: Limit number of instruments to process (for testing)
- `YEARS`: Number of years to backfill (default: 30)

**Command Line Arguments:**
- `--debug`: Enable debug logging
- `--limit N`: Limit processing to N instruments
- `--years N`: Process N years of historical data
- `--start_date YYYY-MM-DD`: Start date for backfill
- `--end_date YYYY-MM-DD`: End date for backfill
- `--skip_existing`: Skip instruments with existing data (default: true)

## Monitoring and Status

### Progress Monitoring

**Real-time Log Monitoring:**
```bash
# Monitor individual vendor progress
tail -f /tmp/tiingo_30year_backfill.log
tail -f /tmp/polygon_30year_backfill.log  
tail -f /tmp/eodhd_30year_backfill.log

# Monitor all vendors
multitail /tmp/tiingo_30year_backfill.log /tmp/polygon_30year_backfill.log /tmp/eodhd_30year_backfill.log
```

**Database Record Counts:**
```bash
# Check progress across all vendors
python3 scripts/run_dev.py query --query "
SELECT 
    'Tiingo' as vendor, 
    COUNT(*) as records,
    COUNT(DISTINCT instrument_id) as instruments,
    MIN(date) as earliest,
    MAX(date) as latest
FROM dev_daily_prices_tiingo
UNION ALL
SELECT 
    'Polygon' as vendor,
    COUNT(*) as records,
    COUNT(DISTINCT instrument_id) as instruments,
    MIN(date) as earliest,
    MAX(date) as latest
FROM dev_daily_prices_polygon
UNION ALL
SELECT 
    'EODHD' as vendor,
    COUNT(*) as records,
    COUNT(DISTINCT instrument_id) as instruments,
    MIN(date) as earliest,
    MAX(date) as latest
FROM dev_daily_prices_eodhd
ORDER BY vendor;
"

# Check specific symbol coverage
python3 scripts/run_dev.py query --query "
SELECT 
    i.symbol,
    COUNT(CASE WHEN dp_t.date IS NOT NULL THEN 1 END) as tiingo_records,
    COUNT(CASE WHEN dp_p.date IS NOT NULL THEN 1 END) as polygon_records,
    COUNT(CASE WHEN dp_e.date IS NOT NULL THEN 1 END) as eodhd_records,
    MIN(COALESCE(dp_t.date, dp_p.date, dp_e.date)) as earliest_data,
    MAX(COALESCE(dp_t.date, dp_p.date, dp_e.date)) as latest_data
FROM dev_instruments i
LEFT JOIN dev_daily_prices_tiingo dp_t ON i.id = dp_t.instrument_id
LEFT JOIN dev_daily_prices_polygon dp_p ON i.id = dp_p.instrument_id
LEFT JOIN dev_daily_prices_eodhd dp_e ON i.id = dp_e.instrument_id
WHERE i.symbol IN ('AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN')
GROUP BY i.id, i.symbol
ORDER BY i.symbol;
"
```

**Collection Status Script:**
```bash
# Use monitoring script for comprehensive status
./scripts/collection_status.sh
```

### Performance Monitoring

**API Rate Limit Status:**
- **Tiingo**: 1000 calls/hour = ~16.7 calls/minute (1 second delays)
- **Polygon**: 5 calls/minute (12 second delays) 
- **EODHD**: 20 calls/minute (3 second delays)

**Expected Processing Times:**
- **Small Test (100 instruments)**: 2-4 hours
- **Complete Backfill (~18,000 instruments)**: 15-24 hours per vendor
- **Multi-vendor (3 vendors parallel)**: 15-24 hours total

**Resource Usage Monitoring:**
```bash
# Monitor container resource usage
docker stats --no-stream | grep -E "(ats-dev|postgres)"

# Monitor disk usage
df -h /mnt/d/
du -sh /var/snap/docker/common/var-lib-docker/volumes/postgres*

# Monitor system resources
htop
iostat -x 1
```

## Troubleshooting

### Common Issues and Solutions

**1. API Authentication Errors**
```
❌ API_KEY environment variable not set
❌ API error: 401 Unauthorized
```
**Solution:**
```bash
# Verify API keys are set correctly
echo $TIINGO_API_KEY
echo $POLYGON_API_KEY
echo $EODHD_API_KEY

# Test API endpoints directly
curl "https://api.tiingo.com/tiingo/daily/AAPL/prices?token=$TIINGO_API_KEY&startDate=2024-01-01&endDate=2024-01-02"
```

**2. Database Connection Errors**
```
❌ could not connect to server: Connection refused
❌ relation "intg_daily_prices_polygon" does not exist
```
**Solution:**
```bash
# Check database containers are running
docker ps | grep postgres

# Check database connectivity
python3 scripts/run_dev.py query --query "SELECT version()"

# Create missing tables
PYTHONPATH=src python3 -m src.db.create_all_tables
```

**3. Rate Limit Exceeded**
```
⚠️ Rate limit hit for AAPL, waiting...
❌ API error: 429 Too Many Requests
```
**Solution:**
- Wait for rate limit reset (automatic in scripts)
- Increase delays between requests if needed
- Use `--limit` parameter for testing with fewer symbols

**4. Primary Key Sequence Issues (EODHD)**
```
❌ duplicate key value violates unique constraint intg_daily_prices_eodhd_pkey Key (id)=(2) already exists
```
**Solution:**
```bash
# Reset sequence to match table data
python3 scripts/run_dev.py query --query "SELECT setval('intg_daily_prices_eodhd_id_seq', COALESCE((SELECT MAX(id) FROM intg_daily_prices_eodhd), 1), true);"
```

**5. Docker Network Connectivity (Fixed 2025-08-30)**
```
❌ container not attached to default bridge network
❌ Could not translate host name "ats-dev-postgres" to address
```
**Solution:**
This issue has been resolved by updating run_dev.py to use modern Docker networking. If you encounter this:
```bash
# Verify network configuration
docker network inspect ats-network --format "{{range .Containers}}{{.Name}} {{end}}"

# Restart with proper networking
python3 scripts/run_dev.py setup
```

### Data Quality Validation

**Verify Data Consistency:**
```bash
# Check for gaps in date sequences
python3 scripts/run_dev.py query --query "
WITH date_gaps AS (
    SELECT 
        symbol,
        date,
        LAG(date) OVER (PARTITION BY symbol ORDER BY date) as prev_date,
        date - LAG(date) OVER (PARTITION BY symbol ORDER BY date) as gap_days
    FROM dev_daily_prices_tiingo 
    WHERE symbol = 'AAPL'
)
SELECT symbol, date, prev_date, gap_days
FROM date_gaps 
WHERE gap_days > 3  -- More than 3 days (weekend + holiday)
ORDER BY gap_days DESC
LIMIT 10;
"

# Check for price anomalies
python3 scripts/run_dev.py query --query "
SELECT 
    symbol, date, close, volume,
    LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_close,
    ABS(close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / LAG(close) OVER (PARTITION BY symbol ORDER BY date) * 100 as price_change_pct
FROM dev_daily_prices_polygon
WHERE symbol IN ('AAPL', 'TSLA', 'MSFT')
    AND ABS(close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / LAG(close) OVER (PARTITION BY symbol ORDER BY date) > 0.2  -- >20% change
ORDER BY price_change_pct DESC
LIMIT 20;
"
```

**Cross-Vendor Data Validation:**
```bash
# Compare prices across vendors for same symbol/date
python3 scripts/run_dev.py query --query "
SELECT 
    t.date,
    t.symbol,
    t.close as tiingo_close,
    p.close as polygon_close,
    e.close as eodhd_close,
    ABS(t.close - p.close) as tiingo_polygon_diff,
    ABS(t.close - e.close) as tiingo_eodhd_diff,
    ABS(p.close - e.close) as polygon_eodhd_diff
FROM dev_daily_prices_tiingo t
JOIN dev_daily_prices_polygon p ON t.instrument_id = p.instrument_id AND t.date = p.date
JOIN dev_daily_prices_eodhd e ON t.instrument_id = e.instrument_id AND t.date = e.date
WHERE t.symbol = 'AAPL'
    AND (ABS(t.close - p.close) > 0.50 OR ABS(t.close - e.close) > 0.50 OR ABS(p.close - e.close) > 0.50)
ORDER BY t.date DESC
LIMIT 10;
"
```

## Recovery and Resume Operations

### Resume Failed Backfill

All backfill scripts use idempotent UPSERT operations with `ON CONFLICT (date, instrument_id)` handling, making them safe to re-run:

```bash
# Simply re-run the same command - will skip existing data
TIINGO_API_KEY=your_key python3 scripts/run_dev.py run --script scripts/tiingo_30_year_daily_backfill.py
```

### Selective Backfill

**Backfill Specific Symbols:**
```bash
# Backfill only specific symbols
POLYGON_API_KEY=your_key python3 scripts/run_dev.py run --script scripts/polygon_30_year_daily_backfill.py --env '{"TARGET_SYMBOLS": "AAPL,TSLA,NVDA"}'
```

**Backfill Specific Date Range:**
```bash
# Backfill specific date range
EODHD_API_KEY=your_key python3 scripts/run_dev.py run --script scripts/eodhd_30_year_daily_backfill.py --env '{"START_DATE": "2020-01-01", "END_DATE": "2020-12-31"}'
```

**Force Re-populate (Skip Existing = False):**
```bash
# Re-populate data even if it exists (careful - may create duplicates)
python3 scripts/run_dev.py run --script scripts/tiingo_30_year_daily_backfill.py --skip_existing false
```

## Expected Results

### Successful Backfill Metrics

**Scale and Coverage:**
- **Instruments**: ~18,000 active US exchange symbols
- **Time Span**: 30 years (1995-2025) 
- **Expected Records**: 40+ million daily price records total across 3 vendors
- **Completion Time**: 15-20 hours per vendor depending on rate limits

**Example Successful TSLA Coverage:**
- **Tiingo**: 3,817 records (2010-06-29 to 2025-08-29) - Full IPO coverage
- **Polygon**: 2,511 records (2015-09-04 to 2025-08-29) - 10-year coverage
- **EODHD**: 3,817 records (2010-06-29 to 2025-08-29) - Full IPO coverage
- **Total**: 10,145 daily price records providing robust multi-vendor redundancy

### Final Summary Output

Each script provides comprehensive completion summary:
```
================================================================================
🎉 TIINGO 30-YEAR DAILY PRICE BACKFILL COMPLETE
================================================================================
📊 PROCESSING SUMMARY:
  Total Instruments: 18,296
  Processed Instruments: 17,422
  Skipped Instruments: 874
  Total Records Inserted: 12,547,832
  API Calls Made: 17,422
  Errors: 0

✅ Success Rate: 95.2%
📈 Average Records per Instrument: 720.3
================================================================================
```

## Security and API Management

### API Key Best Practices

1. **Never hardcode API keys** in scripts or configuration files
2. **Use environment variables** for all API key references
3. **Store keys securely** using password managers or secure vaults
4. **Rotate keys regularly** according to vendor recommendations
5. **Monitor usage** to avoid unexpected charges or rate limits

### Rate Limit Management

- **Respect vendor limits** - scripts include appropriate delays
- **Monitor API usage** through vendor dashboards
- **Plan for long-running operations** - 30-year backfills take 15-24 hours
- **Use background execution** with nohup for production operations

### Data Security

- **Database credentials** stored in environment variables
- **Network security** via Docker internal networking
- **Access control** via PostgreSQL user permissions
- **Backup and recovery** procedures in place

## Maintenance

### Regular Operations

**Weekly:**
- Check API key usage and remaining quotas
- Monitor disk usage for database volumes
- Review error logs for any recurring issues

**Monthly:**
- Update instruments population to capture new listings
- Run data quality validation queries
- Clean up old log files

**Quarterly:**
- Review and update API keys as needed
- Validate data completeness across all vendors
- Performance tune database indices if needed

### Disaster Recovery

**Database Recovery:**
```bash
# Stop services
python3 scripts/run_dev.py stop --service analytics
docker stop ats-dev-postgres

# Restore from backup (if available)
# Note: Manual backup restoration only - no automatic restoration

# Restart services
python3 scripts/run_dev.py setup
```

**Complete Re-population:**
If database is lost and backups unavailable:
1. Start fresh database environment
2. Populate instruments using bulk scripts
3. Run multi-vendor backfill operations in parallel
4. Validate data quality and completeness

---

**Last Updated**: 2025-09-01  
**Version**: 1.0  
**Reviewed By**: ATS Operations Team