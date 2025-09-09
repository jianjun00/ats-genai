# Daily Prices Backfill Operations Guide

This document provides comprehensive operational procedures for daily price data backfills across all vendors in the ATS platform.

## 🏗️ **Database Architecture**

### **Database Environments**

| Environment | Host | Port | Database | Purpose |
|-------------|------|------|----------|---------|
| **DEV** | localhost | 3432 | dev_db | Development and staging |
| **INTG** | localhost | 4432 | intg_db | Integration and testing |
| **PROD** | ats-prod-postgres | 5432 | prod_db | Production environment |

### **Daily Prices Tables**

| Vendor | DEV Table | INTG Table | PROD Table | Schema |
|--------|-----------|------------|------------|---------|
| **EODHD** | `dev_daily_prices_eodhd` | `intg_daily_prices_eodhd` | `prod_daily_prices_eodhd` | `(date, symbol, open, high, low, close, adjusted_close, volume, instrument_id)` |
| **Tiingo** | `dev_daily_prices_tiingo` | `intg_daily_prices_tiingo` | `prod_daily_prices_tiingo` | `(date, symbol, open, high, low, close, volume, instrument_id)` |
| **Polygon** | `dev_daily_prices_polygon` | `intg_daily_prices_polygon` | `prod_daily_prices_polygon` | `(date, symbol, open, high, low, close, volume, market_cap, instrument_id)` |

### **Table Constraints**

All tables have:
- **Primary Key**: `(date, instrument_id)` 
- **Indexes**: 
  - `btree (date, instrument_id)` (primary)
  - `btree (created_at)` (for tracking)
- **Foreign Key**: `instrument_id` references respective instruments table

## 📊 **Data Status Queries**

### **Check Current Data Status**

```sql
-- EODHD Status
SELECT 'EODHD DEV' as source, COUNT(*) as total_records, 
       MAX(date) as latest_date, MIN(date) as earliest_date 
FROM dev_daily_prices_eodhd;

SELECT 'EODHD INTG' as source, COUNT(*) as total_records, 
       MAX(date) as latest_date, MIN(date) as earliest_date 
FROM intg_daily_prices_eodhd;

-- Tiingo Status  
SELECT 'TIINGO DEV' as source, COUNT(*) as total_records, 
       MAX(date) as latest_date, MIN(date) as earliest_date 
FROM dev_daily_prices_tiingo;

SELECT 'TIINGO INTG' as source, COUNT(*) as total_records, 
       MAX(date) as latest_date, MIN(date) as earliest_date 
FROM intg_daily_prices_tiingo;

-- Polygon Status
SELECT 'POLYGON DEV' as source, COUNT(*) as total_records, 
       MAX(date) as latest_date, MIN(date) as earliest_date 
FROM dev_daily_prices_polygon;

SELECT 'POLYGON INTG' as source, COUNT(*) as total_records, 
       MAX(date) as latest_date, MIN(date) as earliest_date 
FROM intg_daily_prices_polygon;
```

### **Check Recent Data (Past 30 Days)**

```sql
-- Recent data for all vendors
SELECT 'EODHD' as vendor, COUNT(*) as recent_count 
FROM intg_daily_prices_eodhd 
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
UNION ALL
SELECT 'TIINGO' as vendor, COUNT(*) as recent_count 
FROM intg_daily_prices_tiingo 
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
UNION ALL
SELECT 'POLYGON' as vendor, COUNT(*) as recent_count 
FROM intg_daily_prices_polygon 
WHERE date >= CURRENT_DATE - INTERVAL '30 days';
```

### **Identify Data Gaps**

```sql
-- Find missing dates for a specific vendor (example: EODHD)
WITH date_series AS (
  SELECT generate_series(
    CURRENT_DATE - INTERVAL '30 days',
    CURRENT_DATE - INTERVAL '1 day',
    '1 day'::interval
  )::date as date
),
existing_dates AS (
  SELECT DISTINCT date 
  FROM intg_daily_prices_eodhd 
  WHERE date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT ds.date as missing_date
FROM date_series ds
LEFT JOIN existing_dates ed ON ds.date = ed.date
WHERE ed.date IS NULL
AND EXTRACT(DOW FROM ds.date) NOT IN (0, 6); -- Exclude weekends
```

## 🔄 **Backfill Operations**

### **1. Database-to-Database Sync (Recommended)**

#### **Unified Vendor Sync Service**
Location: `scripts/eodhd_database_sync.py`

```bash
# Sync EODHD from DEV to INTG
cd /home/jianjun/ats-genai-data
PYTHONPATH=src python3 scripts/eodhd_database_sync.py --vendor eodhd

# Sync Tiingo from DEV to INTG  
PYTHONPATH=src python3 scripts/eodhd_database_sync.py --vendor tiingo

# Sync Polygon from DEV to INTG
PYTHONPATH=src python3 scripts/eodhd_database_sync.py --vendor polygon

# Custom database configuration
PYTHONPATH=src python3 scripts/eodhd_database_sync.py \
  --vendor eodhd \
  --source-port 3432 \
  --target-port 4432 \
  --source-db dev_db \
  --target-db intg_db
```

#### **Sync Performance**
- **Rate**: ~1,000-1,200 records/second
- **Batch Size**: 10,000 records per batch
- **Safety**: Uses `ON CONFLICT DO NOTHING` (no data loss risk)
- **Monitoring**: Real-time progress with batch counters

### **2. API-Based Backfills**

#### **EODHD API Backfill**
Location: `src/infrastructure/vendor/eodhd/services/eodhd_30_year_daily_backfill.py`

```bash
# Backfill past 30 days to INTG
cd /home/jianjun/ats-genai-data
ENV_TYPE=intg EODHD_API_KEY=your_api_key PYTHONPATH=src python3 \
  src/infrastructure/vendor/eodhd/services/eodhd_30_year_daily_backfill.py \
  --start_date 2025-08-10 \
  --end_date 2025-09-09 \
  --limit 100

# Backfill specific date range
ENV_TYPE=intg EODHD_API_KEY=your_api_key PYTHONPATH=src python3 \
  src/infrastructure/vendor/eodhd/services/eodhd_30_year_daily_backfill.py \
  --start_date 2025-09-01 \
  --end_date 2025-09-09 \
  --skip_existing
```

#### **Tiingo API Backfill**
Location: `src/infrastructure/vendor/tiingo/services/tiingo_30_year_daily_backfill.py`

```bash
# Backfill past 30 days to INTG
cd /home/jianjun/ats-genai-data
ENV_TYPE=intg TIINGO_API_KEY=your_api_key PYTHONPATH=src python3 \
  src/infrastructure/vendor/tiingo/services/tiingo_30_year_daily_backfill.py \
  --start_date 2025-08-10 \
  --end_date 2025-09-09 \
  --limit 50

# Process specific symbols
ENV_TYPE=intg TIINGO_API_KEY=your_api_key PYTHONPATH=src python3 \
  src/infrastructure/vendor/tiingo/services/tiingo_30_year_daily_backfill.py \
  --start_date 2025-09-01 \
  --end_date 2025-09-09 \
  --limit 10 \
  --skip_existing
```

#### **API Rate Limits**

| Vendor | Free Tier | Paid Tier | Rate Limit |
|--------|-----------|-----------|------------|
| **EODHD** | 20 req/min | 1000 req/min | 3.0s delay |
| **Tiingo** | 500 req/hour | 1000 req/hour | 3.6s delay |
| **Polygon** | 5 req/min | 1000 req/min | 12.0s delay |

### **3. Minute Bar Backfills**

#### **FirstRate Incremental Backfill**
Location: `scripts/firstrate_incremental_backfill.py`

```bash
# Backfill past 30 days minute bars
cd /home/jianjun/ats-genai-data
PYTHONPATH=src python3 scripts/firstrate_incremental_backfill.py \
  --limit 10 \
  --days 30 \
  --output /mnt/d/ats-data/minute-bars/firstrate

# Process specific symbol
PYTHONPATH=src python3 scripts/firstrate_incremental_backfill.py \
  --symbol AAPL \
  --days 30 \
  --output /mnt/d/ats-data/minute-bars/firstrate
```

#### **Tiingo Minute Bar Backfill**
Location: `scripts/tiingo_incremental_backfill.py`

```bash
# Backfill past 30 days minute bars
cd /home/jianjun/ats-genai-data
source .env && PYTHONPATH=src python3 scripts/tiingo_incremental_backfill.py \
  --limit 10 \
  --days 30 \
  --output /mnt/d/ats-data/minute-bars/tiingo

# Process specific symbol
source .env && PYTHONPATH=src python3 scripts/tiingo_incremental_backfill.py \
  --symbol TSLA \
  --days 30 \
  --output /mnt/d/ats-data/minute-bars/tiingo
```

## 🕐 **Cron Jobs and Automation**

### **Daily Sync Schedule**

Create crontab entries for automated daily backfills:

```bash
# Edit crontab
crontab -e

# Add daily sync jobs (run at 2 AM each day)
0 2 * * 1-5 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/eodhd_database_sync.py --vendor eodhd >> /var/log/ats/eodhd_sync.log 2>&1
15 2 * * 1-5 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/eodhd_database_sync.py --vendor tiingo >> /var/log/ats/tiingo_sync.log 2>&1
30 2 * * 1-5 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/eodhd_database_sync.py --vendor polygon >> /var/log/ats/polygon_sync.log 2>&1

# Weekly minute bar updates (Sundays at 1 AM)
0 1 * * 0 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/firstrate_incremental_backfill.py --limit 50 --days 7 >> /var/log/ats/firstrate_weekly.log 2>&1
```

### **Systemd Services**

Create systemd service for daily operations:

```bash
# /etc/systemd/system/ats-daily-sync.service
[Unit]
Description=ATS Daily Prices Sync
After=postgresql.service

[Service]
Type=oneshot
User=jianjun
WorkingDirectory=/home/jianjun/ats-genai-data
Environment=PYTHONPATH=/home/jianjun/ats-genai-data/src
ExecStart=/usr/bin/python3 scripts/eodhd_database_sync.py --vendor eodhd
ExecStartPost=/usr/bin/python3 scripts/eodhd_database_sync.py --vendor tiingo  
ExecStartPost=/usr/bin/python3 scripts/eodhd_database_sync.py --vendor polygon

[Install]
WantedBy=multi-user.target

# /etc/systemd/system/ats-daily-sync.timer
[Unit]
Description=Run ATS Daily Sync
Requires=ats-daily-sync.service

[Timer]
OnCalendar=Mon-Fri 02:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enable the timer:
```bash
sudo systemctl enable ats-daily-sync.timer
sudo systemctl start ats-daily-sync.timer
sudo systemctl status ats-daily-sync.timer
```

## 📋 **Operational Procedures**

### **Daily Checklist**

1. **Morning Data Validation** (9 AM):
   ```bash
   # Quick status check
   cd /home/jianjun/ats-genai-data
   PYTHONPATH=src python3 -c "
   import asyncpg
   import asyncio
   
   async def check_data():
       conn = await asyncpg.connect(
           host='localhost', port=4432, user='postgres', 
           password='intg_password', database='intg_db'
       )
       
       # Check yesterday's data
       result = await conn.fetch('''
           SELECT 'EODHD' as vendor, COUNT(*) as count FROM intg_daily_prices_eodhd WHERE date = CURRENT_DATE - 1
           UNION ALL
           SELECT 'TIINGO' as vendor, COUNT(*) as count FROM intg_daily_prices_tiingo WHERE date = CURRENT_DATE - 1  
           UNION ALL
           SELECT 'POLYGON' as vendor, COUNT(*) as count FROM intg_daily_prices_polygon WHERE date = CURRENT_DATE - 1
       ''')
       
       for row in result:
           print(f'{row[\"vendor\"]}: {row[\"count\"]:,} records for yesterday')
       
       await conn.close()
   
   asyncio.run(check_data())
   "
   ```

2. **Weekly Deep Validation** (Mondays):
   ```bash
   # Comprehensive gap analysis
   cd /home/jianjun/ats-genai-data
   PYTHONPATH=src python3 -c "
   # Run gap detection queries for past 7 days
   # Generate data quality reports
   # Check for orphaned records
   "
   ```

### **Emergency Procedures**

#### **Large Data Gap Recovery**

```bash
# 1. Identify the gap
cd /home/jianjun/ats-genai-data
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "
SELECT vendor, missing_dates, record_count FROM (
  -- Run gap analysis query
) ORDER BY missing_dates DESC;
"

# 2. Emergency API backfill (if gap > 5 days)
ENV_TYPE=intg EODHD_API_KEY=your_key PYTHONPATH=src python3 \
  src/infrastructure/vendor/eodhd/services/eodhd_30_year_daily_backfill.py \
  --start_date 2025-08-01 \
  --end_date 2025-09-09 \
  --limit 500

# 3. Database sync for recent data  
PYTHONPATH=src python3 scripts/eodhd_database_sync.py --vendor eodhd
```

#### **Performance Issues**

```bash
# Check database locks
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "
SELECT pid, usename, application_name, client_addr, state, query_start, query 
FROM pg_stat_activity 
WHERE state = 'active' AND query NOT ILIKE '%pg_stat_activity%';
"

# Check table sizes
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "
SELECT schemaname, tablename, 
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE tablename LIKE '%daily_prices%' 
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
"

# Vacuum analyze tables
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "
VACUUM ANALYZE intg_daily_prices_eodhd;
VACUUM ANALYZE intg_daily_prices_tiingo;
VACUUM ANALYZE intg_daily_prices_polygon;
"
```

## 🔧 **Troubleshooting**

### **Common Issues**

| Issue | Symptoms | Resolution |
|-------|----------|------------|
| **API Rate Limits** | 429 errors, slow progress | Increase `request_delay`, use database sync |
| **Connection Timeout** | Connection refused errors | Check database status, restart PostgreSQL |
| **Orphaned Records** | Foreign key violations | Clean up instruments table, re-run with `--skip_existing` |
| **Duplicate Data** | Constraint violations | Use `ON CONFLICT DO NOTHING`, check date formats |
| **Memory Issues** | OOM errors | Reduce batch size, add swap space |

### **Log Locations**

```bash
# Application logs
/var/log/ats/
├── eodhd_sync.log
├── tiingo_sync.log
├── polygon_sync.log
├── firstrate_weekly.log
└── daily_operations.log

# Database logs
/var/log/postgresql/postgresql-13-main.log

# System logs
journalctl -u ats-daily-sync.service
journalctl -u ats-daily-sync.timer
```

### **Monitoring Commands**

```bash
# Real-time sync monitoring
tail -f /var/log/ats/eodhd_sync.log

# Check sync service status  
systemctl status ats-daily-sync.service
systemctl status ats-daily-sync.timer

# Database connection test
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "SELECT version();"

# Check recent sync activity
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "
SELECT 'EODHD' as vendor, MAX(created_at) as last_update FROM intg_daily_prices_eodhd
UNION ALL  
SELECT 'TIINGO' as vendor, MAX(created_at) as last_update FROM intg_daily_prices_tiingo
UNION ALL
SELECT 'POLYGON' as vendor, MAX(created_at) as last_update FROM intg_daily_prices_polygon;
"
```

## 🚀 **Performance Optimization**

### **Database Tuning**

```sql
-- Optimize for bulk inserts
SET maintenance_work_mem = '1GB';
SET checkpoint_completion_target = 0.9;
SET wal_buffers = '64MB';
SET shared_buffers = '2GB';

-- Create optimal indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_eodhd_symbol_date 
ON intg_daily_prices_eodhd (symbol, date DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tiingo_symbol_date 
ON intg_daily_prices_tiingo (symbol, date DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_polygon_symbol_date 
ON intg_daily_prices_polygon (symbol, date DESC);
```

### **Batch Size Optimization**

```bash
# For large datasets (>1M records)
export BATCH_SIZE=50000

# For API-limited vendors
export BATCH_SIZE=1000

# For high-performance local sync
export BATCH_SIZE=100000
```

---

## 📞 **Support Contacts**

- **Database Issues**: DBA Team
- **API Issues**: Data Engineering Team  
- **Infrastructure**: DevOps Team
- **Emergency**: On-call rotation

## 📝 **Change Log**

| Date | Version | Changes |
|------|---------|---------|
| 2025-09-09 | 1.0 | Initial documentation with all vendor support |

---

**Note**: This documentation covers operational procedures as of September 2025. Always verify current infrastructure status and API availability before executing backfill operations.