# ATS-INTG Incremental Sync Management

Comprehensive system for handling ongoing incremental changes from ATS-DEV to ATS-INTG with real-time synchronization, conflict resolution, and automated scheduling.

## 📋 Overview

The incremental sync system provides:
- **Real-time Change Detection** - Monitors ATS-DEV for new/updated records
- **Intelligent Sync Strategies** - Different approaches for different data types
- **Conflict Resolution** - Handles duplicates and data conflicts automatically
- **Checkpoint Management** - Resume capability and progress tracking
- **Automated Scheduling** - Market-aware sync timing optimization

## 🏗️ Sync Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   ATS-DEV DB    │    │  Incremental    │    │   ATS-INTG DB   │
│                 │    │  Sync Engine    │    │                 │
│ Change Detection│───▶│                 │───▶│ Upsert/Merge    │
│ (timestamp-based│    │ • Transformation│    │ (conflict       │
│  or trigger)    │    │ • Batching      │    │  resolution)    │
│                 │    │ • Validation    │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
           │                      │                      │
           ▼                      ▼                      ▼
    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
    │ Checkpoint  │    │ Sync Stats  │    │ Conflict    │
    │ Tracking    │    │ & History   │    │ Resolution  │
    └─────────────┘    └─────────────┘    └─────────────┘
```

## 🚀 Quick Start

### Initial Setup
```bash
# 1. Setup incremental sync infrastructure
python scripts/intg_incremental_sync.py setup

# 2. Run first incremental sync
python scripts/intg_incremental_sync.py sync --lookback-hours 24

# 3. Install automated sync daemon
./scripts/intg_sync_daemon.sh install-cron
```

### Manual Sync Operations
```bash
# Sync all tables (last 25 hours of changes)
python scripts/intg_incremental_sync.py sync

# Sync specific tables only
python scripts/intg_incremental_sync.py sync --tables dev_daily_prices dev_instruments

# Extended lookback for catching up
python scripts/intg_incremental_sync.py sync --lookback-hours 72

# Dry run to preview changes
python scripts/intg_incremental_sync.py sync --dry-run
```

## 📊 Sync Strategies by Data Type

### 1. **Instruments Data** (`dev_instruments` → `intg_instruments`)
**Strategy: UPSERT with DEV-wins conflict resolution**

```sql
-- Sync approach
INSERT INTO intg_instruments (symbol, name, exchange, sector, ...)
VALUES (...) 
ON CONFLICT (symbol) DO UPDATE SET
  name = EXCLUDED.name,
  exchange = EXCLUDED.exchange,
  updated_at = CURRENT_TIMESTAMP
```

**Characteristics:**
- ✅ **Change Detection**: `updated_at` timestamp
- ✅ **Lookback**: 24 hours (instruments don't change frequently)
- ✅ **Conflict Resolution**: DEV database wins (authoritative source)
- ✅ **Sync Frequency**: Daily at 6 AM UTC

### 2. **Daily Prices** (`dev_*_daily_prices` → `intg_daily_prices`)
**Strategy: APPEND-ONLY with duplicate prevention**

```sql
-- Sync approach (with vendor identification)
INSERT INTO intg_daily_prices (symbol, date, vendor, open_price, ...)
VALUES (..., 'tiingo', ...)
ON CONFLICT (symbol, date, vendor) DO NOTHING
```

**Characteristics:**
- ✅ **Change Detection**: `created_at` timestamp  
- ✅ **Lookback**: 4 hours (price data is time-sensitive)
- ✅ **Conflict Resolution**: Skip duplicates (prices shouldn't change once recorded)
- ✅ **Sync Frequency**: Every 4 hours during market hours
- ✅ **Vendor Mapping**: Automatic vendor detection based on source table

### 3. **Fundamentals Data** (`dev_fundamentals_comprehensive`)
**Strategy: UPSERT with DEV-wins resolution**

```sql
-- Sync approach
INSERT INTO intg_fundamentals_comprehensive (symbol, date, fiscal_period, ...)
VALUES (...)
ON CONFLICT (symbol, date, vendor, fiscal_period) DO UPDATE SET
  revenue = EXCLUDED.revenue,
  net_income = EXCLUDED.net_income,
  updated_at = CURRENT_TIMESTAMP
```

**Characteristics:**
- ✅ **Change Detection**: `updated_at` timestamp
- ✅ **Lookback**: 48 hours (fundamentals updated less frequently)
- ✅ **Conflict Resolution**: DEV wins (earnings may be restated)
- ✅ **Sync Frequency**: Daily at 7 AM UTC

## ⏰ Automated Sync Schedule

### Market-Aware Scheduling

| Time (UTC) | Sync Type | Target Tables | Rationale |
|------------|-----------|---------------|-----------|
| **Every 4h during 14-21 UTC** | Price Sync | Daily prices | Market hours (9 AM - 4 PM ET) |
| **Every 8h (0,8,16 UTC)** | Comprehensive | All tables | Off-hours complete sync |
| **Daily 6 AM UTC** | Instruments | Instruments only | Before market open |
| **Daily 7 AM UTC** | Fundamentals | Fundamentals only | After overnight processing |
| **Hourly :30** | Health Check | System status | Monitoring |
| **Sunday 2 AM UTC** | Reconciliation | All tables (7-day lookback) | Weekly validation |
| **Daily 3 AM UTC** | Cleanup | Log maintenance | Housekeeping |

### Schedule Installation
```bash
# Install cron-based scheduler
./scripts/intg_sync_daemon.sh install-cron

# Or run persistent daemon
./scripts/intg_sync_daemon.sh start

# Check daemon status
./scripts/intg_sync_daemon.sh status
```

## 📈 Monitoring and Status

### Real-Time Monitoring
```bash
# Check sync status
python scripts/intg_incremental_sync.py status

# Monitor sync logs
tail -f /mnt/d/ats-logs/intg/incremental_sync.log

# Get comprehensive status report
python scripts/intg_incremental_sync.py status > current_status.md
```

### Sync Health Dashboard
```bash
# Check system health
./scripts/intg_sync_daemon.sh health-check

# View recent sync history
python scripts/run_intg.py query --query "
SELECT 
    table_name,
    sync_date,
    records_checked,
    records_inserted + records_updated as records_synced,
    sync_duration_seconds,
    status
FROM intg_sync_history 
WHERE sync_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY sync_timestamp DESC
LIMIT 20"
```

### Key Metrics to Monitor
- **Sync Lag**: Time between DEV changes and INTG sync
- **Success Rate**: Percentage of successful sync operations
- **Record Volume**: Number of records synced per operation
- **Conflict Rate**: Frequency of data conflicts requiring resolution
- **Performance**: Sync duration and throughput metrics

## 🔧 Advanced Configuration

### Custom Sync Strategies

#### Modify Table Sync Configuration
Edit `scripts/intg_incremental_sync.py` to customize sync behavior:

```python
def get_table_sync_strategy() -> dict:
    return {
        'dev_your_table': {
            'target_table': 'intg_your_table',
            'sync_method': 'upsert',  # or 'append_only'
            'timestamp_column': 'updated_at',  # or 'created_at'
            'unique_columns': ['id', 'symbol'],
            'conflict_resolution': 'dev_wins',  # or 'skip_duplicate'
            'change_detection': 'timestamp'  # or 'hash'
        }
    }
```

#### Custom Lookback Periods
```bash
# Price data - short lookback (market data is time-sensitive)
python scripts/intg_incremental_sync.py sync --tables dev_daily_prices --lookback-hours 2

# Reference data - longer lookback (changes less frequently)
python scripts/intg_incremental_sync.py sync --tables dev_instruments --lookback-hours 48

# Full reconciliation - very long lookback
python scripts/intg_incremental_sync.py sync --lookback-hours 168  # 1 week
```

### Performance Optimization

#### Batch Size Tuning
```python
# In scripts/intg_incremental_sync.py
SYNC_BATCH_SIZE = 1000  # Increase for better throughput
MAX_WORKERS = 4         # Increase for more parallelism
```

#### Index Optimization
```sql
-- Optimize sync queries with proper indexing
CREATE INDEX IF NOT EXISTS idx_dev_instruments_updated_at 
ON dev_instruments(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_dev_daily_prices_created_at 
ON dev_daily_prices(created_at DESC);

-- INTG database indexes for conflict resolution
CREATE INDEX IF NOT EXISTS idx_intg_daily_prices_symbol_date_vendor 
ON intg_daily_prices(symbol, date, vendor);
```

## 🛠️ Troubleshooting

### Common Issues and Solutions

#### 1. Sync Lag Issues
```bash
Problem: INTG data is behind DEV by several hours
Diagnosis:
  - Check sync daemon status: ./scripts/intg_sync_daemon.sh status
  - Review sync logs: tail -n 100 /mnt/d/ats-logs/intg/incremental_sync.log
  - Check for failed syncs: python scripts/intg_incremental_sync.py status

Solutions:
  - Restart sync daemon: ./scripts/intg_sync_daemon.sh restart
  - Run manual catch-up sync: python scripts/intg_incremental_sync.py sync --lookback-hours 8
  - Check database connectivity and performance
```

#### 2. Conflict Resolution Issues
```bash
Problem: Data conflicts between DEV and INTG
Diagnosis:
  - Check conflict log: python scripts/run_intg.py query --query "SELECT * FROM intg_sync_conflicts ORDER BY conflict_timestamp DESC LIMIT 10"
  - Review sync strategy configuration

Solutions:
  - Adjust conflict resolution strategy (dev_wins vs skip_duplicate)
  - Implement custom conflict resolution logic
  - Increase sync frequency to reduce conflict windows
```

#### 3. Performance Issues
```bash
Problem: Sync operations taking too long
Diagnosis:
  - Monitor sync duration in history table
  - Check database query performance
  - Review batch sizes and worker threads

Solutions:
  - Optimize database indexes
  - Adjust batch size: smaller batches for frequent sync, larger for bulk
  - Increase worker threads if CPU allows
  - Run sync during off-peak hours
```

#### 4. Missing Data
```bash
Problem: Some DEV changes not appearing in INTG
Diagnosis:
  - Check timestamp columns are being updated in DEV
  - Verify sync checkpoint timestamps
  - Look for records with NULL timestamps

Solutions:
  - Extend lookback hours: --lookback-hours 48
  - Run manual reconciliation: ./scripts/intg_sync_daemon.sh reconciliation
  - Fix timestamp columns in DEV database
```

### Recovery Procedures

#### Reset Sync Checkpoints
```bash
# Reset all checkpoints to start fresh
python scripts/run_intg.py query --query "
UPDATE intg_sync_checkpoint 
SET last_sync_timestamp = CURRENT_TIMESTAMP - INTERVAL '7 days',
    error_count = 0,
    last_error_message = NULL"

# Reset specific table checkpoint
python scripts/run_intg.py query --query "
UPDATE intg_sync_checkpoint 
SET last_sync_timestamp = CURRENT_TIMESTAMP - INTERVAL '24 hours'
WHERE table_name = 'dev_daily_prices'"
```

#### Force Full Reconciliation
```bash
# Full reconciliation with 30-day lookback
python scripts/intg_incremental_sync.py sync --lookback-hours 720

# Or use reconciliation action
./scripts/intg_sync_daemon.sh reconciliation
```

## 📊 Performance Metrics

### Expected Sync Performance

| Data Type | Typical Volume | Sync Duration | Frequency |
|-----------|----------------|---------------|-----------|
| **Daily Prices** | 10K-50K records/hour | 30-60 seconds | Every 4h |
| **Instruments** | 100-500 records/day | 5-10 seconds | Daily |
| **Fundamentals** | 50-200 records/day | 10-15 seconds | Daily |
| **Full Reconciliation** | 100K-500K records | 5-10 minutes | Weekly |

### Sync Efficiency Metrics
- **Throughput**: 1,000-5,000 records/minute (depending on data complexity)
- **Latency**: 4-25 hours (depending on sync schedule)
- **Accuracy**: >99.9% (with conflict resolution)
- **Availability**: 99.5% (with automatic retry and monitoring)

## 🔄 Integration with Daily Jobs

### Sync Coordination with Data Jobs

The incremental sync system coordinates with the daily refresh jobs to avoid conflicts:

```bash
# Sync sequence coordination
06:00 UTC - Instruments sync (before daily jobs start)
07:00 UTC - Fundamentals sync
08:00 UTC - Daily jobs start (prices, fundamentals, news)
10:00 UTC - Price sync continues during daily jobs
```

### Data Consistency Checks
```bash
# Verify sync consistency with daily jobs
python scripts/run_intg.py query --query "
SELECT 
    'Daily Sync' as source,
    COUNT(*) as records,
    MAX(sync_timestamp) as latest_sync
FROM intg_daily_prices 
WHERE sync_source LIKE 'dev_%'

UNION ALL

SELECT 
    'Daily Jobs' as source,
    COUNT(*) as records,
    MAX(created_at) as latest_sync
FROM intg_daily_prices 
WHERE vendor IN ('polygon', 'fmp', 'tiingo')"
```

## 📞 Support and Maintenance

### Regular Maintenance Tasks

#### Daily
- [ ] Monitor sync logs for errors
- [ ] Check sync lag metrics
- [ ] Verify critical table sync completion

#### Weekly  
- [ ] Review sync performance metrics
- [ ] Run manual reconciliation if needed
- [ ] Clean up old log files
- [ ] Update sync schedules based on data patterns

#### Monthly
- [ ] Optimize database indexes
- [ ] Review and adjust sync strategies
- [ ] Update documentation with lessons learned
- [ ] Plan capacity adjustments

### Escalation Procedures

**Level 1 - Operational Issues**
- Sync lag > 8 hours
- Individual table sync failures
- Performance degradation

**Level 2 - System Issues**
- Multiple table sync failures
- Database connectivity problems
- Data corruption detected

**Level 3 - Critical Issues**
- Complete sync system failure
- Data integrity compromised
- Production impact

---

## 🎯 Quick Reference Commands

```bash
# Setup and Status
python scripts/intg_incremental_sync.py setup
python scripts/intg_incremental_sync.py status

# Manual Sync Operations
python scripts/intg_incremental_sync.py sync
python scripts/intg_incremental_sync.py sync --tables dev_daily_prices --lookback-hours 4
python scripts/intg_incremental_sync.py sync --dry-run

# Daemon Management
./scripts/intg_sync_daemon.sh start
./scripts/intg_sync_daemon.sh status
./scripts/intg_sync_daemon.sh stop

# Cron Installation
./scripts/intg_sync_daemon.sh install-cron

# Manual Operations
./scripts/intg_sync_daemon.sh price-sync
./scripts/intg_sync_daemon.sh comprehensive-sync
./scripts/intg_sync_daemon.sh reconciliation

# Health and Monitoring
./scripts/intg_sync_daemon.sh health-check
tail -f /mnt/d/ats-logs/intg/incremental_sync.log
```

This incremental sync system provides production-ready continuous data synchronization with comprehensive monitoring, conflict resolution, and automated scheduling for the ATS-INTG environment.