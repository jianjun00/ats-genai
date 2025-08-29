# ATS-INTG Data Migration Guide

Complete guide for migrating data from ATS-DEV to ATS-INTG environment with validation, transformation, and monitoring.

## 📋 Overview

This migration process populates the ATS-INTG database with historical data from ATS-DEV to provide a complete dataset for:
- Daily job testing and validation
- Integration testing with real data
- Model training and analytics
- Performance benchmarking

## 🏗️ Migration Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   ATS-DEV DB    │    │   Migration     │    │   ATS-INTG DB   │
│                 │    │   Pipeline      │    │                 │
│ dev_instruments │───▶│ Export          │───▶│ intg_instruments│
│ dev_daily_prices│───▶│ Transform       │───▶│ intg_daily_prices│
│ dev_fundamentals│───▶│ Load            │───▶│ intg_fundamentals│
│ dev_tiingo_*    │───▶│ Merge           │───▶│ (merged tables) │
│ dev_polygon_*   │───▶│ Validate        │───▶│                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Key Features
- **Data Transformation**: Column name standardization and schema mapping
- **Table Merging**: Multiple vendor-specific tables merged into unified structures
- **Conflict Resolution**: ON CONFLICT clauses prevent duplicate data
- **Progress Tracking**: Comprehensive progress monitoring and checkpoints
- **Validation**: Pre and post-migration data integrity checks

## 🚀 Quick Start Migration

### Prerequisites
```bash
# 1. Ensure DEV database is accessible
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_instruments"

# 2. Start INTG PostgreSQL container
docker-compose -f docker-compose.postgres-intg.yml up -d

# 3. Verify INTG database is ready
docker exec postgres-intg pg_isready -U postgres -d intg_db
```

### Simple One-Command Migration
```bash
# Complete migration with all transformations
./scripts/intg_db_migration.sh full

# This performs:
# - Environment validation
# - Data export from DEV
# - Schema transformation
# - Data loading to INTG
# - Migration summary report
```

## 📊 Migration Methods

### Method 1: Direct Database Migration (Recommended)
**Best for: Complete data migration with transformations**

```bash
# Full automated migration
./scripts/intg_db_migration.sh full

# Step-by-step migration
./scripts/intg_db_migration.sh validate    # Check environment
./scripts/intg_db_migration.sh export     # Export DEV data
./scripts/intg_db_migration.sh import     # Import to INTG
```

**Advantages:**
- ✅ Fast and efficient (direct PostgreSQL dump/restore)
- ✅ Automatic schema transformation
- ✅ Handles table merging (multiple dev tables → single intg table)
- ✅ Built-in conflict resolution
- ✅ Comprehensive progress reporting

### Method 2: Application-Level Migration
**Best for: Complex transformations or selective data migration**

```bash
# Validate environment
python scripts/intg_data_backfill.py validate

# Run selective table migration
python scripts/intg_data_backfill.py backfill --tables dev_instruments dev_daily_prices

# Full application-level migration
python scripts/intg_data_backfill.py backfill
```

**Advantages:**
- ✅ Granular control over transformations
- ✅ Selective table migration
- ✅ Advanced data validation
- ✅ Resume capability for failed migrations
- ✅ Custom transformation logic

## 🔄 Data Transformation Rules

### Table Mapping Strategy

| DEV Tables | INTG Target | Strategy |
|------------|-------------|-----------|
| `dev_instruments` | `intg_instruments` | **1:1 Direct mapping** |
| `dev_fundamentals_comprehensive` | `intg_fundamentals_comprehensive` | **1:1 Direct mapping** |
| `dev_daily_prices` | `intg_daily_prices` | **1:1 with vendor addition** |
| `dev_tiingo_daily_prices` | `intg_daily_prices` | **N:1 Merge with vendor='tiingo'** |
| `dev_polygon_daily_prices` | `intg_daily_prices` | **N:1 Merge with vendor='polygon'** |
| `dev_fmp_daily_prices` | `intg_daily_prices` | **N:1 Merge with vendor='fmp'** |

### Column Transformations

```sql
-- Column name standardization
creation_timestamp → created_at
last_updated → updated_at

-- Vendor identification for merged tables
-- Automatic vendor detection based on source table
dev_tiingo_* → vendor = 'tiingo'
dev_polygon_* → vendor = 'polygon' 
dev_fmp_* → vendor = 'fmp'
dev_daily_prices → vendor = 'dev_migration'
```

### Conflict Resolution

```sql
-- Instruments: Prevent symbol duplicates
ON CONFLICT (symbol) DO NOTHING

-- Daily Prices: Prevent symbol/date/vendor duplicates  
ON CONFLICT (symbol, date, vendor) DO NOTHING

-- Fundamentals: Prevent symbol/date/vendor/period duplicates
ON CONFLICT (symbol, date, vendor, fiscal_period) DO NOTHING
```

## 📈 Migration Monitoring

### Real-Time Progress Tracking

```bash
# Monitor migration progress
tail -f /mnt/d/ats-logs/intg/migration.log

# Check table counts during migration
docker exec postgres-intg psql -U postgres -d intg_db -c "
SELECT 
    'intg_instruments' as table_name, COUNT(*) as records
FROM intg_instruments
UNION ALL
SELECT 
    'intg_daily_prices' as table_name, COUNT(*) as records  
FROM intg_daily_prices
UNION ALL
SELECT
    'intg_fundamentals' as table_name, COUNT(*) as records
FROM intg_fundamentals_comprehensive"
```

### Migration Status Dashboard

```bash
# Get detailed migration status
python scripts/intg_data_backfill.py status

# Sample output:
# Backfill Status:
# source_table        | target_table           | records_processed | status
# dev_instruments     | intg_instruments       | 12,450           | completed
# dev_daily_prices    | intg_daily_prices      | 2,340,000        | running  
# dev_fundamentals    | intg_fundamentals      | 45,000           | pending
```

## 🔍 Data Validation

### Pre-Migration Validation

```bash
# Validate source data quality
python scripts/run_dev.py query --query "
SELECT 
    COUNT(*) as total_instruments,
    COUNT(DISTINCT symbol) as unique_symbols,
    MIN(created_at) as earliest_record,
    MAX(created_at) as latest_record
FROM dev_instruments"

# Check for data anomalies
python scripts/run_dev.py query --query "
SELECT 
    COUNT(*) as total_prices,
    COUNT(DISTINCT symbol) as symbols_with_prices,
    MIN(date) as earliest_date,
    MAX(date) as latest_date
FROM dev_daily_prices"
```

### Post-Migration Validation

```bash
# Compare record counts DEV vs INTG
python scripts/intg_data_backfill.py validate

# Data integrity checks
docker exec postgres-intg psql -U postgres -d intg_db -c "
-- Check for data completeness
SELECT 
    vendor,
    COUNT(*) as records,
    COUNT(DISTINCT symbol) as symbols,
    MIN(date) as earliest,
    MAX(date) as latest
FROM intg_daily_prices 
GROUP BY vendor
ORDER BY vendor"

# Verify merged data integrity
docker exec postgres-intg psql -U postgres -d intg_db -c "
-- Ensure no duplicate prices for same symbol/date
SELECT 
    symbol, date, COUNT(*) as duplicate_count
FROM intg_daily_prices 
GROUP BY symbol, date
HAVING COUNT(*) > 1
LIMIT 10"
```

## 🛠️ Troubleshooting

### Common Issues and Solutions

#### 1. Connection Issues
```bash
# DEV database not accessible
Problem: Cannot connect to DEV database
Solution: 
  - Check if DEV environment is running
  - Verify database credentials in scripts
  - Test: python scripts/run_dev.py query --query "SELECT 1"

# INTG container not running  
Problem: INTG PostgreSQL container not accessible
Solution:
  - Start container: docker-compose -f docker-compose.postgres-intg.yml up -d
  - Wait for readiness: docker exec postgres-intg pg_isready -U postgres
```

#### 2. Schema Mismatches
```bash
Problem: Column 'xyz' does not exist in target table
Solution:
  - Check table schemas: 
    python scripts/run_dev.py query --query "\d dev_table_name"
    docker exec postgres-intg psql -U postgres -d intg_db -c "\d intg_table_name"
  - Update transformation rules in migration scripts
  - Re-run migration with --dry-run first
```

#### 3. Data Conflicts
```bash
Problem: Duplicate key errors during import
Solution:
  - ON CONFLICT clauses handle most duplicates automatically
  - For persistent issues, clean target table:
    docker exec postgres-intg psql -U postgres -d intg_db -c "TRUNCATE intg_daily_prices"
  - Re-run migration
```

#### 4. Performance Issues
```bash
Problem: Migration running very slowly
Solution:
  - Reduce batch size: --batch-size 500
  - Use direct DB method instead of application-level
  - Monitor disk space: df -h /mnt/d/ats-data/intg/
  - Check PostgreSQL logs: docker logs postgres-intg
```

### Recovery Procedures

#### Partial Migration Failure
```bash
# Resume incomplete migration
python scripts/intg_data_backfill.py resume

# Or restart specific tables
python scripts/intg_data_backfill.py backfill --tables dev_daily_prices
```

#### Complete Migration Reset
```bash
# 1. Stop INTG services
docker-compose -f docker-compose.intg-jobs.yml down

# 2. Clear INTG data directory
sudo rm -rf /mnt/d/ats-data/intg/postgresql/*

# 3. Restart PostgreSQL container
docker-compose -f docker-compose.postgres-intg.yml up -d

# 4. Wait for initialization
sleep 30

# 5. Re-run migration
./scripts/intg_db_migration.sh full
```

## 📋 Migration Checklist

### Pre-Migration
- [ ] DEV database is accessible and stable
- [ ] INTG PostgreSQL container is running
- [ ] Sufficient disk space available (estimate 2-3x source data size)
- [ ] Migration directories exist with proper permissions
- [ ] Database schemas are compatible

### During Migration  
- [ ] Monitor progress logs: `tail -f /mnt/d/ats-logs/intg/migration.log`
- [ ] Check resource utilization: `docker stats postgres-intg`
- [ ] Validate incremental progress with status commands
- [ ] Address any error messages immediately

### Post-Migration
- [ ] Validate record counts match expected values
- [ ] Verify data integrity with sample queries
- [ ] Test daily job functionality with migrated data
- [ ] Update documentation with migration results
- [ ] Archive migration artifacts for future reference

## 🔄 Incremental Updates

### Daily Sync from DEV to INTG

After initial migration, set up daily incremental updates:

```bash
# Create incremental sync job
cat > /tmp/daily_dev_to_intg_sync.sh << 'EOF'
#!/bin/bash
# Daily incremental sync DEV → INTG

YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)

# Sync new daily prices  
python scripts/run_dev.py query --query "
INSERT INTO intg_daily_prices 
SELECT *, 'dev_sync' as vendor, CURRENT_TIMESTAMP as created_at
FROM dev_daily_prices 
WHERE date = '$YESTERDAY'
ON CONFLICT (symbol, date, vendor) DO NOTHING"

# Sync new instruments
python scripts/run_dev.py query --query "
INSERT INTO intg_instruments
SELECT *, CURRENT_TIMESTAMP as created_at  
FROM dev_instruments
WHERE created_at::date = '$YESTERDAY'
ON CONFLICT (symbol) DO NOTHING"

echo "Daily sync completed for $YESTERDAY"
EOF

chmod +x /tmp/daily_dev_to_intg_sync.sh

# Schedule daily at 7 AM UTC
echo "0 7 * * * /tmp/daily_dev_to_intg_sync.sh >> /mnt/d/ats-logs/intg/daily_sync.log 2>&1" | crontab -
```

## 📊 Performance Optimization

### Migration Performance Tips

```bash
# 1. Use direct PostgreSQL migration for large datasets
./scripts/intg_db_migration.sh full  # Faster than application-level

# 2. Optimize PostgreSQL settings temporarily during migration
docker exec postgres-intg psql -U postgres -d intg_db -c "
-- Temporarily disable fsync for faster bulk loading
SET fsync = off;
SET synchronous_commit = off;
SET checkpoint_segments = 32;
SET checkpoint_completion_target = 0.9;
SET wal_buffers = 16MB;"

# 3. Create indexes AFTER bulk loading
docker exec postgres-intg psql -U postgres -d intg_db -c "
-- Drop indexes before bulk loading
DROP INDEX IF EXISTS idx_intg_daily_prices_symbol_date;
-- Recreate after migration
CREATE INDEX idx_intg_daily_prices_symbol_date ON intg_daily_prices(symbol, date DESC);"
```

## 📞 Support and Escalation

### Migration Support Process

1. **Self-Service**: Use validation and status commands
2. **Documentation**: Review troubleshooting section
3. **Logs Review**: Check migration logs and database logs
4. **Team Escalation**: Contact development team with:
   - Migration method used
   - Error messages and logs
   - Current migration status
   - Data volume estimates

### Success Criteria

✅ **Migration Successful When:**
- All target tables populated with expected record counts
- Data integrity validation passes
- No orphaned or corrupted records
- Daily jobs can process migrated data successfully
- Performance meets acceptable thresholds

---

## 🎯 Quick Reference Commands

```bash
# Complete automated migration
./scripts/intg_db_migration.sh full

# Validate environment before migration
./scripts/intg_db_migration.sh validate

# Application-level migration with progress tracking
python scripts/intg_data_backfill.py backfill

# Check migration status
python scripts/intg_data_backfill.py status

# Validate results
python scripts/intg_data_backfill.py validate

# Resume failed migration
python scripts/intg_data_backfill.py resume

# Monitor PostgreSQL
docker logs postgres-intg -f
docker exec postgres-intg psql -U postgres -d intg_db
```

This migration strategy ensures reliable, monitored data migration with comprehensive validation and rollback capabilities for populating ATS-INTG from ATS-DEV.