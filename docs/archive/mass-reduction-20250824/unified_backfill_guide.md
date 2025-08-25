# Unified 5-Year Multi-Vendor Backfill Guide

This guide explains how to use the unified backfill system to fetch 1-minute data from both Polygon and Tiingo for the past 5 years, with cross-vendor reconciliation to reduce data errors.

## System Overview

The unified backfill system consists of:

1. **Multi-Vendor Data Adapters**: Polygon and Tiingo APIs for 1-minute data
2. **Cross-Vendor Reconciliation**: Combines data from multiple sources to reduce errors
3. **Hybrid Storage Manager**: Automatically manages hot (database) and cold (disk) storage
4. **Progress Tracking**: Checkpointing and recovery for large backfill operations

## Prerequisites

### Environment Variables

Set the following environment variables:

```bash
# API Keys
export POLYGON_API_KEY="your_polygon_api_key"
export TIINGO_API_KEY="your_tiingo_api_key"

# Database Connection
export DB_HOST="localhost"
export DB_PORT="5433"
export DB_USER="postgres"
export DB_PASSWORD="postgres"
export DB_NAME="dev_db"

# Optional: Set Python path
export PYTHONPATH="src"
```

### Directory Structure

The system will create the following storage structure:

```
/home/jianjun/ats/data/STK/1min/
├── hot/           # Recent data (database backup)
├── warm/          # 30-90 day data (uncompressed Parquet)
├── cold/          # >90 day data (compressed Parquet)
└── archive/       # Long-term archival
```

## Quick Start

### 1. Test System Configuration

First, validate your setup:

```bash
cd /home/jianjun/ats-genai
python scripts/backfill/test_unified_backfill.py
```

This will test:
- API connections to Polygon and Tiingo
- Database connectivity
- Data reconciliation logic
- Storage system setup

### 2. Run Sample Backfill

Start with a small test:

```bash
# Sample: 5 symbols, 30 days
python scripts/backfill/run_unified_5year_backfill.py --mode sample
```

### 3. Run Custom Backfill

For specific symbols:

```bash
# Custom symbols
python scripts/backfill/run_unified_5year_backfill.py \
    --mode custom \
    --symbols "AAPL,MSFT,GOOGL,AMZN,TSLA" \
    --chunk-days 7 \
    --batch-size 5
```

### 4. Run Full S&P 500 Backfill

For production backfill:

```bash
# Full S&P 500 (60+ symbols, 5 years)
python scripts/backfill/run_unified_5year_backfill.py \
    --mode full \
    --limit 50 \
    --chunk-days 7 \
    --max-concurrent 3
```

## Configuration Options

### Backfill Modes

- `sample`: Test with 5 symbols, 30 days
- `custom`: User-specified symbols
- `full`: S&P 500 companies  
- `resume`: Continue interrupted backfill

### Performance Tuning

```bash
--chunk-days 7          # Days per processing chunk (7-30)
--batch-size 10         # Symbols per batch (5-20)
--max-concurrent 3      # Concurrent symbol processing (1-5)
```

### Data Quality Settings

The system uses configurable reconciliation methods:

- `weighted_average`: Combines data with vendor-specific weights (default)
- `polygon_priority`: Prefers Polygon data when available
- `tiingo_priority`: Prefers Tiingo data when available
- `best_quality`: Uses highest quality data per timestamp
- `conservative`: Uses most conservative values

## Storage Tiers

### Hot Storage (Database)
- **Duration**: Last 30 days
- **Purpose**: Fast queries and real-time analysis
- **Format**: PostgreSQL/TimescaleDB tables

### Warm Storage (Disk)
- **Duration**: 30-90 days
- **Purpose**: Recent historical analysis
- **Format**: Uncompressed Parquet files

### Cold Storage (Disk)
- **Duration**: >90 days
- **Purpose**: Long-term storage and backfill
- **Format**: Compressed Parquet files (Snappy)

## Data Reconciliation

### Reconciliation Process

1. **Timeline Alignment**: Creates unified timeline from both vendors
2. **Quality Scoring**: Evaluates data quality per vendor and timestamp
3. **Variance Detection**: Identifies outliers and conflicts
4. **Method Application**: Applies configured reconciliation method
5. **Gap Filling**: Interpolates small data gaps (<2 minutes)
6. **Anomaly Flagging**: Marks potential data quality issues

### Quality Metrics

- **Price Variance**: Standard deviation of prices across vendors
- **Volume Variance**: Standard deviation of volume across vendors  
- **Data Completeness**: Percentage of expected data points received
- **Vendor Coverage**: Percentage of data from each vendor

## Progress Tracking

### Checkpointing

The system automatically saves progress to:
```
/home/jianjun/ats/data/STK/1min/unified_backfill_checkpoint.json
```

### Resume Interrupted Backfill

```bash
python scripts/backfill/run_unified_5year_backfill.py --mode resume
```

### Monitor Progress

Check logs:
```bash
tail -f /tmp/unified_backfill.log
```

View checkpoint:
```bash
cat /home/jianjun/ats/data/STK/1min/unified_backfill_checkpoint.json | jq .
```

## Troubleshooting

### Common Issues

#### API Rate Limits
- Polygon: 5 calls/minute (free), 200 calls/minute (premium)
- Tiingo: 500 calls/day (free), 50,000 calls/day (premium)

**Solution**: Reduce `--max-concurrent` and increase `--chunk-days`

#### Database Connection Issues
```bash
# Test database connectivity
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT version();"
```

#### Storage Space Issues
- Monitor disk usage: `df -h /home/jianjun/ats/data/`
- 5 years of 1-minute data for 50 symbols ≈ 50-100GB

#### Memory Issues
- Reduce `batch_size` in configuration
- Lower `max_concurrent_symbols`

### Validation Queries

Check stored data:

```sql
-- Count records by vendor
SELECT vendor, COUNT(*) 
FROM dev_minute_bars 
GROUP BY vendor;

-- Check data coverage
SELECT 
    symbol,
    MIN(timestamp) as first_bar,
    MAX(timestamp) as last_bar,
    COUNT(*) as total_bars
FROM dev_minute_bars 
GROUP BY symbol 
ORDER BY symbol;

-- Quality metrics
SELECT 
    symbol,
    AVG(quality_score) as avg_quality,
    COUNT(CASE WHEN vendor = 'unified' THEN 1 END) as reconciled_bars
FROM dev_minute_bars 
WHERE timestamp >= '2019-01-01'
GROUP BY symbol;
```

## Performance Expectations

### Typical Performance
- **Sample Backfill**: 5 symbols, 30 days ≈ 5-10 minutes
- **Custom Backfill**: 10 symbols, 1 year ≈ 2-4 hours  
- **Full S&P 500**: 50 symbols, 5 years ≈ 24-48 hours

### Factors Affecting Speed
- API rate limits (primary constraint)
- Network connectivity
- Database write speed
- Disk I/O for file storage
- Number of concurrent processes

## Best Practices

### Production Deployment

1. **Run during off-hours** to avoid market data conflicts
2. **Use premium API accounts** for higher rate limits
3. **Monitor disk space** throughout the process
4. **Set up log rotation** for long-running jobs
5. **Test with sample data** before full backfill

### Data Quality

1. **Validate results** using provided SQL queries
2. **Compare vendor coverage** to identify data gaps
3. **Review anomaly flags** in reconciled data
4. **Archive raw vendor data** for future reprocessing

### Recovery Planning

1. **Save checkpoint files** to persistent storage
2. **Document symbol lists** used for backfill
3. **Keep API key backups** in secure storage
4. **Test recovery procedures** with sample data

## Next Steps

After successful backfill:

1. **Set up incremental updates** for ongoing data collection
2. **Configure data validation** and quality monitoring
3. **Implement alert systems** for data gaps or quality issues
4. **Optimize query performance** with appropriate indexes
5. **Plan storage archival** for older data

## Support

For issues or questions:

1. Check logs: `/tmp/unified_backfill.log`
2. Review test results: `scripts/backfill/test_unified_backfill.py`
3. Validate environment variables and database connectivity
4. Monitor API usage and rate limits