# Polygon 30-Year Minute Bar Population System

A comprehensive system for populating 30 years of 1-minute OHLCV data from Polygon API to local file storage on D: drive. Built on existing ATS platform infrastructure for maximum compatibility and performance.

## 🌟 Key Features

- **Enterprise-Scale Processing**: Handles 30 years × 8000+ symbols of minute data
- **Polygon API Optimized**: Uses existing `PolygonMinuteAdapter` with rate limit management
- **Intelligent Rate Limiting**: Automatic detection and handling of free vs premium plans
- **Checkpoint-Based Resume**: Resume processing from any interruption point
- **Quality Validation**: Built-in data quality scoring with technical indicators
- **File-Based Storage**: Monthly Parquet files with Snappy compression
- **D: Drive Optimized**: Designed for Windows/WSL D: drive high-capacity storage
- **Robust Error Recovery**: Comprehensive error handling with detailed logging

## 📁 System Files

| File | Purpose |
|------|---------|
| `populate_30year_polygon_minute_bars.py` | Main population script |
| `setup_polygon_d_drive_storage.py` | D: drive setup with Polygon-specific validation |
| `test_polygon_population.py` | Comprehensive test suite |
| `README_POLYGON_30YEAR_POPULATION.md` | This documentation |

## 🚀 Quick Start Guide

### 1. Prerequisites Setup

```bash
# Set Polygon API key (required)
export POLYGON_API_KEY='your-polygon-api-key-here'

# Ensure D: drive access (WSL/Windows)
ls /mnt/d

# Verify Python environment with required packages
python -c "import aiohttp, aiofiles, pandas, pyarrow"
```

### 2. System Setup and Validation

```bash
# Setup D: drive with Polygon-specific configuration
python scripts/setup_polygon_d_drive_storage.py

# This will:
# - Validate Polygon API access and detect plan type
# - Check D: drive capacity (estimates ~1TB needed)
# - Create Polygon directory structure
# - Generate optimized configuration
# - Provide time estimates based on your plan
```

### 3. Test Suite Validation

```bash
# Run comprehensive validation tests
python scripts/test_polygon_population.py

# Tests include:
# - Polygon API connectivity
# - Rate limiting behavior
# - File storage operations
# - Checkpoint functionality
# - Sample data population
```

### 4. Population Execution

```bash
# Debug Mode (5 symbols, 1 year) - Start here!
python scripts/populate_30year_polygon_minute_bars.py \
  --debug \
  --limit 5 \
  --start-date 2023-01-01 \
  --end-date 2024-01-01

# Production Mode - Free Tier
python scripts/populate_30year_polygon_minute_bars.py \
  --mode full \
  --concurrent 1 \
  --storage-path /mnt/d/ats-data

# Production Mode - Premium Tier
python scripts/populate_30year_polygon_minute_bars.py \
  --mode full \
  --premium \
  --concurrent 3 \
  --storage-path /mnt/d/ats-data

# Resume from interruption
python scripts/populate_30year_polygon_minute_bars.py --resume
```

## 📊 Polygon-Specific Considerations

### API Plan Comparison

| Feature | Free Tier | Premium Tier |
|---------|-----------|--------------|
| **Rate Limit** | 5 requests/minute | 100+ requests/minute |
| **Concurrent Requests** | 1 | 2-5 |
| **Processing Time** | ~2-3 years | ~1-3 months |
| **Cost** | Free | $99+/month |
| **Data History** | 2 years | 30+ years |

### Storage Requirements

- **Symbol Count**: ~8,000 (extensive US market coverage)
- **Data Richness**: 60 bytes per bar (includes VWAP, trade count)
- **Estimated Size**: ~1TB for full 30-year dataset
- **Recommended Space**: 1.5TB free on D: drive

### Rate Limiting Strategy

The system automatically adapts to your Polygon plan:

```python
# Free Tier (Conservative)
requests_per_minute = 5
delay_between_requests = 12.0  # seconds

# Premium Tier (Optimized)
requests_per_minute = 100
delay_between_requests = 0.6  # seconds
```

## ⚙️ Command Line Interface

### Complete Option Reference

```bash
python scripts/populate_30year_polygon_minute_bars.py [OPTIONS]
```

| Option | Description | Default | Example |
|--------|-------------|---------|---------|
| `--mode` | Population mode | `full` | `--mode incremental` |
| `--start-date` | Start date (YYYY-MM-DD) | 30 years ago | `--start-date 2020-01-01` |
| `--end-date` | End date (YYYY-MM-DD) | Today | `--end-date 2024-12-31` |
| `--symbols` | Specific symbol list | All available | `--symbols AAPL,MSFT,GOOGL` |
| `--limit` | Max symbols to process | None | `--limit 100` |
| `--storage-path` | D: drive storage path | `/mnt/d/ats-data` | `--storage-path /mnt/d/market-data` |
| `--checkpoint-file` | Checkpoint file path | Auto-generated | `--checkpoint-file my_checkpoint.json` |
| `--resume` | Resume from checkpoint | False | `--resume` |
| `--debug` | Debug mode (limited data) | False | `--debug` |
| `--concurrent` | Concurrent operations | 2 | `--concurrent 1` |
| `--premium` | Use premium rate limits | False | `--premium` |

### Usage Examples by Scenario

#### Development and Testing
```bash
# Small test (5 symbols, 1 week)
python scripts/populate_30year_polygon_minute_bars.py \
  --debug --limit 5 \
  --start-date 2024-01-01 --end-date 2024-01-07

# Major stocks only (last 5 years)
python scripts/populate_30year_polygon_minute_bars.py \
  --symbols AAPL,MSFT,GOOGL,AMZN,TSLA,META,NVDA \
  --start-date 2019-01-01
```

#### Production Deployment
```bash
# Full historical backfill (free tier)
python scripts/populate_30year_polygon_minute_bars.py \
  --mode full \
  --concurrent 1 \
  --storage-path /mnt/d/ats-data/minute-bars

# Full historical backfill (premium tier)
python scripts/populate_30year_polygon_minute_bars.py \
  --mode full \
  --premium \
  --concurrent 3 \
  --storage-path /mnt/d/ats-data/minute-bars

# Priority symbols first (reduce risk)
python scripts/populate_30year_polygon_minute_bars.py \
  --symbols $(cat priority_symbols.txt | tr '\n' ',') \
  --start-date 1994-01-01
```

#### Resumable Processing
```bash
# Resume after interruption
python scripts/populate_30year_polygon_minute_bars.py \
  --resume \
  --checkpoint-file polygon_30year_checkpoint.json

# Resume with different settings
python scripts/populate_30year_polygon_minute_bars.py \
  --resume \
  --premium \
  --concurrent 5
```

## 📈 Progress Monitoring and Statistics

### Real-Time Progress Tracking

```
2024-01-15 10:30:00 - INFO - Starting AAPL population: 1994-01-01 to 2024-01-01
2024-01-15 10:31:45 - INFO - AAPL: Fetching 2020-01-01 to 2020-03-30
2024-01-15 10:32:10 - INFO - AAPL: Got 24,570 bars for chunk 2020-01-01 to 2020-03-30
2024-01-15 10:45:20 - INFO - AAPL: 2,847,650 bars collected, 2,847,650 stored, 12 files created, quality: 0.95
2024-01-15 10:45:20 - INFO - Progress: 1250/8000 (15.6%) - Current: MSFT
2024-01-15 10:45:20 - INFO - Quality: 0.95, API calls: 1,234,567, Estimated remaining: 45.3h
```

### Comprehensive Final Statistics

```
===============================================================================
POLYGON 30-YEAR POPULATION FINAL STATISTICS
===============================================================================
Symbols processed: 8000
Symbols completed: 7950
Symbols failed: 50
Total bars collected: 23,542,350,000
Total bars stored: 23,542,350,000
Total files created: 96,000
Total API calls: 67,680,000
Rate limit delays: 1,250
Average quality score: 0.943

Data quality summary:
  - Min quality: 0.654
  - Max quality: 1.000
  - Avg quality: 0.943

Storage statistics:
  - Total files: 96,000
  - Total symbols: 8,000
  - Total size: 987.5 GB
===============================================================================
```

## 🔄 Checkpoint System Deep Dive

The checkpoint system enables resilient processing across months of execution:

### Checkpoint Structure
```json
{
  "start_date": "1994-01-01",
  "end_date": "2024-01-01",
  "total_symbols": 8000,
  "processed_symbols": 3450,
  "current_symbol": "MSFT",
  "symbols_completed": ["AAPL", "GOOGL", "AMZN", ...],
  "symbols_failed": ["BADSTOCK", "DELISTED"],
  "total_bars_stored": 12500000000,
  "total_files_created": 41400,
  "total_api_calls": 29875000,
  "rate_limit_delays": 450,
  "quality_scores": {
    "AAPL": 0.95,
    "GOOGL": 0.93,
    "AMZN": 0.97
  },
  "last_update_timestamp": "2024-01-15T10:30:00",
  "errors": [],
  "processing_stats": {}
}
```

### Resume Capabilities
- **Symbol-level granularity**: Resume from any symbol
- **Progress preservation**: Maintains all statistics and quality scores
- **Error tracking**: Tracks failed symbols for retry
- **Time estimation**: Provides accurate remaining time estimates

## 🗂️ File Organization and Storage

### Directory Structure
```
/mnt/d/ats-data/
├── minute-bars/
│   ├── polygon/              # Polygon-specific data
│   │   ├── AAPL/
│   │   │   ├── 1994-01.parquet
│   │   │   ├── 1994-02.parquet
│   │   │   └── ... (360 monthly files)
│   │   ├── MSFT/
│   │   └── ... (8000 symbol directories)
│   ├── backups/              # Automatic backups during updates
│   ├── metadata/             # File checksums and metadata
│   └── quality-reports/      # Data quality validation reports
├── checkpoints/
│   └── polygon/              # Polygon checkpoint files
├── logs/
│   └── polygon/              # Detailed processing logs
└── reports/
    └── polygon/              # Summary reports and statistics
```

### File Format Details
- **Format**: Parquet with Snappy compression
- **Partitioning**: Monthly files per symbol
- **Columns**: timestamp, open, high, low, close, volume, vwap, trade_count, vendor, quality_score
- **Indexing**: Timestamp-based indexing for fast queries
- **Compression**: ~60% size reduction with Snappy

## ⚡ Performance Optimization

### Recommended System Configuration

#### Hardware Requirements
```bash
# Minimum System Specs
CPU: 4+ cores (I/O intensive, not CPU bound)
RAM: 8GB (4GB during processing + 4GB system)
Storage: 1.5TB free on D: drive (fast SSD recommended)
Network: Stable internet (continuous API calls)

# Optimal System Specs
CPU: 8+ cores
RAM: 16GB
Storage: 2TB+ NVMe SSD
Network: High-speed broadband with stable connection
```

#### Configuration by Plan Type
```bash
# Free Tier Optimization (5 req/min)
--concurrent 1 --premium false
# Expected time: 2-3 years continuous
# Cost: Free

# Premium Tier Optimization (100 req/min)
--concurrent 3 --premium true
# Expected time: 1-3 months
# Cost: $99+/month during population
```

### Memory and CPU Usage
- **Memory**: ~500MB base + ~100MB per concurrent operation
- **CPU**: Light usage, mostly I/O and network bound
- **Disk I/O**: Write-intensive, benefits from SSD
- **Network**: Sustained 1-5 Mbps API traffic

## 🛠️ Troubleshooting Guide

### Common Issues and Solutions

#### API Key and Authentication
```bash
# Issue: API key not recognized
❌ Error: "Please set your POLYGON_API_KEY environment variable"

# Solution: Set API key properly
export POLYGON_API_KEY='your-key-here'
echo $POLYGON_API_KEY  # Verify it's set

# Test API access
curl "https://api.polygon.io/v3/reference/tickers?active=true&limit=1&apikey=$POLYGON_API_KEY"
```

#### Rate Limiting Problems
```bash
# Issue: Frequent rate limit errors
❌ Error: "Rate limit exceeded for AAPL, retrying..."

# Solution 1: Use conservative settings
--concurrent 1 --premium false

# Solution 2: Upgrade to premium plan
--premium --concurrent 3

# Solution 3: Check your actual plan limits
python scripts/setup_polygon_d_drive_storage.py
```

#### Storage Space Issues
```bash
# Issue: Disk space full during processing
❌ Error: "No space left on device"

# Check current usage
df -h /mnt/d

# Clean up old backups
find /mnt/d/ats-data -name "*.backup" -mtime +7 -delete

# Resume with smaller batches
--symbols AAPL,MSFT,GOOGL --limit 100
```

#### Network and Connectivity
```bash
# Issue: Network timeouts or API errors
❌ Error: "Error fetching minute bars for AAPL: timeout"

# Solution 1: Check internet connection
ping api.polygon.io

# Solution 2: Resume from checkpoint
--resume --checkpoint-file polygon_30year_checkpoint.json

# Solution 3: Use longer delays
# Edit the script to increase delay_between_requests
```

#### Data Quality Issues
```bash
# Issue: Low quality scores or gaps in data
⚠️ Warning: "Quality score: 0.65 for SOMESTOCK"

# Check quality report
cat /mnt/d/ats-data/minute-bars/quality-reports/SOMESTOCK_quality.json

# Manual investigation
python scripts/test_polygon_population.py

# Skip problematic symbols
--symbols $(grep -v BADSTOCK symbols_list.txt | tr '\n' ',')
```

### Error Recovery Procedures

#### 1. Process Interruption Recovery
```bash
# Always try to resume first
python scripts/populate_30year_polygon_minute_bars.py --resume

# If checkpoint is corrupted, start fresh with processed symbols excluded
python scripts/populate_30year_polygon_minute_bars.py \
  --symbols $(comm -23 all_symbols.txt completed_symbols.txt | tr '\n' ',')
```

#### 2. Data Corruption Recovery
```bash
# Check file integrity
python -c "
import pandas as pd
df = pd.read_parquet('/mnt/d/ats-data/minute-bars/AAPL/2020-01.parquet')
print(f'File OK: {len(df)} records')
"

# Restore from backup if needed
cp /mnt/d/ats-data/minute-bars/backups/AAPL_2020-01.parquet.backup \
   /mnt/d/ats-data/minute-bars/AAPL/2020-01.parquet
```

## 🔍 Data Quality and Validation

### Built-in Quality Metrics

The system validates data quality using multiple criteria:

```python
quality_metrics = {
    "valid": True/False,              # Overall validity
    "total_bars": 24570,              # Total bars processed
    "time_gaps": 12,                  # Missing time periods
    "price_outliers": 3,              # Suspicious price moves
    "zero_volume_bars": 45,           # Bars with no volume
    "avg_volume": 1234567,            # Average volume per bar
    "data_completeness": 0.95         # Percentage completeness
}
```

### Quality Score Calculation
- **Perfect data (1.0)**: No gaps, no outliers, consistent volume
- **Good data (0.8-0.99)**: Minor gaps or occasional outliers
- **Fair data (0.6-0.79)**: Moderate gaps, some data quality issues
- **Poor data (<0.6)**: Significant gaps, major quality problems

### Technical Indicators Validation
The system also calculates technical indicators for additional validation:
- Moving averages (SMA 5, 20; EMA 12, 26)
- MACD and signal line
- RSI (14-period)
- Bollinger Bands
- Volume indicators
- Volatility measures

## 🤝 Integration with ATS Platform

### Compatibility with Existing Systems

The populated data integrates seamlessly with existing ATS infrastructure:

```python
# Query populated data using existing FileBasedMinuteManager
from storage.file_based_minute_manager import FileBasedMinuteManager

manager = FileBasedMinuteManager(base_path="/mnt/d/ats-data/minute-bars")

# Query 30 years of AAPL data
historical_data = await manager.query_minute_data(
    'AAPL',
    datetime(1994, 1, 1),
    datetime(2024, 1, 1)
)

print(f"Retrieved {len(historical_data):,} minute bars spanning 30 years")
```

### Model Training Integration
```python
# Use with TFT models
from models.temporal_fusion_transformer import TemporalFusionTransformer

# Load data for model training
training_data = await manager.query_minute_data(
    'AAPL',
    datetime(2020, 1, 1),
    datetime(2023, 1, 1)
)

# Train TFT model with 30-year historical context
model = TemporalFusionTransformer(...)
model.train(training_data)
```

## 📞 Monitoring and Maintenance

### Automated Monitoring
```bash
# Monitor progress with custom scripts
watch -n 60 'tail -20 /mnt/d/ats-data/logs/polygon/population.log'

# Check storage usage
watch -n 300 'df -h /mnt/d'

# Monitor API usage
watch -n 60 'grep "API calls:" /mnt/d/ats-data/logs/polygon/population.log | tail -1'
```

### Maintenance Tasks
```bash
# Weekly: Clean old backups
find /mnt/d/ats-data -name "*.backup" -mtime +30 -delete

# Monthly: Verify data integrity
python -c "
import asyncio
from populate_30year_polygon_minute_bars import Polygon30YearPopulator
async def check():
    p = Polygon30YearPopulator()
    await p.file_manager.verify_data_integrity()
asyncio.run(check())
"

# Quarterly: Update symbol universe
python scripts/populate_30year_polygon_minute_bars.py \
  --mode incremental \
  --start-date $(date -d '3 months ago' +%Y-%m-%d)
```

## 🎯 Success Metrics and Validation

### Key Performance Indicators
- **Data Coverage**: >95% of trading minutes captured
- **Quality Score**: Average >0.90 across all symbols
- **API Efficiency**: <2% rate limit delays
- **Storage Efficiency**: <1TB total with compression
- **Processing Speed**: Matches plan-based estimates

### Validation Checklist
- [ ] All priority symbols (S&P 500) have complete data
- [ ] No gaps >1 hour in major symbols during trading hours
- [ ] Quality scores >0.8 for >90% of symbols
- [ ] File integrity verified with checksums
- [ ] Sample queries return expected data ranges
- [ ] Integration tests pass with existing ATS components

---

## 🚀 Ready for Production

This system provides enterprise-grade 30-year market data population with:
- **Proven Architecture**: Built on existing ATS infrastructure
- **Production Ready**: Comprehensive error handling and recovery
- **Scalable Design**: Handles massive datasets efficiently
- **Quality Assured**: Multiple validation layers
- **Maintainable**: Clear documentation and monitoring

**Start with the debug mode, validate with tests, then scale to full production!**