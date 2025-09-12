# EODHD 30-Year Minute Bar Population System

A comprehensive system for populating 30 years of 1-minute OHLCV data from EODHD API to local file storage on D: drive.

## 🌟 Features

- **Massive Scale Processing**: Handles 30 years × 3000+ symbols of minute data
- **Checkpoint-Based Resumable**: Resume processing from any point if interrupted
- **File-Based Storage**: Monthly Parquet files with Snappy compression
- **Rate Limit Compliant**: Respects EODHD API rate limits (3 seconds between calls)
- **Quality Validation**: Built-in data quality scoring and gap detection
- **D: Drive Optimized**: Designed for Windows/WSL D: drive storage
- **Progress Tracking**: Real-time progress reporting and statistics
- **Error Recovery**: Robust error handling with detailed logging

## 📁 Files Overview

| File | Purpose |
|------|---------|
| `populate_30year_eodhd_minute_bars.py` | Main population script |
| `setup_d_drive_storage.py` | D: drive setup and validation |
| `test_eodhd_population.py` | Test suite for validation |
| `README_EODHD_30YEAR_POPULATION.md` | This documentation |

## 🚀 Quick Start

### 1. Prerequisites

```bash
# Set EODHD API key
export EODHD_API_KEY='your-eodhd-api-key-here'

# Ensure D: drive is accessible (WSL)
ls /mnt/d

# Install required Python packages (if not already installed)
pip install aiohttp aiofiles pandas pyarrow fastparquet
```

### 2. Setup D: Drive Storage

```bash
# Run storage setup script
python scripts/setup_d_drive_storage.py

# This will:
# - Detect D: drive path
# - Check available disk space
# - Create directory structure
# - Generate configuration files
```

### 3. Test the System

```bash
# Run comprehensive test suite
python scripts/test_eodhd_population.py

# This validates:
# - EODHD API connectivity
# - D: drive storage access
# - File operations
# - Checkpoint system
# - Sample data population
```

### 4. Start Population

```bash
# Debug mode (limited symbols for testing)
python scripts/populate_30year_eodhd_minute_bars.py --debug --limit 5

# Full 30-year population
python scripts/populate_30year_eodhd_minute_bars.py --mode full

# Resume from checkpoint if interrupted
python scripts/populate_30year_eodhd_minute_bars.py --resume
```

## 📊 Storage Requirements

### Estimated Space Usage

- **Symbols**: ~3,000 major US stocks
- **Timeframe**: 30 years (1994-2024)
- **Resolution**: 1-minute bars
- **Trading Days**: 252 per year × 30 years = 7,560 days
- **Bars per Symbol**: 7,560 days × 390 minutes/day = ~2.9M bars
- **Total Bars**: 3,000 × 2.9M = ~8.7 billion bars

### File Size Estimates

- **Raw Data**: ~435 GB (50 bytes per bar)
- **Compressed**: ~260 GB (60% compression ratio)
- **With Metadata**: ~310 GB (20% overhead)
- **Recommended Free Space**: 500 GB minimum

## 🗂️ Directory Structure

```
/mnt/d/ats-data/
├── minute-bars/           # Main Parquet files organized by symbol/month
│   ├── AAPL/
│   │   ├── 2024-01.parquet
│   │   ├── 2024-02.parquet
│   │   └── ...
│   ├── MSFT/
│   └── ...
├── backups/              # Backup files during updates
├── metadata/             # File metadata and checksums
├── temp/                # Temporary processing files
├── logs/                # Processing logs
├── checkpoints/         # Checkpoint files for resumable processing
└── reports/             # Population statistics and reports
```

## ⚙️ Command Line Options

### Basic Usage

```bash
python scripts/populate_30year_eodhd_minute_bars.py [OPTIONS]
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--mode` | Population mode: `full` or `incremental` | `full` |
| `--start-date` | Start date (YYYY-MM-DD) | 30 years ago |
| `--end-date` | End date (YYYY-MM-DD) | Today |
| `--symbols` | Comma-separated symbol list | All available |
| `--limit` | Limit number of symbols | None |
| `--storage-path` | Base storage path | `/mnt/d/ats-data` |
| `--checkpoint-file` | Checkpoint file path | `eodhd_30year_checkpoint.json` |
| `--resume` | Resume from checkpoint | False |
| `--debug` | Enable debug mode | False |
| `--concurrent` | Max concurrent operations | 1 |

### Example Commands

```bash
# Test with 10 symbols for past 1 year
python scripts/populate_30year_eodhd_minute_bars.py \
  --start-date 2023-01-01 \
  --end-date 2024-01-01 \
  --limit 10 \
  --debug

# Specific symbols only
python scripts/populate_30year_eodhd_minute_bars.py \
  --symbols AAPL,MSFT,GOOGL,AMZN,TSLA \
  --start-date 2020-01-01

# Resume interrupted processing
python scripts/populate_30year_eodhd_minute_bars.py \
  --resume \
  --checkpoint-file /mnt/d/ats-data/checkpoints/my_checkpoint.json

# Full 30-year population with custom storage
python scripts/populate_30year_eodhd_minute_bars.py \
  --mode full \
  --storage-path /mnt/d/market-data \
  --concurrent 1
```

## 🔄 Checkpoint System

The checkpoint system enables resumable processing for long-running jobs:

### Checkpoint Format

```json
{
  "start_date": "1994-01-01",
  "end_date": "2024-01-01",
  "total_symbols": 3000,
  "processed_symbols": 1250,
  "current_symbol": "MSFT",
  "current_date": "2024-01-01",
  "symbols_completed": ["AAPL", "GOOGL", ...],
  "symbols_failed": ["BADSTOCK"],
  "total_bars_stored": 5000000,
  "total_files_created": 2500,
  "last_update_timestamp": "2024-01-15T10:30:00",
  "errors": [],
  "processing_stats": {}
}
```

### Resuming Processing

1. **Automatic Resume**: Use `--resume` to continue from last checkpoint
2. **Custom Checkpoint**: Specify `--checkpoint-file` to use specific checkpoint
3. **Progress Tracking**: Real-time updates show completion percentage

## 📈 Progress Monitoring

### Real-Time Logging

```
2024-01-15 10:30:00 - INFO - Starting AAPL population: 1994-01-01 to 2024-01-01
2024-01-15 10:31:15 - INFO - AAPL: 2,847,650 bars collected, 2,847,650 stored, 12 files created
2024-01-15 10:31:15 - INFO - Progress: 1250/3000 (41.7%) - Current: MSFT
```

### Final Statistics

```
===============================================================================
EODHD 30-YEAR POPULATION FINAL STATISTICS
===============================================================================
Symbols processed: 3000
Symbols completed: 2995
Symbols failed: 5
Total bars collected: 8,542,350,000
Total bars stored: 8,542,350,000
Total files created: 36,000
Total API calls: 22,680,000
Storage statistics:
  - Total files: 36,000
  - Total symbols: 3,000
  - Total size: 310.5 GB
===============================================================================
```

## ⚠️ Rate Limiting and API Considerations

### EODHD API Limits

- **Free Tier**: 20 requests per minute
- **Paid Tier**: 100 requests per minute
- **Conservative**: Script uses 3-second delays (20 req/min max)

### Processing Time Estimates

- **API Calls Required**: ~22.7 million (30 years × 252 days × 3000 symbols)
- **Time at 20 req/min**: ~19,000 hours (~2.2 years continuous)
- **Realistic Time**: 6-12 months with interruptions and retries

### Optimization Strategies

1. **Parallel Processing**: Limited by API rate limits
2. **Incremental Updates**: Process recent data first
3. **Symbol Prioritization**: Start with most important symbols
4. **Weekend Processing**: Continuous uninterrupted runs

## 🛠️ Troubleshooting

### Common Issues

#### 1. API Key Issues
```bash
# Check API key is set
echo $EODHD_API_KEY

# Test API connectivity
curl "https://eodhistoricaldata.com/api/exchange-symbol-list/US?api_token=$EODHD_API_KEY&fmt=json"
```

#### 2. D: Drive Access Issues
```bash
# Check D: drive is mounted
ls /mnt/d

# Test write permissions
touch /mnt/d/test_file && rm /mnt/d/test_file

# Run setup script
python scripts/setup_d_drive_storage.py
```

#### 3. Disk Space Issues
```bash
# Check available space
df -h /mnt/d

# Clean up old backups
python scripts/populate_30year_eodhd_minute_bars.py --cleanup-backups

# Use higher compression (modify script)
```

#### 4. Processing Interruptions
```bash
# Always use resume to continue
python scripts/populate_30year_eodhd_minute_bars.py --resume

# Check checkpoint file exists
ls -la eodhd_30year_checkpoint.json

# Validate checkpoint integrity
cat eodhd_30year_checkpoint.json | python -m json.tool
```

### Error Recovery

1. **Network Issues**: Script automatically retries API calls
2. **Rate Limit Exceeded**: Automatic backoff and retry
3. **Disk Full**: Process stops safely, resume after cleanup
4. **Corrupted Files**: Backup system allows recovery

## 🔍 Testing and Validation

### Test Suite Components

1. **API Connection Test**: Validates EODHD API access
2. **Storage Test**: Verifies D: drive access and permissions
3. **Population Test**: End-to-end test with sample data
4. **Checkpoint Test**: Validates save/resume functionality
5. **File Integrity Test**: Checks data quality and corruption

### Running Tests

```bash
# Full test suite
python scripts/test_eodhd_population.py

# Individual test components available in script
```

## 📋 Performance Optimization

### Recommended Settings

```bash
# Optimal for most systems
python scripts/populate_30year_eodhd_minute_bars.py \
  --concurrent 1 \              # Respect API limits
  --storage-path /mnt/d/ats-data \
  --mode full

# For faster processing (if you have premium API)
python scripts/populate_30year_eodhd_minute_bars.py \
  --concurrent 2 \              # Higher concurrency
  --mode full
```

### System Resources

- **CPU**: Minimal usage, I/O bound
- **Memory**: ~500MB typical, 1GB peak
- **Disk**: Write-intensive, ensure D: drive has good I/O
- **Network**: Continuous API calls, stable connection recommended

## 🔒 Security and Reliability

### Data Security

- API keys stored in environment variables only
- No sensitive data in checkpoint files
- Backup system prevents data loss during updates

### Reliability Features

- Atomic file operations prevent corruption
- Checksum verification for file integrity
- Automatic backup before file updates
- Graceful handling of interruptions

## 🤝 Integration with ATS Platform

### File Format Compatibility

- Compatible with existing `FileBasedMinuteManager`
- Uses same Parquet format as real-time collection
- Metadata format matches ATS standards

### Querying Populated Data

```python
from storage.file_based_minute_manager import FileBasedMinuteManager

# Initialize manager
manager = FileBasedMinuteManager(base_path="/mnt/d/ats-data/minute-bars")

# Query historical data
data = await manager.query_minute_data(
    'AAPL',
    datetime(2020, 1, 1),
    datetime(2020, 12, 31)
)

print(f"Retrieved {len(data)} minute bars for AAPL in 2020")
```

## 📞 Support and Maintenance

### Monitoring Progress

- Check logs in `/mnt/d/ats-data/logs/`
- Monitor checkpoint file updates
- Watch disk usage: `df -h /mnt/d`

### Maintenance Tasks

- Regular backup cleanup (automated)
- Periodic integrity checks
- Monitor API key usage limits
- Update symbol universe periodically

---

## 🎯 Next Steps After Population

1. **Data Validation**: Run integrity checks on populated data
2. **Performance Testing**: Test query performance on historical data
3. **Integration Testing**: Verify compatibility with ATS analytics
4. **Incremental Updates**: Set up daily/hourly incremental updates
5. **Monitoring Dashboard**: Create monitoring for data freshness

---

**⚡ Ready to populate 30 years of market data!** Start with the test suite and work your way up to the full population.