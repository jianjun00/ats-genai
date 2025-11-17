# Feature Extraction Command Reference

This document provides the detailed command used to run feature extraction for the multi-time horizon PyTorch DataLoader.

## Command Used

```bash
DATABASE_URL=postgresql://postgres:intg_password@localhost:4432/intg_db \
PYTHONPATH=src \
python3 src/domains/services/training_data/feature_extraction_runner.py \
    --symbols TSLA MSFT GOOGL NVDA \
    --start-date 2025-08-01 \
    --end-date 2025-11-14 \
    --environment intg \
    --gin-config config/feature_extraction.gin \
    --debug
```

## Command Breakdown

### Environment Variables
- `DATABASE_URL=postgresql://postgres:intg_password@localhost:4432/intg_db`
  - Sets the integration database connection string
  - Port 4432 is the integration environment postgres port
  - Uses integration database credentials

- `PYTHONPATH=src`
  - Ensures Python can find the source modules

### Parameters
- `--symbols TSLA MSFT GOOGL NVDA`
  - Target symbols for feature extraction
  - Multiple symbols can be specified

- `--start-date 2025-08-01`
  - Start date for feature extraction (YYYY-MM-DD format)

- `--end-date 2025-11-14`
  - End date for feature extraction (YYYY-MM-DD format)

- `--environment intg`
  - Uses integration environment configuration
  - Options: dev, test, intg, prod

- `--gin-config config/feature_extraction.gin`
  - Gin configuration file specifying:
    - Feature types: ohlcv, returns, volatility, technical indicators
    - Multi-timeframe processing: 5m, 15m, 1h, 1d
    - Technical indicators configuration

- `--debug`
  - Enables debug logging for detailed output

## Ray Parallelized Execution (Recommended)

For faster processing, use Ray to parallelize across symbols:

```bash
DATABASE_URL=postgresql://postgres:intg_password@localhost:4432/intg_db \
PYTHONPATH=src \
python3 scripts/feature_extraction_ray.py \
    --symbols AAPL MSFT GOOGL NVDA TSLA \
    --start-date 2024-01-01 \
    --end-date 2025-11-14 \
    --environment intg \
    --gin-config config/feature_extraction.gin \
    --debug
```

### Ray Parallelization Benefits
- **5x speedup**: Each symbol processed on separate CPU core
- **Fault tolerance**: Individual symbol failures don't stop other symbols
- **Resource management**: Automatic CPU and memory allocation
- **Progress tracking**: Real-time status of each symbol's progress

## Background Execution

To run in background with logging:

```bash
DATABASE_URL=postgresql://postgres:intg_password@localhost:4432/intg_db \
PYTHONPATH=src \
nohup python3 src/domains/services/training_data/feature_extraction_runner.py \
    --symbols TSLA MSFT GOOGL NVDA \
    --start-date 2025-08-01 \
    --end-date 2025-11-14 \
    --environment intg \
    --gin-config config/feature_extraction.gin \
    --debug > feature_extraction.log 2>&1 &
```

## Expected Output

The feature extraction process will:
1. Initialize database connections
2. Load gin configuration
3. Create universe manager for specified symbols
4. Process multi-timeframe data (5m, 15m, 1h, 1d)
5. Generate ArrayRecord files with features
6. Store results in `/data/training_data/` directory

### Output Directory Structure
```
/data/training_data/
└── dataset_YYYYMMDD_HHMMSS/
    ├── feature_group/
    │   ├── SYMBOL_YYYY_MM/
    │   │   ├── 5m/
    │   │   │   └── SYMBOL_YYYY_MM_feature_group.arrayrecord
    │   │   ├── 15m/
    │   │   │   └── SYMBOL_YYYY_MM_feature_group.arrayrecord
    │   │   ├── 1h/
    │   │   │   └── SYMBOL_YYYY_MM_feature_group.arrayrecord
    │   │   └── 1d/
    │   │       └── SYMBOL_YYYY_MM_feature_group.arrayrecord
    │   └── SYMBOL_YYYY_MM_columns.json
    └── metadata.json
```

## Prerequisites

1. **Database Services Running**
   - Integration postgres: `ats-intg-postgres` on port 4432
   - Check status: `docker ps | grep ats-intg-postgres`

2. **Market Data Available**
   - Source data must be available in the configured data sources
   - FirstRate, Polygon, Tiingo, or EODHD minute bar data

3. **Configuration Files**
   - `config/feature_extraction.gin` must exist and be properly configured
   - Technical indicators configuration must be valid

## Monitoring Progress

Check process status:
```bash
ps aux | grep feature_extraction
```

Monitor log output:
```bash
tail -f feature_extraction.log
```

Check generated files:
```bash
find /data/training_data -name "*.arrayrecord" -newer /some/reference/file
```

## Troubleshooting

### Database Connection Issues
- Verify postgres container is running: `docker ps | grep postgres`
- Check database credentials match environment
- Test connection: `psql -h localhost -p 4432 -U postgres -d intg_db`

### Gin Configuration Issues
- Verify gin file exists: `ls -la config/feature_extraction.gin`
- Check gin syntax and module paths
- Ensure all required parameters are configured

### Memory Issues
- Monitor process memory usage
- Consider reducing date range for large datasets
- Check available disk space for output files

## Production Tag Backfill

After feature extraction completes, ArrayRecord files must be tagged for production use:

```bash
# Tag all datasets from today's feature extraction
PYTHONPATH=src python3 scripts/production_tag_backfill.py --dataset-pattern "dataset_20251116_*"

# Tag a specific dataset
PYTHONPATH=src python3 scripts/production_tag_backfill.py --dataset-pattern "dataset_20251116_154500"

# Tag all datasets (use with caution) 
PYTHONPATH=src python3 scripts/production_tag_backfill.py --dataset-pattern "dataset_*"
```

### Production Tag Requirements
- Files must contain >0 ArrayRecord entries to be considered valid
- Files are validated using ArrayRecord reader to check actual record count
- Only valid files are tagged with `status = 'production'` in integration database
- Invalid files (empty records, corrupted, unreadable) are skipped and won't be available for training

## Integration with Multi-Time Horizon DataLoader

The generated ArrayRecord files are designed to work with the MultiTimeHorizonDataLoader:

```python
from domains.ml.data_loaders.multi_time_horizon_dataloader import (
    MultiTimeHorizonConfig, create_multi_horizon_dataloader
)

# The DataLoader will automatically find and load the generated ArrayRecord files
config = MultiTimeHorizonConfig(
    timeframes=['5m', '15m', '1h', '1d'],
    feature_groups=['ohlcv_basic', 'technical_momentum'],
    # ... other config
)

dataloader = create_multi_horizon_dataloader(
    symbols=['TSLA', 'MSFT', 'GOOGL', 'NVDA'],
    date_range=(datetime(2025, 8, 1), datetime(2025, 11, 14)),
    config=config
)
```

Last Updated: 2025-11-16
Status: Feature extraction running for symbols AAPL, MSFT, GOOGL, NVDA, TSLA (2024-01-01 to 2025-11-14)