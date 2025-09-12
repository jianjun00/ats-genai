# FirstRate Minute Bar Processing System

## Overview

The FirstRate minute bar processing system converts historical 1-minute OHLCV data from FirstRate zip files into monthly Parquet files for the ATS trading platform.

## Data Structure

### Source Data
- **Location**: `/mnt/d/ats-data/firstrate-data/stock/`
- **Format**: ZIP files containing CSV data for stocks starting with specific letters
- **Example**: `stock_A_full_1min_adjsplitdiv_fz3yij8.zip` contains all A-symbols

### CSV Format (per symbol)
```
AACG_full_1min_adjsplitdiv.txt:
2008-01-29 10:00:00,9.51,9.99,9.5,9.5,89745
2008-01-29 10:01:00,9.5,9.64,9.35,9.6,55200
...
```

**Columns**: timestamp,open,high,low,close,volume
**Timezone**: EDT (Eastern Daylight Time) - automatically converted to UTC

### Output Structure
- **Location**: `/mnt/d/ats-data/minute-bars/firstrate/`
- **Organization**: `{symbol}/{year}/{month}.parquet`
- **Example**: `AAPL/2023/01.parquet`

## Components

### FirstRateAdapter (`src/market_data/agent/firstrate_adapter.py`)
- Reads and processes FirstRate zip files
- Handles EDT to UTC timezone conversion
- Extracts symbol inventory and date ranges
- Yields Tick objects for processing

### FirstRate Backfill Processor (`scripts/populate_firstrate_minute_bars.py`)
- Monthly processing for memory efficiency
- Checkpoint-based resumable processing
- Async processing with FileBasedMinuteManager
- Progress tracking and error recovery

## Usage

### Basic Usage
```bash
# Process all stock symbols (full backfill)
PYTHONPATH=src uv run python scripts/populate_firstrate_minute_bars.py --asset-type stock

# Process specific symbols only
PYTHONPATH=src uv run python scripts/populate_firstrate_minute_bars.py --symbols AAPL,MSFT,GOOGL

# Resume from checkpoint
PYTHONPATH=src uv run python scripts/populate_firstrate_minute_bars.py --checkpoint-file production.json --resume

# Debug mode with limited symbols
PYTHONPATH=src uv run python scripts/populate_firstrate_minute_bars.py --limit 10 --debug
```

### Command Line Options
- `--asset-type`: Choose from stock, etf, fx, index (default: stock)
- `--data-path`: Path to FirstRate data directory (default: /mnt/d/ats-data/firstrate-data)
- `--output-path`: Output directory for processed files (default: /mnt/d/ats-data/minute-bars/firstrate)
- `--checkpoint-file`: Checkpoint file for resumable processing
- `--symbols`: Specific symbols to process (space-separated)
- `--limit`: Limit number of symbols for testing
- `--debug`: Enable debug logging

## Architecture

### Processing Flow
1. **Inventory Building**: Scan all zip files to build symbol inventory
2. **Monthly Processing**: Process each symbol one month at a time
3. **Timezone Conversion**: Convert EDT timestamps to UTC
4. **Storage**: Store as monthly Parquet files using FileBasedMinuteManager
5. **Checkpointing**: Save progress for resumable processing

### Key Features
- **Memory Efficient**: Processes data monthly to avoid memory issues
- **Resumable**: Checkpoint-based processing can resume from interruptions
- **Error Recovery**: Failed months are tracked and can be retried
- **Timezone Aware**: Proper EDT/EST to UTC conversion
- **Scalable**: Handles thousands of symbols across decades of data

### Data Quality
- Automatic duplicate detection and removal
- Data validation and error logging
- Missing data handling
- Comprehensive progress tracking

## Performance

### Expected Processing Time
- **Symbol Inventory**: ~2-3 minutes for all stock zip files
- **Processing Rate**: ~100-500 symbols/hour (depends on data density)
- **Storage**: ~1-2 GB per 1000 symbols per year

### Resource Usage
- **Memory**: ~1-2 GB during processing
- **Disk**: Original zip files + processed Parquet files
- **CPU**: Single-threaded processing per symbol

## Monitoring

### Log Messages
```
🚀 Starting FirstRate minute bar backfill
📊 Asset type: stock
💾 Output path: /mnt/d/ats-data/minute-bars/firstrate
📝 Checkpoint file: firstrate_monthly_production.json
Building symbol inventory from zip files...
Found 5847 symbols across all zip files
🔄 Progress: 1/5847 symbols
Processing symbol: AAPL
AAPL: Processing 192 months from 2008-01-02 to 2024-12-31
✅ AAPL 2008-01: 18,720 records
...
✅ AAPL complete: 192/192 months, 3,456,789 records
```

### Checkpoint File Structure
```json
{
  "completed_months": {
    "AAPL": ["2008-01", "2008-02", "..."],
    "MSFT": ["2008-01", "2008-02", "..."]
  },
  "failed_months": {
    "BADSTOCK": ["2010-03"]
  },
  "last_processed": "2024-08-29T13:45:30.123456",
  "total_symbols": 5847,
  "processing_stats": {
    "symbols_processed": 123,
    "months_processed": 23456,
    "records_written": 123456789,
    "errors": 5
  }
}
```

## Troubleshooting

### Common Issues

1. **Memory Issues**
   - Reduce batch size or process fewer symbols at once
   - Ensure sufficient disk space for output files

2. **Timezone Issues**
   - FirstRate data is in EDT/EST, automatically converted to UTC
   - Verify timezone conversion is working correctly

3. **File Access Issues**
   - Ensure zip files are accessible and not corrupted
   - Check output directory permissions

4. **Performance Issues**
   - Use SSD storage for better I/O performance
   - Consider processing subsets of symbols in parallel

### Recovery

If processing fails:
1. Check the checkpoint file for completed work
2. Resume with `--resume` flag
3. Review logs for specific error messages
4. Process failed symbols individually if needed

## Integration

### Output Compatibility
The processed Parquet files are compatible with:
- ATS FileBasedMinuteManager
- Pandas/PyArrow data analysis
- Other ATS data processing components

### Schema Consistency
Output follows the standard MinuteBar schema:
- symbol: string
- timestamp: datetime (UTC)
- open/high/low/close: float
- volume: int
- vendor: "firstrate"

---

**Status**: ✅ Production Ready
**Last Updated**: 2025-08-29
**Data Coverage**: 2008-2024 (varies by symbol)
**Total Symbols**: ~5,847 stocks