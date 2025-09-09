# FirstRate Daily Data Download System

## Overview

Automated daily download system for FirstRate 1-minute bar data covering stocks, ETFs, and FX markets. Downloads fresh daily data every morning after FirstRate updates their datasets at 2:00 AM EST/EDT.

## Architecture

### Components
1. **FirstRateDownloader** - Core download engine with retry logic and verification
2. **Daily Job Script** - Command-line interface for manual and automated runs  
3. **Cron/Systemd Integration** - Automated scheduling at 2:30 AM EST/EDT
4. **Monitoring & Cleanup** - Log management and old file cleanup

### Data Flow
```
FirstRate API (2:00 AM update) 
    ↓
Daily Download Job (2:30 AM)
    ↓  
/mnt/d/ats-data/firstrate-data/daily/{asset_type}/
    ↓
Processing & Integration with ATS Platform
```

## Installation

### Quick Setup
```bash
# Install complete ATS cron configuration (recommended)
crontab scripts/cron/ats-complete-crontab

# Or install using individual setup script
chmod +x scripts/setup_firstrate_daily_jobs.sh
./scripts/setup_firstrate_daily_jobs.sh --method cron

# Test the setup
./scripts/setup_firstrate_daily_jobs.sh --test
```

### Manual Installation Steps

1. **Create directories:**
```bash
mkdir -p /mnt/d/ats-data/firstrate-data/daily/{stock,etf,fx}
mkdir -p /mnt/d/ats-logs
```

2. **Test download script:**
```bash
cd /home/jianjun/ats-genai-pm
PYTHONPATH=src uv run python scripts/firstrate_daily_download.py --test
```

3. **Install cron jobs:**
```bash
# Install complete ATS cron configuration
crontab scripts/cron/ats-complete-crontab

# Or add individual entries to existing crontab
crontab -e
# Add: 30 2 * * * cd /home/jianjun/ats-genai-data && PYTHONPATH=src uv run python scripts/firstrate_daily_download.py --all
```

## Usage

### Manual Downloads
```bash
# Download all asset types for today
PYTHONPATH=src uv run python scripts/firstrate_daily_download.py --all

# Download specific asset types
PYTHONPATH=src uv run python scripts/firstrate_daily_download.py --asset-types stock etf

# Download for specific date
PYTHONPATH=src uv run python scripts/firstrate_daily_download.py --all --date 2024-08-29

# Download with debug logging
PYTHONPATH=src uv run python scripts/firstrate_daily_download.py --all --debug
```

### Automated Scheduling

**Cron Jobs (Recommended):**
- Runs at 2:30 AM EST/EDT daily
- Backup retry at 8:00 AM if first attempt failed
- Simple, reliable text-based scheduling
- Integrated with complete ATS platform scheduling
- Easy monitoring via standard cron logs

**Complete ATS Schedule:**
- 2:00 AM: Database backups
- 2:30 AM: FirstRate minute bar downloads
- 4:00 AM: Data backups
- 5:00 AM: Backup cleanup
- 6:30 AM: Health monitoring
- 6:45 AM: Daily prices validation

## Data Organization

### Directory Structure
```
/mnt/d/ats-data/firstrate-data/daily/
├── stock/
│   ├── stock_20240829_1min_adj_split.zip
│   ├── stock_20240830_1min_adj_split.zip
│   └── ...
├── etf/
│   ├── etf_20240829_1min_adj_split.zip
│   ├── etf_20240830_1min_adj_split.zip
│   └── ...
└── fx/
    ├── fx_20240829_1min_adj_split.zip
    ├── fx_20240830_1min_adj_split.zip
    └── ...
```

### File Format
- **Naming:** `{asset_type}_{YYYYMMDD}_{timeframe}_{adjustment}.zip`
- **Content:** ZIP archives containing CSV files with 1-minute OHLCV data
- **Timezone:** EDT/EST (automatically converted to UTC during processing)
- **Columns:** `timestamp,open,high,low,close,volume`

### Data Quality
- **Checksum Verification:** MD5 hash calculated for each download
- **ZIP Integrity Check:** Automatic verification of archive structure
- **Content Validation:** Ensures CSV/TXT files are present
- **Size Verification:** Checks for non-empty downloads

## Monitoring

### Job Status
```bash
# Check current cron job status
crontab -l | grep firstrate

# View recent cron logs (varies by system)
sudo tail -f /var/log/cron
# or
journalctl _COMM=cron -f

# Check job execution status
./scripts/cron/daily_health_check.sh

# Manual status check
./scripts/setup_firstrate_daily_jobs.sh --status
```

### Log Files
- **Main Log:** `/mnt/d/ats-logs/firstrate-daily.log`
- **Error Log:** `/mnt/d/ats-logs/firstrate-daily-error.log`
- **Retry Log:** `/mnt/d/ats-logs/firstrate-daily-retry.log`
- **Health Check:** `/mnt/d/ats-logs/health-check.log`
- **System Cron:** `/var/log/cron` (varies by distribution)

### Log Sample
```
2024-08-29 02:30:15 - INFO - 🚀 Starting FirstRate daily download for 20240829
2024-08-29 02:30:15 - INFO - 📊 Asset types: ['stock', 'etf', 'fx']
2024-08-29 02:30:16 - INFO - 📥 Downloading stock data...
2024-08-29 02:31:45 - INFO - ✅ Downloaded stock_20240829_1min_adj_split.zip: 45,234,567 bytes, checksum: a1b2c3d4...
2024-08-29 02:32:30 - INFO - ✅ etf download completed successfully
2024-08-29 02:33:15 - INFO - ✅ fx download completed successfully
2024-08-29 02:33:16 - INFO - 🧹 Cleaned up 3 old files (keeping 7 days)
2024-08-29 02:33:16 - INFO - ✅ Daily download completed successfully: 3/3 asset types
```

## API Integration

### FirstRate API Details
- **Base URL:** `https://firstratedata.com/api/data_file`
- **User ID:** `fg1LcNsv8kWWMJIt0caCFQ`
- **Update Schedule:** Daily at 2:00 AM EST/EDT
- **Data Types:** stock, etf, fx
- **Timeframes:** 1min (with zero-volume filtering)
- **Adjustments:** adj_split, adj_splitdiv, UNADJUSTED

### Rate Limiting
- **No explicit limits** mentioned in API documentation
- **Conservative approach:** 1 request per asset type per day
- **Retry logic:** Exponential backoff on failures
- **Error handling:** Graceful degradation with logging

## Maintenance

### Regular Tasks
1. **Monitor disk space** - Daily downloads can be 50-200MB per asset type
2. **Check logs** - Review for download failures or API issues
3. **Verify data quality** - Spot check downloaded files periodically
4. **Update credentials** - If FirstRate changes user ID or API

### Cleanup Configuration
- **Default retention:** 7 days of daily files
- **Configurable:** Use `--cleanup-days N` parameter
- **Automatic:** Runs after each successful download
- **Manual cleanup:** Run download script with `--cleanup-days 0 --no-cleanup`

### Troubleshooting

**Download Failures:**
1. Check network connectivity to FirstRate API
2. Verify user ID is still valid
3. Check disk space in `/mnt/d/ats-data/`
4. Review API response codes in error logs

**Missing Data:**
1. FirstRate may not have data for weekends/holidays
2. Some assets may have gaps in historical data
3. Check FirstRate website for service announcements

**Storage Issues:**
1. Ensure `/mnt/d/` is mounted and writable
2. Check filesystem permissions
3. Monitor disk usage growth

**Scheduling Problems:**
1. Verify cron daemon is running: `systemctl status cron`
2. Check systemd timer: `systemctl status firstrate-daily.timer`
3. Review timezone settings (jobs run in EST/EDT)

## Performance

### Expected Metrics
- **Download time:** 1-5 minutes per asset type
- **File sizes:** 50-200MB per daily archive
- **Memory usage:** <100MB during download
- **Disk usage:** ~500MB per day (all asset types)
- **Network usage:** ~150MB download per day

### Optimization
- **Parallel downloads:** Each asset type downloaded sequentially
- **Compression:** Data comes pre-compressed from FirstRate
- **Cleanup:** Automatic removal of old files to manage disk usage
- **Resource limits:** Systemd service limits CPU and memory usage

## Security

### Access Control
- **File permissions:** 755 for directories, 644 for data files
- **User context:** Runs as `jianjun` user
- **Network access:** HTTPS only to FirstRate API
- **Systemd security:** NoNewPrivileges, PrivateTmp, ProtectSystem

### Data Protection
- **Checksum verification:** Detects corrupted downloads
- **Atomic operations:** Downloads to temporary files first
- **Backup strategy:** Daily files provide natural backup timeline
- **Access logging:** All operations logged for audit

## Integration with ATS Platform

### Data Processing Pipeline
1. **Daily Download** → Daily ZIP files in `/mnt/d/ats-data/firstrate-data/daily/`
2. **Daily Processing** → Extract and convert to UTC timestamps
3. **Storage** → Monthly Parquet files in `/mnt/d/ats-data/minute-bars/firstrate/`
4. **Integration** → Available for ATS backtesting and analysis

### Future Enhancements
- **Real-time integration** with existing FirstRate monthly backfill
- **Delta processing** to identify only new/changed data
- **Automated validation** against historical monthly data
- **Alert system** for download failures or data quality issues

---

**Status:** ✅ Production Ready  
**Last Updated:** 2024-08-29  
**Data Coverage:** Daily updates for US stocks, ETFs, and FX  
**Automation:** Fully automated with monitoring and cleanup