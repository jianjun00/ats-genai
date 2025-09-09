# ATS Platform Cron Job Configuration

## Overview

Complete cron-based scheduling system for the ATS platform. Replaces SystemD timers with simple, reliable cron job scheduling.

## Files

### **ats-complete-crontab**
Complete cron configuration for the entire ATS platform. Install with:
```bash
crontab scripts/cron/ats-complete-crontab
```

### **daily_health_check.sh**
Daily health monitoring script that checks:
- ATS-DEV/INTG service endpoints
- Database connections
- FirstRate data pipeline
- Backup system status
- Disk space usage
- Docker container health

## Schedule Overview

| Time    | Job | Description |
|---------|-----|-------------|
| 1:00 AM | Daily Sync | Database sync DEV → INTG (Mon-Fri) |
| 2:00 AM | DB Backups | PostgreSQL database backups |
| 2:30 AM | FirstRate | Daily minute bar downloads |
| 4:00 AM | Data Backup | Incremental data sync backup |
| 5:00 AM | Cleanup | Backup maintenance and cleanup |
| 6:00 AM | Log Rotation | Compress large log files (Sundays) |
| 6:30 AM | Health Check | Daily system health monitoring |
| 8:00 AM | FirstRate Retry | Retry if morning download failed |

## Installation

### Quick Install
```bash
# Install complete configuration
crontab scripts/cron/ats-complete-crontab

# Verify installation
crontab -l
```

### Migration from SystemD
```bash
# Automated migration from SystemD timers
./scripts/migrate_systemd_to_cron.sh

# Dry run to see what would be changed
./scripts/migrate_systemd_to_cron.sh --dry-run
```

## Monitoring

### Check Job Status
```bash
# View active cron jobs
crontab -l

# Check cron service status
systemctl status cron

# View cron execution logs
journalctl _COMM=cron -f
sudo tail -f /var/log/cron    # varies by distribution
```

### Manual Health Check
```bash
# Run health check manually
./scripts/cron/daily_health_check.sh

# View health check history
tail -50 /mnt/d/ats-logs/health-check.log
```

### Log Files
All jobs log to `/mnt/d/ats-logs/`:
- `firstrate-daily.log` - FirstRate downloads
- `firstrate-daily-error.log` - FirstRate errors
- `firstrate-daily-retry.log` - Retry attempts
- `daily-sync.log` - Database sync operations
- `daily-sync-error.log` - Database sync errors
- `health-check.log` - Daily health monitoring
- `cron-data-backup.log` - Backup operations

## Troubleshooting

### Common Issues

**Cron jobs not running:**
1. Check cron service: `systemctl status cron`
2. Verify cron jobs installed: `crontab -l`
3. Check system logs: `journalctl _COMM=cron`

**Permission issues:**
1. Ensure scripts are executable: `chmod +x scripts/cron/*.sh`
2. Verify paths in cron jobs match actual file locations
3. Check directory permissions: `/mnt/d/ats-logs`, `/mnt/d/ats-data`

**Path/Environment issues:**
1. All cron jobs use full paths for executables
2. PYTHONPATH is set explicitly in each job
3. Working directory is set with `cd` command

### Testing Individual Jobs

```bash
# Test FirstRate download
cd /home/jianjun/ats-genai-data && PYTHONPATH=src uv run python scripts/firstrate_daily_download.py --all --debug

# Test database sync
cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/eodhd_database_sync.py --vendor eodhd --source-port 3432 --target-port 4432

# Test health check
./scripts/cron/daily_health_check.sh
```

## Advantages of Cron vs SystemD

### Simplicity
- **Cron**: Simple text-based configuration, easy to edit
- **SystemD**: Complex unit files with multiple configuration points

### Portability
- **Cron**: Works on all Unix-like systems
- **SystemD**: Linux-specific, not available on all distributions

### Debugging
- **Cron**: Standard logs, familiar debugging process
- **SystemD**: journalctl and systemctl commands required

### Maintenance
- **Cron**: Single crontab file, easy backup and restore
- **SystemD**: Multiple unit files in system directories

### Monitoring
- **Cron**: Standard syslog integration
- **SystemD**: Requires systemd-specific monitoring tools

## Security

### File Permissions
- Scripts: 755 (executable by owner, readable by others)
- Cron file: 644 (readable by owner, readable by others)
- Log directories: 755 with appropriate ownership

### Environment Isolation
- Full paths used for all executables
- Working directory explicitly set
- PYTHONPATH set per job to avoid conflicts
- Minimal environment variables passed

### Access Control
- Jobs run as `jianjun` user
- No elevated privileges required for normal operations
- Logs written to dedicated directory with controlled access

---

**Status**: ✅ Production Ready  
**Last Updated**: 2025-09-09  
**Migration Status**: Ready to replace SystemD timers