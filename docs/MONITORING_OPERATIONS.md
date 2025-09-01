# ATS Platform Monitoring Operations Guide

## 🚨 Critical Monitoring Systems

### WSL System Monitoring (CRITICAL - Fixed 2025-09-01)

**Issue Resolved:** No Slack notifications since 3pm yesterday due to stopped monitoring process.

**Current Status:**
- ✅ **Active monitoring process** running (PID: 893658)
- ✅ **Hourly Slack alerts** configured to #ats-alerts channel  
- ✅ **Auto-restart on boot** via @reboot cron job
- ✅ **Backup hourly cron** for redundancy

## 📅 Production Cron Schedule

### Daily Operations (Staggered Timing)
```bash
# 2:00 AM - ATS-DEV Database Backup
0 2 * * * /home/jianjun/ats-genai-data/scripts/daily_backup_ats_dev.sh

# 2:15 AM - ATS-INTG Database Backup  
15 2 * * * /home/jianjun/ats-genai-data/scripts/daily_backup_ats_intg.sh

# 2:30 AM - FirstRate Daily Data Download
30 2 * * * PYTHONPATH=src uv run python scripts/firstrate_daily_download.py --all >> /mnt/d/ats-logs/firstrate-daily.log 2>> /mnt/d/ats-logs/firstrate-daily-error.log

# 3:00 AM - Backup Monitoring  
0 3 * * * /home/jianjun/ats-genai-data/scripts/backup_monitor.sh

# 6:00 PM - Evening Backup Status Check
0 18 * * * /home/jianjun/ats-genai-data/scripts/backup_monitor.sh
```

### Continuous Monitoring
```bash
# Every Hour - WSL System Status to Slack
0 * * * * python3 simple_wsl_monitor.py --test >/dev/null 2>&1

# System Boot - Auto-restart Monitoring
@reboot sleep 30 && /home/jianjun/ats-genai-data/restart_monitoring.sh >/dev/null 2>&1
```

## 🔧 Monitoring Management Commands

### Check System Status
```bash
# Verify WSL monitoring is active
ps aux | grep simple_wsl_monitor | grep -v grep

# Check all cron jobs
crontab -l

# View recent cron executions
grep CRON /var/log/syslog | tail -10
```

### Restart Monitoring
```bash
# Restart WSL system monitoring
/home/jianjun/ats-genai-data/restart_monitoring.sh

# Test Slack notifications manually
cd /home/jianjun/ats-genai-data/scripts/monitoring
python3 simple_wsl_monitor.py --test
```

### Monitor Logs
```bash
# WSL monitoring log
tail -f /mnt/d/ats-logs/wsl_monitor.log

# FirstRate download logs
tail -f /mnt/d/ats-logs/firstrate-daily.log
tail -f /mnt/d/ats-logs/firstrate-daily-error.log

# Backup monitoring logs  
tail -f /mnt/d/ats-logs/backup_monitor.log
```

## 🚨 Troubleshooting Guide

### WSL Monitoring Not Sending Alerts

**Symptoms:**
- No Slack notifications in #ats-alerts channel
- Missing hourly system status updates

**Diagnosis:**
```bash
# Check if monitoring process is running
ps aux | grep simple_wsl_monitor | grep -v grep

# If no process found:
echo "❌ WSL monitoring is DOWN"

# Check monitoring log for errors
tail -50 /mnt/d/ats-logs/wsl_monitor.log
```

**Solution:**
```bash
# Restart monitoring system
/home/jianjun/ats-genai-data/restart_monitoring.sh

# Verify it's working
python3 simple_wsl_monitor.py --test
# Expected: "✅ Test alert sent successfully!" + Slack notification
```

### Cron Jobs Not Executing

**Common Issues:**
1. **Cron daemon stopped**
   ```bash
   systemctl status cron
   sudo systemctl start cron
   ```

2. **Environment variables missing**
   ```bash
   # Add to crontab
   PATH=/usr/local/bin:/usr/bin:/bin
   PYTHONPATH=src
   ```

3. **Relative paths**
   ```bash
   # ❌ BAD: 0 2 * * * scripts/backup.sh
   # ✅ GOOD: 0 2 * * * /home/jianjun/ats-genai-data/scripts/backup.sh
   ```

4. **No output redirection**
   ```bash
   # ❌ BAD: 0 * * * * command
   # ✅ GOOD: 0 * * * * command >> /var/log/command.log 2>&1
   ```

### FirstRate Downloads Failing

**Check Download Status:**
```bash
# Check recent downloads
ls -la /mnt/d/ats-data/firstrate-data/ | head -10
find /mnt/d/ats-data/firstrate-data/ -name "*.zip" -mtime -1

# Check download logs
tail -100 /mnt/d/ats-logs/firstrate-daily.log
tail -50 /mnt/d/ats-logs/firstrate-daily-error.log
```

**Manual Test:**
```bash
cd /home/jianjun/ats-genai-data
PYTHONPATH=src uv run python scripts/firstrate_daily_download.py --test
```

## 📊 Monitoring Metrics

### System Health Indicators
- CPU usage, memory utilization, disk space
- Docker container status
- PostgreSQL database connectivity
- ATS backfill process status
- System uptime and load average

### Data Pipeline Health
- FirstRate daily download success/failure
- Database backup completion status
- Data directory sizes and growth trends
- Process execution times and resource usage

## 🔄 Maintenance Schedule

### Daily (Automated)
- ✅ Database backups (ATS-DEV: 2:00 AM, ATS-INTG: 2:15 AM)
- ✅ FirstRate data download (2:30 AM)
- ✅ Backup status monitoring (3:00 AM, 6:00 PM)
- ✅ Hourly system status alerts to Slack

### Weekly (Manual)
- Clean old backup files: `./scripts/manage_backups.sh cleanup`
- Review monitoring logs for trends
- Verify all cron jobs are executing successfully
- Check disk space usage: `du -sh /mnt/d/ats-*`

### Monthly (Manual)  
- Review and update monitoring thresholds
- Analyze system performance trends
- Update documentation with any operational changes
- Test disaster recovery procedures

## 📱 Alert Channels

### Slack Integration
- **Channel:** #ats-alerts
- **Webhook:** Configured in `simple_wsl_monitor.py`
- **Frequency:** Hourly status updates + real-time alerts
- **Format:** Rich formatted messages with system metrics

### Log Files
- **WSL Monitoring:** `/mnt/d/ats-logs/wsl_monitor.log`
- **FirstRate Downloads:** `/mnt/d/ats-logs/firstrate-daily.log`
- **Backup Operations:** `/mnt/d/ats-logs/backup_monitor.log`
- **System Metrics History:** `/mnt/d/ats-logs/monitoring/`

---

**Last Updated:** 2025-09-01  
**Status:** ✅ All monitoring systems active and operational