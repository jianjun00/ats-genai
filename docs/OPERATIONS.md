# 🔧 ATS Operations Guide

**Daily operations, monitoring, troubleshooting, cron jobs, and maintenance procedures for the ATS platform.**

---

## ⚙️ **Environment Management**

### **ATS-DEV Environment**
```bash
# Complete environment setup
python3 scripts/run_dev.py setup

# Individual service management (uses postgres-data-new volume)
python3 scripts/run_dev.py start --service postgres    # PostgreSQL database
python3 scripts/run_dev.py start --service analytics   # Analytics service
python3 scripts/run_dev.py stop --service analytics
python3 scripts/run_dev.py status

# Database operations
python3 scripts/run_dev.py query --query "SELECT version()"
python3 scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices"
```

### **ATS-INTG Environment**
```bash
# Start PostgreSQL database first (uses postgres-intg-data volume)
docker-compose -f docker-compose.ats.yml up -d postgres-intg

# Start INTG services
docker-compose -f docker-compose.intg-jobs.yml up -d

# Database operations (direct connection)
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "SELECT version()"
```

---

## 🚀 **Complete ATS Startup Process**

### **Single Command for Complete Environment Setup**
```bash
# Start both ATS-DEV and ATS-INTG environments
./scripts/ats_startup.sh
```

#### **✅ What the Startup Script Does**

**1. Clean Environment Setup:**
- Stops all existing ATS services cleanly
- Removes old containers to ensure fresh start
- Creates proper Docker network connections

**2. Database Initialization:**
- **ATS-DEV PostgreSQL**: Starts on `localhost:3432` using `postgres-data-new` volume
- **ATS-INTG PostgreSQL**: Starts on `localhost:4432` using `postgres-intg-data` volume
- Waits for both databases to be healthy before proceeding
- **NO automatic backup restoration** (manual initialization required)

**3. Service Health Validation:**
- Checks database table counts (DEV: 62 tables, INTG: 37 tables expected)
- Starts all ATS-DEV services (analytics, monitoring, data collection)
- Starts all ATS-INTG services (analytics, job scheduler, monitoring)
- Validates service health endpoints

**4. Complete Service URLs:**
```bash
# ATS-DEV Environment (Development)
- Analytics Service: http://localhost:3000
- EDA Dashboard: http://localhost:3000/eda  
- Health Check: http://localhost:3000/health
- Database: postgresql://postgres:dev_password@localhost:3432/dev_db

# ATS-INTG Environment (Integration Testing)
- Analytics Service: http://localhost:4000
- EDA Dashboard: http://localhost:4000/eda
- Health Check: http://localhost:4000/health  
- Database: postgresql://postgres:intg_password@localhost:4432/intg_db
- Prometheus Metrics: http://localhost:4080
- Daily Minute Bars: /mnt/d/ats-data/firstrate-data/daily/
```

### **⚡ Quick Health Check**
```bash
# Verify both environments are operational
curl -f http://localhost:3000/health  # ATS-DEV analytics
curl -f http://localhost:4000/health  # ATS-INTG analytics
curl -f http://localhost:4080/health  # ATS-INTG prometheus metrics
docker ps | grep -E "(ats-dev|intg)"  # Container status

# Daily minute bars system health
curl -s http://localhost:4080/metrics | grep "ats_daily_minute_backfill"  # Minute bars metrics
ls -la /mnt/d/ats-data/firstrate-data/daily/$(date +%Y/%m/%d)/  # Today's files
tail -50 /mnt/d/ats-logs/minute-bars-backfill.log  # Recent processing activity
```

---

## 📊 **Real-Time Minute Bar Collection Monitoring**

### **🎯 Primary Dashboard: Grafana**
```bash
🌐 URL: http://localhost:4002/d/f9afe708-9be9-4c39-b901-f5c43a0a479f/ats-vendor-monitoring-dashboard-fixed
📊 Login: admin/admin (change on first login)
🔧 Data Source: PostgreSQL queries + Prometheus metrics
```

**Minute Bar Metrics Available:**
- **Real-time Collection Rate**: Live minute bar collection per vendor/symbol
- **Processing Stats**: Records processed, success rates, error counts
- **File Organization**: Daily parquet file creation and organization
- **Storage Metrics**: File sizes, record counts by instrument type

### **📈 Prometheus Metrics Endpoints**
```bash
# Main metrics server (feeds Grafana)
curl -s http://localhost:4080/metrics | grep "ats_daily_minute"

# Key minute bar metrics:
ats_daily_minute_backfill_instruments_processed    # Number of instruments processed
ats_daily_minute_backfill_total_minute_bars         # Total minute bars processed
ats_daily_minute_backfill_symbols_by_type{type="stock"}     # Stock symbols processed
ats_daily_minute_backfill_symbols_by_type{type="etf"}       # ETF symbols processed
ats_daily_minute_backfill_symbols_by_letter{letter="A"}     # Symbols by first letter

# Health check endpoint
curl -f http://localhost:4080/health
```

### **🔧 Manual Monitoring Commands**
```bash
# Check today's processed minute bar files
find /mnt/d/ats-data/firstrate-data/daily/$(date +%Y/%m/%d)/ -name "*.parquet" | wc -l

# List key symbols processed today
ls -la /mnt/d/ats-data/firstrate-data/daily/$(date +%Y/%m/%d)/*/{AAPL,TSLA,SPY,QQQ,MSFT}_*.parquet

# Check file sizes and record counts
PYTHONPATH=src uv run python -c "
import pandas as pd
import glob
import os
from datetime import date

today_files = glob.glob(f'/mnt/d/ats-data/firstrate-data/daily/{date.today().strftime(\"%Y/%m/%d\")}/*/*.parquet')
total_records = 0
for file_path in today_files[:5]:  # Check first 5 files
    if os.path.exists(file_path):
        df = pd.read_parquet(file_path)
        symbol = os.path.basename(file_path).split('_')[0]
        print(f'✅ {symbol}: {len(df):,} records ({os.path.getsize(file_path):,} bytes)')
        total_records += len(df)
print(f'📊 Sample total: {total_records:,} minute bars')
"

# Check FirstRate source data
ls -la /mnt/d/ats-data/firstrate-data/daily/stock/stock_$(date +%Y%m%d)_*.zip
ls -la /mnt/d/ats-data/firstrate-data/daily/etf/etf_$(date +%Y%m%d)_*.zip
```

### **🚨 Troubleshooting Minute Bar Collection**
```bash
# Restart minute bar collection system
./scripts/restart_minute_bar_collection.sh

# Fix Grafana dashboard if showing no data (adds working panels)
python3 scripts/fix_grafana_minute_bar_dashboard.py

# Check collection logs
tail -50 /mnt/d/ats-logs/minute-bar-restart.log

# Verify FirstRate downloads
find /mnt/d/ats-data/firstrate-data/daily/ -name "*.zip" -mtime -1

# Test processing manually (key symbols only)
cd /home/jianjun/ats-genai-model
PYTHONPATH=src uv run python -c "
import zipfile, pandas as pd, os
from pathlib import Path
zip_path = '/mnt/d/ats-data/firstrate-data/daily/stock/stock_$(date +%Y%m%d)_1min_adj_split.zip'
if os.path.exists(zip_path):
    with zipfile.ZipFile(zip_path) as zf:
        for f in zf.namelist():
            if 'AAPL' in f.upper():
                print(f'✅ Found AAPL data: {f}')
                break
else:
    print(f'❌ No zip file: {zip_path}')
"
```

### **📋 Database Tables for Direct Queries**
```bash
# Connect to ATS-INTG database
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db

# Check live minute bar data (WORKING DATA TABLES)
SELECT vendor, symbol, COUNT(*) as records, MAX(timestamp) as latest_data 
FROM (
  SELECT vendor, symbol, timestamp FROM intg_one_minute_live_polygon 
  UNION ALL 
  SELECT vendor, symbol, timestamp FROM intg_one_minute_live_tiingo
) combined GROUP BY vendor, symbol ORDER BY latest_data DESC;

# Check API call status for minute bar collection
SELECT vendor, status_code, COUNT(*) as calls, AVG(response_time_ms)::int as avg_ms
FROM intg_api_calls 
WHERE endpoint LIKE '%minute%' OR endpoint LIKE '%intraday%'
GROUP BY vendor, status_code 
ORDER BY vendor, calls DESC;
```

---

## 📊 **General Monitoring & Health Checks**

### **Service Health Checks**
```bash
# Check all running services
docker ps
python3 scripts/run_dev.py status                    # ATS-DEV status
docker-compose -f docker-compose.intg-jobs.yml ps    # ATS-INTG status

# Service endpoints
curl -f http://localhost:3000/health     # ATS-DEV analytics
curl -f http://localhost:4000/health     # ATS-INTG dashboard
curl -f http://localhost:4002/login      # ATS-INTG Grafana
curl -f http://localhost:4091/-/ready    # ATS-INTG Prometheus

# Database connectivity tests
python3 scripts/run_dev.py query --query "SELECT version()"
PGPASSWORD=intg_password pg_isready -h localhost -p 4432 -U postgres -d intg_db

# View logs
docker logs ats-dev-analytics        # ATS-DEV analytics logs
docker logs ats-dev-postgres         # ATS-DEV database logs  
docker logs postgres-intg            # ATS-INTG database logs
docker logs ats-intg-scheduler       # ATS-INTG job scheduler logs
```

### **Performance Monitoring**
```bash
# System performance overview
docker stats --no-stream | grep -E "(ats-dev|intg)"
free -h
df -h /mnt/d/

# Container uptime and restart counts
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.RestartCount}}"

# Storage usage
ls -lah /mnt/d/ats-data/     # Data directory usage
ls -lah /mnt/d/ats-backup/   # Backup directory usage
ls -lah /mnt/d/ats-logs/     # Log directory usage
```

---

## 📅 **CRON JOB MANAGEMENT**

### **🚨 CRITICAL: Two-Stream Market Data Collection Schedule**

```bash
# ===============================================================================
# REAL-TIME INTRADAY COLLECTION (Database Storage) - Every 30 minutes during market hours
# ===============================================================================

# Polygon Real-Time Collection - Every 30 minutes (9:30 AM - 4:00 PM EST)
30,0 9-15 * * 1-5 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/polygon_realtime_collect.py --database >> /mnt/d/ats-logs/polygon-realtime.log 2>&1

# Tiingo Real-Time Collection - Every 30 minutes (9:30 AM - 4:00 PM EST)  
30,0 9-15 * * 1-5 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/tiingo_realtime_collect.py --database >> /mnt/d/ats-logs/tiingo-realtime.log 2>&1

# ===============================================================================
# END-OF-DAY COMPLETE COLLECTION (Parquet Files) - After 7:00 PM daily
# ===============================================================================

# Polygon End-of-Day Complete Minute Bars - 7:30 PM EST daily (after settlement)
30 19 * * 1-5 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/polygon_eod_minute_bars.py --parquet >> /mnt/d/ats-logs/polygon-eod.log 2>&1

# Tiingo End-of-Day Complete Minute Bars - 8:00 PM EST daily (after settlement)
0 20 * * 1-5 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/tiingo_eod_minute_bars.py --parquet >> /mnt/d/ats-logs/tiingo-eod.log 2>&1

# ===============================================================================
# FIRSTRATE DIRECT-TO-PARQUET COLLECTION - Single daily download
# ===============================================================================

# FirstRate Daily Download - 2:30 AM EST/EDT daily (previous trading day data)
30 2 * * * cd /home/jianjun/ats-genai-data && PYTHONPATH=src uv run python scripts/firstrate_daily_download.py --all >> /mnt/d/ats-logs/firstrate-daily.log 2>> /mnt/d/ats-logs/firstrate-daily-error.log

# ===============================================================================
# ATS PLATFORM MAINTENANCE
# ===============================================================================

# ATS Platform Daily Backups
0 2 * * * /home/jianjun/ats-genai-data/scripts/daily_backup_ats_dev.sh        # ATS-DEV backup at 2:00 AM
15 2 * * * /home/jianjun/ats-genai-data/scripts/daily_backup_ats_intg.sh      # ATS-INTG backup at 2:15 AM

# Backup Monitoring  
0 3 * * * /home/jianjun/ats-genai-data/scripts/backup_monitor.sh              # Monitor at 3:00 AM
0 18 * * * /home/jianjun/ats-genai-data/scripts/backup_monitor.sh             # Monitor at 6:00 PM

# WSL System Monitoring (CRITICAL - Added 2025-09-01)
0 * * * * python3 simple_wsl_monitor.py --test >/dev/null 2>&1                # Hourly system status to Slack
@reboot sleep 30 && /home/jianjun/ats-genai-data/restart_monitoring.sh >/dev/null 2>&1  # Auto-restart monitoring on boot
```

### **Cron Job Management Commands**
```bash
# Edit cron jobs
crontab -e

# List all cron jobs
crontab -l

# Remove all cron jobs (DANGEROUS)
crontab -r

# Add new cron job
(crontab -l 2>/dev/null; echo "0 * * * * /path/to/command") | crontab -

# Check cron service status
systemctl status cron
sudo systemctl restart cron

# View cron logs
grep CRON /var/log/syslog | tail -20
journalctl -u cron | tail -20
```

### **Cron Job Best Practices**
- ✅ **Always use absolute paths** for commands and scripts
- ✅ **Redirect output** to log files (`>> /path/to/log 2>&1`)
- ✅ **Set environment variables** when needed (`PYTHONPATH=src`)
- ✅ **Use `/dev/null`** to suppress output for monitoring jobs
- ✅ **Stagger timing** to avoid resource conflicts (2:00, 2:15, 2:30)
- ✅ **Include error handling** and logging in scripts
- ❌ **NEVER use relative paths** or assume working directory
- ❌ **NEVER run without output redirection** (fills up mail spool)

---

## 🚨 **WSL MONITORING & TROUBLESHOOTING**

### **WSL Monitoring Issues (CRITICAL - Fixed 2025-09-01)**
```bash
# Problem: No Slack notifications received
# Root Cause: Monitoring process stopped, no auto-restart configured
# Solution: Active monitoring + cron backup + auto-restart

# Check monitoring status
ps aux | grep simple_wsl_monitor | grep -v grep
# Expected: Should show python3 simple_wsl_monitor.py --hourly process

# If monitoring is DOWN:
/home/jianjun/ats-genai-data/restart_monitoring.sh

# Verify Slack webhook works
cd /home/jianjun/ats-genai-data/scripts/monitoring
python3 simple_wsl_monitor.py --test
# Expected: "✅ Test alert sent successfully!" + Slack notification

# Check monitoring log for errors
tail -50 /mnt/d/ats-logs/wsl_monitor.log
```

### **Cron Job Troubleshooting**
```bash
# Check if cron daemon is running
systemctl status cron
sudo systemctl start cron  # If stopped

# View recent cron execution logs
grep CRON /var/log/syslog | tail -20
journalctl -u cron --since "1 hour ago"

# Test cron job manually
# Extract command from crontab -l and run it directly
/home/jianjun/ats-genai-data/scripts/daily_backup_ats_dev.sh

# Common cron issues and fixes:
# 1. Environment variables missing
env > /tmp/cron_env.txt  # Compare with your shell environment

# 2. Path issues - always use absolute paths
which python3  # Use full path in cron jobs

# 3. Permission issues
ls -la /home/jianjun/ats-genai-data/scripts/  # Check execute permissions

# 4. Output redirection missing
# ❌ BAD: 0 * * * * some_command
# ✅ GOOD: 0 * * * * some_command >> /var/log/command.log 2>&1
```

---

## 🔑 **API Keys & Authentication**

### **Market Data Vendor API Keys**

**✅ AUTOMATED: The ATS platform uses [centralized API key management](API_KEY_MANAGEMENT.md) - no manual setup required.**

| Vendor | Environment Variable | Purpose | Rate Limits | Status |
|--------|---------------------|---------|-------------|---------|
| **EODHD** | `EODHD_API_KEY` | EOD prices, fundamentals, intraday | 20 calls/min | ✅ **Auto-configured** |
| **Polygon** | `POLYGON_API_KEY` | Stock prices, fundamentals, news | 5 calls/min | ✅ **Auto-configured** |
| **Tiingo** | `TIINGO_API_KEY` | Daily prices, fundamentals | 1000 calls/hr | ✅ **Auto-configured** |
| **FMP** | `FMP_API_KEY` | Fundamentals, earnings | 250 calls/day | 📋 Available |
| **Alpha Vantage** | `ALPHA_VANTAGE_API_KEY` | Economic indicators | 25 calls/day | 📋 Available |
| **FirstRate** | `FIRSTRATE_USER_ID` | Minute-level OHLCV (direct feed) | Premium | 📋 Available |

### **API Key Usage (Automatic)**
```bash
# ✅ NO SETUP NEEDED - Keys are managed automatically
python3 scripts/run_dev.py run --script scripts/populate_30year_eodhd_minute_bars.py
python3 scripts/run_dev.py run --script scripts/tiingo_30_year_daily_backfill.py

# ✅ Test centralized key management
python3 scripts/run_dev.py run --script scripts/demo_centralized_keys.py

# 🔧 Override with custom keys (optional)
export EODHD_API_KEY="your-premium-key"
python3 scripts/run_dev.py run --script scripts/populate_30year_eodhd_minute_bars.py
```

**📖 For complete details, see [API Key Management Documentation](API_KEY_MANAGEMENT.md)**

---

## 💾 **Database Management**

### **🚨 CRITICAL: No Automatic Backup Restoration**
- ❌ **NEVER automatically restore from backup** - can cause data loss
- ❌ **NEVER assume backup files are current or valid**
- ✅ **Manual database initialization only** - use proper migration scripts
- ✅ **Fresh database creation** - start with clean schema and populate data as needed

### **Proper Database Initialization**
```bash
# 1. Start fresh PostgreSQL container
python3 scripts/run_dev.py start --service postgres

# 2. Run database migrations to create schema
PYTHONPATH=src python3 -m src.db.create_all_tables

# 3. Populate data using proper scripts (not backups)
python3 scripts/run_dev.py run --script scripts/run_tiingo_bulk.py
python3 scripts/run_dev.py run --script scripts/tiingo_30_year_daily_backfill.py
```

---

## 🔧 **Daily Operations Checklist**

```bash
# Morning health check routine
./scripts/manage_backups.sh status      # Check overnight backups
docker ps | grep -E "(ats-dev|intg)"    # Verify containers running
python3 scripts/run_dev.py status       # Check ATS-DEV health

# Critical: Verify Docker networking is working
docker network inspect ats-network --format "{{.Containers}}" | grep -q "ats-dev-postgres" && echo "✅ Docker networking OK" || echo "❌ Docker networking issue"

# Verify WSL monitoring is active
ps aux | grep simple_wsl_monitor | grep -v grep && echo "✅ WSL monitoring active" || echo "❌ WSL monitoring DOWN - run restart_monitoring.sh"

# Daily minute bars health check (ATS-INTG)
curl -s http://localhost:4080/metrics | grep "ats_daily_minute_backfill"  # Processing stats
docker logs ats-intg-minute-bars-scheduler --tail 20  # Recent processing
ls -la /mnt/d/ats-data/firstrate-data/daily/$(date +%Y/%m/%d)/ | wc -l  # Files count today

# Weekly maintenance
./scripts/manage_backups.sh cleanup     # Clean old backups
docker system prune -f                  # Clean unused containers/images
du -sh /mnt/d/ats-*                     # Check storage usage

# Performance monitoring
docker stats --no-stream | head -10     # Container resource usage
tail -50 /mnt/d/ats-logs/backup-*.log   # Recent backup activity
tail -20 /mnt/d/ats-logs/wsl_monitor.log  # WSL monitoring activity
tail -50 /mnt/d/ats-logs/minute-bars-backfill.log  # Daily minute bars processing
```

### **Monitoring Critical Jobs**
```bash
# Check if FirstRate download ran successfully
tail -50 /mnt/d/ats-logs/firstrate-daily.log
ls -la /mnt/d/ats-data/firstrate-data/

# Verify backup completion
ls -la /mnt/d/ats-backup/ | grep $(date +%Y-%m-%d)
./scripts/manage_backups.sh status

# Confirm WSL monitoring is sending alerts
# (Check Slack #ats-alerts channel for hourly updates)
```

---

## 🆘 **Emergency Recovery**

### **Service Recovery**
```bash
# ATS-DEV Service Recovery
python3 scripts/run_dev.py stop --service analytics
python3 scripts/run_dev.py start --service analytics

# ATS-INTG Service Recovery  
docker-compose -f docker-compose.intg-jobs.yml restart ats-intg-scheduler
docker restart grafana-intg prometheus-intg

# Database restart if needed
docker restart ats-dev-postgres
docker restart postgres-intg
```

### **Data Quality Checks**
```bash
# Check instrument populations
python3 scripts/run_dev.py query --query "
SELECT 'Tiingo' as vendor, COUNT(*) as instruments FROM dev_instrument_tiingo
UNION
SELECT 'EODHD' as vendor, COUNT(*) as instruments FROM dev_instrument_eodhd
UNION  
SELECT 'Polygon' as vendor, COUNT(*) as instruments FROM dev_instrument_polygon
"

# Check data freshness
python3 scripts/run_dev.py query --query "
SELECT vendor, MAX(date) as latest_data, COUNT(*) as records_today
FROM dev_daily_prices 
WHERE date >= CURRENT_DATE - 1
GROUP BY vendor
"
```

---

## 🚨 **Critical Anti-Patterns**
- ❌ **DO NOT** use manual operations for routine tasks
- ❌ **DO NOT** use localhost:port in container DB_HOST configs (use container-name:5432)
- ❌ **DO NOT** assume containers can communicate across different networks
- ❌ **DO NOT** start containers without ensuring network connectivity
- ❌ **DO NOT** mix `run_dev.py` commands with `docker-compose` commands for same environment

---

**📋 For comprehensive database connection details and infrastructure setup, see [INFRASTRUCTURE.md](INFRASTRUCTURE.md)**

*This operations guide covers daily maintenance, monitoring, and troubleshooting procedures for reliable ATS platform operations.*