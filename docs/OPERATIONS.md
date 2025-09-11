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
- SigNoz Observability: http://10.0.0.79:4000
- Daily Minute Bars: /mnt/d/ats-data/firstrate-data/daily/
```

### **⚡ Quick Health Check**
```bash
# Verify both environments are operational
curl -f http://localhost:3000/health  # ATS-DEV analytics
curl -f http://localhost:4000/health  # ATS-INTG analytics
curl -f http://localhost:4080/health  # ATS-INTG prometheus metrics
curl -f http://10.0.0.79:4000         # SigNoz observability dashboard
docker ps | grep -E "(ats-dev|intg)"  # Container status

# Daily minute bars system health
curl -s http://localhost:4080/metrics | grep "ats_daily_minute_backfill"  # Minute bars metrics
ls -la /mnt/d/ats-data/firstrate-data/daily/$(date +%Y/%m/%d)/  # Today's files
tail -50 /mnt/d/ats-logs/minute-bars-backfill.log  # Recent processing activity
```

---

## ⏰ **Complete ATS Cron Schedule**

### **Daily Automation Overview**
```bash
# Complete ATS Platform Cron Configuration
# Install with: crontab scripts/cron/ats-complete-crontab

# 2:00 AM - Database backups
0 2 * * *     ATS-DEV database backup
15 2 * * *    ATS-INTG database backup

# 2:30 AM - FirstRate minute bar downloads
30 2 * * *    FirstRate daily download (stock, etf, fx)
0 8 * * *     FirstRate retry job (if morning failed)

# 4:00 AM - Data backups
0 1 * * 0     Full snapshot backup (Sundays)
0 4 * * *     Incremental data sync backup
0 5 * * *     Backup cleanup and management

# 6:00 AM - System maintenance
0 6 * * 0     Log rotation (compress large logs)
30 6 * * *    Daily health check (all services)
45 6 * * *    Daily prices validation (90-day analysis)
```

### **Cron Job Management**
```bash
# Install complete ATS cron configuration
crontab scripts/cron/ats-complete-crontab

# View current cron jobs
crontab -l

# Edit cron jobs
crontab -e

# Check cron service status
systemctl status cron

# View cron execution logs
journalctl _COMM=cron -f
sudo tail -f /var/log/cron    # varies by distribution
```

### **Daily Health Monitoring**
```bash
# Manual health check (runs daily at 6:30 AM)
./scripts/cron/daily_health_check.sh

# View health check history
tail -50 /mnt/d/ats-logs/health-check.log

# Health check components:
# ✅ ATS-DEV/INTG service endpoints
# ✅ Database connections (dev/intg)
# ✅ FirstRate daily downloads
# ✅ Backup system status
# ✅ Disk space monitoring
# ✅ Docker container status
```

## 📊 **Real-Time Minute Bar Collection Monitoring**

### **🎯 Primary Dashboards**

**SigNoz Observability Platform:**
```bash
🌐 URL: http://10.0.0.79:4000
📊 Real-time collector traces and metrics
🔧 Service performance monitoring
📈 Error tracking and alerting
🚀 OpenTelemetry integration
```

**Grafana Vendor Monitoring:**
```bash
🌐 URL: http://10.0.0.79:4002/d/f9afe708-9be9-4c39-b901-f5c43a0a479f/ats-vendor-monitoring-dashboard-fixed
📊 Login: admin/admin (change on first login)
🔧 Data Source: ATS-INTG-PostgreSQL (172.17.0.1:4432/intg_db) + Prometheus metrics
📈 Auto-refresh: 30s intervals

# ⚠️ CRITICAL: Grafana Data Source Configuration Fix
# If dashboard shows "no data" or connection errors:
# 1. Ensure PostgreSQL started with: docker-compose -f docker-compose.ats.yml up -d postgres-intg
# 2. Connect networks: docker network connect ats-network ats-grafana-intg
# 3. Update data source to use: 172.17.0.1:4432 (Docker bridge gateway)
#    - URL: http://10.0.0.79:4002/datasources/edit/2
#    - Host: 172.17.0.1:4432
#    - Database: intg_db
#    - User: postgres
#    - Password: intg_password
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

### **🚀 Real-Time Minute Bar Collector Service**

**Service Overview:**
```bash
# Service: ats-intg-realtime-minute-collector
# Container: Runs on ats-intg-network
# Database: ats-intg-postgres (port 5432 internal, 4432 external)
# Monitoring: SigNoz integration at http://10.0.0.79:4000
# Collection: Every minute for Polygon, Tiingo, EODHD
```

**Service Management:**
```bash
# Check service status
python3 scripts/run_intg.py status
docker ps | grep realtime-minute-collector

# View real-time logs
docker logs ats-intg-realtime-minute-collector --tail 20 -f

# Restart service
docker restart ats-intg-realtime-minute-collector

# Stop/Start service
docker stop ats-intg-realtime-minute-collector
docker start ats-intg-realtime-minute-collector
```

**Database Tables:**
```bash
# Check collected data from each vendor
python3 scripts/run_intg.py query --query "SELECT COUNT(*) FROM intg_one_minute_live_polygon"
python3 scripts/run_intg.py query --query "SELECT COUNT(*) FROM intg_one_minute_live_tiingo"
python3 scripts/run_intg.py query --query "SELECT COUNT(*) FROM intg_one_minute_live_eodhd"

# View latest collected bars
python3 scripts/run_intg.py query --query "
  SELECT vendor, symbol, timestamp, created_at
  FROM (
    SELECT 'polygon' as vendor, symbol, timestamp, created_at FROM intg_one_minute_live_polygon
    UNION ALL
    SELECT 'tiingo' as vendor, symbol, timestamp, created_at FROM intg_one_minute_live_tiingo
    UNION ALL
    SELECT 'eodhd' as vendor, symbol, timestamp, created_at FROM intg_one_minute_live_eodhd
  ) combined
  ORDER BY created_at DESC
  LIMIT 10"
```

**SigNoz Monitoring Features:**
- **Service Traces**: Collection cycle performance
- **Custom Metrics**: Bars collected, API errors, collection duration
- **Error Tracking**: Vendor-specific API failures
- **Performance Monitoring**: Collection latency and throughput
- **Environment Tagging**: INTG environment identification

**Troubleshooting Common Issues:**
```bash
# Issue 1: API authentication errors (401/403)
# Check API keys are properly set in container environment
docker inspect ats-intg-realtime-minute-collector | grep -A 20 "Env"

# Issue 2: Database connection issues
# Verify container is on correct network and can reach postgres
docker exec ats-intg-realtime-minute-collector ping ats-intg-postgres

# Issue 3: No data being collected (422 errors)
# EODHD 422 errors are common during non-market hours - this is expected
# Check logs for actual error details
docker logs ats-intg-realtime-minute-collector --tail 50 | grep -E "(ERROR|422|401|403)"

# Issue 4: Container health issues
# Check if container needs restart due to memory or connection issues
docker stats ats-intg-realtime-minute-collector
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
# NEWS INGESTION AUTOMATION (Docker-based)
# ===============================================================================

# News Backfill (30 days) - Every 6 hours via Docker container
# Managed by: docker ps | grep ats-intg-news-backfill
# Start: python3 scripts/run_intg.py start --service news-backfill
# Script: scripts/multi_vendor_news_backfill.py --vendors tiingo,polygon,eodhd --days 30

# Real-Time News Ingestion - Continuous 24/7 operation via Docker container
# Managed by: docker ps | grep ats-intg-news-realtime
# Start: python3 scripts/run_intg.py start --service news-realtime
# Script: scripts/realtime_news_ingestion.py --vendors tiingo,polygon,eodhd --interval 300 --daemon

# News Health Monitoring - Every 2 hours via Docker container
# Managed by: docker ps | grep ats-intg-news-monitor
# Start: python3 scripts/run_intg.py start --service news-monitor
# Script: scripts/news_health_monitor.py

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

## 📰 **NEWS INGESTION OPERATIONS**

### **🎯 News Data Collection System**
**Multi-vendor news ingestion with 30-day backfill, real-time collection, and comprehensive monitoring**

```bash
🌐 Dashboard: http://10.0.0.79:4002/d/news-monitoring/ats-news-ingestion-dashboard
📊 Login: admin/admin (Grafana)
🔧 Data Sources: ATS-INTG-PostgreSQL + Prometheus metrics
📈 Auto-refresh: 1-minute intervals for real-time monitoring
```

### **📥 News Backfill Operations**

**Daily 30-Day Backfill (Automated):**
```bash
# AUTOMATED - Runs every 6 hours via Docker service
python3 scripts/run_intg.py start --service news-backfill

# Manual execution for testing or recovery
python3 scripts/run_intg.py run --script scripts/multi_vendor_news_backfill.py \
    --vendors tiingo,polygon,eodhd \
    --days 30 \
    --debug

# Status and logs
docker logs ats-intg-news-backfill --tail 50
python3 scripts/run_intg.py logs --service news-backfill
```

**Manual Comprehensive Backfill (90 days):**
```bash
# Manual execution for major data recovery or initial setup
python3 scripts/run_intg.py run --script scripts/multi_vendor_news_backfill.py \
    --vendors tiingo,polygon,eodhd \
    --days 90 \
    --debug
```

### **📡 Real-Time News Ingestion**

**Real-Time Collection Service:**
```bash
# AUTOMATED - Runs continuously via Docker service
python3 scripts/run_intg.py start --service news-realtime

# Manual execution for development/testing
python3 scripts/run_intg.py run --script scripts/realtime_news_ingestion.py \
    --vendors tiingo,polygon,eodhd \
    --interval 300 \
    --daemon

# Service monitoring
docker logs ats-intg-news-realtime --tail 100
curl -f http://localhost:8081/metrics
```

**Rate Limits & Collection Intervals:**
- **Tiingo**: 1-second delays between requests
- **Polygon**: 12-second delays (5 calls/minute limit)
- **EODHD**: 3-second delays between requests
- **Collection Cycle**: Every 5 minutes (300 seconds)

### **📊 News Event Collection Monitoring**

**🚀 PRIMARY DASHBOARD: SigNoz** 
```bash
# Main observability platform
🌐 URL: http://localhost:8080/dashboard
📊 Search: "ATS-INTG News Ingestion Dashboard"

# News service status
docker ps | grep ats-intg-news-realtime
docker logs ats-intg-news-realtime --tail 20
```

**📈 Key Metrics Available:**
```bash
# OpenTelemetry metrics in SigNoz
news_articles_fetched_total{vendor="polygon"}         # Articles fetched from API
news_articles_stored_total{vendor="polygon"}          # Articles stored to database  
news_api_calls_total{vendor="polygon",success="true"} # API call success/failure
news_api_errors_total{vendor="polygon"}               # API errors encountered
news_api_response_duration_ms{vendor="polygon"}       # API response time
news_ingestion_cycle_duration_ms                      # Complete cycle timing
news_data_freshness_minutes{vendor="polygon"}         # Data freshness tracking
```

**⚠️ CRITICAL ALERTS:**
- **Service Down**: News ingestion service stopped (2min threshold)
- **Data Stale**: No new data for >3 hours (5min threshold)  
- **High Error Rate**: API errors >10% (5min threshold)
- **Slow Performance**: Ingestion cycles >30 seconds (10min threshold)

**📖 Complete Documentation**: [NEWS_MONITORING.md](NEWS_MONITORING.md)

**Database Queries for Status:**
```bash
# Connect to ATS-INTG database
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db

# Current news data by vendor
SELECT vendor, COUNT(*) as articles, MAX(published_utc) as latest_article
FROM intg_realtime_news
GROUP BY vendor
ORDER BY latest_article DESC;

# Today's news collection activity
SELECT vendor, COUNT(*) as articles_today,
       AVG(sentiment_score) as avg_sentiment
FROM intg_realtime_news
WHERE DATE(published_utc) = CURRENT_DATE
GROUP BY vendor;

# API call status for news endpoints
SELECT vendor, status_code, COUNT(*) as calls,
       AVG(response_time_ms)::int as avg_response_ms
FROM intg_news_api_calls
WHERE created_at >= CURRENT_DATE - INTERVAL '24 hours'
GROUP BY vendor, status_code
ORDER BY vendor, calls DESC;
```

### **🔧 News System Management**

**Management Interface:**
```bash
# Start all news services
./scripts/start_news_ingestion_intg.sh

# Stop all news services
./scripts/stop_news_ingestion_intg.sh

# Individual service management via run_intg.py
python3 scripts/run_intg.py start --service news-realtime    # Real-time ingestion
python3 scripts/run_intg.py start --service news-backfill    # Daily backfill
python3 scripts/run_intg.py start --service news-monitor     # Health monitoring

# Check system status
python3 scripts/run_intg.py status
docker ps | grep ats-intg-news

# View service logs
docker logs ats-intg-news-realtime --tail 50
docker logs ats-intg-news-backfill --tail 50
docker logs ats-intg-news-monitor --tail 50

# Manual backfill execution
python3 scripts/run_intg.py run --script scripts/multi_vendor_news_backfill.py --days 7 --vendors tiingo,polygon

# Health check and diagnostics
python3 scripts/run_intg.py run --script scripts/news_health_monitor.py
```

### **🏥 News Health Monitoring**

**Automated Health Checks:**
```bash
# AUTOMATED - Runs every 2 hours via Docker service
python3 scripts/run_intg.py start --service news-monitor

# Manual health assessment
python3 scripts/run_intg.py run --script scripts/news_health_monitor.py

# Health check output includes:
# - Vendor-specific collection rates and freshness
# - API response times and error rates
# - Data quality scores and recommendations
# - Slack alerts for critical issues (if configured)
```

**Health Metrics & Thresholds:**
- **Critical Freshness**: >60 minutes without new articles
- **Warning Freshness**: >30 minutes without new articles
- **Low Collection Rate**: <2 articles/hour during business hours
- **API Errors**: >20% error rate in recent calls
- **Overall Health Score**: 0.0-1.0 (weighted by freshness, volume, errors)

### **🚨 News System Troubleshooting**

**Common Issues & Solutions:**
```bash
# Issue 1: No new articles being collected
# Check real-time ingestion service status
docker ps | grep ats-intg-news
docker logs ats-intg-news-realtime --tail 50

# Verify API keys are working
python3 scripts/run_intg.py run --script scripts/multi_vendor_news_backfill.py --days 1 --vendors tiingo --debug

# Issue 2: Parsing errors in news collection
# Common with Tiingo API - check logs for null value handling
docker logs ats-intg-news-realtime --tail 100 | grep -i error

# Issue 3: Database connection issues
# Verify PostgreSQL connectivity from container network
docker exec ats-intg-news-realtime \
    python3 -c "import asyncpg; import asyncio; asyncio.run(asyncpg.connect('postgresql://postgres:intg_password@ats-intg-postgres:5432/intg_db').execute('SELECT 1'))"

# Issue 4: High API error rates
# Check database for recent API call errors
python3 scripts/run_intg.py query --query "SELECT vendor, status_code, COUNT(*) FROM intg_news_api_calls WHERE created_at >= NOW() - INTERVAL '1 hour' GROUP BY vendor, status_code"
```

**Manual Recovery Procedures:**
```bash
# Restart real-time collection service
docker restart ats-intg-news-realtime

# Force manual backfill for missing data
python3 scripts/run_intg.py run --script scripts/multi_vendor_news_backfill.py \
    --days 3 \
    --vendors tiingo,polygon \
    --debug

# Clear and rebuild news metrics
python3 scripts/run_intg.py query --query "DELETE FROM intg_news_collection_metrics WHERE DATE(created_at) = CURRENT_DATE"
python3 scripts/run_intg.py run --script scripts/news_health_monitor.py
```

### **📈 News Data Quality Monitoring**

**Data Quality Checks:**
```bash
# Article completeness and quality metrics
SELECT vendor,
       COUNT(*) as total_articles,
       COUNT(CASE WHEN title IS NOT NULL AND LENGTH(title) > 10 THEN 1 END) as articles_with_title,
       COUNT(CASE WHEN summary IS NOT NULL AND LENGTH(summary) > 50 THEN 1 END) as articles_with_summary,
       COUNT(CASE WHEN sentiment_score IS NOT NULL THEN 1 END) as articles_with_sentiment,
       AVG(CASE WHEN sentiment_score IS NOT NULL THEN sentiment_score END) as avg_sentiment
FROM intg_realtime_news
WHERE DATE(published_utc) >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY vendor;

# Duplicate detection effectiveness
SELECT vendor,
       COUNT(*) as total_processed,
       COUNT(DISTINCT article_id) as unique_articles,
       (COUNT(*) - COUNT(DISTINCT article_id)) as duplicates_removed
FROM intg_realtime_news
WHERE DATE(published_utc) >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY vendor;
```

### **🔄 News System Automation (Docker)**

**Deployed Docker Services:**
```bash
# View all news-related Docker containers
docker ps | grep ats-intg-news

# Service schedules:
# - news-backfill: Continuous every 6 hours
# - news-realtime: Continuous 24/7 operation (5-minute cycles)
# - news-monitor: Continuous every 2 hours

# Service endpoints:
# - Real-time metrics: http://localhost:8081/metrics
# - Database access: python3 scripts/run_intg.py query --query "..."
```

**Resource Requirements:**
- **Daily Backfill**: Moderate CPU/RAM usage (runs every 6 hours)
- **Real-time Ingestion**: Low continuous usage (5-minute cycles)
- **Health Monitoring**: Minimal usage (runs every 2 hours)

---

## 🔑 **API Keys & Authentication**

### **Market Data Vendor API Keys**

**✅ AUTOMATED: The ATS platform uses centralized API key management - no manual setup required.**

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
# ✅ VALIDATE keys before operations
python3 scripts/validate_api_keys.py

# ✅ NO SETUP NEEDED - Keys are managed automatically
python3 scripts/run_dev.py run --script scripts/populate_30year_eodhd_minute_bars.py
python3 scripts/run_dev.py run --script scripts/tiingo_30_year_daily_backfill.py

# 🔧 Override with custom keys (optional)
export EODHD_API_KEY="your-premium-key"
python3 scripts/run_dev.py run --script scripts/populate_30year_eodhd_minute_bars.py
```

**📖 For complete API key management, troubleshooting, and emergency procedures, see [CLAUDE.md - API Key Management](../CLAUDE.md#-api-key-management---single-source-of-truth)**

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

# News ingestion health check (ATS-INTG)
curl -s http://localhost:8081/metrics | grep "ats_news"  # News collection metrics
docker ps | grep ats-intg-news  # News service status
python3 scripts/run_intg.py run --script scripts/news_health_monitor.py  # News system health assessment

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

# Verify news collection is operational
docker ps | grep ats-intg-news  # Check news service status
curl -s http://localhost:8081/metrics | head -10  # Check metrics endpoint
python3 scripts/run_intg.py query --query "SELECT vendor, COUNT(*) as articles_today FROM intg_realtime_news WHERE DATE(published_utc) = CURRENT_DATE GROUP BY vendor"  # Today's articles
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