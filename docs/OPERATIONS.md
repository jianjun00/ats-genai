# 🔧 ATS Operations Guide

**Daily operations, monitoring, troubleshooting, and maintenance for the ATS platform.**

---

## ⚙️ Environment Management

### ATS-DEV Environment
```bash
# Complete environment setup
python3 scripts/run_dev.py setup

# Service management
python3 scripts/run_dev.py start --service postgres
python3 scripts/run_dev.py start --service analytics
python3 scripts/run_dev.py status

# Database operations
python3 scripts/run_dev.py query --query "SELECT version()"
```

### ATS-INTG Environment
```bash
# Start PostgreSQL first
docker-compose -f docker-compose.ats.yml up -d postgres-intg

# Start INTG services
docker-compose -f docker-compose.intg-jobs.yml up -d

# Database operations
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db
```

---

## 🚀 Complete Startup Process

### Single Command Setup
```bash
# Start both environments
./scripts/ats_startup.sh
```

**What it does:**
1. **Clean Environment**: Stops existing services, removes old containers
2. **Database Init**: Starts ATS-DEV (port 3432) and ATS-INTG (port 4432)
3. **Service Health**: Validates database tables and service endpoints
4. **Complete URLs**:
   - ATS-DEV Analytics: http://localhost:3000
   - ATS-INTG Analytics: http://localhost:4000
   - Prometheus Metrics: http://localhost:4080
   - Grafana: http://localhost:4002 (admin/admin)

### Quick Health Check
```bash
curl -f http://localhost:3000/health  # ATS-DEV
curl -f http://localhost:4000/health  # ATS-INTG
curl -f http://localhost:4080/health  # Prometheus
docker ps | grep -E "(ats-dev|intg)"  # Container status
```

---

## ⏰ Complete Cron Schedule

### Daily Automation
```bash
# Complete ATS Platform Cron Configuration
# Install with: crontab scripts/cron/ats-complete-crontab

# 2:00 AM - Database backups
0 2 * * *     ATS-DEV database backup
15 2 * * *    ATS-INTG database backup

# 2:30 AM - FirstRate minute bar downloads
30 2 * * *    FirstRate daily download (stock, etf, fx)
0 8 * * *     FirstRate retry job (if morning failed)

# 9:30 AM - Real-time minute bar collection
30 9 * * 1-5  Start real-time collection (market open)
0 16 * * 1-5  Stop real-time collection (market close)
*/30 9-16 * * 1-5  Health checks during market hours

# 4:00 AM - Data backups
0 1 * * 0     Full snapshot backup (Sundays)
0 4 * * *     Incremental data sync backup
0 5 * * *     Backup cleanup and management

# 6:00 AM - System maintenance
0 6 * * 0     Log rotation (compress large logs)
30 6 * * *    Daily health check (all services)
45 6 * * *    Daily prices validation
```

### Cron Management
```bash
# Install complete configuration
crontab scripts/cron/ats-complete-crontab

# View/edit jobs
crontab -l
crontab -e

# Check cron service
systemctl status cron
journalctl _COMM=cron -f

# Manual health check
./scripts/cron/daily_health_check.sh
tail -50 /mnt/d/ats-logs/health-check.log
```

---

## 📊 Real-Time Minute Bar Collection

### Universe-Based Real-Time Collection
**✅ AUTOMATED: Real-time minute bar collection for high volume large cap universe (877 symbols)**

**Target Universe:** Universe ID 2 (high_volume_large_cap)
- 877 active symbols from major exchanges (NYSE, NASDAQ)
- Includes major stocks: AAPL, MSFT, GOOGL, TSLA, NVDA, etc.
- Data collected from Polygon, Tiingo, and EODHD APIs

### Automated Collection Schedule
```bash
# Market Hours Collection (Monday-Friday)
9:30 AM EST/EDT    - Start real-time collection (market open)
4:00 PM EST/EDT    - Stop real-time collection (market close)

# Health Monitoring
Every 30 minutes   - Health checks during market hours (9:30 AM - 4:00 PM)
11:30 PM daily     - Log cleanup (retain 7 days)
```

### Primary Dashboards

**SigNoz Observability:**
- URL: http://10.0.0.79:4000
- Real-time collector traces and metrics
- Service performance monitoring
- Error tracking and alerting

**Grafana Vendor Monitoring:**
- URL: http://10.0.0.79:4002/d/f9afe708-9be9-4c39-b901-f5c43a0a479f/ats-vendor-monitoring-dashboard-fixed
- Login: admin/admin
- Data Source: PostgreSQL (172.17.0.1:4432/intg_db)
- Auto-refresh: 30s intervals

### Service Management
```bash
# Production service management
./scripts/production_realtime_universe2.sh start     # Start collection
./scripts/production_realtime_universe2.sh stop      # Stop collection
./scripts/production_realtime_universe2.sh status    # Check status
./scripts/production_realtime_universe2.sh logs      # View logs
./scripts/production_realtime_universe2.sh restart   # Restart service

# Check service PID and health
cat /var/run/ats-realtime-universe2.pid
ps -p $(cat /var/run/ats-realtime-universe2.pid 2>/dev/null) 2>/dev/null && echo "✅ Running" || echo "❌ Stopped"
```

### Database Monitoring
```bash
# Check universe configuration
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "
SELECT u.id, u.name, COUNT(um.instrument_id) as symbols
FROM intg_universe u
LEFT JOIN intg_universe_membership um ON u.id = um.universe_id
WHERE u.id = 2 AND (um.end_at IS NULL OR um.end_at > CURRENT_DATE)
GROUP BY u.id, u.name"

# Check collected data by vendor
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "
SELECT
  'polygon' as vendor,
  COUNT(*) as total_records,
  COUNT(DISTINCT symbol) as unique_symbols,
  MAX(created_at) as latest_collection
FROM intg_one_minute_live_polygon
UNION ALL
SELECT
  'tiingo' as vendor,
  COUNT(*) as total_records,
  COUNT(DISTINCT symbol) as unique_symbols,
  MAX(created_at) as latest_collection
FROM intg_one_minute_live_tiingo
UNION ALL
SELECT
  'eodhd' as vendor,
  COUNT(*) as total_records,
  COUNT(DISTINCT symbol) as unique_symbols,
  MAX(created_at) as latest_collection
FROM intg_one_minute_live_eodhd
ORDER BY vendor"

# Latest bars by vendor
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "
SELECT vendor, symbol, timestamp, created_at
FROM (
  SELECT 'polygon' as vendor, symbol, timestamp, created_at FROM intg_one_minute_live_polygon
  UNION ALL
  SELECT 'tiingo' as vendor, symbol, timestamp, created_at FROM intg_one_minute_live_tiingo
  UNION ALL
  SELECT 'eodhd' as vendor, symbol, timestamp, created_at FROM intg_one_minute_live_eodhd
) combined
ORDER BY created_at DESC LIMIT 10"

# Today's collection activity
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "
SELECT
  vendor,
  COUNT(*) as bars_today,
  COUNT(DISTINCT symbol) as symbols_today,
  MIN(created_at) as first_collection,
  MAX(created_at) as last_collection
FROM (
  SELECT 'polygon' as vendor, symbol, created_at FROM intg_one_minute_live_polygon WHERE DATE(created_at) = CURRENT_DATE
  UNION ALL
  SELECT 'tiingo' as vendor, symbol, created_at FROM intg_one_minute_live_tiingo WHERE DATE(created_at) = CURRENT_DATE
  UNION ALL
  SELECT 'eodhd' as vendor, symbol, created_at FROM intg_one_minute_live_eodhd WHERE DATE(created_at) = CURRENT_DATE
) today
GROUP BY vendor
ORDER BY vendor"
```

### Troubleshooting
```bash
# Check service status and logs
./scripts/production_realtime_universe2.sh status
./scripts/production_realtime_universe2.sh logs
tail -50 /var/log/ats-realtime-universe2.log

# API authentication errors (check for 401/403 errors)
tail -100 /var/log/ats-realtime-universe2.log | grep -E "(401|403|ERROR.*API)"

# Database connection issues
PGPASSWORD=intg_password pg_isready -h localhost -p 4432 -U postgres -d intg_db

# Check universe symbol loading
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "
SELECT COUNT(*) as total_symbols
FROM intg_universe_membership um
JOIN intg_instrument i ON um.instrument_id = i.id
WHERE um.universe_id = 2
AND (um.end_at IS NULL OR um.end_at > CURRENT_DATE)
AND i.active = true"

# Rate limiting issues (look for 429 errors)
tail -100 /var/log/ats-realtime-universe2.log | grep -E "(429|rate.*limit|Rate.*limit)"

# Check cron job execution
grep "realtime" /var/log/syslog | tail -10
crontab -l | grep "production_realtime_universe2"

# Manual service recovery
./scripts/production_realtime_universe2.sh stop
sleep 5
./scripts/production_realtime_universe2.sh start

# Emergency kill (if service won't stop)
pkill -f "realtime_minute_collector"
rm -f /var/run/ats-realtime-universe2.pid
```

---

## 📊 General Monitoring

### Service Health Checks
```bash
# Check all services
docker ps
python3 scripts/run_dev.py status

# Service endpoints
curl -f http://localhost:3000/health     # ATS-DEV analytics
curl -f http://localhost:4000/health     # ATS-INTG dashboard
curl -f http://localhost:4091/-/ready    # Prometheus

# Database connectivity
python3 scripts/run_dev.py query --query "SELECT version()"
PGPASSWORD=intg_password pg_isready -h localhost -p 4432 -U postgres -d intg_db

# View logs
docker logs ats-dev-analytics
docker logs ats-dev-postgres
```

### Performance Monitoring
```bash
# System performance
docker stats --no-stream | grep -E "(ats-dev|intg)"
free -h
df -h /mnt/d/

# Container uptime
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.RestartCount}}"

# Storage usage
ls -lah /mnt/d/ats-data/
ls -lah /mnt/d/ats-backup/
ls -lah /mnt/d/ats-logs/
```

---

## 📰 News Ingestion Operations

### News Data Collection System
**Multi-vendor news with 30-day backfill, real-time collection, monitoring**

**Dashboard:** http://10.0.0.79:4002/d/news-monitoring/ats-news-ingestion-dashboard

### News Backfill Operations
```bash
# AUTOMATED - Every 6 hours via Docker
python3 scripts/run_intg.py start --service news-backfill

# Manual execution
python3 scripts/run_intg.py run --script scripts/multi_vendor_news_backfill.py \
    --vendors tiingo,polygon,eodhd --days 30 --debug

# Status and logs
docker logs ats-intg-news-backfill --tail 50
```

### Real-Time News Ingestion
```bash
# AUTOMATED - Continuous via Docker
python3 scripts/run_intg.py start --service news-realtime

# Manual execution
python3 scripts/run_intg.py run --script scripts/realtime_news_ingestion.py \
    --vendors tiingo,polygon,eodhd --interval 300 --daemon

# Monitor
docker logs ats-intg-news-realtime --tail 100
curl -f http://localhost:8081/metrics
```

**Rate Limits:**
- Tiingo: 1-second delays
- Polygon: 12-second delays (5 calls/minute)
- EODHD: 3-second delays
- Collection Cycle: Every 5 minutes

### News System Management
```bash
# Start all news services
./scripts/start_news_ingestion_intg.sh

# Individual service management
python3 scripts/run_intg.py start --service news-realtime
python3 scripts/run_intg.py start --service news-backfill
python3 scripts/run_intg.py start --service news-monitor

# Check status
docker ps | grep ats-intg-news
python3 scripts/run_intg.py status

# Health check
python3 scripts/run_intg.py run --script scripts/news_health_monitor.py
```

### News Troubleshooting
```bash
# No new articles
docker ps | grep ats-intg-news
docker logs ats-intg-news-realtime --tail 50

# Database connection issues
docker exec ats-intg-news-realtime \
    python3 -c "import asyncpg; import asyncio; asyncio.run(asyncpg.connect('postgresql://postgres:intg_password@ats-intg-postgres:5432/intg_db').execute('SELECT 1'))"

# High error rates
python3 scripts/run_intg.py query --query "SELECT vendor, status_code, COUNT(*) FROM intg_news_api_calls WHERE created_at >= NOW() - INTERVAL '1 hour' GROUP BY vendor, status_code"

# Recovery
docker restart ats-intg-news-realtime
python3 scripts/run_intg.py run --script scripts/multi_vendor_news_backfill.py --days 3 --debug
```

---

## 🔑 API Keys & Authentication

### Market Data Vendor API Keys

**✅ AUTOMATED: Centralized API key management - no manual setup required.**

| Vendor | Purpose | Rate Limits | Status |
|--------|---------|-------------|---------|
| **EODHD** | EOD prices, fundamentals | 20 calls/min | ✅ **Auto-configured** |
| **Polygon** | Stock prices, news | 5 calls/min | ✅ **Auto-configured** |
| **Tiingo** | Daily prices, fundamentals | 1000 calls/hr | ✅ **Auto-configured** |

### API Key Usage
```bash
# ✅ VALIDATE keys before operations
python3 scripts/validate_api_keys.py

# ✅ NO SETUP NEEDED - Keys managed automatically
python3 scripts/run_dev.py run --script scripts/populate_30year_eodhd_minute_bars.py

# 🔧 Override with custom keys (optional)
export EODHD_API_KEY="your-premium-key"
python3 scripts/run_dev.py run --script scripts/populate_30year_eodhd_minute_bars.py
```

---

## 📈 FirstRate Daily Data Operations

### FirstRate Data Pipeline Management
**Complete automation for stock, ETF, and FX minute bar data collection**

**Status Dashboard:** Real-time coverage validation and data quality monitoring

### Daily Data Collection & Processing
```bash
# Automated daily jobs (runs via cron at 6:00 AM EST/EDT)
./scripts/cron/firstrate_daily_complete.sh

# Manual execution - complete pipeline
cd /home/jianjun/ats-genai-data && ./scripts/cron/firstrate_daily_complete.sh

# Individual operations
PYTHONPATH=src python3 scripts/firstrate_etf_backfill.py --download-only
PYTHONPATH=src python3 scripts/process_firstrate_etf_zips.py
PYTHONPATH=src python3 scripts/firstrate_quick_coverage_check.py
```

### Daily Job Management
```bash
# Install/manage daily automation
./scripts/cron/install_firstrate_daily_cron.sh          # Install all jobs
./scripts/cron/install_firstrate_daily_cron.sh remove   # Remove all jobs
./scripts/cron/install_firstrate_daily_cron.sh show     # Show current jobs
./scripts/cron/install_firstrate_daily_cron.sh schedule # Show schedule info

# Check cron status
crontab -l | grep -i firstrate                          # View FirstRate jobs
journalctl _COMM=cron -f | grep firstrate               # Monitor cron execution
```

### Data Coverage & Validation
```bash
# Quick coverage check (key symbols only)
PYTHONPATH=src python3 scripts/firstrate_quick_coverage_check.py

# Trading days validation
PYTHONPATH=src python3 scripts/firstrate_trading_days_validation.py --sample

# Weekly comprehensive validation
PYTHONPATH=src python3 scripts/minute_bar_validation.py --days 7 --dry-run
```

### ETF Backfill Operations
```bash
# Critical ETF backfill (30 days)
PYTHONPATH=src python3 scripts/firstrate_etf_backfill.py

# Process downloaded ETF zip files
PYTHONPATH=src python3 scripts/process_firstrate_etf_zips.py

# Check ETF coverage after processing
PYTHONPATH=src python3 scripts/firstrate_quick_coverage_check.py
```

### FirstRate Data Locations
```bash
# Raw downloaded data
ls -la /mnt/d/ats-data/firstrate-data/daily/stock/
ls -la /mnt/d/ats-data/firstrate-data/daily/etf/

# Processed parquet files (monthly structure)
ls -la /mnt/d/ats-data/minute-bars/firstrate/A/AAPL/2025/08/
ls -la /mnt/d/ats-data/minute-bars/firstrate/S/SPY/2025/09/

# Logs and monitoring
tail -f /mnt/d/ats-logs/firstrate-daily-*.log
tail -f /mnt/d/ats-logs/firstrate-coverage.log
```

### FirstRate Daily Schedule
```bash
# Complete cron schedule (runs Monday-Friday)
0 6 * * 1-5    # Daily data download & processing
0 8 * * 1-5    # Coverage validation
0 9 * * 1-5    # Trading days validation
0 7 * * 6      # Weekly comprehensive validation (Saturdays)
0 23 * * *     # Log cleanup (daily)
```

### Troubleshooting FirstRate Jobs
```bash
# Check last job execution
tail -50 /mnt/d/ats-logs/firstrate-daily-cron.log

# Manual job execution (testing)
cd /home/jianjun/ats-genai-data
./scripts/cron/firstrate_daily_complete.sh

# Coverage issues
PYTHONPATH=src python3 scripts/firstrate_quick_coverage_check.py
# Expected: 80%+ coverage for key symbols (AAPL, MSFT, SPY, QQQ, etc.)

# Data quality validation
find /mnt/d/ats-data/minute-bars/firstrate -name "*.parquet" -mtime -1 | wc -l
# Expected: New parquet files from recent processing

# Disk space monitoring
df -h /mnt/d/ats-data/
du -sh /mnt/d/ats-data/firstrate-data/
```

---

## 💾 Database Management

### 🚨 CRITICAL: No Automatic Backup Restoration
- ❌ **NEVER automatically restore** from backup - can cause data loss
- ✅ **Manual initialization only** - use migration scripts
- ✅ **Fresh database creation** - start with clean schema

### Proper Database Initialization
```bash
# 1. Start fresh PostgreSQL
python3 scripts/run_dev.py start --service postgres

# 2. Run migrations
PYTHONPATH=src python3 -m src.db.create_all_tables

# 3. Populate data
python3 scripts/run_dev.py run --script scripts/run_tiingo_bulk.py
python3 scripts/run_dev.py run --script scripts/tiingo_30_year_daily_backfill.py
```

---

## 🔧 Daily Operations Checklist

```bash
# Morning health check
./scripts/manage_backups.sh status
docker ps | grep -E "(ats-dev|intg)"
python3 scripts/run_dev.py status

# Docker networking check
docker network inspect ats-network --format "{{.Containers}}" | grep -q "ats-dev-postgres" && echo "✅ OK" || echo "❌ Issue"

# WSL monitoring check
ps aux | grep simple_wsl_monitor | grep -v grep && echo "✅ Active" || echo "❌ DOWN - run restart_monitoring.sh"

# Minute bars health (ATS-INTG)
curl -s http://localhost:4080/metrics | grep "ats_daily_minute_backfill"
ls -la /mnt/d/ats-data/firstrate-data/daily/$(date +%Y/%m/%d)/ | wc -l

# Real-time collection health
./scripts/production_realtime_universe2.sh status
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "SELECT COUNT(*) FROM intg_one_minute_live_polygon WHERE DATE(created_at) = CURRENT_DATE"

# News ingestion health (ATS-INTG)
curl -s http://localhost:8081/metrics | grep "ats_news"
docker ps | grep ats-intg-news
python3 scripts/run_intg.py run --script scripts/news_health_monitor.py

# Weekly maintenance
./scripts/manage_backups.sh cleanup
docker system prune -f
du -sh /mnt/d/ats-*

# Performance monitoring
docker stats --no-stream | head -10
tail -50 /mnt/d/ats-logs/backup-*.log
```

### Critical Job Monitoring
```bash
# FirstRate download status
tail -50 /mnt/d/ats-logs/firstrate-daily.log
ls -la /mnt/d/ats-data/firstrate-data/

# Backup completion
ls -la /mnt/d/ats-backup/ | grep $(date +%Y-%m-%d)
./scripts/manage_backups.sh status

# News collection operational
docker ps | grep ats-intg-news
python3 scripts/run_intg.py query --query "SELECT vendor, COUNT(*) FROM intg_realtime_news WHERE DATE(published_utc) = CURRENT_DATE GROUP BY vendor"
```

---

## 🆘 Emergency Recovery

### Service Recovery
```bash
# ATS-DEV Service Recovery
python3 scripts/run_dev.py stop --service analytics
python3 scripts/run_dev.py start --service analytics

# ATS-INTG Service Recovery
docker-compose -f docker-compose.intg-jobs.yml restart ats-intg-scheduler
docker restart grafana-intg prometheus-intg

# Database restart
docker restart ats-dev-postgres
docker restart postgres-intg
```

### Data Quality Checks
```bash
# Check instrument populations
python3 scripts/run_dev.py query --query "
SELECT 'Tiingo' as vendor, COUNT(*) as instruments FROM dev_instrument_tiingo
UNION
SELECT 'EODHD' as vendor, COUNT(*) as instruments FROM dev_instrument_eodhd"

# Check data freshness
python3 scripts/run_dev.py query --query "
SELECT vendor, MAX(date) as latest_data, COUNT(*) as records_today
FROM dev_daily_prices
WHERE date >= CURRENT_DATE - 1
GROUP BY vendor"
```

---

## 🚨 Critical Anti-Patterns

- ❌ **DO NOT** use manual operations for routine tasks
- ❌ **DO NOT** use localhost:port in container DB_HOST configs
- ❌ **DO NOT** assume containers can communicate across different networks
- ❌ **DO NOT** start containers without network connectivity
- ❌ **DO NOT** mix `run_dev.py` with `docker-compose` for same environment

---

**📋 For database connections and infrastructure details, see INFRASTRUCTURE.md**

*This operations guide covers daily maintenance, monitoring, and troubleshooting procedures for reliable ATS platform operations.*