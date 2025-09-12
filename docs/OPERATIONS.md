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
# Check service status
python3 scripts/run_intg.py status
docker ps | grep realtime-minute-collector

# View logs
docker logs ats-intg-realtime-minute-collector --tail 20 -f

# Restart service
docker restart ats-intg-realtime-minute-collector
```

### Database Monitoring
```bash
# Check collected data
python3 scripts/run_intg.py query --query "SELECT COUNT(*) FROM intg_one_minute_live_polygon"
python3 scripts/run_intg.py query --query "SELECT COUNT(*) FROM intg_one_minute_live_tiingo"

# Latest bars
python3 scripts/run_intg.py query --query "
SELECT vendor, symbol, timestamp, created_at
FROM (
  SELECT 'polygon' as vendor, symbol, timestamp, created_at FROM intg_one_minute_live_polygon
  UNION ALL
  SELECT 'tiingo' as vendor, symbol, timestamp, created_at FROM intg_one_minute_live_tiingo
) combined
ORDER BY created_at DESC LIMIT 10"
```

### Troubleshooting
```bash
# API authentication errors
docker inspect ats-intg-realtime-minute-collector | grep -A 20 "Env"

# Database connection issues
docker exec ats-intg-realtime-minute-collector ping ats-intg-postgres

# Check error logs
docker logs ats-intg-realtime-minute-collector --tail 50 | grep -E "(ERROR|422|401|403)"
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