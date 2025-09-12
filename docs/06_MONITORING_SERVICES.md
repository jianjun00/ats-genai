# 📊 ATS Monitoring & Services Guide

**Comprehensive monitoring, alerting, service management, and automation for the ATS platform.**

---

## 📈 Monitoring Architecture

### Real-Time Monitoring Stack

**Monitoring Services:**
- **Prometheus**: Metrics collection and storage (port 4080)
- **Grafana**: Visualization dashboards (port 4002, admin/admin)
- **AlertManager**: Alert routing and notification
- **Custom Metrics**: Application-specific monitoring

**Key Monitoring Endpoints:**
```bash
# Prometheus metrics
http://localhost:4080/metrics        # Raw metrics endpoint
http://localhost:4080/api/v1/query   # Prometheus query API
http://localhost:4091/-/ready        # Prometheus server ready check

# Grafana dashboards
http://localhost:4002/               # Main dashboard interface
http://localhost:4002/api/health     # Grafana health check

# Custom application metrics
http://localhost:4000/health         # ATS analytics health
http://localhost:3000/health         # ATS dev analytics health
```

### Grafana Dashboards

**Available Dashboards:**
1. **ATS Vendor Monitoring**: Real-time API performance, rate limits, error rates
   - URL: http://localhost:4002/d/f9afe708-9be9-4c39-b901-f5c43a0a479f
   - Metrics: API response times, success rates, quota usage

2. **Database Usage**: PostgreSQL performance, connection pools, query performance
   - Tables: Query execution times, connection counts, buffer hit ratios

3. **Code Usage Analytics**: Development metrics, deployment frequency
   - Metrics: Commit frequency, test coverage, build success rates

4. **Batch Jobs Monitoring**: Data processing job status and performance
   - Jobs: Training data generation, daily backfills, system maintenance

### Custom Metrics Collection

**Application Metrics:**
```bash
# ATS-specific metrics
curl -s http://localhost:4080/metrics | grep "ats_"

# Example metrics:
# ats_api_requests_total
# ats_database_connections_active
# ats_training_jobs_duration_seconds
# ats_data_quality_score
# ats_vendor_api_calls_total
```

**Database Metrics:**
```bash
# PostgreSQL metrics
curl -s http://localhost:4080/metrics | grep "pg_"

# Example metrics:
# pg_up{instance="ats-intg-postgres:5432"}
# pg_database_size_bytes{datname="intg_db"}
# pg_stat_database_tup_returned{datname="intg_db"}
```

---

## ⚡ Service Management & Autostart

### Automatic Service Startup

**Complete ATS Autostart Service:**
```bash
# Install autostart service (runs on system boot)
sudo cp scripts/ats-autostart.service /etc/systemd/system/
sudo systemctl enable ats-autostart.service
sudo systemctl start ats-autostart.service

# Check autostart status
sudo systemctl status ats-autostart.service

# View autostart logs
sudo journalctl -u ats-autostart.service -f
```

**What the autostart service does:**
1. **Environment Check**: Verifies Docker is running, required volumes exist
2. **Database Startup**: Starts ATS-DEV (port 3432) and ATS-INTG (port 4432) PostgreSQL
3. **Core Services**: Launches analytics, monitoring, and API services
4. **Health Validation**: Confirms all services are responsive
5. **Dependency Management**: Ensures correct startup order and networking

### Manual Service Control

**ATS-DEV Services:**
```bash
# Start all dev services
python scripts/run_dev.py setup

# Individual service control
python scripts/run_dev.py start --service postgres
python scripts/run_dev.py start --service analytics
python scripts/run_dev.py stop --service analytics
python scripts/run_dev.py restart --service postgres

# Service status
python scripts/run_dev.py status
```

**ATS-INTG Services:**
```bash
# Start integration environment
docker-compose -f docker-compose.ats.yml up -d postgres-intg
docker-compose -f docker-compose.intg-jobs.yml up -d

# Individual service control
python scripts/run_intg.py start --service analytics
python scripts/run_intg.py start --service realtime-minute-collector
python scripts/run_intg.py start --service news-realtime

# Service status
python scripts/run_intg.py status
```

### Service Health Monitoring

**Health Check Commands:**
```bash
# Quick health verification
curl -f http://localhost:3000/health  # ATS-DEV analytics
curl -f http://localhost:4000/health  # ATS-INTG analytics
curl -f http://localhost:4080/health  # Prometheus metrics
curl -f http://localhost:4002/api/health  # Grafana

# Container status
docker ps | grep -E "(ats-dev|intg)"

# Database connectivity
python scripts/run_dev.py query --query "SELECT 1"
python scripts/run_intg.py query --query "SELECT 1"
```

**Service Discovery:**
```bash
# Get all running ATS services with ports
docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Status}}" | grep ats

# Network connectivity check
docker network inspect ats-network --format "{{range .Containers}}{{.Name}} {{.IPv4Address}} {{end}}"
docker network inspect ats-intg-network --format "{{range .Containers}}{{.Name}} {{.IPv4Address}} {{end}}"
```

---

## ⏰ Automated Operations - Complete Cron Schedule

### Production Cron Configuration

**Complete ATS Platform Cron Jobs:**
```bash
# Install complete cron configuration
crontab scripts/cron/ats-complete-crontab

# View current cron jobs
crontab -l

# Edit cron jobs
crontab -e
```

**Daily Automation Schedule:**
```cron
# 2:00 AM - Database backups
0 2 * * *     /home/jianjun/ats-genai-data/scripts/daily_backup_ats_dev.sh
15 2 * * *    /home/jianjun/ats-genai-data/scripts/daily_backup_ats_intg.sh

# 2:30 AM - FirstRate minute bar downloads
30 2 * * *    /home/jianjun/ats-genai-data/scripts/firstrate_daily_download.py
0 8 * * *     /home/jianjun/ats-genai-data/scripts/firstrate_daily_download.py --retry

# 4:00 AM - Data backups and sync
0 1 * * 0     /home/jianjun/ats-genai-data/scripts/full_snapshot_ats_data.sh  # Sundays
0 4 * * *     /home/jianjun/ats-genai-data/scripts/incremental_sync_ats_data.sh
0 5 * * *     /home/jianjun/ats-genai-data/scripts/manage_ats_data_backups.sh

# 6:00 AM - System maintenance
0 6 * * 0     /home/jianjun/ats-genai-data/scripts/weekly_maintenance.py    # Log rotation
30 6 * * *    /home/jianjun/ats-genai-data/scripts/cron/daily_health_check.sh
45 6 * * *    /home/jianjun/ats-genai-data/scripts/daily_prices_validation.py

# Real-time data collection (every 5 minutes during market hours)
*/5 9-16 * * 1-5  /home/jianjun/ats-genai-data/scripts/realtime_minute_collector.py

# News collection (every 15 minutes)
*/15 * * * *      /home/jianjun/ats-genai-data/scripts/realtime_news_ingestion.py

# Hourly system health checks
0 * * * *         /home/jianjun/ats-genai-data/scripts/monitoring/simple_wsl_monitor.py
```

### Cron Job Management

**Monitor Cron Execution:**
```bash
# Check cron service status
sudo systemctl status cron

# View cron logs
sudo tail -f /var/log/cron
grep "ats-genai" /var/log/syslog

# Test cron job manually
/home/jianjun/ats-genai-data/scripts/cron/daily_health_check.sh
```

**Cron Job Health Monitoring:**
```bash
# Create cron job monitoring
cat > /home/jianjun/ats-genai-data/scripts/cron/monitor_cron_jobs.sh << 'EOF'
#!/bin/bash
# Monitor critical cron job execution
LOGFILE="/var/log/ats-cron-monitor.log"
DATE=$(date '+%Y-%m-%d %H:%M:%S')

echo "[$DATE] Checking cron job health..." >> $LOGFILE

# Check if daily backup ran successfully
if [ $(find /mnt/d/ats-backup -name "*.sql" -mtime -1 | wc -l) -gt 0 ]; then
    echo "[$DATE] ✅ Daily backup successful" >> $LOGFILE
else
    echo "[$DATE] ❌ Daily backup failed" >> $LOGFILE
fi

# Check data collection
if [ $(find /mnt/d/ats-data -name "*.parquet" -mtime -1 | wc -l) -gt 0 ]; then
    echo "[$DATE] ✅ Data collection successful" >> $LOGFILE
else
    echo "[$DATE] ❌ Data collection failed" >> $LOGFILE
fi
EOF

chmod +x /home/jianjun/ats-genai-data/scripts/cron/monitor_cron_jobs.sh

# Add to cron to run every hour
echo "0 * * * * /home/jianjun/ats-genai-data/scripts/cron/monitor_cron_jobs.sh" | crontab -
```

---

## 🔔 Alerting & Notifications

### Slack Integration

**Configure Slack Alerts:**
```bash
# Setup Slack webhook for alerts
./scripts/setup_slack_alerts.sh

# Configure alert rules
cp config/monitoring/alert_rules.yml /etc/prometheus/
sudo systemctl reload prometheus
```

**Alert Rule Examples:**
```yaml
# /etc/prometheus/alert_rules.yml
groups:
- name: ats_alerts
  rules:
  - alert: ATS_ServiceDown
    expr: up{job="ats-analytics"} == 0
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "ATS Analytics service is down"

  - alert: ATS_DatabaseConnections
    expr: pg_stat_database_numbackends > 80
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "High database connection count: {{ $value }}"

  - alert: ATS_DataQuality
    expr: ats_data_quality_score < 0.9
    for: 15m
    labels:
      severity: warning
    annotations:
      summary: "Data quality score below threshold: {{ $value }}"
```

### Custom Health Checks

**Daily Health Check Script:**
```bash
#!/bin/bash
# /home/jianjun/ats-genai-data/scripts/cron/daily_health_check.sh

LOGFILE="/var/log/ats-health-check.log"
DATE=$(date '+%Y-%m-%d %H:%M:%S')
HEALTHY=true

echo "[$DATE] Starting ATS health check..." >> $LOGFILE

# Check database connectivity
if python scripts/run_dev.py query --query "SELECT 1" > /dev/null 2>&1; then
    echo "[$DATE] ✅ ATS-DEV database: HEALTHY" >> $LOGFILE
else
    echo "[$DATE] ❌ ATS-DEV database: FAILED" >> $LOGFILE
    HEALTHY=false
fi

# Check service endpoints
if curl -f http://localhost:3000/health > /dev/null 2>&1; then
    echo "[$DATE] ✅ ATS-DEV analytics: HEALTHY" >> $LOGFILE
else
    echo "[$DATE] ❌ ATS-DEV analytics: FAILED" >> $LOGFILE
    HEALTHY=false
fi

# Check disk space
DISK_USAGE=$(df /mnt/d | tail -1 | awk '{print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -gt 90 ]; then
    echo "[$DATE] ⚠️  Disk usage high: ${DISK_USAGE}%" >> $LOGFILE
    HEALTHY=false
else
    echo "[$DATE] ✅ Disk usage: ${DISK_USAGE}%" >> $LOGFILE
fi

# Send summary
if [ "$HEALTHY" = true ]; then
    echo "[$DATE] 🎉 All systems healthy" >> $LOGFILE
else
    echo "[$DATE] 🚨 System issues detected" >> $LOGFILE
    # Send Slack alert if webhook configured
    if [ -f "/home/jianjun/.ats-slack-webhook" ]; then
        curl -X POST -H 'Content-type: application/json' \
             --data '{"text":"🚨 ATS Health Check Failed - See logs for details"}' \
             $(cat /home/jianjun/.ats-slack-webhook)
    fi
fi
```

---

## 📊 Performance Monitoring

### System Resource Monitoring

**Real-time Resource Usage:**
```bash
# Monitor container resources
docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}"

# Monitor disk space
df -h /mnt/d  # ATS data storage
df -h /home   # System storage

# Monitor database performance
python scripts/run_dev.py query --query "
SELECT schemaname, tablename, n_tup_ins, n_tup_upd, n_tup_del
FROM pg_stat_user_tables
ORDER BY n_tup_ins DESC LIMIT 10
"
```

**Performance Metrics Collection:**
```bash
# Collect system metrics
python scripts/monitoring/collect_system_metrics.py

# Performance profiling
python -m cProfile -o performance.stats your_script.py
python -c "
import pstats
p = pstats.Stats('performance.stats')
p.sort_stats('cumulative').print_stats(10)
"

# Memory profiling
python -m memory_profiler your_script.py
```

### Database Performance Monitoring

**Query Performance Analysis:**
```bash
# Slow query monitoring
python scripts/run_dev.py query --query "
SELECT query, calls, total_time, mean_time, stddev_time
FROM pg_stat_statements
ORDER BY total_time DESC LIMIT 10
"

# Connection monitoring
python scripts/run_dev.py query --query "
SELECT count(*) as active_connections,
       max(now() - query_start) as longest_query
FROM pg_stat_activity
WHERE state = 'active'
"

# Database size monitoring
python scripts/run_dev.py query --query "
SELECT schemaname, tablename,
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
"
```

---

## 🚨 Incident Response

### Emergency Procedures

**Service Recovery:**
```bash
# Emergency service restart
sudo systemctl stop ats-autostart
docker stop $(docker ps -q --filter "name=ats")
docker rm $(docker ps -aq --filter "name=ats")

# Clean restart
./scripts/ats_startup.sh

# Verify recovery
curl -f http://localhost:3000/health
curl -f http://localhost:4000/health
```

**Database Recovery:**
```bash
# Database backup restoration
sudo systemctl stop ats-autostart

# Restore from latest backup
pg_restore -h localhost -p 3432 -U postgres -d dev_db /mnt/d/ats-backup/latest-dev-backup.sql

# Restart services
sudo systemctl start ats-autostart
```

**Data Pipeline Recovery:**
```bash
# Restart data collection services
python scripts/run_intg.py restart --service realtime-minute-collector
python scripts/run_intg.py restart --service news-realtime

# Verify data flow
python scripts/run_intg.py query --query "
SELECT MAX(created_at) as latest_data
FROM intg_minute_bars
WHERE created_at >= CURRENT_DATE
"
```

### Troubleshooting Runbook

**Common Issues & Solutions:**

1. **Service Connection Failures:**
```bash
# Symptom: "Connection refused" errors
# Cause: Services on different Docker networks
# Fix: Ensure containers use correct network
docker inspect <container> | grep NetworkMode
docker network connect ats-network <container>
```

2. **Database Connection Issues:**
```bash
# Symptom: Database connection timeouts
# Cause: Connection pool exhaustion
# Fix: Check and restart database service
python scripts/run_dev.py query --query "SELECT count(*) FROM pg_stat_activity"
python scripts/run_dev.py restart --service postgres
```

3. **Disk Space Issues:**
```bash
# Symptom: "No space left on device"
# Cause: Log files or data accumulation
# Fix: Clean up old files
find /mnt/d/ats-logs -name "*.log" -mtime +30 -delete
find /mnt/d/ats-backup -name "*.sql" -mtime +90 -delete
```

---

## 📋 Maintenance Procedures

### Weekly Maintenance

**Automated Weekly Tasks:**
```bash
# Run weekly maintenance script
./scripts/weekly_maintenance.py

# What it does:
# - Compress and archive old log files
# - Clean up temporary data files
# - Update system metrics
# - Generate weekly health report
# - Optimize database indexes
```

**Manual Weekly Checks:**
```bash
# Review system health trends
open http://localhost:4002/d/system-health

# Check error logs for patterns
grep -E "(ERROR|CRITICAL)" /mnt/d/ats-logs/*.log | tail -50

# Validate data integrity
python scripts/validate_data_integrity.py --full-check

# Update API keys if needed
python scripts/validate_api_keys.py
```

### Monthly Maintenance

**Monthly Tasks:**
```bash
# Database maintenance
python scripts/run_dev.py query --query "VACUUM ANALYZE"
python scripts/run_dev.py query --query "REINDEX DATABASE dev_db"

# Backup validation
python scripts/validate_backups.py --test-restore

# Security updates
sudo apt update && sudo apt upgrade -y
pip install --upgrade -r requirements.txt

# Performance review
python scripts/generate_performance_report.py --period 30d
```

---

**🎯 This monitoring and services guide ensures robust, automated operation of the ATS platform with comprehensive observability and incident response capabilities.**