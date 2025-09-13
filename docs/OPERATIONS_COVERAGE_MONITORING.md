# ATS Data Coverage Monitoring - Operations Runbook

## 🎯 Overview

This runbook provides comprehensive operational guidance for the ATS Data Coverage Monitoring System. The system tracks daily prices and minute bar coverage across multiple vendors, identifies gaps requiring backfill, and provides real-time alerting and visualization.

## 🏗️ System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Monitoring    │    │   Alerting &    │
│                 │    │     System      │    │  Visualization  │
│ • FirstRate     │───▶│                 │───▶│                 │
│ • Polygon       │    │ • Coverage      │    │ • Slack Alerts  │
│ • Tiingo        │    │   Monitor       │    │ • Grafana       │
│ • FMP           │    │ • Gap Detection │    │ • Dashboard     │
│ • EODHD         │    │ • Priority      │    │ • Prometheus    │
└─────────────────┘    │   Scoring       │    │   Metrics       │
                       └─────────────────┘    └─────────────────┘
                                │
                       ┌─────────────────┐
                       │   PostgreSQL    │
                       │    Database     │
                       │                 │
                       │ • Coverage Data │
                       │ • Gap Queue     │
                       │ • Metrics       │
                       │ • Priorities    │
                       └─────────────────┘
```

## 🚀 Quick Start

### 1. Initial Setup

```bash
# Clone and navigate to project
cd /home/jianjun/ats-genai-pm

# Set up monitoring cron jobs
./scripts/setup_coverage_monitoring_cron.sh

# Configure environment
# Edit .env.monitoring with your settings
vim .env.monitoring

# Set up Grafana monitoring stack
./scripts/setup_grafana_monitoring.sh

# Start monitoring stack
./scripts/start_monitoring_stack.sh
```

### 2. Verification

```bash
# Check service health
curl http://localhost:8080/health    # Coverage Dashboard
curl http://localhost:3000/api/health # Grafana
curl http://localhost:9090/-/healthy  # Prometheus

# View logs
tail -f logs/monitoring/daily_monitoring_$(date +%Y%m%d).log
```

## 📊 Core Components

### 1. Coverage Monitor (`src/monitoring/coverage_monitor.py`)

**Purpose**: Scans data sources, tracks coverage, detects gaps
**Key Methods**:
- `scan_coverage_data()` - Main scanning routine
- `detect_gaps()` - Gap detection algorithm
- `calculate_gap_priority()` - Priority scoring

**Configuration**:
```python
# Environment variables
DB_HOST=localhost
DB_PORT=4432
DB_USER=postgres
DB_PASSWORD=intg_password
DB_NAME=intg_db
```

### 2. Alert System (`src/monitoring/alert_system.py`)

**Purpose**: Sends notifications for coverage issues
**Alert Types**:
- Coverage below 85% (warning) / 70% (critical)
- High priority gaps (priority ≥ 7)
- Stale data (> 24 hours old)

**Slack Configuration**:
```bash
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
SLACK_CHANNEL=#ats-data-alerts
SLACK_USERNAME=ATS Coverage Monitor
```

### 3. Prometheus Exporter (`src/monitoring/prometheus_exporter.py`)

**Purpose**: Exports metrics for Grafana visualization
**Key Metrics**:
- `ats_data_coverage_percentage` - Overall coverage
- `ats_data_gaps_total` - Total gaps
- `ats_data_gaps_high_priority` - Priority gaps
- `ats_data_coverage_by_symbol` - Symbol-specific coverage

### 4. Web Dashboard (`coverage_dashboard_fixed.py`)

**Purpose**: Real-time web interface for coverage monitoring
**Features**:
- Coverage trending charts
- Gap prioritization table
- Symbol status overview
- Auto-refresh every 30 seconds

## 🗄️ Database Schema

### Core Tables

#### `dev_data_coverage_tracking`
Primary coverage tracking table
```sql
CREATE TABLE dev_data_coverage_tracking (
    tracking_id SERIAL PRIMARY KEY,
    vendor VARCHAR(50) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    trading_date DATE NOT NULL,
    coverage_status VARCHAR(20) NOT NULL,
    data_quality_score DECIMAL(5,2),
    record_count INTEGER,
    file_path TEXT,
    file_size_bytes BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### `dev_coverage_gaps`
Actionable gap queue
```sql
CREATE TABLE dev_coverage_gaps (
    gap_id SERIAL PRIMARY KEY,
    vendor VARCHAR(50) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    gap_start_date DATE NOT NULL,
    gap_end_date DATE NOT NULL,
    gap_days INTEGER NOT NULL,
    priority_score INTEGER NOT NULL,
    backfill_status VARCHAR(20) DEFAULT 'pending',
    estimated_effort_minutes INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Key Views

#### `v_current_coverage_summary`
Real-time coverage summary by vendor/type
```sql
SELECT vendor, data_type, 
       AVG(CASE WHEN coverage_status = 'complete' THEN 100.0 ELSE 0.0 END) as coverage_percentage,
       COUNT(*) as total_symbols,
       SUM(CASE WHEN coverage_status = 'complete' THEN 1 ELSE 0 END) as symbols_complete
FROM dev_data_coverage_tracking
WHERE trading_date = CURRENT_DATE - INTERVAL '1 day'
GROUP BY vendor, data_type;
```

#### `v_active_backfill_queue`
Priority-ordered backfill queue
```sql
SELECT g.*, ps.priority_level, ps.business_impact,
       (g.priority_score * ps.priority_level) as adjusted_priority
FROM dev_coverage_gaps g
LEFT JOIN dev_priority_symbols ps ON g.symbol = ps.symbol
WHERE g.backfill_status = 'pending'
ORDER BY adjusted_priority DESC;
```

## ⚙️ Configuration

### Environment Variables

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=4432
DB_USER=postgres
DB_PASSWORD=intg_password
DB_NAME=intg_db

# Alert Configuration
ALERT_EMAIL_ENABLED=false
ALERT_SLACK_ENABLED=true
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
SLACK_CHANNEL=#ats-data-alerts

# Dashboard Configuration
DASHBOARD_PORT=8080
DASHBOARD_HOST=localhost

# Monitoring Paths
FIRSTRATE_DATA_PATH=/mnt/d/ats-data/minute-bars/firstrate
POLYGON_DATA_PATH=/mnt/d/ats-data/minute-bars/polygon
```

### Cron Schedule

```bash
# Daily comprehensive monitoring at 6:00 AM
0 6 * * * /home/jianjun/ats-genai-pm/scripts/run_daily_coverage_monitoring.sh

# Hourly alert checks during business hours
0 8-18 * * * /home/jianjun/ats-genai-pm/scripts/run_hourly_alert_check.sh

# Dashboard health check every 15 minutes
*/15 * * * * /home/jianjun/ats-genai-pm/scripts/check_dashboard_health.sh

# Prometheus metrics export every 5 minutes
*/5 * * * * PYTHONPATH=/home/jianjun/ats-genai-pm/src python3 /home/jianjun/ats-genai-pm/src/monitoring/prometheus_exporter.py
```

## 🔧 Daily Operations

### Morning Checklist (6:30 AM)

1. **Check Alert Summary**
   ```bash
   # Check overnight alerts
   grep -i "critical\|error" logs/monitoring/daily_monitoring_$(date +%Y%m%d).log
   
   # Review Slack #ats-data-alerts channel
   ```

2. **Dashboard Review**
   ```bash
   # Open dashboard
   open http://localhost:8080
   
   # Check key metrics:
   # - Overall coverage % (target: >95%)
   # - High priority gaps (target: <5)
   # - Data freshness (target: <1 day)
   ```

3. **Priority Gap Review**
   ```sql
   -- Check high priority gaps
   SELECT symbol, gap_start_date, gap_end_date, priority_score, urgency_level
   FROM v_recent_gaps
   WHERE urgency_level IN ('critical', 'high')
   ORDER BY priority_score DESC
   LIMIT 10;
   ```

### Weekly Operations (Monday 8:00 AM)

1. **Coverage Trend Analysis**
   ```sql
   -- Check 7-day coverage trend
   SELECT vendor, data_type, 
          AVG(coverage_percentage) as avg_coverage,
          MIN(coverage_percentage) as min_coverage,
          MAX(coverage_percentage) as max_coverage
   FROM dev_daily_coverage_metrics
   WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'
   GROUP BY vendor, data_type
   ORDER BY avg_coverage ASC;
   ```

2. **Backfill Performance Review**
   ```sql
   -- Review completed backfills
   SELECT vendor, data_type,
          COUNT(*) as backfills_completed,
          AVG(EXTRACT(hours FROM completed_at - started_at)) as avg_hours
   FROM dev_backfill_operations
   WHERE completed_at >= CURRENT_DATE - INTERVAL '7 days'
   GROUP BY vendor, data_type;
   ```

3. **System Health Check**
   ```bash
   # Check service status
   docker ps | grep -E "(prometheus|grafana|coverage)"
   
   # Check disk usage
   df -h /mnt/d/ats-data
   
   # Check log sizes
   du -sh logs/monitoring/
   ```

## 🚨 Troubleshooting

### Common Issues

#### 1. Coverage Dashboard Not Responding

**Symptoms**: HTTP 500 errors, connection timeouts
**Diagnosis**:
```bash
# Check dashboard process
ps aux | grep coverage_dashboard_fixed.py

# Check logs
tail -f logs/monitoring/dashboard_health_$(date +%Y%m%d).log

# Test database connectivity
PYTHONPATH=src python3 -c "
from monitoring.coverage_monitor import CoverageMonitor
import asyncio
async def test():
    monitor = CoverageMonitor()
    try:
        conn = await monitor.get_db_connection()
        print('✅ Database connection successful')
        await conn.close()
    except Exception as e:
        print(f'❌ Database connection failed: {e}')
asyncio.run(test())
"
```

**Resolution**:
```bash
# Restart dashboard
pkill -f coverage_dashboard_fixed.py
nohup python3 coverage_dashboard_fixed.py --port 8080 > logs/dashboard.log 2>&1 &

# Check database connection
# Verify environment variables
env | grep -E "(DB_HOST|DB_PORT|DB_USER|DB_PASSWORD|DB_NAME)"
```

#### 2. No Slack Alerts Received

**Symptoms**: Critical gaps detected but no Slack notifications
**Diagnosis**:
```bash
# Check alert system logs
grep -i slack logs/monitoring/hourly_alerts_$(date +%Y%m%d).log

# Test webhook manually
curl -X POST -H 'Content-type: application/json' \
    --data '{"text":"Test message from ATS monitoring"}' \
    $SLACK_WEBHOOK_URL
```

**Resolution**:
```bash
# Verify Slack configuration
echo "SLACK_WEBHOOK_URL: $SLACK_WEBHOOK_URL"
echo "SLACK_CHANNEL: $SLACK_CHANNEL"

# Test alert system
PYTHONPATH=src python3 -c "
from monitoring.alert_system import AlertManager
import asyncio
async def test():
    alert_manager = AlertManager()
    print('Alert manager configuration:')
    print(f'Slack enabled: {alert_manager.alert_config[\"slack_enabled\"]}')
    print(f'Webhook URL set: {bool(alert_manager.slack_config[\"webhook_url\"])}')
asyncio.run(test())
"
```

#### 3. Prometheus Metrics Not Updating

**Symptoms**: Grafana shows stale data, metrics file not updated
**Diagnosis**:
```bash
# Check metrics export cron
crontab -l | grep prometheus_exporter

# Check metrics file
ls -la /tmp/ats_coverage_metrics.prom
cat /tmp/ats_coverage_metrics.prom | head -20

# Test manual export
PYTHONPATH=src python3 src/monitoring/prometheus_exporter.py
```

**Resolution**:
```bash
# Manually run metrics export
PYTHONPATH=src python3 src/monitoring/prometheus_exporter.py

# Check cron service
systemctl status cron

# Verify file permissions
chmod 644 /tmp/ats_coverage_metrics.prom
```

#### 4. High Memory Usage

**Symptoms**: System slow, OOM errors in logs
**Diagnosis**:
```bash
# Check memory usage
free -h
ps aux --sort=-%mem | head -10

# Check database connections
PYTHONPATH=src python3 -c "
import asyncpg
import asyncio
async def check():
    pool = await asyncpg.create_pool(
        host='localhost', port=4432, user='postgres', 
        password='intg_password', database='intg_db',
        min_size=1, max_size=1
    )
    print(f'Pool size: {pool.get_size()}')
    await pool.close()
asyncio.run(check())
"
```

**Resolution**:
```bash
# Restart services with lower memory footprint
docker-compose -f docker-compose.monitoring.yml restart

# Reduce database pool sizes in code
# Check for memory leaks in long-running processes
```

### Database Issues

#### Schema Migration Problems

```bash
# Check current schema version using migration manager
PYTHONPATH=src python3 src/infrastructure/database/migration_manager.py version --db-url "postgresql://postgres:intg_password@localhost:4432/intg_db"

# Run migrations using migration manager
PYTHONPATH=src python3 src/infrastructure/database/migration_manager.py migrate --environment intg --db-url "postgresql://postgres:intg_password@localhost:4432/intg_db"

# For ats-dev environment
PYTHONPATH=src python3 src/infrastructure/database/migration_manager.py migrate --environment dev --db-url "postgresql://postgres:dev_password@localhost:3432/dev_db"

# Apply coverage monitoring schema manually if needed
psql -h localhost -p 4432 -U postgres -d intg_db -f src/infrastructure/database/migrations/coverage_monitoring_schema.sql
```

#### Performance Issues

```sql
-- Check table sizes
SELECT schemaname, tablename, 
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE tablename LIKE 'dev_%'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Check slow queries
SELECT query, calls, total_time, mean_time
FROM pg_stat_statements
WHERE query LIKE '%dev_%'
ORDER BY total_time DESC
LIMIT 10;

-- Analyze table statistics
ANALYZE dev_data_coverage_tracking;
ANALYZE dev_coverage_gaps;
ANALYZE dev_daily_coverage_metrics;
```

## 📈 Performance Optimization

### Database Optimization

```sql
-- Create additional indexes for common queries
CREATE INDEX CONCURRENTLY idx_coverage_tracking_symbol_date 
ON dev_data_coverage_tracking(symbol, trading_date DESC);

CREATE INDEX CONCURRENTLY idx_gaps_priority_status 
ON dev_coverage_gaps(priority_score DESC, backfill_status);

-- Partition large tables by date
CREATE TABLE dev_data_coverage_tracking_2024 
PARTITION OF dev_data_coverage_tracking
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
```

### Application Optimization

```python
# Optimize database connection pooling
# In coverage_monitor.py
async def optimize_db_pool(self):
    self.db_pool = await asyncpg.create_pool(
        **self.db_config,
        min_size=2,     # Reduce minimum connections
        max_size=8,     # Limit maximum connections
        max_inactive_connection_lifetime=300  # 5 minutes
    )
```

## 📊 Monitoring Metrics

### Key Performance Indicators (KPIs)

1. **Coverage Percentage** (Target: >95%)
   - Overall coverage across all vendors
   - Coverage by data type (daily_prices, minute_bars)
   - Coverage by priority symbols

2. **Gap Resolution Time** (Target: <24 hours)
   - Time from gap detection to backfill completion
   - Backfill success rate

3. **Data Freshness** (Target: <1 day)
   - Days since last data update
   - Data delay by vendor

4. **System Health**
   - Dashboard uptime (Target: >99%)
   - Alert response time (Target: <5 minutes)
   - Database query performance (Target: <100ms)

### Alert Thresholds

```python
ALERT_THRESHOLDS = {
    'critical_coverage': 70.0,    # Coverage below 70%
    'warning_coverage': 85.0,     # Coverage below 85%
    'critical_gaps': 20,          # More than 20 high priority gaps
    'warning_gaps': 5,            # More than 5 high priority gaps
    'stale_data_hours': 24,       # Data older than 24 hours
    'gap_age_hours': 48           # Gaps unresolved for 48+ hours
}
```

## 🔄 Maintenance Schedule

### Daily (Automated)
- Coverage data scanning (6:00 AM)
- Gap detection and prioritization
- Alert generation
- Metrics export to Prometheus

### Weekly (Manual)
- Coverage trend analysis
- Backfill performance review
- System health check
- Log rotation and cleanup

### Monthly (Manual)
- Database maintenance (VACUUM, ANALYZE)
- Performance optimization review
- Alert threshold tuning
- Documentation updates

## 🛡️ Security Considerations

### Database Security
```sql
-- Create monitoring-specific user
CREATE USER ats_monitor WITH PASSWORD 'secure_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ats_monitor;
GRANT INSERT, UPDATE ON dev_coverage_gaps TO ats_monitor;
GRANT INSERT ON dev_backfill_operations TO ats_monitor;
```

### Network Security
```bash
# Restrict dashboard access
iptables -A INPUT -p tcp --dport 8080 -s 10.0.0.0/8 -j ACCEPT
iptables -A INPUT -p tcp --dport 8080 -j DROP

# Secure Slack webhook
# Store webhook URL in encrypted environment file
gpg --cipher-algo AES256 --compress-algo 1 --s2k-mode 3 \
    --s2k-digest-algo SHA512 --s2k-count 65536 --symmetric .env.monitoring
```

## 📞 Support Contacts

### Escalation Matrix

| Severity | Response Time | Contact |
|----------|---------------|---------|
| Critical (Coverage <70%) | 15 minutes | Primary On-call |
| High (Coverage <85%) | 1 hour | Secondary On-call |
| Medium (System issues) | 4 hours | Team Lead |
| Low (Enhancement requests) | Next business day | Team |

### Contact Information
- **Primary On-call**: [Your primary contact]
- **Secondary On-call**: [Your secondary contact]  
- **Team Lead**: [Team lead contact]
- **Slack Channel**: #ats-data-alerts
- **Emergency Email**: ats-monitoring@company.com

---

## 📚 Additional Resources

- [Database Schema Documentation](../src/db/migrations/coverage_monitoring_schema.sql)
- [Grafana Dashboard Configuration](../grafana/ats-coverage-dashboard.json)
- [Prometheus Alert Rules](../grafana/ats_coverage_alerts.yml)
- [System Validation Script](../scripts/validate_monitoring_system.sh)

**Last Updated**: $(date +%Y-%m-%d)
**Document Version**: 1.0
**Maintained By**: ATS Platform Team