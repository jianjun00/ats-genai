# ⚙️ Operations Guide

**DevOps, Monitoring, Infrastructure Management, and Troubleshooting**

Complete operations guide consolidating all DevOps procedures, monitoring setup, infrastructure management, and troubleshooting resources.

---

## 🎯 Operations Overview

This guide covers all operational aspects of the ATS platform including Docker container management, monitoring and alerting, automated backup systems, security operations, and comprehensive troubleshooting procedures.

### **Core Responsibilities**
- **Infrastructure Management** - Docker containers, networking, persistent storage
- **Monitoring & Alerting** - Prometheus, Grafana, automated backup monitoring
- **Security Operations** - Container access control, database security
- **Backup & Recovery** - Automated daily backups, disaster recovery procedures
- **Performance Optimization** - Container resource tuning, database optimization
- **Incident Response** - Service health checks, log analysis, recovery procedures

---

## 🚀 Environment Startup & Management

### **ATS-DEV Environment (Development)**
**🔧 Management Method: `run_dev.py` script (uses individual Docker commands)**

```bash
# Start complete ATS-DEV environment
python3 scripts/run_dev.py setup

# Start individual services (uses docker run commands)
python3 scripts/run_dev.py start --service postgres    # PostgreSQL database
python3 scripts/run_dev.py start --service analytics   # Analytics service

# Stop individual services (uses docker stop commands)
python3 scripts/run_dev.py stop --service analytics
python3 scripts/run_dev.py stop --service postgres

# Check environment status
python3 scripts/run_dev.py status
docker ps | grep ats-dev

# Database operations
python3 scripts/run_dev.py query --query "SELECT version()"
python3 scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices"

# Run tests
python3 scripts/run_dev.py test
```

**ATS-DEV Database Configuration:**
- **Host**: `localhost`
- **Port**: `5432`
- **Database Name**: `dev_db`
- **Username**: `postgres`
- **Password**: *(no password required)*
- **Connection String**: `postgresql://postgres@localhost:5432/dev_db`
- **Container**: `ats-dev-postgres` (PostgreSQL 13)
- **Data Files**: Docker volume `postgres-data-new` → `/var/lib/postgresql/data` (container)

**ATS-DEV Service Configuration:**
- **Analytics**: `http://localhost:3000` (container: `ats-dev-analytics`)
- **Management**: Individual Docker containers via `run_dev.py`

### **ATS-INTG Environment (Integration)**
**🔧 Management Method: Docker Compose (orchestrates multiple services)**

```bash
# Start complete ATS-INTG environment with Docker Compose
docker-compose -f docker-compose.intg-jobs.yml up -d

# Stop complete ATS-INTG environment
docker-compose -f docker-compose.intg-jobs.yml down

# Start monitoring stack (separate containers)
docker run -d --name prometheus-intg -p 9091:9090 prom/prometheus:latest
docker run -d --name grafana-intg -p 3002:3000 -e "GF_SECURITY_ADMIN_PASSWORD=ats-intg-monitoring" grafana/grafana:latest

# Check environment status
docker-compose -f docker-compose.intg-jobs.yml ps
docker ps | grep intg

# Database operations (direct connection)
PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db
PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db -c "SELECT version()"
```

**ATS-INTG Database Configuration:**
- **Host**: `localhost`
- **Port**: `5434`
- **Database Name**: `intg_db`
- **Username**: `postgres`
- **Password**: `intg_password`
- **Connection String**: `postgresql://postgres:intg_password@localhost:5434/intg_db`
- **Container**: `postgres-intg` (PostgreSQL 13)
- **Data Files**: Docker volume `ats-genai-data_postgres_intg_data` → `/var/lib/postgresql/data` (container)

**ATS-INTG Service Configuration:**
- **Scheduler**: Background job processing (container: `ats-intg-scheduler`)
- **Monitoring**: Grafana at `http://localhost:3002` (admin/ats-intg-monitoring)
- **Metrics**: Prometheus at `http://localhost:9091` (no authentication)
- **Management**: Docker Compose orchestration for multiple services

### **Data File Locations & Persistent Storage**

**ATS-DEV Data Locations:**
```bash
# Database data files (PostgreSQL)
Docker Volume: postgres-data-new
Container Path: /var/lib/postgresql/data
Access: docker exec ats-dev-postgres ls -la /var/lib/postgresql/data
Inspect: docker volume inspect postgres-data-new

# Application data (mounted in containers)
Host Path: /mnt/d/ats-data/
Container Path: /data (available in application containers)
Subdirs: checkpoints/, config/, db/, firstrate-data/, logs/, minute-bars/, polygon/, reports/

# Backup files
Host Path: /mnt/d/ats-backup/dev/
Pattern: daily_backup_YYYYMMDD_HHMMSS.sql
Latest: /mnt/d/ats-backup/dev/latest_daily_backup.sql (symlink)
Current: ls -la /mnt/d/ats-backup/dev/

# Log files
Host Path: /mnt/d/ats-logs/
Files: backup-dev.log, analytics-dev.log
Current: ls -la /mnt/d/ats-logs/
```

**ATS-INTG Data Locations:**
```bash
# Database data files (PostgreSQL)
Docker Volume: ats-genai-data_postgres_intg_data
Container Path: /var/lib/postgresql/data
Host Volume Path: /var/snap/docker/common/var-lib-docker/volumes/ats-genai-data_postgres_intg_data/_data
Access: docker volume inspect ats-genai-data_postgres_intg_data
Container Access: docker exec postgres-intg ls -la /var/lib/postgresql/data

# Application data
Host Path: /mnt/d/ats-data/intg/
Container Path: /data (mounted in scheduler container)

# Backup files
Host Path: /mnt/d/ats-backup/intg/
Pattern: daily_backup_YYYYMMDD_HHMMSS.sql
Latest: /mnt/d/ats-backup/intg/latest_daily_backup.sql (symlink)

# Log files
Host Path: /mnt/d/ats-logs/intg/
Files: backup-intg.log, scheduler.log
```

### **Quick Reference - Database Connections**

| Environment | Host | Port | Database | Username | Password | Connection String |
|-------------|------|------|----------|----------|----------|-------------------|
| **ATS-DEV** | localhost | 5432 | dev_db | postgres | *(none)* | `postgresql://postgres@localhost:5432/dev_db` |
| **ATS-INTG** | localhost | 5434 | intg_db | postgres | intg_password | `postgresql://postgres:intg_password@localhost:5434/intg_db` |

### **Quick Reference - Data Locations**

| Environment | Database Files | Application Data | Backup Files | Log Files |
|-------------|----------------|------------------|--------------|-----------|
| **ATS-DEV** | Docker volume: `postgres-data-new` | `/mnt/d/ats-data/` | `/mnt/d/ats-backup/dev/` | `/mnt/d/ats-logs/` |
| **ATS-INTG** | Docker volume: `ats-genai-data_postgres_intg_data` | `/mnt/d/ats-data/intg/` | `/mnt/d/ats-backup/intg/` | `/mnt/d/ats-logs/intg/` |

### **Connection Examples**
```bash
# ATS-DEV Database Access
python3 scripts/run_dev.py query --query "SELECT version()"
docker exec ats-dev-postgres psql -U postgres -d dev_db

# ATS-INTG Database Access  
PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db
PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db -c "SELECT version()"

# Service Health Checks
curl -f http://localhost:3000/health     # ATS-DEV analytics
curl -f http://localhost:3002/login      # ATS-INTG Grafana
curl -f http://localhost:9091/-/ready    # ATS-INTG Prometheus
```

## ⚡ **SERVICE MANAGEMENT SUMMARY**

### **🚨 CRITICAL: DO NOT MIX MANAGEMENT METHODS**

### **🎯 Clear Management Rules:**

**ATS-DEV (Development):**
- **Method**: `run_dev.py` script 
- **Technology**: Individual Docker containers
- **Analytics Service**: `python3 scripts/run_dev.py start --service analytics`
- **Stop Analytics**: `python3 scripts/run_dev.py stop --service analytics`

**ATS-INTG (Integration):**  
- **Method**: Docker Compose
- **Technology**: Orchestrated container stack
- **All Services**: `docker-compose -f docker-compose.intg-jobs.yml up -d`
- **Stop All**: `docker-compose -f docker-compose.intg-jobs.yml down`

**Monitoring (ATS-INTG):**
- **Method**: Individual Docker containers (separate from main stack)
- **Start**: Individual `docker run` commands for Prometheus/Grafana
- **Stop**: `docker stop prometheus-intg grafana-intg`

**❌ DO NOT:**
- Use `docker run` for ATS-INTG services (use Docker Compose)
- Use Docker Compose for ATS-DEV services (use `run_dev.py`)
- Mix `run_dev.py` commands with `docker-compose` commands

### **Common Operations**
```bash
# Check all running services
docker ps
python3 scripts/run_dev.py status                    # ATS-DEV status
docker-compose -f docker-compose.intg-jobs.yml ps    # ATS-INTG status

# View logs
docker logs ats-dev-analytics        # ATS-DEV analytics logs
docker logs ats-dev-postgres         # ATS-DEV database logs  
docker logs postgres-intg            # ATS-INTG database logs
docker logs ats-intg-scheduler       # ATS-INTG job scheduler logs

# Database health checks
python3 scripts/run_dev.py query --query "SELECT COUNT(*) as dev_instruments FROM dev_instrument_tiingo"
PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'intg_%'"

# Backup operations (automated system)
./scripts/manage_backups.sh status    # Check backup health
./scripts/manage_backups.sh run-dev   # Manual ATS-DEV backup
./scripts/manage_backups.sh run-intg  # Manual ATS-INTG backup
```

---

## 📊 Monitoring & Alerting

### **Current Monitoring Setup**

**ATS-DEV Environment:**
- **Basic Health**: Check via `python3 scripts/run_dev.py status`
- **Application Logs**: `docker logs ats-dev-analytics`
- **Database Logs**: `docker logs ats-dev-postgres`
- **Database Health**: `python3 scripts/run_dev.py query --query "SELECT version()"`

**ATS-INTG Environment:**
- **Grafana Dashboard**: `http://localhost:3002` (admin/ats-intg-monitoring)
- **Prometheus Metrics**: `http://localhost:9091` (no authentication)
- **Database Health**: `PGPASSWORD=intg_password pg_isready -h localhost -p 5434 -U postgres -d intg_db`
- **Container Status**: `docker ps | grep intg`

### **Backup Monitoring**
```bash
# Automated daily backup system (runs via cron)
./scripts/manage_backups.sh status    # Overall backup health
./scripts/manage_backups.sh logs      # Recent backup activity
./scripts/manage_backups.sh cleanup   # Clean old backups (7+ days)

# Manual backup operations
./scripts/manage_backups.sh run-dev   # Backup ATS-DEV now
./scripts/manage_backups.sh run-intg  # Backup ATS-INTG now
./scripts/manage_backups.sh run-all   # Backup both environments

# Backup schedule (crontab -l)
# 0 2 * * * - ATS-DEV backup (2:00 AM daily)
# 15 2 * * * - ATS-INTG backup (2:15 AM daily)
# 0 3 * * * - Backup monitoring (3:00 AM daily)
# 0 18 * * * - Backup monitoring (6:00 PM daily)
```

### **Setting Up Monitoring Exporters**
```bash
# Add PostgreSQL metrics to Prometheus (optional)
docker run -d --name postgres-exporter-dev \
  --network host \
  -e DATA_SOURCE_NAME="postgresql://postgres@localhost:5432/dev_db?sslmode=disable" \
  prometheuscommunity/postgres-exporter:latest

docker run -d --name postgres-exporter-intg \
  --network host \
  -e DATA_SOURCE_NAME="postgresql://postgres:intg_password@localhost:5434/intg_db?sslmode=disable" \
  prometheuscommunity/postgres-exporter:latest

# Configure Prometheus to scrape exporters (add to prometheus.yml)
# - job_name: 'postgres-dev'
#   static_configs:
#     - targets: ['localhost:9187']
# - job_name: 'postgres-intg'  
#   static_configs:
#     - targets: ['localhost:9188']
```

### **Docker Container Health Monitoring**
```bash
# Check container health
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Container resource usage
docker stats --no-stream

# Service-specific health checks
curl -f http://localhost:3000/health  # ATS-DEV analytics
curl -f http://localhost:3002/health  # ATS-INTG Grafana
curl -f http://localhost:9091/-/ready # ATS-INTG Prometheus

# Database connectivity tests
python3 scripts/run_dev.py query --query "SELECT version()"
PGPASSWORD=intg_password pg_isready -h localhost -p 5434 -U postgres -d intg_db
```

### **Automated Monitoring & Alerting**
```bash
# Backup monitoring (runs automatically via cron)
./scripts/manage_backups.sh status    # Check backup health
tail -20 /mnt/d/ats-logs/backup-*.log  # View backup logs
cat /tmp/backup_alerts.txt             # View recent alerts

# Manual health checks
docker exec ats-dev-postgres pg_isready -U postgres
docker exec postgres-intg pg_isready -U postgres

# Service performance monitoring
docker logs ats-dev-analytics --tail=50
docker logs ats-intg-scheduler --tail=50
```

### **Grafana Dashboard Setup**
```bash
# Access Grafana for ATS-INTG
# URL: http://localhost:3002
# Login: admin / ats-intg-monitoring

# Add Prometheus data source:
# 1. Go to Configuration → Data Sources
# 2. Add Prometheus source: http://localhost:9091
# 3. Test connection

# Import common dashboards:
# - Docker Container Metrics (Dashboard ID: 193)
# - PostgreSQL Database (Dashboard ID: 9628)
# - System Metrics (Dashboard ID: 1860)

# Custom ATS metrics queries:
# - Container uptime: up{job="docker"}
# - Database connections: postgres_stat_database_numbackends
# - Backup success rate: increase(backup_completed_total[24h])
```

---

## 🔐 Security Operations

### **Environment Security**
```bash
# Database credentials are configured in Docker Compose files:
# - ATS-DEV: No password (development only)
# - ATS-INTG: Password 'intg_password' (integration testing)

# API keys are configured via environment variables:
# - POLYGON_API_KEY=wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD
# - TIINGO_API_KEY=5f40b4f36e171405746304ec0e5a6f3aa9ca77e5
# - FMP_API_KEY=Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr
# - ALPHA_VANTAGE_API_KEY=9GI0NZ3V4VNFX271

# Access control for monitoring:
# - Prometheus: Open access (localhost:9091)
# - Grafana: admin/ats-intg-monitoring (localhost:3002)

# Container security
docker exec ats-dev-postgres whoami     # Check process user
docker exec postgres-intg whoami        # Check process user

# Network security (containers are on localhost only)
netstat -tlnp | grep -E ":3000|:3002|:5432|:5434|:9091"
```

### **Access Control**
```bash
# File system permissions
ls -la /mnt/d/ats-data/    # Data directory access
ls -la /mnt/d/ats-backup/  # Backup directory access
ls -la /mnt/d/ats-logs/    # Log directory access

# Docker daemon access (requires docker group membership)
groups $USER | grep docker

# Database access control
# ATS-DEV: Local development (no password)
# ATS-INTG: Integration testing (password protected)

# Monitoring access
# Grafana: Web UI with admin authentication
# Prometheus: Read-only metrics (no authentication required)
```

---

## 💾 Backup & Recovery

### **Automated Daily Backup System**
```bash
# ✅ AUTOMATED SYSTEM IS CONFIGURED AND RUNNING
# Daily backups run automatically via cron:
# - ATS-DEV: Daily at 2:00 AM
# - ATS-INTG: Daily at 2:15 AM  
# - Monitoring: Daily at 3:00 AM & 6:00 PM
# - Retention: 7 days automatic cleanup

# Check backup status
./scripts/manage_backups.sh status

# Manual backup operations
./scripts/manage_backups.sh run-dev     # Backup ATS-DEV now
./scripts/manage_backups.sh run-intg    # Backup ATS-INTG now
./scripts/manage_backups.sh run-all     # Backup both environments
./scripts/manage_backups.sh cleanup     # Clean old backups
./scripts/manage_backups.sh logs        # View recent logs

# Backup locations
# ATS-DEV: /mnt/d/ats-backup/dev/daily_backup_YYYYMMDD_HHMMSS.sql
# ATS-INTG: /mnt/d/ats-backup/intg/daily_backup_YYYYMMDD_HHMMSS.sql
# Logs: /mnt/d/ats-logs/backup-*.log
```

### **Database Recovery Procedures**
```bash
# ATS-DEV Database Recovery
#!/bin/bash
BACKUP_FILE="$1"  # e.g., /mnt/d/ats-backup/dev/daily_backup_20250830_020000.sql

if [ -z "$BACKUP_FILE" ]; then
  echo "Usage: $0 <backup_file>"
  echo "Available backups:"
  ls -la /mnt/d/ats-backup/dev/daily_backup_*.sql | tail -5
  exit 1
fi

echo "🔄 Starting ATS-DEV database recovery..."

# Stop analytics service
docker stop ats-dev-analytics

# Restore database
docker exec ats-dev-postgres psql -U postgres -c "DROP DATABASE IF EXISTS dev_db;"
docker exec ats-dev-postgres psql -U postgres -c "CREATE DATABASE dev_db;"
docker exec -i ats-dev-postgres psql -U postgres -d dev_db < "$BACKUP_FILE"

# Restart analytics service
python3 scripts/run_dev.py start --service analytics

echo "✅ ATS-DEV recovery completed"

# ATS-INTG Database Recovery
#!/bin/bash
BACKUP_FILE="$1"  # e.g., /mnt/d/ats-backup/intg/daily_backup_20250830_021500.sql

echo "🔄 Starting ATS-INTG database recovery..."

# Stop INTG services
docker-compose -f docker-compose.intg-jobs.yml down

# Start just database
docker-compose -f docker-compose.postgres-intg.yml up -d
sleep 10

# Restore database
PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -c "DROP DATABASE IF EXISTS intg_db;"
PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -c "CREATE DATABASE intg_db;"
PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db < "$BACKUP_FILE"

# Restart full INTG environment
docker-compose -f docker-compose.intg-jobs.yml up -d

echo "✅ ATS-INTG recovery completed"
```

---

## 🔧 Infrastructure Management

### **Capacity Planning**
```python
class CapacityMonitor:
    def analyze_resource_utilization(self, namespace: str = "ats-dev") -> CapacityReport:
        """
        Analyze current resource utilization and forecast needs
        """
        # Get current resource usage
        pods = self.k8s_client.list_namespaced_pod(namespace)
        
        cpu_usage = []
        memory_usage = []
        storage_usage = []
        
        for pod in pods.items:
            metrics = self.metrics_client.get_pod_metrics(pod.metadata.name, namespace)
            cpu_usage.append(metrics['cpu'])
            memory_usage.append(metrics['memory']) 
            storage_usage.append(metrics['storage'])
        
        # Calculate utilization percentages
        cluster_capacity = self.get_cluster_capacity()
        
        current_utilization = {
            'cpu': sum(cpu_usage) / cluster_capacity['cpu'],
            'memory': sum(memory_usage) / cluster_capacity['memory'],
            'storage': sum(storage_usage) / cluster_capacity['storage']
        }
        
        # Forecast future needs based on growth trend
        growth_trend = self.calculate_growth_trend(namespace, days=30)
        
        projected_needs = {
            'cpu': current_utilization['cpu'] * (1 + growth_trend['cpu']),
            'memory': current_utilization['memory'] * (1 + growth_trend['memory']),
            'storage': current_utilization['storage'] * (1 + growth_trend['storage'])
        }
        
        # Generate recommendations
        recommendations = []
        if projected_needs['cpu'] > 0.8:
            recommendations.append("Consider adding CPU capacity within 30 days")
        if projected_needs['memory'] > 0.8:
            recommendations.append("Consider adding memory capacity within 30 days")
        if projected_needs['storage'] > 0.8:
            recommendations.append("Consider adding storage capacity within 30 days")
            
        return CapacityReport(
            current_utilization=current_utilization,
            projected_needs=projected_needs,
            recommendations=recommendations,
            forecast_horizon_days=30
        )
```

### **Performance Optimization**
```bash
# Database performance tuning
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -d dev_db -c "
-- Update PostgreSQL configuration for time-series workloads
ALTER SYSTEM SET shared_buffers = '2GB';
ALTER SYSTEM SET effective_cache_size = '6GB'; 
ALTER SYSTEM SET work_mem = '256MB';
ALTER SYSTEM SET maintenance_work_mem = '512MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '64MB';
ALTER SYSTEM SET random_page_cost = 1.1;

-- Reload configuration
SELECT pg_reload_conf();
"

# Optimize TimescaleDB settings
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -d dev_db -c "
-- Set TimescaleDB chunk time intervals
SELECT set_chunk_time_interval('dev_daily_prices', interval '1 month');
SELECT set_chunk_time_interval('dev_minute_prices', interval '1 day');

-- Enable compression on older chunks
SELECT add_compression_policy('dev_daily_prices', interval '7 days');
SELECT add_compression_policy('dev_minute_prices', interval '2 days');
"
```

---

## 🆘 Comprehensive Troubleshooting

### **Common Issues & Solutions**

#### **🚨 CRITICAL: Docker Networking Issues (FIXED 2025-08-30)**
```bash
# Symptom: "container not attached to default bridge network" when running jobs
# Error: docker: Error response from daemon: container d1420b0d4c95... not attached to default bridge network

# ✅ ROOT CAUSE IDENTIFIED AND FIXED:
# - run_dev.py was using deprecated --link instead of --network
# - Job containers couldn't communicate with PostgreSQL on different networks
# - Fixed by updating run_dev.py line 193:
#   OLD: network_link = "--link ats-dev-postgres:postgres"
#   NEW: network_link = "--network ats-network"

# Verify the fix is working:
python3 scripts/run_dev.py run --script any_script.py   # Should work now
docker network ls                                       # Show available networks
docker network inspect ats-network                     # Show containers on ats-network

# If you see networking errors, check these:
docker ps | grep ats-dev-postgres                      # Ensure postgres is running
docker network inspect ats-network --format "{{.Containers}}"  # Show network members
```

#### **Database Connection Issues**
```bash
# Symptom: Applications can't connect to database
# Diagnosis:
docker ps | grep postgres
docker logs ats-dev-postgres --tail=50
docker logs postgres-intg --tail=50

# Solutions:
# 1. Check if database containers are running
docker ps | grep -E "(ats-dev-postgres|postgres-intg)"

# 2. Test database connectivity
docker exec ats-dev-postgres pg_isready -U postgres
PGPASSWORD=intg_password pg_isready -h localhost -p 5434 -U postgres -d intg_db

# 3. Test connection from host
python3 scripts/run_dev.py query --query "SELECT version()"
PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db -c "SELECT version()"

# 4. Check container networking
netstat -tlnp | grep -E ":5432|:5434"
docker port ats-dev-postgres
docker port postgres-intg

# 5. Restart database if needed
docker restart ats-dev-postgres
docker restart postgres-intg
```

#### **High Memory Usage**
```bash
# Symptom: Containers using excessive memory or getting killed
# Diagnosis:
docker stats --no-stream
docker inspect ats-dev-analytics | grep -A 10 "Memory"

# Solutions:
# 1. Check current memory usage
docker stats ats-dev-analytics --no-stream
docker stats postgres-intg --no-stream

# 2. Analyze memory usage inside container
docker exec ats-dev-analytics python -c "
import psutil
process = psutil.Process()
print(f'Memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB')
print(f'Memory percent: {process.memory_percent():.2f}%')
"

# 3. Restart container to clear memory
docker restart ats-dev-analytics

# 4. Check host system memory
free -h
df -h /mnt/d/  # Check persistent storage space
```

#### **Service Health Issues**
```bash
# Symptom: Analytics services returning errors or timeouts

# ATS-DEV Service Diagnosis:
docker logs ats-dev-analytics --tail=100
curl -f http://localhost:3000/health

# ATS-DEV Service Recovery:
python3 scripts/run_dev.py stop --service analytics
python3 scripts/run_dev.py start --service analytics

# ATS-INTG Service Diagnosis:
docker-compose -f docker-compose.intg-jobs.yml logs ats-intg-scheduler
curl -f http://localhost:3002/login     # Grafana
curl -f http://localhost:9091/-/ready   # Prometheus

# ATS-INTG Service Recovery:
docker-compose -f docker-compose.intg-jobs.yml restart ats-intg-scheduler
docker restart grafana-intg prometheus-intg

# General Health Checks:
# 1. Verify container networking
docker port ats-dev-analytics
netstat -tlnp | grep -E ":3000|:3002|:9091|:5432|:5434"

# 2. Check container resource usage
docker stats --no-stream | grep -E "(ats-dev|intg)"

# 3. Check application logs for errors
docker logs ats-dev-analytics | grep -i error
docker logs ats-intg-scheduler | grep -i error
```

#### **Data Quality Issues**
```bash
# Symptom: Data validation failures or inconsistent data
# Diagnosis:
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

# Solutions:
# 1. Run vendor-specific population scripts
python3 scripts/run_dev.py run --script scripts/run_tiingo_bulk.py
python3 scripts/run_dev.py run --script scripts/run_eodhd_bulk.py
python3 scripts/run_dev.py run --script scripts/run_polygon_instruments.py

# 2. Check API connectivity
curl -f "https://api.tiingo.com/api/test?token=5f40b4f36e171405746304ec0e5a6f3aa9ca77e5"
curl -f "https://eodhd.com/api/exchange-symbol-list/US?api_token=demo"

# 3. Validate schema integrity
python3 scripts/validate_schema.py --check-all
```

### **Emergency Response Procedures**
```bash
# CRITICAL: Service disruption incident response
#!/bin/bash

echo "🚨 ATS PLATFORM INCIDENT RESPONSE INITIATED"

# 0. Check for Docker networking issues (CRITICAL FIX APPLIED 2025-08-30)
echo "Checking Docker networking (common cause of job failures)..."
docker network inspect ats-network --format "{{.Containers}}" | grep -q "ats-dev-postgres" || echo "❌ PostgreSQL not on ats-network - networking issue detected"
python3 scripts/run_dev.py run --script /workspace/scripts/test_simple.py 2>&1 | grep -q "not attached to default bridge network" && echo "❌ Docker networking issue detected - check run_dev.py --network configuration"

# 1. Assess overall system impact
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
free -h && df -h /mnt/d/

# 2. Check critical services health
echo "Checking ATS-DEV services..."
curl -f http://localhost:3000/health || echo "ATS-DEV analytics DOWN"
python3 scripts/run_dev.py query --query "SELECT version()" || echo "ATS-DEV database DOWN"

echo "Checking ATS-INTG services..."
curl -f http://localhost:3002/login || echo "ATS-INTG Grafana DOWN"
curl -f http://localhost:9091 || echo "ATS-INTG Prometheus DOWN"
PGPASSWORD=intg_password pg_isready -h localhost -p 5434 -U postgres -d intg_db || echo "ATS-INTG database DOWN"

# 3. Check recent container activity
docker ps -a | grep -E "(ats-dev|intg)" | head -10
docker logs ats-dev-analytics --tail=20
docker logs postgres-intg --tail=20

# 4. Quick recovery actions
read -p "Restart failed services? (y/n): " restart
if [ "$restart" = "y" ]; then
  docker restart ats-dev-analytics
  docker restart postgres-intg
  # Wait and recheck
  sleep 10
  curl -f http://localhost:3000/health && echo "ATS-DEV recovered"
  curl -f http://localhost:3002/login && echo "ATS-INTG recovered"
fi

echo "✅ Initial response complete. Check detailed logs for root cause."
```

---

## 📈 Performance Monitoring

### **Real-Time Performance Checks**
```bash
# System performance overview
docker stats --no-stream | grep -E "(ats-dev|intg)"
free -h
df -h /mnt/d/

# Database performance metrics
python3 scripts/run_dev.py query --query "
SELECT 
  datname,
  numbackends as active_connections,
  pg_size_pretty(pg_database_size(datname)) as size,
  stats_reset as last_stats_reset
FROM pg_stat_database 
WHERE datname IN ('dev_db', 'postgres')
"

# Application response time testing
time curl -s http://localhost:3000/health
time curl -s http://localhost:3002/login

# Storage performance monitoring
ls -lah /mnt/d/ats-data/     # Data directory usage
ls -lah /mnt/d/ats-backup/   # Backup directory usage
ls -lah /mnt/d/ats-logs/     # Log directory usage

# Container uptime and restart counts
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.RestartCount}}"
```

---

## 🎯 Operational Success Metrics

### **Current Environment Health Targets**
- **Container Availability**: Services should restart automatically on failure
- **Database Response Time**: Query responses < 100ms for simple queries
- **Database Availability**: 99%+ uptime during development hours
- **Backup Success Rate**: 100% daily backups completed (7-day retention)
- **Data Freshness**: Vendor API data updated within 24 hours
- **Monitoring Coverage**: All services have health endpoints

### **Daily Operations Checklist**
```bash
# Morning health check routine
./scripts/manage_backups.sh status      # Check overnight backups
docker ps | grep -E "(ats-dev|intg)"    # Verify containers running
python3 scripts/run_dev.py status       # Check ATS-DEV health
python3 scripts/run_intg.py status      # Check ATS-INTG health

# Critical: Verify Docker networking is working (FIXED 2025-08-30)
docker network inspect ats-network --format "{{.Containers}}" | grep -q "ats-dev-postgres" && echo "✅ Docker networking OK" || echo "❌ Docker networking issue"

# Weekly maintenance
./scripts/manage_backups.sh cleanup     # Clean old backups
docker system prune -f                  # Clean unused containers/images
du -sh /mnt/d/ats-*                     # Check storage usage

# Performance monitoring
docker stats --no-stream | head -10     # Container resource usage
tail -50 /mnt/d/ats-logs/backup-*.log   # Recent backup activity
```

---

---

## 🎯 **CRITICAL OPERATIONAL FIX (2025-08-30)**

**✅ RESOLVED: Docker Networking Issue**
- **Issue**: Job containers failing with "not attached to default bridge network"
- **Impact**: All script execution via `run_dev.py` was broken
- **Root Cause**: Deprecated `--link` instead of modern `--network` configuration
- **Fix**: Updated `run_dev.py` to use `--network ats-network`
- **Result**: All Docker job execution now works seamlessly

**Critical Lesson**: Modern Docker requires custom bridge networks, not deprecated `--link` patterns. This fix ensures all future development operations execute without networking barriers.

---

**🎯 This operations guide ensures reliable, monitored, and secure operation of the ATS platform using Docker containers with automated backup systems, monitoring infrastructure, and comprehensive troubleshooting procedures for both ATS-DEV and ATS-INTG environments.**