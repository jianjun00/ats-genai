# 🏗️ ATS Infrastructure & Operations Guide

**Database connections, Docker networking, deployment strategies, and daily operations for the ATS platform.**

---

## 🐳 Docker Network Architecture

### CRITICAL: Service Communication Requirements

**ALL services MUST use Docker networks for inter-service communication:**

```bash
# Create networks (done automatically by run_dev/run_intg)
docker network create ats-network        # ATS-DEV services
docker network create ats-intg-network   # ATS-INTG services
```

**Container Naming Pattern:**
- **DEV**: `ats-dev-{service}` (e.g., `ats-dev-analytics`, `ats-dev-postgres`)
- **INTG**: `ats-intg-{service}` (e.g., `ats-intg-analytics`, `ats-intg-postgres`)

### Port Architecture - Environment Isolation

| Service | DEV Environment | INTG Environment | Internal Port | Container Host |
|---------|----------------|------------------|---------------|----------------|
| **Analytics** | `localhost:3000` | `localhost:4000` | `3000` | ats-dev/intg-analytics |
| **PostgreSQL** | `localhost:3432` | `localhost:4432` | `5432` | ats-dev/intg-postgres |
| **API** | `localhost:8000` | `localhost:8001` | `8000` | ats-dev/intg-api |
| **Grafana** | `localhost:3001` | `localhost:4002` | `3000` | ats-intg-grafana |
| **Prometheus** | N/A | `localhost:4080` | `9090` | ats-intg-prometheus |

**Critical Network Rules:**
- **External ports differ** between environments to avoid conflicts
- **Internal container ports stay same** (analytics always uses 3000 internally)
- **Database connections use internal ports** (ats-dev-postgres:5432, ats-intg-postgres:5432)
- **Container-to-container communication** uses container names as hostnames

### Network Troubleshooting
```bash
# Check container networks (MANDATORY when services can't communicate)
docker inspect <container_name> | grep NetworkMode
docker network ls
docker network inspect ats-network    # See which containers are connected

# Fix network issues
docker stop <container>
docker rm <container>
# Restart with correct network via run_dev/run_intg scripts
```

---

## 📊 Database Connection Reference

### Two-Environment Architecture

**ATS-DEV (Development):**
- **Purpose**: Primary development, unit testing, feature development
- **Database**: PostgreSQL 13 on `localhost:3432`
- **Container**: `ats-dev-postgres`
- **Connection**: `dev_db` database, `postgres` user, `dev_password`
- **Table Prefix**: `dev_*` (e.g., `dev_instruments`, `dev_daily_prices`)

**ATS-INTG (Integration):**
- **Purpose**: CI/CD integration testing, pre-production validation
- **Database**: TimescaleDB (PostgreSQL 13.15) on `localhost:4432`
- **Container**: `ats-intg-postgres`
- **Connection**: `intg_db` database, `postgres` user, `intg_password`
- **Table Prefix**: `intg_*` (e.g., `intg_instruments`, `intg_daily_prices`)

### Database Connection Examples

**Auto-Detection (Recommended):**
```bash
# Automatically detects available environment based on running containers
python3 scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_instruments"     # → dev env
python3 scripts/run_dev.py query --query "SELECT COUNT(*) FROM intg_instruments"    # → intg env
```

**Direct Database Connections:**
```bash
# ATS-DEV (password required)
PGPASSWORD=dev_password psql -h localhost -p 3432 -U postgres -d dev_db -c "SELECT version()"

# ATS-INTG (password required)
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "SELECT version()"
```

### Quick Reference - Database Connections

| Environment | Host | Port | Database | Username | Password | Connection String |
|-------------|------|------|----------|----------|----------|-------------------|
| **ATS-DEV** | localhost | 3432 | dev_db | postgres | dev_password | `postgresql://postgres:dev_password@localhost:3432/dev_db` |
| **ATS-INTG** | localhost | 4432 | intg_db | postgres | intg_password | `postgresql://postgres:intg_password@localhost:4432/intg_db` |

---

## 💾 Volume Architecture - Data Persistence

**Critical Volume Mounts (NEVER change these):**
```bash
# Core application volumes
-v /home/jianjun/ats-genai-admin:/workspace                    # Source code
-v /mnt/d/ats-data:/data                                       # Training data, minute bars
-v /mnt/d/ats-backup:/backup                                   # Database backups
-v /mnt/d/ats-logs:/logs                                       # Service logs

# Database volumes (persistent data)
-v postgres-dev-data:/var/lib/postgresql/data                  # DEV database
-v postgres-intg-data:/var/lib/postgresql/data                 # INTG database

# Working directory (MANDATORY)
-w /workspace                                                  # All containers work from here
```

**Data Structure (READ-ONLY - Do not modify):**
```
/mnt/d/ats-data/
├── minute-bars/firstrate/           # Raw OHLCV data INPUT (parquet files)
├── training-data/                   # ML-ready datasets OUTPUT (arrayrecord)
├── checkpoints/                     # API rate limiting checkpoints
└── temp/                           # Temporary processing files
```

---

## ⚙️ Environment Management & Service Startup

### Complete ATS-DEV Setup
```bash
# Complete environment setup (databases, services, health checks)
python3 scripts/run_dev.py setup

# Service management
python3 scripts/run_dev.py start --service postgres
python3 scripts/run_dev.py start --service analytics
python3 scripts/run_dev.py status

# Database operations
python3 scripts/run_dev.py query --query "SELECT version()"
```

### Complete ATS-INTG Setup
```bash
# Start PostgreSQL first
docker-compose -f docker-compose.ats.yml up -d postgres-intg

# Start INTG services
docker-compose -f docker-compose.intg-jobs.yml up -d

# Database operations
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db
```

### Single Command Complete Startup
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

### Service Health Checks
```bash
# Quick health verification
curl -f http://localhost:3000/health  # ATS-DEV
curl -f http://localhost:4000/health  # ATS-INTG
curl -f http://localhost:4080/health  # Prometheus
docker ps | grep -E "(ats-dev|intg)"  # Container status
```

---

## 🔑 Environment Variables - Service Configuration

**DEV Environment Variables:**
```bash
# Database connection (internal docker network)
DB_HOST=ats-dev-postgres             # Container name, NOT localhost
DB_PORT=5432                        # Internal port, NOT external 3432
DB_USER=postgres
DB_PASSWORD=dev_password
DB_NAME=dev_db
ENVIRONMENT=dev

# File paths (container perspective)
ATS_DATA_PATH=/data                 # Maps to /mnt/d/ats-data
ATS_BACKUP_PATH=/backup            # Maps to /mnt/d/ats-backup
ATS_LOGS_PATH=/logs                # Maps to /mnt/d/ats-logs
PYTHONPATH=/workspace/src          # Critical for Python imports
```

**INTG Environment Variables:**
```bash
# Database connection (internal docker network)
DB_HOST=ats-intg-postgres           # Container name, NOT localhost
DB_PORT=5432                       # Internal port, NOT external 4432
DB_USER=postgres
DB_PASSWORD=intg_password
DB_NAME=intg_db
ENVIRONMENT=intg

# Same file paths as DEV (same volume mounts)
ATS_DATA_PATH=/data
ATS_BACKUP_PATH=/backup
ATS_LOGS_PATH=/logs
PYTHONPATH=/workspace/src
```

---

## ⏰ Automated Operations - Complete Cron Schedule

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

# Check cron logs
sudo tail -f /var/log/cron
```

### Health Monitoring
```bash
# Daily system health check
./scripts/cron/daily_health_check.sh

# Service status monitoring
python scripts/run_dev.py status
python scripts/run_intg.py status

# Data validation
python scripts/daily_prices_validation.py
```

---

## 🚀 Deployment Strategies

### GitOps Deployment Workflow
```bash
# 1. Feature development on branch
git checkout -b feature/new-algorithm
# ... development work ...
git push origin feature/new-algorithm

# 2. Create pull request
gh pr create --title "feat: new trading algorithm"

# 3. Automated CI/CD pipeline runs
# - Unit tests
# - Integration tests
# - Security scans
# - Performance benchmarks

# 4. Manual review and approval
gh pr review --approve

# 5. Automated deployment to staging
git merge main  # Triggers staging deployment

# 6. Production deployment (manual approval required)
gh workflow run deploy-production.yml
```

### Environment Promotion
```bash
# Development → Staging
python scripts/deploy.py --source dev --target staging --validate

# Staging → Production
python scripts/deploy.py --source staging --target production --validate --require-approval

# Rollback if needed
python scripts/rollback.py --environment production --to-version v1.2.3
```

### Safety Checks Before Deployment
```bash
# 1. Schema validation
python scripts/validate_schema.py --target production

# 2. Data integrity check
python scripts/validate_data_integrity.py --environment staging

# 3. Performance benchmarks
python scripts/run_performance_tests.py --baseline production

# 4. Security scan
bandit -r src/ -f json -o security-report.json

# 5. Dependency audit
pip-audit --format json --output audit-report.json
```

---

## 🔍 Monitoring & Observability

### Real-Time Monitoring Endpoints
```bash
# Service health endpoints
curl -s http://localhost:3000/health | jq          # ATS-DEV analytics
curl -s http://localhost:4000/health | jq          # ATS-INTG analytics
curl -s http://localhost:4080/metrics              # Prometheus metrics

# Database health
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM pg_stat_activity"
python scripts/run_intg.py query --query "SELECT COUNT(*) FROM pg_stat_activity"
```

### Grafana Dashboards
- **ATS Vendor Monitoring**: http://localhost:4002/d/f9afe708-9be9-4c39-b901-f5c43a0a479f
- **Database Usage**: http://localhost:4002/d/database-usage
- **Code Usage Analytics**: http://localhost:4002/d/code-usage
- **Batch Jobs Monitoring**: http://localhost:4002/d/batch-jobs

### Log Management
```bash
# Service logs
docker logs ats-dev-analytics --tail 100
docker logs ats-intg-postgres --tail 100

# Application logs
tail -f /mnt/d/ats-logs/analytics.log
tail -f /mnt/d/ats-logs/data-pipeline.log

# Structured log queries
grep -E "(ERROR|CRITICAL)" /mnt/d/ats-logs/*.log | tail -20
```

---

## 🆘 Troubleshooting Common Issues

### "Connection refused" errors
```bash
# Symptom: Services can't reach database
# Root Cause: Containers on different networks
# Fix: Ensure both containers use --network ats-network

# Debug:
docker inspect <container> | grep NetworkMode
# Should show "ats-network", not "bridge"
```

### "Loading database tables..." (dummy content)
```bash
# Symptom: Analytics shows loading screens instead of data
# Root Cause: Database connection misconfigured
# Fix: Check DB_HOST uses container name, not localhost

# Debug:
docker logs ats-intg-analytics --tail 20
# Look for connection errors to wrong host/port
```

### Port conflicts
```bash
# Symptom: "Port already in use" errors
# Root Cause: Dev and intg services using same external ports
# Fix: Use correct port mappings (3000 vs 4000, 3432 vs 4432)

# Debug:
docker ps | grep -E "(3000|4000|3432|4432)"
netstat -tulpn | grep -E "(3000|4000|3432|4432)"
```

### Character encoding issues
```bash
# Symptom: "ðŸš€" instead of "🚀"
# Root Cause: Missing charset=utf-8 in HTTP headers
# Fix: Add charset to Content-Type header in analytics service
```

---

## ✅ Deployment Verification Checklist

**After starting any service, ALWAYS verify:**
```bash
# 1. Container is running on correct network
docker inspect <container> | grep -A 5 NetworkMode

# 2. Database connectivity works
curl -f http://localhost:<port>/health

# 3. Service logs show no connection errors
docker logs <container> --tail 10

# 4. Port mappings are correct
docker ps | grep <service_name>

# 5. External API access works
curl -s "http://localhost:3000/api/datasets" | jq
```

### Success Metrics
- All services healthy (green status)
- Database connections under 100ms latency
- Zero connection errors in logs
- API endpoints responding < 200ms
- Memory usage < 80% per container
- CPU usage < 70% under normal load

---

**🎯 This infrastructure guide ensures reliable, scalable deployment and operations of the ATS platform across all environments.**