# 🚀 ATS Integration Environment Setup

**Complete guide for the ATS Integration Environment with CI/CD deployment pipeline**

---

## 📋 Overview

The ATS Integration Environment provides:
- **🐳 Docker-based PostgreSQL with TimescaleDB** (Port 5433)
- **🔄 Automated CI/CD Pipeline** that deploys on unit test success
- **📊 Full Database Migration Management** 
- **🛠️ Complete Service Management** via `run_intg.py`

---

## ⚡ Quick Setup

### 1. Start Integration Environment
```bash
# Setup complete integration environment
python scripts/run_intg.py setup
# ✅ Success = TimescaleDB PostgreSQL started, migrations applied
```

### 2. Verify Setup
```bash
# Check services status
python scripts/run_intg.py status

# Test database connection
python scripts/run_intg.py query --query "SELECT version()"

# Validate TimescaleDB and tables
python scripts/run_intg.py query --query "
SELECT 'TimescaleDB' as type, extversion as info FROM pg_extension WHERE extname = 'timescaledb'
UNION ALL 
SELECT 'Tables', COUNT(*)::text FROM information_schema.tables WHERE table_name LIKE 'intg_%'"
```

---

## 🗄️ Database Configuration

### Connection Details
- **Host**: `localhost`
- **Port**: `5433` (different from dev: 5432)
- **Database**: `intg_db`
- **User**: `postgres` 
- **Password**: `intg_password`
- **Container**: `ats-intg-postgres`
- **Engine**: TimescaleDB (PostgreSQL 13.15 + TimescaleDB 2.15.3)

### Migration Status
```bash
# Check current migration version
python scripts/run_intg.py query --query "SELECT version, description, applied_at FROM intg_db_version ORDER BY version DESC LIMIT 5"

# Run pending migrations
docker run --rm --network host -v $(pwd):/workspace -e PYTHONPATH=/workspace/src -e DB_HOST=localhost -e DB_PORT=5433 -e DB_USER=postgres -e DB_PASSWORD=intg_password -e DB_NAME=intg_db dragonflyer762/ats-genai:latest python /workspace/scripts/run_intg_migrations.py
```

---

## 🛠️ Service Management

### Start/Stop Services
```bash
# Start integration PostgreSQL
python scripts/run_intg.py start --service postgres

# Start analytics service (Port 4000)
python scripts/run_intg.py start --service analytics

# Start API service (Port 8001)  
python scripts/run_intg.py start --service api

# Stop services
python scripts/run_intg.py stop --service postgres
python scripts/run_intg.py stop --service analytics
python scripts/run_intg.py stop --service api

# Check all running services
python scripts/run_intg.py status
```

### Service Ports
- **PostgreSQL**: `4434`
- **Analytics Service**: `4000` 
- **API Service**: `8001`

---

## 🔄 CI/CD Pipeline

### Automatic Deployment Trigger
The integration environment automatically deploys when:
1. ✅ **Code is pushed to `main` branch**
2. ✅ **Unit tests pass successfully**  
3. ✅ **Schema validation passes**

### Workflow Files
- **Main Pipeline**: `.github/workflows/ats-intg-deployment.yaml`
- **Trigger Pipeline**: `.github/workflows/green-build.yaml`

### Pipeline Steps
1. **Unit Tests** - Core unit and integration tests must pass
2. **Schema Validation** - Database schema changes validated
3. **Docker Build** - New image built with `intg-latest` tag
4. **Database Migration** - Latest migrations applied automatically
5. **Service Deployment** - Analytics and API services restarted
6. **Health Checks** - Service endpoints validated
7. **Integration Tests** - Live environment tests executed

### Manual Deployment
```bash
# Trigger deployment via GitHub API
gh workflow run ats-intg-deployment.yaml

# Or with specific options
gh workflow run ats-intg-deployment.yaml --field skip_tests=false --field force_deploy=true
```

---

## 📊 Database Operations

### Query Database
```bash
# Direct SQL queries
python scripts/run_intg.py query --query "SELECT COUNT(*) FROM intg_daily_prices"

# Check table structure
python scripts/run_intg.py query --query "\\d+ intg_daily_prices"

# Analyze data quality
python scripts/run_intg.py query --query "
SELECT 
  COUNT(*) as total_records,
  MIN(date) as earliest_date,
  MAX(date) as latest_date,
  COUNT(DISTINCT symbol) as unique_symbols
FROM intg_daily_prices"
```

### Backup & Restore
```bash
# Manual backup
PGPASSWORD=intg_password pg_dump -h localhost -p 5433 -U postgres intg_db > /mnt/d/ats-backup/intg/backup_$(date +%Y%m%d_%H%M%S).sql

# Automatic backup (handled by run_intg.py on stop)
python scripts/run_intg.py stop --service postgres
# ✅ Creates backup at /mnt/d/ats-backup/intg/latest_backup.sql

# Restore from backup (automatic on start if backup exists)
python scripts/run_intg.py start --service postgres  
# ✅ Restores from /mnt/d/ats-backup/intg/latest_backup.sql if available
```

---

## 🧪 Testing

### Integration Tests
```bash
# Run integration tests
python scripts/run_intg.py test

# Specific test patterns
python scripts/run_intg.py test --test tests/integration/test_analytics_platform_integration.py
```

### Service Health Checks
```bash
# Test service endpoints
curl -s "http://localhost:4000/health" | jq
curl -s "http://localhost:8001/health" | jq

# Database connectivity test
python scripts/run_intg.py query --query "SELECT 1"
```

---

## 🚨 Troubleshooting

### Common Issues

#### "Database connection failed"
```bash
# Check if postgres container is running
python scripts/run_intg.py status

# Restart postgres if needed
python scripts/run_intg.py stop --service postgres
python scripts/run_intg.py start --service postgres

# Verify connection
python scripts/run_intg.py query --query "SELECT version()"
```

#### "TimescaleDB extension not found"
```bash
# Check TimescaleDB status
python scripts/run_intg.py query --query "SELECT * FROM pg_extension WHERE extname='timescaledb'"

# If missing, the container needs to be recreated with proper TimescaleDB image
docker stop ats-intg-postgres && docker rm ats-intg-postgres
python scripts/run_intg.py start --service postgres
```

#### "Migration failed"
```bash
# Check current migration status
python scripts/run_intg.py query --query "SELECT version FROM intg_db_version ORDER BY version DESC LIMIT 1"

# Check migration logs
docker run --rm --network host -v $(pwd):/workspace -e PYTHONPATH=/workspace/src -e DB_HOST=localhost -e DB_PORT=5433 -e DB_USER=postgres -e DB_PASSWORD=intg_password -e DB_NAME=intg_db dragonflyer762/ats-genai:latest python /workspace/scripts/run_intg_migrations.py
```

### Container Management
```bash
# View container logs
docker logs ats-intg-postgres

# Restart container
docker restart ats-intg-postgres

# Remove and recreate container
docker stop ats-intg-postgres && docker rm ats-intg-postgres
python scripts/run_intg.py start --service postgres
```

---

## 📈 Monitoring & Maintenance

### Regular Checks
- **Daily**: Service health checks via CI/CD pipeline
- **Weekly**: Database backup verification
- **Monthly**: Container image updates and security patches

### Key Metrics
- **Database Size**: Monitor table growth and storage usage
- **Service Uptime**: Track analytics and API service availability  
- **Migration Status**: Ensure all environments are on latest schema version
- **CI/CD Success Rate**: Monitor deployment success/failure rates

---

## 🎯 Success Criteria

**Integration environment is properly configured when:**
- [ ] `python scripts/run_intg.py setup` completes successfully
- [ ] Database responds to `SELECT version()` query
- [ ] TimescaleDB extension is available and functional
- [ ] CI/CD pipeline deploys automatically on main branch commits
- [ ] Unit tests pass before deployment
- [ ] Services start and respond to health checks
- [ ] Database migrations apply without errors

---

**🔗 Related Documentation:**
- [DEVELOPMENT.md](DEVELOPMENT.md) - Development workflow and TDD
- [DEPLOYMENT.md](DEPLOYMENT.md) - Complete deployment strategies  
- [CLAUDE.md](../CLAUDE.md) - Database connection info and commands