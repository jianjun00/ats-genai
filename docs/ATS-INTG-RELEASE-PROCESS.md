# ATS-INTG Release Management Process

Complete release management strategy for ATS Integration environment with GitOps workflow, validation, and rollback procedures.

## 📋 Overview

ATS-INTG follows a structured release process with comprehensive validation, automated deployment, and rollback capabilities. The system supports both scheduled releases and hotfix deployments with full monitoring and health checks.

## 🏗️ Release Strategy

### Three-Tier Release Process

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Development   │    │  Integration    │    │   Production    │
│    (ats-dev)    │───▶│   (ats-intg)    │───▶│   (ats-prod)    │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • Rapid testing │    │ • Staged release│    │ • Customer data │
│ • Feature dev   │    │ • Full validation│   │ • Live trading  │
│ • Mock data OK  │    │ • Real data only│    │ • SLA compliance│
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Release Types

1. **Major Release** (Monthly) - New features, breaking changes
2. **Minor Release** (Bi-weekly) - Feature additions, improvements  
3. **Patch Release** (Weekly) - Bug fixes, security updates
4. **Hotfix Release** (As needed) - Critical production issues

## 🚀 Release Process Steps

### 1. Pre-Release Validation

#### Automated Validation
```bash
# Run comprehensive validation
python scripts/intg_release_manager.py validate

# Checks performed:
# - Git working directory is clean
# - All integration tests pass
# - Docker build succeeds  
# - Database schema validation
# - Job configuration validation
```

#### Manual Validation Checklist
- [ ] All planned features are complete
- [ ] Documentation is updated
- [ ] API keys are configured
- [ ] Database migrations are tested
- [ ] Performance benchmarks meet targets
- [ ] Security scan passes
- [ ] Team approval obtained

### 2. Release Creation

#### Create Release
```bash
# Create new release with automated changelog
python scripts/intg_release_manager.py create --version v1.2.0 --previous-version v1.1.0

# This will:
# - Create rollback point
# - Generate release notes
# - Create Git tag
# - Push to remote repository
```

#### Release Artifacts Generated
- **Git Tag**: `v1.2.0` with release metadata
- **Release Notes**: `RELEASE-v1.2.0.md` with comprehensive changelog
- **Rollback Tag**: `rollback-intg-TIMESTAMP` for emergency rollback
- **Docker Images**: Tagged with release version

### 3. Deployment to ATS-INTG

#### Automated Deployment
```bash
# Deploy to integration environment
./scripts/intg_deploy.sh

# Or use release manager
python scripts/intg_release_manager.py deploy --monitor-duration 60
```

#### Deployment Steps Performed
1. **Environment Setup** - Create host directories and permissions
2. **Configuration Validation** - Validate Docker Compose and job configs
3. **Backup Creation** - Backup existing data before deployment
4. **Service Deployment** - Deploy PostgreSQL and job scheduler
5. **Health Verification** - Wait for services and run smoke tests
6. **Monitoring Setup** - Enable health monitoring for specified duration

#### Smoke Tests Included
- Database connectivity and table creation
- Job scheduler service status
- Cron job configuration
- API key validation
- Data persistence verification

### 4. Post-Deployment Monitoring

#### Automated Health Monitoring
```bash
# Monitor release health for 60 minutes
python scripts/intg_release_manager.py monitor --monitor-duration 60

# Monitor with dashboard
python scripts/monitor_daily_jobs.py

# Check deployment status
python scripts/intg_release_manager.py status
```

#### Key Health Metrics
- **Service Uptime** - All containers running
- **Database Connectivity** - PostgreSQL accessible
- **Job Execution** - Daily jobs completing successfully
- **Data Quality** - Data integrity validation
- **API Response** - Vendor API connectivity
- **Resource Usage** - CPU, Memory, Disk utilization

### 5. Release Validation

#### Functional Testing
```bash
# Test individual job types manually
python scripts/daily_job_scheduler.py manual --job prices
python scripts/daily_job_scheduler.py manual --job fundamentals
python scripts/daily_job_scheduler.py manual --job news

# Verify data quality
python scripts/run_intg.py query --query "SELECT * FROM intg_data_quality ORDER BY data_date DESC LIMIT 10"

# Check job performance
python scripts/run_intg.py query --query "SELECT * FROM intg_job_performance ORDER BY job_date DESC LIMIT 10"
```

#### Performance Validation
- Job completion times within SLA
- Database query response times
- API rate limiting compliance
- Resource utilization within limits

## 🔄 Rollback Procedures

### Automatic Rollback Triggers
- Smoke tests fail during deployment
- Service health monitoring detects failures
- Database connectivity issues
- Critical job failures

### Manual Rollback Process
```bash
# Emergency rollback to previous version
python scripts/intg_release_manager.py rollback --rollback-tag rollback-intg-20250128_143022

# Or use deployment script rollback
./scripts/intg_deploy.sh --rollback

# Verify rollback
python scripts/intg_release_manager.py status
```

### Rollback Verification
- All services restored to previous state  
- Database data integrity maintained
- Job schedules restored
- Monitoring confirms system stability

## 📊 Release Environments

### ATS-INTG Environment Specifications

#### Infrastructure
- **Platform**: Docker Compose with host-mounted persistence
- **Database**: TimescaleDB with PostgreSQL 13
- **Storage**: Host-mounted volumes for data persistence
- **Networking**: Bridge network with external port mapping
- **Monitoring**: Container health checks and job monitoring

#### Database Configuration
```yaml
# PostgreSQL optimizations for time-series data
max_connections: 200
shared_buffers: 256MB
effective_cache_size: 1GB
work_mem: 4MB
maintenance_work_mem: 64MB
```

#### Job Scheduling
- **Daily Prices**: 05:00 UTC (all vendors)
- **Daily Fundamentals**: 06:30 UTC (FMP, Polygon, Alpha Vantage)
- **Daily News**: 08:00 UTC (all vendors)
- **Weekly Validation**: 02:00 UTC Sundays

#### Data Sources
- **Polygon API**: Daily prices and news
- **FMP API**: Fundamentals and price validation
- **Tiingo API**: Price cross-validation
- **Alpha Vantage API**: News sentiment analysis

## 🔐 Security and Access Control

### Environment Access
- **Database**: `postgresql://postgres:intg_password@localhost:5433/intg_db`
- **Containers**: Docker network isolation
- **API Keys**: Environment variable configuration
- **Logs**: Host-mounted log directory

### Security Checklist
- [ ] API keys stored securely in environment variables
- [ ] Database passwords rotated regularly
- [ ] Network access restricted to necessary ports
- [ ] Log files secured and rotated
- [ ] Container images scanned for vulnerabilities

## 📈 Monitoring and Alerting

### Health Monitoring
```bash
# Container health
docker ps --filter "name=ats-intg"

# Database health
docker exec postgres-intg pg_isready -U postgres -d intg_db

# Job execution logs
docker logs ats-intg-scheduler -f

# Data quality dashboard
python scripts/monitor_daily_jobs.py
```

### Alert Conditions
- **Critical**: Service containers stopped
- **Critical**: Database connectivity lost
- **Warning**: Job execution failures
- **Warning**: API rate limit exceeded
- **Info**: Scheduled maintenance windows

### Log Locations
- **Application logs**: `/mnt/d/ats-logs/intg/`
- **Container logs**: `docker logs <container-name>`
- **Database logs**: PostgreSQL container logs
- **Job execution**: Individual job log files

## 📋 Release Schedule

### Regular Release Cadence

#### Monthly Major Release (First Sunday)
- New features and significant improvements
- Database schema changes
- Infrastructure updates
- Comprehensive testing required

#### Bi-weekly Minor Release (Third Sunday) 
- Feature additions and enhancements
- Performance optimizations
- Non-breaking configuration changes
- Standard testing process

#### Weekly Patch Release (Every Sunday)
- Bug fixes and security updates
- Configuration tweaks
- Vendor API updates
- Minimal testing required

#### Hotfix Release (As Needed)
- Critical production issues
- Security vulnerabilities
- Data corruption fixes
- Emergency deployment process

### Release Windows
- **Deployment Time**: 02:00 UTC Sundays (minimal market impact)
- **Monitoring Period**: 4 hours post-deployment
- **Rollback Window**: 2 hours if issues detected
- **Communication**: 24-hour advance notice for major releases

## 🛠️ Troubleshooting Guide

### Common Issues and Solutions

#### Deployment Failures
```bash
# Check deployment lock
ls -la /tmp/ats-intg-deployment.lock

# Validate configurations
docker-compose -f docker-compose.intg-jobs.yml config

# Check prerequisites
docker info
docker-compose --version
```

#### Service Start Issues
```bash
# Check container status
docker ps -a --filter "name=ats-intg"

# View container logs
docker logs postgres-intg
docker logs ats-intg-scheduler

# Check resource usage
docker stats --no-stream
```

#### Database Connection Issues
```bash
# Test database connectivity
docker exec postgres-intg pg_isready -U postgres -d intg_db

# Check database logs
docker logs postgres-intg | tail -n 50

# Verify data persistence
ls -la /mnt/d/ats-data/intg/postgresql/
```

#### Job Execution Issues
```bash
# Check cron jobs
docker exec ats-intg-scheduler crontab -l

# View job logs
docker logs ats-intg-scheduler | grep "python scripts/"

# Manual job testing
python scripts/daily_job_scheduler.py manual --job prices
```

### Recovery Procedures

#### Service Recovery
1. Stop all services: `docker-compose -f docker-compose.intg-jobs.yml down`
2. Check logs and fix issues
3. Restart services: `docker-compose -f docker-compose.intg-jobs.yml up -d`
4. Verify health: `python scripts/intg_release_manager.py status`

#### Data Recovery
1. Locate latest backup: `ls -t /mnt/d/ats-backup/intg/`
2. Restore database: `cat backup.sql | docker exec -i postgres-intg psql -U postgres -d intg_db`
3. Verify data integrity: `python scripts/monitor_daily_jobs.py`
4. Resume job scheduling

## 📞 Support and Escalation

### Support Contacts
- **Primary**: Development Team
- **Secondary**: DevOps Team  
- **Emergency**: On-call Engineering

### Escalation Matrix
- **Severity 1** (Production Down): Immediate response
- **Severity 2** (Degraded Performance): 2-hour response
- **Severity 3** (Minor Issues): Next business day

### Documentation
- **Release Notes**: Generated automatically with each release
- **Troubleshooting**: This document and inline help
- **API Documentation**: Vendor API integration guides
- **Monitoring**: Dashboard and log analysis guides

---

## 🎯 Quick Reference Commands

```bash
# Validation and Release
python scripts/intg_release_manager.py validate
python scripts/intg_release_manager.py create --version v1.2.0

# Deployment
./scripts/intg_deploy.sh
python scripts/intg_release_manager.py deploy

# Monitoring
python scripts/intg_release_manager.py monitor
python scripts/monitor_daily_jobs.py
docker logs ats-intg-scheduler -f

# Testing
python scripts/daily_job_scheduler.py manual --job prices
python scripts/daily_job_scheduler.py status

# Rollback
python scripts/intg_release_manager.py rollback --rollback-tag <tag>

# Status
python scripts/intg_release_manager.py status
docker ps --filter "name=ats-intg"
```

This release process ensures reliable, monitored deployments with comprehensive validation and rollback capabilities for the ATS-INTG environment.