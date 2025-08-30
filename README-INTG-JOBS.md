# ATS-INTG Daily Jobs Setup Guide

Complete setup and deployment guide for daily data refresh jobs in the ATS Integration environment.

## 📋 Overview

This system provides automated daily refresh of:
- **Daily Prices** from Polygon, FMP, Tiingo, Alpha Vantage
- **Fundamentals** from FMP, Polygon, Alpha Vantage  
- **News** from Polygon, FMP, Alpha Vantage

## 🚀 Quick Start

### 1. Setup Environment (Required First Step)

```bash
# Setup host directories and permissions
./scripts/setup_intg_environment.sh

# This creates persistent directories:
# - /mnt/d/ats-data/intg/postgresql (database data)
# - /mnt/d/ats-backup/intg (backups)
# - /mnt/d/ats-logs/intg (job logs)
```

### 2. Deploy with Docker Compose (Recommended)

```bash
# Start the complete integration environment
docker-compose -f docker-compose.intg-jobs.yml up -d

# View logs
docker logs ats-intg-scheduler -f

# Check status
docker ps | grep ats-intg
```

### 3. Database-Only Deployment (Alternative)

```bash
# Start only PostgreSQL database
docker-compose -f docker-compose.postgres-intg.yml up -d

# Then run jobs manually or with cron
python scripts/daily_job_scheduler.py manual --job prices
```

### 4. Manual Job Testing

```bash
# Test individual jobs manually
python scripts/daily_job_scheduler.py manual --job prices
python scripts/daily_job_scheduler.py manual --job fundamentals  
python scripts/daily_job_scheduler.py manual --job news

# Check job status
python scripts/daily_job_scheduler.py status
```

### 5. Generate Configuration Files

```bash
# Generate cron configuration
python scripts/daily_job_scheduler.py config --format cron

# Generate Docker Compose configuration  
python scripts/daily_job_scheduler.py config --format docker

# Generate systemd configuration
python scripts/daily_job_scheduler.py config --format systemd
```

## ⏰ Scheduling Strategy

### Optimal Daily Schedule
- **05:00 UTC** - Daily Price Refresh (all vendors)
- **06:30 UTC** - Daily Fundamentals Refresh (selected vendors)
- **08:00 UTC** - Daily News Refresh (all vendors)
- **02:00 UTC Sunday** - Weekly Data Validation

### Vendor Priority Rotation
- **Monday/Thursday**: Polygon → FMP → Alpha Vantage → Tiingo
- **Tuesday/Friday**: FMP → Polygon → Tiingo → Alpha Vantage  
- **Wednesday/Saturday**: Tiingo → Alpha Vantage → Polygon → FMP
- **Sunday**: Alpha Vantage → Tiingo → FMP → Polygon

## 📊 Job Details

### Daily Price Refresh Job
- **File**: `scripts/daily_price_refresh_job.py`
- **Purpose**: Fetch previous day's closing prices
- **Vendors**: Polygon, FMP, Tiingo, Alpha Vantage
- **Rate Limiting**: 0.5s between requests
- **Checkpoint**: `intg_daily_price_checkpoint`

### Daily Fundamentals Refresh Job
- **File**: `scripts/daily_fundamentals_refresh_job.py`
- **Purpose**: Update quarterly/annual fundamental data
- **Vendors**: FMP, Polygon, Alpha Vantage
- **Rate Limiting**: 1.0s between requests
- **Checkpoint**: `intg_fundamentals_checkpoint`

### Daily News Refresh Job
- **File**: `scripts/daily_news_refresh_job.py`
- **Purpose**: Fetch latest news and sentiment
- **Vendors**: Polygon, FMP, Alpha Vantage
- **Rate Limiting**: 1.5s between requests
- **Checkpoint**: `intg_news_checkpoint`

## 🎯 Database Schema

### Core Tables
- `intg_instruments` - Active symbols and metadata
- `intg_daily_prices` - Daily OHLCV data with TimescaleDB
- `intg_fundamentals_comprehensive` - Income, balance, cash flow statements
- `intg_news` - News articles with sentiment scores

### Checkpoint Tables
- `intg_daily_price_checkpoint` - Price refresh progress
- `intg_fundamentals_checkpoint` - Fundamentals refresh progress  
- `intg_news_checkpoint` - News refresh progress

### Monitoring Views
- `intg_job_performance` - Job execution metrics
- `intg_data_quality` - Data completeness statistics

## 🔧 Configuration

### Environment Variables
```bash
# Database Connection
DB_HOST=postgres-intg
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=intg_password
DB_NAME=intg_db

# API Keys
POLYGON_API_KEY=wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD
FMP_API_KEY=Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr
TIINGO_API_KEY=5f40b4f36e171405746304ec0e5a6f3aa9ca77e5
ALPHA_VANTAGE_API_KEY=9GI0NZ3V4VNFX271

# Job Configuration
RATE_LIMIT_ENABLED=true
CHECKPOINT_ENABLED=true
LOG_LEVEL=INFO
```

### Volume Mounts (Host-Mounted for Persistence)
- `/workspace` - ATS codebase (bind mount)
- `/mnt/d/ats-data/intg/postgresql` - PostgreSQL data directory (CRITICAL for data persistence)
- `/mnt/d/ats-backup/intg` - Database backups (automated and manual)
- `/mnt/d/ats-logs/intg` - Job execution logs

### Data Persistence Strategy
- **PostgreSQL data** is stored on host at `/mnt/d/ats-data/intg/postgresql`
- **No Docker volumes** used - all data survives container rebuilds
- **Automatic backups** to `/mnt/d/ats-backup/intg` during job runs
- **TimescaleDB optimizations** for time-series data performance

## 📈 Monitoring and Alerting

### Monitoring Dashboard
```bash
# Generate monitoring script
python scripts/daily_job_scheduler.py monitor

# Run dashboard
python scripts/monitor_daily_jobs.py
```

### Log Files
- `/logs/daily_prices.log` - Price refresh logs
- `/logs/daily_fundamentals.log` - Fundamentals refresh logs
- `/logs/daily_news.log` - News refresh logs
- `/logs/health_check.log` - System health logs

### Health Checks
```bash
# Database connectivity
python scripts/run_intg.py query --query "SELECT CURRENT_TIMESTAMP"

# Recent job status
python scripts/run_intg.py query --query "SELECT * FROM intg_job_performance ORDER BY job_date DESC LIMIT 10"

# Data quality metrics
python scripts/run_intg.py query --query "SELECT * FROM intg_data_quality ORDER BY data_date DESC LIMIT 20"
```

## 🛠️ Troubleshooting

### Common Issues

#### 1. API Rate Limiting
```bash
# Check recent failures
docker logs ats-intg-scheduler | grep "rate limit"

# Increase delay in job scripts
# Edit RATE_LIMIT_DELAY in individual job files
```

#### 2. Database Connection Issues
```bash
# Check PostgreSQL status
docker logs postgres-intg

# Test connection
python scripts/run_intg.py query --query "SELECT version()"
```

#### 3. Missing Data
```bash
# Check checkpoint status
python scripts/run_intg.py query --query "SELECT * FROM intg_daily_price_checkpoint WHERE job_date = CURRENT_DATE"

# Resume from checkpoint
python scripts/daily_job_scheduler.py manual --job prices
```

### Recovery Procedures

#### Restart Failed Jobs
```bash
# Stop scheduler
docker stop ats-intg-scheduler

# Clear failed checkpoints
python scripts/run_intg.py query --query "UPDATE intg_daily_price_checkpoint SET status = 'pending' WHERE status = 'failed'"

# Restart scheduler
docker start ats-intg-scheduler
```

#### Database Backup and Recovery
```bash
# Manual backup
docker exec postgres-intg pg_dump -U postgres intg_db > /backup/manual_backup.sql

# Restore from backup
cat /backup/manual_backup.sql | docker exec -i postgres-intg psql -U postgres -d intg_db
```

## 🔄 Deployment Options

### 1. Docker Compose (Production)
- **Pros**: Full automation, monitoring, persistence
- **Cons**: Requires Docker environment
- **Use Case**: Production deployments

### 2. Cron Jobs (Lightweight)  
- **Pros**: Simple, resource efficient
- **Cons**: Manual setup, limited monitoring
- **Use Case**: Development, testing

### 3. Systemd Timers (Linux)
- **Pros**: System integration, reliable scheduling
- **Cons**: Linux-specific, more complex setup
- **Use Case**: Linux production servers

## 📋 Maintenance Tasks

### Daily
- Monitor job completion via logs
- Check data quality metrics
- Verify API key quotas

### Weekly  
- Review job performance metrics
- Clean up old log files
- Backup database

### Monthly
- Update vendor API configurations
- Review and optimize job schedules
- Archive historical data

## 🚀 Scaling Considerations

### High Volume Processing
- Increase `MAX_WORKERS` in job scripts
- Implement job splitting by symbol ranges
- Use multiple scheduler instances

### API Quota Management
- Rotate API keys across multiple accounts
- Implement adaptive rate limiting
- Add vendor failover logic

### Database Optimization
- Partition tables by date ranges
- Implement data retention policies
- Add read replicas for analytics

---

## 📞 Support

For issues and questions:
1. Check logs: `docker logs ats-intg-scheduler -f`
2. Run manual tests: `python scripts/daily_job_scheduler.py manual --job <type>`
3. Check database status: `python scripts/monitor_daily_jobs.py`
4. Review this documentation and job configurations

**🎯 The system is designed for reliability, monitoring, and easy troubleshooting. All jobs include comprehensive logging, checkpoint recovery, and graceful error handling.**