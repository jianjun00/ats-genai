# ATS-INTG Deployment Guide

## Overview

ATS-INTG (Integration Environment) is a production-ready data integration system that automatically manages data migration from ATS-DEV to a dedicated integration database with intelligent startup orchestration and continuous scheduling.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   ATS-DEV DB    │────│  ATS-INTG       │────│   INTG DB       │
│   (Source)      │    │  Startup Mgr    │    │   (Target)      │
│                 │    │                 │    │                 │
│ • Instruments   │    │ • Auto-Migration│    │ • intg_*        │
│ • Daily Prices  │    │ • DEV Connectivity│  │   tables        │
│ • Fundamentals  │    │ • Decision Logic│    │ • Persistence   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  Continuous     │
                    │  Scheduler      │
                    │                 │
                    │ • Hourly Status │
                    │ • Health Checks │
                    │ • Log Reports   │
                    └─────────────────┘
```

## Quick Start

### Prerequisites
- Docker and Docker Compose
- ATS-DEV database running on port 3434
- Available ports: 4434 (PostgreSQL), 4000 (Dashboard)

### Deployment Commands
```bash
# Deploy complete ATS-INTG system
docker-compose -f docker-compose.intg-jobs.yml up -d

# Monitor startup process
docker logs ats-intg-scheduler -f

# Check system status
docker ps | grep ats-intg
```

### Verification
```bash
# Check startup manager logs
docker logs ats-intg-scheduler --tail 20

# Connect to INTG database
PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db

# View startup report
docker exec ats-intg-scheduler cat /logs/startup_report.md
```

## Startup Manager Decision Logic

The intelligent startup manager (`scripts/intg_startup_manager.py`) follows this decision tree:

```
Start
  │
  ▼
┌─────────────────┐
│ Wait for        │
│ PostgreSQL      │ ──── Timeout ────► Exit with Error
└─────────────────┘
  │ Success
  ▼
┌─────────────────┐
│ Check INTG      │
│ Database Status │
└─────────────────┘
  │
  ▼
Has Data? ──── Yes ────► Run Incremental Sync ────► Success
  │
  │ No
  ▼
┌─────────────────┐
│ Check DEV       │      No DEV      ┌─────────────────┐
│ Connectivity    │ ──────────────────│ Start Empty     │
└─────────────────┘                  │ Database        │
  │                                  └─────────────────┘
  │ DEV Available                              │
  ▼                                            │
┌─────────────────┐                           │
│ Check DEV       │      No Data             │
│ Data Available  │ ─────────────────────────┤
└─────────────────┘                          │
  │                                           │
  │ Data Available                            │
  ▼                                           │
┌─────────────────┐                          │
│ Run Full        │                          │
│ Migration       │                          │
└─────────────────┘                          │
  │                                           │
  ▼                                           ▼
┌─────────────────────────────────────────────────┐
│        Start Continuous Scheduler              │
│     • Hourly status updates                    │
│     • Daily refresh job scheduling             │
│     • Health monitoring                        │
└─────────────────────────────────────────────────┘
```

## Configuration

### Environment Variables
```yaml
# Docker Compose Environment
AUTO_MIGRATION_ENABLED: true          # Enable automatic migration
DEV_DB_HOST: 172.17.0.1               # DEV database host
DEV_DB_PORT: 5433                     # DEV database port
DEV_DB_USER: postgres                 # DEV database user
DEV_DB_PASSWORD: postgres             # DEV database password
DEV_DB_NAME: dev_db                   # DEV database name

# API Keys (for future data refresh jobs)
POLYGON_API_KEY: [key]
FMP_API_KEY: [key]
TIINGO_API_KEY: [key]
ALPHA_VANTAGE_API_KEY: [key]
```

### Database Schema
The system creates these tables automatically:
- `intg_instruments` - Instrument reference data
- `intg_daily_prices` - Price data from all vendors
- `intg_fundamentals_comprehensive` - Fundamental data
- `intg_sync_history` - Migration tracking
- `intg_backfill_tracking` - Backfill progress

## Migration Strategies

### Full Migration
- **Trigger**: Empty INTG database + DEV data available
- **Process**: Complete data copy from DEV to INTG
- **Scripts**: `scripts/intg_data_backfill.py`
- **Duration**: Varies based on data volume

### Incremental Sync  
- **Trigger**: Existing INTG data
- **Process**: Sync only changed records
- **Scripts**: `scripts/intg_incremental_sync.py`
- **Frequency**: Every 6 hours (configurable)

## Scheduled Jobs Framework

The system includes a framework for daily refresh jobs:

```bash
# Incremental sync from DEV (every 6 hours)
0 */6 * * * python scripts/intg_incremental_sync.py sync --lookback-hours 8

# Daily Price Refresh - 05:00 UTC
0 5 * * * python scripts/daily_price_refresh_job.py

# Daily Fundamentals Refresh - 06:30 UTC  
30 6 * * * python scripts/daily_fundamentals_refresh_job.py

# Weekly Data Validation - Sundays 02:00 UTC
0 2 * * 0 python scripts/weekly_data_validation_job.py
```

## Monitoring & Troubleshooting

### Health Checks
```bash
# Container status
docker ps | grep ats-intg

# Startup manager logs
docker logs ats-intg-scheduler --tail 50

# Database connectivity
docker exec ats-intg-scheduler python3 -c "
import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print('INTG DB:', sock.connect_ex(('postgres-intg', 5432)) == 0)
print('DEV DB:', sock.connect_ex(('172.17.0.1', 5433)) == 0)
"
```

### Common Issues

#### Startup Manager Won't Start
```bash
# Check PostgreSQL status
docker logs postgres-intg

# Check for port conflicts
netstat -tulpn | grep 5434

# Recreate containers
docker-compose -f docker-compose.intg-jobs.yml down
docker-compose -f docker-compose.intg-jobs.yml up -d
```

#### Migration Fails
```bash
# Check DEV connectivity
docker exec ats-intg-scheduler python3 scripts/run_dev.py query --query "SELECT 1"

# Manual migration
docker exec ats-intg-scheduler python3 scripts/intg_data_backfill.py validate
docker exec ats-intg-scheduler python3 scripts/intg_data_backfill.py backfill
```

#### No Data in INTG
```bash
# Check migration status
PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db -c "
SELECT 
  table_name,
  (xpath('/row/count/text()', xml_count))[1]::text::int as row_count
FROM (
  SELECT 
    table_name,
    query_to_xml('SELECT count(*) as count FROM ' || table_name, false, true, '') as xml_count
  FROM information_schema.tables 
  WHERE table_name LIKE 'intg_%'
) t;
"

# Force migration
docker exec ats-intg-scheduler python3 scripts/intg_startup_manager.py
```

## File Structure

```
ats-genai-data/
├── docker-compose.intg-jobs.yml       # Main deployment configuration
├── scripts/
│   ├── intg_startup_manager.py        # Intelligent startup orchestration
│   ├── intg_data_backfill.py          # Full migration scripts
│   ├── intg_incremental_sync.py       # Incremental sync scripts
│   ├── daily_price_refresh_job.py     # Daily price data jobs
│   ├── daily_fundamentals_refresh_job.py # Daily fundamentals jobs
│   └── weekly_data_validation_job.py  # Weekly validation jobs
├── docs/
│   ├── ATS-INTG-DEPLOYMENT-GUIDE.md   # This file
│   └── ATS-INTG-INCREMENTAL-SYNC.md   # Detailed sync documentation
└── tests/
    └── integration/
        └── test_intg_startup_manager.py # Integration tests
```

## Production Considerations

### Security
- Use secure passwords for production databases
- Rotate API keys regularly
- Enable SSL/TLS for database connections
- Restrict network access to necessary ports

### Performance
- Monitor database disk usage growth
- Set up log rotation for container logs
- Consider read replicas for query-heavy workloads
- Implement alerting for failed migrations

### Scaling
- Use Docker Swarm or Kubernetes for multiple instances
- Implement leader election for singleton jobs
- Add Prometheus metrics endpoints
- Set up horizontal pod autoscaling

### Backup & Recovery
- Implement database backup strategy
- Test restoration procedures
- Document rollback procedures
- Maintain migration history

## Support

For issues and questions:
1. Check logs: `docker logs ats-intg-scheduler`
2. Review startup report: `/logs/startup_report.md`
3. Verify database connectivity
4. Check Docker container health
5. Review this documentation

---

**Last Updated**: 2025-08-28  
**Version**: 1.0.0  
**Status**: Production Ready