# ATS Data Quality Agent - Troubleshooting Guide

## 🔍 Quick Diagnostics

### 1. System Health Check
```bash
# Quick health validation
python scripts/quick_health_check.py

# Comprehensive system validation  
python scripts/validate_system.py

# API endpoint testing
PYTHONPATH=src python scripts/test_api_endpoints.py
```

### 2. Service Status
```bash
# Check Docker services
docker-compose -f docker-compose.production.yml ps

# Check individual service logs
docker logs ats-prod-analytics --tail 50
docker logs ats-prod-postgres --tail 50

# Test database connectivity
docker exec ats-prod-postgres pg_isready -U postgres
```

### 3. Agent Status
```bash
# Check agent status via API
curl http://localhost:4000/agent/status

# Check system health
curl http://localhost:4000/agent/system-health

# Check active alerts
curl http://localhost:4000/agent/alerts
```

## 🚨 Common Issues

### Agent Won't Start

#### Symptoms
- Agent status shows "STOPPED"
- Dashboard shows connection errors
- Start button doesn't work

#### Diagnosis Steps
```bash
# Check service logs
docker logs ats-prod-analytics --tail 100

# Check configuration
curl http://localhost:4000/agent/config

# Test database connection
python scripts/test_database_integration.py

# Check resource usage
docker stats
```

#### Common Causes & Solutions

**1. Database Connection Failed**
```bash
# Check database status
docker exec ats-prod-postgres pg_isready -U postgres

# Check network connectivity
docker network inspect ats-network

# Verify connection settings
docker exec ats-prod-analytics cat config/production.env
```

**2. Configuration Issues**
```bash
# Reset to default configuration
curl -X POST http://localhost:4000/agent/config/preset/production

# Validate configuration
python -c "
import sys
sys.path.append('src')
from agents.agent_config import get_config_manager
print(get_config_manager().validate_config())
"
```

**3. Resource Constraints**
```bash
# Check system resources
free -h
df -h
docker stats

# Reduce resource usage
curl -X PUT http://localhost:4000/agent/config \
  -H "Content-Type: application/json" \
  -d '{"monitoring": {"max_concurrent_workflows": 10}}'
```

### High Resource Usage

#### Symptoms
- High CPU/memory in system health
- Slow dashboard response
- System alerts for resource usage

#### Immediate Actions
```bash
# Check current resource usage
curl http://localhost:4000/agent/system-health

# Review active workflows
curl http://localhost:4000/agent/workflows

# Check for stuck processes
docker exec ats-prod-analytics ps aux
```

#### Optimization Steps

**1. Reduce Monitoring Frequency**
```bash
curl -X PUT http://localhost:4000/agent/config \
  -H "Content-Type: application/json" \
  -d '{"monitoring": {"cycle_interval_seconds": 600}}'
```

**2. Limit Concurrent Operations**
```bash
curl -X PUT http://localhost:4000/agent/config \
  -H "Content-Type: application/json" \
  -d '{"monitoring": {"max_concurrent_workflows": 5}}'
```

**3. Clean Up Old Data**
```bash
# Clean old logs
find logs/ -name "*.log" -mtime +7 -delete

# Clean database (if safe)
docker exec ats-prod-postgres psql -U postgres -c "
DELETE FROM agent_issues WHERE created_at < NOW() - INTERVAL '30 days';
DELETE FROM agent_workflows WHERE completed_at < NOW() - INTERVAL '7 days';
"
```

### Database Connection Issues

#### Symptoms
- "Database connection error" in logs
- Agent status shows database as unhealthy
- API endpoints return 500 errors

#### Diagnosis
```bash
# Test database connectivity
docker exec ats-prod-postgres pg_isready -U postgres

# Check database logs
docker logs ats-prod-postgres --tail 100

# Test from analytics container
docker exec ats-prod-analytics python -c "
import psycopg2
try:
    conn = psycopg2.connect(
        host='ats-prod-postgres',
        port=5432,
        user='postgres',
        password='your_password',
        database='prod_db'
    )
    print('Connection successful')
    conn.close()
except Exception as e:
    print(f'Connection failed: {e}')
"
```

#### Solutions

**1. Network Issues**
```bash
# Check network configuration
docker network inspect ats-network

# Ensure containers are on same network
docker inspect ats-prod-analytics | grep NetworkMode
docker inspect ats-prod-postgres | grep NetworkMode
```

**2. Authentication Issues**
```bash
# Verify credentials
docker exec ats-prod-analytics env | grep DB_

# Test credentials manually
docker exec ats-prod-postgres psql -U postgres -d prod_db -c "SELECT 1;"
```

**3. Database Health Issues**
```bash
# Check database size and connections
docker exec ats-prod-postgres psql -U postgres -c "
SELECT datname, pg_size_pretty(pg_database_size(datname)) 
FROM pg_database WHERE datname = 'prod_db';
"

# Check active connections
docker exec ats-prod-postgres psql -U postgres -c "
SELECT count(*) FROM pg_stat_activity WHERE datname = 'prod_db';
"
```

### Dashboard Not Loading

#### Symptoms
- Dashboard URL returns errors
- Empty dashboard with no data
- JavaScript errors in browser console

#### Diagnosis
```bash
# Check analytics service
curl http://localhost:4000/health

# Check dashboard endpoint
curl http://localhost:4000/data-quality/dashboard/status

# Check logs for errors
docker logs ats-prod-analytics | grep -i error
```

#### Solutions

**1. Service Not Running**
```bash
# Restart analytics service
docker-compose -f docker-compose.production.yml restart ats-prod-analytics

# Check startup logs
docker logs ats-prod-analytics --follow
```

**2. Configuration Issues**
```bash
# Check port mapping
docker port ats-prod-analytics

# Verify configuration
curl http://localhost:4000/agent/config | jq .
```

**3. Database Connection**
```bash
# Test dashboard data endpoint
curl http://localhost:4000/data-quality/dashboard/data

# If fails, check database connection as above
```

### No Issues Detected

#### Symptoms
- Dashboard shows 0 issues despite known problems
- Quality score stuck at 100
- No alerts generated

#### Diagnosis
```bash
# Check if agent is scanning
curl http://localhost:4000/agent/status | jq .last_scan_time

# Check scanning configuration
curl http://localhost:4000/agent/config | jq .monitoring

# Test manual quality scan
curl -X POST http://localhost:4000/agent/tools/quality_scan/execute \
  -H "Content-Type: application/json" \
  -d '{
    "parameters": {
      "table_name": "intg_daily_prices",
      "date_range": {"start_date": "2024-11-30", "end_date": "2024-12-01"}
    }
  }'
```

#### Solutions

**1. Configuration Issues**
```bash
# Check if monitoring is enabled
curl http://localhost:4000/agent/config | jq .monitoring.enable_automatic_scanning

# Reset thresholds if too lenient
curl -X PUT http://localhost:4000/agent/config \
  -H "Content-Type: application/json" \
  -d '{
    "issue_thresholds": {
      "quality_score_critical_threshold": 70,
      "extreme_volume_multiplier": 10.0
    }
  }'
```

**2. Data Source Issues**
```bash
# Check if there's actually data to scan
docker exec ats-prod-postgres psql -U postgres -d prod_db -c "
SELECT COUNT(*) FROM intg_daily_prices WHERE date >= CURRENT_DATE - INTERVAL '7 days';
"

# Check data quality manually
python scripts/manual_quality_check.py
```

### Performance Issues

#### Slow API Response
```bash
# Check database query performance
docker exec ats-prod-postgres psql -U postgres -d prod_db -c "
SELECT query, mean_time, calls 
FROM pg_stat_statements 
WHERE query LIKE '%agent_%' 
ORDER BY mean_time DESC LIMIT 10;
"

# Check for table locks
docker exec ats-prod-postgres psql -U postgres -d prod_db -c "
SELECT * FROM pg_locks WHERE NOT granted;
"
```

#### Memory Leaks
```bash
# Monitor memory usage over time
watch -n 10 'docker stats --no-stream ats-prod-analytics'

# Check for growing log files
du -sh logs/*

# Check for growing database tables
docker exec ats-prod-postgres psql -U postgres -d prod_db -c "
SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
FROM pg_tables 
WHERE schemaname = 'public' 
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
"
```

## 🛠️ Advanced Troubleshooting

### Debug Mode Activation

```bash
# Enable debug logging
curl -X PUT http://localhost:4000/agent/config \
  -H "Content-Type: application/json" \
  -d '{"logging": {"log_level": "DEBUG"}}'

# Check debug logs
docker logs ats-prod-analytics | grep DEBUG
```

### Database Debugging

```bash
# Check database constraints and indexes
docker exec ats-prod-postgres psql -U postgres -d prod_db -c "
\d+ agent_issues
\d+ agent_workflows
\d+ agent_alerts
"

# Check database statistics
docker exec ats-prod-postgres psql -U postgres -d prod_db -c "
SELECT schemaname, tablename, n_tup_ins, n_tup_upd, n_tup_del
FROM pg_stat_user_tables 
WHERE schemaname = 'public';
"
```

### Network Debugging

```bash
# Test container-to-container networking
docker exec ats-prod-analytics ping ats-prod-postgres

# Test port accessibility
docker exec ats-prod-analytics telnet ats-prod-postgres 5432

# Check firewall rules (if applicable)
sudo iptables -L -n | grep 4000
```

### Log Analysis

```bash
# Find error patterns
grep -i error logs/agent/*.log | tail -20

# Find performance issues
grep "slow_query\|timeout\|failed" logs/agent/*.log

# Analyze alert patterns
jq '.severity' logs/alerts/alerts_production.jsonl | sort | uniq -c
```

## 🚨 Emergency Procedures

### Complete System Recovery

```bash
# 1. Stop all services
docker-compose -f docker-compose.production.yml down

# 2. Check system resources
free -h && df -h

# 3. Clean up if needed
docker system prune -f
docker volume prune -f

# 4. Restart with fresh logs
mv logs logs.backup.$(date +%Y%m%d_%H%M%S)
mkdir -p logs/{agent,alerts,system}

# 5. Start services
docker-compose -f docker-compose.production.yml up -d

# 6. Validate recovery
python scripts/validate_system.py
```

### Database Recovery

```bash
# 1. Create backup
docker exec ats-prod-postgres pg_dump -U postgres prod_db > emergency_backup.sql

# 2. Check database integrity
docker exec ats-prod-postgres psql -U postgres -d prod_db -c "
SELECT datname, pg_database_size(datname) FROM pg_database WHERE datname = 'prod_db';
"

# 3. If corruption detected, restore from backup
docker exec ats-prod-postgres psql -U postgres -d prod_db -c "DROP DATABASE prod_db;"
docker exec ats-prod-postgres psql -U postgres -c "CREATE DATABASE prod_db;"
docker exec -i ats-prod-postgres psql -U postgres -d prod_db < latest_backup.sql
```

### Configuration Recovery

```bash
# 1. Backup current config
curl http://localhost:4000/agent/config > config_backup.json

# 2. Reset to factory defaults
curl -X POST http://localhost:4000/agent/config/preset/production

# 3. Validate configuration
python -c "
import sys, json
sys.path.append('src')
from agents.agent_config import get_config_manager
config = get_config_manager().get_config()
print(json.dumps(config, indent=2))
"
```

## 📞 Getting Help

### Gather Diagnostic Information

Before contacting support, gather this information:

```bash
# System information
uname -a > diagnostic_info.txt
docker --version >> diagnostic_info.txt
docker-compose --version >> diagnostic_info.txt

# Service status
docker-compose -f docker-compose.production.yml ps >> diagnostic_info.txt

# Recent logs
docker logs ats-prod-analytics --tail 100 >> diagnostic_info.txt
docker logs ats-prod-postgres --tail 50 >> diagnostic_info.txt

# Configuration
curl http://localhost:4000/agent/config >> diagnostic_info.txt

# System health
curl http://localhost:4000/agent/system-health >> diagnostic_info.txt

# Resource usage
docker stats --no-stream >> diagnostic_info.txt
```

### Common Solutions Summary

| Issue | Quick Fix | 
|-------|-----------|
| Agent won't start | `curl -X POST http://localhost:4000/agent/start` |
| Database connection | Check network: `docker network inspect ats-network` |
| High CPU usage | Reduce frequency: increase `cycle_interval_seconds` |
| Dashboard not loading | Restart service: `docker-compose restart ats-prod-analytics` |
| No issues detected | Check thresholds in configuration |
| Memory issues | Clean logs and reduce `max_concurrent_workflows` |

### Log Locations

- **Agent logs**: `logs/agent/`
- **Alert logs**: `logs/alerts/`
- **System logs**: `logs/system/`
- **Docker logs**: `docker logs <container_name>`

Remember to always check logs first before implementing solutions!