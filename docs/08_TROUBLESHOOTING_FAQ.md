# 🆘 ATS Troubleshooting & FAQ Guide

**Common issues, emergency procedures, debugging guides, and comprehensive solutions for the ATS platform.**

---

## 🚨 Emergency Procedures

### System Recovery (Critical Issues)

**Complete System Restart:**
```bash
# Emergency: Stop all ATS services
sudo systemctl stop ats-autostart
docker stop $(docker ps -q --filter "name=ats")
docker rm $(docker ps -aq --filter "name=ats")

# Clean restart with full validation
./scripts/ats_startup.sh

# Verify all services are healthy
curl -f http://localhost:3000/health  # ATS-DEV
curl -f http://localhost:4000/health  # ATS-INTG
curl -f http://localhost:4080/health  # Prometheus
python scripts/run_dev.py status
python scripts/run_intg.py status
```

**Database Emergency Recovery:**
```bash
# If databases are corrupted or inaccessible
sudo systemctl stop ats-autostart

# Restore from latest backup
PGPASSWORD=dev_password pg_restore -h localhost -p 3432 -U postgres -d dev_db /mnt/d/ats-backup/latest-dev-backup.sql
PGPASSWORD=intg_password pg_restore -h localhost -p 4432 -U postgres -d intg_db /mnt/d/ats-backup/latest-intg-backup.sql

# Restart services
sudo systemctl start ats-autostart
```

**Data Pipeline Recovery:**
```bash
# If data collection stops working
python scripts/run_intg.py stop --service realtime-minute-collector
python scripts/run_intg.py stop --service news-realtime

# Clear any stuck processes
docker logs ats-intg-realtime-minute-collector --tail 50
docker logs ats-intg-news-realtime --tail 50

# Restart data services
python scripts/run_intg.py start --service realtime-minute-collector
python scripts/run_intg.py start --service news-realtime

# Verify data flow resumed
python scripts/run_intg.py query --query "SELECT MAX(created_at) FROM intg_minute_bars WHERE created_at >= CURRENT_DATE"
```

---

## 🐳 Docker & Container Issues

### "Connection refused" Errors

**Symptom**: Services can't reach database or other services
**Root Cause**: Containers on different Docker networks
**Solution**:
```bash
# Debug network connectivity
docker inspect <container_name> | grep NetworkMode
# Should show "ats-network" or "ats-intg-network", not "bridge"

docker network inspect ats-network
docker network inspect ats-intg-network

# Fix: Ensure containers are on correct network
docker network connect ats-network ats-dev-postgres
docker network connect ats-intg-network ats-intg-postgres

# Restart services to apply network changes
python scripts/run_dev.py restart --service analytics
python scripts/run_intg.py restart --service analytics
```

### Port Conflicts

**Symptom**: "Port already in use" errors during startup
**Root Cause**: Dev and intg services using same external ports
**Solution**:
```bash
# Check what's using the ports
netstat -tulpn | grep -E "(3000|4000|3432|4432)"
docker ps | grep -E "(3000|4000|3432|4432)"

# Kill processes using conflicting ports
sudo lsof -ti:3000 | xargs kill -9
sudo lsof -ti:4000 | xargs kill -9

# Ensure correct port mappings in docker-compose files
# DEV: 3000, 3432
# INTG: 4000, 4432, 4080, 4002
```

### Container Startup Failures

**Symptom**: Containers exit immediately or fail to start
**Debug Steps**:
```bash
# Check container logs for errors
docker logs ats-dev-analytics --tail 50
docker logs ats-intg-postgres --tail 50

# Common issues and fixes:
# 1. Volume mount failures
ls -la /mnt/d/ats-data /mnt/d/ats-backup /mnt/d/ats-logs
sudo chown -R 1000:1000 /mnt/d/ats-data /mnt/d/ats-backup /mnt/d/ats-logs

# 2. Environment variable issues
docker inspect ats-dev-analytics | grep -A 20 "Env"
# Verify DB_HOST, API keys, PYTHONPATH are set correctly

# 3. Image issues
docker pull your-image:latest
docker system prune -f  # Remove unused images
```

---

## 🗄️ Database Issues

### Database Connection Failures

**Symptom**: "Connection refused", "password authentication failed"
**Solutions**:
```bash
# Test direct database connection
PGPASSWORD=dev_password psql -h localhost -p 3432 -U postgres -d dev_db -c "SELECT version()"
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "SELECT version()"

# If connection fails:
# 1. Check PostgreSQL container is running
docker ps | grep postgres

# 2. Check PostgreSQL logs
docker logs ats-dev-postgres --tail 50
docker logs ats-intg-postgres --tail 50

# 3. Restart PostgreSQL service
python scripts/run_dev.py restart --service postgres
docker restart ats-intg-postgres

# 4. Check database files aren't corrupted
docker exec ats-dev-postgres pg_controldata /var/lib/postgresql/data
```

### Database Performance Issues

**Symptom**: Slow queries, connection timeouts
**Diagnostic Steps**:
```bash
# Check active connections
python scripts/run_dev.py query --query "
SELECT count(*) as active_connections, 
       max(now() - query_start) as longest_query,
       state
FROM pg_stat_activity 
GROUP BY state
"

# Identify slow queries
python scripts/run_dev.py query --query "
SELECT query, calls, total_time, mean_time
FROM pg_stat_statements 
ORDER BY total_time DESC LIMIT 10
"

# Check database size and bloat
python scripts/run_dev.py query --query "
SELECT schemaname, tablename, 
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 10
"

# Solutions:
# 1. Kill long-running queries
python scripts/run_dev.py query --query "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query_start < now() - interval '1 hour'"

# 2. Run VACUUM and REINDEX
python scripts/run_dev.py query --query "VACUUM ANALYZE"
python scripts/run_dev.py query --query "REINDEX DATABASE dev_db"

# 3. Restart PostgreSQL if needed
python scripts/run_dev.py restart --service postgres
```

### Migration Issues

**Symptom**: Migration failures, schema conflicts
**Solutions**:
```bash
# Check migration status
python scripts/run_migrations.py --status --environment dev
python scripts/run_migrations.py --status --environment intg

# View failed migration details
python scripts/run_dev.py query --query "SELECT * FROM db_version ORDER BY version DESC LIMIT 10"

# Rollback problematic migration
python scripts/run_migrations.py --rollback --version 071 --environment dev

# Fix schema conflicts manually
python scripts/run_dev.py query --query "DROP TABLE IF EXISTS conflicting_table CASCADE"

# Re-run migration
python scripts/run_migrations.py --migrate --environment dev
```

---

## 🚀 API & Service Issues

### Service Health Check Failures

**Symptom**: HTTP 5xx errors, service unavailable
**Debug Process**:
```bash
# Check service status
python scripts/run_dev.py status
python scripts/run_intg.py status

# Test individual service endpoints
curl -v http://localhost:3000/health
curl -v http://localhost:4000/health
curl -v http://localhost:8000/health

# Check service logs
docker logs ats-dev-analytics --tail 100
docker logs ats-intg-analytics --tail 100

# Common fixes:
# 1. Service restart
python scripts/run_dev.py restart --service analytics
python scripts/run_intg.py restart --service analytics

# 2. Check resource usage
docker stats ats-dev-analytics ats-intg-analytics

# 3. Verify environment variables
docker inspect ats-dev-analytics | grep -A 10 "Env"
```

### API Key Authentication Failures

**Symptom**: 401/403 errors, "Invalid API key" messages
**Solutions**:
```bash
# Validate all API keys
python scripts/validate_api_keys.py

# Test specific vendor APIs directly
curl -s "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2025-09-11/2025-09-11?adjusted=true&sort=asc&limit=1&apikey=wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"

# Check service API key configuration
docker inspect ats-intg-realtime-minute-collector | grep -A 5 "POLYGON_API_KEY"
docker inspect ats-intg-news-realtime | grep -A 5 "TIINGO_API_KEY"

# Fix: Restart services with correct API keys
docker stop ats-intg-realtime-minute-collector
docker rm ats-intg-realtime-minute-collector
python scripts/run_intg.py start --service realtime-minute-collector

# Monitor for API rate limits
curl -s http://localhost:4080/metrics | grep "api_rate_limit"
```

### Data Loading Issues

**Symptom**: "Loading database tables..." screens, no data displayed
**Solutions**:
```bash
# Check database connectivity from service
docker exec ats-intg-analytics python -c "
import psycopg2
conn = psycopg2.connect('postgresql://postgres:intg_password@ats-intg-postgres:5432/intg_db')
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM intg_instruments')
print(f'Instruments count: {cursor.fetchone()[0]}')
"

# Verify data exists
python scripts/run_intg.py query --query "SELECT COUNT(*) FROM intg_instruments"
python scripts/run_intg.py query --query "SELECT COUNT(*) FROM intg_training_dataset"

# Check for data loading errors in service logs
docker logs ats-intg-analytics | grep -E "(ERROR|FAIL|Exception)"

# Common fixes:
# 1. Wrong database host in container
# Check DB_HOST=ats-intg-postgres (not localhost)

# 2. Missing data
python scripts/populate_sample_data.py --environment intg

# 3. Service restart
python scripts/run_intg.py restart --service analytics
```

---

## 💾 Data & Storage Issues

### Disk Space Issues

**Symptom**: "No space left on device" errors
**Solutions**:
```bash
# Check disk usage
df -h /mnt/d  # ATS data storage
df -h /home   # System storage
df -h /var    # Logs and temp files

# Find large files
find /mnt/d -type f -size +1G -exec ls -lh {} + | sort -k5 -hr | head -20
find /var/log -name "*.log" -size +100M -exec ls -lh {} +

# Clean up solutions:
# 1. Compress old log files
find /mnt/d/ats-logs -name "*.log" -mtime +30 -exec gzip {} \;

# 2. Remove old backup files
find /mnt/d/ats-backup -name "*.sql" -mtime +90 -delete

# 3. Clean Docker system
docker system prune -f --volumes

# 4. Archive old training data
find /mnt/d/ats-data/training-data -name "*.arrayrecord" -mtime +180 -exec tar czf archived-training-data.tar.gz {} + --remove-files
```

### Training Data Issues

**Symptom**: ArrayRecord files not found, training data generation failures
**Solutions**:
```bash
# Check training data structure
ls -la /mnt/d/ats-data/training-data/
find /mnt/d/ats-data/training-data -name "*.arrayrecord" | head -10

# Verify training dataset registry
python scripts/run_dev.py query --query "SELECT id, dataset_name, file_path FROM dev_training_dataset ORDER BY id DESC LIMIT 5"

# Debug training data generation
python scripts/debug_training_data.py --dataset-id 1 --verbose

# Check for permission issues
sudo chown -R 1000:1000 /mnt/d/ats-data/training-data
chmod -R 755 /mnt/d/ats-data/training-data

# Regenerate missing training data
python scripts/run_dev.py run --script src/domains/ml/services/training_data/runners/training_data_callback_runner.py

# Validate ArrayRecord files
python scripts/validate_arrayrecord.py --path /mnt/d/ats-data/training-data --check-all
```

### Data Quality Issues

**Symptom**: Incorrect data values, missing data, data corruption
**Diagnostic Steps**:
```bash
# Check data completeness
python scripts/run_dev.py query --query "
SELECT symbol, COUNT(*) as records, MIN(date) as start_date, MAX(date) as end_date
FROM dev_daily_prices 
WHERE date >= '2024-01-01' 
GROUP BY symbol 
HAVING COUNT(*) < 200  -- Expected ~250 trading days
ORDER BY records
"

# Check for anomalous values
python scripts/run_dev.py query --query "
SELECT symbol, date, open, high, low, close, volume
FROM dev_daily_prices 
WHERE close <= 0 OR volume < 0 OR close > 10000 OR high < low
ORDER BY date DESC LIMIT 20
"

# Validate training data quality
python scripts/validate_training_data.py --dataset-id 1 --check-quality

# Solutions:
# 1. Re-fetch problematic data
python scripts/refetch_data.py --symbol AAPL --start-date 2024-01-01 --end-date 2024-12-31

# 2. Run data cleaning pipeline
python scripts/clean_data_anomalies.py --fix-prices --fix-volumes

# 3. Regenerate training datasets
python scripts/regenerate_training_data.py --dataset-id 1 --force
```

---

## 🌐 Network & Connectivity Issues

### WSL Networking Issues (Windows)

**Symptom**: Services unreachable from host, DNS resolution failures
**Solutions**:
```bash
# Check WSL networking
ip addr show eth0
cat /etc/resolv.conf

# Fix WSL networking
sudo /mnt/c/Windows/System32/wsl.exe --shutdown
# Restart WSL from Windows

# Test connectivity from WSL to host
ping $(ip route | grep default | awk '{print $3}')

# Fix Docker networking in WSL
sudo service docker restart
docker network prune -f

# Re-create ATS networks
docker network rm ats-network ats-intg-network
docker network create ats-network
docker network create ats-intg-network
```

### External API Connectivity

**Symptom**: API calls timing out, connection errors to external services
**Debug Steps**:
```bash
# Test external API connectivity
curl -v "https://api.polygon.io/v2/ping"
curl -v "https://api.tiingo.com/api/ping"
curl -v "https://eodhistoricaldata.com/api/ping"

# Check DNS resolution
nslookup api.polygon.io
nslookup api.tiingo.com

# Test from within containers
docker exec ats-intg-realtime-minute-collector curl -v "https://api.polygon.io/v2/ping"

# Common fixes:
# 1. Check firewall/proxy settings
# 2. Verify system time is correct (API timestamps)
date
sudo ntpdate -s time.nist.gov

# 3. Check SSL certificates
curl -k https://api.polygon.io/v2/ping  # Skip SSL verification for testing
```

---

## 🧪 Testing & Development Issues

### Test Failures

**Symptom**: Tests failing during development or CI/CD
**Debug Process**:
```bash
# Run tests with detailed output
PYTHONPATH=src pytest tests/integration/ -v -s --tb=short

# Run specific failing test
PYTHONPATH=src pytest tests/integration/test_specific.py::test_function -vvv

# Check test environment setup
python scripts/run_dev.py status  # Ensure test services are running

# Common test issues:
# 1. Database not populated with test data
python scripts/populate_test_data.py

# 2. Services not started
python scripts/run_dev.py start --service postgres
python scripts/run_dev.py start --service analytics

# 3. Environment variable issues
export PYTHONPATH=/home/jianjun/ats-genai-data/src
export ENVIRONMENT=dev

# 4. Port conflicts during testing
sudo lsof -i :3000  # Check if port is in use
```

### Browser Tests (Playwright) Issues

**Symptom**: Browser tests timing out, element not found errors
**Solutions**:
```bash
# Install/update Playwright browsers
python -m playwright install chromium

# Run browser tests with debug mode
PYTHONPATH=src pytest tests/browser_tests/test_eda_playwright.py -v --headed --slowmo=1000

# Check if analytics service is accessible
curl -f http://localhost:3000/health
curl -f http://localhost:3000/eda

# Common browser test issues:
# 1. Service not running or responding slowly
python scripts/run_dev.py start --service analytics
sleep 10  # Wait for service startup

# 2. JavaScript errors on page
# Check browser console in headed mode

# 3. Element selectors changed
# Update selectors in test files

# Debug specific test
PYTHONPATH=src python -m pytest tests/browser_tests/test_eda_playwright.py::test_dataset_loading -v --headed
```

---

## 🔧 Performance Issues

### Slow Query Performance

**Symptom**: Database queries taking too long
**Optimization Steps**:
```bash
# Enable query logging
python scripts/run_dev.py query --query "ALTER SYSTEM SET log_min_duration_statement = 1000"  # Log queries >1s
python scripts/run_dev.py query --query "SELECT pg_reload_conf()"

# Identify slow queries
python scripts/run_dev.py query --query "
SELECT query, calls, total_time, mean_time, stddev_time
FROM pg_stat_statements 
WHERE mean_time > 1000  -- Queries averaging >1s
ORDER BY total_time DESC LIMIT 10
"

# Create missing indexes
python scripts/run_dev.py query --query "
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_prices_symbol_date 
ON dev_daily_prices(symbol, date)
"

# Update table statistics
python scripts/run_dev.py query --query "ANALYZE"
```

### High Memory Usage

**Symptom**: Services consuming excessive memory, OOM kills
**Solutions**:
```bash
# Monitor memory usage
docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"

# Identify memory leaks
python -m memory_profiler scripts/your_script.py

# Configure memory limits
docker update --memory 2g ats-dev-analytics
docker update --memory 4g ats-intg-postgres

# Restart services to free memory
python scripts/run_dev.py restart --service analytics
python scripts/run_intg.py restart --service analytics

# Clean up memory usage
echo 3 | sudo tee /proc/sys/vm/drop_caches  # Clear system cache
```

---

## 📋 FAQ - Frequently Asked Questions

### Q: How do I reset the entire development environment?
```bash
# Complete reset (WARNING: destroys all data)
sudo systemctl stop ats-autostart
docker stop $(docker ps -aq)
docker rm $(docker ps -aq)
docker volume prune -f
docker network prune -f

# Recreate from scratch
python scripts/run_dev.py setup
```

### Q: How do I backup and restore data?
```bash
# Backup
mkdir -p /mnt/d/ats-backup/manual
PGPASSWORD=dev_password pg_dump -h localhost -p 3432 -U postgres dev_db > /mnt/d/ats-backup/manual/dev_backup_$(date +%Y%m%d).sql

# Restore
PGPASSWORD=dev_password psql -h localhost -p 3432 -U postgres -d dev_db < /mnt/d/ats-backup/manual/dev_backup_20250101.sql
```

### Q: How do I access logs for debugging?
```bash
# Service logs
docker logs ats-dev-analytics --tail 100 -f
docker logs ats-intg-postgres --tail 100 -f

# Application logs
tail -f /mnt/d/ats-logs/analytics.log
tail -f /mnt/d/ats-logs/data-pipeline.log

# System logs
sudo journalctl -u ats-autostart -f
```

### Q: How do I update API keys?
```bash
# Validate current keys
python scripts/validate_api_keys.py

# Update keys in environment
export POLYGON_API_KEY="new_key_here"

# Restart services to use new keys
python scripts/run_intg.py restart --service realtime-minute-collector
python scripts/run_intg.py restart --service news-realtime
```

### Q: How do I add a new symbol to the system?
```bash
# Add to instruments table
python scripts/run_dev.py query --query "
INSERT INTO dev_instruments (symbol, company_name, sector, industry, exchange)
VALUES ('NEWSYM', 'New Company', 'Technology', 'Software', 'NASDAQ')
"

# Add to universe if needed
python scripts/run_dev.py query --query "
INSERT INTO dev_universe_membership (symbol, in_universe, entry_date)
VALUES ('NEWSYM', true, CURRENT_DATE)
"

# Collect historical data
python scripts/collect_symbol_data.py --symbol NEWSYM --start-date 2020-01-01
```

---

**🎯 This troubleshooting guide provides comprehensive solutions for common ATS platform issues, emergency procedures, and debugging workflows.**