# PostgreSQL Monitoring Integration with ATS Prometheus/Grafana

This document describes the complete integration of PostgreSQL monitoring with the existing ATS Prometheus/Grafana infrastructure.

## 🏗️ Architecture Overview

The PostgreSQL monitoring system integrates seamlessly with the existing ATS monitoring stack:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │  Data Agent     │    │   Prometheus    │
│   Database      │    │  Metrics        │    │   (port 9090)   │
│   (port 5432)   │    │  (port 8000)    │    │                 │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          │                      │                      │
          v                      v                      v
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ PostgreSQL      │───▶│   Prometheus    │───▶│    Grafana      │
│ Metrics Exporter│    │   Scraper       │    │  (port 3000)    │
│ (port 8001)     │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🎯 Features

### PostgreSQL Metrics Exported

**Connection Metrics:**
- `postgresql_connections_total` - Total number of connections
- `postgresql_connections_active` - Active connections
- `postgresql_connections_idle` - Idle connections  
- `postgresql_connections_max` - Maximum connections allowed

**Performance Metrics:**
- `postgresql_queries_per_second` - Query rate
- `postgresql_transactions_per_second` - Transaction rate
- `postgresql_cache_hit_ratio` - Buffer cache hit ratio
- `postgresql_index_usage_ratio` - Index usage effectiveness

**Resource Metrics:**
- `postgresql_cpu_percent` - CPU usage by PostgreSQL processes
- `postgresql_memory_mb` - Memory usage in MB
- `postgresql_disk_usage_percent` - Disk usage percentage

**Process Metrics:**
- `postgresql_worker_processes` - Number of worker processes
- `postgresql_connection_processes` - Number of connection processes
- `postgresql_uptime_seconds` - Database uptime

**Health & Issues:**
- `postgresql_healthy` - Health status (1=healthy, 0=unhealthy)
- `postgresql_blocked_queries` - Number of blocked queries
- `postgresql_long_running_queries` - Queries running >5 minutes

**Database Size:**
- `postgresql_database_size_mb` - Total database size
- `postgresql_temp_files_count` - Temporary files created
- `postgresql_temp_files_size_mb` - Temporary files size

## 🚀 Quick Start

### 1. Start Complete Monitoring Stack

```bash
# Setup integrated PostgreSQL monitoring
python scripts/monitoring/setup_postgres_monitoring.py
```

This will:
- ✅ Start PostgreSQL metrics exporter on port 8001
- ✅ Integrate with existing Prometheus configuration
- ✅ Import PostgreSQL dashboard into Grafana
- ✅ Provide continuous monitoring

### 2. Access Monitoring Interfaces

**PostgreSQL Metrics (Direct):**
```bash
curl http://localhost:8001/metrics
```

**Prometheus UI:**
- URL: http://localhost:9090
- PostgreSQL target: http://localhost:8001/metrics

**Grafana Dashboards:**
- URL: http://localhost:3000
- Username: `admin` / Password: `admin`
- PostgreSQL Dashboard: "PostgreSQL Database Dashboard"

## 📊 Grafana Dashboard

The PostgreSQL dashboard includes these panels:

1. **Connection Overview** - Total, active, idle connections vs max
2. **Resource Usage** - CPU and disk usage percentages  
3. **Health Status** - Overall database health indicator
4. **Query Performance** - Queries/sec and transactions/sec
5. **Cache Performance** - Cache hit ratio and index usage
6. **Process Monitoring** - Worker and connection processes
7. **Memory Usage** - PostgreSQL memory consumption
8. **Blocking Issues** - Blocked and long-running queries

## 🔧 Configuration

### Prometheus Configuration

The PostgreSQL job is automatically added to Prometheus:

```yaml
scrape_configs:
  - job_name: "postgresql"
    static_configs:
      - targets: ["localhost:8001"]
    metrics_path: "/metrics"
    scrape_interval: 30s
```

### Environment Variables

Control PostgreSQL monitoring with these variables:

```bash
# Database connection (uses existing ATS configuration)
export ENVIRONMENT=dev
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=postgres
export DB_PASSWORD=dev_password
export DB_NAME=dev_db

# Monitoring configuration
export POSTGRESQL_METRICS_PORT=8001
export POSTGRESQL_METRICS_INTERVAL=30
export POSTGRESQL_METRICS_PREFIX=postgresql
```

## 🎛️ Advanced Usage

### Standalone PostgreSQL Monitoring

```bash
# Run PostgreSQL monitoring without Prometheus integration
python src/monitoring/postgres_prometheus_exporter.py --port 8001 --interval 30

# Run system-level monitoring (no database auth required)
python scripts/monitoring/postgres_monitor.py --watch 30 --include-processes
```

### Custom Metrics Integration

Extend the monitoring with custom metrics:

```python
from monitoring.postgres_prometheus_exporter import PostgreSQLPrometheusExporter
from config.environment import Environment

# Setup custom exporter
env = Environment()
exporter = PostgreSQLPrometheusExporter(env, metrics_prefix="custom_pg", port=8002)

# Collect and export metrics
metrics = await exporter.collect_database_metrics()
exporter.update_prometheus_metrics(metrics)
```

### Kubernetes Integration

Deploy to Kubernetes using existing ATS infrastructure:

```yaml
# Add to existing prometheus-deployment.yaml
- job_name: 'postgresql'
  kubernetes_sd_configs:
  - role: pod
  relabel_configs:
  - source_labels: [__meta_kubernetes_pod_annotation_postgresql_prometheus_io_scrape]
    action: keep
    regex: true
  - source_labels: [__meta_kubernetes_pod_annotation_postgresql_prometheus_io_port]
    action: replace
    target_label: __address__
    regex: ([^:]+)(?::\d+)?;(\d+)
    replacement: $1:$2
```

## 📈 Monitoring Best Practices

### Alert Thresholds

Recommended alert thresholds for PostgreSQL:

```yaml
# High connection usage
postgresql_connections_active / postgresql_connections_max > 0.8

# Poor cache performance  
postgresql_cache_hit_ratio < 0.95

# High CPU usage
postgresql_cpu_percent > 80

# Blocking queries
postgresql_blocked_queries > 5

# Long running queries
postgresql_long_running_queries > 3

# Low disk space
postgresql_disk_usage_percent > 85
```

### Performance Tuning

Monitor these key metrics for performance:

1. **Cache Hit Ratio** - Should be >95%
2. **Connection Usage** - Should be <80% of max
3. **Query Rate** - Track for capacity planning
4. **Blocking Queries** - Should be minimal
5. **Memory Usage** - Monitor for memory leaks

## 🔍 Troubleshooting

### Common Issues

**1. Metrics Not Appearing in Prometheus**
```bash
# Check if exporter is running
curl http://localhost:8001/metrics

# Check Prometheus targets
curl http://localhost:9090/api/v1/targets

# Verify Prometheus configuration
grep -A5 postgresql /path/to/prometheus.yml
```

**2. Database Connection Errors**
```bash
# Test database connectivity
PGPASSWORD=dev_password psql -h localhost -p 5432 -U postgres -d dev_db -c "SELECT version();"

# Check environment variables
env | grep -E "(DB_|POSTGRESQL_)"

# Verify database is running
python scripts/monitoring/postgres_monitor.py
```

**3. Grafana Dashboard Issues**
```bash
# Check Grafana datasource
curl -u admin:admin http://localhost:3000/api/datasources

# Verify dashboard import
curl -u admin:admin http://localhost:3000/api/search?query=PostgreSQL

# Manual dashboard import
curl -X POST -u admin:admin \
  -H "Content-Type: application/json" \
  -d @k8s/data-agent/postgres-grafana-dashboard.json \
  http://localhost:3000/api/dashboards/db
```

### Debug Mode

Enable debug logging for troubleshooting:

```bash
# Run with debug logging
python scripts/monitoring/setup_postgres_monitoring.py --debug

# Check system resources
python scripts/monitoring/postgres_monitor.py --include-processes --format json
```

## 🔄 Integration with Existing Systems

### Data Agent Integration

The PostgreSQL monitoring complements the existing data agent monitoring:

- **Data Agent**: Monitors market data ingestion (port 8000)
- **PostgreSQL**: Monitors database performance (port 8001)
- **Prometheus**: Scrapes both systems (port 9090)
- **Grafana**: Visualizes all metrics (port 3000)

### Shared Infrastructure

Leverages existing ATS components:

- ✅ **Environment Configuration** - Uses existing `Environment` class
- ✅ **Database Connection** - Uses existing connection patterns
- ✅ **Prometheus Setup** - Extends existing Prometheus config
- ✅ **Grafana Infrastructure** - Adds to existing dashboards
- ✅ **Kubernetes Deployment** - Integrates with existing K8s setup

## 📚 API Reference

### PostgreSQL Exporter API

```python
# Main exporter class
class PostgreSQLPrometheusExporter:
    def __init__(self, env: Environment, metrics_prefix: str, port: int)
    def start_server(self)
    async def collect_database_metrics(self) -> PostgreSQLConnectionMetrics
    def update_prometheus_metrics(self, metrics: PostgreSQLConnectionMetrics)

# Setup function
def setup_postgresql_monitoring(
    env: Environment = None,
    port: int = 8001, 
    update_interval: int = 30,
    metrics_prefix: str = "postgresql"
) -> PostgreSQLMonitor
```

### Available Endpoints

```bash
# Metrics endpoint
GET http://localhost:8001/metrics

# Health check (if using health API)
GET http://localhost:8001/health
```

## 🎯 Next Steps

### Future Enhancements

1. **Custom Alerts** - Add PostgreSQL-specific alert rules
2. **Query Analytics** - Monitor slow query performance
3. **Replication Monitoring** - Add replication lag metrics
4. **Index Analysis** - Monitor index usage and efficiency
5. **Lock Analysis** - Detailed blocking and deadlock analysis
6. **Connection Pool Metrics** - Monitor connection pool efficiency

### Scaling Considerations

- **Multiple Databases** - Monitor multiple PostgreSQL instances
- **Cloud Integration** - Integrate with cloud database metrics
- **Cross-Environment** - Monitor dev/staging/prod environments
- **Historical Analysis** - Long-term performance trending

---

**Integration Status:** ✅ Ready for Production

This PostgreSQL monitoring integration provides comprehensive observability into your database performance while seamlessly working with the existing ATS monitoring infrastructure.