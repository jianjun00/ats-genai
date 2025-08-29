# ATS Monitoring Setup Guide

## Overview

Complete Docker-based monitoring solution for ATS platform with Prometheus, Grafana, and AlertManager for data quality and system health monitoring.

## Architecture

```
┌─────────────────────────────────────────┐
│         Docker Monitoring Stack        │
├─────────────────────────────────────────┤
│ Grafana           :3001                 │
│ Prometheus        :9090                 │
│ AlertManager      :9093                 │
│ Data Quality Exp  :8080                 │
│ Node Exporter     :9100                 │
└─────────────────────────────────────────┘
                      │
            ┌─────────────────┐
            │ Slack Channels  │
            │ #ats-alerts     │
            │ #ats-critical   │
            │ #ats-data-quality│
            └─────────────────┘
```

## Components

### 1. Prometheus
- **Purpose**: Metrics collection and alerting
- **Port**: 9090
- **Config**: Custom rules for ATS data quality alerts
- **Retention**: 15 days

### 2. Grafana
- **Purpose**: Metrics visualization and dashboards
- **Port**: 3001
- **Credentials**: admin / ats-monitoring-password
- **Dashboards**: ATS Data Quality Dashboard pre-configured

### 3. AlertManager
- **Purpose**: Alert routing and Slack notifications
- **Port**: 9093
- **Channels**: Configurable Slack channels

### 4. Data Quality Exporter
- **Purpose**: Custom metrics exporter for ATS-specific data quality
- **Port**: 8080
- **Metrics**: Instrument counts, price counts, data freshness

### 5. Node Exporter
- **Purpose**: System metrics collection
- **Port**: 9100
- **Metrics**: CPU, memory, disk, network statistics

## Key Metrics

### Instrument Metrics
- `ats_instruments_count{vendor,environment}` - Count of instruments per vendor
- `ats_data_quality_score{vendor,metric,environment}` - Quality scores (0-1)

### Price Data Metrics  
- `ats_daily_prices_count{vendor,environment}` - Count of daily prices per vendor
- `ats_daily_prices_last_update{vendor,environment}` - Timestamp of last update

### System Health
- `ats_metrics_scrape_errors_total{error_type}` - Scrape error counts
- `ats_metrics_scrape_duration_seconds` - Scrape duration histogram

## Alerts Configured

### Data Quality Alerts
- **InstrumentCountLow**: Triggers when instrument count < 1000
- **DailyPricesStale**: Triggers when prices haven't updated in 24+ hours
- **InstrumentCountDrop**: Triggers on >5% drop in instrument count

### System Alerts
- **DatabaseConnectionDown**: Database connectivity issues
- **HighMemoryUsage**: Memory usage > 85%
- **HighDiskUsage**: Disk usage > 85%

## Deployment

### Quick Start
```bash
# Start monitoring stack
./scripts/start_monitoring.sh

# Stop monitoring stack
./scripts/stop_monitoring.sh
```

### Manual Deployment
```bash
# Start all monitoring services
docker-compose -f docker-compose.monitoring.yml up -d

# Check service status
docker-compose -f docker-compose.monitoring.yml ps

# View logs
docker-compose -f docker-compose.monitoring.yml logs -f

# Stop services
docker-compose -f docker-compose.monitoring.yml down

# Stop and remove volumes
docker-compose -f docker-compose.monitoring.yml down -v
```

## Access Information

### Monitoring Services
- **Grafana**: http://localhost:3001 (admin/ats-monitoring-password)
- **Prometheus**: http://localhost:9090
- **AlertManager**: http://localhost:9093
- **Data Quality Exporter**: http://localhost:8080/metrics
- **Node Exporter**: http://localhost:9100/metrics

### Service Health Checks
```bash
# Check all services are running
docker-compose -f docker-compose.monitoring.yml ps

# Test service endpoints
curl http://localhost:3001/api/health      # Grafana
curl http://localhost:9090/-/ready         # Prometheus  
curl http://localhost:9093/-/ready         # AlertManager
curl http://localhost:8080/metrics         # Data Quality Metrics
curl http://localhost:9100/metrics         # Node Exporter
```

## Slack Integration Setup

### 1. Create Slack App
1. Go to https://api.slack.com/apps
2. Create new app for your workspace
3. Add Incoming Webhooks
4. Create webhooks for channels:
   - `#ats-dev-alerts`
   - `#ats-intg-alerts` 
   - `#ats-data-quality`

### 2. Configure Webhook URLs
Update the webhook URLs in AlertManager configs:
```yaml
# monitoring/alertmanager/alertmanager.yml
global:
  slack_api_url: 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'
```

### 3. Test Alerts
```bash
# Trigger test alert via Prometheus
curl -X POST http://localhost:9090/api/v1/alerts
```

## Monitoring Dashboards

### ATS Data Quality Dashboard
- **Instrument Counts**: Real-time counts by vendor
- **Price Data Metrics**: Daily price counts and freshness
- **Quality Scores**: Completeness and freshness scores (0-1)
- **Trend Analysis**: 24-hour trending for key metrics

### Key Thresholds
- **Instruments**: Green >5k, Yellow >1k, Red <1k
- **Prices**: Green >50k, Yellow >10k, Red <10k
- **Quality Scores**: Green >90%, Yellow >70%, Red <70%
- **Data Freshness**: Green <24h, Yellow <48h, Red >48h

## Troubleshooting

### Common Issues

#### Data Quality Exporter Not Starting
```bash
# Check logs
docker-compose -f docker-compose.monitoring.yml logs ats-data-quality-exporter

# Common fixes:
# 1. Database connection issues - verify config
# 2. Missing Python dependencies - check image
# 3. Gin config file not found - verify path
```

#### Metrics Not Appearing
```bash
# Check Prometheus targets
curl http://localhost:9090/api/v1/targets

# Verify all services are running
docker-compose -f docker-compose.monitoring.yml ps

# Check service logs
docker-compose -f docker-compose.monitoring.yml logs prometheus
```

#### Grafana Dashboard Empty
1. Verify Prometheus data source connection
2. Check metric names match exporter output
3. Verify time range and environment labels
4. Test data source: http://prometheus:9090

#### Alerts Not Firing
```bash
# Check AlertManager status
curl http://localhost:9093/api/v1/status

# Check AlertManager logs
docker-compose -f docker-compose.monitoring.yml logs alertmanager

# Verify Slack webhook
curl -X POST -H 'Content-type: application/json' \
  --data '{"text":"Test message"}' \
  YOUR_SLACK_WEBHOOK_URL
```

### Verification Commands
```bash
# Check all services are running
docker-compose -f docker-compose.monitoring.yml ps

# Check service logs
docker-compose -f docker-compose.monitoring.yml logs

# Check metrics endpoint
curl http://localhost:8080/metrics | grep ats_

# Test Prometheus queries
curl "http://localhost:9090/api/v1/query?query=up"

# Check Grafana health
curl http://localhost:3001/api/health
```

## Maintenance

### Regular Tasks
1. **Monitor disk usage**: Prometheus data retention set to 15 days
2. **Update dashboards**: Import new panels as metrics evolve  
3. **Review alerts**: Adjust thresholds based on baseline metrics
4. **Backup configs**: Preserve custom Grafana dashboards

### Scaling
- Increase Prometheus retention for longer history
- Add more data quality exporters for additional metrics
- Create custom dashboards for specific use cases
- Scale services using Docker Compose scaling: `docker-compose -f docker-compose.monitoring.yml up -d --scale prometheus=2`

## Security Considerations
- Grafana admin passwords should be changed from defaults
- Slack webhook URLs should be stored as environment variables or Docker secrets
- Docker network isolation restricts access between containers
- Use Docker secrets for sensitive configuration data

---

For questions or issues, refer to the troubleshooting section or check component logs using `docker-compose logs`.