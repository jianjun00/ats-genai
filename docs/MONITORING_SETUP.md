# ATS Monitoring Setup Guide

## Overview

Complete monitoring solution for ATS platform with Prometheus, Grafana, and AlertManager across both `ats-dev` and `ats-intg` environments.

## Architecture

```
┌─────────────────────┐    ┌─────────────────────┐
│     ats-dev         │    │     ats-intg        │
├─────────────────────┤    ├─────────────────────┤
│ Prometheus :30090   │    │ Prometheus :30091   │
│ Grafana    :30300   │    │ Grafana    :30301   │
│ AlertMgr   :30093   │    │ AlertMgr   :30094   │
│ DataExp    :8080    │    │ DataExp    :8080    │
└─────────────────────┘    └─────────────────────┘
           │                          │
           └──────────┬─────────────────┘
                      │
            ┌─────────────────┐
            │ Slack Channels  │
            │ #ats-dev-alerts │
            │ #ats-intg-alerts│
            │ #ats-data-quality│
            └─────────────────┘
```

## Components

### 1. Prometheus
- **Purpose**: Metrics collection and alerting
- **Port**: 30090 (dev), 30091 (intg)  
- **Config**: Custom rules for ATS data quality alerts
- **Retention**: 15 days

### 2. Grafana
- **Purpose**: Metrics visualization and dashboards
- **Port**: 30300 (dev), 30301 (intg)
- **Credentials**: admin / ats-dev-password, admin / ats-intg-password
- **Dashboards**: ATS Data Quality Dashboard pre-configured

### 3. AlertManager
- **Purpose**: Alert routing and Slack notifications
- **Port**: 30093 (dev), 30094 (intg)
- **Channels**: Environment-specific Slack channels

### 4. Data Quality Exporter
- **Purpose**: Custom metrics exporter for ATS-specific data quality
- **Port**: 8080 (internal)
- **Metrics**: Instrument counts, price counts, data freshness

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
# Deploy to both environments
./scripts/deploy_monitoring.sh both

# Deploy to specific environment
./scripts/deploy_monitoring.sh dev
./scripts/deploy_monitoring.sh intg
```

### Manual Deployment
```bash
# Create namespaces
kubectl apply -f k8s/monitoring/namespaces.yaml

# Deploy Prometheus stack
kubectl apply -f k8s/monitoring/prometheus-config.yaml
kubectl apply -f k8s/monitoring/prometheus-deployment.yaml

# Deploy Grafana
kubectl apply -f k8s/monitoring/grafana-config.yaml
kubectl apply -f k8s/monitoring/grafana-dashboards.yaml
kubectl apply -f k8s/monitoring/grafana-deployment.yaml

# Deploy AlertManager
kubectl apply -f k8s/monitoring/alertmanager-config.yaml
kubectl apply -f k8s/monitoring/alertmanager-deployment.yaml

# Deploy Data Quality Exporter
kubectl apply -f k8s/monitoring/data-quality-exporter.yaml
```

## Access Information

### Development Environment (ats-dev)
- **Grafana**: http://NODE_IP:30300 (admin/ats-dev-password)
- **Prometheus**: http://NODE_IP:30090
- **AlertManager**: http://NODE_IP:30093

### Integration Environment (ats-intg)  
- **Grafana**: http://NODE_IP:30301 (admin/ats-intg-password)
- **Prometheus**: http://NODE_IP:30091  
- **AlertManager**: http://NODE_IP:30094

### Data Quality Metrics
```bash
# Access via port-forward
kubectl port-forward -n ats-dev service/ats-data-quality-exporter 8080:8080
curl http://localhost:8080/metrics
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
# k8s/monitoring/alertmanager-config.yaml
global:
  slack_api_url: 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'
```

### 3. Test Alerts
```bash
# Trigger test alert via Prometheus
curl -X POST http://NODE_IP:30090/api/v1/alerts
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
kubectl logs -n ats-dev deployment/ats-data-quality-exporter

# Common fixes:
# 1. Database connection issues - verify config
# 2. Missing Python dependencies - check image
# 3. Gin config file not found - verify path
```

#### Metrics Not Appearing
```bash
# Check Prometheus targets
curl http://NODE_IP:30090/api/v1/targets

# Verify service discovery
kubectl get services -n ats-dev
kubectl get endpoints -n ats-dev
```

#### Grafana Dashboard Empty
1. Verify Prometheus data source connection
2. Check metric names match exporter output
3. Verify time range and environment labels

#### Alerts Not Firing
```bash
# Check AlertManager status
curl http://NODE_IP:30093/api/v1/status

# Verify Slack webhook
curl -X POST -H 'Content-type: application/json' \
  --data '{"text":"Test message"}' \
  YOUR_SLACK_WEBHOOK_URL
```

### Verification Commands
```bash
# Check all pods are running
kubectl get pods -n ats-dev
kubectl get pods -n ats-intg

# Check services and endpoints
kubectl get svc,endpoints -n ats-dev

# Check metrics endpoint
kubectl port-forward -n ats-dev svc/ats-data-quality-exporter 8080:8080
curl http://localhost:8080/metrics | grep ats_

# Test Prometheus queries
curl "http://NODE_IP:30090/api/v1/query?query=up"
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
- Implement cross-environment comparison views

## Security Considerations
- Grafana admin passwords should be changed from defaults
- Slack webhook URLs should be stored as Kubernetes secrets
- Network policies can restrict access between namespaces
- RBAC policies should limit monitoring service permissions

---

For questions or issues, refer to the troubleshooting section or check component logs using `kubectl logs`.