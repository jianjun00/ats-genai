# ATS Real-time Collection Monitoring System

Comprehensive monitoring and alerting infrastructure for the ATS real-time data collection system supporting AAPL and TSLA minute-bar data from Tiingo and Polygon vendors.

## 🎯 Overview

The monitoring system provides:

- **Real-time Data Monitoring**: Continuous monitoring of AAPL/TSLA data collection from Tiingo and Polygon
- **Multi-channel Alerting**: Slack, Discord, Email, PagerDuty, and Teams integration
- **Web Dashboard**: Live monitoring dashboard with interactive charts and real-time updates
- **Prometheus Integration**: Metrics export and Grafana dashboard automation
- **Data Quality Validation**: Cross-vendor consistency checks and OHLC validation
- **Performance Tracking**: Collection rate, latency, and system health monitoring
- **SLA Monitoring**: Availability, freshness, and quality SLA compliance tracking

## 🚀 Quick Start

### Start Complete Monitoring System

```bash
# Start all components (recommended)
python3 scripts/start_realtime_monitoring.py

# Start with custom configuration
python3 scripts/start_realtime_monitoring.py --config monitoring_config.json

# Start specific components only
python3 scripts/start_realtime_monitoring.py --components monitor,dashboard

# Test environment validation
python3 scripts/start_realtime_monitoring.py --test
```

### Access Points

After startup, the system provides these endpoints:

- **Dashboard**: http://localhost:8090
- **Health Check**: http://localhost:8090/health  
- **Prometheus Metrics**: http://localhost:8091/metrics
- **WebSocket Updates**: ws://localhost:8090/ws
- **Alerting Rules**: http://localhost:8091/config/rules
- **Grafana Config**: http://localhost:8091/config/grafana

## 🏗️ Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                 ATS Real-time Monitoring System                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐  ┌──────────────────┐  ┌────────────────┐  │
│  │ Data Collection │  │   Monitoring     │  │    Alerting    │  │
│  │    Monitor      │◄─┤     Engine       │─►│   Channels     │  │
│  │                 │  │                  │  │                │  │
│  │ • Freshness     │  │ • Quality Checks │  │ • Slack        │  │
│  │ • Quality       │  │ • Cross-vendor   │  │ • Discord      │  │
│  │ • Consistency   │  │ • Performance    │  │ • Email        │  │
│  └─────────────────┘  └──────────────────┘  │ • PagerDuty    │  │
│                                             │ • Teams        │  │
│  ┌─────────────────┐  ┌──────────────────┐  └────────────────┘  │
│  │   Web Dashboard │  │   Prometheus     │                      │
│  │                 │  │  Integration     │                      │
│  │ • Live Charts   │  │                  │                      │
│  │ • WebSocket     │  │ • Metrics Export │                      │
│  │ • Health Status │  │ • Grafana Config │                      │
│  │ • Alert History │  │ • Alert Rules    │                      │
│  └─────────────────┘  └──────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
PostgreSQL DB ──► Monitoring Engine ──► Alert Evaluation ──► Multi-channel Alerts
     │                     │                     │
     │                     │                     ▼
     │                     ▼              ┌─────────────┐
     │              ┌─────────────┐       │   Slack     │
     │              │ Prometheus  │       │   Discord   │
     │              │  Metrics    │       │   Email     │
     │              └─────────────┘       │  PagerDuty  │
     │                     │              │   Teams     │
     │                     ▼              └─────────────┘
     ▼              ┌─────────────┐
┌─────────────┐     │   Grafana   │
│   Web       │     │ Dashboards  │
│ Dashboard   │     └─────────────┘
└─────────────┘
```

## 📊 Monitoring Capabilities

### Data Quality Monitoring

- **Freshness Tracking**: Alerts when data is > 5 minutes old
- **Quality Score Monitoring**: Tracks vendor-provided quality scores (threshold: 70%)
- **OHLC Validation**: Ensures High ≥ Low, High ≥ Open/Close relationships
- **Cross-vendor Consistency**: Alerts when vendor prices diverge > 5%
- **Volume Validation**: Detects volume anomalies and spikes

### Performance Monitoring

- **Collection Rate**: Records collected per minute/hour tracking
- **Processing Latency**: Time from market data to database storage
- **Database Health**: Connection pool monitoring and query performance
- **System Availability**: Overall system uptime and reliability tracking
- **SLA Compliance**: Automated SLA monitoring with configurable thresholds

### Alert Categories

| Category | Description | Default Threshold |
|----------|-------------|-------------------|
| `data_freshness` | Data staleness detection | 5 minutes |
| `data_quality` | Quality score degradation | < 70% |
| `data_integrity` | OHLC relationship violations | 0 violations |
| `vendor_consistency` | Price divergence between vendors | > 5% |
| `price_movement` | Significant price changes | > 10% |
| `system_availability` | System downtime/failures | < 80% |

## 🔧 Configuration

### Environment Variables

```bash
# Alert Channels
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
ALERT_EMAIL_RECIPIENTS=admin@example.com,team@example.com

# Email Configuration (if using email alerts)
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your-email@gmail.com
SMTP_PASSWORD=your-app-password
SMTP_USE_TLS=true

# PagerDuty Integration
PAGERDUTY_INTEGRATION_KEY=your-integration-key

# Microsoft Teams
TEAMS_WEBHOOK_URL=https://outlook.office.com/webhook/...
```

### Configuration File Example

```json
{
  "components": {
    "monitor": {
      "enabled": true,
      "interval_seconds": 60,
      "database": {
        "host": "ats-intg-postgres",
        "port": 5432,
        "user": "postgres",
        "password": "intg_password",
        "database": "intg_db"
      }
    },
    "alerting": {
      "enabled": true,
      "test_on_startup": true,
      "channels": {
        "slack": {
          "enabled": true,
          "webhook_url": "${SLACK_WEBHOOK_URL}",
          "min_level": "warning"
        },
        "email": {
          "enabled": true,
          "recipients": ["admin@example.com"],
          "min_level": "critical"
        }
      }
    },
    "dashboard": {
      "enabled": true,
      "host": "0.0.0.0", 
      "port": 8090,
      "update_interval_seconds": 30
    },
    "prometheus": {
      "enabled": true,
      "port": 8091,
      "existing_prometheus_url": "http://localhost:8080"
    }
  }
}
```

## 📈 Prometheus Metrics

### Core Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `ats_realtime_data_freshness_seconds` | Gauge | Seconds since last data update |
| `ats_realtime_quality_score` | Gauge | Data quality score (0-1) |
| `ats_realtime_records_per_hour` | Gauge | Records collected in last hour |
| `ats_realtime_price_divergence_pct` | Gauge | Price difference between vendors (%) |
| `ats_realtime_consistency_score` | Gauge | Cross-vendor consistency (0-1) |
| `ats_realtime_system_availability` | Gauge | System availability (0-1) |
| `ats_realtime_processing_latency_seconds` | Gauge | Processing latency |
| `ats_realtime_price_volatility` | Gauge | Price volatility measure |
| `ats_realtime_sla_compliance` | Gauge | SLA compliance by metric |
| `ats_realtime_active_alerts` | Gauge | Active alerts by category |

### Example Queries

```promql
# Data freshness by vendor and symbol
ats_realtime_data_freshness_seconds{vendor="tiingo",symbol="AAPL"}

# Average quality score across all vendors
avg(ats_realtime_quality_score)

# Collection rate trend
rate(ats_realtime_records_per_hour[5m])

# Price divergence alerts
ats_realtime_price_divergence_pct > 5

# System availability over time
avg_over_time(ats_realtime_system_availability[1h])
```

## 🚨 Alerting Rules

The system includes pre-configured Prometheus alerting rules:

### Critical Alerts

- **ATSRealtimeSystemDown**: System availability < 80%
- **ATSRealtimeDataStale**: Data > 5 minutes old
- **ATSRealtimeQualityLow**: Quality score < 70%

### Warning Alerts

- **ATSRealtimePriceDivergence**: Vendor price difference > 5%
- **ATSRealtimeSLAViolation**: SLA compliance < 95%

### Info Alerts

- **ATSRealtimeHighVolatility**: Unusual price volatility detected

## 📱 Web Dashboard

The monitoring dashboard provides:

### Live Monitoring

- **System Status**: Overall health and availability
- **Data Freshness**: Real-time staleness tracking
- **Quality Metrics**: Vendor quality score trends
- **Collection Rates**: Records per minute charts
- **Price Updates**: Latest prices with timestamps

### Interactive Features

- **Real-time Updates**: WebSocket-based live data updates
- **Historical Charts**: Plotly.js interactive visualizations  
- **Alert Management**: View, filter, and acknowledge alerts
- **Health Checks**: Component status and connectivity
- **Configuration**: Runtime configuration management

### Mobile Support

The dashboard is fully responsive and optimized for mobile access.

## 🔌 API Endpoints

### Dashboard API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/status` | GET | System status overview |
| `/api/metrics/current` | GET | Current metrics snapshot |
| `/api/metrics/history` | GET | Historical metrics (query params: hours, limit) |
| `/api/alerts/current` | GET | Active alerts |
| `/api/alerts/history` | GET | Alert history (query params: hours, level, category) |
| `/api/alerts/acknowledge` | POST | Acknowledge alerts |
| `/api/alerts/test` | POST | Test alert channels |

### Prometheus API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/metrics` | GET | Prometheus metrics export |
| `/health` | GET | Health check |
| `/config/rules` | GET | Prometheus alerting rules (YAML) |
| `/config/grafana` | GET | Grafana dashboard JSON |

## 🔄 Integration Guide

### Grafana Integration

1. **Add Prometheus Data Source**:
   ```
   URL: http://localhost:8091
   Scrape interval: 30s
   ```

2. **Import Dashboard**:
   ```bash
   curl http://localhost:8091/config/grafana > ats-realtime-dashboard.json
   # Import in Grafana UI
   ```

### Prometheus Integration

1. **Add Scrape Target** to `prometheus.yml`:
   ```yaml
   scrape_configs:
     - job_name: 'ats-realtime'
       static_configs:
         - targets: ['localhost:8091']
       scrape_interval: 30s
   ```

2. **Add Alerting Rules**:
   ```bash
   curl http://localhost:8091/config/rules > ats-realtime-rules.yml
   # Add to Prometheus rules directory
   ```

### Slack Integration

1. **Create Slack App**: https://api.slack.com/apps
2. **Add Incoming Webhooks**: Copy webhook URL
3. **Set Environment Variable**: `SLACK_WEBHOOK_URL=your_webhook_url`

### Discord Integration

1. **Create Webhook**: Server Settings → Integrations → Webhooks
2. **Set Environment Variable**: `DISCORD_WEBHOOK_URL=your_webhook_url`

## 🧪 Testing

### Unit Tests

```bash
# Test monitoring components
PYTHONPATH=src python3 -m pytest tests/market_data/realtime/monitoring/ -v

# Test alert channels
PYTHONPATH=src python3 -c "
from market_data.realtime.monitoring.alert_channels import AlertChannelManager
import asyncio
asyncio.run(AlertChannelManager().test_channels())
"
```

### Integration Tests

```bash
# Test complete system
python3 scripts/start_realtime_monitoring.py --test

# Test specific components
python3 scripts/start_realtime_monitoring.py --components monitor --test
```

### Load Testing

```bash
# Stress test monitoring system
PYTHONPATH=src python3 -c "
from market_data.realtime.monitoring.realtime_collection_monitor import RealtimeCollectionMonitor
import asyncio
monitor = RealtimeCollectionMonitor(monitoring_interval=5)  # 5-second intervals
asyncio.run(monitor.start_monitoring())
"
```

## 🛠️ Troubleshooting

### Common Issues

#### Database Connection Errors
```bash
# Check database connectivity
PGPASSWORD=intg_password psql -h ats-intg-postgres -U postgres -d intg_db -c "SELECT version()"

# Verify required tables exist
PGPASSWORD=intg_password psql -h ats-intg-postgres -U postgres -d intg_db -c "
SELECT table_name FROM information_schema.tables 
WHERE table_name LIKE '%one_minute_live%'"
```

#### Port Conflicts
```bash
# Check port usage
netstat -tlnp | grep -E ':8090|:8091'

# Use alternative ports
python3 scripts/start_realtime_monitoring.py --config custom_config.json
```

#### Alert Channel Issues
```bash
# Test alert channels
PYTHONPATH=src python3 -c "
from market_data.realtime.monitoring.alert_channels import AlertChannelManager
import asyncio
asyncio.run(AlertChannelManager().test_channels(['slack']))
"
```

#### WebSocket Connection Issues
```javascript
// Browser console test
const ws = new WebSocket('ws://localhost:8090/ws');
ws.onopen = () => console.log('Connected');
ws.onmessage = (e) => console.log('Data:', JSON.parse(e.data));
```

### Log Analysis

```bash
# Monitor system logs
tail -f /var/log/ats-monitoring.log

# Check specific component logs
python3 scripts/start_realtime_monitoring.py --log-level DEBUG

# Filter alert logs
grep "ALERT" /var/log/ats-monitoring.log | tail -20
```

### Performance Debugging

```bash
# Monitor resource usage
docker stats ats-intg-postgres ats-intg-analytics

# Database query performance
PGPASSWORD=intg_password psql -h ats-intg-postgres -U postgres -d intg_db -c "
SELECT query, mean_time, calls 
FROM pg_stat_statements 
ORDER BY mean_time DESC LIMIT 10;"
```

## 📋 Maintenance

### Daily Operations

1. **Check Dashboard**: Verify system health at http://localhost:8090
2. **Review Alerts**: Check for any overnight alerts or degradations
3. **Data Quality**: Ensure quality scores remain > 80%
4. **Performance**: Monitor collection rates and latency

### Weekly Tasks

1. **Alert History Review**: Analyze alert patterns and trends
2. **SLA Reporting**: Generate availability and performance reports
3. **Configuration Updates**: Update thresholds based on observed patterns
4. **System Updates**: Apply security patches and dependency updates

### Monthly Maintenance

1. **Performance Analysis**: Comprehensive performance review
2. **Capacity Planning**: Assess resource usage trends
3. **Alert Tuning**: Adjust thresholds to reduce noise
4. **Documentation Updates**: Update procedures and configurations

## 🤝 Contributing

1. **Add New Metrics**: Extend `RealtimeCollectionMonitor.generate_prometheus_metrics()`
2. **Add Alert Channels**: Implement new channel in `AlertChannelManager`
3. **Dashboard Features**: Add panels to `MonitoringDashboard`
4. **Testing**: Add tests for new functionality

## 📄 License

Part of the ATS (Algorithmic Trading System) platform. See main project license.

---

**🎯 The ATS Real-time Collection Monitoring System provides comprehensive observability for critical market data infrastructure with enterprise-grade alerting and dashboard capabilities.**