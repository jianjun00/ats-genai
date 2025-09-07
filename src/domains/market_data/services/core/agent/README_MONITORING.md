# Data Agent Monitoring and Operational Features

This document describes the monitoring and operational features implemented for the unified US stock price data agent.

## Overview

The data agent has been enhanced with comprehensive monitoring, alerting, and operational capabilities to ensure robust, reliable, and observable operation in production environments. These features include:

1. **Metrics Collection**: Comprehensive metrics tracking for data processing, source performance, and reconciliation
2. **Enhanced Alerting**: Multi-channel alerting with severity levels and configurable thresholds
3. **Dashboard Integration**: Prometheus metrics export for Grafana dashboards
4. **Resilience Patterns**: Circuit breaker and retry logic for robust data fetching
5. **Health API**: HTTP endpoints for health checks and metrics
6. **Configurable Logging**: Flexible logging configuration with multiple formats and destinations
7. **Graceful Shutdown**: Clean shutdown mechanism for all components

## Metrics Collection

The `DataAgentMetrics` class collects and tracks various metrics:

- **Core Metrics**: Processed data points, failure rates, processing times
- **Source Metrics**: Per-source success rates, latencies, and call counts
- **Reconciliation Metrics**: Conflict rates, data availability, source counts
- **Performance Metrics**: Throughput, points per second, uptime

Metrics are collected automatically during data agent operation and can be accessed via:
- Periodic logging (configurable interval)
- Prometheus metrics endpoint
- Health API metrics endpoint
- Direct access to the metrics object

## Enhanced Alerting

The alerting system supports multiple notification channels with different severity levels:

### Alert Handlers

- **LoggingAlertHandler**: Logs alerts to the standard logging system
- **SlackAlertHandler**: Sends alerts to Slack via webhook
- **EmailAlertHandler**: Sends email notifications
- **CompositeAlertHandler**: Aggregates multiple handlers

### Alert Severity Levels

- **INFO**: Informational alerts, no action required
- **WARNING**: Potential issues that may require attention
- **CRITICAL**: Serious issues requiring immediate attention

### Configurable Thresholds

Alerts are triggered based on configurable thresholds for:
- Failure rate
- Source success rate
- Processing time
- Throughput
- Reconciliation conflict rate

## Dashboard Integration (Prometheus)

The data agent exports metrics to Prometheus for visualization in Grafana dashboards:

### Prometheus Integration

- **PrometheusMetricsExporter**: Exports metrics to Prometheus
- **PrometheusMonitor**: Periodically updates Prometheus metrics
- **HTTP Server**: Exposes metrics on a configurable port (default: 8000)

### Available Metrics

All metrics collected by `DataAgentMetrics` are exported to Prometheus with appropriate types:
- Counters for cumulative metrics (processed, failed, etc.)
- Gauges for current values (failure rate, uptime, etc.)
- Histograms for distributions (processing time, batch size, etc.)

## Resilience Patterns

The data agent implements resilience patterns to handle temporary failures and prevent cascading failures:

### Circuit Breaker

- Prevents cascading failures when data sources are unavailable
- Three states: CLOSED (normal), OPEN (blocking requests), HALF-OPEN (testing recovery)
- Configurable failure threshold and recovery timeout
- Per-source circuit breakers

### Retry Logic

- Automatically retries failed operations with exponential backoff
- Configurable retry count, initial backoff, and maximum backoff
- Jitter to prevent thundering herd problem
- Exception filtering to only retry on specific exceptions

## Health API

The health API provides HTTP endpoints for monitoring the data agent's health and performance:

### Endpoints

- **/health**: Basic health check (200 OK if healthy, 503 if unhealthy)
- **/health/detailed**: Detailed health status including all health checks
- **/metrics**: Current metrics from the metrics collector

### Health Checks

- **Database Connection**: Checks database connectivity
- **Adapter Health**: Verifies adapter configuration
- **Metrics Health**: Ensures metrics collection is working

## Configurable Logging

The logging system is highly configurable to adapt to different environments:

### Configuration Options

- **Log Level**: DEBUG, INFO, WARNING, ERROR, CRITICAL
- **Log File**: File path for log output
- **JSON Format**: Structured logging for machine parsing
- **External Config**: Load configuration from JSON or YAML file

### Environment Variables

- **DATA_AGENT_LOG_LEVEL**: Override log level
- **DATA_AGENT_LOG_FILE**: Override log file path
- **DATA_AGENT_JSON_LOGS**: Enable JSON logging format

## Graceful Shutdown

The data agent implements graceful shutdown to ensure clean termination:

- Stops health API server
- Stops monitoring tasks
- Logs final metrics
- Handles SIGINT and SIGTERM signals

## Usage

### Enhanced Runner Script

The `run_enhanced_data_agent.py` script demonstrates all monitoring and operational features:

```bash
# Run with mock adapters and all monitoring features
python -m src.market_data.agent.run_enhanced_data_agent --mock --backfill --prometheus --health-api --circuit-breaker

# Run with real adapters (requires API keys)
python -m src.market_data.agent.run_enhanced_data_agent --backfill --frontfill

# Run in monitoring-only mode
python -m src.market_data.agent.run_enhanced_data_agent --prometheus --health-api

# Configure logging
python -m src.market_data.agent.run_enhanced_data_agent --log-level DEBUG --log-file data_agent.log --json-logs
```

### Environment Configuration

Set these environment variables to configure the data agent:

```bash
# API Keys
export TIINGO_API_KEY="your_tiingo_api_key"
export POLYGON_API_KEY="your_polygon_api_key"

# Database
export DATABASE_URL="postgresql://username:password@hostname:port/database"

# Monitoring
export ENABLE_PROMETHEUS="true"
export PROMETHEUS_PORT="8000"
export ENABLE_HEALTH_API="true"
export HEALTH_API_PORT="8081"

# Alerting
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
export ALERT_EMAIL_RECIPIENTS="alerts@example.com"
export ALERT_EMAIL_SENDER="data-agent@example.com"
export SMTP_SERVER="smtp.example.com"

# Logging
export DATA_AGENT_LOG_LEVEL="INFO"
export DATA_AGENT_LOG_FILE="data_agent.log"
export DATA_AGENT_JSON_LOGS="true"
```

## Integration with External Systems

### Prometheus/Grafana

1. Configure Prometheus to scrape metrics from the data agent's metrics endpoint
2. Create Grafana dashboards to visualize the metrics

Example Prometheus configuration:

```yaml
scrape_configs:
  - job_name: 'data-agent'
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:8000']
```

### Slack

1. Create a Slack app and webhook URL
2. Set the `SLACK_WEBHOOK_URL` environment variable

### Email

1. Configure email settings via environment variables
2. Ensure SMTP server is accessible

## Future Enhancements

Potential future improvements to the monitoring and operational features:

1. **Distributed Tracing**: Add OpenTelemetry integration for distributed tracing
2. **Adaptive Batch Sizing**: Dynamically adjust batch size based on performance metrics
3. **Rate Limiting**: Add rate limiting for external API calls
4. **Anomaly Detection**: Implement anomaly detection for metrics
5. **Auto-scaling**: Add support for auto-scaling based on metrics
