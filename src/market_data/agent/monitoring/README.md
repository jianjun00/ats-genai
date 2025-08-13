# Data Agent Monitoring Setup

This directory contains the configuration files needed to set up monitoring for the Data Agent using Prometheus and Grafana.

## Overview

The monitoring setup includes:

1. **Prometheus**: For collecting and storing metrics from the Data Agent
2. **Grafana**: For visualizing the metrics with pre-configured dashboards

## Directory Structure

```
monitoring/
├── docker-compose.yml         # Docker Compose configuration for Prometheus and Grafana
├── prometheus.yml            # Prometheus configuration
├── dashboards/
│   ├── data_agent_dashboard.json  # Pre-configured Grafana dashboard
│   └── provisioning/         # Grafana provisioning configurations
│       ├── dashboards.yml    # Dashboard provisioning config
│       └── datasources/
│           └── datasources.yml  # Datasource provisioning config
```

## Setup Instructions

### Prerequisites

- Docker and Docker Compose installed
- Data Agent running with Prometheus metrics enabled (`--prometheus` flag)

### Starting the Monitoring Stack

1. Navigate to the monitoring directory:

```bash
cd src/market_data/agent/monitoring
```

2. Start the Prometheus and Grafana containers:

```bash
docker-compose up -d
```

3. Access the Grafana dashboard:
   - Open a web browser and navigate to `http://localhost:3000`
   - Login with username `admin` and password `admin`
   - The Data Agent dashboard should be automatically loaded

### Configuring the Data Agent

To enable metrics collection in the Data Agent, run it with the following flags:

```bash
python src/market_data/agent/run_enhanced_data_agent.py --prometheus --prometheus-port 8000
```

This will start the Data Agent with Prometheus metrics enabled on port 8000, which is the port that Prometheus is configured to scrape.

## Available Metrics

The Data Agent dashboard includes the following metrics:

1. **Data Points Processed**: Total number of data points processed over time
2. **Average Data Point Processing Time**: Average time taken to process each data point
3. **Data Processing Success/Error Rate**: Ratio of successful to failed data point processing
4. **Reconciliations Performed**: Number of data reconciliations performed
5. **Circuit Breaker Open Count**: Number of times the circuit breaker has opened
6. **Retry Attempts**: Number of retry attempts made
7. **Adapter Calls by Source**: Breakdown of calls made to each data source adapter

## Customizing the Dashboard

You can customize the dashboard by:

1. Logging into Grafana
2. Navigating to the Data Agent dashboard
3. Clicking the gear icon in the top right corner
4. Making your changes and saving the dashboard

## Alerting

To set up alerts:

1. In Grafana, navigate to the panel you want to add an alert for
2. Click the panel title and select "Edit"
3. Go to the "Alert" tab
4. Configure your alert conditions and notification channels
5. Save the alert

## Troubleshooting

If metrics are not showing up in Grafana:

1. Check that the Data Agent is running with Prometheus metrics enabled
2. Verify that Prometheus can reach the Data Agent by checking the Prometheus targets page at `http://localhost:9090/targets`
3. Ensure that the Prometheus datasource is correctly configured in Grafana
4. Check the Prometheus logs for any scraping errors

## Additional Resources

- [Prometheus Documentation](https://prometheus.io/docs/introduction/overview/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Prometheus Client Library for Python](https://github.com/prometheus/client_python)
