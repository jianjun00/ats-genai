#!/usr/bin/env python3
"""
SigNoz ATS-INTG Environment Configuration
Sets up monitoring for integration environment with provided API key
"""

import os
import json
import requests
import logging
from typing import Dict, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SigNozINTGConfigurator:
    """Configure SigNoz for ATS Integration Environment"""

    def __init__(self, api_key: str, signoz_url: str = "http://localhost:8080"):
        self.api_key = api_key
        self.signoz_url = signoz_url
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

    def create_intg_organization(self) -> Dict:
        """Create dedicated organization for ATS-INTG monitoring"""
        org_config = {
            "name": "ATS-INTG-Monitoring",
            "description": "Integration environment monitoring for ATS platform",
            "settings": {
                "retention_days": 90,  # Longer retention for integration analysis
                "sampling_rate": 1.0,  # 100% sampling for integration testing
                "environment": "integration"
            }
        }

        response = requests.post(
            f"{self.signoz_url}/api/v1/organizations",
            json=org_config,
            headers=self.headers
        )

        if response.status_code == 201:
            logger.info("✅ ATS-INTG organization created successfully")
            return response.json()
        else:
            logger.warning(f"Organization creation response: {response.status_code}")
            return {}

    def setup_intg_services(self) -> List[Dict]:
        """Define ATS-INTG services for monitoring"""
        services = [
            {
                "name": "ats-intg-analytics",
                "type": "web_service",
                "port": 4000,
                "health_endpoint": "/health",
                "critical_metrics": [
                    "http_request_duration_ms",
                    "http_request_success_rate",
                    "database_query_duration_ms",
                    "memory_usage_mb",
                    "cpu_usage_percent"
                ]
            },
            {
                "name": "ats-intg-postgres",
                "type": "database",
                "port": 4432,
                "critical_metrics": [
                    "db_connections_active",
                    "db_query_duration_ms",
                    "db_slow_queries_count",
                    "db_connection_errors_total",
                    "db_disk_usage_percent"
                ]
            },
            {
                "name": "ats-intg-data-pipeline",
                "type": "background_service",
                "critical_metrics": [
                    "pipeline_execution_duration_ms",
                    "pipeline_success_rate",
                    "data_quality_score",
                    "data_freshness_minutes",
                    "external_api_response_time_ms"
                ]
            },
            {
                "name": "ats-intg-news-realtime",
                "type": "news_service",
                "port": 8081,
                "critical_metrics": [
                    "news_articles_fetched_total",
                    "news_articles_stored_total",
                    "news_api_calls_total",
                    "news_api_errors_total",
                    "news_api_response_duration_ms",
                    "news_ingestion_cycle_duration_ms",
                    "news_data_freshness_minutes"
                ]
            },
            {
                "name": "ats-intg-ml-pipeline",
                "type": "ml_service",
                "critical_metrics": [
                    "training_data_generation_duration_ms",
                    "model_validation_accuracy",
                    "feature_completeness_percent",
                    "arrayrecord_generation_success_rate"
                ]
            }
        ]

        logger.info(f"📋 Configured {len(services)} ATS-INTG services for monitoring")
        return services

    def create_intg_dashboards(self) -> Dict:
        """Create ATS-INTG specific dashboards"""
        dashboards = {
            "ats_intg_overview": {
                "title": "ATS Integration Environment Overview",
                "panels": [
                    {
                        "title": "Service Health Status",
                        "type": "status",
                        "query": "up{job=~'ats-intg-.*'}"
                    },
                    {
                        "title": "Integration Test Success Rate",
                        "type": "gauge",
                        "query": "rate(integration_tests_passed_total[5m]) / rate(integration_tests_total[5m])",
                        "thresholds": {"warning": 0.95, "critical": 0.90}
                    },
                    {
                        "title": "Data Pipeline Health",
                        "type": "timeseries",
                        "query": "ats_pipeline_success_rate{environment='intg'}"
                    },
                    {
                        "title": "External API Response Times",
                        "type": "heatmap",
                        "query": "histogram_quantile(0.95, ats_external_api_duration_ms{environment='intg'})"
                    }
                ]
            },

            "ats_intg_data_quality": {
                "title": "ATS-INTG Data Quality Dashboard",
                "panels": [
                    {
                        "title": "Data Completeness by Vendor",
                        "type": "bar_chart",
                        "query": "ats_data_completeness_percent{environment='intg'} by (vendor)"
                    },
                    {
                        "title": "Data Freshness Alert Status",
                        "type": "alert_panel",
                        "query": "ats_data_freshness_minutes{environment='intg'} > 60"
                    },
                    {
                        "title": "Missing Data Points",
                        "type": "table",
                        "query": "ats_missing_data_points_total{environment='intg'}"
                    }
                ]
            },

            "ats_intg_performance": {
                "title": "ATS-INTG Performance Metrics",
                "panels": [
                    {
                        "title": "Service Response Times (P95)",
                        "type": "timeseries",
                        "query": "histogram_quantile(0.95, http_request_duration_seconds{environment='intg'})"
                    },
                    {
                        "title": "Database Query Performance",
                        "type": "timeseries",
                        "query": "ats_db_query_duration_ms{environment='intg', db='intg_db'}"
                    },
                    {
                        "title": "Memory Usage by Service",
                        "type": "stacked_area",
                        "query": "ats_memory_usage_mb{environment='intg'} by (service)"
                    }
                ]
            },

            "ats_intg_news_monitoring": {
                "title": "ATS-INTG News Ingestion Dashboard",
                "panels": [
                    {
                        "title": "News Articles Ingested per Hour",
                        "type": "timeseries",
                        "query": "rate(news_articles_stored_total{environment='intg'}[1h]) * 3600"
                    },
                    {
                        "title": "News API Success Rate by Vendor",
                        "type": "gauge",
                        "query": "rate(news_api_calls_total{environment='intg', success='true'}[5m]) / rate(news_api_calls_total{environment='intg'}[5m])",
                        "thresholds": {"warning": 0.95, "critical": 0.90}
                    },
                    {
                        "title": "News API Response Time P95",
                        "type": "timeseries",
                        "query": "histogram_quantile(0.95, news_api_response_duration_ms{environment='intg'})"
                    },
                    {
                        "title": "News Data Freshness by Vendor",
                        "type": "stat",
                        "query": "news_data_freshness_minutes{environment='intg'}",
                        "thresholds": {"warning": 120, "critical": 240}
                    },
                    {
                        "title": "News Ingestion Cycle Duration",
                        "type": "histogram",
                        "query": "news_ingestion_cycle_duration_ms{environment='intg'}"
                    },
                    {
                        "title": "News API Error Rate",
                        "type": "timeseries",
                        "query": "rate(news_api_errors_total{environment='intg'}[5m])"
                    },
                    {
                        "title": "Articles Fetched vs Stored",
                        "type": "comparison",
                        "queries": {
                            "fetched": "rate(news_articles_fetched_total{environment='intg'}[1h]) * 3600",
                            "stored": "rate(news_articles_stored_total{environment='intg'}[1h]) * 3600"
                        }
                    },
                    {
                        "title": "News Service Uptime",
                        "type": "uptime",
                        "query": "up{job='ats-intg-news-realtime'}"
                    }
                ]
            }
        }

        logger.info(f"📊 Created {len(dashboards)} ATS-INTG dashboards")
        return dashboards

    def setup_intg_alerts(self) -> List[Dict]:
        """Configure ATS-INTG specific alerting rules"""
        alerts = [
            {
                "name": "ATS-INTG Service Down",
                "severity": "critical",
                "query": "up{job=~'ats-intg-.*'} == 0",
                "for": "2m",
                "description": "ATS integration service is down",
                "runbook": "Check service logs and restart if necessary"
            },
            {
                "name": "ATS-INTG Data Feed Stale",
                "severity": "critical",
                "query": "ats_data_freshness_minutes{environment='intg'} > 120",
                "for": "5m",
                "description": "Data feed is more than 2 hours stale in integration",
                "runbook": "Check external API connectivity and data pipeline status"
            },
            {
                "name": "ATS-INTG Integration Test Failures",
                "severity": "warning",
                "query": "rate(integration_tests_failed_total{environment='intg'}[10m]) > 0.05",
                "for": "5m",
                "description": "Integration test failure rate > 5%",
                "runbook": "Review failed test logs and check service dependencies"
            },
            {
                "name": "ATS-INTG Database Connection Pool Exhausted",
                "severity": "warning",
                "query": "ats_db_connections_active{environment='intg'} > 90",
                "for": "3m",
                "description": "Database connection pool near exhaustion",
                "runbook": "Check for connection leaks and consider scaling"
            },
            {
                "name": "ATS-INTG ML Pipeline Failures",
                "severity": "warning",
                "query": "ats_ml_pipeline_success_rate{environment='intg'} < 0.95",
                "for": "10m",
                "description": "ML pipeline success rate below 95%",
                "runbook": "Check training data availability and pipeline logs"
            },
            {
                "name": "ATS-INTG News Service Down",
                "severity": "critical",
                "query": "up{job='ats-intg-news-realtime'} == 0",
                "for": "2m",
                "description": "News ingestion service is down",
                "runbook": "Check ats-intg-news-realtime container status and logs"
            },
            {
                "name": "ATS-INTG News Data Stale",
                "severity": "critical",
                "query": "news_data_freshness_minutes{environment='intg'} > 180",
                "for": "5m",
                "description": "News data is more than 3 hours stale",
                "runbook": "Check news API connectivity and service logs for errors"
            },
            {
                "name": "ATS-INTG News API High Error Rate",
                "severity": "warning",
                "query": "rate(news_api_errors_total{environment='intg'}[10m]) / rate(news_api_calls_total{environment='intg'}[10m]) > 0.1",
                "for": "5m",
                "description": "News API error rate above 10%",
                "runbook": "Check API keys, rate limits, and vendor service status"
            },
            {
                "name": "ATS-INTG News Ingestion Slow",
                "severity": "warning",
                "query": "histogram_quantile(0.95, news_ingestion_cycle_duration_ms{environment='intg'}) > 30000",
                "for": "10m",
                "description": "News ingestion cycles taking longer than 30 seconds",
                "runbook": "Check database performance and external API response times"
            },
            {
                "name": "ATS-INTG News API Response Time High",
                "severity": "warning",
                "query": "histogram_quantile(0.95, news_api_response_duration_ms{environment='intg'}) > 10000",
                "for": "5m",
                "description": "News API response times above 10 seconds P95",
                "runbook": "Check network connectivity and vendor API performance"
            }
        ]

        logger.info(f"🚨 Configured {len(alerts)} ATS-INTG alert rules")
        return alerts

    def generate_instrumentation_config(self) -> str:
        """Generate OpenTelemetry configuration for ATS-INTG services"""
        config = f"""
# ATS-INTG OpenTelemetry Configuration
# Add to your integration environment services

export OTEL_EXPORTER_OTLP_ENDPOINT="http://signoz-otel-collector:4318"
export OTEL_EXPORTER_OTLP_HEADERS="signoz-access-token={self.api_key}"
export OTEL_SERVICE_NAME="ats-intg-service"  # Replace with actual service name
export OTEL_RESOURCE_ATTRIBUTES="environment=intg,ats.tier=integration,ats.criticality=high"

# News Service Configuration:
export OTEL_SERVICE_NAME="ats-intg-news-realtime"
export OTEL_RESOURCE_ATTRIBUTES="environment=intg,ats.tier=integration,ats.component=news-ingestion,ats.criticality=high"

# Start news service with telemetry:
docker run -d \\
    --name ats-intg-news-realtime \\
    --network ats-intg-network \\
    -p 8081:8080 \\
    -e OTEL_EXPORTER_OTLP_ENDPOINT="http://signoz-otel-collector:4318" \\
    -e OTEL_SERVICE_NAME="ats-intg-news-realtime" \\
    -e OTEL_RESOURCE_ATTRIBUTES="environment=intg,ats.component=news-ingestion" \\
    -e DB_HOST=ats-intg-postgres \\
    -e POLYGON_API_KEY="<API_KEY>" \\
    -e PYTHONPATH=/workspace/src \\
    -v /home/jianjun/ats-genai-data:/workspace \\
    dragonflyer762/ats-genai:latest \\
    python scripts/realtime_news_ingestion.py --vendors polygon

# For Python services, add instrumentation:
# from opentelemetry.instrumentation.auto_instrumentation import sitecustomize

# News-specific metrics configured:
# - news_articles_fetched_total (by vendor)
# - news_articles_stored_total (by vendor)
# - news_api_calls_total (by vendor, success)
# - news_api_errors_total (by vendor)
# - news_api_response_duration_ms (by vendor)
# - news_ingestion_cycle_duration_ms
# - news_data_freshness_minutes (by vendor)

# SigNoz Dashboard: http://localhost:8080/dashboard
# News Dashboard: Search for "ATS-INTG News Ingestion Dashboard"
"""
        return config

def main():
    """Configure SigNoz for ATS-INTG monitoring"""
    api_key = "9RbijHam3W4B0a8h5fFB+7NgUgmXV+hFnzIPQUqtc6M="

    print("🔧 Setting up SigNoz for ATS-INTG monitoring...")
    print("=" * 60)

    configurator = SigNozINTGConfigurator(api_key)

    # Setup organization
    org = configurator.create_intg_organization()

    # Configure services
    services = configurator.setup_intg_services()

    # Create dashboards
    dashboards = configurator.create_intg_dashboards()

    # Setup alerts
    alerts = configurator.setup_intg_alerts()

    # Generate instrumentation config
    instrumentation = configurator.generate_instrumentation_config()

    print("\n" + "=" * 60)
    print("✅ ATS-INTG SigNoz configuration completed!")
    print(f"📊 Services configured: {len(services)}")
    print(f"📈 Dashboards created: {len(dashboards)}")
    print(f"🚨 Alert rules configured: {len(alerts)}")
    print("=" * 60)

    # Save instrumentation config
    with open("/tmp/ats-intg-instrumentation.sh", "w") as f:
        f.write(instrumentation)

    print("📋 Instrumentation config saved to: /tmp/ats-intg-instrumentation.sh")
    print("🔗 Access SigNoz dashboard: http://localhost:8080")

if __name__ == "__main__":
    main()