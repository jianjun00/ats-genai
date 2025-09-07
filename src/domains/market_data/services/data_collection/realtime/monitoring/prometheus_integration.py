#!/usr/bin/env python3
"""
Prometheus Integration for Real-time Collection Monitoring

Enhanced Prometheus metrics integration that extends the existing ATS Prometheus
infrastructure with real-time collection-specific metrics and alerting rules.

Features:
- Real-time collection metrics export
- Grafana dashboard automation
- Prometheus alerting rules
- Integration with existing ATS Prometheus setup
- Custom metric collectors
- Alert manager integration

Usage:
    from src.market_data.realtime.monitoring.prometheus_integration import RealtimePrometheusIntegration

    integration = RealtimePrometheusIntegration()
    await integration.start_metrics_server(port=8091)
"""

import asyncio
import asyncpg
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import aiohttp
from aiohttp import web
import yaml

# Add src to path
sys.path.insert(0, '/workspace/src')

from .realtime_collection_monitor import RealtimeCollectionMonitor

logger = logging.getLogger(__name__)


class RealtimePrometheusIntegration:
    """Prometheus integration for real-time collection monitoring."""

    def __init__(self,
                 monitor_interval: int = 60,
                 metrics_port: int = 8091,
                 existing_prometheus_url: Optional[str] = None):
        """Initialize Prometheus integration."""

        self.monitor_interval = monitor_interval
        self.metrics_port = metrics_port
        self.existing_prometheus_url = existing_prometheus_url or "http://localhost:8080"

        # Components
        self.monitor = RealtimeCollectionMonitor(monitoring_interval=monitor_interval)

        # Metrics cache
        self.metrics_cache = {}
        self.last_metrics_update = datetime.now()

        # HTTP server for metrics endpoint
        self.app = None
        self.server = None

        logger.info("🔗 Realtime Prometheus Integration initialized")

    async def initialize(self):
        """Initialize the Prometheus integration."""

        try:
            await self.monitor.initialize()

            # Setup HTTP server for metrics
            self.app = web.Application()
            self.app.router.add_get('/metrics', self.metrics_endpoint)
            self.app.router.add_get('/health', self.health_endpoint)
            self.app.router.add_get('/config/rules', self.alerting_rules_endpoint)
            self.app.router.add_get('/config/grafana', self.grafana_dashboard_endpoint)

            logger.info("✅ Prometheus integration initialized")

        except Exception as e:
            logger.error(f"❌ Failed to initialize Prometheus integration: {e}")
            raise

    async def collect_realtime_metrics(self) -> Dict[str, Any]:
        """Collect all real-time metrics for Prometheus export."""

        try:
            # Collect monitoring data
            freshness_metrics = await self.monitor.collect_data_freshness_metrics()
            quality_metrics = await self.monitor.collect_quality_metrics()
            consistency_metrics = await self.monitor.collect_cross_vendor_consistency_metrics()

            # Generate Prometheus metrics
            prometheus_text = await self.monitor.generate_prometheus_metrics(
                freshness_metrics, quality_metrics, consistency_metrics
            )

            # Add additional custom metrics
            additional_metrics = await self._generate_additional_metrics(
                freshness_metrics, quality_metrics, consistency_metrics
            )

            # Combine metrics
            combined_metrics = prometheus_text + "\n" + additional_metrics

            # Update cache
            self.metrics_cache = {
                'metrics_text': combined_metrics,
                'timestamp': datetime.now(),
                'freshness_count': len(freshness_metrics),
                'quality_count': len(quality_metrics),
                'consistency_count': len(consistency_metrics)
            }

            self.last_metrics_update = datetime.now()

            return self.metrics_cache

        except Exception as e:
            logger.error(f"❌ Error collecting realtime metrics: {e}")
            return {
                'metrics_text': f"# Error collecting metrics: {str(e)}\n",
                'timestamp': datetime.now(),
                'error': str(e)
            }

    async def _generate_additional_metrics(self,
                                         freshness_metrics: List[Any],
                                         quality_metrics: List[Any],
                                         consistency_metrics: List[Any]) -> str:
        """Generate additional Prometheus metrics not covered by the base monitor."""

        timestamp = int(datetime.now().timestamp())
        metrics_lines = []

        # System availability metrics
        metrics_lines.extend([
            "# HELP ats_realtime_system_availability System availability (0-1)",
            "# TYPE ats_realtime_system_availability gauge",
            "",
            "# HELP ats_realtime_collection_errors_total Total collection errors",
            "# TYPE ats_realtime_collection_errors_total counter",
            "",
            "# HELP ats_realtime_database_connections Active database connections",
            "# TYPE ats_realtime_database_connections gauge",
            "",
            "# HELP ats_realtime_processing_latency_seconds Processing latency in seconds",
            "# TYPE ats_realtime_processing_latency_seconds gauge",
            ""
        ])

        # System availability (based on recent successful collections)
        availability_score = await self._calculate_system_availability()
        metrics_lines.append(f'ats_realtime_system_availability {availability_score:.3f} {timestamp}')

        # Collection errors (from recent monitoring cycles)
        error_count = len([a for a in self.monitor.alerts if a.level.value in ['critical', 'fatal']])
        metrics_lines.append(f'ats_realtime_collection_errors_total {error_count} {timestamp}')

        # Database connections (if available)
        db_connections = await self._get_database_connection_count()
        metrics_lines.append(f'ats_realtime_database_connections {db_connections} {timestamp}')

        # Processing latency metrics
        for vendor in ['tiingo', 'polygon']:
            for symbol in ['AAPL', 'TSLA']:
                latency = await self._get_processing_latency(vendor, symbol)
                if latency is not None:
                    metrics_lines.append(f'ats_realtime_processing_latency_seconds{{vendor="{vendor}",symbol="{symbol}"}} {latency:.3f} {timestamp}')

        # Price volatility metrics
        metrics_lines.extend([
            "",
            "# HELP ats_realtime_price_volatility Price volatility over last hour",
            "# TYPE ats_realtime_price_volatility gauge",
            ""
        ])

        volatility_metrics = await self._calculate_price_volatility()
        for vendor, symbol, volatility in volatility_metrics:
            metrics_lines.append(f'ats_realtime_price_volatility{{vendor="{vendor}",symbol="{symbol}"}} {volatility:.6f} {timestamp}')

        # SLA compliance metrics
        metrics_lines.extend([
            "",
            "# HELP ats_realtime_sla_compliance SLA compliance percentage (0-1)",
            "# TYPE ats_realtime_sla_compliance gauge",
            ""
        ])

        sla_compliance = await self._calculate_sla_compliance()
        for metric_type, compliance in sla_compliance.items():
            metrics_lines.append(f'ats_realtime_sla_compliance{{metric="{metric_type}"}} {compliance:.3f} {timestamp}')

        return '\n'.join(metrics_lines) + '\n'

    async def _calculate_system_availability(self) -> float:
        """Calculate overall system availability score."""

        try:
            # Look at last hour of data
            cutoff_time = datetime.now() - timedelta(hours=1)

            recent_alerts = [
                a for a in self.monitor.alerts
                if a.timestamp >= cutoff_time and a.level.value in ['critical', 'fatal']
            ]

            # Base availability on absence of critical alerts
            if len(recent_alerts) == 0:
                return 1.0
            elif len(recent_alerts) <= 2:
                return 0.9
            elif len(recent_alerts) <= 5:
                return 0.8
            else:
                return 0.7

        except Exception:
            return 0.5  # Default partial availability

    async def _get_database_connection_count(self) -> int:
        """Get current database connection count."""

        try:
            if self.monitor.db_pool:
                return self.monitor.db_pool.get_size()
        except Exception:
            pass
        return 0

    async def _get_processing_latency(self, vendor: str, symbol: str) -> Optional[float]:
        """Get processing latency for a specific vendor/symbol."""

        try:
            async with self.monitor.db_pool.acquire() as conn:
                # Get timestamp of most recent record
                table_name = f"intg_one_minute_live_{vendor}"

                latest_timestamp = await conn.fetchval(f"""
                    SELECT timestamp FROM {table_name}
                    WHERE symbol = $1
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, symbol)

                if latest_timestamp:
                    # Calculate how long ago this was (processing latency)
                    latency = (datetime.now() - latest_timestamp).total_seconds()
                    return max(0, latency)

        except Exception as e:
            logger.debug(f"❌ Error getting processing latency for {vendor} {symbol}: {e}")

        return None

    async def _calculate_price_volatility(self) -> List[Tuple[str, str, float]]:
        """Calculate price volatility metrics."""

        volatility_metrics = []

        try:
            async with self.monitor.db_pool.acquire() as conn:
                for vendor in ['tiingo', 'polygon']:
                    for symbol in ['AAPL', 'TSLA']:
                        table_name = f"intg_one_minute_live_{vendor}"

                        # Get last hour of prices
                        hour_ago = datetime.now() - timedelta(hours=1)

                        prices = await conn.fetch(f"""
                            SELECT close_price FROM {table_name}
                            WHERE symbol = $1 AND timestamp >= $2
                            ORDER BY timestamp ASC
                        """, symbol, hour_ago)

                        if len(prices) >= 2:
                            price_values = [float(p['close_price']) for p in prices]

                            # Calculate volatility as standard deviation of returns
                            returns = []
                            for i in range(1, len(price_values)):
                                if price_values[i-1] > 0:
                                    return_pct = (price_values[i] - price_values[i-1]) / price_values[i-1]
                                    returns.append(return_pct)

                            if len(returns) >= 2:
                                import statistics
                                volatility = statistics.stdev(returns) if len(returns) > 1 else 0.0
                                volatility_metrics.append((vendor, symbol, volatility))

        except Exception as e:
            logger.debug(f"❌ Error calculating price volatility: {e}")

        return volatility_metrics

    async def _calculate_sla_compliance(self) -> Dict[str, float]:
        """Calculate SLA compliance metrics."""

        compliance = {}

        try:
            # Data freshness SLA: 95% of data should be < 2 minutes old
            freshness_violations = 0
            total_freshness_checks = 0

            for metric_data in self.monitor.metrics_history[-12:]:  # Last 12 cycles (~ last hour)
                freshness_metrics = metric_data.get('freshness_metrics', [])
                for fm in freshness_metrics:
                    total_freshness_checks += 1
                    if fm.get('seconds_since_last_update', 0) > 120:  # 2 minutes
                        freshness_violations += 1

            if total_freshness_checks > 0:
                compliance['data_freshness'] = 1.0 - (freshness_violations / total_freshness_checks)
            else:
                compliance['data_freshness'] = 1.0

            # Quality SLA: 90% of data should have quality score > 0.8
            quality_violations = 0
            total_quality_checks = 0

            for metric_data in self.monitor.metrics_history[-12:]:
                quality_metrics = metric_data.get('quality_metrics', [])
                for qm in quality_metrics:
                    total_quality_checks += 1
                    if qm.get('quality_score', 1.0) < 0.8:
                        quality_violations += 1

            if total_quality_checks > 0:
                compliance['data_quality'] = 1.0 - (quality_violations / total_quality_checks)
            else:
                compliance['data_quality'] = 1.0

            # Availability SLA: 99.5% uptime
            uptime_hours = min(24, len(self.monitor.metrics_history) * (self.monitor_interval / 3600))
            downtime_events = len([
                a for a in self.monitor.alerts[-100:]
                if a.category == 'data_freshness' and a.level.value in ['critical', 'fatal']
            ])

            # Estimate downtime (assume each critical alert = 5 minutes downtime)
            estimated_downtime_hours = (downtime_events * 5) / 60
            availability = max(0, 1.0 - (estimated_downtime_hours / max(1, uptime_hours)))
            compliance['system_availability'] = availability

        except Exception as e:
            logger.debug(f"❌ Error calculating SLA compliance: {e}")
            # Default values
            compliance = {
                'data_freshness': 0.95,
                'data_quality': 0.90,
                'system_availability': 0.995
            }

        return compliance

    async def metrics_endpoint(self, request):
        """HTTP endpoint for Prometheus metrics scraping."""

        try:
            # Check if metrics are cached and recent (last 30 seconds)
            cache_age = (datetime.now() - self.last_metrics_update).total_seconds()

            if cache_age < 30 and 'metrics_text' in self.metrics_cache:
                metrics_text = self.metrics_cache['metrics_text']
                logger.debug(f"📊 Serving cached realtime metrics (age: {cache_age:.1f}s)")
            else:
                # Collect fresh metrics
                logger.debug("📊 Collecting fresh realtime metrics")
                metrics_data = await self.collect_realtime_metrics()
                metrics_text = metrics_data.get('metrics_text', '# No metrics available\n')

            return web.Response(
                text=metrics_text,
                content_type='text/plain; version=0.0.4'
            )

        except Exception as e:
            logger.error(f"❌ Error serving metrics: {e}")
            return web.Response(
                text=f"# Error serving metrics: {str(e)}\n",
                content_type='text/plain',
                status=500
            )

    async def health_endpoint(self, request):
        """Health check endpoint."""

        health_data = {
            'status': 'healthy' if self.monitor.running else 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'last_metrics_update': self.last_metrics_update.isoformat(),
            'metrics_cache_size': len(self.metrics_cache),
            'monitor_running': self.monitor.running
        }

        return web.json_response(health_data)

    async def alerting_rules_endpoint(self, request):
        """Endpoint to serve Prometheus alerting rules."""

        alerting_rules = self.generate_prometheus_alerting_rules()

        return web.Response(
            text=alerting_rules,
            content_type='text/yaml'
        )

    async def grafana_dashboard_endpoint(self, request):
        """Endpoint to serve Grafana dashboard configuration."""

        dashboard_config = self.generate_grafana_dashboard()

        return web.json_response(dashboard_config)

    def generate_prometheus_alerting_rules(self) -> str:
        """Generate Prometheus alerting rules for real-time collection."""

        rules = {
            'groups': [{
                'name': 'ats_realtime_collection',
                'interval': '30s',
                'rules': [
                    {
                        'alert': 'ATSRealtimeDataStale',
                        'expr': 'ats_realtime_data_freshness_seconds > 300',
                        'for': '2m',
                        'labels': {
                            'severity': 'warning',
                            'service': 'ats-realtime-collection'
                        },
                        'annotations': {
                            'summary': 'ATS real-time data is stale',
                            'description': 'Real-time data for {{ $labels.symbol }} from {{ $labels.vendor }} is {{ $value }} seconds old'
                        }
                    },
                    {
                        'alert': 'ATSRealtimeQualityLow',
                        'expr': 'ats_realtime_quality_score < 0.7',
                        'for': '5m',
                        'labels': {
                            'severity': 'warning',
                            'service': 'ats-realtime-collection'
                        },
                        'annotations': {
                            'summary': 'ATS real-time data quality is low',
                            'description': 'Quality score for {{ $labels.symbol }} from {{ $labels.vendor }} is {{ $value }}'
                        }
                    },
                    {
                        'alert': 'ATSRealtimePriceDivergence',
                        'expr': 'ats_realtime_price_divergence_pct > 5',
                        'for': '3m',
                        'labels': {
                            'severity': 'warning',
                            'service': 'ats-realtime-collection'
                        },
                        'annotations': {
                            'summary': 'ATS real-time price divergence detected',
                            'description': 'Price divergence for {{ $labels.symbol }} is {{ $value }}%'
                        }
                    },
                    {
                        'alert': 'ATSRealtimeSystemDown',
                        'expr': 'ats_realtime_system_availability < 0.8',
                        'for': '1m',
                        'labels': {
                            'severity': 'critical',
                            'service': 'ats-realtime-collection'
                        },
                        'annotations': {
                            'summary': 'ATS real-time collection system availability is low',
                            'description': 'System availability is {{ $value }}, indicating major issues'
                        }
                    },
                    {
                        'alert': 'ATSRealtimeSLAViolation',
                        'expr': 'ats_realtime_sla_compliance < 0.95',
                        'for': '10m',
                        'labels': {
                            'severity': 'warning',
                            'service': 'ats-realtime-collection'
                        },
                        'annotations': {
                            'summary': 'ATS real-time SLA compliance violation',
                            'description': 'SLA compliance for {{ $labels.metric }} is {{ $value }}'
                        }
                    },
                    {
                        'alert': 'ATSRealtimeHighVolatility',
                        'expr': 'ats_realtime_price_volatility > 0.05',
                        'for': '1m',
                        'labels': {
                            'severity': 'info',
                            'service': 'ats-realtime-collection'
                        },
                        'annotations': {
                            'summary': 'High price volatility detected',
                            'description': 'Price volatility for {{ $labels.symbol }} from {{ $labels.vendor }} is {{ $value }}'
                        }
                    }
                ]
            }]
        }

        return yaml.dump(rules, default_flow_style=False)

    def generate_grafana_dashboard(self) -> Dict[str, Any]:
        """Generate Grafana dashboard configuration."""

        dashboard = {
            "dashboard": {
                "id": None,
                "title": "ATS Real-time Collection Monitoring",
                "tags": ["ats", "realtime", "collection"],
                "timezone": "utc",
                "refresh": "30s",
                "time": {
                    "from": "now-1h",
                    "to": "now"
                },
                "panels": [
                    {
                        "id": 1,
                        "title": "System Availability",
                        "type": "stat",
                        "targets": [{
                            "expr": "ats_realtime_system_availability",
                            "legendFormat": "Availability"
                        }],
                        "fieldConfig": {
                            "defaults": {
                                "unit": "percentunit",
                                "min": 0,
                                "max": 1,
                                "thresholds": {
                                    "steps": [
                                        {"color": "red", "value": 0},
                                        {"color": "yellow", "value": 0.95},
                                        {"color": "green", "value": 0.99}
                                    ]
                                }
                            }
                        },
                        "gridPos": {"h": 8, "w": 6, "x": 0, "y": 0}
                    },
                    {
                        "id": 2,
                        "title": "Data Freshness",
                        "type": "graph",
                        "targets": [{
                            "expr": "ats_realtime_data_freshness_seconds",
                            "legendFormat": "{{vendor}} {{symbol}}"
                        }],
                        "yAxes": [{
                            "label": "Seconds",
                            "min": 0
                        }],
                        "gridPos": {"h": 8, "w": 12, "x": 6, "y": 0}
                    },
                    {
                        "id": 3,
                        "title": "Quality Scores",
                        "type": "graph",
                        "targets": [{
                            "expr": "ats_realtime_quality_score",
                            "legendFormat": "{{vendor}} {{symbol}}"
                        }],
                        "yAxes": [{
                            "label": "Quality Score",
                            "min": 0,
                            "max": 1
                        }],
                        "gridPos": {"h": 8, "w": 6, "x": 18, "y": 0}
                    },
                    {
                        "id": 4,
                        "title": "Collection Rate",
                        "type": "graph",
                        "targets": [{
                            "expr": "rate(ats_realtime_records_per_hour[5m]) * 60",
                            "legendFormat": "{{vendor}} {{symbol}}"
                        }],
                        "yAxes": [{
                            "label": "Records/min",
                            "min": 0
                        }],
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8}
                    },
                    {
                        "id": 5,
                        "title": "Price Divergence",
                        "type": "graph",
                        "targets": [{
                            "expr": "ats_realtime_price_divergence_pct",
                            "legendFormat": "{{symbol}}"
                        }],
                        "yAxes": [{
                            "label": "Percentage",
                            "min": 0
                        }],
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8}
                    },
                    {
                        "id": 6,
                        "title": "SLA Compliance",
                        "type": "table",
                        "targets": [{
                            "expr": "ats_realtime_sla_compliance",
                            "legendFormat": "{{metric}}",
                            "format": "table"
                        }],
                        "gridPos": {"h": 8, "w": 24, "x": 0, "y": 16}
                    }
                ]
            }
        }

        return dashboard

    async def start_metrics_server(self):
        """Start the Prometheus metrics server."""

        await self.initialize()

        # Start monitoring in background
        monitor_task = asyncio.create_task(self.monitor.start_monitoring())

        # Start HTTP server
        runner = web.AppRunner(self.app)
        await runner.setup()

        site = web.TCPSite(runner, '0.0.0.0', self.metrics_port)
        await site.start()

        logger.info(f"🚀 Realtime Prometheus metrics server started on port {self.metrics_port}")
        logger.info(f"📊 Metrics endpoint: http://localhost:{self.metrics_port}/metrics")
        logger.info(f"🚨 Alerting rules: http://localhost:{self.metrics_port}/config/rules")
        logger.info(f"📈 Grafana dashboard: http://localhost:{self.metrics_port}/config/grafana")

        try:
            await monitor_task
        except KeyboardInterrupt:
            logger.info("📤 Received keyboard interrupt")
        finally:
            await self.close()

    async def close(self):
        """Close Prometheus integration."""

        await self.monitor.close()

        if self.server:
            self.server.close()
            await self.server.wait_closed()

        logger.info("✅ Realtime Prometheus Integration closed")


async def main():
    """Main function for standalone Prometheus integration."""

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    import argparse

    parser = argparse.ArgumentParser(description='ATS Real-time Collection Prometheus Integration')
    parser.add_argument('--port', type=int, default=8091, help='Metrics server port (default: 8091)')
    parser.add_argument('--monitor-interval', type=int, default=60, help='Monitoring interval in seconds (default: 60)')

    args = parser.parse_args()

    integration = RealtimePrometheusIntegration(
        monitor_interval=args.monitor_interval,
        metrics_port=args.port
    )

    await integration.start_metrics_server()


if __name__ == "__main__":
    asyncio.run(main())