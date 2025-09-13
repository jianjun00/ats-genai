#!/usr/bin/env python3
"""
ATS-INTG Prometheus Metrics HTTP Server

Exposes daily price coverage metrics via HTTP endpoint for Prometheus scraping.
Runs as a background service and updates metrics periodically.

Features:
- HTTP endpoint on /metrics for Prometheus scraping
- Real-time metrics from database queries
- Configurable refresh intervals
- Health check endpoint
- Graceful shutdown handling

Usage:
    python3 scripts/prometheus_metrics_server.py
    python3 scripts/prometheus_metrics_server.py --port 8080 --refresh-interval 300
    curl http://localhost:8080/metrics
    curl http://localhost:8080/health
"""

import asyncio
import asyncpg
import logging
import os
import sys
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import json
from pathlib import Path
import aiohttp
from aiohttp import web
import time
import signal
import shutil
import psutil
# import holidays  # Not available in container - use simple weekend check

# Add src to path for imports
sys.path.insert(0, '/workspace/src')

logger = logging.getLogger(__name__)

class PrometheusMetricsServer:
    """HTTP server that exposes ATS price coverage metrics for Prometheus."""

    def __init__(self, port: int = 8080, refresh_interval: int = 300):
        self.port = port
        self.refresh_interval = refresh_interval  # seconds between metric updates
        self.db_pool = None
        self.app = None
        self.server = None
        self.running = False

        # Current metrics cache
        self.metrics_cache = {
            'timestamp': datetime.now(),
            'content': '# No metrics available yet\n'
        }

        # Major US holidays (simplified for container compatibility)
        self.us_holidays = self._get_major_holidays()

    def _get_major_holidays(self):
        """Get major US market holidays."""
        current_year = datetime.now().year
        holidays_set = set()
        for year in [current_year - 1, current_year]:
            holidays_set.add(date(year, 1, 1))  # New Year's Day
            holidays_set.add(date(year, 7, 4))  # Independence Day
            holidays_set.add(date(year, 12, 25)) # Christmas
        return holidays_set

    async def initialize(self):
        """Initialize database connection and HTTP server."""
        try:
            # Database connection for INTG environment
            db_url = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'intg_password')}@{os.getenv('DB_HOST', 'ats-intg-postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'intg_db')}"

            self.db_pool = await asyncpg.create_pool(
                db_url,
                min_size=1,
                max_size=5,
                command_timeout=30
            )

            # Create aiohttp application
            self.app = web.Application()
            self.app.router.add_get('/metrics', self.metrics_handler)
            self.app.router.add_get('/health', self.health_handler)
            self.app.router.add_get('/api/v1/query', self.prometheus_query_handler)
            self.app.router.add_get('/', self.root_handler)

            logger.info("✅ Prometheus metrics server initialized")

        except Exception as e:
            logger.error(f"❌ Failed to initialize metrics server: {e}")
            raise

    async def close(self):
        """Close database connections and HTTP server."""
        self.running = False

        if self.server:
            self.server.close()
            await self.server.wait_closed()

        if self.db_pool:
            await self.db_pool.close()

        logger.info("✅ Prometheus metrics server closed")

    def is_trading_day(self, check_date: date) -> bool:
        """Check if a date is a trading day (excludes weekends and US holidays)."""
        # Skip weekends
        if check_date.weekday() >= 5:
            return False
        # Skip US holidays
        return check_date not in self.us_holidays

    def collect_system_metrics(self) -> List[str]:
        """Collect system metrics including disk usage."""
        metrics = []

        try:
            # Disk usage metrics
            disk_usage = shutil.disk_usage('/')
            total_bytes = disk_usage.total
            free_bytes = disk_usage.free
            used_bytes = total_bytes - free_bytes
            usage_percent = (used_bytes / total_bytes) * 100 if total_bytes > 0 else 0

            metrics.extend([
                "# HELP ats_filesystem_size_bytes Total filesystem size in bytes",
                "# TYPE ats_filesystem_size_bytes gauge",
                f'ats_filesystem_size_bytes{{mountpoint="/"}} {total_bytes}',
                "# HELP ats_filesystem_free_bytes Free filesystem space in bytes",
                "# TYPE ats_filesystem_free_bytes gauge",
                f'ats_filesystem_free_bytes{{mountpoint="/"}} {free_bytes}',
                "# HELP ats_filesystem_used_bytes Used filesystem space in bytes",
                "# TYPE ats_filesystem_used_bytes gauge",
                f'ats_filesystem_used_bytes{{mountpoint="/"}} {used_bytes}',
                "# HELP ats_filesystem_usage_percent Filesystem usage percentage",
                "# TYPE ats_filesystem_usage_percent gauge",
                f'ats_filesystem_usage_percent{{mountpoint="/"}} {usage_percent:.2f}'
            ])

            # Memory metrics
            memory = psutil.virtual_memory()
            metrics.extend([
                "# HELP ats_memory_total_bytes Total system memory in bytes",
                "# TYPE ats_memory_total_bytes gauge",
                f'ats_memory_total_bytes {memory.total}',
                "# HELP ats_memory_used_bytes Used system memory in bytes",
                "# TYPE ats_memory_used_bytes gauge",
                f'ats_memory_used_bytes {memory.used}',
                "# HELP ats_memory_usage_percent Memory usage percentage",
                "# TYPE ats_memory_usage_percent gauge",
                f'ats_memory_usage_percent {memory.percent}'
            ])

        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")

        return metrics

    async def collect_metrics(self) -> str:
        """Collect current metrics from database and format for Prometheus."""
        try:
            metrics_lines = []
            timestamp = int(datetime.now().timestamp())

            # Add system metrics (disk, memory)
            metrics_lines.extend(self.collect_system_metrics())
            metrics_lines.append("")  # Empty line separator

            # Include API status tracking metrics
            try:
                from api_status_tracker import get_global_tracker
                api_tracker = get_global_tracker()
                api_metrics = api_tracker.get_prometheus_metrics()
                if api_metrics:
                    metrics_lines.append("# API Status Tracking Metrics")
                    metrics_lines.append(api_metrics)
            except Exception as e:
                logger.debug(f"API tracker metrics not available: {e}")

            async with self.db_pool.acquire() as conn:
                # Total active instruments
                total_instruments_query = """
                SELECT COUNT(*)
                FROM intg_instrument
                WHERE active = true
                """
                total_instruments = await conn.fetchval(total_instruments_query)

                metrics_lines.extend([
                    "# HELP ats_total_instruments Total number of active instruments",
                    "# TYPE ats_total_instruments gauge",
                    f"ats_total_instruments {total_instruments} {timestamp}"
                ])

                # Per-vendor metrics
                vendors = ['tiingo', 'polygon', 'eodhd']

                for vendor in vendors:
                    table_name = f"intg_daily_price_polygon_{vendor}"

                    # Check if table exists
                    table_exists_query = """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = $1
                    )
                    """
                    table_exists = await conn.fetchval(table_exists_query, table_name)

                    if not table_exists:
                        # Set zero metrics for non-existent tables
                        metrics_lines.extend([
                            f'ats_instruments_with_recent_data{{vendor="{vendor}"}} 0 {timestamp}',
                            f'ats_missing_price_data_alerts{{vendor="{vendor}"}} {total_instruments} {timestamp}',
                            f'ats_data_freshness_hours{{vendor="{vendor}"}} 168 {timestamp}',  # 1 week
                            f'ats_price_coverage_percentage{{vendor="{vendor}"}} 0.00 {timestamp}'
                        ])
                        continue

                    # Instruments with recent data (last 7 days)
                    recent_cutoff = date.today() - timedelta(days=7)
                    recent_data_query = f"""
                    SELECT COUNT(DISTINCT instrument_id)
                    FROM {table_name}
                    WHERE date >= $1
                    """
                    instruments_with_recent = await conn.fetchval(recent_data_query, recent_cutoff)

                    # Missing data alerts (instruments without data on most recent trading day)
                    latest_trading_day = date.today() - timedelta(days=1)
                    while not self.is_trading_day(latest_trading_day) and latest_trading_day > date.today() - timedelta(days=7):
                        latest_trading_day -= timedelta(days=1)

                    missing_today_query = f"""
                    SELECT COUNT(*)
                    FROM intg_instrument i
                    WHERE i.active = true
                      AND i.id NOT IN (
                          SELECT DISTINCT instrument_id
                          FROM {table_name}
                          WHERE date = $1
                      )
                    """
                    missing_today = await conn.fetchval(missing_today_query, latest_trading_day)

                    # Data freshness (hours since most recent data)
                    freshness_query = f"SELECT MAX(date) FROM {table_name}"
                    latest_date = await conn.fetchval(freshness_query)

                    if latest_date:
                        hours_since = (datetime.now().date() - latest_date).days * 24
                        # Add hours within the day
                        hours_since += datetime.now().hour
                    else:
                        hours_since = 168  # 1 week default

                    # Coverage percentage (last 30 days)
                    coverage_start = date.today() - timedelta(days=30)
                    coverage_end = date.today() - timedelta(days=1)

                    # Count trading days in period
                    trading_days = []
                    current_date = coverage_start
                    while current_date <= coverage_end:
                        if self.is_trading_day(current_date):
                            trading_days.append(current_date)
                        current_date += timedelta(days=1)

                    if trading_days:
                        expected_records = total_instruments * len(trading_days)

                        actual_records_query = f"""
                        SELECT COUNT(*)
                        FROM {table_name}
                        WHERE date >= $1 AND date <= $2
                        """
                        actual_records = await conn.fetchval(actual_records_query, coverage_start, coverage_end)

                        coverage_percentage = (actual_records / expected_records * 100) if expected_records > 0 else 0
                    else:
                        coverage_percentage = 0

                    # Add vendor metrics
                    metrics_lines.extend([
                        f'ats_instruments_with_recent_data{{vendor="{vendor}"}} {instruments_with_recent} {timestamp}',
                        f'ats_missing_price_data_alerts{{vendor="{vendor}"}} {missing_today} {timestamp}',
                        f'ats_data_freshness_hours{{vendor="{vendor}"}} {hours_since:.1f} {timestamp}',
                        f'ats_price_coverage_percentage{{vendor="{vendor}"}} {coverage_percentage:.2f} {timestamp}'
                    ])

                # Add metric definitions at the beginning (after total instruments)
                coverage_help = [
                    "",
                    "# HELP ats_instruments_with_recent_data Number of instruments with data in last 7 days",
                    "# TYPE ats_instruments_with_recent_data gauge"
                ]

                missing_help = [
                    "",
                    "# HELP ats_missing_price_data_alerts Number of instruments missing data on most recent trading day",
                    "# TYPE ats_missing_price_data_alerts gauge"
                ]

                freshness_help = [
                    "",
                    "# HELP ats_data_freshness_hours Hours since most recent price data",
                    "# TYPE ats_data_freshness_hours gauge"
                ]

                coverage_pct_help = [
                    "",
                    "# HELP ats_price_coverage_percentage Daily price coverage percentage over last 30 days",
                    "# TYPE ats_price_coverage_percentage gauge"
                ]

                # Add batch job metrics help text
                batch_jobs_help = [
                    "",
                    "# HELP ats_daily_prices_sync_symbols_processed_total Total number of symbols processed during daily prices sync",
                    "# TYPE ats_daily_prices_sync_symbols_processed_total counter",
                    "",
                    "# HELP ats_daily_prices_sync_prices_processed_total Total number of price records processed during sync",
                    "# TYPE ats_daily_prices_sync_prices_processed_total counter",
                    "",
                    "# HELP ats_daily_prices_sync_duration_seconds Duration of daily prices sync operations in seconds",
                    "# TYPE ats_daily_prices_sync_duration_seconds histogram",
                    "",
                    "# HELP ats_daily_prices_sync_success_rate Success rate of daily prices sync operations (0.0 to 1.0)",
                    "# TYPE ats_daily_prices_sync_success_rate gauge",
                    "",
                    "# HELP ats_daily_prices_backfill_symbols_processed_total Total number of symbols processed during daily prices backfill",
                    "# TYPE ats_daily_prices_backfill_symbols_processed_total counter",
                    "",
                    "# HELP ats_daily_prices_backfill_prices_collected_total Total number of price records collected during backfill",
                    "# TYPE ats_daily_prices_backfill_prices_collected_total counter",
                    "",
                    "# HELP ats_daily_prices_backfill_api_calls_total Total number of API calls made during backfill",
                    "# TYPE ats_daily_prices_backfill_api_calls_total counter",
                    "",
                    "# HELP ats_daily_prices_backfill_duration_seconds Duration of daily prices backfill operations in seconds",
                    "# TYPE ats_daily_prices_backfill_duration_seconds histogram",
                    "",
                    "# HELP ats_daily_prices_backfill_success_rate Success rate of daily prices backfill operations (0.0 to 1.0)",
                    "# TYPE ats_daily_prices_backfill_success_rate gauge"
                ]

                # Add minute bars metrics
                minute_bars_help = [
                    "",
                    "# HELP ats_daily_minute_backfill_instruments_processed Number of instruments processed in daily minute bar backfill",
                    "# TYPE ats_daily_minute_backfill_instruments_processed gauge",
                    "",
                    "# HELP ats_daily_minute_backfill_total_minute_bars Total number of minute bars processed",
                    "# TYPE ats_daily_minute_backfill_total_minute_bars gauge",
                    "",
                    "# HELP ats_daily_minute_backfill_symbols_by_type Number of symbols by instrument type",
                    "# TYPE ats_daily_minute_backfill_symbols_by_type gauge",
                    "",
                    "# HELP ats_daily_minute_backfill_bars_by_type Number of minute bars by instrument type",
                    "# TYPE ats_daily_minute_backfill_bars_by_type gauge",
                    "",
                    "# HELP ats_daily_minute_backfill_symbols_by_letter Number of symbols by first letter",
                    "# TYPE ats_daily_minute_backfill_symbols_by_letter gauge"
                ]

                # Insert help text after total instruments
                final_metrics = metrics_lines[:3]  # Total instruments
                final_metrics.extend(coverage_help)
                final_metrics.extend(missing_help)
                final_metrics.extend(freshness_help)
                final_metrics.extend(coverage_pct_help)
                final_metrics.extend(batch_jobs_help)
                final_metrics.extend(minute_bars_help)
                final_metrics.append("")
                final_metrics.extend(metrics_lines[3:])  # All vendor metrics

                metrics_content = '\n'.join(final_metrics) + '\n'

                # Update cache
                self.metrics_cache = {
                    'timestamp': datetime.now(),
                    'content': metrics_content
                }

                logger.debug(f"📊 Updated metrics cache: {len(final_metrics)} lines")
                return metrics_content

        except Exception as e:
            logger.error(f"❌ Error collecting metrics: {e}")
            # Return error metric
            error_timestamp = int(datetime.now().timestamp())
            return f"""# Error collecting metrics
# HELP ats_metrics_collection_errors Total number of metrics collection errors
# TYPE ats_metrics_collection_errors counter
ats_metrics_collection_errors 1 {error_timestamp}
"""

    async def metrics_handler(self, request):
        """HTTP handler for /metrics endpoint."""
        try:
            # Return cached metrics if recent (within 30 seconds)
            cache_age = (datetime.now() - self.metrics_cache['timestamp']).total_seconds()

            if cache_age < 30:
                content = self.metrics_cache['content']
                logger.debug(f"📊 Serving cached metrics (age: {cache_age:.1f}s)")
            else:
                content = await self.collect_metrics()
                logger.debug(f"📊 Serving fresh metrics")

            return web.Response(
                text=content,
                content_type='text/plain'
            )

        except Exception as e:
            logger.error(f"❌ Error in metrics handler: {e}")
            return web.Response(
                text=f"# Error: {str(e)}\n",
                status=500,
                content_type='text/plain'
            )

    async def health_handler(self, request):
        """HTTP handler for /health endpoint."""
        try:
            # Quick database health check
            async with self.db_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")

            health_data = {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'metrics_cache_age': (datetime.now() - self.metrics_cache['timestamp']).total_seconds(),
                'database_connection': 'ok'
            }

            return web.json_response(health_data)

        except Exception as e:
            logger.error(f"❌ Health check failed: {e}")
            return web.json_response(
                {
                    'status': 'unhealthy',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                },
                status=503
            )

    async def prometheus_query_handler(self, request):
        """Prometheus-compatible query endpoint for Grafana."""
        try:
            query = request.query.get('query', '')

            if not query:
                return web.json_response({
                    'status': 'error',
                    'errorType': 'bad_data',
                    'error': 'query parameter is required'
                }, status=400)

            # Get current metrics
            metrics_text = await self.collect_metrics()

            # Parse metrics to find the requested one
            result_value = None
            current_timestamp = datetime.now().timestamp()

            # Simple metric parsing for common queries
            for line in metrics_text.split('\n'):
                line = line.strip()
                if line.startswith('#') or not line:
                    continue

                parts = line.split(' ')
                if len(parts) >= 2:
                    metric_name = parts[0]
                    metric_value = parts[1]

                    # Handle exact match queries
                    if query == metric_name:
                        result_value = float(metric_value)
                        break

                    # Handle sum() queries like sum(ats_instruments_with_recent_data)
                    if query.startswith('sum(') and query.endswith(')'):
                        inner_query = query[4:-1]  # Remove sum() wrapper
                        if metric_name.startswith(inner_query):
                            if result_value is None:
                                result_value = 0
                            result_value += float(metric_value)

            if result_value is not None:
                # Return Prometheus-compatible response
                return web.json_response({
                    'status': 'success',
                    'data': {
                        'resultType': 'vector',
                        'result': [{
                            'metric': {'__name__': query},
                            'value': [current_timestamp, str(result_value)]
                        }]
                    }
                })
            else:
                # No data found
                return web.json_response({
                    'status': 'success',
                    'data': {
                        'resultType': 'vector',
                        'result': []
                    }
                })

        except Exception as e:
            logger.error(f"❌ Error in Prometheus query handler: {e}")
            return web.json_response({
                'status': 'error',
                'errorType': 'internal',
                'error': str(e)
            }, status=500)

    async def root_handler(self, request):
        """HTTP handler for root / endpoint."""
        html_content = """
<!DOCTYPE html>
<html>
<head>
    <title>ATS-INTG Prometheus Metrics</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .endpoint { background: #f5f5f5; padding: 10px; margin: 10px 0; }
        .metric { color: #0066cc; }
    </style>
</head>
<body>
    <h1>🎯 ATS-INTG Prometheus Metrics Server</h1>

    <h2>Available Endpoints</h2>
    <div class="endpoint">
        <strong>GET /metrics</strong> - Prometheus metrics in text format
    </div>
    <div class="endpoint">
        <strong>GET /health</strong> - Health check endpoint (JSON)
    </div>

    <h2>Exposed Metrics</h2>
    <ul>
        <li><code class="metric">ats_total_instruments</code> - Total active instruments</li>
        <li><code class="metric">ats_instruments_with_recent_data</code> - Instruments with data in last 7 days (by vendor)</li>
        <li><code class="metric">ats_missing_price_data_alerts</code> - Instruments missing recent data (by vendor)</li>
        <li><code class="metric">ats_data_freshness_hours</code> - Hours since most recent data (by vendor)</li>
        <li><code class="metric">ats_price_coverage_percentage</code> - Coverage % over last 30 days (by vendor)</li>
    </ul>
    <h3>Batch Job Metrics</h3>
    <ul>
        <li><code class="metric">ats_daily_prices_sync_symbols_processed_total</code> - Symbols processed in sync jobs (by vendor)</li>
        <li><code class="metric">ats_daily_prices_sync_prices_processed_total</code> - Price records processed in sync jobs (by vendor)</li>
        <li><code class="metric">ats_daily_prices_sync_success_rate</code> - Success rate of sync operations (by vendor)</li>
        <li><code class="metric">ats_daily_prices_backfill_symbols_processed_total</code> - Symbols processed in backfill jobs (by vendor)</li>
        <li><code class="metric">ats_daily_prices_backfill_prices_collected_total</code> - Price records collected in backfill jobs (by vendor)</li>
        <li><code class="metric">ats_daily_prices_backfill_api_calls_total</code> - API calls made during backfill (by vendor, status)</li>
        <li><code class="metric">ats_daily_prices_backfill_success_rate</code> - Success rate of backfill operations (by vendor)</li>
    </ul>

    <h2>Usage</h2>
    <p>Configure Prometheus to scrape <code>http://&lt;host&gt;:""" + str(self.port) + """/metrics</code></p>

    <p><a href="/metrics">View Raw Metrics</a> | <a href="/health">Health Check</a></p>
</body>
</html>
"""
        return web.Response(text=html_content, content_type='text/html')

    async def metrics_update_loop(self):
        """Background loop to update metrics cache periodically."""
        logger.info(f"📊 Starting metrics update loop (refresh interval: {self.refresh_interval}s)")

        while self.running:
            try:
                await self.collect_metrics()
                logger.debug(f"📊 Metrics updated at {datetime.now()}")

                # Sleep for refresh interval
                await asyncio.sleep(self.refresh_interval)

            except Exception as e:
                logger.error(f"❌ Error in metrics update loop: {e}")
                # Sleep shorter on error
                await asyncio.sleep(min(60, self.refresh_interval))

    async def start_server(self):
        """Start the HTTP server and background tasks."""
        try:
            self.running = True

            # Start HTTP server
            self.server = await aiohttp.web._run_app(
                self.app,
                host='0.0.0.0',
                port=self.port,
                handle_signals=False,
                print=None  # Disable default logging
            )

            logger.info(f"🚀 Prometheus metrics server started on port {self.port}")
            logger.info(f"📊 Metrics endpoint: http://localhost:{self.port}/metrics")
            logger.info(f"❤️ Health endpoint: http://localhost:{self.port}/health")

            # Start background metrics update loop
            metrics_task = asyncio.create_task(self.metrics_update_loop())

            # Initial metrics collection
            await self.collect_metrics()

            # Wait for shutdown signal
            try:
                await metrics_task
            except asyncio.CancelledError:
                logger.info("📊 Metrics update loop cancelled")

        except Exception as e:
            logger.error(f"❌ Failed to start server: {e}")
            raise

    def setup_signal_handlers(self):
        """Setup graceful shutdown signal handlers."""
        def signal_handler(signum, frame):
            logger.info(f"📤 Received signal {signum}, initiating graceful shutdown...")
            asyncio.create_task(self.close())

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

async def main():
    """Main function for Prometheus metrics server."""
    import argparse

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description='ATS-INTG Prometheus Metrics Server')
    parser.add_argument('--port', type=int, default=8080, help='HTTP server port (default: 8080)')
    parser.add_argument('--refresh-interval', type=int, default=300, help='Metrics refresh interval in seconds (default: 300)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("="*80)
    logger.info("ATS-INTG PROMETHEUS METRICS SERVER")
    logger.info("="*80)
    logger.info(f"Port: {args.port}")
    logger.info(f"Refresh interval: {args.refresh_interval} seconds")

    # Initialize and start server
    server = PrometheusMetricsServer(
        port=args.port,
        refresh_interval=args.refresh_interval
    )

    try:
        await server.initialize()
        server.setup_signal_handlers()

        await server.start_server()

    except KeyboardInterrupt:
        logger.info("📤 Received keyboard interrupt")
    finally:
        await server.close()
        logger.info("✅ Metrics server shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())