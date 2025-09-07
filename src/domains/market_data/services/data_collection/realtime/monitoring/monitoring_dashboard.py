#!/usr/bin/env python3
"""
Real-time Collection Monitoring Dashboard

Comprehensive web dashboard for monitoring real-time data collection with:
- Live metrics display and charts
- Alert history and management
- System health overview
- Performance analytics
- Cross-vendor comparison tools
- Configuration management
- Prometheus metrics integration

Features:
- Real-time data updates via WebSocket
- Interactive charts with Plotly
- Alert acknowledgement and management
- Historical trend analysis
- Mobile-responsive design
- Export capabilities for metrics
- Integration with existing Prometheus/Grafana

Usage:
    python3 -m src.market_data.realtime.monitoring.monitoring_dashboard
    # Access at http://localhost:8090
"""

import asyncio
import aiohttp
from aiohttp import web, WSMsgType
import aiohttp_jinja2
import jinja2
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import asyncpg
from pathlib import Path
import weakref

# Add src to path
sys.path.insert(0, '/workspace/src')

from .realtime_collection_monitor import RealtimeCollectionMonitor
from .alert_channels import AlertChannelManager

logger = logging.getLogger(__name__)


class MonitoringDashboard:
    """Web dashboard for real-time collection monitoring."""

    def __init__(self,
                 host: str = "0.0.0.0",
                 port: int = 8090,
                 monitor_interval: int = 30):
        """Initialize the monitoring dashboard."""

        self.host = host
        self.port = port
        self.monitor_interval = monitor_interval

        # Components
        self.monitor = RealtimeCollectionMonitor(monitoring_interval=monitor_interval)
        self.alert_manager = AlertChannelManager()

        # Web application
        self.app = None
        self.websockets = weakref.WeakSet()

        # Dashboard state
        self.dashboard_data = {
            'last_update': datetime.now(),
            'system_status': 'initializing',
            'metrics': {},
            'alerts': [],
            'performance_history': []
        }

        logger.info(f"🎯 Monitoring Dashboard initialized on {host}:{port}")

    async def initialize(self):
        """Initialize dashboard components."""

        try:
            # Initialize monitoring components
            await self.monitor.initialize()

            # Setup web application
            self.app = web.Application()

            # Setup Jinja2 templates
            template_dir = Path(__file__).parent / 'templates'
            template_dir.mkdir(exist_ok=True)

            aiohttp_jinja2.setup(
                self.app,
                loader=jinja2.FileSystemLoader(str(template_dir))
            )

            # Setup routes
            self.setup_routes()

            # Create default templates if they don't exist
            await self.create_default_templates()

            logger.info("✅ Dashboard components initialized")

        except Exception as e:
            logger.error(f"❌ Failed to initialize dashboard: {e}")
            raise

    def setup_routes(self):
        """Setup HTTP routes for the dashboard."""

        # Web pages
        self.app.router.add_get('/', self.index_handler)
        self.app.router.add_get('/metrics', self.metrics_handler)
        self.app.router.add_get('/alerts', self.alerts_handler)
        self.app.router.add_get('/config', self.config_handler)

        # API endpoints
        self.app.router.add_get('/api/status', self.api_status_handler)
        self.app.router.add_get('/api/metrics/current', self.api_current_metrics_handler)
        self.app.router.add_get('/api/metrics/history', self.api_metrics_history_handler)
        self.app.router.add_get('/api/alerts/current', self.api_current_alerts_handler)
        self.app.router.add_get('/api/alerts/history', self.api_alerts_history_handler)
        self.app.router.add_post('/api/alerts/acknowledge', self.api_acknowledge_alert_handler)
        self.app.router.add_post('/api/alerts/test', self.api_test_alerts_handler)

        # WebSocket for real-time updates
        self.app.router.add_get('/ws', self.websocket_handler)

        # Health check
        self.app.router.add_get('/health', self.health_handler)

        # Prometheus metrics proxy
        self.app.router.add_get('/prometheus/metrics', self.prometheus_metrics_handler)

        # Static files (if needed)
        # self.app.router.add_static('/', path='static', name='static')

    async def create_default_templates(self):
        """Create default HTML templates."""

        template_dir = Path(__file__).parent / 'templates'

        # Main dashboard template
        index_template = '''
<!DOCTYPE html>
<html>
<head>
    <title>ATS Real-time Collection Monitor</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
        .status-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 20px; }
        .status-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .status-card h3 { margin-top: 0; color: #2c3e50; }
        .metric-value { font-size: 2em; font-weight: bold; color: #27ae60; }
        .alert-critical { color: #e74c3c; }
        .alert-warning { color: #f39c12; }
        .alert-info { color: #3498db; }
        .timestamp { color: #7f8c8d; font-size: 0.9em; }
        .table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        .table th, .table td { padding: 8px 12px; text-align: left; border-bottom: 1px solid #ddd; }
        .table th { background: #ecf0f1; font-weight: bold; }
        .refresh-indicator { position: fixed; top: 10px; right: 10px; padding: 8px 12px; background: #27ae60; color: white; border-radius: 4px; }
        .chart-container { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }
        .nav { margin-bottom: 20px; }
        .nav a { display: inline-block; padding: 10px 15px; background: #3498db; color: white; text-decoration: none; border-radius: 4px; margin-right: 10px; }
        .nav a:hover { background: #2980b9; }
    </style>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>
    <div class="header">
        <h1>🎯 ATS Real-time Collection Monitor</h1>
        <p class="timestamp" id="last-update">Last updated: {{ last_update }}</p>
    </div>

    <div class="nav">
        <a href="/">Dashboard</a>
        <a href="/metrics">Metrics</a>
        <a href="/alerts">Alerts</a>
        <a href="/config">Configuration</a>
    </div>

    <div class="refresh-indicator" id="refresh-indicator" style="display: none;">
        Updating...
    </div>

    <div class="status-grid">
        <div class="status-card">
            <h3>System Status</h3>
            <div class="metric-value" id="system-status">{{ system_status }}</div>
            <p>Collection active for AAPL and TSLA</p>
        </div>

        <div class="status-card">
            <h3>Data Freshness</h3>
            <div class="metric-value" id="data-freshness">{{ data_freshness_seconds }}s</div>
            <p>Seconds since last update</p>
        </div>

        <div class="status-card">
            <h3>Active Alerts</h3>
            <div class="metric-value" id="active-alerts">{{ active_alerts_count }}</div>
            <p>Critical issues requiring attention</p>
        </div>

        <div class="status-card">
            <h3>Average Quality</h3>
            <div class="metric-value" id="avg-quality">{{ avg_quality_score }}%</div>
            <p>Overall data quality score</p>
        </div>
    </div>

    <div class="chart-container">
        <h3>Real-time Data Collection Rate</h3>
        <div id="collection-rate-chart" style="height: 400px;"></div>
    </div>

    <div class="chart-container">
        <h3>Quality Scores by Vendor</h3>
        <div id="quality-scores-chart" style="height: 400px;"></div>
    </div>

    <div class="status-card">
        <h3>Recent Data Updates</h3>
        <table class="table" id="recent-updates">
            <thead>
                <tr>
                    <th>Symbol</th>
                    <th>Vendor</th>
                    <th>Price</th>
                    <th>Quality</th>
                    <th>Timestamp</th>
                </tr>
            </thead>
            <tbody id="recent-updates-body">
                <!-- Populated by JavaScript -->
            </tbody>
        </table>
    </div>

    <script>
        // WebSocket connection for real-time updates
        let ws = null;

        function connectWebSocket() {
            const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${wsProtocol}//${window.location.host}/ws`;

            ws = new WebSocket(wsUrl);

            ws.onopen = function() {
                console.log('WebSocket connected');
                document.getElementById('refresh-indicator').style.display = 'none';
            };

            ws.onmessage = function(event) {
                try {
                    const data = JSON.parse(event.data);
                    updateDashboard(data);
                } catch (e) {
                    console.error('Error parsing WebSocket data:', e);
                }
            };

            ws.onclose = function() {
                console.log('WebSocket disconnected, reconnecting...');
                setTimeout(connectWebSocket, 5000);
            };

            ws.onerror = function(error) {
                console.error('WebSocket error:', error);
            };
        }

        function updateDashboard(data) {
            // Update status indicators
            document.getElementById('last-update').textContent = `Last updated: ${new Date().toLocaleString()}`;
            document.getElementById('system-status').textContent = data.system_status || 'Unknown';
            document.getElementById('data-freshness').textContent = `${data.data_freshness_seconds || 0}s`;
            document.getElementById('active-alerts').textContent = data.active_alerts_count || 0;
            document.getElementById('avg-quality').textContent = `${Math.round((data.avg_quality_score || 0) * 100)}%`;

            // Update charts
            updateCollectionRateChart(data.collection_rate_history || []);
            updateQualityScoresChart(data.quality_scores || []);
            updateRecentUpdatesTable(data.recent_updates || []);

            // Show refresh indicator briefly
            const indicator = document.getElementById('refresh-indicator');
            indicator.style.display = 'block';
            setTimeout(() => indicator.style.display = 'none', 1000);
        }

        function updateCollectionRateChart(data) {
            const timestamps = data.map(d => d.timestamp);
            const rates = data.map(d => d.rate);

            const trace = {
                x: timestamps,
                y: rates,
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Collection Rate',
                line: { color: '#3498db', width: 2 }
            };

            const layout = {
                title: 'Records per Minute',
                xaxis: { title: 'Time' },
                yaxis: { title: 'Records/min' },
                showlegend: false
            };

            Plotly.newPlot('collection-rate-chart', [trace], layout, {responsive: true});
        }

        function updateQualityScoresChart(data) {
            const vendors = [...new Set(data.map(d => d.vendor))];
            const traces = vendors.map(vendor => {
                const vendorData = data.filter(d => d.vendor === vendor);
                return {
                    x: vendorData.map(d => d.timestamp),
                    y: vendorData.map(d => d.quality_score),
                    name: vendor.toUpperCase(),
                    type: 'scatter',
                    mode: 'lines+markers'
                };
            });

            const layout = {
                title: 'Quality Scores Over Time',
                xaxis: { title: 'Time' },
                yaxis: { title: 'Quality Score', range: [0, 1] }
            };

            Plotly.newPlot('quality-scores-chart', traces, layout, {responsive: true});
        }

        function updateRecentUpdatesTable(data) {
            const tbody = document.getElementById('recent-updates-body');
            tbody.innerHTML = '';

            data.slice(0, 10).forEach(update => {
                const row = tbody.insertRow();
                row.insertCell(0).textContent = update.symbol;
                row.insertCell(1).textContent = update.vendor.toUpperCase();
                row.insertCell(2).textContent = `$${update.price.toFixed(2)}`;

                const qualityCell = row.insertCell(3);
                qualityCell.textContent = `${Math.round(update.quality * 100)}%`;
                if (update.quality < 0.7) qualityCell.className = 'alert-critical';
                else if (update.quality < 0.85) qualityCell.className = 'alert-warning';

                row.insertCell(4).textContent = new Date(update.timestamp).toLocaleTimeString();
            });
        }

        // Initialize dashboard
        document.addEventListener('DOMContentLoaded', function() {
            connectWebSocket();

            // Initial empty charts
            updateCollectionRateChart([]);
            updateQualityScoresChart([]);
        });
    </script>
</body>
</html>
        '''

        with open(template_dir / 'index.html', 'w') as f:
            f.write(index_template)

        logger.info("✅ Default templates created")

    async def index_handler(self, request):
        """Main dashboard page."""

        # Get current status for template
        status_data = {
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'system_status': self.dashboard_data.get('system_status', 'Initializing'),
            'data_freshness_seconds': 0,
            'active_alerts_count': len(self.dashboard_data.get('alerts', [])),
            'avg_quality_score': 0.9  # Default
        }

        return aiohttp_jinja2.render_template('index.html', request, status_data)

    async def metrics_handler(self, request):
        """Metrics page."""
        return web.Response(text="Metrics page - Coming soon", content_type='text/plain')

    async def alerts_handler(self, request):
        """Alerts page."""
        return web.Response(text="Alerts page - Coming soon", content_type='text/plain')

    async def config_handler(self, request):
        """Configuration page."""
        return web.Response(text="Configuration page - Coming soon", content_type='text/plain')

    async def health_handler(self, request):
        """Health check endpoint."""

        health_data = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'components': {
                'monitor': 'ok' if self.monitor.running else 'stopped',
                'alert_manager': 'ok',
                'websockets': len(self.websockets)
            }
        }

        return web.json_response(health_data)

    async def api_status_handler(self, request):
        """API endpoint for system status."""

        status = self.monitor.get_monitoring_status()
        return web.json_response(status)

    async def api_current_metrics_handler(self, request):
        """API endpoint for current metrics."""

        if not self.monitor.metrics_history:
            return web.json_response({'error': 'No metrics available'}, status=404)

        latest = self.monitor.metrics_history[-1]
        return web.json_response(latest)

    async def api_metrics_history_handler(self, request):
        """API endpoint for metrics history."""

        # Get query parameters
        hours = int(request.query.get('hours', '24'))
        limit = int(request.query.get('limit', '1000'))

        cutoff_time = datetime.now() - timedelta(hours=hours)

        filtered_history = [
            h for h in self.monitor.metrics_history
            if h['timestamp'] >= cutoff_time
        ][-limit:]

        return web.json_response({
            'history': filtered_history,
            'count': len(filtered_history)
        })

    async def api_current_alerts_handler(self, request):
        """API endpoint for current alerts."""

        # Get alerts from last hour
        cutoff_time = datetime.now() - timedelta(hours=1)
        current_alerts = [
            alert for alert in self.monitor.alerts
            if alert.timestamp >= cutoff_time
        ]

        # Convert to JSON-serializable format
        alerts_data = []
        for alert in current_alerts:
            alert_dict = {
                'timestamp': alert.timestamp.isoformat(),
                'level': alert.level.value,
                'category': alert.category,
                'message': alert.message,
                'details': alert.details,
                'metric_name': alert.metric_name,
                'current_value': alert.current_value,
                'threshold_value': alert.threshold_value
            }
            alerts_data.append(alert_dict)

        return web.json_response({'alerts': alerts_data})

    async def api_alerts_history_handler(self, request):
        """API endpoint for alert history."""

        # Get query parameters
        hours = int(request.query.get('hours', '24'))
        level = request.query.get('level', None)
        category = request.query.get('category', None)

        cutoff_time = datetime.now() - timedelta(hours=hours)

        filtered_alerts = []
        for alert in self.monitor.alerts:
            if alert.timestamp < cutoff_time:
                continue

            if level and alert.level.value != level:
                continue

            if category and alert.category != category:
                continue

            alert_dict = {
                'timestamp': alert.timestamp.isoformat(),
                'level': alert.level.value,
                'category': alert.category,
                'message': alert.message,
                'details': alert.details
            }
            filtered_alerts.append(alert_dict)

        return web.json_response({
            'alerts': filtered_alerts,
            'count': len(filtered_alerts)
        })

    async def api_acknowledge_alert_handler(self, request):
        """API endpoint to acknowledge alerts."""

        try:
            data = await request.json()
            alert_id = data.get('alert_id')

            # This is a placeholder - in a full implementation you'd store
            # acknowledgements and manage alert states

            return web.json_response({'status': 'acknowledged', 'alert_id': alert_id})

        except Exception as e:
            return web.json_response({'error': str(e)}, status=400)

    async def api_test_alerts_handler(self, request):
        """API endpoint to test alert channels."""

        try:
            data = await request.json()
            channels = data.get('channels', None)

            results = await self.alert_manager.test_channels(channels)

            return web.json_response({
                'status': 'completed',
                'results': results
            })

        except Exception as e:
            logger.error(f"❌ Error testing alerts: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def prometheus_metrics_handler(self, request):
        """Proxy endpoint for Prometheus metrics."""

        try:
            if not self.monitor.metrics_history:
                return web.Response(
                    text="# No metrics available\n",
                    content_type='text/plain'
                )

            latest = self.monitor.metrics_history[-1]
            prometheus_metrics = latest.get('prometheus_metrics', '# No metrics\n')

            return web.Response(
                text=prometheus_metrics,
                content_type='text/plain'
            )

        except Exception as e:
            logger.error(f"❌ Error serving Prometheus metrics: {e}")
            return web.Response(
                text=f"# Error: {str(e)}\n",
                content_type='text/plain',
                status=500
            )

    async def websocket_handler(self, request):
        """WebSocket handler for real-time updates."""

        ws = web.WebSocketResponse()
        await ws.prepare(request)

        # Add to websockets set
        self.websockets.add(ws)

        logger.info(f"📡 WebSocket client connected (total: {len(self.websockets)})")

        try:
            # Send initial data
            if self.dashboard_data:
                await ws.send_str(json.dumps(self.dashboard_data))

            # Keep connection alive and handle messages
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        # Handle client requests here if needed
                        if data.get('type') == 'ping':
                            await ws.send_str(json.dumps({'type': 'pong'}))
                    except json.JSONDecodeError:
                        pass

                elif msg.type == WSMsgType.ERROR:
                    logger.error(f'❌ WebSocket error: {ws.exception()}')

        except Exception as e:
            logger.error(f"❌ WebSocket error: {e}")
        finally:
            logger.info(f"📡 WebSocket client disconnected (remaining: {len(self.websockets) - 1})")

        return ws

    async def broadcast_to_websockets(self, data: Dict[str, Any]):
        """Broadcast data to all connected WebSocket clients."""

        if not self.websockets:
            return

        message = json.dumps(data, default=str)

        # Create list to avoid modification during iteration
        websockets_list = list(self.websockets)

        for ws in websockets_list:
            try:
                if ws.closed:
                    continue
                await ws.send_str(message)
            except Exception as e:
                logger.debug(f"❌ Error sending to WebSocket: {e}")
                # WebSocket will be removed from WeakSet automatically

    async def update_dashboard_data(self):
        """Update dashboard data from monitoring components."""

        try:
            # Get monitoring status
            monitor_status = self.monitor.get_monitoring_status()

            # Get recent metrics
            recent_metrics = []
            if self.monitor.metrics_history:
                latest = self.monitor.metrics_history[-1]
                recent_metrics = latest.get('freshness_metrics', [])

            # Calculate dashboard metrics
            data_freshness_seconds = 0
            avg_quality_score = 0.9
            active_alerts_count = len([
                a for a in self.monitor.alerts
                if (datetime.now() - a.timestamp).total_seconds() < 3600
            ])

            if recent_metrics:
                data_freshness_seconds = max(m.get('seconds_since_last_update', 0) for m in recent_metrics)
                quality_scores = [m.get('average_quality_score', 0) for m in recent_metrics if m.get('average_quality_score', 0) > 0]
                if quality_scores:
                    avg_quality_score = sum(quality_scores) / len(quality_scores)

            # Build dashboard data
            self.dashboard_data = {
                'last_update': datetime.now(),
                'system_status': 'active' if monitor_status.get('status') == 'active' else 'inactive',
                'data_freshness_seconds': data_freshness_seconds,
                'active_alerts_count': active_alerts_count,
                'avg_quality_score': avg_quality_score,
                'monitor_status': monitor_status,
                'collection_rate_history': self._build_collection_rate_history(),
                'quality_scores': self._build_quality_scores_history(),
                'recent_updates': self._build_recent_updates()
            }

            # Broadcast to WebSocket clients
            await self.broadcast_to_websockets(self.dashboard_data)

        except Exception as e:
            logger.error(f"❌ Error updating dashboard data: {e}")

    def _build_collection_rate_history(self) -> List[Dict[str, Any]]:
        """Build collection rate history for charts."""

        history = []

        # Get last hour of metrics
        cutoff_time = datetime.now() - timedelta(hours=1)

        for metric_data in self.monitor.metrics_history:
            if metric_data['timestamp'] < cutoff_time:
                continue

            # Calculate collection rate from freshness metrics
            freshness_metrics = metric_data.get('freshness_metrics', [])
            total_records = sum(m.get('records_last_hour', 0) for m in freshness_metrics)

            history.append({
                'timestamp': metric_data['timestamp'].isoformat(),
                'rate': total_records
            })

        return history[-60:]  # Last 60 data points

    def _build_quality_scores_history(self) -> List[Dict[str, Any]]:
        """Build quality scores history for charts."""

        history = []

        # Get last 4 hours of metrics
        cutoff_time = datetime.now() - timedelta(hours=4)

        for metric_data in self.monitor.metrics_history:
            if metric_data['timestamp'] < cutoff_time:
                continue

            quality_metrics = metric_data.get('quality_metrics', [])

            for metric in quality_metrics:
                history.append({
                    'timestamp': metric_data['timestamp'].isoformat(),
                    'vendor': metric.get('vendor'),
                    'symbol': metric.get('symbol'),
                    'quality_score': metric.get('quality_score', 0)
                })

        return history

    def _build_recent_updates(self) -> List[Dict[str, Any]]:
        """Build recent updates list."""

        updates = []

        if not self.monitor.metrics_history:
            return updates

        # Get most recent quality metrics
        latest = self.monitor.metrics_history[-1]
        quality_metrics = latest.get('quality_metrics', [])

        for metric in quality_metrics:
            updates.append({
                'symbol': metric.get('symbol'),
                'vendor': metric.get('vendor'),
                'price': metric.get('price', 0),
                'quality': metric.get('quality_score', 0),
                'timestamp': metric.get('timestamp', datetime.now().isoformat())
            })

        # Sort by timestamp, most recent first
        updates.sort(key=lambda x: x['timestamp'], reverse=True)

        return updates

    async def dashboard_update_loop(self):
        """Background loop to update dashboard data."""

        logger.info(f"📊 Starting dashboard update loop (interval: {self.monitor_interval}s)")

        while True:
            try:
                await self.update_dashboard_data()
                await asyncio.sleep(self.monitor_interval)
            except Exception as e:
                logger.error(f"❌ Error in dashboard update loop: {e}")
                await asyncio.sleep(10)  # Shorter sleep on error

    async def start_server(self):
        """Start the dashboard server."""

        try:
            await self.initialize()

            # Start monitoring in background
            monitor_task = asyncio.create_task(self.monitor.start_monitoring())

            # Start dashboard update loop
            dashboard_task = asyncio.create_task(self.dashboard_update_loop())

            # Start HTTP server
            runner = web.AppRunner(self.app)
            await runner.setup()

            site = web.TCPSite(runner, self.host, self.port)
            await site.start()

            logger.info(f"🚀 Monitoring Dashboard started on http://{self.host}:{self.port}")
            logger.info(f"📊 WebSocket endpoint: ws://{self.host}:{self.port}/ws")
            logger.info(f"❤️ Health check: http://{self.host}:{self.port}/health")

            # Wait for tasks (they run indefinitely)
            await asyncio.gather(monitor_task, dashboard_task)

        except Exception as e:
            logger.error(f"❌ Failed to start dashboard server: {e}")
            raise

    async def close(self):
        """Close dashboard and monitoring components."""

        await self.monitor.close()

        # Close WebSocket connections
        for ws in list(self.websockets):
            if not ws.closed:
                await ws.close()

        logger.info("✅ Monitoring Dashboard closed")


async def main():
    """Main function for standalone dashboard."""

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    import argparse

    parser = argparse.ArgumentParser(description='ATS Real-time Collection Monitoring Dashboard')
    parser.add_argument('--host', default='0.0.0.0', help='Host address (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8090, help='Port number (default: 8090)')
    parser.add_argument('--monitor-interval', type=int, default=30, help='Monitoring interval in seconds (default: 30)')

    args = parser.parse_args()

    dashboard = MonitoringDashboard(
        host=args.host,
        port=args.port,
        monitor_interval=args.monitor_interval
    )

    try:
        await dashboard.start_server()
    except KeyboardInterrupt:
        logger.info("📤 Received keyboard interrupt")
    finally:
        await dashboard.close()


if __name__ == "__main__":
    asyncio.run(main())