#!/usr/bin/env python3
"""
ATS Real-time Collection Monitoring - Standalone Working Version

This is a completely self-contained monitoring system that works with
only the basic dependencies available in the Docker environment.

Features:
- Live web dashboard with real-time updates
- Health monitoring and data freshness tracking
- Slack notifications using simple HTTP requests
- Prometheus metrics endpoint
- Real database integration with fallback to mock data

Usage:
    python3 scripts/start_standalone_monitoring.py
"""

import asyncio
import logging
import sys
import os
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

try:
    import aiohttp
    from aiohttp import web, WSMsgType
    import asyncpg
except ImportError as e:
    print(f"❌ Required dependency missing: {e}")
    print("🔧 This script must be run in the Docker environment")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StandaloneMonitoringDashboard:
    """Standalone monitoring dashboard."""
    
    def __init__(self, host: str = "0.0.0.0", port: int = 4008):
        self.host = host
        self.port = port
        self.app = web.Application()
        self.websockets: List[web.WebSocketResponse] = []
        self.setup_routes()
        
    def setup_routes(self):
        """Setup HTTP routes."""
        self.app.router.add_get('/', self.dashboard)
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/ws', self.websocket_handler)
        self.app.router.add_get('/api/metrics', self.get_metrics_endpoint)
        
    async def dashboard(self, request):
        """Main dashboard page."""
        html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ATS Real-time Collection Monitoring</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: rgba(255,255,255,0.95); color: #333; padding: 30px; border-radius: 15px; margin-bottom: 30px; box-shadow: 0 8px 32px rgba(0,0,0,0.1); }
        .header h1 { margin: 0; font-size: 2.8em; font-weight: 300; }
        .header p { margin: 10px 0 0 0; font-size: 1.2em; opacity: 0.7; }
        .status-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 25px; margin-bottom: 30px; }
        .status-card { background: rgba(255,255,255,0.95); padding: 25px; border-radius: 15px; box-shadow: 0 8px 32px rgba(0,0,0,0.1); backdrop-filter: blur(10px); }
        .status-card h3 { margin-top: 0; color: #333; font-size: 1.4em; font-weight: 500; }
        .metric-value { font-size: 3em; font-weight: 200; margin: 15px 0; }
        .metric-value.good { color: #4CAF50; }
        .metric-value.warning { color: #FF9800; }
        .metric-value.critical { color: #F44336; }
        .metric-label { color: #666; font-size: 1em; }
        .alerts-section { background: rgba(255,255,255,0.95); padding: 25px; border-radius: 15px; box-shadow: 0 8px 32px rgba(0,0,0,0.1); }
        .alert-item { padding: 20px; margin: 15px 0; border-radius: 10px; border-left: 5px solid; }
        .alert-critical { background: linear-gradient(90deg, #ffebee 0%, #fce4ec 100%); border-left-color: #f44336; }
        .alert-warning { background: linear-gradient(90deg, #fff3e0 0%, #fce4ec 100%); border-left-color: #ff9800; }
        .alert-info { background: linear-gradient(90deg, #e3f2fd 0%, #e1f5fe 100%); border-left-color: #2196f3; }
        .timestamp { color: #666; font-size: 0.9em; float: right; }
        .loading { text-align: center; padding: 40px; color: #666; font-size: 1.1em; }
        .vendor-status { display: flex; align-items: center; margin: 8px 0; padding: 10px; border-radius: 8px; background: rgba(0,0,0,0.02); }
        .status-indicator { width: 14px; height: 14px; border-radius: 50%; margin-right: 12px; }
        .status-healthy { background-color: #4CAF50; box-shadow: 0 0 0 3px rgba(76, 175, 80, 0.2); }
        .status-warning { background-color: #FF9800; box-shadow: 0 0 0 3px rgba(255, 152, 0, 0.2); }
        .status-critical { background-color: #F44336; box-shadow: 0 0 0 3px rgba(244, 67, 54, 0.2); }
        .data-source-badge { display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 0.8em; font-weight: 500; margin-left: 10px; }
        .badge-real { background: #4CAF50; color: white; }
        .badge-mock { background: #FF9800; color: white; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎯 ATS Real-time Collection Monitoring</h1>
            <p>Live monitoring dashboard for AAPL & TSLA minute-bar data collection</p>
            <span id="data-source-badge" class="data-source-badge badge-mock">Mock Data</span>
        </div>
        
        <div class="status-grid">
            <div class="status-card">
                <h3>📊 Data Freshness</h3>
                <div id="freshness-status" class="loading">Loading...</div>
            </div>
            
            <div class="status-card">
                <h3>🎯 Data Quality</h3>
                <div id="quality-status" class="loading">Loading...</div>
            </div>
            
            <div class="status-card">
                <h3>🔄 Collection Rate</h3>
                <div id="collection-status" class="loading">Loading...</div>
            </div>
            
            <div class="status-card">
                <h3>⚡ System Health</h3>
                <div id="system-status" class="loading">Loading...</div>
            </div>
        </div>
        
        <div class="alerts-section">
            <h3>🚨 Active Alerts</h3>
            <div id="alerts-container" class="loading">Loading alerts...</div>
        </div>
    </div>
    
    <script>
        let ws;
        let reconnectInterval;
        
        function connect() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
            
            ws.onopen = function() {
                console.log('WebSocket connected');
                clearInterval(reconnectInterval);
            };
            
            ws.onmessage = function(event) {
                const data = JSON.parse(event.data);
                updateDashboard(data);
            };
            
            ws.onclose = function() {
                console.log('WebSocket disconnected, attempting to reconnect...');
                reconnectInterval = setInterval(connect, 5000);
            };
        }
        
        function updateDashboard(data) {
            // Update data source badge
            const badge = document.getElementById('data-source-badge');
            if (data.data_source === 'real') {
                badge.textContent = 'Live Data';
                badge.className = 'data-source-badge badge-real';
            } else {
                badge.textContent = 'Mock Data';
                badge.className = 'data-source-badge badge-mock';
            }
            
            // Update freshness status
            const freshnessDiv = document.getElementById('freshness-status');
            if (data.freshness_metrics) {
                let html = '';
                data.freshness_metrics.forEach(metric => {
                    const status = metric.seconds_since_last_update < 300 ? 'healthy' : 
                                 metric.seconds_since_last_update < 900 ? 'warning' : 'critical';
                    const minutes = Math.round(metric.seconds_since_last_update / 60);
                    html += `<div class="vendor-status">
                        <div class="status-indicator status-${status}"></div>
                        <strong>${metric.vendor.toUpperCase()} ${metric.symbol}</strong>: ${minutes}m ago
                        <small style="margin-left: auto; opacity: 0.7;">${metric.records_last_hour || 0} records/hr</small>
                    </div>`;
                });
                freshnessDiv.innerHTML = html || '<p>No data available</p>';
            }
            
            // Update quality status
            const qualityDiv = document.getElementById('quality-status');
            if (data.quality_metrics) {
                let avgQuality = 0;
                let count = 0;
                data.quality_metrics.forEach(metric => {
                    avgQuality += metric.quality_score;
                    count++;
                });
                avgQuality = count > 0 ? avgQuality / count : 0;
                const qualityClass = avgQuality > 0.8 ? 'good' : avgQuality > 0.6 ? 'warning' : 'critical';
                qualityDiv.innerHTML = `
                    <div class="metric-value ${qualityClass}">${(avgQuality * 100).toFixed(1)}%</div>
                    <div class="metric-label">Average Data Quality</div>
                `;
            }
            
            // Update alerts
            const alertsDiv = document.getElementById('alerts-container');
            if (data.alerts && data.alerts.length > 0) {
                let html = '';
                data.alerts.forEach(alert => {
                    const alertClass = alert.level === 'critical' ? 'alert-critical' :
                                     alert.level === 'warning' ? 'alert-warning' : 'alert-info';
                    const timestamp = new Date(alert.timestamp).toLocaleTimeString();
                    html += `<div class="alert-item ${alertClass}">
                        <strong>${alert.level.toUpperCase()}: ${alert.message}</strong>
                        <div class="timestamp">${timestamp}</div>
                    </div>`;
                });
                alertsDiv.innerHTML = html;
            } else {
                alertsDiv.innerHTML = '<p style="color: #4CAF50; text-align: center; padding: 30px; font-size: 1.1em;">✅ No active alerts - all systems normal</p>';
            }
            
            // Update system status
            const systemDiv = document.getElementById('system-status');
            const alertCount = data.alerts ? data.alerts.length : 0;
            const systemClass = alertCount === 0 ? 'good' : alertCount < 3 ? 'warning' : 'critical';
            systemDiv.innerHTML = `
                <div class="metric-value ${systemClass}">${alertCount === 0 ? '✅' : '⚠️'}</div>
                <div class="metric-label">${alertCount} Active Alerts</div>
            `;
            
            // Update collection rate
            const collectionDiv = document.getElementById('collection-status');
            if (data.freshness_metrics) {
                const recentData = data.freshness_metrics.filter(m => m.seconds_since_last_update < 300).length;
                const totalStreams = data.freshness_metrics.length;
                const rate = totalStreams > 0 ? (recentData / totalStreams) * 100 : 0;
                const rateClass = rate > 80 ? 'good' : rate > 50 ? 'warning' : 'critical';
                collectionDiv.innerHTML = `
                    <div class="metric-value ${rateClass}">${rate.toFixed(0)}%</div>
                    <div class="metric-label">${recentData}/${totalStreams} streams active</div>
                `;
            }
        }
        
        // Load initial data
        fetch('/api/metrics')
            .then(response => response.json())
            .then(data => updateDashboard(data))
            .catch(error => console.error('Error loading initial data:', error));
        
        // Connect WebSocket
        connect();
        
        // Refresh data every 30 seconds as fallback
        setInterval(() => {
            fetch('/api/metrics')
                .then(response => response.json())
                .then(data => updateDashboard(data))
                .catch(error => console.error('Error refreshing data:', error));
        }, 30000);
    </script>
</body>
</html>"""
        return web.Response(text=html_content, content_type='text/html')
    
    async def health_check(self, request):
        """Health check endpoint."""
        return web.json_response({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "service": "ats-realtime-monitoring",
            "version": "standalone-v1"
        })
    
    async def get_metrics_endpoint(self, request):
        """Metrics endpoint for dashboard."""
        # This will be overridden by the main system
        return web.json_response({"error": "Not implemented in standalone dashboard"})
    
    async def websocket_handler(self, request):
        """WebSocket handler for live updates."""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        self.websockets.append(ws)
        logger.info(f"WebSocket client connected. Total: {len(self.websockets)}")
        
        try:
            async for msg in ws:
                if msg.type == WSMsgType.ERROR:
                    logger.error(f'WebSocket error: {ws.exception()}')
                    break
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            if ws in self.websockets:
                self.websockets.remove(ws)
            logger.info(f"WebSocket client disconnected. Total: {len(self.websockets)}")
        
        return ws
    
    async def broadcast_update(self, data: Dict[str, Any]):
        """Broadcast update to all WebSocket clients."""
        if not self.websockets:
            return
        
        message = json.dumps(data, default=str)
        disconnected = []
        
        for ws in self.websockets:
            try:
                await ws.send_str(message)
            except Exception as e:
                logger.error(f"Error sending WebSocket message: {e}")
                disconnected.append(ws)
        
        for ws in disconnected:
            if ws in self.websockets:
                self.websockets.remove(ws)
    
    async def start(self):
        """Start the dashboard server."""
        logger.info(f"🚀 Starting dashboard on {self.host}:{self.port}")
        
        runner = web.AppRunner(self.app)
        await runner.setup()
        
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        
        logger.info(f"✅ Dashboard available at http://{self.host}:{self.port}")
        return runner


class StandaloneMonitoringSystem:
    """Standalone monitoring system."""
    
    def __init__(self, slack_webhook: str = None):
        self.dashboard = StandaloneMonitoringDashboard()
        self.slack_webhook = slack_webhook
        self.metrics_server = None
        
        # Override dashboard metrics endpoint
        self.dashboard.get_metrics_endpoint = self.get_metrics_endpoint
        
    async def get_real_data_metrics(self) -> Dict[str, Any]:
        """Get real data metrics."""
        try:
            # Try container connection first
            conn = await asyncpg.connect(
                host="ats-intg-postgres",
                port=5432,
                user="postgres",
                password="intg_password", 
                database="intg_db"
            )
            
            query = """
            SELECT vendor, symbol, 
                   EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) as seconds_old,
                   ROUND(AVG(quality_score), 3) as avg_quality,
                   COUNT(*) as records_last_hour
            FROM (
                SELECT 'Tiingo' as vendor, symbol, timestamp, quality_score 
                FROM intg_one_minute_live_tiingo 
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
                UNION ALL 
                SELECT 'Polygon' as vendor, symbol, timestamp, quality_score 
                FROM intg_one_minute_live_polygon 
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
            ) combined 
            GROUP BY vendor, symbol 
            ORDER BY vendor, symbol
            """
            
            rows = await conn.fetch(query)
            await conn.close()
            
            freshness_metrics = []
            for row in rows:
                freshness_metrics.append({
                    "vendor": row["vendor"].lower(),
                    "symbol": row["symbol"],
                    "seconds_since_last_update": int(row["seconds_old"] or 0),
                    "quality_score": float(row["avg_quality"] or 0.0),
                    "records_last_hour": int(row["records_last_hour"] or 0)
                })
                
            # Group quality by vendor
            vendor_quality = {}
            for metric in freshness_metrics:
                vendor = metric["vendor"]
                if vendor not in vendor_quality:
                    vendor_quality[vendor] = []
                vendor_quality[vendor].append(metric["quality_score"])
            
            quality_metrics = []
            for vendor, scores in vendor_quality.items():
                if scores:
                    quality_metrics.append({
                        "vendor": vendor,
                        "quality_score": sum(scores) / len(scores)
                    })
            
            logger.info(f"📊 Real data: {len(freshness_metrics)} streams")
            return {
                "freshness_metrics": freshness_metrics,
                "quality_metrics": quality_metrics,
                "data_source": "real"
            }
            
        except Exception as e:
            logger.info(f"📊 Using mock data: {str(e)[:50]}...")
            return {
                "freshness_metrics": [
                    {"vendor": "tiingo", "symbol": "AAPL", "seconds_since_last_update": 45, "quality_score": 0.92, "records_last_hour": 58},
                    {"vendor": "tiingo", "symbol": "TSLA", "seconds_since_last_update": 120, "quality_score": 0.89, "records_last_hour": 55},
                    {"vendor": "polygon", "symbol": "AAPL", "seconds_since_last_update": 30, "quality_score": 0.94, "records_last_hour": 60},
                    {"vendor": "polygon", "symbol": "TSLA", "seconds_since_last_update": 350, "quality_score": 0.87, "records_last_hour": 52}
                ],
                "quality_metrics": [
                    {"vendor": "tiingo", "quality_score": 0.905},
                    {"vendor": "polygon", "quality_score": 0.905}
                ],
                "data_source": "mock"
            }
    
    async def get_metrics_endpoint(self, request):
        """Metrics endpoint."""
        metrics = await self.get_real_data_metrics()
        alerts = await self.evaluate_alerts(metrics)
        
        return web.json_response({
            **metrics,
            "alerts": alerts,
            "timestamp": datetime.now().isoformat()
        })
    
    async def evaluate_alerts(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Evaluate and generate alerts."""
        alerts = []
        current_time = datetime.now()
        
        for metric in metrics.get("freshness_metrics", []):
            seconds_old = metric["seconds_since_last_update"]
            if seconds_old > 300:  # 5 minutes
                level = "warning" if seconds_old < 900 else "critical"
                alerts.append({
                    "level": level,
                    "category": "data_freshness", 
                    "message": f"Stale data for {metric['vendor']} {metric['symbol']} ({seconds_old//60}m old)",
                    "timestamp": current_time.isoformat(),
                    "details": metric
                })
        
        for metric in metrics.get("quality_metrics", []):
            quality = metric["quality_score"]
            if quality < 0.7:
                level = "warning" if quality > 0.5 else "critical"
                alerts.append({
                    "level": level,
                    "category": "data_quality",
                    "message": f"Poor quality for {metric['vendor']} ({quality:.1%})",
                    "timestamp": current_time.isoformat(),
                    "details": metric
                })
        
        return alerts
    
    async def send_slack_alert(self, alert: Dict[str, Any]):
        """Send alert to Slack."""
        if not self.slack_webhook:
            return
            
        try:
            color_map = {"critical": "danger", "warning": "warning", "info": "good"}
            color = color_map.get(alert["level"], "good")
            
            payload = {
                "attachments": [{
                    "color": color,
                    "title": f"🚨 ATS Monitoring Alert",
                    "text": alert["message"],
                    "fields": [
                        {"title": "Level", "value": alert["level"].upper(), "short": True},
                        {"title": "Category", "value": alert["category"], "short": True},
                        {"title": "Time", "value": alert["timestamp"], "short": False}
                    ]
                }]
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(self.slack_webhook, json=payload) as response:
                    if response.status == 200:
                        logger.info(f"✅ Slack alert sent: {alert['message']}")
                    else:
                        logger.error(f"❌ Slack alert failed: {response.status}")
                        
        except Exception as e:
            logger.error(f"❌ Slack alert error: {e}")
    
    async def start_metrics_server(self):
        """Start Prometheus metrics server."""
        async def metrics_handler(request):
            metrics = await self.get_real_data_metrics()
            timestamp = int(datetime.now().timestamp())
            
            lines = [
                "# HELP ats_realtime_data_freshness_seconds Seconds since last update",
                "# TYPE ats_realtime_data_freshness_seconds gauge",
                "",
                "# HELP ats_realtime_quality_score Data quality score (0-1)",
                "# TYPE ats_realtime_quality_score gauge",
                ""
            ]
            
            for metric in metrics.get("freshness_metrics", []):
                vendor = metric["vendor"]
                symbol = metric["symbol"]
                lines.append(f'ats_realtime_data_freshness_seconds{{vendor="{vendor}",symbol="{symbol}"}} {metric["seconds_since_last_update"]} {timestamp}')
                lines.append(f'ats_realtime_quality_score{{vendor="{vendor}",symbol="{symbol}"}} {metric["quality_score"]} {timestamp}')
            
            return web.Response(text='\\n'.join(lines) + '\\n', content_type='text/plain')
        
        app = web.Application()
        app.router.add_get('/metrics', metrics_handler)
        
        runner = web.AppRunner(app)
        await runner.setup()
        
        site = web.TCPSite(runner, "0.0.0.0", 8091)
        await site.start()
        
        logger.info("📈 Prometheus metrics on port 8091")
        return runner
    
    async def monitoring_loop(self):
        """Main monitoring loop."""
        logger.info("🔄 Starting monitoring loop...")
        
        last_alerts = []
        
        while True:
            try:
                metrics = await self.get_real_data_metrics()
                alerts = await self.evaluate_alerts(metrics)
                
                # Send new alerts to Slack
                for alert in alerts:
                    if alert not in last_alerts:
                        await self.send_slack_alert(alert)
                
                last_alerts = alerts.copy()
                
                # Broadcast to dashboard
                dashboard_data = {
                    **metrics,
                    "alerts": alerts,
                    "timestamp": datetime.now().isoformat()
                }
                
                await self.dashboard.broadcast_update(dashboard_data)
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"❌ Monitoring loop error: {e}")
                await asyncio.sleep(60)
    
    async def start(self):
        """Start all components."""
        logger.info("🚀 Starting ATS Monitoring System")
        
        dashboard_runner = await self.dashboard.start()
        metrics_runner = await self.start_metrics_server()
        monitoring_task = asyncio.create_task(self.monitoring_loop())
        
        logger.info("✅ All components started")
        logger.info("")
        logger.info("📊 Access Points:")
        logger.info("   Dashboard:  http://localhost:4008")
        logger.info("   Health:     http://localhost:4008/health")
        logger.info("   Metrics:    http://localhost:8091/metrics")
        logger.info("")
        
        try:
            await monitoring_task
        except KeyboardInterrupt:
            logger.info("⏹️ Shutting down...")
        finally:
            monitoring_task.cancel()
            await dashboard_runner.cleanup()
            await metrics_runner.cleanup()


async def main():
    """Main function."""
    logger.info("🎯 ATS Real-time Collection Monitoring")
    logger.info("🔧 Standalone Working Version")
    logger.info("=" * 60)
    
    # Get Slack webhook from config
    slack_webhook = "https://hooks.slack.com/services/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"
    
    monitoring_system = StandaloneMonitoringSystem(slack_webhook=slack_webhook)
    await monitoring_system.start()


if __name__ == "__main__":
    asyncio.run(main())