#!/usr/bin/env python3
"""
Simplified ATS Real-time Collection Monitoring Dashboard

This version works with the dependencies available in the Docker image
and provides a functional monitoring interface without aiohttp_jinja2.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import aiohttp
from aiohttp import web, WSMsgType
import jinja2

logger = logging.getLogger(__name__)


class SimpleMonitoringDashboard:
    """Simplified monitoring dashboard using basic HTML templates."""
    
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
        self.app.router.add_get('/api/metrics', self.get_metrics)
        
    async def dashboard(self, request):
        """Main dashboard page."""
        html_content = self.get_dashboard_html()
        return web.Response(text=html_content, content_type='text/html')
    
    def get_dashboard_html(self) -> str:
        """Generate dashboard HTML using simple string templating."""
        return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ATS Real-time Collection Monitoring</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; }
        .header h1 { margin: 0; font-size: 2.5em; }
        .header p { margin: 10px 0 0 0; font-size: 1.1em; opacity: 0.9; }
        .status-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .status-card { background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .status-card h3 { margin-top: 0; color: #333; font-size: 1.3em; }
        .metric-value { font-size: 2.5em; font-weight: bold; margin: 10px 0; }
        .metric-value.good { color: #4CAF50; }
        .metric-value.warning { color: #FF9800; }
        .metric-value.critical { color: #F44336; }
        .metric-label { color: #666; font-size: 0.9em; }
        .alerts-section { background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .alert-item { padding: 15px; margin: 10px 0; border-radius: 5px; border-left: 5px solid; }
        .alert-critical { background-color: #ffebee; border-left-color: #f44336; }
        .alert-warning { background-color: #fff3e0; border-left-color: #ff9800; }
        .alert-info { background-color: #e3f2fd; border-left-color: #2196f3; }
        .timestamp { color: #666; font-size: 0.8em; float: right; }
        .loading { text-align: center; padding: 40px; color: #666; }
        .vendor-status { display: flex; align-items: center; margin: 5px 0; }
        .status-indicator { width: 12px; height: 12px; border-radius: 50%; margin-right: 8px; }
        .status-healthy { background-color: #4CAF50; }
        .status-warning { background-color: #FF9800; }
        .status-critical { background-color: #F44336; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🎯 ATS Real-time Collection Monitoring</h1>
        <p>Live monitoring dashboard for AAPL & TSLA minute-bar data collection</p>
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
            
            ws.onerror = function(error) {
                console.error('WebSocket error:', error);
            };
        }
        
        function updateDashboard(data) {
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
                alertsDiv.innerHTML = '<p style="color: #4CAF50; text-align: center; padding: 20px;">✅ No active alerts - all systems normal</p>';
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
    
    async def health_check(self, request):
        """Health check endpoint."""
        return web.json_response({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "service": "ats-realtime-monitoring"
        })
    
    async def get_metrics(self, request):
        """Get current metrics (mock data for now)."""
        # Generate mock data for demonstration
        current_time = datetime.now()
        
        mock_data = {
            "freshness_metrics": [
                {
                    "vendor": "tiingo",
                    "symbol": "AAPL",
                    "seconds_since_last_update": 45,
                    "quality_score": 0.92
                },
                {
                    "vendor": "tiingo", 
                    "symbol": "TSLA",
                    "seconds_since_last_update": 120,
                    "quality_score": 0.89
                },
                {
                    "vendor": "polygon",
                    "symbol": "AAPL", 
                    "seconds_since_last_update": 30,
                    "quality_score": 0.94
                },
                {
                    "vendor": "polygon",
                    "symbol": "TSLA",
                    "seconds_since_last_update": 180,
                    "quality_score": 0.87
                }
            ],
            "quality_metrics": [
                {"vendor": "tiingo", "quality_score": 0.905},
                {"vendor": "polygon", "quality_score": 0.905}
            ],
            "alerts": [],
            "timestamp": current_time.isoformat()
        }
        
        # Add a sample alert if data is stale
        if any(m["seconds_since_last_update"] > 300 for m in mock_data["freshness_metrics"]):
            mock_data["alerts"].append({
                "level": "warning",
                "message": "Some data streams are stale",
                "timestamp": current_time.isoformat()
            })
        
        return web.json_response(mock_data)
    
    async def websocket_handler(self, request):
        """WebSocket handler for live updates."""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        self.websockets.append(ws)
        logger.info(f"WebSocket client connected. Total clients: {len(self.websockets)}")
        
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
            logger.info(f"WebSocket client disconnected. Total clients: {len(self.websockets)}")
        
        return ws
    
    async def broadcast_update(self, data: Dict[str, Any]):
        """Broadcast update to all connected WebSocket clients."""
        if not self.websockets:
            return
        
        message = json.dumps(data)
        disconnected = []
        
        for ws in self.websockets:
            try:
                await ws.send_str(message)
            except Exception as e:
                logger.error(f"Error sending WebSocket message: {e}")
                disconnected.append(ws)
        
        # Clean up disconnected clients
        for ws in disconnected:
            if ws in self.websockets:
                self.websockets.remove(ws)
    
    async def start(self):
        """Start the dashboard server."""
        logger.info(f"🚀 Starting monitoring dashboard on {self.host}:{self.port}")
        
        runner = web.AppRunner(self.app)
        await runner.setup()
        
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        
        logger.info(f"✅ Dashboard available at http://{self.host}:{self.port}")
        return runner


if __name__ == "__main__":
    async def main():
        dashboard = SimpleMonitoringDashboard()
        runner = await dashboard.start()
        
        try:
            await asyncio.Event().wait()  # Keep running
        except KeyboardInterrupt:
            logger.info("Shutting down dashboard...")
        finally:
            await runner.cleanup()
    
    asyncio.run(main())