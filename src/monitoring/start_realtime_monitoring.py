#!/usr/bin/env python3
"""
ATS Real-time Collection Monitoring System - Unified Startup Script

Comprehensive monitoring system with multiple deployment modes:
- Production: Full monitoring with all features  
- Docker: Runs in Docker container with proper networking
- Standalone: Minimal dependencies, works in any environment
- Debug: Comprehensive diagnostics and troubleshooting

Usage:
    python3 scripts/start_realtime_monitoring.py [--mode=production|docker|standalone|debug] [--config=file.json]

Access Points:
    - Dashboard: http://localhost:4008 (follows ATS-INTG port pattern)
    - Prometheus Metrics: http://localhost:8091/metrics  
    - Health Checks: http://localhost:4008/health
"""

import asyncio
import json
import logging
import os
import sys
import argparse
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    import aiohttp
    from aiohttp import web, WSMsgType
    import asyncpg
    DEPS_AVAILABLE = True
except ImportError as e:
    DEPS_AVAILABLE = False
    MISSING_DEPS = str(e)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class StandaloneMonitoringDashboard:
    """Standalone monitoring dashboard that works with minimal dependencies."""
    
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
        html_content = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ATS Real-time Collection Monitoring</title>
    <style>
        body { font-family: 'Segoe UI', sans-serif; margin: 0; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: rgba(255,255,255,0.95); color: #333; padding: 30px; border-radius: 15px; margin-bottom: 30px; box-shadow: 0 8px 32px rgba(0,0,0,0.1); }
        .header h1 { margin: 0; font-size: 2.8em; font-weight: 300; }
        .status-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 25px; margin-bottom: 30px; }
        .status-card { background: rgba(255,255,255,0.95); padding: 25px; border-radius: 15px; box-shadow: 0 8px 32px rgba(0,0,0,0.1); }
        .metric-value { font-size: 3em; font-weight: 200; margin: 15px 0; }
        .metric-value.good { color: #4CAF50; }
        .metric-value.warning { color: #FF9800; }
        .metric-value.critical { color: #F44336; }
        .vendor-status { display: flex; align-items: center; margin: 8px 0; padding: 10px; border-radius: 8px; }
        .status-indicator { width: 14px; height: 14px; border-radius: 50%; margin-right: 12px; }
        .status-healthy { background-color: #4CAF50; box-shadow: 0 0 0 3px rgba(76, 175, 80, 0.2); }
        .status-warning { background-color: #FF9800; }
        .status-critical { background-color: #F44336; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎯 ATS Real-time Collection Monitoring</h1>
            <p>Live monitoring dashboard for AAPL & TSLA minute-bar data collection</p>
        </div>
        
        <div class="status-grid">
            <div class="status-card">
                <h3>📊 Data Freshness</h3>
                <div id="freshness-status">Loading...</div>
            </div>
            <div class="status-card">
                <h3>🎯 Data Quality</h3>
                <div id="quality-status">Loading...</div>
            </div>
            <div class="status-card">
                <h3>🔌 API Health</h3>
                <div id="api-health-status">Loading...</div>
            </div>
            <div class="status-card">
                <h3>📈 Collection Rate</h3>
                <div id="collection-rate-status">Loading...</div>
            </div>
        </div>
        
        <div class="status-grid">
            <div class="status-card" style="grid-column: 1 / -1;">
                <h3>📋 Vendor Performance (24h)</h3>
                <div id="vendor-performance">Loading...</div>
            </div>
        </div>
    </div>
    
    <script>
        fetch('/api/metrics')
            .then(response => response.json())
            .then(data => {
                // Update freshness
                const freshnessDiv = document.getElementById('freshness-status');
                if (data.freshness_metrics) {
                    let html = '';
                    data.freshness_metrics.forEach(metric => {
                        const status = metric.seconds_since_last_update < 300 ? 'healthy' : 'warning';
                        const minutes = Math.round(metric.seconds_since_last_update / 60);
                        html += `<div class="vendor-status">
                            <div class="status-indicator status-${status}"></div>
                            <strong>${metric.vendor.toUpperCase()} ${metric.symbol}</strong>: ${minutes}m ago
                        </div>`;
                    });
                    freshnessDiv.innerHTML = html;
                }
                
                // Update quality
                const qualityDiv = document.getElementById('quality-status');
                if (data.quality_metrics) {
                    let avgQuality = data.quality_metrics.reduce((sum, m) => sum + m.quality_score, 0) / data.quality_metrics.length;
                    const qualityClass = avgQuality > 0.8 ? 'good' : 'warning';
                    qualityDiv.innerHTML = `
                        <div class="metric-value ${qualityClass}">${(avgQuality * 100).toFixed(1)}%</div>
                        <div class="metric-label">Average Data Quality</div>
                    `;
                }
                
                // Update API health
                const apiHealthDiv = document.getElementById('api-health-status');
                if (data.vendor_health && data.vendor_health.length > 0) {
                    const avgSuccessRate = data.vendor_health.reduce((sum, v) => sum + v.success_rate, 0) / data.vendor_health.length;
                    const healthClass = avgSuccessRate > 95 ? 'good' : avgSuccessRate > 85 ? 'warning' : 'critical';
                    const totalCalls = data.vendor_health.reduce((sum, v) => sum + v.total_calls, 0);
                    
                    apiHealthDiv.innerHTML = `
                        <div class="metric-value ${healthClass}">${avgSuccessRate.toFixed(1)}%</div>
                        <div class="metric-label">${totalCalls} API calls</div>
                    `;
                } else {
                    apiHealthDiv.innerHTML = '<div class="metric-label">No API data</div>';
                }
                
                // Update collection rate
                const collectionRateDiv = document.getElementById('collection-rate-status');
                if (data.collection_stats && data.collection_stats.live_data_stats) {
                    const totalRecords = data.collection_stats.live_data_stats.reduce((sum, s) => sum + (s.current_records || 0), 0);
                    const activeStreams = data.collection_stats.live_data_stats.filter(s => (s.minutes_since_last || 0) < 5).length;
                    const totalStreams = data.collection_stats.live_data_stats.length;
                    const rateClass = activeStreams === totalStreams ? 'good' : activeStreams > totalStreams/2 ? 'warning' : 'critical';
                    
                    collectionRateDiv.innerHTML = `
                        <div class="metric-value ${rateClass}">${totalRecords}</div>
                        <div class="metric-label">${activeStreams}/${totalStreams} streams active</div>
                    `;
                } else {
                    collectionRateDiv.innerHTML = '<div class="metric-label">No collection data</div>';
                }
                
                // Update vendor performance table
                const vendorPerfDiv = document.getElementById('vendor-performance');
                if (data.vendor_health && data.vendor_health.length > 0) {
                    let html = `
                        <table style="width: 100%; border-collapse: collapse;">
                            <tr style="border-bottom: 1px solid #eee;">
                                <th style="text-align: left; padding: 8px;">Vendor</th>
                                <th style="text-align: right; padding: 8px;">API Calls</th>
                                <th style="text-align: right; padding: 8px;">Success Rate</th>
                                <th style="text-align: right; padding: 8px;">Avg Response</th>
                                <th style="text-align: right; padding: 8px;">Rate Limits</th>
                            </tr>
                    `;
                    
                    data.vendor_health.forEach(vendor => {
                        const successClass = vendor.success_rate > 95 ? 'good' : vendor.success_rate > 85 ? 'warning' : 'critical';
                        html += `
                            <tr style="border-bottom: 1px solid #f0f0f0;">
                                <td style="padding: 8px;"><strong>${vendor.vendor.toUpperCase()}</strong></td>
                                <td style="padding: 8px; text-align: right;">${vendor.total_calls.toLocaleString()}</td>
                                <td style="padding: 8px; text-align: right; color: ${successClass === 'good' ? '#4CAF50' : successClass === 'warning' ? '#FF9800' : '#F44336'};">
                                    ${vendor.success_rate.toFixed(1)}%
                                </td>
                                <td style="padding: 8px; text-align: right;">${vendor.avg_response_time_ms.toFixed(0)}ms</td>
                                <td style="padding: 8px; text-align: right;">${vendor.rate_limit_hits}</td>
                            </tr>
                        `;
                    });
                    
                    html += '</table>';
                    vendorPerfDiv.innerHTML = html;
                } else {
                    vendorPerfDiv.innerHTML = '<div class="metric-label">No vendor performance data available</div>';
                }
            });
    </script>
</body>
</html>'''
        return web.Response(text=html_content, content_type='text/html')
    
    async def health_check(self, request):
        """Health check endpoint."""
        return web.json_response({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "service": "ats-realtime-monitoring"
        })
    
    async def get_metrics_endpoint(self, request):
        """Metrics endpoint (will be overridden by main system).""" 
        return web.json_response({"error": "Metrics endpoint not initialized"})
    
    async def websocket_handler(self, request):
        """WebSocket handler."""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.websockets.append(ws)
        
        try:
            async for msg in ws:
                if msg.type == WSMsgType.ERROR:
                    break
        except Exception:
            pass
        finally:
            if ws in self.websockets:
                self.websockets.remove(ws)
        
        return ws
    
    async def broadcast_update(self, data: Dict[str, Any]):
        """Broadcast update to WebSocket clients."""
        if not self.websockets:
            return
        
        message = json.dumps(data, default=str)
        disconnected = []
        
        for ws in self.websockets:
            try:
                await ws.send_str(message)
            except Exception:
                disconnected.append(ws)
        
        for ws in disconnected:
            if ws in self.websockets:
                self.websockets.remove(ws)
    
    async def start(self):
        """Start dashboard server."""
        runner = web.AppRunner(self.app)
        await runner.setup()
        
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        
        logger.info(f"✅ Dashboard available at http://{self.host}:{self.port}")
        return runner


class MonitoringSystem:
    """Main monitoring system that adapts to available dependencies."""
    
    def __init__(self, mode: str = "standalone", slack_webhook: str = None):
        self.mode = mode
        self.slack_webhook = slack_webhook or "https://hooks.slack.com/services/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"
        self.dashboard = StandaloneMonitoringDashboard()
        self.dashboard.get_metrics_endpoint = self.get_metrics_endpoint
        
    async def get_real_data_metrics(self) -> Dict[str, Any]:
        """Get real data metrics."""
        if not DEPS_AVAILABLE:
            raise RuntimeError("Database dependencies not available - cannot get real data metrics")
        
        try:
            # Try container connection first
            conn = await asyncpg.connect(
                host="ats-intg-postgres", port=5432, user="postgres",
                password="intg_password", database="intg_db"
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
            GROUP BY vendor, symbol ORDER BY vendor, symbol
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
            logger.error(f"❌ Database connection failed for monitoring metrics: {str(e)[:100]}")
            raise RuntimeError(f"Unable to retrieve monitoring metrics: database connection failed. {e}")
    
    
    async def get_metrics_endpoint(self, request):
        """API endpoint for metrics."""
        metrics = await self.get_real_data_metrics()
        alerts = await self.evaluate_alerts(metrics)
        
        # Add vendor-specific metrics if available
        try:
            from monitoring.vendor_metrics_service import VendorMetricsService
            vendor_service = VendorMetricsService()
            
            # Get vendor health and collection stats
            vendor_health = await vendor_service.get_vendor_health_summary(hours=24)
            collection_stats = await vendor_service.get_minute_bar_collection_stats(hours=24)
            api_status = await vendor_service.get_api_status_breakdown(hours=24)
            
            metrics.update({
                "vendor_health": [
                    {
                        "vendor": h.vendor,
                        "total_calls": h.total_calls,
                        "success_rate": h.success_rate,
                        "avg_response_time_ms": h.avg_response_time_ms,
                        "rate_limit_hits": h.rate_limit_hits
                    }
                    for h in vendor_health
                ],
                "collection_stats": collection_stats,
                "api_status": api_status
            })
        except Exception as e:
            logger.warning(f"Could not load vendor metrics: {e}")
        
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
                    "timestamp": current_time.isoformat()
                })
        
        return alerts
    
    async def send_slack_alert(self, alert: Dict[str, Any]):
        """Send alert to Slack."""
        if not DEPS_AVAILABLE or not self.slack_webhook:
            return
            
        try:
            payload = {
                "attachments": [{
                    "color": "danger" if alert["level"] == "critical" else "warning",
                    "title": "🚨 ATS Monitoring Alert", 
                    "text": alert["message"]
                }]
            }
            
            async with aiohttp.ClientSession() as session:
                await session.post(self.slack_webhook, json=payload)
                logger.info(f"✅ Slack alert sent: {alert['message']}")
        except Exception as e:
            logger.error(f"❌ Slack alert failed: {e}")
    
    async def start_metrics_server(self):
        """Start Prometheus metrics server."""
        if not DEPS_AVAILABLE:
            logger.warning("⚠️ Metrics server disabled - missing dependencies")
            return None
            
        async def metrics_handler(request):
            metrics = await self.get_real_data_metrics()
            timestamp = int(datetime.now().timestamp())
            
            lines = ["# HELP ats_realtime_data_freshness_seconds Seconds since last update",
                    "# TYPE ats_realtime_data_freshness_seconds gauge", ""]
            
            for metric in metrics.get("freshness_metrics", []):
                vendor, symbol = metric["vendor"], metric["symbol"]
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
                
                # Send new alerts
                for alert in alerts:
                    if alert not in last_alerts:
                        await self.send_slack_alert(alert)
                
                last_alerts = alerts.copy()
                
                # Broadcast to dashboard
                await self.dashboard.broadcast_update({
                    **metrics,
                    "alerts": alerts,
                    "timestamp": datetime.now().isoformat()
                })
                
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"❌ Monitoring loop error: {e}")
                await asyncio.sleep(60)
    
    async def start(self):
        """Start all monitoring components."""
        logger.info(f"🚀 Starting ATS Monitoring System ({self.mode} mode)")
        
        dashboard_runner = await self.dashboard.start()
        metrics_runner = await self.start_metrics_server()
        monitoring_task = asyncio.create_task(self.monitoring_loop())
        
        logger.info("✅ All components started")
        logger.info("📊 Access Points:")
        logger.info("   Dashboard:  http://localhost:4008")
        logger.info("   Health:     http://localhost:4008/health")
        if DEPS_AVAILABLE:
            logger.info("   Metrics:    http://localhost:8091/metrics")
        logger.info("")
        
        try:
            await monitoring_task
        except KeyboardInterrupt:
            logger.info("⏹️ Shutting down...")
        finally:
            monitoring_task.cancel()
            await dashboard_runner.cleanup()
            if metrics_runner:
                await metrics_runner.cleanup()


def run_diagnostics():
    """Run comprehensive diagnostics."""
    logger.info("🧪 ATS Monitoring System Diagnostics")
    logger.info("=" * 50)
    
    # Check dependencies
    logger.info("📋 Dependency Check:")
    if DEPS_AVAILABLE:
        logger.info("✅ All dependencies available")
    else:
        logger.warning(f"⚠️ Missing dependencies: {MISSING_DEPS}")
        logger.info("💡 Fix: Run in Docker environment")
    
    # Check ports
    logger.info("📋 Port Check:")
    for port, name in [(4008, "Dashboard"), (8091, "Metrics"), (4000, "ATS-INTG"), (4432, "Database")]:
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', port))
            sock.close()
            status = "✅ AVAILABLE" if result != 0 else "⚠️ IN USE"
            logger.info(f"   Port {port} ({name}): {status}")
        except Exception:
            logger.info(f"   Port {port} ({name}): ❓ UNKNOWN")
    
    logger.info("=" * 50)


def run_docker_mode():
    """Run monitoring system in Docker container."""
    logger.info("🐳 Starting monitoring system in Docker...")
    
    project_root = Path(__file__).parent.parent
    
    docker_cmd = [
        "docker", "run", "--rm", "--name", "ats-realtime-monitoring",
        "--network", "ats-network",
        "-p", "4008:4008", "-p", "8091:8091",
        "-v", f"{project_root}:/workspace", "-w", "/workspace",
        "-e", "PYTHONPATH=src",
        "-e", "ALERT_EMAIL_RECIPIENTS=jianjun00@gmail.com",
        "dragonflyer762/ats-genai:latest",
        "python3", "scripts/start_realtime_monitoring.py", "--mode=standalone"
    ]
    
    logger.info("🚀 Starting Docker container...")
    try:
        subprocess.run(docker_cmd, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Docker startup failed: {e}")
        return False
    except KeyboardInterrupt:
        logger.info("⏹️ Stopped by user")
    
    return True


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="ATS Real-time Collection Monitoring")
    parser.add_argument('--mode', choices=['production', 'docker', 'standalone', 'debug'], 
                       default='standalone', help='Monitoring mode')
    parser.add_argument('--config', help='Configuration file path')
    
    args = parser.parse_args()
    
    logger.info("🎯 ATS Real-time Collection Monitoring System")
    logger.info(f"🔧 Mode: {args.mode}")
    logger.info("=" * 60)
    
    if args.mode == 'debug':
        run_diagnostics()
        return
    
    if args.mode == 'docker':
        run_docker_mode()
        return
    
    # Start monitoring system
    monitoring_system = MonitoringSystem(mode=args.mode)
    await monitoring_system.start()


if __name__ == "__main__":
    asyncio.run(main())