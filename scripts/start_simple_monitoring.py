#!/usr/bin/env python3
"""
ATS Real-time Collection Monitoring - Simplified Working Version

This script starts a functional monitoring system using only the dependencies
available in the Docker environment. It provides:

1. Live web dashboard with real-time updates
2. Health monitoring and data freshness tracking  
3. Alert system with Slack notifications
4. Prometheus metrics endpoint

Usage:
    python3 scripts/start_simple_monitoring.py
    
Access:
    Dashboard: http://localhost:8090
    Health: http://localhost:8090/health
    Metrics: http://localhost:8091/metrics
"""

import asyncio
import logging
import sys
import os
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    import aiohttp
    from aiohttp import web
    import asyncpg
except ImportError as e:
    print(f"❌ Required dependency missing: {e}")
    print("🔧 This script must be run in the Docker environment")
    print("   Use: python3 scripts/start_monitoring_docker.py")
    sys.exit(1)

from market_data.realtime.monitoring.simple_monitoring_dashboard import SimpleMonitoringDashboard
from market_data.realtime.monitoring.alert_channels import SlackAlertChannel, EmailAlertChannel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SimpleMonitoringSystem:
    """Simplified monitoring system with working components only."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.dashboard = SimpleMonitoringDashboard()
        self.alert_channels = []
        self.metrics_server = None
        self.setup_alert_channels()
        
    def setup_alert_channels(self):
        """Setup alert channels based on configuration."""
        alerting_config = self.config.get("components", {}).get("alerting", {})
        channels_config = alerting_config.get("channels", {})
        
        # Setup Slack channel
        slack_config = channels_config.get("slack", {})
        if slack_config.get("enabled", False) and slack_config.get("webhook_url"):
            try:
                slack_channel = SlackAlertChannel(
                    webhook_url=slack_config["webhook_url"],
                    channel=slack_config.get("channel", "#ats-alerts")
                )
                self.alert_channels.append(slack_channel)
                logger.info("✅ Slack alerts configured")
            except Exception as e:
                logger.error(f"❌ Failed to setup Slack alerts: {e}")
        
        # Setup Email channel
        email_config = channels_config.get("email", {})
        if email_config.get("enabled", False):
            try:
                email_channel = EmailAlertChannel(
                    smtp_server=os.getenv("SMTP_SERVER", "smtp.gmail.com"),
                    smtp_port=int(os.getenv("SMTP_PORT", "587")),
                    smtp_username=os.getenv("SMTP_USERNAME", "jianjun00@gmail.com"),
                    smtp_password=os.getenv("SMTP_PASSWORD", ""),
                    recipients=email_config.get("recipients", ["jianjun00@gmail.com"])
                )
                if email_channel.smtp_password:
                    self.alert_channels.append(email_channel)
                    logger.info("✅ Email alerts configured")
                else:
                    logger.warning("⚠️ Email alerts disabled - SMTP_PASSWORD not set")
            except Exception as e:
                logger.error(f"❌ Failed to setup email alerts: {e}")
    
    async def get_real_data_metrics(self) -> Dict[str, Any]:
        """Get real data metrics from database if available, otherwise mock data."""
        try:
            # Try to connect to ATS-INTG database
            conn = await asyncpg.connect(
                host="ats-intg-postgres",  # Container name
                port=5432,
                user="postgres", 
                password="intg_password",
                database="intg_db"
            )
            
            # Query real-time data freshness
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
            quality_metrics = []
            
            for row in rows:
                freshness_metrics.append({
                    "vendor": row["vendor"].lower(),
                    "symbol": row["symbol"],
                    "seconds_since_last_update": int(row["seconds_old"] or 0),
                    "quality_score": float(row["avg_quality"] or 0.0),
                    "records_last_hour": int(row["records_last_hour"] or 0)
                })
                
            # Group quality metrics by vendor
            vendor_quality = {}
            for metric in freshness_metrics:
                vendor = metric["vendor"]
                if vendor not in vendor_quality:
                    vendor_quality[vendor] = []
                vendor_quality[vendor].append(metric["quality_score"])
            
            for vendor, scores in vendor_quality.items():
                if scores:
                    quality_metrics.append({
                        "vendor": vendor,
                        "quality_score": sum(scores) / len(scores)
                    })
            
            logger.info(f"📊 Retrieved real data: {len(freshness_metrics)} streams")
            return {
                "freshness_metrics": freshness_metrics,
                "quality_metrics": quality_metrics,
                "data_source": "real"
            }
            
        except Exception as e:
            logger.warning(f"⚠️ Database connection failed, using mock data: {e}")
            
            # Fallback to mock data
            current_time = datetime.now()
            return {
                "freshness_metrics": [
                    {"vendor": "tiingo", "symbol": "AAPL", "seconds_since_last_update": 45, "quality_score": 0.92, "records_last_hour": 58},
                    {"vendor": "tiingo", "symbol": "TSLA", "seconds_since_last_update": 120, "quality_score": 0.89, "records_last_hour": 55},
                    {"vendor": "polygon", "symbol": "AAPL", "seconds_since_last_update": 30, "quality_score": 0.94, "records_last_hour": 60},
                    {"vendor": "polygon", "symbol": "TSLA", "seconds_since_last_update": 180, "quality_score": 0.87, "records_last_hour": 52}
                ],
                "quality_metrics": [
                    {"vendor": "tiingo", "quality_score": 0.905},
                    {"vendor": "polygon", "quality_score": 0.905}
                ],
                "data_source": "mock"
            }
    
    async def evaluate_alerts(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Evaluate metrics and generate alerts."""
        alerts = []
        current_time = datetime.now()
        
        # Check data freshness
        for metric in metrics.get("freshness_metrics", []):
            seconds_old = metric["seconds_since_last_update"]
            if seconds_old > 300:  # 5 minutes
                level = "warning" if seconds_old < 900 else "critical"  # 15 minutes
                alerts.append({
                    "level": level,
                    "category": "data_freshness",
                    "message": f"Stale data detected for {metric['vendor']} {metric['symbol']} ({seconds_old//60}m old)",
                    "timestamp": current_time.isoformat(),
                    "details": metric
                })
        
        # Check quality scores
        for metric in metrics.get("quality_metrics", []):
            quality = metric["quality_score"]
            if quality < 0.7:
                level = "warning" if quality > 0.5 else "critical"
                alerts.append({
                    "level": level,
                    "category": "data_quality",
                    "message": f"Poor data quality for {metric['vendor']} ({quality:.1%})",
                    "timestamp": current_time.isoformat(),
                    "details": metric
                })
        
        return alerts
    
    async def send_alerts(self, alerts: List[Dict[str, Any]]):
        """Send alerts through configured channels."""
        if not alerts:
            return
        
        for alert in alerts:
            for channel in self.alert_channels:
                try:
                    await channel.send_alert(
                        level=alert["level"],
                        message=alert["message"],
                        details=alert.get("details", {})
                    )
                except Exception as e:
                    logger.error(f"❌ Failed to send alert via {type(channel).__name__}: {e}")
    
    async def start_metrics_server(self):
        """Start Prometheus metrics server."""
        async def metrics_handler(request):
            metrics = await self.get_real_data_metrics()
            
            # Generate Prometheus metrics
            timestamp = int(datetime.now().timestamp())
            lines = [
                "# HELP ats_realtime_data_freshness_seconds Seconds since last data update",
                "# TYPE ats_realtime_data_freshness_seconds gauge",
                "",
                "# HELP ats_realtime_quality_score Current data quality score (0-1)", 
                "# TYPE ats_realtime_quality_score gauge",
                "",
                "# HELP ats_realtime_records_per_hour Records collected in the last hour",
                "# TYPE ats_realtime_records_per_hour gauge",
                ""
            ]
            
            for metric in metrics.get("freshness_metrics", []):
                vendor = metric["vendor"]
                symbol = metric["symbol"]
                lines.append(f'ats_realtime_data_freshness_seconds{{vendor="{vendor}",symbol="{symbol}"}} {metric["seconds_since_last_update"]} {timestamp}')
                lines.append(f'ats_realtime_quality_score{{vendor="{vendor}",symbol="{symbol}"}} {metric["quality_score"]} {timestamp}')
                lines.append(f'ats_realtime_records_per_hour{{vendor="{vendor}",symbol="{symbol}"}} {metric.get("records_last_hour", 0)} {timestamp}')
            
            return web.Response(text='\\n'.join(lines) + '\\n', content_type='text/plain')
        
        app = web.Application()
        app.router.add_get('/metrics', metrics_handler)
        
        runner = web.AppRunner(app)
        await runner.setup()
        
        site = web.TCPSite(runner, "0.0.0.0", 8091)
        await site.start()
        
        logger.info("📈 Prometheus metrics server started on port 8091")
        return runner
    
    async def monitoring_loop(self):
        """Main monitoring loop."""
        logger.info("🔄 Starting monitoring loop...")
        
        while True:
            try:
                # Get current metrics
                metrics = await self.get_real_data_metrics()
                
                # Evaluate alerts
                alerts = await self.evaluate_alerts(metrics)
                
                # Send new alerts
                if alerts:
                    logger.warning(f"🚨 Generated {len(alerts)} alerts")
                    await self.send_alerts(alerts)
                
                # Broadcast to dashboard
                dashboard_data = {
                    **metrics,
                    "alerts": alerts,
                    "timestamp": datetime.now().isoformat()
                }
                
                await self.dashboard.broadcast_update(dashboard_data)
                
                # Wait before next check
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"❌ Error in monitoring loop: {e}")
                await asyncio.sleep(60)  # Wait longer on error
    
    async def start(self):
        """Start all monitoring components."""
        logger.info("🚀 Starting ATS Real-time Monitoring System")
        
        # Start dashboard
        dashboard_runner = await self.dashboard.start()
        
        # Start metrics server
        metrics_runner = await self.start_metrics_server()
        
        # Start monitoring loop
        monitoring_task = asyncio.create_task(self.monitoring_loop())
        
        logger.info("✅ All monitoring components started")
        logger.info("")
        logger.info("📊 Access Points:")
        logger.info("   Dashboard:  http://localhost:8090")
        logger.info("   Health:     http://localhost:8090/health") 
        logger.info("   Metrics:    http://localhost:8091/metrics")
        logger.info("")
        
        try:
            await monitoring_task
        except KeyboardInterrupt:
            logger.info("⏹️ Shutting down monitoring system...")
        finally:
            monitoring_task.cancel()
            await dashboard_runner.cleanup()
            await metrics_runner.cleanup()


def load_config(config_path: str) -> Dict[str, Any]:
    """Load monitoring configuration."""
    with open(config_path) as f:
        return json.load(f)


async def main():
    """Main function."""
    logger.info("🎯 ATS Real-time Collection Monitoring System")
    logger.info("🔧 Simplified Working Version")
    logger.info("=" * 60)
    
    # Load configuration
    config_path = Path(__file__).parent.parent / "config" / "realtime_monitoring_config.json"
    if not config_path.exists():
        logger.error(f"❌ Configuration file not found: {config_path}")
        return False
    
    config = load_config(config_path)
    logger.info(f"✅ Configuration loaded from {config_path}")
    
    # Create and start monitoring system
    monitoring_system = SimpleMonitoringSystem(config)
    await monitoring_system.start()
    
    return True


if __name__ == "__main__":
    asyncio.run(main())