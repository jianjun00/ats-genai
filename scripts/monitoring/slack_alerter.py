#!/usr/bin/env python3
"""
Slack Alerter for Minute Data Backfill Monitoring

Sends real-time alerts to Slack about backfill progress, resource usage, and issues.
"""

import asyncio
import json
import aiohttp
import os
import logging
from datetime import datetime
from enum import Enum

class AlertLevel(Enum):
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class SlackAlerter:
    def __init__(self, webhook_url=None, channel="#trading-alerts"):
        self.webhook_url = webhook_url or os.getenv('SLACK_WEBHOOK_URL')
        self.channel = channel
        self.logger = logging.getLogger(__name__)
        
        if not self.webhook_url:
            self.logger.warning("No Slack webhook URL provided - alerts will be logged only")
    
    def _get_color_for_level(self, level: AlertLevel):
        """Get color code for alert level"""
        colors = {
            AlertLevel.INFO: "#36a64f",      # Green
            AlertLevel.SUCCESS: "#2eb886",   # Bright green
            AlertLevel.WARNING: "#ff9500",   # Orange
            AlertLevel.ERROR: "#ff4444",     # Red
            AlertLevel.CRITICAL: "#8b0000"   # Dark red
        }
        return colors.get(level, "#36a64f")
    
    def _get_emoji_for_level(self, level: AlertLevel):
        """Get emoji for alert level"""
        emojis = {
            AlertLevel.INFO: "ℹ️",
            AlertLevel.SUCCESS: "✅",
            AlertLevel.WARNING: "⚠️",
            AlertLevel.ERROR: "❌",
            AlertLevel.CRITICAL: "🚨"
        }
        return emojis.get(level, "ℹ️")
    
    async def send_alert(self, title, message, level=AlertLevel.INFO, fields=None):
        """Send alert to Slack"""
        if not self.webhook_url:
            self.logger.info(f"[{level.value.upper()}] {title}: {message}")
            return
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        emoji = self._get_emoji_for_level(level)
        color = self._get_color_for_level(level)
        
        payload = {
            "channel": self.channel,
            "username": "Minute Backfill Monitor",
            "icon_emoji": ":chart_with_upwards_trend:",
            "attachments": [
                {
                    "color": color,
                    "title": f"{emoji} {title}",
                    "text": message,
                    "ts": int(datetime.now().timestamp()),
                    "footer": "ATS Minute Data Backfill",
                    "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png"
                }
            ]
        }
        
        if fields:
            payload["attachments"][0]["fields"] = [
                {"title": k, "value": v, "short": True} for k, v in fields.items()
            ]
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.webhook_url, json=payload) as response:
                    if response.status == 200:
                        self.logger.info(f"Slack alert sent: {title}")
                    else:
                        self.logger.error(f"Failed to send Slack alert: {response.status}")
        except Exception as e:
            self.logger.error(f"Error sending Slack alert: {e}")
    
    async def alert_job_started(self, job_name, stage, instruments_count, date_range):
        """Alert when a backfill job starts"""
        await self.send_alert(
            title=f"Minute Backfill Started - {stage}",
            message=f"Job `{job_name}` has started processing minute data backfill",
            level=AlertLevel.INFO,
            fields={
                "Stage": stage,
                "Instruments": f"{instruments_count:,}",
                "Date Range": date_range,
                "Status": "Running"
            }
        )
    
    async def alert_job_completed(self, job_name, stage, duration, records_processed):
        """Alert when a backfill job completes successfully"""
        await self.send_alert(
            title=f"Minute Backfill Completed - {stage}",
            message=f"Job `{job_name}` completed successfully! 🎉",
            level=AlertLevel.SUCCESS,
            fields={
                "Stage": stage,
                "Duration": duration,
                "Records Processed": f"{records_processed:,}",
                "Status": "Success"
            }
        )
    
    async def alert_job_failed(self, job_name, stage, error_message):
        """Alert when a backfill job fails"""
        await self.send_alert(
            title=f"Minute Backfill Failed - {stage}",
            message=f"Job `{job_name}` failed with error: {error_message}",
            level=AlertLevel.ERROR,
            fields={
                "Stage": stage,
                "Error": error_message,
                "Status": "Failed",
                "Action": "Review logs and restart"
            }
        )
    
    async def alert_resource_pressure(self, memory_percent, cpu_percent, action_taken):
        """Alert when resource pressure is detected"""
        level = AlertLevel.CRITICAL if action_taken == "Emergency stop" else AlertLevel.WARNING
        
        await self.send_alert(
            title="Resource Pressure Detected",
            message=f"System resources are under pressure during minute data backfill",
            level=level,
            fields={
                "Memory Usage": f"{memory_percent:.1f}%",
                "CPU Usage": f"{cpu_percent:.1f}%",
                "Action Taken": action_taken,
                "Recommendation": "Monitor system stability"
            }
        )
    
    async def alert_batch_progress(self, batch_num, total_batches, stage, records_in_batch):
        """Alert on significant batch progress (every 10th batch)"""
        if batch_num % 10 == 0 or batch_num == total_batches:
            progress_percent = (batch_num / total_batches) * 100
            
            await self.send_alert(
                title=f"Batch Progress Update - {stage}",
                message=f"Processing batch {batch_num}/{total_batches} ({progress_percent:.1f}% complete)",
                level=AlertLevel.INFO,
                fields={
                    "Batch": f"{batch_num}/{total_batches}",
                    "Progress": f"{progress_percent:.1f}%",
                    "Records in Batch": f"{records_in_batch:,}",
                    "Stage": stage
                }
            )
    
    async def alert_system_status(self, memory_percent, cpu_percent, minute_records_total):
        """Send periodic system status update"""
        await self.send_alert(
            title="System Status Update",
            message="Minute data backfill system status report",
            level=AlertLevel.INFO,
            fields={
                "Memory Usage": f"{memory_percent:.1f}%",
                "CPU Usage": f"{cpu_percent:.1f}%",
                "Total Minute Records": f"{minute_records_total:,}",
                "System": "Stable"
            }
        )
    
    async def alert_emergency_stop(self, reason, memory_percent, cpu_percent):
        """Alert when emergency stop is triggered"""
        await self.send_alert(
            title="🚨 EMERGENCY STOP TRIGGERED",
            message=f"Minute data backfill has been emergency stopped: {reason}",
            level=AlertLevel.CRITICAL,
            fields={
                "Reason": reason,
                "Memory": f"{memory_percent:.1f}%",
                "CPU": f"{cpu_percent:.1f}%",
                "Action Required": "Investigate and restart manually"
            }
        )

# Example usage and testing
async def test_slack_alerts():
    """Test Slack alerting functionality"""
    alerter = SlackAlerter()
    
    # Test different alert types
    await alerter.alert_job_started(
        job_name="stage1-minute-backfill-job",
        stage="Stage 1 Conservative",
        instruments_count=500,
        date_range="2019-12-28 to 2019-12-31"
    )
    
    await asyncio.sleep(1)
    
    await alerter.alert_batch_progress(
        batch_num=5,
        total_batches=10,
        stage="Stage 1",
        records_in_batch=40950
    )
    
    await asyncio.sleep(1)
    
    await alerter.alert_resource_pressure(
        memory_percent=75.0,
        cpu_percent=65.0,
        action_taken="Paused processing"
    )
    
    await asyncio.sleep(1)
    
    await alerter.alert_job_completed(
        job_name="stage1-minute-backfill-job",
        stage="Stage 1 Conservative",
        duration="5 minutes",
        records_processed=409500
    )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Set your Slack webhook URL here or as environment variable
    # os.environ['SLACK_WEBHOOK_URL'] = 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL'
    
    asyncio.run(test_slack_alerts())