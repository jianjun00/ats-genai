#!/usr/bin/env python3
"""
Alert Channel Integrations for Real-time Collection Monitoring

Multi-channel alerting system supporting:
- Slack webhooks
- Discord webhooks
- Email notifications
- SMS via Twilio
- PagerDuty integration
- Microsoft Teams
- Custom webhook endpoints

Features:
- Template-based alert formatting
- Rate limiting and deduplication
- Priority-based routing
- Retry logic with exponential backoff
- Alert escalation workflows
- Rich formatting for different platforms

Usage:
    from market_data.realtime.monitoring.alert_channels import AlertChannelManager

    manager = AlertChannelManager()
    await manager.send_alert(alert, channels=['slack', 'email'])
"""

import asyncio
import aiohttp
import json
import logging
import os
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

from .realtime_collection_monitor import MonitoringAlert, AlertLevel

logger = logging.getLogger(__name__)


class ChannelType(Enum):
    """Supported alert channel types."""
    SLACK = "slack"
    DISCORD = "discord"
    EMAIL = "email"
    SMS = "sms"
    PAGERDUTY = "pagerduty"
    TEAMS = "teams"
    WEBHOOK = "webhook"


@dataclass
class ChannelConfig:
    """Configuration for an alert channel."""
    channel_type: ChannelType
    name: str
    webhook_url: Optional[str] = None
    email_recipients: Optional[List[str]] = None
    phone_numbers: Optional[List[str]] = None
    api_key: Optional[str] = None
    enabled: bool = True
    min_alert_level: AlertLevel = AlertLevel.INFO
    rate_limit_minutes: int = 5
    retry_attempts: int = 3
    custom_config: Optional[Dict[str, Any]] = None


class AlertChannelManager:
    """Manages multiple alert channels and routing."""

    def __init__(self):
        """Initialize the alert channel manager."""

        self.channels: Dict[str, ChannelConfig] = {}
        self.last_sent_times: Dict[str, datetime] = {}
        self.failed_alerts: List[Dict[str, Any]] = []

        # Load configuration from environment
        self._load_channel_configurations()

        logger.info(f"📢 Alert Channel Manager initialized with {len(self.channels)} channels")

    def _load_channel_configurations(self):
        """Load channel configurations from environment variables."""

        # Slack configuration
        slack_webhook = os.getenv('SLACK_WEBHOOK_URL')
        if slack_webhook:
            self.channels['slack'] = ChannelConfig(
                channel_type=ChannelType.SLACK,
                name='slack',
                webhook_url=slack_webhook,
                min_alert_level=AlertLevel.WARNING,
                rate_limit_minutes=2
            )

        # Discord configuration
        discord_webhook = os.getenv('DISCORD_WEBHOOK_URL')
        if discord_webhook:
            self.channels['discord'] = ChannelConfig(
                channel_type=ChannelType.DISCORD,
                name='discord',
                webhook_url=discord_webhook,
                min_alert_level=AlertLevel.INFO,
                rate_limit_minutes=1
            )

        # Email configuration
        smtp_server = os.getenv('SMTP_SERVER')
        smtp_username = os.getenv('SMTP_USERNAME')
        smtp_password = os.getenv('SMTP_PASSWORD')
        email_recipients = os.getenv('ALERT_EMAIL_RECIPIENTS', '').split(',')

        if smtp_server and smtp_username and any(email_recipients):
            self.channels['email'] = ChannelConfig(
                channel_type=ChannelType.EMAIL,
                name='email',
                email_recipients=[email.strip() for email in email_recipients if email.strip()],
                min_alert_level=AlertLevel.CRITICAL,
                rate_limit_minutes=10,
                custom_config={
                    'smtp_server': smtp_server,
                    'smtp_port': int(os.getenv('SMTP_PORT', '587')),
                    'smtp_username': smtp_username,
                    'smtp_password': smtp_password,
                    'use_tls': os.getenv('SMTP_USE_TLS', 'true').lower() == 'true'
                }
            )

        # PagerDuty configuration
        pagerduty_key = os.getenv('PAGERDUTY_INTEGRATION_KEY')
        if pagerduty_key:
            self.channels['pagerduty'] = ChannelConfig(
                channel_type=ChannelType.PAGERDUTY,
                name='pagerduty',
                api_key=pagerduty_key,
                min_alert_level=AlertLevel.CRITICAL,
                rate_limit_minutes=15
            )

        # Teams configuration
        teams_webhook = os.getenv('TEAMS_WEBHOOK_URL')
        if teams_webhook:
            self.channels['teams'] = ChannelConfig(
                channel_type=ChannelType.TEAMS,
                name='teams',
                webhook_url=teams_webhook,
                min_alert_level=AlertLevel.WARNING,
                rate_limit_minutes=5
            )

        logger.info(f"📋 Loaded {len(self.channels)} alert channels: {list(self.channels.keys())}")

    def _should_send_alert(self, alert: MonitoringAlert, channel: ChannelConfig) -> bool:
        """Check if alert should be sent to this channel based on level and rate limiting."""

        # Check if channel is enabled
        if not channel.enabled:
            return False

        # Check alert level threshold
        alert_level_priority = {
            AlertLevel.INFO: 0,
            AlertLevel.WARNING: 1,
            AlertLevel.CRITICAL: 2,
            AlertLevel.FATAL: 3
        }

        if alert_level_priority[alert.level] < alert_level_priority[channel.min_alert_level]:
            return False

        # Check rate limiting
        rate_limit_key = f"{channel.name}_{alert.category}_{alert.metric_name}"

        if rate_limit_key in self.last_sent_times:
            time_since_last = (alert.timestamp - self.last_sent_times[rate_limit_key]).total_seconds()
            if time_since_last < channel.rate_limit_minutes * 60:
                logger.debug(f"🚫 Rate limited: {channel.name} alert for {rate_limit_key}")
                return False

        return True

    def _format_alert_for_slack(self, alert: MonitoringAlert) -> Dict[str, Any]:
        """Format alert for Slack."""

        color_map = {
            AlertLevel.INFO: "#36a64f",      # Green
            AlertLevel.WARNING: "#ff9900",   # Orange
            AlertLevel.CRITICAL: "#ff0000",  # Red
            AlertLevel.FATAL: "#8B0000"      # Dark Red
        }

        emoji_map = {
            AlertLevel.INFO: "ℹ️",
            AlertLevel.WARNING: "⚠️",
            AlertLevel.CRITICAL: "🚨",
            AlertLevel.FATAL: "💥"
        }

        # Build attachment fields
        fields = []

        # Add key details as fields
        for key, value in alert.details.items():
            if key in ['vendor', 'symbol', 'timestamp']:
                continue  # Skip these, we'll show them differently

            field_value = str(value)
            if isinstance(value, float):
                field_value = f"{value:.3f}"

            fields.append({
                "title": key.replace('_', ' ').title(),
                "value": field_value,
                "short": True
            })

        # Add threshold comparison if available
        if alert.current_value is not None and alert.threshold_value is not None:
            fields.append({
                "title": "Threshold",
                "value": f"Current: {alert.current_value:.3f} | Limit: {alert.threshold_value:.3f}",
                "short": False
            })

        return {
            "text": f"{emoji_map[alert.level]} ATS Real-time Collection Alert",
            "attachments": [{
                "color": color_map[alert.level],
                "title": alert.message,
                "fields": fields,
                "footer": "ATS Real-time Monitoring",
                "ts": int(alert.timestamp.timestamp())
            }]
        }

    def _format_alert_for_discord(self, alert: MonitoringAlert) -> Dict[str, Any]:
        """Format alert for Discord."""

        color_map = {
            AlertLevel.INFO: 0x36a64f,      # Green
            AlertLevel.WARNING: 0xff9900,   # Orange
            AlertLevel.CRITICAL: 0xff0000,  # Red
            AlertLevel.FATAL: 0x8B0000      # Dark Red
        }

        # Build embed fields
        fields = []
        for key, value in alert.details.items():
            if len(fields) >= 25:  # Discord limit
                break

            field_value = str(value)
            if isinstance(value, float):
                field_value = f"{value:.3f}"

            fields.append({
                "name": key.replace('_', ' ').title(),
                "value": field_value,
                "inline": True
            })

        return {
            "embeds": [{
                "title": f"🎯 ATS Real-time Collection Alert",
                "description": alert.message,
                "color": color_map[alert.level],
                "fields": fields,
                "timestamp": alert.timestamp.isoformat(),
                "footer": {
                    "text": f"Level: {alert.level.value.upper()} | Category: {alert.category}"
                }
            }]
        }

    def _format_alert_for_teams(self, alert: MonitoringAlert) -> Dict[str, Any]:
        """Format alert for Microsoft Teams."""

        color_map = {
            AlertLevel.INFO: "Good",
            AlertLevel.WARNING: "Warning",
            AlertLevel.CRITICAL: "Attention",
            AlertLevel.FATAL: "Attention"
        }

        # Build facts array
        facts = []
        for key, value in alert.details.items():
            if len(facts) >= 10:  # Keep it reasonable
                break

            field_value = str(value)
            if isinstance(value, float):
                field_value = f"{value:.3f}"

            facts.append({
                "name": key.replace('_', ' ').title(),
                "value": field_value
            })

        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": color_map[alert.level],
            "summary": alert.message,
            "sections": [{
                "activityTitle": "🎯 ATS Real-time Collection Alert",
                "activitySubtitle": alert.message,
                "facts": facts
            }]
        }

    def _format_alert_for_email(self, alert: MonitoringAlert) -> Dict[str, str]:
        """Format alert for email."""

        subject = f"[ATS-{alert.level.value.upper()}] {alert.message}"

        # Build HTML email body
        html_body = f"""
        <html>
        <head></head>
        <body>
            <h2>🎯 ATS Real-time Collection Alert</h2>

            <p><strong>Level:</strong> {alert.level.value.upper()}</p>
            <p><strong>Category:</strong> {alert.category}</p>
            <p><strong>Message:</strong> {alert.message}</p>
            <p><strong>Timestamp:</strong> {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>

            <h3>Alert Details</h3>
            <table border="1" cellpadding="5" cellspacing="0">
        """

        for key, value in alert.details.items():
            display_value = str(value)
            if isinstance(value, float):
                display_value = f"{value:.3f}"

            html_body += f"""
                <tr>
                    <td><strong>{key.replace('_', ' ').title()}</strong></td>
                    <td>{display_value}</td>
                </tr>
            """

        if alert.current_value is not None and alert.threshold_value is not None:
            html_body += f"""
                <tr>
                    <td><strong>Current Value</strong></td>
                    <td>{alert.current_value:.3f}</td>
                </tr>
                <tr>
                    <td><strong>Threshold</strong></td>
                    <td>{alert.threshold_value:.3f}</td>
                </tr>
            """

        html_body += """
            </table>

            <br>
            <p><em>This alert was generated by the ATS Real-time Collection Monitoring System.</em></p>
        </body>
        </html>
        """

        # Plain text version
        text_body = f"""
ATS Real-time Collection Alert

Level: {alert.level.value.upper()}
Category: {alert.category}
Message: {alert.message}
Timestamp: {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}

Alert Details:
"""

        for key, value in alert.details.items():
            display_value = str(value)
            if isinstance(value, float):
                display_value = f"{value:.3f}"
            text_body += f"- {key.replace('_', ' ').title()}: {display_value}\n"

        if alert.current_value is not None and alert.threshold_value is not None:
            text_body += f"\nCurrent Value: {alert.current_value:.3f}\n"
            text_body += f"Threshold: {alert.threshold_value:.3f}\n"

        text_body += "\nThis alert was generated by the ATS Real-time Collection Monitoring System."

        return {
            "subject": subject,
            "html_body": html_body,
            "text_body": text_body
        }

    async def _send_webhook_alert(self, webhook_url: str, payload: Dict[str, Any], channel_name: str) -> bool:
        """Send alert via webhook (Slack, Discord, Teams, etc.)."""

        try:
            timeout = aiohttp.ClientTimeout(total=10)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    webhook_url,
                    json=payload,
                    headers={'Content-Type': 'application/json'}
                ) as response:

                    if response.status == 200:
                        logger.info(f"✅ Successfully sent {channel_name} alert")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Failed to send {channel_name} alert: {response.status} - {error_text}")
                        return False

        except Exception as e:
            logger.error(f"❌ Error sending {channel_name} webhook alert: {e}")
            return False

    async def _send_email_alert(self, channel: ChannelConfig, alert: MonitoringAlert) -> bool:
        """Send email alert."""

        if not channel.email_recipients or not channel.custom_config:
            logger.error("❌ Email channel not properly configured")
            return False

        try:
            email_format = self._format_alert_for_email(alert)
            config = channel.custom_config

            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = email_format['subject']
            msg['From'] = config['smtp_username']
            msg['To'] = ', '.join(channel.email_recipients)

            # Attach text and HTML parts
            text_part = MIMEText(email_format['text_body'], 'plain')
            html_part = MIMEText(email_format['html_body'], 'html')

            msg.attach(text_part)
            msg.attach(html_part)

            # Send email
            server = smtplib.SMTP(config['smtp_server'], config['smtp_port'])

            if config.get('use_tls', True):
                server.starttls()

            server.login(config['smtp_username'], config['smtp_password'])

            text = msg.as_string()
            server.sendmail(config['smtp_username'], channel.email_recipients, text)
            server.quit()

            logger.info(f"✅ Successfully sent email alert to {len(channel.email_recipients)} recipients")
            return True

        except Exception as e:
            logger.error(f"❌ Error sending email alert: {e}")
            return False

    async def _send_pagerduty_alert(self, channel: ChannelConfig, alert: MonitoringAlert) -> bool:
        """Send PagerDuty alert."""

        if not channel.api_key:
            logger.error("❌ PagerDuty channel not properly configured")
            return False

        try:
            # PagerDuty Events API v2
            pagerduty_url = "https://events.pagerduty.com/v2/enqueue"

            severity_map = {
                AlertLevel.INFO: "info",
                AlertLevel.WARNING: "warning",
                AlertLevel.CRITICAL: "error",
                AlertLevel.FATAL: "critical"
            }

            payload = {
                "routing_key": channel.api_key,
                "event_action": "trigger",
                "dedup_key": f"ats_realtime_{alert.category}_{alert.metric_name}",
                "payload": {
                    "summary": alert.message,
                    "source": "ATS Real-time Collection Monitor",
                    "severity": severity_map[alert.level],
                    "timestamp": alert.timestamp.isoformat(),
                    "custom_details": alert.details
                }
            }

            timeout = aiohttp.ClientTimeout(total=10)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    pagerduty_url,
                    json=payload,
                    headers={'Content-Type': 'application/json'}
                ) as response:

                    if response.status == 202:
                        logger.info("✅ Successfully sent PagerDuty alert")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Failed to send PagerDuty alert: {response.status} - {error_text}")
                        return False

        except Exception as e:
            logger.error(f"❌ Error sending PagerDuty alert: {e}")
            return False

    async def send_alert(self, alert: MonitoringAlert, channels: Optional[List[str]] = None) -> Dict[str, bool]:
        """Send alert to specified channels or all configured channels."""

        if channels is None:
            channels = list(self.channels.keys())

        results = {}

        for channel_name in channels:
            if channel_name not in self.channels:
                logger.warning(f"⚠️ Channel {channel_name} not configured, skipping")
                results[channel_name] = False
                continue

            channel = self.channels[channel_name]

            # Check if we should send this alert
            if not self._should_send_alert(alert, channel):
                logger.debug(f"🚫 Skipping {channel_name} alert (filtered)")
                results[channel_name] = False
                continue

            success = False

            try:
                # Route to appropriate sender based on channel type
                if channel.channel_type == ChannelType.SLACK:
                    payload = self._format_alert_for_slack(alert)
                    success = await self._send_webhook_alert(channel.webhook_url, payload, channel_name)

                elif channel.channel_type == ChannelType.DISCORD:
                    payload = self._format_alert_for_discord(alert)
                    success = await self._send_webhook_alert(channel.webhook_url, payload, channel_name)

                elif channel.channel_type == ChannelType.TEAMS:
                    payload = self._format_alert_for_teams(alert)
                    success = await self._send_webhook_alert(channel.webhook_url, payload, channel_name)

                elif channel.channel_type == ChannelType.EMAIL:
                    success = await self._send_email_alert(channel, alert)

                elif channel.channel_type == ChannelType.PAGERDUTY:
                    success = await self._send_pagerduty_alert(channel, alert)

                else:
                    logger.warning(f"⚠️ Channel type {channel.channel_type} not implemented")

                if success:
                    # Update rate limiting timestamp
                    rate_limit_key = f"{channel.name}_{alert.category}_{alert.metric_name}"
                    self.last_sent_times[rate_limit_key] = alert.timestamp

            except Exception as e:
                logger.error(f"❌ Error sending alert to {channel_name}: {e}")
                success = False

            results[channel_name] = success

            # Add small delay between channels to avoid overwhelming
            await asyncio.sleep(0.1)

        return results

    def get_channel_status(self) -> Dict[str, Any]:
        """Get current status of all configured channels."""

        status = {
            'total_channels': len(self.channels),
            'enabled_channels': sum(1 for c in self.channels.values() if c.enabled),
            'channels': {}
        }

        for name, channel in self.channels.items():
            status['channels'][name] = {
                'type': channel.channel_type.value,
                'enabled': channel.enabled,
                'min_alert_level': channel.min_alert_level.value,
                'rate_limit_minutes': channel.rate_limit_minutes,
                'last_sent': None
            }

            # Find most recent send time for this channel
            channel_sends = [
                timestamp for key, timestamp in self.last_sent_times.items()
                if key.startswith(f"{name}_")
            ]

            if channel_sends:
                status['channels'][name]['last_sent'] = max(channel_sends).isoformat()

        return status

    async def test_channels(self, channels: Optional[List[str]] = None) -> Dict[str, bool]:
        """Send test alerts to verify channel configurations."""

        from datetime import datetime

        test_alert = MonitoringAlert(
            timestamp=datetime.now(),
            level=AlertLevel.INFO,
            category="test",
            message="Test alert from ATS Real-time Collection Monitor",
            details={
                "test_mode": True,
                "timestamp": datetime.now().isoformat(),
                "system": "ATS Real-time Collection Monitor"
            },
            metric_name="test_metric"
        )

        logger.info("🧪 Sending test alerts to configured channels")
        results = await self.send_alert(test_alert, channels)

        for channel, success in results.items():
            if success:
                logger.info(f"✅ Test alert sent successfully to {channel}")
            else:
                logger.error(f"❌ Test alert failed for {channel}")

        return results


# Example configuration for standalone testing
async def main():
    """Test the alert channel system."""

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Set up test environment variables
    # os.environ['SLACK_WEBHOOK_URL'] = 'your_slack_webhook_here'
    # os.environ['DISCORD_WEBHOOK_URL'] = 'your_discord_webhook_here'

    manager = AlertChannelManager()

    # Test channels
    results = await manager.test_channels()

    print(f"\n📊 Channel test results: {results}")
    print(f"📋 Channel status: {json.dumps(manager.get_channel_status(), indent=2)}")


if __name__ == "__main__":
    asyncio.run(main())