#!/usr/bin/env python3
"""
Alert System for Data Coverage Monitoring
Sends notifications when coverage drops below thresholds or critical gaps are detected
"""

import asyncio
import asyncpg
import logging
import smtplib
import json
import requests
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AlertManager:
    """Manages alert thresholds and notifications."""

    def __init__(self, db_config: Optional[Dict] = None):
        if db_config is None:
            self.db_config = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': int(os.getenv('DB_PORT', 4432)),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'intg_password'),
                'database': os.getenv('DB_NAME', 'intg_db'),
            }
        else:
            self.db_config = db_config
        self.db_pool = None

        # Alert configuration
        self.alert_config = {
            'critical_coverage_threshold': 70.0,  # Alert if coverage < 70%
            'warning_coverage_threshold': 85.0,   # Warning if coverage < 85%
            'critical_gap_priority': 20.0,        # Alert for gaps with priority >= 20
            'max_gap_age_hours': 24,              # Alert if gap older than 24h
            'email_enabled': os.getenv('ALERT_EMAIL_ENABLED', 'false').lower() == 'true',
            'slack_enabled': os.getenv('ALERT_SLACK_ENABLED', 'true').lower() == 'true'
        }

        # Slack configuration
        self.slack_config = {
            'webhook_url': os.getenv('SLACK_WEBHOOK_URL'),
            'channel': os.getenv('SLACK_CHANNEL', '#ats-data-alerts'),
            'username': os.getenv('SLACK_USERNAME', 'ATS Coverage Monitor'),
            'icon_emoji': os.getenv('SLACK_ICON', ':chart_with_upwards_trend:')
        }

        # Email configuration
        self.email_config = {
            'smtp_server': os.getenv('SMTP_SERVER', 'localhost'),
            'smtp_port': int(os.getenv('SMTP_PORT', 587)),
            'smtp_username': os.getenv('SMTP_USERNAME'),
            'smtp_password': os.getenv('SMTP_PASSWORD'),
            'from_email': os.getenv('ALERT_FROM_EMAIL', 'ats-monitoring@company.com'),
            'to_emails': os.getenv('ALERT_TO_EMAILS', '').split(',')
        }

    async def initialize(self):
        """Initialize database connection pool."""
        self.db_pool = await asyncpg.create_pool(
            host=self.db_config['host'],
            port=self.db_config['port'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            min_size=2,
            max_size=5
        )
        logger.info(f"✅ Alert system connected to database: {self.db_config['host']}:{self.db_config['port']}")

    async def close(self):
        """Close database connections."""
        if self.db_pool:
            await self.db_pool.close()

    async def check_coverage_alerts(self) -> List[Dict]:
        """Check for coverage alerts."""
        alerts = []

        async with self.db_pool.acquire() as conn:
            # Check current coverage levels
            coverage_data = await conn.fetch("""
                SELECT vendor, data_type, coverage_percentage,
                       symbols_complete, total_symbols, last_scan_time
                FROM v_current_coverage_summary
                WHERE coverage_percentage < $1
                ORDER BY coverage_percentage ASC
            """, self.alert_config['warning_coverage_threshold'])

            for row in coverage_data:
                severity = 'critical' if row['coverage_percentage'] < self.alert_config['critical_coverage_threshold'] else 'warning'

                alerts.append({
                    'type': 'coverage_low',
                    'severity': severity,
                    'vendor': row['vendor'],
                    'data_type': row['data_type'],
                    'coverage_percentage': float(row['coverage_percentage']),
                    'symbols_complete': row['symbols_complete'],
                    'total_symbols': row['total_symbols'],
                    'last_scan_time': row['last_scan_time'],
                    'message': f"{row['vendor']} {row['data_type']} coverage is {row['coverage_percentage']:.1f}% "
                              f"({row['symbols_complete']}/{row['total_symbols']} symbols)"
                })

        return alerts

    async def check_priority_gap_alerts(self) -> List[Dict]:
        """Check for high-priority gap alerts."""
        alerts = []

        async with self.db_pool.acquire() as conn:
            # Check for critical priority gaps
            gap_data = await conn.fetch("""
                SELECT vendor, data_type, symbol, gap_start_date, gap_end_date,
                       gap_days, adjusted_priority, created_at, backfill_status
                FROM v_active_backfill_queue
                WHERE adjusted_priority >= $1
                   OR (created_at < NOW() - INTERVAL '%d hours' AND backfill_status = 'pending')
                ORDER BY adjusted_priority DESC
                LIMIT 20
            """ % self.alert_config['max_gap_age_hours'], self.alert_config['critical_gap_priority'])

            for row in gap_data:
                gap_age_hours = (datetime.now() - row['created_at']).total_seconds() / 3600

                if row['adjusted_priority'] >= self.alert_config['critical_gap_priority']:
                    severity = 'critical'
                    reason = f"High priority gap ({row['adjusted_priority']:.1f})"
                else:
                    severity = 'warning'
                    reason = f"Gap pending for {gap_age_hours:.1f}h"

                alerts.append({
                    'type': 'priority_gap',
                    'severity': severity,
                    'vendor': row['vendor'],
                    'data_type': row['data_type'],
                    'symbol': row['symbol'],
                    'gap_days': row['gap_days'],
                    'priority': float(row['adjusted_priority']),
                    'gap_start_date': row['gap_start_date'],
                    'gap_end_date': row['gap_end_date'],
                    'age_hours': gap_age_hours,
                    'backfill_status': row['backfill_status'],
                    'message': f"{row['symbol']} ({row['vendor']}) has {row['gap_days']}-day gap. {reason}"
                })

        return alerts

    async def check_backfill_failure_alerts(self) -> List[Dict]:
        """Check for backfill failure alerts."""
        alerts = []

        async with self.db_pool.acquire() as conn:
            # Check for recent failures
            failure_data = await conn.fetch("""
                SELECT vendor, data_type, status, created_at, error_log,
                       array_length(symbols_requested, 1) as symbols_count
                FROM dev_backfill_operations
                WHERE status = 'failed'
                  AND created_at >= NOW() - INTERVAL '24 hours'
                ORDER BY created_at DESC
                LIMIT 10
            """)

            for row in failure_data:
                alerts.append({
                    'type': 'backfill_failure',
                    'severity': 'warning',
                    'vendor': row['vendor'],
                    'data_type': row['data_type'],
                    'symbols_count': row['symbols_count'],
                    'created_at': row['created_at'],
                    'error_log': row['error_log'][:200] if row['error_log'] else None,
                    'message': f"{row['vendor']} {row['data_type']} backfill failed "
                              f"({row['symbols_count']} symbols)"
                })

        return alerts

    async def run_alert_check(self) -> Dict[str, List]:
        """Run complete alert check and return all alerts."""
        logger.info("🔍 Running alert check...")

        coverage_alerts = await self.check_coverage_alerts()
        gap_alerts = await self.check_priority_gap_alerts()
        failure_alerts = await self.check_backfill_failure_alerts()

        all_alerts = {
            'coverage': coverage_alerts,
            'gaps': gap_alerts,
            'failures': failure_alerts,
            'timestamp': datetime.now()
        }

        total_alerts = len(coverage_alerts) + len(gap_alerts) + len(failure_alerts)
        critical_count = len([a for alerts in all_alerts.values()
                            for a in (alerts if isinstance(alerts, list) else [])
                            if isinstance(a, dict) and a.get('severity') == 'critical'])

        logger.info(f"📊 Alert summary: {total_alerts} total, {critical_count} critical")

        if total_alerts > 0:
            await self.send_alerts(all_alerts)

        return all_alerts

    async def send_alerts(self, alerts: Dict):
        """Send alerts via configured channels."""
        if self.alert_config['email_enabled']:
            await self.send_email_alerts(alerts)

        if self.alert_config['slack_enabled']:
            await self.send_slack_alerts(alerts)

        # Log all alerts to console
        self.log_alerts(alerts)

    async def send_email_alerts(self, alerts: Dict):
        """Send email alerts."""
        if not self.email_config['to_emails'] or not self.email_config['to_emails'][0]:
            logger.warning("📧 Email alerts enabled but no recipients configured")
            return

        try:
            # Generate email content
            subject, body = self.generate_email_content(alerts)

            # Create email message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.email_config['from_email']
            msg['To'] = ', '.join(self.email_config['to_emails'])

            # Add HTML body
            html_part = MIMEText(body, 'html')
            msg.attach(html_part)

            # Send email
            if self.email_config['smtp_username']:
                # Authenticated SMTP
                server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
                server.starttls()
                server.login(self.email_config['smtp_username'], self.email_config['smtp_password'])
            else:
                # Local SMTP
                server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])

            server.send_message(msg)
            server.quit()

            logger.info(f"📧 Email alerts sent to {len(self.email_config['to_emails'])} recipients")

        except Exception as e:
            logger.error(f"❌ Failed to send email alerts: {e}")

    async def send_slack_alerts(self, alerts: Dict):
        """Send Slack alerts."""
        if not self.slack_config['webhook_url']:
            logger.warning("📢 Slack alerts enabled but webhook URL not configured")
            return

        try:
            # Generate Slack message
            message = self.generate_slack_message(alerts)

            # Send to Slack (use webhook's default channel)
            payload = {
                'username': self.slack_config['username'],
                'icon_emoji': self.slack_config['icon_emoji'],
                'attachments': [message]
            }

            response = requests.post(
                self.slack_config['webhook_url'],
                json=payload,
                timeout=10
            )

            if response.status_code == 200:
                logger.info(f"📢 Slack alerts sent to {self.slack_config['channel']}")
            else:
                logger.error(f"❌ Slack webhook failed: {response.status_code} - {response.text}")

        except Exception as e:
            logger.error(f"❌ Failed to send Slack alerts: {e}")

    def generate_slack_message(self, alerts: Dict) -> Dict:
        """Generate Slack message attachment."""
        total_alerts = len(alerts['coverage']) + len(alerts['gaps']) + len(alerts['failures'])
        critical_count = sum(1 for alert_list in alerts.values()
                           for alert in (alert_list if isinstance(alert_list, list) else [])
                           if isinstance(alert, dict) and alert.get('severity') == 'critical')

        # Determine color and title
        if critical_count > 0:
            color = '#e74c3c'  # Red
            title = f"🚨 CRITICAL: ATS Data Coverage Alerts"
            pretext = f"*{critical_count} critical alerts detected!*"
        elif total_alerts > 0:
            color = '#f39c12'  # Orange
            title = f"⚠️ ATS Data Coverage Alerts"
            pretext = f"*{total_alerts} alerts detected*"
        else:
            color = '#27ae60'  # Green
            title = "✅ ATS Data Coverage - All Clear"
            pretext = "No alerts detected"

        # Generate fields
        fields = []

        # Coverage alerts
        if alerts['coverage']:
            coverage_text = []
            for alert in alerts['coverage'][:5]:  # Limit to 5
                severity_emoji = "🔴" if alert['severity'] == 'critical' else "🟡"
                coverage_text.append(
                    f"{severity_emoji} *{alert['vendor']} {alert['data_type']}*: "
                    f"{alert['coverage_percentage']:.1f}% ({alert['symbols_complete']}/{alert['total_symbols']})"
                )

            if len(alerts['coverage']) > 5:
                coverage_text.append(f"... and {len(alerts['coverage']) - 5} more")

            fields.append({
                'title': f"📊 Coverage Issues ({len(alerts['coverage'])})",
                'value': '\n'.join(coverage_text),
                'short': False
            })

        # Gap alerts
        if alerts['gaps']:
            gap_text = []
            for alert in alerts['gaps'][:5]:  # Limit to 5
                severity_emoji = "🔴" if alert['severity'] == 'critical' else "🟡"
                gap_text.append(
                    f"{severity_emoji} *{alert['symbol']}* ({alert['vendor']}): "
                    f"{alert['gap_days']} days, Priority {alert['priority']:.1f}"
                )

            if len(alerts['gaps']) > 5:
                gap_text.append(f"... and {len(alerts['gaps']) - 5} more gaps")

            fields.append({
                'title': f"🚨 Priority Gaps ({len(alerts['gaps'])})",
                'value': '\n'.join(gap_text),
                'short': False
            })

        # Failure alerts
        if alerts['failures']:
            failure_text = []
            for alert in alerts['failures'][:3]:  # Limit to 3
                failure_text.append(
                    f"❌ *{alert['vendor']} {alert['data_type']}*: "
                    f"{alert['symbols_count']} symbols failed"
                )

            if len(alerts['failures']) > 3:
                failure_text.append(f"... and {len(alerts['failures']) - 3} more failures")

            fields.append({
                'title': f"❌ Backfill Failures ({len(alerts['failures'])})",
                'value': '\n'.join(failure_text),
                'short': False
            })

        # Add dashboard link
        fields.append({
            'title': "📊 Dashboard",
            'value': f"<http://localhost:8080|View Coverage Dashboard>",
            'short': True
        })

        return {
            'color': color,
            'pretext': pretext,
            'title': title,
            'fields': fields,
            'footer': 'ATS Data Coverage Monitoring',
            'footer_icon': 'https://platform.slack-edge.com/img/default_application_icon.png',
            'ts': int(alerts['timestamp'].timestamp())
        }

    def generate_email_content(self, alerts: Dict) -> tuple:
        """Generate email subject and HTML body."""
        total_alerts = len(alerts['coverage']) + len(alerts['gaps']) + len(alerts['failures'])
        critical_count = sum(1 for alert_list in alerts.values()
                           for alert in (alert_list if isinstance(alert_list, list) else [])
                           if isinstance(alert, dict) and alert.get('severity') == 'critical')

        # Subject
        if critical_count > 0:
            subject = f"🚨 CRITICAL: ATS Data Coverage Alerts ({critical_count} critical, {total_alerts} total)"
        else:
            subject = f"⚠️ ATS Data Coverage Alerts ({total_alerts} alerts)"

        # HTML Body
        body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background: #2c3e50; color: white; padding: 15px; border-radius: 5px; }}
                .alert-section {{ margin: 20px 0; }}
                .alert-critical {{ background: #ffe6e6; border-left: 4px solid #e74c3c; padding: 10px; margin: 5px 0; }}
                .alert-warning {{ background: #fff3e0; border-left: 4px solid #f39c12; padding: 10px; margin: 5px 0; }}
                .timestamp {{ color: #7f8c8d; font-size: 0.9em; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🔍 ATS Data Coverage Monitoring Alert</h2>
                <p>Generated at: {alerts['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        """

        # Coverage alerts
        if alerts['coverage']:
            body += """
            <div class="alert-section">
                <h3>📊 Coverage Alerts</h3>
            """
            for alert in alerts['coverage']:
                alert_class = f"alert-{alert['severity']}"
                body += f"""
                <div class="{alert_class}">
                    <strong>{alert['vendor']} {alert['data_type']}</strong>:
                    {alert['coverage_percentage']:.1f}% coverage
                    ({alert['symbols_complete']}/{alert['total_symbols']} symbols)
                </div>
                """
            body += "</div>"

        # Gap alerts
        if alerts['gaps']:
            body += """
            <div class="alert-section">
                <h3>🚨 Priority Gap Alerts</h3>
            """
            for alert in alerts['gaps']:
                alert_class = f"alert-{alert['severity']}"
                body += f"""
                <div class="{alert_class}">
                    <strong>{alert['symbol']}</strong> ({alert['vendor']} {alert['data_type']}):
                    {alert['gap_days']}-day gap, Priority: {alert['priority']:.1f}
                    <br><small>Gap: {alert['gap_start_date']} to {alert['gap_end_date']}</small>
                </div>
                """
            body += "</div>"

        # Failure alerts
        if alerts['failures']:
            body += """
            <div class="alert-section">
                <h3>❌ Backfill Failure Alerts</h3>
            """
            for alert in alerts['failures']:
                body += f"""
                <div class="alert-warning">
                    <strong>{alert['vendor']} {alert['data_type']}</strong>:
                    Backfill failed ({alert['symbols_count']} symbols)
                    <br><small>Error: {alert['error_log'] or 'No error details available'}</small>
                </div>
                """
            body += "</div>"

        body += """
            <div class="alert-section">
                <p><a href="http://localhost:8080">📊 View Coverage Dashboard</a></p>
                <p class="timestamp">This is an automated alert from the ATS Data Coverage Monitoring system.</p>
            </div>
        </body>
        </html>
        """

        return subject, body

    def log_alerts(self, alerts: Dict):
        """Log alerts to console."""
        total_alerts = len(alerts['coverage']) + len(alerts['gaps']) + len(alerts['failures'])
        if total_alerts == 0:
            return

        logger.warning(f"🚨 ALERTS DETECTED ({total_alerts} total)")

        # Log coverage alerts
        for alert in alerts['coverage']:
            logger.warning(f"📊 COVERAGE {alert['severity'].upper()}: {alert['message']}")

        # Log gap alerts
        for alert in alerts['gaps']:
            logger.warning(f"🚨 GAP {alert['severity'].upper()}: {alert['message']}")

        # Log failure alerts
        for alert in alerts['failures']:
            logger.warning(f"❌ FAILURE {alert['severity'].upper()}: {alert['message']}")

async def main():
    """Main entry point for alert system."""
    # Database configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 4432)),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'intg_password'),
        'database': os.getenv('DB_NAME', 'intg_db')
    }

    alert_manager = AlertManager(db_config)

    try:
        await alert_manager.initialize()
        alerts = await alert_manager.run_alert_check()

        # Print summary
        total_alerts = len(alerts['coverage']) + len(alerts['gaps']) + len(alerts['failures'])
        print(f"\n🎯 ALERT SUMMARY")
        print(f"📊 Coverage alerts: {len(alerts['coverage'])}")
        print(f"🚨 Gap alerts: {len(alerts['gaps'])}")
        print(f"❌ Failure alerts: {len(alerts['failures'])}")
        print(f"📈 Total: {total_alerts}")

    except Exception as e:
        logger.error(f"💥 Alert check failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        await alert_manager.close()

if __name__ == "__main__":
    asyncio.run(main())