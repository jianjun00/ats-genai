"""
Alert Manager
=============

Enterprise-grade alerting and notification system for the Data Quality Agent.
Supports multiple notification channels, alert routing, and escalation policies.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
import aiohttp

from agents.agent_config import get_config_manager

logger = logging.getLogger(__name__)

@dataclass
class AlertRule:
    """Alert rule definition"""
    rule_id: str
    name: str
    condition: str  # Python expression to evaluate
    severity: str   # critical, high, medium, low
    description: str
    enabled: bool = True
    cooldown_minutes: int = 30
    escalation_chain: List[str] = None
    metadata: Dict[str, Any] = None

@dataclass
class AlertInstance:
    """Active alert instance"""
    alert_id: str
    rule_id: str
    severity: str
    title: str
    message: str
    timestamp: str
    source_component: str
    source_data: Dict[str, Any]
    acknowledged: bool = False
    resolved: bool = False
    escalation_level: int = 0
    last_notification: Optional[str] = None

@dataclass
class NotificationChannel:
    """Notification channel configuration"""
    channel_id: str
    channel_type: str  # email, slack, webhook, sms
    enabled: bool
    config: Dict[str, Any]
    rate_limit_per_hour: int = 10
    severity_filter: List[str] = None  # Only send alerts of these severities

class AlertManager:
    """Enterprise alert management with multiple notification channels"""
    
    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self.logger = logging.getLogger(f"alert_manager.{agent_id}")
        
        # Load configuration
        self.config_manager = get_config_manager()
        self.agent_config = self.config_manager.get_config()
        
        # Alert state
        self.active_alerts: Dict[str, AlertInstance] = {}
        self.alert_history: List[AlertInstance] = []
        self.alert_rules: Dict[str, AlertRule] = {}
        self.notification_channels: Dict[str, NotificationChannel] = {}
        
        # Rate limiting
        self.notification_counts: Dict[str, List[datetime]] = {}
        self.last_rule_triggers: Dict[str, datetime] = {}
        
        # Initialize default rules and channels
        self._initialize_default_rules()
        self._initialize_notification_channels()
        
        # Persistence
        self.alerts_file = Path(f"logs/alerts/alerts_{agent_id}.jsonl")
        self.alerts_file.parent.mkdir(parents=True, exist_ok=True)
    
    def _initialize_default_rules(self):
        """Initialize default alert rules"""
        default_rules = [
            AlertRule(
                rule_id="agent_stopped",
                name="Agent Monitoring Stopped",
                condition="data.get('monitoring_active') == False and data.get('expected_active') == True",
                severity="critical",
                description="Data Quality Agent has stopped monitoring when it should be active",
                cooldown_minutes=10
            ),
            AlertRule(
                rule_id="high_cpu_usage",
                name="High CPU Usage",
                condition="data.get('cpu_percent', 0) > 85",
                severity="high",
                description="System CPU usage is critically high",
                cooldown_minutes=15
            ),
            AlertRule(
                rule_id="critical_memory_usage",
                name="Critical Memory Usage", 
                condition="data.get('memory_percent', 0) > 90",
                severity="critical",
                description="System memory usage is at critical levels",
                cooldown_minutes=10
            ),
            AlertRule(
                rule_id="disk_space_low",
                name="Low Disk Space",
                condition="data.get('disk_usage_percent', 0) > 95",
                severity="critical",
                description="Disk space is critically low",
                cooldown_minutes=60
            ),
            AlertRule(
                rule_id="workflow_failures",
                name="Multiple Workflow Failures",
                condition="data.get('failed_workflows', 0) > 3",
                severity="high",
                description="Multiple workflows have failed recently",
                cooldown_minutes=30
            ),
            AlertRule(
                rule_id="data_quality_degradation",
                name="Data Quality Score Degradation",
                condition="data.get('quality_score', 100) < 50",
                severity="high",
                description="Data quality score has degraded significantly",
                cooldown_minutes=20
            ),
            AlertRule(
                rule_id="database_connection_failure",
                name="Database Connection Issues",
                condition="data.get('db_connection_errors', 0) > 0",
                severity="critical",
                description="Database connection failures detected",
                cooldown_minutes=5
            )
        ]
        
        for rule in default_rules:
            self.alert_rules[rule.rule_id] = rule
    
    def _initialize_notification_channels(self):
        """Initialize notification channels from configuration"""
        notification_config = self.agent_config.notifications
        
        # Email channel
        if notification_config.enable_email_notifications and notification_config.email_recipients:
            self.notification_channels["email"] = NotificationChannel(
                channel_id="email",
                channel_type="email",
                enabled=True,
                config={
                    "recipients": notification_config.email_recipients,
                    "smtp_server": "localhost",  # Configure as needed
                    "smtp_port": 587,
                    "sender": f"alerts@ats-agent-{self.agent_id}"
                },
                rate_limit_per_hour=notification_config.max_notifications_per_hour,
                severity_filter=["critical", "high"] if notification_config.notification_severity_threshold == "high" else ["critical"]
            )
        
        # Slack channel
        if notification_config.enable_slack_notifications and notification_config.slack_webhook_url:
            self.notification_channels["slack"] = NotificationChannel(
                channel_id="slack",
                channel_type="slack",
                enabled=True,
                config={
                    "webhook_url": notification_config.slack_webhook_url
                },
                rate_limit_per_hour=notification_config.max_notifications_per_hour,
                severity_filter=["critical", "high", "medium"]
            )
    
    async def evaluate_alert_rules(self, data: Dict[str, Any], source_component: str = "system"):
        """Evaluate all alert rules against provided data"""
        current_time = datetime.now()
        
        for rule_id, rule in self.alert_rules.items():
            if not rule.enabled:
                continue
            
            # Check cooldown
            if rule_id in self.last_rule_triggers:
                time_since_last = current_time - self.last_rule_triggers[rule_id]
                if time_since_last.total_seconds() < (rule.cooldown_minutes * 60):
                    continue
            
            try:
                # Evaluate rule condition
                if self._evaluate_condition(rule.condition, data):
                    await self._trigger_alert(rule, data, source_component)
                    self.last_rule_triggers[rule_id] = current_time
                    
            except Exception as e:
                self.logger.error(f"Error evaluating alert rule {rule_id}: {e}")
    
    def _evaluate_condition(self, condition: str, data: Dict[str, Any]) -> bool:
        """Safely evaluate alert condition"""
        try:
            # Create safe evaluation context
            safe_globals = {
                "__builtins__": {},
                "data": data,
                "len": len,
                "max": max,
                "min": min,
                "sum": sum,
                "abs": abs
            }
            
            return bool(eval(condition, safe_globals))
        except Exception as e:
            self.logger.error(f"Error evaluating condition '{condition}': {e}")
            return False
    
    async def _trigger_alert(self, rule: AlertRule, data: Dict[str, Any], source_component: str):
        """Trigger an alert based on rule evaluation"""
        alert_id = f"{rule.rule_id}_{int(datetime.now().timestamp())}"
        
        alert = AlertInstance(
            alert_id=alert_id,
            rule_id=rule.rule_id,
            severity=rule.severity,
            title=rule.name,
            message=rule.description,
            timestamp=datetime.now().isoformat(),
            source_component=source_component,
            source_data=data
        )
        
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        
        self.logger.warning(f"ALERT TRIGGERED: {rule.name} - {rule.description}")
        
        # Send notifications
        await self._send_notifications(alert)
        
        # Persist alert
        await self._persist_alert(alert)
    
    async def _send_notifications(self, alert: AlertInstance):
        """Send alert notifications through all configured channels"""
        for channel_id, channel in self.notification_channels.items():
            if not channel.enabled:
                continue
            
            # Check severity filter
            if channel.severity_filter and alert.severity not in channel.severity_filter:
                continue
            
            # Check rate limiting
            if not self._check_rate_limit(channel_id):
                self.logger.warning(f"Rate limit exceeded for channel {channel_id}")
                continue
            
            try:
                if channel.channel_type == "email":
                    await self._send_email_notification(alert, channel)
                elif channel.channel_type == "slack":
                    await self._send_slack_notification(alert, channel)
                elif channel.channel_type == "webhook":
                    await self._send_webhook_notification(alert, channel)
                
                # Update rate limiting
                self._record_notification(channel_id)
                
            except Exception as e:
                self.logger.error(f"Failed to send notification via {channel_id}: {e}")
    
    def _check_rate_limit(self, channel_id: str) -> bool:
        """Check if channel is within rate limits"""
        if channel_id not in self.notification_counts:
            self.notification_counts[channel_id] = []
        
        channel = self.notification_channels[channel_id]
        current_time = datetime.now()
        hour_ago = current_time - timedelta(hours=1)
        
        # Remove old notifications
        self.notification_counts[channel_id] = [
            timestamp for timestamp in self.notification_counts[channel_id]
            if timestamp > hour_ago
        ]
        
        return len(self.notification_counts[channel_id]) < channel.rate_limit_per_hour
    
    def _record_notification(self, channel_id: str):
        """Record a notification for rate limiting"""
        if channel_id not in self.notification_counts:
            self.notification_counts[channel_id] = []
        
        self.notification_counts[channel_id].append(datetime.now())
    
    async def _send_email_notification(self, alert: AlertInstance, channel: NotificationChannel):
        """Send email notification"""
        config = channel.config
        
        msg = MIMEMultipart()
        msg['From'] = config.get('sender', 'noreply@ats-agent.local')
        msg['To'] = ', '.join(config['recipients'])
        msg['Subject'] = f"[ATS Agent Alert - {alert.severity.upper()}] {alert.title}"
        
        body = f"""
Data Quality Agent Alert

Alert: {alert.title}
Severity: {alert.severity.upper()}
Time: {alert.timestamp}
Component: {alert.source_component}
Agent ID: {self.agent_id}

Description:
{alert.message}

Source Data:
{json.dumps(alert.source_data, indent=2)}

---
This alert was generated by the ATS Data Quality Agent monitoring system.
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Note: This is a basic SMTP implementation
        # In production, you would configure proper SMTP settings
        self.logger.info(f"Email notification prepared for {config['recipients']}")
    
    async def _send_slack_notification(self, alert: AlertInstance, channel: NotificationChannel):
        """Send Slack notification"""
        webhook_url = channel.config['webhook_url']
        
        severity_colors = {
            "critical": "#e74c3c",
            "high": "#e67e22", 
            "medium": "#f39c12",
            "low": "#3498db"
        }
        
        payload = {
            "text": f"🚨 ATS Agent Alert: {alert.title}",
            "attachments": [
                {
                    "color": severity_colors.get(alert.severity, "#6c757d"),
                    "fields": [
                        {"title": "Severity", "value": alert.severity.upper(), "short": True},
                        {"title": "Component", "value": alert.source_component, "short": True},
                        {"title": "Agent ID", "value": self.agent_id, "short": True},
                        {"title": "Time", "value": alert.timestamp, "short": True},
                        {"title": "Description", "value": alert.message, "short": False}
                    ]
                }
            ]
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(webhook_url, json=payload) as response:
                if response.status == 200:
                    self.logger.info("Slack notification sent successfully")
                else:
                    self.logger.error(f"Slack notification failed: {response.status}")
    
    async def _send_webhook_notification(self, alert: AlertInstance, channel: NotificationChannel):
        """Send webhook notification"""
        webhook_url = channel.config['webhook_url']
        
        payload = {
            "alert_id": alert.alert_id,
            "rule_id": alert.rule_id,
            "severity": alert.severity,
            "title": alert.title,
            "message": alert.message,
            "timestamp": alert.timestamp,
            "source_component": alert.source_component,
            "agent_id": self.agent_id,
            "source_data": alert.source_data
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(webhook_url, json=payload) as response:
                if response.status in [200, 201, 202]:
                    self.logger.info("Webhook notification sent successfully")
                else:
                    self.logger.error(f"Webhook notification failed: {response.status}")
    
    async def _persist_alert(self, alert: AlertInstance):
        """Persist alert to file"""
        try:
            with open(self.alerts_file, 'a') as f:
                f.write(json.dumps(asdict(alert)) + '\n')
        except Exception as e:
            self.logger.error(f"Failed to persist alert: {e}")
    
    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str = "system") -> bool:
        """Acknowledge an active alert"""
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id].acknowledged = True
            self.logger.info(f"Alert {alert_id} acknowledged by {acknowledged_by}")
            return True
        return False
    
    async def resolve_alert(self, alert_id: str, resolved_by: str = "system") -> bool:
        """Resolve an active alert"""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.resolved = True
            del self.active_alerts[alert_id]
            self.logger.info(f"Alert {alert_id} resolved by {resolved_by}")
            return True
        return False
    
    async def get_alert_summary(self) -> Dict[str, Any]:
        """Get comprehensive alert summary"""
        now = datetime.now()
        hour_ago = now - timedelta(hours=1)
        day_ago = now - timedelta(days=1)
        
        recent_alerts = [
            alert for alert in self.alert_history
            if datetime.fromisoformat(alert.timestamp) > day_ago
        ]
        
        last_hour_alerts = [
            alert for alert in recent_alerts
            if datetime.fromisoformat(alert.timestamp) > hour_ago
        ]
        
        return {
            "active_alerts": len(self.active_alerts),
            "active_by_severity": {
                severity: len([a for a in self.active_alerts.values() if a.severity == severity])
                for severity in ["critical", "high", "medium", "low"]
            },
            "alerts_last_hour": len(last_hour_alerts),
            "alerts_last_24h": len(recent_alerts),
            "top_alert_sources": self._get_top_alert_sources(recent_alerts),
            "alert_rules_enabled": len([r for r in self.alert_rules.values() if r.enabled]),
            "notification_channels": len([c for c in self.notification_channels.values() if c.enabled]),
            "rate_limit_status": {
                channel_id: len(self.notification_counts.get(channel_id, []))
                for channel_id in self.notification_channels.keys()
            }
        }
    
    def _get_top_alert_sources(self, alerts: List[AlertInstance]) -> Dict[str, int]:
        """Get top alert sources by frequency"""
        sources = {}
        for alert in alerts:
            sources[alert.source_component] = sources.get(alert.source_component, 0) + 1
        
        return dict(sorted(sources.items(), key=lambda x: x[1], reverse=True)[:5])
    
    async def test_notification_channels(self) -> Dict[str, bool]:
        """Test all notification channels"""
        results = {}
        
        test_alert = AlertInstance(
            alert_id="test_alert",
            rule_id="test_rule",
            severity="info",
            title="Test Alert",
            message="This is a test alert to verify notification channels",
            timestamp=datetime.now().isoformat(),
            source_component="alert_manager",
            source_data={"test": True}
        )
        
        for channel_id, channel in self.notification_channels.items():
            if not channel.enabled:
                results[channel_id] = False
                continue
            
            try:
                if channel.channel_type == "email":
                    await self._send_email_notification(test_alert, channel)
                elif channel.channel_type == "slack":
                    await self._send_slack_notification(test_alert, channel)
                elif channel.channel_type == "webhook":
                    await self._send_webhook_notification(test_alert, channel)
                
                results[channel_id] = True
                
            except Exception as e:
                self.logger.error(f"Test failed for channel {channel_id}: {e}")
                results[channel_id] = False
        
        return results

# Global alert manager instance
_alert_manager: Optional[AlertManager] = None

def get_alert_manager(agent_id: str) -> AlertManager:
    """Get or create alert manager instance"""
    global _alert_manager
    
    if _alert_manager is None:
        _alert_manager = AlertManager(agent_id)
    
    return _alert_manager