"""
Alert handlers for the Data Agent monitoring system.

This module provides various alert handlers that can be used to send alerts
to different destinations like logging, Slack, email, etc.
"""

import logging
import os
import json
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

class AlertSeverity:
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

class AlertHandler:
    """Base class for alert handlers"""

    def send_alert(self, title: str, message: str, severity: str = AlertSeverity.WARNING, metadata: Optional[Dict[str, Any]] = None):
        """
        Send an alert

        Args:
            title: Alert title
            message: Alert message
            severity: Alert severity (info, warning, critical)
            metadata: Additional metadata for the alert
        """
        raise NotImplementedError("Subclasses must implement send_alert")

class LoggingAlertHandler(AlertHandler):
    """Alert handler that logs alerts"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def send_alert(self, title: str, message: str, severity: str = AlertSeverity.WARNING, metadata: Optional[Dict[str, Any]] = None):
        """
        Log an alert

        Args:
            title: Alert title
            message: Alert message
            severity: Alert severity (info, warning, critical)
            metadata: Additional metadata for the alert
        """
        if severity == AlertSeverity.INFO:
            log_level = logging.INFO
        elif severity == AlertSeverity.WARNING:
            log_level = logging.WARNING
        else:  # CRITICAL
            log_level = logging.ERROR

        self.logger.log(log_level, f"ALERT - {title}: {message}")
        if metadata:
            self.logger.log(log_level, f"ALERT METADATA: {json.dumps(metadata, default=str)}")

class SlackAlertHandler(AlertHandler):
    """Alert handler that sends alerts to Slack"""

    def __init__(self, webhook_url: Optional[str] = None):
        """
        Initialize with webhook URL from env var or parameter

        Args:
            webhook_url: Slack webhook URL (optional, defaults to SLACK_WEBHOOK_URL env var)
        """
        self.webhook_url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL")
        self.logger = logging.getLogger(__name__)

    def send_alert(self, title: str, message: str, severity: str = AlertSeverity.WARNING, metadata: Optional[Dict[str, Any]] = None):
        """
        Send an alert to Slack

        Args:
            title: Alert title
            message: Alert message
            severity: Alert severity (info, warning, critical)
            metadata: Additional metadata for the alert
        """
        if not self.webhook_url:
            self.logger.warning("Slack webhook URL not configured, alert not sent")
            return

        # Color coding based on severity
        color = {
            AlertSeverity.INFO: "#36a64f",  # green
            AlertSeverity.WARNING: "#ffcc00",  # yellow
            AlertSeverity.CRITICAL: "#ff0000"  # red
        }.get(severity, "#ffcc00")

        # Construct payload
        payload = {
            "attachments": [
                {
                    "fallback": f"{title}: {message}",
                    "color": color,
                    "title": title,
                    "text": message,
                    "fields": []
                }
            ]
        }

        # Add metadata fields if provided
        if metadata:
            for key, value in metadata.items():
                payload["attachments"][0]["fields"].append({
                    "title": key,
                    "value": str(value),
                    "short": True
                })

        # In a real implementation, we would send this to Slack
        # For now, just log it
        self.logger.info(f"Would send to Slack: {json.dumps(payload, default=str)}")

        # Actual implementation would use requests:
        # import requests
        # try:
        #     response = requests.post(self.webhook_url, json=payload)
        #     response.raise_for_status()
        # except Exception as e:
        #     self.logger.error(f"Failed to send Slack alert: {e}")

class EmailAlertHandler(AlertHandler):
    """Alert handler that sends alerts via email"""

    def __init__(self, recipients: Optional[List[str]] = None,
                 sender: Optional[str] = None,
                 smtp_server: Optional[str] = None):
        """
        Initialize with email configuration

        Args:
            recipients: List of email recipients (optional, defaults to ALERT_EMAIL_RECIPIENTS env var)
            sender: Sender email address (optional, defaults to ALERT_EMAIL_SENDER env var)
            smtp_server: SMTP server (optional, defaults to SMTP_SERVER env var)
        """
        self.recipients = recipients or (os.environ.get("ALERT_EMAIL_RECIPIENTS", "").split(",") if os.environ.get("ALERT_EMAIL_RECIPIENTS") else [])
        self.sender = sender or os.environ.get("ALERT_EMAIL_SENDER", "data-agent-alerts@example.com")
        self.smtp_server = smtp_server or os.environ.get("SMTP_SERVER", "localhost")
        self.logger = logging.getLogger(__name__)

    def send_alert(self, title: str, message: str, severity: str = AlertSeverity.WARNING, metadata: Optional[Dict[str, Any]] = None):
        """
        Send an alert via email

        Args:
            title: Alert title
            message: Alert message
            severity: Alert severity (info, warning, critical)
            metadata: Additional metadata for the alert
        """
        if not self.recipients:
            self.logger.warning("Email recipients not configured, alert not sent")
            return

        # Construct email subject with severity prefix
        subject = f"[{severity.upper()}] {title}"

        # Construct email body
        body = f"{message}\n\n"
        if metadata:
            body += "Additional Information:\n"
            for key, value in metadata.items():
                body += f"{key}: {value}\n"

        # In a real implementation, we would send this email
        # For now, just log it
        self.logger.info(f"Would send email:\nTo: {self.recipients}\nSubject: {subject}\nBody: {body}")

        # Actual implementation would use smtplib:
        # import smtplib
        # from email.message import EmailMessage
        # try:
        #     msg = EmailMessage()
        #     msg.set_content(body)
        #     msg["Subject"] = subject
        #     msg["From"] = self.sender
        #     msg["To"] = ", ".join(self.recipients)
        #
        #     with smtplib.SMTP(self.smtp_server) as server:
        #         server.send_message(msg)
        # except Exception as e:
        #     self.logger.error(f"Failed to send email alert: {e}")

class CompositeAlertHandler(AlertHandler):
    """Alert handler that delegates to multiple handlers"""

    def __init__(self, handlers: Optional[List[AlertHandler]] = None):
        """
        Initialize with a list of handlers

        Args:
            handlers: List of alert handlers
        """
        self.handlers = handlers or []

    def add_handler(self, handler: AlertHandler):
        """
        Add a handler

        Args:
            handler: Alert handler to add
        """
        self.handlers.append(handler)

    def send_alert(self, title: str, message: str, severity: str = AlertSeverity.WARNING, metadata: Optional[Dict[str, Any]] = None):
        """
        Send an alert to all handlers

        Args:
            title: Alert title
            message: Alert message
            severity: Alert severity (info, warning, critical)
            metadata: Additional metadata for the alert
        """
        for handler in self.handlers:
            try:
                handler.send_alert(title, message, severity, metadata)
            except Exception as e:
                logging.error(f"Error in alert handler {handler.__class__.__name__}: {e}")
