#!/usr/bin/env python3
"""
ATS Slack Webhook Proxy Service
Converts AlertManager webhook calls to Slack notifications
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Any

from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
import httpx
import uvicorn

# Configuration
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL', '')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
PORT = int(os.getenv('PORT', 8080))

# Setup logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger(__name__)

app = FastAPI(title="ATS Slack Webhook Proxy", version="1.0.0")

class AlertManagerWebhook(BaseModel):
    version: str
    groupKey: str
    truncatedAlerts: int
    status: str
    receiver: str
    groupLabels: Dict[str, str]
    commonLabels: Dict[str, str]
    commonAnnotations: Dict[str, str]
    externalURL: str
    alerts: List[Dict[str, Any]]

def format_alert_for_slack(alert_data: AlertManagerWebhook, alert_type: str = "default") -> Dict[str, Any]:
    """Convert AlertManager webhook to Slack message format"""
    
    # Determine status icon
    status_icon = "🔥" if alert_data.status == "firing" else "✅"
    
    # Color coding
    color_map = {
        "critical": "danger",
        "warning": "warning", 
        "info": "good"
    }
    
    # Get severity from alerts
    severity = "info"
    for alert in alert_data.alerts:
        if "severity" in alert.get("labels", {}):
            severity = alert["labels"]["severity"]
            break
    
    color = color_map.get(severity, "warning")
    
    # Build message blocks
    blocks = []
    
    # Header block
    title_text = f"{status_icon} ATS Alert - {alert_data.status.title()}"
    if alert_type == "critical":
        title_text = f"🚨 CRITICAL ATS Alert - {alert_data.status.title()}"
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<!channel> **CRITICAL ALERT REQUIRES IMMEDIATE ATTENTION**"
            }
        })
    elif alert_type == "warning":
        title_text = f"⚠️ ATS Warning - {alert_data.status.title()}"
    elif alert_type == "services":
        title_text = f"🔧 ATS Service Alert - {alert_data.status.title()}"
    
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": title_text
        }
    })
    
    # Alert details
    for alert in alert_data.alerts:
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})
        
        # Service information
        service = labels.get("service", labels.get("job", "Unknown"))
        instance = labels.get("instance", "N/A")
        
        # Build fields
        fields = [
            {
                "type": "mrkdwn",
                "text": f"*Service:*\n{service}"
            },
            {
                "type": "mrkdwn", 
                "text": f"*Severity:*\n{labels.get('severity', 'Unknown')}"
            }
        ]
        
        if instance != "N/A":
            fields.append({
                "type": "mrkdwn",
                "text": f"*Instance:*\n{instance}"
            })
        
        if "component" in labels:
            fields.append({
                "type": "mrkdwn",
                "text": f"*Component:*\n{labels['component']}"
            })
        
        blocks.append({
            "type": "section",
            "fields": fields
        })
        
        # Alert message
        summary = annotations.get("summary", "No summary available")
        description = annotations.get("description", "")
        
        alert_text = f"*Alert:* {summary}"
        if description and description != summary:
            alert_text += f"\n*Details:* {description}"
        
        # Current value and threshold if available
        if "current_value" in annotations:
            alert_text += f"\n*Current Value:* {annotations['current_value']}"
        if "threshold" in annotations:
            alert_text += f"\n*Threshold:* {annotations['threshold']}"
        
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": alert_text
            }
        })
        
        # Timing information
        starts_at = alert.get("startsAt", "")
        if starts_at:
            try:
                # Parse and format timestamp
                dt = datetime.fromisoformat(starts_at.replace('Z', '+00:00'))
                formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
                blocks.append({
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"⏰ *Started:* {formatted_time}"
                        }
                    ]
                })
            except:
                pass
        
        # Add divider between alerts
        if len(alert_data.alerts) > 1:
            blocks.append({"type": "divider"})
    
    # Add action buttons for critical alerts
    if alert_type == "critical":
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "View Grafana"
                    },
                    "url": "http://localhost:3000",  # Update with actual Grafana URL
                    "style": "primary"
                },
                {
                    "type": "button", 
                    "text": {
                        "type": "plain_text",
                        "text": "View Logs"
                    },
                    "url": "http://localhost:8080",  # Update with actual logs URL
                }
            ]
        })
    
    return {
        "text": f"ATS Alert: {alert_data.status}",
        "blocks": blocks
    }

async def send_to_slack(message: Dict[str, Any]) -> bool:
    """Send message to Slack webhook"""
    if not SLACK_WEBHOOK_URL or SLACK_WEBHOOK_URL == "PLACEHOLDER_SLACK_WEBHOOK_URL":
        logger.warning("Slack webhook URL not configured")
        return False
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                SLACK_WEBHOOK_URL,
                json=message,
                headers={"Content-Type": "application/json"},
                timeout=10.0
            )
            
            if response.status_code == 200:
                logger.info("Successfully sent alert to Slack")
                return True
            else:
                logger.error(f"Failed to send to Slack: {response.status_code} - {response.text}")
                return False
                
    except Exception as e:
        logger.error(f"Error sending to Slack: {str(e)}")
        return False

@app.get("/")
async def root():
    return {
        "service": "ATS Slack Webhook Proxy",
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "webhook_configured": bool(SLACK_WEBHOOK_URL and SLACK_WEBHOOK_URL != "PLACEHOLDER_SLACK_WEBHOOK_URL")
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "ATS Slack Webhook Proxy",
        "timestamp": datetime.now().isoformat(),
        "webhook_url_configured": bool(SLACK_WEBHOOK_URL and SLACK_WEBHOOK_URL != "PLACEHOLDER_SLACK_WEBHOOK_URL")
    }

@app.post("/critical")
async def critical_alerts(request: Request):
    """Handle critical alerts"""
    try:
        data = await request.json()
        alert_data = AlertManagerWebhook(**data)
        
        logger.info(f"Received critical alert: {alert_data.groupLabels}")
        
        slack_message = format_alert_for_slack(alert_data, "critical")
        success = await send_to_slack(slack_message)
        
        if not success:
            logger.error("Failed to send critical alert to Slack")
        
        return {"status": "received", "sent_to_slack": success}
        
    except Exception as e:
        logger.error(f"Error processing critical alert: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/warning")
async def warning_alerts(request: Request):
    """Handle warning alerts"""
    try:
        data = await request.json()
        alert_data = AlertManagerWebhook(**data)
        
        logger.info(f"Received warning alert: {alert_data.groupLabels}")
        
        slack_message = format_alert_for_slack(alert_data, "warning")
        success = await send_to_slack(slack_message)
        
        return {"status": "received", "sent_to_slack": success}
        
    except Exception as e:
        logger.error(f"Error processing warning alert: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/services")
async def service_alerts(request: Request):
    """Handle service-specific alerts"""
    try:
        data = await request.json()
        alert_data = AlertManagerWebhook(**data)
        
        logger.info(f"Received service alert: {alert_data.groupLabels}")
        
        slack_message = format_alert_for_slack(alert_data, "services")
        success = await send_to_slack(slack_message)
        
        return {"status": "received", "sent_to_slack": success}
        
    except Exception as e:
        logger.error(f"Error processing service alert: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/test")
async def test_slack():
    """Test Slack webhook connectivity"""
    test_message = {
        "text": "🧪 ATS Slack Integration Test",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*🧪 ATS Slack Webhook Test*\nThis is a test message to verify Slack integration is working correctly."
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Timestamp:*\n{datetime.now().isoformat()}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": "*Status:*\n✅ Webhook Proxy Active"
                    }
                ]
            }
        ]
    }
    
    success = await send_to_slack(test_message)
    
    return {
        "test_sent": success,
        "webhook_configured": bool(SLACK_WEBHOOK_URL and SLACK_WEBHOOK_URL != "PLACEHOLDER_SLACK_WEBHOOK_URL"),
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    logger.info(f"Starting ATS Slack Webhook Proxy on port {PORT}")
    logger.info(f"Slack webhook configured: {bool(SLACK_WEBHOOK_URL and SLACK_WEBHOOK_URL != 'PLACEHOLDER_SLACK_WEBHOOK_URL')}")
    
    uvicorn.run(app, host="0.0.0.0", port=PORT)