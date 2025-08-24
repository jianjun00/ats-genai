#!/usr/bin/env python
"""
Flyte Workflow for Kubernetes Job Notifications

This script defines a Flyte workflow for sending notifications about Kubernetes job status changes.
It provides a scalable way to handle notifications for job events across multiple namespaces.
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

import flytekit
from flytekit import task, workflow, dynamic

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('job_notification')

@task
def send_slack_notification(
    job_name: str,
    namespace: str,
    status: str,
    start_time: Optional[str] = None,
    completion_time: Optional[str] = None,
    duration: Optional[str] = None,
    active_pods: int = 0,
    succeeded_pods: int = 0,
    failed_pods: int = 0,
    webhook_url: Optional[str] = None,
    channel: str = "#ats-dev-alerts"
) -> Dict[str, Any]:
    """
    Send a Slack notification about a job status change.
    
    Args:
        job_name: Name of the job
        namespace: Kubernetes namespace
        status: Job status (Running, Complete, Failed, Pending)
        start_time: Job start time (ISO format)
        completion_time: Job completion time (ISO format)
        duration: Job duration
        active_pods: Number of active pods
        succeeded_pods: Number of succeeded pods
        failed_pods: Number of failed pods
        webhook_url: Slack webhook URL (if not provided, will use env var)
        channel: Slack channel to send notification to
        
    Returns:
        Dictionary with notification result
    """
    import aiohttp
    import asyncio
    import time
    
    async def send_to_slack(payload):
        slack_url = webhook_url or os.environ.get('SLACK_WEBHOOK_URL')
        if not slack_url:
            logger.warning("No Slack webhook URL provided - notification not sent")
            return {"success": False, "error": "No webhook URL provided"}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(slack_url, json=payload, timeout=10) as response:
                    if response.status == 200:
                        logger.info(f"✅ Slack notification sent for job {job_name}")
                        return {"success": True}
                    else:
                        error_msg = f"Failed to send Slack notification: {response.status}"
                        logger.error(error_msg)
                        return {"success": False, "error": error_msg}
        except Exception as e:
            error_msg = f"Error sending Slack notification: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
    
    # Determine message color and emoji based on status
    if status == 'Complete':
        color = "good"
        emoji = "✅"
        status_text = "completed successfully"
    elif status == 'Failed':
        color = "danger"
        emoji = "❌"
        status_text = "failed"
    elif status == 'Running':
        color = "warning"
        emoji = "🔄"
        status_text = "is running"
    else:  # Pending
        color = "#808080"
        emoji = "⏳"
        status_text = "is pending"
    
    # Build title and message
    title = f"{emoji} Job Status: {namespace}/{job_name}"
    message = f"Job `{job_name}` in namespace `{namespace}` {status_text}."
    
    # Build fields with job details
    fields = [
        {"title": "Namespace", "value": namespace, "short": True},
        {"title": "Status", "value": status, "short": True}
    ]
    
    if active_pods > 0:
        fields.append({"title": "Active Pods", "value": str(active_pods), "short": True})
    
    if succeeded_pods > 0:
        fields.append({"title": "Succeeded", "value": str(succeeded_pods), "short": True})
        
    if failed_pods > 0:
        fields.append({"title": "Failed", "value": str(failed_pods), "short": True})
    
    if start_time:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        fields.append({
            "title": "Started",
            "value": start_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "short": True
        })
    
    if completion_time:
        completion_dt = datetime.fromisoformat(completion_time.replace('Z', '+00:00'))
        fields.append({
            "title": "Completed",
            "value": completion_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "short": True
        })
    
    if duration:
        fields.append({
            "title": "Duration",
            "value": duration.split('.')[0],
            "short": True
        })
    
    # Create payload
    payload = {
        "channel": channel,
        "username": "Universal Job Monitor",
        "icon_emoji": ":kubernetes:",
        "attachments": [{
            "color": color,
            "title": title,
            "text": message,
            "fields": fields,
            "footer": "Kubernetes Universal Job Monitor via Flyte",
            "ts": int(time.time())
        }]
    }
    
    # Send notification
    result = asyncio.run(send_to_slack(payload))
    
    # Add metadata to result
    result.update({
        "job_name": job_name,
        "namespace": namespace,
        "status": status,
        "timestamp": datetime.now().isoformat()
    })
    
    return result


@task
def send_email_notification(
    job_name: str,
    namespace: str,
    status: str,
    recipients: List[str],
    start_time: Optional[str] = None,
    completion_time: Optional[str] = None,
    duration: Optional[str] = None,
    active_pods: int = 0,
    succeeded_pods: int = 0,
    failed_pods: int = 0,
    smtp_server: str = "smtp.gmail.com",
    smtp_port: int = 587,
    smtp_username: Optional[str] = None,
    smtp_password: Optional[str] = None
) -> Dict[str, Any]:
    """
    Send an email notification about a job status change.
    
    Args:
        job_name: Name of the job
        namespace: Kubernetes namespace
        status: Job status (Running, Complete, Failed, Pending)
        recipients: List of email recipients
        start_time: Job start time (ISO format)
        completion_time: Job completion time (ISO format)
        duration: Job duration
        active_pods: Number of active pods
        succeeded_pods: Number of succeeded pods
        failed_pods: Number of failed pods
        smtp_server: SMTP server
        smtp_port: SMTP port
        smtp_username: SMTP username (if not provided, will use env var)
        smtp_password: SMTP password (if not provided, will use env var)
        
    Returns:
        Dictionary with notification result
    """
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    # Get SMTP credentials from environment if not provided
    username = smtp_username or os.environ.get('SMTP_USERNAME')
    password = smtp_password or os.environ.get('SMTP_PASSWORD')
    
    if not username or not password:
        logger.warning("No SMTP credentials provided - email notification not sent")
        return {
            "success": False, 
            "error": "No SMTP credentials provided",
            "job_name": job_name,
            "namespace": namespace,
            "status": status
        }
    
    # Determine status text and subject prefix
    if status == 'Complete':
        status_text = "completed successfully"
        subject_prefix = "✅"
    elif status == 'Failed':
        status_text = "failed"
        subject_prefix = "❌"
    elif status == 'Running':
        status_text = "is running"
        subject_prefix = "🔄"
    else:  # Pending
        status_text = "is pending"
        subject_prefix = "⏳"
    
    # Create email subject and body
    subject = f"{subject_prefix} Kubernetes Job {namespace}/{job_name} {status_text}"
    
    # Build email body
    body = f"""
    <html>
    <body>
    <h2>Kubernetes Job Status Update</h2>
    <p>Job <strong>{job_name}</strong> in namespace <strong>{namespace}</strong> {status_text}.</p>
    
    <h3>Job Details:</h3>
    <ul>
        <li><strong>Status:</strong> {status}</li>
    """
    
    if start_time:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        body += f"<li><strong>Started:</strong> {start_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}</li>"
    
    if completion_time:
        completion_dt = datetime.fromisoformat(completion_time.replace('Z', '+00:00'))
        body += f"<li><strong>Completed:</strong> {completion_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}</li>"
    
    if duration:
        body += f"<li><strong>Duration:</strong> {duration.split('.')[0]}</li>"
    
    if active_pods > 0:
        body += f"<li><strong>Active Pods:</strong> {active_pods}</li>"
    
    if succeeded_pods > 0:
        body += f"<li><strong>Succeeded Pods:</strong> {succeeded_pods}</li>"
    
    if failed_pods > 0:
        body += f"<li><strong>Failed Pods:</strong> {failed_pods}</li>"
    
    body += """
    </ul>
    <p>This notification was sent by the Universal Job Monitor via Flyte.</p>
    </body>
    </html>
    """
    
    # Create message
    msg = MIMEMultipart()
    msg['From'] = username
    msg['To'] = ', '.join(recipients)
    msg['Subject'] = subject
    
    # Attach HTML body
    msg.attach(MIMEText(body, 'html'))
    
    try:
        # Connect to SMTP server
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(username, password)
        
        # Send email
        server.send_message(msg)
        server.quit()
        
        logger.info(f"✅ Email notification sent for job {job_name} to {len(recipients)} recipients")
        return {
            "success": True,
            "job_name": job_name,
            "namespace": namespace,
            "status": status,
            "recipients": recipients,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        error_msg = f"Error sending email notification: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "job_name": job_name,
            "namespace": namespace,
            "status": status
        }


@task
def send_webhook_notification(
    job_name: str,
    namespace: str,
    status: str,
    webhook_url: str,
    start_time: Optional[str] = None,
    completion_time: Optional[str] = None,
    duration: Optional[str] = None,
    active_pods: int = 0,
    succeeded_pods: int = 0,
    failed_pods: int = 0,
    custom_headers: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Send a webhook notification about a job status change.
    
    Args:
        job_name: Name of the job
        namespace: Kubernetes namespace
        status: Job status (Running, Complete, Failed, Pending)
        webhook_url: Webhook URL
        start_time: Job start time (ISO format)
        completion_time: Job completion time (ISO format)
        duration: Job duration
        active_pods: Number of active pods
        succeeded_pods: Number of succeeded pods
        failed_pods: Number of failed pods
        custom_headers: Custom headers to include in the request
        
    Returns:
        Dictionary with notification result
    """
    import aiohttp
    import asyncio
    
    async def send_to_webhook(url, payload, headers):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers, timeout=10) as response:
                    if response.status in (200, 201, 202):
                        logger.info(f"✅ Webhook notification sent for job {job_name}")
                        return {"success": True, "status_code": response.status}
                    else:
                        error_msg = f"Failed to send webhook notification: {response.status}"
                        logger.error(error_msg)
                        return {"success": False, "error": error_msg, "status_code": response.status}
        except Exception as e:
            error_msg = f"Error sending webhook notification: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
    
    # Create payload
    payload = {
        "job_name": job_name,
        "namespace": namespace,
        "status": status,
        "timestamp": datetime.now().isoformat()
    }
    
    if start_time:
        payload["start_time"] = start_time
    
    if completion_time:
        payload["completion_time"] = completion_time
    
    if duration:
        payload["duration"] = duration
    
    if active_pods > 0:
        payload["active_pods"] = active_pods
    
    if succeeded_pods > 0:
        payload["succeeded_pods"] = succeeded_pods
    
    if failed_pods > 0:
        payload["failed_pods"] = failed_pods
    
    # Set headers
    headers = {
        "Content-Type": "application/json"
    }
    
    if custom_headers:
        headers.update(custom_headers)
    
    # Send notification
    result = asyncio.run(send_to_webhook(webhook_url, payload, headers))
    
    # Add metadata to result
    result.update({
        "job_name": job_name,
        "namespace": namespace,
        "status": status,
        "timestamp": datetime.now().isoformat()
    })
    
    return result


@workflow
def job_notification_workflow(
    job_name: str,
    namespace: str,
    status: str,
    notification_types: List[str] = ["slack"],
    start_time: Optional[str] = None,
    completion_time: Optional[str] = None,
    duration: Optional[str] = None,
    active_pods: int = 0,
    succeeded_pods: int = 0,
    failed_pods: int = 0,
    slack_webhook_url: Optional[str] = None,
    slack_channel: str = "#ats-dev-alerts",
    email_recipients: Optional[List[str]] = None,
    webhook_url: Optional[str] = None,
    webhook_headers: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Workflow for sending notifications about job status changes.
    
    Args:
        job_name: Name of the job
        namespace: Kubernetes namespace
        status: Job status (Running, Complete, Failed, Pending)
        notification_types: List of notification types to send (slack, email, webhook)
        start_time: Job start time (ISO format)
        completion_time: Job completion time (ISO format)
        duration: Job duration
        active_pods: Number of active pods
        succeeded_pods: Number of succeeded pods
        failed_pods: Number of failed pods
        slack_webhook_url: Slack webhook URL
        slack_channel: Slack channel to send notification to
        email_recipients: List of email recipients
        webhook_url: Webhook URL
        webhook_headers: Custom headers for webhook
        
    Returns:
        Dictionary with notification results
    """
    results = {}
    
    # Send Slack notification if requested
    if "slack" in notification_types:
        slack_result = send_slack_notification(
            job_name=job_name,
            namespace=namespace,
            status=status,
            start_time=start_time,
            completion_time=completion_time,
            duration=duration,
            active_pods=active_pods,
            succeeded_pods=succeeded_pods,
            failed_pods=failed_pods,
            webhook_url=slack_webhook_url,
            channel=slack_channel
        )
        results["slack"] = slack_result
    
    # Send email notification if requested
    if "email" in notification_types and email_recipients:
        email_result = send_email_notification(
            job_name=job_name,
            namespace=namespace,
            status=status,
            recipients=email_recipients,
            start_time=start_time,
            completion_time=completion_time,
            duration=duration,
            active_pods=active_pods,
            succeeded_pods=succeeded_pods,
            failed_pods=failed_pods
        )
        results["email"] = email_result
    
    # Send webhook notification if requested
    if "webhook" in notification_types and webhook_url:
        webhook_result = send_webhook_notification(
            job_name=job_name,
            namespace=namespace,
            status=status,
            webhook_url=webhook_url,
            start_time=start_time,
            completion_time=completion_time,
            duration=duration,
            active_pods=active_pods,
            succeeded_pods=succeeded_pods,
            failed_pods=failed_pods,
            custom_headers=webhook_headers
        )
        results["webhook"] = webhook_result
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Send notifications for Kubernetes job status changes")
    parser.add_argument('--job-name', type=str, required=True, help='Name of the job')
    parser.add_argument('--namespace', type=str, required=True, help='Kubernetes namespace')
    parser.add_argument('--status', type=str, required=True, choices=['Running', 'Complete', 'Failed', 'Pending'],
                        help='Job status')
    parser.add_argument('--notification-types', type=str, default="slack",
                        help='Comma-separated list of notification types (slack,email,webhook)')
    parser.add_argument('--start-time', type=str, help='Job start time (ISO format)')
    parser.add_argument('--completion-time', type=str, help='Job completion time (ISO format)')
    parser.add_argument('--duration', type=str, help='Job duration')
    parser.add_argument('--active-pods', type=int, default=0, help='Number of active pods')
    parser.add_argument('--succeeded-pods', type=int, default=0, help='Number of succeeded pods')
    parser.add_argument('--failed-pods', type=int, default=0, help='Number of failed pods')
    parser.add_argument('--slack-webhook-url', type=str, help='Slack webhook URL')
    parser.add_argument('--slack-channel', type=str, default="#ats-dev-alerts", help='Slack channel')
    parser.add_argument('--email-recipients', type=str, help='Comma-separated list of email recipients')
    parser.add_argument('--webhook-url', type=str, help='Webhook URL')
    
    args = parser.parse_args()
    
    # Parse notification types
    notification_types = [nt.strip() for nt in args.notification_types.split(',')]
    
    # Parse email recipients if provided
    email_recipients = None
    if args.email_recipients:
        email_recipients = [email.strip() for email in args.email_recipients.split(',')]
    
    # Run the workflow
    result = job_notification_workflow(
        job_name=args.job_name,
        namespace=args.namespace,
        status=args.status,
        notification_types=notification_types,
        start_time=args.start_time,
        completion_time=args.completion_time,
        duration=args.duration,
        active_pods=args.active_pods,
        succeeded_pods=args.succeeded_pods,
        failed_pods=args.failed_pods,
        slack_webhook_url=args.slack_webhook_url,
        slack_channel=args.slack_channel,
        email_recipients=email_recipients,
        webhook_url=args.webhook_url
    )
    
    print(json.dumps(result, indent=2))
