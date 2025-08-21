#!/usr/bin/env python3
"""
Demo Slack Notifications

Shows what the Slack notifications would look like for different job status changes.
This demo script simulates the notification payloads without sending to Slack.
"""

import json
from datetime import datetime, timedelta

def demo_notification_payload(job_name: str, status: str, start_time: datetime, 
                             completion_time: datetime = None, is_update: bool = False):
    """Generate demo notification payload"""
    
    # Determine message color and emoji
    if status == 'Complete':
        color = "good"
        emoji = "✅"
        status_text = "completed successfully"
    elif status == 'Failed':
        color = "danger"
        emoji = "❌"
        status_text = "failed"
    else:  # Running
        color = "warning"
        emoji = "🔄"
        status_text = "is running" if not is_update else "progress update"
    
    title = f"{emoji} Job {status_text.title()}: {job_name}"
    
    if is_update:
        message = f"Job `{job_name}` is still running and making progress."
    else:
        message = f"Job `{job_name}` {status_text}."
    
    # Build fields
    fields = [
        {"title": "Namespace", "value": "ats-dev", "short": True},
        {"title": "Status", "value": status, "short": True},
        {"title": "Started", "value": start_time.strftime("%Y-%m-%d %H:%M:%S UTC"), "short": True}
    ]
    
    if completion_time:
        fields.append({
            "title": "Completed",
            "value": completion_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "short": True
        })
        
        duration = completion_time - start_time
        fields.append({
            "title": "Duration",
            "value": str(duration).split('.')[0],  # Remove microseconds
            "short": True
        })
    else:
        running_time = datetime.now() - start_time
        fields.append({
            "title": "Running Time",
            "value": str(running_time).split('.')[0],
            "short": True
        })
    
    payload = {
        "channel": "#ats-dev-alerts",
        "username": "ATS Job Monitor",
        "icon_emoji": ":robot_face:",
        "attachments": [
            {
                "color": color,
                "title": title,
                "text": message,
                "fields": fields,
                "footer": "ATS Kubernetes Cluster",
                "ts": int(datetime.now().timestamp())
            }
        ]
    }
    
    return payload

def main():
    print("🔔 Slack Notification Demo for ATS Job Monitoring")
    print("=" * 60)
    
    # Demo timestamps
    now = datetime.now()
    start_time = now - timedelta(hours=33, minutes=15)
    completion_time = now
    
    # Demo notifications
    scenarios = [
        {
            "description": "Job Start Notification",
            "job_name": "comprehensive-30year-backfill",
            "status": "Running",
            "start_time": start_time,
            "is_update": False
        },
        {
            "description": "Progress Update (after 4 hours)",
            "job_name": "comprehensive-30year-backfill", 
            "status": "Running",
            "start_time": start_time,
            "is_update": True
        },
        {
            "description": "Job Completion",
            "job_name": "comprehensive-30year-backfill",
            "status": "Complete",
            "start_time": start_time,
            "completion_time": completion_time
        },
        {
            "description": "Job Failure",
            "job_name": "market-data-sync",
            "status": "Failed",
            "start_time": now - timedelta(minutes=15),
            "completion_time": now
        }
    ]
    
    for scenario in scenarios:
        print(f"\n📋 {scenario['description']}")
        print("-" * 40)
        
        payload = demo_notification_payload(
            job_name=scenario['job_name'],
            status=scenario['status'],
            start_time=scenario['start_time'],
            completion_time=scenario.get('completion_time'),
            is_update=scenario.get('is_update', False)
        )
        
        # Show what the Slack message would look like
        attachment = payload['attachments'][0]
        print(f"Channel: {payload['channel']}")
        print(f"Title: {attachment['title']}")
        print(f"Message: {attachment['text']}")
        print(f"Color: {attachment['color']}")
        print("Fields:")
        for field in attachment['fields']:
            print(f"  {field['title']}: {field['value']}")
    
    print(f"\n🚀 Setup Instructions:")
    print("=" * 60)
    print("1. Create Slack webhook URL in your workspace")
    print("2. Run: ./scripts/monitoring/setup_slack_notifications.sh --webhook-url 'YOUR_URL'")
    print("3. Deploy: ./scripts/monitoring/setup_slack_notifications.sh --deploy")
    print("4. Test: ./scripts/monitoring/setup_slack_notifications.sh --test")
    
    print(f"\n📊 Current Job Being Monitored:")
    print("=" * 60)
    print("Job: comprehensive-30year-backfill")
    print("Status: Running (33+ hours)")
    print("Progress: 14.3% complete")
    print("Records: 206,732 inserted")
    print("Estimated completion: ~6 more days")
    
    print(f"\n✅ Benefits of Slack Notifications:")
    print("=" * 60)
    print("• Real-time job completion alerts")
    print("• Progress updates for long-running jobs")
    print("• Immediate failure notifications")
    print("• Team collaboration around data operations")
    print("• Historical tracking of job performance")

if __name__ == "__main__":
    main()