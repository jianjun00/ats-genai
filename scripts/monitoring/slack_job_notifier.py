#!/usr/bin/env python3
"""
Slack Job Status Notifier

Monitors Kubernetes jobs and sends Slack notifications for status changes:
- Job completion (success/failure)
- Job start notifications
- Long-running job progress updates
- Error alerts

Usage:
    python scripts/monitoring/slack_job_notifier.py --mode monitor
    python scripts/monitoring/slack_job_notifier.py --test-notification
"""

import asyncio
import aiohttp
import json
import os
import time
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
import subprocess

@dataclass
class JobStatus:
    name: str
    namespace: str
    status: str  # Running, Complete, Failed
    start_time: Optional[datetime]
    completion_time: Optional[datetime]
    duration: Optional[str]
    conditions: List[Dict]

class SlackJobNotifier:
    def __init__(self):
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        self.slack_channel = os.getenv('SLACK_CHANNEL', '#ats-dev-alerts')
        self.notification_interval = int(os.getenv('NOTIFICATION_INTERVAL_HOURS', '4'))
        
        if not self.slack_webhook_url:
            raise ValueError("SLACK_WEBHOOK_URL environment variable required")
        
        self.logger = logging.getLogger(__name__)
        self.tracked_jobs = {}  # Job name -> last notification time
        
        # Monitor all jobs (no pattern filtering)
        self.monitor_all_jobs = os.getenv('MONITOR_ALL_JOBS', 'true').lower() == 'true'
        
        # Job patterns to monitor (only used if MONITOR_ALL_JOBS=false)
        self.monitored_patterns = [
            'comprehensive-30year-minute-backfill',
            'polygon-30year-minute-backfill',
            'tiingo-30year-minute-backfill',
            'fmp-30year-minute-backfill',
            'eodhd-30year-minute-backfill',
            'minute-backfill',
            'price-unification',
            'market-data',
            'model-training'
        ]
        
        if self.monitor_all_jobs:
            logger.info("🔍 Monitoring ALL Kubernetes jobs for status changes")
        else:
            logger.info(f"🔍 Monitoring jobs matching patterns: {self.monitored_patterns}")
    
    async def send_slack_message(self, message: str, color: str = "good", 
                                title: str = "ATS Job Status", 
                                fields: List[Dict] = None) -> bool:
        """Send message to Slack"""
        
        if not fields:
            fields = []
        
        payload = {
            "channel": self.slack_channel,
            "username": "ATS Job Monitor",
            "icon_emoji": ":robot_face:",
            "attachments": [
                {
                    "color": color,
                    "title": title,
                    "text": message,
                    "fields": fields,
                    "footer": "ATS Kubernetes Cluster",
                    "ts": int(time.time())
                }
            ]
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.slack_webhook_url, json=payload) as response:
                    if response.status == 200:
                        self.logger.info(f"Slack notification sent: {title}")
                        return True
                    else:
                        self.logger.error(f"Slack notification failed: {response.status}")
                        return False
        except Exception as e:
            self.logger.error(f"Error sending Slack notification: {e}")
            return False
    
    def get_kubernetes_jobs(self, namespace: str = "ats-dev") -> List[JobStatus]:
        """Get current Kubernetes jobs"""
        
        try:
            # Get jobs in JSON format
            result = subprocess.run([
                'kubectl', 'get', 'jobs', '-n', namespace, '-o', 'json'
            ], capture_output=True, text=True, check=True)
            
            jobs_data = json.loads(result.stdout)
            job_statuses = []
            
            for job in jobs_data.get('items', []):
                metadata = job.get('metadata', {})
                status = job.get('status', {})
                
                job_name = metadata.get('name', 'unknown')
                start_time = None
                completion_time = None
                duration = None
                
                # Parse timestamps
                if status.get('startTime'):
                    start_time = datetime.fromisoformat(
                        status['startTime'].replace('Z', '+00:00')
                    )
                
                if status.get('completionTime'):
                    completion_time = datetime.fromisoformat(
                        status['completionTime'].replace('Z', '+00:00')
                    )
                    
                    if start_time:
                        duration = str(completion_time - start_time)
                
                # Determine job status
                conditions = status.get('conditions', [])
                job_status = "Running"
                
                if status.get('succeeded', 0) > 0:
                    job_status = "Complete"
                elif status.get('failed', 0) > 0:
                    job_status = "Failed"
                elif conditions:
                    # Check for specific conditions
                    for condition in conditions:
                        if condition.get('type') == 'Failed' and condition.get('status') == 'True':
                            job_status = "Failed"
                        elif condition.get('type') == 'Complete' and condition.get('status') == 'True':
                            job_status = "Complete"
                
                job_statuses.append(JobStatus(
                    name=job_name,
                    namespace=namespace,
                    status=job_status,
                    start_time=start_time,
                    completion_time=completion_time,
                    duration=duration,
                    conditions=conditions
                ))
            
            return job_statuses
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error getting Kubernetes jobs: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error parsing job data: {e}")
            return []
    
    def should_notify_about_job(self, job: JobStatus) -> bool:
        """Determine if we should send a notification about this job"""
        
        # If monitoring all jobs, skip pattern check
        if not self.monitor_all_jobs:
            # Check if job matches our monitoring patterns
            matches_pattern = any(
                pattern in job.name for pattern in self.monitored_patterns
            )
            
            if not matches_pattern:
                return False
        
        # Always notify on completion or failure
        if job.status in ['Complete', 'Failed']:
            return True
        
        # For running jobs, check if enough time has passed since last notification
        if job.status == 'Running':
            last_notification = self.tracked_jobs.get(job.name)
            if not last_notification:
                return True  # First time seeing this job
            
            time_since_last = datetime.now() - last_notification
            return time_since_last >= timedelta(hours=self.notification_interval)
        
        return False
    
    async def send_job_notification(self, job: JobStatus, is_update: bool = False):
        """Send notification for a specific job"""
        
        # Determine message color and emoji
        if job.status == 'Complete':
            color = "good"
            emoji = "✅"
            status_text = "completed successfully"
        elif job.status == 'Failed':
            color = "danger"
            emoji = "❌"
            status_text = "failed"
        else:  # Running
            color = "warning"
            emoji = "🔄"
            status_text = "is running" if not is_update else "progress update"
        
        # Build message
        title = f"{emoji} Job {status_text.title()}: {job.name}"
        
        if is_update:
            message = f"Job `{job.name}` is still running and making progress."
        else:
            message = f"Job `{job.name}` {status_text}."
        
        # Build fields with job details
        fields = [
            {
                "title": "Namespace",
                "value": job.namespace,
                "short": True
            },
            {
                "title": "Status",
                "value": job.status,
                "short": True
            }
        ]
        
        if job.start_time:
            fields.append({
                "title": "Started",
                "value": job.start_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "short": True
            })
        
        if job.completion_time:
            fields.append({
                "title": "Completed",
                "value": job.completion_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "short": True
            })
        
        if job.duration:
            fields.append({
                "title": "Duration",
                "value": job.duration,
                "short": True
            })
        
        # Add progress info for long-running jobs
        if job.status == 'Running' and job.start_time:
            running_time = datetime.now() - job.start_time.replace(tzinfo=None)
            fields.append({
                "title": "Running Time",
                "value": str(running_time).split('.')[0],  # Remove microseconds
                "short": True
            })
        
        # Send notification
        await self.send_slack_message(
            message=message,
            color=color,
            title=title,
            fields=fields
        )
        
        # Update tracking
        self.tracked_jobs[job.name] = datetime.now()
    
    async def monitor_jobs(self):
        """Main monitoring loop"""
        
        self.logger.info("Starting Kubernetes job monitoring...")
        
        # Send startup notification
        await self.send_slack_message(
            message="🚀 ATS Job Monitor started - monitoring Kubernetes jobs for status changes",
            color="good",
            title="Job Monitor Started"
        )
        
        previous_jobs = {}
        
        while True:
            try:
                # Get current jobs
                current_jobs = self.get_kubernetes_jobs()
                current_job_map = {job.name: job for job in current_jobs}
                
                # Check for status changes and new jobs
                for job_name, job in current_job_map.items():
                    previous_job = previous_jobs.get(job_name)
                    
                    if not previous_job:
                        # New job detected
                        if self.should_notify_about_job(job):
                            await self.send_job_notification(job)
                    
                    elif previous_job.status != job.status:
                        # Status changed
                        if self.should_notify_about_job(job):
                            await self.send_job_notification(job)
                    
                    elif job.status == 'Running' and self.should_notify_about_job(job):
                        # Running job progress update
                        await self.send_job_notification(job, is_update=True)
                
                # Update previous jobs
                previous_jobs = current_job_map
                
                # Wait before next check
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(30)  # Shorter wait on error
    
    async def test_notification(self):
        """Send a test notification"""
        
        test_fields = [
            {"title": "Test Type", "value": "Slack Integration Test", "short": True},
            {"title": "Timestamp", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "short": True}
        ]
        
        success = await self.send_slack_message(
            message="This is a test notification to verify Slack integration is working correctly.",
            color="good",
            title="🧪 Test Notification",
            fields=test_fields
        )
        
        if success:
            print("✅ Test notification sent successfully!")
        else:
            print("❌ Test notification failed!")
        
        return success

async def main():
    parser = argparse.ArgumentParser(description='Kubernetes Job Slack Notifier')
    parser.add_argument('--mode', choices=['monitor', 'test'], default='monitor',
                       help='Mode: monitor jobs or send test notification')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        notifier = SlackJobNotifier()
        
        if args.mode == 'test':
            await notifier.test_notification()
        else:
            await notifier.monitor_jobs()
            
    except KeyboardInterrupt:
        print("\n⏹️  Monitoring stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())