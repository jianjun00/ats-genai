#!/usr/bin/env python3
"""
Universal Kubernetes Job Monitor with Slack Notifications

Monitors ALL Kubernetes jobs across all namespaces and sends Slack notifications
for status changes. This is a comprehensive monitoring solution for all jobs.

Usage:
    python monitor_all_k8s_jobs.py --mode monitor
    python monitor_all_k8s_jobs.py --test-notification
    python monitor_all_k8s_jobs.py --status-report
"""

import asyncio
import aiohttp
import json
import os
import time
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
import subprocess

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class JobStatus:
    name: str
    namespace: str
    status: str  # Running, Complete, Failed, Pending
    start_time: Optional[datetime]
    completion_time: Optional[datetime]
    duration: Optional[str]
    conditions: List[Dict]
    active_pods: int = 0
    succeeded_pods: int = 0
    failed_pods: int = 0

class UniversalJobMonitor:
    """Monitors all Kubernetes jobs across all namespaces."""
    
    def __init__(self):
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        self.slack_channel = os.getenv('SLACK_CHANNEL', '#ats-dev-alerts')
        self.notification_interval = int(os.getenv('NOTIFICATION_INTERVAL_HOURS', '4'))
        self.namespaces = self._get_monitored_namespaces()
        
        if not self.slack_webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not configured - notifications disabled")
        else:
            logger.info(f"Slack notifications enabled for channel: {self.slack_channel}")
        
        self.tracked_jobs = {}  # Job name -> last notification time
        self.previous_jobs = {}  # Previous job states
        
        logger.info(f"🔍 Monitoring ALL jobs in namespaces: {', '.join(self.namespaces)}")
    
    def _get_monitored_namespaces(self) -> List[str]:
        """Get list of namespaces to monitor."""
        # Default namespaces to monitor
        default_namespaces = ['ats-dev', 'ats-intg', 'ats-prod', 'default', 'kube-system']
        
        # Check if specific namespaces are configured
        env_namespaces = os.getenv('MONITOR_NAMESPACES', '').strip()
        if env_namespaces:
            return [ns.strip() for ns in env_namespaces.split(',') if ns.strip()]
        
        # Get all available namespaces if configured to do so
        monitor_all_ns = os.getenv('MONITOR_ALL_NAMESPACES', 'false').lower() == 'true'
        if monitor_all_ns:
            try:
                result = subprocess.run(['kubectl', 'get', 'namespaces', '-o', 'name'], 
                                      capture_output=True, text=True, check=True)
                return [ns.replace('namespace/', '') for ns in result.stdout.strip().split('\n') if ns]
            except Exception as e:
                logger.error(f"Failed to get all namespaces: {e}")
        
        return default_namespaces
    
    async def send_slack_message(self, message: str, color: str = "good", 
                                title: str = "Kubernetes Job Status", 
                                fields: List[Dict] = None) -> bool:
        """Send message to Slack."""
        
        if not self.slack_webhook_url:
            logger.debug(f"Slack disabled - would send: {title}: {message}")
            return True
        
        if not fields:
            fields = []
        
        payload = {
            "channel": self.slack_channel,
            "username": "ATS Job Monitor",
            "icon_emoji": ":kubernetes:",
            "attachments": [
                {
                    "color": color,
                    "title": title,
                    "text": message,
                    "fields": fields,
                    "footer": "Kubernetes Job Monitor",
                    "ts": int(time.time())
                }
            ]
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.slack_webhook_url, json=payload, timeout=10) as response:
                    if response.status == 200:
                        logger.info(f"✅ Slack notification sent: {title}")
                        return True
                    else:
                        logger.error(f"❌ Slack notification failed: {response.status}")
                        return False
        except Exception as e:
            logger.error(f"❌ Error sending Slack notification: {e}")
            return False
    
    def get_jobs_in_namespace(self, namespace: str) -> List[JobStatus]:
        """Get all jobs in a specific namespace."""
        
        try:
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
                job_status = "Pending"
                
                active = status.get('active', 0)
                succeeded = status.get('succeeded', 0)
                failed = status.get('failed', 0)
                
                if succeeded > 0:
                    job_status = "Complete"
                elif failed > 0:
                    job_status = "Failed"
                elif active > 0:
                    job_status = "Running"
                
                job_statuses.append(JobStatus(
                    name=job_name,
                    namespace=namespace,
                    status=job_status,
                    start_time=start_time,
                    completion_time=completion_time,
                    duration=duration,
                    conditions=conditions,
                    active_pods=active,
                    succeeded_pods=succeeded,
                    failed_pods=failed
                ))
            
            return job_statuses
            
        except subprocess.CalledProcessError as e:
            if "NotFound" in e.stderr:
                logger.debug(f"No jobs found in namespace {namespace}")
            else:
                logger.error(f"Error getting jobs in namespace {namespace}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error parsing job data for namespace {namespace}: {e}")
            return []
    
    def get_all_jobs(self) -> List[JobStatus]:
        """Get all jobs across all monitored namespaces."""
        all_jobs = []
        
        for namespace in self.namespaces:
            jobs = self.get_jobs_in_namespace(namespace)
            all_jobs.extend(jobs)
        
        return all_jobs
    
    def should_notify_about_job(self, job: JobStatus) -> bool:
        """Determine if we should send a notification about this job."""
        
        # Always notify on completion or failure
        if job.status in ['Complete', 'Failed']:
            return True
        
        # For running jobs, check if enough time has passed since last notification
        if job.status == 'Running':
            job_key = f"{job.namespace}/{job.name}"
            last_notification = self.tracked_jobs.get(job_key)
            if not last_notification:
                return True  # First time seeing this job
            
            time_since_last = datetime.now() - last_notification
            return time_since_last >= timedelta(hours=self.notification_interval)
        
        return False
    
    async def send_job_notification(self, job: JobStatus, is_status_change: bool = True):
        """Send notification for a specific job."""
        
        # Determine message color and emoji
        if job.status == 'Complete':
            color = "good"
            emoji = "✅"
            status_text = "completed successfully"
        elif job.status == 'Failed':
            color = "danger"
            emoji = "❌"
            status_text = "failed"
        elif job.status == 'Running':
            color = "warning"
            emoji = "🔄"
            status_text = "is running"
        else:  # Pending
            color = "#808080"
            emoji = "⏳"
            status_text = "is pending"
        
        # Build title and message
        action = "status changed" if is_status_change else "progress update"
        title = f"{emoji} Job {action.title()}: {job.name}"
        message = f"Job `{job.name}` in namespace `{job.namespace}` {status_text}."
        
        # Build fields with job details
        fields = [
            {"title": "Namespace", "value": job.namespace, "short": True},
            {"title": "Status", "value": job.status, "short": True}
        ]
        
        if job.active_pods > 0:
            fields.append({"title": "Active Pods", "value": str(job.active_pods), "short": True})
        
        if job.succeeded_pods > 0:
            fields.append({"title": "Succeeded Pods", "value": str(job.succeeded_pods), "short": True})
            
        if job.failed_pods > 0:
            fields.append({"title": "Failed Pods", "value": str(job.failed_pods), "short": True})
        
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
                "value": job.duration.split('.')[0],  # Remove microseconds
                "short": True
            })
        
        # Add running time for active jobs
        if job.status == 'Running' and job.start_time:
            running_time = datetime.now() - job.start_time.replace(tzinfo=None)
            fields.append({
                "title": "Running Time",
                "value": str(running_time).split('.')[0],
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
        job_key = f"{job.namespace}/{job.name}"
        self.tracked_jobs[job_key] = datetime.now()
    
    async def generate_status_report(self):
        """Generate and send a comprehensive status report."""
        jobs = self.get_all_jobs()
        
        if not jobs:
            await self.send_slack_message(
                message="No Kubernetes jobs found across all monitored namespaces.",
                color="#808080",
                title="📊 Job Status Report"
            )
            return
        
        # Categorize jobs by status
        running = [j for j in jobs if j.status == 'Running']
        completed = [j for j in jobs if j.status == 'Complete']
        failed = [j for j in jobs if j.status == 'Failed']
        pending = [j for j in jobs if j.status == 'Pending']
        
        # Group by namespace
        namespaces = {}
        for job in jobs:
            if job.namespace not in namespaces:
                namespaces[job.namespace] = {'running': 0, 'completed': 0, 'failed': 0, 'pending': 0}
            namespaces[job.namespace][job.status.lower()] += 1
        
        # Build message
        message_parts = [
            f"📊 **Kubernetes Jobs Status Report**",
            f"",
            f"**Overall Summary:**",
            f"• 🔄 Running: {len(running)}",
            f"• ✅ Completed: {len(completed)}",
            f"• ❌ Failed: {len(failed)}",
            f"• ⏳ Pending: {len(pending)}",
            f"",
            f"**By Namespace:**"
        ]
        
        for ns, counts in namespaces.items():
            total = sum(counts.values())
            message_parts.append(f"• `{ns}`: {total} jobs ({counts['running']} running, {counts['completed']} completed, {counts['failed']} failed, {counts['pending']} pending)")
        
        # Add recently completed/failed jobs
        if completed or failed:
            message_parts.extend(["", "**Recent Activity:**"])
            recent_jobs = sorted(
                [j for j in completed + failed if j.completion_time], 
                key=lambda x: x.completion_time, 
                reverse=True
            )[:5]  # Last 5 jobs
            
            for job in recent_jobs:
                status_emoji = "✅" if job.status == 'Complete' else "❌"
                duration = job.duration.split('.')[0] if job.duration else "N/A"
                message_parts.append(f"• {status_emoji} `{job.namespace}/{job.name}` - {duration}")
        
        message = "\n".join(message_parts)
        
        fields = [
            {"title": "Total Jobs", "value": str(len(jobs)), "short": True},
            {"title": "Namespaces", "value": str(len(namespaces)), "short": True},
            {"title": "Running", "value": str(len(running)), "short": True},
            {"title": "Completed", "value": str(len(completed)), "short": True}
        ]
        
        await self.send_slack_message(
            message=message,
            color="good" if not failed else "warning",
            title="📊 Kubernetes Jobs Status Report",
            fields=fields
        )
    
    async def monitor_jobs(self):
        """Main monitoring loop."""
        
        logger.info("🚀 Starting universal Kubernetes job monitoring...")
        
        # Send startup notification
        await self.send_slack_message(
            message=f"🚀 Universal Kubernetes Job Monitor started\n\nMonitoring namespaces: {', '.join(self.namespaces)}\n\nWill notify on all job status changes and provide periodic updates.",
            color="good",
            title="🚀 Job Monitor Started"
        )
        
        while True:
            try:
                # Get current jobs
                current_jobs = self.get_all_jobs()
                current_job_map = {f"{job.namespace}/{job.name}": job for job in current_jobs}
                
                # Check for status changes and new jobs
                for job_key, job in current_job_map.items():
                    previous_job = self.previous_jobs.get(job_key)
                    
                    if not previous_job:
                        # New job detected
                        if self.should_notify_about_job(job):
                            await self.send_job_notification(job, is_status_change=False)
                    
                    elif previous_job.status != job.status:
                        # Status changed
                        await self.send_job_notification(job, is_status_change=True)
                    
                    elif job.status == 'Running' and self.should_notify_about_job(job):
                        # Running job progress update
                        await self.send_job_notification(job, is_status_change=False)
                
                # Update previous jobs
                self.previous_jobs = current_job_map
                
                # Log summary
                running = sum(1 for job in current_jobs if job.status == 'Running')
                completed = sum(1 for job in current_jobs if job.status == 'Complete')
                failed = sum(1 for job in current_jobs if job.status == 'Failed')
                logger.info(f"📊 Current jobs: {len(current_jobs)} total ({running} running, {completed} completed, {failed} failed)")
                
                # Wait before next check
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(30)  # Shorter wait on error
    
    async def test_notification(self):
        """Send a test notification."""
        
        test_fields = [
            {"title": "Test Type", "value": "Universal Job Monitor", "short": True},
            {"title": "Monitored Namespaces", "value": ', '.join(self.namespaces), "short": True},
            {"title": "Timestamp", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "short": True}
        ]
        
        success = await self.send_slack_message(
            message="🧪 This is a test notification from the Universal Kubernetes Job Monitor. All job status changes across monitored namespaces will be reported here.",
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
    parser = argparse.ArgumentParser(description='Universal Kubernetes Job Monitor with Slack Notifications')
    parser.add_argument('--mode', choices=['monitor', 'test', 'report'], default='monitor',
                       help='Mode: monitor jobs, send test notification, or generate status report')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        monitor = UniversalJobMonitor()
        
        if args.mode == 'test':
            await monitor.test_notification()
        elif args.mode == 'report':
            await monitor.generate_status_report()
        else:
            await monitor.monitor_jobs()
            
    except KeyboardInterrupt:
        print("\n⏹️  Monitoring stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())