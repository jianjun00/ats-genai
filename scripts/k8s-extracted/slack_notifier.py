#!/usr/bin/env python3

#!/usr/bin/env python3
import asyncio
import aiohttp
import json
import os
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
import subprocess

@dataclass
class JobStatus:
name: str
namespace: str
status: str
start_time: Optional[datetime]
completion_time: Optional[datetime]
duration: Optional[str]

class SlackJobNotifier:
def __init__(self):
self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
self.slack_channel = os.getenv('SLACK_CHANNEL', '#ats-dev-alerts')
self.notification_interval = int(os.getenv('NOTIFICATION_INTERVAL_HOURS', '4'))

if not self.slack_webhook_url:
raise ValueError("SLACK_WEBHOOK_URL environment variable required")

self.logger = logging.getLogger(__name__)
self.tracked_jobs = {}

self.monitored_patterns = [
'comprehensive-30year-backfill',
'minute-backfill',
'price-unification',
'market-data',
'model-training',
'query-'
]

async def send_slack_message(self, message: str, color: str = "good", 
title: str = "ATS Job Status", 
fields: List[Dict] = None) -> bool:
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
job_status = "Running"

if status.get('succeeded', 0) > 0:
job_status = "Complete"
elif status.get('failed', 0) > 0:
job_status = "Failed"

job_statuses.append(JobStatus(
name=job_name,
namespace=namespace,
status=job_status,
start_time=start_time,
completion_time=completion_time,
duration=duration
))

return job_statuses

except Exception as e:
self.logger.error(f"Error getting Kubernetes jobs: {e}")
return []

def should_notify_about_job(self, job: JobStatus) -> bool:
matches_pattern = any(
pattern in job.name for pattern in self.monitored_patterns
)

if not matches_pattern:
return False

if job.status in ['Complete', 'Failed']:
return True

if job.status == 'Running':
last_notification = self.tracked_jobs.get(job.name)
if not last_notification:
return True

time_since_last = datetime.now() - last_notification
return time_since_last >= timedelta(hours=self.notification_interval)

return False

async def send_job_notification(self, job: JobStatus, is_update: bool = False):
if job.status == 'Complete':
color = "good"
emoji = "✅"
status_text = "completed successfully"
elif job.status == 'Failed':
color = "danger"
emoji = "❌"
status_text = "failed"
else:
color = "warning"
emoji = "🔄"
status_text = "is running" if not is_update else "progress update"

title = f"{emoji} Job {status_text.title()}: {job.name}"

if is_update:
message = f"Job `{job.name}` is still running and making progress."
else:
message = f"Job `{job.name}` {status_text}."

fields = [
{"title": "Namespace", "value": job.namespace, "short": True},
{"title": "Status", "value": job.status, "short": True}
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

if job.status == 'Running' and job.start_time:
running_time = datetime.now() - job.start_time.replace(tzinfo=None)
fields.append({
"title": "Running Time",
"value": str(running_time).split('.')[0],
"short": True
})

await self.send_slack_message(
message=message,
color=color,
title=title,
fields=fields
)

self.tracked_jobs[job.name] = datetime.now()

async def monitor_jobs(self):
self.logger.info("Starting Kubernetes job monitoring...")

await self.send_slack_message(
message="🚀 ATS Job Monitor started - monitoring Kubernetes jobs for status changes",
color="good",
title="Job Monitor Started"
)

previous_jobs = {}

while True:
try:
current_jobs = self.get_kubernetes_jobs()
current_job_map = {job.name: job for job in current_jobs}

for job_name, job in current_job_map.items():
previous_job = previous_jobs.get(job_name)

if not previous_job:
if self.should_notify_about_job(job):
await self.send_job_notification(job)

elif previous_job.status != job.status:
if self.should_notify_about_job(job):
await self.send_job_notification(job)

elif job.status == 'Running' and self.should_notify_about_job(job):
await self.send_job_notification(job, is_update=True)

previous_jobs = current_job_map
await asyncio.sleep(60)

except Exception as e:
self.logger.error(f"Error in monitoring loop: {e}")
await asyncio.sleep(30)

async def main():
logging.basicConfig(
level=logging.INFO,
format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

try:
notifier = SlackJobNotifier()
await notifier.monitor_jobs()
except KeyboardInterrupt:
print("\n⏹️  Monitoring stopped by user")
except Exception as e:
print(f"❌ Error: {e}")
raise

if __name__ == "__main__":
asyncio.run(main())
