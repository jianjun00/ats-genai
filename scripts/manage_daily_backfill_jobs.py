#!/usr/bin/env python3
"""
Daily Backfill Jobs Management Script

Manages daily vendor data collection jobs for ATS-INTG environment.
Provides deployment, monitoring, and troubleshooting capabilities.

Usage:
    python3 scripts/manage_daily_backfill_jobs.py deploy
    python3 scripts/manage_daily_backfill_jobs.py status
    python3 scripts/manage_daily_backfill_jobs.py logs --job daily-multi-vendor-backfill
    python3 scripts/manage_daily_backfill_jobs.py test-run --vendor tiingo
"""

import subprocess
import json
import argparse
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BackfillJobsManager:
    """Manager for daily backfill Kubernetes jobs."""

    def __init__(self, namespace: str = "ats-intg"):
        self.namespace = namespace
        self.cronjob_file = "k8s/intg/daily-vendor-backfill-cronjobs.yaml"

        # Job definitions
        self.jobs = {
            'daily-multi-vendor-backfill': {
                'description': 'Daily collection from all vendors (Tiingo, Polygon, EODHD)',
                'schedule': '0 12 * * *',  # 7 AM EST
                'runtime': '2 hours',
                'symbols': '500'
            },
            'daily-tiingo-backfill': {
                'description': 'Daily Tiingo fallback collection',
                'schedule': '0 13 * * *',  # 8 AM EST
                'runtime': '1 hour',
                'symbols': '200'
            },
            'weekly-comprehensive-vendor-backfill': {
                'description': 'Weekly comprehensive 30-day backfill',
                'schedule': '0 8 * * 0',  # 3 AM EST Sunday
                'runtime': '4 hours',
                'symbols': 'all'
            }
        }

    def run_kubectl(self, command: List[str]) -> Dict:
        """Run kubectl command and return result."""
        try:
            result = subprocess.run(
                ['kubectl'] + command,
                capture_output=True,
                text=True,
                timeout=60
            )

            return {
                'success': result.returncode == 0,
                'stdout': result.stdout.strip(),
                'stderr': result.stderr.strip(),
                'returncode': result.returncode
            }
        except subprocess.TimeoutExpired:
            return {
                'success': False,
                'stdout': '',
                'stderr': 'Command timed out',
                'returncode': -1
            }
        except Exception as e:
            return {
                'success': False,
                'stdout': '',
                'stderr': str(e),
                'returncode': -1
            }

    def deploy_jobs(self) -> bool:
        """Deploy the daily backfill CronJobs to Kubernetes."""
        logger.info("🚀 Deploying daily backfill jobs...")

        # Check if namespace exists
        ns_result = self.run_kubectl(['get', 'namespace', self.namespace])
        if not ns_result['success']:
            logger.info(f"📁 Creating namespace: {self.namespace}")
            create_ns = self.run_kubectl(['create', 'namespace', self.namespace])
            if not create_ns['success']:
                logger.error(f"❌ Failed to create namespace: {create_ns['stderr']}")
                return False

        # Apply the CronJobs
        logger.info(f"📄 Applying CronJob definitions from {self.cronjob_file}")
        apply_result = self.run_kubectl(['apply', '-f', self.cronjob_file])

        if apply_result['success']:
            logger.info("✅ Daily backfill jobs deployed successfully")

            # Show deployed jobs
            logger.info("\n📋 Deployed CronJobs:")
            for job_name, job_info in self.jobs.items():
                logger.info(f"  • {job_name}: {job_info['description']}")
                logger.info(f"    Schedule: {job_info['schedule']} ({self._cron_to_readable(job_info['schedule'])})")
                logger.info(f"    Runtime: {job_info['runtime']}, Symbols: {job_info['symbols']}")

            return True
        else:
            logger.error(f"❌ Failed to deploy jobs: {apply_result['stderr']}")
            return False

    def get_cronjob_status(self) -> Dict:
        """Get status of all CronJobs."""
        logger.info(f"📊 Checking CronJob status in namespace: {self.namespace}")

        # Get CronJobs
        cronjobs_result = self.run_kubectl([
            'get', 'cronjobs',
            '-n', self.namespace,
            '-o', 'json'
        ])

        if not cronjobs_result['success']:
            logger.error(f"❌ Failed to get CronJobs: {cronjobs_result['stderr']}")
            return {}

        try:
            cronjobs_data = json.loads(cronjobs_result['stdout'])
        except json.JSONDecodeError:
            logger.error("❌ Failed to parse CronJobs JSON")
            return {}

        status = {}

        for item in cronjobs_data.get('items', []):
            name = item['metadata']['name']
            spec = item['spec']
            status_info = item.get('status', {})

            status[name] = {
                'schedule': spec['schedule'],
                'timezone': spec.get('timeZone', 'UTC'),
                'suspend': spec.get('suspend', False),
                'last_schedule': status_info.get('lastScheduleTime'),
                'last_successful': status_info.get('lastSuccessfulTime'),
                'active_jobs': len(status_info.get('active', [])),
                'job_history': {
                    'successful': spec.get('successfulJobsHistoryLimit', 3),
                    'failed': spec.get('failedJobsHistoryLimit', 1)
                }
            }

        return status

    def print_status(self):
        """Print formatted status of all CronJobs."""
        status = self.get_cronjob_status()

        if not status:
            logger.warning("⚠️ No CronJobs found or failed to retrieve status")
            return

        logger.info("\n📊 DAILY BACKFILL JOBS STATUS")
        logger.info("=" * 80)

        for job_name, info in status.items():
            job_desc = self.jobs.get(job_name, {}).get('description', 'Unknown job')

            logger.info(f"\n🔧 {job_name}")
            logger.info(f"   Description: {job_desc}")
            logger.info(f"   Schedule: {info['schedule']} ({info['timezone']})")
            logger.info(f"   Next run: {self._next_cron_time(info['schedule'])}")
            logger.info(f"   Suspended: {info['suspend']}")
            logger.info(f"   Active jobs: {info['active_jobs']}")

            if info['last_schedule']:
                logger.info(f"   Last scheduled: {info['last_schedule']}")
            if info['last_successful']:
                logger.info(f"   Last successful: {info['last_successful']}")

            # Status indicator
            if info['suspend']:
                logger.info("   Status: 🔴 SUSPENDED")
            elif info['active_jobs'] > 0:
                logger.info("   Status: 🟡 RUNNING")
            elif info['last_successful']:
                logger.info("   Status: 🟢 HEALTHY")
            else:
                logger.info("   Status: ⚪ NOT RUN YET")

    def get_job_logs(self, cronjob_name: str, lines: int = 100) -> Optional[str]:
        """Get logs from the most recent job of a CronJob."""
        logger.info(f"📝 Getting logs for {cronjob_name} (last {lines} lines)")

        # Get jobs for this CronJob
        jobs_result = self.run_kubectl([
            'get', 'jobs',
            '-n', self.namespace,
            '-l', f'job-name={cronjob_name}',
            '-o', 'json'
        ])

        if not jobs_result['success']:
            logger.error(f"❌ Failed to get jobs: {jobs_result['stderr']}")
            return None

        try:
            jobs_data = json.loads(jobs_result['stdout'])
        except json.JSONDecodeError:
            logger.error("❌ Failed to parse jobs JSON")
            return None

        jobs = jobs_data.get('items', [])
        if not jobs:
            logger.warning(f"⚠️ No jobs found for CronJob: {cronjob_name}")
            return None

        # Get most recent job
        latest_job = max(jobs, key=lambda j: j['metadata']['creationTimestamp'])
        job_name = latest_job['metadata']['name']

        logger.info(f"📋 Getting logs from job: {job_name}")

        # Get logs
        logs_result = self.run_kubectl([
            'logs',
            f'job/{job_name}',
            '-n', self.namespace,
            '--tail', str(lines)
        ])

        if logs_result['success']:
            return logs_result['stdout']
        else:
            logger.error(f"❌ Failed to get logs: {logs_result['stderr']}")
            return None

    def run_test_job(self, vendor: str = "tiingo") -> bool:
        """Run a test job to verify the data collection works."""
        logger.info(f"🧪 Running test job for {vendor} vendor...")

        # Create test job
        job_name = f"test-{vendor}-collection-{int(datetime.now().timestamp())}"

        if vendor == "tiingo":
            script = "scripts/tiingo_data_collector_intg.py"
            args = ["--days", "3", "--symbols", "10", "--debug"]
        else:
            script = "scripts/multi_vendor_daily_collector.py"
            args = ["--vendors", vendor, "--days", "3", "--symbols", "10", "--debug"]

        # Create job YAML
        job_yaml = f"""
apiVersion: batch/v1
kind: Job
metadata:
  name: {job_name}
  namespace: {self.namespace}
spec:
  template:
    spec:
      serviceAccountName: ats-service-account
      restartPolicy: Never
      containers:
      - name: test-collector
        image: ats-genai:latest
        command: ["python3", "{script}"]
        args: {json.dumps(args)}
        env:
        - name: PYTHONPATH
          value: "src"
        - name: ENVIRONMENT
          value: "intg"
        - name: DB_HOST
          value: "ats-intg-postgres"
        - name: DB_PORT
          value: "5432"
        - name: DB_USER
          value: "postgres"
        - name: DB_PASSWORD
          value: "intg_password"
        - name: DB_NAME
          value: "intg_db"
        - name: TIINGO_API_KEY
          valueFrom:
            secretKeyRef:
              name: vendor-api-keys
              key: tiingo-api-key
        - name: POLYGON_API_KEY
          valueFrom:
            secretKeyRef:
              name: vendor-api-keys
              key: polygon-api-key
        - name: EODHD_API_KEY
          valueFrom:
            secretKeyRef:
              name: vendor-api-keys
              key: eodhd-api-key
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
"""

        # Apply test job
        apply_result = self.run_kubectl(['apply', '-f', '-'], input=job_yaml)

        if not apply_result['success']:
            logger.error(f"❌ Failed to create test job: {apply_result['stderr']}")
            return False

        logger.info(f"✅ Test job created: {job_name}")
        logger.info("🔍 Waiting for job to complete...")

        # Wait for completion (up to 10 minutes)
        for i in range(60):  # 60 * 10 = 10 minutes
            job_status = self.run_kubectl([
                'get', 'job', job_name,
                '-n', self.namespace,
                '-o', 'jsonpath={.status.conditions[0].type}'
            ])

            if job_status['success'] and job_status['stdout'] == 'Complete':
                logger.info("✅ Test job completed successfully")

                # Show logs
                logs = self.get_job_logs(job_name)
                if logs:
                    logger.info("\n📝 Test job logs:")
                    print(logs[-2000:])  # Last 2000 characters

                # Cleanup
                self.run_kubectl(['delete', 'job', job_name, '-n', self.namespace])
                return True
            elif job_status['success'] and job_status['stdout'] == 'Failed':
                logger.error("❌ Test job failed")

                # Show logs
                logs = self.get_job_logs(job_name)
                if logs:
                    logger.error("\n📝 Test job error logs:")
                    print(logs[-2000:])

                # Cleanup
                self.run_kubectl(['delete', 'job', job_name, '-n', self.namespace])
                return False

            time.sleep(10)

        logger.error("❌ Test job timed out")
        self.run_kubectl(['delete', 'job', job_name, '-n', self.namespace])
        return False

    def _cron_to_readable(self, cron: str) -> str:
        """Convert cron expression to readable format."""
        cron_map = {
            "0 12 * * *": "Daily at 7:00 AM EST",
            "0 13 * * *": "Daily at 8:00 AM EST",
            "0 8 * * 0": "Weekly Sunday at 3:00 AM EST"
        }
        return cron_map.get(cron, cron)

    def _next_cron_time(self, cron: str) -> str:
        """Calculate next execution time for cron expression."""
        # This is a simplified version - would need a proper cron parser for production
        now = datetime.now()
        return f"~{now.strftime('%Y-%m-%d %H:%M')} (estimated)"

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Daily Backfill Jobs Manager')
    parser.add_argument('command', choices=['deploy', 'status', 'logs', 'test-run', 'help'],
                       help='Command to execute')
    parser.add_argument('--job', type=str, help='Specific job name for logs command')
    parser.add_argument('--vendor', type=str, default='tiingo',
                       choices=['tiingo', 'polygon', 'eodhd'],
                       help='Vendor for test-run command')
    parser.add_argument('--lines', type=int, default=100,
                       help='Number of log lines to show')
    parser.add_argument('--namespace', type=str, default='ats-intg',
                       help='Kubernetes namespace')

    args = parser.parse_args()

    if args.command == 'help':
        parser.print_help()
        return

    manager = BackfillJobsManager(args.namespace)

    try:
        if args.command == 'deploy':
            success = manager.deploy_jobs()
            sys.exit(0 if success else 1)

        elif args.command == 'status':
            manager.print_status()

        elif args.command == 'logs':
            if not args.job:
                logger.error("❌ --job parameter required for logs command")
                sys.exit(1)

            logs = manager.get_job_logs(args.job, args.lines)
            if logs:
                print("\n" + "="*80)
                print(f"LOGS FOR {args.job} (last {args.lines} lines)")
                print("="*80)
                print(logs)
            else:
                logger.error("❌ Could not retrieve logs")
                sys.exit(1)

        elif args.command == 'test-run':
            success = manager.run_test_job(args.vendor)
            sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.info("\n👋 Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()