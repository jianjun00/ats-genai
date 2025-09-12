#!/usr/bin/env python3
"""
News Ingestion Management Script

Manages news backfill and real-time ingestion systems for ATS-INTG.
Provides deployment, monitoring, and troubleshooting capabilities.

Usage:
    python3 scripts/manage_news_ingestion.py deploy
    python3 scripts/manage_news_ingestion.py status
    python3 scripts/manage_news_ingestion.py logs --service realtime-news-ingestion
    python3 scripts/manage_news_ingestion.py backfill --vendor tiingo --days 7
    python3 scripts/manage_news_ingestion.py health-check
"""

import subprocess
import json
import argparse
import sys
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NewsIngestionManager:
    """Manager for news ingestion systems."""

    def __init__(self, namespace: str = "ats-intg"):
        self.namespace = namespace
        self.cronjob_file = "k8s/intg/news-ingestion-cronjobs.yaml"

        # News system components
        self.components = {
            'news-backfill-30days': {
                'type': 'cronjob',
                'description': 'Daily 30-day news backfill from all vendors',
                'schedule': '0 10 * * *',  # 5 AM EST
                'runtime': '2 hours'
            },
            'news-backfill-comprehensive': {
                'type': 'cronjob',
                'description': 'Weekly 90-day comprehensive backfill',
                'schedule': '0 7 * * 0',  # 2 AM EST Sunday
                'runtime': '4 hours'
            },
            'realtime-news-ingestion': {
                'type': 'deployment',
                'description': 'Continuous real-time news ingestion',
                'schedule': 'continuous',
                'runtime': 'persistent'
            },
            'news-health-monitoring': {
                'type': 'cronjob',
                'description': 'News system health monitoring',
                'schedule': '0 */4 * * 1-5',  # Every 4h, Mon-Fri
                'runtime': '30 minutes'
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

    def deploy_news_systems(self) -> bool:
        """Deploy news ingestion systems to Kubernetes."""
        logger.info("🚀 Deploying news ingestion systems...")

        # Check if namespace exists
        ns_result = self.run_kubectl(['get', 'namespace', self.namespace])
        if not ns_result['success']:
            logger.info(f"📁 Creating namespace: {self.namespace}")
            create_ns = self.run_kubectl(['create', 'namespace', self.namespace])
            if not create_ns['success']:
                logger.error(f"❌ Failed to create namespace: {create_ns['stderr']}")
                return False

        # Apply the resources
        logger.info(f"📄 Applying news ingestion definitions from {self.cronjob_file}")
        apply_result = self.run_kubectl(['apply', '-f', self.cronjob_file])

        if apply_result['success']:
            logger.info("✅ News ingestion systems deployed successfully")

            # Show deployed components
            logger.info("\n📋 Deployed Components:")
            for component, info in self.components.items():
                logger.info(f"  • {component} ({info['type']}): {info['description']}")
                logger.info(f"    Schedule: {info['schedule']}, Runtime: {info['runtime']}")

            return True
        else:
            logger.error(f"❌ Failed to deploy systems: {apply_result['stderr']}")
            return False

    def get_news_system_status(self) -> Dict:
        """Get status of all news system components."""
        logger.info(f"📊 Checking news system status in namespace: {self.namespace}")

        status = {
            'cronjobs': {},
            'deployments': {},
            'services': {}
        }

        # Get CronJobs
        cronjobs_result = self.run_kubectl([
            'get', 'cronjobs',
            '-n', self.namespace,
            '-l', 'component in (data-collection,data-validation,monitoring)',
            '-o', 'json'
        ])

        if cronjobs_result['success']:
            try:
                cronjobs_data = json.loads(cronjobs_result['stdout'])
                for item in cronjobs_data.get('items', []):
                    name = item['metadata']['name']
                    if 'news' in name:  # Filter to news-related jobs
                        spec = item['spec']
                        status_info = item.get('status', {})

                        status['cronjobs'][name] = {
                            'schedule': spec['schedule'],
                            'suspend': spec.get('suspend', False),
                            'last_schedule': status_info.get('lastScheduleTime'),
                            'last_successful': status_info.get('lastSuccessfulTime'),
                            'active_jobs': len(status_info.get('active', []))
                        }
            except json.JSONDecodeError:
                logger.error("❌ Failed to parse CronJobs JSON")

        # Get Deployments
        deployments_result = self.run_kubectl([
            'get', 'deployments',
            '-n', self.namespace,
            '-l', 'app=realtime-news-ingestion',
            '-o', 'json'
        ])

        if deployments_result['success']:
            try:
                deployments_data = json.loads(deployments_result['stdout'])
                for item in deployments_data.get('items', []):
                    name = item['metadata']['name']
                    spec = item['spec']
                    status_info = item.get('status', {})

                    status['deployments'][name] = {
                        'desired_replicas': spec.get('replicas', 0),
                        'ready_replicas': status_info.get('readyReplicas', 0),
                        'available_replicas': status_info.get('availableReplicas', 0),
                        'conditions': status_info.get('conditions', [])
                    }
            except json.JSONDecodeError:
                logger.error("❌ Failed to parse Deployments JSON")

        # Get Services
        services_result = self.run_kubectl([
            'get', 'services',
            '-n', self.namespace,
            '-l', 'app=realtime-news-ingestion',
            '-o', 'json'
        ])

        if services_result['success']:
            try:
                services_data = json.loads(services_result['stdout'])
                for item in services_data.get('items', []):
                    name = item['metadata']['name']
                    spec = item['spec']

                    status['services'][name] = {
                        'type': spec.get('type', 'ClusterIP'),
                        'ports': spec.get('ports', []),
                        'selector': spec.get('selector', {})
                    }
            except json.JSONDecodeError:
                logger.error("❌ Failed to parse Services JSON")

        return status

    def print_status(self):
        """Print formatted status of news systems."""
        status = self.get_news_system_status()

        logger.info("\n📊 NEWS INGESTION SYSTEMS STATUS")
        logger.info("=" * 80)

        # CronJobs status
        if status['cronjobs']:
            logger.info("\n🕐 CRONJOBS:")
            for name, info in status['cronjobs'].items():
                component_info = self.components.get(name, {})
                description = component_info.get('description', 'News CronJob')

                logger.info(f"\n📋 {name}")
                logger.info(f"   Description: {description}")
                logger.info(f"   Schedule: {info['schedule']}")
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

        # Deployments status
        if status['deployments']:
            logger.info("\n🚀 DEPLOYMENTS:")
            for name, info in status['deployments'].items():
                logger.info(f"\n📦 {name}")
                logger.info(f"   Desired replicas: {info['desired_replicas']}")
                logger.info(f"   Ready replicas: {info['ready_replicas']}")
                logger.info(f"   Available replicas: {info['available_replicas']}")

                # Status indicator
                if info['ready_replicas'] == info['desired_replicas'] > 0:
                    logger.info("   Status: 🟢 HEALTHY")
                elif info['ready_replicas'] > 0:
                    logger.info("   Status: 🟡 PARTIAL")
                else:
                    logger.info("   Status: 🔴 UNHEALTHY")

        # Services status
        if status['services']:
            logger.info("\n🔗 SERVICES:")
            for name, info in status['services'].items():
                logger.info(f"\n🌐 {name}")
                logger.info(f"   Type: {info['type']}")
                logger.info(f"   Ports: {[f\"{p['port']}:{p['targetPort']}\" for p in info['ports']]}")

        if not any(status.values()):
            logger.warning("⚠️ No news ingestion components found")

    def get_component_logs(self, component_name: str, lines: int = 100) -> Optional[str]:
        """Get logs from a news system component."""
        logger.info(f"📝 Getting logs for {component_name} (last {lines} lines)")

        # Determine component type and get appropriate logs
        if component_name in ['realtime-news-ingestion']:
            # Get deployment logs
            logs_result = self.run_kubectl([
                'logs',
                f'deployment/{component_name}',
                '-n', self.namespace,
                '--tail', str(lines)
            ])
        else:
            # Get job logs for CronJobs
            jobs_result = self.run_kubectl([
                'get', 'jobs',
                '-n', self.namespace,
                '-l', f'job-name={component_name}',
                '--sort-by', '.metadata.creationTimestamp',
                '-o', 'jsonpath={.items[-1:].metadata.name}'
            ])

            if jobs_result['success'] and jobs_result['stdout']:
                job_name = jobs_result['stdout']
                logs_result = self.run_kubectl([
                    'logs',
                    f'job/{job_name}',
                    '-n', self.namespace,
                    '--tail', str(lines)
                ])
            else:
                logger.warning(f"⚠️ No recent jobs found for {component_name}")
                return None

        if logs_result['success']:
            return logs_result['stdout']
        else:
            logger.error(f"❌ Failed to get logs: {logs_result['stderr']}")
            return None

    async def run_backfill(self, vendor: str = "all", days: int = 30, symbols: Optional[str] = None) -> bool:
        """Run news backfill operation."""
        logger.info(f"🔄 Running news backfill: {vendor} vendor, {days} days")

        # Prepare command
        vendors = vendor if vendor != "all" else "tiingo,polygon,eodhd"
        cmd = [
            "python3", "scripts/multi_vendor_news_backfill.py",
            "--vendors", vendors,
            "--days", str(days)
        ]

        if symbols:
            cmd.extend(["--symbols", symbols])

        # Create one-time job
        job_name = f"news-backfill-manual-{int(datetime.now().timestamp())}"

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
      - name: news-backfill
        image: ats-genai:latest
        command: {json.dumps(cmd)}
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
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
"""

        # Apply job
        apply_result = self.run_kubectl(['apply', '-f', '-'], input=job_yaml)

        if not apply_result['success']:
            logger.error(f"❌ Failed to create backfill job: {apply_result['stderr']}")
            return False

        logger.info(f"✅ Backfill job created: {job_name}")
        logger.info("🔍 Waiting for job to complete...")

        # Wait for completion (up to 30 minutes)
        for i in range(180):  # 180 * 10 = 30 minutes
            job_status = self.run_kubectl([
                'get', 'job', job_name,
                '-n', self.namespace,
                '-o', 'jsonpath={.status.conditions[0].type}'
            ])

            if job_status['success']:
                if job_status['stdout'] == 'Complete':
                    logger.info("✅ Backfill job completed successfully")

                    # Show logs
                    logs = self.get_component_logs(job_name, 200)
                    if logs:
                        logger.info("\n📝 Backfill job logs:")
                        print(logs[-3000:])  # Last 3000 characters

                    # Cleanup
                    self.run_kubectl(['delete', 'job', job_name, '-n', self.namespace])
                    return True
                elif job_status['stdout'] == 'Failed':
                    logger.error("❌ Backfill job failed")

                    # Show error logs
                    logs = self.get_component_logs(job_name, 200)
                    if logs:
                        logger.error("\n📝 Backfill job error logs:")
                        print(logs[-3000:])

                    # Cleanup
                    self.run_kubectl(['delete', 'job', job_name, '-n', self.namespace])
                    return False

            await asyncio.sleep(10)

        logger.error("❌ Backfill job timed out")
        self.run_kubectl(['delete', 'job', job_name, '-n', self.namespace])
        return False

    async def run_health_check(self) -> bool:
        """Run news system health check."""
        logger.info("🔍 Running news system health check...")

        # Create health check job
        job_name = f"news-health-check-{int(datetime.now().timestamp())}"

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
      - name: health-check
        image: ats-genai:latest
        command: ["python3", "scripts/news_health_monitor.py"]
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
        - name: PROMETHEUS_GATEWAY
          value: "prometheus-pushgateway:9091"
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
"""

        # Apply and wait for completion
        apply_result = self.run_kubectl(['apply', '-f', '-'], input=job_yaml)

        if not apply_result['success']:
            logger.error(f"❌ Failed to create health check job: {apply_result['stderr']}")
            return False

        # Wait for completion
        for i in range(30):  # 5 minutes max
            job_status = self.run_kubectl([
                'get', 'job', job_name,
                '-n', self.namespace,
                '-o', 'jsonpath={.status.conditions[0].type}'
            ])

            if job_status['success']:
                if job_status['stdout'] == 'Complete':
                    # Show health check results
                    logs = self.get_component_logs(job_name, 500)
                    if logs:
                        print("\n" + "="*80)
                        print("HEALTH CHECK RESULTS")
                        print("="*80)
                        print(logs)

                    # Cleanup and return success
                    self.run_kubectl(['delete', 'job', job_name, '-n', self.namespace])
                    return True
                elif job_status['stdout'] == 'Failed':
                    logger.error("❌ Health check failed")
                    self.run_kubectl(['delete', 'job', job_name, '-n', self.namespace])
                    return False

            await asyncio.sleep(10)

        logger.error("❌ Health check timed out")
        self.run_kubectl(['delete', 'job', job_name, '-n', self.namespace])
        return False

async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='News Ingestion Management')
    parser.add_argument('command', choices=['deploy', 'status', 'logs', 'backfill', 'health-check', 'help'],
                       help='Command to execute')
    parser.add_argument('--service', type=str, help='Service name for logs command')
    parser.add_argument('--vendor', type=str, default='all',
                       choices=['all', 'tiingo', 'polygon', 'eodhd'],
                       help='Vendor for backfill command')
    parser.add_argument('--days', type=int, default=30,
                       help='Number of days for backfill')
    parser.add_argument('--symbols', type=str,
                       help='Comma-separated symbols for backfill')
    parser.add_argument('--lines', type=int, default=100,
                       help='Number of log lines to show')
    parser.add_argument('--namespace', type=str, default='ats-intg',
                       help='Kubernetes namespace')

    args = parser.parse_args()

    if args.command == 'help':
        parser.print_help()
        return

    manager = NewsIngestionManager(args.namespace)

    try:
        if args.command == 'deploy':
            success = manager.deploy_news_systems()
            sys.exit(0 if success else 1)

        elif args.command == 'status':
            manager.print_status()

        elif args.command == 'logs':
            if not args.service:
                logger.error("❌ --service parameter required for logs command")
                sys.exit(1)

            logs = manager.get_component_logs(args.service, args.lines)
            if logs:
                print("\n" + "="*80)
                print(f"LOGS FOR {args.service} (last {args.lines} lines)")
                print("="*80)
                print(logs)
            else:
                logger.error("❌ Could not retrieve logs")
                sys.exit(1)

        elif args.command == 'backfill':
            success = await manager.run_backfill(args.vendor, args.days, args.symbols)
            sys.exit(0 if success else 1)

        elif args.command == 'health-check':
            success = await manager.run_health_check()
            sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.info("\n👋 Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())