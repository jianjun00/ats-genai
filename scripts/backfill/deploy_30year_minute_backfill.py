#!/usr/bin/env python3
"""
30-Year Minute Data Backfill Deployment Manager

Manages deployment and monitoring of 30-year minute data backfill jobs across all vendors.
Uses the checkpoint framework for resumable processing.

Usage:
    # Deploy all vendor jobs
    python deploy_30year_minute_backfill.py --deploy all
    
    # Deploy specific vendor
    python deploy_30year_minute_backfill.py --deploy polygon
    
    # Check status of all jobs
    python deploy_30year_minute_backfill.py --status
    
    # Monitor progress
    python deploy_30year_minute_backfill.py --monitor
    
    # Clean up completed jobs
    python deploy_30year_minute_backfill.py --cleanup
"""

import os
import sys
import subprocess
import argparse
import logging
import time
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path
import asyncio
import aiohttp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SlackNotifier:
    """Handles Slack notifications for job status changes."""
    
    def __init__(self):
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        self.slack_channel = os.getenv('SLACK_CHANNEL', '#ats-dev-alerts')
        self.enabled = bool(self.slack_webhook_url)
        
        if not self.enabled:
            logger.warning("Slack notifications disabled - SLACK_WEBHOOK_URL not configured")
        else:
            logger.info(f"Slack notifications enabled for channel: {self.slack_channel}")
    
    async def send_message(self, message: str, color: str = "good", 
                          title: str = "30-Year Minute Backfill", 
                          fields: List[Dict] = None) -> bool:
        """Send message to Slack"""
        
        if not self.enabled:
            logger.debug(f"Slack disabled - would send: {title}: {message}")
            return True
        
        if not fields:
            fields = []
        
        payload = {
            "channel": self.slack_channel,
            "username": "ATS Backfill Manager",
            "icon_emoji": ":chart_with_upwards_trend:",
            "attachments": [
                {
                    "color": color,
                    "title": title,
                    "text": message,
                    "fields": fields,
                    "footer": "30-Year Minute Data Backfill System",
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
    
    async def notify_deployment(self, vendor: str, success: bool, details: str = "") -> bool:
        """Notify about job deployment"""
        if success:
            return await self.send_message(
                message=f"🚀 **{vendor.upper()}** 30-year minute backfill job deployed successfully\n{details}",
                color="good",
                title="🚀 Backfill Job Deployed",
                fields=[
                    {"title": "Vendor", "value": vendor.upper(), "short": True},
                    {"title": "Status", "value": "Deployed", "short": True},
                    {"title": "Scope", "value": "30 years (1995-2025)", "short": True},
                    {"title": "Data Type", "value": "1-minute OHLCV bars", "short": True}
                ]
            )
        else:
            return await self.send_message(
                message=f"❌ **{vendor.upper()}** 30-year minute backfill job deployment failed\n{details}",
                color="danger",
                title="❌ Backfill Job Deployment Failed",
                fields=[
                    {"title": "Vendor", "value": vendor.upper(), "short": True},
                    {"title": "Status", "value": "Failed", "short": True},
                    {"title": "Error", "value": details[:100] + "..." if len(details) > 100 else details, "short": False}
                ]
            )
    
    async def notify_all_deployed(self, results: Dict[str, bool]) -> bool:
        """Notify about all jobs deployment results"""
        successful = [vendor for vendor, success in results.items() if success]
        failed = [vendor for vendor, success in results.items() if not success]
        
        if not failed:
            # All successful
            vendors_list = ", ".join([v.upper() for v in successful])
            return await self.send_message(
                message=f"🎉 **All 30-year minute backfill jobs deployed successfully!**\n\nVendors: {vendors_list}\n\n📊 Expected: ~120 billion bars\n⏱️ Duration: 60-90 days\n💾 Storage: ~5.6TB",
                color="good",
                title="🎉 Complete Deployment Success",
                fields=[
                    {"title": "Total Jobs", "value": str(len(successful)), "short": True},
                    {"title": "Success Rate", "value": "100%", "short": True},
                    {"title": "Est. Duration", "value": "60-90 days", "short": True},
                    {"title": "Est. Storage", "value": "~5.6TB", "short": True}
                ]
            )
        else:
            # Some failed
            success_count = len(successful)
            total_count = len(results)
            success_rate = f"{success_count}/{total_count} ({success_count/total_count*100:.0f}%)"
            
            return await self.send_message(
                message=f"⚠️ **Partial deployment completed**\n\n✅ Successful: {', '.join([v.upper() for v in successful])}\n❌ Failed: {', '.join([v.upper() for v in failed])}",
                color="warning",
                title="⚠️ Partial Deployment",
                fields=[
                    {"title": "Success Rate", "value": success_rate, "short": True},
                    {"title": "Action Required", "value": "Check failed deployments", "short": True}
                ]
            )
    
    async def notify_status_summary(self, statuses: Dict[str, Dict[str, Any]]) -> bool:
        """Notify about job status summary"""
        running = []
        completed = []
        failed = []
        not_found = []
        
        for vendor, status in statuses.items():
            if status.get("status") == "not_found":
                not_found.append(vendor)
            elif status.get("succeeded", 0) > 0:
                completed.append(vendor)
            elif status.get("failed", 0) > 0:
                failed.append(vendor)
            elif status.get("active", 0) > 0:
                running.append(vendor)
            else:
                not_found.append(vendor)
        
        # Determine overall status
        if completed and not running and not failed:
            color = "good"
            emoji = "✅"
            title = "All Backfill Jobs Complete"
        elif failed and not running:
            color = "danger"
            emoji = "❌"
            title = "Backfill Jobs Failed"
        elif running:
            color = "warning"
            emoji = "🔄"
            title = "Backfill Jobs In Progress"
        else:
            color = "#808080"
            emoji = "⏳"
            title = "Backfill Jobs Status"
        
        message_parts = []
        if running:
            message_parts.append(f"🔄 **Running:** {', '.join([v.upper() for v in running])}")
        if completed:
            message_parts.append(f"✅ **Completed:** {', '.join([v.upper() for v in completed])}")
        if failed:
            message_parts.append(f"❌ **Failed:** {', '.join([v.upper() for v in failed])}")
        if not_found:
            message_parts.append(f"⏳ **Not Deployed:** {', '.join([v.upper() for v in not_found])}")
        
        message = "\n".join(message_parts) if message_parts else "No backfill jobs found"
        
        return await self.send_message(
            message=message,
            color=color,
            title=f"{emoji} {title}",
            fields=[
                {"title": "Total Jobs", "value": str(len(statuses)), "short": True},
                {"title": "Running", "value": str(len(running)), "short": True},
                {"title": "Completed", "value": str(len(completed)), "short": True},
                {"title": "Failed", "value": str(len(failed)), "short": True}
            ]
        )


class MinuteBackfillDeploymentManager:
    """Manages deployment and monitoring of 30-year minute backfill jobs."""
    
    def __init__(self):
        self.namespace = "ats-dev"
        self.k8s_dir = Path(__file__).parent.parent.parent / "k8s"
        self.slack_notifier = SlackNotifier()
        
        # Available vendor jobs
        self.vendor_jobs = {
            "polygon": "30year-minute-backfill-polygon.yaml",
            "tiingo": "30year-minute-backfill-tiingo.yaml", 
            "fmp": "30year-minute-backfill-fmp.yaml",
            "eodhd": "30year-minute-backfill-eodhd.yaml"
        }
        
        # Orchestrator job
        self.orchestrator_job = "30year-minute-backfill-orchestrator.yaml"
        
        # Job naming patterns
        self.job_names = {
            "polygon": "polygon-30year-minute-backfill",
            "tiingo": "tiingo-30year-minute-backfill",
            "fmp": "fmp-30year-minute-backfill", 
            "eodhd": "eodhd-30year-minute-backfill",
            "orchestrator": "comprehensive-30year-minute-backfill"
        }
    
    def run_kubectl(self, command: str) -> subprocess.CompletedProcess:
        """Run kubectl command."""
        full_command = f"kubectl {command}"
        logger.debug(f"Running: {full_command}")
        
        result = subprocess.run(
            full_command,
            shell=True,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"kubectl command failed: {result.stderr}")
        
        return result
    
    def deploy_vendor(self, vendor: str) -> bool:
        """Deploy a specific vendor backfill job."""
        if vendor not in self.vendor_jobs:
            logger.error(f"Unknown vendor: {vendor}")
            return False
        
        job_file = self.k8s_dir / self.vendor_jobs[vendor]
        if not job_file.exists():
            logger.error(f"Job file not found: {job_file}")
            return False
        
        logger.info(f"🚀 Deploying {vendor.upper()} 30-year minute backfill...")
        
        # Apply the job
        result = self.run_kubectl(f"apply -f {job_file}")
        
        success = result.returncode == 0
        
        if success:
            logger.info(f"✅ {vendor.upper()} backfill job deployed successfully")
            # Send Slack notification
            asyncio.run(self.slack_notifier.notify_deployment(
                vendor=vendor,
                success=True,
                details=f"Job file: {job_file.name}"
            ))
        else:
            logger.error(f"❌ Failed to deploy {vendor.upper()} backfill job")
            # Send Slack notification for failure
            asyncio.run(self.slack_notifier.notify_deployment(
                vendor=vendor,
                success=False,
                details=result.stderr
            ))
        
        return success
    
    def deploy_orchestrator(self) -> bool:
        """Deploy the comprehensive orchestrator job."""
        job_file = self.k8s_dir / self.orchestrator_job
        if not job_file.exists():
            logger.error(f"Orchestrator job file not found: {job_file}")
            return False
        
        logger.info("🚀 Deploying comprehensive 30-year minute backfill orchestrator...")
        
        result = self.run_kubectl(f"apply -f {job_file}")
        
        success = result.returncode == 0
        
        if success:
            logger.info("✅ Orchestrator job deployed successfully")
            # Send Slack notification
            asyncio.run(self.slack_notifier.notify_deployment(
                vendor="orchestrator",
                success=True,
                details="Master orchestrator coordinating all vendors"
            ))
        else:
            logger.error("❌ Failed to deploy orchestrator job")
            # Send Slack notification for failure
            asyncio.run(self.slack_notifier.notify_deployment(
                vendor="orchestrator",
                success=False,
                details=result.stderr
            ))
        
        return success
    
    def deploy_all(self) -> Dict[str, bool]:
        """Deploy all vendor backfill jobs."""
        logger.info("🚀 Deploying all 30-year minute backfill jobs...")
        
        results = {}
        
        # Deploy orchestrator first
        results["orchestrator"] = self.deploy_orchestrator()
        
        # Deploy all vendor jobs
        for vendor in self.vendor_jobs.keys():
            results[vendor] = self.deploy_vendor(vendor)
        
        successful = sum(results.values())
        total = len(results)
        
        logger.info(f"📊 Deployment summary: {successful}/{total} jobs deployed successfully")
        
        # Send summary Slack notification
        asyncio.run(self.slack_notifier.notify_all_deployed(results))
        
        return results
    
    def get_job_status(self, job_name: str) -> Dict[str, Any]:
        """Get status of a specific job."""
        result = self.run_kubectl(f"get job {job_name} -n {self.namespace} -o json")
        
        if result.returncode != 0:
            return {"status": "not_found", "error": result.stderr}
        
        try:
            job_data = json.loads(result.stdout)
            status = job_data.get("status", {})
            
            return {
                "name": job_name,
                "active": status.get("active", 0),
                "succeeded": status.get("succeeded", 0),
                "failed": status.get("failed", 0),
                "start_time": status.get("startTime"),
                "completion_time": status.get("completionTime"),
                "conditions": status.get("conditions", [])
            }
            
        except json.JSONDecodeError:
            return {"status": "error", "error": "Failed to parse job status"}
    
    def get_all_job_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all backfill jobs."""
        logger.info("📊 Checking status of all 30-year minute backfill jobs...")
        
        statuses = {}
        
        # Check orchestrator
        statuses["orchestrator"] = self.get_job_status(self.job_names["orchestrator"])
        
        # Check all vendor jobs
        for vendor, job_name in self.job_names.items():
            if vendor != "orchestrator":
                statuses[vendor] = self.get_job_status(job_name)
        
        return statuses
    
    def print_status_summary(self, statuses: Dict[str, Dict[str, Any]], send_slack: bool = False):
        """Print a formatted status summary."""
        print("\n" + "="*80)
        print("📊 30-YEAR MINUTE BACKFILL JOB STATUS SUMMARY")
        print("="*80)
        
        for vendor, status in statuses.items():
            if status.get("status") == "not_found":
                print(f"{vendor.upper():<15}: ❌ Not deployed")
            elif status.get("status") == "error":
                print(f"{vendor.upper():<15}: ⚠️  Error: {status.get('error', 'Unknown')}")
            else:
                active = status.get("active", 0)
                succeeded = status.get("succeeded", 0)
                failed = status.get("failed", 0)
                
                if succeeded > 0:
                    print(f"{vendor.upper():<15}: ✅ Completed")
                elif failed > 0:
                    print(f"{vendor.upper():<15}: ❌ Failed")
                elif active > 0:
                    print(f"{vendor.upper():<15}: 🔄 Running")
                else:
                    print(f"{vendor.upper():<15}: ⏳ Pending")
                
                if status.get("start_time"):
                    print(f"{'  Start Time:':<15} {status['start_time']}")
                if status.get("completion_time"):
                    print(f"{'  Completed:':<15} {status['completion_time']}")
        
        print("="*80)
        
        # Send Slack notification if requested
        if send_slack:
            asyncio.run(self.slack_notifier.notify_status_summary(statuses))
    
    def get_job_logs(self, vendor: str, tail_lines: int = 50) -> str:
        """Get logs from a specific vendor job."""
        if vendor not in self.job_names:
            return f"Unknown vendor: {vendor}"
        
        job_name = self.job_names[vendor]
        result = self.run_kubectl(f"logs job/{job_name} -n {self.namespace} --tail={tail_lines}")
        
        if result.returncode == 0:
            return result.stdout
        else:
            return f"Failed to get logs: {result.stderr}"
    
    def monitor_progress(self, refresh_interval: int = 60):
        """Monitor progress of all backfill jobs."""
        logger.info(f"🔍 Monitoring 30-year minute backfill progress (refresh every {refresh_interval}s)")
        logger.info("Press Ctrl+C to stop monitoring")
        
        try:
            while True:
                statuses = self.get_all_job_status()
                
                # Clear screen (simple approach)
                os.system('clear' if os.name == 'posix' else 'cls')
                
                print(f"📊 30-Year Minute Backfill Progress Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                self.print_status_summary(statuses)
                
                # Check if all jobs are complete
                all_complete = True
                for vendor, status in statuses.items():
                    if status.get("succeeded", 0) == 0 and status.get("status") != "not_found":
                        all_complete = False
                        break
                
                if all_complete:
                    print("🎉 All backfill jobs completed!")
                    break
                
                print(f"\n⏰ Next refresh in {refresh_interval} seconds...")
                time.sleep(refresh_interval)
                
        except KeyboardInterrupt:
            print("\n\n⚠️  Monitoring stopped by user")
    
    def cleanup_jobs(self, keep_completed: bool = False) -> Dict[str, bool]:
        """Clean up backfill jobs."""
        logger.info("🧹 Cleaning up 30-year minute backfill jobs...")
        
        statuses = self.get_all_job_status()
        results = {}
        
        for vendor, job_name in self.job_names.items():
            status = statuses.get(vendor, {})
            
            # Skip if job doesn't exist
            if status.get("status") == "not_found":
                results[vendor] = True  # Nothing to clean
                continue
            
            # Skip completed jobs if keeping them
            if keep_completed and status.get("succeeded", 0) > 0:
                logger.info(f"⏭️  Keeping completed job: {vendor}")
                results[vendor] = True
                continue
            
            # Delete the job
            result = self.run_kubectl(f"delete job {job_name} -n {self.namespace}")
            
            if result.returncode == 0:
                logger.info(f"✅ Cleaned up {vendor} job")
                results[vendor] = True
            else:
                logger.error(f"❌ Failed to clean up {vendor} job")
                results[vendor] = False
        
        successful = sum(results.values())
        total = len(results)
        logger.info(f"📊 Cleanup summary: {successful}/{total} jobs cleaned successfully")
        
        return results
    
    def create_checkpoint_directories(self):
        """Create checkpoint directories on the host."""
        checkpoint_base = Path("/home/jianjun/ats-data/checkpoints")
        
        # Create directories for each vendor
        for vendor in self.vendor_jobs.keys():
            vendor_dir = checkpoint_base / vendor
            vendor_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"📁 Created checkpoint directory: {vendor_dir}")
        
        # Create master checkpoint directory
        master_dir = checkpoint_base / "master"
        master_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Created master checkpoint directory: {master_dir}")
    
    def show_checkpoint_status(self):
        """Show status of checkpoint files."""
        checkpoint_base = Path("/home/jianjun/ats-data/checkpoints")
        
        print("\n" + "="*60)
        print("📂 CHECKPOINT STATUS")
        print("="*60)
        
        if not checkpoint_base.exists():
            print("❌ Checkpoint directory not found")
            return
        
        for vendor in ["master"] + list(self.vendor_jobs.keys()):
            vendor_dir = checkpoint_base / vendor
            
            if not vendor_dir.exists():
                print(f"{vendor.upper():<15}: ❌ No checkpoint directory")
                continue
            
            checkpoint_files = list(vendor_dir.glob("*.json"))
            if checkpoint_files:
                print(f"{vendor.upper():<15}: ✅ {len(checkpoint_files)} checkpoint file(s)")
                
                # Show latest checkpoint info
                latest = max(checkpoint_files, key=lambda f: f.stat().st_mtime)
                mod_time = datetime.fromtimestamp(latest.stat().st_mtime)
                print(f"{'  Latest:':<15} {latest.name} ({mod_time.strftime('%Y-%m-%d %H:%M:%S')})")
            else:
                print(f"{vendor.upper():<15}: ⏳ No checkpoint files yet")
        
        print("="*60)


def main():
    parser = argparse.ArgumentParser(
        description="30-Year Minute Data Backfill Deployment Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Deploy all vendor backfill jobs
    python deploy_30year_minute_backfill.py --deploy all
    
    # Deploy specific vendor
    python deploy_30year_minute_backfill.py --deploy polygon
    
    # Deploy only the orchestrator
    python deploy_30year_minute_backfill.py --deploy orchestrator
    
    # Check status of all jobs
    python deploy_30year_minute_backfill.py --status
    
    # Monitor progress with live updates
    python deploy_30year_minute_backfill.py --monitor
    
    # Show job logs
    python deploy_30year_minute_backfill.py --logs polygon
    
    # Show checkpoint status
    python deploy_30year_minute_backfill.py --checkpoints
    
    # Clean up all jobs (keeping completed ones)
    python deploy_30year_minute_backfill.py --cleanup --keep-completed
        """
    )
    
    parser.add_argument(
        '--deploy',
        choices=['all', 'orchestrator', 'polygon', 'tiingo', 'fmp', 'eodhd'],
        help='Deploy backfill job(s)'
    )
    parser.add_argument(
        '--status',
        action='store_true',
        help='Show status of all backfill jobs'
    )
    parser.add_argument(
        '--status-slack',
        action='store_true',
        help='Show status and send Slack notification'
    )
    parser.add_argument(
        '--monitor',
        action='store_true',
        help='Monitor job progress with live updates'
    )
    parser.add_argument(
        '--logs',
        choices=['orchestrator', 'polygon', 'tiingo', 'fmp', 'eodhd'],
        help='Show logs for specific job'
    )
    parser.add_argument(
        '--checkpoints',
        action='store_true',
        help='Show checkpoint status'
    )
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Clean up backfill jobs'
    )
    parser.add_argument(
        '--keep-completed',
        action='store_true',
        help='Keep completed jobs during cleanup'
    )
    parser.add_argument(
        '--refresh-interval',
        type=int,
        default=60,
        help='Monitoring refresh interval in seconds (default: 60)'
    )
    
    args = parser.parse_args()
    
    manager = MinuteBackfillDeploymentManager()
    
    # Ensure checkpoint directories exist
    manager.create_checkpoint_directories()
    
    if args.deploy:
        if args.deploy == "all":
            results = manager.deploy_all()
            print(f"\n📊 Deployment Results: {results}")
        elif args.deploy == "orchestrator":
            manager.deploy_orchestrator()
        else:
            manager.deploy_vendor(args.deploy)
    
    elif args.status or args.status_slack:
        statuses = manager.get_all_job_status()
        manager.print_status_summary(statuses, send_slack=args.status_slack)
    
    elif args.monitor:
        manager.monitor_progress(args.refresh_interval)
    
    elif args.logs:
        logs = manager.get_job_logs(args.logs)
        print(f"\n📋 {args.logs.upper()} Job Logs:")
        print("="*60)
        print(logs)
    
    elif args.checkpoints:
        manager.show_checkpoint_status()
    
    elif args.cleanup:
        manager.cleanup_jobs(keep_completed=args.keep_completed)
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()