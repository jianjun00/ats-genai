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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MinuteBackfillDeploymentManager:
    """Manages deployment and monitoring of 30-year minute backfill jobs."""
    
    def __init__(self):
        self.namespace = "ats-dev"
        self.k8s_dir = Path(__file__).parent.parent.parent / "k8s"
        
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
        
        if result.returncode == 0:
            logger.info(f"✅ {vendor.upper()} backfill job deployed successfully")
            return True
        else:
            logger.error(f"❌ Failed to deploy {vendor.upper()} backfill job")
            return False
    
    def deploy_orchestrator(self) -> bool:
        """Deploy the comprehensive orchestrator job."""
        job_file = self.k8s_dir / self.orchestrator_job
        if not job_file.exists():
            logger.error(f"Orchestrator job file not found: {job_file}")
            return False
        
        logger.info("🚀 Deploying comprehensive 30-year minute backfill orchestrator...")
        
        result = self.run_kubectl(f"apply -f {job_file}")
        
        if result.returncode == 0:
            logger.info("✅ Orchestrator job deployed successfully")
            return True
        else:
            logger.error("❌ Failed to deploy orchestrator job")
            return False
    
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
    
    def print_status_summary(self, statuses: Dict[str, Dict[str, Any]]):
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
    
    elif args.status:
        statuses = manager.get_all_job_status()
        manager.print_status_summary(statuses)
    
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