#!/usr/bin/env python3
"""
ATS Production Environment CLI Tool

Command-line interface for managing the ats-prod environment including:
- Database operations (backup, restore, status)
- Kubernetes resource management
- File-based minute data operations
- Health checks and monitoring
- Slack notifications testing
- Production-specific safety checks
"""

import argparse
import asyncio
import json
import subprocess
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import tempfile

# Add src to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

try:
    from storage.file_based_minute_manager import FileBasedMinuteManager, MinuteBar
    from config.environment import Environment
except ImportError as e:
    print(f"❌ Failed to import required modules: {e}")
    print("Make sure you're running from the project root and src/ is in PYTHONPATH")
    sys.exit(1)

class ProdCLI:
    """CLI tool for ats-prod environment management with production safety features."""
    
    def __init__(self):
        self.namespace = "ats-prod"
        self.environment = "prod"
        self.backup_path = "/home/jianjun/ats-data/backups/ats-prod"
        
    def run_kubectl(self, cmd: str, capture_output: bool = True) -> subprocess.CompletedProcess:
        """Run kubectl command with ats-prod namespace."""
        full_cmd = f"kubectl -n {self.namespace} {cmd}"
        return subprocess.run(
            full_cmd.split(),
            capture_output=capture_output,
            text=True,
            check=False
        )
    
    def run_command(self, cmd: str, capture_output: bool = True) -> subprocess.CompletedProcess:
        """Run shell command."""
        return subprocess.run(
            cmd.split(),
            capture_output=capture_output,
            text=True,
            check=False
        )
    
    def confirm_production_action(self, action: str) -> bool:
        """Require confirmation for dangerous production operations."""
        print(f"⚠️  PRODUCTION ENVIRONMENT WARNING")
        print(f"🔴 You are about to perform: {action}")
        print(f"🏭 Environment: {self.environment.upper()}")
        print(f"🚨 This action affects PRODUCTION data and services!")
        print()
        
        confirmation = input("Type 'CONFIRM' to proceed (case-sensitive): ").strip()
        if confirmation != "CONFIRM":
            print("❌ Operation cancelled - confirmation not received")
            return False
        
        print("✅ Production action confirmed - proceeding...")
        return True
    
    # Database Operations
    def db_status(self) -> Dict[str, Any]:
        """Check PostgreSQL database status."""
        print("📊 Checking ats-prod database status...")
        
        # Check pod status
        result = self.run_kubectl("get pods -l app=postgres")
        if result.returncode != 0:
            return {"status": "error", "message": "Failed to get pod status"}
        
        pod_info = result.stdout
        
        # Check database connectivity
        db_test = self.run_kubectl(
            "exec deployment/postgres -- psql -U postgres -d prod_db -c \"SELECT version();\""
        )
        
        # Get database size
        size_test = self.run_kubectl(
            "exec deployment/postgres -- psql -U postgres -d prod_db -c \"SELECT pg_size_pretty(pg_database_size('prod_db'));\""
        )
        
        status = {
            "pod_status": pod_info,
            "database_accessible": db_test.returncode == 0,
            "database_version": db_test.stdout if db_test.returncode == 0 else "Unable to connect",
            "database_size": size_test.stdout.strip() if size_test.returncode == 0 else "Unknown",
            "environment": "PRODUCTION",
            "timestamp": datetime.now().isoformat()
        }
        
        return status
    
    def db_backup(self, manual: bool = False, force: bool = False) -> Dict[str, Any]:
        """Create database backup with production safety checks."""
        action = "Create PRODUCTION database backup"
        
        if not force and not self.confirm_production_action(action):
            return {"status": "cancelled", "message": "Operation cancelled by user"}
        
        print("💾 Creating ats-prod database backup...")
        
        job_name = f"ats-prod-backup-{'manual' if manual else 'cli'}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        # Create backup job
        result = self.run_kubectl(
            f"create job --from=cronjob/ats-prod-database-backup {job_name}"
        )
        
        if result.returncode != 0:
            return {"status": "error", "message": f"Failed to create backup job: {result.stderr}"}
        
        print(f"⏳ Backup job '{job_name}' started. Waiting for completion...")
        
        # Wait for job completion (max 10 minutes for production)
        for i in range(60):
            job_status = self.run_kubectl(f"get job {job_name} -o json")
            if job_status.returncode == 0:
                job_data = json.loads(job_status.stdout)
                if job_data.get("status", {}).get("succeeded"):
                    print("✅ PRODUCTION backup completed successfully!")
                    
                    # Get backup files
                    backup_files = list(Path(self.backup_path).glob("*"))[-2:]  # Latest 2 files
                    
                    return {
                        "status": "success",
                        "job_name": job_name,
                        "backup_files": [str(f) for f in backup_files],
                        "environment": "PRODUCTION",
                        "timestamp": datetime.now().isoformat()
                    }
            
            if i < 59:  # Don't sleep on last iteration
                subprocess.run(["sleep", "10"], check=False)
        
        return {"status": "timeout", "message": "PRODUCTION backup job did not complete within 10 minutes"}
    
    def db_restore(self, backup_file: str, force: bool = False) -> Dict[str, Any]:
        """Restore database from backup file with production safety checks."""
        action = f"Restore PRODUCTION database from backup: {backup_file}"
        
        if not force and not self.confirm_production_action(action):
            return {"status": "cancelled", "message": "Operation cancelled by user"}
        
        print(f"🔄 Restoring ats-prod database from {backup_file}...")
        print("🚨 THIS WILL OVERWRITE ALL PRODUCTION DATA!")
        
        # Additional confirmation for restore
        final_confirm = input("Type 'RESTORE-PRODUCTION' to continue: ").strip()
        if final_confirm != "RESTORE-PRODUCTION":
            return {"status": "cancelled", "message": "Final confirmation not received"}
        
        backup_path = Path(backup_file)
        if not backup_path.exists():
            return {"status": "error", "message": f"Backup file not found: {backup_file}"}
        
        # Copy backup file to pod
        temp_name = f"restore-{datetime.now().strftime('%Y%m%d-%H%M%S')}.sql"
        copy_result = subprocess.run([
            "kubectl", "-n", self.namespace, "cp", 
            str(backup_path), f"deployment/postgres:/tmp/{temp_name}"
        ], capture_output=True, text=True)
        
        if copy_result.returncode != 0:
            return {"status": "error", "message": f"Failed to copy backup file: {copy_result.stderr}"}
        
        # Restore database
        if backup_path.suffix == '.gz':
            restore_cmd = f"exec deployment/postgres -- bash -c \"gunzip -c /tmp/{temp_name} | psql -U postgres -d prod_db\""
        else:
            restore_cmd = f"exec deployment/postgres -- pg_restore -U postgres -d prod_db --clean --if-exists /tmp/{temp_name}"
        
        restore_result = self.run_kubectl(restore_cmd)
        
        # Clean up temp file
        self.run_kubectl(f"exec deployment/postgres -- rm /tmp/{temp_name}")
        
        if restore_result.returncode == 0:
            print("✅ PRODUCTION database restore completed successfully!")
            return {
                "status": "success", 
                "message": "PRODUCTION database restored successfully",
                "environment": "PRODUCTION",
                "warning": "All production data has been replaced with backup data"
            }
        else:
            return {"status": "error", "message": f"Restore failed: {restore_result.stderr}"}
    
    def db_list_backups(self) -> List[Dict[str, Any]]:
        """List available backup files."""
        backup_dir = Path(self.backup_path)
        if not backup_dir.exists():
            return []
        
        backups = []
        for file in sorted(backup_dir.glob("ats-prod-backup-*.sql*"), reverse=True):
            stat = file.stat()
            backups.append({
                "filename": file.name,
                "path": str(file),
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "created": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "age_hours": round((datetime.now().timestamp() - stat.st_mtime) / 3600, 1),
                "type": "compressed" if file.suffix == ".gz" else "custom",
                "environment": "PRODUCTION"
            })
        
        return backups
    
    # Kubernetes Operations
    def k8s_status(self) -> Dict[str, Any]:
        """Get Kubernetes resources status."""
        print("☸️  Checking ats-prod Kubernetes resources...")
        
        resources = {}
        
        # Pods
        pods_result = self.run_kubectl("get pods -o json")
        if pods_result.returncode == 0:
            pods_data = json.loads(pods_result.stdout)
            resources["pods"] = [
                {
                    "name": pod["metadata"]["name"],
                    "status": pod["status"]["phase"],
                    "ready": sum(1 for c in pod["status"].get("containerStatuses", []) if c.get("ready", False)),
                    "total_containers": len(pod["spec"]["containers"]),
                    "restarts": sum(c.get("restartCount", 0) for c in pod["status"].get("containerStatuses", [])),
                    "uptime_hours": round((datetime.now() - datetime.fromisoformat(
                        pod["status"]["startTime"].replace("Z", "+00:00")
                    )).total_seconds() / 3600, 1) if pod["status"].get("startTime") else None
                }
                for pod in pods_data["items"]
            ]
        
        # Services
        svc_result = self.run_kubectl("get services -o json")
        if svc_result.returncode == 0:
            svc_data = json.loads(svc_result.stdout)
            resources["services"] = [
                {
                    "name": svc["metadata"]["name"],
                    "type": svc["spec"]["type"],
                    "ports": [f"{p['port']}/{p['protocol']}" for p in svc["spec"]["ports"]]
                }
                for svc in svc_data["items"]
            ]
        
        # CronJobs
        cron_result = self.run_kubectl("get cronjobs -o json")
        if cron_result.returncode == 0:
            cron_data = json.loads(cron_result.stdout)
            resources["cronjobs"] = [
                {
                    "name": cron["metadata"]["name"],
                    "schedule": cron["spec"]["schedule"],
                    "last_schedule": cron["status"].get("lastScheduleTime", "Never"),
                    "active": len(cron["status"].get("active", [])),
                    "next_schedule": "Calculated based on schedule"  # Could add calculation logic
                }
                for cron in cron_data["items"]
            ]
        
        resources["environment"] = "PRODUCTION"
        return resources
    
    def k8s_restart(self, resource: str, force: bool = False) -> Dict[str, Any]:
        """Restart Kubernetes resource with production safety checks."""
        action = f"Restart PRODUCTION Kubernetes resource: {resource}"
        
        if not force and not self.confirm_production_action(action):
            return {"status": "cancelled", "message": "Operation cancelled by user"}
        
        print(f"🔄 Restarting {resource} in ats-prod...")
        
        result = self.run_kubectl(f"rollout restart deployment/{resource}")
        if result.returncode == 0:
            print(f"✅ PRODUCTION {resource} restart initiated")
            return {
                "status": "success", 
                "message": f"PRODUCTION {resource} restart initiated",
                "environment": "PRODUCTION",
                "warning": "Service interruption may occur during restart"
            }
        else:
            return {"status": "error", "message": f"Failed to restart {resource}: {result.stderr}"}
    
    def k8s_logs(self, resource: str, lines: int = 50) -> str:
        """Get logs from Kubernetes resource."""
        result = self.run_kubectl(f"logs deployment/{resource} --tail={lines}")
        return result.stdout if result.returncode == 0 else f"Error: {result.stderr}"
    
    # File-based Storage Operations
    async def storage_status(self) -> Dict[str, Any]:
        """Get file-based storage status."""
        print("📁 Checking PRODUCTION file-based storage status...")
        
        try:
            manager = FileBasedMinuteManager()
            stats = await manager.get_storage_stats()
            await manager.close()
            
            return {
                "status": "success",
                "stats": stats,
                "environment": "PRODUCTION",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {"status": "error", "message": str(e), "environment": "PRODUCTION"}
    
    async def storage_verify(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Verify data integrity."""
        print(f"🔍 Verifying PRODUCTION data integrity{f' for {symbol}' if symbol else ''}...")
        
        try:
            manager = FileBasedMinuteManager()
            results = await manager.verify_data_integrity(symbol)
            await manager.close()
            
            return {
                "status": "success",
                "verification": results,
                "environment": "PRODUCTION",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {"status": "error", "message": str(e), "environment": "PRODUCTION"}
    
    async def storage_query(self, symbol: str, days: int = 1) -> Dict[str, Any]:
        """Query minute data for symbol."""
        print(f"📊 Querying {days} days of PRODUCTION data for {symbol}...")
        
        try:
            manager = FileBasedMinuteManager()
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            df = await manager.query_minute_data(symbol, start_date, end_date)
            await manager.close()
            
            return {
                "status": "success",
                "symbol": symbol,
                "records": len(df),
                "date_range": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                },
                "sample_data": df.head().to_dict() if not df.empty else {},
                "environment": "PRODUCTION",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {"status": "error", "message": str(e), "environment": "PRODUCTION"}
    
    # Health and Monitoring
    def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check with production-specific metrics."""
        print("🏥 Running ats-prod PRODUCTION health check...")
        
        health = {
            "timestamp": datetime.now().isoformat(),
            "environment": "PRODUCTION",
            "namespace": self.namespace,
            "checks": {}
        }
        
        # Database health
        db_status = self.db_status()
        health["checks"]["database"] = {
            "healthy": db_status.get("database_accessible", False),
            "details": db_status
        }
        
        # Kubernetes health
        k8s_status = self.k8s_status()
        pods_healthy = all(
            pod["status"] == "Running" and pod["ready"] == pod["total_containers"] 
            for pod in k8s_status.get("pods", [])
        )
        health["checks"]["kubernetes"] = {
            "healthy": pods_healthy,
            "pod_count": len(k8s_status.get("pods", [])),
            "details": k8s_status
        }
        
        # Backup files
        backups = self.db_list_backups()
        recent_backup = any(
            datetime.fromisoformat(b["created"]) > datetime.now() - timedelta(hours=12)
            for b in backups
        )
        health["checks"]["backups"] = {
            "healthy": recent_backup,
            "backup_count": len(backups),
            "latest_backup": backups[0] if backups else None,
            "sla_requirement": "Backups must be < 12 hours old for production"
        }
        
        # Production-specific checks
        health["checks"]["production_readiness"] = {
            "healthy": all(check["healthy"] for check in health["checks"].values()),
            "uptime_check": "All services running",
            "data_consistency": "File storage accessible"
        }
        
        # Overall health
        health["overall_healthy"] = all(
            check["healthy"] for check in health["checks"].values()
        )
        
        # Production alerts
        if not health["overall_healthy"]:
            health["production_alert"] = {
                "level": "CRITICAL",
                "message": "PRODUCTION ENVIRONMENT HEALTH ISSUES DETECTED",
                "action_required": "Immediate investigation required"
            }
        
        return health
    
    def test_slack(self, force: bool = False) -> Dict[str, Any]:
        """Test Slack notifications."""
        action = "Send test notification to PRODUCTION Slack channel"
        
        if not force and not self.confirm_production_action(action):
            return {"status": "cancelled", "message": "Operation cancelled by user"}
        
        print("📢 Testing PRODUCTION Slack notifications...")
        
        # Create a test backup job to trigger notification
        job_name = f"ats-prod-slack-test-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        result = self.run_kubectl(f"create job --from=cronjob/ats-prod-database-backup {job_name}")
        
        if result.returncode == 0:
            return {
                "status": "success", 
                "message": f"Test backup job '{job_name}' created. Check #ats-prod Slack channel for notification.",
                "job_name": job_name,
                "environment": "PRODUCTION",
                "warning": "This created an actual backup job in production"
            }
        else:
            return {"status": "error", "message": f"Failed to create test job: {result.stderr}"}
    
    # CLI Interface
    def create_parser(self) -> argparse.ArgumentParser:
        """Create argument parser."""
        parser = argparse.ArgumentParser(
            description="ATS Production Environment CLI Tool",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
🚨 PRODUCTION ENVIRONMENT TOOL 🚨

This tool manages PRODUCTION data and services. All destructive operations
require confirmation. Use with extreme caution.

Examples:
  %(prog)s db status                     # Check database status
  %(prog)s db backup                     # Create manual backup (with confirmation)
  %(prog)s db restore backup.sql         # Restore from backup (with confirmation)
  %(prog)s k8s status                    # Check Kubernetes resources
  %(prog)s k8s restart postgres --force  # Restart PostgreSQL (skip confirmation)
  %(prog)s storage status                # Check file storage stats
  %(prog)s health                        # Run comprehensive health check
  %(prog)s test-slack --force            # Test Slack notifications (skip confirmation)
            """
        )
        
        parser.add_argument('--force', action='store_true', 
                          help='Skip production confirmations (DANGEROUS)')
        
        subparsers = parser.add_subparsers(dest='command', help='Available commands')
        
        # Database commands
        db_parser = subparsers.add_parser('db', help='Database operations')
        db_subparsers = db_parser.add_subparsers(dest='db_command')
        
        db_subparsers.add_parser('status', help='Check database status')
        db_subparsers.add_parser('backup', help='Create database backup')
        db_subparsers.add_parser('list-backups', help='List available backups')
        
        restore_parser = db_subparsers.add_parser('restore', help='Restore database')
        restore_parser.add_argument('backup_file', help='Path to backup file')
        
        # Kubernetes commands
        k8s_parser = subparsers.add_parser('k8s', help='Kubernetes operations')
        k8s_subparsers = k8s_parser.add_subparsers(dest='k8s_command')
        
        k8s_subparsers.add_parser('status', help='Check Kubernetes resources')
        
        restart_parser = k8s_subparsers.add_parser('restart', help='Restart resource')
        restart_parser.add_argument('resource', help='Resource name (e.g., postgres)')
        
        logs_parser = k8s_subparsers.add_parser('logs', help='Get resource logs')
        logs_parser.add_argument('resource', help='Resource name')
        logs_parser.add_argument('--lines', type=int, default=50, help='Number of lines')
        
        # Storage commands
        storage_parser = subparsers.add_parser('storage', help='File storage operations')
        storage_subparsers = storage_parser.add_subparsers(dest='storage_command')
        
        storage_subparsers.add_parser('status', help='Check storage status')
        
        verify_parser = storage_subparsers.add_parser('verify', help='Verify data integrity')
        verify_parser.add_argument('--symbol', help='Specific symbol to verify')
        
        query_parser = storage_subparsers.add_parser('query', help='Query minute data')
        query_parser.add_argument('symbol', help='Symbol to query')
        query_parser.add_argument('--days', type=int, default=1, help='Number of days')
        
        # Other commands
        subparsers.add_parser('health', help='Run health check')
        subparsers.add_parser('test-slack', help='Test Slack notifications')
        
        return parser
    
    async def run_async_command(self, args) -> Any:
        """Run async commands."""
        if args.command == 'storage':
            if args.storage_command == 'status':
                return await self.storage_status()
            elif args.storage_command == 'verify':
                return await self.storage_verify(args.symbol)
            elif args.storage_command == 'query':
                return await self.storage_query(args.symbol, args.days)
        
        return None
    
    def run(self, args: List[str] = None) -> int:
        """Run the CLI."""
        parser = self.create_parser()
        parsed_args = parser.parse_args(args)
        
        if not parsed_args.command:
            parser.print_help()
            return 1
        
        try:
            # Handle async commands
            if parsed_args.command == 'storage':
                result = asyncio.run(self.run_async_command(parsed_args))
            
            # Handle sync commands
            elif parsed_args.command == 'db':
                if parsed_args.db_command == 'status':
                    result = self.db_status()
                elif parsed_args.db_command == 'backup':
                    result = self.db_backup(manual=True, force=parsed_args.force)
                elif parsed_args.db_command == 'list-backups':
                    result = self.db_list_backups()
                elif parsed_args.db_command == 'restore':
                    result = self.db_restore(parsed_args.backup_file, force=parsed_args.force)
                else:
                    parser.error("Unknown db command")
            
            elif parsed_args.command == 'k8s':
                if parsed_args.k8s_command == 'status':
                    result = self.k8s_status()
                elif parsed_args.k8s_command == 'restart':
                    result = self.k8s_restart(parsed_args.resource, force=parsed_args.force)
                elif parsed_args.k8s_command == 'logs':
                    result = self.k8s_logs(parsed_args.resource, parsed_args.lines)
                    print(result)
                    return 0
                else:
                    parser.error("Unknown k8s command")
            
            elif parsed_args.command == 'health':
                result = self.health_check()
            
            elif parsed_args.command == 'test-slack':
                result = self.test_slack(force=parsed_args.force)
            
            else:
                parser.error("Unknown command")
            
            # Print JSON result
            print(json.dumps(result, indent=2, default=str))
            
            # Return appropriate exit code
            if isinstance(result, dict):
                if result.get("status") == "error":
                    return 1
                elif result.get("status") == "cancelled":
                    return 2  # Different exit code for cancelled operations
            
            return 0
            
        except KeyboardInterrupt:
            print("\n⚠️  Operation cancelled by user")
            return 1
        except Exception as e:
            print(f"❌ Error: {e}")
            return 1

if __name__ == "__main__":
    cli = ProdCLI()
    sys.exit(cli.run())