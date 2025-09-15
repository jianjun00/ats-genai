#!/usr/bin/env python3
"""
Integration CLI for running integration environment operations with Docker and localhost services

Automatically handles Docker operations, database connections, and service management
for the integration environment without requiring Kubernetes knowledge.
"""

import subprocess
import sys
import time
import argparse
import os
import json
import yaml
from pathlib import Path

class IntgCLI:
    def __init__(self):
        self.db_host = "localhost"
        self.db_port = "4432"  # Integration PostgreSQL port (matches docker-compose.ats.yml)
        self.db_user = "postgres"
        self.db_password = "intg_password"
        self.db_name = "intg_db"

        # ATS persistent volume paths (D: drive) - Integration specific
        self.ats_data_path = "/mnt/d/ats-data/intg"
        self.ats_backup_path = "/mnt/d/ats-backup/intg"
        self.ats_logs_path = "/mnt/d/ats-logs/intg"

        # Ensure ATS directories exist
        self.ensure_ats_directories()

        # Check if we need to use port-forwarded connection
        self.check_database_connection()

    def ensure_ats_directories(self):
        """Ensure ATS directories exist on D: drive"""
        for path in [self.ats_data_path, self.ats_backup_path, self.ats_logs_path]:
            if not os.path.exists(path):
                try:
                    os.makedirs(path, exist_ok=True)
                    print(f"📁 Created directory: {path}")
                except Exception as e:
                    print(f"⚠️  Could not create {path}: {e}")

    def get_volume_mounts(self):
        """Get Docker volume mount string for ATS directories"""
        volumes = []
        volumes.append(f"-v {os.getcwd()}:/workspace")

        # Add ATS persistent volumes if they exist
        if os.path.exists(self.ats_data_path):
            volumes.append(f"-v {self.ats_data_path}:/data")
        if os.path.exists(self.ats_backup_path):
            volumes.append(f"-v {self.ats_backup_path}:/backup")
        if os.path.exists(self.ats_logs_path):
            volumes.append(f"-v {self.ats_logs_path}:/logs")

        return " ".join(volumes)

    def check_database_connection(self):
        """Check which database connection works"""
        # Try localhost:4432 first (integration PostgreSQL)
        if self.test_db_connection("localhost", "4432"):
            self.db_host = "localhost"
            self.db_port = "4432"
            return

        print("⚠️  No integration database connection available. You may need to:")
        print("   1. Start integration PostgreSQL: python scripts/run_intg.py start --service postgres")
        print("   2. Or check integration database is running on port 4432")

    def test_db_connection(self, host, port):
        """Test database connection"""
        try:
            cmd = f'PGPASSWORD={self.db_password} psql -h {host} -p {port} -U {self.db_user} -d {self.db_name} -c "SELECT 1" > /dev/null 2>&1'
            result = subprocess.run(cmd, shell=True, capture_output=True)
            return result.returncode == 0
        except:
            return False

    def run_command(self, cmd, description=None):
        """Run command and handle output"""
        if description:
            print(f"🔧 {description}")

        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                return result.stdout.strip()
            else:
                print(f"❌ Command failed: {cmd}")
                print(f"Error: {result.stderr}")
                return None
        except Exception as e:
            print(f"❌ Exception running command: {e}")
            return None

    def run_docker_job(self, script_path, job_name=None, gpu=False, environment=None):
        """Run a job using Docker instead of Kubernetes"""
        if not os.path.exists(script_path):
            print(f"❌ Script not found: {script_path}")
            return False

        print(f"🐳 Running Integration Docker job: {script_path}")

        # Build Docker command
        gpu_flag = "--gpus all" if gpu else ""
        env_vars = ""
        if environment:
            env_vars = " ".join([f"-e {k}={v}" for k, v in environment.items()])

        # Use our official image
        image = "dragonflyer762/ats-genai:latest"

        # Mount directories and set database connection
        volume_mounts = self.get_volume_mounts()
        cmd = f"""docker run --rm --network host {gpu_flag} \\
            {volume_mounts} \\
            -w /workspace \\
            -e DB_HOST={self.db_host} \\
            -e DB_PORT={self.db_port} \\
            -e DB_USER={self.db_user} \\
            -e DB_PASSWORD={self.db_password} \\
            -e DB_NAME={self.db_name} \\
            -e PYTHONPATH=/workspace/src \\
            -e ATS_DATA_PATH=/data \\
            -e ATS_BACKUP_PATH=/backup \\
            -e ATS_LOGS_PATH=/logs \\
            -e ENVIRONMENT=intg \\
            {env_vars} \\
            {image} \\
            python {script_path}"""

        print(f"🚀 Running: docker run ... python {script_path}")
        result = subprocess.run(cmd, shell=True)

        if result.returncode == 0:
            print("✅ Integration job completed successfully")
            return True
        else:
            print(f"❌ Integration job failed with exit code: {result.returncode}")
            return False

    def start_service(self, service_name, port=None, gpu=False, environment=None):
        """Start a service using Docker"""
        print(f"🚀 Starting integration service: {service_name}")

        # Integration service configurations
        services = {
            "postgres": {
                "image": "timescale/timescaledb:latest-pg13",
                "port": "5433:5432",  # Integration port
                "env": {
                    "POSTGRES_USER": self.db_user,
                    "POSTGRES_PASSWORD": self.db_password,
                    "POSTGRES_DB": self.db_name
                },
                "volumes": [
                    "postgres-intg-data:/var/lib/postgresql/data",
                    f"{self.ats_backup_path}:/backup"
                ],
                "backup_restore": True
            },
            "analytics": {
                "image": "dragonflyer762/ats-genai:latest",
                "port": "4000:3000",  # Integration analytics on external port 4000, internal port 3000
                "command": "python src/services/analytics_service.py",
                "env": {
                    "DB_HOST": "ats-intg-postgres",
                    "DB_PORT": "5432",
                    "DB_USER": "postgres",
                    "DB_PASSWORD": "intg_password",
                    "DB_NAME": "intg_db",
                    "ENVIRONMENT": "intg",
                    "POLYGON_API_KEY": os.getenv('POLYGON_API_KEY', 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD'),
                    "TIINGO_API_KEY": os.getenv('TIINGO_API_KEY', '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5'),
                    "EODHD_API_KEY": os.getenv('EODHD_API_KEY', '68aa0c7d2fe831.67386369'),
                    "FMP_API_KEY": os.getenv('FMP_API_KEY', 'Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr'),
                    "ALPHA_VANTAGE_API_KEY": os.getenv('ALPHA_VANTAGE_API_KEY', '9GI0NZ3V4VNFX271'),
                    "FIRSTRATE_USER_ID": os.getenv('FIRSTRATE_USER_ID', 'ats-genai-user'),
                    "OPENAI_API_KEY": os.getenv('OPENAI_API_KEY', '')
                }
            },
            "api": {
                "image": "dragonflyer762/ats-genai:latest",
                "port": "8001:8000",  # Different port for integration
                "command": "python src/api/main.py"
            },
            "news-realtime": {
                "image": "dragonflyer762/ats-genai:latest",
                "port": "8081:8080",  # News metrics port
                "command": "python scripts/realtime_news_ingestion.py --vendors tiingo,polygon,eodhd --interval 300 --daemon",
                "env": {
                    "DB_HOST": "ats-intg-postgres",
                    "DB_PORT": "5432",
                    "DB_USER": "postgres",
                    "DB_PASSWORD": "intg_password",
                    "DB_NAME": "intg_db",
                    "METRICS_PORT": "8080",
                    "TIINGO_API_KEY": os.getenv('TIINGO_API_KEY', '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5'),
                    "POLYGON_API_KEY": os.getenv('POLYGON_API_KEY', 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD'),
                    "EODHD_API_KEY": os.getenv('EODHD_API_KEY', '675b5a33b36f43.67825763')
                }
            },
            "news-backfill": {
                "image": "dragonflyer762/ats-genai:latest",
                "command": "bash -c 'while true; do python3 scripts/multi_vendor_news_backfill.py --vendors tiingo,polygon,eodhd --days 30; echo \"Backfill completed, sleeping 6 hours...\"; sleep 21600; done'",
                "env": {
                    "DB_HOST": "ats-intg-postgres",
                    "DB_PORT": "5432",
                    "DB_USER": "postgres",
                    "DB_PASSWORD": "intg_password",
                    "DB_NAME": "intg_db",
                    "TIINGO_API_KEY": os.getenv('TIINGO_API_KEY', '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5'),
                    "POLYGON_API_KEY": os.getenv('POLYGON_API_KEY', 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD'),
                    "EODHD_API_KEY": os.getenv('EODHD_API_KEY', '675b5a33b36f43.67825763')
                }
            },
            "news-monitor": {
                "image": "dragonflyer762/ats-genai:latest",
                "command": "bash -c 'while true; do python3 scripts/news_health_monitor.py; echo \"Health check completed, sleeping 2 hours...\"; sleep 7200; done'",
                "env": {
                    "DB_HOST": "ats-intg-postgres",
                    "DB_PORT": "5432",
                    "DB_USER": "postgres",
                    "DB_PASSWORD": "intg_password",
                    "DB_NAME": "intg_db"
                }
            },
            "realtime-minute-collector": {
                "image": "dragonflyer762/ats-genai:latest",
                "command": "python3 scripts/realtime_minute_collector.py --test",
                "env": {
                    "DB_HOST": "ats-intg-postgres",
                    "DB_PORT": "5432",
                    "DB_USER": "postgres",
                    "DB_PASSWORD": "intg_password",
                    "DB_NAME": "intg_db",
                    "ENVIRONMENT": "intg",
                    "POLYGON_API_KEY": os.getenv('POLYGON_API_KEY', 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD'),
                    "TIINGO_API_KEY": os.getenv('TIINGO_API_KEY', '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5'),
                    "EODHD_API_KEY": os.getenv('EODHD_API_KEY', '68aa0c7d2fe831.67386369'),
                    "PYTHONPATH": "/workspace/src"
                },
                "healthcheck": "curl -f http://localhost:8080/ || exit 1"
            }
        }

        if service_name not in services:
            print(f"❌ Unknown integration service: {service_name}")
            print(f"Available services: {', '.join(services.keys())}")
            return False

        config = services[service_name]

        # Build Docker command
        gpu_flag = "--gpus all" if gpu else ""
        port_flag = f"-p {port or config['port']}" if ('port' in config or port) else ""

        env_vars = ""
        if 'env' in config:
            env_vars = " ".join([f"-e {k}={v}" for k, v in config['env'].items()])
        if environment:
            env_vars += " " + " ".join([f"-e {k}={v}" for k, v in environment.items()])

        # Container name with intg suffix
        container_name = f"ats-intg-{service_name}"

        # Check if container is already running
        check_cmd = f"docker ps -q -f name={container_name}"
        if subprocess.run(check_cmd, shell=True, capture_output=True).stdout.strip():
            print(f"⚠️  Container {container_name} is already running")
            return True

        # Build volume mounts for service
        volume_mounts = self.get_volume_mounts()
        if 'volumes' in config:
            for volume in config['volumes']:
                volume_mounts += f" -v {volume}"

        # Add healthcheck if defined
        healthcheck_flag = ""
        if 'healthcheck' in config:
            healthcheck_flag = f'--health-cmd="{config["healthcheck"]}" --health-interval=30s --health-timeout=10s --health-start-period=5s --health-retries=3'

        cmd = f"""docker run -d --name {container_name} {gpu_flag} \\
            --network ats-intg-network \\
            {volume_mounts} \\
            -w /workspace \\
            {port_flag} \\
            {healthcheck_flag} \\
            -e PYTHONPATH=/workspace/src \\
            -e ATS_DATA_PATH=/data \\
            -e ATS_BACKUP_PATH=/backup \\
            -e ATS_LOGS_PATH=/logs \\
            -e ENVIRONMENT=intg \\
            -e POSTGRES_INITDB_ARGS="--auth-host=md5 --auth-local=trust" \\
            {env_vars} \\
            {config['image']}"""

        if 'command' in config:
            cmd += f" {config['command']}"

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            print(f"✅ Integration service {service_name} started successfully")
            print(f"🌐 Container name: {container_name}")
            if 'port' in config:
                print(f"🔗 Access at: http://localhost:{config['port'].split(':')[0]}")

            # Handle backup/restore for PostgreSQL services
            if service_name == "postgres" and config.get("backup_restore"):
                self._handle_postgres_backup_restore(container_name, self.db_name, self.ats_backup_path)

            return True
        else:
            print(f"❌ Failed to start integration service: {result.stderr}")
            return False

    def _handle_postgres_backup_restore(self, container_name, db_name, backup_dir):
        """Handle PostgreSQL backup/restore to D: drive"""
        import time

        print("💾 Setting up D: drive backup/restore for integration DB...")

        # Ensure backup directory exists
        os.makedirs(backup_dir, exist_ok=True)

        # Wait for PostgreSQL to be ready
        print("⏳ Waiting for PostgreSQL to be ready...")
        for i in range(30):
            try:
                result = subprocess.run(
                    f"docker exec {container_name} pg_isready -U postgres",
                    shell=True, capture_output=True
                )
                if result.returncode == 0:
                    break
            except:
                pass
            time.sleep(1)
        else:
            print("⚠️  PostgreSQL not ready for backup/restore")
            return

        # Check for existing backup on D: drive
        backup_file = f"{backup_dir}/latest_backup.sql"
        if os.path.exists(backup_file):
            print(f"📤 Restoring from D: drive backup: {backup_file}")
            try:
                # Restore from backup
                restore_cmd = f"cat '{backup_file}' | docker exec -i {container_name} psql -U postgres -d {db_name}"
                subprocess.run(restore_cmd, shell=True, check=True)
                print("✅ Integration database restored from D: drive backup")
            except Exception as e:
                print(f"⚠️  Restore failed: {e}")
        else:
            print("ℹ️  No backup found on D: drive")

        print("🔄 D: drive persistence configured - database will auto-backup on shutdown")

    def stop_service(self, service_name):
        """Stop a Docker service"""
        container_name = f"ats-intg-{service_name}"

        print(f"🛑 Stopping integration service: {service_name}")

        # Backup PostgreSQL before stopping
        if service_name == "postgres":
            self._backup_postgres_to_d_drive(container_name, self.db_name, self.ats_backup_path)

        cmd = f"docker stop {container_name} && docker rm {container_name}"
        result = subprocess.run(cmd, shell=True, capture_output=True)

        if result.returncode == 0:
            print(f"✅ Integration service {service_name} stopped")
            return True
        else:
            print(f"❌ Failed to stop integration service {service_name}")
            return False

    def _backup_postgres_to_d_drive(self, container_name, db_name, backup_dir):
        """Backup PostgreSQL to D: drive before stopping"""
        try:
            print("💾 Backing up integration database to D: drive...")

            # Ensure backup directory exists
            os.makedirs(backup_dir, exist_ok=True)

            # Create timestamped backup
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"{backup_dir}/backup_{timestamp}.sql"
            latest_backup = f"{backup_dir}/latest_backup.sql"

            # Perform backup
            backup_cmd = f"docker exec {container_name} pg_dump -U postgres -d {db_name}"
            with open(backup_file, 'w') as f:
                result = subprocess.run(backup_cmd, shell=True, stdout=f, stderr=subprocess.PIPE)

            if result.returncode == 0:
                # Copy to latest backup
                import shutil
                shutil.copy2(backup_file, latest_backup)
                print(f"✅ Integration database backed up to: {backup_file}")
                print(f"✅ Latest backup: {latest_backup}")
            else:
                print(f"⚠️  Backup failed: {result.stderr.decode()}")

        except Exception as e:
            print(f"⚠️  Backup error: {e}")

    def list_services(self):
        """List running Docker services"""
        print("🐳 Running ATS Integration services:")
        cmd = "docker ps --filter name=ats-intg- --format 'table {{.Names}}\\\\t{{.Status}}\\\\t{{.Ports}}'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.stdout.strip():
            print(result.stdout)
        else:
            print("No ATS integration services currently running")

    def run_test(self, test_path=None, pattern=None):
        """Run tests using Docker"""
        print("🧪 Running integration tests...")

        test_cmd = "pytest"
        if test_path:
            test_cmd += f" {test_path}"
        if pattern:
            test_cmd += f" -k {pattern}"

        test_cmd += " -v"

        volume_mounts = self.get_volume_mounts()
        cmd = f"""docker run --rm \\
            {volume_mounts} \\
            -w /workspace \\
            -e PYTHONPATH=/workspace/src \\
            -e ATS_DATA_PATH=/data \\
            -e ATS_BACKUP_PATH=/backup \\
            -e ATS_LOGS_PATH=/logs \\
            -e ENVIRONMENT=intg \\
            dragonflyer762/ats-genai:latest \\
            {test_cmd}"""

        result = subprocess.run(cmd, shell=True)
        return result.returncode == 0

    def query_db(self, sql_query, description=None):
        """Run database query directly"""
        if description:
            print(f"📊 {description}")

        cmd = f'PGPASSWORD={self.db_password} psql -h {self.db_host} -p {self.db_port} -U {self.db_user} -d {self.db_name} -c "{sql_query}"'
        result = self.run_command(cmd)

        if result:
            print(result)
        return result

    def get_issue(self, issue_id):
        """Get a specific data quality issue by ID"""
        sql = f"""
        SELECT id, issue_type, issue_category, vendor, data_type, symbol,
               affected_date_start, affected_date_end, severity, status,
               complexity, priority_score, resolution_strategy,
               assigned_agent, workflow_id,
               issue_metadata, resolution_metadata,
               created_at, updated_at, resolved_at
        FROM dev_data_quality_issues 
        WHERE id = {issue_id}
        """
        
        print(f"🔍 Getting issue {issue_id}...")
        return self.query_db(sql, f"Data Quality Issue #{issue_id}")

    def list_issues(self, tag=None, symbol=None, status=None, vendor=None, 
                   category=None, severity=None, limit=20):
        """List data quality issues with optional filters"""
        
        where_conditions = []
        
        if tag:
            # Tag is stored in issue_metadata JSONB
            where_conditions.append(f"issue_metadata->>'tag' = '{tag}'")
        if symbol:
            where_conditions.append(f"symbol = '{symbol.upper()}'")
        if status:
            where_conditions.append(f"status = '{status}'")
        if vendor:
            where_conditions.append(f"vendor = '{vendor}'")
        if category:
            where_conditions.append(f"issue_category = '{category}'")
        if severity:
            where_conditions.append(f"severity = '{severity}'")
            
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
            
        sql = f"""
        SELECT id, issue_type, issue_category, vendor, data_type, symbol,
               affected_date_start, affected_date_end, severity, status,
               priority_score, assigned_agent,
               created_at, updated_at
        FROM dev_data_quality_issues 
        {where_clause}
        ORDER BY created_at DESC, priority_score DESC
        LIMIT {limit}
        """
        
        filter_desc = []
        if tag: filter_desc.append(f"tag={tag}")
        if symbol: filter_desc.append(f"symbol={symbol}")
        if status: filter_desc.append(f"status={status}")
        if vendor: filter_desc.append(f"vendor={vendor}")
        if category: filter_desc.append(f"category={category}")
        if severity: filter_desc.append(f"severity={severity}")
        
        desc = "Data Quality Issues"
        if filter_desc:
            desc += f" (filtered by {', '.join(filter_desc)})"
            
        print(f"📋 Listing {desc}...")
        return self.query_db(sql, desc)

    def create_issue(self, issue_type, vendor, symbol, date_start, date_end,
                    category="validation", severity="medium", description=None, tag=None):
        """Create a new data quality issue"""
        
        # Build metadata
        metadata = {}
        if description:
            metadata["description"] = description
        if tag:
            metadata["tag"] = tag
            
        metadata_json = "'{}'::jsonb"
        if metadata:
            import json
            metadata_json = f"'{json.dumps(metadata)}'::jsonb"
        
        sql = f"""
        INSERT INTO dev_data_quality_issues (
            issue_type, issue_category, vendor, data_type, symbol,
            affected_date_start, affected_date_end, severity, status,
            issue_metadata, created_at, updated_at
        ) VALUES (
            '{issue_type}', '{category}', '{vendor}', 'daily_prices', '{symbol.upper()}',
            '{date_start}', '{date_end}', '{severity}', 'pending',
            {metadata_json}, NOW(), NOW()
        ) RETURNING id, issue_type, symbol, severity, status
        """
        
        print(f"➕ Creating issue: {issue_type} for {vendor}/{symbol} ({date_start} to {date_end})")
        return self.query_db(sql, "New Data Quality Issue Created")

    def update_issue(self, issue_id, **kwargs):
        """Update an existing data quality issue"""
        
        valid_fields = {
            'status', 'severity', 'resolution_strategy', 'assigned_agent',
            'complexity', 'priority_score'
        }
        
        updates = []
        for field, value in kwargs.items():
            if field in valid_fields and value is not None:
                updates.append(f"{field} = '{value}'")
        
        if not updates:
            print("❌ No valid fields provided for update")
            return False
            
        updates.append("updated_at = NOW()")
        
        sql = f"""
        UPDATE dev_data_quality_issues 
        SET {', '.join(updates)}
        WHERE id = {issue_id}
        RETURNING id, issue_type, symbol, status, severity, updated_at
        """
        
        print(f"📝 Updating issue {issue_id}...")
        return self.query_db(sql, f"Updated Issue #{issue_id}")

    def resolve_issue(self, issue_id, resolution_notes=None):
        """Mark an issue as resolved"""
        
        metadata = {}
        if resolution_notes:
            metadata["resolution_notes"] = resolution_notes
            metadata["resolved_by"] = "run_intg_cli"
            
        metadata_json = "'{}'::jsonb"
        if metadata:
            import json
            metadata_json = f"'{json.dumps(metadata)}'::jsonb"
        
        sql = f"""
        UPDATE dev_data_quality_issues 
        SET status = 'resolved',
            resolved_at = NOW(),
            updated_at = NOW(),
            resolution_metadata = {metadata_json}
        WHERE id = {issue_id}
        RETURNING id, issue_type, symbol, status, resolved_at
        """
        
        print(f"✅ Resolving issue {issue_id}...")
        return self.query_db(sql, f"Resolved Issue #{issue_id}")

    def delete_issue(self, issue_id):
        """Delete a data quality issue (use with caution)"""
        
        # First show what we're about to delete
        print(f"⚠️  About to delete issue {issue_id}:")
        self.get_issue(issue_id)
        
        confirm = input("Are you sure you want to delete this issue? (yes/NO): ")
        if confirm.lower() != 'yes':
            print("❌ Delete cancelled")
            return False
        
        sql = f"DELETE FROM dev_data_quality_issues WHERE id = {issue_id}"
        
        print(f"🗑️  Deleting issue {issue_id}...")
        return self.query_db(sql, f"Deleted Issue #{issue_id}")

    def setup_intg_env(self):
        """Setup complete integration environment"""
        print("🏗️  Setting up integration environment...")

        # Start PostgreSQL
        if not self.start_service("postgres"):
            return False

        # Wait for database to be ready
        print("⏳ Waiting for integration database to be ready...")
        for i in range(30):
            if self.test_db_connection("localhost", "4434"):
                break
            time.sleep(1)
        else:
            print("❌ Integration database failed to start")
            return False

        print("✅ Integration environment ready!")
        print("🔗 Database: postgresql://postgres:intg_password@localhost:5433/intg_db")
        return True

def main():
    parser = argparse.ArgumentParser(description="Integration CLI for localhost/Docker integration operations")
    
    # Use subparsers for better command structure
    subparsers = parser.add_subparsers(dest="action", help="Available commands")
    
    # Original commands
    parser_run = subparsers.add_parser("run", help="Run a script")
    parser_run.add_argument("--script", "-s", required=True, help="Script to run")
    parser_run.add_argument("--gpu", action="store_true", help="Enable GPU support")
    parser_run.add_argument("--env", help="Environment variables (JSON format)")
    
    parser_start = subparsers.add_parser("start", help="Start a service")
    parser_start.add_argument("--service", required=True, help="Service name")
    parser_start.add_argument("--port", "-p", help="Port mapping")
    parser_start.add_argument("--gpu", action="store_true", help="Enable GPU support")
    parser_start.add_argument("--env", help="Environment variables (JSON format)")
    
    parser_stop = subparsers.add_parser("stop", help="Stop a service")
    parser_stop.add_argument("--service", required=True, help="Service name")
    
    subparsers.add_parser("status", help="Show service status")
    
    parser_test = subparsers.add_parser("test", help="Run tests")
    parser_test.add_argument("--test", "-t", help="Test path or pattern")
    
    parser_query = subparsers.add_parser("query", help="Run database query")
    parser_query.add_argument("--query", "-q", required=True, help="SQL query to run")
    
    subparsers.add_parser("setup", help="Setup integration environment")
    
    parser_logs = subparsers.add_parser("logs", help="Show service logs")
    parser_logs.add_argument("--service", required=True, help="Service name")
    
    # New issue management commands
    parser_issue = subparsers.add_parser("issue", help="Manage data quality issues")
    issue_subparsers = parser_issue.add_subparsers(dest="issue_action", help="Issue commands")
    
    # issue get <id>
    parser_issue_get = issue_subparsers.add_parser("get", help="Get issue by ID")
    parser_issue_get.add_argument("issue_id", type=int, help="Issue ID")
    
    # issue list [filters]
    parser_issue_list = issue_subparsers.add_parser("list", help="List issues with optional filters")
    parser_issue_list.add_argument("--tag", help="Filter by tag")
    parser_issue_list.add_argument("--symbol", help="Filter by symbol")
    parser_issue_list.add_argument("--status", help="Filter by status")
    parser_issue_list.add_argument("--vendor", help="Filter by vendor")
    parser_issue_list.add_argument("--category", help="Filter by category")
    parser_issue_list.add_argument("--severity", help="Filter by severity")
    parser_issue_list.add_argument("--limit", type=int, default=20, help="Limit results (default: 20)")
    
    # issue create
    parser_issue_create = issue_subparsers.add_parser("create", help="Create new issue")
    parser_issue_create.add_argument("issue_type", help="Issue type (e.g., coverage_gap, missing_data)")
    parser_issue_create.add_argument("vendor", help="Vendor (polygon, tiingo, eodhd)")
    parser_issue_create.add_argument("symbol", help="Symbol")
    parser_issue_create.add_argument("date_start", help="Start date (YYYY-MM-DD)")
    parser_issue_create.add_argument("date_end", help="End date (YYYY-MM-DD)")
    parser_issue_create.add_argument("--category", default="validation", help="Category (default: validation)")
    parser_issue_create.add_argument("--severity", default="medium", help="Severity (default: medium)")
    parser_issue_create.add_argument("--description", help="Description")
    parser_issue_create.add_argument("--tag", help="Tag for categorization")
    
    # issue update <id>
    parser_issue_update = issue_subparsers.add_parser("update", help="Update issue")
    parser_issue_update.add_argument("issue_id", type=int, help="Issue ID")
    parser_issue_update.add_argument("--status", help="New status")
    parser_issue_update.add_argument("--severity", help="New severity")
    parser_issue_update.add_argument("--resolution-strategy", help="Resolution strategy")
    parser_issue_update.add_argument("--assigned-agent", help="Assigned agent")
    parser_issue_update.add_argument("--complexity", help="Complexity level")
    parser_issue_update.add_argument("--priority-score", type=int, help="Priority score")
    
    # issue resolve <id>
    parser_issue_resolve = issue_subparsers.add_parser("resolve", help="Resolve issue")
    parser_issue_resolve.add_argument("issue_id", type=int, help="Issue ID")
    parser_issue_resolve.add_argument("--notes", help="Resolution notes")
    
    # issue delete <id>
    parser_issue_delete = issue_subparsers.add_parser("delete", help="Delete issue")
    parser_issue_delete.add_argument("issue_id", type=int, help="Issue ID")

    args = parser.parse_args()

    cli = IntgCLI()

    # Parse environment variables if provided
    environment = None
    if args.env:
        try:
            environment = json.loads(args.env)
        except json.JSONDecodeError:
            print("❌ Invalid JSON format for --env")
            sys.exit(1)

    if args.action == "run":
        if not args.script:
            print("❌ --script required for run action")
            sys.exit(1)
        cli.run_docker_job(args.script, gpu=args.gpu, environment=environment)

    elif args.action == "start":
        if not args.service:
            print("❌ --service required for start action")
            sys.exit(1)
        cli.start_service(args.service, args.port, args.gpu, environment)

    elif args.action == "stop":
        if not args.service:
            print("❌ --service required for stop action")
            sys.exit(1)
        cli.stop_service(args.service)

    elif args.action == "status":
        cli.list_services()

    elif args.action == "test":
        cli.run_test(args.test)

    elif args.action == "query":
        if not args.query:
            print("❌ --query required for query action")
            sys.exit(1)
        cli.query_db(args.query)

    elif args.action == "setup":
        cli.setup_intg_env()

    elif args.action == "logs":
        if not args.service:
            print("❌ --service required for logs action")
            sys.exit(1)
        container_name = f"ats-intg-{args.service}"
        cmd = f"docker logs -f {container_name}"
        subprocess.run(cmd, shell=True)

if __name__ == "__main__":
    main()