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
        self.db_port = "4434"  # Integration PostgreSQL port (updated to 400x range)
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
        # Try localhost:5433 first (integration PostgreSQL)
        if self.test_db_connection("localhost", "4434"):
            self.db_host = "localhost"
            self.db_port = "4434"
            return

        print("⚠️  No integration database connection available. You may need to:")
        print("   1. Start integration PostgreSQL: python scripts/run_intg.py start --service postgres")
        print("   2. Or check integration database is running on port 5433")

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
                "port": "4000:4000",  # Consistent with ATS-DEV analytics on 3000
                "command": "python src/analytics/server.py"
            },
            "api": {
                "image": "dragonflyer762/ats-genai:latest",
                "port": "8001:8000",  # Different port for integration
                "command": "python src/api/main.py"
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

        cmd = f"""docker run -d --name {container_name} {gpu_flag} \\
            {volume_mounts} \\
            -w /workspace \\
            {port_flag} \\
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
    parser.add_argument("action", choices=[
        "run", "start", "stop", "status", "test", "query", "setup", "logs"
    ], help="Action to perform")

    parser.add_argument("--script", "-s", help="Script to run")
    parser.add_argument("--service", help="Service name")
    parser.add_argument("--query", "-q", help="SQL query to run")
    parser.add_argument("--test", "-t", help="Test path or pattern")
    parser.add_argument("--gpu", action="store_true", help="Enable GPU support")
    parser.add_argument("--port", "-p", help="Port mapping")
    parser.add_argument("--env", help="Environment variables (JSON format)")

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