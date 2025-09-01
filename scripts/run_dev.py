#!/usr/bin/env python3
"""
Dev CLI for running development operations with Docker and localhost services

Automatically handles Docker operations, database connections, and service management
without requiring Kubernetes knowledge.
"""

import subprocess
import sys
import time
import argparse
import os
import json
import yaml
from pathlib import Path

class DevCLI:
    def __init__(self, environment=None):
        # Detect environment based on containers or explicit parameter
        self.environment = environment or self.detect_environment()
        
        # Set database configuration based on environment
        self.configure_database()
        
        # ATS persistent volume paths (D: drive)
        self.ats_data_path = "/mnt/d/ats-data"
        self.ats_backup_path = "/mnt/d/ats-backup"
        self.ats_logs_path = "/mnt/d/ats-logs"
        
        # Ensure ATS directories exist
        self.ensure_ats_directories()
        
        # Check if we need to use port-forwarded connection
        self.check_database_connection()
    
    def detect_environment(self):
        """Auto-detect environment based on running containers"""
        try:
            result = subprocess.run("docker ps --format '{{.Names}}'", shell=True, capture_output=True, text=True)
            containers = result.stdout.strip().split('\n')
            
            # run_dev.py should prefer dev environment even if both are running
            if 'ats-dev-postgres' in containers:
                return 'dev'
            elif 'ats-intg-postgres' in containers:
                return 'intg'
            else:
                print("⚠️  No ATS database containers found, defaulting to dev")
                return 'dev'
        except:
            print("⚠️  Could not detect environment, defaulting to dev")
            return 'dev'
    
    def configure_database(self):
        """Configure database settings based on environment"""
        if self.environment == 'intg':
            self.db_host = "localhost"
            self.db_port = "4432"  # ats-intg-postgres port
            self.db_user = "postgres"
            self.db_password = "intg_password"  # TimescaleDB might use password
            self.db_name = "intg_db"
            self.table_prefix = "intg_"
        else:  # dev environment
            self.db_host = "localhost"
            self.db_port = "3432"  # ats-dev-postgres port
            self.db_user = "postgres"
            self.db_password = "dev_password"  # ATS-DEV PostgreSQL password
            self.db_name = "dev_db"
            self.table_prefix = "dev_"
            
        print(f"🔧 Configured for {self.environment} environment: {self.db_host}:{self.db_port}/{self.db_name}")
    
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
        # Try localhost:3432 first (ATS-DEV PostgreSQL)
        if self.test_db_connection("localhost", "3432"):
            self.db_host = "localhost"
            self.db_port = "3432"
            return
            
        # Try localhost:4432 (ATS-INTG)
        if self.test_db_connection("localhost", "4432"):
            self.db_host = "localhost"
            self.db_port = "4432"
            return
            
        print("⚠️  No database connection available. You may need to:")
        print("   1. Start Docker PostgreSQL: python scripts/run_dev.py start --service postgres")
        print("   2. Start local PostgreSQL")
        print("   3. Or use existing database connection")
        
    def test_db_connection(self, host, port):
        """Test database connection"""
        try:
            # Try with the configured password first
            if self.db_password:
                cmd = f'PGPASSWORD={self.db_password} psql -h {host} -p {port} -U {self.db_user} -d {self.db_name} -c "SELECT 1" > /dev/null 2>&1'
                result = subprocess.run(cmd, shell=True, capture_output=True)
                if result.returncode == 0:
                    return True
            
            # Try without password (for Docker containers)
            cmd = f'psql -h {host} -p {port} -U {self.db_user} -d {self.db_name} -c "SELECT 1" > /dev/null 2>&1'
            result = subprocess.run(cmd, shell=True, capture_output=True)
            if result.returncode == 0:
                self.db_password = ""
                return True
                
            # Try common passwords based on environment
            passwords_to_try = []
            if self.environment == 'intg':
                passwords_to_try = ['intg_password', 'password', 'postgres']
            else:
                passwords_to_try = ['dev_password', 'password', 'postgres']
                
            for password in passwords_to_try:
                cmd = f'PGPASSWORD={password} psql -h {host} -p {port} -U {self.db_user} -d {self.db_name} -c "SELECT 1" > /dev/null 2>&1'
                result = subprocess.run(cmd, shell=True, capture_output=True)
                if result.returncode == 0:
                    self.db_password = password
                    return True
                
            return False
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
        # Handle command with arguments (e.g., "python script.py arg1 arg2")
        if script_path.startswith('python '):
            # Extract the actual script path for existence check
            parts = script_path.split()
            actual_script = parts[1] if len(parts) > 1 else script_path
            if not os.path.exists(actual_script):
                print(f"❌ Script not found: {actual_script}")
                return False
            command_to_run = script_path
        else:
            # Single script path
            if not os.path.exists(script_path):
                print(f"❌ Script not found: {script_path}")
                return False
            command_to_run = f"python {script_path}"
            
        print(f"🐳 Running Docker job: {script_path}")
        
        # Build Docker command
        gpu_flag = "--gpus all" if gpu else ""
        env_vars = ""
        if environment:
            env_vars = " ".join([f"-e {k}={v}" for k, v in environment.items()])
        
        # Use our official image
        image = "dragonflyer762/ats-genai:latest"
        
        # Mount directories and set database connection
        volume_mounts = self.get_volume_mounts()
        
        # Add network connection to PostgreSQL if it exists
        network_link = ""
        db_port_for_container = self.db_port  # Default to host port
        if self.environment == 'intg':
            postgres_check = subprocess.run("docker ps -q -f name=ats-intg-postgres", shell=True, capture_output=True)
            if postgres_check.stdout.strip():
                network_link = "--network ats-intg-network"
                db_host_for_container = "ats-intg-postgres"
                db_port_for_container = "5432"  # Use internal PostgreSQL port in Docker network
            else:
                db_host_for_container = self.db_host
                db_port_for_container = self.db_port
        else:
            postgres_check = subprocess.run("docker ps -q -f name=ats-dev-postgres", shell=True, capture_output=True)
            if postgres_check.stdout.strip():
                network_link = "--network ats-network"
                db_host_for_container = "ats-dev-postgres"
                db_port_for_container = "5432"  # Use internal PostgreSQL port in Docker network
            else:
                db_host_for_container = self.db_host
                db_port_for_container = self.db_port

        cmd = f"""docker run --rm {gpu_flag} \
            {volume_mounts} \
            {network_link} \
            -w /workspace \
            -e DB_HOST={db_host_for_container} \
            -e DB_PORT={db_port_for_container} \
            -e DB_USER={self.db_user} \
            -e DB_PASSWORD={self.db_password} \
            -e DB_NAME={self.db_name} \
            -e ENVIRONMENT={self.environment} \
            -e PYTHONPATH=/workspace/src \
            -e ATS_DATA_PATH=/data \
            -e ATS_BACKUP_PATH=/backup \
            -e ATS_LOGS_PATH=/logs \
            -e IDEMPOTENT_MODE={os.getenv('IDEMPOTENT_MODE', 'false')} \
            -e FORCE_REBUILD={os.getenv('FORCE_REBUILD', 'false')} \
            -e EXCHANGE_FILTER={os.getenv('EXCHANGE_FILTER', 'all')} \
            -e POLYGON_API_KEY={os.getenv('POLYGON_API_KEY', 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD')} \
            -e TIINGO_API_KEY={os.getenv('TIINGO_API_KEY', '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5')} \
            -e EODHD_API_KEY={os.getenv('EODHD_API_KEY', '675b5a33b36f43.67825763')} \
            -e FMP_API_KEY={os.getenv('FMP_API_KEY', 'Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr')} \
            -e ALPHA_VANTAGE_API_KEY={os.getenv('ALPHA_VANTAGE_API_KEY', '9GI0NZ3V4VNFX271')} \
            -e FIRSTRATE_USER_ID={os.getenv('FIRSTRATE_USER_ID', 'ats-genai-user')} \
            -e OPENAI_API_KEY={os.getenv('OPENAI_API_KEY', '')} \
            {env_vars} \
            {image} \
            {command_to_run}"""
        
        print(f"🚀 Running: docker run ... {command_to_run}")
        result = subprocess.run(cmd, shell=True)
        
        if result.returncode == 0:
            print("✅ Job completed successfully")
            return True
        else:
            print(f"❌ Job failed with exit code: {result.returncode}")
            return False
    
    def start_service(self, service_name, port=None, gpu=False, environment=None):
        """Start a service using Docker"""
        print(f"🚀 Starting service: {service_name}")
        
        # Common service configurations
        services = {
            "postgres": {
                "image": "postgres:13",
                "port": "3432:5432",  # ATS-DEV PostgreSQL port 
                "env": {
                    "POSTGRES_USER": self.db_user,
                    "POSTGRES_PASSWORD": self.db_password,
                    "POSTGRES_DB": self.db_name
                },
                "volumes": [
                    "postgres-data-new:/var/lib/postgresql/data",
                    f"{self.ats_backup_path}:/backup"
                ],
                "backup_only": True  # Enable backup only (no automatic restore)
            },
            "postgres-intg": {
                "image": "postgres:13",
                "port": "4432:5432",
                "env": {
                    "POSTGRES_USER": self.db_user,
                    "POSTGRES_PASSWORD": self.db_password,
                    "POSTGRES_DB": "intg_db"  # Integration database
                },
                "volumes": [
                    "postgres-intg-data:/var/lib/postgresql/data",
                    f"{self.ats_backup_path}/intg:/backup"
                ],
                "backup_only": True,
                "environment": "intg"
            },
            "analytics": {
                "image": "dragonflyer762/ats-genai:latest",
                "port": "3000:3000",
                "command": "python src/services/analytics_service.py",
                "env": {
                    "DB_HOST": "ats-dev-postgres",
                    "DB_PORT": "5432",
                    "DB_USER": self.db_user,
                    "DB_PASSWORD": self.db_password,
                    "DB_NAME": self.db_name,
                    "ENVIRONMENT": "dev",
                    "POLYGON_API_KEY": os.getenv('POLYGON_API_KEY', 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD'),
                    "TIINGO_API_KEY": os.getenv('TIINGO_API_KEY', '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5'),
                    "EODHD_API_KEY": os.getenv('EODHD_API_KEY', '675b5a33b36f43.67825763'),
                    "FMP_API_KEY": os.getenv('FMP_API_KEY', 'Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr'),
                    "ALPHA_VANTAGE_API_KEY": os.getenv('ALPHA_VANTAGE_API_KEY', '9GI0NZ3V4VNFX271'),
                    "FIRSTRATE_USER_ID": os.getenv('FIRSTRATE_USER_ID', 'ats-genai-user'),
                    "OPENAI_API_KEY": os.getenv('OPENAI_API_KEY', '')
                }
            },
            "api": {
                "image": "dragonflyer762/ats-genai:latest", 
                "port": "8000:8000",
                "command": "python src/api/main.py"
            }
        }
        
        if service_name not in services:
            print(f"❌ Unknown service: {service_name}")
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
        
        # Container name
        container_name = f"ats-dev-{service_name}"
        
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
        
        # Special handling for PostgreSQL to fix D: drive permissions
        additional_args = ""
        if service_name == "postgres":
            # Initialize database directory with proper permissions
            db_dir = f"{self.ats_data_path}/db"
            if not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
                print(f"📁 Created database directory: {db_dir}")
            # Run as root initially to fix permissions, then PostgreSQL will handle user switching
            additional_args = "--user root"
        
        cmd = f"""docker run -d --name {container_name} {gpu_flag} \
            --network ats-network \
            {volume_mounts} \
            -w /workspace \
            {port_flag} \
            {additional_args} \
            -e PYTHONPATH=/workspace/src \
            -e ATS_DATA_PATH=/data \
            -e ATS_BACKUP_PATH=/backup \
            -e ATS_LOGS_PATH=/logs \
            -e POSTGRES_INITDB_ARGS="--auth-host=md5 --auth-local=trust" \
            {env_vars} \
            {config['image']}"""
        
        if 'command' in config:
            cmd += f" {config['command']}"
        
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Service {service_name} started successfully")
            print(f"🌐 Container name: {container_name}")
            if 'port' in config:
                print(f"🔗 Access at: http://localhost:{config['port'].split(':')[0]}")
            
            # Note: Automatic backup/restore removed - use manual initialization only
            
            return True
        else:
            print(f"❌ Failed to start service: {result.stderr}")
            return False
    
    def _removed_backup_restore_function(self):
        """
        REMOVED: Automatic backup/restore functionality
        Reason: Can cause data loss and unpredictable behavior
        Use manual database initialization instead
        """
        pass
    
    def stop_service(self, service_name):
        """Stop a Docker service"""
        container_name = f"ats-dev-{service_name}"
        
        print(f"🛑 Stopping service: {service_name}")
        
        # Backup PostgreSQL before stopping
        if "postgres" in service_name:
            # Get service config to determine database name and backup directory
            services = {
                "postgres": {"db": self.db_name, "backup_dir": self.ats_backup_path},
                "postgres-intg": {"db": "intg_db", "backup_dir": f"{self.ats_backup_path}/intg"}
            }
            if service_name in services:
                service_config = services[service_name]
                self._backup_postgres_to_d_drive(container_name, service_config["db"], service_config["backup_dir"])
        
        cmd = f"docker stop {container_name} && docker rm {container_name}"
        result = subprocess.run(cmd, shell=True, capture_output=True)
        
        if result.returncode == 0:
            print(f"✅ Service {service_name} stopped")
            return True
        else:
            print(f"❌ Failed to stop service {service_name}")
            return False
    
    def _backup_postgres_to_d_drive(self, container_name, db_name, backup_dir):
        """Backup PostgreSQL to D: drive before stopping"""
        try:
            print("💾 Backing up database to D: drive...")
            
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
                print(f"✅ Database backed up to: {backup_file}")
                print(f"✅ Latest backup: {latest_backup}")
            else:
                print(f"⚠️  Backup failed: {result.stderr.decode()}")
                
        except Exception as e:
            print(f"⚠️  Backup error: {e}")
    
    def list_services(self):
        """List running Docker services"""
        print("🐳 Running ATS services:")
        cmd = "docker ps --filter name=ats-dev- --format 'table {{.Names}}\\t{{.Status}}\\t{{.Ports}}'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.stdout.strip():
            print(result.stdout)
        else:
            print("No ATS services currently running")
    
    def run_test(self, test_path=None, pattern=None):
        """Run tests using Docker"""
        print("🧪 Running tests...")
        
        test_cmd = "pytest"
        if test_path:
            test_cmd += f" {test_path}"
        if pattern:
            test_cmd += f" -k {pattern}"
        
        test_cmd += " -v"
        
        volume_mounts = self.get_volume_mounts()
        cmd = f"""docker run --rm \
            {volume_mounts} \
            -w /workspace \
            -e PYTHONPATH=/workspace/src \
            -e ATS_DATA_PATH=/data \
            -e ATS_BACKUP_PATH=/backup \
            -e ATS_LOGS_PATH=/logs \
            dragonflyer762/ats-genai:latest \
            {test_cmd}"""
        
        result = subprocess.run(cmd, shell=True)
        return result.returncode == 0
    
    def query_db(self, sql_query, description=None):
        """Run database query directly"""
        if description:
            print(f"📊 {description}")
        
        # Use password if we have one, otherwise connect without password
        if self.db_password:
            cmd = f'PGPASSWORD={self.db_password} psql -h {self.db_host} -p {self.db_port} -U {self.db_user} -d {self.db_name} -c "{sql_query}"'
        else:
            cmd = f'psql -h {self.db_host} -p {self.db_port} -U {self.db_user} -d {self.db_name} -c "{sql_query}"'
        
        result = self.run_command(cmd)
        
        if result:
            print(result)
        return result
    
    def setup_dev_env(self):
        """Setup complete development environment"""
        print("🏗️  Setting up development environment...")
        
        # Start PostgreSQL
        if not self.start_service("postgres"):
            return False
        
        # Wait for database to be ready
        print("⏳ Waiting for database to be ready...")
        for i in range(30):
            if self.test_db_connection("localhost", "3432"):
                break
            time.sleep(1)
        else:
            print("❌ Database failed to start")
            return False
        
        print("✅ Development environment ready!")
        print("🔗 Database: postgresql://postgres:dev_password@localhost:3432/dev_db")
        return True

def main():
    parser = argparse.ArgumentParser(description="Dev CLI for localhost/Docker development operations")
    parser.add_argument("action", choices=[
        "run", "start", "stop", "status", "test", "query", "setup", "logs"
    ], help="Action to perform")
    
    parser.add_argument("--environment", choices=["dev", "intg"], help="Environment to use (auto-detected if not specified)")
    
    parser.add_argument("--script", "-s", help="Script to run")
    parser.add_argument("--service", help="Service name")
    parser.add_argument("--query", "-q", help="SQL query to run")
    parser.add_argument("--test", "-t", help="Test path or pattern")
    parser.add_argument("--gpu", action="store_true", help="Enable GPU support")
    parser.add_argument("--port", "-p", help="Port mapping")
    parser.add_argument("--env", help="Environment variables (JSON format)")
    
    args = parser.parse_args()
    
    cli = DevCLI(environment=args.environment)
    
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
        cli.setup_dev_env()
        
    elif args.action == "logs":
        if not args.service:
            print("❌ --service required for logs action")
            sys.exit(1)
        container_name = f"ats-dev-{args.service}"
        cmd = f"docker logs -f {container_name}"
        subprocess.run(cmd, shell=True)

if __name__ == "__main__":
    main()