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
import random
import numpy as np
import pandas as pd
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
            bash -c "pip install array-record tensorflow && python {script_path} --symbols $SYMBOLS --start-date $START_DATE --end-date $END_DATE --environment $ENVIRONMENT --output-dir $OUTPUT_DIR --gin-config $GIN_CONFIG --debug" """.format(script_path=script_path)
        
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
                "command": "uvicorn src.main:app --host 0.0.0.0 --port 8000"
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
    
    def get_run(self, run_id):
        """Get run information from runs table"""
        if not run_id:
            print("❌ Run ID is required")
            return False
        
        print(f"📊 Getting run information for ID: {run_id}")
        
        # Query runs table for the specific run_id
        runs_table = f"{self.table_prefix}runs"
        query = f"""
        SELECT 
            id,
            run_type,
            status,
            start_time,
            end_time,
            command_line,
            git_commit_hash,
            git_branch,
            environment,
            parameters,
            created_at,
            created_by,
            working_directory,
            python_version
        FROM {runs_table} 
        WHERE id = {run_id}
        """
        
        return self.query_db(query, f"Run details for ID {run_id}")
    
    def get_training_dataset(self, dataset_id):
        """Get training dataset information from training datasets table"""
        if not dataset_id:
            print("❌ Dataset ID is required")
            return False
        
        print(f"📊 Getting training dataset information for ID: {dataset_id}")
        
        # Query training datasets table for the specific dataset_id
        datasets_table = f"{self.table_prefix}training_datasets"
        query = f"""
        SELECT 
            id,
            dataset_name,
            symbols,
            date_range_start,
            date_range_end,
            sequence_length,
            feature_count,
            label_count,
            file_size_mb,
            data_quality_score,
            feature_completeness,
            label_completeness,
            created_at as creation_timestamp,
            status,
            run_id,
            features_file_path,
            labels_file_path,
            metadata_file_path,
            feature_metadata,
            technical_indicators,
            total_sequences
        FROM {datasets_table} 
        WHERE id = {dataset_id}
        """
        
        return self.query_db(query, f"Training dataset details for ID {dataset_id}")
    
    def sample_training_dataset(self, dataset_id, sample_size):
        """Sample N rows from a training dataset by ID"""
        if not dataset_id:
            print("❌ Dataset ID is required")
            return False
            
        if not sample_size or sample_size <= 0:
            print("❌ Sample size must be a positive integer")
            return False
            
        print(f"🎯 Sampling {sample_size} rows from training dataset ID: {dataset_id}")
        
        # First get dataset information to find file paths
        datasets_table = f"{self.table_prefix}training_datasets"
        query = f"""
        SELECT 
            id,
            dataset_name,
            total_sequences,
            feature_count,
            label_count,
            symbols,
            date_range_start,
            date_range_end,
            technical_indicators,
            file_metadata
        FROM {datasets_table} 
        WHERE id = {dataset_id}
        """
        
        # Execute query and capture result
        try:
            # Use the same database connection method as other commands
            if self.db_password:
                cmd = f'PGPASSWORD={self.db_password} psql -h {self.db_host} -p {self.db_port} -U {self.db_user} -d {self.db_name} -t -c "{query}"'
            else:
                cmd = f'psql -h {self.db_host} -p {self.db_port} -U {self.db_user} -d {self.db_name} -t -c "{query}"'
            
            result_process = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
            
            if result_process.returncode != 0:
                print(f"❌ Database query failed: {result_process.stderr}")
                return False
                
            result_lines = result_process.stdout.strip().split('\n')
            if not result_lines or not result_lines[0].strip():
                print(f"❌ No training dataset found with ID: {dataset_id}")
                return False
                
            # Parse the result row
            row_data = [item.strip() for item in result_lines[0].split('|')]
            if len(row_data) < 10:
                print(f"❌ Incomplete dataset information for ID: {dataset_id}")
                return False
                
            dataset_name = row_data[1]
            total_sequences = int(row_data[2]) if row_data[2].isdigit() else 0
            feature_count = int(row_data[3]) if row_data[3].isdigit() else 0
            label_count = int(row_data[4]) if row_data[4].isdigit() else 0
            symbols = row_data[5]
            date_range_start = row_data[6]
            date_range_end = row_data[7]
            technical_indicators = row_data[8]
            file_metadata = row_data[9] if len(row_data) > 9 else '{}'
            
            print(f"📋 Dataset: {dataset_name}")
            print(f"🔢 Total sequences: {total_sequences}")
            print(f"📊 Features: {feature_count}, Labels: {label_count}")
            print(f"🎯 Symbols: {symbols}")
            print(f"📅 Date range: {date_range_start} to {date_range_end}")
            print(f"🔧 Technical indicators: {technical_indicators}")
            
            if sample_size > total_sequences:
                print(f"⚠️  Requested sample size ({sample_size}) exceeds total sequences ({total_sequences})")
                print(f"🔧 Adjusting sample size to {total_sequences}")
                sample_size = total_sequences
            
            # Try to find and sample actual data files
            # Use default data format for newer training datasets
            data_format = "arrayrecord"  # Modern datasets use arrayrecord format
            return self._sample_dataset_files(dataset_name, file_metadata, sample_size, data_format, 
                                           "", "")
                                           
        except subprocess.TimeoutExpired:
            print("❌ Database query timed out")
            return False
        except Exception as e:
            print(f"❌ Error sampling dataset: {e}")
            return False
    
    def _sample_dataset_files(self, dataset_name, run_id, sample_size, data_format, 
                            features_file_path, labels_file_path):
        """Sample data from actual dataset files"""
        
        # Define potential file locations
        training_data_paths = [
            f"/mnt/d/ats-data/training/{run_id}",
            f"/mnt/d/ats-data/training_data",
            f"/data/training/{run_id}",
            f"./training_data_output"
        ]
        
        print(f"🔍 Searching for training data files...")
        
        # Look for files in potential locations
        for base_path in training_data_paths:
            if os.path.exists(base_path):
                print(f"📁 Found training data directory: {base_path}")
                
                # Look for files matching dataset pattern
                for file_path in Path(base_path).rglob("*"):
                    if dataset_name.replace(" ", "_") in str(file_path) or (run_id and run_id in str(file_path)):
                        print(f"📄 Found potential dataset file: {file_path}")
                        
                        try:
                            return self._sample_file(file_path, sample_size, data_format)
                        except Exception as e:
                            print(f"⚠️  Could not sample {file_path}: {e}")
                            continue
                            
                # Look for numpy files, JSON files, ArrayRecord files, or other common formats
                for file_ext in ["*.npy", "*.json", "*.csv", "*.parquet", "*.arrayrecord"]:
                    for file_path in Path(base_path).rglob(file_ext):
                        if any(keyword in str(file_path).lower() for keyword in [
                            'features', 'labels', 'training', 'dataset', dataset_name.lower()
                        ]):
                            print(f"📄 Found potential data file: {file_path}")
                            try:
                                return self._sample_file(file_path, sample_size, data_format)
                            except Exception as e:
                                print(f"⚠️  Could not sample {file_path}: {e}")
                                continue
        
        print("⚠️  No accessible training data files found")
        print("💡 Files may be stored in different location or format")
        return True
        
    def _sample_file(self, file_path, sample_size, data_format):
        """Sample data from a specific file"""
        file_path = Path(file_path)
        file_ext = file_path.suffix.lower()
        
        print(f"📖 Attempting to sample {sample_size} rows from: {file_path}")
        
        try:
            if file_ext == '.npy':
                # NumPy array
                data = np.load(file_path, allow_pickle=True)
                if len(data) == 0:
                    print("❌ Empty numpy array")
                    return False
                    
                total_rows = len(data)
                actual_sample_size = min(sample_size, total_rows)
                
                # Random sample
                indices = np.random.choice(total_rows, size=actual_sample_size, replace=False)
                sampled_data = data[indices]
                
                print(f"✅ Sampled {actual_sample_size} rows from {total_rows} total")
                print(f"📊 Sample shape: {sampled_data.shape}")
                print(f"🔢 Data type: {sampled_data.dtype}")
                
                # Show first few elements
                if sampled_data.ndim > 1:
                    print(f"📋 First row preview: {sampled_data[0]}")
                else:
                    print(f"📋 First values preview: {sampled_data[:min(10, len(sampled_data))]}")
                    
                return True
                
            elif file_ext == '.json':
                # JSON file
                import json
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                if isinstance(data, list):
                    total_rows = len(data)
                    actual_sample_size = min(sample_size, total_rows)
                    sampled_data = np.random.choice(data, size=actual_sample_size, replace=False)
                    
                    print(f"✅ Sampled {actual_sample_size} items from {total_rows} total")
                    print(f"📋 Sample preview: {sampled_data[:3] if len(sampled_data) > 3 else sampled_data}")
                elif isinstance(data, dict):
                    print(f"📋 JSON metadata: {list(data.keys())}")
                    for key, value in list(data.items())[:5]:
                        print(f"   {key}: {value}")
                        
                return True
                
            elif file_ext == '.csv':
                # CSV file  
                df = pd.read_csv(file_path)
                total_rows = len(df)
                actual_sample_size = min(sample_size, total_rows)
                
                sampled_df = df.sample(n=actual_sample_size)
                print(f"✅ Sampled {actual_sample_size} rows from {total_rows} total")
                print(f"📊 Columns: {list(df.columns)}")
                print(f"📋 Sample preview:")
                print(sampled_df.head())
                
                return True
                
            elif file_ext == '.parquet':
                # Parquet file
                df = pd.read_parquet(file_path)
                total_rows = len(df)
                actual_sample_size = min(sample_size, total_rows)
                
                sampled_df = df.sample(n=actual_sample_size)
                print(f"✅ Sampled {actual_sample_size} rows from {total_rows} total")
                print(f"📊 Columns: {list(df.columns)}")
                print(f"📋 Sample preview:")
                print(sampled_df.head())
                
                return True
                
            elif file_ext == '.arrayrecord':
                # ArrayRecord file
                try:
                    from array_record.python.array_record_module import ArrayRecordReader
                    
                    reader = ArrayRecordReader(str(file_path))
                    total_records = reader.num_records()
                    
                    if total_records == 0:
                        print("❌ Empty ArrayRecord file")
                        return False
                    
                    actual_sample_size = min(sample_size, total_records)
                    
                    print(f"✅ Sampling {actual_sample_size} records from {total_records} total")
                    
                    # Sample random records
                    import random
                    record_indices = sorted(random.sample(range(total_records), actual_sample_size))
                    
                    print(f"📋 ArrayRecord sample preview:")
                    for i, record_idx in enumerate(record_indices[:3]):  # Show first 3 samples
                        reader.seek(record_idx)
                        record = reader.read()
                        
                        print(f"   Record {record_idx}: {type(record)}")
                        if isinstance(record, np.ndarray):
                            print(f"      Shape: {record.shape}, Dtype: {record.dtype}")
                            non_zero = np.count_nonzero(record)
                            print(f"      Non-zero elements: {non_zero}/{len(record)}")
                            if len(record) > 0:
                                print(f"      Sample values: {record[:10]}")
                        else:
                            print(f"      Content: {str(record)[:100]}")
                    
                    reader.close()
                    return True
                    
                except ImportError:
                    print("❌ ArrayRecord module not available. Install with: pip install array_record")
                    return False
                except Exception as e:
                    print(f"❌ Error reading ArrayRecord file: {e}")
                    return False
                
            else:
                print(f"❌ Unsupported file format: {file_ext}")
                return False
                
        except Exception as e:
            print(f"❌ Error reading file {file_path}: {e}")
            return False
    
    def read_arrayrecord(self, file_path, sample_size=5):
        """Read and sample an ArrayRecord file directly"""
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            return False
        
        print(f"📖 Reading ArrayRecord file: {file_path}")
        
        try:
            from array_record.python.array_record_module import ArrayRecordReader
            
            reader = ArrayRecordReader(str(file_path))
            total_records = reader.num_records()
            
            if total_records == 0:
                print("❌ Empty ArrayRecord file")
                return False
            
            actual_sample_size = min(sample_size, total_records)
            
            print(f"📊 Total records: {total_records}")
            print(f"✅ Sampling {actual_sample_size} records:")
            
            # Sample random records
            record_indices = sorted(random.sample(range(total_records), actual_sample_size)) if total_records > actual_sample_size else list(range(actual_sample_size))
            
            print(f"\n📋 ArrayRecord contents:")
            for i, record_idx in enumerate(record_indices):
                reader.seek(record_idx)
                record = reader.read()
                
                print(f"\n🔍 Record {record_idx}:")
                if isinstance(record, np.ndarray):
                    print(f"   📐 Shape: {record.shape}")
                    print(f"   🔢 Dtype: {record.dtype}")
                    non_zero = np.count_nonzero(record)
                    print(f"   📈 Non-zero elements: {non_zero:,}/{len(record):,} ({non_zero/len(record)*100:.1f}%)")
                    
                    if len(record) > 0:
                        print(f"   📋 First 10 values: {record[:10]}")
                        if non_zero > 0:
                            # Show some non-zero values
                            non_zero_indices = np.nonzero(record)[0][:10]
                            non_zero_values = record[non_zero_indices]
                            print(f"   📊 Sample non-zero values: {non_zero_values}")
                            
                            # Statistics for non-zero values
                            if len(non_zero_values) > 1:
                                print(f"   📈 Non-zero stats: min={non_zero_values.min():.4f}, max={non_zero_values.max():.4f}, mean={non_zero_values.mean():.4f}")
                    
                elif isinstance(record, (list, tuple)):
                    print(f"   📏 Length: {len(record)}")
                    print(f"   📋 Content sample: {record[:10]}")
                elif isinstance(record, dict):
                    print(f"   🗂️  Dictionary keys: {list(record.keys())[:10]}")
                    for key, value in list(record.items())[:3]:
                        print(f"      {key}: {value}")
                else:
                    print(f"   📄 Type: {type(record)}")
                    print(f"   📋 Content: {str(record)[:200]}")
            
            reader.close()
            print(f"\n✅ Successfully read ArrayRecord file with {total_records:,} records")
            return True
            
        except ImportError:
            print("❌ ArrayRecord module not available. Install with:")
            print("   pip install array_record")
            return False
        except Exception as e:
            print(f"❌ Error reading ArrayRecord file: {e}")
            import traceback
            traceback.print_exc()
            return False
    
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
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Add existing actions as subcommands to avoid conflicts
    # Main arguments that apply to all commands
    parser.add_argument("--environment", choices=["dev", "intg"], help="Environment to use (auto-detected if not specified)")
    
    # Legacy support - add old actions as subcommands
    for action in ["run", "start", "stop", "status", "test", "query", "setup", "logs", "get", "arrayrecord"]:
        action_parser = subparsers.add_parser(action, help=f"{action.capitalize()} action")
        if action == "run":
            action_parser.add_argument("--script", "-s", required=True, help="Script to run")
            action_parser.add_argument("--gpu", action="store_true", help="Enable GPU support")
            action_parser.add_argument("--env", help="Environment variables (JSON format)")
        elif action == "start":
            action_parser.add_argument("--service", required=True, help="Service name")
            action_parser.add_argument("--port", "-p", help="Port mapping")
            action_parser.add_argument("--gpu", action="store_true", help="Enable GPU support")
            action_parser.add_argument("--env", help="Environment variables (JSON format)")
        elif action == "stop":
            action_parser.add_argument("--service", required=True, help="Service name")
        elif action == "query":
            action_parser.add_argument("--query", "-q", required=True, help="SQL query to run")
        elif action == "arrayrecord":
            action_parser.add_argument("--file", "-f", required=True, help="ArrayRecord file path to read")
            action_parser.add_argument("--sample-size", "-n", type=int, default=5, help="Number of records to sample (default: 5)")
        elif action == "test":
            action_parser.add_argument("--test", "-t", help="Test path or pattern")
        elif action == "logs":
            action_parser.add_argument("--service", required=True, help="Service name")
        elif action == "get":
            action_parser.add_argument("--run-id", required=True, help="Run ID")
    
    # Training dataset subcommand
    training_parser = subparsers.add_parser("training_dataset", help="Training dataset operations")
    training_subparsers = training_parser.add_subparsers(dest="training_action", help="Training dataset actions")
    
    # training_dataset get subcommand
    get_parser = training_subparsers.add_parser("get", help="Get training dataset details")
    get_parser.add_argument("dataset_id", help="Training dataset ID")
    
    # training_dataset sample subcommand
    sample_parser = training_subparsers.add_parser("sample", help="Sample N rows from training dataset")
    sample_parser.add_argument("dataset_id", help="Training dataset ID")
    sample_parser.add_argument("sample_size", type=int, help="Number of rows to sample")
    
    args = parser.parse_args()
    
    cli = DevCLI(environment=args.environment)
    
    # Parse environment variables if provided
    environment = None
    if hasattr(args, 'env') and args.env:
        try:
            environment = json.loads(args.env)
        except json.JSONDecodeError:
            print("❌ Invalid JSON format for --env")
            sys.exit(1)
    
    # Handle commands based on subcommand structure
    if args.command == "training_dataset":
        if args.training_action == "get":
            cli.get_training_dataset(args.dataset_id)
        elif args.training_action == "sample":
            cli.sample_training_dataset(args.dataset_id, args.sample_size)
        else:
            print("❌ Unknown training_dataset action")
            sys.exit(1)
    
    elif args.command == "run":
        gpu = getattr(args, 'gpu', False)
        cli.run_docker_job(args.script, gpu=gpu, environment=environment)
        
    elif args.command == "start":
        port = getattr(args, 'port', None)
        gpu = getattr(args, 'gpu', False)
        cli.start_service(args.service, port, gpu, environment)
        
    elif args.command == "stop":
        cli.stop_service(args.service)
        
    elif args.command == "status":
        cli.list_services()
        
    elif args.command == "test":
        cli.run_test(args.test)
        
    elif args.command == "query":
        cli.query_db(args.query)
        
    elif args.command == "arrayrecord":
        cli.read_arrayrecord(args.file, args.sample_size)
        
    elif args.command == "setup":
        cli.setup_dev_env()
        
    elif args.command == "logs":
        container_name = f"ats-dev-{args.service}"
        cmd = f"docker logs -f {container_name}"
        subprocess.run(cmd, shell=True)
        
    elif args.command == "get":
        cli.get_run(args.run_id)
        
    else:
        print("❌ No command specified. Use --help for available options.")
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()