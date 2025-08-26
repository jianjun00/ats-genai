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
    def __init__(self):
        self.db_host = "localhost"
        self.db_port = "5432"  # Default PostgreSQL port
        self.db_user = "postgres"
        self.db_password = "dev_password"
        self.db_name = "dev_db"
        
        # Check if we need to use port-forwarded connection
        self.check_database_connection()
        
    def check_database_connection(self):
        """Check which database connection works"""
        # Try localhost:5432 first (direct PostgreSQL)
        if self.test_db_connection("localhost", "5432"):
            self.db_host = "localhost"
            self.db_port = "5432"
            return
            
        # Try localhost:5433 (port-forwarded from k8s)
        if self.test_db_connection("localhost", "5433"):
            self.db_host = "localhost"
            self.db_port = "5433"
            return
            
        print("⚠️  No database connection available. You may need to:")
        print("   1. Start local PostgreSQL")
        print("   2. Start port-forwarding: kubectl port-forward svc/postgres -n ats-dev 5433:5432")
        print("   3. Or run Docker PostgreSQL container")
        
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
            
        print(f"🐳 Running Docker job: {script_path}")
        
        # Build Docker command
        gpu_flag = "--gpus all" if gpu else ""
        env_vars = ""
        if environment:
            env_vars = " ".join([f"-e {k}={v}" for k, v in environment.items()])
        
        # Use our official image
        image = "dragonflyer762/ats-genai:latest"
        
        # Mount current directory and set database connection
        cmd = f"""docker run --rm {gpu_flag} \
            -v {os.getcwd()}:/workspace \
            -w /workspace \
            -e DB_HOST={self.db_host} \
            -e DB_PORT={self.db_port} \
            -e DB_USER={self.db_user} \
            -e DB_PASSWORD={self.db_password} \
            -e DB_NAME={self.db_name} \
            -e PYTHONPATH=/workspace/src \
            {env_vars} \
            {image} \
            python {script_path}"""
        
        print(f"🚀 Running: docker run ... python {script_path}")
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
                "port": "5432:5432",
                "env": {
                    "POSTGRES_USER": self.db_user,
                    "POSTGRES_PASSWORD": self.db_password,
                    "POSTGRES_DB": self.db_name
                }
            },
            "analytics": {
                "image": "dragonflyer762/ats-genai:latest",
                "port": "3001:3001",
                "command": "python src/analytics/server.py"
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
        
        cmd = f"""docker run -d --name {container_name} {gpu_flag} \
            -v {os.getcwd()}:/workspace \
            -w /workspace \
            {port_flag} \
            -e PYTHONPATH=/workspace/src \
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
            return True
        else:
            print(f"❌ Failed to start service: {result.stderr}")
            return False
    
    def stop_service(self, service_name):
        """Stop a Docker service"""
        container_name = f"ats-dev-{service_name}"
        
        print(f"🛑 Stopping service: {service_name}")
        cmd = f"docker stop {container_name} && docker rm {container_name}"
        result = subprocess.run(cmd, shell=True, capture_output=True)
        
        if result.returncode == 0:
            print(f"✅ Service {service_name} stopped")
            return True
        else:
            print(f"❌ Failed to stop service {service_name}")
            return False
    
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
        
        cmd = f"""docker run --rm \
            -v {os.getcwd()}:/workspace \
            -w /workspace \
            -e PYTHONPATH=/workspace/src \
            dragonflyer762/ats-genai:latest \
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
    
    def setup_dev_env(self):
        """Setup complete development environment"""
        print("🏗️  Setting up development environment...")
        
        # Start PostgreSQL
        if not self.start_service("postgres"):
            return False
        
        # Wait for database to be ready
        print("⏳ Waiting for database to be ready...")
        for i in range(30):
            if self.test_db_connection("localhost", "5432"):
                break
            time.sleep(1)
        else:
            print("❌ Database failed to start")
            return False
        
        print("✅ Development environment ready!")
        print("🔗 Database: postgresql://postgres:dev_password@localhost:5432/dev_db")
        return True

def main():
    parser = argparse.ArgumentParser(description="Dev CLI for localhost/Docker development operations")
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
    
    cli = DevCLI()
    
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