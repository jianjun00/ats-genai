#!/usr/bin/env python3
"""
Dev K8s Runner

CLI tool to run all dev environment operations in Kubernetes.
This ensures we never accidentally run dev operations locally.

Usage:
    python scripts/dev_k8s_runner.py migration --name price-unification
    python scripts/dev_k8s_runner.py job --script path/to/script.py --args "--symbols AAPL,MSFT"
    python scripts/dev_k8s_runner.py pipeline --type daily-price-unification --date 2024-01-15
    python scripts/dev_k8s_runner.py query --sql "SELECT COUNT(*) FROM dev_daily_prices"
"""

import argparse
import subprocess
import sys
import os
import yaml
from datetime import datetime
from pathlib import Path
import tempfile


class DevK8sRunner:
    """CLI for running all dev operations in Kubernetes"""
    
    def __init__(self):
        self.namespace = "ats-dev"
        self.db_host = "postgres-simple"
        self.db_port = "5432"
        self.db_user = "postgres"
        self.db_password = "dev_password"
        self.db_name = "dev_db"
        
    def get_db_url(self):
        """Get database URL for K8s environment"""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    def run_kubectl(self, cmd: list, capture_output=False):
        """Run kubectl command with proper namespace"""
        full_cmd = ["kubectl"] + cmd + ["-n", self.namespace]
        print(f"🚀 Running: {' '.join(full_cmd)}")
        
        if capture_output:
            result = subprocess.run(full_cmd, capture_output=True, text=True)
            return result.stdout.strip() if result.returncode == 0 else None
        else:
            return subprocess.run(full_cmd)
    
    def create_job_yaml(self, job_name: str, script_content: str, command: list, description: str = "") -> str:
        """Create a K8s job YAML with embedded script"""
        
        job_yaml = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": f"{job_name}-script",
                "namespace": self.namespace
            },
            "data": {
                "script.py": script_content,
                "run.sh": f"""#!/bin/bash
echo "📦 Installing dependencies..."
pip install --no-cache-dir -r /app/requirements.txt || echo "No requirements.txt found"

echo "🔧 Running {description}..."
cd /app
{' '.join(command)}

echo "✅ {description} completed!"
"""
            }
        }
        
        # Create the job
        job_spec = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": self.namespace
            },
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "name": "runner",
                            "image": "python:3.12-slim",
                            "command": ["/bin/bash", "/scripts/run.sh"],
                            "env": [
                                {"name": "DB_URL", "value": self.get_db_url()},
                                {"name": "ENVIRONMENT", "value": "dev"},
                                {"name": "DB_HOST", "value": self.db_host},
                                {"name": "DB_PORT", "value": self.db_port},
                                {"name": "DB_USER", "value": self.db_user},
                                {"name": "DB_PASSWORD", "value": self.db_password},
                                {"name": "DB_NAME", "value": self.db_name},
                                {"name": "PYTHONPATH", "value": "/app/src"}
                            ],
                            "volumeMounts": [
                                {"name": "script-volume", "mountPath": "/scripts"},
                                {"name": "app-code", "mountPath": "/app"}
                            ],
                            "workingDir": "/app"
                        }],
                        "volumes": [
                            {
                                "name": "script-volume",
                                "configMap": {"name": f"{job_name}-script"}
                            },
                            {
                                "name": "app-code",
                                "emptyDir": {}
                            }
                        ],
                        "restartPolicy": "Never",
                        "initContainers": [{
                            "name": "code-sync",
                            "image": "alpine/git",
                            "command": ["/bin/sh", "-c"],
                            "args": [f"""
                                cd /app
                                git clone https://github.com/your-org/ats-genai.git .
                                git checkout {self.get_git_branch()}
                                echo "✅ Code synced to K8s"
                            """],
                            "volumeMounts": [{"name": "app-code", "mountPath": "/app"}]
                        }]
                    }
                },
                "backoffLimit": 1
            }
        }
        
        # Create a combined YAML
        combined_yaml = yaml.dump(job_yaml, default_flow_style=False) + "\n---\n" + yaml.dump(job_spec, default_flow_style=False)
        
        return combined_yaml
    
    def get_git_branch(self):
        """Get current git branch"""
        try:
            result = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], 
                                 capture_output=True, text=True)
            return result.stdout.strip() if result.returncode == 0 else "main"
        except:
            return "main"
    
    def run_migration(self, migration_name: str):
        """Run a database migration in K8s"""
        print(f"🗄️  Running migration: {migration_name}")
        
        migration_script = f"""
import asyncio
import asyncpg
import logging

async def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    db_url = "{self.get_db_url()}"
    
    # Connect to database
    max_retries = 10
    for attempt in range(max_retries):
        try:
            conn = await asyncpg.connect(db_url)
            logger.info("✅ Connected to database")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            else:
                raise Exception(f"Could not connect after {{max_retries}} attempts: {{e}}")
    
    try:
        logger.info(f"🔧 Running migration: {migration_name}...")
        
        # Import and run migration manager
        import sys
        sys.path.append('/app/src')
        
        from db.migration_manager import DatabaseMigrationManager
        from config.environment import Environment
        
        env = Environment()
        migration_manager = DatabaseMigrationManager(env)
        
        # Run migration
        await migration_manager.migrate()
        
        logger.info("✅ Migration completed successfully")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
"""
        
        job_name = f"migration-{migration_name}-{int(datetime.now().timestamp())}"
        yaml_content = self.create_job_yaml(
            job_name, 
            migration_script, 
            ["python", "/scripts/script.py"],
            f"Database migration: {migration_name}"
        )
        
        self._apply_and_monitor_job(job_name, yaml_content)
    
    def run_pipeline(self, pipeline_type: str, **kwargs):
        """Run a data pipeline in K8s"""
        print(f"⚙️  Running pipeline: {pipeline_type}")
        
        if pipeline_type == "daily-price-unification":
            return self._run_price_unification_pipeline(**kwargs)
        else:
            print(f"❌ Unknown pipeline type: {pipeline_type}")
            return False
    
    def _run_price_unification_pipeline(self, date: str = None, symbols: str = None, limit: int = None):
        """Run the daily price unification pipeline"""
        
        pipeline_script = f"""
import asyncio
import sys
import logging
from datetime import datetime, date

# Add src to path
sys.path.append('/app/src')

from market_data.eod.unified_daily_price_validator import UnifiedDailyPriceValidator
from config.environment import Environment

async def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    env = Environment()
    validator = UnifiedDailyPriceValidator(env)
    
    try:
        await validator.connect()
        
        # Get processing parameters
        target_date = date.fromisoformat("{date}") if "{date}" else date.today()
        symbols_list = "{symbols}".split(",") if "{symbols}" else None
        limit_val = {limit} if {limit} else None
        
        logger.info(f"Running price unification for date: {{target_date}}")
        if symbols_list:
            logger.info(f"Processing symbols: {{symbols_list}}")
        if limit_val:
            logger.info(f"Limit: {{limit_val}} symbols")
        
        # If no symbols specified, get from universe
        if not symbols_list:
            # Get active universe symbols
            query = '''
                SELECT DISTINCT i.symbol 
                FROM dev_instruments i
                JOIN dev_universe_membership um ON i.symbol = um.symbol
                WHERE um.end_at IS NULL OR um.end_at >= $1
                ORDER BY i.symbol
            '''
            
            rows = await validator.conn.fetch(query, target_date)
            symbols_list = [row['symbol'] for row in rows]
            
            if limit_val:
                symbols_list = symbols_list[:limit_val]
        
        logger.info(f"Processing {{len(symbols_list)}} symbols")
        
        # Process each symbol
        successful = 0
        failed = 0
        
        for symbol in symbols_list:
            try:
                unified_price = await validator.validate_and_unify_price(symbol, target_date)
                if unified_price:
                    # Store unified price would go here
                    successful += 1
                    logger.info(f"✅ {{symbol}}: ${{unified_price.close:.2f}} ({{unified_price.validation_result.status.value}})")
                else:
                    failed += 1
                    logger.warning(f"❌ {{symbol}}: Failed to unify price")
                    
            except Exception as e:
                failed += 1
                logger.error(f"❌ {{symbol}}: Error - {{e}}")
        
        logger.info(f"🎉 Price unification completed: {{successful}} successful, {{failed}} failed")
        
    finally:
        await validator.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
"""
        
        job_name = f"price-unification-{int(datetime.now().timestamp())}"
        yaml_content = self.create_job_yaml(
            job_name,
            pipeline_script,
            ["python", "/scripts/script.py"],
            f"Price Unification Pipeline"
        )
        
        return self._apply_and_monitor_job(job_name, yaml_content)
    
    def run_query(self, sql: str):
        """Run a SQL query in K8s"""
        print(f"🔍 Running query in K8s dev database")
        
        query_script = f"""
import asyncio
import asyncpg
import logging

async def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    db_url = "{self.get_db_url()}"
    
    try:
        conn = await asyncpg.connect(db_url)
        logger.info("✅ Connected to database")
        
        sql = '''{{sql}}'''
        
        logger.info(f"🔍 Executing query...")
        rows = await conn.fetch(sql)
        
        logger.info(f"📊 Query results ({{len(rows)}} rows):")
        for i, row in enumerate(rows[:50]):  # Limit output
            logger.info(f"  {{dict(row)}}")
            if i >= 49 and len(rows) > 50:
                logger.info(f"  ... (showing first 50 of {{len(rows)}} rows)")
                break
        
        logger.info("✅ Query completed")
        
    except Exception as e:
        logger.error(f"❌ Query failed: {{e}}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
""".replace("{sql}", sql.replace("'", "\\'"))
        
        job_name = f"query-{int(datetime.now().timestamp())}"
        yaml_content = self.create_job_yaml(
            job_name,
            query_script, 
            ["python", "/scripts/script.py"],
            "Database Query"
        )
        
        return self._apply_and_monitor_job(job_name, yaml_content, wait_for_completion=True)
    
    def run_custom_job(self, script_path: str, args: str = ""):
        """Run a custom Python script in K8s"""
        print(f"🐍 Running custom script: {script_path}")
        
        # Read the script file
        try:
            with open(script_path, 'r') as f:
                script_content = f.read()
        except FileNotFoundError:
            print(f"❌ Script file not found: {script_path}")
            return False
        
        script_name = Path(script_path).stem
        job_name = f"custom-{script_name}-{int(datetime.now().timestamp())}"
        
        # Create command
        command = ["python", f"/app/{script_path}"]
        if args:
            command.extend(args.split())
        
        yaml_content = self.create_job_yaml(
            job_name,
            script_content,
            command,
            f"Custom script: {script_path}"
        )
        
        return self._apply_and_monitor_job(job_name, yaml_content)
    
    def _apply_and_monitor_job(self, job_name: str, yaml_content: str, wait_for_completion: bool = False):
        """Apply job YAML and optionally monitor completion"""
        
        # Write YAML to temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            yaml_file = f.name
        
        try:
            # Apply the job
            result = self.run_kubectl(["apply", "-f", yaml_file])
            if result.returncode != 0:
                print(f"❌ Failed to apply job")
                return False
            
            print(f"✅ Job {job_name} applied successfully")
            
            if wait_for_completion:
                # Monitor job completion
                print(f"⏳ Monitoring job completion...")
                self.run_kubectl(["wait", "--for=condition=complete", f"job/{job_name}", "--timeout=300s"])
            
            # Show logs
            print(f"📋 Job logs:")
            self.run_kubectl(["logs", f"job/{job_name}", "--follow"])
            
            return True
            
        finally:
            # Clean up temp file
            os.unlink(yaml_file)
    
    def list_jobs(self):
        """List running jobs"""
        print("📋 Current jobs in dev environment:")
        self.run_kubectl(["get", "jobs"])
    
    def get_job_logs(self, job_name: str):
        """Get logs for a specific job"""
        print(f"📋 Logs for job: {job_name}")
        self.run_kubectl(["logs", f"job/{job_name}", "--follow"])
    
    def delete_job(self, job_name: str):
        """Delete a job and its ConfigMap"""
        print(f"🗑️  Deleting job: {job_name}")
        self.run_kubectl(["delete", "job", job_name])
        self.run_kubectl(["delete", "configmap", f"{job_name}-script"])


def main():
    parser = argparse.ArgumentParser(description="Run dev operations in Kubernetes")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Migration command
    migration_parser = subparsers.add_parser('migration', help='Run database migration')
    migration_parser.add_argument('--name', required=True, help='Migration name')
    
    # Pipeline command
    pipeline_parser = subparsers.add_parser('pipeline', help='Run data pipeline')
    pipeline_parser.add_argument('--type', required=True, help='Pipeline type')
    pipeline_parser.add_argument('--date', help='Target date (YYYY-MM-DD)')
    pipeline_parser.add_argument('--symbols', help='Comma-separated symbols')
    pipeline_parser.add_argument('--limit', type=int, help='Limit number of symbols')
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Run SQL query')
    query_parser.add_argument('--sql', required=True, help='SQL query to execute')
    
    # Custom job command
    job_parser = subparsers.add_parser('job', help='Run custom script')
    job_parser.add_argument('--script', required=True, help='Path to script file')
    job_parser.add_argument('--args', help='Script arguments')
    
    # Utility commands
    subparsers.add_parser('list', help='List current jobs')
    
    logs_parser = subparsers.add_parser('logs', help='Get job logs')
    logs_parser.add_argument('--job', required=True, help='Job name')
    
    delete_parser = subparsers.add_parser('delete', help='Delete job')
    delete_parser.add_argument('--job', required=True, help='Job name')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    runner = DevK8sRunner()
    
    if args.command == 'migration':
        runner.run_migration(args.name)
    elif args.command == 'pipeline':
        pipeline_kwargs = {}
        if args.date:
            pipeline_kwargs['date'] = args.date
        if args.symbols:
            pipeline_kwargs['symbols'] = args.symbols
        if args.limit:
            pipeline_kwargs['limit'] = args.limit
        runner.run_pipeline(args.type, **pipeline_kwargs)
    elif args.command == 'query':
        runner.run_query(args.sql)
    elif args.command == 'job':
        runner.run_custom_job(args.script, args.args or "")
    elif args.command == 'list':
        runner.list_jobs()
    elif args.command == 'logs':
        runner.get_job_logs(args.job)
    elif args.command == 'delete':
        runner.delete_job(args.job)


if __name__ == "__main__":
    main()