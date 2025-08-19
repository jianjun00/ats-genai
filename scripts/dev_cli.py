#!/usr/bin/env python3
"""
Simple Dev CLI for K8s Operations

Ensures all dev operations run in Kubernetes, never locally.

Usage:
    python scripts/dev_cli.py query "SELECT COUNT(*) FROM dev_daily_prices"
    python scripts/dev_cli.py migrate price-unification
    python scripts/dev_cli.py job price-unification --symbols AAPL,MSFT --date 2024-01-15
    python scripts/dev_cli.py list
    python scripts/dev_cli.py logs job-name
"""

import argparse
import subprocess
import sys
import os
import tempfile
import yaml
from datetime import datetime


def run_kubectl(cmd: list, namespace="ats-dev"):
    """Run kubectl command with proper namespace"""
    full_cmd = ["kubectl"] + cmd + ["-n", namespace]
    print(f"🚀 {' '.join(full_cmd)}")
    return subprocess.run(full_cmd)


def create_simple_job(job_name: str, script_content: str, description: str):
    """Create a simple K8s job using ConfigMap pattern like existing jobs"""
    
    configmap_yaml = f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: {job_name}-script
  namespace: ats-dev
data:
  run_job.py: |
{script_content}
"""

    job_yaml = f"""apiVersion: batch/v1
kind: Job
metadata:
  name: {job_name}
  namespace: ats-dev
spec:
  template:
    spec:
      containers:
      - name: runner
        image: python:3.12-slim
        command: ["/bin/bash"]
        args:
          - -c
          - |
            echo "📦 Installing dependencies..."
            pip install asyncpg yfinance pandas numpy
            
            echo "🔧 Running {description}..."
            python /scripts/run_job.py
            
            echo "✅ {description} completed!"
        volumeMounts:
        - name: script-volume
          mountPath: /scripts
      volumes:
      - name: script-volume
        configMap:
          name: {job_name}-script
      restartPolicy: Never
  backoffLimit: 3
"""

    combined_yaml = configmap_yaml + "\n---\n" + job_yaml
    return combined_yaml


def query_command(sql: str):
    """Run a SQL query in K8s"""
    job_name = f"query-{int(datetime.now().timestamp())}"
    
    script_content = f'''    import asyncio
    import asyncpg
    import logging

    async def main():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        db_url = "postgresql://postgres:dev_password@postgres-simple:5432/dev_db"
        
        try:
            conn = await asyncpg.connect(db_url)
            logger.info("✅ Connected to database")
            
            sql = """{sql}"""
            
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
        asyncio.run(main())'''

    yaml_content = create_simple_job(job_name, script_content, "Database Query")
    apply_and_monitor_job(job_name, yaml_content)


def migrate_command(migration_name: str):
    """Run a database migration"""
    job_name = f"migrate-{migration_name}-{int(datetime.now().timestamp())}"
    
    script_content = f'''    import asyncio
    import asyncpg
    import logging

    async def main():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        db_url = "postgresql://postgres:dev_password@postgres-simple:5432/dev_db"
        
        try:
            conn = await asyncpg.connect(db_url)
            logger.info("✅ Connected to database")
            
            logger.info(f"🔧 Running migration: {migration_name}...")
            
            # Run specific migration logic here
            if "{migration_name}" == "test":
                await conn.execute("SELECT 1")
                logger.info("✅ Test migration completed")
            else:
                logger.info(f"Migration {migration_name} not implemented yet")
            
        except Exception as e:
            logger.error(f"❌ Migration failed: {{e}}")
        finally:
            await conn.close()

    if __name__ == "__main__":
        asyncio.run(main())'''

    yaml_content = create_simple_job(job_name, script_content, f"Migration: {migration_name}")
    apply_and_monitor_job(job_name, yaml_content)


def job_command(job_type: str, **kwargs):
    """Run a specific job type"""
    job_name = f"{job_type}-{int(datetime.now().timestamp())}"
    
    if job_type == "price-unification":
        script_content = f'''    import asyncio
    import sys
    import logging
    from datetime import date

    # Add the src directory to Python path
    sys.path.append('/scripts')

    async def main():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        try:
            # Import our pipeline modules
            from unified_daily_price_pipeline import UnifiedDailyPricePipeline
            from config.environment import Environment
            
            symbols = "{kwargs.get('symbols', 'AAPL,MSFT')}"
            target_date = "{kwargs.get('date', '2025-08-15')}"
            limit = {kwargs.get('limit', 5)}
            
            logger.info(f"🔧 Running price unification for {{symbols}} on {{target_date}}")
            
            # Initialize pipeline
            env = Environment()
            pipeline = UnifiedDailyPricePipeline(env)
            
            await pipeline.connect()
            
            # Parse parameters
            start_date = date.fromisoformat(target_date)
            symbol_list = symbols.split(',') if symbols else None
            
            # Run pipeline
            results = await pipeline.run_pipeline(
                start_date=start_date,
                end_date=start_date,
                symbols=symbol_list,
                limit=limit,
                skip_existing=False
            )
            
            logger.info(f"✅ Price unification completed successfully!")
            logger.info(f"📊 Results: {{results['successful']}}/{{results['total_processed']}} successful")
            logger.info(f"📋 Run ID: {{results['run_id']}}")
            
        except Exception as e:
            logger.error(f"❌ Job failed: {{e}}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            if 'pipeline' in locals():
                await pipeline.disconnect()

    if __name__ == "__main__":
        asyncio.run(main())'''
    else:
        script_content = f'''    import asyncio
    import logging

    async def main():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        logger.info(f"🔧 Running job: {job_type}")
        logger.info(f"Parameters: {kwargs}")
        logger.info("✅ Job completed (placeholder)")

    if __name__ == "__main__":
        asyncio.run(main())'''

    yaml_content = create_simple_job(job_name, script_content, f"Job: {job_type}")
    apply_and_monitor_job(job_name, yaml_content)


def apply_and_monitor_job(job_name: str, yaml_content: str):
    """Apply job YAML and monitor logs"""
    
    # Write YAML to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(yaml_content)
        yaml_file = f.name
    
    try:
        # Apply the job
        result = run_kubectl(["apply", "-f", yaml_file])
        if result.returncode != 0:
            print(f"❌ Failed to apply job")
            return False
        
        print(f"✅ Job {job_name} applied successfully")
        
        # Show logs
        print(f"📋 Following job logs...")
        run_kubectl(["logs", f"job/{job_name}", "--follow"])
        
        return True
        
    finally:
        # Clean up temp file
        os.unlink(yaml_file)


def list_command():
    """List current jobs"""
    print("📋 Current jobs in dev environment:")
    run_kubectl(["get", "jobs", "-o", "wide"])


def logs_command(job_name: str):
    """Get logs for a specific job"""
    print(f"📋 Logs for job: {job_name}")
    run_kubectl(["logs", f"job/{job_name}", "--follow"])


def main():
    parser = argparse.ArgumentParser(description="Simple Dev CLI for K8s operations")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Run SQL query')
    query_parser.add_argument('sql', help='SQL query to execute')
    
    # Migration command  
    migrate_parser = subparsers.add_parser('migrate', help='Run database migration')
    migrate_parser.add_argument('name', help='Migration name')
    
    # Job command
    job_parser = subparsers.add_parser('job', help='Run specific job')
    job_parser.add_argument('type', help='Job type (e.g., price-unification)')
    job_parser.add_argument('--symbols', help='Comma-separated symbols')
    job_parser.add_argument('--date', help='Target date (YYYY-MM-DD)')
    job_parser.add_argument('--limit', type=int, help='Limit number of items')
    
    # Utility commands
    subparsers.add_parser('list', help='List current jobs')
    
    logs_parser = subparsers.add_parser('logs', help='Get job logs')
    logs_parser.add_argument('job', help='Job name')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    print(f"🎯 Dev Environment CLI - All operations run in Kubernetes (ats-dev namespace)")
    print(f"🔄 Command: {args.command}")
    
    if args.command == 'query':
        query_command(args.sql)
    elif args.command == 'migrate':
        migrate_command(args.name)
    elif args.command == 'job':
        job_kwargs = {}
        if hasattr(args, 'symbols') and args.symbols:
            job_kwargs['symbols'] = args.symbols
        if hasattr(args, 'date') and args.date:
            job_kwargs['date'] = args.date
        if hasattr(args, 'limit') and args.limit:
            job_kwargs['limit'] = args.limit
        job_command(args.type, **job_kwargs)
    elif args.command == 'list':
        list_command()
    elif args.command == 'logs':
        logs_command(args.job)


if __name__ == "__main__":
    main()