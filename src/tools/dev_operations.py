#!/usr/bin/env python3
"""
Dev CLI for running jobs in ats-dev Kubernetes namespace

Automatically handles kubectl commands, namespace targeting, and monitoring
without requiring manual password entry or kubectl knowledge.
"""

import subprocess
import sys
import time
import argparse
import os
from pathlib import Path

class DevCLI:
    def __init__(self):
        self.namespace = "ats-dev"
        self.kubectl = "kubectl"
        
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
    
    def deploy_job(self, yaml_file):
        """Deploy job to ats-dev namespace"""
        if not os.path.exists(yaml_file):
            print(f"❌ Job file not found: {yaml_file}")
            return False
            
        print(f"🚀 Deploying job: {yaml_file}")
        result = self.run_command(f"{self.kubectl} apply -f {yaml_file}")
        
        if result is not None:
            print("✅ Job deployed successfully")
            return True
        else:
            print("❌ Job deployment failed")
            return False
    
    def get_jobs(self, filter_pattern=None):
        """Get jobs in ats-dev namespace"""
        cmd = f"{self.kubectl} get jobs -n {self.namespace}"
        if filter_pattern:
            cmd += f" | grep {filter_pattern}"
        
        result = self.run_command(cmd, "Getting jobs status")
        if result:
            print(result)
        return result
    
    def get_job_logs(self, job_name, tail_lines=20, follow=False):
        """Get logs for a specific job"""
        cmd = f"{self.kubectl} logs job/{job_name} -n {self.namespace}"
        if tail_lines:
            cmd += f" --tail={tail_lines}"
        if follow:
            cmd += " --follow"
            
        result = self.run_command(cmd, f"Getting logs for job: {job_name}")
        if result:
            print(result)
        return result
    
    def monitor_job(self, job_name, check_interval=10):
        """Monitor job until completion"""
        print(f"📊 Monitoring job: {job_name}")
        
        while True:
            # Check job status
            status_cmd = f"{self.kubectl} get job {job_name} -n {self.namespace} -o jsonpath='{{.status.conditions[0].type}}'"
            status = self.run_command(status_cmd)
            
            if status == "Complete":
                print("✅ Job completed successfully!")
                self.get_job_logs(job_name, tail_lines=10)
                break
            elif status == "Failed":
                print("❌ Job failed!")
                self.get_job_logs(job_name, tail_lines=20)
                break
            else:
                print(f"🔄 Job still running... (status: {status})")
                self.get_job_logs(job_name, tail_lines=5)
                
            time.sleep(check_interval)
    
    def delete_job(self, job_name):
        """Delete a job"""
        cmd = f"{self.kubectl} delete job {job_name} -n {self.namespace}"
        result = self.run_command(cmd, f"Deleting job: {job_name}")
        
        if result is not None:
            print(f"✅ Job {job_name} deleted")
            return True
        else:
            print(f"❌ Failed to delete job: {job_name}")
            return False
    
    def run_and_monitor(self, yaml_file, job_name=None):
        """Deploy job and monitor until completion"""
        if not self.deploy_job(yaml_file):
            return False
            
        # Extract job name from yaml if not provided
        if not job_name:
            with open(yaml_file, 'r') as f:
                content = f.read()
                for line in content.split('\n'):
                    if 'name:' in line and 'job' in line.lower():
                        job_name = line.split('name:')[1].strip()
                        break
        
        if not job_name:
            print("❌ Could not determine job name")
            return False
            
        print(f"🎯 Job name: {job_name}")
        time.sleep(2)  # Give job time to start
        
        self.monitor_job(job_name)
        return True
    
    def query_db(self, sql_query, description=None):
        """Run database query in ats-dev"""
        if description:
            print(f"📊 {description}")
            
        # Create a simple query job
        query_job_yaml = f"""
apiVersion: batch/v1
kind: Job
metadata:
  name: query-{int(time.time())}
  namespace: ats-dev
spec:
  template:
    spec:
      containers:
      - name: query-runner
        image: postgres:13
        command: ["psql"]
        args:
          - "postgresql://postgres:dev_password@postgres:5432/dev_db"
          - "-c"
          - "{sql_query}"
      restartPolicy: Never
  backoffLimit: 1
"""
        
        temp_file = f"/tmp/query_{int(time.time())}.yaml"
        with open(temp_file, 'w') as f:
            f.write(query_job_yaml)
            
        self.run_and_monitor(temp_file)
        os.remove(temp_file)
    
    def psql(self, sql_query, description=None):
        """Run psql query directly using local connection"""
        if description:
            print(f"📊 {description}")
        
        # Use the same connection pattern as before
        cmd = f'PGPASSWORD=dev_password psql -h localhost -p 5433 -U postgres -d dev_db -c "{sql_query}"'
        result = self.run_command(cmd)
        
        if result:
            print(result)
        return result

def main():
    parser = argparse.ArgumentParser(description="Dev CLI for ats-dev Kubernetes operations")
    parser.add_argument("action", choices=[
        "deploy", "monitor", "logs", "status", "delete", "query", "run", "psql"
    ], help="Action to perform")
    
    parser.add_argument("--file", "-f", help="YAML file to deploy")
    parser.add_argument("--job", "-j", help="Job name")
    parser.add_argument("--query", "-q", help="SQL query to run")
    parser.add_argument("--tail", "-t", type=int, default=20, help="Number of log lines to show")
    parser.add_argument("--follow", action="store_true", help="Follow logs")
    parser.add_argument("--filter", help="Filter jobs by pattern")
    
    args = parser.parse_args()
    
    cli = DevCLI()
    
    if args.action == "deploy":
        if not args.file:
            print("❌ --file required for deploy action")
            sys.exit(1)
        cli.deploy_job(args.file)
        
    elif args.action == "monitor":
        if not args.job:
            print("❌ --job required for monitor action")
            sys.exit(1)
        cli.monitor_job(args.job)
        
    elif args.action == "logs":
        if not args.job:
            print("❌ --job required for logs action")
            sys.exit(1)
        cli.get_job_logs(args.job, args.tail, args.follow)
        
    elif args.action == "status":
        cli.get_jobs(args.filter)
        
    elif args.action == "delete":
        if not args.job:
            print("❌ --job required for delete action")
            sys.exit(1)
        cli.delete_job(args.job)
        
    elif args.action == "query":
        if not args.query:
            print("❌ --query required for query action")
            sys.exit(1)
        cli.query_db(args.query)
        
    elif args.action == "run":
        if not args.file:
            print("❌ --file required for run action")
            sys.exit(1)
        cli.run_and_monitor(args.file, args.job)
        
    elif args.action == "psql":
        if not args.query:
            print("❌ --query required for psql action")
            sys.exit(1)
        cli.psql(args.query)

if __name__ == "__main__":
    main()