#!/usr/bin/env python3
"""
Database management script for Kubernetes.
This script helps manage the database configuration in Kubernetes.
"""

import argparse
import subprocess
import sys
import time
from typing import List, Dict, Any, Tuple


def run_command(command: List[str], check: bool = True) -> Tuple[bool, str]:
    """Run a command and return the output."""
    try:
        result = subprocess.run(
            command,
            check=check,
            capture_output=True,
            text=True
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, f"Error: {e.stderr}"


def check_minikube_status() -> bool:
    """Check if Minikube is running."""
    success, output = run_command(["minikube", "status"], check=False)
    return success and "apiserver: Running" in output


def check_namespace_exists(namespace: str) -> bool:
    """Check if a namespace exists."""
    success, output = run_command(["kubectl", "get", "namespace", namespace], check=False)
    return success


def create_namespace(namespace: str) -> bool:
    """Create a namespace if it doesn't exist."""
    if not check_namespace_exists(namespace):
        print(f"Creating namespace {namespace}...")
        success, output = run_command(["kubectl", "create", "namespace", namespace])
        if success:
            print(f"Namespace {namespace} created successfully.")
            return True
        else:
            print(output)
            return False
    else:
        print(f"Namespace {namespace} already exists.")
        return True


def apply_database_config(namespace: str, config_file: str) -> bool:
    """Apply the database configuration."""
    print(f"Applying database configuration from {config_file}...")
    success, output = run_command(["kubectl", "apply", "-f", config_file])
    if success:
        print("Database configuration applied successfully.")
        return True
    else:
        print(output)
        return False


def apply_backup_job(namespace: str, backup_file: str) -> bool:
    """Apply the backup job configuration."""
    print(f"Applying backup job configuration from {backup_file}...")
    success, output = run_command(["kubectl", "apply", "-f", backup_file])
    if success:
        print("Backup job configuration applied successfully.")
        return True
    else:
        print(output)
        return False


def wait_for_database(namespace: str, timeout: int = 120) -> bool:
    """Wait for the database to be ready."""
    print(f"Waiting for database to be ready (timeout: {timeout}s)...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        success, output = run_command(
            ["kubectl", "get", "pods", "-n", namespace, "-l", "app=postgres", "-o", "jsonpath={.items[0].status.phase}"],
            check=False
        )
        if success and "Running" in output:
            print("Database is running.")
            
            # Check if it's actually ready using the readiness probe
            success, output = run_command(
                ["kubectl", "get", "pods", "-n", namespace, "-l", "app=postgres", "-o", "jsonpath={.items[0].status.containerStatuses[0].ready}"],
                check=False
            )
            if success and "true" in output:
                print("Database is ready.")
                return True
        
        print("Database not ready yet. Waiting...")
        time.sleep(5)
    
    print(f"Database did not become ready within {timeout} seconds.")
    return False


def check_database_status(namespace: str) -> None:
    """Check the status of the database components."""
    print("\nChecking database status:")
    
    # Check deployment
    print("\nDatabase deployment:")
    run_command(["kubectl", "get", "deployments", "-n", namespace, "-l", "app=postgres"])
    
    # Check pods
    print("\nDatabase pods:")
    run_command(["kubectl", "get", "pods", "-n", namespace, "-l", "app=postgres"])
    
    # Check service
    print("\nDatabase service:")
    run_command(["kubectl", "get", "services", "-n", namespace, "-l", "app=postgres"])
    
    # Check PVCs
    print("\nPersistent Volume Claims:")
    run_command(["kubectl", "get", "pvc", "-n", namespace])
    
    # Check init job
    print("\nDatabase initialization job:")
    run_command(["kubectl", "get", "jobs", "-n", namespace, "db-init"])
    
    # Check backup cronjob if it exists
    print("\nDatabase backup job:")
    run_command(["kubectl", "get", "cronjobs", "-n", namespace, "db-backup"], check=False)


def trigger_backup(namespace: str) -> bool:
    """Trigger a manual database backup."""
    print("Triggering manual database backup...")
    job_name = f"db-backup-manual-{int(time.time())}"
    success, output = run_command(
        ["kubectl", "create", "job", "--from=cronjob/db-backup", job_name, "-n", namespace],
        check=False
    )
    if success:
        print(f"Manual backup job {job_name} created successfully.")
        print("Use the following command to check the status:")
        print(f"kubectl get jobs -n {namespace} {job_name}")
        return True
    else:
        print(output)
        return False


def port_forward_database(namespace: str, local_port: int = 5432) -> None:
    """Port forward the database service to a local port."""
    print(f"Port forwarding database service to localhost:{local_port}...")
    print("Press Ctrl+C to stop.")
    try:
        subprocess.run(
            ["kubectl", "port-forward", f"service/postgres", f"{local_port}:5432", "-n", namespace],
            check=True
        )
    except KeyboardInterrupt:
        print("\nPort forwarding stopped.")
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")


def main():
    parser = argparse.ArgumentParser(description="Manage database in Kubernetes")
    parser.add_argument("--namespace", "-n", default="ats-dev", help="Kubernetes namespace")
    parser.add_argument("--config-file", default="/home/jianjun/ats-genai/k8s/dev/database.yaml", help="Database configuration file")
    parser.add_argument("--backup-file", default="/home/jianjun/ats-genai/k8s/dev/db-backup-job.yaml", help="Backup job configuration file")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Apply command
    apply_parser = subparsers.add_parser("apply", help="Apply database configuration")
    apply_parser.add_argument("--with-backup", action="store_true", help="Also apply backup job")
    apply_parser.add_argument("--wait", action="store_true", help="Wait for database to be ready")
    
    # Status command
    subparsers.add_parser("status", help="Check database status")
    
    # Backup command
    subparsers.add_parser("backup", help="Trigger a manual database backup")
    
    # Port-forward command
    port_forward_parser = subparsers.add_parser("port-forward", help="Port forward database to local port")
    port_forward_parser.add_argument("--port", type=int, default=5432, help="Local port to forward to")
    
    args = parser.parse_args()
    
    # Check if Minikube is running
    if not check_minikube_status():
        print("Error: Minikube is not running. Please start Minikube first.")
        return 1
    
    if args.command == "apply":
        # Create namespace if it doesn't exist
        if not create_namespace(args.namespace):
            return 1
        
        # Apply database configuration
        if not apply_database_config(args.namespace, args.config_file):
            return 1
        
        # Apply backup job if requested
        if args.with_backup:
            if not apply_backup_job(args.namespace, args.backup_file):
                return 1
        
        # Wait for database to be ready if requested
        if args.wait:
            if not wait_for_database(args.namespace):
                return 1
        
        # Check database status
        check_database_status(args.namespace)
    
    elif args.command == "status":
        check_database_status(args.namespace)
    
    elif args.command == "backup":
        trigger_backup(args.namespace)
    
    elif args.command == "port-forward":
        port_forward_database(args.namespace, args.port)
    
    else:
        parser.print_help()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
