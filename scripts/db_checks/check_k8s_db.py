#!/usr/bin/env python3
import subprocess
import sys
import json

def run_kubectl_command(command):
    """Run a kubectl command and return the output."""
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {' '.join(command)}", file=sys.stderr)
        print(f"Error output: {e.stderr}", file=sys.stderr)
        return None

def check_database():
    """Check the database in the Kubernetes cluster using the db-client pod."""
    print("Checking database in Kubernetes cluster...")
    
    # Check if db-client pod exists
    pod_check = run_kubectl_command(["kubectl", "get", "pod", "db-client", "-n", "ats-dev", "-o", "json"])
    if not pod_check:
        print("db-client pod not found. Creating it...")
        create_pod = run_kubectl_command([
            "kubectl", "apply", "-f", "k8s/dev/db-client-pod.yaml"
        ])
        if not create_pod:
            print("Failed to create db-client pod.")
            return 1
        
        # Wait for pod to be ready
        print("Waiting for db-client pod to be ready...")
        wait_result = run_kubectl_command([
            "kubectl", "wait", "--for=condition=ready", "pod/db-client", 
            "-n", "ats-dev", "--timeout=30s"
        ])
        if not wait_result:
            print("Timed out waiting for db-client pod to be ready.")
            return 1
    
    print("\nChecking database tables...")
    tables_output = run_kubectl_command([
        "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
        "psql", "-c", "\\dt dev_*"
    ])
    if tables_output:
        print("\nDatabase tables:")
        print(tables_output)
    
    print("\nChecking instruments table...")
    instruments_output = run_kubectl_command([
        "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
        "psql", "-c", "SELECT * FROM dev_instruments;"
    ])
    if instruments_output:
        print("\nInstruments:")
        print(instruments_output)
    
    print("\nChecking instrument xrefs table...")
    xrefs_output = run_kubectl_command([
        "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
        "psql", "-c", "SELECT ix.id, i.symbol, v.name as vendor, ix.vendor_symbol FROM dev_instrument_xrefs ix JOIN dev_instruments i ON ix.instrument_id = i.id JOIN dev_vendors v ON ix.vendor_id = v.id;"
    ])
    if xrefs_output:
        print("\nInstrument xrefs:")
        print(xrefs_output)
    
    print("\nDatabase check completed successfully!")
    return 0

if __name__ == "__main__":
    exit_code = check_database()
    sys.exit(exit_code)
