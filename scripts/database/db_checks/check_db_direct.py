#!/usr/bin/env python3
import subprocess
import sys

def run_command(cmd, shell=False):
    """Run a command and return the output."""
    print(f"Running: {cmd if shell else ' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            shell=shell,
            capture_output=True,
            text=True
        )
        print(f"Return code: {result.returncode}")
        if result.stdout:
            print(f"Stdout: {result.stdout[:200]}...")
        if result.stderr:
            print(f"Stderr: {result.stderr[:200]}...")
        return result
    except Exception as e:
        print(f"Exception: {str(e)}")
        return None

def main():
    """Check the database directly using kubectl exec."""
    print("Checking database in Kubernetes cluster...")
    
    # Check if db-client pod exists
    print("\nChecking if db-client pod exists...")
    pod_check = run_command(["kubectl", "get", "pod", "db-client", "-n", "ats-dev"])
    
    if pod_check.returncode != 0:
        print("\nCreating db-client pod...")
        create_pod = run_command(["kubectl", "apply", "-f", "k8s/dev/db-client-pod.yaml"])
        if create_pod.returncode != 0:
            print("Failed to create db-client pod.")
            return 1
        
        print("\nWaiting for pod to be ready...")
        wait_cmd = "kubectl wait --for=condition=ready pod/db-client -n ats-dev --timeout=30s"
        wait_result = run_command(wait_cmd, shell=True)
        if wait_result.returncode != 0:
            print("Pod not ready within timeout.")
            return 1
    
    # Simple database checks
    print("\nChecking database tables...")
    tables_cmd = "kubectl exec db-client -n ats-dev -- psql -c '\\dt dev_*'"
    tables_result = run_command(tables_cmd, shell=True)
    
    print("\nChecking instruments...")
    instruments_cmd = "kubectl exec db-client -n ats-dev -- psql -c 'SELECT COUNT(*) FROM dev_instruments;'"
    instruments_result = run_command(instruments_cmd, shell=True)
    
    print("\nChecking xrefs...")
    xrefs_cmd = "kubectl exec db-client -n ats-dev -- psql -c 'SELECT COUNT(*) FROM dev_instrument_xrefs;'"
    xrefs_result = run_command(xrefs_cmd, shell=True)
    
    print("\nDatabase check completed!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
