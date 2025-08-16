#!/usr/bin/env python3
"""
Test script to verify Kubernetes jobs work correctly with Minikube.
This script creates a simple test job and verifies it runs successfully.
"""

import os
import argparse
import subprocess
import time
import yaml
import tempfile
from typing import Dict, Any, Optional, List, Tuple

def check_minikube_status() -> bool:
    """Check if Minikube is running."""
    try:
        result = subprocess.run(
            ["minikube", "status"],
            capture_output=True,
            text=True,
            check=True
        )
        return "apiserver: Running" in result.stdout
    except subprocess.CalledProcessError:
        return False

def create_test_job(name: str = "minikube-test-job", namespace: str = "ats-dev") -> Dict[str, Any]:
    """Create a simple test job configuration."""
    job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": namespace
        },
        "spec": {
            "backoffLimit": 1,
            "ttlSecondsAfterFinished": 100,
            "template": {
                "spec": {
                    "containers": [{
                        "name": "test-container",
                        "image": "busybox",
                        "command": ["sh", "-c", "echo 'Minikube test job running successfully!' && sleep 5"],
                        "resources": {
                            "requests": {
                                "memory": "64Mi",
                                "cpu": "100m"
                            },
                            "limits": {
                                "memory": "128Mi",
                                "cpu": "200m"
                            }
                        }
                    }],
                    "restartPolicy": "Never"
                }
            }
        }
    }
    return job

def apply_job(job_yaml: Dict[str, Any]) -> Tuple[bool, str]:
    """Apply a job to the Kubernetes cluster."""
    with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False) as tmp:
        yaml_content = yaml.dump(job_yaml, default_flow_style=False)
        tmp.write(yaml_content.encode('utf-8'))
        tmp_path = tmp.name
    
    try:
        result = subprocess.run(
            ['kubectl', 'apply', '-f', tmp_path],
            capture_output=True,
            text=True,
            check=True
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr
    finally:
        os.unlink(tmp_path)

def wait_for_job_completion(job_name: str, namespace: str, timeout: int = 60) -> Tuple[bool, str]:
    """Wait for a job to complete and return its status."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Check job status
            result = subprocess.run(
                ['kubectl', 'get', 'job', job_name, '-n', namespace, '-o', 'json'],
                capture_output=True,
                text=True,
                check=True
            )
            
            import json
            job_status = json.loads(result.stdout)
            
            # Check if job completed successfully
            if 'status' in job_status and 'succeeded' in job_status['status'] and job_status['status']['succeeded'] > 0:
                # Get logs from the job's pod
                pods_result = subprocess.run(
                    ['kubectl', 'get', 'pods', '-n', namespace, '--selector=job-name=' + job_name, '-o', 'jsonpath={.items[0].metadata.name}'],
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                pod_name = pods_result.stdout.strip()
                
                if pod_name:
                    logs_result = subprocess.run(
                        ['kubectl', 'logs', pod_name, '-n', namespace],
                        capture_output=True,
                        text=True,
                        check=True
                    )
                    return True, logs_result.stdout
                
                return True, "Job completed successfully, but couldn't retrieve logs."
            
            # Check if job failed
            if 'status' in job_status and 'failed' in job_status['status'] and job_status['status']['failed'] > 0:
                return False, "Job failed."
                
            # Wait before checking again
            time.sleep(2)
            
        except subprocess.CalledProcessError as e:
            return False, f"Error checking job status: {e.stderr}"
    
    return False, f"Job did not complete within {timeout} seconds."

def cleanup_job(job_name: str, namespace: str) -> None:
    """Clean up the test job."""
    try:
        subprocess.run(
            ['kubectl', 'delete', 'job', job_name, '-n', namespace],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"Job {job_name} deleted.")
    except subprocess.CalledProcessError as e:
        print(f"Error deleting job: {e.stderr}")

def test_instrument_polygon_job(job_type: str, tickers: str) -> Tuple[bool, str]:
    """Test an instrument polygon job using the existing job generator."""
    try:
        # Import might fail if the module is not in the path
        import sys
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        from scripts.run_k8s_job import run_job
        from argparse import Namespace
        
        # Create arguments for the job
        args = Namespace(
            job_type=job_type,
            tickers=tickers,
            memory_request="128Mi",
            memory_limit="256Mi",
            cpu_request="100m",
            cpu_limit="200m",
            debug=True,
            custom_name=f"test-{job_type}-{int(time.time())}",
            output=None,
            apply=True,
            dry_run=False
        )
        
        # Run the job
        run_job(args)
        
        return True, f"Job {args.custom_name} submitted successfully."
    except Exception as e:
        return False, f"Error running instrument polygon job: {str(e)}"

def main():
    parser = argparse.ArgumentParser(description="Test Kubernetes jobs with Minikube")
    parser.add_argument('--job-type', choices=['simple', 'polygon-test', 'polygon-backfill'], 
                        default='simple', help='Type of job to test')
    parser.add_argument('--tickers', type=str, default="AAPL,MSFT",
                        help='Tickers for polygon test job')
    parser.add_argument('--namespace', type=str, default="ats-dev",
                        help='Kubernetes namespace to use')
    parser.add_argument('--no-cleanup', action='store_true',
                        help='Do not clean up the job after testing')
    
    args = parser.parse_args()
    
    # Check if Minikube is running
    print("Checking Minikube status...")
    if not check_minikube_status():
        print("ERROR: Minikube is not running. Please start Minikube first.")
        return 1
    
    print("✅ Minikube is running.")
    
    # Run the appropriate test based on job type
    if args.job_type == 'simple':
        # Create and apply a simple test job
        print("Creating simple test job...")
        job_name = f"minikube-test-{int(time.time())}"
        job_yaml = create_test_job(job_name, args.namespace)
        
        success, message = apply_job(job_yaml)
        if not success:
            print(f"ERROR: Failed to apply job: {message}")
            return 1
        
        print("✅ Job applied successfully.")
        print("Waiting for job to complete...")
        
        success, logs = wait_for_job_completion(job_name, args.namespace)
        if not success:
            print(f"ERROR: {logs}")
            if not args.no_cleanup:
                cleanup_job(job_name, args.namespace)
            return 1
        
        print("✅ Job completed successfully!")
        print("Job logs:")
        print("---")
        print(logs)
        print("---")
        
        if not args.no_cleanup:
            cleanup_job(job_name, args.namespace)
    
    elif args.job_type in ['polygon-test', 'polygon-backfill']:
        # Test using the instrument polygon job generator
        polygon_job_type = 'test' if args.job_type == 'polygon-test' else 'backfill'
        print(f"Testing instrument polygon {polygon_job_type} job...")
        
        success, message = test_instrument_polygon_job(polygon_job_type, args.tickers)
        if not success:
            print(f"ERROR: {message}")
            return 1
        
        print("✅ Instrument polygon job test initiated.")
        print(message)
        print("Note: Check job status manually with:")
        print(f"  kubectl get jobs -n {args.namespace}")
        print(f"  kubectl get pods -n {args.namespace}")
    
    print("\n✅ Minikube job test completed successfully!")
    return 0

if __name__ == "__main__":
    exit(main())
