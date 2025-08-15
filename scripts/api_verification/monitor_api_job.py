#!/usr/bin/env python3
"""
Script to monitor the API test job in Kubernetes.

This script checks if the API test job is running, gets its status,
and can display the logs if needed.
"""

import argparse
import subprocess
import json
import time
import sys
from datetime import datetime

def run_command(command):
    """Run a shell command and return the result."""
    try:
        result = subprocess.run(command, shell=True, check=True, 
                               capture_output=True, text=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")
        print(f"Error message: {e.stderr}")
        return None

def get_job_status(namespace="ats-dev", job_name="api-test-job"):
    """Get the status of the API test job."""
    command = f"kubectl get job {job_name} -n {namespace} -o json"
    output = run_command(command)
    
    if not output:
        return None
    
    try:
        job_data = json.loads(output)
        status = job_data.get("status", {})
        
        # Extract useful information
        active = status.get("active", 0)
        succeeded = status.get("succeeded", 0)
        failed = status.get("failed", 0)
        completion_time = status.get("completionTime")
        start_time = status.get("startTime")
        
        # Format times
        if start_time:
            start_time = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        if completion_time:
            completion_time = datetime.fromisoformat(completion_time.replace("Z", "+00:00"))
            
        # Calculate duration if possible
        duration = None
        if completion_time and start_time:
            duration = completion_time - start_time
            
        return {
            "active": active,
            "succeeded": succeeded,
            "failed": failed,
            "start_time": start_time,
            "completion_time": completion_time,
            "duration": duration,
            "raw_status": status
        }
    except json.JSONDecodeError:
        print(f"Error parsing JSON from command output: {output}")
        return None

def get_job_pods(namespace="ats-dev", job_name="api-test-job"):
    """Get the pods associated with the job."""
    command = f"kubectl get pods -n {namespace} -l job-name={job_name} -o json"
    output = run_command(command)
    
    if not output:
        return []
    
    try:
        pods_data = json.loads(output)
        pods = []
        
        for pod in pods_data.get("items", []):
            pod_name = pod.get("metadata", {}).get("name")
            pod_status = pod.get("status", {}).get("phase")
            container_statuses = pod.get("status", {}).get("containerStatuses", [])
            
            # Get container status
            container_status = "Unknown"
            if container_statuses:
                container = container_statuses[0]
                if container.get("ready"):
                    container_status = "Ready"
                elif container.get("state", {}).get("running"):
                    container_status = "Running"
                elif container.get("state", {}).get("terminated"):
                    termination = container.get("state", {}).get("terminated", {})
                    exit_code = termination.get("exitCode")
                    reason = termination.get("reason")
                    container_status = f"Terminated ({reason}, exit code: {exit_code})"
                elif container.get("state", {}).get("waiting"):
                    waiting = container.get("state", {}).get("waiting", {})
                    reason = waiting.get("reason")
                    container_status = f"Waiting ({reason})"
            
            pods.append({
                "name": pod_name,
                "status": pod_status,
                "container_status": container_status
            })
            
        return pods
    except json.JSONDecodeError:
        print(f"Error parsing JSON from command output: {output}")
        return []

def get_pod_logs(pod_name, namespace="ats-dev", tail=50):
    """Get logs from a specific pod."""
    command = f"kubectl logs {pod_name} -n {namespace} --tail={tail}"
    return run_command(command)

def check_port_forward(namespace="ats-dev", job_name="api-test-job"):
    """Check if port-forwarding is possible and set it up if requested."""
    pods = get_job_pods(namespace, job_name)
    
    if not pods:
        print("No pods found for the API test job.")
        return False
    
    running_pods = [pod for pod in pods if pod["status"] == "Running"]
    
    if not running_pods:
        print("No running pods found for the API test job.")
        return False
    
    pod = running_pods[0]
    print(f"Found running pod: {pod['name']}")
    
    # Check if port 8080 is exposed
    command = f"kubectl get pod {pod['name']} -n {namespace} -o json"
    output = run_command(command)
    
    if not output:
        return False
    
    try:
        pod_data = json.loads(output)
        containers = pod_data.get("spec", {}).get("containers", [])
        
        port_found = False
        for container in containers:
            ports = container.get("ports", [])
            for port in ports:
                if port.get("containerPort") == 8080:
                    port_found = True
                    break
        
        if not port_found:
            print("Warning: Port 8080 is not explicitly exposed in the pod spec.")
            print("Port-forwarding may still work if the API is listening on this port.")
        
        return True
    except json.JSONDecodeError:
        print(f"Error parsing JSON from command output: {output}")
        return False

def setup_port_forward(namespace="ats-dev", job_name="api-test-job", local_port=8080):
    """Set up port forwarding to the API pod."""
    pods = get_job_pods(namespace, job_name)
    
    if not pods:
        print("No pods found for the API test job.")
        return None
    
    running_pods = [pod for pod in pods if pod["status"] == "Running"]
    
    if not running_pods:
        print("No running pods found for the API test job.")
        return None
    
    pod = running_pods[0]
    print(f"Setting up port-forwarding to pod {pod['name']} on port {local_port}:8080...")
    
    command = f"kubectl port-forward {pod['name']} {local_port}:8080 -n {namespace}"
    print(f"Running command: {command}")
    
    # Start port-forwarding in a subprocess
    process = subprocess.Popen(
        command, 
        shell=True, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Wait a bit for port-forwarding to establish
    time.sleep(2)
    
    # Check if process is still running
    if process.poll() is not None:
        stderr = process.stderr.read()
        print(f"Error setting up port-forwarding: {stderr}")
        return None
    
    print(f"Port-forwarding established. API should be accessible at http://localhost:{local_port}")
    return process

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Monitor the API test job in Kubernetes")
    parser.add_argument("--namespace", default="ats-dev", help="Kubernetes namespace (default: ats-dev)")
    parser.add_argument("--job-name", default="api-test-job", help="Job name (default: api-test-job)")
    parser.add_argument("--logs", action="store_true", help="Show logs from the job pods")
    parser.add_argument("--tail", type=int, default=50, help="Number of log lines to show (default: 50)")
    parser.add_argument("--port-forward", action="store_true", help="Set up port-forwarding to the API pod")
    parser.add_argument("--local-port", type=int, default=8080, help="Local port for port-forwarding (default: 8080)")
    parser.add_argument("--watch", action="store_true", help="Watch job status continuously")
    parser.add_argument("--interval", type=int, default=5, help="Watch interval in seconds (default: 5)")
    args = parser.parse_args()
    
    # Check if job exists
    job_status = get_job_status(args.namespace, args.job_name)
    if not job_status:
        print(f"Job {args.job_name} not found in namespace {args.namespace}")
        return 1
    
    port_forward_process = None
    
    try:
        if args.watch:
            print(f"Watching job {args.job_name} in namespace {args.namespace}...")
            try:
                while True:
                    job_status = get_job_status(args.namespace, args.job_name)
                    if not job_status:
                        print(f"Job {args.job_name} not found in namespace {args.namespace}")
                        break
                    
                    print("\n" + "="*50)
                    print(f"Job Status at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:")
                    print(f"Active: {job_status['active']}")
                    print(f"Succeeded: {job_status['succeeded']}")
                    print(f"Failed: {job_status['failed']}")
                    
                    pods = get_job_pods(args.namespace, args.job_name)
                    print(f"\nPods ({len(pods)}):")
                    for pod in pods:
                        print(f"- {pod['name']}: {pod['status']} ({pod['container_status']})")
                    
                    if args.logs and pods:
                        for pod in pods:
                            print(f"\nLogs from {pod['name']} (last {args.tail} lines):")
                            logs = get_pod_logs(pod['name'], args.namespace, args.tail)
                            if logs:
                                print(logs)
                            else:
                                print("No logs available")
                    
                    time.sleep(args.interval)
            except KeyboardInterrupt:
                print("\nStopped watching job status.")
        else:
            # Print job status
            print(f"Job Status for {args.job_name} in namespace {args.namespace}:")
            print(f"Active: {job_status['active']}")
            print(f"Succeeded: {job_status['succeeded']}")
            print(f"Failed: {job_status['failed']}")
            
            if job_status['start_time']:
                print(f"Start Time: {job_status['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            
            if job_status['completion_time']:
                print(f"Completion Time: {job_status['completion_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            
            if job_status['duration']:
                print(f"Duration: {job_status['duration']}")
            
            # Get pods
            pods = get_job_pods(args.namespace, args.job_name)
            print(f"\nPods ({len(pods)}):")
            for pod in pods:
                print(f"- {pod['name']}: {pod['status']} ({pod['container_status']})")
            
            # Show logs if requested
            if args.logs and pods:
                for pod in pods:
                    print(f"\nLogs from {pod['name']} (last {args.tail} lines):")
                    logs = get_pod_logs(pod['name'], args.namespace, args.tail)
                    if logs:
                        print(logs)
                    else:
                        print("No logs available")
            
            # Set up port-forwarding if requested
            if args.port_forward:
                if check_port_forward(args.namespace, args.job_name):
                    port_forward_process = setup_port_forward(
                        args.namespace, args.job_name, args.local_port
                    )
                    
                    if port_forward_process:
                        print("Press Ctrl+C to stop port-forwarding...")
                        try:
                            # Keep the script running while port-forwarding is active
                            while port_forward_process.poll() is None:
                                time.sleep(1)
                        except KeyboardInterrupt:
                            print("\nStopping port-forwarding...")
        
        return 0
    finally:
        # Clean up port-forwarding process if it exists
        if port_forward_process and port_forward_process.poll() is None:
            port_forward_process.terminate()
            port_forward_process.wait()
            print("Port-forwarding stopped")

if __name__ == "__main__":
    sys.exit(main())
