#!/usr/bin/env python
"""
Fast Run Script for Flyte Instrument Polygon Workflow

This script uses pyflyte fast register/run to execute the instrument polygon workflow
with local code changes without rebuilding the container image.
"""

import os
import sys
import subprocess
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

def run_fast_workflow(job_type, tickers="", memory_request="", memory_limit="", 
                     cpu_request="", cpu_limit="", custom_name="", should_apply=False):
    """
    Run the instrument polygon workflow using pyflyte fast register/run.
    
    Args:
        job_type: Type of job (backfill or test)
        tickers: Comma-separated list of tickers (for test job)
        memory_request: Memory request override
        memory_limit: Memory limit override
        cpu_request: CPU request override
        cpu_limit: CPU limit override
        custom_name: Custom job name
        should_apply: Whether to apply the job to the cluster
    """
    # Activate the virtual environment
    venv_path = os.path.join(project_root, "flyte-venv", "bin", "activate")
    
    # Build the pyflyte command
    cmd = [
        f"source {venv_path} && ",
        "pyflyte run",
        "--remote",
        "--fast",
        "--image dragonflyer762/ats-genai:dev-latest",
        f"{os.path.join('scripts', 'flyte', 'flyte_instrument_polygon_workflow.py')}",
        "instrument_polygon_workflow",
        f"--job_type {job_type}"
    ]
    
    # Add optional parameters
    if tickers:
        cmd.append(f"--tickers '{tickers}'")
    if memory_request:
        cmd.append(f"--memory_request '{memory_request}'")
    if memory_limit:
        cmd.append(f"--memory_limit '{memory_limit}'")
    if cpu_request:
        cmd.append(f"--cpu_request '{cpu_request}'")
    if cpu_limit:
        cmd.append(f"--cpu_limit '{cpu_limit}'")
    if custom_name:
        cmd.append(f"--custom_name '{custom_name}'")
    if should_apply:
        cmd.append("--should_apply True")
    
    # Join the command parts
    full_cmd = " ".join(cmd)
    print(f"Running command: {full_cmd}")
    
    # Execute the command
    process = subprocess.Popen(full_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    
    print("STDOUT:")
    print(stdout.decode())
    
    if stderr:
        print("STDERR:")
        print(stderr.decode())
    
    return process.returncode

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run instrument polygon workflow with fast register/run")
    parser.add_argument('--job-type', choices=['backfill', 'test'], required=True, 
                        help='Type of job to generate')
    parser.add_argument('--tickers', type=str, default="",
                        help='Comma-separated list of tickers (for test job only)')
    parser.add_argument('--memory-request', type=str, default="",
                        help='Memory request (e.g., 256Mi)')
    parser.add_argument('--memory-limit', type=str, default="",
                        help='Memory limit (e.g., 512Mi)')
    parser.add_argument('--cpu-request', type=str, default="",
                        help='CPU request (e.g., 100m)')
    parser.add_argument('--cpu-limit', type=str, default="",
                        help='CPU limit (e.g., 250m)')
    parser.add_argument('--custom-name', type=str, default="",
                        help='Custom job name')
    parser.add_argument('--apply', action='store_true',
                        help='Apply the job to the cluster')
    
    args = parser.parse_args()
    
    exit_code = run_fast_workflow(
        job_type=args.job_type,
        tickers=args.tickers,
        memory_request=args.memory_request,
        memory_limit=args.memory_limit,
        cpu_request=args.cpu_request,
        cpu_limit=args.cpu_limit,
        custom_name=args.custom_name,
        should_apply=args.apply
    )
    
    sys.exit(exit_code)
