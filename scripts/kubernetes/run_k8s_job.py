#!/usr/bin/env python
"""
Kubernetes Job Runner for Instrument Polygon Jobs

This script generates and applies Kubernetes job configurations directly to the cluster.
It uses the instrument_polygon_job_generator module to create job configurations.
"""

import os
import argparse
import subprocess
import tempfile
import yaml
import sys
from typing import Dict, Any, Optional

# Import job generator functions
from instrument_polygon_job_generator import (
    JobConfig, create_backfill_job, create_test_job, save_yaml
)


def generate_job_config(
    job_type: str,
    tickers: Optional[str] = None,
    memory_request: Optional[str] = None,
    memory_limit: Optional[str] = None,
    cpu_request: Optional[str] = None,
    cpu_limit: Optional[str] = None,
    debug: bool = False,
    custom_name: Optional[str] = None
) -> JobConfig:
    """
    Generate a job configuration based on the provided parameters.
    
    Args:
        job_type: Type of job ('backfill' or 'test')
        tickers: Comma-separated list of tickers (for test job only)
        memory_request: Memory request override
        memory_limit: Memory limit override
        cpu_request: CPU request override
        cpu_limit: CPU limit override
        debug: Whether to add debug flag
        custom_name: Custom job name
        
    Returns:
        JobConfig object
    """
    # Create the appropriate job config based on the job type
    if job_type == 'backfill':
        job_config = create_backfill_job()
    else:  # test
        job_config = create_test_job(tickers or "NFLX,GOOG,AVGO,ADBE,COST", debug)
    
    # Override parameters if provided
    if custom_name:
        job_config.name = custom_name
    
    if memory_request:
        job_config.memory_request = memory_request
    
    if memory_limit:
        job_config.memory_limit = memory_limit
    
    if cpu_request:
        job_config.cpu_request = cpu_request
    
    if cpu_limit:
        job_config.cpu_limit = cpu_limit
    
    return job_config


def apply_job_to_k8s(yaml_content: str, dry_run: bool = False) -> str:
    """
    Apply the job YAML directly to the Kubernetes cluster.
    
    Args:
        yaml_content: YAML content to apply
        dry_run: If True, only validate but don't apply
        
    Returns:
        Result message
    """
    # Create a temporary file to store the YAML
    with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False) as tmp:
        tmp.write(yaml_content.encode('utf-8'))
        tmp_path = tmp.name
    
    try:
        # Build the kubectl command
        cmd = ['kubectl', 'apply', '-f', tmp_path]
        if dry_run:
            cmd.append('--dry-run=server')
        
        # Run the kubectl command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        return f"Successfully applied job: {result.stdout}"
    except subprocess.CalledProcessError as e:
        return f"Failed to apply job: {e.stderr}"
    finally:
        # Clean up the temporary file
        os.unlink(tmp_path)


def run_job(args: argparse.Namespace) -> None:
    """
    Generate and run a Kubernetes job based on command line arguments.
    
    Args:
        args: Command line arguments
    """
    # Generate the job configuration
    job_config = generate_job_config(
        job_type=args.job_type,
        tickers=args.tickers,
        memory_request=args.memory_request,
        memory_limit=args.memory_limit,
        cpu_request=args.cpu_request,
        cpu_limit=args.cpu_limit,
        debug=args.debug,
        custom_name=args.custom_name
    )
    
    # Generate YAML content
    yaml_dict = job_config.generate_yaml()
    yaml_content = yaml.dump(yaml_dict, default_flow_style=False)
    
    # Save YAML to file if requested
    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, 'w') as f:
            f.write(yaml_content)
        print(f"Job YAML saved to {args.output}")
    
    # Apply job to Kubernetes cluster
    if args.apply:
        result = apply_job_to_k8s(yaml_content, args.dry_run)
        print(result)
    elif not args.output:
        # If not applying and not saving to file, print to stdout
        print(yaml_content)


def main():
    parser = argparse.ArgumentParser(description="Run Kubernetes jobs for instrument polygon operations")
    parser.add_argument('--job-type', choices=['backfill', 'test'], required=True, 
                        help='Type of job to generate')
    parser.add_argument('--tickers', type=str, default=None,
                        help='Comma-separated list of tickers (for test job only)')
    parser.add_argument('--memory-request', type=str, default=None,
                        help='Memory request (e.g., 256Mi)')
    parser.add_argument('--memory-limit', type=str, default=None,
                        help='Memory limit (e.g., 512Mi)')
    parser.add_argument('--cpu-request', type=str, default=None,
                        help='CPU request (e.g., 100m)')
    parser.add_argument('--cpu-limit', type=str, default=None,
                        help='CPU limit (e.g., 250m)')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug mode')
    parser.add_argument('--custom-name', type=str, default=None,
                        help='Custom job name')
    parser.add_argument('--output', type=str, default=None,
                        help='Output YAML file path')
    parser.add_argument('--apply', action='store_true',
                        help='Apply the job to the Kubernetes cluster')
    parser.add_argument('--dry-run', action='store_true',
                        help='Validate the job without applying it (only with --apply)')
    
    args = parser.parse_args()
    run_job(args)


if __name__ == "__main__":
    main()
