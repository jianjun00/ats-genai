#!/usr/bin/env python
"""
Flyte Workflow for Instrument Polygon Operations

This script defines a Flyte workflow for managing instrument polygon operations.
It dynamically generates Kubernetes job configurations and can apply them to the cluster.
"""

import os
import subprocess
import tempfile
import yaml
from typing import Dict, List, Optional, Any, Tuple

import flytekit
from flytekit import task, workflow, dynamic
from flytekit.types.file import FlyteFile

# Import from local module if running directly
try:
    from scripts.kubernetes.k8s_job_generator import JobConfig, create_backfill_job, create_test_job
except ImportError:
    # For Flyte registration, use absolute import
    from scripts.kubernetes.k8s_job_generator import JobConfig, create_backfill_job, create_test_job


@task
def generate_test_job_yaml(tickers: str, custom_name: str, 
                          memory_request: str, memory_limit: str,
                          cpu_request: str, cpu_limit: str) -> Tuple[str, str]:
    """
    Generate a test job YAML based on the provided parameters.
    
    Args:
        tickers: Comma-separated list of tickers
        custom_name: Custom job name
        memory_request: Memory request override
        memory_limit: Memory limit override
        cpu_request: CPU request override
        cpu_limit: CPU limit override
        
    Returns:
        Tuple of (job_name, yaml_content)
    """
    # Use default tickers if not provided
    tickers_to_use = tickers if tickers else "NFLX,GOOG,AVGO,ADBE,COST"
    
    # Create the job config
    job_config = create_test_job(tickers_to_use)
    
    # Apply overrides if provided
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
    
    # Generate the YAML
    yaml_dict = job_config.generate_yaml()
    yaml_content = yaml.dump(yaml_dict, default_flow_style=False)
    
    return job_config.name, yaml_content


@task
def generate_backfill_job_yaml(custom_name: str, memory_request: str, memory_limit: str,
                              cpu_request: str, cpu_limit: str) -> Tuple[str, str]:
    """
    Generate a backfill job YAML based on the provided parameters.
    
    Args:
        custom_name: Custom job name
        memory_request: Memory request override
        memory_limit: Memory limit override
        cpu_request: CPU request override
        cpu_limit: CPU limit override
        
    Returns:
        Tuple of (job_name, yaml_content)
    """
    # Create the job config
    job_config = create_backfill_job()
    
    # Apply overrides if provided
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
    
    # Generate the YAML
    yaml_dict = job_config.generate_yaml()
    yaml_content = yaml.dump(yaml_dict, default_flow_style=False)
    
    return job_config.name, yaml_content


@task
def save_yaml_to_file(job_name: str, yaml_content: str, output_dir: str) -> str:
    """
    Save the generated YAML to a file.
    
    Args:
        job_name: Name of the job
        yaml_content: YAML content to save
        output_dir: Directory to save the file in
        
    Returns:
        Path to the saved file
    """
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{job_name}.yaml")
    
    with open(output_path, 'w') as f:
        f.write(yaml_content)
    
    return output_path


@task
def apply_to_kubernetes(job_name: str, yaml_content: str) -> str:
    """
    Apply the generated YAML to the Kubernetes cluster.
    
    Args:
        job_name: Name of the job
        yaml_content: YAML content to apply
        
    Returns:
        Result message
    """
    # Create a temporary file to store the YAML
    with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False) as tmp:
        tmp.write(yaml_content.encode('utf-8'))
        tmp_path = tmp.name
    
    try:
        # Apply the job to the cluster
        result = subprocess.run(
            ['kubectl', 'apply', '-f', tmp_path],
            capture_output=True,
            text=True,
            check=True
        )
        return f"Successfully applied job {job_name}: {result.stdout}"
    except subprocess.CalledProcessError as e:
        return f"Failed to apply job {job_name}: {e.stderr}"
    finally:
        # Clean up the temporary file
        os.unlink(tmp_path)


@task
def format_result(output_path: str, apply_result: str = "") -> str:
    """
    Format the result message.
    
    Args:
        output_path: Path where the YAML was saved
        apply_result: Result of applying the job to the cluster (if any)
        
    Returns:
        Formatted result message
    """
    if apply_result:
        return f"Job saved to {output_path} and applied to cluster: {apply_result}"
    else:
        return f"Job saved to {output_path}"


@dynamic
def dynamic_job_workflow(
    job_type: str,
    tickers: str = "",
    memory_request: str = "",
    memory_limit: str = "",
    cpu_request: str = "",
    cpu_limit: str = "",
    custom_name: str = "",
    should_apply: bool = False,
    output_dir: str = "/home/jianjun/ats-genai/k8s/generated"
) -> str:
    """
    Dynamic workflow that handles job type selection at runtime.
    
    Args:
        job_type: Type of job (backfill or test)
        tickers: Comma-separated list of tickers (for test job)
        memory_request: Memory request override
        memory_limit: Memory limit override
        cpu_request: CPU request override
        cpu_limit: CPU limit override
        custom_name: Custom job name
        should_apply: Whether to apply the job to the cluster
        output_dir: Directory to save the generated YAML
        
    Returns:
        Result message
    """
    # Generate job YAML based on job type
    if job_type == "test":
        job_name, yaml_content = generate_test_job_yaml(
            tickers=tickers,
            custom_name=custom_name,
            memory_request=memory_request,
            memory_limit=memory_limit,
            cpu_request=cpu_request,
            cpu_limit=cpu_limit
        )
    else:  # backfill
        job_name, yaml_content = generate_backfill_job_yaml(
            custom_name=custom_name,
            memory_request=memory_request,
            memory_limit=memory_limit,
            cpu_request=cpu_request,
            cpu_limit=cpu_limit
        )
    
    # Save the YAML to a file
    output_path = save_yaml_to_file(
        job_name=job_name,
        yaml_content=yaml_content,
        output_dir=output_dir
    )
    
    # Apply to cluster if requested
    if should_apply:
        apply_result = apply_to_kubernetes(
            job_name=job_name,
            yaml_content=yaml_content
        )
        return format_result(output_path=output_path, apply_result=apply_result)
    else:
        return format_result(output_path=output_path)


@workflow
def instrument_polygon_workflow(
    job_type: str,
    tickers: str = "",
    memory_request: str = "",
    memory_limit: str = "",
    cpu_request: str = "",
    cpu_limit: str = "",
    custom_name: str = "",
    should_apply: bool = False,
    output_dir: str = "/home/jianjun/ats-genai/k8s/generated"
) -> str:
    """
    Main workflow for instrument polygon operations.
    
    Args:
        job_type: Type of job (backfill or test)
        tickers: Comma-separated list of tickers (for test job)
        memory_request: Memory request override
        memory_limit: Memory limit override
        cpu_request: CPU request override
        cpu_limit: CPU limit override
        custom_name: Custom job name
        should_apply: Whether to apply the job to the cluster
        output_dir: Directory to save the generated YAML
        
    Returns:
        Result message
    """
    return dynamic_job_workflow(
        job_type=job_type,
        tickers=tickers,
        memory_request=memory_request,
        memory_limit=memory_limit,
        cpu_request=cpu_request,
        cpu_limit=cpu_limit,
        custom_name=custom_name,
        should_apply=should_apply,
        output_dir=output_dir
    )


# Note: create_backfill_job is imported from scripts.kubernetes.k8s_job_generator


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run instrument polygon workflow")
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
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug mode')
    parser.add_argument('--custom-name', type=str, default="",
                        help='Custom job name')
    parser.add_argument('--apply', action='store_true',
                        help='Apply the job to the cluster')
    parser.add_argument('--output-dir', type=str, default="/home/jianjun/ats-genai/k8s/generated",
                        help='Directory to save the generated YAML')
    
    args = parser.parse_args()
    
    # Run the workflow locally
    result = instrument_polygon_workflow(
        job_type=args.job_type,
        tickers=args.tickers,
        memory_request=args.memory_request,
        memory_limit=args.memory_limit,
        cpu_request=args.cpu_request,
        cpu_limit=args.cpu_limit,
        custom_name=args.custom_name,
        should_apply=args.apply,
        output_dir=args.output_dir
    )
    
    print(result)
