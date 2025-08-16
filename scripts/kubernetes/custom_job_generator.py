#!/usr/bin/env python
"""
Custom Job Generator for Instrument Polygon Backfill

This script creates a custom job YAML with specific environment variables
to fix the database connection issue with connect_timeout parameter.
"""

import os
import yaml
from instrument_polygon_job_generator import JobConfig

def create_custom_backfill_job() -> JobConfig:
    """Create a configuration for the instrument-polygon-backfill job with custom environment variables."""
    job = JobConfig(
        name="instrument-polygon-backfill",
        memory_request="512Mi",
        memory_limit="1Gi",
        cpu_request="250m",
        cpu_limit="500m",
        active_deadline_seconds=7200,  # 2 hours timeout
    )
    
    # Add environment variables
    job.add_env_var("PYTHONPATH", "/app/src")
    job.add_env_var("LOG_LEVEL", "INFO")
    job.add_env_var("ENVIRONMENT", "dev")
    
    # Add environment variable to disable connect_timeout
    job.add_env_var("PYTHONDONTWRITEBYTECODE", "1")  # Prevent Python from writing .pyc files
    job.add_env_var("DB_DISABLE_CONNECT_TIMEOUT", "true")  # Custom flag to disable connect_timeout
    
    # Add secrets
    job.add_secret_env_var("DB_USER", "db-credentials-dev", "DB_USER")
    job.add_secret_env_var("DB_PASSWORD", "db-credentials-dev", "DB_PASSWORD")
    job.add_secret_env_var("DB_NAME", "db-credentials-dev", "DB_NAME")
    job.add_secret_env_var("POLYGON_API_KEY", "api-keys", "polygon-api-key")
    
    # Add command arguments
    job.add_command_arg("--environment")
    job.add_command_arg("dev")
    job.add_command_arg("--gin_config")
    job.add_command_arg("/app/config/app_docker.gin")
    
    # Add labels
    job.add_label("app", "ats-api")
    job.add_label("component", "secmaster")
    job.add_label("environment", "dev")
    
    # Add image pull secret
    job.add_image_pull_secret("registry-credentials")
    
    return job

def main():
    """Generate and save the job YAML."""
    # Create output directory if it doesn't exist
    os.makedirs("k8s/generated", exist_ok=True)
    
    # Create job configuration
    job = create_custom_backfill_job()
    
    # Generate YAML
    job_yaml = job.generate_yaml()
    
    # Save to file
    output_path = "k8s/generated/custom-instrument-polygon-backfill.yaml"
    with open(output_path, "w") as f:
        yaml.dump(job_yaml, f, default_flow_style=False)
    
    print(f"Job YAML saved to {output_path}")
    
    # Apply to cluster if requested
    apply_to_cluster = input("Apply to cluster? (y/n): ").lower() == 'y'
    if apply_to_cluster:
        import subprocess
        result = subprocess.run(["kubectl", "apply", "-f", output_path])
        if result.returncode == 0:
            print("Job successfully applied to cluster")
        else:
            print("Failed to apply job to cluster")

if __name__ == "__main__":
    main()
