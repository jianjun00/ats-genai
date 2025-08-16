#!/usr/bin/env python
"""
Kubernetes Job Generator for ATS-GenAI

This script generates Kubernetes job YAML files for instrument-polygon operations.
It uses a parameterized approach to create different job configurations from a single template.
"""

import os
import argparse
import yaml
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass, field


@dataclass
class JobConfig:
    """Configuration for a Kubernetes job."""
    name: str
    namespace: str = "ats-dev"
    image: str = "dragonflyer762/ats-genai:dev-latest"
    command: List[str] = field(default_factory=lambda: ["python", "-m", "src.secmaster.populate_instrument_polygon"])
    args: List[str] = field(default_factory=list)
    env_vars: Dict[str, Union[str, Dict[str, str]]] = field(default_factory=dict)
    memory_request: str = "256Mi"
    memory_limit: str = "512Mi"
    cpu_request: str = "100m"
    cpu_limit: str = "250m"
    restart_policy: str = "Never"
    backoff_limit: int = 2
    ttl_seconds_after_finished: int = 86400
    active_deadline_seconds: Optional[int] = None
    labels: Dict[str, str] = field(default_factory=dict)
    image_pull_secrets: List[Dict[str, str]] = field(default_factory=list)
    
    def add_env_var(self, name: str, value: str) -> None:
        """Add a simple environment variable."""
        self.env_vars[name] = value
    
    def add_secret_env_var(self, name: str, secret_name: str, secret_key: str) -> None:
        """Add an environment variable from a Kubernetes secret."""
        self.env_vars[name] = {
            "valueFrom": {
                "secretKeyRef": {
                    "name": secret_name,
                    "key": secret_key
                }
            }
        }
    
    def add_command_arg(self, arg: str) -> None:
        """Add a command line argument."""
        self.args.append(arg)
    
    def add_image_pull_secret(self, name: str) -> None:
        """Add an image pull secret."""
        self.image_pull_secrets.append({"name": name})
    
    def add_label(self, key: str, value: str) -> None:
        """Add a label to the job."""
        self.labels[key] = value
    
    def generate_yaml(self) -> Dict[str, Any]:
        """Generate a Kubernetes job YAML configuration."""
        # Convert environment variables to Kubernetes format
        env = []
        for name, value in self.env_vars.items():
            if isinstance(value, dict):
                env.append({"name": name, **value})
            else:
                env.append({"name": name, "value": value})
        
        job = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": self.name,
                "namespace": self.namespace
            },
            "spec": {
                "backoffLimit": self.backoff_limit,
                "ttlSecondsAfterFinished": self.ttl_seconds_after_finished,
                "template": {
                    "spec": {
                        "containers": [{
                            "name": self.name,
                            "image": self.image,
                            "command": self.command,
                            "env": env,
                            "resources": {
                                "requests": {
                                    "memory": self.memory_request,
                                    "cpu": self.cpu_request
                                },
                                "limits": {
                                    "memory": self.memory_limit,
                                    "cpu": self.cpu_limit
                                }
                            }
                        }],
                        "restartPolicy": self.restart_policy
                    }
                }
            }
        }
        
        # Add command arguments if specified
        if self.args:
            job["spec"]["template"]["spec"]["containers"][0]["args"] = self.args
        
        # Add labels if specified
        if self.labels:
            job["metadata"]["labels"] = self.labels
        
        # Add image pull secrets if specified
        if self.image_pull_secrets:
            job["spec"]["template"]["spec"]["imagePullSecrets"] = self.image_pull_secrets
        
        # Add active deadline seconds if specified
        if self.active_deadline_seconds:
            job["spec"]["activeDeadlineSeconds"] = self.active_deadline_seconds
        
        return job


def create_backfill_job() -> JobConfig:
    """Create a configuration for the instrument-polygon-backfill job."""
    job = JobConfig(
        name="instrument-polygon-backfill",
        memory_request="512Mi",
        memory_limit="1Gi",
        cpu_request="200m",
        cpu_limit="500m",
        active_deadline_seconds=7200,  # 2 hours timeout
    )
    
    # Add environment variables
    job.add_env_var("PYTHONPATH", "/app/src")
    job.add_env_var("LOG_LEVEL", "INFO")
    job.add_env_var("ENVIRONMENT", "dev")
    
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
    
    # Add image pull secrets
    job.add_image_pull_secret("registry-credentials")
    
    return job


def create_test_job(tickers: str = "NFLX,GOOG,AVGO,ADBE,COST") -> JobConfig:
    """Create a configuration for the test-populate-instrument-polygon job."""
    job = JobConfig(
        name="test-populate-instrument-polygon-multi",
        memory_request="256Mi",
        memory_limit="512Mi",
        cpu_request="100m",
        cpu_limit="250m",
        restart_policy="OnFailure"
    )
    
    # Add environment variables
    job.add_env_var("PYTHONPATH", "/app/src")
    job.add_env_var("DB_HOST", "timescaledb.ats-dev.svc.cluster.local")
    job.add_env_var("DB_PORT", "5432")
    job.add_env_var("DB_USER", "postgres")
    job.add_env_var("DB_NAME", "dev_db")
    
    # Add secrets
    job.add_secret_env_var("DB_PASSWORD", "db-credentials", "DB_PASSWORD")
    job.add_secret_env_var("POLYGON_API_KEY", "api-keys", "polygon-api-key")
    
    # Add command arguments
    job.add_command_arg("--ticker")
    job.add_command_arg(tickers)
    job.add_command_arg("--environment")
    job.add_command_arg("dev")
    job.add_command_arg("--gin_config")
    job.add_command_arg("config/app_docker.gin")
    job.add_command_arg("--debug")
    
    return job


def save_yaml(config: JobConfig, output_path: str) -> None:
    """Save the job configuration as a YAML file."""
    yaml_content = yaml.dump(config.generate_yaml(), default_flow_style=False)
    
    with open(output_path, 'w') as f:
        f.write(yaml_content)
    
    print(f"Job YAML saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate Kubernetes job YAML files")
    parser.add_argument('--job-type', choices=['backfill', 'test'], required=True, 
                        help='Type of job to generate')
    parser.add_argument('--output', type=str, required=True, 
                        help='Output YAML file path')
    parser.add_argument('--tickers', type=str, default="NFLX,GOOG,AVGO,ADBE,COST",
                        help='Comma-separated list of tickers (for test job only)')
    
    args = parser.parse_args()
    
    if args.job_type == 'backfill':
        job_config = create_backfill_job()
    else:  # test
        job_config = create_test_job(args.tickers)
    
    save_yaml(job_config, args.output)


if __name__ == "__main__":
    main()
