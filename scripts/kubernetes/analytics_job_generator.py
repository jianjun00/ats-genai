#!/usr/bin/env python3
"""
Analytics Job Generator for ATS-Dev Kubernetes

Generates Kubernetes jobs to run analytics and backtests in the ats-dev environment
with access to real database and market data.
"""

import os
import argparse
import yaml
from datetime import datetime, date
from typing import Dict, List, Optional

def create_analytics_api_job(job_name: str = None) -> Dict:
    """Create Kubernetes job to run analytics API server"""
    
    if not job_name:
        job_name = f"analytics-api-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": "ats-dev",
            "labels": {
                "app": "ats-analytics",
                "component": "api-server",
                "environment": "dev"
            }
        },
        "spec": {
            "ttlSecondsAfterFinished": 3600,  # 1 hour
            "template": {
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [{
                        "name": "analytics-api",
                        "image": "dragonflyer762/ats-genai:dev-latest",
                        "command": ["python", "-m", "uvicorn"],
                        "args": [
                            "src.api.backtest_analytics_api:app",
                            "--host", "0.0.0.0",
                            "--port", "8000",
                            "--log-level", "info"
                        ],
                        "env": [
                            {"name": "PYTHONPATH", "value": "src"},
                            {"name": "ENVIRONMENT", "value": "dev"},
                            {"name": "DB_CONNECTION_PARAMS", "value": "sslmode=disable"},
                            {
                                "name": "DB_HOST",
                                "value": "postgres"
                            },
                            {
                                "name": "DB_PORT", 
                                "value": "5432"
                            },
                            {
                                "name": "DB_USER",
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "name": "ats-dev-secrets", 
                                        "key": "db_user"
                                    }
                                }
                            },
                            {
                                "name": "DB_PASSWORD",
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "name": "db-credentials-dev",
                                        "key": "DB_PASSWORD"
                                    }
                                }
                            },
                            {
                                "name": "DB_NAME",
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "name": "db-credentials-dev",
                                        "key": "DB_NAME"
                                    }
                                }
                            }
                        ],
                        "ports": [{
                            "containerPort": 8000,
                            "name": "http"
                        }],
                        "resources": {
                            "requests": {
                                "memory": "512Mi",
                                "cpu": "250m"
                            },
                            "limits": {
                                "memory": "1Gi", 
                                "cpu": "500m"
                            }
                        },
                        "livenessProbe": {
                            "httpGet": {
                                "path": "/health",
                                "port": 8000
                            },
                            "initialDelaySeconds": 30,
                            "periodSeconds": 10
                        },
                        "readinessProbe": {
                            "httpGet": {
                                "path": "/health",
                                "port": 8000
                            },
                            "initialDelaySeconds": 5,
                            "periodSeconds": 5
                        }
                    }]
                }
            }
        }
    }

def create_production_backtest_job(
    start_date: str,
    end_date: str, 
    universe: str = "sp500_liquid",
    capital: float = 1000000.0,
    job_name: str = None
) -> Dict:
    """Create Kubernetes job to run production backtest"""
    
    if not job_name:
        job_name = f"backtest-{start_date}-{end_date}-{datetime.now().strftime('%H%M%S')}"
    
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": "ats-dev",
            "labels": {
                "app": "ats-analytics",
                "component": "backtest-runner",
                "environment": "dev"
            }
        },
        "spec": {
            "ttlSecondsAfterFinished": 7200,  # 2 hours
            "activeDeadlineSeconds": 3600,   # 1 hour timeout
            "backoffLimit": 1,
            "template": {
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [{
                        "name": "backtest-runner",
                        "image": "dragonflyer762/ats-genai:dev-latest",
                        "command": ["python"],
                        "args": [
                            "scripts/analytics/production_backtest_runner.py",
                            "--start-date", start_date,
                            "--end-date", end_date,
                            "--universe", universe,
                            "--capital", str(capital)
                        ],
                        "env": [
                            {"name": "PYTHONPATH", "value": "src"},
                            {"name": "ENVIRONMENT", "value": "dev"},
                            {"name": "DB_CONNECTION_PARAMS", "value": "sslmode=disable"},
                            {"name": "RAY_SCHEDULER_EVENTS", "value": "0"},
                            {"name": "RAY_DISABLE_AUTOMATIC_AUTOSCALING", "value": "1"},
                            {
                                "name": "DB_HOST",
                                "value": "postgres"
                            },
                            {
                                "name": "DB_PORT",
                                "value": "5432"
                            },
                            {
                                "name": "DB_USER",
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "name": "db-credentials-dev",
                                        "key": "DB_USER"
                                    }
                                }
                            },
                            {
                                "name": "DB_PASSWORD",
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "name": "db-credentials-dev",
                                        "key": "DB_PASSWORD"
                                    }
                                }
                            },
                            {
                                "name": "DB_NAME",
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "name": "db-credentials-dev",
                                        "key": "DB_NAME"
                                    }
                                }
                            },
                            {
                                "name": "POLYGON_API_KEY", 
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "name": "api-keys",
                                        "key": "polygon-api-key"
                                    }
                                }
                            },
                            {
                                "name": "TIINGO_API_KEY",
                                "value": ""
                            }
                        ],
                        "resources": {
                            "requests": {
                                "memory": "2Gi",
                                "cpu": "500m"
                            },
                            "limits": {
                                "memory": "4Gi",
                                "cpu": "1000m"
                            }
                        }
                    }]
                }
            }
        }
    }

def create_analytics_service() -> Dict:
    """Create Kubernetes service to expose analytics API"""
    
    return {
        "apiVersion": "v1",
        "kind": "Service", 
        "metadata": {
            "name": "analytics-api-service",
            "namespace": "ats-dev",
            "labels": {
                "app": "ats-analytics",
                "component": "api-service"
            }
        },
        "spec": {
            "type": "LoadBalancer",
            "ports": [{
                "port": 8000,
                "targetPort": 8000,
                "protocol": "TCP",
                "name": "http"
            }],
            "selector": {
                "app": "ats-analytics",
                "component": "api-server"
            }
        }
    }

def main():
    parser = argparse.ArgumentParser(description="Generate analytics jobs for ats-dev Kubernetes")
    parser.add_argument("--job-type", choices=["api", "backtest", "service"], required=True,
                       help="Type of job to generate")
    parser.add_argument("--start-date", help="Backtest start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Backtest end date (YYYY-MM-DD)")
    parser.add_argument("--universe", default="sp500_liquid", help="Trading universe")
    parser.add_argument("--capital", type=float, default=1000000.0, help="Initial capital")
    parser.add_argument("--output", help="Output file path")
    parser.add_argument("--apply", action="store_true", help="Apply directly to cluster")
    
    args = parser.parse_args()
    
    if args.job_type == "api":
        job_config = create_analytics_api_job()
    elif args.job_type == "backtest":
        if not args.start_date or not args.end_date:
            parser.error("--start-date and --end-date required for backtest jobs")
        job_config = create_production_backtest_job(
            args.start_date, args.end_date, args.universe, args.capital
        )
    elif args.job_type == "service":
        job_config = create_analytics_service()
    
    # Generate YAML
    yaml_content = yaml.dump(job_config, default_flow_style=False)
    
    if args.output:
        with open(args.output, 'w') as f:
            f.write(yaml_content)
        print(f"✅ Generated {args.job_type} configuration: {args.output}")
    else:
        print(yaml_content)
    
    if args.apply:
        import subprocess
        import tempfile
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_file = f.name
        
        try:
            subprocess.run(["kubectl", "apply", "-f", temp_file], check=True)
            print(f"✅ Applied {args.job_type} to ats-dev cluster")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to apply configuration: {e}")
        finally:
            os.unlink(temp_file)

if __name__ == "__main__":
    main()