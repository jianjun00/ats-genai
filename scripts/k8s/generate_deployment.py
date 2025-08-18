#!/usr/bin/env python3
"""
ATS Kubernetes Deployment Generator

Generates consistent deployments using centralized credential management.
Prevents credential-related issues by using standardized templates.

Usage:
    python scripts/k8s/generate_deployment.py --app analytics-api --env dev
    python scripts/k8s/generate_deployment.py --app backtest-webapp --env intg
"""

import argparse
import os
import sys
from pathlib import Path
import yaml

# Predefined deployment configurations
DEPLOYMENT_CONFIGS = {
    "analytics-api": {
        "container_name": "analytics-api",
        "image": "dragonflyer762/ats-genai:dev-latest",
        "command": ["python", "-m", "uvicorn"],
        "args": ["main:app", "--host", "0.0.0.0", "--port", "8000"],
        "component": "api-server",
        "port": 8000,
        "health_path": "/health",
        "service_type": "ClusterIP",
        "node_port": None
    },
    
    "backtest-webapp": {
        "container_name": "webapp",
        "image": "dragonflyer762/ats-genai:dev-latest", 
        "command": ["python"],
        "args": ["/app/webapp/simple_backtest_webapp.py"],
        "component": "webapp",
        "port": 8000,
        "health_path": "/health",
        "service_type": "NodePort",
        "node_port": {"dev": 30802, "intg": 30812, "prod": 30822}
    },
    
    "data-agent": {
        "container_name": "data-agent",
        "image": "dragonflyer762/ats-genai:dev-latest",
        "command": ["python", "-m"],
        "args": ["market_data.agent.data_agent_main"],
        "component": "data-agent", 
        "port": 8080,
        "health_path": "/health",
        "service_type": "ClusterIP",
        "node_port": None
    },
    
    "secmaster-job": {
        "container_name": "secmaster",
        "image": "dragonflyer762/ats-genai:dev-latest",
        "command": ["python"],
        "args": ["src/secmaster/populate_instrument_polygon.py", "--environment", "${ENVIRONMENT}"],
        "component": "job",
        "port": None,
        "health_path": None,
        "service_type": None,
        "node_port": None
    }
}

# Environment configurations
ENV_CONFIGS = {
    "dev": {
        "namespace": "ats-dev",
        "replicas": 1,
        "memory_request": "1Gi",
        "memory_limit": "2Gi", 
        "cpu_request": "500m",
        "cpu_limit": "1000m"
    },
    
    "intg": {
        "namespace": "ats-intg",
        "replicas": 2,
        "memory_request": "1Gi",
        "memory_limit": "3Gi",
        "cpu_request": "750m", 
        "cpu_limit": "1500m"
    },
    
    "prod": {
        "namespace": "ats-prod",
        "replicas": 3,
        "memory_request": "2Gi",
        "memory_limit": "4Gi",
        "cpu_request": "1000m",
        "cpu_limit": "2000m"
    }
}

def generate_deployment(app_name: str, environment: str, output_file: str = None) -> str:
    """Generate deployment YAML with standardized credentials"""
    
    if app_name not in DEPLOYMENT_CONFIGS:
        raise ValueError(f"Unknown app: {app_name}. Available: {list(DEPLOYMENT_CONFIGS.keys())}")
    
    if environment not in ENV_CONFIGS:
        raise ValueError(f"Unknown environment: {environment}. Available: {list(ENV_CONFIGS.keys())}")
    
    app_config = DEPLOYMENT_CONFIGS[app_name]
    env_config = ENV_CONFIGS[environment]
    
    # Read template
    template_path = Path(__file__).parent.parent.parent / "k8s" / "templates" / "deployment-template.yaml"
    template_content = template_path.read_text()
    
    # Prepare substitution variables
    substitutions = {
        "APP_NAME": app_name,
        "NAMESPACE": env_config["namespace"],
        "ENVIRONMENT": environment,
        "COMPONENT": app_config["component"],
        "CONTAINER_NAME": app_config["container_name"],
        "IMAGE": app_config["image"],
        "COMMAND": str(app_config["command"]),
        "ARGS": str(app_config["args"]),
        "REPLICAS": str(env_config["replicas"]),
        "PORT": str(app_config["port"]) if app_config["port"] else "8000",
        "HEALTH_PATH": app_config["health_path"] or "/health",
        "SERVICE_NAME": f"{app_name}-service",
        "SERVICE_TYPE": app_config["service_type"] or "ClusterIP",
        "MEMORY_REQUEST": env_config["memory_request"],
        "MEMORY_LIMIT": env_config["memory_limit"],
        "CPU_REQUEST": env_config["cpu_request"],
        "CPU_LIMIT": env_config["cpu_limit"]
    }
    
    # Handle NodePort
    if app_config["node_port"] and isinstance(app_config["node_port"], dict):
        substitutions["NODE_PORT"] = str(app_config["node_port"].get(environment, ""))
    elif app_config["node_port"]:
        substitutions["NODE_PORT"] = str(app_config["node_port"])
    else:
        substitutions["NODE_PORT"] = ""
    
    # Perform substitutions
    result = template_content
    for key, value in substitutions.items():
        result = result.replace(f"${{{key}}}", value)
        result = result.replace(f"${{{key}:-", f"${{{key[:-2]}}}")  # Handle defaults
    
    # Clean up any remaining template variables
    import re
    result = re.sub(r'\$\{[^}]+\}', '', result)
    
    # Remove nodePort line if not needed
    if not substitutions["NODE_PORT"]:
        result = re.sub(r'^\s*nodePort:.*\n', '', result, flags=re.MULTILINE)
    
    # Remove service section if not needed
    if not app_config["service_type"]:
        result = re.sub(r'^---\napiVersion: v1\nkind: Service.*$', '', result, flags=re.MULTILINE | re.DOTALL)
    
    # Write output
    if output_file:
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(result)
        print(f"✅ Generated deployment: {output_path}")
    
    return result

def validate_credentials(environment: str) -> bool:
    """Validate that required credentials exist in Kubernetes"""
    
    import subprocess
    
    namespace = ENV_CONFIGS[environment]["namespace"]
    
    # Check database credentials
    try:
        result = subprocess.run([
            "kubectl", "get", "secret", f"db-credentials-{environment}",
            "-n", namespace, "--no-headers"
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Missing database credentials: db-credentials-{environment} in {namespace}")
            return False
            
        print(f"✅ Database credentials found: db-credentials-{environment}")
        
    except Exception as e:
        print(f"❌ Error checking credentials: {e}")
        return False
    
    return True

def main():
    parser = argparse.ArgumentParser(description="Generate ATS Kubernetes deployments with standardized credentials")
    parser.add_argument("--app", required=True, choices=list(DEPLOYMENT_CONFIGS.keys()),
                       help="Application to deploy")
    parser.add_argument("--env", required=True, choices=list(ENV_CONFIGS.keys()),
                       help="Target environment")
    parser.add_argument("--output", help="Output file path")
    parser.add_argument("--validate", action="store_true", help="Validate credentials exist")
    parser.add_argument("--apply", action="store_true", help="Apply to Kubernetes directly")
    parser.add_argument("--dry-run", action="store_true", help="Show generated YAML without writing")
    
    args = parser.parse_args()
    
    try:
        # Validate credentials if requested
        if args.validate:
            if not validate_credentials(args.env):
                print("❌ Credential validation failed")
                sys.exit(1)
        
        # Generate deployment
        output_file = args.output or f"k8s/generated/{args.app}-{args.env}.yaml"
        
        if args.dry_run:
            yaml_content = generate_deployment(args.app, args.env)
            print("Generated YAML:")
            print("=" * 80)
            print(yaml_content)
        else:
            yaml_content = generate_deployment(args.app, args.env, output_file)
            
            if args.apply:
                import subprocess
                result = subprocess.run(["kubectl", "apply", "-f", output_file], 
                                      capture_output=True, text=True)
                if result.returncode == 0:
                    print(f"✅ Applied deployment to Kubernetes")
                    print(result.stdout)
                else:
                    print(f"❌ Failed to apply deployment:")
                    print(result.stderr)
                    sys.exit(1)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()