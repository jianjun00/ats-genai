#!/usr/bin/env python
"""
Script to convert .env files to Kubernetes secrets.
This script reads environment variables from .env files and generates Kubernetes secret YAML files.
"""

import argparse
import base64
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Default namespaces for different environments
DEFAULT_NAMESPACES = {
    "dev": "ats-dev",
    "intg": "ats-intg",
    "prod": "ats-prod",
}

def parse_env_file(file_path: str) -> Dict[str, str]:
    """Parse a .env file and return a dictionary of key-value pairs."""
    env_vars = {}
    
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist.")
        return env_vars
        
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
                
            # Handle export statements
            if line.startswith('export '):
                line = line[7:]  # Remove 'export ' prefix
                
            # Split by first equals sign
            if '=' in line:
                key, value = line.split('=', 1)
                # Remove quotes if present
                value = value.strip('\'"')
                env_vars[key] = value
                
    return env_vars

def filter_db_credentials(env_vars: Dict[str, str], include_all: bool = False) -> Dict[str, str]:
    """Filter environment variables to only include database credentials."""
    db_prefixes = ['DB_', 'TSDB_']
    
    if include_all:
        return env_vars
        
    return {k: v for k, v in env_vars.items() if any(k.startswith(prefix) for prefix in db_prefixes)}

def encode_for_k8s_secret(value: str) -> str:
    """Base64 encode a string for Kubernetes secrets."""
    return base64.b64encode(value.encode('utf-8')).decode('utf-8')

def generate_k8s_secret_yaml(
    env_vars: Dict[str, str], 
    secret_name: str, 
    namespace: str
) -> str:
    """Generate a Kubernetes secret YAML from environment variables."""
    yaml_lines = [
        "apiVersion: v1",
        "kind: Secret",
        "metadata:",
        f"  name: {secret_name}",
        f"  namespace: {namespace}",
        "type: Opaque",
        "data:"
    ]
    
    # Add each environment variable as a base64-encoded value
    for key, value in sorted(env_vars.items()):
        encoded_value = encode_for_k8s_secret(value)
        yaml_lines.append(f"  {key}: {encoded_value}")
        
    return '\n'.join(yaml_lines)

def write_yaml_file(yaml_content: str, output_path: str) -> None:
    """Write YAML content to a file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w') as f:
        f.write(yaml_content)
        
    print(f"Generated Kubernetes secret YAML: {output_path}")

def main():
    parser = argparse.ArgumentParser(description='Convert .env files to Kubernetes secrets.')
    parser.add_argument('--env-file', required=True, help='Path to the .env file')
    parser.add_argument('--output-dir', default='k8s/secrets', help='Output directory for YAML files')
    parser.add_argument('--secret-name', default='db-credentials', help='Name of the Kubernetes secret')
    parser.add_argument('--namespace', help='Kubernetes namespace')
    parser.add_argument('--include-all', action='store_true', help='Include all environment variables, not just DB credentials')
    
    args = parser.parse_args()
    
    # Extract environment from filename (e.g., .env.dev -> dev)
    env_file = Path(args.env_file)
    filename = env_file.name
    
    # Handle different .env file naming patterns
    if filename.startswith('.env.'):
        env_name = filename[5:]  # Remove '.env.' prefix
    elif filename == '.env':
        env_name = 'default'
    else:
        env_name = env_file.stem
    
    # Determine namespace
    namespace = args.namespace or DEFAULT_NAMESPACES.get(env_name, f"ats-{env_name}")
    
    # Parse .env file
    env_vars = parse_env_file(args.env_file)
    if not env_vars:
        print(f"No environment variables found in {args.env_file}")
        sys.exit(1)
        
    # Filter for database credentials
    filtered_vars = filter_db_credentials(env_vars, args.include_all)
    if not filtered_vars:
        print(f"No database credentials found in {args.env_file}")
        sys.exit(1)
        
    # Generate secret name with environment suffix
    secret_name = f"{args.secret_name}-{env_name}"
    
    # Generate Kubernetes secret YAML
    yaml_content = generate_k8s_secret_yaml(filtered_vars, secret_name, namespace)
    
    # Write to file
    output_path = os.path.join(args.output_dir, f"{secret_name}.yaml")
    write_yaml_file(yaml_content, output_path)
    
    print(f"Successfully created Kubernetes secret for {env_name} environment")
    print(f"Secret name: {secret_name}")
    print(f"Namespace: {namespace}")
    print(f"Variables: {', '.join(sorted(filtered_vars.keys()))}")

if __name__ == "__main__":
    main()
