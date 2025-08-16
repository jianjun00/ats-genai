#!/usr/bin/env python
"""
Script to update Kubernetes scripts to work with Minikube instead of Docker Desktop.
This script will search for references to Docker Desktop Kubernetes in your scripts
and replace them with Minikube references.
"""

import os
import re
import argparse
import glob
from typing import List, Tuple

def find_k8s_scripts(base_dir: str) -> List[str]:
    """Find all potential Kubernetes-related scripts."""
    script_files = []
    print("Scanning for Kubernetes-related files...")
    
    # Look for Python scripts
    py_count = 0
    for py_file in glob.glob(f"{base_dir}/**/*.py", recursive=True):
        py_count += 1
        try:
            with open(py_file, 'r', encoding='utf-8') as f:
                content = f.read()
                if 'kubectl' in content or 'kubernetes' in content.lower() or 'minikube' in content:
                    print(f"Found Kubernetes reference in: {py_file}")
                    script_files.append(py_file)
        except Exception as e:
            print(f"Error reading {py_file}: {e}")
    
    # Look for shell scripts
    sh_count = 0
    for sh_file in glob.glob(f"{base_dir}/**/*.sh", recursive=True):
        sh_count += 1
        try:
            with open(sh_file, 'r', encoding='utf-8') as f:
                content = f.read()
                if 'kubectl' in content or 'kubernetes' in content.lower() or 'minikube' in content:
                    print(f"Found Kubernetes reference in: {sh_file}")
                    script_files.append(sh_file)
        except Exception as e:
            print(f"Error reading {sh_file}: {e}")
    
    print(f"Scanned {py_count} Python files and {sh_count} shell scripts")
    return script_files

def update_script(file_path: str, dry_run: bool = True) -> Tuple[bool, List[str]]:
    """Update a script to use Minikube instead of Docker Desktop."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    changes = []
    
    # Replace Docker Desktop context with Minikube
    if 'minikube' in content:
        content = content.replace('minikube', 'minikube')
        changes.append("Replaced 'minikube' context with 'minikube'")
    
    # Replace Docker Desktop Kubernetes config paths
    docker_desktop_config_pattern = re.compile(r'(~|/home/[^/]+)/\.docker/desktop/kubernetes-admin-conf\.yml')
    if docker_desktop_config_pattern.search(content):
        content = docker_desktop_config_pattern.sub(r'\1/.kube/config', content)
        changes.append("Updated Kubernetes config path to use ~/.kube/config")
    
    # Update any Docker Desktop specific checks
    if "docker info | grep -q \"Kubernetes.*running\"" in content:
        content = content.replace(
            "docker info | grep -q \"Kubernetes.*running\"", 
            "minikube status | grep -q \"apiserver: Running\""
        )
        changes.append("Updated Kubernetes status check to use minikube status")
    
    # If no changes were made, return False
    if content == original_content:
        return False, []
    
    # If this is not a dry run, write the changes
    if not dry_run:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
    
    return True, changes

def main():
    parser = argparse.ArgumentParser(description="Update Kubernetes scripts to use Minikube")
    parser.add_argument('--base-dir', type=str, default='/home/jianjun/ats-genai',
                        help='Base directory to search for scripts')
    parser.add_argument('--apply', action='store_true',
                        help='Apply changes (without this flag, only shows what would change)')
    
    args = parser.parse_args()
    
    # Find all Kubernetes-related scripts
    print(f"Searching for Kubernetes scripts in {args.base_dir}...")
    scripts = find_k8s_scripts(args.base_dir)
    print(f"Found {len(scripts)} potential Kubernetes-related scripts.")
    
    # Update each script
    updated_count = 0
    for script in scripts:
        was_updated, changes = update_script(script, dry_run=not args.apply)
        if was_updated:
            updated_count += 1
            print(f"\n{script}:")
            for change in changes:
                print(f"  - {change}")
    
    # Summary
    print(f"\nSummary: {updated_count} of {len(scripts)} scripts need updates.")
    if not args.apply and updated_count > 0:
        print("Run with --apply to apply these changes.")

if __name__ == "__main__":
    main()
