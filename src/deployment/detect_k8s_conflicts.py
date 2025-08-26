#!/usr/bin/env python3
"""
Kubernetes Resource Conflict Detection Tool
Analyzes YAML files to identify duplicate resource definitions

Usage: python scripts/detect_k8s_conflicts.py k8s/
"""

import os
import sys
import yaml
from collections import defaultdict
from pathlib import Path

def analyze_k8s_directory(directory: str):
    """Analyze Kubernetes YAML files for conflicts"""
    yaml_files = list(Path(directory).glob("**/*.yaml"))
    resources = defaultdict(list)
    parsing_errors = []
    
    print(f"🔍 Analyzing {len(yaml_files)} YAML files in {directory}")
    
    for file_path in yaml_files:
        try:
            with open(file_path, 'r') as f:
                docs = list(yaml.safe_load_all(f))
            
            for doc_index, doc in enumerate(docs):
                if not doc or 'kind' not in doc:
                    continue
                
                kind = doc.get('kind', 'Unknown')
                metadata = doc.get('metadata', {})
                name = metadata.get('name', 'unnamed')
                namespace = metadata.get('namespace', 'default')
                
                resource_key = f"{kind}/{namespace}/{name}"
                resources[resource_key].append({
                    'file': str(file_path),
                    'doc_index': doc_index
                })
        
        except yaml.YAMLError as e:
            parsing_errors.append(f"❌ YAML Error in {file_path}: {e}")
        except Exception as e:
            parsing_errors.append(f"❌ Error processing {file_path}: {e}")
    
    # Report parsing errors
    if parsing_errors:
        print(f"\n🚨 {len(parsing_errors)} PARSING ERRORS FOUND:")
        for error in parsing_errors:
            print(f"  {error}")
    
    # Find conflicts
    conflicts = {r: files for r, files in resources.items() if len(files) > 1}
    
    if conflicts:
        print(f"\n🚨 {len(conflicts)} RESOURCE CONFLICTS FOUND:")
        for resource_key, files in conflicts.items():
            print(f"  ❌ {resource_key}:")
            for file_info in files:
                print(f"    - {file_info['file']} (document {file_info['doc_index']})")
    else:
        print(f"\n✅ NO CONFLICTS FOUND")
    
    # Summary
    total_resources = sum(len(files) for files in resources.values())
    unique_resources = len(resources)
    
    print(f"\n📊 SUMMARY:")
    print(f"  Files analyzed: {len(yaml_files)}")
    print(f"  Total resources: {total_resources}")
    print(f"  Unique resources: {unique_resources}")
    print(f"  Conflicts: {len(conflicts)}")
    print(f"  Parsing errors: {len(parsing_errors)}")
    
    return len(conflicts) == 0 and len(parsing_errors) == 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python detect_k8s_conflicts.py <k8s_directory>")
        sys.exit(1)
    
    directory = sys.argv[1]
    if not os.path.exists(directory):
        print(f"❌ Directory {directory} does not exist")
        sys.exit(1)
    
    success = analyze_k8s_directory(directory)
    sys.exit(0 if success else 1)