#!/usr/bin/env python3
"""
Kubernetes Resource Cleanup Tool
Performs systematic cleanup of duplicate and problematic Kubernetes manifests

Usage: python scripts/k8s_resource_cleanup.py k8s/ [--dry-run]
"""

import os
import sys
import shutil
import yaml
import argparse
from datetime import datetime
from pathlib import Path
from collections import defaultdict

def create_backup(k8s_dir: str) -> str:
    """Create backup of k8s directory"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"../k8s-backup-{timestamp}"
    
    print(f"📁 Creating backup at {backup_dir}")
    shutil.copytree(k8s_dir, backup_dir)
    return backup_dir

def analyze_directory(k8s_dir: str) -> dict:
    """Analyze directory structure and identify cleanup targets"""
    analysis = {
        'total_files': 0,
        'test_files': [],
        'debug_files': [],
        'working_files': [],
        'temp_files': [],
        'old_files': [],
        'conflicts': {},
        'parsing_errors': []
    }
    
    yaml_files = list(Path(k8s_dir).glob("**/*.yaml"))
    analysis['total_files'] = len(yaml_files)
    
    resources = defaultdict(list)
    
    for file_path in yaml_files:
        file_name = file_path.name.lower()
        file_str = str(file_path)
        
        # Categorize files by naming patterns
        if file_name.startswith('test-'):
            analysis['test_files'].append(file_str)
        elif file_name.startswith('debug-'):
            analysis['debug_files'].append(file_str)
        elif file_name.startswith('working-'):
            analysis['working_files'].append(file_str)
        elif any(pattern in file_name for pattern in ['tmp-', 'temp-', 'temporary-']):
            analysis['temp_files'].append(file_str)
        elif any(pattern in file_name for pattern in ['-old.yaml', '-backup.yaml', '-deprecated.yaml']):
            analysis['old_files'].append(file_str)
        
        # Analyze for resource conflicts
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
                    'file': file_str,
                    'doc_index': doc_index,
                    'size': file_path.stat().st_size
                })
        
        except yaml.YAMLError as e:
            analysis['parsing_errors'].append(f"{file_str}: {e}")
        except Exception as e:
            analysis['parsing_errors'].append(f"{file_str}: {e}")
    
    # Identify conflicts (resources defined in multiple files)
    analysis['conflicts'] = {r: files for r, files in resources.items() if len(files) > 1}
    
    return analysis

def recommend_cleanup_actions(analysis: dict) -> dict:
    """Generate cleanup recommendations based on analysis"""
    recommendations = {
        'remove_files': [],
        'resolve_conflicts': {},
        'move_files': []
    }
    
    # Recommend removing test/debug/working files from main directory
    recommendations['remove_files'].extend(analysis['test_files'])
    recommendations['remove_files'].extend(analysis['debug_files'])  
    recommendations['remove_files'].extend(analysis['working_files'])
    recommendations['remove_files'].extend(analysis['temp_files'])
    recommendations['remove_files'].extend(analysis['old_files'])
    
    # Recommend conflict resolution (keep largest/most comprehensive file)
    for resource_key, files in analysis['conflicts'].items():
        if len(files) <= 1:
            continue
            
        # Sort by file size (largest first) and prefer certain naming patterns
        files_sorted = sorted(files, key=lambda x: (
            x['size'],  # Larger files first
            'deployment' in x['file'].lower(),  # Prefer deployment files
            not any(pattern in x['file'].lower() for pattern in ['simple', 'basic', 'minimal'])  # Avoid simple versions
        ), reverse=True)
        
        keep_file = files_sorted[0]['file']
        remove_files = [f['file'] for f in files_sorted[1:]]
        
        recommendations['resolve_conflicts'][resource_key] = {
            'keep': keep_file,
            'remove': remove_files
        }
        recommendations['remove_files'].extend(remove_files)
    
    return recommendations

def execute_cleanup(recommendations: dict, dry_run: bool = True) -> dict:
    """Execute cleanup actions"""
    results = {
        'files_removed': [],
        'conflicts_resolved': 0,
        'errors': []
    }
    
    if dry_run:
        print("🔍 DRY RUN MODE - No files will be actually removed")
    
    # Remove files
    unique_removes = list(set(recommendations['remove_files']))
    
    for file_path in unique_removes:
        try:
            if not dry_run:
                os.remove(file_path)
                print(f"✅ Removed: {file_path}")
            else:
                print(f"🔍 Would remove: {file_path}")
            
            results['files_removed'].append(file_path)
        
        except Exception as e:
            error_msg = f"❌ Error removing {file_path}: {e}"
            print(error_msg)
            results['errors'].append(error_msg)
    
    # Count resolved conflicts
    results['conflicts_resolved'] = len(recommendations['resolve_conflicts'])
    
    return results

def print_analysis_summary(analysis: dict):
    """Print detailed analysis summary"""
    print(f"\n📊 ANALYSIS SUMMARY:")
    print(f"  Total YAML files: {analysis['total_files']}")
    print(f"  Test files: {len(analysis['test_files'])}")
    print(f"  Debug files: {len(analysis['debug_files'])}")
    print(f"  Working files: {len(analysis['working_files'])}")
    print(f"  Temporary files: {len(analysis['temp_files'])}")
    print(f"  Old/deprecated files: {len(analysis['old_files'])}")
    print(f"  Resource conflicts: {len(analysis['conflicts'])}")
    print(f"  Parsing errors: {len(analysis['parsing_errors'])}")
    
    if analysis['conflicts']:
        print(f"\n🚨 RESOURCE CONFLICTS:")
        for resource_key, files in analysis['conflicts'].items():
            print(f"  ❌ {resource_key}:")
            for file_info in files:
                size_kb = file_info['size'] // 1024
                print(f"    - {file_info['file']} ({size_kb}KB)")
    
    if analysis['parsing_errors']:
        print(f"\n🚨 PARSING ERRORS:")
        for error in analysis['parsing_errors']:
            print(f"  ❌ {error}")

def print_cleanup_recommendations(recommendations: dict):
    """Print cleanup recommendations"""
    total_removals = len(set(recommendations['remove_files']))
    conflicts_to_resolve = len(recommendations['resolve_conflicts'])
    
    print(f"\n🧹 CLEANUP RECOMMENDATIONS:")
    print(f"  Files to remove: {total_removals}")
    print(f"  Conflicts to resolve: {conflicts_to_resolve}")
    
    if recommendations['resolve_conflicts']:
        print(f"\n🔧 CONFLICT RESOLUTIONS:")
        for resource_key, resolution in recommendations['resolve_conflicts'].items():
            print(f"  📝 {resource_key}:")
            print(f"    ✅ Keep: {resolution['keep']}")
            for remove_file in resolution['remove']:
                print(f"    ❌ Remove: {remove_file}")

def main():
    parser = argparse.ArgumentParser(description='Kubernetes Resource Cleanup Tool')
    parser.add_argument('k8s_directory', help='Path to k8s directory')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be done without making changes')
    parser.add_argument('--no-backup', action='store_true',
                       help='Skip creating backup (not recommended)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.k8s_directory):
        print(f"❌ Directory {args.k8s_directory} does not exist")
        sys.exit(1)
    
    print(f"🚀 Kubernetes Resource Cleanup Tool")
    print(f"📁 Target directory: {args.k8s_directory}")
    print(f"🔧 Mode: {'DRY RUN' if args.dry_run else 'EXECUTE'}")
    
    # Create backup unless explicitly disabled or in dry-run mode
    if not args.no_backup and not args.dry_run:
        backup_dir = create_backup(args.k8s_directory)
        print(f"✅ Backup created at: {backup_dir}")
    
    # Analyze directory
    print(f"\n🔍 Analyzing directory structure...")
    analysis = analyze_directory(args.k8s_directory)
    print_analysis_summary(analysis)
    
    # Generate recommendations
    print(f"\n💡 Generating cleanup recommendations...")
    recommendations = recommend_cleanup_actions(analysis)
    print_cleanup_recommendations(recommendations)
    
    # Execute cleanup
    if not args.dry_run:
        print(f"\n🧹 Executing cleanup...")
        user_input = input("Continue with cleanup? (y/N): ")
        if user_input.lower() != 'y':
            print("🛑 Cleanup cancelled by user")
            sys.exit(0)
    
    results = execute_cleanup(recommendations, dry_run=args.dry_run)
    
    # Final summary
    print(f"\n✅ CLEANUP COMPLETED:")
    print(f"  Files removed: {len(results['files_removed'])}")
    print(f"  Conflicts resolved: {results['conflicts_resolved']}")
    print(f"  Errors: {len(results['errors'])}")
    
    if results['errors']:
        print(f"\n⚠️  ERRORS ENCOUNTERED:")
        for error in results['errors']:
            print(f"  {error}")
    
    # Recommend verification
    if not args.dry_run and len(results['files_removed']) > 0:
        print(f"\n🔍 RECOMMENDED NEXT STEPS:")
        print(f"  1. Verify no conflicts remain: python scripts/detect_k8s_conflicts.py {args.k8s_directory}")
        print(f"  2. Test YAML validity: kubectl apply --dry-run=client -f {args.k8s_directory}/")
        print(f"  3. Test ArgoCD sync if applicable")

if __name__ == "__main__":
    main()