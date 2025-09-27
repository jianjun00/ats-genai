#!/usr/bin/env python3
"""
Fix critical broken imports that affect current training data callback work
Following fail-fast principles - remove broken code rather than workarounds
"""

import os
import re
from pathlib import Path

def find_critical_broken_imports():
    """Find the most critical broken imports affecting current work"""
    
    critical_files = [
        'src/domains/ml/services/training_data/callbacks/training_data_callback.py',
        'src/domains/trading/services/state/universe_state_builder.py', 
        'src/domains/trading/services/core/app/runner.py',
        'src/infrastructure/database/connection_manager.py',
        'src/storage/file_based_minute_manager.py'
    ]
    
    print("🎯 CRITICAL IMPORT ANALYSIS")
    print("=" * 80)
    
    for file_path in critical_files:
        if Path(file_path).exists():
            print(f"\n📁 {file_path}")
            check_critical_imports_in_file(file_path)
        else:
            print(f"\n❌ {file_path} - FILE NOT FOUND")

def check_critical_imports_in_file(file_path):
    """Check imports in a specific critical file"""
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    broken_imports = []
    for line_no, line in enumerate(lines, 1):
        line = line.strip()
        if line.startswith('from ') or line.startswith('import '):
            # Check for suspicious import patterns
            if any(pattern in line for pattern in [
                'database_manager', 'DatabaseManager',
                'connection_manager', 'ConnectionManager', 
                'universe_manager', 'UniverseManager',
                'training_dataset_dao', 'TrainingDatasetDAO',
                'monthly_training_data_dao', 'MonthlyTrainingDataDAO'
            ]):
                broken_imports.append((line_no, line))
    
    if broken_imports:
        for line_no, import_line in broken_imports:
            print(f"  ❌ Line {line_no}: {import_line}")
    else:
        print("  ✅ No critical broken imports found")
        
def check_database_manager_references():
    """Find all references to the non-existent DatabaseManager"""
    print("\n🔍 SEARCHING FOR DatabaseManager REFERENCES")
    print("=" * 80)
    
    cmd = 'grep -r "DatabaseManager\\|database_manager" src/ --include="*.py" | head -20'
    os.system(cmd)

def check_connection_manager_references():
    """Find all references to connection_manager imports"""
    print("\n🔍 SEARCHING FOR connection_manager REFERENCES") 
    print("=" * 80)
    
    cmd = 'grep -r "connection_manager" src/ --include="*.py" | head -20'
    os.system(cmd)

def check_dao_references():
    """Find all references to DAO imports that might be broken"""
    print("\n🔍 SEARCHING FOR DAO IMPORT REFERENCES")
    print("=" * 80)
    
    cmd = 'grep -r "from.*dao\\." src/ --include="*.py" | grep -E "(training_dataset_dao|monthly_training_data_dao)" | head -10'
    os.system(cmd)

def main():
    """Run critical import analysis"""
    find_critical_broken_imports()
    check_database_manager_references()
    check_connection_manager_references() 
    check_dao_references()
    
    print("\n📋 NEXT STEPS:")
    print("1. Fix DatabaseManager import errors (highest priority)")
    print("2. Fix connection_manager import paths")
    print("3. Fix DAO import paths for training data")
    print("4. Remove broken legacy imports")
    print("5. Test critical paths after each fix")

if __name__ == "__main__":
    main()