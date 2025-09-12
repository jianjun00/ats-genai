#!/usr/bin/env python3
"""
Test case to reproduce and fix the single-day training data issue.

Issue: When start_date == end_date, training data generation only produces
7 records instead of expected full day coverage.
"""

import asyncio
import os
import tempfile
import shutil
from datetime import datetime, date
from pathlib import Path
import subprocess
import sys

# Add src to path for imports
sys.path.insert(0, 'src')

from array_record.python.array_record_module import ArrayRecordReader

def count_arrayrecord_records(file_path):
    """Count records in ArrayRecord file."""
    try:
        reader = ArrayRecordReader(str(file_path))
        count = 0
        for _ in reader:
            count += 1
        return count
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0

def run_training_data_generation(symbols, start_date, end_date, output_dir, test_name):
    """Run training data generation with specified parameters."""
    print(f"\n🧪 TEST: {test_name}")
    print(f"   Symbols: {symbols}")
    print(f"   Date range: {start_date} to {end_date}")
    print(f"   Output: {output_dir}")
    
    # Build command
    cmd = [
        "python3", 
        "src/domains/ml/services/training_data/runners/training_data_callback_runner.py",
        "--symbols", *symbols,
        "--start-date", start_date,
        "--end-date", end_date,
        "--environment", "dev",
        "--storage-format", "arrayrecord", 
        "--output-dir", output_dir,
        "--debug",
        "--gin-config", "config/training_data.gin",
        "--base-duration", "60m"
    ]
    
    print(f"🔄 Running command:")
    print(f"   {' '.join(cmd)}")
    
    # Set environment variables
    env = os.environ.copy()
    env['PYTHONPATH'] = 'src'
    env['DB_HOST'] = 'localhost'
    env['DB_PORT'] = '3432'
    env['DB_USER'] = 'postgres'
    env['DB_PASSWORD'] = 'dev_password'
    env['DB_NAME'] = 'dev_db'
    env['ENVIRONMENT_TYPE'] = 'dev'
    
    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes timeout
        )
        
        print(f"📤 Exit code: {result.returncode}")
        if result.stdout:
            print(f"📝 STDOUT:\n{result.stdout}")
        if result.stderr:
            print(f"❌ STDERR:\n{result.stderr}")
            
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print("❌ Command timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"❌ Error running command: {e}")
        return False

def analyze_generated_data(output_dir, test_name):
    """Analyze the generated training data files."""
    print(f"\n📊 ANALYSIS: {test_name}")
    
    # Find all ArrayRecord files
    output_path = Path(output_dir)
    arrayrecord_files = list(output_path.rglob("*.arrayrecord"))
    
    print(f"   ArrayRecord files found: {len(arrayrecord_files)}")
    
    total_records = 0
    for file_path in arrayrecord_files:
        record_count = count_arrayrecord_records(file_path)
        relative_path = file_path.relative_to(output_path)
        print(f"   📁 {relative_path}: {record_count} records")
        total_records += record_count
    
    print(f"   🎯 Total records across all files: {total_records}")
    
    return {
        'files_count': len(arrayrecord_files),
        'total_records': total_records,
        'files': [(str(f.relative_to(output_path)), count_arrayrecord_records(f)) for f in arrayrecord_files]
    }

def main():
    """Main test function."""
    print("🧪 TESTING: Single-day training data generation issue")
    print("="*80)
    
    # Create temporary output directories
    base_temp_dir = Path(tempfile.mkdtemp(prefix="training_test_"))
    print(f"📁 Base temp directory: {base_temp_dir}")
    
    test_results = {}
    
    try:
        # TEST 1: Same start/end date (reproduce issue)
        print("\n" + "="*60)
        test1_dir = base_temp_dir / "test1_same_date"
        test1_dir.mkdir(exist_ok=True)
        
        success1 = run_training_data_generation(
            symbols=["TSLA"],
            start_date="2025-07-01", 
            end_date="2025-07-01",  # Same date
            output_dir=str(test1_dir),
            test_name="Same start/end date (reproduce issue)"
        )
        
        if success1:
            test_results['test1'] = analyze_generated_data(str(test1_dir), "Test 1")
        else:
            print("❌ Test 1 failed to run")
            test_results['test1'] = {'files_count': 0, 'total_records': 0, 'files': []}
        
        # TEST 2: Multi-day range for comparison
        print("\n" + "="*60)
        test2_dir = base_temp_dir / "test2_multi_day"
        test2_dir.mkdir(exist_ok=True)
        
        success2 = run_training_data_generation(
            symbols=["TSLA"],
            start_date="2025-07-01",
            end_date="2025-07-03",  # 3 days
            output_dir=str(test2_dir),
            test_name="Multi-day range (comparison)"
        )
        
        if success2:
            test_results['test2'] = analyze_generated_data(str(test2_dir), "Test 2")
        else:
            print("❌ Test 2 failed to run")
            test_results['test2'] = {'files_count': 0, 'total_records': 0, 'files': []}
        
        # ANALYZE RESULTS
        print("\n" + "="*80)
        print("📊 FINAL ANALYSIS")
        print("="*80)
        
        test1_records = test_results['test1']['total_records']
        test2_records = test_results['test2']['total_records']
        
        print(f"🔍 Test 1 (same date): {test1_records} total records")
        print(f"🔍 Test 2 (multi-day): {test2_records} total records")
        
        # Expected calculation
        # For same date: should have ~6-8 hours of market data = 6-8 records per timeframe
        # For 3 days: should have ~18-24 hours = 18-24 records per timeframe
        
        if test1_records == 7:
            print("✅ REPRODUCED: Test 1 shows exactly 7 records (confirms issue)")
        elif test1_records < 10:
            print(f"⚠️ SIMILAR ISSUE: Test 1 shows {test1_records} records (low count)")
        else:
            print(f"❓ UNEXPECTED: Test 1 shows {test1_records} records (higher than expected)")
            
        if test2_records > test1_records * 2:
            print("✅ COMPARISON: Test 2 shows significantly more records (expected for multi-day)")
        else:
            print("⚠️ ISSUE: Test 2 doesn't show proportionally more records")
        
        # Detailed file analysis
        print(f"\n📁 Test 1 detailed breakdown:")
        for file_path, count in test_results['test1']['files']:
            print(f"   {file_path}: {count} records")
            
        print(f"\n📁 Test 2 detailed breakdown:")
        for file_path, count in test_results['test2']['files']:
            print(f"   {file_path}: {count} records")
        
        # Recommendations
        print(f"\n🔧 DEBUGGING RECOMMENDATIONS:")
        if test1_records <= 10:
            print("1. Issue confirmed - single day generates limited records")
            print("2. Check if training_interval_minutes (60m) is causing hourly limitation")
            print("3. Verify market hours filtering in runner configuration")
            print("4. Check if collection window vs target window logic is correct")
        
        return test_results
        
    finally:
        # Cleanup temp directories
        try:
            shutil.rmtree(base_temp_dir)
            print(f"\n🧹 Cleaned up temp directory: {base_temp_dir}")
        except Exception as e:
            print(f"⚠️ Failed to cleanup {base_temp_dir}: {e}")

if __name__ == "__main__":
    results = main()
    
    # Exit with error code if we confirmed the issue
    if results.get('test1', {}).get('total_records', 0) <= 10:
        print("\n❌ ISSUE CONFIRMED: Single-day training data generation produces limited records")
        sys.exit(1)
    else:
        print("\n✅ No issue detected")
        sys.exit(0)