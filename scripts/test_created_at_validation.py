#!/usr/bin/env python3
"""
Test script to validate that all dev tables have created_at columns
"""
import subprocess
import sys

def check_created_at_columns():
    """Check all dev tables have created_at columns"""
    print("🧪 Testing created_at column requirements...")
    
    # Query to find tables missing created_at
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_name LIKE 'dev_%' 
    AND table_schema = 'public' 
    AND table_name NOT IN (
        SELECT table_name 
        FROM information_schema.columns 
        WHERE column_name = 'created_at' 
        AND table_schema = 'public'
    ) 
    ORDER BY table_name
    """
    
    cmd = [
        "python3", "scripts/run_dev.py", "query", 
        "--query", query
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Query failed: {result.stderr}")
        return False
    
    output_lines = result.stdout.strip().split('\n')
    
    # Check if we have any missing tables (more than just headers)
    missing_tables = []
    for line in output_lines:
        line = line.strip()
        if line and not line.startswith('table_name') and not line.startswith('---') and line != '(0 rows)':
            missing_tables.append(line)
    
    if missing_tables:
        print(f"❌ {len(missing_tables)} tables missing created_at columns:")
        for table in missing_tables:
            print(f"  - {table}")
        return False
    
    print("✅ All dev tables have created_at columns!")
    return True

def check_created_at_data_types():
    """Check that created_at columns have correct data type"""
    print("🧪 Testing created_at column data types...")
    
    query = """
    SELECT table_name, column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name LIKE 'dev_%' 
    AND column_name = 'created_at' 
    AND data_type NOT LIKE '%timestamp%'
    ORDER BY table_name
    """
    
    cmd = [
        "python3", "scripts/run_dev.py", "query",
        "--query", query
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Query failed: {result.stderr}")
        return False
    
    output_lines = result.stdout.strip().split('\n')
    
    # Check for incorrect data types
    bad_types = []
    for line in output_lines:
        line = line.strip()
        if (line and not line.startswith('table_name') and 
            not line.startswith('---') and line != '(0 rows)'):
            bad_types.append(line)
    
    if bad_types:
        print(f"⚠️  {len(bad_types)} tables have incorrect created_at data types:")
        for bad_type in bad_types:
            print(f"  - {bad_type}")
        return False
    
    print("✅ All created_at columns have correct timestamp data types!")
    return True

def main():
    """Run all validation tests"""
    print("🔍 Validating created_at column requirements...")
    print("=" * 50)
    
    tests_passed = 0
    total_tests = 2
    
    if check_created_at_columns():
        tests_passed += 1
    
    if check_created_at_data_types():
        tests_passed += 1
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {tests_passed}/{total_tests} passed")
    
    if tests_passed == total_tests:
        print("🎉 ALL CREATED_AT VALIDATION TESTS PASSED!")
        print("✅ Schema is consistent with created_at requirements")
        return 0
    else:
        print("❌ SOME VALIDATION TESTS FAILED!")
        print("❌ Please fix schema issues before deploying")
        return 1

if __name__ == "__main__":
    sys.exit(main())