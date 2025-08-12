#!/usr/bin/env python
"""
Simple script to test database name generation logic.
"""
import os
import hashlib
import uuid

def generate_test_db_name(test_file, test_name):
    """Generate a test database name using the same logic as unit_test_db fixture."""
    # Take only first 8 chars of file name to keep DB name short
    test_file_base = os.path.splitext(os.path.basename(test_file))[0]
    test_file_base = ''.join(c for c in test_file_base if c.isalnum())[:8]
    
    if test_name:
        hash_part = hashlib.sha1(test_name.encode('utf-8')).hexdigest()[:8]
        # Use shorter name format to avoid PostgreSQL's 63-char limit
        db_name = f"test_db_{test_file_base}_{hash_part}"
    else:
        db_name = f"test_db_{test_file_base}_{uuid.uuid4().hex[:8]}"
    
    # Ensure DB name doesn't exceed PostgreSQL's 63-char limit
    if len(db_name) > 63:
        db_name = db_name[:63]
    
    return db_name

# Test with a very long file name and test name
long_file_name = "/home/jianjun/ats-genai/tests/market_data/eod/test_daily_polygon_with_extremely_long_name_that_would_cause_issues.py"
long_test_name = "test_daily_polygon_inserts_prices_with_extremely_long_name_that_would_cause_database_name_issues"

db_name = generate_test_db_name(long_file_name, long_test_name)
print(f"Generated DB name: {db_name}")
print(f"Length: {len(db_name)}")
print(f"Within PostgreSQL limit (63 chars): {len(db_name) <= 63}")

# Test with the actual file name and test name
actual_file_name = "/home/jianjun/ats-genai/tests/market_data/eod/test_daily_polygon.py"
actual_test_name = "test_daily_polygon_inserts_prices"

db_name = generate_test_db_name(actual_file_name, actual_test_name)
print(f"\nGenerated DB name for actual test: {db_name}")
print(f"Length: {len(db_name)}")
print(f"Within PostgreSQL limit (63 chars): {len(db_name) <= 63}")

# Test with the problematic test name that was causing issues
problematic_test_name = "test_daily_polygon_inserts_prices_testreconciledrecorddao_"

db_name = generate_test_db_name(actual_file_name, problematic_test_name)
print(f"\nGenerated DB name for problematic test: {db_name}")
print(f"Length: {len(db_name)}")
print(f"Within PostgreSQL limit (63 chars): {len(db_name) <= 63}")
