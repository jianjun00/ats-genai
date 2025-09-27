#!/usr/bin/env python3
"""
Debug test to understand what actual file keys are being generated that cause the ValueError.

This will help identify file key patterns we haven't accounted for in the parsing logic.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock

import sys
sys.path.insert(0, 'src')

from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback


def test_debug_actual_file_key_patterns():
    """
    Debug test to understand what file keys might be causing int('5m') errors.
    
    Based on the error at line 575: year = int(parts[3]) where parts[3] = '5m'
    This means we're in the 'new format' branch (len(parts) >= 5) but the format
    is still not what we expect.
    """
    print("🔍 Debugging actual file key patterns that cause ValueError")
    
    # These are potential file key patterns that might exist
    possible_problematic_patterns = [
        "AAPL_5m_basic_2025_07",      # symbol_timeframe_featuregroup_YYYY_MM
        "AAPL_5m_2025_07_basic",      # symbol_timeframe_YYYY_MM_featuregroup  
        "AAPL_TSLA_5m_2025_07",       # symbol_symbol_timeframe_YYYY_MM (multi-symbol?)
        "AAPL_basic_5m_advanced_2025_07", # symbol_featuregroup_timeframe_featuregroup_YYYY_MM
        "dataset_AAPL_5m_2025_07",    # dataset_symbol_timeframe_YYYY_MM
        "AAPL_basic_5m_2025_07_v1",  # symbol_featuregroup_timeframe_YYYY_MM_version
    ]
    
    print("📊 Testing various file key patterns:")
    
    for file_key in possible_problematic_patterns:
        parts = file_key.split('_')
        print(f"\n   File key: {file_key}")
        print(f"   Parts ({len(parts)}): {parts}")
        
        # Simulate the current parsing logic
        if len(parts) < 5:
            print("     → Would use LEGACY format parsing")
            if len(parts) >= 4:
                symbol = parts[0]
                timeframe = parts[1]
                year = int(parts[2])
                month = int(parts[3])
                print(f"     ✅ Legacy: symbol={symbol}, timeframe={timeframe}, year={year}, month={month}")
                print("     ⚠️  Would be skipped (insufficient parts)")
        else:
            print("     → Would use NEW format parsing")
            symbol = parts[0]
            feature_group = parts[1]
            timeframe = parts[2]
            year = int(parts[3])  # This is line 575 where error occurs
            month = int(parts[4])
            print(f"     ✅ New: symbol={symbol}, feature_group={feature_group}, timeframe={timeframe}, year={year}, month={month}")
def test_reproduce_exact_error_condition():
    """
    Try to reproduce the exact error condition from the traceback.
    
    Error: int(parts[3]) where parts[3] = '5m' at line 575
    Line 575 is in the "new format" branch, so len(parts) >= 5
    """
    print("\n🔍 Reproducing exact error condition")
    
    # Find a file key pattern where:
    # - len(parts) >= 5 (so we use new format)
    # - parts[3] = '5m' (causing the error)
    
    candidate_keys = [
        "AAPL_basic_advanced_5m_2025_07",  # 6 parts, parts[3] = '5m'
        "AAPL_dataset_basic_5m_2025_07",   # 6 parts, parts[3] = '5m'
        "AAPL_v1_basic_5m_2025_07",        # 6 parts, parts[3] = '5m'
    ]
    
    for file_key in candidate_keys:
        parts = file_key.split('_')
        print(f"\n   Testing: {file_key}")
        print(f"   Parts: {parts}")
        print(f"   len(parts): {len(parts)}")
        
        if len(parts) >= 5:
            print(f"   parts[3]: '{parts[3]}' (expected to be year)")
            
            if parts[3] == '5m':
                print(f"   🎯 FOUND IT! This would cause: ValueError: invalid literal for int() with base 10: '5m'")
                print(f"   💡 This suggests the file key format is more complex than expected")

def test_analyze_generation_vs_parsing_mismatch():
    """
    Analyze potential mismatches between file key generation and parsing.
    """
    print("\n🔍 Analyzing generation vs parsing format mismatches")
    
    print("📋 Expected formats:")
    print("   Legacy: symbol_timeframe_YYYY_MM (4 parts)")
    print("   New:    symbol_featuregroup_timeframe_YYYY_MM (5 parts)")
    
    print("\n🚨 Problematic formats that would cause the error:")
    print("   Format: symbol_X_Y_timeframe_YYYY_MM (6+ parts)")
    print("   Where X and Y are additional components")
    print("   Result: parts[3] = timeframe (e.g., '5m') instead of year")
    
    print("\n💡 Potential root causes:")
    print("   1. File key generation changed to include more components")
    print("   2. Multiple feature groups or versions in file keys")
    print("   3. Dataset prefixes or additional metadata in keys")
    print("   4. Legacy vs new format detection is incorrect")


if __name__ == "__main__":
    """
    Run debug analysis to understand the file key patterns causing ValueError.
    """
    print("🔍 DEBUGGING FILE KEY PARSING VALUEERROR")
    print("=" * 60)
    print("Goal: Understand actual file key patterns causing int('5m') error")
    print("Error location: line 575, parts[3] = '5m' in new format branch")
    print("=" * 60)
    
    test_debug_actual_file_key_patterns()
    test_reproduce_exact_error_condition()
    test_analyze_generation_vs_parsing_mismatch()
    
    print("\n📋 RECOMMENDATIONS:")
    print("1. Add logging to capture actual file keys in production")
    print("2. Update parsing logic to handle variable-length formats")
    print("3. Consider using a more robust parsing strategy (regex or structured approach)")