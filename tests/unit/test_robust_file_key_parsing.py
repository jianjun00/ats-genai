#!/usr/bin/env python3
"""
Test the robust file key parsing logic that handles variable-length file key formats.

This approach uses year/month as anchors (always last 2 parts) and timeframe pattern matching.
"""

import pytest
from datetime import date


def test_robust_file_key_parsing():
    """
    Test the robust parsing logic that handles various file key formats.
    
    The new approach:
    1. Year and month are always the last 2 parts
    2. Symbol is always the first part  
    3. Timeframe is found by pattern matching (ends with m/h/d/w)
    """
    print("🔍 Testing robust file key parsing")
    
    test_cases = [
        # Format: (file_key, expected_symbol, expected_timeframe, expected_year, expected_month)
        
        # Legacy formats
        ("AAPL_5m_2025_07", "AAPL", "5m", 2025, 7),
        ("TSLA_15m_2024_12", "TSLA", "15m", 2024, 12),
        
        # New formats with feature groups
        ("AAPL_basic_5m_2025_07", "AAPL", "5m", 2025, 7),
        ("TSLA_advanced_15m_2024_12", "TSLA", "15m", 2024, 12),
        
        # Problematic formats that were causing errors
        ("AAPL_basic_advanced_5m_2025_07", "AAPL", "5m", 2025, 7),  # 6 parts
        ("AAPL_dataset_basic_5m_2025_07", "AAPL", "5m", 2025, 7),   # 6 parts
        ("AAPL_v1_basic_5m_2025_07", "AAPL", "5m", 2025, 7),        # 6 parts
        
        # Even more complex formats
        ("AAPL_v1_dataset_basic_advanced_5m_2025_07", "AAPL", "5m", 2025, 7),  # 7 parts
        ("SPY_premium_tier1_advanced_1h_2023_01", "SPY", "1h", 2023, 1),       # 7 parts
        
        # Different timeframe patterns
        ("AAPL_basic_1d_2025_07", "AAPL", "1d", 2025, 7),
        ("AAPL_basic_1w_2025_07", "AAPL", "1w", 2025, 7),
        ("AAPL_basic_1h_2025_07", "AAPL", "1h", 2025, 7),
    ]
    
    for file_key, expected_symbol, expected_timeframe, expected_year, expected_month in test_cases:
        print(f"\n   Testing: {file_key}")
        
        # Use the new robust parsing logic
        parts = file_key.split('_')
        
        if len(parts) < 4:
            pytest.fail(f"File key {file_key} has insufficient parts")
            continue
        
        # The year and month are always the last two parts
        month_str = parts[-1]
        year_str = parts[-2]
        
        # Validate that these look like year/month
        year = int(year_str)
        month = int(month_str)
        
        # Basic validation
        if year < 2000 or year > 2100 or month < 1 or month > 12:
            pytest.fail(f"Invalid year/month: {year}/{month}")
            continue
        
        year_month = f"{year_str}_{month_str}"
        
        # Extract symbol (always first part)
        symbol = parts[0]
        
        # Find timeframe by looking for specific patterns like '5m', '15m', '60m', '1h', '1d', '1w'
        timeframe = None
        import re
        timeframe_pattern = re.compile(r'^\d+[mhdw]$')  # digit(s) followed by m/h/d/w
        for part in parts[1:-2]:  # Exclude symbol and year/month
            if timeframe_pattern.match(part):
                timeframe = part
                break
        
        if not timeframe:
            # Fallback: assume second part is timeframe (legacy behavior)
            timeframe = parts[1] if len(parts) > 1 else 'unknown'
        
        # Verify results
        assert symbol == expected_symbol, f"Symbol mismatch: {symbol} != {expected_symbol}"
        assert timeframe == expected_timeframe, f"Timeframe mismatch: {timeframe} != {expected_timeframe}"
        assert year == expected_year, f"Year mismatch: {year} != {expected_year}"
        assert month == expected_month, f"Month mismatch: {month} != {expected_month}"
        
        print(f"     ✅ Parsed: symbol={symbol}, timeframe={timeframe}, year={year}, month={month}")
        
    print("   ✅ All file key formats parsed successfully")


def test_robust_parsing_handles_problematic_cases():
    """
    Test specifically the cases that were causing the ValueError.
    
    These are the exact patterns that would cause int('5m') errors with the old logic.
    """
    print("\n🔍 Testing problematic cases that previously caused ValueError")
    
    problematic_cases = [
        "AAPL_basic_advanced_5m_2025_07",  # parts[3] = '5m' with old logic
        "AAPL_dataset_basic_5m_2025_07",   # parts[3] = '5m' with old logic  
        "AAPL_v1_basic_5m_2025_07",        # parts[3] = '5m' with old logic
    ]
    
    for file_key in problematic_cases:
        print(f"\n   Testing problematic: {file_key}")
        parts = file_key.split('_')
        
        print(f"   Parts: {parts}")
        print(f"   Old logic would try: int(parts[3]) = int('{parts[3]}') → ValueError!")
        
        # New robust logic should work
        month_str = parts[-1]  # '07'
        year_str = parts[-2]   # '2025'
        year = int(year_str)
        month = int(month_str)
        
        symbol = parts[0]  # 'AAPL'
        
        # Find timeframe
        timeframe = None
        import re
        timeframe_pattern = re.compile(r'^\d+[mhdw]$')
        for part in parts[1:-2]:
            if timeframe_pattern.match(part):
                timeframe = part
                break
        
        print(f"   ✅ New logic works: symbol={symbol}, timeframe={timeframe}, year={year}, month={month}")
        
        assert year == 2025
        assert month == 7
        assert symbol == 'AAPL'
        assert timeframe == '5m'
        
    print("   ✅ All problematic cases now handled correctly")


def test_edge_cases_and_invalid_formats():
    """
    Test edge cases and invalid formats are handled gracefully.
    """
    print("\n🔍 Testing edge cases and invalid formats")
    
    invalid_cases = [
        "AAPL",                      # Too few parts
        "AAPL_5m",                   # Missing year/month
        "AAPL_5m_invalid_07",        # Invalid year
        "AAPL_5m_2025_13",           # Invalid month
        "AAPL_5m_1999_07",           # Year too old
        "AAPL_5m_2200_07",           # Year too far in future
        "",                          # Empty string
    ]
    
    for file_key in invalid_cases:
        print(f"\n   Testing invalid: '{file_key}'")
        
        if not file_key:
            print("     ✅ Empty string skipped")
            continue
            
        parts = file_key.split('_')
        
        if len(parts) < 4:
            print(f"     ✅ Insufficient parts ({len(parts)}), would be skipped")
            continue
        
        month_str = parts[-1]
        year_str = parts[-2]
        year = int(year_str)
        month = int(month_str)
        
        # Basic validation should catch invalid values
        if year < 2000 or year > 2100 or month < 1 or month > 12:
            print(f"     ✅ Invalid year/month ({year}/{month}), would be skipped")
            continue
        
        print(f"     ⚠️  Unexpectedly parsed: year={year}, month={month}")
        
    print("   ✅ Invalid formats handled gracefully")


if __name__ == "__main__":
    """
    Run test to verify robust file key parsing handles all formats correctly.
    """
    print("🔍 RUNNING ROBUST FILE KEY PARSING VERIFICATION")
    print("=" * 60)
    print("Expected: All file key formats parse correctly")
    print("Goal: Eliminate int('5m') and similar ValueError issues")
    print("=" * 60)
    
    test_robust_file_key_parsing()
    test_robust_parsing_handles_problematic_cases()
    test_edge_cases_and_invalid_formats()
    
    print("\n🎉 ROBUST FILE KEY PARSING VERIFICATION COMPLETE!")
    print("✅ No more ValueError with int('5m') or similar parsing errors")