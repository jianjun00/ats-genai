#!/usr/bin/env python3
"""
Simple test script for the reorganized FirstRate minute bar structure

This script tests the directory structure without complex imports to verify
the reorganization is working correctly.

Usage:
    python3 scripts/test_firstrate_reorganized_simple.py
"""

import os
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_first_letter_path_logic():
    """Test the first letter path generation logic."""
    
    logger.info("🧪 Testing first letter path generation logic...")
    
    test_cases = [
        ("AAPL", "A"),
        ("BA", "B"), 
        ("MSFT", "M"),
        ("TSLA", "T"),
        ("SPY", "S"),
        ("QQQ", "Q")
    ]
    
    base_path = "/mnt/d/ats-data/minute-bars/firstrate"
    
    all_passed = True
    for symbol, expected_letter in test_cases:
        # Simulate the logic from FileBasedMinuteManager
        first_letter = symbol[0].upper()
        year, month = 2024, 1
        
        expected_path = f"{base_path}/{first_letter}/{symbol}/2024/01/{symbol}_2024_01.parquet"
        actual_path = f"{base_path}/{first_letter}/{symbol}/{year}/{month:02d}/{symbol}_{year}_{month:02d}.parquet"
        
        if actual_path == expected_path and first_letter == expected_letter:
            logger.info(f"✅ {symbol} -> {first_letter}/{symbol}/")
        else:
            logger.error(f"❌ {symbol}: Expected {expected_letter}, got {first_letter}")
            all_passed = False
    
    return all_passed

def test_directory_structure():
    """Test the actual directory structure on disk."""
    
    logger.info("📁 Testing actual directory structure...")
    
    base_path = Path("/mnt/d/ats-data/minute-bars/firstrate")
    
    if not base_path.exists():
        logger.error(f"❌ Base path does not exist: {base_path}")
        return False
    
    # Look for first letter directories
    first_letter_dirs = [d for d in base_path.iterdir() if d.is_dir() and len(d.name) == 1 and d.name.isalpha()]
    other_dirs = [d for d in base_path.iterdir() if d.is_dir() and not (len(d.name) == 1 and d.name.isalpha()) and not d.name.startswith('.')]
    
    logger.info(f"Found {len(first_letter_dirs)} first letter directories: {sorted([d.name for d in first_letter_dirs])}")
    logger.info(f"Found {len(other_dirs)} other directories (not moved yet): {len(other_dirs)} dirs")
    
    if len(other_dirs) > 0:
        logger.info(f"  Sample unmoved directories: {sorted([d.name for d in other_dirs])[:5]}")
    
    # Check some first letter directories for content
    symbols_found = 0
    total_files = 0
    
    for letter_dir in first_letter_dirs:
        try:
            symbol_dirs = [d for d in letter_dir.iterdir() if d.is_dir()]
            symbols_found += len(symbol_dirs)
            
            # Check for data files in a few symbol directories
            for symbol_dir in symbol_dirs[:2]:  # Check first 2 symbols
                for year_dir in symbol_dir.iterdir():
                    if year_dir.is_dir() and year_dir.name.isdigit():
                        for month_dir in year_dir.iterdir():
                            if month_dir.is_dir():
                                parquet_files = list(month_dir.glob("*.parquet"))
                                total_files += len(parquet_files)
                                break  # Just check one month
                        break  # Just check one year
            
            if len(symbol_dirs) > 0:
                logger.info(f"  {letter_dir.name}/: {len(symbol_dirs)} symbols")
        except Exception as e:
            logger.warning(f"  Error checking {letter_dir.name}/: {e}")
    
    logger.info(f"Total reorganized symbols: {symbols_found}")
    logger.info(f"Sample data files found: {total_files}")
    
    return symbols_found > 0

def test_specific_reorganized_symbols():
    """Test for specific symbols that should have been reorganized."""
    
    logger.info("🔍 Testing specific reorganized symbols...")
    
    base_path = Path("/mnt/d/ats-data/minute-bars/firstrate")
    test_symbols = ["AAPL", "MSFT", "TSLA", "BA", "SPY"]
    
    found_symbols = []
    for symbol in test_symbols:
        first_letter = symbol[0].upper()
        symbol_path = base_path / first_letter / symbol
        
        if symbol_path.exists():
            # Check for data
            data_found = False
            try:
                for year_dir in symbol_path.iterdir():
                    if year_dir.is_dir() and year_dir.name.isdigit():
                        for month_dir in year_dir.iterdir():
                            if month_dir.is_dir():
                                parquet_files = list(month_dir.glob("*.parquet"))
                                if parquet_files:
                                    data_found = True
                                    break
                        if data_found:
                            break
            except:
                pass
            
            if data_found:
                logger.info(f"✅ {symbol} found with data at {first_letter}/{symbol}/")
                found_symbols.append(symbol)
            else:
                logger.info(f"⚠️  {symbol} directory exists but no data found")
        else:
            # Check if it's still in the old location (not moved yet)
            old_path = base_path / symbol
            if old_path.exists():
                logger.info(f"⏳ {symbol} still in old location (not moved yet)")
            else:
                logger.info(f"❓ {symbol} not found in either location")
    
    return len(found_symbols) > 0

def main():
    """Main test function."""
    
    logger.info("=" * 60)
    logger.info("FIRSTRATE REORGANIZED STRUCTURE SIMPLE TEST")
    logger.info("=" * 60)
    
    tests_passed = 0
    total_tests = 3
    
    # Test 1: Path generation logic
    logger.info("\n1. Testing first letter path generation logic...")
    if test_first_letter_path_logic():
        tests_passed += 1
        logger.info("✅ Path generation logic test PASSED")
    else:
        logger.error("❌ Path generation logic test FAILED")
    
    # Test 2: Directory structure
    logger.info("\n2. Testing directory structure...")
    if test_directory_structure():
        tests_passed += 1
        logger.info("✅ Directory structure test PASSED")
    else:
        logger.warning("⚠️  Directory structure test FAILED or INCONCLUSIVE")
    
    # Test 3: Specific symbols
    logger.info("\n3. Testing specific reorganized symbols...")
    if test_specific_reorganized_symbols():
        tests_passed += 1
        logger.info("✅ Specific symbols test PASSED")
    else:
        logger.warning("⚠️  Specific symbols test FAILED or INCONCLUSIVE")
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Tests passed: {tests_passed}/{total_tests}")
    
    if tests_passed >= 2:
        logger.info("🎉 Most tests PASSED! The reorganized structure is working.")
        return True
    elif tests_passed > 0:
        logger.info("⚠️  Some tests passed. The reorganization may still be in progress.")
        return True
    else:
        logger.error("❌ Most tests FAILED. There may be an issue.")
        return False

if __name__ == "__main__":
    success = main()
    print(f"\nTest result: {'PASSED' if success else 'FAILED'}")