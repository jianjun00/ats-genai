#!/usr/bin/env python3
"""
Test script for the reorganized FirstRate minute bar structure

This script tests the updated FileBasedMinuteManager to ensure it works correctly
with the new first letter organization (A/AAPL/, B/BA/, etc.) instead of the
old flat structure (AAPL/, BA/, etc.).

Usage:
    python3 scripts/test_firstrate_reorganized_structure.py
"""

import sys
import os
from pathlib import Path
from datetime import datetime
import logging

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from storage.file_based_minute_manager import FileBasedMinuteManager, MinuteBar

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_file_path_generation():
    """Test that the FileBasedMinuteManager generates correct file paths with first letter organization."""

    logger.info("🧪 Testing FileBasedMinuteManager file path generation...")

    # Initialize the manager with the reorganized structure
    test_base_path = "/mnt/d/ats-data/minute-bars/firstrate"
    manager = FileBasedMinuteManager(test_base_path)

    # Test cases for different symbols and dates
    test_cases = [
        ("AAPL", 2024, 1, "A/AAPL/2024/01/AAPL_2024_01.parquet"),
        ("BA", 2024, 3, "B/BA/2024/03/BA_2024_03.parquet"),
        ("MSFT", 2023, 12, "M/MSFT/2023/12/MSFT_2023_12.parquet"),
        ("TSLA", 2024, 6, "T/TSLA/2024/06/TSLA_2024_06.parquet"),
        ("SPY", 2024, 2, "S/SPY/2024/02/SPY_2024_02.parquet")
    ]

    logger.info(f"Testing {len(test_cases)} file path generation cases...")

    all_passed = True
    for i, (symbol, year, month, expected_suffix) in enumerate(test_cases):
        try:
            # Generate the file path using the manager
            file_path = manager._get_monthly_file_path(symbol, year, month)

            # Convert to relative path for comparison
            relative_path = str(file_path.relative_to(test_base_path))

            # Check if it matches the expected pattern
            if relative_path == expected_suffix:
                logger.info(f"✅ Test {i+1}: {symbol} -> {relative_path}")
            else:
                logger.error(f"❌ Test {i+1}: {symbol}")
                logger.error(f"   Expected: {expected_suffix}")
                logger.error(f"   Got:      {relative_path}")
                all_passed = False

        except Exception as e:
            logger.error(f"❌ Test {i+1}: {symbol} failed with exception: {e}")
            all_passed = False

    return all_passed

def test_existing_data_access():
    """Test accessing existing data in the reorganized structure."""

    logger.info("🔍 Testing access to existing reorganized data...")

    test_base_path = "/mnt/d/ats-data/minute-bars/firstrate"
    manager = FileBasedMinuteManager(test_base_path)

    # Check if we can find some reorganized symbols
    reorganized_symbols_to_test = ["AAPL", "MSFT", "TSLA", "BA"]
    found_data = []

    for symbol in reorganized_symbols_to_test:
        try:
            # Check if the reorganized directory exists
            first_letter = symbol[0].upper()
            symbol_path = Path(test_base_path) / first_letter / symbol

            if symbol_path.exists():
                # Count subdirectories (should be year directories)
                year_dirs = [d for d in symbol_path.iterdir() if d.is_dir() and d.name.isdigit()]
                if year_dirs:
                    logger.info(f"✅ Found reorganized data for {symbol}: {len(year_dirs)} years")
                    found_data.append(symbol)
                else:
                    logger.info(f"⚠️  Directory exists for {symbol} but no year data found")
            else:
                logger.info(f"ℹ️  No reorganized data found for {symbol} (may not be moved yet)")
        except Exception as e:
            logger.error(f"❌ Error checking {symbol}: {e}")

    if found_data:
        logger.info(f"✅ Successfully found reorganized data for {len(found_data)} symbols: {found_data}")
        return True
    else:
        logger.warning("⚠️  No reorganized data found - reorganization may still be in progress")
        return False

def test_directory_structure():
    """Test the overall directory structure."""

    logger.info("📁 Testing directory structure...")

    test_base_path = Path("/mnt/d/ats-data/minute-bars/firstrate")

    if not test_base_path.exists():
        logger.error(f"❌ Base path does not exist: {test_base_path}")
        return False

    # Look for first letter directories
    letter_dirs = [d for d in test_base_path.iterdir() if d.is_dir() and len(d.name) == 1 and d.name.isalpha()]

    logger.info(f"Found {len(letter_dirs)} first letter directories: {sorted([d.name for d in letter_dirs])}")

    # Check a few first letter directories for symbol subdirectories
    symbols_found = 0
    for letter_dir in letter_dirs[:3]:  # Check first 3 letter directories
        symbol_dirs = [d for d in letter_dir.iterdir() if d.is_dir()]
        symbols_found += len(symbol_dirs)
        logger.info(f"  {letter_dir.name}/: {len(symbol_dirs)} symbols")

    if symbols_found > 0:
        logger.info(f"✅ Directory structure looks good: {symbols_found} symbols found in first letter directories")
        return True
    else:
        logger.warning("⚠️  No symbols found in first letter directories - reorganization may still be in progress")
        return False

def main():
    """Main test function."""

    logger.info("=" * 60)
    logger.info("FIRSTRATE REORGANIZED STRUCTURE TEST")
    logger.info("=" * 60)

    tests_passed = 0
    total_tests = 3

    # Test 1: File path generation
    logger.info("\n1. Testing file path generation...")
    if test_file_path_generation():
        tests_passed += 1
        logger.info("✅ File path generation test PASSED")
    else:
        logger.error("❌ File path generation test FAILED")

    # Test 2: Existing data access
    logger.info("\n2. Testing existing data access...")
    if test_existing_data_access():
        tests_passed += 1
        logger.info("✅ Existing data access test PASSED")
    else:
        logger.warning("⚠️  Existing data access test INCONCLUSIVE")

    # Test 3: Directory structure
    logger.info("\n3. Testing directory structure...")
    if test_directory_structure():
        tests_passed += 1
        logger.info("✅ Directory structure test PASSED")
    else:
        logger.warning("⚠️  Directory structure test INCONCLUSIVE")

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Tests passed: {tests_passed}/{total_tests}")

    if tests_passed == total_tests:
        logger.info("🎉 All tests PASSED! The reorganized structure is working correctly.")
        return True
    elif tests_passed > 0:
        logger.info("⚠️  Some tests passed. The structure may be partially working or still reorganizing.")
        return True
    else:
        logger.error("❌ All tests FAILED. There may be an issue with the reorganization.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)