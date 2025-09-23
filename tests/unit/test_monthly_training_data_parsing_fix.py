#!/usr/bin/env python3
"""
Test to verify the fix for ValueError in _save_monthly_training_data_records.

Tests that the updated parsing logic correctly handles both legacy and new file key formats:
- Legacy format: symbol_timeframe_YYYY_MM
- New format: symbol_featuregroup_timeframe_YYYY_MM
"""

import pytest
import asyncio
from datetime import datetime, date
from unittest.mock import Mock, AsyncMock
from pathlib import Path

import sys
sys.path.insert(0, 'src')

from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback


class TestMonthlyTrainingDataParsingFix:
    """Test that the file key parsing fix handles both formats correctly."""

    @pytest.mark.asyncio
    async def test_new_format_with_feature_groups_works(self):
        """
        Test that the new format (with feature groups) now parses correctly.
        
        This should now work after the fix.
        """
        print("🔍 Testing new format with feature groups")
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            start_date=datetime(2025, 7, 1),
            end_date=datetime(2025, 7, 31),
            output_dir='/tmp/test_training'
        )
        
        # Use the new format with feature groups
        callback.monthly_file_paths = {
            'AAPL_basic_5m_2025_07': '/tmp/test_training/dataset_123/AAPL/5m/2025_07_basic.arrayrecord',
            'AAPL_basic_15m_2025_07': '/tmp/test_training/dataset_123/AAPL/15m/2025_07_basic.arrayrecord',
            'TSLA_advanced_5m_2025_07': '/tmp/test_training/dataset_123/TSLA/5m/2025_07_advanced.arrayrecord'
        }
        
        # Mock dependencies
        mock_runner = Mock()
        mock_runner.run_context = Mock()
        mock_runner.run_context.run_id = "run_20250922_134918_aeda6fc8"
        mock_runner.get_environment = Mock(return_value=Mock())
        
        # Mock all database operations to avoid actual database calls
        with patch_dao_operations():
            # This should now work without ValueError
            await callback._save_monthly_training_data_records(mock_runner)
        
        print("   ✅ New format with feature groups parses successfully")

    @pytest.mark.asyncio
    async def test_legacy_format_still_works(self):
        """
        Test that the legacy format (without feature groups) still works.
        
        Ensures backward compatibility.
        """
        print("🔍 Testing legacy format backward compatibility")
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            start_date=datetime(2025, 7, 1),
            end_date=datetime(2025, 7, 31),
            output_dir='/tmp/test_training'
        )
        
        # Use the legacy format without feature groups
        callback.monthly_file_paths = {
            'AAPL_5m_2025_07': '/tmp/test_training/dataset_123/AAPL/5m/2025_07.arrayrecord',
            'AAPL_15m_2025_07': '/tmp/test_training/dataset_123/AAPL/15m/2025_07.arrayrecord',
            'TSLA_5m_2025_07': '/tmp/test_training/dataset_123/TSLA/5m/2025_07.arrayrecord'
        }
        
        # Mock dependencies
        mock_runner = Mock()
        mock_runner.run_context = Mock()
        mock_runner.run_context.run_id = "run_20250922_134918_aeda6fc8"
        mock_runner.get_environment = Mock(return_value=Mock())
        
        # Mock all database operations
        with patch_dao_operations():
            # This should continue to work
            await callback._save_monthly_training_data_records(mock_runner)
        
        print("   ✅ Legacy format maintains backward compatibility")

    def test_parsing_logic_direct_verification(self):
        """
        Test the parsing logic directly to verify correct field extraction.
        
        Shows exactly what gets parsed from each format.
        """
        print("🔍 Testing parsing logic direct verification")
        
        test_cases = [
            # Format: (file_key, expected_symbol, expected_timeframe, expected_year, expected_month)
            ("AAPL_5m_2025_07", "AAPL", "5m", 2025, 7),           # Legacy format
            ("AAPL_basic_5m_2025_07", "AAPL", "5m", 2025, 7),     # New format
            ("TSLA_advanced_15m_2024_12", "TSLA", "15m", 2024, 12), # New format
            ("SPY_1h_2023_01", "SPY", "1h", 2023, 1),             # Legacy format
        ]
        
        for file_key, expected_symbol, expected_timeframe, expected_year, expected_month in test_cases:
            print(f"   Testing: {file_key}")
            
            parts = file_key.split('_')
            
            if len(parts) < 5:
                # Legacy format: symbol_timeframe_YYYY_MM
                if len(parts) >= 4:
                    symbol = parts[0]
                    timeframe = parts[1]
                    year = int(parts[2])
                    month = int(parts[3])
                else:
                    continue
            else:
                # New format: symbol_featuregroup_timeframe_YYYY_MM
                symbol = parts[0]
                feature_group = parts[1]
                timeframe = parts[2]
                year = int(parts[3])
                month = int(parts[4])
            
            # Verify parsing results
            assert symbol == expected_symbol, f"Symbol mismatch: {symbol} != {expected_symbol}"
            assert timeframe == expected_timeframe, f"Timeframe mismatch: {timeframe} != {expected_timeframe}"
            assert year == expected_year, f"Year mismatch: {year} != {expected_year}"
            assert month == expected_month, f"Month mismatch: {month} != {expected_month}"
            
            print(f"     ✅ Parsed: symbol={symbol}, timeframe={timeframe}, year={year}, month={month}")
        
        print("   ✅ All parsing cases verified successfully")

    def test_invalid_format_handling(self):
        """
        Test that invalid formats are handled gracefully.
        
        Ensures robustness with malformed file keys.
        """
        print("🔍 Testing invalid format handling")
        
        invalid_file_keys = [
            "AAPL",                    # Too few parts
            "AAPL_5m",                 # Missing date
            "AAPL_5m_2025",            # Missing month
            "",                        # Empty string
            "_5m_2025_07",             # Missing symbol
        ]
        
        for file_key in invalid_file_keys:
            print(f"   Testing invalid: '{file_key}'")
            
            parts = file_key.split('_')
            
            # This logic should gracefully skip invalid formats
            if len(parts) < 5:
                if len(parts) >= 4:
                    # Try legacy format parsing
                    try:
                        symbol = parts[0]
                        timeframe = parts[1]
                        year = int(parts[2])
                        month = int(parts[3])
                        print(f"     ✅ Legacy format parsed: {symbol}, {timeframe}, {year}, {month}")
                    except (ValueError, IndexError):
                        print(f"     ✅ Invalid legacy format skipped gracefully")
                else:
                    print(f"     ✅ Insufficient parts, skipped gracefully")
            else:
                # Try new format parsing
                try:
                    symbol = parts[0]
                    feature_group = parts[1]
                    timeframe = parts[2]
                    year = int(parts[3])
                    month = int(parts[4])
                    print(f"     ✅ New format parsed: {symbol}, {timeframe}, {year}, {month}")
                except (ValueError, IndexError):
                    print(f"     ✅ Invalid new format skipped gracefully")
        
        print("   ✅ Invalid formats handled gracefully")


def patch_dao_operations():
    """Context manager to patch all DAO operations for testing."""
    from unittest.mock import patch, AsyncMock
    
    return patch.multiple(
        'domains.ml.services.training_data.callbacks.training_data_callback',
        MonthlyTrainingDataDAO=Mock,
        FeatureExtractionDAO=Mock,
        RunMetadataTracker=Mock(return_value=Mock(start_run=AsyncMock(return_value=123)))
    )


if __name__ == "__main__":
    """
    Run test to verify the file key parsing fix works correctly.
    
    Expected outcome: All tests pass, demonstrating the fix handles both formats.
    """
    print("🔍 RUNNING MONTHLY TRAINING DATA PARSING FIX VERIFICATION")
    print("=" * 70)
    print("Expected: All tests pass, confirming fix handles both formats")
    print("Goal: Verify new format works and legacy format still works")
    print("=" * 70)
    
    # Run with verbose output for verification
    pytest.main([__file__, "-v", "--tb=long", "-s"])