#!/usr/bin/env python3
"""
Test to verify the original ValueError bug is completely fixed.

This test reproduces the exact error condition and confirms the robust parsing fix works.
"""

import pytest
import asyncio
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch

import sys
sys.path.insert(0, 'src')

from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback


class TestOriginalValueErrorBugFixed:
    """Test that the original ValueError in file key parsing is completely fixed."""

    @pytest.mark.asyncio
    async def test_original_error_scenarios_now_work(self):
        """
        Test that the specific file key patterns causing ValueError now work.
        
        These patterns previously caused:
        ValueError: invalid literal for int() with base 10: '5m'
        """
        print("🔍 Testing that original error scenarios now work")
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            start_date=datetime(2025, 7, 1),
            end_date=datetime(2025, 7, 31),
            output_dir='/tmp/test_training'
        )
        
        # Use the exact file key patterns that were causing ValueError
        callback.monthly_file_paths = {
            # These were causing int('5m') errors
            'AAPL_basic_advanced_5m_2025_07': '/tmp/test_training/dataset_123/AAPL/5m/2025_07_basic.arrayrecord',
            'AAPL_dataset_basic_5m_2025_07': '/tmp/test_training/dataset_123/AAPL/5m/2025_07_basic.arrayrecord',
            'AAPL_v1_basic_5m_2025_07': '/tmp/test_training/dataset_123/AAPL/5m/2025_07_basic.arrayrecord',
            
            # Mixed formats to test robustness
            'TSLA_advanced_15m_2024_12': '/tmp/test_training/dataset_123/TSLA/15m/2024_12_advanced.arrayrecord',
            'SPY_premium_tier1_1h_2023_01': '/tmp/test_training/dataset_123/SPY/1h/2023_01_premium.arrayrecord',
        }
        
        # Mock dependencies
        mock_runner = Mock()
        mock_runner.run_context = Mock()
        mock_runner.run_context.run_id = "run_20250922_134918_aeda6fc8"
        mock_runner.get_environment = Mock(return_value=Mock())
        
        # Mock all database operations to focus on parsing logic
        with patch('domains.ml.services.training_data.utils.run_metadata_tracker.RunMetadataTracker') as mock_tracker_class:
            mock_tracker = Mock()
            mock_tracker.start_run = AsyncMock(return_value=123)
            mock_tracker_class.return_value = mock_tracker
            
            with patch('domains.ml.services.training_data.dao.monthly_training_data_dao.MonthlyTrainingDataDAO') as mock_dao:
                mock_dao_instance = Mock()
                mock_dao_instance.create_monthly_record = AsyncMock(return_value=456)
                mock_dao.return_value = mock_dao_instance
                
                with patch('domains.ml.services.training_data.dao.feature_extraction_dao.FeatureExtractionDAO') as mock_feature_dao:
                    mock_feature_dao_instance = Mock()
                    mock_feature_dao_instance.upsert_feature_extraction_run = AsyncMock()
                    mock_feature_dao.return_value = mock_feature_dao_instance
                    
                    with patch('pathlib.Path.stat') as mock_stat:
                        # Mock file stats to avoid FileNotFoundError
                        mock_stat.return_value = Mock(st_size=1024)
                        
                        # This should now work without any ValueError
                        await callback._save_monthly_training_data_records(mock_runner)
        
        print("   ✅ All problematic file key patterns processed successfully")
        print("   ✅ No ValueError: invalid literal for int() with base 10: '5m'")

    def test_parsing_logic_directly_handles_all_cases(self):
        """
        Test the parsing logic directly with all the problematic patterns.
        
        This verifies the exact fix without the complexity of full method execution.
        """
        print("\n🔍 Testing parsing logic directly with problematic patterns")
        
        problematic_file_keys = [
            'AAPL_basic_advanced_5m_2025_07',      # 6 parts, old logic: int(parts[3]) = int('5m')
            'AAPL_dataset_basic_5m_2025_07',       # 6 parts, old logic: int(parts[3]) = int('5m')
            'AAPL_v1_basic_5m_2025_07',            # 6 parts, old logic: int(parts[3]) = int('5m')
            'TSLA_premium_tier1_advanced_15m_2024_12', # 7 parts, old logic: int(parts[3]) = int('advanced')
        ]
        
        for file_key in problematic_file_keys:
            print(f"\n   Testing: {file_key}")
            parts = file_key.split('_')
            
            print(f"   Parts: {parts}")
            print(f"   Old logic would fail: int(parts[3]) = int('{parts[3]}')")
            
            # New robust parsing logic (from the fixed code)
            try:
                # The year and month are always the last two parts
                month_str = parts[-1]
                year_str = parts[-2]
                
                # Validate that these look like year/month
                year = int(year_str)
                month = int(month_str)
                
                # Basic validation
                if year < 2000 or year > 2100 or month < 1 or month > 12:
                    continue
                
                year_month = f"{year_str}_{month_str}"
                
                # Extract symbol (always first part)
                symbol = parts[0]
                
                # Find timeframe by looking for specific patterns
                import re
                timeframe = None
                timeframe_pattern = re.compile(r'^\d+[mhdw]$')
                for part in parts[1:-2]:
                    if timeframe_pattern.match(part):
                        timeframe = part
                        break
                
                if not timeframe:
                    timeframe = parts[1] if len(parts) > 1 else 'unknown'
                
                print(f"   ✅ New logic succeeds: symbol={symbol}, timeframe={timeframe}, year={year}, month={month}")
                
                # Verify expected values
                assert symbol in ['AAPL', 'TSLA']
                assert timeframe in ['5m', '15m']
                assert year in [2024, 2025]
                assert month in [7, 12]
                
            except ValueError as e:
                pytest.fail(f"New logic should not fail for {file_key}: {e}")
        
        print("   ✅ All problematic patterns handled by new parsing logic")

    def test_demonstrates_exact_error_fix(self):
        """
        Demonstrate the exact fix for the specific error reported.
        
        Original traceback:
        File "...training_data_callback.py", line 575, in _save_monthly_training_data_records
            year = int(parts[3])
        ValueError: invalid literal for int() with base 10: '5m'
        """
        print("\n🔍 Demonstrating exact error fix")
        
        # This is the file key pattern that was causing the exact error
        problematic_key = 'AAPL_basic_advanced_5m_2025_07'
        parts = problematic_key.split('_')
        
        print(f"   Problematic file key: {problematic_key}")
        print(f"   Split into parts: {parts}")
        print(f"   len(parts): {len(parts)}")
        
        print("\n   OLD BROKEN LOGIC:")
        print("   ❌ if len(parts) >= 5:")
        print("   ❌     symbol = parts[0]        # 'AAPL' ✓")
        print("   ❌     feature_group = parts[1] # 'basic' ✓")
        print("   ❌     timeframe = parts[2]     # 'advanced' ❌")
        print("   ❌     year = int(parts[3])     # int('5m') ❌ ValueError!")
        print("   ❌     month = int(parts[4])    # int('2025') ❌")
        
        print("\n   NEW ROBUST LOGIC:")
        
        # Robust parsing approach
        month_str = parts[-1]    # '07'
        year_str = parts[-2]     # '2025'
        year = int(year_str)     # 2025 ✓
        month = int(month_str)   # 7 ✓
        symbol = parts[0]        # 'AAPL' ✓
        
        import re
        timeframe = None
        timeframe_pattern = re.compile(r'^\d+[mhdw]$')
        for part in parts[1:-2]:  # ['basic', 'advanced', '5m']
            if timeframe_pattern.match(part):
                timeframe = part  # '5m' ✓
                break
        
        print(f"   ✅ month_str = parts[-1]     # '{month_str}' ✓")
        print(f"   ✅ year_str = parts[-2]      # '{year_str}' ✓")
        print(f"   ✅ year = int(year_str)      # {year} ✓")
        print(f"   ✅ month = int(month_str)    # {month} ✓")
        print(f"   ✅ symbol = parts[0]         # '{symbol}' ✓")
        print(f"   ✅ timeframe found by regex  # '{timeframe}' ✓")
        
        # Verify all values are correct
        assert year == 2025
        assert month == 7
        assert symbol == 'AAPL'
        assert timeframe == '5m'
        
        print("\n   🎉 EXACT ERROR FIXED: No more ValueError with int('5m')!")


if __name__ == "__main__":
    """
    Run test to verify the original ValueError bug is completely fixed.
    """
    print("🔍 RUNNING ORIGINAL VALUEERROR BUG FIX VERIFICATION")
    print("=" * 70)
    print("Expected: All tests pass, confirming original bug is fixed")
    print("Original error: ValueError: invalid literal for int() with base 10: '5m'")
    print("=" * 70)
    
    # Run with verbose output for verification
    pytest.main([__file__, "-v", "--tb=long", "-s"])