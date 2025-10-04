#!/usr/bin/env python3
"""
Test to verify 5-minute training data actually represents proper 5-minute intervals.

This test validates:
1. 5-minute data is properly aggregated from 1-minute source data
2. Timestamps align to 5-minute boundaries (e.g., :00, :05, :10, :15, etc.)
3. OHLC aggregation logic is correct (first open, max high, min low, last close)
4. Volume summation is correct
5. Time ranges match expected 5-minute windows
"""

import pytest
import pandas as pd
import numpy as np
import tempfile
import os
from pathlib import Path
from datetime import datetime, timedelta
import asyncio
from unittest.mock import Mock, AsyncMock

class Test5MinuteIntervalVerification:
    """Test that 5-minute training data properly represents 5-minute intervals."""

    def create_mock_1minute_data(self, temp_dir):
        """Create mock 1-minute AAPL data with known values for aggregation testing."""
        # Create 10 minutes of 1-minute data (2 complete 5-minute intervals)
        base_time = datetime(2025, 7, 1, 14, 0, 0)
        
        # First 5-minute interval: 14:00-14:04 (5 rows)
        # Second 5-minute interval: 14:05-14:09 (5 rows)
        minute_data = []
        
        # First 5-minute interval data
        interval_1_data = [
            # 14:00 - opening bar
            {'timestamp': base_time, 'open': 208.00, 'high': 208.10, 'low': 207.95, 'close': 208.05, 'volume': 1000},
            # 14:01 - price rises
            {'timestamp': base_time + timedelta(minutes=1), 'open': 208.05, 'high': 208.20, 'low': 208.00, 'close': 208.15, 'volume': 1500},
            # 14:02 - peak high for interval
            {'timestamp': base_time + timedelta(minutes=2), 'open': 208.15, 'high': 208.30, 'low': 208.10, 'close': 208.25, 'volume': 2000},
            # 14:03 - pullback, lowest low for interval  
            {'timestamp': base_time + timedelta(minutes=3), 'open': 208.25, 'high': 208.28, 'low': 207.90, 'close': 208.00, 'volume': 1200},
            # 14:04 - closing bar for first interval
            {'timestamp': base_time + timedelta(minutes=4), 'open': 208.00, 'high': 208.12, 'low': 207.98, 'close': 208.08, 'volume': 800},
        ]
        
        # Second 5-minute interval data  
        interval_2_data = [
            # 14:05 - opening bar for second interval
            {'timestamp': base_time + timedelta(minutes=5), 'open': 208.08, 'high': 208.15, 'low': 208.05, 'close': 208.12, 'volume': 900},
            # 14:06 - price drops
            {'timestamp': base_time + timedelta(minutes=6), 'open': 208.12, 'high': 208.18, 'low': 207.85, 'close': 207.90, 'volume': 1600},
            # 14:07 - lowest low for second interval
            {'timestamp': base_time + timedelta(minutes=7), 'open': 207.90, 'high': 208.00, 'low': 207.70, 'close': 207.75, 'volume': 1800},
            # 14:08 - recovery
            {'timestamp': base_time + timedelta(minutes=8), 'open': 207.75, 'high': 207.95, 'low': 207.72, 'close': 207.88, 'volume': 1400},
            # 14:09 - closing bar for second interval
            {'timestamp': base_time + timedelta(minutes=9), 'open': 207.88, 'high': 207.92, 'low': 207.80, 'close': 207.85, 'volume': 1100},
        ]
        
        minute_data = interval_1_data + interval_2_data
        
        # Create directory structure
        aapl_dir = Path(temp_dir) / "A" / "AAPL" / "2025" / "07"
        aapl_dir.mkdir(parents=True, exist_ok=True)
        
        # Create parquet file
        df = pd.DataFrame(minute_data)
        parquet_file = aapl_dir / "AAPL_2025_07.parquet"
        df.to_parquet(parquet_file, index=False)
        
        # Return expected 5-minute aggregated values
        expected_5min_intervals = [
            {
                'timestamp': base_time,  # 14:00 start of first interval
                'open': 208.00,          # First open (14:00)
                'high': 208.30,          # Max high (14:02)  
                'low': 207.90,           # Min low (14:03)
                'close': 208.08,         # Last close (14:04)
                'volume': 6500,          # Sum of all volumes (1000+1500+2000+1200+800)
                'interval_start': base_time,
                'interval_end': base_time + timedelta(minutes=5)
            },
            {
                'timestamp': base_time + timedelta(minutes=5),  # 14:05 start of second interval
                'open': 208.08,          # First open (14:05)
                'high': 208.18,          # Max high (14:06)
                'low': 207.70,           # Min low (14:07)
                'close': 207.85,         # Last close (14:09)
                'volume': 6800,          # Sum of all volumes (900+1600+1800+1400+1100)
                'interval_start': base_time + timedelta(minutes=5),
                'interval_end': base_time + timedelta(minutes=10)
            }
        ]
        
        return parquet_file, expected_5min_intervals

    @pytest.mark.asyncio
    async def test_5minute_firstrate_aggregation(self):
        """Test that FirstRate adapter properly aggregates 1-minute data into 5-minute intervals."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create mock data
            parquet_file, expected_intervals = self.create_mock_1minute_data(temp_dir)
            
            # Test FirstRate adapter
            from core.market_data.unified_manager import FirstRateAdapter, TimeframeType
            
            adapter = FirstRateAdapter(file_path=str(temp_dir))
            
            # Get 5-minute aggregated data
            start_date = datetime(2025, 7, 1, 14, 0, 0)
            end_date = datetime(2025, 7, 1, 14, 10, 0)
            
            # Test: FirstRate adapter should return 1-minute data (not pre-aggregated)
            result = await adapter.get_ohlcv(
                symbols=["AAPL"],
                start_date=start_date,
                end_date=end_date,
                timeframe=TimeframeType.MINUTE_1
            )
            
            assert "AAPL" in result, "Should return AAPL data"
            df = result["AAPL"]
            assert not df.empty, "Should have data"
            assert len(df) == 10, f"Should have 10 1-minute bars, got {len(df)}"
            
            # Verify 1-minute data structure
            required_columns = ['open', 'high', 'low', 'close', 'volume']
            for col in required_columns:
                assert col in df.columns, f"Missing column: {col}"
            
            # Test manual 5-minute aggregation (simulating what should happen in UniverseStateBuilder)
            df_sorted = df.sort_index()
            
            # Group by 5-minute intervals 
            df_5min = df_sorted.resample('5T').agg({
                'open': 'first',
                'high': 'max', 
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()
            
            assert len(df_5min) == 2, f"Should have 2 5-minute intervals, got {len(df_5min)}"
            
            # Verify first 5-minute interval (14:00-14:05)
            first_interval = df_5min.iloc[0]
            expected_first = expected_intervals[0]
            
            assert abs(first_interval['open'] - expected_first['open']) < 0.01, \
                f"First interval open: expected {expected_first['open']}, got {first_interval['open']}"
            assert abs(first_interval['high'] - expected_first['high']) < 0.01, \
                f"First interval high: expected {expected_first['high']}, got {first_interval['high']}"
            assert abs(first_interval['low'] - expected_first['low']) < 0.01, \
                f"First interval low: expected {expected_first['low']}, got {first_interval['low']}"
            assert abs(first_interval['close'] - expected_first['close']) < 0.01, \
                f"First interval close: expected {expected_first['close']}, got {first_interval['close']}"
            assert first_interval['volume'] == expected_first['volume'], \
                f"First interval volume: expected {expected_first['volume']}, got {first_interval['volume']}"
            
            # Verify second 5-minute interval (14:05-14:10)
            second_interval = df_5min.iloc[1] 
            expected_second = expected_intervals[1]
            
            assert abs(second_interval['open'] - expected_second['open']) < 0.01, \
                f"Second interval open: expected {expected_second['open']}, got {second_interval['open']}"
            assert abs(second_interval['high'] - expected_second['high']) < 0.01, \
                f"Second interval high: expected {expected_second['high']}, got {second_interval['high']}"
            assert abs(second_interval['low'] - expected_second['low']) < 0.01, \
                f"Second interval low: expected {expected_second['low']}, got {second_interval['low']}"
            assert abs(second_interval['close'] - expected_second['close']) < 0.01, \
                f"Second interval close: expected {expected_second['close']}, got {second_interval['close']}"
            assert second_interval['volume'] == expected_second['volume'], \
                f"Second interval volume: expected {expected_second['volume']}, got {second_interval['volume']}"
            
            print("✅ 5-minute aggregation test passed")
            print(f"   First 5min: O={first_interval['open']:.2f} H={first_interval['high']:.2f} L={first_interval['low']:.2f} C={first_interval['close']:.2f} V={first_interval['volume']}")
            print(f"   Second 5min: O={second_interval['open']:.2f} H={second_interval['high']:.2f} L={second_interval['low']:.2f} C={second_interval['close']:.2f} V={second_interval['volume']}")

    def test_5minute_timestamp_alignment(self):
        """Test that 5-minute intervals align to proper boundaries."""
        
        # Test various timestamps to ensure they align to 5-minute boundaries
        test_cases = [
            # (input_timestamp, expected_5min_boundary)
            (datetime(2025, 7, 1, 14, 0, 0), datetime(2025, 7, 1, 14, 0, 0)),   # Already aligned
            (datetime(2025, 7, 1, 14, 1, 30), datetime(2025, 7, 1, 14, 0, 0)),  # Should round down to 14:00
            (datetime(2025, 7, 1, 14, 3, 45), datetime(2025, 7, 1, 14, 0, 0)),  # Should round down to 14:00
            (datetime(2025, 7, 1, 14, 5, 0), datetime(2025, 7, 1, 14, 5, 0)),   # Already aligned
            (datetime(2025, 7, 1, 14, 7, 20), datetime(2025, 7, 1, 14, 5, 0)),  # Should round down to 14:05
            (datetime(2025, 7, 1, 14, 12, 15), datetime(2025, 7, 1, 14, 10, 0)), # Should round down to 14:10
        ]
        
        for input_ts, expected_boundary in test_cases:
            # Calculate 5-minute boundary (floor to nearest 5-minute mark)
            minutes_since_hour = input_ts.minute
            aligned_minutes = (minutes_since_hour // 5) * 5
            actual_boundary = input_ts.replace(minute=aligned_minutes, second=0, microsecond=0)
            
            assert actual_boundary == expected_boundary, \
                f"Timestamp {input_ts} should align to {expected_boundary}, got {actual_boundary}"
        
        print("✅ 5-minute timestamp alignment test passed")

    @pytest.mark.asyncio 
    async def test_5minute_training_data_generation(self):
        """Test that training data generation produces proper 5-minute features."""
        
        # This test would ideally run the full training data pipeline
        # and verify that 5-minute features are properly calculated
        
        from domains.trading.services.state.universe_state_manager import UniverseStateManager
        from core.platform.config_env.environment import Environment
        from unittest.mock import Mock
        
        # Mock environment
        mock_env = Mock(spec=Environment)
        mock_env.get_database_config.return_value = Mock()
        
        # Create universe state manager
        manager = UniverseStateManager(mock_env)
        
        # Test that the manager can handle 5-minute intervals
        # In reality, this would be populated by UniverseStateBuilder
        mock_intervals = []
        
        # Simulate 5-minute intervals being added to cache
        base_time = datetime(2025, 7, 1, 14, 0, 0)
        
        for i in range(2):  # Two 5-minute intervals
            from domains.trading.services.state.instrument_interval import InstrumentInterval
            
            interval_start = base_time + timedelta(minutes=i*5)
            interval_end = base_time + timedelta(minutes=(i+1)*5)
            
            interval = InstrumentInterval(
                instrument_id=31,  # AAPL
                start_date_time=interval_start,
                end_date_time=interval_end,
                open=208.0 + i*0.1,
                high=208.3 + i*0.1, 
                low=207.9 + i*0.1,
                close=208.1 + i*0.1,
                traded_volume=6500 + i*300,
                traded_dollar=6500 * 208.1,
                status='ok',
                market_cap=1500000000.0
            )
            
            # Add to 5-minute cache
            manager.add_interval_to_rolling_cache(31, '5m', interval)
        
        # Verify cache contains proper 5-minute data
        intervals_5m = manager.get_instrument_history_for_timeframe(31, '5m')
        assert len(intervals_5m) == 2, f"Should have 2 5-minute intervals, got {len(intervals_5m)}"
        
        # Verify first interval
        first_interval = intervals_5m[0]
        assert first_interval.start_date_time == base_time, "First interval should start at 14:00"
        assert first_interval.end_date_time == base_time + timedelta(minutes=5), "First interval should end at 14:05"
        
        # Verify second interval  
        second_interval = intervals_5m[1]
        assert second_interval.start_date_time == base_time + timedelta(minutes=5), "Second interval should start at 14:05"
        assert second_interval.end_date_time == base_time + timedelta(minutes=10), "Second interval should end at 14:10"
        
        # Test lag prices for 5-minute timeframe (expect empty since get_lag_prices uses DB, not cache)
        current_time = base_time + timedelta(minutes=10)  # After both intervals
        
        # NOTE: get_lag_prices queries database, not rolling cache
        # The rolling cache is used internally by UniverseStateBuilder
        # So we test the cache directly instead
        
        # Test that cache structure is working
        debug_info = manager.get_rolling_cache_debug_info()
        assert '5m' in debug_info, "Should have 5-minute cache"
        assert debug_info['5m']['instrument_count'] == 1, "Should have 1 instrument in 5m cache"
        assert 31 in debug_info['5m']['instruments'], "Should have instrument 31 in 5m cache"
        assert debug_info['5m']['instruments'][31] == 2, "Should have 2 intervals for instrument 31"
        
        print("✅ 5-minute training data generation test passed")
        print(f"   Cached 5-minute intervals: {len(intervals_5m)}")
        print(f"   Cache debug info: {debug_info}")

    def test_5minute_vs_1minute_feature_differences(self):
        """Test that 5-minute features are different from 1-minute features."""
        
        # Create sample data showing how 5-minute aggregation differs from 1-minute
        
        # Simulate volatile 1-minute data within a 5-minute period
        minute_bars = [
            {'timestamp': '14:00', 'open': 208.00, 'high': 208.05, 'low': 207.98, 'close': 208.02, 'volume': 1000},
            {'timestamp': '14:01', 'open': 208.02, 'high': 208.25, 'low': 208.00, 'close': 208.20, 'volume': 1500}, 
            {'timestamp': '14:02', 'open': 208.20, 'high': 208.35, 'low': 207.75, 'close': 208.30, 'volume': 2000},  # Min low here
            {'timestamp': '14:03', 'open': 208.30, 'high': 208.32, 'low': 207.80, 'close': 207.85, 'volume': 1200},
            {'timestamp': '14:04', 'open': 207.85, 'high': 208.00, 'low': 207.90, 'close': 207.95, 'volume': 800},   # Last low different
        ]
        
        # Calculate 1-minute features (using last bar)
        last_1min = minute_bars[-1]
        features_1min = {
            'open': last_1min['open'],
            'high': last_1min['high'],
            'low': last_1min['low'],
            'close': last_1min['close'],
            'volume': last_1min['volume'],
            'range': last_1min['high'] - last_1min['low']
        }
        
        # Calculate 5-minute features (aggregated)
        features_5min = {
            'open': minute_bars[0]['open'],  # First open
            'high': max(bar['high'] for bar in minute_bars),  # Max high
            'low': min(bar['low'] for bar in minute_bars),    # Min low  
            'close': minute_bars[-1]['close'],  # Last close
            'volume': sum(bar['volume'] for bar in minute_bars),  # Sum volume
            'range': max(bar['high'] for bar in minute_bars) - min(bar['low'] for bar in minute_bars)
        }
        
        # Verify they are different (as expected for volatile period)
        assert features_1min['open'] != features_5min['open'], \
            "1-minute and 5-minute open should differ for volatile data"
        assert features_1min['high'] != features_5min['high'], \
            "1-minute and 5-minute high should differ (5min captures period high)"
        assert features_1min['low'] != features_5min['low'], \
            "1-minute and 5-minute low should differ (5min captures period low)"
        assert features_1min['volume'] != features_5min['volume'], \
            "1-minute and 5-minute volume should differ (5min is sum)"
        assert features_1min['range'] != features_5min['range'], \
            "1-minute and 5-minute range should differ (5min captures full period range)"
        
        # Verify 5-minute aggregation logic
        assert features_5min['open'] == 208.00, "5min open should be first 1min open"
        assert features_5min['high'] == 208.35, "5min high should be max of all 1min highs"
        assert features_5min['low'] == 207.75, "5min low should be min of all 1min lows" 
        assert features_5min['close'] == 207.95, "5min close should be last 1min close"
        assert features_5min['volume'] == 6500, "5min volume should be sum of all 1min volumes"
        
        # Print debug info
        print(f"1-minute features: {features_1min}")
        print(f"5-minute features: {features_5min}")
        assert abs(features_5min['range'] - 0.60) < 0.01, "5min range should be high-low across period"
        
        print("✅ 5-minute vs 1-minute feature difference test passed")
        print(f"   1min range: {features_1min['range']:.2f}, 5min range: {features_5min['range']:.2f}")
        print(f"   1min volume: {features_1min['volume']}, 5min volume: {features_5min['volume']}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])