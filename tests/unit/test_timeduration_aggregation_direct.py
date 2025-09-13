"""
Direct unit test of TimeDuration.aggregate_intervals() method.

This tests the core OHLC aggregation logic that UniverseStateBuilder uses
for creating multi-timeframe InstrumentInterval objects.
"""

import pytest
from datetime import datetime

from core.business.calendars.time_duration import TimeDuration
from domains.trading.services.state.instrument_interval import InstrumentInterval


class TestTimeDurationAggregation:
    """Test TimeDuration aggregation logic directly."""
    
    def test_aggregate_intervals_ohlc_computation(self):
        """Test that aggregate_intervals correctly computes OHLC from multiple intervals."""
        
        # Create 3 test intervals representing 5-minute periods
        interval_1 = InstrumentInterval(
            instrument_id=31,  # AAPL
            start_date_time=datetime(2025, 7, 1, 9, 30, 0),
            end_date_time=datetime(2025, 7, 1, 9, 35, 0),
            open=100.0, high=102.0, low=98.0, close=101.0, 
            traded_volume=5000, traded_dollar=500000, status='ok'
        )
        
        interval_2 = InstrumentInterval(
            instrument_id=31,  # AAPL
            start_date_time=datetime(2025, 7, 1, 9, 35, 0),
            end_date_time=datetime(2025, 7, 1, 9, 40, 0),
            open=105.0, high=107.0, low=103.0, close=106.0,
            traded_volume=10000, traded_dollar=1060000, status='ok'
        )
        
        interval_3 = InstrumentInterval(
            instrument_id=31,  # AAPL
            start_date_time=datetime(2025, 7, 1, 9, 40, 0),
            end_date_time=datetime(2025, 7, 1, 9, 45, 0),
            open=110.0, high=112.0, low=108.0, close=111.0,
            traded_volume=15000, traded_dollar=1665000, status='ok'
        )
        
        # Test 15-minute aggregation (3 x 5-minute intervals)
        duration_15m = TimeDuration.create_15_minutes()
        aggregated = duration_15m.aggregate_intervals([interval_1, interval_2, interval_3])
        
        # Verify OHLC aggregation rules
        assert aggregated.open == 100.0, (
            f"Open should be first interval's open (100.0), got {aggregated.open}"
        )
        assert aggregated.close == 111.0, (
            f"Close should be last interval's close (111.0), got {aggregated.close}"
        )
        assert aggregated.high == 112.0, (
            f"High should be max of all highs (112.0), got {aggregated.high}"
        )
        assert aggregated.low == 98.0, (
            f"Low should be min of all lows (98.0), got {aggregated.low}"
        )
        assert aggregated.traded_volume == 30000, (
            f"Volume should be sum (30000), got {aggregated.traded_volume}"
        )
        assert aggregated.traded_dollar == 3225000, (
            f"Traded dollar should be sum (3225000), got {aggregated.traded_dollar}"
        )
        
        # Verify time range
        assert aggregated.start_date_time == interval_1.start_date_time
        assert aggregated.end_date_time == interval_3.end_date_time
        assert aggregated.instrument_id == 31
        assert aggregated.status == 'ok'
        
    def test_aggregate_intervals_with_mixed_status(self):
        """Test aggregation when some intervals have non-ok status."""
        
        interval_1 = InstrumentInterval(
            instrument_id=31, start_date_time=datetime(2025, 7, 1, 9, 30, 0),
            end_date_time=datetime(2025, 7, 1, 9, 35, 0),
            open=100.0, high=102.0, low=98.0, close=101.0,
            traded_volume=5000, traded_dollar=500000, status='ok'
        )
        
        interval_2 = InstrumentInterval(
            instrument_id=31, start_date_time=datetime(2025, 7, 1, 9, 35, 0),
            end_date_time=datetime(2025, 7, 1, 9, 40, 0),
            open=105.0, high=107.0, low=103.0, close=106.0,
            traded_volume=10000, traded_dollar=1060000, status='missing'  # Non-ok status
        )
        
        # Test aggregation marks as 'unreliable' when any interval is not 'ok'
        duration_10m = TimeDuration.create_15_minutes()
        aggregated = duration_10m.aggregate_intervals([interval_1, interval_2])
        
        assert aggregated.status == 'unreliable', (
            f"Status should be 'unreliable' when any interval is not 'ok', got {aggregated.status}"
        )
        
    def test_aggregate_intervals_single_interval(self):
        """Test aggregation with single interval (should return identical values)."""
        
        single_interval = InstrumentInterval(
            instrument_id=31, start_date_time=datetime(2025, 7, 1, 9, 30, 0),
            end_date_time=datetime(2025, 7, 1, 9, 35, 0),
            open=100.0, high=102.0, low=98.0, close=101.0,
            traded_volume=5000, traded_dollar=500000, status='ok'
        )
        
        duration_5m = TimeDuration.create_5_minutes()
        aggregated = duration_5m.aggregate_intervals([single_interval])
        
        # Should be identical to input
        assert aggregated.open == single_interval.open
        assert aggregated.high == single_interval.high  
        assert aggregated.low == single_interval.low
        assert aggregated.close == single_interval.close
        assert aggregated.traded_volume == single_interval.traded_volume
        assert aggregated.traded_dollar == single_interval.traded_dollar
        assert aggregated.status == single_interval.status
        
    def test_aggregate_intervals_empty_list_raises_error(self):
        """Test that empty interval list raises appropriate error."""
        
        duration_5m = TimeDuration.create_5_minutes()
        
        with pytest.raises(ValueError, match="No intervals to aggregate"):
            duration_5m.aggregate_intervals([])
            
    def test_multiple_timeframe_aggregation_ratios(self):
        """Test that different timeframe ratios work correctly."""
        
        # Create 12 intervals (1 hour worth of 5-minute intervals)
        intervals = []
        for i in range(12):
            base_minute = 30 + i*5
            hour_offset = base_minute // 60
            minute = base_minute % 60
            start_time = datetime(2025, 7, 1, 9 + hour_offset, minute, 0)
            
            end_minute = 35 + i*5
            end_hour_offset = end_minute // 60
            end_minute = end_minute % 60
            end_time = datetime(2025, 7, 1, 9 + end_hour_offset, end_minute, 0)
            
            intervals.append(InstrumentInterval(
                instrument_id=31,
                start_date_time=start_time,
                end_date_time=end_time,
                open=100.0 + i,          # Increasing opens
                high=102.0 + i,          # Increasing highs
                low=98.0 + i,            # Increasing lows  
                close=101.0 + i,         # Increasing closes
                traded_volume=1000 + i*100,  # Increasing volume
                traded_dollar=(1000 + i*100) * (101.0 + i),
                status='ok'
            ))
        
        # Test 15-minute aggregation (3 intervals)
        duration_15m = TimeDuration.create_15_minutes()
        agg_15m = duration_15m.aggregate_intervals(intervals[:3])
        
        assert agg_15m.open == 100.0    # First interval's open
        assert agg_15m.close == 103.0   # Third interval's close (101.0 + 2)
        assert agg_15m.high == 104.0    # Max high (102.0 + 2)
        assert agg_15m.low == 98.0      # Min low (98.0 + 0)
        
        # Test 60-minute aggregation (12 intervals)
        duration_60m = TimeDuration.create_60_minutes()
        agg_60m = duration_60m.aggregate_intervals(intervals)
        
        assert agg_60m.open == 100.0     # First interval's open
        assert agg_60m.close == 112.0    # Last interval's close (101.0 + 11)
        assert agg_60m.high == 113.0     # Max high (102.0 + 11)
        assert agg_60m.low == 98.0       # Min low (98.0 + 0)
        
        # Volume should be sum of all 12 intervals
        expected_volume = sum(1000 + i*100 for i in range(12))
        assert agg_60m.traded_volume == expected_volume