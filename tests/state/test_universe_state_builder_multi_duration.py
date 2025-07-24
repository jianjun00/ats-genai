import pytest
from datetime import datetime, date
from unittest.mock import Mock
from src.state.universe_state_builder import UniverseStateBuilder
from trading.universe import Universe
from trading.market_data_manager import MarketDataManager
from trading.time_duration import TimeDuration
from trading.indicator_config import IndicatorConfig
from state.universe_interval import UniverseInterval
from state.instrument_interval import InstrumentInterval


import pytest

@pytest.mark.skip(reason="Duration logic moved to Environment; UniverseStateBuilder no longer manages durations.")
def test_multi_duration_initialization():
    """Test UniverseStateBuilder initialization with multi-duration support."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2])
    
    # Test with default durations
    builder = UniverseStateBuilder()
    assert builder.get_base_duration().get_duration_string() == "5m"
    assert len(builder.get_target_durations()) == 1
    assert builder.get_target_durations()[0].get_duration_string() == "5m"
    
    # Test with custom base and target durations
    base_duration = TimeDuration.create_5_minutes()
    target_durations = [
        TimeDuration.create_5_minutes(),
        TimeDuration.create_15_minutes(),
        TimeDuration.create_60_minutes()
    ]
    
    builder_custom = UniverseStateBuilder()
    
    assert builder_custom.get_base_duration().get_duration_string() == "5m"
    target_strings = [d.get_duration_string() for d in builder_custom.get_target_durations()]
    assert target_strings == ["5m", "15m", "60m"]


@pytest.mark.skip(reason="Duration logic moved to Environment; UniverseStateBuilder no longer manages durations.")
def test_duration_compatibility_validation():
    """Test validation of duration compatibility."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    
    # Valid case: 15m and 60m are multiples of 5m
    base_duration = TimeDuration.create_5_minutes()
    valid_targets = [
        TimeDuration.create_5_minutes(),
        TimeDuration.create_15_minutes(),
        TimeDuration.create_60_minutes()
    ]
    
    # Should not raise an exception
    builder = UniverseStateBuilder(
        universe,
        base_duration=base_duration,
        target_durations=valid_targets
    )
    
    # Invalid case: 7 minutes would not be a multiple of 5 minutes
    # We can't easily test this without creating a custom duration, 
    # but we can test the validation logic with set_target_durations
    
    # Test that daily durations are allowed (skip validation for date-based)
    mixed_targets = [
        TimeDuration.create_5_minutes(),
        TimeDuration.create_15_minutes(),
        TimeDuration.create_daily()
    ]
    
    builder.set_target_durations(mixed_targets)
    target_strings = [d.get_duration_string() for d in builder.get_target_durations()]
    assert target_strings == ["5m", "15m", "1d"]


@pytest.mark.skip(reason="Duration logic moved to Environment; UniverseStateBuilder no longer manages durations.")
def test_build_multi_duration_intervals():
    """Test building intervals for multiple durations."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2])
    
    # Mock market data manager
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0, 'volume': 1000.0},
        2: {'open': 200.0, 'high': 210.0, 'low': 195.0, 'close': 208.0, 'volume': 500.0}
    }
    
    # Set up builder with multiple target durations
    target_durations = [
        TimeDuration.create_5_minutes(),
        TimeDuration.create_15_minutes(),
        TimeDuration.create_60_minutes()
    ]
    
    builder = UniverseStateBuilder(
        universe,
        market_data_manager=mock_manager,
        base_duration=TimeDuration.create_5_minutes(),
        target_durations=target_durations
    )
    
    start_time = datetime(2023, 1, 1, 9, 30)
    
    # Build intervals for all durations
    intervals = builder.build_multi_duration_intervals(start_time)
    
    # Verify we have intervals for all target durations
    assert len(intervals) == 3
    assert "5m" in intervals
    assert "15m" in intervals
    assert "60m" in intervals
    
    # Verify each interval has correct time bounds
    assert intervals["5m"].start_date_time == start_time
    assert intervals["5m"].end_date_time == datetime(2023, 1, 1, 9, 35)
    
    assert intervals["15m"].start_date_time == start_time
    assert intervals["15m"].end_date_time == datetime(2023, 1, 1, 9, 45)
    
    assert intervals["60m"].start_date_time == start_time
    assert intervals["60m"].end_date_time == datetime(2023, 1, 1, 10, 30)
    
    # Verify market data manager was called for each duration
    assert mock_manager.get_ohlc_batch.call_count == 3


@pytest.mark.skip(reason="Duration logic moved to Environment; UniverseStateBuilder no longer manages durations.")
def test_add_multi_duration_intervals():
    """Test adding intervals for multiple durations to the builder."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0}
    }
    
    target_durations = [
        TimeDuration.create_5_minutes(),
        TimeDuration.create_15_minutes()
    ]
    
    builder = UniverseStateBuilder(
        universe,
        market_data_manager=mock_manager,
        target_durations=target_durations
    )
    
    start_time = datetime(2023, 1, 1, 9, 30)
    
    # Add intervals for all durations
    builder.add_multi_duration_intervals(start_time)
    
    # Verify intervals were added to universe state
    assert len(builder.universe_state.intervals) == 2
    
    # Verify intervals have correct durations
    intervals = builder.universe_state.intervals
    durations_found = set()
    
    for interval in intervals:
        if interval.end_date_time == datetime(2023, 1, 1, 9, 35):
            durations_found.add("5m")
        elif interval.end_date_time == datetime(2023, 1, 1, 9, 45):
            durations_found.add("15m")
    
    assert durations_found == {"5m", "15m"}


def test_build_aggregated_interval():
    """Test building aggregated intervals from base intervals."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    builder = UniverseStateBuilder()
    
    # Create three 5-minute base intervals to aggregate into 15-minute interval
    base_intervals = []
    
    for i in range(3):
        start_time = datetime(2023, 1, 1, 9, 30 + i * 5)
        end_time = datetime(2023, 1, 1, 9, 35 + i * 5)
        
        instrument_intervals = {
            1: InstrumentInterval(
                instrument_id=1,
                start_date_time=start_time,
                end_date_time=end_time,
                open=100.0 + i,
                high=105.0 + i,
                low=99.0 + i,
                close=104.0 + i,
                traded_volume=1000.0,
                traded_dollar=104000.0,
                status='ok'
            )
        }
        
        base_interval = UniverseInterval(
            start_date_time=start_time,
            end_date_time=end_time,
            instrument_intervals=instrument_intervals
        )
        base_intervals.append(base_interval)
    
    # Aggregate into 15-minute interval
    target_duration = TimeDuration.create_15_minutes()
    aggregated = builder.build_aggregated_interval(base_intervals, target_duration)
    
    # Verify aggregated interval properties
    assert aggregated.start_date_time == datetime(2023, 1, 1, 9, 30)
    assert aggregated.end_date_time == datetime(2023, 1, 1, 9, 45)
    
    # Verify aggregated instrument data
    assert 1 in aggregated.instrument_intervals
    inst_interval = aggregated.instrument_intervals[1]
    
    # OHLC aggregation: Open from first, Close from last, High/Low from all
    assert inst_interval.open == 100.0  # From first interval
    assert inst_interval.close == 106.0  # From last interval (104.0 + 2)
    assert inst_interval.high == 107.0   # Max high (105.0 + 2)
    assert inst_interval.low == 99.0     # Min low (99.0 + 0)
    
    # Volume aggregation: Sum of all
    assert inst_interval.traded_volume == 3000.0  # 1000.0 * 3
    assert inst_interval.traded_dollar == 312000.0  # 104000.0 * 3
    assert inst_interval.status == 'ok'


def test_build_aggregated_interval_with_mixed_status():
    """Test aggregated interval with mixed status intervals."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    builder = UniverseStateBuilder()
    
    # Create intervals with mixed status
    base_intervals = []
    statuses = ['ok', 'invalid', 'ok']
    
    for i, status in enumerate(statuses):
        start_time = datetime(2023, 1, 1, 9, 30 + i * 5)
        end_time = datetime(2023, 1, 1, 9, 35 + i * 5)
        
        instrument_intervals = {
            1: InstrumentInterval(
                instrument_id=1,
                start_date_time=start_time,
                end_date_time=end_time,
                open=100.0, high=105.0, low=99.0, close=104.0,
                traded_volume=1000.0, traded_dollar=104000.0,
                status=status
            )
        }
        
        base_interval = UniverseInterval(
            start_date_time=start_time,
            end_date_time=end_time,
            instrument_intervals=instrument_intervals
        )
        base_intervals.append(base_interval)
    
    # Aggregate intervals
    target_duration = TimeDuration.create_15_minutes()
    aggregated = builder.build_aggregated_interval(base_intervals, target_duration)
    
    # Verify that status is 'invalid' due to mixed statuses
    inst_interval = aggregated.instrument_intervals[1]
    assert inst_interval.status == 'invalid'


def test_build_aggregated_interval_empty_list():
    """Test that aggregating empty list raises ValueError."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    builder = UniverseStateBuilder()
    
    target_duration = TimeDuration.create_15_minutes()
    
    with pytest.raises(ValueError, match="Cannot aggregate empty list of intervals"):
        builder.build_aggregated_interval([], target_duration)


@pytest.mark.skip(reason="Duration logic moved to Environment; UniverseStateBuilder no longer manages durations.")
def test_set_target_durations():
    """Test setting new target durations."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    builder = UniverseStateBuilder()
    
    # Initially has default 5m duration
    assert len(builder.get_target_durations()) == 1
    
    # Set new target durations
    new_durations = [
        TimeDuration.create_5_minutes(),
        TimeDuration.create_15_minutes(),
        TimeDuration.create_daily()
    ]
    
    builder.set_target_durations(new_durations)
    
    # Verify new durations are set
    target_strings = [d.get_duration_string() for d in builder.get_target_durations()]
    assert target_strings == ["5m", "15m", "1d"]


@pytest.mark.skip(reason="Duration logic moved to Environment; UniverseStateBuilder no longer manages durations.")
def test_multi_duration_with_universe_advance():
    """Test multi-duration intervals with universe advancement."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        2: {'open': 200.0, 'high': 210.0, 'low': 195.0, 'close': 208.0}
    }
    
    target_durations = [TimeDuration.create_5_minutes(), TimeDuration.create_15_minutes()]
    builder = UniverseStateBuilder(
        universe,
        market_data_manager=mock_manager,
        target_durations=target_durations
    )
    
    start_time = datetime(2023, 1, 2, 9, 30)
    new_date = date(2023, 1, 2)
    new_instrument_ids = [2]
    
    # Add intervals with universe advancement
    builder.add_multi_duration_intervals(start_time, new_date, new_instrument_ids)
    
    # Verify universe was advanced
    assert universe.current_date == new_date
    assert universe.instrument_ids == new_instrument_ids
    
    # Verify intervals were created for new instrument
    assert len(builder.universe_state.intervals) == 2
    for interval in builder.universe_state.intervals:
        assert 2 in interval.instrument_intervals
        assert 1 not in interval.instrument_intervals  # Old instrument not present


@pytest.mark.skip(reason="Duration logic moved to Environment; UniverseStateBuilder no longer manages durations.")
def test_multi_duration_integration_with_indicators():
    """Test multi-duration functionality with indicator computation."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    indicator_config = IndicatorConfig.basic_config()
    
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0}
    }
    
    target_durations = [TimeDuration.create_5_minutes(), TimeDuration.create_15_minutes()]
    builder = UniverseStateBuilder(
        universe,
        indicator_config=indicator_config,
        market_data_manager=mock_manager,
        target_durations=target_durations
    )
    
    start_time = datetime(2023, 1, 1, 9, 30)
    
    # Add multi-duration intervals
    builder.add_multi_duration_intervals(start_time)
    
    # Build universe state with indicators
    universe_state = builder.build()
    
    # Verify intervals and indicators
    assert len(universe_state.intervals) == 2
    assert len(universe_state.indicator_intervals) == 1  # Computed for latest interval
    assert 1 in universe_state.indicator_intervals
    
    # Verify indicator computation worked
    indicator_interval = universe_state.indicator_intervals[1]
    expected_indicators = ['OneOneDot', 'OneOneHigh', 'OneOneLow']
    for indicator_name in expected_indicators:
        assert indicator_interval.has_indicator(indicator_name)
