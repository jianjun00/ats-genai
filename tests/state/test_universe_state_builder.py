import pytest
from datetime import datetime, timedelta, date
from unittest.mock import Mock, MagicMock
from state.universe_state_builder import UniverseStateBuilder
from trading.universe import Universe
from trading.market_data_manager import MarketDataManager
from state.instrument_interval import InstrumentInterval
from trading.indicator_config import IndicatorConfig
from trading.indicator import PL, UniverseState
from state.universe_interval import UniverseInterval
from state.indicator_interval import IndicatorInterval

def test_universe_state_builder_init():
    """Test UniverseStateBuilder initialization."""
    # Create a test universe
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2, 3])

    # Test with defaults
    builder = UniverseStateBuilder(universe)
    assert builder.universe_state.intervals == []
    assert builder.universe_state.instrument_history == {}
    assert builder.universe == universe
    assert isinstance(builder.indicator_config, IndicatorConfig)
    assert isinstance(builder.market_data_manager, MarketDataManager)
    assert isinstance(builder.universe_state, UniverseState)
    
    # Test with custom indicator config and MarketDataManager
    custom_config = IndicatorConfig.basic_config()
    custom_manager = Mock(spec=MarketDataManager)
    builder_with_config = UniverseStateBuilder(universe, custom_config, custom_manager)
    assert builder_with_config.indicator_config == custom_config
    assert builder_with_config.market_data_manager is custom_manager
    assert builder_with_config.universe is universe


def test_build_next_interval():
    """Test building next interval with market data."""
    # Create a test universe
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2, 3])
    
    # Create mock MarketDataManager
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0, 'volume': 1000.0, 'dollar_volume': 103000.0},
        2: {'open': 200.0, 'high': 210.0, 'low': 195.0, 'close': 208.0, 'volume': 500.0, 'dollar_volume': 104000.0},
        3: None  # No data for instrument 3
    }
    
    builder = UniverseStateBuilder(universe, None, mock_manager)
    
    start_time = datetime(2023, 1, 1, 9, 30)
    end_time = datetime(2023, 1, 1, 10, 30)
    
    # Build next interval (uses universe's instrument_ids)
    interval = builder.build_next_interval(start_time, end_time)
    
    # Verify MarketDataManager was called correctly with universe's instrument_ids
    mock_manager.get_ohlc_batch.assert_called_once_with([1, 2, 3], start_time, end_time)
    
    # Verify interval structure
    assert isinstance(interval, UniverseInterval)
    assert interval.start_date_time == start_time
    assert interval.end_date_time == end_time
    
    # Verify instrument intervals (should only have data for instruments 1 and 2)
    assert len(interval.instrument_intervals) == 2
    assert 1 in interval.instrument_intervals
    assert 2 in interval.instrument_intervals
    assert 3 not in interval.instrument_intervals  # No data available
    
    # Verify instrument 1 data
    inst1 = interval.instrument_intervals[1]
    assert isinstance(inst1, InstrumentInterval)
    assert inst1.instrument_id == 1
    assert inst1.start_date_time == start_time
    assert inst1.end_date_time == end_time
    assert inst1.open == 100.0
    assert inst1.high == 105.0
    assert inst1.low == 99.0
    assert inst1.close == 104.0
    assert inst1.traded_volume == 1000.0
    assert inst1.traded_dollar == 103000.0
    assert inst1.status == 'ok'
    
    # Verify instrument 2 data
    inst2 = interval.instrument_intervals[2]
    assert isinstance(inst2, InstrumentInterval)
    assert inst2.instrument_id == 2
    assert inst2.open == 200.0
    assert inst2.high == 210.0
    assert inst2.low == 195.0
    assert inst2.close == 208.0
    assert inst2.traded_volume == 500.0
    assert inst2.traded_dollar == 104000.0
    assert inst2.status == 'ok'


def test_build_next_interval_with_minimal_data():
    """Test building next interval with minimal OHLC data (no volume info)."""
    # Create a test universe with one instrument
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0}  # No volume/dollar_volume
    }
    
    builder = UniverseStateBuilder(universe, None, mock_manager)
    
    start_time = datetime(2023, 1, 1, 9, 30)
    end_time = datetime(2023, 1, 1, 10, 30)
    
    interval = builder.build_next_interval(start_time, end_time)
    
    # Verify defaults are used for missing volume data
    inst1 = interval.instrument_intervals[1]
    assert inst1.traded_volume == 0.0
    assert inst1.traded_dollar == 0.0


def test_add_next_interval():
    """Test adding next interval to the builder."""
    # Create a test universe
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0}
    }
    
    builder = UniverseStateBuilder(universe, None, mock_manager)
    
    start_time = datetime(2023, 1, 1, 9, 30)
    end_time = datetime(2023, 1, 1, 10, 30)
    
    # Initially no intervals
    assert len(builder.universe_state.intervals) == 0
    
    # Add next interval (uses universe's instruments)
    builder.add_next_interval(start_time, end_time)
    
    # Verify interval was added
    assert len(builder.universe_state.intervals) == 1
    assert builder.universe_state.intervals[0].start_date_time == start_time
    assert builder.universe_state.intervals[0].end_date_time == end_time
    assert 1 in builder.universe_state.intervals[0].instrument_intervals


def test_build_universe_state():
    """Test building UniverseState from intervals."""
    # Create a test universe
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    
    builder = UniverseStateBuilder(universe)
    
    # Add some intervals manually
    interval1 = UniverseInterval(
        start_date_time=datetime(2023, 1, 1, 9, 30),
        end_date_time=datetime(2023, 1, 1, 10, 30)
    )
    interval2 = UniverseInterval(
        start_date_time=datetime(2023, 1, 1, 10, 30),
        end_date_time=datetime(2023, 1, 1, 11, 30)
    )
    
    builder.add_interval(interval1)
    builder.add_interval(interval2)
    
    # Build UniverseState
    universe_state = builder.build()
    
    # Verify the built UniverseState
    assert len(universe_state.intervals) == 2
    assert universe_state.intervals[0] is builder.universe_state.intervals[0]  # Should be the same reference now
    assert universe_state.intervals[1] is builder.universe_state.intervals[1]  # Should be the same reference now
    assert universe_state.intervals[0].start_date_time == interval1.start_date_time
    assert universe_state.intervals[1].start_date_time == interval2.start_date_time


def test_reset():
    """Test resetting the builder."""
    # Create a test universe
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2])
    
    builder = UniverseStateBuilder(universe)
    
    # Add an interval
    interval = UniverseInterval(
        start_date_time=datetime(2023, 1, 1, 9, 30),
        end_date_time=datetime(2023, 1, 1, 10, 30)
    )
    builder.add_interval(interval)
    
    assert len(builder.universe_state.intervals) == 1
    
    # Reset
    builder.reset()
    
    assert len(builder.universe_state.intervals) == 0


def test_integration_workflow():
    """Test complete workflow of building multiple intervals."""
    # Create a test universe
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    
    mock_manager = Mock(spec=MarketDataManager)
    
    # Mock different data for different time periods
    def mock_get_ohlc_batch(instrument_ids, start_time, end_time):
        if start_time.hour == 9:  # First interval
            return {1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0}}
        elif start_time.hour == 10:  # Second interval
            return {1: {'open': 104.0, 'high': 108.0, 'low': 103.0, 'close': 107.0}}
        else:
            return {}
    
    mock_manager.get_ohlc_batch.side_effect = mock_get_ohlc_batch
    
    builder = UniverseStateBuilder(universe, None, mock_manager)
    
    # Add two intervals (uses universe's instruments)
    builder.add_next_interval(datetime(2023, 1, 1, 9, 30), datetime(2023, 1, 1, 10, 30))
    builder.add_next_interval(datetime(2023, 1, 1, 10, 30), datetime(2023, 1, 1, 11, 30))
    
    # Build universe state
    universe_state = builder.build()
    
    # Verify we have two intervals with correct data
    assert len(universe_state.intervals) == 2
    
    # First interval
    assert universe_state.intervals[0].instrument_intervals[1].open == 100.0
    assert universe_state.intervals[0].instrument_intervals[1].close == 104.0
    
    # Second interval
    assert universe_state.intervals[1].instrument_intervals[1].open == 104.0
    assert universe_state.intervals[1].instrument_intervals[1].close == 107.0
    
    # Verify MarketDataManager was called twice
    assert mock_manager.get_ohlc_batch.call_count == 2


def test_universe_advance_to():
    """Test advancing universe date and instruments via builder's universe."""
    # Create initial universe
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2])
    builder = UniverseStateBuilder(universe)
    
    # Verify initial state
    assert builder.universe.current_date == date(2023, 1, 1)
    assert builder.universe.instrument_ids == [1, 2]
    
    # Advance date only
    new_date = date(2023, 1, 2)
    builder.universe.advanceTo(new_date)
    assert builder.universe.current_date == new_date
    assert builder.universe.instrument_ids == [1, 2]  # Should remain the same
    
    # Advance date and instruments
    newer_date = date(2023, 1, 3)
    new_instruments = [1, 2, 3, 4]
    builder.universe.advanceTo(newer_date, new_instruments)
    assert builder.universe.current_date == newer_date
    assert builder.universe.instrument_ids == new_instruments


def test_add_next_interval_with_universe_advance():
    """Test adding interval with universe advance using consolidated method."""
    # Create initial universe
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0},
        2: {'open': 200.0, 'high': 210.0, 'low': 195.0, 'close': 208.0}
    }
    
    builder = UniverseStateBuilder(universe, None, mock_manager)
    
    start_time = datetime(2023, 1, 2, 9, 30)
    end_time = datetime(2023, 1, 2, 10, 30)
    
    # Add interval with universe advance (new date and instruments)
    new_date = date(2023, 1, 2)
    new_instruments = [1, 2]
    builder.add_next_interval(
        start_time, end_time, new_date, new_instruments
    )
    
    # Verify universe was advanced
    assert builder.universe.current_date == new_date
    assert builder.universe.instrument_ids == new_instruments
    
    # Verify interval was added with new instruments
    assert len(builder.universe_state.intervals) == 1
    assert 1 in builder.universe_state.intervals[0].instrument_intervals
    assert 2 in builder.universe_state.intervals[0].instrument_intervals
    
    # Verify MarketDataManager was called with updated instruments
    mock_manager.get_ohlc_batch.assert_called_once_with([1, 2], start_time, end_time)


def test_add_next_interval_basic():
    """Test basic add_next_interval usage without optional parameters."""
    # Create initial universe
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2])
    
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0},
        2: {'open': 200.0, 'high': 210.0, 'low': 195.0, 'close': 208.0}
    }
    
    builder = UniverseStateBuilder(universe, None, mock_manager)
    
    start_time = datetime(2023, 1, 1, 9, 30)
    end_time = datetime(2023, 1, 1, 10, 30)
    
    # Add interval without universe changes (basic usage)
    builder.add_next_interval(start_time, end_time)
    
    # Verify universe was not changed
    assert builder.universe.current_date == date(2023, 1, 1)
    assert builder.universe.instrument_ids == [1, 2]
    
    # Verify interval was added with current instruments
    assert len(builder.universe_state.intervals) == 1
    assert 1 in builder.universe_state.intervals[0].instrument_intervals
    assert 2 in builder.universe_state.intervals[0].instrument_intervals
    
    # Verify MarketDataManager was called with current instruments
    mock_manager.get_ohlc_batch.assert_called_once_with([1, 2], start_time, end_time)


def test_indicator_computation_with_basic_config():
    """Test indicator computation with basic configuration."""
    # Create universe and basic indicator config
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    indicator_config = IndicatorConfig.basic_config()
    
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0}
    }
    
    builder = UniverseStateBuilder(universe, indicator_config, mock_manager)
    
    # Add first interval
    start_time1 = datetime(2023, 1, 1, 9, 30)
    end_time1 = datetime(2023, 1, 1, 10, 30)
    builder.add_next_interval(start_time1, end_time1)
    
    # Verify instrument history was updated
    assert 1 in builder.universe_state.instrument_history
    assert len(builder.universe_state.instrument_history[1]) == 1
    
    # Build universe state
    universe_state = builder.build()
    
    # Verify indicator intervals were computed
    assert 1 in universe_state.indicator_intervals
    indicator_interval = universe_state.indicator_intervals[1]
    
    # Verify basic indicators were computed
    expected_indicators = ['OneOneDot', 'OneOneHigh', 'OneOneLow']
    for indicator_name in expected_indicators:
        assert indicator_interval.has_indicator(indicator_name)
        assert indicator_interval.is_indicator_valid(indicator_name)
    
    # Verify OneOneDot calculation
    expected_oneonedot = (105.0 + 99.0 + 104.0) / 3.0  # (high + low + close) / 3
    assert indicator_interval.get_indicator_value('OneOneDot') == expected_oneonedot
    
    # Verify OneOneHigh calculation (2 * OneOneDot - low)
    expected_oneonehigh = 2 * expected_oneonedot - 99.0
    assert indicator_interval.get_indicator_value('OneOneHigh') == expected_oneonehigh
    
    # Verify OneOneLow calculation (2 * OneOneDot - high)
    expected_oneonelow = 2 * expected_oneonedot - 105.0
    assert indicator_interval.get_indicator_value('OneOneLow') == expected_oneonelow


def test_indicator_computation_with_default_config():
    """Test indicator computation with default configuration."""
    # Create universe and default indicator config
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    indicator_config = IndicatorConfig.default_config()
    
    mock_manager = Mock(spec=MarketDataManager)
    # Set up data for multiple intervals (EBot/ETop need at least 4 intervals)
    mock_manager.get_ohlc_batch.side_effect = [
        {1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0}},
        {1: {'open': 104.0, 'high': 108.0, 'low': 103.0, 'close': 107.0}},
        {1: {'open': 107.0, 'high': 110.0, 'low': 106.0, 'close': 109.0}},
        {1: {'open': 109.0, 'high': 112.0, 'low': 108.0, 'close': 111.0}}
    ]
    
    builder = UniverseStateBuilder(universe, indicator_config, mock_manager)
    
    # Add four intervals (needed for EBot, ETop to be valid)
    for i in range(4):
        start_time = datetime(2023, 1, 1, 9 + i, 30)
        end_time = datetime(2023, 1, 1, 10 + i, 30)
        builder.add_next_interval(start_time, end_time)
    
    # Build universe state
    universe_state = builder.build()
    
    # Verify indicator intervals were computed
    assert 1 in universe_state.indicator_intervals
    indicator_interval = universe_state.indicator_intervals[1]
    
    # Verify all default indicators were computed
    expected_indicators = ['PL', 'OneOneHigh', 'OneOneLow', 'OneOneDot', 'EBot', 'ETop']
    for indicator_name in expected_indicators:
        assert indicator_interval.has_indicator(indicator_name)
        # All should be valid since we have sufficient data
        assert indicator_interval.is_indicator_valid(indicator_name)
        assert indicator_interval.get_indicator_value(indicator_name) is not None


def test_indicator_computation_with_empty_config():
    """Test that no indicators are computed with empty configuration."""
    # Create universe and empty indicator config
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    indicator_config = IndicatorConfig.empty_config()
    
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0}
    }
    
    builder = UniverseStateBuilder(universe, indicator_config, mock_manager)
    
    # Add interval
    start_time = datetime(2023, 1, 1, 9, 30)
    end_time = datetime(2023, 1, 1, 10, 30)
    builder.add_next_interval(start_time, end_time)
    
    # Build universe state
    universe_state = builder.build()
    
    # Verify indicator intervals were computed but are empty
    assert 1 in universe_state.indicator_intervals
    indicator_interval = universe_state.indicator_intervals[1]
    assert len(indicator_interval.get_indicator_names()) == 0


def test_indicator_computation_multiple_instruments():
    """Test indicator computation for multiple instruments."""
    # Create universe with multiple instruments
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2])
    indicator_config = IndicatorConfig.basic_config()
    
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0},
        2: {'open': 200.0, 'high': 210.0, 'low': 195.0, 'close': 208.0}
    }
    
    builder = UniverseStateBuilder(universe, indicator_config, mock_manager)
    
    # Add interval
    start_time = datetime(2023, 1, 1, 9, 30)
    end_time = datetime(2023, 1, 1, 10, 30)
    builder.add_next_interval(start_time, end_time)
    
    # Build universe state
    universe_state = builder.build()
    
    # Verify indicator intervals were computed for both instruments
    assert 1 in universe_state.indicator_intervals
    assert 2 in universe_state.indicator_intervals
    
    # Verify indicators for instrument 1
    indicator_interval_1 = universe_state.indicator_intervals[1]
    expected_oneonedot_1 = (105.0 + 99.0 + 104.0) / 3.0
    assert indicator_interval_1.get_indicator_value('OneOneDot') == expected_oneonedot_1
    
    # Verify indicators for instrument 2
    indicator_interval_2 = universe_state.indicator_intervals[2]
    expected_oneonedot_2 = (210.0 + 195.0 + 208.0) / 3.0
    assert indicator_interval_2.get_indicator_value('OneOneDot') == expected_oneonedot_2


def test_indicator_computation_with_insufficient_data():
    """Test indicator computation when there's insufficient historical data."""
    # Create universe and config that requires multiple intervals
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    indicator_config = IndicatorConfig()
    indicator_config.add_indicator('PL', PL)  # PL requires 3 intervals
    
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0}
    }
    
    builder = UniverseStateBuilder(universe, indicator_config, mock_manager)
    
    # Add only one interval (insufficient for PL)
    start_time = datetime(2023, 1, 1, 9, 30)
    end_time = datetime(2023, 1, 1, 10, 30)
    builder.add_next_interval(start_time, end_time)
    
    # Build universe state
    universe_state = builder.build()
    
    # Verify indicator intervals were computed
    assert 1 in universe_state.indicator_intervals
    indicator_interval = universe_state.indicator_intervals[1]
    
    # Verify PL indicator is invalid due to insufficient data
    assert indicator_interval.has_indicator('PL')
    assert not indicator_interval.is_indicator_valid('PL')
    assert indicator_interval.get_indicator_value('PL') is None
    assert indicator_interval.get_indicator_status('PL') == 'invalid'


def test_indicator_computation_with_invalid_instrument_data():
    """Test indicator computation when instrument data is invalid."""
    # Create universe and basic indicator config
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    indicator_config = IndicatorConfig.basic_config()
    
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0}
    }
    
    builder = UniverseStateBuilder(universe, indicator_config, mock_manager)
    
    # Add a normal interval first to establish the time period
    start_time = datetime(2023, 1, 1, 9, 30)
    end_time = datetime(2023, 1, 1, 10, 30)
    builder.add_next_interval(start_time, end_time)
    
    # Manually replace the valid interval with an invalid one
    invalid_interval = InstrumentInterval(
        instrument_id=1,
        start_date_time=start_time,
        end_date_time=end_time,
        open=100.0, high=105.0, low=99.0, close=104.0,
        traded_volume=1000.0, traded_dollar=103000.0,
        status='invalid'  # Mark as invalid
    )
    
    # Replace the valid interval with invalid one
    builder.universe_state.instrument_history[1] = [invalid_interval]
    
    # Build universe state
    universe_state = builder.build()
    
    # Verify indicator intervals were computed
    assert 1 in universe_state.indicator_intervals
    indicator_interval = universe_state.indicator_intervals[1]
    
    # Verify all indicators are invalid due to invalid input data
    expected_indicators = ['OneOneDot', 'OneOneHigh', 'OneOneLow']
    for indicator_name in expected_indicators:
        assert indicator_interval.has_indicator(indicator_name)
        assert not indicator_interval.is_indicator_valid(indicator_name)
        assert indicator_interval.get_indicator_value(indicator_name) is None


def test_instrument_history_management():
    """Test that instrument history is properly managed."""
    # Create universe and basic indicator config
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2])
    indicator_config = IndicatorConfig.basic_config()
    
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.side_effect = [
        {1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0},
         2: {'open': 200.0, 'high': 210.0, 'low': 195.0, 'close': 208.0}},
        {1: {'open': 104.0, 'high': 108.0, 'low': 103.0, 'close': 107.0},
         2: {'open': 208.0, 'high': 215.0, 'low': 205.0, 'close': 212.0}}
    ]
    
    builder = UniverseStateBuilder(universe, indicator_config, mock_manager)
    
    # Add first interval
    builder.add_next_interval(datetime(2023, 1, 1, 9, 30), datetime(2023, 1, 1, 10, 30))
    
    # Verify history has one interval for each instrument
    assert len(builder.universe_state.instrument_history[1]) == 1
    assert len(builder.universe_state.instrument_history[2]) == 1
    
    # Add second interval
    builder.add_next_interval(datetime(2023, 1, 1, 10, 30), datetime(2023, 1, 1, 11, 30))
    
    # Verify history has two intervals for each instrument
    assert len(builder.universe_state.instrument_history[1]) == 2
    assert len(builder.universe_state.instrument_history[2]) == 2
    
    # Test reset clears history
    builder.reset()
    assert builder.universe_state.instrument_history == {}
    assert len(builder.universe_state.intervals) == 0


def test_universe_state_with_indicator_intervals():
    """Test UniverseState includes indicator intervals correctly."""
    # Create universe and basic indicator config
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1])
    indicator_config = IndicatorConfig.basic_config()
    
    mock_manager = Mock(spec=MarketDataManager)
    mock_manager.get_ohlc_batch.return_value = {
        1: {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0}
    }
    
    builder = UniverseStateBuilder(universe, indicator_config, mock_manager)
    
    # Add interval
    start_time = datetime(2023, 1, 1, 9, 30)
    end_time = datetime(2023, 1, 1, 10, 30)
    builder.add_next_interval(start_time, end_time)
    
    # Build universe state
    universe_state = builder.build()
    
    # Verify UniverseState structure
    assert len(universe_state.intervals) == 1
    assert len(universe_state.indicator_intervals) == 1
    assert 1 in universe_state.indicator_intervals
    
    # Verify indicator interval details
    indicator_interval = universe_state.indicator_intervals[1]
    assert indicator_interval.instrument_id == 1
    assert indicator_interval.start_date_time == start_time
    assert indicator_interval.end_date_time == end_time
    assert len(indicator_interval.get_indicator_names()) == 3  # Basic config has 3 indicators
