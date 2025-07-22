import pytest
from datetime import datetime, timedelta, date
from unittest.mock import Mock, MagicMock
from trading.universe_state_builder import UniverseStateBuilder
from trading.market_data_manager import MarketDataManager
from trading.universe_interval import UniverseInterval
from trading.instrument_interval import InstrumentInterval
from trading.universe import Universe


def test_universe_state_builder_init():
    """Test UniverseStateBuilder initialization."""
    # Create a test universe
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2, 3])
    
    # Test with default MarketDataManager
    builder = UniverseStateBuilder(universe)
    assert builder.intervals == []
    assert builder.universe is universe
    assert isinstance(builder.market_data_manager, MarketDataManager)
    
    # Test with custom MarketDataManager
    custom_manager = Mock(spec=MarketDataManager)
    builder = UniverseStateBuilder(universe, custom_manager)
    assert builder.market_data_manager is custom_manager
    assert builder.universe is universe


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
    
    builder = UniverseStateBuilder(universe, mock_manager)
    
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
    
    builder = UniverseStateBuilder(universe, mock_manager)
    
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
    
    builder = UniverseStateBuilder(universe, mock_manager)
    
    start_time = datetime(2023, 1, 1, 9, 30)
    end_time = datetime(2023, 1, 1, 10, 30)
    
    # Initially no intervals
    assert len(builder.intervals) == 0
    
    # Add next interval (uses universe's instruments)
    builder.add_next_interval(start_time, end_time)
    
    # Verify interval was added
    assert len(builder.intervals) == 1
    assert builder.intervals[0].start_date_time == start_time
    assert builder.intervals[0].end_date_time == end_time
    assert 1 in builder.intervals[0].instrument_intervals


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
    
    # Verify UniverseState
    assert len(universe_state.intervals) == 2
    assert universe_state.intervals[0] is not interval1  # Should be a copy
    assert universe_state.intervals[1] is not interval2  # Should be a copy
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
    
    assert len(builder.intervals) == 1
    
    # Reset
    builder.reset()
    
    assert len(builder.intervals) == 0


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
    
    builder = UniverseStateBuilder(universe, mock_manager)
    
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
    
    builder = UniverseStateBuilder(universe, mock_manager)
    
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
    assert len(builder.intervals) == 1
    interval = builder.intervals[0]
    assert 1 in interval.instrument_intervals
    assert 2 in interval.instrument_intervals
    
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
    
    builder = UniverseStateBuilder(universe, mock_manager)
    
    start_time = datetime(2023, 1, 1, 9, 30)
    end_time = datetime(2023, 1, 1, 10, 30)
    
    # Add interval without universe changes (basic usage)
    builder.add_next_interval(start_time, end_time)
    
    # Verify universe was not changed
    assert builder.universe.current_date == date(2023, 1, 1)
    assert builder.universe.instrument_ids == [1, 2]
    
    # Verify interval was added with current instruments
    assert len(builder.intervals) == 1
    interval = builder.intervals[0]
    assert 1 in interval.instrument_intervals
    assert 2 in interval.instrument_intervals
    
    # Verify MarketDataManager was called with current instruments
    mock_manager.get_ohlc_batch.assert_called_once_with([1, 2], start_time, end_time)
