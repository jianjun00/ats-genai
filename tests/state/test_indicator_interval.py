import pytest
from datetime import datetime
from state.indicator_interval import IndicatorInterval


def test_indicator_interval_init():
    """Test IndicatorInterval initialization."""
    start_time = datetime(2023, 1, 1, 9, 30)
    end_time = datetime(2023, 1, 1, 10, 30)
    
    interval = IndicatorInterval(
        instrument_id=1,
        start_date_time=start_time,
        end_date_time=end_time
    )
    
    assert interval.instrument_id == 1
    assert interval.start_date_time == start_time
    assert interval.end_date_time == end_time
    assert interval.indicators == {}


def test_add_indicator():
    """Test adding indicators to interval."""
    interval = IndicatorInterval(
        instrument_id=1,
        start_date_time=datetime(2023, 1, 1, 9, 30),
        end_date_time=datetime(2023, 1, 1, 10, 30)
    )
    
    update_time = datetime(2023, 1, 1, 10, 35)
    
    # Add valid indicator
    interval.add_indicator('PL', 105.5, 'ok', update_time)
    assert interval.has_indicator('PL')
    assert interval.get_indicator_value('PL') == 105.5
    assert interval.get_indicator_status('PL') == 'ok'
    
    # Add invalid indicator
    interval.add_indicator('OneOneLow', None, 'invalid', update_time)
    assert interval.has_indicator('OneOneLow')
    assert interval.get_indicator_value('OneOneLow') is None
    assert interval.get_indicator_status('OneOneLow') == 'invalid'


def test_indicator_queries():
    """Test querying indicator information."""
    interval = IndicatorInterval(
        instrument_id=1,
        start_date_time=datetime(2023, 1, 1, 9, 30),
        end_date_time=datetime(2023, 1, 1, 10, 30)
    )
    
    # Add multiple indicators
    interval.add_indicator('PL', 105.5, 'ok')
    interval.add_indicator('OneOneLow', None, 'invalid')
    interval.add_indicator('OneOneHigh', 108.2, 'ok')
    
    # Test has_indicator
    assert interval.has_indicator('PL')
    assert interval.has_indicator('OneOneLow')
    assert interval.has_indicator('OneOneHigh')
    assert not interval.has_indicator('NonExistent')
    
    # Test get_indicator_names
    names = interval.get_indicator_names()
    assert len(names) == 3
    assert 'PL' in names
    assert 'OneOneLow' in names
    assert 'OneOneHigh' in names
    
    # Test is_indicator_valid
    assert interval.is_indicator_valid('PL')
    assert not interval.is_indicator_valid('OneOneLow')
    assert interval.is_indicator_valid('OneOneHigh')
    assert not interval.is_indicator_valid('NonExistent')


def test_get_nonexistent_indicator():
    """Test querying non-existent indicators."""
    interval = IndicatorInterval(
        instrument_id=1,
        start_date_time=datetime(2023, 1, 1, 9, 30),
        end_date_time=datetime(2023, 1, 1, 10, 30)
    )
    
    assert interval.get_indicator_value('NonExistent') is None
    assert interval.get_indicator_status('NonExistent') is None
    assert not interval.has_indicator('NonExistent')
    assert not interval.is_indicator_valid('NonExistent')


def test_indicator_update_time():
    """Test indicator update time handling."""
    interval = IndicatorInterval(
        instrument_id=1,
        start_date_time=datetime(2023, 1, 1, 9, 30),
        end_date_time=datetime(2023, 1, 1, 10, 30)
    )
    
    # Add indicator without explicit update time (should use current time)
    interval.add_indicator('PL', 105.5, 'ok')
    
    # Check that update_at was set
    indicator_data = interval.indicators['PL']
    assert 'update_at' in indicator_data
    assert indicator_data['update_at'] is not None
    
    # Add indicator with explicit update time
    explicit_time = datetime(2023, 1, 1, 10, 35)
    interval.add_indicator('OneOneLow', 102.3, 'ok', explicit_time)
    
    indicator_data = interval.indicators['OneOneLow']
    assert indicator_data['update_at'] == explicit_time
