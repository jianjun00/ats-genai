#!/usr/bin/env python3
"""
Test to verify UniverseStateIntervalBuilder correctly requests trading session data range.
"""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock
from core.platform.config.environment import Environment, EnvironmentType

async def test_universe_state_builder_data_request():
    """Test that UniverseStateIntervalBuilder requests correct time range."""
    print("🔧 Testing UniverseStateIntervalBuilder data request logic...")
    
    # Create test environment
    environment = Environment(env_type=EnvironmentType.TEST)
    
    # Import and create UniverseStateIntervalBuilder
    from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
    
    builder = UniverseStateIntervalBuilder(
        env=environment,
        base_duration='5m',
        target_durations='5m,15m,60m'
    )
    
    # Create mock runner with required attributes
    mock_runner = MagicMock()
    mock_runner.universe_manager = MagicMock()
    mock_runner.universe_manager.instrument_ids = [999999]
    mock_runner.get_environment.return_value = environment
    
    # Create mock market data manager to capture the requested time range
    mock_market_data_manager = AsyncMock()
    mock_runner.market_data_manager = mock_market_data_manager
    
    # Mock the xrefs DAO to return AAPL symbol
    from unittest.mock import patch
    
    mock_xrefs_dao = AsyncMock()
    mock_xrefs_dao.get_symbols_by_instrument_ids_batch.return_value = {999999: 'AAPL'}
    
    # Mock market cap DAO
    mock_market_cap_dao = AsyncMock()
    mock_market_cap_dao.list_market_caps_for_date.return_value = [
        {'instrument_id': 999999, 'market_cap': 3000000000000}
    ]
    builder.market_cap_dao = mock_market_cap_dao
    
    # Mock universe state manager
    mock_universe_state_manager = MagicMock()
    mock_universe_state_manager.ensure_timeframe_cache = MagicMock()
    mock_universe_state_manager.get_instrument_history_for_timeframe.return_value = []
    mock_universe_state_manager.add_interval_to_rolling_cache = MagicMock()
    builder.universe_state_manager = mock_universe_state_manager
    
    # Test with a specific time (should be during trading hours)
    test_time = datetime(2024, 8, 1, 20, 0, 0)  # 8:00 PM UTC (4:00 PM EDT - market close)
    
    print(f"   📅 Test time: {test_time}")
    
    # Mock the market data response
    mock_market_data_manager.get_minute_ohlc_batch.return_value = {
        'AAPL': {
            'open': 220.0,
            'high': 225.0,
            'low': 218.0,
            'close': 223.0,
            'volume': 1000000
        }
    }
    
    with patch('core.dao.instruments.instrument_xrefs_dao.InstrumentXrefsDAO', return_value=mock_xrefs_dao):
        # Call handleInterval
        await builder.handleInterval(mock_runner, test_time)
    
    # Verify that get_minute_ohlc_batch was called with the correct time range
    assert mock_market_data_manager.get_minute_ohlc_batch.called, "Market data manager should have been called"
    
    call_args = mock_market_data_manager.get_minute_ohlc_batch.call_args
    symbols, start_time, end_time = call_args[0]
    
    print(f"   📊 Requested symbols: {symbols}")
    print(f"   ⏰ Requested start time: {start_time}")
    print(f"   ⏰ Requested end time: {end_time}")
    
    # Verify the symbols
    assert 'AAPL' in symbols, f"Should request AAPL data, got {symbols}"
    
    # Verify the time range is the full trading session, not just current minute
    expected_start_hour = 13  # 9:30 AM EDT = 13:30 UTC
    expected_start_minute = 30
    expected_end_hour = 20    # 4:00 PM EDT = 20:00 UTC  
    expected_end_minute = 0
    
    assert start_time.hour == expected_start_hour and start_time.minute == expected_start_minute, \
        f"Start time should be 13:30 UTC (market open), got {start_time}"
    
    assert end_time.hour == expected_end_hour and end_time.minute == expected_end_minute, \
        f"End time should be 20:00 UTC (market close), got {end_time}"
    
    # Verify it's requesting the full 6.5 hour trading session
    session_duration = end_time - start_time
    expected_duration_hours = 6.5
    
    print(f"   📊 Requested session duration: {session_duration}")
    print(f"   📊 Expected duration: {expected_duration_hours} hours")
    
    assert session_duration.total_seconds() == expected_duration_hours * 3600, \
        f"Should request full 6.5 hour trading session, got {session_duration}"
    
    print("   ✅ UniverseStateIntervalBuilder correctly requests FULL trading session!")
    print("   ✅ Time range: 13:30 UTC to 20:00 UTC (9:30 AM - 4:00 PM EDT)")
    print("   ✅ Duration: 6.5 hours")
    
    print("\n🎉 UniverseStateIntervalBuilder data request fix is working correctly!")

if __name__ == "__main__":
    asyncio.run(test_universe_state_builder_data_request())