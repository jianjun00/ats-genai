"""Simple test to isolate golden file generation issue."""
import pytest
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock

# Test configuration
TEST_DIR = Path(__file__).parent
GOLDEN_FILES_DIR = TEST_DIR / "golden_files"

@pytest.mark.asyncio
async def test_simple_golden_generation(request):
    # Skip gin setup for this test
    if hasattr(request, 'fixturenames') and 'gin_test_setup' in request.fixturenames:
        request.fixturenames.remove('gin_test_setup')
    """Simple test to generate golden file with real UniverseStateBuilder."""
    from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
    from core.platform.config.environment import Environment, EnvironmentType
    from domains.market_data.services.core.unified_market_data_manager import (
        UnifiedMarketDataManager, MarketDataConfig, VendorType, StorageBackend
    )
    
    # Set up real market data manager
    config = MarketDataConfig(
        vendors=[VendorType.FIRSTRATE],
        storage_backend=StorageBackend.FILE,
        file_storage_path="/mnt/d/ats-data/minute-bars/firstrate",
        enable_cache=True,
        enable_validation=True
    )
    real_market_data_manager = UnifiedMarketDataManager(config)
    
    # Use Eastern timezone for market hours (9:31 AM Eastern = 13:31 GMT)
    current_time = datetime(2024, 8, 1, 9, 31, tzinfo=ZoneInfo("America/New_York"))
    
    # Create real environment and UniverseStateBuilder
    environment = Environment(env_type=EnvironmentType.TEST)
    
    # Create mock universe state manager that actually stores rolling cache
    mock_universe_state_manager = Mock()
    rolling_cache = {}  # Store cache data: {inst_id: {timeframe: [intervals]}}
    
    def add_to_cache(inst_id, timeframe, interval):
        if inst_id not in rolling_cache:
            rolling_cache[inst_id] = {}
        if timeframe not in rolling_cache[inst_id]:
            rolling_cache[inst_id][timeframe] = []
        rolling_cache[inst_id][timeframe].append(interval)
        # Keep only last 50 intervals to simulate real rolling cache
        rolling_cache[inst_id][timeframe] = rolling_cache[inst_id][timeframe][-50:]
    
    def get_history(inst_id, timeframe):
        return rolling_cache.get(inst_id, {}).get(timeframe, [])
    
    mock_universe_state_manager.add_interval_to_rolling_cache = add_to_cache
    mock_universe_state_manager.get_instrument_history_for_timeframe = get_history
    
    # Create UniverseStateBuilder with real configuration and working cache
    builder = UniverseStateIntervalBuilder(
        env=environment,
        base_duration="1m",
        target_durations="1m",
        universe_state_manager=mock_universe_state_manager
    )
    
    # Replace market data manager with our real one
    builder.market_data_manager = real_market_data_manager
    
    # Add real IndicatorBuilder for technical indicator calculations
    from domains.trading.services.indicators.indicator_builder import IndicatorBuilder
    from domains.trading.services.indicators.indicator_config import IndicatorConfig
    
    # Create indicator config from training_data.gin configuration
    indicator_config = IndicatorConfig.from_gin_config() if hasattr(IndicatorConfig, 'from_gin_config') else IndicatorConfig.default_config()
    builder.indicator_builder = IndicatorBuilder(indicator_config)
    
    # Create mock runner that provides the context the builder needs
    mock_runner = Mock()
    mock_runner.get_environment.return_value = environment
    mock_runner.universe_id = 1
    mock_runner.market_data_manager = real_market_data_manager
    
    # Mock universe manager with real instrument ID for AAPL
    mock_universe_manager = Mock()
    mock_universe_manager.instrument_ids = [363996]  # AAPL
    mock_runner.universe_manager = mock_universe_manager
    
    # Mock InstrumentXrefsDAO to map instrument ID to symbol
    with patch('core.dao.instruments.instrument_xrefs_dao.InstrumentXrefsDAO') as mock_xrefs_dao_class:
        mock_xrefs_dao = Mock()
        mock_xrefs_dao.get_symbols_by_instrument_ids_batch = AsyncMock(return_value={
            363996: 'AAPL'
        })
        mock_xrefs_dao_class.return_value = mock_xrefs_dao
        
        # Mock market cap DAO 
        with patch.object(builder, 'market_cap_dao') as mock_market_cap_dao:
            mock_market_cap_dao.list_market_caps_for_date = AsyncMock(return_value=[])
            
            # Build universe state using the internal method
            duration_states = await builder._build_universe_state_for_duration(
                builder.target_durations[0],  # 1m duration
                current_time, 
                mock_universe_manager.instrument_ids,
                mock_runner
            )
            
            # Extract the universe state
            universe_state = list(duration_states.values())[0] if duration_states else None
            
            if universe_state:
                # Simple assertion to test the basic functionality
                assert universe_state.instrument_intervals, "Should have instrument intervals"
                print(f"✅ Successfully generated universe state with {len(universe_state.instrument_intervals)} instruments")
                
                # Check if we have OHLC data
                for inst_id, interval in universe_state.instrument_intervals.items():
                    print(f"Instrument {inst_id}: open={interval.open_micro_dollars}, close={interval.close_micro_dollars}, volume={interval.traded_volume}")
                
                return universe_state
            else:
                assert False, "UniverseStateBuilder failed to create universe state"