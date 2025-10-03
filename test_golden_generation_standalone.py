#!/usr/bin/env python3
"""
Standalone golden file generation for UniverseStateBuilder.
This bypasses pytest fixtures and gin configuration issues.
"""
import asyncio
import sys
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock

# Add src to path
sys.path.insert(0, 'src')

# Enable detailed logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def main():
    """Generate golden files directly without pytest framework."""
    
    print("🚀 Starting standalone golden file generation...")
    
    # Import after path setup
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
    
    # Test scenarios
    scenarios = [
        {
            "name": "test_single_symbol_1m_market_open_golden",
            "symbols": ["AAPL"],
            "timeframe": "1m",
            "current_time": datetime(2024, 8, 1, 9, 31, tzinfo=ZoneInfo("America/New_York")),
            "instrument_ids": [363996],
            "symbol_mapping": {363996: 'AAPL'}
        },
        {
            "name": "test_single_symbol_5m_aggregation_golden", 
            "symbols": ["AAPL"],
            "timeframe": "5m",
            "current_time": datetime(2024, 8, 1, 10, 35, tzinfo=ZoneInfo("America/New_York")),
            "instrument_ids": [363996],
            "symbol_mapping": {363996: 'AAPL'}
        },
        {
            "name": "test_multi_symbol_5m_universe_golden",
            "symbols": ["AAPL", "TSLA"],
            "timeframe": "5m", 
            "current_time": datetime(2024, 8, 1, 12, 5, tzinfo=ZoneInfo("America/New_York")),
            "instrument_ids": [363996, 465844],
            "symbol_mapping": {363996: 'AAPL', 465844: 'TSLA'}
        }
    ]
    
    golden_files_dir = Path("tests/domains/trading/services/state/golden_files/universe_state_interval")
    golden_files_dir.mkdir(parents=True, exist_ok=True)
    
    for scenario in scenarios:
        print(f"\n📊 Processing scenario: {scenario['name']}")
        
        try:
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
                target_durations=scenario["timeframe"],
                universe_state_manager=mock_universe_state_manager
            )
            
            # Replace market data manager with our real one
            builder.market_data_manager = real_market_data_manager
            
            # Test market data manager directly
            print(f"🔍 Testing market data manager for symbols {scenario['symbols']} at {scenario['current_time']}")
            try:
                test_data = await real_market_data_manager.get_minute_ohlc_batch(
                    symbols=scenario["symbols"], 
                    start=scenario["current_time"],
                    end=scenario["current_time"]
                )
                print(f"📊 Market data test result: {len(test_data)} symbols with data")
                for symbol, data in test_data.items():
                    print(f"   {symbol}: {len(data)} records")
                    if len(data) > 0:
                        print(f"      Latest: {data.iloc[-1].to_dict()}")
            except Exception as e:
                print(f"❌ Market data test failed: {e}")
                import traceback
                traceback.print_exc()
            
            # Add real IndicatorBuilder for technical indicator calculations
            from domains.trading.services.indicators.indicator_builder import IndicatorBuilder
            from domains.trading.services.indicators.indicator_config import IndicatorConfig
            
            # Create indicator config with default configuration
            indicator_config = IndicatorConfig.default_config()
            builder.indicator_builder = IndicatorBuilder(indicator_config)
            
            # Create mock runner that provides the context the builder needs
            mock_runner = Mock()
            mock_runner.get_environment.return_value = environment
            mock_runner.universe_id = 1
            mock_runner.market_data_manager = real_market_data_manager
            
            # Mock universe manager with real instrument IDs
            mock_universe_manager = Mock()
            mock_universe_manager.instrument_ids = scenario["instrument_ids"]
            mock_runner.universe_manager = mock_universe_manager
            
            # Mock InstrumentXrefsDAO to map instrument ID to symbol
            with patch('core.dao.instruments.instrument_xrefs_dao.InstrumentXrefsDAO') as mock_xrefs_dao_class:
                mock_xrefs_dao = Mock()
                mock_xrefs_dao.get_symbols_by_instrument_ids_batch = AsyncMock(return_value=scenario["symbol_mapping"])
                mock_xrefs_dao_class.return_value = mock_xrefs_dao
                
                # Mock market cap DAO 
                with patch.object(builder, 'market_cap_dao') as mock_market_cap_dao:
                    mock_market_cap_dao.list_market_caps_for_date = AsyncMock(return_value=[])
                    
                    # Build universe state using the internal method
                    duration_states = await builder._build_universe_state_for_duration(
                        builder.target_durations[0],  # Target timeframe
                        scenario["current_time"], 
                        mock_universe_manager.instrument_ids,
                        mock_runner
                    )
                    
                    # Extract the universe state
                    universe_state = list(duration_states.values())[0] if duration_states else None
                    
                    if universe_state:
                        print(f"✅ Successfully generated universe state with {len(universe_state.instrument_intervals)} instruments")
                        
                        # Convert to golden file format
                        golden_data = {
                            "business_object": {
                                "duration": {
                                    "duration_minutes": universe_state.duration.get_duration_minutes(),
                                    "duration_string": universe_state.duration.get_duration_string()
                                },
                                "start_timestamp_utc_micros": universe_state.start_timestamp_utc_micros,
                                "end_timestamp_utc_micros": universe_state.end_timestamp_utc_micros,
                                "universe_id": universe_state.universe_id,
                                "factor_intervals": [
                                    {
                                        "factor_type": fi.factor_type,
                                        "start_timestamp_utc_micros": fi.start_timestamp_utc_micros,
                                        "end_timestamp_utc_micros": fi.end_timestamp_utc_micros,
                                        "properties": fi.properties
                                    }
                                    for fi in universe_state.factor_intervals
                                ],
                                "instrument_intervals": {
                                    str(inst_id): {
                                        "instrument_id": interval.instrument_id,
                                        "start_timestamp_utc_micros": interval.start_timestamp_utc_micros,
                                        "end_timestamp_utc_micros": interval.end_timestamp_utc_micros,
                                        "open_micro_dollars": interval.open_micro_dollars,
                                        "high_micro_dollars": interval.high_micro_dollars,
                                        "low_micro_dollars": interval.low_micro_dollars,
                                        "close_micro_dollars": interval.close_micro_dollars,
                                        "traded_volume": interval.traded_volume,
                                        "traded_dollar_micro": interval.traded_dollar_micro,
                                        "market_cap_micro_dollars": interval.market_cap_micro_dollars,
                                        "status": interval.status
                                    }
                                    for inst_id, interval in universe_state.instrument_intervals.items()
                                },
                                "instrument_indicator_intervals": universe_state.instrument_indicator_intervals,
                                "instrument_forecast_intervals": universe_state.instrument_forecast_intervals
                            },
                            "object_type": "universe_state_interval",
                            "schema_version": "1.0.0",
                            "generated_at_utc_micros": int(datetime.now().timestamp() * 1_000_000),
                            "test_method": scenario["name"],
                            "test_metadata": {
                                "symbols": scenario["symbols"],
                                "timeframe": scenario["timeframe"],
                                "duration_string": scenario["timeframe"],
                                "instrument_count": len(universe_state.instrument_intervals),
                                "start_datetime": scenario["current_time"].strftime("%Y-%m-%dT%H:%M:%S"),
                                "end_datetime": scenario["current_time"].strftime("%Y-%m-%dT%H:%M:%S"),
                                "indicator_types": ["technical_indicators"],
                                "test_data_hash": "generated_with_real_data"
                            }
                        }
                        
                        # Save golden file
                        output_file = golden_files_dir / f"{scenario['name']}.json"
                        with open(output_file, 'w') as f:
                            json.dump(golden_data, f, indent=2)
                        
                        print(f"💾 Saved golden file: {output_file}")
                        
                        # Log details
                        for inst_id, interval in universe_state.instrument_intervals.items():
                            symbol = scenario["symbol_mapping"].get(inst_id, f"UNKNOWN_{inst_id}")
                            print(f"   📈 {symbol}: open=${interval.open_micro_dollars/1_000_000:.2f}, close=${interval.close_micro_dollars/1_000_000:.2f}, volume={interval.traded_volume}")
                        
                    else:
                        print(f"❌ Failed to generate universe state for {scenario['name']}")
                        
        except Exception as e:
            print(f"❌ Error in scenario {scenario['name']}: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n🎉 Golden file generation complete!")

if __name__ == "__main__":
    asyncio.run(main())