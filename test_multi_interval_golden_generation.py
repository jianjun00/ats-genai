#!/usr/bin/env python3
"""
Multi-interval golden file generation for UniverseStateBuilder.
Generates sequences of 3-5 consecutive intervals to test rolling windows and technical indicators.
"""
import asyncio
import sys
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock

# Add src to path
sys.path.insert(0, 'src')

# Enable detailed logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """Generate multi-interval golden files with real market data."""
    
    print("🚀 Starting multi-interval golden file generation...")
    
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
    
    # Test scenarios with multiple intervals
    scenarios = [
        {
            "name": "test_single_symbol_1m_sequence_golden",
            "symbols": ["AAPL"],
            "timeframe": "1m",
            "base_time": datetime(2024, 8, 1, 9, 30, tzinfo=ZoneInfo("America/New_York")),
            "interval_count": 5,  # 5 consecutive 1-minute intervals
            "instrument_ids": [363996],
            "symbol_mapping": {363996: 'AAPL'}
        },
        {
            "name": "test_single_symbol_5m_sequence_golden", 
            "symbols": ["AAPL"],
            "timeframe": "5m",
            "base_time": datetime(2024, 8, 1, 10, 30, tzinfo=ZoneInfo("America/New_York")),
            "interval_count": 3,  # 3 consecutive 5-minute intervals
            "instrument_ids": [363996],
            "symbol_mapping": {363996: 'AAPL'}
        },
        {
            "name": "test_multi_symbol_5m_sequence_golden",
            "symbols": ["AAPL", "TSLA"],
            "timeframe": "5m", 
            "base_time": datetime(2024, 8, 1, 12, 0, tzinfo=ZoneInfo("America/New_York")),
            "interval_count": 4,  # 4 consecutive 5-minute intervals
            "instrument_ids": [363996, 465844],
            "symbol_mapping": {363996: 'AAPL', 465844: 'TSLA'}
        }
    ]
    
    golden_files_dir = Path("tests/domains/trading/services/state/golden_files/universe_state_interval")
    golden_files_dir.mkdir(parents=True, exist_ok=True)
    
    for scenario in scenarios:
        print(f"\n📊 Processing scenario: {scenario['name']}")
        print(f"   Generating {scenario['interval_count']} consecutive {scenario['timeframe']} intervals")
        
        try:
            # Create real environment and UniverseStateBuilder
            environment = Environment(env_type=EnvironmentType.TEST)
            
            # Create real universe state manager for proper interval handling
            from domains.trading.services.state.universe_state_manager import UniverseStateManager
            
            real_universe_state_manager = UniverseStateManager(env=environment)
            
            # Create UniverseStateBuilder with real configuration
            builder = UniverseStateIntervalBuilder(
                env=environment,
                base_duration="1m",
                target_durations=scenario["timeframe"],
                universe_state_manager=real_universe_state_manager
            )
            
            # Replace market data manager with our real one
            builder.market_data_manager = real_market_data_manager
            
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
            mock_runner.universe_state_manager = real_universe_state_manager
            
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
                    
                    # Build up historical data (20 intervals back) for technical indicators
                    print(f"   📈 Building historical data for technical indicators...")
                    historical_start = scenario["base_time"] - timedelta(minutes=20)
                    for i in range(20):
                        historical_time = historical_start + timedelta(minutes=i)
                        await builder.handleInterval(mock_runner, historical_time)
                    
                    # Generate sequence of consecutive intervals
                    interval_states = []
                    interval_minutes = 5 if scenario["timeframe"] == "5m" else 1
                    
                    for i in range(scenario["interval_count"]):
                        # Calculate current interval time
                        current_time = scenario["base_time"] + timedelta(minutes=interval_minutes * i)
                        
                        print(f"   ⏰ Generating interval {i+1}/{scenario['interval_count']} at {current_time}")
                        
                        # Call handleInterval to build the rolling cache
                        await builder.handleInterval(mock_runner, current_time)
                        
                        # Build universe state for this specific interval  
                        duration_states = await builder._build_universe_state_for_duration(
                            builder.target_durations[0],  # Target timeframe
                            current_time, 
                            mock_universe_manager.instrument_ids,
                            mock_runner
                        )
                        
                        # Extract the universe state for this interval
                        universe_state = list(duration_states.values())[0] if duration_states else None
                        
                        if universe_state:
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
                                "interval_index": i,
                                "current_time": current_time.isoformat()
                            }
                            
                            interval_states.append(golden_data)
                            
                            # Log details for this interval
                            print(f"      📊 Generated interval {i+1}: {len(universe_state.instrument_intervals)} instruments")
                            for inst_id, interval in universe_state.instrument_intervals.items():
                                symbol = scenario["symbol_mapping"].get(inst_id, f"UNKNOWN_{inst_id}")
                                open_price = interval.open_micro_dollars / 1_000_000
                                close_price = interval.close_micro_dollars / 1_000_000
                                volume = interval.traded_volume
                                print(f"         {symbol}: ${open_price:.2f} → ${close_price:.2f}, volume {volume:,}")
                        else:
                            print(f"      ❌ Failed to generate universe state for interval {i+1}")
                    
                    if interval_states:
                        # Create collection golden file with all intervals
                        collection_data = {
                            "collection_type": "universe_state_interval_sequence",
                            "schema_version": "1.0.0",
                            "test_method": scenario["name"],
                            "test_metadata": {
                                "symbols": scenario["symbols"],
                                "timeframe": scenario["timeframe"],
                                "interval_count": len(interval_states),
                                "base_time": scenario["base_time"].isoformat(),
                                "interval_minutes": interval_minutes,
                                "description": f"Sequence of {len(interval_states)} consecutive {scenario['timeframe']} intervals"
                            },
                            "intervals": interval_states,
                            "generated_at_utc_micros": int(datetime.now().timestamp() * 1_000_000)
                        }
                        
                        # Save collection golden file
                        output_file = golden_files_dir / f"{scenario['name']}.json"
                        with open(output_file, 'w') as f:
                            json.dump(collection_data, f, indent=2)
                        
                        print(f"💾 Saved multi-interval golden file: {output_file}")
                        print(f"   📊 Contains {len(interval_states)} consecutive intervals")
                        print(f"   ⏱️  Timeframe: {scenario['timeframe']}")
                        print(f"   📈 Symbols: {', '.join(scenario['symbols'])}")
                        
                        # Also save individual interval files for backward compatibility
                        for i, interval_data in enumerate(interval_states):
                            individual_file = golden_files_dir / f"{scenario['name']}_interval_{i+1}.json"
                            with open(individual_file, 'w') as f:
                                json.dump(interval_data, f, indent=2)
                        
                        print(f"   📁 Also saved {len(interval_states)} individual interval files")
                    else:
                        print(f"❌ No intervals generated for {scenario['name']}")
                        
        except Exception as e:
            print(f"❌ Error in scenario {scenario['name']}: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n🎉 Multi-interval golden file generation complete!")

if __name__ == "__main__":
    asyncio.run(main())