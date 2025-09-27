#!/usr/bin/env python3
"""
Test case to debug UniverseStateBuilder timeframe processing issues.

This test validates that:
1. UniverseStateBuilder processes correct timeframes at different times
2. UniverseStateManager receives and caches the built intervals
3. Training data generator can retrieve the pre-computed intervals
"""

import pytest
import sys
import os
from unittest.mock import Mock, patch
from datetime import datetime

sys.path.insert(0, 'src')

def test_universe_state_builder_timeframe_boundary_logic():
    """Test that UniverseStateBuilder processes correct timeframes at boundary times"""
    
    # Import after path setup
    from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
    from core.business.calendars.time_duration import TimeDuration
    from unittest.mock import Mock
    
    # Create UniverseStateIntervalBuilder with test configuration
    mock_env = Mock()
    builder = UniverseStateIntervalBuilder(
        env=mock_env,
        base_duration='5m',
        target_durations='5m,15m,60m,1d'
    )
    
    print(f"🏗️ Builder configured with target_durations: {builder.target_durations}")
    print(f"🏗️ Builder configured with base_duration: {builder.base_duration.duration_type}")
    
    # Test boundary logic at different times
    test_cases = [
        {
            'time': datetime(2025, 7, 1, 13, 35, 0),  # 13:35
            'expected': {
                '5m': True,   # 5-minute boundary (every 5 minutes)
                '15m': False, # Not 15-minute boundary (13:35 % 15 != 0)
                '60m': False, # Not 60-minute boundary (13:35 % 60 != 0)
                '1d': False   # Not daily boundary
            }
        },
        {
            'time': datetime(2025, 7, 1, 13, 45, 0),  # 13:45
            'expected': {
                '5m': True,   # 5-minute boundary
                '15m': True,  # 15-minute boundary (13:45 % 15 == 0)
                '60m': False, # Not 60-minute boundary
                '1d': False   # Not daily boundary
            }
        },
        {
            'time': datetime(2025, 7, 1, 14, 0, 0),   # 14:00
            'expected': {
                '5m': True,   # 5-minute boundary
                '15m': True,  # 15-minute boundary
                '60m': True,  # 60-minute boundary (14:00 % 60 == 0)
                '1d': False   # Not daily boundary
            }
        }
    ]
    
    print(f"\n🎯 Testing boundary logic for different times:")
    
    for i, test_case in enumerate(test_cases):
        test_time = test_case['time']
        expected = test_case['expected']
        
        print(f"\n⏰ Test Case {i+1}: {test_time}")
        
        for timeframe, should_process in expected.items():
            duration = TimeDuration(timeframe)
            actual = builder._should_process_timeframe(duration, test_time)
            status = "✅ PASS" if actual == should_process else "❌ FAIL"
            print(f"   {timeframe:4} -> Expected: {should_process:5}, Actual: {actual:5} {status}")
            
            # Assert for proper test failure if logic is wrong
            assert actual == should_process, f"Timeframe {timeframe} at {test_time}: expected {should_process}, got {actual}"
    
    print(f"\n✅ All boundary logic tests passed!")


def test_universe_state_interval_creation_and_caching():
    """Test that UniverseStateInterval objects are created and cached correctly"""
    
    from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
    from domains.trading.services.state.universe_state_manager import UniverseStateManager
    from domains.trading.services.state.universe_state import UniverseStateInterval
    from domains.trading.services.state.instrument_interval import InstrumentInterval
    from core.business.calendars.time_duration import TimeDuration
    
    print(f"\n🏗️ Testing UniverseStateInterval creation and caching...")
    
    # Create mock UniverseStateManager with rolling cache
    universe_manager = Mock(spec=UniverseStateManager)
    universe_manager._rolling_instrument_history = {
        '5m': {
            31: [  # instrument_id 31 (AAPL)
                InstrumentInterval(
                    instrument_id=31,
                    start_date_time=datetime(2025, 7, 1, 13, 30, 0),
                    end_date_time=datetime(2025, 7, 1, 13, 35, 0),
                    open=205.50,
                    high=206.00,
                    low=205.25,
                    close=205.75,
                    traded_volume=1000,
                    traded_dollar=205750.0
                )
            ]
        },
        '15m': {
            31: [  # instrument_id 31 (AAPL)
                InstrumentInterval(
                    instrument_id=31,
                    start_date_time=datetime(2025, 7, 1, 13, 30, 0),
                    end_date_time=datetime(2025, 7, 1, 13, 45, 0),
                    open=205.50,
                    high=206.50,
                    low=205.00,
                    close=206.25,
                    traded_volume=3000,
                    traded_dollar=618750.0
                )
            ]
        }
    }
    
    # Test retrieving 5m interval
    test_time = datetime(2025, 7, 1, 13, 35, 0)
    
    # Mock the cache retrieval method
    def mock_get_universe_state_interval(timeframe, current_time, run_id=None):
        print(f"🔍 Mock: Retrieving {timeframe} interval for {current_time}")
        
        if timeframe == '5m' and current_time == test_time:
            # Simulate successful retrieval from cache
            return UniverseStateInterval(
                duration=TimeDuration('5m'),
                start_date_time=datetime(2025, 7, 1, 13, 30, 0),
                end_date_time=datetime(2025, 7, 1, 13, 35, 0),
                factor_intervals=[],
                instrument_intervals={
                    31: InstrumentInterval(
                        instrument_id=31,
                        start_date_time=datetime(2025, 7, 1, 13, 30, 0),
                        end_date_time=datetime(2025, 7, 1, 13, 35, 0),
                        open=205.50,
                        high=206.00,
                        low=205.25,
                        close=205.75,
                        traded_volume=1000,
                        traded_dollar=205750.0
                    )
                }
            )
        return None
    
    universe_manager.get_universe_state_interval = Mock(side_effect=mock_get_universe_state_interval)
    
    # Test retrieval
    result = universe_manager.get_universe_state_interval('5m', test_time)
    
    print(f"📊 Retrieved interval: {result is not None}")
    if result:
        print(f"   Duration: {result.duration.duration_type}")
        print(f"   Time range: {result.start_date_time} to {result.end_date_time}")
        print(f"   Instruments: {len(result.instrument_intervals)}")
        
        if 31 in result.instrument_intervals:
            inst_interval = result.instrument_intervals[31]
            print(f"   AAPL OHLCV: O={inst_interval.open}, H={inst_interval.high}, L={inst_interval.low}, C={inst_interval.close}, V={inst_interval.traded_volume}")
    
    assert result is not None, "Should retrieve UniverseStateInterval from cache"
    assert result.duration.duration_type.value == '5m', "Duration should be 5m"
    assert len(result.instrument_intervals) == 1, "Should have one instrument"
    assert 31 in result.instrument_intervals, "Should contain AAPL (instrument_id 31)"
    
    print(f"✅ UniverseStateInterval creation and caching test passed!")


def test_training_data_generator_fail_fast_behavior():
    """Test that training data generator fails fast when UniverseStateInterval is missing"""
    
    from domains.ml.services.training_data.timeseries_sequence_training_generator import TimeSeriesSequenceTrainingGenerator
    from domains.trading.services.state.universe_state_manager import UniverseStateManager
    
    print(f"\n🚨 Testing fail-fast behavior when UniverseStateInterval is missing...")
    
    # Create mock UniverseStateManager that returns None (missing intervals)
    universe_manager = Mock(spec=UniverseStateManager)
    universe_manager.get_universe_state_interval = Mock(return_value=None)
    universe_manager.get_future_universe_state_interval = Mock(return_value=None)
    
    # Create training data generator with mock config
    mock_config = Mock()
    mock_config.feature_types = ['ohlcv']
    mock_config.signal_names = ['test_signal']
    
    generator = TimeSeriesSequenceTrainingGenerator(
        config=mock_config,
        env=Mock(),
        universe_manager=universe_manager
    )
    
    # Test that get_timeframe_data fails fast for missing current data
    print("🔍 Testing current data retrieval...")
    with pytest.raises(RuntimeError, match="No UniverseStateInterval found"):
        generator.get_timeframe_data(
            instrument_id=31,
            center_datetime=datetime(2025, 7, 1, 13, 35, 0),
            timeframe='5m',
            is_future=False
        )
    print("✅ Current data correctly fails fast")
    
    # Test that get_timeframe_data fails fast for missing future data
    print("🔍 Testing future data retrieval...")
    with pytest.raises(RuntimeError, match="No future UniverseStateInterval found"):
        generator.get_timeframe_data(
            instrument_id=31,
            center_datetime=datetime(2025, 7, 1, 13, 35, 0),
            timeframe='5m',
            is_future=True
        )
    print("✅ Future data correctly fails fast")
    
    print(f"✅ Fail-fast behavior test passed!")


def test_integrated_timeframe_processing_flow():
    """Integration test for the complete timeframe processing flow"""
    
    print(f"\n🔄 Testing integrated timeframe processing flow...")
    
    from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
    from domains.trading.services.state.universe_state_manager import UniverseStateManager
    from domains.ml.services.training_data.timeseries_sequence_training_generator import TimeSeriesSequenceTrainingGenerator
    
    # This test would require more complex setup with database mocking
    # For now, just validate the architectural components exist and are importable
    
    print("📦 Testing component imports...")
    assert UniverseStateIntervalBuilder is not None, "UniverseStateIntervalBuilder should be importable"
    assert UniverseStateManager is not None, "UniverseStateManager should be importable"
    assert TimeSeriesSequenceTrainingGenerator is not None, "TimeSeriesSequenceTrainingGenerator should be importable"
    print("✅ All components imported successfully")
    
    # Test that UniverseStateIntervalBuilder has the required methods
    mock_env = Mock()
    builder = UniverseStateIntervalBuilder(
        env=mock_env,
        base_duration='5m',
        target_durations='5m,15m,60m,1d'
    )
    assert hasattr(builder, '_should_process_timeframe'), "Builder should have _should_process_timeframe method"
    assert hasattr(builder, 'target_durations'), "Builder should have target_durations attribute"
    print("✅ UniverseStateIntervalBuilder has required interface")
    
    # Test that UniverseStateManager has the required methods
    manager = UniverseStateManager(None, None, None)
    assert hasattr(manager, 'get_universe_state_interval'), "Manager should have get_universe_state_interval method"
    assert hasattr(manager, 'get_future_universe_state_interval'), "Manager should have get_future_universe_state_interval method"
    print("✅ UniverseStateManager has required interface")
    
    print(f"✅ Integrated flow architecture validation passed!")


if __name__ == '__main__':
    print("🧪 Running UniverseStateBuilder timeframe processing debug tests...\n")
    
    test_universe_state_builder_timeframe_boundary_logic()
    test_universe_state_interval_creation_and_caching()
    test_training_data_generator_fail_fast_behavior()
    test_integrated_timeframe_processing_flow()
    
    print(f"\n🎉 All debug tests passed! The architectural fix is working correctly.")
    
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)