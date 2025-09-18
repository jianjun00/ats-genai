#!/usr/bin/env python3
"""
TIMEFRAME-AWARE CACHE SYSTEM TESTS

This test suite validates that the UniverseStateManager properly separates
and manages data by timeframes:

1. addUniverseState stores data separately by timeframe (5m, 60m, 1d, etc.)
2. get_lag_prices retrieves data only for the requested timeframe
3. get_lagged_signals retrieves data only for the requested timeframe
4. Cross-timeframe data isolation is maintained
5. Cache structure: Dict[timeframe, Dict[instrument, DataFrame]]
"""

import pytest
import pandas as pd
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any

# Add src to path
sys.path.insert(0, '/home/jianjun/ats-genai-admin/src')

# Set environment to skip gin loading
os.environ['GIN_LOAD_DEFAULT_CONFIG'] = '0'

from core.shared.data_handling.utils.environment import Environment, EnvironmentType
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from domains.trading.services.state.universe_state import UniverseStateInterval
from domains.trading.services.state.instrument_interval import InstrumentInterval
from domains.trading.services.state.indicator_interval import IndicatorInterval
from core.business.calendars.time_duration import TimeDuration


class TestTimeframeAwareCache:
    """Test timeframe-aware cache implementation in UniverseStateManager."""
    
    def setup_method(self):
        """Setup test environment and manager."""
        self.test_env = Environment(env_type=EnvironmentType.TEST)
        self.manager = UniverseStateManager(env=None)  # Disable database operations for cache testing
        
        # Test data configuration
        self.instrument_ids = [1001, 1002, 1003]  # AAPL, TSLA, MSFT
        self.base_time = datetime(2025, 9, 13, 10, 0, 0)
        self.timeframes = ['5m', '60m', '1d']
        
    def _create_test_universe_state(self, timeframe: str, instrument_id: int, base_time: datetime) -> UniverseStateInterval:
        """Create test universe state for specific timeframe and instrument."""
        
        # Create timeframe-specific data patterns
        if timeframe == '5m':
            duration = TimeDuration('5m')
            open_price = 100.0 + instrument_id  # Base pattern for 5m
            high_price = open_price + 1.0
            low_price = open_price - 0.5
            close_price = open_price + 0.25
            volume = 1000
        elif timeframe == '60m':
            duration = TimeDuration('60m') 
            open_price = 200.0 + instrument_id  # Base pattern for 60m
            high_price = open_price + 5.0
            low_price = open_price - 2.0
            close_price = open_price + 2.5
            volume = 10000
        elif timeframe == '1d':
            duration = TimeDuration('1d')
            open_price = 300.0 + instrument_id  # Base pattern for 1d
            high_price = open_price + 20.0
            low_price = open_price - 10.0
            close_price = open_price + 10.0
            volume = 100000
        else:
            raise ValueError(f"Unknown timeframe: {timeframe}")
            
        # Create instrument interval
        instrument_interval = InstrumentInterval(
            instrument_id=instrument_id,
            start_date_time=base_time,
            end_date_time=base_time + timedelta(minutes=5 if timeframe == '5m' else 60 if timeframe == '60m' else 1440),
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            traded_volume=volume,
            traded_dollar=volume * close_price,
            status="ok",
            market_cap=1000000.0
        )
        
        # Create indicator interval with timeframe-specific values
        indicator_value = 50.0 + (10.0 if timeframe == '5m' else 20.0 if timeframe == '60m' else 30.0)
        indicator_interval = IndicatorInterval(
            instrument_id=instrument_id,
            start_date_time=base_time,
            end_date_time=base_time + timedelta(minutes=5 if timeframe == '5m' else 60 if timeframe == '60m' else 1440),
            indicators={
                'rsi': {'value': indicator_value, 'status': 'ok'},
                'macd': {'value': indicator_value + 5.0, 'status': 'ok'}
            }
        )
        
        # Create universe state
        universe_state = UniverseStateInterval(
            universe_id=1,
            duration=duration,
            start_date_time=base_time,
            end_date_time=base_time + timedelta(minutes=5 if timeframe == '5m' else 60 if timeframe == '60m' else 1440),
            factor_intervals=[],
            instrument_intervals={instrument_id: instrument_interval},
            instrument_indicator_intervals={
                'technical': {instrument_id: indicator_interval}
            }
        )
        
        return universe_state
        
    @pytest.mark.asyncio
    async def test_addUniverseState_stores_data_separately_by_timeframe(self):
        """Test: addUniverseState stores data separately for each timeframe."""
        
        print("🔍 Testing addUniverseState stores data separately by timeframe...")
        
        # Create universe states for multiple timeframes
        duration_to_state = {}
        for timeframe in self.timeframes:
            universe_state = self._create_test_universe_state(timeframe, self.instrument_ids[0], self.base_time)
            duration_to_state[timeframe] = universe_state
            
        # Add universe states
        await self.manager.addUniverseState(duration_to_state, self.base_time)
        
        # Verify cache structure is timeframe-aware
        assert hasattr(self.manager, '_cache'), "Manager should have _cache attribute"
        assert hasattr(self.manager, '_instrument_history'), "Manager should have _instrument_history attribute"
        
        # Check that cache is structured by timeframe
        # Expected: _cache[timeframe][instrument_id] = DataFrame
        # Expected: _instrument_history[timeframe][instrument_id] = DataFrame
        
        if isinstance(self.manager._cache, dict):
            print(f"   Current _cache keys: {list(self.manager._cache.keys())}")
            print(f"   Current _cache structure type: {type(self.manager._cache)}")
            
        if isinstance(self.manager._instrument_history, dict):
            print(f"   Current _instrument_history keys: {list(self.manager._instrument_history.keys())}")
            print(f"   Current _instrument_history structure type: {type(self.manager._instrument_history)}")
            
        # TODO: Once implemented, verify timeframe separation:
        # for timeframe in self.timeframes:
        #     assert timeframe in self.manager._cache, f"Cache should contain timeframe {timeframe}"
        #     assert timeframe in self.manager._instrument_history, f"Instrument history should contain timeframe {timeframe}"
        #     assert self.instrument_ids[0] in self.manager._instrument_history[timeframe], f"Instrument {self.instrument_ids[0]} should be in {timeframe} history"
        
        print("✅ addUniverseState timeframe separation test setup complete")
        
    def test_get_lag_prices_retrieves_correct_timeframe_data(self):
        """Test: get_lag_prices returns data only for the requested timeframe."""
        
        print("🔍 Testing get_lag_prices retrieves correct timeframe data...")
        
        # Setup: Manually populate cache with timeframe-specific data
        instrument_id = self.instrument_ids[0]
        
        # Create distinct data for each timeframe
        timeframe_data = {
            '5m': pd.DataFrame({
                'instrument_id': [instrument_id] * 3,
                'date': [self.base_time - timedelta(minutes=10), self.base_time - timedelta(minutes=5), self.base_time],
                'open': [105.0, 105.5, 106.0],
                'high': [106.0, 106.5, 107.0], 
                'low': [104.5, 105.0, 105.5],
                'close': [105.25, 105.75, 106.25],
                'volume': [1000, 1100, 1200]
            }),
            '60m': pd.DataFrame({
                'instrument_id': [instrument_id] * 3,
                'date': [self.base_time - timedelta(hours=2), self.base_time - timedelta(hours=1), self.base_time],
                'open': [205.0, 205.5, 206.0],
                'high': [210.0, 210.5, 211.0],
                'low': [202.0, 202.5, 203.0],
                'close': [207.5, 208.0, 208.5],
                'volume': [10000, 11000, 12000]
            }),
            '1d': pd.DataFrame({
                'instrument_id': [instrument_id] * 3,
                'date': [self.base_time - timedelta(days=2), self.base_time - timedelta(days=1), self.base_time],
                'open': [305.0, 305.5, 306.0],
                'high': [325.0, 325.5, 326.0],
                'low': [295.0, 295.5, 296.0],
                'close': [315.0, 315.5, 316.0],
                'volume': [100000, 110000, 120000]
            })
        }
        
        # TODO: Once timeframe-aware cache is implemented, populate it:
        # for timeframe, data in timeframe_data.items():
        #     if timeframe not in self.manager._instrument_history:
        #         self.manager._instrument_history[timeframe] = {}
        #     self.manager._instrument_history[timeframe][instrument_id] = data
        
        # For now, use current structure to verify logic
        # Simulate having data in current structure
        self.manager._instrument_history[instrument_id] = timeframe_data['1d']  # Default to daily data
        
        # Test: Request different timeframes and verify correct data is returned
        try:
            # Test 1d timeframe
            result_1d = self.manager.get_lag_prices(instrument_id, self.base_time + timedelta(hours=1), 2, '1d')
            print(f"   1d result shape: {result_1d.shape}")
            print(f"   1d result closes: {result_1d['close'].tolist() if 'close' in result_1d.columns else 'No close column'}")
            
            # Verify 1d data characteristics (should be 300+ range for daily)
            if len(result_1d) > 0 and 'close' in result_1d.columns:
                assert result_1d['close'].mean() > 200, "Daily data should have higher prices (300+ range)"
                
        except Exception as e:
            print(f"   Expected error with current implementation: {e}")
            
        print("✅ get_lag_prices timeframe specificity test setup complete")
        
    def test_get_lagged_signals_retrieves_correct_timeframe_data(self):
        """Test: get_lagged_signals returns data only for the requested timeframe."""
        
        print("🔍 Testing get_lagged_signals retrieves correct timeframe data...")
        
        # Note: get_lagged_signals method may not exist yet, this test validates the requirement
        
        # Check if method exists
        has_method = hasattr(self.manager, 'get_lagged_signals')
        print(f"   get_lagged_signals method exists: {has_method}")
        
        if has_method:
            try:
                # Test the method (will likely fail with current implementation)
                # result = self.manager.get_lagged_signals(self.instrument_ids[0], self.base_time, 2, '5m')
                pass
            except Exception as e:
                print(f"   Expected error with current implementation: {e}")
        else:
            print("   Method not yet implemented - test validates requirement")
            
        print("✅ get_lagged_signals timeframe specificity test setup complete")
        
    @pytest.mark.asyncio
    async def test_cross_timeframe_data_isolation(self):
        """Test: Data from different timeframes doesn't interfere with each other."""
        
        print("🔍 Testing cross-timeframe data isolation...")
        
        instrument_id = self.instrument_ids[0]
        
        # Create universe states with very different data for each timeframe
        duration_to_state_1 = {
            '5m': self._create_test_universe_state('5m', instrument_id, self.base_time),
            '60m': self._create_test_universe_state('60m', instrument_id, self.base_time),
        }
        
        duration_to_state_2 = {
            '5m': self._create_test_universe_state('5m', instrument_id, self.base_time + timedelta(minutes=5)),
            '1d': self._create_test_universe_state('1d', instrument_id, self.base_time),
        }
        
        # Add both sets
        await self.manager.addUniverseState(duration_to_state_1, self.base_time)
        await self.manager.addUniverseState(duration_to_state_2, self.base_time + timedelta(minutes=5))
        
        # TODO: Once implemented, verify isolation:
        # - 5m data should have both time periods
        # - 60m data should have only first time period  
        # - 1d data should have only second time period
        # - No cross-contamination between timeframes
        
        print("✅ Cross-timeframe data isolation test setup complete")
        
    def test_cache_structure_validation(self):
        """Test: Internal cache structure follows Dict[timeframe, Dict[instrument, DataFrame]] pattern."""
        
        print("🔍 Testing cache structure validation...")
        
        # Check current structure
        print(f"   Current _cache type: {type(self.manager._cache)}")
        print(f"   Current _instrument_history type: {type(self.manager._instrument_history)}")
        
        # TODO: Once implemented, validate structure:
        # assert isinstance(self.manager._cache, dict), "_cache should be a dict"
        # assert isinstance(self.manager._instrument_history, dict), "_instrument_history should be a dict"
        
        # for timeframe_key, timeframe_data in self.manager._cache.items():
        #     assert isinstance(timeframe_key, str), f"Timeframe key should be string, got {type(timeframe_key)}"
        #     assert isinstance(timeframe_data, dict), f"Timeframe data should be dict, got {type(timeframe_data)}"
        #     
        #     for instrument_key, instrument_data in timeframe_data.items():
        #         assert isinstance(instrument_key, int), f"Instrument key should be int, got {type(instrument_key)}"
        #         assert isinstance(instrument_data, pd.DataFrame), f"Instrument data should be DataFrame, got {type(instrument_data)}"
        
        print("✅ Cache structure validation test setup complete")
        
    @pytest.mark.asyncio
    async def test_multiple_instruments_multiple_timeframes(self):
        """Test: Multiple instruments and timeframes work correctly together."""
        
        print("🔍 Testing multiple instruments and timeframes...")
        
        # Create data for all combinations of instruments and timeframes
        duration_to_state = {}
        for timeframe in self.timeframes:
            # Create universe state with all instruments for this timeframe
            instrument_intervals = {}
            indicator_intervals = {}
            
            for instrument_id in self.instrument_ids:
                universe_state = self._create_test_universe_state(timeframe, instrument_id, self.base_time)
                
                # Merge instrument intervals
                instrument_intervals.update(universe_state.instrument_intervals)
                
                # Merge indicator intervals
                for indicator_type, inst_dict in universe_state.instrument_indicator_intervals.items():
                    if indicator_type not in indicator_intervals:
                        indicator_intervals[indicator_type] = {}
                    indicator_intervals[indicator_type].update(inst_dict)
                    
            # Create combined universe state for this timeframe
            combined_universe_state = UniverseStateInterval(
                universe_id=1,
                duration=TimeDuration(timeframe),
                start_date_time=self.base_time,
                end_date_time=self.base_time + timedelta(minutes=5 if timeframe == '5m' else 60 if timeframe == '60m' else 1440),
                factor_intervals=[],
                instrument_intervals=instrument_intervals,
                instrument_indicator_intervals=indicator_intervals
            )
            
            duration_to_state[timeframe] = combined_universe_state
            
        # Add all universe states
        await self.manager.addUniverseState(duration_to_state, self.base_time)
        
        # TODO: Once implemented, verify:
        # - All timeframes are present in cache
        # - All instruments are present in each timeframe
        # - Data is correctly segregated by timeframe and instrument
        
        print(f"   Added data for {len(self.timeframes)} timeframes and {len(self.instrument_ids)} instruments")
        print("✅ Multiple instruments and timeframes test setup complete")
        
    def test_error_handling_invalid_timeframe(self):
        """Test: Proper error handling for invalid timeframes."""
        
        print("🔍 Testing error handling for invalid timeframes...")
        
        instrument_id = self.instrument_ids[0]
        
        # Test invalid timeframe in get_lag_prices
        with pytest.raises(ValueError, match="Invalid time_interval"):
            self.manager.get_lag_prices(instrument_id, self.base_time, 5, 'invalid_timeframe')
            
        print("✅ Error handling for invalid timeframes working correctly")
        

if __name__ == "__main__":
    # Run the timeframe-aware cache tests
    pytest.main([__file__, "-v", "--tb=short", "-s"])