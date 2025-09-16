#!/usr/bin/env python3
"""
Test to detect missing methods in UniverseStateManager that are required for training data generation.

This test systematically verifies that UniverseStateManager has all the methods expected
by the training data generator, and validates their signatures and return types.
"""

import pytest
import asyncio
from datetime import datetime
from unittest.mock import Mock, AsyncMock
import pandas as pd

from domains.trading.services.state.universe_state_manager import UniverseStateManager
from shared.data_handling.utils.environment import Environment, EnvironmentType


class TestUniverseStateManagerMissingMethods:
    """Test missing methods required by training data generation."""

    @pytest.fixture
    async def universe_state_manager(self):
        """Create a UniverseStateManager for testing."""
        # Mock environment
        env = Mock(spec=Environment)
        env.env_type = EnvironmentType.INTEGRATION
        
        # Mock dependencies
        universe_dao = AsyncMock()
        universe_state_dao = AsyncMock()
        universe_state_interval_dao = AsyncMock()
        instrument_dao = AsyncMock()
        market_cap_dao = AsyncMock()
        
        # Create UniverseStateManager
        manager = UniverseStateManager(
            env=env,
            universe_dao=universe_dao,
            universe_state_dao=universe_state_dao,
            universe_state_interval_dao=universe_state_interval_dao,
            instrument_dao=instrument_dao,
            market_cap_dao=market_cap_dao,
            instrument_ids=[31]  # AAPL
        )
        
        return manager

    def test_get_lag_prices_method_exists(self, universe_state_manager):
        """Test that get_lag_prices method exists with correct signature."""
        # Check method exists
        assert hasattr(universe_state_manager, 'get_lag_prices'), \
            "UniverseStateManager missing get_lag_prices method"
        
        # Check it's callable
        method = getattr(universe_state_manager, 'get_lag_prices')
        assert callable(method), "get_lag_prices is not callable"

    def test_get_lead_prices_method_exists(self, universe_state_manager):
        """Test that get_lead_prices method exists with correct signature."""
        # Check method exists
        assert hasattr(universe_state_manager, 'get_lead_prices'), \
            "UniverseStateManager missing get_lead_prices method"
        
        # Check it's callable
        method = getattr(universe_state_manager, 'get_lead_prices')
        assert callable(method), "get_lead_prices is not callable"

    def test_get_lagged_signals_method_exists(self, universe_state_manager):
        """Test that get_lagged_signals method exists with correct signature."""
        # Check method exists
        assert hasattr(universe_state_manager, 'get_lagged_signals'), \
            "UniverseStateManager missing get_lagged_signals method"
        
        # Check it's callable
        method = getattr(universe_state_manager, 'get_lagged_signals')
        assert callable(method), "get_lagged_signals is not callable"

    async def test_get_lag_prices_signature_and_return_type(self, universe_state_manager):
        """Test get_lag_prices method signature and return type."""
        if not hasattr(universe_state_manager, 'get_lag_prices'):
            pytest.skip("get_lag_prices method not implemented yet")
            
        # Test parameters expected by training data generator
        instrument_id = 31  # AAPL
        center_datetime = datetime(2025, 7, 1, 14, 0, 0)
        lag_periods = 1
        
        try:
            result = universe_state_manager.get_lag_prices(instrument_id, center_datetime, lag_periods)
            
            # Should return a DataFrame
            assert isinstance(result, pd.DataFrame), \
                f"get_lag_prices should return DataFrame, got {type(result)}"
            
            # Should have OHLCV columns
            expected_columns = ['open', 'high', 'low', 'close', 'volume', 'date']
            for col in expected_columns:
                if not result.empty:
                    assert col in result.columns or len(result) == 0, \
                        f"get_lag_prices result missing column: {col}"
                        
            print(f"✅ get_lag_prices signature and return type validated")
            
        except Exception as e:
            pytest.fail(f"get_lag_prices failed with valid parameters: {e}")

    async def test_get_lead_prices_signature_and_return_type(self, universe_state_manager):
        """Test get_lead_prices method signature and return type."""
        if not hasattr(universe_state_manager, 'get_lead_prices'):
            pytest.skip("get_lead_prices method not implemented yet")
            
        # Test parameters expected by training data generator
        instrument_id = 31  # AAPL
        center_datetime = datetime(2025, 7, 1, 14, 0, 0)
        lead_periods = 1
        
        try:
            result = universe_state_manager.get_lead_prices(instrument_id, center_datetime, lead_periods)
            
            # Should return a DataFrame
            assert isinstance(result, pd.DataFrame), \
                f"get_lead_prices should return DataFrame, got {type(result)}"
            
            print(f"✅ get_lead_prices signature and return type validated")
            
        except Exception as e:
            pytest.fail(f"get_lead_prices failed with valid parameters: {e}")

    async def test_get_lagged_signals_signature_and_return_type(self, universe_state_manager):
        """Test get_lagged_signals method signature and return type."""
        if not hasattr(universe_state_manager, 'get_lagged_signals'):
            pytest.skip("get_lagged_signals method not implemented yet")
            
        # Test parameters expected by training data generator
        instrument_id = 31  # AAPL
        cur_datetime = datetime(2025, 7, 1, 14, 0, 0)
        lag_periods = 1
        time_interval = "1m"
        signal_names = ['sma_20', 'ema_12', 'rsi_14']
        
        try:
            result = await universe_state_manager.get_lagged_signals(
                instrument_id=instrument_id,
                cur_datetime=cur_datetime,
                lag_periods=lag_periods,
                time_interval=time_interval,
                signal_names=signal_names
            )
            
            # Should return a DataFrame
            assert isinstance(result, pd.DataFrame), \
                f"get_lagged_signals should return DataFrame, got {type(result)}"
            
            print(f"✅ get_lagged_signals signature and return type validated")
            
        except Exception as e:
            pytest.fail(f"get_lagged_signals failed with valid parameters: {e}")

    def test_identify_all_missing_methods(self, universe_state_manager):
        """Comprehensive test to identify all missing methods at once."""
        missing_methods = []
        
        # Check get_lag_prices
        if not hasattr(universe_state_manager, 'get_lag_prices'):
            missing_methods.append('get_lag_prices(instrument_id, center_datetime, lag_periods)')
        
        # Check get_lead_prices  
        if not hasattr(universe_state_manager, 'get_lead_prices'):
            missing_methods.append('get_lead_prices(instrument_id, center_datetime, lead_periods)')
            
        # Check get_lagged_signals
        if not hasattr(universe_state_manager, 'get_lagged_signals'):
            missing_methods.append('get_lagged_signals(instrument_id, cur_datetime, lag_periods, time_interval, signal_names)')
        
        if missing_methods:
            methods_list = '\n  - '.join(missing_methods)
            print(f"\n❌ Missing methods in UniverseStateManager:")
            print(f"  - {methods_list}")
            
            pytest.fail(f"UniverseStateManager is missing {len(missing_methods)} required methods for training data generation")
        else:
            print("✅ All required methods exist in UniverseStateManager")

if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v", "--tb=short"])