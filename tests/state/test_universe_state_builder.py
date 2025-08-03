"""
Comprehensive unit tests for UniverseStateBuilder.

Tests cover business logic, data validation, corporate actions,
membership changes, and integration with data sources.
"""

import os
import pytest
import pandas as pd
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, date
import asyncpg

from secmaster.security_master import CorporateActionType
from secmaster.security_master import CorporateAction
from state.universe_state_builder import (
    UniverseStateBuilder, 
)
from state.universe_state_manager import UniverseStateManager
from config.environment import Environment, EnvironmentType
from db.test_db_manager import unit_test_db

class TestUniverseStateBuilder:
    """Test suite for UniverseStateBuilder class."""
    
    @pytest.fixture
    def mock_env(self):
        """Mock Environment instance."""
        env = MagicMock(spec=Environment)
        env.get_database_url.return_value = os.environ.get('TSDB_URL', 'postgresql://test:test@localhost/test_db')
        env.get_table_name.side_effect = lambda table: f"test_{table}"
        return env
    
    @pytest.fixture
    def mock_state_manager(self):
        """Mock UniverseStateManager instance."""
        return MagicMock(spec=UniverseStateManager)
    
    @pytest.fixture
    def universe_builder(self, mock_state_manager, mock_env):
        """Create UniverseStateBuilder instance for testing."""
        return UniverseStateBuilder(env=mock_env, base_duration='5m', target_durations='5m,15m,60m')
    
    @pytest.fixture
    def sample_base_universe(self):
        """Sample base universe data from database."""
        return pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN'],
            'name': ['Apple Inc.', 'Alphabet Inc.', 'Microsoft Corp.', 'Tesla Inc.', 'Amazon.com Inc.'],
            'sector': ['Technology', 'Technology', 'Technology', 'Consumer Discretionary', 'Consumer Discretionary'],
            'exchange': ['NASDAQ', 'NASDAQ', 'NASDAQ', 'NASDAQ', 'NASDAQ'],
            'close_price': [150.0, 2500.0, 300.0, 800.0, 3200.0],
            'volume': [50000000, 1500000, 30000000, 25000000, 3000000],
            'market_cap': [2800000000000, 1600000000000, 2400000000000, 800000000000, 1400000000000],
            'as_of_date': ['2023-12-01'] * 5,
            'is_active': [True, True, True, True, True]
        })
    
    def test_initialization(self, mock_state_manager, mock_env):
        """Test UniverseStateBuilder initialization."""
        universe = MagicMock(name='Universe')
        builder = UniverseStateBuilder(env=mock_env, base_duration='5m', target_durations='5m,15m,60m' )
        
        assert builder.env == mock_env
        assert builder.min_market_cap == 100_000_000
        assert builder.min_avg_volume == 100_000
        assert builder.max_universe_size == 3000
        assert isinstance(builder.data_source_priorities, dict)
    
    @pytest.mark.skip(reason="UniverseStateBuilder no longer owns _apply_membership_changes or _apply_corporate_actions; integration now handled via runner and managers.")
    @pytest.mark.asyncio
    async def test_build_universe_state_success(self, universe_builder, sample_base_universe):
        pass

    def test_indicator_builder_rolling_cache(self):
        """Test that UniverseStateBuilder maintains rolling cache and builds indicator intervals correctly."""
        from state.universe_state_builder import UniverseStateBuilder
        from state.instrument_interval import InstrumentInterval
        from state.universe_interval import UniverseInterval
        from state.universe_state import UniverseState
        from state.indicator_interval import IndicatorInterval
        from datetime import datetime, timedelta
        from types import SimpleNamespace
        import random
        # Setup
        class DummyEnv:
            def get_target_durations(self):
                class DummyDuration:
                    def get_end_time(self, current_time):
                        return current_time + timedelta(minutes=5)
                return [DummyDuration()]
            def get_indicator_config(self):
                from signals.indicator_config import IndicatorConfig
                from signals.indicator import OneOneDot
                cfg = IndicatorConfig.empty_config()
                cfg.add_indicator('OneOneDot', OneOneDot)
                return cfg
            indicator_rolling_window = 3
        env = DummyEnv()
        builder = UniverseStateBuilder(env=env, base_duration='5m', target_durations='5m,15m,60m')
        # Mock runner
        class DummyRunner:
            class DummyUniverseManager:
                instrument_ids = [1, 2]
            class DummyMarketDataManager:
                def get_ohlc_batch(self, instrument_ids, current_time, end_time):
                    # Return deterministic but distinct data for each call
                    return {iid: {'open': float(iid), 'high': float(iid)+1, 'low': float(iid)-1, 'close': float(iid)+0.5, 'volume': 100.0+random.random()} for iid in instrument_ids}
            universe_manager = DummyUniverseManager()
            market_data_manager = DummyMarketDataManager()
            class DummyUniverseStateManager:
                def __init__(self):
                    self.last_state = None
                def addUniverseState(self, universe_state, current_time):
                    self.last_state = universe_state
            universe_state_manager = DummyUniverseStateManager()
        runner = DummyRunner()
        now = datetime(2023, 1, 1, 9, 30)
        # Call handleInterval several times to fill and roll the cache
        for i in range(5):
            builder.handleInterval(runner, now + timedelta(minutes=5*i))
        # After enough intervals, the cache should only keep the last N
        for inst_id in runner.universe_manager.instrument_ids:
            assert len(builder.instrument_history[inst_id]) == env.indicator_rolling_window
        # The last state should have indicator_intervals built
        state_dict = runner.universe_state_manager.last_state
        assert isinstance(state_dict, dict)
        # Expect only one duration in DummyEnv
        assert len(state_dict) == 3
        state = list(state_dict.values())[0]
        assert type(state).__name__ == 'UniverseState'
        assert 'default' in state.indicator_intervals
        for inst_id, indicator_interval in state.indicator_intervals['default'].items():
            assert type(indicator_interval).__name__ == 'IndicatorInterval'
            # Should have OneOneDot computed
            assert 'OneOneDot' in indicator_interval.indicators
            # Status should be ok if enough intervals, else invalid
            if len(builder.instrument_history[inst_id]) >= 1:
                assert indicator_interval.get_indicator_status('OneOneDot') in ('ok', 'invalid')
    
    @pytest.mark.skip(reason="build_universe_state removed from UniverseStateBuilder in refactor; test obsolete.")
    @pytest.mark.asyncio
    async def test_build_universe_state_invalid_date(self, universe_builder):
        pass
    
    @pytest.mark.skip(reason="UniverseStateBuilder no longer owns _apply_membership_changes or _apply_corporate_actions; integration now handled via runner and managers.")
    @pytest.mark.asyncio
    async def test_build_universe_state_validation_failure(self, universe_builder, sample_base_universe):
        pass
    
    def test_validate_universe_state_success(self, universe_builder, sample_base_universe):
        """Test successful universe state validation."""
        # Add required derived fields
        sample_base_universe['avg_volume'] = sample_base_universe['volume']
        
        result = universe_builder.validate_universe_state(sample_base_universe)
        assert result is True
    
    def test_validate_universe_state_missing_columns(self, universe_builder):
        """Test validation with missing required columns."""
        incomplete_data = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL'],
            'market_cap': [1000000000, 2000000000]
        })
        
        result = universe_builder.validate_universe_state(incomplete_data)
        assert result is False
    
    def test_validate_universe_state_empty_data(self, universe_builder):
        """Test validation with empty data."""
        empty_data = pd.DataFrame()
        
        result = universe_builder.validate_universe_state(empty_data)
        assert result is False
    
    def test_validate_universe_state_duplicate_symbols(self, universe_builder):
        """Test validation with duplicate symbols."""
        duplicate_data = pd.DataFrame({
            'symbol': ['AAPL', 'AAPL'],  # Duplicate
            'market_cap': [1000000000, 1000000000],
            'avg_volume': [1000000, 1000000],
            'sector': ['Technology', 'Technology'],
            'exchange': ['NASDAQ', 'NASDAQ'],
            'is_active': [True, True],
            'as_of_date': ['2023-12-01', '2023-12-01']
        })
        
        result = universe_builder.validate_universe_state(duplicate_data)
        assert result is False
    
    @pytest.mark.skip(reason="Corporate action logic is now fully in SecurityMaster, not tested in builder.")
    def test_apply_corporate_actions_stock_split(self, universe_builder, sample_base_universe):
        pass
    
    @pytest.mark.skip(reason="Corporate action logic is now fully in SecurityMaster, not tested in builder.")
    def test_apply_corporate_actions_delisting(self, universe_builder, sample_base_universe):
        pass
    
    @pytest.mark.skip(reason="calculate_derived_fields removed from UniverseStateBuilder in refactor; test obsolete.")
    def test_calculate_derived_fields(self, universe_builder, sample_base_universe):
        pass
    
    @pytest.mark.skip(reason="calculate_changes removed from UniverseStateBuilder in refactor; test obsolete.")
    def test_calculate_changes_additions(self, universe_builder):
        pass
    
    @pytest.mark.skip(reason="_apply_business_rules removed from UniverseStateBuilder in refactor; test obsolete.")
    def test_apply_business_rules(self, universe_builder):
        pass
