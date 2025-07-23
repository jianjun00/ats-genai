"""
Comprehensive unit tests for UniverseStateBuilder.

Tests cover business logic, data validation, corporate actions,
membership changes, and integration with data sources.
"""

import pytest
import pandas as pd
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, date
import asyncpg

from src.universe.universe_state_builder import (
    UniverseStateBuilder, 
    UniverseAction, 
    CorporateActionType,
    UniverseMembershipChange,
    CorporateAction
)
from src.universe.universe_state_manager import UniverseStateManager
from src.config.environment import Environment


class TestUniverseStateBuilder:
    """Test suite for UniverseStateBuilder class."""
    
    @pytest.fixture
    def mock_env(self):
        """Mock Environment instance."""
        env = MagicMock(spec=Environment)
        env.get_database_url.return_value = "postgresql://test:test@localhost/test_db"
        env.get_table_name.side_effect = lambda table: f"test_{table}"
        return env
    
    @pytest.fixture
    def mock_state_manager(self):
        """Mock UniverseStateManager instance."""
        return MagicMock(spec=UniverseStateManager)
    
    @pytest.fixture
    def universe_builder(self, mock_state_manager, mock_env):
        """Create UniverseStateBuilder instance for testing."""
        return UniverseStateBuilder(mock_state_manager, mock_env)
    
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
        builder = UniverseStateBuilder(mock_state_manager, mock_env)
        
        assert builder.state_manager == mock_state_manager
        assert builder.env == mock_env
        assert builder.min_market_cap == 100_000_000
        assert builder.min_avg_volume == 100_000
        assert builder.max_universe_size == 3000
        assert isinstance(builder.data_source_priorities, dict)
    
    @pytest.mark.asyncio
    async def test_build_universe_state_success(self, universe_builder, sample_base_universe):
        """Test successful universe state building."""
        # Mock database operations
        with patch.object(universe_builder, '_get_base_universe', return_value=sample_base_universe), \
             patch.object(universe_builder, '_apply_membership_changes', return_value=sample_base_universe), \
             patch.object(universe_builder, '_apply_corporate_actions', return_value=sample_base_universe), \
             patch.object(universe_builder, 'validate_universe_state', return_value=True):
            
            result = await universe_builder.build_universe_state('2023-12-01')
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 5
            assert 'market_cap_tier' in result.columns
            assert 'liquidity_tier' in result.columns
            assert 'market_cap_rank' in result.columns
    
    @pytest.mark.asyncio
    async def test_build_universe_state_invalid_date(self, universe_builder):
        """Test building universe state with invalid date format."""
        with pytest.raises(RuntimeError) as excinfo:
            await universe_builder.build_universe_state('invalid-date')
        assert 'does not match format' in str(excinfo.value) or 'Universe building failed' in str(excinfo.value)
    
    @pytest.mark.asyncio
    async def test_build_universe_state_validation_failure(self, universe_builder, sample_base_universe):
        """Test universe building with validation failure."""
        with patch.object(universe_builder, '_get_base_universe', return_value=sample_base_universe), \
             patch.object(universe_builder, '_apply_membership_changes', return_value=sample_base_universe), \
             patch.object(universe_builder, '_apply_corporate_actions', return_value=sample_base_universe), \
             patch.object(universe_builder, 'validate_universe_state', return_value=False):
            
            with pytest.raises(RuntimeError, match="Universe state validation failed"):
                await universe_builder.build_universe_state('2023-12-01')
    
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
    
    def test_apply_corporate_actions_stock_split(self, universe_builder, sample_base_universe):
        """Test applying stock split corporate action."""
        split_action = CorporateAction(
            symbol='AAPL',
            action_type=CorporateActionType.SPLIT,
            effective_date='2023-12-01',
            ratio=2.0
        )
        
        result = universe_builder.apply_corporate_actions(sample_base_universe, [split_action])
        
        # Check that AAPL price was adjusted for split
        aapl_row = result[result['symbol'] == 'AAPL'].iloc[0]
        original_aapl = sample_base_universe[sample_base_universe['symbol'] == 'AAPL'].iloc[0]
        
        assert aapl_row['close_price'] == original_aapl['close_price'] / 2.0  # Price halved
        assert aapl_row['volume'] == original_aapl['volume'] * 2.0  # Volume doubled
    
    def test_apply_corporate_actions_delisting(self, universe_builder, sample_base_universe):
        """Test applying delisting corporate action."""
        delisting_action = CorporateAction(
            symbol='TSLA',
            action_type=CorporateActionType.DELISTING,
            effective_date='2023-12-01'
        )
        
        result = universe_builder.apply_corporate_actions(sample_base_universe, [delisting_action])
        
        # TSLA should be removed
        assert 'TSLA' not in result['symbol'].values
        assert len(result) == 4  # One less than original
    
    def test_calculate_derived_fields(self, universe_builder, sample_base_universe):
        """Test calculation of derived fields."""
        result = universe_builder.calculate_derived_fields(sample_base_universe)
        
        # Check that derived fields were added
        assert 'market_cap_tier' in result.columns
        assert 'liquidity_tier' in result.columns
        assert 'price_tier' in result.columns
        assert 'market_cap_rank' in result.columns
        assert 'sector_weight' in result.columns
        assert 'avg_volume' in result.columns
        
        # Check market cap tiers
        assert result['market_cap_tier'].notna().all()
        
        # Check rankings
        assert result['market_cap_rank'].min() == 1
        assert result['market_cap_rank'].max() <= len(result)
    
    def test_calculate_changes_additions(self, universe_builder):
        """Test calculating changes - additions."""
        old_state = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL'],
            'market_cap': [1000000000, 2000000000],
            'sector': ['Technology', 'Technology'],
            'exchange': ['NASDAQ', 'NASDAQ'],
            'is_active': [True, True]
        })
        
        new_state = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL', 'MSFT'],  # MSFT added
            'market_cap': [1000000000, 2000000000, 1500000000],
            'sector': ['Technology', 'Technology', 'Technology'],
            'exchange': ['NASDAQ', 'NASDAQ', 'NASDAQ'],
            'is_active': [True, True, True]
        })
        
        changes = universe_builder.calculate_changes(old_state, new_state)
        
        additions = changes[changes['change_type'] == 'addition']
        assert len(additions) == 1
        assert additions.iloc[0]['symbol'] == 'MSFT'
    
    def test_apply_business_rules(self, universe_builder):
        """Test application of business rules."""
        test_data = pd.DataFrame({
            'symbol': ['AAPL', 'PENNY', 'LOWVOL', 'INACTIVE'],
            'market_cap': [2000000000000, 50000000, 200000000000, 500000000000],  # PENNY below min
            'volume': [50000000, 1000000, 50000, 10000000],  # LOWVOL below min
            'avg_volume': [50000000, 1000000, 50000, 10000000],
            'is_active': [True, True, True, False],  # INACTIVE is false
            'sector': ['Technology'] * 4,
            'exchange': ['NASDAQ'] * 4,
            'as_of_date': ['2023-12-01'] * 4
        })
        
        result = universe_builder._apply_business_rules(test_data)
        
        # Only AAPL should pass all filters
        assert len(result) == 1
        assert result.iloc[0]['symbol'] == 'AAPL'
