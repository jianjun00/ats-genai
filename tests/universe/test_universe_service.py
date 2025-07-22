"""
Comprehensive unit tests for UniverseService.

Tests cover the high-level orchestration service that combines
UniverseStateManager and UniverseStateBuilder functionality.
"""

import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from src.universe.universe_service import UniverseService
from src.universe.universe_state_manager import UniverseStateManager
from src.universe.universe_state_builder import (
    UniverseStateBuilder,
    UniverseMembershipChange,
    CorporateAction,
    UniverseAction,
    CorporateActionType
)
from src.config.environment import Environment


class TestUniverseService:
    """Test suite for UniverseService class."""
    
    @pytest.fixture
    def mock_env(self):
        """Mock Environment instance."""
        env = MagicMock(spec=Environment)
        env.get_database_url.return_value = "postgresql://test:test@localhost/test_db"
        return env
    
    @pytest.fixture
    def mock_state_manager(self):
        """Mock UniverseStateManager instance."""
        return MagicMock(spec=UniverseStateManager)
    
    @pytest.fixture
    def mock_state_builder(self):
        """Mock UniverseStateBuilder instance."""
        return MagicMock(spec=UniverseStateBuilder)
    
    @pytest.fixture
    def universe_service(self, mock_env):
        """Create UniverseService instance for testing."""
        with patch('src.universe.universe_service.UniverseStateManager') as mock_manager_class, \
             patch('src.universe.universe_service.UniverseStateBuilder') as mock_builder_class:
            
            service = UniverseService(base_path="/tmp/test", env=mock_env)
            service.state_manager = MagicMock(spec=UniverseStateManager)
            service.state_builder = MagicMock(spec=UniverseStateBuilder)
            return service
    
    @pytest.fixture
    def sample_universe_data(self):
        """Sample universe data for testing."""
        return pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL', 'MSFT'],
            'market_cap': [2800000000000, 1600000000000, 2400000000000],
            'sector': ['Technology', 'Technology', 'Technology'],
            'is_active': [True, True, True]
        })
    
    def test_initialization(self):
        """Test UniverseService initialization."""
        with patch('src.universe.universe_service.get_environment') as mock_get_env, \
             patch('src.universe.universe_service.UniverseStateManager') as mock_manager, \
             patch('src.universe.universe_service.UniverseStateBuilder') as mock_builder:
            
            mock_env = MagicMock()
            mock_get_env.return_value = mock_env
            
            service = UniverseService()
            
            assert service.env == mock_env
            mock_manager.assert_called_once_with(None)
            mock_builder.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_current_universe_success(self, universe_service, sample_universe_data):
        """Test getting current universe successfully."""
        universe_service.state_manager.load_universe_state.return_value = sample_universe_data
        
        result = await universe_service.get_current_universe()
        
        universe_service.state_manager.load_universe_state.assert_called_once_with(
            timestamp=None,
            filters=None,
            columns=None,
            use_cache=True
        )
        pd.testing.assert_frame_equal(result, sample_universe_data)
    
    @pytest.mark.asyncio
    async def test_get_current_universe_with_filters(self, universe_service, sample_universe_data):
        """Test getting current universe with filters."""
        filters = [('sector', '=', 'Technology')]
        columns = ['symbol', 'market_cap']
        
        universe_service.state_manager.load_universe_state.return_value = sample_universe_data
        
        result = await universe_service.get_current_universe(
            filters=filters,
            columns=columns,
            use_cache=False
        )
        
        universe_service.state_manager.load_universe_state.assert_called_once_with(
            timestamp=None,
            filters=filters,
            columns=columns,
            use_cache=False
        )
    
    @pytest.mark.asyncio
    async def test_get_current_universe_not_found(self, universe_service):
        """Test getting current universe when none exists."""
        universe_service.state_manager.load_universe_state.side_effect = FileNotFoundError("No states found")
        
        with pytest.raises(FileNotFoundError):
            await universe_service.get_current_universe()
    
    @pytest.mark.asyncio
    async def test_get_universe_at_date_success(self, universe_service, sample_universe_data):
        """Test getting universe at specific date."""
        universe_service.state_manager.list_available_states.return_value = [
            "20231201_120000", "20231130_120000", "20231129_120000"
        ]
        universe_service.state_manager.load_universe_state.return_value = sample_universe_data
        
        result = await universe_service.get_universe_at_date('2023-12-01')
        
        # Should find closest timestamp
        universe_service.state_manager.load_universe_state.assert_called_once_with(
            timestamp="20231201_120000",
            filters=None,
            columns=None
        )
        pd.testing.assert_frame_equal(result, sample_universe_data)
    
    @pytest.mark.asyncio
    async def test_get_universe_at_date_not_found(self, universe_service):
        """Test getting universe at date when no suitable state exists."""
        universe_service.state_manager.list_available_states.return_value = [
            "20231203_120000", "20231202_120000"  # All after requested date
        ]
        
        with pytest.raises(FileNotFoundError, match="No universe state found for date"):
            await universe_service.get_universe_at_date('2023-12-01')
    
    @pytest.mark.asyncio
    async def test_update_universe_success(self, universe_service, sample_universe_data):
        """Test updating universe successfully."""
        universe_service.state_builder.build_universe_state.return_value = sample_universe_data
        universe_service.state_manager.save_universe_state.return_value = "/path/to/saved/file.parquet"
        
        with patch('src.universe.universe_service.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
            
            result = await universe_service.update_universe('2023-12-01')
            
            universe_service.state_builder.build_universe_state.assert_called_once_with('2023-12-01')
            universe_service.state_manager.save_universe_state.assert_called_once()
            
            # Check metadata
            call_args = universe_service.state_manager.save_universe_state.call_args
            metadata = call_args[0][2]
            assert metadata['as_of_date'] == '2023-12-01'
            assert metadata['build_method'] == 'incremental'
            
            assert result == "/path/to/saved/file.parquet"
    
    @pytest.mark.asyncio
    async def test_update_universe_force_rebuild(self, universe_service, sample_universe_data):
        """Test updating universe with force rebuild."""
        universe_service.state_builder.rebuild_from_scratch.return_value = sample_universe_data
        universe_service.state_manager.save_universe_state.return_value = "/path/to/saved/file.parquet"
        
        with patch('src.universe.universe_service.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
            
            result = await universe_service.update_universe('2023-12-01', force_rebuild=True)
            
            universe_service.state_builder.rebuild_from_scratch.assert_called_once_with('2023-12-01')
            
            # Check metadata indicates rebuild
            call_args = universe_service.state_manager.save_universe_state.call_args
            metadata = call_args[0][2]
            assert metadata['build_method'] == 'rebuild'
    
    @pytest.mark.asyncio
    async def test_apply_membership_changes(self, universe_service):
        """Test applying membership changes."""
        changes = [
            UniverseMembershipChange(
                symbol='NVDA',
                action=UniverseAction.ADD,
                effective_date='2023-12-01',
                reason='Added'
            )
        ]
        
        await universe_service.apply_membership_changes(changes)
        
        universe_service.state_builder.update_universe_membership.assert_called_once_with(changes)
    
    @pytest.mark.asyncio
    async def test_apply_corporate_actions(self, universe_service, sample_universe_data):
        """Test applying corporate actions."""
        actions = [
            CorporateAction(
                symbol='AAPL',
                action_type=CorporateActionType.SPLIT,
                effective_date='2023-12-01',
                ratio=2.0
            )
        ]
        
        universe_service.get_current_universe = AsyncMock(return_value=sample_universe_data)
        universe_service.state_builder.apply_corporate_actions.return_value = sample_universe_data
        universe_service.state_manager.save_universe_state.return_value = "/path/to/file.parquet"
        
        with patch('src.universe.universe_service.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
            
            await universe_service.apply_corporate_actions(actions)
            
            universe_service.state_builder.apply_corporate_actions.assert_called_once_with(
                sample_universe_data, actions
            )
            universe_service.state_manager.save_universe_state.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_universe_changes(self, universe_service, sample_universe_data):
        """Test getting universe changes between dates."""
        old_data = sample_universe_data.copy()
        new_data = sample_universe_data.copy()
        changes_data = pd.DataFrame({'symbol': ['NVDA'], 'change_type': ['addition']})
        
        universe_service.get_universe_at_date = AsyncMock()
        universe_service.get_universe_at_date.side_effect = [old_data, new_data]
        universe_service.state_builder.calculate_changes.return_value = changes_data
        
        result = await universe_service.get_universe_changes('2023-11-01', '2023-12-01')
        
        assert universe_service.get_universe_at_date.call_count == 2
        universe_service.state_builder.calculate_changes.assert_called_once_with(old_data, new_data)
        pd.testing.assert_frame_equal(result, changes_data)
    
    def test_get_universe_statistics_success(self, universe_service, sample_universe_data):
        """Test getting universe statistics."""
        storage_stats = {
            'total_states': 5,
            'total_size_mb': 10.5,
            'cache_size': 2
        }
        
        universe_service.state_manager.get_storage_stats.return_value = storage_stats
        universe_service.state_manager.load_universe_state.return_value = sample_universe_data
        
        result = universe_service.get_universe_statistics()
        
        # Should include both storage and business stats
        assert 'total_states' in result
        assert 'sector_distribution' in result
        assert 'market_cap_stats' in result
        assert result['active_securities'] == 3
    
    def test_get_universe_statistics_no_current_data(self, universe_service):
        """Test getting statistics when no current universe exists."""
        storage_stats = {'total_states': 0}
        
        universe_service.state_manager.get_storage_stats.return_value = storage_stats
        universe_service.state_manager.load_universe_state.side_effect = Exception("No data")
        
        result = universe_service.get_universe_statistics()
        
        # Should still return storage stats
        assert result['total_states'] == 0
        # Business stats should be empty
        assert result == storage_stats
    
    def test_list_available_dates(self, universe_service):
        """Test listing available universe dates."""
        timestamps = ["20231203_120000", "20231202_150000", "20231201_090000"]
        universe_service.state_manager.list_available_states.return_value = timestamps
        
        result = universe_service.list_available_dates()
        
        expected_dates = ["2023-12-03", "2023-12-02", "2023-12-01"]
        assert result == expected_dates
    
    def test_list_available_dates_with_limit(self, universe_service):
        """Test listing available dates with limit."""
        timestamps = ["20231203_120000", "20231202_150000", "20231201_090000"]
        universe_service.state_manager.list_available_states.return_value = timestamps
        
        result = universe_service.list_available_dates(limit=2)
        
        universe_service.state_manager.list_available_states.assert_called_once_with(2)
        expected_dates = ["2023-12-03", "2023-12-02", "2023-12-01"]
        assert result == expected_dates
    
    def test_list_available_dates_invalid_timestamps(self, universe_service):
        """Test listing dates with some invalid timestamps."""
        timestamps = ["20231203_120000", "invalid_timestamp", "20231201_090000"]
        universe_service.state_manager.list_available_states.return_value = timestamps
        
        result = universe_service.list_available_dates()
        
        # Should skip invalid timestamp
        expected_dates = ["2023-12-03", "2023-12-01"]
        assert result == expected_dates
    
    @pytest.mark.asyncio
    async def test_validate_current_universe_success(self, universe_service, sample_universe_data):
        """Test validating current universe successfully."""
        universe_service.get_current_universe = AsyncMock(return_value=sample_universe_data)
        universe_service.state_builder.validate_universe_state.return_value = True
        
        result = await universe_service.validate_current_universe()
        
        assert result is True
        universe_service.state_builder.validate_universe_state.assert_called_once_with(sample_universe_data)
    
    @pytest.mark.asyncio
    async def test_validate_current_universe_failure(self, universe_service):
        """Test validating current universe with failure."""
        universe_service.get_current_universe = AsyncMock(side_effect=Exception("No universe"))
        
        result = await universe_service.validate_current_universe()
        
        assert result is False
    
    def test_cleanup_old_states(self, universe_service):
        """Test cleaning up old states."""
        universe_service.state_manager.cleanup_old_states.return_value = 3
        
        result = universe_service.cleanup_old_states(keep_days=15)
        
        universe_service.state_manager.cleanup_old_states.assert_called_once_with(15)
        assert result == 3
    
    def test_clear_cache(self, universe_service):
        """Test clearing cache."""
        universe_service.clear_cache()
        
        universe_service.state_manager.clear_cache.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_universe_metadata_success(self, universe_service):
        """Test getting universe metadata."""
        from src.universe.universe_state_manager import UniverseStateMetadata
        
        metadata = UniverseStateMetadata(
            timestamp="20231201_120000",
            record_count=1000,
            file_size_bytes=5242880,  # 5MB
            checksum="abc123",
            created_at="2023-12-01T12:00:00",
            columns=['symbol', 'market_cap'],
            data_sources=['database'],
            universe_type='equity'
        )
        
        universe_service.state_manager.get_latest_timestamp.return_value = "20231201_120000"
        universe_service.state_manager.get_state_metadata.return_value = metadata
        
        result = await universe_service.get_universe_metadata()
        
        assert result['timestamp'] == "20231201_120000"
        assert result['record_count'] == 1000
        assert result['file_size_mb'] == 5.0
        assert result['checksum'] == "abc123"
    
    @pytest.mark.asyncio
    async def test_get_universe_metadata_specific_timestamp(self, universe_service):
        """Test getting metadata for specific timestamp."""
        from src.universe.universe_state_manager import UniverseStateMetadata
        
        metadata = UniverseStateMetadata(
            timestamp="20231201_120000",
            record_count=500,
            file_size_bytes=1048576,  # 1MB
            checksum="def456",
            created_at="2023-12-01T12:00:00",
            columns=['symbol'],
            data_sources=['test'],
            universe_type='test'
        )
        
        universe_service.state_manager.get_state_metadata.return_value = metadata
        
        result = await universe_service.get_universe_metadata("20231201_120000")
        
        universe_service.state_manager.get_state_metadata.assert_called_once_with("20231201_120000")
        assert result['file_size_mb'] == 1.0
    
    @pytest.mark.asyncio
    async def test_get_universe_metadata_no_states(self, universe_service):
        """Test getting metadata when no states exist."""
        universe_service.state_manager.get_latest_timestamp.return_value = None
        
        with pytest.raises(FileNotFoundError, match="No universe states found"):
            await universe_service.get_universe_metadata()
    
    def test_error_handling_update_universe(self, universe_service):
        """Test error handling during universe update."""
        universe_service.state_builder.build_universe_state.side_effect = Exception("Build failed")
        
        with pytest.raises(Exception, match="Build failed"):
            asyncio.run(universe_service.update_universe('2023-12-01'))
    
    @pytest.mark.asyncio
    async def test_integration_full_workflow(self, universe_service, sample_universe_data):
        """Test full workflow integration."""
        # Mock all dependencies
        universe_service.state_builder.build_universe_state.return_value = sample_universe_data
        universe_service.state_manager.save_universe_state.return_value = "/path/to/file.parquet"
        universe_service.state_manager.load_universe_state.return_value = sample_universe_data
        universe_service.state_builder.validate_universe_state.return_value = True
        
        with patch('src.universe.universe_service.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
            
            # Update universe
            file_path = await universe_service.update_universe('2023-12-01')
            assert file_path == "/path/to/file.parquet"
            
            # Get current universe
            current = await universe_service.get_current_universe()
            pd.testing.assert_frame_equal(current, sample_universe_data)
            
            # Validate universe
            is_valid = await universe_service.validate_current_universe()
            assert is_valid is True
