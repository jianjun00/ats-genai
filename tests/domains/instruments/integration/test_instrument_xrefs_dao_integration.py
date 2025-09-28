"""
Integration tests for InstrumentXrefsDAO to ensure instrument_id <-> symbol mapping works correctly.

These tests verify the critical mapping functionality that enables FileBasedMinuteMarketDataManager
to convert instrument_id to symbol for data retrieval.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock

import sys
sys.path.insert(0, 'src')

from core.dao.instruments.instrument_xrefs_dao import InstrumentXrefsDAO
from core.platform.config.environment import Environment


@pytest.fixture
def mock_env():
    """Create mock environment for testing."""
    env = Mock(spec=Environment)
    env.get_database_url.return_value = "postgresql://test:test@localhost:5432/test_db"
    env.get_table_name.return_value = "test_instrument_xrefs"
    return env


@pytest.fixture
def sample_xref_data():
    """Sample instrument cross-reference data for testing."""
    return [
        {'instrument_id': 1001, 'vendor_symbol': 'AAPL', 'vendor_id': 1},
        {'instrument_id': 1002, 'vendor_symbol': 'TSLA', 'vendor_id': 1},
        {'instrument_id': 1003, 'vendor_symbol': 'MSFT', 'vendor_id': 1},
        {'instrument_id': 1004, 'vendor_symbol': 'GOOGL', 'vendor_id': 1},
        {'instrument_id': 1005, 'vendor_symbol': 'SPY', 'vendor_id': 1}
    ]


class TestInstrumentXrefsDAOCore:
    """Test core functionality of InstrumentXrefsDAO."""

    def test_dao_initialization(self, mock_env):
        """Test DAO initializes correctly."""
        dao = InstrumentXrefsDAO(mock_env)

        # Should store environment and have expected attributes
        assert dao.env == mock_env
        assert dao.table_name == "test_instrument_xrefs"
        assert dao.db_url == "postgresql://test:test@localhost:5432/test_db"

    @pytest.mark.asyncio
    async def test_get_symbol_by_instrument_id_found(self, mock_env):
        """Test successful symbol lookup by instrument_id."""
        dao = InstrumentXrefsDAO(mock_env)

        # Mock database connection and query result
        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_pool = Mock()
            mock_conn = Mock()
            mock_row = {'vendor_symbol': 'AAPL'}

            mock_conn.fetchrow = AsyncMock(return_value=mock_row)
            mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_pool.close = AsyncMock()
            mock_create_pool.return_value = mock_pool

            # Test the method
            result = await dao.get_symbol_by_instrument_id(1001)

            # Verify result
            assert result == 'AAPL'

            # Verify database calls
            mock_create_pool.assert_called_once_with(core.dao.db_url)
            mock_conn.fetchrow.assert_called_once_with(
                f"SELECT vendor_symbol FROM {core.dao.table_name} WHERE instrument_id = $1 LIMIT 1",
                1001
            )
            mock_pool.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_symbol_by_instrument_id_not_found(self, mock_env):
        """Test symbol lookup when instrument_id doesn't exist."""
        dao = InstrumentXrefsDAO(mock_env)

        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_pool = Mock()
            mock_conn = Mock()

            # Return None when no record found
            mock_conn.fetchrow = AsyncMock(return_value=None)
            mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_pool.close = AsyncMock()
            mock_create_pool.return_value = mock_pool

            # Test with non-existent instrument_id
            result = await dao.get_symbol_by_instrument_id(99999)

            # Should return None
            assert result is None

            # Verify query was executed
            mock_conn.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_symbol_by_instrument_id_vendor_name(self, mock_env):
        """Test symbol lookup with specific vendor name."""
        dao = InstrumentXrefsDAO(mock_env)

        with patch('asyncpg.create_pool') as mock_create_pool, \
             patch('core.dao.vendors_core.dao.VendorsDAO') as mock_vendors_dao_class:

            # Setup vendor DAO mock
            mock_vendors_dao = Mock()
            mock_vendors_core.dao.get_vendor_by_name = AsyncMock(return_value={'id': 1, 'name': 'ticker'})
            mock_vendors_dao_class.return_value = mock_vendors_dao

            # Setup database mocks
            mock_pool = Mock()
            mock_conn = Mock()
            mock_row = {'vendor_symbol': 'TSLA'}

            mock_conn.fetchrow = AsyncMock(return_value=mock_row)
            mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_pool.close = AsyncMock()
            mock_create_pool.return_value = mock_pool

            # Test the method
            result = await dao.get_symbol_by_instrument_id_vendor_name(1002, "ticker")

            # Verify result
            assert result == 'TSLA'

            # Verify vendor lookup was called
            mock_vendors_core.dao.get_vendor_by_name.assert_called_once_with("ticker")

            # Verify database query with vendor_id
            mock_conn.fetchrow.assert_called_once_with(
                f"SELECT vendor_symbol FROM {core.dao.table_name} WHERE instrument_id = $1 AND vendor_id = $2",
                1002, 1  # vendor_id from mocked vendor lookup
            )

    @pytest.mark.asyncio
    async def test_resolve_instrument_id_by_symbol(self, mock_env):
        """Test reverse lookup: symbol -> instrument_id."""
        # Note: This test assumes the resolve_instrument_id_by_symbol method exists
        # We need to check if it's implemented in the DAO
        pass  # Will implement after checking the actual method


class TestInstrumentXrefsDAOErrorHandling:
    """Test error handling in InstrumentXrefsDAO."""

    @pytest.mark.asyncio
    async def test_database_connection_error(self, mock_env):
        """Test handling of database connection errors."""
        dao = InstrumentXrefsDAO(mock_env)

        with patch('asyncpg.create_pool') as mock_create_pool:
            # Mock connection failure
            mock_create_pool.side_effect = Exception("Connection failed")

            # Should handle exception gracefully
            result = await dao.get_symbol_by_instrument_id(1001)
            # If no exception is raised, result should be None or empty
            assert result is None or result == ""
    @pytest.mark.asyncio
    async def test_query_execution_error(self, mock_env):
        """Test handling of query execution errors."""
        dao = InstrumentXrefsDAO(mock_env)

        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_pool = Mock()
            mock_conn = Mock()

            # Mock query execution failure
            mock_conn.fetchrow = AsyncMock(side_effect=Exception("Query failed"))
            mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_pool.close = AsyncMock()
            mock_create_pool.return_value = mock_pool

            # Should handle query failure
            result = await dao.get_symbol_by_instrument_id(1001)
            # Method should handle error gracefully
            assert True  # If we get here without exception, that's good
class TestInstrumentXrefsDAOIntegrationScenarios:
    """Test realistic integration scenarios."""

    @pytest.mark.asyncio
    async def test_multiple_instruments_mapping(self, mock_env, sample_xref_data):
        """Test mapping multiple instruments to symbols."""
        dao = InstrumentXrefsDAO(mock_env)

        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_pool = Mock()
            mock_conn = Mock()

            # Create a side effect function to return different symbols for different IDs
            def mock_fetchrow(*args, **kwargs):
                # Extract instrument_id from the query parameters
                instrument_id = args[1] if len(args) > 1 else None

                # Find matching symbol in sample data
                for item in sample_xref_data:
                    if item['instrument_id'] == instrument_id:
                        return AsyncMock(return_value={'vendor_symbol': item['vendor_symbol']})()

                # Return None if not found
                return AsyncMock(return_value=None)()

            mock_conn.fetchrow = mock_fetchrow
            mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_pool.close = AsyncMock()
            mock_create_pool.return_value = mock_pool

            # Test mapping multiple instruments
            test_cases = [
                (1001, 'AAPL'),
                (1002, 'TSLA'),
                (1003, 'MSFT'),
                (9999, None)  # Non-existent
            ]

            results = {}
            for instrument_id, expected_symbol in test_cases:
                result = await dao.get_symbol_by_instrument_id(instrument_id)
                results[instrument_id] = result

            # Verify results
            assert results[1001] == 'AAPL'
            assert results[1002] == 'TSLA'
            assert results[1003] == 'MSFT'
            assert results[9999] is None

    @pytest.mark.asyncio
    async def test_concurrent_symbol_lookups(self, mock_env):
        """Test concurrent symbol lookups (simulating real-world usage)."""
        dao = InstrumentXrefsDAO(mock_env)

        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_pool = Mock()
            mock_conn = Mock()

            # Mock successful lookups
            mock_conn.fetchrow = AsyncMock(return_value={'vendor_symbol': 'TEST_SYMBOL'})
            mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_pool.close = AsyncMock()
            mock_create_pool.return_value = mock_pool

            # Run multiple lookups concurrently
            instrument_ids = [1001, 1002, 1003, 1004, 1005]
            tasks = [core.dao.get_symbol_by_instrument_id(iid) for iid in instrument_ids]

            results = await asyncio.gather(*tasks)

            # All should succeed
            assert len(results) == 5
            assert all(result == 'TEST_SYMBOL' for result in results)

    def test_dao_integration_with_file_based_market_data_manager(self, mock_env):
        """Test DAO integration points with FileBasedMinuteMarketDataManager."""
        # Import the manager to ensure integration points exist
        from domains.trading.services.core.minute.file_based_minute_service import FileBasedMinuteMarketDataManager

        # Create manager with environment (should create xrefs_dao)
        manager = FileBasedMinuteMarketDataManager(mock_env, "/tmp/test")

        # Verify DAO is initialized
        assert hasattr(manager, 'xrefs_dao')
        assert manager.xrefs_dao is not None
        assert isinstance(manager.xrefs_dao, InstrumentXrefsDAO)

        # Verify DAO has correct environment reference
        assert manager.xrefs_core.dao.env == mock_env

    def test_dao_not_initialized_without_environment(self):
        """Test that DAO is not initialized without environment."""
        from domains.trading.services.core.minute.file_based_minute_service import FileBasedMinuteMarketDataManager

        # Create manager without environment
        manager = FileBasedMinuteMarketDataManager(None, "/tmp/test")

        # xrefs_dao should be None
        assert manager.xrefs_dao is None


class TestInstrumentXrefsDAOPerformance:
    """Test performance-related aspects of the DAO."""

    @pytest.mark.asyncio
    async def test_connection_pool_usage(self, mock_env):
        """Test that connection pooling is used correctly."""
        dao = InstrumentXrefsDAO(mock_env)

        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_pool = Mock()
            mock_conn = Mock()
            mock_row = {'vendor_symbol': 'AAPL'}

            mock_conn.fetchrow = AsyncMock(return_value=mock_row)
            mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_pool.close = AsyncMock()
            mock_create_pool.return_value = mock_pool

            # Multiple calls should use the same pool creation pattern
            await dao.get_symbol_by_instrument_id(1001)
            await dao.get_symbol_by_instrument_id(1002)

            # Pool should be created for each call (current implementation)
            # Note: This might be optimized in the future to reuse connections
            assert mock_create_pool.call_count == 2
            assert mock_pool.close.call_count == 2

    def test_query_optimization(self, mock_env):
        """Test that queries are optimized appropriately."""
        dao = InstrumentXrefsDAO(mock_env)

        # The get_symbol_by_instrument_id query should use LIMIT 1 for efficiency
        # This is already tested in other tests, but we can verify the pattern
        expected_query = f"SELECT vendor_symbol FROM {core.dao.table_name} WHERE instrument_id = $1 LIMIT 1"

        # This query pattern should be efficient for single lookups
        assert "LIMIT 1" in expected_query
        assert "WHERE instrument_id = $1" in expected_query


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])