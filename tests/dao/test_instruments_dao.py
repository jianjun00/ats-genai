import pytest
import asyncpg
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import date
from typing import List, Dict
from domains.instruments.repositories.instruments_dao import InstrumentsDAO
from shared.utils.environment import Environment


class TestInstrumentsDAO:
    """Comprehensive test coverage for InstrumentsDAO."""

    @pytest.fixture
    def mock_environment(self):
        """Mock environment with test database configuration."""
        env = MagicMock(spec=Environment)
        env.get_table_name.return_value = "test_instruments"
        env.get_database_url.return_value = "postgresql://test:test@localhost/test_db"
        return env

    @pytest.fixture
    def dao(self, mock_environment):
        """Create InstrumentsDAO instance with mocked environment."""
        return InstrumentsDAO(mock_environment)

    @pytest.mark.asyncio

    async def test_count_instruments_success(self, dao):
        """Test successful instrument count retrieval."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock successful count query
        mock_connection.fetchrow.return_value = {'count': 2500}

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.count_instruments()

        assert result == 2500
        mock_connection.fetchrow.assert_called_once_with("SELECT COUNT(*) as count FROM test_instruments")
        mock_pool.close.assert_called_once()

    @pytest.mark.asyncio

    async def test_count_instruments_empty_result(self, dao):
        """Test count_instruments when no rows returned."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock empty result
        mock_connection.fetchrow.return_value = None

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.count_instruments()

        assert result == 0

    @pytest.mark.asyncio

    async def test_create_instrument_success(self, dao):
        """Test successful instrument creation with all parameters."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock successful insertion returning ID
        mock_connection.fetchrow.return_value = {'id': 123}

        test_params = {
            'symbol': 'AAPL',
            'name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'type_': 'CS',
            'currency': 'USD',
            'list_date': date(1980, 12, 12),
            'delist_date': None
        }

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.create_instrument(**test_params)

        assert result == 123

        # Verify the fetchrow was called with correct SQL and parameters
        mock_connection.fetchrow.assert_called_once()
        call_args = mock_connection.fetchrow.call_args
        sql_query = call_args[0][0]
        params = call_args[0][1:]

        # Check SQL structure
        assert "INSERT INTO test_instruments" in sql_query
        assert "RETURNING id" in sql_query
        assert "VALUES ($1, $2, $3, $4, $5, $6, $7)" in sql_query

        # Check parameters
        assert params[0] == 'AAPL'  # symbol
        assert params[1] == 'Apple Inc.'  # name
        assert params[2] == 'NASDAQ'  # exchange
        assert params[3] == 'CS'  # type_
        assert params[4] == 'USD'  # currency
        assert params[5] == date(1980, 12, 12)  # list_date
        assert params[6] is None  # delist_date

    @pytest.mark.asyncio

    async def test_create_instrument_minimal_params(self, dao):
        """Test instrument creation with only required parameter."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        mock_connection.fetchrow.return_value = {'id': 456}

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.create_instrument('GOOGL')

        assert result == 456

        # Verify optional parameters are passed as None
        call_args = mock_connection.fetchrow.call_args[0]
        params = call_args[1:]
        assert params[0] == 'GOOGL'  # symbol
        assert params[1] is None     # name
        assert params[2] is None     # exchange
        assert params[3] is None     # type_
        assert params[4] is None     # currency
        assert params[5] is None     # list_date
        assert params[6] is None     # delist_date

    @pytest.mark.asyncio

    async def test_create_instrument_no_result(self, dao):
        """Test create_instrument when no result is returned."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock no result (could happen with constraint violations)
        mock_connection.fetchrow.return_value = None

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.create_instrument('DUPLICATE')

        assert result is None

    @pytest.mark.asyncio

    async def test_get_instrument_success(self, dao):
        """Test successful instrument retrieval by ID."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock successful query result
        test_result = {
            'id': 123,
            'symbol': 'AAPL',
            'name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'type': 'CS',
            'currency': 'USD',
            'list_date': date(1980, 12, 12),
            'delist_date': None
        }
        mock_connection.fetchrow.return_value = test_result

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_instrument(123)

        assert result == test_result
        mock_connection.fetchrow.assert_called_once_with(
            "SELECT * FROM test_instruments WHERE id = $1", 123
        )

    @pytest.mark.asyncio

    async def test_get_instrument_not_found(self, dao):
        """Test get_instrument when ID not found."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock not found
        mock_connection.fetchrow.return_value = None

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_instrument(999)

        assert result is None

    @pytest.mark.asyncio

    async def test_list_instruments_success(self, dao):
        """Test successful retrieval of all instruments."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock successful query result
        test_instruments = [
            {'id': 1, 'symbol': 'AAPL', 'name': 'Apple Inc.'},
            {'id': 2, 'symbol': 'GOOGL', 'name': 'Alphabet Inc.'},
            {'id': 3, 'symbol': 'MSFT', 'name': 'Microsoft Corporation'}
        ]
        mock_connection.fetch.return_value = test_instruments

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.list_instruments()

        assert result == test_instruments
        assert len(result) == 3
        mock_connection.fetch.assert_called_once_with("SELECT * FROM test_instruments")

    @pytest.mark.asyncio

    async def test_list_instruments_empty(self, dao):
        """Test list_instruments when no instruments exist."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        mock_connection.fetch.return_value = []

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.list_instruments()

        assert result == []

    @pytest.mark.asyncio

    async def test_get_instrument_by_symbol_success(self, dao):
        """Test successful instrument retrieval by symbol."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock successful query result
        test_result = {
            'id': 123,
            'symbol': 'AAPL',
            'name': 'Apple Inc.',
            'exchange': 'NASDAQ'
        }
        mock_connection.fetchrow.return_value = test_result

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_instrument_by_symbol('AAPL')

        assert result == test_result
        mock_connection.fetchrow.assert_called_once_with(
            "SELECT * FROM test_instruments WHERE symbol = $1",
            'AAPL'
        )

    @pytest.mark.asyncio

    async def test_get_instrument_by_symbol_not_found(self, dao):
        """Test get_instrument_by_symbol when symbol not found."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        mock_connection.fetchrow.return_value = None

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_instrument_by_symbol('NONEXISTENT')

        assert result is None

    @pytest.mark.asyncio

    async def test_create_instruments_batch_success(self, dao):
        """Test successful batch instrument creation."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock successful batch insertion
        test_rows = [{'id': 100}, {'id': 101}, {'id': 102}]
        mock_connection.fetch.return_value = test_rows

        # Test data
        test_instruments = [
            {
                'symbol': 'AAPL',
                'name': 'Apple Inc.',
                'exchange': 'NASDAQ',
                'type_': 'CS',
                'currency': 'USD',
                'list_date': date(1980, 12, 12),
                'delist_date': None
            },
            {
                'symbol': 'GOOGL',
                'name': 'Alphabet Inc.',
                'exchange': 'NASDAQ',
                'type_': 'CS',
                'currency': 'USD',
                'list_date': date(2004, 8, 19),
                'delist_date': None
            },
            {
                'symbol': 'MSFT',
                'name': 'Microsoft Corporation',
                'exchange': 'NASDAQ',
                'type_': 'CS',
                'currency': 'USD',
                'list_date': date(1986, 3, 13),
                'delist_date': None
            }
        ]

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.create_instruments_batch(test_instruments)

        expected_ids = [100, 101, 102]
        assert result == expected_ids

        # Verify SQL structure
        mock_connection.fetch.assert_called_once()
        call_args = mock_connection.fetch.call_args
        sql_query = call_args[0][0]
        params = call_args[0][1:]

        # Check SQL structure
        assert "INSERT INTO test_instruments" in sql_query
        assert "UNNEST(" in sql_query
        assert "ON CONFLICT (symbol) DO NOTHING" in sql_query
        assert "RETURNING id" in sql_query

        # Check that parameters were properly extracted from instruments
        symbols, names, exchanges, types, currencies, list_dates, delist_dates = params
        assert symbols == ['AAPL', 'GOOGL', 'MSFT']
        assert names == ['Apple Inc.', 'Alphabet Inc.', 'Microsoft Corporation']
        assert exchanges == ['NASDAQ', 'NASDAQ', 'NASDAQ']

    @pytest.mark.asyncio

    async def test_create_instruments_batch_empty_list(self, dao):
        """Test batch creation with empty instruments list."""
        result = await dao.create_instruments_batch([])
        assert result == []

    @pytest.mark.asyncio

    async def test_create_instruments_batch_custom_pool_settings(self, dao):
        """Test batch creation with custom pool settings."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        mock_connection.fetch.return_value = [{'id': 200}]

        test_instruments = [{'symbol': 'TEST', 'name': None, 'exchange': None, 'type_': None, 'currency': None, 'list_date': None, 'delist_date': None}]

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool) as mock_create_pool:
            result = await dao.create_instruments_batch(test_instruments, pool_min_size=2, pool_max_size=5)

        assert result == [200]
        # Verify custom pool settings were used
        mock_create_pool.assert_called_once()
        call_kwargs = mock_create_pool.call_args[1]
        assert call_kwargs['min_size'] == 2
        assert call_kwargs['max_size'] == 5

    @pytest.mark.asyncio

    async def test_get_symbols_by_ids_success(self, dao):
        """Test successful symbol retrieval by IDs."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock successful query result
        test_rows = [
            {'id': 1, 'symbol': 'AAPL'},
            {'id': 2, 'symbol': 'GOOGL'},
            {'id': 3, 'symbol': 'MSFT'}
        ]
        mock_connection.fetch.return_value = test_rows

        test_ids = [1, 2, 3]

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_symbols_by_ids(test_ids)

        expected_result = {1: 'AAPL', 2: 'GOOGL', 3: 'MSFT'}
        assert result == expected_result

        # Verify query structure
        mock_connection.fetch.assert_called_once()
        call_args = mock_connection.fetch.call_args
        sql_query = call_args[0][0]
        params = call_args[0][1]

        assert "SELECT id, symbol FROM test_instruments" in sql_query
        assert "WHERE id = ANY($1)" in sql_query
        assert params == test_ids

    @pytest.mark.asyncio

    async def test_get_symbols_by_ids_partial_data(self, dao):
        """Test get_symbols_by_ids when only some IDs exist."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock partial data (only 2 of 3 IDs exist)
        test_rows = [
            {'id': 1, 'symbol': 'AAPL'},
            {'id': 3, 'symbol': 'MSFT'}
        ]
        mock_connection.fetch.return_value = test_rows

        test_ids = [1, 2, 3]  # ID 2 doesn't exist

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_symbols_by_ids(test_ids)

        expected_result = {1: 'AAPL', 3: 'MSFT'}  # ID 2 is missing
        assert result == expected_result
        assert 2 not in result

    @pytest.mark.asyncio

    async def test_get_symbols_by_ids_empty_list(self, dao):
        """Test get_symbols_by_ids with empty ID list."""
        result = await dao.get_symbols_by_ids([])
        assert result == {}

    @pytest.mark.asyncio

    async def test_database_connection_error(self, dao):
        """Test handling of database connection errors."""
        connection_error = Exception("Connection failed")
        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, side_effect=connection_error):
            with pytest.raises(Exception, match="Connection failed"):
                await dao.count_instruments()

    @pytest.mark.asyncio

    async def test_sql_injection_protection_symbol_queries(self, dao):
        """Test that symbol queries use parameterized queries for SQL injection protection."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        mock_connection.fetchrow.return_value = None

        # Test with malicious input
        malicious_symbol = "'; DROP TABLE test_instruments; --"

        with patch(\'domains.market_data.repositories.instruments_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_instrument_by_symbol(malicious_symbol)

        # Verify the malicious input was passed as parameter, not concatenated into SQL
        mock_connection.fetchrow.assert_called_once_with(
            "SELECT * FROM test_instruments WHERE symbol = $1",
            malicious_symbol
        )
        assert result is None

    def test_dao_initialization(self, mock_environment):
        """Test DAO initialization sets correct attributes."""
        dao = InstrumentsDAO(mock_environment)

        assert dao.env == mock_environment
        assert dao.table_name == "test_instruments"
        assert dao.db_url == "postgresql://test:test@localhost/test_db"

        # Verify environment methods were called correctly
        mock_environment.get_table_name.assert_called_once_with('instruments')
        mock_environment.get_database_url.assert_called_once()