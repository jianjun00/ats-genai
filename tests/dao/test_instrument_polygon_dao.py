import pytest
import asyncpg
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from dao.instrument_polygon_dao import InstrumentPolygonDAO
from config.environment import Environment


class TestInstrumentPolygonDAO:
    """Comprehensive test coverage for InstrumentPolygonDAO."""
    
    @pytest.fixture
    def mock_environment(self):
        """Mock environment with test database configuration."""
        env = MagicMock(spec=Environment)
        env.get_table_name.return_value = "test_instrument_polygon"
        env.get_database_url.return_value = "postgresql://test:test@localhost/test_db"
        return env
    
    @pytest.fixture
    def dao(self, mock_environment):
        """Create DAO instance with mocked environment."""
        return InstrumentPolygonDAO(mock_environment)
    
    async def test_count_instruments_success(self, dao):
        """Test successful instrument count retrieval."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        # Setup async context manager for pool.acquire()
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        
        # Mock successful count query
        mock_connection.fetchrow.return_value = {'count': 1500}
        mock_pool.close = AsyncMock()
        
        with patch('dao.instrument_polygon_dao.asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.count_instruments()
        
        assert result == 1500
        mock_connection.fetchrow.assert_called_once_with("SELECT COUNT(*) as count FROM test_instrument_polygon")
        mock_pool.close.assert_called_once()
    
    async def test_count_instruments_empty_result(self, dao):
        """Test count_instruments when no rows returned."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        
        # Mock empty result
        mock_connection.fetchrow.return_value = None
        mock_pool.close = AsyncMock()
        
        with patch('dao.instrument_polygon_dao.asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.count_instruments()
        
        assert result == 0
        mock_pool.close.assert_called_once()
    
    async def test_count_instruments_pool_close_timeout(self, dao):
        """Test count_instruments handles pool close timeout gracefully."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        
        mock_connection.fetchrow.return_value = {'count': 500}
        
        # Mock timeout on pool close
        import asyncio
        mock_pool.close = AsyncMock(side_effect=asyncio.TimeoutError())
        
        with patch('dao.instrument_polygon_dao.asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            with patch('builtins.print') as mock_print:
                result = await dao.count_instruments()
        
        assert result == 500
        mock_print.assert_called_with("[WARN] pool.close() timed out after 2 seconds")
    
    async def test_get_latest_update_timestamp_success(self, dao):
        """Test successful latest timestamp retrieval."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        
        # Mock successful timestamp query
        test_timestamp = datetime(2024, 1, 15, 10, 30, 0)
        mock_connection.fetchrow.return_value = {'latest': test_timestamp}
        mock_pool.close = AsyncMock()
        
        with patch('dao.instrument_polygon_dao.asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_latest_update_timestamp()
        
        assert result == test_timestamp
        mock_connection.fetchrow.assert_called_once_with("SELECT MAX(updated_at) as latest FROM test_instrument_polygon")
    
    async def test_get_latest_update_timestamp_no_data(self, dao):
        """Test get_latest_update_timestamp when no data exists."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        
        # Mock empty result
        mock_connection.fetchrow.return_value = None
        mock_pool.close = AsyncMock()
        
        with patch('dao.instrument_polygon_dao.asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_latest_update_timestamp()
        
        assert result is None
    
    async def test_insert_instrument_success(self, dao):
        """Test successful instrument insertion with all parameters."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        
        mock_connection.execute = AsyncMock()
        mock_pool.close = AsyncMock()
        
        # Test data
        test_params = {
            'symbol': 'AAPL',
            'name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'type_': 'CS',
            'currency': 'USD',
            'figi': 'BBG000B9XRY4',
            'isin': 'US0378331005',
            'cusip': '037833100',
            'composite_figi': 'BBG000B9XRY4',
            'active': True,
            'list_date': datetime(1980, 12, 12).date(),
            'delist_date': None,
            'raw': '{"test": "data"}'
        }
        
        with patch('dao.instrument_polygon_dao.asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            await dao.insert_instrument(**test_params)
        
        # Verify the execute was called with correct SQL and parameters
        mock_connection.execute.assert_called_once()
        call_args = mock_connection.execute.call_args
        sql_query = call_args[0][0]
        params = call_args[0][1:]
        
        # Check SQL contains required elements
        assert "INSERT INTO test_instrument_polygon" in sql_query
        assert "ON CONFLICT (symbol) DO UPDATE SET" in sql_query
        assert "updated_at=now()" in sql_query
        
        # Check parameters match
        assert params[0] == 'AAPL'  # symbol
        assert params[1] == 'Apple Inc.'  # name
        assert params[2] == 'NASDAQ'  # exchange
    
    async def test_get_instrument_by_symbol_success(self, dao):
        """Test successful instrument retrieval by symbol."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        
        # Mock successful query result
        test_result = {
            'symbol': 'AAPL',
            'name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'active': True
        }
        mock_connection.fetchrow.return_value = test_result
        mock_pool.close = AsyncMock()
        
        with patch('dao.instrument_polygon_dao.asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            with patch('builtins.print'):  # Suppress debug prints
                result = await dao.get_instrument_by_symbol('AAPL')
        
        assert result == test_result
        mock_connection.fetchrow.assert_called_once_with(
            "SELECT * FROM test_instrument_polygon WHERE symbol = $1", 
            'AAPL'
        )
    
    async def test_get_instrument_by_symbol_not_found(self, dao):
        """Test get_instrument_by_symbol when symbol not found."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        
        # Mock not found
        mock_connection.fetchrow.return_value = None
        mock_pool.close = AsyncMock()
        
        with patch('dao.instrument_polygon_dao.asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            with patch('builtins.print'):  # Suppress debug prints
                result = await dao.get_instrument_by_symbol('NONEXISTENT')
        
        assert result is None
    
    async def test_get_all_symbols_success(self, dao):
        """Test successful retrieval of all symbols."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        
        # Mock successful query result
        test_rows = [
            {'symbol': 'AAPL'},
            {'symbol': 'GOOGL'},
            {'symbol': 'MSFT'}
        ]
        mock_connection.fetch.return_value = test_rows
        mock_pool.close = AsyncMock()
        
        with patch('dao.instrument_polygon_dao.asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            with patch('builtins.print'):  # Suppress debug prints
                result = await dao.get_all_symbols()
        
        expected_symbols = ['AAPL', 'GOOGL', 'MSFT']
        assert result == expected_symbols
        mock_connection.fetch.assert_called_once_with("SELECT symbol FROM test_instrument_polygon")
    
    async def test_get_all_symbols_empty_table(self, dao):
        """Test get_all_symbols when table is empty."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        
        # Mock empty result
        mock_connection.fetch.return_value = []
        mock_pool.close = AsyncMock()
        
        with patch('dao.instrument_polygon_dao.asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            with patch('builtins.print'):  # Suppress debug prints
                result = await dao.get_all_symbols()
        
        assert result == []
    
    async def test_database_connection_error(self, dao):
        """Test handling of database connection errors."""
        connection_error = Exception("Connection failed")
        with patch('dao.instrument_polygon_dao.asyncpg.create_pool', new_callable=AsyncMock, side_effect=connection_error):
            with pytest.raises(Exception, match="Connection failed"):
                await dao.count_instruments()
    
    async def test_sql_injection_protection(self, dao):
        """Test that parameterized queries protect against SQL injection."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        
        mock_connection.fetchrow.return_value = None
        mock_pool.close = AsyncMock()
        
        # Test with malicious input
        malicious_symbol = "'; DROP TABLE test_instrument_polygon; --"
        
        with patch('dao.instrument_polygon_dao.asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            with patch('builtins.print'):  # Suppress debug prints
                result = await dao.get_instrument_by_symbol(malicious_symbol)
        
        # Verify the malicious input was passed as parameter, not concatenated into SQL
        mock_connection.fetchrow.assert_called_once_with(
            "SELECT * FROM test_instrument_polygon WHERE symbol = $1", 
            malicious_symbol
        )
        assert result is None
    
    def test_dao_initialization(self, mock_environment):
        """Test DAO initialization sets correct attributes."""
        dao = InstrumentPolygonDAO(mock_environment)
        
        assert dao.env == mock_environment
        assert dao.table_name == "test_instrument_polygon"
        assert dao.db_url == "postgresql://test:test@localhost/test_db"
        
        # Verify environment methods were called correctly
        mock_environment.get_table_name.assert_called_once_with('instrument_polygon')
        mock_environment.get_database_url.assert_called_once()