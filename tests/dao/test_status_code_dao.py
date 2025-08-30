import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from dao.status_code_dao import StatusCodeDAO
from config.environment import Environment


class TestStatusCodeDAO:
    """Test cases for StatusCodeDAO class."""
    
    @pytest.fixture
    def mock_environment(self):
        """Create a mock Environment for testing."""
        env = Mock(spec=Environment)
        env.get_table_name.return_value = 'dev_status_code'
        env.get_database_url.return_value = 'postgresql://user:pass@localhost:5432/test_db'
        return env
    
    @pytest.fixture
    def dao(self, mock_environment):
        """Create StatusCodeDAO instance with mocked environment."""
        return StatusCodeDAO(mock_environment)
    
    def test_init(self, mock_environment):
        """Test DAO initialization."""
        dao = StatusCodeDAO(mock_environment)
        
        assert dao.env == mock_environment
        assert dao.table_name == 'dev_status_code'
        assert dao.db_url == 'postgresql://user:pass@localhost:5432/test_db'
        
        # Verify environment methods were called
        mock_environment.get_table_name.assert_called_once_with('status_code')
        mock_environment.get_database_url.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_insert_status(self, dao):
        """Test inserting a status code."""
        # Create async context manager mock
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        # Mock the async context manager behavior
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        async_context_manager.__aexit__.return_value = None
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()
        
        with patch('dao.status_code_dao.asyncpg') as mock_asyncpg:
            # Make create_pool an async function that returns the mock pool
            async def create_pool_mock(*args, **kwargs):
                return mock_pool
            mock_asyncpg.create_pool = create_pool_mock
            
            await dao.insert_status('OK', 'Operation successful')
            
            # Verify connection usage
            mock_pool.acquire.assert_called_once()
            # Check that execute was called with the right parameters (ignore whitespace differences)
            mock_connection.execute.assert_called_once()
            call_args = mock_connection.execute.call_args
            assert 'INSERT INTO dev_status_code' in call_args[0][0]
            assert 'VALUES ($1, $2)' in call_args[0][0]
            assert call_args[0][1] == 'OK'
            assert call_args[0][2] == 'Operation successful'
            
            # Verify pool cleanup
            mock_pool.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_status(self, dao):
        """Test getting a status code."""
        # Create async context manager mock
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        # Mock the async context manager behavior
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        async_context_manager.__aexit__.return_value = None
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()
        mock_connection.fetchrow.return_value = {'code': 'OK', 'description': 'Operation successful'}
        
        with patch('dao.status_code_dao.asyncpg') as mock_asyncpg:
            # Make create_pool an async function that returns the mock pool
            async def create_pool_mock(*args, **kwargs):
                return mock_pool
            mock_asyncpg.create_pool = create_pool_mock
            
            result = await dao.get_status('OK')
            
            # Verify connection usage
            mock_pool.acquire.assert_called_once()
            mock_connection.fetchrow.assert_called_once_with(
                "SELECT * FROM dev_status_code WHERE code = $1", 'OK'
            )
            
            # Verify result
            assert result == {'code': 'OK', 'description': 'Operation successful'}
            
            # Verify pool cleanup
            mock_pool.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_statuses(self, dao):
        """Test listing all status codes."""
        # Create async context manager mock
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        expected_statuses = [
            {'code': 'OK', 'description': 'Operation successful'},
            {'code': 'ERROR', 'description': 'Operation failed'}
        ]
        
        # Mock the async context manager behavior
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        async_context_manager.__aexit__.return_value = None
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()
        mock_connection.fetch.return_value = expected_statuses
        
        with patch('dao.status_code_dao.asyncpg') as mock_asyncpg:
            # Make create_pool an async function that returns the mock pool
            async def create_pool_mock(*args, **kwargs):
                return mock_pool
            mock_asyncpg.create_pool = create_pool_mock
            
            result = await dao.list_statuses()
            
            # Verify connection usage
            mock_pool.acquire.assert_called_once()
            mock_connection.fetch.assert_called_once_with("SELECT * FROM dev_status_code")
            
            # Verify result
            assert result == expected_statuses
            
            # Verify pool cleanup
            mock_pool.close.assert_called_once()
    
    def test_environment_integration(self, mock_environment):
        """Test that DAO correctly integrates with Environment configuration."""
        # Test with different table prefix
        mock_environment.get_table_name.return_value = 'intg_status_code'
        mock_environment.get_database_url.return_value = 'postgresql://user:pass@localhost:5433/intg_db'
        
        dao = StatusCodeDAO(mock_environment)
        
        assert dao.table_name == 'intg_status_code'
        assert dao.db_url == 'postgresql://user:pass@localhost:5433/intg_db'
        
        # Verify environment configuration was used
        mock_environment.get_table_name.assert_called_with('status_code')
        mock_environment.get_database_url.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_sql_injection_protection(self, dao):
        """Test that DAO uses parameterized queries to prevent SQL injection."""
        # Create async context manager mock
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        
        # Mock the async context manager behavior
        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        async_context_manager.__aexit__.return_value = None
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()
        
        with patch('dao.status_code_dao.asyncpg') as mock_asyncpg:
            # Make create_pool an async function that returns the mock pool
            async def create_pool_mock(*args, **kwargs):
                return mock_pool
            mock_asyncpg.create_pool = create_pool_mock
            
            # Attempt to insert malicious code
            malicious_code = "'; DROP TABLE users; --"
            malicious_description = "<script>alert('xss')</script>"
            
            await dao.insert_status(malicious_code, malicious_description)
            
            # Verify that the values are passed as parameters, not embedded in SQL
            mock_connection.execute.assert_called_once()
            call_args = mock_connection.execute.call_args
            
            # First argument should be the SQL with placeholders
            sql = call_args[0][0]
            assert '$1' in sql
            assert '$2' in sql
            assert malicious_code not in sql  # Malicious code should not be in SQL string
            
            # Parameters should be passed separately
            assert call_args[0][1] == malicious_code
            assert call_args[0][2] == malicious_description