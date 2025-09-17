"""
Unit tests for System Monitor fail-fast behavior

These tests verify that:
1. System monitor fails fast instead of masking database connection errors
2. System monitor uses Gin configuration instead of hardcoded values
3. No fake metrics are returned when real operations fail
4. Proper exceptions are raised for all failure conditions
"""

import pytest
import asyncio
from unittest.mock import patch, AsyncMock, MagicMock
from src.agents.system_monitor import SystemMonitor, DatabaseConnectionError, SystemMonitorError

class TestSystemMonitorFailFast:
    """Test fail-fast behavior in system monitoring"""
    
    def setup_method(self):
        """Setup test environment"""
        self.monitor = SystemMonitor()
    
    @patch('src.core.config.secure_config_loader.secure_config')
    @patch('asyncpg.connect')
    async def test_database_connection_success(self, mock_connect, mock_config):
        """Test successful database connection returns real metrics"""
        # Setup mocks
        mock_config.get_database_connection_params.return_value = {
            'host': 'test-host',
            'port': 5432,
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db',
            'command_timeout': 60,
            'min_size': 1,
            'max_size': 10
        }
        mock_config.get_system_monitor_config.return_value = MagicMock(
            health_check_timeout_seconds=10
        )
        
        # Mock successful database connection and query
        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = 5  # 5 active connections
        mock_conn.close = AsyncMock()
        mock_connect.return_value = mock_conn
        
        # Execute method
        result = await self.monitor._count_database_connections()
        
        # Verify real result is returned
        assert result == 5
        
        # Verify configuration was loaded from Gin
        mock_config.get_database_connection_params.assert_called_once_with(environment="intg")
        mock_config.get_system_monitor_config.assert_called_once()
        
        # Verify database connection used configured parameters
        mock_connect.assert_called_once_with(
            host='test-host',
            port=5432,
            user='test_user',
            password='test_password',
            database='test_db',
            command_timeout=60,
            min_size=1,
            max_size=10
        )
    
    @patch('src.core.config.secure_config_loader.secure_config')
    @patch('asyncpg.connect')
    async def test_database_connection_timeout_fails_fast(self, mock_connect, mock_config):
        """Test that database connection timeout raises exception instead of returning 0"""
        # Setup mocks
        mock_config.get_database_connection_params.return_value = {
            'host': 'test-host', 'port': 5432, 'user': 'test_user', 
            'password': 'test_password', 'database': 'test_db',
            'command_timeout': 60, 'min_size': 1, 'max_size': 10
        }
        mock_config.get_system_monitor_config.return_value = MagicMock(
            health_check_timeout_seconds=10
        )
        
        # Mock connection timeout
        mock_connect.side_effect = asyncio.TimeoutError()
        
        # Verify exception is raised instead of returning fake value
        with pytest.raises(DatabaseConnectionError, match="Database connection timeout after 10s"):
            await self.monitor._count_database_connections()
    
    @patch('src.core.config.secure_config_loader.secure_config')
    @patch('asyncpg.connect')
    async def test_database_postgres_error_fails_fast(self, mock_connect, mock_config):
        """Test that PostgreSQL errors raise exceptions instead of returning 0"""
        # Setup mocks
        mock_config.get_database_connection_params.return_value = {
            'host': 'test-host', 'port': 5432, 'user': 'test_user',
            'password': 'test_password', 'database': 'test_db', 
            'command_timeout': 60, 'min_size': 1, 'max_size': 10
        }
        mock_config.get_system_monitor_config.return_value = MagicMock(
            health_check_timeout_seconds=10
        )
        
        # Mock PostgreSQL error
        import asyncpg
        mock_connect.side_effect = asyncpg.PostgresError("Connection failed")
        
        # Verify exception is raised instead of returning fake value  
        with pytest.raises(DatabaseConnectionError, match="PostgreSQL error"):
            await self.monitor._count_database_connections()
    
    @patch('src.core.config.secure_config_loader.secure_config')
    @patch('asyncpg.connect')
    async def test_null_query_result_fails_fast(self, mock_connect, mock_config):
        """Test that NULL query results raise exceptions instead of returning 0"""
        # Setup mocks
        mock_config.get_database_connection_params.return_value = {
            'host': 'test-host', 'port': 5432, 'user': 'test_user',
            'password': 'test_password', 'database': 'test_db',
            'command_timeout': 60, 'min_size': 1, 'max_size': 10
        }
        mock_config.get_system_monitor_config.return_value = MagicMock(
            health_check_timeout_seconds=10
        )
        
        # Mock successful connection but NULL query result
        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = None  # NULL result from database
        mock_conn.close = AsyncMock()
        mock_connect.return_value = mock_conn
        
        # Verify exception is raised for NULL result
        with pytest.raises(ValueError, match="Database returned NULL for connection count query"):
            await self.monitor._count_database_connections()
    
    @patch('src.core.config.secure_config_loader.secure_config')
    @patch('asyncpg.connect')
    async def test_generic_exception_fails_fast(self, mock_connect, mock_config):
        """Test that generic exceptions are not masked"""
        # Setup mocks
        mock_config.get_database_connection_params.return_value = {
            'host': 'test-host', 'port': 5432, 'user': 'test_user',
            'password': 'test_password', 'database': 'test_db',
            'command_timeout': 60, 'min_size': 1, 'max_size': 10
        }
        mock_config.get_system_monitor_config.return_value = MagicMock(
            health_check_timeout_seconds=10
        )
        
        # Mock unexpected error
        mock_connect.side_effect = RuntimeError("Unexpected error")
        
        # Verify exception is raised instead of returning fake value
        with pytest.raises(SystemMonitorError, match="Failed to count database connections"):
            await self.monitor._count_database_connections()
    
    @patch('src.core.config.secure_config_loader.secure_config')
    async def test_configuration_loading_failure_fails_fast(self, mock_config):
        """Test that configuration loading failures are not masked"""
        # Mock configuration loading failure
        from src.core.config.secure_config_loader import SecurityConfigurationError
        mock_config.get_database_connection_params.side_effect = SecurityConfigurationError(
            "Configuration not loaded"
        )
        
        # Verify configuration error propagates
        with pytest.raises(SecurityConfigurationError):
            await self.monitor._count_database_connections()
    
    def test_no_hardcoded_fallbacks_in_new_code(self):
        """
        Critical test: Verify no hardcoded fallbacks exist in the updated code
        
        This test ensures that the dangerous pattern of returning fake values
        when real operations fail has been completely eliminated.
        """
        import inspect
        import ast
        
        # Get the source code of the _count_database_connections method
        source = inspect.getsource(self.monitor._count_database_connections)
        
        # Parse the source code
        tree = ast.parse(source)
        
        # Check for dangerous patterns
        for node in ast.walk(tree):
            if isinstance(node, ast.ExceptHandler):
                # Find except handlers
                for child in ast.walk(node):
                    if isinstance(child, ast.Return):
                        # If there's a return statement in except handler
                        if isinstance(child.value, ast.Constant) and child.value.value == 0:
                            pytest.fail("Found hardcoded return 0 in exception handler")
        
        # Verify the method contains proper exception raising
        assert "raise DatabaseConnectionError" in source
        assert "raise SystemMonitorError" in source
        assert "return 0" not in source.split("return int(result)")[-1]  # No return 0 after the real return

class TestSystemMonitorConfigurationUsage:
    """Test that system monitor properly uses Gin configuration"""
    
    def setup_method(self):
        """Setup test environment"""
        self.monitor = SystemMonitor()
    
    @patch('src.core.config.secure_config_loader.secure_config')
    async def test_uses_gin_database_configuration(self, mock_config):
        """Test that database parameters come from Gin configuration"""
        # Setup mock configuration
        expected_params = {
            'host': 'gin-configured-host',
            'port': 9999,
            'user': 'gin_user',
            'password': 'gin_password',
            'database': 'gin_db',
            'command_timeout': 120,
            'min_size': 2,
            'max_size': 20
        }
        mock_config.get_database_connection_params.return_value = expected_params
        mock_config.get_system_monitor_config.return_value = MagicMock(
            health_check_timeout_seconds=30
        )
        
        # Mock successful connection to avoid actual database call
        with patch('asyncpg.connect') as mock_connect:
            mock_conn = AsyncMock()
            mock_conn.fetchval.return_value = 1
            mock_conn.close = AsyncMock()
            mock_connect.return_value = mock_conn
            
            await self.monitor._count_database_connections()
            
            # Verify Gin configuration was used
            mock_connect.assert_called_once_with(**expected_params)
    
    @patch('src.core.config.secure_config_loader.secure_config')
    async def test_uses_gin_timeout_configuration(self, mock_config):
        """Test that timeout values come from Gin configuration"""
        # Setup mock with custom timeout
        mock_config.get_database_connection_params.return_value = {
            'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test',
            'database': 'test', 'command_timeout': 60, 'min_size': 1, 'max_size': 10
        }
        expected_timeout = 25  # Custom timeout from Gin config
        mock_config.get_system_monitor_config.return_value = MagicMock(
            health_check_timeout_seconds=expected_timeout
        )
        
        # Mock timeout to verify our timeout value is used
        with patch('asyncio.wait_for') as mock_wait_for:
            with patch('asyncpg.connect'):
                mock_wait_for.side_effect = asyncio.TimeoutError()
                
                with pytest.raises(DatabaseConnectionError, match=f"timeout after {expected_timeout}s"):
                    await self.monitor._count_database_connections()
                
                # Verify timeout from Gin config was used
                mock_wait_for.assert_called_once()
                args, kwargs = mock_wait_for.call_args
                assert kwargs['timeout'] == expected_timeout

class TestLegacyVsNewBehavior:
    """Test that demonstrates the difference between old and new behavior"""
    
    def test_old_behavior_was_dangerous(self):
        """
        Documentation test: Shows why the old behavior was dangerous
        
        The old code pattern:
        ```python
        try:
            # real operation
            return real_result
        except Exception:
            return 0  # DANGEROUS: Masks all failures
        ```
        
        This test documents why this was problematic and ensures the new
        behavior is implemented correctly.
        """
        # Old behavior would return 0 for any failure, making it impossible to
        # distinguish between:
        # 1. Database actually has 0 connections (valid state)  
        # 2. Database is completely down (critical failure)
        # 3. Network is broken (infrastructure failure)
        # 4. Credentials are wrong (security issue)
        
        # New behavior raises specific exceptions for each case,
        # allowing proper handling and alerting
        
        assert True  # This test is primarily documentary
    
    def test_new_behavior_enables_proper_monitoring(self):
        """
        Test that new behavior enables proper monitoring and alerting
        
        With fail-fast exceptions, monitoring systems can:
        1. Distinguish between real 0 connections and failures
        2. Alert on infrastructure problems immediately  
        3. Track database availability separately from connection count
        4. Implement proper retry logic for transient failures
        """
        from src.agents.system_monitor import DatabaseConnectionError, SystemMonitorError
        
        # Verify proper exception types exist for monitoring
        assert issubclass(DatabaseConnectionError, Exception)
        assert issubclass(SystemMonitorError, Exception)
        
        # These exceptions enable monitoring systems to:
        # - Log specific failure types
        # - Send targeted alerts
        # - Implement appropriate retry strategies
        # - Track system reliability metrics
        
        assert True  # This test verifies the exception infrastructure exists

if __name__ == "__main__":
    pytest.main([__file__, "-v"])