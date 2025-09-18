#!/usr/bin/env python3
"""
Comprehensive unit tests for shared.utils.database_connections module.

Tests the database connection utility that provides standardized database
connections with automatic fallbacks for standalone scripts.
"""

import pytest
import os
from unittest.mock import patch, Mock, AsyncMock
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / 'src'))

from core.shared.utils.database_connections import (
    get_database_pool,
    get_simple_db_config,
    get_table_name,
    test_database_connection,
    DatabaseConnectionManager
)


class TestGetDatabasePool:
    """Test the get_database_pool function"""

    @pytest.mark.asyncio
    async def test_get_database_pool_advanced_system_success(self):
        """Test successful connection using advanced Database class"""
        mock_pool = AsyncMock()
        mock_env = Mock()

        # Mock the advanced system imports and classes
        mock_database_class = Mock()
        mock_database_class.create_connection_pool = AsyncMock(return_value=mock_pool)

        mock_env_class = Mock()
        mock_env_class.return_value = mock_env

        mock_env_type = Mock()

        with patch.dict('sys.modules', {
            'shared.data_handling.utils.database': Mock(Database=mock_database_class),
            'shared.data_handling.utils.environment': Mock(
                Environment=mock_env_class,
                EnvironmentType=mock_env_type
            )
        }):
            result = await get_database_pool('dev', max_retries=3, timeout=10.0)

            assert result == mock_pool
            mock_database_class.create_connection_pool.assert_called_once_with(
                env=mock_env, max_retries=3, timeout=10.0
            )

    @pytest.mark.asyncio
    async def test_get_database_pool_fallback_to_simple(self):
        """Test fallback to simple asyncpg connection when advanced system fails"""
        mock_pool = AsyncMock()

        # Mock asyncpg.create_pool
        with patch('shared.utils.database_connections.asyncpg.create_pool', new_callable=AsyncMock) as mock_create_pool:
            mock_create_pool.return_value = mock_pool

            # Force import error for advanced system
            with patch('builtins.__import__', side_effect=ImportError("Module not found")):
                result = await get_database_pool('dev')

                assert result == mock_pool
                mock_create_pool.assert_called_once()

                # Check that it was called with default dev configuration
                call_kwargs = mock_create_pool.call_args[1]
                assert call_kwargs['host'] == 'localhost'
                assert call_kwargs['port'] == 3432
                assert call_kwargs['database'] == 'dev_db'

    @pytest.mark.asyncio
    async def test_get_database_pool_environment_setting(self):
        """Test that environment variable is set correctly"""
        mock_pool = AsyncMock()

        with patch('shared.utils.database_connections.asyncpg.create_pool', new_callable=AsyncMock) as mock_create_pool:
            mock_create_pool.return_value = mock_pool

            with patch('builtins.__import__', side_effect=ImportError("Module not found")):
                await get_database_pool('test')

                assert os.environ.get("ENVIRONMENT") == 'test'

    @pytest.mark.asyncio
    async def test_get_database_pool_different_environments(self):
        """Test database pool creation for different environments"""
        environments = ['dev', 'test', 'intg', 'prod']

        for env in environments:
            mock_pool = AsyncMock()

            with patch('shared.utils.database_connections.asyncpg.create_pool', new_callable=AsyncMock) as mock_create_pool:
                mock_create_pool.return_value = mock_pool

                with patch('builtins.__import__', side_effect=ImportError("Module not found")):
                    result = await get_database_pool(env)

                    assert result == mock_pool
                    mock_create_pool.assert_called_once()

                    # Verify environment-specific configuration was used
                    call_kwargs = mock_create_pool.call_args[1]
                    if env == 'dev':
                        assert call_kwargs['port'] == 3432
                    elif env == 'test':
                        assert call_kwargs['port'] == 5433

    @pytest.mark.asyncio
    async def test_get_database_pool_connection_failure(self):
        """Test handling of database connection failures"""
        with patch('shared.utils.database_connections.asyncpg.create_pool', new_callable=AsyncMock) as mock_create_pool:
            mock_create_pool.side_effect = Exception("Connection failed")

            with patch('builtins.__import__', side_effect=ImportError("Module not found")):
                with pytest.raises(Exception, match="Connection failed"):
                    await get_database_pool('dev')

    @pytest.mark.asyncio
    async def test_get_database_pool_custom_parameters(self):
        """Test database pool creation with custom parameters"""
        mock_pool = AsyncMock()

        with patch('shared.utils.database_connections.asyncpg.create_pool', new_callable=AsyncMock) as mock_create_pool:
            mock_create_pool.return_value = mock_pool

            with patch('builtins.__import__', side_effect=ImportError("Module not found")):
                result = await get_database_pool('dev', max_retries=5, timeout=30.0)

                assert result == mock_pool
                call_kwargs = mock_create_pool.call_args[1]
                assert call_kwargs['command_timeout'] == 30.0


class TestGetSimpleDbConfig:
    """Test the get_simple_db_config function"""

    def test_dev_environment_config(self):
        """Test configuration for dev environment"""
        config = get_simple_db_config('dev')

        assert config['host'] == 'localhost'
        assert config['port'] == 3432
        assert config['database'] == 'dev_db'
        assert config['user'] == 'postgres'
        assert config['password'] == 'dev_password'

    def test_test_environment_config(self):
        """Test configuration for test environment"""
        config = get_simple_db_config('test')

        assert config['host'] == 'localhost'
        assert config['port'] == 5433
        assert config['database'] == 'test_db'
        assert config['user'] == 'postgres'
        assert config['password'] == 'test_password'

    def test_intg_environment_config(self):
        """Test configuration for intg environment"""
        config = get_simple_db_config('intg')

        assert config['host'] == 'ats-intg-postgres'
        assert config['port'] == 5432
        assert config['database'] == 'intg_db'
        assert config['user'] == 'postgres'
        assert config['password'] == 'intg_password'

    def test_prod_environment_config(self):
        """Test configuration for prod environment"""
        config = get_simple_db_config('prod')

        assert config['host'] == 'ats-prod-postgres'
        assert config['port'] == 5432
        assert config['database'] == 'prod_db'
        assert config['user'] == 'postgres'
        assert config['password'] == 'prod_password'

    def test_unknown_environment_defaults_to_dev(self):
        """Test that unknown environment defaults to dev configuration"""
        config = get_simple_db_config('unknown')
        dev_config = get_simple_db_config('dev')

        assert config == dev_config

    @patch.dict(os.environ, {
        'DB_HOST': 'custom_host',
        'DB_PORT': '9999',
        'DB_NAME': 'custom_db',
        'DB_USER': 'custom_user',
        'DB_PASSWORD': 'custom_password'
    })
    def test_environment_variable_override(self):
        """Test that environment variables override default configuration"""
        config = get_simple_db_config('dev')

        assert config['host'] == 'custom_host'
        assert config['port'] == 9999
        assert config['database'] == 'custom_db'
        assert config['user'] == 'custom_user'
        assert config['password'] == 'custom_password'

    @patch.dict(os.environ, {'DB_PORT': 'invalid_port'})
    def test_invalid_port_environment_variable(self):
        """Test handling of invalid port in environment variable"""
        with pytest.raises(ValueError):
            get_simple_db_config('dev')


class TestGetTableName:
    """Test the get_table_name function"""

    def test_get_table_name_simple_fallback(self):
        """Test getting table name using simple fallback"""
        # Force import error to use simple naming convention
        with patch('builtins.__import__', side_effect=ImportError("Module not found")):
            result = get_table_name('news', 'dev')
            assert result == 'dev_news'

            result = get_table_name('prices', 'prod')
            assert result == 'prod_prices'

            result = get_table_name('instruments', 'test')
            assert result == 'test_instruments'

    def test_get_table_name_advanced_system(self):
        """Test getting table name using advanced Environment system"""
        mock_env = Mock()
        mock_env.get_table_name.return_value = 'advanced_table_name'

        mock_env_class = Mock()
        mock_env_class.return_value = mock_env

        mock_env_type = Mock()

        with patch.dict('sys.modules', {
            'shared.data_handling.utils.environment': Mock(
                Environment=mock_env_class,
                EnvironmentType=mock_env_type
            )
        }):
            result = get_table_name('news', 'dev')
            assert result == 'advanced_table_name'
            mock_env.get_table_name.assert_called_once_with('news')

    def test_get_table_name_different_bases(self):
        """Test getting table names for different base names"""
        base_names = ['news', 'prices', 'instruments', 'fundamentals', 'events']

        with patch('builtins.__import__', side_effect=ImportError("Module not found")):
            for base_name in base_names:
                result = get_table_name(base_name, 'dev')
                assert result == f'dev_{base_name}'

    def test_get_table_name_different_environments(self):
        """Test getting table names for different environments"""
        environments = ['dev', 'test', 'intg', 'prod']

        with patch('builtins.__import__', side_effect=ImportError("Module not found")):
            for env in environments:
                result = get_table_name('news', env)
                assert result == f'{env}_news'

    def test_get_table_name_advanced_system_exception(self):
        """Test fallback when advanced system throws exception"""
        mock_env = Mock()
        mock_env.get_table_name.side_effect = Exception("Environment error")

        mock_env_class = Mock()
        mock_env_class.return_value = mock_env

        mock_env_type = Mock()

        with patch.dict('sys.modules', {
            'shared.data_handling.utils.environment': Mock(
                Environment=mock_env_class,
                EnvironmentType=mock_env_type
            )
        }):
            result = get_table_name('news', 'dev')
            assert result == 'dev_news'  # Falls back to simple naming


class TestTestDatabaseConnection:
    """Test the test_database_connection function"""

    @pytest.mark.asyncio
    async def test_database_connection_success(self):
        """Test successful database connection test"""
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = ['PostgreSQL 13.21']
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.close = AsyncMock()

        with patch('shared.utils.database_connections.get_database_pool') as mock_get_pool:
            mock_get_pool.return_value = mock_pool

            result = await test_database_connection('dev')

            assert result is True
            mock_get_pool.assert_called_once_with('dev')
            mock_conn.fetchrow.assert_called_once_with('SELECT version()')
            mock_pool.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_database_connection_failure(self):
        """Test database connection test failure"""
        with patch('shared.utils.database_connections.get_database_pool') as mock_get_pool:
            mock_get_pool.side_effect = Exception("Connection failed")

            with patch('shared.utils.database_connections.logger') as mock_logger:
                result = await test_database_connection('dev')

                assert result is False
                mock_logger.error.assert_called_once()
                assert 'Connection failed' in mock_logger.error.call_args[0][0]

    @pytest.mark.asyncio
    async def test_database_connection_query_failure(self):
        """Test database connection test with query failure"""
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.fetchrow.side_effect = Exception("Query failed")
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.close = AsyncMock()

        with patch('shared.utils.database_connections.get_database_pool') as mock_get_pool:
            mock_get_pool.return_value = mock_pool

            with patch('shared.utils.database_connections.logger') as mock_logger:
                result = await test_database_connection('dev')

                assert result is False
                mock_logger.error.assert_called_once()


class TestDatabaseConnectionManager:
    """Test the DatabaseConnectionManager context manager"""

    @pytest.mark.asyncio
    async def test_connection_manager_success(self):
        """Test successful database connection manager usage"""
        mock_pool = AsyncMock()

        with patch('shared.utils.database_connections.get_database_pool') as mock_get_pool:
            mock_get_pool.return_value = mock_pool
            mock_pool.close = AsyncMock()

            async with DatabaseConnectionManager('dev') as pool:
                assert pool == mock_pool

            mock_get_pool.assert_called_once_with(
                environment='dev', max_retries=3, timeout=10.0
            )
            mock_pool.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_manager_custom_parameters(self):
        """Test database connection manager with custom parameters"""
        mock_pool = AsyncMock()

        with patch('shared.utils.database_connections.get_database_pool') as mock_get_pool:
            mock_get_pool.return_value = mock_pool
            mock_pool.close = AsyncMock()

            async with DatabaseConnectionManager('prod', max_retries=5, timeout=30.0) as pool:
                assert pool == mock_pool

            mock_get_pool.assert_called_once_with(
                environment='prod', max_retries=5, timeout=30.0
            )
            mock_pool.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_manager_exception_handling(self):
        """Test database connection manager exception handling"""
        mock_pool = AsyncMock()
        mock_pool.close = AsyncMock()

        with patch('shared.utils.database_connections.get_database_pool') as mock_get_pool:
            mock_get_pool.return_value = mock_pool

            try:
                async with DatabaseConnectionManager('dev') as pool:
                    raise Exception("Test exception")
            except Exception as e:
                assert str(e) == "Test exception"

            # Pool should still be closed even when exception occurs
            mock_pool.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_manager_pool_creation_failure(self):
        """Test database connection manager when pool creation fails"""
        with patch('shared.utils.database_connections.get_database_pool') as mock_get_pool:
            mock_get_pool.side_effect = Exception("Pool creation failed")

            with pytest.raises(Exception, match="Pool creation failed"):
                async with DatabaseConnectionManager('dev') as pool:
                    pass  # Should not reach this point

    @pytest.mark.asyncio
    async def test_connection_manager_pool_none_handling(self):
        """Test connection manager when pool is None"""
        manager = DatabaseConnectionManager('dev')
        manager.pool = None  # Simulate pool being None

        # Should not raise exception when closing None pool
        await manager.__aexit__(None, None, None)


class TestEnvironmentVariableHandling:
    """Test environment variable handling across functions"""

    @patch.dict(os.environ, {}, clear=True)
    def test_environment_variables_cleared(self):
        """Test behavior when environment variables are cleared"""
        config = get_simple_db_config('dev')

        # Should use default values when env vars are not set
        assert config['host'] == 'localhost'
        assert config['port'] == 3432
        assert config['database'] == 'dev_db'

    @patch.dict(os.environ, {
        'DB_HOST': '',
        'DB_PORT': '',
        'DB_NAME': '',
        'DB_USER': '',
        'DB_PASSWORD': ''
    })
    def test_empty_environment_variables(self):
        """Test behavior with empty environment variables"""
        config = get_simple_db_config('dev')

        # Empty strings should be used as-is
        assert config['host'] == ''
        assert config['database'] == ''
        assert config['user'] == ''
        assert config['password'] == ''

    @patch.dict(os.environ, {'DB_PORT': '0'})
    def test_zero_port_environment_variable(self):
        """Test handling of zero port in environment variable"""
        config = get_simple_db_config('dev')
        assert config['port'] == 0


class TestLogging:
    """Test logging behavior"""

    @pytest.mark.asyncio
    async def test_successful_connection_logging(self):
        """Test logging on successful database connections"""
        mock_pool = AsyncMock()

        with patch('shared.utils.database_connections.asyncpg.create_pool', new_callable=AsyncMock) as mock_create_pool:
            mock_create_pool.return_value = mock_pool

            with patch('builtins.__import__', side_effect=ImportError("Module not found")):
                with patch('shared.utils.database_connections.logger') as mock_logger:
                    await get_database_pool('dev')

                    # Should log successful connection
                    mock_logger.info.assert_called()
                    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
                    assert any('simple connection' in call for call in log_calls)
                    assert any('Connected to database' in call for call in log_calls)

    @pytest.mark.asyncio
    async def test_connection_test_logging(self):
        """Test logging in connection test scenarios"""
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = ['PostgreSQL 13.21 (Test version info)']
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.close = AsyncMock()

        with patch('shared.utils.database_connections.get_database_pool') as mock_get_pool:
            mock_get_pool.return_value = mock_pool

            with patch('shared.utils.database_connections.logger') as mock_logger:
                result = await test_database_connection('dev')

                assert result is True
                mock_logger.info.assert_called()
                # Should log truncated version info
                log_message = mock_logger.info.call_args[0][0]
                assert 'Database connection test successful' in log_message
                assert '...' in log_message  # Should be truncated


class TestEdgeCases:
    """Test edge cases and error conditions"""

    def test_get_table_name_empty_base_name(self):
        """Test getting table name with empty base name"""
        with patch('builtins.__import__', side_effect=ImportError("Module not found")):
            result = get_table_name('', 'dev')
            assert result == 'dev_'

    def test_get_table_name_none_base_name(self):
        """Test getting table name with None base name"""
        with patch('builtins.__import__', side_effect=ImportError("Module not found")):
            with pytest.raises(TypeError):
                get_table_name(None, 'dev')

    def test_get_simple_db_config_none_environment(self):
        """Test getting config with None environment"""
        config = get_simple_db_config(None)
        # Should default to dev config
        dev_config = get_simple_db_config('dev')
        assert config == dev_config

    @pytest.mark.asyncio
    async def test_get_database_pool_none_environment(self):
        """Test getting database pool with None environment"""
        mock_pool = AsyncMock()

        with patch('shared.utils.database_connections.asyncpg.create_pool', new_callable=AsyncMock) as mock_create_pool:
            mock_create_pool.return_value = mock_pool

            with patch('builtins.__import__', side_effect=ImportError("Module not found")):
                result = await get_database_pool(None)
                assert result == mock_pool


if __name__ == '__main__':
    pytest.main([__file__])