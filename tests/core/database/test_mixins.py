"""
Tests for database connection mixins.

This module tests the database mixin patterns that standardize
database access across services.
"""

import pytest
from unittest.mock import Mock, patch
import asyncio

from core.platform.database.mixins import (
    DatabaseMixin,
    AsyncDatabaseMixin,
    DatabaseInitializationMixin,
    create_database_service
)
from core.security.exceptions.custom_exceptions import DatabaseError


class TestDatabaseMixin:
    """Test cases for DatabaseMixin."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mixin = DatabaseMixin()

    @patch('src.core.database.mixins.get_connection_manager')
    def test_db_manager_lazy_initialization(self, mock_get_manager):
        """Test that db_manager is initialized lazily."""
        mock_manager = Mock()
        mock_get_manager.return_value = mock_manager

        # First access should initialize
        assert self.mixin.db_manager == mock_manager
        mock_get_manager.assert_called_once()

        # Second access should use cached value
        mock_get_manager.reset_mock()
        assert self.mixin.db_manager == mock_manager
        mock_get_manager.assert_not_called()

    @patch('src.core.database.mixins.get_settings')
    def test_settings_lazy_initialization(self, mock_get_settings):
        """Test that settings are initialized lazily."""
        mock_settings = Mock()
        mock_get_settings.return_value = mock_settings

        assert self.mixin.settings == mock_settings
        mock_get_settings.assert_called_once()

    @patch('src.core.database.mixins.get_connection_manager')
    def test_initialize_database_success(self, mock_get_manager):
        """Test successful database initialization."""
        mock_manager = Mock()
        mock_manager.check_connection.return_value = True
        mock_get_manager.return_value = mock_manager

        result = self.mixin.initialize_database()

        assert result is True
        mock_manager.check_connection.assert_called_once()

    @patch('src.core.database.mixins.get_connection_manager')
    def test_initialize_database_failure(self, mock_get_manager):
        """Test database initialization failure."""
        mock_manager = Mock()
        mock_manager.check_connection.return_value = False
        mock_get_manager.return_value = mock_manager

        result = self.mixin.initialize_database()

        assert result is False

    @patch('src.core.database.mixins.get_connection_manager')
    def test_initialize_database_exception(self, mock_get_manager):
        """Test database initialization with exception."""
        mock_manager = Mock()
        mock_manager.check_connection.side_effect = Exception("Connection failed")
        mock_get_manager.return_value = mock_manager

        with pytest.raises(DatabaseError, match="Database initialization failed"):
            self.mixin.initialize_database()

    @patch('src.core.database.mixins.get_raw_connection')
    def test_execute_query_success(self, mock_get_connection):
        """Test successful query execution."""
        # Mock connection context manager
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=None)

        # Mock cursor results
        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.fetchall.return_value = [(1, 'test'), (2, 'example')]

        result = self.mixin.execute_query("SELECT id, name FROM test")

        expected = [
            {'id': 1, 'name': 'test'},
            {'id': 2, 'name': 'example'}
        ]
        assert result == expected
        mock_cursor.execute.assert_called_once_with("SELECT id, name FROM test", None)

    @patch('src.core.database.mixins.get_raw_connection')
    def test_execute_query_with_params(self, mock_get_connection):
        """Test query execution with parameters."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=None)

        mock_cursor.description = [('count',)]
        mock_cursor.fetchall.return_value = [(5,)]

        result = self.mixin.execute_query("SELECT COUNT(*) FROM test WHERE id = %s", (1,))

        assert result == [{'count': 5}]
        mock_cursor.execute.assert_called_once_with("SELECT COUNT(*) FROM test WHERE id = %s", (1,))

    @patch('src.core.database.mixins.get_raw_connection')
    def test_execute_update_success(self, mock_get_connection):
        """Test successful update query execution."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 3
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=None)

        result = self.mixin.execute_update("UPDATE test SET status = %s", ('active',))

        assert result == 3
        mock_cursor.execute.assert_called_once_with("UPDATE test SET status = %s", ('active',))

    @patch('src.core.database.mixins.get_settings')
    def test_get_table_name(self, mock_get_settings):
        """Test table name prefixing."""
        mock_settings = Mock()
        mock_settings.get_table_name.return_value = 'dev_instrument'
        mock_get_settings.return_value = mock_settings

        result = self.mixin.get_table_name('instruments')

        assert result == 'dev_instrument'
        mock_settings.get_table_name.assert_called_once_with('instruments')


class TestAsyncDatabaseMixin:
    """Test cases for AsyncDatabaseMixin."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mixin = AsyncDatabaseMixin()

    @patch('src.core.database.mixins.get_connection_manager')
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_initialize_database_success(self, mock_get_manager):
        """Test successful async database initialization."""
        mock_manager = Mock()
        mock_manager.check_async_connection.return_value = asyncio.create_task(
            asyncio.coroutine(lambda: True)()
        )
        mock_get_manager.return_value = mock_manager

        result = await self.mixin.initialize_database()

        assert result is True
        mock_manager.check_async_connection.assert_called_once()

    @patch('src.core.database.mixins.get_connection_manager')
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_initialize_database_failure(self, mock_get_manager):
        """Test async database initialization failure."""
        mock_manager = Mock()
        mock_manager.check_async_connection.return_value = asyncio.create_task(
            asyncio.coroutine(lambda: False)()
        )
        mock_get_manager.return_value = mock_manager

        result = await self.mixin.initialize_database()

        assert result is False


class TestDatabaseInitializationMixin:
    """Test cases for DatabaseInitializationMixin."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mixin = DatabaseInitializationMixin()

    @patch('src.core.database.mixins.get_connection_manager')
    def test_check_database_connection_success(self, mock_get_manager):
        """Test successful database connection check."""
        mock_manager = Mock()
        mock_manager.check_connection.return_value = True
        mock_get_manager.return_value = mock_manager

        result = self.mixin.check_database_connection()

        assert result is True
        mock_manager.check_connection.assert_called_once()

    @patch('src.core.database.mixins.get_connection_manager')
    def test_check_database_connection_exception(self, mock_get_manager):
        """Test database connection check with exception."""
        mock_manager = Mock()
        mock_manager.check_connection.side_effect = Exception("Connection error")
        mock_get_manager.return_value = mock_manager

        result = self.mixin.check_database_connection()

        assert result is False

    @patch('src.core.database.mixins.get_connection_manager')
    def test_get_database_stats_success(self, mock_get_manager):
        """Test getting database statistics."""
        mock_manager = Mock()
        expected_stats = {'pool_size': 5, 'active_connections': 2}
        mock_manager.get_connection_stats.return_value = expected_stats
        mock_get_manager.return_value = mock_manager

        result = self.mixin.get_database_stats()

        assert result == expected_stats

    @patch('src.core.database.mixins.get_connection_manager')
    def test_get_database_stats_exception(self, mock_get_manager):
        """Test getting database statistics with exception."""
        mock_manager = Mock()
        mock_manager.get_connection_stats.side_effect = Exception("Stats error")
        mock_get_manager.return_value = mock_manager

        result = self.mixin.get_database_stats()

        assert result == {'error': 'Stats error'}


class TestCreateDatabaseService:
    """Test cases for create_database_service decorator."""

    def test_decorator_functionality(self):
        """Test that decorator adds database capabilities."""

        @create_database_service
        class TestService:
            def __init__(self):
                self.initialized = True

        # Verify the class has database mixin capabilities
        assert issubclass(TestService, DatabaseMixin)

        # Test instantiation (mocked to avoid actual DB connection)
        with patch.object(TestService, 'initialize_database', return_value=True):
            service = TestService()
            assert service.initialized is True
            service.initialize_database.assert_called_once()


class TestDatabaseMixinIntegration:
    """Integration tests for database mixins."""

    def test_mixin_inheritance_pattern(self):
        """Test that mixins work correctly with inheritance."""

        class TestService(DatabaseMixin):
            def __init__(self):
                super().__init__()
                self.service_name = "test"

        service = TestService()

        # Verify mixin properties are available
        assert hasattr(service, 'db_manager')
        assert hasattr(service, 'settings')
        assert hasattr(service, 'initialize_database')
        assert hasattr(service, 'execute_query')
        assert service.service_name == "test"

    def test_multiple_mixin_inheritance(self):
        """Test multiple mixin inheritance patterns."""

        class TestService(DatabaseMixin, DatabaseInitializationMixin):
            def __init__(self):
                super().__init__()

        service = TestService()

        # Verify both mixins' methods are available
        assert hasattr(service, 'initialize_database')  # From DatabaseMixin
        assert hasattr(service, 'check_database_connection')  # From DatabaseInitializationMixin
        assert hasattr(service, 'execute_query')  # From DatabaseMixin
        assert hasattr(service, 'get_database_stats')  # From DatabaseInitializationMixin