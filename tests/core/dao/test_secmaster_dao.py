import pytest
import asyncpg
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, date
from typing import List, Dict
from domains.instruments.repositories.secmaster_dao import SecMasterDAO
from shared.utils.environment import Environment

class TestSecMasterDAO:
    """Comprehensive test coverage for SecMasterDAO."""

    @pytest.fixture
    def mock_environment(self):
        """Mock environment with test database configuration."""
        env = MagicMock(spec=Environment)
        env.get_table_name.side_effect = lambda table: f"test_{table}"
        env.get_database_url.return_value = "postgresql://test:test@localhost/test_db"
        return env

    @pytest.fixture
    def dao(self, mock_environment):
        """Create SecMasterDAO instance with mocked environment."""
        return SecMasterDAO(mock_environment)

    @pytest.mark.asyncio

    async def test_get_spy_membership_events_success(self, dao):
        """Test successful retrieval of SPY membership events."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock successful query result
        test_rows = [
            {
                'instrument_id': 1,
                'start_date': date(2023, 1, 1),
                'end_date': date(2023, 12, 31)
            },
            {
                'instrument_id': 2,
                'start_date': date(2023, 6, 1),
                'end_date': None
            }
        ]
        mock_connection.fetch.return_value = test_rows

        with patch(\'domains.market_data.repositories.secmaster_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_spy_membership_events()

        assert len(result) == 2
        assert result[0]['instrument_id'] == 1
        assert result[0]['start_date'] == date(2023, 1, 1)
        assert result[1]['instrument_id'] == 2
        assert result[1]['end_date'] is None

        # Verify SQL query structure
        mock_connection.fetch.assert_called_once()
        call_args = mock_connection.fetch.call_args[0][0]
        assert "test_universe_membership" in call_args
        assert "test_universe" in call_args
        assert "WHERE u.name = 'S&P 500'" in call_args
        assert "ORDER BY m.start_at" in call_args

    @pytest.mark.asyncio

    async def test_get_spy_membership_events_empty(self, dao):
        """Test get_spy_membership_events when no events exist."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        mock_connection.fetch.return_value = []

        with patch(\'domains.market_data.repositories.secmaster_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_spy_membership_events()

        assert result == []
        mock_pool.close.assert_called_once()

    @pytest.mark.asyncio

    async def test_batch_last_close_prices_success(self, dao):
        """Test successful batch retrieval of last close prices."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock successful query result
        test_rows = [
            {'instrument_id': 1, 'close': 150.25},
            {'instrument_id': 2, 'close': 2500.50},
            {'instrument_id': 3, 'close': 85.75}
        ]
        mock_connection.fetch.return_value = test_rows

        test_date = date(2023, 12, 15)
        test_instrument_ids = [1, 2, 3]

        with patch(\'domains.market_data.repositories.secmaster_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.batch_last_close_prices(test_date, test_instrument_ids)

        expected_result = {1: 150.25, 2: 2500.50, 3: 85.75}
        assert result == expected_result

        # Verify query parameters
        mock_connection.fetch.assert_called_once()
        call_args = mock_connection.fetch.call_args
        assert call_args[0][1] == test_date  # as_of_date parameter
        assert call_args[0][2] == test_instrument_ids  # instrument_ids parameter

    @pytest.mark.asyncio

    async def test_batch_last_close_prices_partial_data(self, dao):
        """Test batch_last_close_prices when only some instruments have data."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock partial data (only 2 of 3 instruments have prices)
        test_rows = [
            {'instrument_id': 1, 'close': 150.25},
            {'instrument_id': 3, 'close': 85.75}
        ]
        mock_connection.fetch.return_value = test_rows

        test_date = date(2023, 12, 15)
        test_instrument_ids = [1, 2, 3]

        with patch(\'domains.market_data.repositories.secmaster_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.batch_last_close_prices(test_date, test_instrument_ids)

        expected_result = {1: 150.25, 3: 85.75}  # instrument_id 2 is missing
        assert result == expected_result
        assert 2 not in result

    @pytest.mark.asyncio

    async def test_batch_market_caps_success(self, dao):
        """Test successful batch retrieval of market caps."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock successful query result
        test_rows = [
            {'instrument_id': 1, 'market_cap': 2500000000.0},
            {'instrument_id': 2, 'market_cap': 1800000000.0}
        ]
        mock_connection.fetch.return_value = test_rows

        test_date = date(2023, 12, 15)
        test_instrument_ids = [1, 2]

        with patch(\'domains.market_data.repositories.secmaster_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.batch_market_caps(test_date, test_instrument_ids)

        expected_result = {1: 2500000000.0, 2: 1800000000.0}
        assert result == expected_result

        # Verify SQL structure
        call_args = mock_connection.fetch.call_args[0][0]
        assert "SELECT instrument_id, market_cap FROM test_daily_price_polygon" in call_args
        assert "WHERE date = $1 AND instrument_id = ANY($2)" in call_args

    @pytest.mark.asyncio

    async def test_get_last_close_price_success(self, dao):
        """Test successful retrieval of last close price for single instrument."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock successful price retrieval
        mock_connection.fetchval.return_value = 150.75

        test_instrument_id = 123
        test_date = date(2023, 12, 15)

        with patch(\'domains.market_data.repositories.secmaster_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_last_close_price(test_instrument_id, test_date)

        assert result == 150.75

        # Verify query structure - should get most recent price on or before date
        call_args = mock_connection.fetchval.call_args[0][0]
        assert "SELECT close FROM test_daily_price_polygon" in call_args
        assert "WHERE instrument_id = $1 AND date <= $2" in call_args
        assert "ORDER BY date DESC LIMIT 1" in call_args

    @pytest.mark.asyncio

    async def test_get_last_close_price_no_data(self, dao):
        """Test get_last_close_price when no price data exists."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock no data found
        mock_connection.fetchval.return_value = None

        test_instrument_id = 999
        test_date = date(2023, 12, 15)

        with patch(\'domains.market_data.repositories.secmaster_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_last_close_price(test_instrument_id, test_date)

        assert result is None

    @pytest.mark.asyncio

    async def test_get_market_cap_success(self, dao):
        """Test successful market cap retrieval."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock successful market cap retrieval
        mock_connection.fetchval.return_value = 2500000000.0

        test_instrument_id = 123
        test_date = date(2023, 12, 15)

        with patch(\'domains.market_data.repositories.secmaster_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_market_cap(test_instrument_id, test_date)

        assert result == 2500000000.0

        # Verify query structure
        call_args = mock_connection.fetchval.call_args[0][0]
        assert "SELECT market_cap FROM test_daily_price_polygon" in call_args
        assert "WHERE instrument_id = $1 AND date <= $2" in call_args
        assert "ORDER BY date DESC LIMIT 1" in call_args

    @pytest.mark.asyncio

    async def test_get_average_dollar_volume_default_window(self, dao):
        """Test average dollar volume calculation with default 30-day window."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock successful average calculation
        mock_connection.fetchval.return_value = 50000000.0  # $50M average daily volume

        test_instrument_id = 123
        test_date = date(2023, 12, 15)

        with patch(\'domains.market_data.repositories.secmaster_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_average_dollar_volume(test_instrument_id, test_date)

        assert result == 50000000.0

        # Verify query structure and default window of 30 days
        call_args = mock_connection.fetchval.call_args
        sql_query = call_args[0][0]
        params = call_args[0][1:]

        assert "AVG(close * volume)" in sql_query
        assert "ORDER BY date DESC LIMIT $3" in sql_query
        assert params[0] == test_instrument_id
        assert params[1] == test_date
        assert params[2] == 30  # Default window

    @pytest.mark.asyncio

    async def test_get_average_dollar_volume_custom_window(self, dao):
        """Test average dollar volume calculation with custom window size."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        mock_connection.fetchval.return_value = 75000000.0

        test_instrument_id = 123
        test_date = date(2023, 12, 15)
        custom_window = 90  # 90-day window

        with patch(\'domains.market_data.repositories.secmaster_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_average_dollar_volume(test_instrument_id, test_date, custom_window)

        assert result == 75000000.0

        # Verify custom window was used
        call_args = mock_connection.fetchval.call_args[0]
        assert call_args[3] == custom_window

    @pytest.mark.asyncio

    async def test_get_average_dollar_volume_no_data(self, dao):
        """Test get_average_dollar_volume when no volume data exists."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        # Mock no data found
        mock_connection.fetchval.return_value = None

        test_instrument_id = 999
        test_date = date(2023, 12, 15)

        with patch(\'domains.market_data.repositories.secmaster_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result = await dao.get_average_dollar_volume(test_instrument_id, test_date)

        assert result is None

    @pytest.mark.asyncio

    async def test_database_connection_error(self, dao):
        """Test handling of database connection errors."""
        connection_error = Exception("Connection failed")
        with patch(\'domains.market_data.repositories.secmaster_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, side_effect=connection_error):
            with pytest.raises(Exception, match="Connection failed"):
                await dao.get_spy_membership_events()

    @pytest.mark.asyncio

    async def test_sql_injection_protection_batch_queries(self, dao):
        """Test that batch queries use parameterized queries for SQL injection protection."""
        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        mock_connection.fetch.return_value = []

        # Test with potentially malicious data
        malicious_date = "2023-01-01'; DROP TABLE test_daily_price_polygon; --"
        malicious_ids = [1, 2, 3]

        with patch(\'domains.market_data.repositories.secmaster_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            await dao.batch_last_close_prices(malicious_date, malicious_ids)

        # Verify that parameters were passed safely, not concatenated into SQL
        call_args = mock_connection.fetch.call_args
        sql_query = call_args[0][0]
        params = call_args[0][1:]

        # SQL should use placeholders, not have malicious content embedded
        assert "$1" in sql_query and "$2" in sql_query
        assert "DROP TABLE" not in sql_query
        assert params[0] == malicious_date  # Passed as parameter
        assert params[1] == malicious_ids   # Passed as parameter

    def test_dao_initialization(self, mock_environment):
        """Test DAO initialization sets correct attributes."""
        dao = SecMasterDAO(mock_environment)

        assert dao.env == mock_environment
        assert dao.universe_membership_table == "test_universe_membership"
        assert dao.daily_price_polygon_table == "test_daily_price_polygon"
        assert dao.db_url == "postgresql://test:test@localhost/test_db"

        # Verify environment methods were called correctly
        call_args_list = mock_environment.get_table_name.call_args_list
        actual_calls = [call.args[0] for call in call_args_list]
        assert actual_calls == ['universe_membership', 'daily_price_polygon']
        mock_environment.get_database_url.assert_called_once()

    @pytest.mark.asyncio

    async def test_empty_instrument_ids_handling(self, dao):
        """Test methods handle empty instrument_ids lists gracefully."""
        # This tests methods that should handle edge cases with empty inputs
        test_date = date(2023, 12, 15)
        empty_ids = []

        mock_pool = MagicMock()
        mock_connection = AsyncMock()

        async_context_manager = AsyncMock()
        async_context_manager.__aenter__.return_value = mock_connection
        mock_pool.acquire.return_value = async_context_manager
        mock_pool.close = AsyncMock()

        mock_connection.fetch.return_value = []

        with patch(\'domains.market_data.repositories.secmaster_core.dao.asyncpg.create_pool\', new_callable=AsyncMock, return_value=mock_pool):
            result_prices = await dao.batch_last_close_prices(test_date, empty_ids)
            result_market_caps = await dao.batch_market_caps(test_date, empty_ids)

        # Should return empty dictionaries
        assert result_prices == {}
        assert result_market_caps == {}