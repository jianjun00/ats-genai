"""
Tests for TradingUniverse and SecurityMaster with Environment integration.
"""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import date

from trading.trading_universe import TradingUniverse, SecurityMaster
from config.environment import EnvironmentType, set_environment

def make_mock_asyncpg_pool(mock_conn):
    pool = MagicMock()
    class DummyAcquire:
        async def __aenter__(self):
            return mock_conn
        async def __aexit__(self, exc_type, exc, tb):
            pass
    pool.acquire.return_value = DummyAcquire()
    pool.close = AsyncMock()
    return pool


    """Test TradingUniverse with Environment integration."""
    
    def setup_method(self):
        """Setup test environment."""
        set_environment(EnvironmentType.TEST)
    
    def test_trading_universe_uses_environment_db_url(self):
        """Test that TradingUniverse uses environment database URL when none provided."""
        universe = TradingUniverse()
        
        # Should use test environment database URL
        assert "test_trading_db" in universe.db_url
        assert "postgresql://" in universe.db_url
    
    def test_trading_universe_explicit_db_url_override(self):
        """Test that explicit db_url overrides environment configuration."""
        custom_url = "postgresql://custom:password@localhost:5432/custom_db"
        universe = TradingUniverse(db_url=custom_url)
        
        assert universe.db_url == custom_url
    
    def test_trading_universe_table_name_prefixing(self):
        """Test that TradingUniverse uses environment-specific table names."""
        universe = TradingUniverse()
        
        daily_prices_table = universe.env.get_table_name("daily_prices")
        daily_adjusted_prices_table = universe.env.get_table_name("daily_adjusted_prices")
        
        assert daily_prices_table == "test_daily_prices"
        assert daily_adjusted_prices_table == "test_daily_adjusted_prices"
    
    @patch('asyncpg.create_pool')
    @pytest.mark.asyncio
    async def test_update_for_end_of_day_uses_prefixed_tables(self, mock_create_pool):
        """Test that update_for_end_of_day uses environment-specific table names."""
        mock_conn = AsyncMock()
        mock_pool = make_mock_asyncpg_pool(mock_conn)
        async def create_pool_side_effect(*args, **kwargs):
            return mock_pool
        mock_create_pool.side_effect = create_pool_side_effect
        mock_conn.fetch.return_value = [
            {'symbol': 'AAPL', 'close': 150.0, 'volume': 2000000, 'market_cap': 2000000000}
        ]
        universe = TradingUniverse()
        test_date = date(2023, 1, 1)
        await universe.update_for_end_of_day(test_date)
        call_args = mock_conn.fetch.call_args
        query = call_args[0][0]
        assert "test_daily_adjusted_prices" in query
        assert "test_daily_prices" in query
        assert universe.current_universe == ['AAPL']


class TestSecurityMasterEnvironment:
    """Test SecurityMaster with Environment integration."""
    
    def setup_method(self):
        """Setup test environment."""
        set_environment(EnvironmentType.TEST)
    
    def test_security_master_uses_environment_db_url(self):
        """Test that SecurityMaster uses environment database URL when none provided."""
        master = SecurityMaster()
        
        # Should use test environment database URL
        assert "test_trading_db" in master.db_url
        assert "postgresql://" in master.db_url
    
    def test_security_master_explicit_db_url_override(self):
        """Test that explicit db_url overrides environment configuration."""
        custom_url = "postgresql://custom:password@localhost:5432/custom_db"
        master = SecurityMaster(db_url=custom_url)
        
        assert master.db_url == custom_url
    
    @patch('asyncpg.create_pool', new_callable=AsyncMock)
    @pytest.mark.asyncio
    async def test_get_security_info_uses_prefixed_tables(self, mock_create_pool):
        """Test that get_security_info uses environment-specific table names."""
        # Setup mocks
        mock_conn = AsyncMock()
        mock_pool = make_mock_asyncpg_pool(mock_conn)
        mock_create_pool.return_value = mock_pool
        mock_conn.fetchrow.return_value = {
            'symbol': 'AAPL', 
            'adjusted_price': 150.0, 
            'close': 149.0, 
            'volume': 2000000, 
            'market_cap': 2000000000
        }

        master = SecurityMaster()
        test_date = date(2023, 1, 1)

        result = await master.get_security_info('AAPL', test_date)

        call_args = mock_conn.fetchrow.call_args
        query = call_args[0][0]  # First positional argument is the query

        assert "test_daily_adjusted_prices" in query
        assert "test_daily_prices" in query
        assert result['symbol'] == 'AAPL'
    
    @patch('asyncpg.create_pool', new_callable=AsyncMock)
    @pytest.mark.asyncio
    async def test_get_multiple_securities_info_uses_prefixed_tables(self, mock_create_pool):
        """Test that get_multiple_securities_info uses environment-specific table names."""
        mock_conn = AsyncMock()
        mock_pool = make_mock_asyncpg_pool(mock_conn)
        mock_create_pool.return_value = mock_pool
        mock_conn.fetch.return_value = [
            {'symbol': 'AAPL', 'adjusted_price': 150.0, 'close': 149.0, 'volume': 2000000, 'market_cap': 2000000000},
            {'symbol': 'GOOGL', 'adjusted_price': 2500.0, 'close': 2490.0, 'volume': 1500000, 'market_cap': 1500000000}
        ]

        master = SecurityMaster()
        test_date = date(2023, 1, 1)
        symbols = ['AAPL', 'GOOGL']

        result = await master.get_multiple_securities_info(symbols, test_date)

        call_args = mock_conn.fetch.call_args
        query = call_args[0][0]  # First positional argument is the query

        assert "test_daily_adjusted_prices" in query
        assert "test_daily_prices" in query
        assert len(result) == 2
        assert 'AAPL' in result
        assert 'GOOGL' in result


class TestEnvironmentSpecificBehavior:
    """Test behavior across different environments."""
    
    def test_different_environments_use_different_table_prefixes(self):
        """Test that different environments use different table prefixes."""
        # Test environment
        set_environment(EnvironmentType.TEST)
        test_universe = TradingUniverse()
        test_table = test_universe.env.get_table_name("daily_prices")
        assert test_table == "test_daily_prices"
        
        # Integration environment
        set_environment(EnvironmentType.INTEGRATION)
        intg_universe = TradingUniverse()
        intg_table = intg_universe.env.get_table_name("daily_prices")
        assert intg_table == "intg_daily_prices"
        
        # Production environment
        set_environment(EnvironmentType.PRODUCTION)
        prod_universe = TradingUniverse()
        prod_table = prod_universe.env.get_table_name("daily_prices")
        assert prod_table == "prod_daily_prices"
        
        # Verify they're all different
        assert test_table != intg_table != prod_table
        
        # Reset to test for other tests
        set_environment(EnvironmentType.TEST)
    
    def test_different_environments_use_different_databases(self):
        """Test that different environments use different database names."""
        # Test environment
        set_environment(EnvironmentType.TEST)
        test_universe = TradingUniverse()
        assert "test_trading_db" in test_universe.db_url
        
        # Integration environment
        set_environment(EnvironmentType.INTEGRATION)
        intg_universe = TradingUniverse()
        assert "intg_trading_db" in intg_universe.db_url
        
        # Production environment
        set_environment(EnvironmentType.PRODUCTION)
        prod_universe = TradingUniverse()
        assert "prod_trading_db" in prod_universe.db_url
        
        # Reset to test for other tests
        set_environment(EnvironmentType.TEST)
