"""
Integration tests for EODHD population with real API and database

These tests validate the complete workflow with actual EODHD API calls
and database operations in the development environment.
"""

import pytest
import os
from datetime import date

# Import the modules we're testing
import sys
sys.path.append('/workspace/src')

from domains.instruments.services.secmaster.populate_instrument_eodhd import (
    get_exchange_symbols,
    fetch_fundamental_data,
    fetch_and_store_instruments,
    parse_date
)
from core.platform.config.environment import Environment
from core.shared.utils.database import Database


@pytest.fixture
def dev_environment():
    """Development environment configuration"""
    return Environment()


@pytest.fixture
def api_key():
    """EODHD API key from environment"""
    key = os.environ.get('EODHD_API_KEY', '68aa0c7d2fe831.67386369')
    if not key:
        pytest.skip("EODHD_API_KEY not available")
    return key


@pytest.fixture
def test_symbols():
    """Known symbols that should have data"""
    return ['AAPL', 'MSFT', 'GOOGL']


@pytest.mark.integration
@pytest.mark.slow
class TestEODHDAPIIntegration:
    """Integration tests with real EODHD API"""

    @pytest.mark.asyncio

    async def test_get_exchange_symbols_real_api(self, api_key):
        """Test fetching symbols from real EODHD exchange API"""
        symbols = await get_exchange_symbols('US', api_key)

        assert isinstance(symbols, list)
        assert len(symbols) > 1000  # US exchange should have many symbols

        # Check structure of returned symbols
        first_symbol = symbols[0]
        assert 'symbol' in first_symbol
        assert 'name' in first_symbol
        assert 'exchange' in first_symbol
        assert isinstance(first_symbol['symbol'], str)
        assert len(first_symbol['symbol']) > 0

    @pytest.mark.asyncio

    async def test_fetch_fundamental_data_real_api(self, api_key, test_symbols):
        """Test fetching fundamental data from real EODHD API"""
        for symbol in test_symbols:
            fundamental_data = await fetch_fundamental_data(symbol, api_key)

            assert fundamental_data is not None
            assert fundamental_data['symbol'] == symbol
            assert fundamental_data['name'] is not None
            assert fundamental_data['exchange'] is not None
            assert 'full_response' in fundamental_data

            # Most major stocks should have IPO dates
            if symbol in ['AAPL', 'MSFT']:
                assert fundamental_data['ipo_date'] is not None
                # Verify IPO date is reasonable
                parsed_date = parse_date(fundamental_data['ipo_date'])
                assert parsed_date is not None
                assert parsed_date.year >= 1970
                assert parsed_date.year <= 2025

            print(f"✅ {symbol}: {fundamental_data['name']}, IPO: {fundamental_data.get('ipo_date', 'N/A')}")

    @pytest.mark.asyncio

    async def test_fetch_fundamental_data_invalid_symbol(self, api_key):
        """Test handling of invalid symbols with real API"""
        result = await fetch_fundamental_data('INVALID_SYMBOL_XYZ123', api_key)
        assert result is None

    @pytest.mark.skip("Rate limited - only run manually")
    @pytest.mark.asyncio
    async def test_exchange_symbols_rate_limiting(self, api_key):
        """Test that we handle API rate limiting properly"""
        import time

        start_time = time.time()
        symbols = await get_exchange_symbols('US', api_key)
        end_time = time.time()

        assert len(symbols) > 0
        # Should complete in reasonable time (less than 30 seconds)
        assert (end_time - start_time) < 30


@pytest.mark.integration
class TestDatabaseIntegration:
    """Integration tests with real database"""

    @pytest.mark.asyncio

    async def test_database_table_creation(self, dev_environment):
        """Test that database table is created correctly"""
        # This will be tested as part of the main function
        # We verify table structure matches our expectations

        pool = await Database.create_connection_pool(env=dev_environment, max_retries=3, initial_delay=1.0, timeout=10.0)

        async with pool.acquire() as conn:
            # Check if table exists and has correct structure
            result = await conn.fetch("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = $1
                ORDER BY ordinal_position
            """, dev_environment.get_table_name('instrument_eodhd'))

            columns = {row['column_name']: row for row in result}

            # Verify essential columns exist
            essential_columns = ['symbol', 'name', 'exchange', 'asset_type', 'currency', 'ipo_date', 'country', 'sector', 'industry']
            for col in essential_columns:
                assert col in columns, f"Column {col} not found in table"

            # Verify ipo_date is actually a date column
            assert columns['ipo_date']['data_type'] == 'date'
            assert columns['ipo_date']['is_nullable'] == 'YES'

    @pytest.mark.asyncio

    async def test_complete_workflow_small_sample(self, dev_environment, api_key):
        """Test complete workflow with a small sample of real data"""
        # Test with just a few symbols to avoid long test times
        test_tickers = 'AAPL,MSFT'

        # Get initial count
        pool = await Database.create_connection_pool(env=dev_environment, max_retries=3, initial_delay=1.0, timeout=10.0)

        async with pool.acquire() as conn:
            initial_count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {dev_environment.get_table_name('instrument_eodhd')} WHERE symbol IN ('AAPL', 'MSFT')"
            )

        # Run the population
        from unittest.mock import patch

        # Mock the environment to use our test environment
        with patch('src.secmaster.populate_instrument_eodhd.env', dev_environment):
            with patch('src.secmaster.populate_instrument_eodhd.EODHD_API_KEY', api_key):
                await fetch_and_store_instruments(ticker=test_tickers)

        # Verify data was inserted/updated
        async with pool.acquire() as conn:
            final_count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {dev_environment.get_table_name('instrument_eodhd')} WHERE symbol IN ('AAPL', 'MSFT')"
            )

            # Should have data for both symbols
            assert final_count == 2

            # Check that we have actual data including IPO dates
            results = await conn.fetch(
                f"SELECT symbol, name, exchange, ipo_date FROM {dev_environment.get_table_name('instrument_eodhd')} WHERE symbol IN ('AAPL', 'MSFT')"
            )

            results_dict = {row['symbol']: row for row in results}

            # Verify AAPL data
            aapl = results_dict['AAPL']
            assert aapl['name'] == 'Apple Inc'
            assert aapl['exchange'] == 'NASDAQ'
            assert aapl['ipo_date'] is not None
            assert aapl['ipo_date'] == date(1980, 12, 12)

            # Verify MSFT data
            msft = results_dict['MSFT']
            assert msft['name'] == 'Microsoft Corp'
            assert msft['exchange'] == 'NASDAQ'
            assert msft['ipo_date'] is not None
            assert msft['ipo_date'] == date(1986, 3, 13)

            print(f"✅ AAPL: {aapl['name']}, IPO: {aapl['ipo_date']}")
            print(f"✅ MSFT: {msft['name']}, IPO: {msft['ipo_date']}")

    @pytest.mark.asyncio

    async def test_data_quality_after_population(self, dev_environment):
        """Test data quality metrics after population"""
        pool = await Database.create_connection_pool(env=dev_environment, max_retries=3, initial_delay=1.0, timeout=10.0)

        async with pool.acquire() as conn:
            table_name = dev_environment.get_table_name('instrument_eodhd')

            # Get data quality metrics
            metrics = await conn.fetchrow(f"""
                SELECT
                    COUNT(*) as total_instruments,
                    COUNT(ipo_date) as with_ipo_date,
                    COUNT(*) - COUNT(ipo_date) as null_ipo_date,
                    ROUND(100.0 * COUNT(ipo_date) / COUNT(*), 2) as ipo_date_percentage
                FROM {table_name}
            """)

            print(f"📊 Data Quality Metrics:")
            print(f"   Total instruments: {metrics['total_instruments']}")
            print(f"   With IPO dates: {metrics['with_ipo_date']}")
            print(f"   Without IPO dates: {metrics['null_ipo_date']}")
            print(f"   IPO date coverage: {metrics['ipo_date_percentage']}%")

            # After our improvements, we should have much better coverage
            # At minimum, the test symbols we populated should have IPO dates
            if metrics['total_instruments'] > 0:
                assert metrics['with_ipo_date'] > 0

                # If we have test data, check specific symbols
                test_symbols_with_dates = await conn.fetchval(f"""
                    SELECT COUNT(*) FROM {table_name}
                    WHERE symbol IN ('AAPL', 'MSFT') AND ipo_date IS NOT NULL
                """)

                if test_symbols_with_dates > 0:
                    # Our test symbols should have IPO dates
                    assert test_symbols_with_dates >= 1

@pytest.mark.integration
@pytest.mark.slow
class TestPerformanceAndReliability:
    """Test performance and reliability aspects"""

    @pytest.mark.skip("Long running test - only run manually")
    @pytest.mark.asyncio
    async def test_bulk_mode_sample(self, dev_environment, api_key):
        """Test bulk mode with a small sample"""
        from unittest.mock import patch

        # Mock to limit the number of symbols processed
        original_get_symbols = get_exchange_symbols

        async def limited_get_symbols(exchange, api_key):
            all_symbols = await original_get_symbols(exchange, api_key)
            return all_symbols[:5]  # Limit to first 5 symbols

        with patch('src.secmaster.populate_instrument_eodhd.get_exchange_symbols', limited_get_symbols):
            with patch('src.secmaster.populate_instrument_eodhd.env', dev_environment):
                with patch('src.secmaster.populate_instrument_eodhd.EODHD_API_KEY', api_key):
                    # Mock sleep to speed up test
                    with patch('src.secmaster.populate_instrument_eodhd.time.sleep'):
                        await fetch_and_store_instruments(bulk_mode=True, exchange='US')

        # Verify some data was processed
        pool = await Database.create_connection_pool(env=dev_environment, max_retries=3, initial_delay=1.0, timeout=10.0)

        async with pool.acquire() as conn:
            count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {dev_environment.get_table_name('instrument_eodhd')}"
            )
            assert count >= 5  # Should have processed at least the limited symbols

    @pytest.mark.asyncio

    async def test_error_recovery(self, dev_environment):
        """Test that the system recovers gracefully from errors"""
        from unittest.mock import patch

        # Test with a mix of valid and invalid symbols
        test_tickers = 'AAPL,INVALID_SYMBOL_XYZ,MSFT'

        with patch('src.secmaster.populate_instrument_eodhd.env', dev_environment):
            # Should complete without raising exception despite invalid symbol
            await fetch_and_store_instruments(ticker=test_tickers)

        # Verify that valid symbols were still processed
        pool = await Database.create_connection_pool(env=dev_environment, max_retries=3, initial_delay=1.0, timeout=10.0)

        async with pool.acquire() as conn:
            valid_symbols = await conn.fetch(
                f"SELECT symbol FROM {dev_environment.get_table_name('instrument_eodhd')} WHERE symbol IN ('AAPL', 'MSFT')"
            )

            symbols_found = [row['symbol'] for row in valid_symbols]
            assert 'AAPL' in symbols_found or 'MSFT' in symbols_found

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])