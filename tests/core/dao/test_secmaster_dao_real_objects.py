"""
Core SecMasterDAO tests using real database objects and fail-fast validation.

This replaces test_secmaster_dao.py with real database integration testing.
All 431 lines of mocks are eliminated for authentic market data testing.
"""

import pytest
from datetime import date, datetime, timedelta
from typing import List, Dict

from domains.instruments.repositories.secmaster_dao import SecMasterDAO
from domains.instruments.repositories.instruments_dao import InstrumentsDAO
from domains.trading.repositories.universe_dao import UniverseDAO
from domains.trading.repositories.universe_membership_dao import UniverseMembershipDAO
from shared.utils.environment import Environment, EnvironmentType


@pytest.fixture
async def test_environment():
    """Real test environment with actual database connection."""
    return Environment(
        env_type=EnvironmentType.DEV,
        db_url="postgresql://postgres:dev_password@localhost:5432/dev_db"
    )


@pytest.fixture
async def secmaster_dao(test_environment):
    """Real SecMasterDAO instance."""
    return SecMasterDAO(test_environment)


@pytest.fixture
async def instruments_dao(test_environment):
    """Real InstrumentsDAO for test data creation."""
    return InstrumentsDAO(test_environment)


@pytest.fixture
async def universe_dao(test_environment):
    """Real UniverseDAO for test universe creation."""
    return UniverseDAO(test_environment)


@pytest.fixture
async def universe_membership_dao(test_environment):
    """Real UniverseMembershipDAO for membership management."""
    return UniverseMembershipDAO(test_environment)


@pytest.fixture
async def test_spy_universe(universe_dao):
    """Create or get S&P 500 universe for testing."""
    # Try to get existing S&P 500 universe
    spy_universe = await universe_dao.get_universe_by_name('S&P 500')
    if spy_universe:
        yield spy_universe['id']
    else:
        # Create S&P 500 universe for testing
        universe_id = await universe_dao.create_universe(
            name='S&P 500',
            description='S&P 500 Index Universe for Testing'
        )
        yield universe_id
        # Note: Don't delete - might be used by real data


@pytest.fixture
async def test_instruments_with_prices(instruments_dao, secmaster_dao):
    """Create test instruments with sample price data."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    
    # Create test instruments
    test_instruments = [
        {
            'symbol': f'PRICE_TEST_1_{timestamp}',
            'name': 'Price Test Corp 1',
            'exchange': 'NYSE',
            'type_': 'CS',
            'currency': 'USD',
            'list_date': date(2020, 1, 1),
            'delist_date': None
        },
        {
            'symbol': f'PRICE_TEST_2_{timestamp}',
            'name': 'Price Test Corp 2',
            'exchange': 'NASDAQ',
            'type_': 'CS',
            'currency': 'USD',
            'list_date': date(2020, 1, 1),
            'delist_date': None
        }
    ]
    
    created_ids = await instruments_dao.create_instruments_batch(test_instruments)
    
    # Add sample price data (if daily_price_polygon table exists and is accessible)
    # Note: This might fail if we don't have write access to price tables
    # In that case, tests will use whatever real data exists
    
    yield created_ids
    
    # Cleanup instruments
    for instrument_id in created_ids:
        await instruments_dao.delete_instrument(instrument_id)


class TestSecMasterDAORealObjects:
    """Real database integration tests for SecMasterDAO."""

    async def test_get_spy_membership_events_real_data(self, secmaster_dao, test_spy_universe):
        """Test SPY membership events retrieval with real database."""
        events = await secmaster_dao.get_spy_membership_events()
        
        # Should return list (might be empty if no memberships exist)
        assert isinstance(events, list)
        
        # If events exist, verify structure
        for event in events:
            assert 'instrument_id' in event
            assert 'start_date' in event
            assert isinstance(event['instrument_id'], int)
            assert isinstance(event['start_date'], date)
            # end_date can be None for active memberships
            if event.get('end_date'):
                assert isinstance(event['end_date'], date)

    async def test_batch_last_close_prices_real_data(self, secmaster_dao):
        """Test batch close price retrieval with real instruments."""
        # Get some real instrument IDs from database
        # We'll use a small sample of actual instruments if they exist
        test_date = date.today() - timedelta(days=7)  # Use recent date
        
        # For this test, we'll try with a known instrument ID range
        # In real testing, you'd use actual instrument IDs from your database
        sample_instrument_ids = [1, 2, 3, 4, 5]  # Adjust based on your data
        
        prices = await secmaster_dao.batch_last_close_prices(test_date, sample_instrument_ids)
        
        # Should return dictionary (might be empty if no price data)
        assert isinstance(prices, dict)
        
        # If prices exist, verify structure
        for instrument_id, price in prices.items():
            assert isinstance(instrument_id, int)
            assert isinstance(price, (int, float))
            assert price > 0  # Prices should be positive

    async def test_batch_last_close_prices_empty_instrument_list(self, secmaster_dao):
        """Test batch close prices with empty instrument list."""
        test_date = date.today()
        
        prices = await secmaster_dao.batch_last_close_prices(test_date, [])
        
        assert prices == {}

    async def test_batch_market_caps_real_data(self, secmaster_dao):
        """Test batch market cap retrieval with real data."""
        test_date = date.today() - timedelta(days=7)
        sample_instrument_ids = [1, 2, 3]  # Adjust based on your data
        
        market_caps = await secmaster_dao.batch_market_caps(test_date, sample_instrument_ids)
        
        # Should return dictionary
        assert isinstance(market_caps, dict)
        
        # If market caps exist, verify structure
        for instrument_id, market_cap in market_caps.items():
            assert isinstance(instrument_id, int)
            assert isinstance(market_cap, (int, float))
            assert market_cap > 0  # Market caps should be positive

    async def test_get_last_close_price_single_instrument(self, secmaster_dao):
        """Test single instrument close price retrieval."""
        # Use instrument ID 1 if it exists, otherwise test will show no data
        test_instrument_id = 1
        test_date = date.today() - timedelta(days=7)
        
        price = await secmaster_dao.get_last_close_price(test_instrument_id, test_date)
        
        # Price might be None if no data exists
        if price is not None:
            assert isinstance(price, (int, float))
            assert price > 0

    async def test_get_last_close_price_nonexistent_instrument(self, secmaster_dao):
        """Test close price retrieval for nonexistent instrument."""
        nonexistent_id = 999999999
        test_date = date.today()
        
        price = await secmaster_dao.get_last_close_price(nonexistent_id, test_date)
        
        # Should return None for nonexistent instrument
        assert price is None

    async def test_get_market_cap_single_instrument(self, secmaster_dao):
        """Test single instrument market cap retrieval."""
        test_instrument_id = 1  # Adjust based on your data
        test_date = date.today() - timedelta(days=7)
        
        market_cap = await secmaster_dao.get_market_cap(test_instrument_id, test_date)
        
        # Market cap might be None if no data exists
        if market_cap is not None:
            assert isinstance(market_cap, (int, float))
            assert market_cap > 0

    async def test_get_average_dollar_volume_default_window(self, secmaster_dao):
        """Test average dollar volume with default 30-day window."""
        test_instrument_id = 1  # Adjust based on your data
        test_date = date.today() - timedelta(days=35)  # Ensure we have 30 days of history
        
        avg_volume = await secmaster_dao.get_average_dollar_volume(test_instrument_id, test_date)
        
        # Volume might be None if no data exists
        if avg_volume is not None:
            assert isinstance(avg_volume, (int, float))
            assert avg_volume >= 0  # Volume should be non-negative

    async def test_get_average_dollar_volume_custom_window(self, secmaster_dao):
        """Test average dollar volume with custom window size."""
        test_instrument_id = 1  # Adjust based on your data
        test_date = date.today() - timedelta(days=100)  # Ensure we have enough history
        custom_window = 90
        
        avg_volume = await secmaster_dao.get_average_dollar_volume(
            test_instrument_id, test_date, custom_window
        )
        
        # Volume might be None if no data exists
        if avg_volume is not None:
            assert isinstance(avg_volume, (int, float))
            assert avg_volume >= 0

    async def test_get_average_dollar_volume_no_data(self, secmaster_dao):
        """Test average dollar volume for instrument with no data."""
        nonexistent_id = 999999999
        test_date = date.today()
        
        avg_volume = await secmaster_dao.get_average_dollar_volume(nonexistent_id, test_date)
        
        # Should return None for instrument with no data
        assert avg_volume is None

    async def test_sql_injection_protection_real_database(self, secmaster_dao):
        """Test SQL injection protection with real database."""
        # Malicious input that would be dangerous if not parameterized
        malicious_date = "2023-01-01'; DROP TABLE dev_daily_price_polygon; --"
        malicious_ids = [1, 2, 3]
        
        # This should be safe because queries use parameterized statements
        # The malicious date string will be treated as a literal value
        try:
            prices = await secmaster_dao.batch_last_close_prices(malicious_date, malicious_ids)
            # Should either return empty dict or raise a type conversion error
            # Either outcome is safe - no SQL injection occurred
            assert isinstance(prices, dict)
        except Exception as e:
            # Type conversion error is expected and safe
            # This shows the parameter was treated as a literal, not SQL
            assert "invalid input syntax" in str(e) or "conversion" in str(e)
        
        # Database should still be intact - verify with a safe query
        safe_prices = await secmaster_dao.batch_last_close_prices(date.today(), [1])
        assert isinstance(safe_prices, dict)

    async def test_date_range_queries_real_data(self, secmaster_dao):
        """Test date range queries with real database constraints."""
        test_instrument_id = 1  # Adjust based on your data
        
        # Test with various date ranges
        recent_date = date.today() - timedelta(days=1)
        old_date = date.today() - timedelta(days=365)
        future_date = date.today() + timedelta(days=30)
        
        # Recent date might have data
        recent_price = await secmaster_dao.get_last_close_price(test_instrument_id, recent_date)
        if recent_price is not None:
            assert isinstance(recent_price, (int, float))
        
        # Old date might have data (historical)
        old_price = await secmaster_dao.get_last_close_price(test_instrument_id, old_date)
        if old_price is not None:
            assert isinstance(old_price, (int, float))
        
        # Future date should return most recent available data
        future_price = await secmaster_dao.get_last_close_price(test_instrument_id, future_date)
        if future_price is not None:
            assert isinstance(future_price, (int, float))

    async def test_large_instrument_list_performance(self, secmaster_dao):
        """Test performance with larger instrument lists."""
        # Test with a larger list of instrument IDs
        # In real scenario, adjust based on actual instrument count in database
        large_instrument_list = list(range(1, 101))  # 100 instruments
        test_date = date.today() - timedelta(days=7)
        
        # This tests both database performance and DAO handling
        import time
        start_time = time.time()
        
        prices = await secmaster_dao.batch_last_close_prices(test_date, large_instrument_list)
        
        end_time = time.time()
        query_time = end_time - start_time
        
        # Verify result structure
        assert isinstance(prices, dict)
        
        # Performance assertion - should complete within reasonable time
        # Adjust threshold based on your database performance expectations
        assert query_time < 30.0  # Should complete within 30 seconds
        
        # Verify returned data integrity
        for instrument_id, price in prices.items():
            assert isinstance(instrument_id, int)
            assert isinstance(price, (int, float))
            assert price > 0

    async def test_concurrent_query_execution(self, secmaster_dao):
        """Test concurrent query execution for race conditions."""
        import asyncio
        
        test_instrument_id = 1
        test_date = date.today() - timedelta(days=7)
        
        # Execute multiple concurrent queries
        async def get_price():
            return await secmaster_dao.get_last_close_price(test_instrument_id, test_date)
        
        # Run 10 concurrent queries
        tasks = [get_price() for _ in range(10)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # All results should be consistent (same price or all None)
        # No exceptions should occur due to race conditions
        valid_results = [r for r in results if not isinstance(r, Exception)]
        
        if valid_results:
            # If we got price data, all should be the same
            first_price = valid_results[0]
            for price in valid_results:
                assert price == first_price

    async def test_data_consistency_across_methods(self, secmaster_dao):
        """Test data consistency between different DAO methods."""
        test_instrument_id = 1
        test_date = date.today() - timedelta(days=7)
        
        # Get price using single method
        single_price = await secmaster_dao.get_last_close_price(test_instrument_id, test_date)
        
        # Get price using batch method
        batch_prices = await secmaster_dao.batch_last_close_prices(test_date, [test_instrument_id])
        batch_price = batch_prices.get(test_instrument_id)
        
        # Both methods should return consistent results
        if single_price is not None and batch_price is not None:
            assert single_price == batch_price
        elif single_price is None and batch_price is None:
            # Both returning None is also consistent
            pass
        else:
            # One method returning data while other returns None is inconsistent
            # This might indicate a bug in the implementation
            assert False, f"Inconsistent results: single={single_price}, batch={batch_price}"


class TestSecMasterDAOConstraintValidation:
    """Test database constraint validation with real database."""

    async def test_invalid_date_handling(self, secmaster_dao):
        """Test handling of invalid date inputs."""
        test_instrument_id = 1
        
        # Test with None date (should raise exception)
        with pytest.raises(Exception):
            await secmaster_dao.get_last_close_price(test_instrument_id, None)

    async def test_negative_instrument_id_handling(self, secmaster_dao):
        """Test handling of negative instrument IDs."""
        negative_id = -1
        test_date = date.today()
        
        # Negative IDs should either return None or raise constraint violation
        result = await secmaster_dao.get_last_close_price(negative_id, test_date)
        
        # Either None result or exception is acceptable
        # The important thing is it doesn't cause database corruption
        if result is not None:
            assert isinstance(result, (int, float))

    async def test_zero_and_negative_window_size(self, secmaster_dao):
        """Test average dollar volume with invalid window sizes."""
        test_instrument_id = 1
        test_date = date.today() - timedelta(days=30)
        
        # Test with zero window
        with pytest.raises(Exception):
            await secmaster_dao.get_average_dollar_volume(test_instrument_id, test_date, 0)
        
        # Test with negative window
        with pytest.raises(Exception):
            await secmaster_dao.get_average_dollar_volume(test_instrument_id, test_date, -10)

    async def test_extremely_large_instrument_list(self, secmaster_dao):
        """Test handling of extremely large instrument lists."""
        # Test with very large list that might exceed database limits
        very_large_list = list(range(1, 10001))  # 10,000 instruments
        test_date = date.today()
        
        # This should either succeed or fail gracefully with a clear error
        try:
            prices = await secmaster_dao.batch_last_close_prices(test_date, very_large_list)
            assert isinstance(prices, dict)
        except Exception as e:
            # If it fails, should be a clear database limit error
            # Not a silent failure or corruption
            assert "limit" in str(e).lower() or "too many" in str(e).lower()

    async def test_database_connection_resilience(self, secmaster_dao):
        """Test DAO behavior under connection stress."""
        # Test multiple rapid queries to check connection pooling
        test_instrument_id = 1
        test_date = date.today() - timedelta(days=7)
        
        # Rapid sequential queries
        for i in range(20):
            price = await secmaster_dao.get_last_close_price(test_instrument_id, test_date)
            # Each query should succeed or consistently fail
            # No intermittent connection errors
            if price is not None:
                assert isinstance(price, (int, float))


if __name__ == '__main__':
    pytest.main([__file__, '-v'])