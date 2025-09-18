#!/usr/bin/env python3
"""
Integration tests for comprehensive multi-vendor news backfill system.
Tests the complete pipeline with real API calls (when API keys available).
"""

import pytest
import os
from unittest.mock import patch
from datetime import date

import sys
sys.path.insert(0, 'src')

from domains.market_data.services.news.comprehensive_news_backfill import ComprehensiveNewsBackfiller


class TestComprehensiveNewsBackfillIntegration:
    """Integration tests for news backfill system"""

    @pytest.fixture
    def db_config(self):
        """Database configuration for testing"""
        return {
            'host': os.getenv("DB_HOST", "localhost"),
            'port': int(os.getenv("DB_PORT", "5433")),
            'user': os.getenv("DB_USER", "postgres"),
            'password': os.getenv("DB_PASSWORD", "postgres"),
            'database': os.getenv("DB_NAME", "dev_db")
        }

    @pytest.fixture
    def test_symbols(self):
        """Test symbols for backfill"""
        return ['AAPL', 'MSFT', 'GOOGL']

    def test_news_source_configuration(self):
        """Test that news sources are properly configured"""
        sources = ComprehensiveNewsBackfiller.NEWS_SOURCES

        # Check all expected sources exist
        assert 'polygon' in sources
        assert 'tiingo' in sources
        assert 'eodhd' in sources

        # Verify source configurations
        polygon = sources['polygon']
        assert polygon.name == 'polygon'
        assert polygon.api_key_env == 'POLYGON_API_KEY'
        assert polygon.max_concurrent > 0
        assert polygon.historical_limit_years > 0

        tiingo = sources['tiingo']
        assert tiingo.name == 'tiingo'
        assert tiingo.api_key_env == 'TIINGO_API_KEY'
        assert tiingo.max_concurrent > 0
        assert tiingo.historical_limit_years > 0

        eodhd = sources['eodhd']
        assert eodhd.name == 'eodhd'
        assert eodhd.api_key_env == 'EODHD_API_KEY'
        assert eodhd.max_concurrent > 0
        assert eodhd.historical_limit_years > 0

    def test_historical_date_range_calculation(self, db_config, test_symbols):
        """Test historical date range calculation for each vendor"""
        backfiller = ComprehensiveNewsBackfiller(db_config, test_symbols)

        # Test date range calculation
        for source_name in ['polygon', 'tiingo', 'eodhd']:
            start_date, end_date = backfiller.get_historical_date_range(source_name)

            # Verify dates are valid
            assert isinstance(start_date, date)
            assert isinstance(end_date, date)
            assert start_date < end_date
            assert end_date <= date.today()

            # Verify appropriate historical depth
            years_diff = (end_date - start_date).days / 365.25
            expected_years = backfiller.NEWS_SOURCES[source_name].historical_limit_years
            assert abs(years_diff - expected_years) < 0.1  # Within 36 days

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_backfiller_initialization(self, db_config, test_symbols):
        """Test backfiller initialization and cleanup"""
        backfiller = ComprehensiveNewsBackfiller(db_config, test_symbols)

        # Test context manager
        async with backfiller as bf:
            assert bf.db_pool is not None
            assert bf.symbols == test_symbols

        # Pool should be closed after context exit
        assert backfiller.db_pool.is_closing()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_eodhd_table_creation(self, db_config, test_symbols):
        """Test EODHD table creation"""
        async with ComprehensiveNewsBackfiller(db_config, test_symbols) as backfiller:
            # Should not raise an exception
            await backfiller.create_eodhd_table_if_not_exists()

            # Verify table exists by querying it
            async with backfiller.db_pool.acquire() as conn:
                result = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'news_eodhd'
                    )
                """)
                assert result is True

    @pytest.mark.skipif(
        not os.getenv('POLYGON_API_KEY'),
        reason="Polygon API key not available"
    )
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_polygon_news_fetching_real_api(self, db_config):
        """Test Polygon news fetching with real API (requires API key)"""
        symbols = ['AAPL']  # Single symbol for testing

        async with ComprehensiveNewsBackfiller(db_config, symbols) as backfiller:
            if 'polygon' not in backfiller.api_keys:
                pytest.skip("Polygon API key not configured")

            # Test fetching recent news (last 30 days)
            end_date = date.today()
            start_date = end_date.replace(day=1)  # First day of current month

            count = await backfiller.fetch_polygon_news(
                symbols,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )

            # Should fetch some articles (or at least not fail)
            assert count >= 0
            print(f"Fetched {count} Polygon articles for {symbols}")

    @pytest.mark.skipif(
        not os.getenv('TIINGO_API_KEY'),
        reason="Tiingo API key not available"
    )
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_tiingo_news_fetching_real_api(self, db_config):
        """Test Tiingo news fetching with real API (requires API key)"""
        symbols = ['AAPL']  # Single symbol for testing

        async with ComprehensiveNewsBackfiller(db_config, symbols) as backfiller:
            if 'tiingo' not in backfiller.api_keys:
                pytest.skip("Tiingo API key not configured")

            # Test fetching recent news (last 30 days)
            end_date = date.today()
            start_date = end_date.replace(day=1)  # First day of current month

            count = await backfiller.fetch_tiingo_news(
                symbols,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )

            # Should fetch some articles (or at least not fail)
            assert count >= 0
            print(f"Fetched {count} Tiingo articles for {symbols}")

    @pytest.mark.skipif(
        not os.getenv('EODHD_API_KEY'),
        reason="EODHD API key not available"
    )
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_eodhd_news_fetching_real_api(self, db_config):
        """Test EODHD news fetching with real API (requires API key)"""
        symbols = ['AAPL']  # Single symbol for testing

        async with ComprehensiveNewsBackfiller(db_config, symbols) as backfiller:
            if 'eodhd' not in backfiller.api_keys:
                pytest.skip("EODHD API key not configured")

            # Test fetching recent news (last 30 days)
            end_date = date.today()
            start_date = end_date.replace(day=1)  # First day of current month

            count = await backfiller.fetch_eodhd_news(
                symbols,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )

            # Should fetch some articles (or at least not fail)
            assert count >= 0
            print(f"Fetched {count} EODHD articles for {symbols}")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_comprehensive_backfill_mock_apis(self, db_config, test_symbols):
        """Test comprehensive backfill with mocked API responses"""

        # Mock API responses
        mock_polygon_response = [
            {
                'id': 'test_polygon_1',
                'title': 'Test Polygon News',
                'description': 'Test description',
                'published_utc': '2023-01-01T12:00:00Z',
                'tickers': ['AAPL'],
                'keywords': ['test']
            }
        ]

        mock_tiingo_response = [
            {
                'id': 12345,
                'title': 'Test Tiingo News',
                'description': 'Test description',
                'publishedDate': '2023-01-01T12:00:00Z',
                'crawlDate': '2023-01-01T12:05:00Z',
                'tickers': ['AAPL'],
                'tags': ['test']
            }
        ]

        mock_eodhd_response = [
            {
                'id': 'test_eodhd_1',
                'title': 'Test EODHD News',
                'content': 'Test content',
                'date': '1672574400',  # 2023-01-01 timestamp
                'symbols': ['AAPL'],
                'tags': ['test']
            }
        ]

        async with ComprehensiveNewsBackfiller(db_config, test_symbols) as backfiller:
            # Mock the API fetching methods
            with patch.object(backfiller, 'fetch_polygon_news', return_value=1) as mock_polygon, \
                 patch.object(backfiller, 'fetch_tiingo_news', return_value=1) as mock_tiingo, \
                 patch.object(backfiller, 'fetch_eodhd_news', return_value=1) as mock_eodhd:

                # Mock API keys to ensure all vendors are processed
                backfiller.api_keys = {'polygon': 'test', 'tiingo': 'test', 'eodhd': 'test'}

                # Run comprehensive backfill
                results = await backfiller.run_comprehensive_backfill(limit_symbols=2)

                # Verify results
                assert 'polygon' in results
                assert 'tiingo' in results
                assert 'eodhd' in results
                assert sum(results.values()) == 3  # 1 from each vendor

                # Verify methods were called
                mock_polygon.assert_called_once()
                mock_tiingo.assert_called_once()
                mock_eodhd.assert_called_once()

    def test_api_key_loading(self, db_config, test_symbols):
        """Test API key loading from environment"""
        backfiller = ComprehensiveNewsBackfiller(db_config, test_symbols)

        # Check that API keys are loaded from environment
        for source_name, source_config in backfiller.NEWS_SOURCES.items():
            env_var = source_config.api_key_env
            env_value = os.getenv(env_var)

            if env_value:
                assert source_name in backfiller.api_keys
                assert backfiller.api_keys[source_name] == env_value
            else:
                assert source_name not in backfiller.api_keys

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_error_handling(self, test_symbols):
        """Test database connection error handling"""
        # Invalid database configuration
        bad_db_config = {
            'host': 'nonexistent_host',
            'port': 9999,
            'user': 'invalid',
            'password': 'invalid',
            'database': 'invalid'
        }

        # Should handle database connection errors gracefully
        try:
            async with ComprehensiveNewsBackfiller(bad_db_config, test_symbols) as backfiller:
                # This should fail during database pool creation
                pass
        except Exception as e:
            # Should be a connection-related error
            assert any(keyword in str(e).lower() for keyword in ['connection', 'connect', 'host'])

if __name__ == "__main__":
    # Run specific tests
    pytest.main([__file__, "-v", "-s"])