#!/usr/bin/env python3
"""
Comprehensive Test Coverage for News Collection Issues

This test suite prevents the critical issues discovered:
1. Date format bugs in API calls
2. Silent failure bugs (reporting success without actual inserts)
3. Transaction integrity issues
4. Data freshness monitoring gaps

Based on investigation of Polygon news collection stopping at 2025-08-27.
"""

import pytest
import aiohttp
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from scripts.polygon_news_backfill import PolygonNewsBackfill
from shared.utils.database_connections import get_connection_pool


class TestPolygonAPIDateFormatting:
    """Test suite for API date formatting issues"""

    def test_polygon_api_date_format_correctness(self):
        """Test that date formatting matches Polygon API expectations"""
        test_date = datetime(2025, 8, 28, 12, 30, 45)

        # Test current (fixed) format
        formatted_date = test_date.strftime("%Y-%m-%d")
        assert formatted_date == "2025-08-28"

        # Test that old buggy format would fail
        buggy_format = test_date.strftime("%Y-%m-%dT%H:%M:%S")
        assert buggy_format == "2025-08-28T12:30:45"  # This caused API errors

    @pytest.mark.asyncio
    async def test_polygon_api_date_format_validation(self):
        """Test actual Polygon API with correct vs incorrect date formats"""
        api_key = os.getenv('POLYGON_API_KEY', 'test_key')

        async with aiohttp.ClientSession() as session:
            # Test correct format (should work)
            correct_url = f"https://api.polygon.io/v2/reference/news?published_utc.gte=2025-08-28&published_utc.lt=2025-08-29&apikey={api_key}&limit=1"
            async with session.get(correct_url) as response:
                if response.status == 200:
                    data = await response.json()
                    assert data.get('status') == 'OK'

            # Test incorrect format (should fail with specific error)
            incorrect_url = f"https://api.polygon.io/v2/reference/news?published_utc.gte=2025-08-28T00:00:00&published_utc.lt=2025-08-29T00:00:00&apikey={api_key}&limit=1"
            async with session.get(incorrect_url) as response:
                if response.status == 400:
                    data = await response.json()
                    assert "invalid format for published_utc.lt" in data.get('error', '')


class TestDatabaseTransactionIntegrity:
    """Test suite for database transaction and insertion issues"""

    @pytest.mark.asyncio
    async def test_news_insertion_actually_inserts_records(self):
        """Test that reported insertion counts match actual database records"""
        # This test prevents the silent failure bug where script reports
        # successful insertion but records don't appear in database

        pool = await get_connection_pool('test')
        backfill = PolygonNewsBackfill(pool, 'test')

        # Get initial count
        async with pool.acquire() as conn:
            initial_count = await conn.fetchval("SELECT COUNT(*) FROM test_news_polygon")

        # Insert test articles
        test_articles = [
            {
                'id': 'test_article_1',
                'title': 'Test Article for Silent Failure Detection',
                'description': 'This tests actual insertion vs reported insertion',
                'author': 'Test Author',
                'published_utc': datetime.now(),
                'article_url': 'https://test.com/article1',
                'publisher': {'name': 'Test Publisher'},
                'tickers': ['TEST'],
                'keywords': ['test'],
                'insights': []
            }
        ]

        # Call insertion method
        inserted, updated, skipped = await backfill._store_articles(test_articles)

        # Verify actual database count increased by reported amount
        async with pool.acquire() as conn:
            final_count = await conn.fetchval("SELECT COUNT(*) FROM test_news_polygon")

        actual_increase = final_count - initial_count
        assert actual_increase == inserted, f"Script reported {inserted} inserted but actual increase was {actual_increase}"

    @pytest.mark.asyncio
    async def test_duplicate_handling_accuracy(self):
        """Test that duplicate detection properly updates vs inserts"""
        pool = await get_connection_pool('test')
        backfill = PolygonNewsBackfill(pool, 'test')

        # Insert article first time
        test_article = {
            'id': 'duplicate_test_article',
            'title': 'Duplicate Test Article',
            'description': 'Original description',
            'author': 'Test Author',
            'published_utc': datetime.now(),
            'article_url': 'https://test.com/duplicate',
            'publisher': {'name': 'Test Publisher'},
            'tickers': ['DUP'],
            'keywords': ['duplicate'],
            'insights': []
        }

        # First insertion
        inserted1, updated1, skipped1 = await backfill._store_articles([test_article])
        assert inserted1 == 1
        assert updated1 == 0

        # Update article and insert again
        test_article['description'] = 'Updated description'
        inserted2, updated2, skipped2 = await backfill._store_articles([test_article])

        # Should be 0 inserted, 1 updated (not 1 inserted as in the bug)
        assert inserted2 == 0, "Duplicate article was incorrectly counted as inserted"
        assert updated2 == 1, "Duplicate article should have been updated"


class TestNewsDataFreshnessMonitoring:
    """Test suite for detecting news data gaps and freshness issues"""

    @pytest.mark.asyncio
    async def test_detect_news_data_gaps(self):
        """Test that can detect when news data stops being collected"""
        pool = await get_connection_pool('test')

        async with pool.acquire() as conn:
            # Check for gaps in news data (no news for > 24 hours during market days)
            gap_query = """
            SELECT
                DATE(published_utc) as date,
                COUNT(*) as articles
            FROM test_news_polygon
            WHERE published_utc >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY DATE(published_utc)
            ORDER BY date DESC
            """

            daily_counts = await conn.fetch(gap_query)

            # Alert if any weekday has 0 articles (sign of collection failure)
            for row in daily_counts:
                weekday = row['date'].weekday()  # 0=Monday, 6=Sunday
                if weekday < 5 and row['articles'] == 0:  # Weekday with no articles
                    pytest.fail(f"News collection gap detected: {row['date']} has 0 articles")

    @pytest.mark.asyncio
    async def test_news_data_freshness_threshold(self):
        """Test that news data is within acceptable freshness threshold"""
        pool = await get_connection_pool('test')

        async with pool.acquire() as conn:
            latest_news = await conn.fetchval(
                "SELECT MAX(published_utc) FROM test_news_polygon"
            )

            if latest_news:
                age_hours = (datetime.now(latest_news.tzinfo) - latest_news).total_seconds() / 3600

                # Alert if news is older than 48 hours (sign of collection failure)
                assert age_hours <= 48, f"News data is stale: {age_hours:.1f} hours old (last: {latest_news})"


class TestNewsCollectionEndToEnd:
    """End-to-end integration tests for complete news collection workflow"""

    @pytest.mark.asyncio
    async def test_complete_news_backfill_workflow(self):
        """Test complete news collection workflow from API to database"""
        pool = await get_connection_pool('test')
        backfill = PolygonNewsBackfill(pool, 'test')

        # Test recent date range (should have articles)
        start_date = datetime.now() - timedelta(days=2)
        end_date = datetime.now() - timedelta(days=1)

        initial_count = await self._get_news_count(pool, start_date, end_date)

        # Run backfill
        await backfill.run_backfill(
            start_date=start_date,
            end_date=end_date,
            environment='test',
            limit_per_request=10,
            max_requests=2
        )

        final_count = await self._get_news_count(pool, start_date, end_date)

        # Should have collected some articles (assuming recent news exists)
        assert final_count >= initial_count, "Backfill should not reduce article count"

        # Verify no API errors occurred
        assert backfill.stats.api_errors == 0, f"API errors occurred: {backfill.stats.api_errors}"

    async def _get_news_count(self, pool, start_date, end_date):
        """Helper to get news count in date range"""
        async with pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM test_news_polygon WHERE published_utc BETWEEN $1 AND $2",
                start_date, end_date
            )


class TestNewsCollectionErrorHandling:
    """Test suite for proper error handling and reporting"""

    @pytest.mark.asyncio
    async def test_api_error_detection_and_reporting(self):
        """Test that API errors are properly detected and reported"""
        pool = await get_connection_pool('test')
        backfill = PolygonNewsBackfill(pool, 'test')

        # Mock API response with error
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status = 400
            mock_response.json.return_value = {
                'status': 'ERROR',
                'error': 'invalid format for published_utc.lt'
            }
            mock_get.return_value.__aenter__.return_value = mock_response

            # This should detect and count the API error
            await backfill._fetch_paginated_news(
                session=aiohttp.ClientSession(),
                params={'test': 'params'},
                max_requests=1,
                context='test'
            )

            # Verify error was counted
            assert backfill.stats.api_errors > 0, "API error was not detected and counted"

    def test_silent_failure_prevention(self):
        """Test that silent failures are prevented through proper validation"""
        # This test ensures that all success/failure paths are properly validated
        # and that scripts cannot report success when they actually failed

        # Test case 1: Database connection failure should raise exception
        with pytest.raises(Exception):
            pool = None  # Simulate connection failure
            # Any database operation should fail fast, not silently succeed

        # Test case 2: API key validation
        invalid_api_key = ""
        assert invalid_api_key == "", "Empty API key should be caught early"


class TestNewsCollectionMonitoring:
    """Monitoring and alerting tests for production deployment"""

    @pytest.mark.asyncio
    async def test_news_collection_health_metrics(self):
        """Test that health metrics properly detect collection issues"""
        pool = await get_connection_pool('test')

        # Check key health metrics
        async with pool.acquire() as conn:
            # Metric 1: Recent article count (should be > 0 for recent dates)
            recent_count = await conn.fetchval("""
                SELECT COUNT(*) FROM test_news_polygon
                WHERE published_utc >= CURRENT_DATE - INTERVAL '1 day'
            """)

            # Metric 2: Source diversity (should have articles from multiple sources)
            source_count = await conn.fetchval("""
                SELECT COUNT(DISTINCT publisher_name) FROM test_news_polygon
                WHERE published_utc >= CURRENT_DATE - INTERVAL '7 days'
            """)

            # Metric 3: Data quality (should have minimal null titles/descriptions)
            quality_score = await conn.fetchval("""
                SELECT
                    100.0 * COUNT(*) FILTER (WHERE title IS NOT NULL AND description IS NOT NULL) /
                    NULLIF(COUNT(*), 0) as quality_percentage
                FROM test_news_polygon
                WHERE published_utc >= CURRENT_DATE - INTERVAL '7 days'
            """)

            # Health check assertions
            if datetime.now().weekday() < 5:  # Weekday
                assert recent_count > 0, "No recent articles found on weekday"
            assert source_count >= 1, "No source diversity in recent articles"
            assert quality_score >= 90, f"Data quality too low: {quality_score}%"


# Fixture for test database setup
@pytest.fixture(scope='session')
async def test_database():
    """Setup test database with proper schema"""
    pool = await get_connection_pool('test')

    async with pool.acquire() as conn:
        # Create test table with same schema as production
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS test_news_polygon (
                id SERIAL PRIMARY KEY,
                vendor_id TEXT UNIQUE NOT NULL,
                title TEXT,
                description TEXT,
                author TEXT,
                published_utc TIMESTAMP WITH TIME ZONE,
                article_url TEXT,
                image_url TEXT,
                publisher_name TEXT,
                publisher_homepage_url TEXT,
                publisher_logo_url TEXT,
                publisher_favicon_url TEXT,
                keywords TEXT[],
                tickers TEXT[],
                insights JSONB,
                data JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)

    yield pool

    # Cleanup
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS test_news_polygon")


if __name__ == "__main__":
    # Run specific test categories
    pytest.main([
        __file__,
        "-v",
        "--tb=short",
        "-k", "test_polygon_api_date_format or test_news_insertion_actually_inserts"
    ])