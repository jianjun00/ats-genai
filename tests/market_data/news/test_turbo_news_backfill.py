import pytest
import asyncio
import json
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, date
import aioresponses

from domains.market_data.services.news.turbo_news_backfill import (
    TurboPolygonNewsFetcher,
    TurboTiingoNewsFetcher,
    TurboNewsDatabaseInserter
)

# Sample response data for mocking API calls
POLYGON_NEWS_SAMPLE_RESPONSE = {
    "status": "OK",
    "results": [
        {
            "id": "test-polygon-id-1",
            "title": "Test Polygon News Article 1",
            "description": "This is a test description for Polygon news",
            "author": "Test Author",
            "published_utc": "2024-08-01T10:00:00Z",
            "article_url": "https://example.com/article1",
            "image_url": "https://example.com/image1.jpg",
            "publisher": {
                "name": "Test Publisher",
                "homepage_url": "https://testpublisher.com",
                "logo_url": "https://testpublisher.com/logo.png",
                "favicon_url": "https://testpublisher.com/favicon.ico"
            },
            "keywords": ["test", "polygon", "news"],
            "tickers": ["AAPL", "MSFT"],
            "insights": [
                {
                    "ticker": "AAPL",
                    "sentiment": "positive",
                    "sentiment_reasoning": "Test sentiment reasoning"
                }
            ]
        },
        {
            "id": "test-polygon-id-2",
            "title": "Test Polygon News Article 2",
            "description": "This is another test description",
            "author": "Another Author",
            "published_utc": "2024-08-02T15:00:00Z",
            "article_url": "https://example.com/article2",
            "image_url": "https://example.com/image2.jpg",
            "publisher": {
                "name": "Another Publisher",
                "homepage_url": "https://anotherpublisher.com",
                "logo_url": "https://anotherpublisher.com/logo.png",
                "favicon_url": "https://anotherpublisher.com/favicon.ico"
            },
            "keywords": ["finance", "stocks"],
            "tickers": ["GOOGL"],
            "insights": [
                {
                    "ticker": "GOOGL",
                    "sentiment": "neutral",
                    "sentiment_reasoning": "Mixed signals in the article"
                }
            ]
        }
    ]
}

TIINGO_NEWS_SAMPLE_RESPONSE = [
    {
        "id": 12345,
        "title": "Test Tiingo News Article 1",
        "description": "This is a test description for Tiingo news",
        "publishedDate": "2024-08-01T10:00:00Z",
        "crawlDate": "2024-08-01T11:00:00Z",
        "url": "https://example.com/tiingo-article1",
        "source": "test-source.com",
        "tags": ["finance", "technology"],
        "tickers": ["aapl", "msft"]
    },
    {
        "id": 67890,
        "title": "Test Tiingo News Article 2",
        "description": "Another test description for Tiingo",
        "publishedDate": "2024-08-02T14:00:00Z",
        "crawlDate": "2024-08-02T15:00:00Z",
        "url": "https://example.com/tiingo-article2",
        "source": "another-source.com",
        "tags": ["market", "analysis"],
        "tickers": ["googl"]
    }
]


class TestTurboPolygonNewsFetcher:
    """Unit tests for TurboPolygonNewsFetcher."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_news_for_symbol_success(self):
        """Test successful news fetch from Polygon API."""
        with aioresponses.aioresponses() as m:
            m.get(
                'https://api.polygon.io/v2/reference/news',
                payload=POLYGON_NEWS_SAMPLE_RESPONSE
            )
            
            async with TurboPolygonNewsFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_news_for_symbol(
                    "AAPL",
                    published_gte="2024-08-01",
                    published_lte="2024-08-02"
                )
                
                assert len(results) == 2
                
                # Verify first article
                first_article = results[0]
                assert first_article['polygon_id'] == "test-polygon-id-1"
                assert first_article['title'] == "Test Polygon News Article 1"
                assert first_article['author'] == "Test Author"
                assert first_article['publisher_name'] == "Test Publisher"
                assert first_article['keywords'] == ["test", "polygon", "news"]
                assert first_article['tickers'] == ["AAPL", "MSFT"]
                assert isinstance(first_article['published_utc'], datetime)
                
                # Verify insights are properly stored
                assert first_article['insights'] is not None
                assert len(first_article['insights']) == 1
                assert first_article['insights'][0]['sentiment'] == "positive"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_news_rate_limit_retry(self):
        """Test retry logic for rate limiting."""
        with aioresponses.aioresponses() as m:
            # First request returns 429 (rate limited)
            m.get('https://api.polygon.io/v2/reference/news', status=429)
            # Second request succeeds
            m.get('https://api.polygon.io/v2/reference/news', payload=POLYGON_NEWS_SAMPLE_RESPONSE)
            
            async with TurboPolygonNewsFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_news_for_symbol("AAPL")
                
                assert len(results) == 2

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_news_server_error_retry(self):
        """Test retry logic for server errors."""
        with aioresponses.aioresponses() as m:
            # First request returns 500 (server error)
            m.get('https://api.polygon.io/v2/reference/news', status=500)
            # Second request succeeds
            m.get('https://api.polygon.io/v2/reference/news', payload=POLYGON_NEWS_SAMPLE_RESPONSE)
            
            async with TurboPolygonNewsFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_news_for_symbol("AAPL")
                
                assert len(results) == 2

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_news_max_retries_exceeded(self):
        """Test behavior when max retries are exceeded."""
        with aioresponses.aioresponses() as m:
            # All requests return 429 (rate limited)
            for _ in range(6):  # More than max_retries
                m.get('https://api.polygon.io/v2/reference/news', status=429)
            
            async with TurboPolygonNewsFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_news_for_symbol("AAPL")
                
                assert results == []

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_news_no_results(self):
        """Test handling of API response with no results."""
        with aioresponses.aioresponses() as m:
            m.get(
                'https://api.polygon.io/v2/reference/news',
                payload={"status": "OK", "results": []}
            )
            
            async with TurboPolygonNewsFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_news_for_symbol("AAPL")
                
                assert results == []

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_news_malformed_date(self):
        """Test handling of malformed date in API response."""
        malformed_response = {
            "status": "OK",
            "results": [
                {
                    "id": "test-id",
                    "title": "Test Article",
                    "published_utc": "invalid-date-format",
                    "tickers": ["AAPL"],
                    "keywords": []
                }
            ]
        }
        
        with aioresponses.aioresponses() as m:
            m.get('https://api.polygon.io/v2/reference/news', payload=malformed_response)
            
            async with TurboPolygonNewsFetcher("test_api_key", max_concurrent=1) as fetcher:
                # This should handle the error gracefully and return empty results
                results = await fetcher.fetch_news_for_symbol("AAPL")
                
                # Should return empty list due to date parsing error
                assert results == []


class TestTurboTiingoNewsFetcher:
    """Unit tests for TurboTiingoNewsFetcher."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_news_for_symbol_success(self):
        """Test successful news fetch from Tiingo API."""
        with aioresponses.aioresponses() as m:
            m.get(
                'https://api.tiingo.com/tiingo/news',
                payload=TIINGO_NEWS_SAMPLE_RESPONSE
            )
            
            async with TurboTiingoNewsFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_news_for_symbol(
                    "AAPL",
                    start_date="2024-08-01",
                    end_date="2024-08-02"
                )
                
                assert len(results) == 2
                
                # Verify first article
                first_article = results[0]
                assert first_article['tiingo_id'] == 12345
                assert first_article['title'] == "Test Tiingo News Article 1"
                assert first_article['source'] == "test-source.com"
                assert first_article['tags'] == ["finance", "technology"]
                assert first_article['tickers'] == ["aapl", "msft"]
                assert isinstance(first_article['published_date'], datetime)
                assert isinstance(first_article['crawl_date'], datetime)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_news_rate_limit_retry(self):
        """Test retry logic for rate limiting."""
        with aioresponses.aioresponses() as m:
            # First request returns 429 (rate limited)
            m.get('https://api.tiingo.com/tiingo/news', status=429)
            # Second request succeeds
            m.get('https://api.tiingo.com/tiingo/news', payload=TIINGO_NEWS_SAMPLE_RESPONSE)
            
            async with TurboTiingoNewsFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_news_for_symbol("AAPL")
                
                assert len(results) == 2

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_news_empty_response(self):
        """Test handling of empty API response."""
        with aioresponses.aioresponses() as m:
            m.get('https://api.tiingo.com/tiingo/news', payload=[])
            
            async with TurboTiingoNewsFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_news_for_symbol("AAPL")
                
                assert results == []

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_news_timeout_retry(self):
        """Test retry logic for timeouts."""
        with aioresponses.aioresponses() as m:
            # First request times out
            m.get('https://api.tiingo.com/tiingo/news', exception=asyncio.TimeoutError())
            # Second request succeeds
            m.get('https://api.tiingo.com/tiingo/news', payload=TIINGO_NEWS_SAMPLE_RESPONSE)
            
            async with TurboTiingoNewsFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_news_for_symbol("AAPL")
                
                assert len(results) == 2


class TestTurboNewsDatabaseInserter:
    """Unit tests for TurboNewsDatabaseInserter."""

    @pytest.fixture
    def db_config(self):
        """Database configuration for testing."""
        return {
            'host': 'localhost',
            'port': 5432,
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db'
        }

    @pytest.fixture
    def sample_polygon_news_data(self):
        """Sample Polygon news data for testing."""
        return [
            {
                'polygon_id': 'test-id-1',
                'title': 'Test News 1',
                'description': 'Test description 1',
                'author': 'Test Author',
                'published_utc': datetime(2024, 8, 1, 10, 0, 0),
                'article_url': 'https://example.com/1',
                'image_url': 'https://example.com/image1.jpg',
                'publisher_name': 'Test Publisher',
                'publisher_homepage_url': 'https://publisher.com',
                'publisher_logo_url': 'https://publisher.com/logo.png',
                'publisher_favicon_url': 'https://publisher.com/favicon.ico',
                'keywords': ['test', 'news'],
                'tickers': ['AAPL', 'MSFT'],
                'insights': [{'sentiment': 'positive'}],
                'data': {'original': 'data'}
            }
        ]

    @pytest.fixture
    def sample_tiingo_news_data(self):
        """Sample Tiingo news data for testing."""
        return [
            {
                'tiingo_id': 12345,
                'title': 'Test Tiingo News',
                'description': 'Test Tiingo description',
                'published_date': datetime(2024, 8, 1, 10, 0, 0),
                'crawl_date': datetime(2024, 8, 1, 11, 0, 0),
                'url': 'https://example.com/tiingo',
                'source': 'test-source.com',
                'tags': ['finance', 'tech'],
                'tickers': ['aapl'],
                'data': {'original': 'tiingo_data'}
            }
        ]

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_bulk_insert_polygon_news_success(self, db_config, sample_polygon_news_data):
        """Test successful bulk insert for Polygon news data."""
        with patch('src.market_data.news.turbo_news_backfill.asyncpg.create_pool') as mock_pool:
            # Mock the database pool and connection
            mock_conn = AsyncMock()
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aenter__.return_value = mock_conn
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aexit__.return_value = None
            mock_pool.return_value.__aenter__.return_value = mock_pool.return_value
            mock_pool.return_value.__aexit__.return_value = None
            
            inserter = TurboNewsDatabaseInserter(db_config)
            result = await inserter.bulk_insert_polygon_news(sample_polygon_news_data)
            
            assert result == 1
            assert mock_conn.executemany.called
            
            # Verify the SQL call structure
            call_args = mock_conn.executemany.call_args
            sql_query = call_args[0][0]
            records = call_args[0][1]
            
            assert "INSERT INTO dev_news_polygon" in sql_query
            assert "ON CONFLICT (polygon_id) DO NOTHING" in sql_query
            assert len(records) == 1
            assert records[0][0] == 'test-id-1'  # polygon_id

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_bulk_insert_tiingo_news_success(self, db_config, sample_tiingo_news_data):
        """Test successful bulk insert for Tiingo news data."""
        with patch('src.market_data.news.turbo_news_backfill.asyncpg.create_pool') as mock_pool:
            # Mock the database pool and connection
            mock_conn = AsyncMock()
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aenter__.return_value = mock_conn
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aexit__.return_value = None
            mock_pool.return_value.__aenter__.return_value = mock_pool.return_value
            mock_pool.return_value.__aexit__.return_value = None
            
            inserter = TurboNewsDatabaseInserter(db_config)
            result = await inserter.bulk_insert_tiingo_news(sample_tiingo_news_data)
            
            assert result == 1
            assert mock_conn.executemany.called
            
            # Verify the SQL call structure
            call_args = mock_conn.executemany.call_args
            sql_query = call_args[0][0]
            records = call_args[0][1]
            
            assert "INSERT INTO dev_news_tiingo" in sql_query
            assert "ON CONFLICT (tiingo_id) DO NOTHING" in sql_query
            assert len(records) == 1
            assert records[0][0] == 12345  # tiingo_id

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_bulk_insert_empty_data(self, db_config):
        """Test bulk insert with empty data."""
        inserter = TurboNewsDatabaseInserter(db_config)
        
        with patch('src.market_data.news.turbo_news_backfill.asyncpg.create_pool'):
            result_polygon = await inserter.bulk_insert_polygon_news([])
            result_tiingo = await inserter.bulk_insert_tiingo_news([])
            
            assert result_polygon == 0
            assert result_tiingo == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_bulk_insert_database_error(self, db_config, sample_polygon_news_data):
        """Test handling of database errors during insert."""
        with patch('src.market_data.news.turbo_news_backfill.asyncpg.create_pool') as mock_pool:
            # Mock database error
            mock_conn = AsyncMock()
            mock_conn.executemany.side_effect = Exception("Database error")
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aenter__.return_value = mock_conn
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aexit__.return_value = None
            mock_pool.return_value.__aenter__.return_value = mock_pool.return_value
            mock_pool.return_value.__aexit__.return_value = None
            
            inserter = TurboNewsDatabaseInserter(db_config)
            result = await inserter.bulk_insert_polygon_news(sample_polygon_news_data)
            
            assert result == 0  # Should return 0 on error

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_json_serialization(self, db_config, sample_polygon_news_data):
        """Test that complex data structures are properly JSON serialized."""
        with patch('src.market_data.news.turbo_news_backfill.asyncpg.create_pool') as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aenter__.return_value = mock_conn
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aexit__.return_value = None
            mock_pool.return_value.__aenter__.return_value = mock_pool.return_value
            mock_pool.return_value.__aexit__.return_value = None
            
            inserter = TurboNewsDatabaseInserter(db_config)
            await inserter.bulk_insert_polygon_news(sample_polygon_news_data)
            
            # Verify JSON serialization
            call_args = mock_conn.executemany.call_args
            records = call_args[0][1]
            
            # The insights field should be JSON serialized
            insights_json = records[0][13]  # insights is the 14th field (0-indexed)
            assert isinstance(insights_json, str)
            
            # Should be able to parse back to original structure
            parsed_insights = json.loads(insights_json)
            assert parsed_insights == [{'sentiment': 'positive'}]


class TestTurboNewsBackfillIntegration:
    """Integration tests for the complete turbo news backfill workflow."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_concurrent_news_api_calls(self):
        """Test that concurrent news API calls work properly."""
        with aioresponses.aioresponses() as m:
            # Mock multiple API responses for different symbols
            for symbol in ['AAPL', 'MSFT', 'GOOGL']:
                m.get(
                    'https://api.polygon.io/v2/reference/news',
                    payload=POLYGON_NEWS_SAMPLE_RESPONSE
                )
                m.get(
                    'https://api.tiingo.com/tiingo/news',
                    payload=TIINGO_NEWS_SAMPLE_RESPONSE
                )
            
            # Test concurrent fetching
            async with TurboPolygonNewsFetcher("test_api_key", max_concurrent=3) as polygon_fetcher:
                async with TurboTiingoNewsFetcher("test_api_key", max_concurrent=3) as tiingo_fetcher:
                    
                    # Create concurrent tasks
                    polygon_tasks = [
                        polygon_fetcher.fetch_news_for_symbol(
                            symbol, 
                            published_gte="2024-08-01", 
                            published_lte="2024-08-02"
                        )
                        for symbol in ['AAPL', 'MSFT', 'GOOGL']
                    ]
                    
                    tiingo_tasks = [
                        tiingo_fetcher.fetch_news_for_symbol(
                            symbol, 
                            start_date="2024-08-01", 
                            end_date="2024-08-02"
                        )
                        for symbol in ['AAPL', 'MSFT', 'GOOGL']
                    ]
                    
                    # Execute all tasks concurrently
                    polygon_results = await asyncio.gather(*polygon_tasks)
                    tiingo_results = await asyncio.gather(*tiingo_tasks)
                    
                    # Verify results
                    assert len(polygon_results) == 3
                    assert len(tiingo_results) == 3
                    
                    for result in polygon_results:
                        assert len(result) == 2  # 2 news articles
                        
                    for result in tiingo_results:
                        assert len(result) == 2  # 2 news articles

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_mixed_success_failure_batch(self):
        """Test handling of mixed success/failure in batch processing."""
        with aioresponses.aioresponses() as m:
            # AAPL succeeds
            m.get(
                'https://api.polygon.io/v2/reference/news',
                payload=POLYGON_NEWS_SAMPLE_RESPONSE
            )
            # MSFT fails with 404
            m.get(
                'https://api.polygon.io/v2/reference/news',
                status=404
            )
            # GOOGL succeeds
            m.get(
                'https://api.polygon.io/v2/reference/news',
                payload=POLYGON_NEWS_SAMPLE_RESPONSE
            )
            
            async with TurboPolygonNewsFetcher("test_api_key", max_concurrent=3) as fetcher:
                tasks = [
                    fetcher.fetch_news_for_symbol("AAPL"),
                    fetcher.fetch_news_for_symbol("MSFT"),
                    fetcher.fetch_news_for_symbol("GOOGL")
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Verify that successful results are returned and failed ones return empty lists
                assert len(results[0]) == 2  # AAPL success
                assert len(results[1]) == 0  # MSFT failed -> empty list
                assert len(results[2]) == 2  # GOOGL success

    def test_date_parsing_accuracy(self):
        """Test that date parsing from API responses is accurate."""
        # Test Polygon date parsing
        polygon_date_str = "2024-08-01T15:30:00Z"
        parsed_date = datetime.fromisoformat(polygon_date_str.replace('Z', '+00:00'))
        expected_date = datetime(2024, 8, 1, 15, 30, 0)
        
        # Compare without timezone info for simplicity
        assert parsed_date.replace(tzinfo=None) == expected_date
        
        # Test Tiingo date parsing
        tiingo_date_str = "2024-08-01T15:30:00.000Z"
        parsed_tiingo_date = datetime.fromisoformat(tiingo_date_str.replace('Z', '+00:00'))
        
        assert parsed_tiingo_date.replace(tzinfo=None) == expected_date

    def test_data_structure_validation(self):
        """Test that the data structures match expected database schema."""
        # Sample Polygon news item
        polygon_item = {
            "id": "test-id",
            "title": "Test Title",
            "description": "Test Description",
            "author": "Test Author",
            "published_utc": "2024-08-01T10:00:00Z",
            "article_url": "https://example.com",
            "image_url": "https://example.com/image.jpg",
            "publisher": {
                "name": "Test Publisher",
                "homepage_url": "https://publisher.com",
                "logo_url": "https://publisher.com/logo.png",
                "favicon_url": "https://publisher.com/favicon.ico"
            },
            "keywords": ["test", "keywords"],
            "tickers": ["AAPL", "MSFT"],
            "insights": [{"sentiment": "positive"}]
        }
        
        # Transform to our format (simulating what the fetcher does)
        transformed = {
            'polygon_id': polygon_item.get('id'),
            'title': polygon_item.get('title', ''),
            'description': polygon_item.get('description', ''),
            'author': polygon_item.get('author'),
            'published_utc': datetime.fromisoformat(polygon_item['published_utc'].replace('Z', '+00:00')),
            'article_url': polygon_item.get('article_url'),
            'image_url': polygon_item.get('image_url'),
            'publisher_name': polygon_item.get('publisher', {}).get('name'),
            'publisher_homepage_url': polygon_item.get('publisher', {}).get('homepage_url'),
            'publisher_logo_url': polygon_item.get('publisher', {}).get('logo_url'),
            'publisher_favicon_url': polygon_item.get('publisher', {}).get('favicon_url'),
            'keywords': polygon_item.get('keywords', []),
            'tickers': polygon_item.get('tickers', []),
            'insights': polygon_item.get('insights'),
            'data': polygon_item
        }
        
        # Verify all required fields are present and correctly typed
        assert transformed['polygon_id'] == "test-id"
        assert transformed['title'] == "Test Title"
        assert isinstance(transformed['published_utc'], datetime)
        assert isinstance(transformed['keywords'], list)
        assert isinstance(transformed['tickers'], list)
        assert isinstance(transformed['data'], dict)