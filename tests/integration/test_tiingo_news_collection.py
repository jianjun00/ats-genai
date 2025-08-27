#!/usr/bin/env python3
"""
Integration tests for Tiingo news collection

Tests the complete Tiingo news collection pipeline including:
- API endpoint validation
- News data structure handling
- Database schema creation and insertion
- ID conversion (integer to string)
- Error handling and rate limiting
"""

import pytest
import asyncio
import os
import sys
from datetime import datetime, date, timedelta
from unittest.mock import patch, AsyncMock, MagicMock

sys.path.append('/workspace/src')

# Import the class we're testing
import importlib.util
spec = importlib.util.spec_from_file_location(
    "tiingo_news", 
    "/workspace/scripts/tiingo_30_year_news_backfill.py"
)
tiingo_news = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tiingo_news)

class TestTiingoNewsCollection:
    """Test Tiingo news collection functionality."""
    
    @pytest.fixture
    def collector(self):
        """Create a TiingoNewsCollector instance for testing."""
        return tiingo_news.TiingoNewsCollector("test_api_key")
    
    @pytest.fixture 
    def mock_db_pool(self):
        """Mock database connection pool."""
        pool = AsyncMock()
        conn = AsyncMock()
        pool.acquire.return_value.__aenter__.return_value = conn
        
        conn.execute = AsyncMock()
        conn.executemany = AsyncMock()
        conn.fetchval = AsyncMock()
        conn.fetchrow = AsyncMock()
        conn.fetch = AsyncMock()
        
        return pool
    
    @pytest.fixture
    def mock_session(self):
        """Mock aiohttp session for API calls."""
        session = AsyncMock()
        response = AsyncMock()
        session.get.return_value.__aenter__.return_value = response
        return session, response
    
    def test_collector_initialization(self, collector):
        """Test collector initializes with correct parameters."""
        assert collector.api_key == "test_api_key"
        assert collector.start_time is not None
        assert collector.total_articles_collected == 0
        assert collector.total_articles_inserted == 0
    
    async def test_news_api_call_structure(self, collector):
        """Test that news API calls are structured correctly."""
        collector.session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = [
            {
                'id': 83408655,
                'publishedDate': '2024-08-27T12:00:00Z',
                'title': 'Apple Stock Analysis',
                'description': 'Detailed analysis of Apple stock performance',
                'url': 'https://example.com/news/1',
                'source': 'Financial News',
                'tickers': ['AAPL'],
                'tags': ['technology', 'earnings']
            }
        ]
        collector.session.get.return_value.__aenter__.return_value = mock_response
        
        symbol = "AAPL"
        year = 2024
        
        result = await collector.fetch_news_for_symbol_year(symbol, year)
        
        # Verify API call structure
        collector.session.get.assert_called_once()
        call_args = collector.session.get.call_args
        
        assert call_args[0][0] == "https://api.tiingo.com/tiingo/news"
        params = call_args[1]['params']
        assert params['tickers'] == symbol
        assert params['startDate'] == '2024-01-01'
        assert params['endDate'] == '2024-12-31'
        assert params['token'] == "test_api_key"
        assert params['limit'] == 1000
        
        # Verify response processing
        assert len(result) == 1
        article = result[0]
        assert article['tiingo_id'] == '83408655'  # Should be converted to string
        assert article['title'] == 'Apple Stock Analysis'
        assert article['tickers'] == ['AAPL']
    
    def test_article_standardization(self, collector):
        """Test that Tiingo articles are properly standardized."""
        raw_article = {
            'id': 83408655,  # Integer ID from Tiingo
            'publishedDate': '2024-08-27T12:00:00Z',
            'title': 'Apple Stock Analysis',
            'description': 'Detailed analysis of Apple stock performance',
            'url': 'https://example.com/news/1',
            'source': 'Financial News',
            'tickers': ['AAPL'],
            'tags': ['technology', 'earnings'],
            'author': 'John Analyst'
        }
        
        result = collector.standardize_tiingo_article(raw_article)
        
        # Verify standardization
        assert result['tiingo_id'] == '83408655'  # Converted to string
        assert result['title'] == 'Apple Stock Analysis'
        assert result['description'] == 'Detailed analysis of Apple stock performance'
        assert result['author'] == 'John Analyst'
        assert result['article_url'] == 'https://example.com/news/1'
        assert result['source'] == 'Financial News'
        assert result['tickers'] == ['AAPL']
        assert result['tags'] == ['technology', 'earnings']
        assert result['data'] == raw_article  # Original data preserved
        
        # Verify date parsing
        assert result['published_utc'] is not None
        assert isinstance(result['published_utc'], datetime)
    
    def test_id_conversion_critical_fix(self, collector):
        """Test the critical ID conversion fix (integer to string)."""
        # Test various ID formats that could come from Tiingo
        test_cases = [
            {'id': 83408655, 'expected': '83408655'},
            {'id': '83408655', 'expected': '83408655'}, 
            {'id': None, 'expected': ''},
            {'id': 0, 'expected': '0'},
        ]
        
        for test_case in test_cases:
            article = {
                'id': test_case['id'],
                'publishedDate': '2024-08-27T12:00:00Z',
                'title': 'Test Article',
                'url': 'https://example.com'
            }
            
            result = collector.standardize_tiingo_article(article)
            assert result['tiingo_id'] == test_case['expected']
            assert isinstance(result['tiingo_id'], str)
    
    def test_date_parsing_robustness(self, collector):
        """Test robust date parsing for various formats."""
        test_dates = [
            ('2024-08-27T12:00:00Z', datetime(2024, 8, 27, 12, 0, 0)),
            ('2024-08-27T12:00:00+00:00', datetime(2024, 8, 27, 12, 0, 0)),
            ('invalid_date', None),
            (None, None),
            ('', None)
        ]
        
        for input_date, expected in test_dates:
            article = {
                'id': 123,
                'publishedDate': input_date,
                'title': 'Test Article'
            }
            
            result = collector.standardize_tiingo_article(article)
            
            if expected is None:
                assert result['published_utc'] is None
            else:
                assert result['published_utc'] is not None
                # Check year/month/day (ignore timezone complexities in test)
                assert result['published_utc'].year == expected.year
                assert result['published_utc'].month == expected.month
                assert result['published_utc'].day == expected.day
    
    async def test_table_creation(self, collector, mock_db_pool):
        """Test that news table is created with correct schema."""
        conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        
        await collector.ensure_tiingo_news_table(mock_db_pool)
        
        # Verify table creation was called
        conn.execute.assert_called()
        
        create_table_sql = conn.execute.call_args[0][0]
        
        # Verify table structure
        assert "CREATE TABLE IF NOT EXISTS dev_news_tiingo" in create_table_sql
        assert "tiingo_id character varying(255) NOT NULL" in create_table_sql or "tiingo_id CHARACTER VARYING NOT NULL" in create_table_sql
        assert "title TEXT NOT NULL" in create_table_sql
        assert "published_date TIMESTAMP WITH TIME ZONE" in create_table_sql
        assert "tickers ARRAY" in create_table_sql
        assert "tags ARRAY" in create_table_sql
        assert "data JSONB" in create_table_sql
        assert "PRIMARY KEY (tiingo_id)" in create_table_sql
    
    async def test_article_insertion(self, collector, mock_db_pool):
        """Test news article database insertion."""
        conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        
        articles = [
            {
                'tiingo_id': '83408655',
                'title': 'Apple Stock Analysis',
                'description': 'Detailed analysis',
                'author': 'John Analyst',
                'published_utc': datetime(2024, 8, 27, 12, 0, 0),
                'article_url': 'https://example.com/1',
                'image_url': 'https://example.com/image1.jpg',
                'source': 'Financial News',
                'tickers': ['AAPL'],
                'tags': ['technology'],
                'data': {'original': 'data'}
            },
            {
                'tiingo_id': '83408656',
                'title': 'Microsoft Earnings',
                'description': 'Q3 earnings report',
                'author': 'Jane Reporter', 
                'published_utc': datetime(2024, 8, 27, 14, 0, 0),
                'article_url': 'https://example.com/2',
                'image_url': None,
                'source': 'Tech Daily',
                'tickers': ['MSFT'],
                'tags': ['earnings', 'microsoft'],
                'data': {'original': 'data2'}
            }
        ]
        
        result = await collector.insert_tiingo_news_articles(mock_db_pool, articles)
        
        # Should return count of successfully inserted articles
        assert result == 2
        
        # Verify database insertion calls
        assert conn.execute.call_count >= 2  # At least one per article
        
        # Check that all articles were processed
        insertion_calls = [call for call in conn.execute.call_args_list 
                          if 'INSERT INTO dev_news_tiingo' in str(call)]
        assert len(insertion_calls) >= 2
    
    async def test_insertion_idempotency(self, collector, mock_db_pool):
        """Test that insertions are idempotent (ON CONFLICT handling)."""
        conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        
        article = [{
            'tiingo_id': '83408655',
            'title': 'Apple Stock Analysis',
            'description': 'Detailed analysis',
            'author': 'John Analyst',
            'published_utc': datetime(2024, 8, 27, 12, 0, 0),
            'article_url': 'https://example.com/1',
            'source': 'Financial News',
            'tickers': ['AAPL'],
            'tags': ['technology'],
            'data': {}
        }]
        
        # Insert twice to test idempotency
        result1 = await collector.insert_tiingo_news_articles(mock_db_pool, article)
        result2 = await collector.insert_tiingo_news_articles(mock_db_pool, article)
        
        # Both should succeed
        assert result1 == 1
        assert result2 == 1
        
        # Check SQL contains ON CONFLICT clause
        insert_calls = [call for call in conn.execute.call_args_list 
                       if 'INSERT INTO dev_news_tiingo' in str(call)]
        
        for call in insert_calls:
            sql = call[0][0]
            assert "ON CONFLICT (tiingo_id) DO UPDATE SET" in sql
    
    async def test_api_error_handling(self, collector):
        """Test API error handling for various scenarios."""
        collector.session = AsyncMock()
        
        # Test different error scenarios
        error_scenarios = [
            (404, []),  # Not found -> empty list
            (429, []),  # Rate limited -> empty list after retries
            (500, []),  # Server error -> empty list after retries
        ]
        
        for status_code, expected_result in error_scenarios:
            mock_response = AsyncMock()
            mock_response.status = status_code
            collector.session.get.return_value.__aenter__.return_value = mock_response
            
            result = await collector.fetch_news_for_symbol_year("AAPL", 2024)
            assert result == expected_result
    
    async def test_rate_limiting(self, collector, mock_db_pool):
        """Test that rate limiting delays are applied."""
        # Mock successful API calls
        collector.session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = []
        collector.session.get.return_value.__aenter__.return_value = mock_response
        
        symbols = ["AAPL", "MSFT"]
        year = 2024
        
        start_time = datetime.now()
        
        with patch('asyncio.sleep') as mock_sleep:
            await collector.process_symbol_year_batch(mock_db_pool, symbols, year)
            
            # Should have rate limiting delays
            assert mock_sleep.call_count >= len(symbols)  # At least one delay per symbol
            
            # Check delay duration (2 seconds as per implementation)
            for call in mock_sleep.call_args_list:
                delay = call[0][0]
                assert delay >= 2.0  # At least 2 seconds
    
    async def test_statistics_tracking(self, collector, mock_db_pool):
        """Test that collection statistics are properly tracked."""
        # Mock API returning articles
        collector.session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = [
            {'id': 1, 'title': 'Article 1', 'publishedDate': '2024-01-01T12:00:00Z'},
            {'id': 2, 'title': 'Article 2', 'publishedDate': '2024-01-02T12:00:00Z'}
        ]
        collector.session.get.return_value.__aenter__.return_value = mock_response
        
        # Mock successful insertion
        with patch.object(collector, 'insert_tiingo_news_articles', return_value=2):
            symbols = ["AAPL"]
            year = 2024
            
            initial_collected = collector.total_articles_collected
            initial_inserted = collector.total_articles_inserted
            
            results = await collector.process_symbol_year_batch(mock_db_pool, symbols, year)
            
            # Verify statistics were updated
            assert collector.total_articles_collected > initial_collected
            assert collector.total_articles_inserted > initial_inserted
            
            # Verify results structure
            assert 'total_articles' in results
            assert 'total_inserted' in results
            assert 'symbols_processed' in results
            assert 'errors' in results
    
    async def test_empty_response_handling(self, collector, mock_db_pool):
        """Test handling of empty API responses."""
        collector.session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = []  # Empty response
        collector.session.get.return_value.__aenter__.return_value = mock_response
        
        result = await collector.fetch_news_for_symbol_year("AAPL", 2024)
        assert result == []
        
        # Test insertion of empty list
        inserted = await collector.insert_tiingo_news_articles(mock_db_pool, [])
        assert inserted == 0

@pytest.mark.asyncio 
class TestTiingoNewsIntegration:
    """Integration tests for Tiingo news collection."""
    
    async def test_end_to_end_collection_flow(self):
        """Test complete news collection workflow."""
        collector = tiingo_news.TiingoNewsCollector("test_api_key")
        
        # Mock database pool
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Mock API session
        collector.session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = [
            {
                'id': 12345,
                'publishedDate': '2024-08-27T12:00:00Z',
                'title': 'Test News Article',
                'description': 'Test description',
                'url': 'https://example.com/news',
                'source': 'Test Source',
                'tickers': ['AAPL'],
                'tags': ['technology']
            }
        ]
        collector.session.get.return_value.__aenter__.return_value = mock_response
        
        # Run collection for single symbol/year
        symbols = ["AAPL"]
        year = 2024
        
        results = await collector.process_symbol_year_batch(mock_pool, symbols, year)
        
        # Verify end-to-end flow worked
        assert results['total_articles'] == 1
        assert results['symbols_processed'] == 1
        assert results['errors'] == 0
        
        # Verify API was called
        collector.session.get.assert_called_once()
        
        # Verify database insertion attempt
        mock_conn.execute.assert_called()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])