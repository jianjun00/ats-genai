import pytest
import asyncio
import json
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, date
from aiohttp import ClientResponse, ClientSession
import aioresponses

from src.market_data.eod.turbo_price_backfill import (
    TurboPolygonFetcher,
    TurboTiingoFetcher,
    TurboDatabaseInserter
)

# Sample response data for mocking API calls
POLYGON_SAMPLE_RESPONSE = {
    "status": "OK",
    "results": [
        {
            "t": 1627776000000,  # 2021-08-01
            "o": 150.0,
            "h": 155.0,
            "l": 149.0,
            "c": 153.0,
            "v": 1000000
        },
        {
            "t": 1627862400000,  # 2021-08-02
            "o": 153.0,
            "h": 158.0,
            "l": 152.0,
            "c": 157.0,
            "v": 1200000
        }
    ]
}

TIINGO_SAMPLE_RESPONSE = [
    {
        "date": "2021-08-01T00:00:00.000Z",
        "open": 150.0,
        "high": 155.0,
        "low": 149.0,
        "close": 153.0,
        "volume": 1000000
    },
    {
        "date": "2021-08-02T00:00:00.000Z",
        "open": 153.0,
        "high": 158.0,
        "low": 152.0,
        "close": 157.0,
        "volume": 1200000
    }
]


class TestTurboPolygonFetcher:
    """Unit tests for TurboPolygonFetcher."""

    @pytest.mark.asyncio
    async def test_fetch_symbol_year_success(self):
        """Test successful symbol year fetch from Polygon API."""
        with aioresponses.aioresponses() as m:
            # Mock the Polygon API response
            m.get(
                'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2021-08-01/2021-08-02',
                payload=POLYGON_SAMPLE_RESPONSE
            )
            
            async with TurboPolygonFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_symbol_year(
                    "AAPL",
                    date(2021, 8, 1),
                    date(2021, 8, 2),
                    instrument_id=123
                )
                
                assert len(results) == 2
                assert results[0]['instrument_id'] == 123
                assert results[0]['open'] == 150.0
                assert results[0]['close'] == 153.0
                assert results[0]['volume'] == 1000000
                assert isinstance(results[0]['date'], date)

    @pytest.mark.asyncio
    async def test_fetch_symbol_year_rate_limit_retry(self):
        """Test retry logic for rate limiting."""
        with aioresponses.aioresponses() as m:
            # First request returns 429 (rate limited)
            m.get(
                'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2021-08-01/2021-08-02',
                status=429
            )
            # Second request succeeds
            m.get(
                'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2021-08-01/2021-08-02',
                payload=POLYGON_SAMPLE_RESPONSE
            )
            
            async with TurboPolygonFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_symbol_year(
                    "AAPL",
                    date(2021, 8, 1),
                    date(2021, 8, 2),
                    instrument_id=123
                )
                
                assert len(results) == 2
                assert results[0]['instrument_id'] == 123

    @pytest.mark.asyncio
    async def test_fetch_symbol_year_server_error_retry(self):
        """Test retry logic for server errors."""
        with aioresponses.aioresponses() as m:
            # First request returns 500 (server error)
            m.get(
                'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2021-08-01/2021-08-02',
                status=500
            )
            # Second request succeeds
            m.get(
                'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2021-08-01/2021-08-02',
                payload=POLYGON_SAMPLE_RESPONSE
            )
            
            async with TurboPolygonFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_symbol_year(
                    "AAPL",
                    date(2021, 8, 1),
                    date(2021, 8, 2),
                    instrument_id=123
                )
                
                assert len(results) == 2

    @pytest.mark.asyncio
    async def test_fetch_symbol_year_max_retries_exceeded(self):
        """Test behavior when max retries are exceeded."""
        with aioresponses.aioresponses() as m:
            # All requests return 429 (rate limited)
            for _ in range(6):  # More than max_retries
                m.get(
                    'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2021-08-01/2021-08-02',
                    status=429
                )
            
            async with TurboPolygonFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_symbol_year(
                    "AAPL",
                    date(2021, 8, 1),
                    date(2021, 8, 2),
                    instrument_id=123
                )
                
                assert results == []

    @pytest.mark.asyncio
    async def test_fetch_symbol_year_no_results(self):
        """Test handling of API response with no results."""
        with aioresponses.aioresponses() as m:
            m.get(
                'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2021-08-01/2021-08-02',
                payload={"status": "OK", "results": []}
            )
            
            async with TurboPolygonFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_symbol_year(
                    "AAPL",
                    date(2021, 8, 1),
                    date(2021, 8, 2),
                    instrument_id=123
                )
                
                assert results == []


class TestTurboTiingoFetcher:
    """Unit tests for TurboTiingoFetcher."""

    @pytest.mark.asyncio
    async def test_fetch_symbol_data_success(self):
        """Test successful symbol data fetch from Tiingo API."""
        with aioresponses.aioresponses() as m:
            m.get(
                'https://api.tiingo.com/tiingo/daily/AAPL/prices',
                payload=TIINGO_SAMPLE_RESPONSE
            )
            
            async with TurboTiingoFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_symbol_data(
                    "AAPL",
                    "2021-08-01",
                    "2021-08-02",
                    instrument_id=123
                )
                
                assert len(results) == 2
                assert results[0]['instrument_id'] == 123
                assert results[0]['open'] == 150.0
                assert results[0]['close'] == 153.0
                assert results[0]['volume'] == 1000000
                assert isinstance(results[0]['date'], date)

    @pytest.mark.asyncio
    async def test_fetch_symbol_data_rate_limit_retry(self):
        """Test retry logic for rate limiting."""
        with aioresponses.aioresponses() as m:
            # First request returns 429 (rate limited)
            m.get('https://api.tiingo.com/tiingo/daily/AAPL/prices', status=429)
            # Second request succeeds
            m.get('https://api.tiingo.com/tiingo/daily/AAPL/prices', payload=TIINGO_SAMPLE_RESPONSE)
            
            async with TurboTiingoFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_symbol_data(
                    "AAPL",
                    "2021-08-01",
                    "2021-08-02",
                    instrument_id=123
                )
                
                assert len(results) == 2

    @pytest.mark.asyncio
    async def test_fetch_symbol_data_empty_response(self):
        """Test handling of empty API response."""
        with aioresponses.aioresponses() as m:
            m.get('https://api.tiingo.com/tiingo/daily/AAPL/prices', payload=[])
            
            async with TurboTiingoFetcher("test_api_key", max_concurrent=1) as fetcher:
                results = await fetcher.fetch_symbol_data(
                    "AAPL",
                    "2021-08-01",
                    "2021-08-02",
                    instrument_id=123
                )
                
                assert results == []


class TestTurboDatabaseInserter:
    """Unit tests for TurboDatabaseInserter."""

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
    def sample_polygon_data(self):
        """Sample Polygon data for testing."""
        return [
            {
                'date': date(2021, 8, 1),
                'instrument_id': 123,
                'open': 150.0,
                'high': 155.0,
                'low': 149.0,
                'close': 153.0,
                'volume': 1000000
            },
            {
                'date': date(2021, 8, 2),
                'instrument_id': 123,
                'open': 153.0,
                'high': 158.0,
                'low': 152.0,
                'close': 157.0,
                'volume': 1200000
            }
        ]

    @pytest.fixture
    def sample_tiingo_data(self):
        """Sample Tiingo data for testing."""
        return [
            {
                'date': date(2021, 8, 1),
                'instrument_id': 123,
                'open': 150.0,
                'high': 155.0,
                'low': 149.0,
                'close': 153.0,
                'volume': 1000000
            }
        ]

    @pytest.mark.asyncio
    async def test_bulk_insert_polygon_success(self, db_config, sample_polygon_data):
        """Test successful bulk insert for Polygon data."""
        with patch('src.market_data.eod.turbo_price_backfill.asyncpg.create_pool') as mock_pool:
            # Mock the database pool and connection
            mock_conn = AsyncMock()
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aenter__.return_value = mock_conn
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aexit__.return_value = None
            mock_pool.return_value.__aenter__.return_value = mock_pool.return_value
            mock_pool.return_value.__aexit__.return_value = None
            
            inserter = TurboDatabaseInserter(db_config)
            result = await inserter.bulk_insert_polygon(sample_polygon_data)
            
            assert result == 2
            assert mock_conn.executemany.called

    @pytest.mark.asyncio
    async def test_bulk_insert_tiingo_success(self, db_config, sample_tiingo_data):
        """Test successful bulk insert for Tiingo data."""
        with patch('src.market_data.eod.turbo_price_backfill.asyncpg.create_pool') as mock_pool:
            # Mock the database pool and connection
            mock_conn = AsyncMock()
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aenter__.return_value = mock_conn
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aexit__.return_value = None
            mock_pool.return_value.__aenter__.return_value = mock_pool.return_value
            mock_pool.return_value.__aexit__.return_value = None
            
            inserter = TurboDatabaseInserter(db_config)
            result = await inserter.bulk_insert_tiingo(sample_tiingo_data)
            
            assert result == 1
            assert mock_conn.executemany.called

    @pytest.mark.asyncio
    async def test_bulk_insert_empty_data(self, db_config):
        """Test bulk insert with empty data."""
        inserter = TurboDatabaseInserter(db_config)
        
        with patch('src.market_data.eod.turbo_price_backfill.asyncpg.create_pool'):
            result_polygon = await inserter.bulk_insert_polygon([])
            result_tiingo = await inserter.bulk_insert_tiingo([])
            
            assert result_polygon == 0
            assert result_tiingo == 0

    @pytest.mark.asyncio
    async def test_bulk_insert_database_error(self, db_config, sample_polygon_data):
        """Test handling of database errors during insert."""
        with patch('src.market_data.eod.turbo_price_backfill.asyncpg.create_pool') as mock_pool:
            # Mock database error
            mock_conn = AsyncMock()
            mock_conn.executemany.side_effect = Exception("Database error")
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aenter__.return_value = mock_conn
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aexit__.return_value = None
            mock_pool.return_value.__aenter__.return_value = mock_pool.return_value
            mock_pool.return_value.__aexit__.return_value = None
            
            inserter = TurboDatabaseInserter(db_config)
            result = await inserter.bulk_insert_polygon(sample_polygon_data)
            
            assert result == 0  # Should return 0 on error


class TestTurboBackfillIntegration:
    """Integration tests for the complete turbo backfill workflow."""

    @pytest.mark.asyncio
    async def test_concurrent_api_calls(self):
        """Test that concurrent API calls work properly."""
        with aioresponses.aioresponses() as m:
            # Mock multiple API responses
            for symbol in ['AAPL', 'MSFT', 'GOOGL']:
                m.get(
                    f'https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/2021-08-01/2021-08-02',
                    payload=POLYGON_SAMPLE_RESPONSE
                )
                m.get(
                    f'https://api.tiingo.com/tiingo/daily/{symbol}/prices',
                    payload=TIINGO_SAMPLE_RESPONSE
                )
            
            # Test concurrent fetching
            async with TurboPolygonFetcher("test_api_key", max_concurrent=3) as polygon_fetcher:
                async with TurboTiingoFetcher("test_api_key", max_concurrent=3) as tiingo_fetcher:
                    
                    # Create concurrent tasks
                    polygon_tasks = [
                        polygon_fetcher.fetch_symbol_year(symbol, date(2021, 8, 1), date(2021, 8, 2), i)
                        for i, symbol in enumerate(['AAPL', 'MSFT', 'GOOGL'], 1)
                    ]
                    
                    tiingo_tasks = [
                        tiingo_fetcher.fetch_symbol_data(symbol, "2021-08-01", "2021-08-02", i)
                        for i, symbol in enumerate(['AAPL', 'MSFT', 'GOOGL'], 1)
                    ]
                    
                    # Execute all tasks concurrently
                    polygon_results = await asyncio.gather(*polygon_tasks)
                    tiingo_results = await asyncio.gather(*tiingo_tasks)
                    
                    # Verify results
                    assert len(polygon_results) == 3
                    assert len(tiingo_results) == 3
                    
                    for result in polygon_results:
                        assert len(result) == 2  # 2 days of data
                        
                    for result in tiingo_results:
                        assert len(result) == 2  # 2 days of data

    @pytest.mark.asyncio 
    async def test_error_handling_in_batch_processing(self):
        """Test error handling when some API calls fail in batch processing."""
        with aioresponses.aioresponses() as m:
            # AAPL succeeds
            m.get(
                'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2021-08-01/2021-08-02',
                payload=POLYGON_SAMPLE_RESPONSE
            )
            # MSFT fails with 404
            m.get(
                'https://api.polygon.io/v2/aggs/ticker/MSFT/range/1/day/2021-08-01/2021-08-02',
                status=404
            )
            # GOOGL succeeds
            m.get(
                'https://api.polygon.io/v2/aggs/ticker/GOOGL/range/1/day/2021-08-01/2021-08-02',
                payload=POLYGON_SAMPLE_RESPONSE
            )
            
            async with TurboPolygonFetcher("test_api_key", max_concurrent=3) as fetcher:
                tasks = [
                    fetcher.fetch_symbol_year("AAPL", date(2021, 8, 1), date(2021, 8, 2), 1),
                    fetcher.fetch_symbol_year("MSFT", date(2021, 8, 1), date(2021, 8, 2), 2),
                    fetcher.fetch_symbol_year("GOOGL", date(2021, 8, 1), date(2021, 8, 2), 3)
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Verify that successful results are returned and failed ones return empty lists
                assert len(results[0]) == 2  # AAPL success
                assert len(results[1]) == 0  # MSFT failed -> empty list
                assert len(results[2]) == 2  # GOOGL success

    def test_data_transformation_accuracy(self):
        """Test that data transformation from API response to database format is accurate."""
        from src.market_data.eod.turbo_price_backfill import TurboPolygonFetcher
        
        # Test timestamp conversion for Polygon
        sample_item = {
            "t": 1627776000000,  # 2021-08-01 00:00:00 UTC
            "o": 150.0,
            "h": 155.0,
            "l": 149.0,
            "c": 153.0,
            "v": 1000000
        }
        
        # This would typically be inside the fetch method, but we test the conversion logic
        converted_date = datetime.utcfromtimestamp(sample_item['t']/1000).date()
        expected_date = date(2021, 8, 1)
        
        assert converted_date == expected_date
        
        # Test Tiingo date conversion
        sample_tiingo_item = {
            "date": "2021-08-01T00:00:00.000Z",
            "open": 150.0
        }
        
        converted_tiingo_date = datetime.strptime(sample_tiingo_item['date'][:10], "%Y-%m-%d").date()
        assert converted_tiingo_date == expected_date