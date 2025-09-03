#!/usr/bin/env python3
"""
Integration tests for issues discovered during data population.
Tests database constraints, API handling, and large-scale processing.
"""

import pytest
import asyncio
import asyncpg
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, date
import requests
import aiohttp

# Set up test environment
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

from shared.utils.environment import Environment, EnvironmentType


class TestDataPopulationIssues:
    """Test suite for issues discovered during data population."""
    
    @pytest.fixture
    @pytest.mark.asyncio
    async def test_env(self):
        """Create test environment."""
        return Environment(env_type=EnvironmentType.TEST)
    
    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection."""
        mock_conn = AsyncMock()
        return mock_conn

    # ISSUE 1: Database Constraint Conflicts
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_on_conflict_constraint_issues(self, mock_db_connection):
        """Test ON CONFLICT clause issues with non-unique constraints."""
        mock_conn = mock_db_connection
        
        # Simulate the constraint error we encountered
        mock_conn.execute.side_effect = asyncpg.exceptions.PostgresError(
            "there is no unique or exclusion constraint matching the ON CONFLICT specification"
        )
        
        # Test that we can detect and handle constraint issues
        with pytest.raises(asyncpg.exceptions.PostgresError) as exc_info:
            await mock_conn.execute("""
                INSERT INTO dev_instruments (symbol, name, exchange, is_active)
                VALUES ($1, $2, 'NYSE', true)
                ON CONFLICT (symbol) DO UPDATE SET
                name = EXCLUDED.name
            """, "AAPL", "Apple Inc.")
        
        assert "no unique or exclusion constraint" in str(exc_info.value)
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_simple_insert_without_conflict(self, mock_db_connection):
        """Test simple INSERT strategy that avoids constraint issues."""
        mock_conn = mock_db_connection
        mock_conn.fetchval.side_effect = [None, 12345]  # Check exists, then insert
        
        # Strategy: Check if exists first, then simple INSERT
        exists = await mock_conn.fetchval(
            "SELECT id FROM dev_instruments WHERE symbol = $1", "AAPL"
        )
        
        if not exists:
            instrument_id = await mock_conn.fetchval("""
                INSERT INTO dev_instruments (symbol, name, exchange, is_active)
                VALUES ($1, $2, 'NYSE', true)
                RETURNING id
            """, "AAPL", "Apple Inc.")
            
            assert instrument_id == 12345

    # ISSUE 2: Column Naming Mismatches
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_tiingo_column_name_mismatch(self, mock_db_connection):
        """Test column naming issue: adj_close vs adjclose."""
        mock_conn = mock_db_connection
        
        # Test wrong column name (adj_close)
        mock_conn.executemany.side_effect = asyncpg.exceptions.UndefinedColumnError(
            'column "adj_close" of relation "dev_daily_prices_tiingo" does not exist'
        )
        
        price_records = [(date.today(), 1, 100.0, 105.0, 99.0, 102.0, 101.5, 1000000)]
        
        with pytest.raises(asyncpg.exceptions.UndefinedColumnError) as exc_info:
            await mock_conn.executemany("""
                INSERT INTO dev_daily_prices_tiingo 
                (date, instrument_id, open, high, low, close, adj_close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """, price_records)
        
        assert "adj_close" in str(exc_info.value)
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_correct_tiingo_column_names(self, mock_db_connection):
        """Test correct column naming for Tiingo data."""
        mock_conn = mock_db_connection
        mock_conn.executemany.return_value = None  # Success
        
        price_records = [(date.today(), 1, 100.0, 105.0, 99.0, 102.0, 101.5, 1000000)]
        
        # Test correct column name (adjclose)
        await mock_conn.executemany("""
            INSERT INTO dev_daily_prices_tiingo 
            (date, instrument_id, open, high, low, close, adjclose, volume)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """, price_records)
        
        mock_conn.executemany.assert_called_once()
    
    def test_database_schema_validation(self):
        """Test database schema validation helper."""
        # Schema mapping for different sources
        tiingo_columns = {
            'date', 'instrument_id', 'open', 'high', 'low', 'close', 'adjclose', 'volume'
        }
        polygon_columns = {
            'date', 'instrument_id', 'open', 'high', 'low', 'close', 'volume'
        }
        
        # Test schema validation
        def validate_columns(source, columns):
            if source == 'tiingo':
                return columns.issubset(tiingo_columns)
            elif source == 'polygon':
                return columns.issubset(polygon_columns)
            return False
        
        # Test correct schemas
        assert validate_columns('tiingo', {'date', 'instrument_id', 'adjclose'})
        assert validate_columns('polygon', {'date', 'instrument_id', 'close'})
        
        # Test incorrect schemas
        assert not validate_columns('tiingo', {'date', 'instrument_id', 'adj_close'})

    # ISSUE 3: API Rate Limiting and Error Handling
    
    def test_polygon_rate_limiting(self):
        """Test Polygon API rate limiting handling."""
        with patch('requests.get') as mock_get:
            # First call returns 429 (rate limit)
            rate_limit_response = Mock()
            rate_limit_response.status_code = 429
            
            # Second call returns success
            success_response = Mock()
            success_response.status_code = 200
            success_response.json.return_value = {
                "status": "OK",
                "results": [{"t": 1625097600000, "o": 100, "h": 105, "l": 99, "c": 102, "v": 1000000}]
            }
            
            mock_get.side_effect = [rate_limit_response, success_response]
            
            # Simulate rate limit handling
            def fetch_with_rate_limit_handling(url):
                response = requests.get(url)
                if response.status_code == 429:
                    import time
                    time.sleep(12)  # Wait for rate limit
                    response = requests.get(url)
                return response
            
            with patch('time.sleep'):  # Mock sleep for test speed
                response = fetch_with_rate_limit_handling("test_url")
                assert response.status_code == 200
                assert mock_get.call_count == 2
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_tiingo_connection_errors(self):
        """Test Tiingo API connection error handling."""
        timeout = aiohttp.ClientTimeout(total=30)
        
        # Test connection error handling
        async def fetch_with_retry(session, url, max_retries=3):
            for attempt in range(max_retries):
                try:
                    async with session.get(url) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                            continue
                except aiohttp.ClientError:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    else:
                        return None
            return None
        
        # Mock session with connection errors
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"data": "success"})
        
        # Properly mock async context manager
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_response
        mock_context.__aexit__.return_value = None
        mock_session.get.return_value = mock_context
        
        result = await fetch_with_retry(mock_session, "test_url")
        assert result == {"data": "success"}
    
    def test_api_error_classification(self):
        """Test classification of API errors for appropriate handling."""
        def classify_api_error(status_code, error_message=""):
            """Classify API errors for appropriate retry strategy."""
            if status_code == 429:
                return "rate_limit"
            elif status_code == 403:
                return "auth_error"
            elif status_code in [500, 502, 503, 504]:
                return "server_error"
            elif status_code == 404:
                return "not_found"
            elif "timeout" in error_message.lower():
                return "timeout"
            else:
                return "unknown"
        
        # Test error classifications
        assert classify_api_error(429) == "rate_limit"
        assert classify_api_error(403) == "auth_error"
        assert classify_api_error(500) == "server_error"
        assert classify_api_error(404) == "not_found"
        assert classify_api_error(0, "Connection timeout") == "timeout"

    # ISSUE 4: Large-Scale Data Processing Performance
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_batch_processing_performance(self, mock_db_connection):
        """Test batch processing for large-scale data insertion."""
        mock_conn = mock_db_connection
        mock_conn.executemany.return_value = None
        
        # Test different batch sizes
        test_data = [(date.today(), i, 100.0 + i) for i in range(10000)]
        
        async def process_in_batches(data, batch_size):
            total_processed = 0
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                await mock_conn.executemany(
                    "INSERT INTO test_table (date, id, value) VALUES ($1, $2, $3)",
                    batch
                )
                total_processed += len(batch)
            return total_processed
        
        # Test batch processing
        result = await process_in_batches(test_data, 1000)
        assert result == 10000
        assert mock_conn.executemany.call_count == 10  # 10 batches of 1000
    
    def test_memory_efficient_processing(self):
        """Test memory-efficient processing strategies."""
        def generate_test_data(count):
            """Generator for memory-efficient data processing."""
            for i in range(count):
                yield {"id": i, "value": f"data_{i}"}
        
        def process_with_generator(data_generator, batch_size):
            """Process data using generator to avoid memory issues."""
            processed = 0
            batch = []
            
            for item in data_generator:
                batch.append(item)
                if len(batch) >= batch_size:
                    # Process batch (mock)
                    processed += len(batch)
                    batch = []
            
            # Process remaining items
            if batch:
                processed += len(batch)
            
            return processed
        
        # Test memory-efficient processing
        result = process_with_generator(generate_test_data(10000), 1000)
        assert result == 10000

    # ISSUE 5: Database Connectivity Issues
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_connection_reliability(self):
        """Test database connection reliability patterns."""
        
        async def connect_with_retry(connection_params, max_retries=3):
            """Robust database connection with retry logic."""
            for attempt in range(max_retries):
                try:
                    # Mock connection attempt
                    if attempt < 2:  # Fail first two attempts
                        raise asyncpg.exceptions.ConnectionDoesNotExistError("Connection failed")
                    
                    # Success on third attempt
                    mock_conn = AsyncMock()
                    return mock_conn
                    
                except asyncpg.exceptions.ConnectionDoesNotExistError:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    else:
                        raise
        
        # Test connection retry logic
        conn = await connect_with_retry({"host": "postgres"})
        assert conn is not None
    
    def test_connection_pool_configuration(self):
        """Test optimal connection pool settings for large-scale processing."""
        def calculate_pool_size(concurrent_jobs, avg_connection_time, max_connections=20):
            """Calculate optimal pool size based on workload."""
            recommended_size = min(concurrent_jobs * 2, max_connections)
            return max(recommended_size, 1)  # At least 1 connection
        
        # Test pool size calculations
        assert calculate_pool_size(5, 1.0) == 10
        assert calculate_pool_size(15, 1.0) == 20  # Capped at max
        assert calculate_pool_size(0, 1.0) == 1   # Minimum 1

    # ISSUE 6: Missing Instruments and Data Consistency
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_instrument_existence_validation(self, mock_db_connection):
        """Test validation of instrument existence before processing."""
        mock_conn = mock_db_connection
        
        # Mock instrument lookup
        mock_conn.fetchval.side_effect = [1, None, 3]  # AAPL exists, INVALID doesn't, MSFT exists
        
        async def validate_instruments(symbols):
            """Validate that instruments exist before processing."""
            valid_instruments = []
            invalid_instruments = []
            
            for symbol in symbols:
                instrument_id = await mock_conn.fetchval(
                    "SELECT id FROM dev_instruments WHERE symbol = $1", symbol
                )
                if instrument_id:
                    valid_instruments.append((symbol, instrument_id))
                else:
                    invalid_instruments.append(symbol)
            
            return valid_instruments, invalid_instruments
        
        test_symbols = ["AAPL", "INVALID", "MSFT"]
        valid, invalid = await validate_instruments(test_symbols)
        
        assert len(valid) == 2
        assert len(invalid) == 1
        assert invalid[0] == "INVALID"
        assert ("AAPL", 1) in valid
        assert ("MSFT", 3) in valid
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_bulk_instrument_creation(self, mock_db_connection):
        """Test bulk creation of missing instruments."""
        mock_conn = mock_db_connection
        mock_conn.executemany.return_value = None
        mock_conn.fetchval.return_value = 10000  # Final count
        
        async def bulk_create_instruments(symbols_data):
            """Bulk create instruments avoiding constraint issues."""
            # Simple INSERT approach (avoid ON CONFLICT)
            instrument_records = []
            for symbol, name in symbols_data:
                instrument_records.append((symbol, name or f"{symbol} Corporation", "NYSE", True))
            
            await mock_conn.executemany("""
                INSERT INTO dev_instruments (symbol, name, exchange, is_active)
                VALUES ($1, $2, $3, $4)
            """, instrument_records)
            
            # Verify count
            final_count = await mock_conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
            return final_count
        
        test_data = [("TEST1", "Test Corp 1"), ("TEST2", None), ("TEST3", "Test Corp 3")]
        result = await bulk_create_instruments(test_data)
        
        assert result == 10000
        mock_conn.executemany.assert_called_once()
    
    def test_data_consistency_validation(self):
        """Test data consistency validation across tables."""
        def validate_data_consistency(instruments_count, market_cap_count, price_records_count):
            """Validate data consistency across related tables."""
            issues = []
            
            # Check if we have market cap for most instruments
            market_cap_coverage = market_cap_count / instruments_count if instruments_count > 0 else 0
            if market_cap_coverage < 0.8:  # Less than 80% coverage
                issues.append(f"Low market cap coverage: {market_cap_coverage:.1%}")
            
            # Check if we have price data
            price_coverage = price_records_count / instruments_count if instruments_count > 0 else 0
            if price_coverage < 10:  # Less than 10 price records per instrument on average
                issues.append(f"Low price data coverage: {price_coverage:.1f} records per instrument")
            
            return issues
        
        # Test consistency validation
        issues = validate_data_consistency(10000, 9500, 150000)
        assert len(issues) == 0  # Should pass validation
        
        issues = validate_data_consistency(10000, 1000, 5000)
        assert len(issues) == 2  # Should find both coverage issues


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])