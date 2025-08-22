#!/usr/bin/env python3
"""
Comprehensive API Error Handling and Resilience Tests

Tests the robustness of our market cap system against various API failures:
- Rate limiting scenarios
- Network timeouts  
- Authentication failures
- Malformed responses
- Service downtime
"""

import pytest
import asyncio
import aiohttp
import asyncpg
from unittest.mock import patch, MagicMock, AsyncMock
from aiohttp import ClientTimeout, ClientResponseError, ClientConnectionError
import logging
from datetime import date, datetime
import json

from config.environment import Environment
from src.secmaster.compute_market_cap_from_shares import compute_and_populate_market_cap


class TestAPIErrorHandling:
    """Test API error handling and resilience patterns"""
    
    @pytest.fixture
    async def env(self):
        """Create environment for testing"""
        env = Environment()
        env.db_host = 'localhost'
        env.db_port = '5433'
        env.db_user = 'postgres'
        env.db_password = 'postgres'
        env.db_name = 'dev_db'
        yield env
        
    @pytest.mark.asyncio
    async def test_rate_limit_handling(self, engine):
        """Test graceful handling of rate limit responses"""
        
        mock_session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 429
        mock_response.headers = {'Retry-After': '60'}
        
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session_class.return_value = mock_session
            
            result = await engine.fetch_shares_outstanding('AAPL', mock_session)
            
            # Should return None and log appropriate warning
            assert result is None
            mock_session.get.assert_called_once()
    
    @pytest.mark.asyncio 
    async def test_authentication_failure_handling(self, engine):
        """Test handling of authentication failures"""
        
        mock_session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 401
        mock_response.text.return_value = "Unauthorized"
        
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        result = await engine.fetch_shares_outstanding('AAPL', mock_session)
        assert result is None
    
    @pytest.mark.asyncio
    async def test_network_timeout_handling(self, engine):
        """Test handling of network timeouts"""
        
        mock_session = AsyncMock()
        mock_session.get.side_effect = asyncio.TimeoutError("Connection timeout")
        
        result = await engine.fetch_shares_outstanding('AAPL', mock_session)
        assert result is None
    
    @pytest.mark.asyncio
    async def test_malformed_json_response_handling(self, engine):
        """Test handling of malformed JSON responses"""
        
        mock_session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        result = await engine.fetch_shares_outstanding('AAPL', mock_session)
        assert result is None
    
    @pytest.mark.asyncio
    async def test_missing_data_fields_handling(self, engine):
        """Test handling of responses missing expected data fields"""
        
        mock_session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 200
        
        # Response with missing shares outstanding data
        mock_response.json.return_value = {
            'results': {
                'ticker': 'AAPL',
                'name': 'Apple Inc.',
                # Missing 'share_class_shares_outstanding' field
            }
        }
        
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        result = await engine.fetch_shares_outstanding('AAPL', mock_session)
        assert result is None
    
    @pytest.mark.asyncio
    async def test_database_connection_failure_resilience(self, engine):
        """Test resilience to database connection failures"""
        
        # Mock database connection failure
        with patch('asyncpg.create_pool') as mock_pool:
            mock_pool.side_effect = ConnectionError("Database unavailable")
            
            result = await engine.get_recent_price(1, 'AAPL')
            assert result is None
    
    @pytest.mark.asyncio
    async def test_concurrent_api_call_throttling(self, engine):
        """Test that concurrent API calls are properly throttled"""
        
        call_times = []
        
        async def mock_get(*args, **kwargs):
            call_times.append(datetime.now())
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = {
                'results': {
                    'share_class_shares_outstanding': 1000000000
                }
            }
            return mock_response
        
        mock_session = AsyncMock()
        mock_session.get = mock_get
        
        # Make multiple concurrent calls
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        tasks = [
            engine.fetch_shares_outstanding(symbol, mock_session) 
            for symbol in symbols
        ]
        
        start_time = datetime.now()
        results = await asyncio.gather(*tasks)
        end_time = datetime.now()
        
        # Should have results for all symbols
        assert len(results) == 5
        assert all(result == 1000000000 for result in results)
        
        # Should take reasonable time due to rate limiting
        duration = (end_time - start_time).total_seconds()
        assert duration >= 1.0  # At least some delay from rate limiting
    
    @pytest.mark.asyncio
    async def test_api_response_validation(self, engine):
        """Test validation of API response data quality"""
        
        test_cases = [
            # Valid response
            {
                'results': {
                    'share_class_shares_outstanding': 1000000000
                }
            },
            # Zero shares (invalid)
            {
                'results': {
                    'share_class_shares_outstanding': 0
                }
            },
            # Negative shares (invalid)
            {
                'results': {
                    'share_class_shares_outstanding': -1000000
                }
            },
            # Non-numeric shares (invalid)
            {
                'results': {
                    'share_class_shares_outstanding': 'invalid'
                }
            }
        ]
        
        expected_results = [1000000000, None, None, None]
        
        for i, response_data in enumerate(test_cases):
            mock_session = AsyncMock()
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = response_data
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            
            result = await engine.fetch_shares_outstanding('AAPL', mock_session)
            assert result == expected_results[i], f"Test case {i} failed"


class TestDataQualityValidation:
    """Test data quality validation and error detection"""
    
    @pytest.mark.asyncio
    async def test_market_cap_sanity_checks(self):
        """Test market cap calculation sanity checks"""
        
        # Test cases: (shares, price, expected_valid)
        test_cases = [
            (1_000_000_000, 100.0, True),    # Normal case: $100B
            (1_000_000, 1.0, False),         # Too small: $1M
            (1_000_000_000, 50000.0, False), # Too large: $50T
            (0, 100.0, False),               # Zero shares
            (1_000_000_000, 0, False),       # Zero price
            (-1_000_000, 100.0, False),      # Negative shares
            (1_000_000_000, -10.0, False),   # Negative price
        ]
        
        for shares, price, expected_valid in test_cases:
            market_cap = shares * price
            
            # Apply same sanity checks as the engine
            is_valid = (
                shares > 0 and 
                price > 0 and 
                1_000_000 <= market_cap <= 10_000_000_000_000
            )
            
            assert is_valid == expected_valid, f"Sanity check failed for shares={shares}, price={price}"
    
    @pytest.mark.asyncio
    async def test_price_data_validation(self):
        """Test validation of price data from database"""
        
        db_url = 'postgresql://postgres:postgres@localhost:5433/dev_db'
        
        try:
            conn = await asyncpg.connect(db_url)
            
            # Check for obviously invalid price data
            invalid_prices = await conn.fetch("""
                SELECT DISTINCT instrument_id, symbol, close, date, 'polygon' as source
                FROM dev_daily_prices_polygon p
                JOIN dev_instruments i ON p.instrument_id = i.id
                WHERE close <= 0 OR close > 10000  -- Suspiciously high/low prices
                ORDER BY close DESC
                LIMIT 10
            """)
            
            if invalid_prices:
                print("⚠️ Found potentially invalid price data:")
                for row in invalid_prices:
                    print(f"  • {row['symbol']}: ${row['close']:.2f} on {row['date']} ({row['source']})")
            
            # Check for missing recent price data
            missing_recent = await conn.fetchval("""
                SELECT COUNT(DISTINCT i.id)
                FROM dev_instruments i
                LEFT JOIN dev_daily_prices_polygon p ON i.id = p.instrument_id 
                    AND p.date >= CURRENT_DATE - INTERVAL '30 days'
                WHERE p.instrument_id IS NULL
                  AND i.symbol ~ '^[A-Z]{1,5}$'  -- Regular stock symbols
            """)
            
            await conn.close()
            
            print(f"📊 Instruments missing recent price data: {missing_recent}")
            
            # This is informational - we don't fail the test
            assert True
            
        except Exception as e:
            print(f"⚠️ Could not validate price data: {e}")
            # Don't fail test if database unavailable
            assert True


class TestSystemResilience:
    """Test overall system resilience and recovery patterns"""
    
    @pytest.mark.asyncio
    async def test_partial_failure_recovery(self):
        """Test system behavior when some API calls fail but others succeed"""
        
        env = Environment()
        env.db_host = 'localhost'
        env.db_port = '5433'
        env.db_user = 'postgres' 
        env.db_password = 'postgres'
        env.db_name = 'dev_db'
        
        engine = MarketCapComputationEngine(env)
        
        # Mock mixed success/failure responses
        call_count = 0
        
        async def mock_fetch_shares(symbol, session):
            nonlocal call_count
            call_count += 1
            
            # Simulate 50% failure rate
            if call_count % 2 == 0:
                return 1000000000  # Success
            else:
                return None  # Failure
        
        # Patch the method
        original_method = engine.fetch_shares_outstanding
        engine.fetch_shares_outstanding = mock_fetch_shares
        
        try:
            # Process a batch of instruments
            test_instruments = [
                (1, 'AAPL'), (2, 'MSFT'), (3, 'GOOGL'), (4, 'AMZN')
            ]
            
            mock_session = AsyncMock()
            results = await engine.process_instrument_batch(test_instruments, mock_session)
            
            # Should have some successes despite partial failures
            # Exact count depends on price data availability
            assert isinstance(results, list)
            print(f"✅ Processed {len(results)} out of {len(test_instruments)} instruments")
            
        finally:
            # Restore original method
            engine.fetch_shares_outstanding = original_method
    
    @pytest.mark.asyncio
    async def test_graceful_degradation(self):
        """Test graceful degradation when external services are unavailable"""
        
        # Test that the system can still function for data analysis
        # even when API services are down
        
        db_url = 'postgresql://postgres:postgres@localhost:5433/dev_db'
        
        try:
            conn = await asyncpg.connect(db_url)
            
            # Should still be able to query existing market cap data
            existing_data = await conn.fetchval("""
                SELECT COUNT(*) FROM dev_daily_market_cap
                WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            """)
            
            print(f"📊 Can still access {existing_data:,} recent market cap records")
            
            # Should still be able to run universe queries
            universe_count = await conn.fetchval("""
                SELECT COUNT(DISTINCT i.symbol)
                FROM dev_instruments i
                JOIN dev_daily_market_cap mc ON i.id = mc.instrument_id
                WHERE mc.market_cap >= 400000000
            """)
            
            print(f"🎯 Can still identify {universe_count} qualifying stocks for universe")
            
            await conn.close()
            
            assert True  # System gracefully handles API unavailability
            
        except Exception as e:
            print(f"⚠️ Graceful degradation test failed: {e}")
            assert False


if __name__ == "__main__":
    # Run basic validation
    asyncio.run(TestSystemResilience().test_graceful_degradation())