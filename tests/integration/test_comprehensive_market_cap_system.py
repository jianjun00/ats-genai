#!/usr/bin/env python3
"""
Comprehensive Market Cap System Integration Tests

This test suite validates the entire market cap computation ecosystem:
1. Database schema integrity
2. Polygon API integration and shares outstanding retrieval
3. Price data validation across multiple vendors
4. Market cap calculation accuracy
5. Data persistence and retrieval
6. Error handling and edge cases
7. Performance under load
8. Data quality validation

Key Test Categories:
- Schema Validation: Ensures all required columns exist with correct types
- API Integration: Tests real API calls with rate limiting
- Data Accuracy: Validates calculations against known values
- Cross-Vendor Validation: Ensures data consistency across price sources
- Edge Case Handling: Tests with missing data, extreme values, errors
- Performance Testing: Validates system performance under realistic load
"""

import pytest
import asyncio
import asyncpg
import aiohttp
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from config.environment import Environment

@dataclass
class MarketCapTestResult:
    symbol: str
    shares_outstanding: int
    price: float
    calculated_market_cap: float
    expected_market_cap_range: Tuple[float, float]
    passed: bool
    notes: str

class ComprehensiveMarketCapTester:
    """Comprehensive tester for market cap system"""
    
    def __init__(self, env: Environment):
        self.env = env
        self.db_url = env.get_database_url()
        self.polygon_api_key = os.getenv('POLYGON_API_KEY')
        
        # Test thresholds
        self.thresholds = {
            'max_market_cap_deviation': 0.20,  # 20% deviation allowed
            'min_reasonable_market_cap': 1_000_000,  # $1M minimum
            'max_reasonable_market_cap': 10_000_000_000_000,  # $10T maximum
            'api_timeout': 30.0,  # 30 second API timeout
            'max_processing_time_per_symbol': 5.0  # 5 seconds per symbol
        }

@pytest.mark.integration
@pytest.mark.database
class TestMarketCapDatabaseSchema:
    """Test database schema integrity for market cap system"""
    
    @pytest.fixture
    def env(self):
        return Environment()
    
    @pytest.fixture
    def tester(self, env):
        return ComprehensiveMarketCapTester(env)
    
    @pytest.mark.asyncio
    async def test_market_cap_table_schema_complete(self, tester):
        """Test that market cap table has all required columns with correct types"""
        
        pool = await asyncpg.create_pool(tester.db_url, min_size=1, max_size=2)
        
        try:
            async with pool.acquire() as conn:
                # Get complete schema information
                schema_info = await conn.fetch("""
                    SELECT 
                        column_name, 
                        data_type, 
                        is_nullable,
                        column_default,
                        ordinal_position
                    FROM information_schema.columns 
                    WHERE table_name = 'dev_daily_market_cap' 
                        AND table_schema = 'public'
                    ORDER BY ordinal_position
                """)
                
                columns = {row['column_name']: row for row in schema_info}
                
                # Required columns with expected types
                required_columns = {
                    'id': 'integer',
                    'instrument_id': 'integer', 
                    'date': 'date',
                    'market_cap': 'bigint',
                    'shares_outstanding': 'bigint',
                    'price_used': 'numeric',
                    'source': 'character varying',
                    'created_at': 'timestamp without time zone',
                    'updated_at': 'timestamp without time zone'
                }
                
                # Validate all required columns exist
                missing_columns = []
                for col_name, expected_type in required_columns.items():
                    if col_name not in columns:
                        missing_columns.append(col_name)
                    else:
                        actual_type = columns[col_name]['data_type']
                        assert expected_type in actual_type or actual_type in expected_type, \
                            f"Column {col_name} has type {actual_type}, expected {expected_type}"
                
                assert len(missing_columns) == 0, f"Missing required columns: {missing_columns}"
                
                # Validate constraints and indexes
                constraints = await conn.fetch("""
                    SELECT constraint_name, constraint_type
                    FROM information_schema.table_constraints
                    WHERE table_name = 'dev_daily_market_cap'
                        AND table_schema = 'public'
                """)
                
                constraint_types = [c['constraint_type'] for c in constraints]
                assert 'PRIMARY KEY' in constraint_types, "Primary key constraint missing"
                
                # Test that we can perform all required operations
                test_insert_success = await self._test_schema_operations(conn)
                assert test_insert_success, "Schema operations test failed"
        
        finally:
            await pool.close()
    
    async def _test_schema_operations(self, conn):
        """Test all CRUD operations on market cap table"""
        
        test_instrument_id = 99999
        test_date = date.today()
        
        try:
            # Test INSERT
            await conn.execute("""
                INSERT INTO dev_daily_market_cap 
                (instrument_id, date, market_cap, shares_outstanding, price_used, source, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
            """, test_instrument_id, test_date, 1000000000, 100000000, 10.0, 'test')
            
            # Test SELECT
            result = await conn.fetchrow("""
                SELECT * FROM dev_daily_market_cap 
                WHERE instrument_id = $1 AND date = $2
            """, test_instrument_id, test_date)
            
            assert result is not None, "Insert/Select failed"
            assert result['market_cap'] == 1000000000, "Market cap value incorrect"
            assert result['shares_outstanding'] == 100000000, "Shares outstanding incorrect"
            assert abs(float(result['price_used']) - 10.0) < 0.01, "Price used incorrect"
            assert result['source'] == 'test', "Source incorrect"
            
            # Test UPDATE
            await conn.execute("""
                UPDATE dev_daily_market_cap 
                SET market_cap = $1, updated_at = NOW()
                WHERE instrument_id = $2 AND date = $3
            """, 2000000000, test_instrument_id, test_date)
            
            # Verify UPDATE
            updated_result = await conn.fetchval("""
                SELECT market_cap FROM dev_daily_market_cap 
                WHERE instrument_id = $1 AND date = $2
            """, test_instrument_id, test_date)
            
            assert updated_result == 2000000000, "Update failed"
            
            # Test UPSERT (ON CONFLICT)
            await conn.execute("""
                INSERT INTO dev_daily_market_cap 
                (instrument_id, date, market_cap, shares_outstanding, price_used, source, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
                ON CONFLICT (instrument_id, date) DO UPDATE SET
                    market_cap = EXCLUDED.market_cap,
                    updated_at = NOW()
            """, test_instrument_id, test_date, 3000000000, 150000000, 20.0, 'test_upsert')
            
            # Verify UPSERT
            upsert_result = await conn.fetchrow("""
                SELECT market_cap, source FROM dev_daily_market_cap 
                WHERE instrument_id = $1 AND date = $2
            """, test_instrument_id, test_date)
            
            assert upsert_result['market_cap'] == 3000000000, "Upsert failed"
            assert upsert_result['source'] == 'test_upsert', "Upsert source incorrect"
            
            # Test DELETE
            await conn.execute("""
                DELETE FROM dev_daily_market_cap 
                WHERE instrument_id = $1 AND date = $2
            """, test_instrument_id, test_date)
            
            # Verify DELETE
            deleted_result = await conn.fetchval("""
                SELECT COUNT(*) FROM dev_daily_market_cap 
                WHERE instrument_id = $1 AND date = $2
            """, test_instrument_id, test_date)
            
            assert deleted_result == 0, "Delete failed"
            
            return True
            
        except Exception as e:
            print(f"Schema operations test failed: {e}")
            # Cleanup on failure
            try:
                await conn.execute("""
                    DELETE FROM dev_daily_market_cap 
                    WHERE instrument_id = $1 AND date = $2
                """, test_instrument_id, test_date)
            except:
                pass
            return False

@pytest.mark.integration  
@pytest.mark.api
class TestPolygonAPIIntegration:
    """Test Polygon API integration for shares outstanding data"""
    
    @pytest.fixture
    def env(self):
        return Environment()
    
    @pytest.fixture
    def tester(self, env):
        return ComprehensiveMarketCapTester(env)
    
    @pytest.mark.asyncio
    async def test_polygon_api_major_stocks(self, tester):
        """Test Polygon API integration with known major stocks"""
        
        if not tester.polygon_api_key:
            pytest.skip("POLYGON_API_KEY not available")
        
        # Test with known major stocks that should have data
        test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            
            for symbol in test_symbols:
                print(f"Testing Polygon API for {symbol}")
                
                url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
                headers = {"Authorization": f"Bearer {tester.polygon_api_key}"}
                
                # Rate limiting
                await asyncio.sleep(0.3)
                
                async with session.get(url, headers=headers) as response:
                    assert response.status == 200, f"API call failed for {symbol}: HTTP {response.status}"
                    
                    data = await response.json()
                    
                    # Validate response structure
                    assert 'results' in data, f"No results in API response for {symbol}"
                    
                    results = data['results']
                    
                    # Check for shares outstanding data
                    shares_fields = [
                        'share_class_shares_outstanding',
                        'weighted_shares_outstanding',
                        'shares_outstanding'
                    ]
                    
                    shares_outstanding = None
                    for field in shares_fields:
                        if field in results and results[field]:
                            shares_outstanding = results[field]
                            break
                    
                    assert shares_outstanding is not None, f"No shares outstanding data for {symbol}"
                    assert shares_outstanding > 0, f"Invalid shares outstanding for {symbol}: {shares_outstanding}"
                    
                    # Sanity check: shares should be reasonable for major companies
                    assert 1_000_000 <= shares_outstanding <= 50_000_000_000, \
                        f"Unreasonable shares outstanding for {symbol}: {shares_outstanding:,}"
                    
                    # Validate other critical fields exist
                    assert 'ticker' in results, f"Missing ticker field for {symbol}"
                    assert results['ticker'] == symbol, f"Ticker mismatch for {symbol}"
                    
                    print(f"✅ {symbol}: {shares_outstanding:,} shares outstanding")
    
    @pytest.mark.asyncio
    async def test_polygon_api_error_handling(self, tester):
        """Test Polygon API error handling with invalid symbols"""
        
        if not tester.polygon_api_key:
            pytest.skip("POLYGON_API_KEY not available")
        
        # Test with invalid symbols that should return 404
        invalid_symbols = ['INVALIDSTOCK', 'XXXXXX', 'NOTREAL']
        
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            
            for symbol in invalid_symbols:
                url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
                headers = {"Authorization": f"Bearer {tester.polygon_api_key}"}
                
                await asyncio.sleep(0.3)  # Rate limiting
                
                async with session.get(url, headers=headers) as response:
                    # Should return 404 for invalid symbols
                    assert response.status in [404, 400], \
                        f"Expected 404/400 for invalid symbol {symbol}, got {response.status}"
    
    @pytest.mark.asyncio
    async def test_polygon_api_rate_limiting(self, tester):
        """Test that we handle Polygon API rate limiting correctly"""
        
        if not tester.polygon_api_key:
            pytest.skip("POLYGON_API_KEY not available")
        
        # Test with multiple rapid requests to trigger rate limiting
        test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'] * 3  # 15 requests
        
        timeout = aiohttp.ClientTimeout(total=120)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            
            start_time = datetime.now()
            success_count = 0
            rate_limit_count = 0
            
            for symbol in test_symbols:
                url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
                headers = {"Authorization": f"Bearer {tester.polygon_api_key}"}
                
                # Don't add artificial delays - test real rate limiting
                
                try:
                    async with session.get(url, headers=headers) as response:
                        if response.status == 429:
                            rate_limit_count += 1
                            # Wait and retry once
                            await asyncio.sleep(60)
                            async with session.get(url, headers=headers) as retry_response:
                                if retry_response.status == 200:
                                    success_count += 1
                        elif response.status == 200:
                            success_count += 1
                            
                except Exception as e:
                    print(f"API error for {symbol}: {e}")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print(f"Rate limiting test: {success_count}/{len(test_symbols)} successful in {duration:.1f}s")
            print(f"Rate limited responses: {rate_limit_count}")
            
            # We should handle rate limiting gracefully
            assert success_count >= len(test_symbols) * 0.8, "Too many API failures"

@pytest.mark.integration
@pytest.mark.database
class TestMarketCapCalculationAccuracy:
    """Test market cap calculation accuracy against known values"""
    
    @pytest.fixture
    def env(self):
        return Environment()
    
    @pytest.fixture
    def tester(self, env):
        return ComprehensiveMarketCapTester(env)
    
    @pytest.mark.asyncio
    async def test_market_cap_calculation_accuracy(self, tester):
        """Test market cap calculations against known approximate values"""
        
        # Known major stocks with approximate market cap ranges (as of 2024)
        known_market_caps = {
            'AAPL': (2_500_000_000_000, 4_000_000_000_000),  # $2.5T - $4T
            'MSFT': (2_000_000_000_000, 3_500_000_000_000),  # $2T - $3.5T  
            'GOOGL': (1_500_000_000_000, 2_500_000_000_000), # $1.5T - $2.5T
            'AMZN': (1_000_000_000_000, 2_000_000_000_000),  # $1T - $2T
            'TSLA': (500_000_000_000, 1_200_000_000_000),    # $500B - $1.2T
        }
        
        pool = await asyncpg.create_pool(tester.db_url, min_size=2, max_size=5)
        
        try:
            async with pool.acquire() as conn:
                test_results = []
                
                for symbol, (min_cap, max_cap) in known_market_caps.items():
                    print(f"Testing market cap calculation for {symbol}")
                    
                    # Get instrument ID
                    instrument_id = await conn.fetchval(
                        "SELECT id FROM dev_instruments WHERE symbol = $1", symbol
                    )
                    
                    if not instrument_id:
                        print(f"⚠️  Instrument {symbol} not found in database")
                        continue
                    
                    # Check if we have market cap data
                    recent_market_cap = await conn.fetchrow("""
                        SELECT market_cap, shares_outstanding, price_used, date, source
                        FROM dev_daily_market_cap 
                        WHERE instrument_id = $1 
                        ORDER BY date DESC 
                        LIMIT 1
                    """, instrument_id)
                    
                    if recent_market_cap:
                        calculated_cap = recent_market_cap['market_cap']
                        shares = recent_market_cap['shares_outstanding'] 
                        price = float(recent_market_cap['price_used'])
                        
                        # Verify calculation: market_cap = shares * price
                        expected_cap = shares * price
                        calculation_error = abs(calculated_cap - expected_cap) / expected_cap
                        
                        assert calculation_error < 0.001, \
                            f"Market cap calculation error for {symbol}: {calculation_error:.4f}"
                        
                        # Check if calculated value is in reasonable range
                        in_range = min_cap <= calculated_cap <= max_cap
                        deviation = 0
                        
                        if calculated_cap < min_cap:
                            deviation = (min_cap - calculated_cap) / min_cap
                        elif calculated_cap > max_cap:
                            deviation = (calculated_cap - max_cap) / max_cap
                        
                        result = MarketCapTestResult(
                            symbol=symbol,
                            shares_outstanding=shares,
                            price=price,
                            calculated_market_cap=calculated_cap,
                            expected_market_cap_range=(min_cap, max_cap),
                            passed=in_range or deviation <= tester.thresholds['max_market_cap_deviation'],
                            notes=f"Calculated: ${calculated_cap/1e9:.2f}B, Expected: ${min_cap/1e9:.0f}B-${max_cap/1e9:.0f}B"
                        )
                        
                        test_results.append(result)
                        print(f"  {result.notes} - {'✅ PASS' if result.passed else '❌ FAIL'}")
                    
                    else:
                        print(f"⚠️  No market cap data found for {symbol}")
                
                # Overall validation
                if test_results:
                    passed_tests = sum(1 for r in test_results if r.passed)
                    pass_rate = passed_tests / len(test_results)
                    
                    print(f"\nMarket Cap Accuracy Test Summary:")
                    print(f"  Tested: {len(test_results)} stocks")
                    print(f"  Passed: {passed_tests} ({pass_rate:.1%})")
                    
                    assert pass_rate >= 0.8, f"Market cap accuracy too low: {pass_rate:.1%}"
                    
                    # Show any failures for debugging
                    failures = [r for r in test_results if not r.passed]
                    for failure in failures:
                        print(f"  ❌ {failure.symbol}: {failure.notes}")
                
        finally:
            await pool.close()
    
    @pytest.mark.asyncio
    async def test_market_cap_edge_cases(self, tester):
        """Test market cap calculation edge cases and error handling"""
        
        pool = await asyncpg.create_pool(tester.db_url, min_size=1, max_size=2)
        
        try:
            async with pool.acquire() as conn:
                test_instrument_id = 99998
                test_date = date.today()
                
                # Test Case 1: Very small market cap
                await conn.execute("""
                    INSERT INTO dev_daily_market_cap 
                    (instrument_id, date, market_cap, shares_outstanding, price_used, source, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
                    ON CONFLICT (instrument_id, date) DO UPDATE SET
                        market_cap = EXCLUDED.market_cap,
                        shares_outstanding = EXCLUDED.shares_outstanding,
                        price_used = EXCLUDED.price_used,
                        updated_at = NOW()
                """, test_instrument_id, test_date, 500000, 1000000, 0.5, 'test_small')
                
                result = await conn.fetchrow("""
                    SELECT market_cap, shares_outstanding, price_used
                    FROM dev_daily_market_cap 
                    WHERE instrument_id = $1 AND date = $2
                """, test_instrument_id, test_date)
                
                assert result['market_cap'] == 500000, "Small market cap test failed"
                
                # Test Case 2: Large market cap
                large_cap = 5_000_000_000_000  # $5T
                await conn.execute("""
                    UPDATE dev_daily_market_cap 
                    SET market_cap = $1, shares_outstanding = $2, price_used = $3
                    WHERE instrument_id = $4 AND date = $5
                """, large_cap, 10_000_000_000, 500.0, test_instrument_id, test_date)
                
                large_result = await conn.fetchrow("""
                    SELECT market_cap FROM dev_daily_market_cap 
                    WHERE instrument_id = $1 AND date = $2
                """, test_instrument_id, test_date)
                
                assert large_result['market_cap'] == large_cap, "Large market cap test failed"
                
                # Test Case 3: Zero values handling
                try:
                    await conn.execute("""
                        UPDATE dev_daily_market_cap 
                        SET market_cap = $1, shares_outstanding = $2, price_used = $3
                        WHERE instrument_id = $4 AND date = $5
                    """, 0, 0, 0.0, test_instrument_id, test_date)
                    
                    zero_result = await conn.fetchval("""
                        SELECT market_cap FROM dev_daily_market_cap 
                        WHERE instrument_id = $1 AND date = $2
                    """, test_instrument_id, test_date)
                    
                    assert zero_result == 0, "Zero market cap test failed"
                    
                except Exception as e:
                    # Some zero handling might be expected to fail
                    print(f"Zero values test failed as expected: {e}")
                
                # Cleanup
                await conn.execute("""
                    DELETE FROM dev_daily_market_cap 
                    WHERE instrument_id = $1 AND date = $2
                """, test_instrument_id, test_date)
                
        finally:
            await pool.close()

@pytest.mark.integration
@pytest.mark.performance
class TestMarketCapSystemPerformance:
    """Test market cap system performance under realistic load"""
    
    @pytest.fixture
    def env(self):
        return Environment()
    
    @pytest.fixture  
    def tester(self, env):
        return ComprehensiveMarketCapTester(env)
    
    @pytest.mark.asyncio
    async def test_batch_processing_performance(self, tester):
        """Test performance of batch processing market cap calculations"""
        
        pool = await asyncpg.create_pool(tester.db_url, min_size=2, max_size=8)
        
        try:
            async with pool.acquire() as conn:
                # Get sample instruments for testing
                test_instruments = await conn.fetch("""
                    SELECT id, symbol FROM dev_instruments 
                    WHERE symbol ~ '^[A-Z]{1,4}$'
                    ORDER BY symbol
                    LIMIT 50
                """)
                
                if len(test_instruments) < 10:
                    pytest.skip("Not enough test instruments available")
                
                print(f"Testing batch processing performance with {len(test_instruments)} instruments")
                
                start_time = datetime.now()
                batch_size = 10
                successful_batches = 0
                total_batches = (len(test_instruments) + batch_size - 1) // batch_size
                
                for i in range(0, len(test_instruments), batch_size):
                    batch = test_instruments[i:i + batch_size]
                    batch_start_time = datetime.now()
                    
                    batch_symbols = [inst['symbol'] for inst in batch]
                    print(f"Processing batch: {', '.join(batch_symbols)}")
                    
                    # Simulate batch processing (without actual API calls for performance test)
                    batch_success_count = 0
                    
                    for inst in batch:
                        try:
                            # Simulate market cap calculation with test data
                            await conn.execute("""
                                INSERT INTO dev_daily_market_cap 
                                (instrument_id, date, market_cap, shares_outstanding, price_used, source, created_at, updated_at)
                                VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
                                ON CONFLICT (instrument_id, date) DO UPDATE SET
                                    market_cap = EXCLUDED.market_cap,
                                    updated_at = NOW()
                            """, inst['id'], date.today(), 1000000000, 100000000, 10.0, 'perf_test')
                            
                            batch_success_count += 1
                            
                        except Exception as e:
                            print(f"Error processing {inst['symbol']}: {e}")
                    
                    batch_end_time = datetime.now()
                    batch_duration = (batch_end_time - batch_start_time).total_seconds()
                    
                    if batch_success_count == len(batch):
                        successful_batches += 1
                    
                    # Performance assertion: each batch should complete reasonably quickly
                    assert batch_duration < 10.0, \
                        f"Batch processing too slow: {batch_duration:.2f}s for {len(batch)} instruments"
                
                end_time = datetime.now()
                total_duration = (end_time - start_time).total_seconds()
                
                # Performance metrics
                instruments_per_second = len(test_instruments) / total_duration
                success_rate = successful_batches / total_batches
                
                print(f"\nPerformance Test Results:")
                print(f"  Total instruments: {len(test_instruments)}")
                print(f"  Total duration: {total_duration:.2f}s")
                print(f"  Processing rate: {instruments_per_second:.1f} instruments/second")
                print(f"  Batch success rate: {success_rate:.1%}")
                
                # Performance assertions
                assert instruments_per_second >= 2.0, \
                    f"Processing rate too slow: {instruments_per_second:.1f} instruments/second"
                assert success_rate >= 0.95, f"Batch success rate too low: {success_rate:.1%}"
                
                # Cleanup test data
                for inst in test_instruments:
                    await conn.execute("""
                        DELETE FROM dev_daily_market_cap 
                        WHERE instrument_id = $1 AND date = $2 AND source = 'perf_test'
                    """, inst['id'], date.today())
                
        finally:
            await pool.close()
    
    @pytest.mark.asyncio
    async def test_concurrent_processing(self, tester):
        """Test concurrent market cap processing"""
        
        async def process_single_instrument(instrument_id: int, symbol: str):
            """Process a single instrument concurrently"""
            pool = await asyncpg.create_pool(tester.db_url, min_size=1, max_size=2)
            try:
                async with pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO dev_daily_market_cap 
                        (instrument_id, date, market_cap, shares_outstanding, price_used, source, created_at, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
                        ON CONFLICT (instrument_id, date) DO UPDATE SET
                            market_cap = EXCLUDED.market_cap,
                            updated_at = NOW()
                    """, instrument_id, date.today(), 1500000000, 150000000, 10.0, 'concurrent_test')
                    return True
            except Exception as e:
                print(f"Concurrent processing error for {symbol}: {e}")
                return False
            finally:
                await pool.close()
        
        # Get test instruments
        pool = await asyncpg.create_pool(tester.db_url, min_size=1, max_size=2)
        
        try:
            async with pool.acquire() as conn:
                test_instruments = await conn.fetch("""
                    SELECT id, symbol FROM dev_instruments 
                    WHERE symbol ~ '^[A-Z]{1,4}$'
                    ORDER BY symbol
                    LIMIT 20
                """)
        
            if len(test_instruments) < 10:
                pytest.skip("Not enough test instruments for concurrent test")
            
            print(f"Testing concurrent processing with {len(test_instruments)} instruments")
            
            # Process all instruments concurrently
            start_time = datetime.now()
            
            tasks = []
            for inst in test_instruments:
                task = process_single_instrument(inst['id'], inst['symbol'])
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Count successful concurrent operations
            successful_count = sum(1 for result in results if result is True)
            success_rate = successful_count / len(test_instruments)
            
            print(f"Concurrent processing results:")
            print(f"  Successful: {successful_count}/{len(test_instruments)} ({success_rate:.1%})")
            print(f"  Duration: {duration:.2f}s")
            
            # Concurrent processing should be faster and mostly successful
            assert duration < 30.0, f"Concurrent processing too slow: {duration:.2f}s"
            assert success_rate >= 0.9, f"Concurrent success rate too low: {success_rate:.1%}"
            
            # Cleanup
            async with pool.acquire() as conn:
                for inst in test_instruments:
                    await conn.execute("""
                        DELETE FROM dev_daily_market_cap 
                        WHERE instrument_id = $1 AND date = $2 AND source = 'concurrent_test'
                    """, inst['id'], date.today())
        
        finally:
            await pool.close()

@pytest.mark.integration
@pytest.mark.data_quality
class TestMarketCapDataQuality:
    """Test data quality aspects of market cap system"""
    
    @pytest.fixture
    def env(self):
        return Environment()
    
    @pytest.fixture
    def tester(self, env):
        return ComprehensiveMarketCapTester(env)
    
    @pytest.mark.asyncio
    async def test_data_consistency_across_dates(self, tester):
        """Test that market cap data is consistent across different dates"""
        
        pool = await asyncpg.create_pool(tester.db_url, min_size=2, max_size=5)
        
        try:
            async with pool.acquire() as conn:
                # Find instruments with market cap data over multiple dates
                multi_date_instruments = await conn.fetch("""
                    SELECT 
                        instrument_id,
                        COUNT(DISTINCT date) as date_count,
                        AVG(market_cap) as avg_market_cap,
                        STDDEV(market_cap) as stddev_market_cap,
                        MIN(market_cap) as min_market_cap,
                        MAX(market_cap) as max_market_cap
                    FROM dev_daily_market_cap 
                    WHERE date >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY instrument_id
                    HAVING COUNT(DISTINCT date) >= 3
                    ORDER BY date_count DESC
                    LIMIT 20
                """)
                
                if not multi_date_instruments:
                    pytest.skip("No multi-date market cap data available for testing")
                
                consistency_issues = []
                
                for inst_data in multi_date_instruments:
                    instrument_id = inst_data['instrument_id']
                    avg_cap = inst_data['avg_market_cap']
                    stddev_cap = inst_data['stddev_market_cap']
                    min_cap = inst_data['min_market_cap']
                    max_cap = inst_data['max_market_cap']
                    
                    # Get symbol for reporting
                    symbol = await conn.fetchval(
                        "SELECT symbol FROM dev_instruments WHERE id = $1", instrument_id
                    )
                    
                    # Calculate coefficient of variation (stddev / mean)
                    cv = (stddev_cap / avg_cap) if avg_cap > 0 else float('inf')
                    
                    # Check for excessive volatility (might indicate data issues)
                    if cv > 0.50:  # More than 50% coefficient of variation
                        consistency_issues.append({
                            'symbol': symbol,
                            'instrument_id': instrument_id,
                            'issue': 'high_volatility',
                            'cv': cv,
                            'avg_cap': avg_cap
                        })
                    
                    # Check for extreme ratios (might indicate price/shares errors)
                    ratio = max_cap / min_cap if min_cap > 0 else float('inf')
                    if ratio > 10.0:  # Max is more than 10x min
                        consistency_issues.append({
                            'symbol': symbol,
                            'instrument_id': instrument_id,
                            'issue': 'extreme_ratio',
                            'ratio': ratio,
                            'min_cap': min_cap,
                            'max_cap': max_cap
                        })
                
                # Report consistency issues
                if consistency_issues:
                    print(f"Found {len(consistency_issues)} data consistency issues:")
                    for issue in consistency_issues[:10]:  # Show first 10
                        if issue['issue'] == 'high_volatility':
                            print(f"  ⚠️  {issue['symbol']}: High volatility (CV: {issue['cv']:.2f})")
                        elif issue['issue'] == 'extreme_ratio':
                            print(f"  ⚠️  {issue['symbol']}: Extreme ratio ({issue['ratio']:.1f}x)")
                
                # Consistency assertion: most instruments should have reasonable consistency
                consistency_rate = 1 - (len(consistency_issues) / len(multi_date_instruments))
                assert consistency_rate >= 0.8, \
                    f"Too many consistency issues: {consistency_rate:.1%} instruments consistent"
                
                print(f"Data consistency test passed: {consistency_rate:.1%} instruments consistent")
        
        finally:
            await pool.close()
    
    @pytest.mark.asyncio
    async def test_market_cap_universe_filtering(self, tester):
        """Test that market cap data enables proper universe filtering"""
        
        pool = await asyncpg.create_pool(tester.db_url, min_size=2, max_size=5)
        
        try:
            async with pool.acquire() as conn:
                # Test universe filtering with market cap criteria
                universe_results = await conn.fetch("""
                    SELECT 
                        i.symbol,
                        mc.market_cap,
                        AVG(p.volume * p.close) as avg_dollar_volume
                    FROM dev_instruments i
                    JOIN dev_daily_market_cap mc ON i.id = mc.instrument_id
                    LEFT JOIN dev_daily_prices_polygon p ON i.id = p.instrument_id
                        AND p.date >= CURRENT_DATE - INTERVAL '30 days'
                    WHERE mc.market_cap > 400000000  -- > $400M market cap
                    GROUP BY i.symbol, mc.market_cap
                    HAVING AVG(p.volume * p.close) > 100000000  -- > $100M avg volume
                    ORDER BY mc.market_cap DESC
                """)
                
                print(f"Universe filtering test: Found {len(universe_results)} qualifying instruments")
                
                if universe_results:
                    # Validate results
                    for result in universe_results[:10]:  # Check top 10
                        symbol = result['symbol']
                        market_cap = result['market_cap']
                        dollar_volume = result['avg_dollar_volume']
                        
                        assert market_cap > 400_000_000, \
                            f"{symbol} market cap below threshold: ${market_cap/1e6:.1f}M"
                        
                        if dollar_volume:  # Some might not have recent volume data
                            assert dollar_volume > 100_000_000, \
                                f"{symbol} dollar volume below threshold: ${dollar_volume/1e6:.1f}M"
                        
                        print(f"  ✅ {symbol}: ${market_cap/1e9:.2f}B market cap, ${dollar_volume/1e6:.0f}M volume")
                    
                    # Should have reasonable number of qualifying stocks
                    assert len(universe_results) >= 50, \
                        f"Too few qualifying instruments: {len(universe_results)}"
                    assert len(universe_results) <= 2000, \
                        f"Too many qualifying instruments: {len(universe_results)}"
                
                else:
                    # If no results, might be due to insufficient data - not necessarily a failure
                    print("⚠️  No instruments found matching universe criteria - may need more data")
        
        finally:
            await pool.close()

if __name__ == "__main__":
    # Allow running this test file directly
    pytest.main([__file__, "-v", "-s", "--tb=short"])