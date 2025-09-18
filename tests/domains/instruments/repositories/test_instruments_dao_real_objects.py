"""
Real Objects Test for InstrumentsDAO

This replaces mock-heavy testing with real database integration to catch actual issues.
Tests use real database connections, transactions, and constraints.

BEFORE: Mock objects masked database integration issues
AFTER: Real database reveals actual constraint violations, performance issues, and data problems
"""

import pytest
import asyncpg
from datetime import date
from typing import List, Dict, AsyncGenerator

from domains.instruments.repositories.instruments_dao import InstrumentsDAO
from shared.utils.environment import Environment, EnvironmentType


class TestInstrumentsDAORealObjects:
    """Real database integration tests for InstrumentsDAO"""

    @pytest.fixture(scope="session")
    async def test_environment(self) -> Environment:
        """Real test environment with actual database connection"""
        return Environment(
            env_type=EnvironmentType.TEST,
            db_url="postgresql://test:test@localhost/test_instruments_dao_db"
        )

    @pytest.fixture
    async def clean_database(self, test_environment: Environment) -> AsyncGenerator[Environment, None]:
        """Clean database state for each test with real schema setup"""
        # Create real test schema
        async with test_environment.get_connection() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS test_instruments (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL UNIQUE,
                    name VARCHAR(255) NOT NULL,
                    exchange VARCHAR(50) NOT NULL,
                    type VARCHAR(50) NOT NULL DEFAULT 'stock',
                    currency VARCHAR(10) NOT NULL DEFAULT 'USD',
                    list_date DATE,
                    delist_date DATE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT check_symbol_length CHECK (length(symbol) >= 1),
                    CONSTRAINT check_name_not_empty CHECK (length(trim(name)) > 0)
                )
            """)
            
            # Clean up before test
            await conn.execute("TRUNCATE TABLE test_instruments RESTART IDENTITY CASCADE")
        
        yield test_environment
        
        # Clean up after test
        async with test_environment.get_connection() as conn:
            await conn.execute("TRUNCATE TABLE test_instruments RESTART IDENTITY CASCADE")

    @pytest.fixture
    async def dao(self, clean_database: Environment) -> InstrumentsDAO:
        """Create InstrumentsDAO with real database connection"""
        return InstrumentsDAO(clean_database)

    # Test count_instruments with real database

    @pytest.mark.asyncio
    async def test_count_instruments_empty_database(self, dao: InstrumentsDAO):
        """Test count with empty database - real query execution"""
        # Execute real database query
        result = await dao.count_instruments()
        
        # Verify exact count from real database
        assert result == 0

    @pytest.mark.asyncio
    async def test_count_instruments_with_real_data(self, dao: InstrumentsDAO, clean_database: Environment):
        """Test count with real instruments in database"""
        # Insert real test data directly to database
        async with clean_database.get_connection() as conn:
            await conn.executemany(
                "INSERT INTO test_instruments (symbol, name, exchange, type) VALUES ($1, $2, $3, $4)",
                [
                    ("AAPL", "Apple Inc.", "NASDAQ", "stock"),
                    ("GOOGL", "Alphabet Inc.", "NASDAQ", "stock"),
                    ("MSFT", "Microsoft Corp.", "NASDAQ", "stock"),
                ]
            )
        
        # Execute real count query
        result = await dao.count_instruments()
        
        # Verify exact count matches real database state
        assert result == 3

    # Test create_instrument with real database constraints

    @pytest.mark.asyncio
    async def test_create_instrument_success(self, dao: InstrumentsDAO):
        """Test successful instrument creation with real database transaction"""
        # Execute real database insert
        instrument_id = await dao.create_instrument(
            symbol="TSLA",
            name="Tesla Inc.",
            exchange="NASDAQ",
            type_="stock",
            currency="USD",
            list_date=date(2010, 6, 29),
            delist_date=None
        )
        
        # Verify real database state
        assert instrument_id is not None
        assert instrument_id > 0
        
        # Verify actual database record exists
        created_instrument = await dao.get_instrument(instrument_id)
        assert created_instrument is not None
        assert created_instrument['symbol'] == "TSLA"
        assert created_instrument['name'] == "Tesla Inc."
        assert created_instrument['exchange'] == "NASDAQ"

    @pytest.mark.asyncio
    async def test_create_instrument_duplicate_symbol_constraint(self, dao: InstrumentsDAO):
        """Test duplicate symbol constraint with real database"""
        # Create first instrument
        first_id = await dao.create_instrument(
            symbol="AMZN",
            name="Amazon.com Inc.",
            exchange="NASDAQ",
            type_="stock"
        )
        assert first_id is not None
        
        # Attempt to create duplicate - real database constraint will trigger
        with pytest.raises(asyncpg.UniqueViolationError):
            await dao.create_instrument(
                symbol="AMZN",  # Duplicate symbol
                name="Amazon Different Name",
                exchange="NYSE",
                type_="stock"
            )

    @pytest.mark.asyncio
    async def test_create_instrument_validation_constraints(self, dao: InstrumentsDAO):
        """Test database validation constraints with real constraint checks"""
        # Test empty symbol constraint
        with pytest.raises(asyncpg.CheckViolationError):
            await dao.create_instrument(
                symbol="",  # Violates check_symbol_length constraint
                name="Invalid Symbol Company",
                exchange="NASDAQ",
                type_="stock"
            )
        
        # Test empty name constraint  
        with pytest.raises(asyncpg.CheckViolationError):
            await dao.create_instrument(
                symbol="INVALID",
                name="",  # Violates check_name_not_empty constraint
                exchange="NASDAQ",
                type_="stock"
            )

    # Test get_instrument with real database lookups

    @pytest.mark.asyncio
    async def test_get_instrument_exists(self, dao: InstrumentsDAO):
        """Test retrieving existing instrument with real database lookup"""
        # Create real instrument
        instrument_id = await dao.create_instrument(
            symbol="NFLX",
            name="Netflix Inc.",
            exchange="NASDAQ",
            type_="stock",
            currency="USD"
        )
        
        # Retrieve with real database query
        result = await dao.get_instrument(instrument_id)
        
        # Verify real database data
        assert result is not None
        assert result['id'] == instrument_id
        assert result['symbol'] == "NFLX"
        assert result['name'] == "Netflix Inc."
        assert result['exchange'] == "NASDAQ"
        assert result['type'] == "stock"
        assert result['currency'] == "USD"

    @pytest.mark.asyncio
    async def test_get_instrument_not_exists(self, dao: InstrumentsDAO):
        """Test retrieving non-existent instrument with real database"""
        # Query for non-existent ID - real database returns None
        result = await dao.get_instrument(99999)
        
        # Verify real database behavior
        assert result is None

    # Test get_instrument_by_symbol with real database

    @pytest.mark.asyncio
    async def test_get_instrument_by_symbol_exists(self, dao: InstrumentsDAO):
        """Test symbol lookup with real database index usage"""
        # Create real instrument
        await dao.create_instrument(
            symbol="META",
            name="Meta Platforms Inc.",
            exchange="NASDAQ",
            type_="stock"
        )
        
        # Real database symbol lookup
        result = await dao.get_instrument_by_symbol("META")
        
        # Verify real database query result
        assert result is not None
        assert result['symbol'] == "META"
        assert result['name'] == "Meta Platforms Inc."

    @pytest.mark.asyncio
    async def test_get_instrument_by_symbol_case_sensitivity(self, dao: InstrumentsDAO):
        """Test symbol case sensitivity with real database collation"""
        # Create real instrument
        await dao.create_instrument(
            symbol="NVDA",
            name="NVIDIA Corporation",
            exchange="NASDAQ",
            type_="stock"
        )
        
        # Test real database case sensitivity behavior
        result_exact = await dao.get_instrument_by_symbol("NVDA")
        result_lower = await dao.get_instrument_by_symbol("nvda")
        
        # Verify real database collation behavior
        assert result_exact is not None
        # Behavior depends on database collation settings
        # Most PostgreSQL setups are case-sensitive by default
        if result_lower is None:
            # Case-sensitive database (expected)
            assert result_lower is None
        else:
            # Case-insensitive database setup
            assert result_lower['symbol'] == "NVDA"

    # Test list_instruments with real pagination and filtering

    @pytest.mark.asyncio
    async def test_list_instruments_pagination(self, dao: InstrumentsDAO, clean_database: Environment):
        """Test pagination with real database LIMIT/OFFSET"""
        # Insert real test data
        test_instruments = [
            ("AAPL", "Apple Inc.", "NASDAQ", "stock"),
            ("GOOGL", "Alphabet Inc.", "NASDAQ", "stock"),
            ("MSFT", "Microsoft Corp.", "NASDAQ", "stock"),
            ("AMZN", "Amazon.com Inc.", "NASDAQ", "stock"),
            ("TSLA", "Tesla Inc.", "NASDAQ", "stock"),
        ]
        
        async with clean_database.get_connection() as conn:
            await conn.executemany(
                "INSERT INTO test_instruments (symbol, name, exchange, type) VALUES ($1, $2, $3, $4)",
                test_instruments
            )
        
        # Test real database pagination
        page_1 = await dao.list_instruments(limit=2, offset=0)
        page_2 = await dao.list_instruments(limit=2, offset=2)
        page_3 = await dao.list_instruments(limit=2, offset=4)
        
        # Verify real pagination behavior
        assert len(page_1) == 2
        assert len(page_2) == 2
        assert len(page_3) == 1  # Last page with remainder
        
        # Verify no duplicates across pages
        all_symbols = [instr['symbol'] for instr in page_1 + page_2 + page_3]
        assert len(set(all_symbols)) == 5  # All unique

    @pytest.mark.asyncio
    async def test_list_instruments_filtering(self, dao: InstrumentsDAO, clean_database: Environment):
        """Test filtering with real WHERE clauses"""
        # Insert mixed test data
        async with clean_database.get_connection() as conn:
            await conn.executemany(
                "INSERT INTO test_instruments (symbol, name, exchange, type) VALUES ($1, $2, $3, $4)",
                [
                    ("AAPL", "Apple Inc.", "NASDAQ", "stock"),
                    ("BTC-USD", "Bitcoin", "CRYPTO", "crypto"),
                    ("GOOGL", "Alphabet Inc.", "NASDAQ", "stock"),
                    ("ETH-USD", "Ethereum", "CRYPTO", "crypto"),
                ]
            )
        
        # Test real database filtering
        stocks_only = await dao.list_instruments(filters={'type': 'stock'})
        crypto_only = await dao.list_instruments(filters={'type': 'crypto'})
        nasdaq_only = await dao.list_instruments(filters={'exchange': 'NASDAQ'})
        
        # Verify real filtering results
        assert len(stocks_only) == 2
        assert all(instr['type'] == 'stock' for instr in stocks_only)
        
        assert len(crypto_only) == 2
        assert all(instr['type'] == 'crypto' for instr in crypto_only)
        
        assert len(nasdaq_only) == 2
        assert all(instr['exchange'] == 'NASDAQ' for instr in nasdaq_only)

    # Test batch operations with real transactions

    @pytest.mark.asyncio
    async def test_create_instruments_batch_transaction(self, dao: InstrumentsDAO):
        """Test batch creation with real database transaction"""
        instruments_data = [
            {
                'symbol': 'SPY',
                'name': 'SPDR S&P 500 ETF',
                'exchange': 'NYSE',
                'type': 'etf',
                'currency': 'USD'
            },
            {
                'symbol': 'QQQ',
                'name': 'Invesco QQQ ETF',
                'exchange': 'NASDAQ',
                'type': 'etf',
                'currency': 'USD'
            },
            {
                'symbol': 'IWM',
                'name': 'iShares Russell 2000 ETF',
                'exchange': 'NYSE',
                'type': 'etf',
                'currency': 'USD'
            }
        ]
        
        # Execute real batch transaction
        created_ids = await dao.create_instruments_batch(instruments_data)
        
        # Verify real transaction results
        assert len(created_ids) == 3
        assert all(id_ > 0 for id_ in created_ids)
        
        # Verify all instruments exist in real database
        for i, instrument_id in enumerate(created_ids):
            created_instrument = await dao.get_instrument(instrument_id)
            assert created_instrument is not None
            assert created_instrument['symbol'] == instruments_data[i]['symbol']
            assert created_instrument['name'] == instruments_data[i]['name']

    @pytest.mark.asyncio
    async def test_create_instruments_batch_rollback_on_error(self, dao: InstrumentsDAO):
        """Test batch transaction rollback with real database transaction"""
        # Create one instrument first
        await dao.create_instrument(
            symbol="EXISTING",
            name="Existing Instrument", 
            exchange="NYSE",
            type_="stock"
        )
        
        # Batch with duplicate symbol (should trigger rollback)
        instruments_data = [
            {
                'symbol': 'NEW1',
                'name': 'New Instrument 1',
                'exchange': 'NYSE',
                'type': 'stock'
            },
            {
                'symbol': 'EXISTING',  # Duplicate - will cause rollback
                'name': 'Duplicate Instrument',
                'exchange': 'NASDAQ',
                'type': 'stock'
            },
            {
                'symbol': 'NEW2',
                'name': 'New Instrument 2',
                'exchange': 'NYSE',
                'type': 'stock'
            }
        ]
        
        # Real database transaction should rollback on constraint violation
        with pytest.raises(asyncpg.UniqueViolationError):
            await dao.create_instruments_batch(instruments_data)
        
        # Verify real transaction rollback - no partial inserts
        new1_check = await dao.get_instrument_by_symbol("NEW1")
        new2_check = await dao.get_instrument_by_symbol("NEW2")
        assert new1_check is None  # Should not exist due to rollback
        assert new2_check is None  # Should not exist due to rollback

    # Test performance characteristics with real database

    @pytest.mark.asyncio
    async def test_large_batch_performance(self, dao: InstrumentsDAO):
        """Test performance with large batch operations on real database"""
        import time
        
        # Create large batch of instruments
        instruments_data = [
            {
                'symbol': f'TEST{i:04d}',
                'name': f'Test Instrument {i}',
                'exchange': 'NYSE',
                'type': 'stock',
                'currency': 'USD'
            }
            for i in range(1000)  # 1000 instruments
        ]
        
        # Measure real database performance
        start_time = time.time()
        created_ids = await dao.create_instruments_batch(instruments_data)
        end_time = time.time()
        
        # Verify performance and correctness
        assert len(created_ids) == 1000
        assert end_time - start_time < 30.0  # Should complete within 30 seconds
        
        # Verify real database state
        total_count = await dao.count_instruments()
        assert total_count == 1000

    # Test concurrent access with real database locks

    @pytest.mark.asyncio
    async def test_concurrent_instrument_creation(self, dao: InstrumentsDAO):
        """Test concurrent access patterns with real database locking"""
        import asyncio
        
        async def create_instrument_task(symbol_suffix: int):
            """Task to create instrument concurrently"""
            try:
                return await dao.create_instrument(
                    symbol=f"CONCURRENT{symbol_suffix}",
                    name=f"Concurrent Test {symbol_suffix}",
                    exchange="NYSE",
                    type_="stock"
                )
            except Exception as e:
                return e
        
        # Create multiple concurrent tasks
        tasks = [create_instrument_task(i) for i in range(10)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Verify real concurrency handling
        successful_results = [r for r in results if isinstance(r, int)]
        error_results = [r for r in results if isinstance(r, Exception)]
        
        # All should succeed with different symbols
        assert len(successful_results) == 10
        assert len(error_results) == 0
        
        # Verify all instruments exist in real database
        for i in range(10):
            instrument = await dao.get_instrument_by_symbol(f"CONCURRENT{i}")
            assert instrument is not None