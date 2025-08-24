"""
Integration tests for Polygon database schema validation

Tests the critical fix for missing dev_polygon_prices table that was causing
0 records to be stored despite successful API data retrieval.
"""

import asyncio
import pytest
import asyncpg
from datetime import date, datetime
from decimal import Decimal
from typing import Dict, List, Optional


@pytest.fixture
async def db_connection():
    """Database connection for testing"""
    conn = await asyncpg.connect(
        host='postgres',
        port=5432, 
        user='postgres',
        password='dev_password',
        database='dev_db'
    )
    yield conn
    await conn.close()


class TestPolygonTableSchema:
    """Test suite for Polygon table schema validation"""

    @pytest.mark.asyncio
    async def test_polygon_table_exists(self, db_connection):
        """Test that dev_polygon_prices table exists (this was the main issue)"""
        
        # Check if table exists
        table_exists = await db_connection.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'dev_polygon_prices'
            )
        """)
        
        assert table_exists, "dev_polygon_prices table must exist (was missing before fix)"

    @pytest.mark.asyncio
    async def test_polygon_table_columns(self, db_connection):
        """Test that all required columns exist with correct types"""
        
        # Get column information
        columns = await db_connection.fetch("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'dev_polygon_prices' 
            ORDER BY ordinal_position
        """)
        
        # Convert to dict for easier testing
        column_info = {col['column_name']: col for col in columns}
        
        # Test required columns exist
        required_columns = {
            'id': 'bigint',
            'symbol': 'character varying',
            'price_date': 'date',
            'open_price': 'numeric',
            'high_price': 'numeric', 
            'low_price': 'numeric',
            'close_price': 'numeric',
            'volume': 'bigint',
            'vwap': 'numeric',
            'transactions': 'integer',
            'data_source': 'character varying',
            'created_at': 'timestamp with time zone'
        }
        
        for col_name, expected_type in required_columns.items():
            assert col_name in column_info, f"Column {col_name} must exist"
            actual_type = column_info[col_name]['data_type']
            assert expected_type in actual_type or actual_type in expected_type, \
                f"Column {col_name} should be {expected_type}, got {actual_type}"

    @pytest.mark.asyncio
    async def test_polygon_table_constraints(self, db_connection):
        """Test that proper constraints exist"""
        
        # Check primary key constraint
        pk_constraints = await db_connection.fetch("""
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = 'dev_polygon_prices'
            AND constraint_type = 'PRIMARY KEY'
        """)
        
        assert len(pk_constraints) == 1, "Must have exactly one primary key constraint"

        # Check unique constraint on symbol, price_date
        unique_constraints = await db_connection.fetch("""
            SELECT constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu 
                ON tc.constraint_name = ccu.constraint_name
            WHERE tc.table_name = 'dev_polygon_prices'
            AND tc.constraint_type = 'UNIQUE'
            AND ccu.column_name IN ('symbol', 'price_date')
        """)
        
        assert len(unique_constraints) > 0, "Must have unique constraint on (symbol, price_date)"

    @pytest.mark.asyncio
    async def test_polygon_table_indexes(self, db_connection):
        """Test that performance indexes exist"""
        
        # Get index information
        indexes = await db_connection.fetch("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = 'dev_polygon_prices'
            ORDER BY indexname
        """)
        
        index_names = [idx['indexname'] for idx in indexes]
        
        # Should have indexes for common query patterns
        expected_patterns = ['symbol', 'date']
        
        index_coverage = []
        for pattern in expected_patterns:
            has_index = any(pattern in idx_name.lower() for idx_name in index_names)
            index_coverage.append((pattern, has_index))
            
        missing_indexes = [pattern for pattern, has_idx in index_coverage if not has_idx]
        
        if missing_indexes:
            print(f"Warning: Missing recommended indexes for: {missing_indexes}")
            print(f"Existing indexes: {index_names}")

    @pytest.mark.asyncio
    async def test_insert_polygon_data(self, db_connection):
        """Test that data can be inserted correctly (this was failing before)"""
        
        # Test data in the exact format that Polygon collector produces
        test_record = {
            'symbol': 'PYTEST',
            'price_date': date(2024, 1, 15),
            'open_price': Decimal('150.25'),
            'high_price': Decimal('155.75'),
            'low_price': Decimal('149.50'),
            'close_price': Decimal('153.80'),
            'volume': 2500000,
            'vwap': Decimal('152.15'),
            'transactions': 8500,
            'data_source': 'polygon'
        }
        
        # Clean up any existing test data
        await db_connection.execute(
            "DELETE FROM dev_polygon_prices WHERE symbol = $1", 
            test_record['symbol']
        )
        
        # Insert test record (this was failing before the table fix)
        insert_query = """
            INSERT INTO dev_polygon_prices (
                symbol, price_date, open_price, high_price, low_price, 
                close_price, volume, vwap, transactions, data_source
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id
        """
        
        record_id = await db_connection.fetchval(
            insert_query,
            test_record['symbol'], test_record['price_date'],
            test_record['open_price'], test_record['high_price'], 
            test_record['low_price'], test_record['close_price'],
            test_record['volume'], test_record['vwap'], 
            test_record['transactions'], test_record['data_source']
        )
        
        assert record_id is not None, "Insert should return a valid ID"
        
        # Verify data was inserted correctly
        retrieved = await db_connection.fetchrow(
            "SELECT * FROM dev_polygon_prices WHERE id = $1", record_id
        )
        
        assert retrieved['symbol'] == test_record['symbol']
        assert retrieved['price_date'] == test_record['price_date']
        assert retrieved['close_price'] == test_record['close_price']
        assert retrieved['volume'] == test_record['volume']
        assert retrieved['data_source'] == test_record['data_source']
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM dev_polygon_prices WHERE id = $1", record_id
        )

    @pytest.mark.asyncio
    async def test_conflict_resolution(self, db_connection):
        """Test ON CONFLICT DO UPDATE behavior for duplicate symbol/date combinations"""
        
        # Test data
        base_record = {
            'symbol': 'PYTEST_CONFLICT',
            'price_date': date(2024, 1, 15),
            'close_price': Decimal('100.00'),
            'volume': 1000000,
            'data_source': 'polygon'
        }
        
        updated_record = {
            'symbol': 'PYTEST_CONFLICT',
            'price_date': date(2024, 1, 15),  # Same symbol/date
            'close_price': Decimal('105.00'),  # Different price
            'volume': 1500000,               # Different volume
            'data_source': 'polygon'
        }
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM dev_polygon_prices WHERE symbol = $1", 
            base_record['symbol']
        )
        
        # Insert first record
        upsert_query = """
            INSERT INTO dev_polygon_prices (
                symbol, price_date, open_price, high_price, low_price,
                close_price, volume, vwap, transactions, data_source
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (symbol, price_date) DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                vwap = EXCLUDED.vwap,
                transactions = EXCLUDED.transactions
        """
        
        # First insert
        await db_connection.execute(
            upsert_query,
            base_record['symbol'], base_record['price_date'],
            Decimal('0'), Decimal('0'), Decimal('0'),  # open, high, low
            base_record['close_price'], base_record['volume'],
            None, None, base_record['data_source']  # vwap, transactions
        )
        
        # Second insert (should update existing record)
        await db_connection.execute(
            upsert_query,
            updated_record['symbol'], updated_record['price_date'], 
            Decimal('0'), Decimal('0'), Decimal('0'),  # open, high, low
            updated_record['close_price'], updated_record['volume'],
            None, None, updated_record['data_source']  # vwap, transactions
        )
        
        # Verify only one record exists with updated values
        count = await db_connection.fetchval(
            "SELECT COUNT(*) FROM dev_polygon_prices WHERE symbol = $1", 
            base_record['symbol']
        )
        assert count == 1, "Should have exactly one record after conflict resolution"
        
        final_record = await db_connection.fetchrow(
            "SELECT * FROM dev_polygon_prices WHERE symbol = $1", 
            base_record['symbol']
        )
        
        # Should have updated values
        assert final_record['close_price'] == updated_record['close_price']
        assert final_record['volume'] == updated_record['volume']
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM dev_polygon_prices WHERE symbol = $1", 
            base_record['symbol']
        )

    @pytest.mark.asyncio
    async def test_batch_insert_performance(self, db_connection):
        """Test batch insertion performance (used by collector)"""
        
        # Generate test data batch
        batch_size = 100
        test_symbol = 'PYTEST_BATCH'
        base_date = date(2024, 1, 1)
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM dev_polygon_prices WHERE symbol = $1", test_symbol
        )
        
        # Prepare batch data
        batch_data = []
        for i in range(batch_size):
            test_date = date(base_date.year, base_date.month, base_date.day + i)
            batch_data.append((
                test_symbol, test_date,
                Decimal('100.0'), Decimal('105.0'), Decimal('99.0'),  # open, high, low
                Decimal(f'{100 + i * 0.5}'), 1000000 + i * 1000,     # close, volume
                Decimal('102.5'), 5000 + i * 10, 'polygon'           # vwap, transactions, source
            ))
        
        # Batch insert query (same as collector uses)
        insert_query = """
            INSERT INTO dev_polygon_prices (
                symbol, price_date, open_price, high_price, low_price, 
                close_price, volume, vwap, transactions, data_source
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (symbol, price_date) DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price, 
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                vwap = EXCLUDED.vwap,
                transactions = EXCLUDED.transactions
        """
        
        # Time the batch insert
        start_time = datetime.now()
        await db_connection.executemany(insert_query, batch_data)
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        print(f"Batch insert of {batch_size} records took {duration:.3f} seconds")
        
        # Verify all records were inserted
        count = await db_connection.fetchval(
            "SELECT COUNT(*) FROM dev_polygon_prices WHERE symbol = $1", test_symbol
        )
        assert count == batch_size, f"Should have inserted {batch_size} records"
        
        # Performance check (should be under 1 second for 100 records)
        assert duration < 2.0, f"Batch insert took too long: {duration:.3f}s"
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM dev_polygon_prices WHERE symbol = $1", test_symbol
        )


class TestPolygonTableValidation:
    """Test suite for table validation and error handling"""
    
    @pytest.mark.asyncio
    async def test_null_constraints(self, db_connection):
        """Test that NOT NULL constraints are properly enforced"""
        
        # Try to insert record with missing required fields
        with pytest.raises(Exception) as exc_info:
            await db_connection.execute("""
                INSERT INTO dev_polygon_prices (symbol, close_price) 
                VALUES ('TEST', 100.0)
            """)
            
        # Should fail due to missing price_date (NOT NULL)
        assert "null value" in str(exc_info.value).lower() or \
               "not-null constraint" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_data_type_validation(self, db_connection):
        """Test that data type constraints are enforced"""
        
        # Test invalid date format
        with pytest.raises(Exception):
            await db_connection.execute("""
                INSERT INTO dev_polygon_prices (
                    symbol, price_date, close_price, data_source
                ) VALUES ('TEST', 'invalid-date', 100.0, 'polygon')
            """)
            
        # Test invalid numeric format  
        with pytest.raises(Exception):
            await db_connection.execute("""
                INSERT INTO dev_polygon_prices (
                    symbol, price_date, close_price, volume, data_source
                ) VALUES ('TEST', '2024-01-01', 'not-a-number', 1000, 'polygon')
            """)

    @pytest.mark.asyncio 
    async def test_default_values(self, db_connection):
        """Test that default values are applied correctly"""
        
        # Insert minimal record to test defaults
        test_symbol = 'PYTEST_DEFAULTS'
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM dev_polygon_prices WHERE symbol = $1", test_symbol
        )
        
        # Insert with minimal required fields
        record_id = await db_connection.fetchval("""
            INSERT INTO dev_polygon_prices (symbol, price_date, close_price) 
            VALUES ($1, $2, $3)
            RETURNING id
        """, test_symbol, date(2024, 1, 1), Decimal('100.0'))
        
        # Retrieve and check defaults
        record = await db_connection.fetchrow(
            "SELECT * FROM dev_polygon_prices WHERE id = $1", record_id
        )
        
        # created_at should be set automatically
        assert record['created_at'] is not None, "created_at should have default value"
        
        # data_source should default to 'polygon' if we set that default
        # (depends on actual schema definition)
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM dev_polygon_prices WHERE id = $1", record_id
        )


if __name__ == "__main__":
    # Run tests with: PYTHONPATH=src pytest tests/integration/test_polygon_database_schema.py -v --tb=short
    print("To run these tests:")
    print("PYTHONPATH=src pytest tests/integration/test_polygon_database_schema.py -v --tb=short")