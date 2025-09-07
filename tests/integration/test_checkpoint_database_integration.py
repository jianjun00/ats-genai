"""
Integration tests for Checkpoint Framework with real PostgreSQL database

These tests verify:
- Database schema creation and migration
- Checkpoint persistence and recovery
- Concurrent job execution
- Data integrity and consistency
- Performance with real database operations
"""

import pytest
import asyncio
import asyncpg
import json
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import os

# Test configuration
TEST_DATABASE_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'user': 'postgres',
    'password': 'dev_password',
    'database': 'dev_db'
}

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseTestCheckpointManager:
    """Real CheckpointManager implementation for integration tests"""

    def __init__(self, db_connection: asyncpg.Connection):
        self.conn = db_connection

    async def setup_checkpoint_tables(self):
        """Create checkpoint tables in test database"""
        logger.info("Setting up checkpoint tables...")

        # Drop existing test tables
        await self.conn.execute("""
            DROP TABLE IF EXISTS test_job_progress CASCADE;
            DROP TABLE IF EXISTS test_job_runs CASCADE;
            DROP TABLE IF EXISTS test_tiingo_prices CASCADE;
            DROP TABLE IF EXISTS test_fmp_prices CASCADE;
        """)

        # Create job runs table
        await self.conn.execute("""
            CREATE TABLE test_job_runs (
                id SERIAL PRIMARY KEY,
                job_id VARCHAR(100) UNIQUE NOT NULL,
                job_name VARCHAR(100) NOT NULL,
                vendor VARCHAR(50),
                iteration_type VARCHAR(20) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                current_position TEXT,
                processed_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                total_items INTEGER,
                last_successful_item TEXT,
                last_error_message TEXT,
                configuration JSONB,
                metadata JSONB DEFAULT '{}',
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT now(),
                updated_at TIMESTAMP DEFAULT now()
            );
        """)

        # Create job progress table
        await self.conn.execute("""
            CREATE TABLE test_job_progress (
                id SERIAL PRIMARY KEY,
                job_id VARCHAR(100) NOT NULL,
                item_key VARCHAR(200) NOT NULL,
                item_type VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                records_processed INTEGER DEFAULT 0,
                error_message TEXT,
                retry_count INTEGER DEFAULT 0,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT now(),
                UNIQUE(job_id, item_key, item_type)
            );
        """)

        # Create test price tables
        await self.conn.execute("""
            CREATE TABLE test_tiingo_prices (
                id BIGSERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                price_date DATE NOT NULL,
                open_price DECIMAL(12,4),
                high_price DECIMAL(12,4),
                low_price DECIMAL(12,4),
                close_price DECIMAL(12,4),
                adj_close_price DECIMAL(12,4),
                volume BIGINT,
                raw_data JSONB,
                job_id VARCHAR(100),
                collected_at TIMESTAMP DEFAULT now(),
                UNIQUE(symbol, price_date)
            );
        """)

        await self.conn.execute("""
            CREATE TABLE test_fmp_prices (
                id BIGSERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                price_date DATE NOT NULL,
                open_price DECIMAL(12,4),
                high_price DECIMAL(12,4),
                low_price DECIMAL(12,4),
                close_price DECIMAL(12,4),
                adj_close_price DECIMAL(12,4),
                volume BIGINT,
                change_percent DECIMAL(8,4),
                vwap DECIMAL(12,4),
                raw_data JSONB,
                job_id VARCHAR(100),
                collected_at TIMESTAMP DEFAULT now(),
                UNIQUE(symbol, price_date)
            );
        """)

        # Create indexes
        await self.conn.execute("""
            CREATE INDEX idx_test_job_runs_status ON test_job_runs(job_name, status, created_at);
            CREATE INDEX idx_test_job_progress_status ON test_job_progress(job_id, status);
            CREATE INDEX idx_test_job_progress_item ON test_job_progress(job_id, item_key, item_type);
            CREATE INDEX idx_test_tiingo_prices_symbol_date ON test_tiingo_prices(symbol, price_date);
            CREATE INDEX idx_test_fmp_prices_symbol_date ON test_fmp_prices(symbol, price_date);
        """)

        logger.info("✅ Checkpoint tables created successfully")

    async def create_job_run(self, job_name: str, vendor: str, iteration_type: str, total_items: int) -> str:
        """Create new job run in database"""
        job_id = f"{job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.getpid()}"

        await self.conn.execute("""
            INSERT INTO test_job_runs
            (job_id, job_name, vendor, iteration_type, total_items, started_at)
            VALUES ($1, $2, $3, $4, $5, now())
        """, job_id, job_name, vendor, iteration_type, total_items)

        return job_id

    async def initialize_items(self, job_id: str, items: List[str], item_type: str):
        """Initialize all items in progress table"""
        for item in items:
            await self.conn.execute("""
                INSERT INTO test_job_progress (job_id, item_key, item_type, status)
                VALUES ($1, $2, $3, 'pending')
                ON CONFLICT (job_id, item_key, item_type) DO NOTHING
            """, job_id, item, item_type)

    async def get_next_pending_items(self, job_id: str, item_type: str, batch_size: int) -> List[str]:
        """Get next batch of pending items"""
        rows = await self.conn.fetch("""
            SELECT item_key FROM test_job_progress
            WHERE job_id = $1 AND item_type = $2 AND status = 'pending'
            ORDER BY item_key
            LIMIT $3
        """, job_id, item_type, batch_size)
        return [row['item_key'] for row in rows]

    async def mark_item_processing(self, job_id: str, item_key: str, item_type: str):
        """Mark item as being processed"""
        await self.conn.execute("""
            UPDATE test_job_progress
            SET status = 'in_progress', started_at = now()
            WHERE job_id = $1 AND item_key = $2 AND item_type = $3
        """, job_id, item_key, item_type)

    async def mark_item_completed(self, job_id: str, item_key: str, item_type: str, records_count: int):
        """Mark item as completed"""
        await self.conn.execute("""
            UPDATE test_job_progress
            SET status = 'completed', records_processed = $4, completed_at = now()
            WHERE job_id = $1 AND item_key = $2 AND item_type = $3
        """, job_id, item_key, item_type, records_count)

    async def mark_item_failed(self, job_id: str, item_key: str, item_type: str, error_message: str):
        """Mark item as failed"""
        await self.conn.execute("""
            UPDATE test_job_progress
            SET status = 'failed', error_message = $4, completed_at = now()
            WHERE job_id = $1 AND item_key = $2 AND item_type = $3
        """, job_id, item_key, item_type, error_message)

    async def update_job_progress(self, job_id: str, processed_count: int, error_count: int, last_item: Optional[str]):
        """Update job-level progress"""
        await self.conn.execute("""
            UPDATE test_job_runs
            SET processed_count = $2, error_count = $3, last_successful_item = $4, updated_at = now()
            WHERE job_id = $1
        """, job_id, processed_count, error_count, last_item)

    async def mark_job_completed(self, job_id: str):
        """Mark job as completed"""
        await self.conn.execute("""
            UPDATE test_job_runs
            SET status = 'completed', completed_at = now()
            WHERE job_id = $1
        """, job_id)

    async def get_job_stats(self, job_id: str) -> Dict:
        """Get comprehensive job statistics"""
        stats = await self.conn.fetchrow("""
            SELECT
                COUNT(*) as total_items,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                COUNT(CASE WHEN status = 'in_progress' THEN 1 END) as in_progress,
                COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
                COALESCE(SUM(records_processed), 0) as total_records
            FROM test_job_progress
            WHERE job_id = $1
        """, job_id)

        return dict(stats) if stats else {}

    async def store_tiingo_prices(self, job_id: str, symbol: str, prices: List[Dict]) -> int:
        """Store Tiingo prices in database"""
        stored_count = 0
        for price in prices:
            try:
                await self.conn.execute("""
                    INSERT INTO test_tiingo_prices
                    (symbol, price_date, open_price, high_price, low_price, close_price,
                     adj_close_price, volume, raw_data, job_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (symbol, price_date) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        adj_close_price = EXCLUDED.adj_close_price,
                        volume = EXCLUDED.volume,
                        raw_data = EXCLUDED.raw_data,
                        job_id = EXCLUDED.job_id
                """, symbol, price['date'], price['open'], price['high'],
                    price['low'], price['close'], price['adj_close'],
                    price['volume'], json.dumps(price['raw_data']), job_id)
                stored_count += 1
            except Exception as e:
                logger.warning(f"Error storing price for {symbol}: {e}")
        return stored_count

    async def cleanup_test_tables(self):
        """Clean up test tables"""
        await self.conn.execute("""
            DROP TABLE IF EXISTS test_job_progress CASCADE;
            DROP TABLE IF EXISTS test_job_runs CASCADE;
            DROP TABLE IF EXISTS test_tiingo_prices CASCADE;
            DROP TABLE IF EXISTS test_fmp_prices CASCADE;
        """)

class TestDatabaseIntegration:
    """Database integration tests"""

    @pytest.fixture
    async def db_connection(self):
        """Real database connection for integration tests"""
        try:
            conn = await asyncpg.connect(**TEST_DATABASE_CONFIG)
            yield conn
        except Exception as e:
            pytest.skip(f"Cannot connect to test database: {e}")
        finally:
            if 'conn' in locals():
                await conn.close()

    @pytest.fixture
    async def checkpoint_manager(self, db_connection):
        """Checkpoint manager with real database"""
        manager = DatabaseTestCheckpointManager(db_connection)
        await manager.setup_checkpoint_tables()
        yield manager
        await manager.cleanup_test_tables()

    @pytest.mark.asyncio

    async def test_checkpoint_table_creation(self, checkpoint_manager, db_connection):
        """Test that checkpoint tables are created correctly"""
        # Verify tables exist
        tables = await db_connection.fetch("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name LIKE 'test_%'
        """)

        table_names = [row['table_name'] for row in tables]
        expected_tables = ['test_job_runs', 'test_job_progress', 'test_tiingo_prices', 'test_fmp_prices']

        for table in expected_tables:
            assert table in table_names, f"Table {table} not created"

        # Verify indexes exist
        indexes = await db_connection.fetch("""
            SELECT indexname FROM pg_indexes
            WHERE tablename LIKE 'test_%'
        """)

        index_names = [row['indexname'] for row in indexes]
        assert any('idx_test_job_runs_status' in idx for idx in index_names)
        assert any('idx_test_job_progress_status' in idx for idx in index_names)

    @pytest.mark.asyncio

    async def test_job_run_crud_operations(self, checkpoint_manager):
        """Test basic CRUD operations on job runs"""
        # Create job run
        job_id = await checkpoint_manager.create_job_run(
            "test_crud_job", "tiingo", "instrument", 10
        )

        assert job_id.startswith("test_crud_job_")

        # Verify job created in database
        job_data = await checkpoint_manager.conn.fetchrow("""
            SELECT * FROM test_job_runs WHERE job_id = $1
        """, job_id)

        assert job_data is not None
        assert job_data['job_name'] == "test_crud_job"
        assert job_data['vendor'] == "tiingo"
        assert job_data['total_items'] == 10
        assert job_data['status'] == 'pending'

        # Update job progress
        await checkpoint_manager.update_job_progress(job_id, 5, 1, 'AAPL')

        updated_job = await checkpoint_manager.conn.fetchrow("""
            SELECT processed_count, error_count, last_successful_item
            FROM test_job_runs WHERE job_id = $1
        """, job_id)

        assert updated_job['processed_count'] == 5
        assert updated_job['error_count'] == 1
        assert updated_job['last_successful_item'] == 'AAPL'

        # Mark job completed
        await checkpoint_manager.mark_job_completed(job_id)

        final_job = await checkpoint_manager.conn.fetchrow("""
            SELECT status, completed_at FROM test_job_runs WHERE job_id = $1
        """, job_id)

        assert final_job['status'] == 'completed'
        assert final_job['completed_at'] is not None

    @pytest.mark.asyncio

    async def test_item_progress_tracking(self, checkpoint_manager):
        """Test item progress tracking functionality"""
        job_id = await checkpoint_manager.create_job_run(
            "test_progress", "tiingo", "instrument", 5
        )

        symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']

        # Initialize items
        await checkpoint_manager.initialize_items(job_id, symbols, 'instrument')

        # Verify all items initialized
        count = await checkpoint_manager.conn.fetchval("""
            SELECT COUNT(*) FROM test_job_progress WHERE job_id = $1
        """, job_id)
        assert count == 5

        # Test item status transitions
        await checkpoint_manager.mark_item_processing(job_id, 'AAPL', 'instrument')
        await checkpoint_manager.mark_item_completed(job_id, 'AAPL', 'instrument', 100)

        aapl_status = await checkpoint_manager.conn.fetchrow("""
            SELECT status, records_processed, completed_at
            FROM test_job_progress
            WHERE job_id = $1 AND item_key = 'AAPL'
        """, job_id)

        assert aapl_status['status'] == 'completed'
        assert aapl_status['records_processed'] == 100
        assert aapl_status['completed_at'] is not None

        # Test failure tracking
        await checkpoint_manager.mark_item_failed(job_id, 'MSFT', 'instrument', 'API Error')

        msft_status = await checkpoint_manager.conn.fetchrow("""
            SELECT status, error_message
            FROM test_job_progress
            WHERE job_id = $1 AND item_key = 'MSFT'
        """, job_id)

        assert msft_status['status'] == 'failed'
        assert msft_status['error_message'] == 'API Error'

        # Test getting next pending items
        pending_items = await checkpoint_manager.get_next_pending_items(job_id, 'instrument', 5)
        assert len(pending_items) == 3  # GOOGL, TSLA, NVDA still pending
        assert 'AAPL' not in pending_items  # Completed
        assert 'MSFT' not in pending_items  # Failed

    @pytest.mark.asyncio

    async def test_job_statistics_calculation(self, checkpoint_manager):
        """Test job statistics calculation with real database aggregation"""
        job_id = await checkpoint_manager.create_job_run(
            "test_stats", "tiingo", "instrument", 6
        )

        symbols = ['SYM1', 'SYM2', 'SYM3', 'SYM4', 'SYM5', 'SYM6']
        await checkpoint_manager.initialize_items(job_id, symbols, 'instrument')

        # Create various states
        await checkpoint_manager.mark_item_completed(job_id, 'SYM1', 'instrument', 50)
        await checkpoint_manager.mark_item_completed(job_id, 'SYM2', 'instrument', 75)
        await checkpoint_manager.mark_item_failed(job_id, 'SYM3', 'instrument', 'Error')
        await checkpoint_manager.mark_item_processing(job_id, 'SYM4', 'instrument')
        # SYM5, SYM6 remain pending

        stats = await checkpoint_manager.get_job_stats(job_id)

        assert stats['total_items'] == 6
        assert stats['completed'] == 2
        assert stats['failed'] == 1
        assert stats['in_progress'] == 1
        assert stats['pending'] == 2
        assert stats['total_records'] == 125  # 50 + 75

    @pytest.mark.asyncio

    async def test_price_data_storage_and_retrieval(self, checkpoint_manager):
        """Test storing and retrieving price data"""
        job_id = await checkpoint_manager.create_job_run(
            "test_prices", "tiingo", "instrument", 1
        )

        # Generate test price data
        test_prices = [
            {
                'date': date(2024, 1, i),
                'open': 100.0 + i,
                'high': 105.0 + i,
                'low': 95.0 + i,
                'close': 102.0 + i,
                'adj_close': 102.0 + i,
                'volume': 1000000 + i * 10000,
                'raw_data': {'test': f'data_{i}'}
            }
            for i in range(1, 11)  # 10 days of data
        ]

        # Store prices
        stored_count = await checkpoint_manager.store_tiingo_prices(job_id, 'AAPL', test_prices)
        assert stored_count == 10

        # Retrieve and verify prices
        stored_prices = await checkpoint_manager.conn.fetch("""
            SELECT symbol, price_date, open_price, close_price, volume, raw_data
            FROM test_tiingo_prices
            WHERE job_id = $1 AND symbol = 'AAPL'
            ORDER BY price_date
        """, job_id)

        assert len(stored_prices) == 10

        first_price = stored_prices[0]
        assert first_price['symbol'] == 'AAPL'
        assert first_price['price_date'] == date(2024, 1, 1)
        assert float(first_price['open_price']) == 101.0
        assert first_price['raw_data']['test'] == 'data_1'

        # Test duplicate handling (ON CONFLICT DO UPDATE)
        updated_prices = [
            {
                'date': date(2024, 1, 1),
                'open': 200.0,  # Changed value
                'high': 205.0,
                'low': 195.0,
                'close': 202.0,
                'adj_close': 202.0,
                'volume': 2000000,
                'raw_data': {'test': 'updated_data_1'}
            }
        ]

        update_count = await checkpoint_manager.store_tiingo_prices(job_id, 'AAPL', updated_prices)
        assert update_count == 1

        updated_price = await checkpoint_manager.conn.fetchrow("""
            SELECT open_price, raw_data FROM test_tiingo_prices
            WHERE job_id = $1 AND symbol = 'AAPL' AND price_date = '2024-01-01'
        """, job_id)

        assert float(updated_price['open_price']) == 200.0
        assert updated_price['raw_data']['test'] == 'updated_data_1'

class TestConcurrentJobExecution:
    """Test concurrent job execution scenarios"""

    @pytest.fixture
    async def db_connection(self):
        """Database connection for concurrency tests"""
        try:
            conn = await asyncpg.connect(**TEST_DATABASE_CONFIG)
            yield conn
        except Exception as e:
            pytest.skip(f"Cannot connect to test database: {e}")
        finally:
            if 'conn' in locals():
                await conn.close()

    @pytest.fixture
    async def checkpoint_manager(self, db_connection):
        """Checkpoint manager for concurrency tests"""
        manager = DatabaseTestCheckpointManager(db_connection)
        await manager.setup_checkpoint_tables()
        yield manager
        await manager.cleanup_test_tables()

    @pytest.mark.asyncio

    async def test_concurrent_job_isolation(self, checkpoint_manager):
        """Test that concurrent jobs don't interfere with each other"""

        async def run_job(job_name: str, vendor: str, symbols: List[str]):
            """Run a single job to completion"""
            job_id = await checkpoint_manager.create_job_run(
                job_name, vendor, "instrument", len(symbols)
            )

            await checkpoint_manager.initialize_items(job_id, symbols, 'instrument')

            # Process all items
            for symbol in symbols:
                await checkpoint_manager.mark_item_processing(job_id, symbol, 'instrument')
                await asyncio.sleep(0.01)  # Simulate processing time
                await checkpoint_manager.mark_item_completed(job_id, symbol, 'instrument', 10)

            await checkpoint_manager.mark_job_completed(job_id)
            return job_id

        # Run three jobs concurrently
        job1_symbols = ['AAPL', 'MSFT', 'GOOGL']
        job2_symbols = ['TSLA', 'NVDA', 'AMD']
        job3_symbols = ['META', 'NFLX', 'AMZN']

        job_ids = await asyncio.gather(
            run_job("concurrent_job_1", "tiingo", job1_symbols),
            run_job("concurrent_job_2", "fmp", job2_symbols),
            run_job("concurrent_job_3", "polygon", job3_symbols)
        )

        # Verify all jobs completed successfully
        for job_id in job_ids:
            stats = await checkpoint_manager.get_job_stats(job_id)
            assert stats['completed'] == 3
            assert stats['failed'] == 0

        # Verify job isolation - each job only processed its own symbols
        job1_items = await checkpoint_manager.conn.fetch("""
            SELECT item_key FROM test_job_progress WHERE job_id = $1
        """, job_ids[0])
        job1_processed = [row['item_key'] for row in job1_items]

        for symbol in job1_symbols:
            assert symbol in job1_processed
        for symbol in job2_symbols + job3_symbols:
            assert symbol not in job1_processed

    @pytest.mark.asyncio

    async def test_concurrent_item_processing_same_job(self, checkpoint_manager):
        """Test concurrent processing of items within the same job"""
        job_id = await checkpoint_manager.create_job_run(
            "concurrent_items", "tiingo", "instrument", 10
        )

        symbols = [f'SYM_{i:02d}' for i in range(10)]
        await checkpoint_manager.initialize_items(job_id, symbols, 'instrument')

        async def process_item(symbol: str):
            """Process a single item"""
            await checkpoint_manager.mark_item_processing(job_id, symbol, 'instrument')
            await asyncio.sleep(0.02)  # Simulate API call
            await checkpoint_manager.mark_item_completed(job_id, symbol, 'instrument', 5)

        # Process all items concurrently
        start_time = datetime.now()
        await asyncio.gather(*[process_item(symbol) for symbol in symbols])
        end_time = datetime.now()

        # Verify all completed
        stats = await checkpoint_manager.get_job_stats(job_id)
        assert stats['completed'] == 10
        assert stats['failed'] == 0
        assert stats['total_records'] == 50

        # Verify concurrent processing was faster than sequential
        duration = (end_time - start_time).total_seconds()
        assert duration < 0.15  # Should be much faster than 10 * 0.02 = 0.2s

    @pytest.mark.asyncio

    async def test_database_connection_under_load(self, checkpoint_manager):
        """Test database performance under concurrent load"""

        async def create_and_process_job(job_index: int):
            """Create and process a job with many items"""
            job_id = await checkpoint_manager.create_job_run(
                f"load_test_{job_index}", "test_vendor", "instrument", 50
            )

            symbols = [f'JOB{job_index}_SYM_{i:02d}' for i in range(50)]
            await checkpoint_manager.initialize_items(job_id, symbols, 'instrument')

            # Process items in batches to simulate realistic usage
            batch_size = 5
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]

                # Process batch concurrently
                async def process_batch_item(symbol):
                    await checkpoint_manager.mark_item_processing(job_id, symbol, 'instrument')
                    await asyncio.sleep(0.001)  # Minimal processing time
                    await checkpoint_manager.mark_item_completed(job_id, symbol, 'instrument', 1)

                await asyncio.gather(*[process_batch_item(symbol) for symbol in batch])

            await checkpoint_manager.mark_job_completed(job_id)
            return job_id

        # Run multiple jobs concurrently to create database load
        num_jobs = 5
        start_time = datetime.now()

        job_ids = await asyncio.gather(*[
            create_and_process_job(i) for i in range(num_jobs)
        ])

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Verify all jobs completed
        total_items_processed = 0
        for job_id in job_ids:
            stats = await checkpoint_manager.get_job_stats(job_id)
            assert stats['completed'] == 50
            assert stats['failed'] == 0
            total_items_processed += stats['completed']

        assert total_items_processed == 250  # 5 jobs * 50 items each

        # Verify reasonable performance (should process ~50 items/second under load)
        processing_rate = total_items_processed / duration
        logger.info(f"Database load test: {processing_rate:.1f} items/second")
        assert processing_rate >= 30, f"Database performance too slow: {processing_rate:.1f} items/sec"

class TestJobRecoveryAndResumption:
    """Test job recovery and resumption scenarios"""

    @pytest.fixture
    async def db_connection(self):
        """Database connection for recovery tests"""
        try:
            conn = await asyncpg.connect(**TEST_DATABASE_CONFIG)
            yield conn
        except Exception as e:
            pytest.skip(f"Cannot connect to test database: {e}")
        finally:
            if 'conn' in locals():
                await conn.close()

    @pytest.fixture
    async def checkpoint_manager(self, db_connection):
        """Checkpoint manager for recovery tests"""
        manager = DatabaseTestCheckpointManager(db_connection)
        await manager.setup_checkpoint_tables()
        yield manager
        await manager.cleanup_test_tables()

    @pytest.mark.asyncio

    async def test_job_resumption_after_partial_completion(self, checkpoint_manager):
        """Test resuming job after partial completion"""
        # Start initial job
        job_id = await checkpoint_manager.create_job_run(
            "resumption_test", "tiingo", "instrument", 5
        )

        symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
        await checkpoint_manager.initialize_items(job_id, symbols, 'instrument')

        # Process first two items
        await checkpoint_manager.mark_item_completed(job_id, 'AAPL', 'instrument', 100)
        await checkpoint_manager.mark_item_completed(job_id, 'MSFT', 'instrument', 150)
        await checkpoint_manager.update_job_progress(job_id, 2, 0, 'MSFT')

        # Simulate job interruption (job not marked complete)
        initial_stats = await checkpoint_manager.get_job_stats(job_id)
        assert initial_stats['completed'] == 2
        assert initial_stats['pending'] == 3

        # Resume job - get remaining items and process them
        remaining_items = await checkpoint_manager.get_next_pending_items(job_id, 'instrument', 10)
        assert len(remaining_items) == 3
        assert 'AAPL' not in remaining_items  # Already completed
        assert 'MSFT' not in remaining_items  # Already completed

        # Process remaining items
        for symbol in remaining_items:
            await checkpoint_manager.mark_item_completed(job_id, symbol, 'instrument', 200)

        await checkpoint_manager.mark_job_completed(job_id)

        # Verify full completion
        final_stats = await checkpoint_manager.get_job_stats(job_id)
        assert final_stats['completed'] == 5
        assert final_stats['pending'] == 0
        assert final_stats['total_records'] == 850  # 100 + 150 + 3*200

    @pytest.mark.asyncio

    async def test_failed_item_retry_logic(self, checkpoint_manager):
        """Test retry logic for failed items"""
        job_id = await checkpoint_manager.create_job_run(
            "retry_test", "tiingo", "instrument", 3
        )

        symbols = ['RETRY1', 'RETRY2', 'RETRY3']
        await checkpoint_manager.initialize_items(job_id, symbols, 'instrument')

        # First attempt - some items fail
        await checkpoint_manager.mark_item_completed(job_id, 'RETRY1', 'instrument', 10)
        await checkpoint_manager.mark_item_failed(job_id, 'RETRY2', 'instrument', 'Temporary API error')
        await checkpoint_manager.mark_item_failed(job_id, 'RETRY3', 'instrument', 'Rate limit exceeded')

        initial_stats = await checkpoint_manager.get_job_stats(job_id)
        assert initial_stats['completed'] == 1
        assert initial_stats['failed'] == 2

        # Simulate retry - reset failed items to pending
        await checkpoint_manager.conn.execute("""
            UPDATE test_job_progress
            SET status = 'pending', error_message = NULL, retry_count = retry_count + 1
            WHERE job_id = $1 AND status = 'failed'
        """, job_id)

        # Get items for retry
        retry_items = await checkpoint_manager.get_next_pending_items(job_id, 'instrument', 10)
        assert len(retry_items) == 2
        assert 'RETRY2' in retry_items
        assert 'RETRY3' in retry_items

        # Retry processing - this time succeed
        await checkpoint_manager.mark_item_completed(job_id, 'RETRY2', 'instrument', 15)
        await checkpoint_manager.mark_item_completed(job_id, 'RETRY3', 'instrument', 20)

        final_stats = await checkpoint_manager.get_job_stats(job_id)
        assert final_stats['completed'] == 3
        assert final_stats['failed'] == 0
        assert final_stats['total_records'] == 45  # 10 + 15 + 20

        # Verify retry count was incremented
        retry_counts = await checkpoint_manager.conn.fetch("""
            SELECT item_key, retry_count FROM test_job_progress WHERE job_id = $1
        """, job_id)

        for row in retry_counts:
            if row['item_key'] in ['RETRY2', 'RETRY3']:
                assert row['retry_count'] == 1  # One retry
            else:
                assert row['retry_count'] == 0  # No retry needed

    @pytest.mark.asyncio

    async def test_data_consistency_after_interruption(self, checkpoint_manager):
        """Test data consistency after job interruption"""
        job_id = await checkpoint_manager.create_job_run(
            "consistency_test", "tiingo", "instrument", 4
        )

        symbols = ['CONS1', 'CONS2', 'CONS3', 'CONS4']
        await checkpoint_manager.initialize_items(job_id, symbols, 'instrument')

        # Start processing items
        await checkpoint_manager.mark_item_processing(job_id, 'CONS1', 'instrument')
        await checkpoint_manager.mark_item_processing(job_id, 'CONS2', 'instrument')

        # Complete one item, leave one in processing state (simulates interruption)
        await checkpoint_manager.mark_item_completed(job_id, 'CONS1', 'instrument', 50)

        # Store some price data
        test_prices = [
            {
                'date': date(2024, 1, 1),
                'open': 100.0,
                'high': 105.0,
                'low': 95.0,
                'close': 102.0,
                'adj_close': 102.0,
                'volume': 1000000,
                'raw_data': {'symbol': 'CONS1'}
            }
        ]
        await checkpoint_manager.store_tiingo_prices(job_id, 'CONS1', test_prices)

        # Check current state
        current_stats = await checkpoint_manager.get_job_stats(job_id)
        assert current_stats['completed'] == 1
        assert current_stats['in_progress'] == 1  # CONS2 left in processing
        assert current_stats['pending'] == 2  # CONS3, CONS4 not started

        # Simulate recovery - reset in_progress items to pending
        await checkpoint_manager.conn.execute("""
            UPDATE test_job_progress
            SET status = 'pending', started_at = NULL
            WHERE job_id = $1 AND status = 'in_progress'
        """, job_id)

        # Verify recovery state
        recovery_stats = await checkpoint_manager.get_job_stats(job_id)
        assert recovery_stats['completed'] == 1  # Still completed
        assert recovery_stats['in_progress'] == 0  # Reset
        assert recovery_stats['pending'] == 3  # CONS2, CONS3, CONS4 now pending

        # Verify price data was not affected
        price_count = await checkpoint_manager.conn.fetchval("""
            SELECT COUNT(*) FROM test_tiingo_prices WHERE job_id = $1
        """, job_id)
        assert price_count == 1

        # Complete recovery by processing remaining items
        remaining_items = await checkpoint_manager.get_next_pending_items(job_id, 'instrument', 10)
        for symbol in remaining_items:
            await checkpoint_manager.mark_item_completed(job_id, symbol, 'instrument', 25)

        final_stats = await checkpoint_manager.get_job_stats(job_id)
        assert final_stats['completed'] == 4
        assert final_stats['total_records'] == 125  # 50 + 3*25

# Test runner configuration
if __name__ == "__main__":
    # Test configuration
    pytest.main(["-v", __file__, "-s"])