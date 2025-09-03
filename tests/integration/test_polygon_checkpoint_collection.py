"""
Integration tests for Polygon checkpoint-based data collection

Tests the checkpoint system that enables fault-tolerant 30-year price collection
across all instruments with resumable progress tracking.
"""

import asyncio
import pytest
import asyncpg
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional
from unittest.mock import AsyncMock, patch, MagicMock


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


@pytest.fixture
def test_job_id():
    """Test job ID for checkpoint tracking"""
    return f"test-polygon-{datetime.now().strftime('%Y%m%d-%H%M%S')}"


class TestPolygonCheckpointSystem:
    """Test suite for Polygon checkpoint-based collection system"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_job_initialization(self, db_connection, test_job_id):
        """Test that job progress tracking initializes correctly"""
        
        # Clean up any existing test data
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id = $1", test_job_id
        )
        
        # Simulate job initialization with test symbols
        test_symbols = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]
        
        # Initialize progress tracking (same as collector does)
        for symbol in test_symbols:
            await db_connection.execute("""
                INSERT INTO vendor_job_progress (
                    job_id, vendor, symbol, status, created_at
                ) VALUES ($1, 'polygon', $2, 'pending', NOW())
                ON CONFLICT (job_id, vendor, symbol) DO NOTHING
            """, test_job_id, symbol)
        
        # Verify initialization
        progress_count = await db_connection.fetchval("""
            SELECT COUNT(*) FROM vendor_job_progress 
            WHERE job_id = $1 AND vendor = 'polygon'
        """, test_job_id)
        
        assert progress_count == len(test_symbols), \
            f"Should initialize tracking for {len(test_symbols)} symbols"
        
        # Verify all symbols are in pending status
        pending_count = await db_connection.fetchval("""
            SELECT COUNT(*) FROM vendor_job_progress 
            WHERE job_id = $1 AND vendor = 'polygon' AND status = 'pending'
        """, test_job_id)
        
        assert pending_count == len(test_symbols), \
            "All symbols should start in pending status"
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id = $1", test_job_id
        )

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_symbol_status_transitions(self, db_connection, test_job_id):
        """Test symbol status transitions: pending -> processing -> completed/failed"""
        
        test_symbol = "AAPL"
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id = $1 AND symbol = $2", 
            test_job_id, test_symbol
        )
        
        # 1. Initialize as pending
        await db_connection.execute("""
            INSERT INTO vendor_job_progress (
                job_id, vendor, symbol, status, created_at
            ) VALUES ($1, 'polygon', $2, 'pending', NOW())
        """, test_job_id, test_symbol)
        
        status = await db_connection.fetchval("""
            SELECT status FROM vendor_job_progress 
            WHERE job_id = $1 AND symbol = $2
        """, test_job_id, test_symbol)
        assert status == 'pending', "Should start in pending status"
        
        # 2. Mark as processing
        await db_connection.execute("""
            UPDATE vendor_job_progress 
            SET status = 'processing', started_at = NOW()
            WHERE job_id = $1 AND vendor = 'polygon' AND symbol = $2
        """, test_job_id, test_symbol)
        
        status = await db_connection.fetchval("""
            SELECT status FROM vendor_job_progress 
            WHERE job_id = $1 AND symbol = $2
        """, test_job_id, test_symbol)
        assert status == 'processing', "Should transition to processing status"
        
        # 3a. Mark as completed
        records_collected = 1500
        await db_connection.execute("""
            UPDATE vendor_job_progress 
            SET status = 'completed', completed_at = NOW(), records_collected = $3
            WHERE job_id = $1 AND vendor = 'polygon' AND symbol = $2
        """, test_job_id, test_symbol, records_collected)
        
        result = await db_connection.fetchrow("""
            SELECT status, records_collected, completed_at FROM vendor_job_progress 
            WHERE job_id = $1 AND symbol = $2
        """, test_job_id, test_symbol)
        
        assert result['status'] == 'completed', "Should transition to completed status"
        assert result['records_collected'] == records_collected, \
            "Should store records collected count"
        assert result['completed_at'] is not None, "Should set completion timestamp"
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id = $1 AND symbol = $2", 
            test_job_id, test_symbol
        )

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_error_handling_and_failure_tracking(self, db_connection, test_job_id):
        """Test error handling and failure status tracking"""
        
        test_symbol = "BADTICKER"
        error_message = "API returned 404 - Symbol not found"
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id = $1 AND symbol = $2", 
            test_job_id, test_symbol
        )
        
        # Initialize and mark as processing
        await db_connection.execute("""
            INSERT INTO vendor_job_progress (
                job_id, vendor, symbol, status, created_at
            ) VALUES ($1, 'polygon', $2, 'processing', NOW())
        """, test_job_id, test_symbol)
        
        # Mark as failed with error message
        await db_connection.execute("""
            UPDATE vendor_job_progress 
            SET status = 'failed', completed_at = NOW(), error_message = $3
            WHERE job_id = $1 AND vendor = 'polygon' AND symbol = $2
        """, test_job_id, test_symbol, error_message[:500])  # Truncate like collector does
        
        result = await db_connection.fetchrow("""
            SELECT status, error_message, completed_at FROM vendor_job_progress 
            WHERE job_id = $1 AND symbol = $2
        """, test_job_id, test_symbol)
        
        assert result['status'] == 'failed', "Should mark as failed status"
        assert error_message in result['error_message'], "Should store error message"
        assert result['completed_at'] is not None, "Should set completion timestamp even for failures"
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id = $1 AND symbol = $2", 
            test_job_id, test_symbol
        )

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_progress_statistics(self, db_connection, test_job_id):
        """Test progress statistics calculation"""
        
        test_symbols = [
            ("AAPL", "completed", 1500),
            ("MSFT", "completed", 1200), 
            ("GOOGL", "failed", 0),
            ("TSLA", "processing", None),
            ("AMZN", "pending", None)
        ]
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id = $1", test_job_id
        )
        
        # Set up test data
        for symbol, status, records in test_symbols:
            if status == 'pending':
                await db_connection.execute("""
                    INSERT INTO vendor_job_progress (
                        job_id, vendor, symbol, status, created_at
                    ) VALUES ($1, 'polygon', $2, $3, NOW())
                """, test_job_id, symbol, status)
            else:
                await db_connection.execute("""
                    INSERT INTO vendor_job_progress (
                        job_id, vendor, symbol, status, created_at, completed_at, records_collected
                    ) VALUES ($1, 'polygon', $2, $3, NOW(), NOW(), $4)
                """, test_job_id, symbol, status, records)
        
        # Calculate statistics (same query as collector uses)
        stats = await db_connection.fetchrow("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing,
                COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending
            FROM vendor_job_progress 
            WHERE job_id = $1 AND vendor = 'polygon'
        """, test_job_id)
        
        assert stats['total'] == 5, "Should count all symbols"
        assert stats['completed'] == 2, "Should count completed symbols"
        assert stats['failed'] == 1, "Should count failed symbols"
        assert stats['processing'] == 1, "Should count processing symbols"
        assert stats['pending'] == 1, "Should count pending symbols"
        
        # Calculate progress percentage
        progress_pct = (stats['completed'] / stats['total'] * 100) if stats['total'] > 0 else 0
        assert progress_pct == 40.0, "Progress should be 40% (2/5 completed)"
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id = $1", test_job_id
        )

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_checkpoint_recovery(self, db_connection, test_job_id):
        """Test recovery from checkpoint after interruption"""
        
        # Simulate a job that was interrupted mid-processing
        interrupted_symbols = [
            ("AAPL", "completed", 1500),    # Previously completed
            ("MSFT", "completed", 1200),    # Previously completed  
            ("GOOGL", "processing", None),  # Was processing when interrupted
            ("TSLA", "pending", None),      # Not yet started
            ("AMZN", "pending", None)       # Not yet started
        ]
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id = $1", test_job_id
        )
        
        # Set up interrupted state
        for symbol, status, records in interrupted_symbols:
            if status == 'completed':
                await db_connection.execute("""
                    INSERT INTO vendor_job_progress (
                        job_id, vendor, symbol, status, created_at, completed_at, records_collected
                    ) VALUES ($1, 'polygon', $2, $3, NOW(), NOW(), $4)
                """, test_job_id, symbol, status, records)
            else:
                await db_connection.execute("""
                    INSERT INTO vendor_job_progress (
                        job_id, vendor, symbol, status, created_at
                    ) VALUES ($1, 'polygon', $2, $3, NOW())
                """, test_job_id, symbol, status)
        
        # Simulate recovery: find symbols that need processing
        pending_symbols = await db_connection.fetch("""
            SELECT symbol FROM vendor_job_progress 
            WHERE job_id = $1 AND vendor = 'polygon' 
            AND status IN ('pending', 'processing')
            ORDER BY symbol
        """, test_job_id)
        
        pending_list = [row['symbol'] for row in pending_symbols]
        expected_pending = ['GOOGL', 'TSLA', 'AMZN']  # processing + pending symbols
        
        assert len(pending_list) == 3, "Should find 3 symbols needing processing"
        assert set(pending_list) == set(expected_pending), \
            f"Should recover correct pending symbols: {expected_pending}"
        
        # Verify completed symbols are not reprocessed
        completed_symbols = await db_connection.fetch("""
            SELECT symbol, records_collected FROM vendor_job_progress 
            WHERE job_id = $1 AND vendor = 'polygon' AND status = 'completed'
            ORDER BY symbol
        """, test_job_id)
        
        assert len(completed_symbols) == 2, "Should preserve completed symbols"
        
        completed_records = {row['symbol']: row['records_collected'] for row in completed_symbols}
        assert completed_records['AAPL'] == 1500, "Should preserve AAPL record count"
        assert completed_records['MSFT'] == 1200, "Should preserve MSFT record count"
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id = $1", test_job_id
        )

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_concurrent_job_isolation(self, db_connection):
        """Test that multiple concurrent jobs don't interfere with each other"""
        
        job1_id = f"test-job-1-{datetime.now().strftime('%H%M%S')}"
        job2_id = f"test-job-2-{datetime.now().strftime('%H%M%S')}"
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id IN ($1, $2)", 
            job1_id, job2_id
        )
        
        # Job 1: Process AAPL, MSFT
        job1_symbols = ["AAPL", "MSFT"]
        for symbol in job1_symbols:
            await db_connection.execute("""
                INSERT INTO vendor_job_progress (
                    job_id, vendor, symbol, status, created_at
                ) VALUES ($1, 'polygon', $2, 'completed', NOW())
            """, job1_id, symbol)
        
        # Job 2: Process GOOGL, TSLA (different symbols)
        job2_symbols = ["GOOGL", "TSLA"]
        for symbol in job2_symbols:
            await db_connection.execute("""
                INSERT INTO vendor_job_progress (
                    job_id, vendor, symbol, status, created_at
                ) VALUES ($1, 'polygon', $2, 'pending', NOW())
            """, job2_id, symbol)
        
        # Verify job isolation
        job1_count = await db_connection.fetchval("""
            SELECT COUNT(*) FROM vendor_job_progress 
            WHERE job_id = $1 AND vendor = 'polygon'
        """, job1_id)
        
        job2_count = await db_connection.fetchval("""
            SELECT COUNT(*) FROM vendor_job_progress 
            WHERE job_id = $1 AND vendor = 'polygon'
        """, job2_id)
        
        assert job1_count == 2, "Job 1 should have 2 symbols"
        assert job2_count == 2, "Job 2 should have 2 symbols"
        
        # Verify job 1 progress doesn't affect job 2
        job1_stats = await db_connection.fetchrow("""
            SELECT 
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
                COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending
            FROM vendor_job_progress WHERE job_id = $1
        """, job1_id)
        
        job2_stats = await db_connection.fetchrow("""
            SELECT 
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
                COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending
            FROM vendor_job_progress WHERE job_id = $1
        """, job2_id)
        
        assert job1_stats['completed'] == 2 and job1_stats['pending'] == 0, \
            "Job 1 should have all completed"
        assert job2_stats['completed'] == 0 and job2_stats['pending'] == 2, \
            "Job 2 should have all pending"
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id IN ($1, $2)", 
            job1_id, job2_id
        )


class TestPolygonDataIntegration:
    """Test integration between checkpoint system and data storage"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_end_to_end_symbol_processing(self, db_connection, test_job_id):
        """Test complete end-to-end processing of a symbol with data storage"""
        
        test_symbol = "INTEGRATION_TEST"
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id = $1 AND symbol = $2", 
            test_job_id, test_symbol
        )
        await db_connection.execute(
            "DELETE FROM dev_polygon_prices WHERE symbol = $1", test_symbol
        )
        
        # 1. Initialize checkpoint
        await db_connection.execute("""
            INSERT INTO vendor_job_progress (
                job_id, vendor, symbol, status, created_at
            ) VALUES ($1, 'polygon', $2, 'pending', NOW())
        """, test_job_id, test_symbol)
        
        # 2. Mark as processing
        await db_connection.execute("""
            UPDATE vendor_job_progress 
            SET status = 'processing', started_at = NOW()
            WHERE job_id = $1 AND vendor = 'polygon' AND symbol = $2
        """, test_job_id, test_symbol)
        
        # 3. Simulate data collection and storage
        test_records = [
            {
                'symbol': test_symbol,
                'price_date': date(2024, 1, 1),
                'open_price': Decimal('100.0'),
                'high_price': Decimal('105.0'),
                'low_price': Decimal('99.0'),
                'close_price': Decimal('102.5'),
                'volume': 1000000,
                'data_source': 'polygon'
            },
            {
                'symbol': test_symbol,
                'price_date': date(2024, 1, 2),
                'open_price': Decimal('102.5'),
                'high_price': Decimal('108.0'),
                'low_price': Decimal('101.0'),
                'close_price': Decimal('106.8'),
                'volume': 1200000,
                'data_source': 'polygon'
            }
        ]
        
        # Store data (same as collector does)
        insert_query = """
            INSERT INTO dev_polygon_prices (
                symbol, price_date, open_price, high_price, low_price, 
                close_price, volume, vwap, transactions, data_source
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (symbol, price_date) DO UPDATE SET
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume
        """
        
        for record in test_records:
            await db_connection.execute(
                insert_query,
                record['symbol'], record['price_date'],
                record['open_price'], record['high_price'], record['low_price'],
                record['close_price'], record['volume'], 
                None, None, record['data_source']  # vwap, transactions
            )
        
        # 4. Mark as completed with record count
        await db_connection.execute("""
            UPDATE vendor_job_progress 
            SET status = 'completed', completed_at = NOW(), records_collected = $3
            WHERE job_id = $1 AND vendor = 'polygon' AND symbol = $2
        """, test_job_id, test_symbol, len(test_records))
        
        # 5. Verify end-to-end results
        checkpoint_result = await db_connection.fetchrow("""
            SELECT status, records_collected, started_at, completed_at
            FROM vendor_job_progress 
            WHERE job_id = $1 AND symbol = $2
        """, test_job_id, test_symbol)
        
        assert checkpoint_result['status'] == 'completed', "Checkpoint should show completed"
        assert checkpoint_result['records_collected'] == len(test_records), \
            "Should track correct record count"
        assert checkpoint_result['started_at'] is not None, "Should have start time"
        assert checkpoint_result['completed_at'] is not None, "Should have completion time"
        
        # Verify data was actually stored
        stored_count = await db_connection.fetchval("""
            SELECT COUNT(*) FROM dev_polygon_prices WHERE symbol = $1
        """, test_symbol)
        
        assert stored_count == len(test_records), \
            "Should have stored all records in database"
        
        # Verify data integrity
        stored_records = await db_connection.fetch("""
            SELECT price_date, close_price, volume 
            FROM dev_polygon_prices 
            WHERE symbol = $1 ORDER BY price_date
        """, test_symbol)
        
        assert len(stored_records) == 2, "Should retrieve both stored records"
        assert stored_records[0]['close_price'] == Decimal('102.5'), "First record close price"
        assert stored_records[1]['close_price'] == Decimal('106.8'), "Second record close price"
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id = $1 AND symbol = $2", 
            test_job_id, test_symbol
        )
        await db_connection.execute(
            "DELETE FROM dev_polygon_prices WHERE symbol = $1", test_symbol
        )

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_duplicate_handling_with_checkpoints(self, db_connection, test_job_id):
        """Test that duplicate data is handled correctly with checkpoint system"""
        
        test_symbol = "DUPLICATE_TEST"
        test_date = date(2024, 1, 15)
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id = $1 AND symbol = $2", 
            test_job_id, test_symbol
        )
        await db_connection.execute(
            "DELETE FROM dev_polygon_prices WHERE symbol = $1", test_symbol
        )
        
        # Initialize checkpoint
        await db_connection.execute("""
            INSERT INTO vendor_job_progress (
                job_id, vendor, symbol, status, created_at
            ) VALUES ($1, 'polygon', $2, 'processing', NOW())
        """, test_job_id, test_symbol)
        
        # First insert
        await db_connection.execute("""
            INSERT INTO dev_polygon_prices (
                symbol, price_date, close_price, volume, data_source
            ) VALUES ($1, $2, $3, $4, 'polygon')
            ON CONFLICT (symbol, price_date) DO UPDATE SET
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume
        """, test_symbol, test_date, Decimal('100.0'), 1000000)
        
        # Second insert (duplicate) with updated data
        await db_connection.execute("""
            INSERT INTO dev_polygon_prices (
                symbol, price_date, close_price, volume, data_source
            ) VALUES ($1, $2, $3, $4, 'polygon')
            ON CONFLICT (symbol, price_date) DO UPDATE SET
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume
        """, test_symbol, test_date, Decimal('105.0'), 1500000)
        
        # Should have only one record with updated values
        result = await db_connection.fetchrow("""
            SELECT COUNT(*) as count, close_price, volume
            FROM dev_polygon_prices 
            WHERE symbol = $1 AND price_date = $2
            GROUP BY close_price, volume
        """, test_symbol, test_date)
        
        assert result['count'] == 1, "Should have exactly one record after duplicate handling"
        assert result['close_price'] == Decimal('105.0'), "Should have updated close price"
        assert result['volume'] == 1500000, "Should have updated volume"
        
        # Mark checkpoint as completed
        await db_connection.execute("""
            UPDATE vendor_job_progress 
            SET status = 'completed', completed_at = NOW(), records_collected = 1
            WHERE job_id = $1 AND vendor = 'polygon' AND symbol = $2
        """, test_job_id, test_symbol)
        
        # Clean up
        await db_connection.execute(
            "DELETE FROM vendor_job_progress WHERE job_id = $1 AND symbol = $2", 
            test_job_id, test_symbol
        )
        await db_connection.execute(
            "DELETE FROM dev_polygon_prices WHERE symbol = $1", test_symbol
        )


if __name__ == "__main__":
    # Run tests with: PYTHONPATH=src pytest tests/integration/test_polygon_checkpoint_collection.py -v --tb=short
    print("To run these tests:")
    print("PYTHONPATH=src pytest tests/integration/test_polygon_checkpoint_collection.py -v --tb=short")