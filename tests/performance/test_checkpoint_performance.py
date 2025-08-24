"""
Performance and Load Testing for Checkpoint Framework

This module contains comprehensive performance tests to validate:
- Scalability under high load
- Memory usage patterns
- Database performance
- Concurrent execution efficiency
- Large dataset handling
- Network resilience
"""

import asyncio
import asyncpg
import aiohttp
import time
import psutil
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
import json
import statistics
from dataclasses import dataclass, asdict
import os
import gc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class PerformanceMetrics:
    """Performance metrics collection"""
    test_name: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    items_processed: int
    processing_rate: float  # items/second
    memory_usage_mb: float
    cpu_usage_percent: float
    database_calls: int
    api_calls: int
    error_count: int
    success_rate: float

class PerformanceMonitor:
    """Monitor system performance during tests"""
    
    def __init__(self):
        self.start_memory = 0
        self.start_time = 0
        self.db_calls = 0
        self.api_calls = 0
        
    def start_monitoring(self):
        """Start performance monitoring"""
        process = psutil.Process()
        self.start_memory = process.memory_info().rss / 1024 / 1024  # MB
        self.start_time = time.time()
        self.db_calls = 0
        self.api_calls = 0
        
    def get_current_metrics(self) -> Dict:
        """Get current performance metrics"""
        process = psutil.Process()
        current_memory = process.memory_info().rss / 1024 / 1024  # MB
        cpu_percent = process.cpu_percent()
        
        return {
            'memory_mb': current_memory,
            'memory_delta_mb': current_memory - self.start_memory,
            'cpu_percent': cpu_percent,
            'elapsed_seconds': time.time() - self.start_time,
            'db_calls': self.db_calls,
            'api_calls': self.api_calls
        }
        
    def record_db_call(self):
        """Record a database call"""
        self.db_calls += 1
        
    def record_api_call(self):
        """Record an API call"""
        self.api_calls += 1

class PerformanceTestCheckpointManager:
    """Checkpoint manager with performance monitoring"""
    
    def __init__(self, db_connection: asyncpg.Connection, monitor: PerformanceMonitor):
        self.conn = db_connection
        self.monitor = monitor
        
    async def setup_performance_tables(self):
        """Setup tables optimized for performance testing"""
        logger.info("Setting up performance test tables...")
        
        # Drop existing tables
        await self.conn.execute("""
            DROP TABLE IF EXISTS perf_job_progress CASCADE;
            DROP TABLE IF EXISTS perf_job_runs CASCADE; 
            DROP TABLE IF EXISTS perf_price_data CASCADE;
        """)
        
        # Create optimized tables with proper indexing
        await self.conn.execute("""
            CREATE TABLE perf_job_runs (
                id SERIAL PRIMARY KEY,
                job_id VARCHAR(100) UNIQUE NOT NULL,
                job_name VARCHAR(100) NOT NULL,
                vendor VARCHAR(50),
                status VARCHAR(20) DEFAULT 'pending',
                processed_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                total_items INTEGER,
                started_at TIMESTAMP DEFAULT now(),
                updated_at TIMESTAMP DEFAULT now()
            );
            
            CREATE INDEX idx_perf_job_runs_lookup ON perf_job_runs(job_id, status);
        """)
        
        await self.conn.execute("""
            CREATE TABLE perf_job_progress (
                id SERIAL PRIMARY KEY,
                job_id VARCHAR(100) NOT NULL,
                item_key VARCHAR(50) NOT NULL,
                status VARCHAR(20) DEFAULT 'pending',
                records_processed INTEGER DEFAULT 0,
                processing_time_ms INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT now(),
                UNIQUE(job_id, item_key)
            );
            
            CREATE INDEX idx_perf_job_progress_lookup ON perf_job_progress(job_id, status);
            CREATE INDEX idx_perf_job_progress_item ON perf_job_progress(job_id, item_key);
        """)
        
        await self.conn.execute("""
            CREATE TABLE perf_price_data (
                id BIGSERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                price_date DATE NOT NULL,
                price_value DECIMAL(10,2) NOT NULL,
                volume BIGINT,
                job_id VARCHAR(100),
                batch_id VARCHAR(50),
                created_at TIMESTAMP DEFAULT now()
            );
            
            CREATE INDEX idx_perf_price_symbol_date ON perf_price_data(symbol, price_date);
            CREATE INDEX idx_perf_price_job ON perf_price_data(job_id);
        """)
        
        logger.info("✅ Performance test tables created")
        
    async def create_job_run(self, job_name: str, vendor: str, total_items: int) -> str:
        """Create job run with monitoring"""
        self.monitor.record_db_call()
        job_id = f"{job_name}_{int(time.time())}_{os.getpid()}"
        
        await self.conn.execute("""
            INSERT INTO perf_job_runs (job_id, job_name, vendor, total_items)
            VALUES ($1, $2, $3, $4)
        """, job_id, job_name, vendor, total_items)
        
        return job_id
        
    async def bulk_initialize_items(self, job_id: str, items: List[str]):
        """Bulk initialize items for better performance"""
        self.monitor.record_db_call()
        
        # Use COPY for bulk insert performance
        values = [(job_id, item) for item in items]
        
        # Create temporary table and bulk insert
        await self.conn.execute("""
            CREATE TEMP TABLE temp_items (job_id VARCHAR(100), item_key VARCHAR(50))
        """)
        
        await self.conn.copy_records_to_table('temp_items', records=values)
        
        await self.conn.execute("""
            INSERT INTO perf_job_progress (job_id, item_key)
            SELECT job_id, item_key FROM temp_items
            ON CONFLICT (job_id, item_key) DO NOTHING
        """)
        
        await self.conn.execute("DROP TABLE temp_items")
        
    async def batch_update_completed_items(self, job_id: str, completed_items: List[Tuple[str, int, int]]):
        """Batch update multiple completed items"""
        self.monitor.record_db_call()
        
        # Use UPDATE with VALUES for batch processing
        if not completed_items:
            return
            
        values_str = ', '.join([
            f"('{job_id}', '{item}', {records}, {processing_time})"
            for item, records, processing_time in completed_items
        ])
        
        await self.conn.execute(f"""
            UPDATE perf_job_progress 
            SET status = 'completed', 
                records_processed = updates.records,
                processing_time_ms = updates.processing_time
            FROM (VALUES {values_str}) AS updates(job_id, item_key, records, processing_time)
            WHERE perf_job_progress.job_id = updates.job_id 
            AND perf_job_progress.item_key = updates.item_key
        """)
        
    async def bulk_store_prices(self, job_id: str, batch_id: str, price_data: List[Dict]):
        """Bulk store price data for performance"""
        self.monitor.record_db_call()
        
        if not price_data:
            return 0
            
        # Prepare data for bulk insert
        records = [
            (
                data['symbol'],
                data['date'],
                data['price'],
                data['volume'],
                job_id,
                batch_id
            )
            for data in price_data
        ]
        
        # Use COPY for maximum insert performance
        await self.conn.copy_records_to_table(
            'perf_price_data',
            records=records,
            columns=['symbol', 'price_date', 'price_value', 'volume', 'job_id', 'batch_id']
        )
        
        return len(records)
        
    async def get_job_stats(self, job_id: str) -> Dict:
        """Get job statistics with monitoring"""
        self.monitor.record_db_call()
        
        stats = await self.conn.fetchrow("""
            SELECT 
                COUNT(*) as total_items,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
                COALESCE(SUM(records_processed), 0) as total_records,
                COALESCE(AVG(processing_time_ms), 0) as avg_processing_time_ms
            FROM perf_job_progress WHERE job_id = $1
        """, job_id)
        
        return dict(stats) if stats else {}
        
    async def cleanup_performance_tables(self):
        """Clean up performance tables"""
        await self.conn.execute("""
            DROP TABLE IF EXISTS perf_job_progress CASCADE;
            DROP TABLE IF EXISTS perf_job_runs CASCADE;
            DROP TABLE IF EXISTS perf_price_data CASCADE;
        """)

class HighVolumeJob:
    """High-performance job implementation for load testing"""
    
    def __init__(self, name: str, item_count: int, records_per_item: int):
        self.name = name
        self.item_count = item_count
        self.records_per_item = records_per_item
        self.processed_count = 0
        
    async def get_items(self) -> List[str]:
        """Generate test items"""
        return [f"ITEM_{i:06d}" for i in range(self.item_count)]
        
    async def process_item(self, item: str, monitor: PerformanceMonitor) -> Tuple[List[Dict], Optional[str]]:
        """Process single item with performance tracking"""
        monitor.record_api_call()
        
        # Simulate API processing time
        await asyncio.sleep(0.001)  # 1ms per item
        
        # Generate mock price data
        prices = []
        base_date = date.today()
        
        for i in range(self.records_per_item):
            prices.append({
                'symbol': item,
                'date': base_date - timedelta(days=i),
                'price': 100.0 + (i % 100),
                'volume': 1000000 + (i * 1000)
            })
        
        self.processed_count += 1
        return prices, None

async def test_high_volume_processing():
    """Test processing large numbers of items"""
    logger.info("🚀 Starting high volume processing test...")
    
    monitor = PerformanceMonitor()
    monitor.start_monitoring()
    
    try:
        conn = await asyncpg.connect(
            host='postgres',
            port=5432,
            user='postgres',
            password='dev_password',
            database='dev_db'
        )
        
        manager = PerformanceTestCheckpointManager(conn, monitor)
        await manager.setup_performance_tables()
        
        # Test with large number of items
        item_count = 1000
        records_per_item = 50
        batch_size = 20
        
        job = HighVolumeJob("high_volume_test", item_count, records_per_item)
        job_id = await manager.create_job_run(job.name, "performance_test", item_count)
        
        # Get all items and initialize
        items = await job.get_items()
        await manager.bulk_initialize_items(job_id, items)
        
        start_time = time.time()
        total_processed = 0
        total_records = 0
        
        # Process in batches for optimal performance
        for i in range(0, len(items), batch_size):
            batch_items = items[i:i + batch_size]
            batch_start = time.time()
            
            # Process batch concurrently
            async def process_batch_item(item: str):
                item_start = time.time()
                prices, error = await job.process_item(item, monitor)
                processing_time = int((time.time() - item_start) * 1000)  # milliseconds
                
                if error:
                    return item, 0, processing_time, []
                    
                return item, len(prices), processing_time, prices
            
            batch_results = await asyncio.gather(*[process_batch_item(item) for item in batch_items])
            
            # Bulk update completed items
            completed_items = []
            all_prices = []
            
            for item, record_count, processing_time, prices in batch_results:
                completed_items.append((item, record_count, processing_time))
                all_prices.extend(prices)
                total_records += record_count
                total_processed += 1
            
            # Bulk database operations
            if completed_items:
                await manager.batch_update_completed_items(job_id, completed_items)
                
            if all_prices:
                batch_id = f"batch_{i//batch_size:04d}"
                await manager.bulk_store_prices(job_id, batch_id, all_prices)
            
            # Progress logging
            if i % 200 == 0:
                current_metrics = monitor.get_current_metrics()
                rate = total_processed / current_metrics['elapsed_seconds']
                logger.info(f"📊 Processed {total_processed}/{item_count} ({rate:.1f}/sec) - "
                           f"Memory: {current_metrics['memory_mb']:.1f}MB")
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Get final statistics
        stats = await manager.get_job_stats(job_id)
        final_metrics = monitor.get_current_metrics()
        
        # Calculate performance metrics
        performance = PerformanceMetrics(
            test_name="High Volume Processing",
            start_time=datetime.fromtimestamp(start_time),
            end_time=datetime.fromtimestamp(end_time),
            duration_seconds=duration,
            items_processed=stats['completed'],
            processing_rate=stats['completed'] / duration,
            memory_usage_mb=final_metrics['memory_mb'],
            cpu_usage_percent=final_metrics['cpu_percent'],
            database_calls=monitor.db_calls,
            api_calls=monitor.api_calls,
            error_count=stats['failed'],
            success_rate=(stats['completed'] / stats['total_items']) * 100
        )
        
        # Log results
        logger.info("=" * 80)
        logger.info("🎯 HIGH VOLUME PROCESSING RESULTS")
        logger.info(f"📊 Items processed: {performance.items_processed:,}")
        logger.info(f"📊 Total records stored: {stats['total_records']:,}")
        logger.info(f"⏱️  Duration: {performance.duration_seconds:.2f} seconds")
        logger.info(f"🚀 Processing rate: {performance.processing_rate:.1f} items/second")
        logger.info(f"💾 Memory usage: {performance.memory_usage_mb:.1f} MB")
        logger.info(f"💾 Memory increase: {final_metrics['memory_delta_mb']:.1f} MB")
        logger.info(f"🔄 Database calls: {performance.database_calls}")
        logger.info(f"🌐 API calls: {performance.api_calls}")
        logger.info(f"✅ Success rate: {performance.success_rate:.1f}%")
        logger.info(f"⚡ Avg processing time: {stats['avg_processing_time_ms']:.1f}ms/item")
        
        # Performance assertions
        assert performance.processing_rate >= 50, f"Processing rate too slow: {performance.processing_rate:.1f}/sec"
        assert performance.success_rate >= 99.0, f"Success rate too low: {performance.success_rate:.1f}%"
        assert final_metrics['memory_delta_mb'] < 500, f"Memory usage too high: {final_metrics['memory_delta_mb']:.1f}MB"
        
        await manager.cleanup_performance_tables()
        await conn.close()
        
        logger.info("✅ High volume processing test PASSED")
        return performance
        
    except Exception as e:
        logger.error(f"❌ High volume processing test FAILED: {e}")
        raise

async def test_concurrent_job_performance():
    """Test performance with multiple concurrent jobs"""
    logger.info("🚀 Starting concurrent job performance test...")
    
    monitor = PerformanceMonitor()
    monitor.start_monitoring()
    
    try:
        conn = await asyncpg.connect(
            host='postgres',
            port=5432,
            user='postgres',
            password='dev_password',
            database='dev_db'
        )
        
        manager = PerformanceTestCheckpointManager(conn, monitor)
        await manager.setup_performance_tables()
        
        async def run_concurrent_job(job_index: int, items_per_job: int):
            """Run a single job concurrently"""
            job = HighVolumeJob(f"concurrent_job_{job_index}", items_per_job, 10)
            job_id = await manager.create_job_run(job.name, f"vendor_{job_index % 3}", items_per_job)
            
            items = await job.get_items()
            await manager.bulk_initialize_items(job_id, items)
            
            processed = 0
            for item in items:
                prices, error = await job.process_item(item, monitor)
                if not error:
                    await manager.bulk_store_prices(job_id, f"single_{item}", prices)
                    processed += 1
                    
            return job_id, processed
        
        # Run multiple jobs concurrently
        num_jobs = 10
        items_per_job = 100
        
        start_time = time.time()
        
        # Execute all jobs concurrently
        results = await asyncio.gather(*[
            run_concurrent_job(i, items_per_job) 
            for i in range(num_jobs)
        ])
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Collect statistics from all jobs
        total_processed = 0
        all_stats = []
        
        for job_id, processed in results:
            stats = await manager.get_job_stats(job_id)
            all_stats.append(stats)
            total_processed += processed
            
        final_metrics = monitor.get_current_metrics()
        
        # Calculate performance metrics
        performance = PerformanceMetrics(
            test_name="Concurrent Job Processing",
            start_time=datetime.fromtimestamp(start_time),
            end_time=datetime.fromtimestamp(end_time),
            duration_seconds=duration,
            items_processed=total_processed,
            processing_rate=total_processed / duration,
            memory_usage_mb=final_metrics['memory_mb'],
            cpu_usage_percent=final_metrics['cpu_percent'],
            database_calls=monitor.db_calls,
            api_calls=monitor.api_calls,
            error_count=0,
            success_rate=100.0
        )
        
        # Log results
        logger.info("=" * 80)
        logger.info("🎯 CONCURRENT JOB PERFORMANCE RESULTS")
        logger.info(f"📊 Concurrent jobs: {num_jobs}")
        logger.info(f"📊 Items per job: {items_per_job}")
        logger.info(f"📊 Total items processed: {performance.items_processed:,}")
        logger.info(f"⏱️  Duration: {performance.duration_seconds:.2f} seconds")
        logger.info(f"🚀 Overall rate: {performance.processing_rate:.1f} items/second")
        logger.info(f"🚀 Per-job rate: {performance.processing_rate/num_jobs:.1f} items/second/job")
        logger.info(f"💾 Memory usage: {performance.memory_usage_mb:.1f} MB")
        logger.info(f"🔄 Database calls: {performance.database_calls}")
        logger.info(f"🌐 API calls: {performance.api_calls}")
        
        # Performance assertions for concurrent execution
        expected_min_rate = 30  # Should process at least 30 items/sec total
        assert performance.processing_rate >= expected_min_rate, f"Concurrent rate too slow: {performance.processing_rate:.1f}/sec"
        
        # Verify all jobs completed successfully
        for i, stats in enumerate(all_stats):
            assert stats['completed'] == items_per_job, f"Job {i} incomplete: {stats['completed']}/{items_per_job}"
            
        await manager.cleanup_performance_tables()
        await conn.close()
        
        logger.info("✅ Concurrent job performance test PASSED")
        return performance
        
    except Exception as e:
        logger.error(f"❌ Concurrent job performance test FAILED: {e}")
        raise

async def test_memory_usage_patterns():
    """Test memory usage patterns and garbage collection"""
    logger.info("🚀 Starting memory usage pattern test...")
    
    monitor = PerformanceMonitor()
    monitor.start_monitoring()
    
    try:
        conn = await asyncpg.connect(
            host='postgres',
            port=5432,
            user='postgres',
            password='dev_password',
            database='dev_db'
        )
        
        manager = PerformanceTestCheckpointManager(conn, monitor)
        await manager.setup_performance_tables()
        
        memory_snapshots = []
        processing_stages = [
            ("Initialization", 100, 10),
            ("Small Load", 500, 20),
            ("Medium Load", 1000, 50),
            ("Large Load", 2000, 100),
        ]
        
        for stage_name, item_count, records_per_item in processing_stages:
            logger.info(f"📊 Memory test stage: {stage_name} ({item_count} items)")
            
            # Force garbage collection before stage
            gc.collect()
            stage_start_metrics = monitor.get_current_metrics()
            
            job = HighVolumeJob(f"memory_test_{stage_name.lower()}", item_count, records_per_item)
            job_id = await manager.create_job_run(job.name, "memory_test", item_count)
            
            items = await job.get_items()
            await manager.bulk_initialize_items(job_id, items)
            
            # Process items in batches to monitor memory usage
            batch_size = 50
            stage_data = []
            
            for i in range(0, len(items), batch_size):
                batch_items = items[i:i + batch_size]
                batch_start_mem = monitor.get_current_metrics()['memory_mb']
                
                # Process batch
                all_prices = []
                completed_items = []
                
                for item in batch_items:
                    prices, error = await job.process_item(item, monitor)
                    if not error:
                        all_prices.extend(prices)
                        completed_items.append((item, len(prices), 1))  # 1ms processing time
                
                # Store batch data
                if completed_items:
                    await manager.batch_update_completed_items(job_id, completed_items)
                    await manager.bulk_store_prices(job_id, f"mem_batch_{i}", all_prices)
                
                batch_end_mem = monitor.get_current_metrics()['memory_mb']
                stage_data.append({
                    'batch_index': i // batch_size,
                    'items_processed': len(batch_items),
                    'memory_before_mb': batch_start_mem,
                    'memory_after_mb': batch_end_mem,
                    'memory_delta_mb': batch_end_mem - batch_start_mem
                })
            
            # Force garbage collection after stage
            gc.collect()
            stage_end_metrics = monitor.get_current_metrics()
            
            # Calculate stage memory statistics
            memory_deltas = [batch['memory_delta_mb'] for batch in stage_data]
            stage_memory_increase = stage_end_metrics['memory_mb'] - stage_start_metrics['memory_mb']
            
            stage_summary = {
                'stage_name': stage_name,
                'items_processed': item_count,
                'records_per_item': records_per_item,
                'total_records': item_count * records_per_item,
                'memory_start_mb': stage_start_metrics['memory_mb'],
                'memory_end_mb': stage_end_metrics['memory_mb'],
                'memory_increase_mb': stage_memory_increase,
                'avg_batch_delta_mb': statistics.mean(memory_deltas) if memory_deltas else 0,
                'max_batch_delta_mb': max(memory_deltas) if memory_deltas else 0,
                'memory_per_record_kb': (stage_memory_increase * 1024) / (item_count * records_per_item) if item_count * records_per_item > 0 else 0
            }
            
            memory_snapshots.append(stage_summary)
            
            logger.info(f"  💾 Memory increase: {stage_memory_increase:.1f} MB")
            logger.info(f"  📊 Memory per record: {stage_summary['memory_per_record_kb']:.2f} KB")
            logger.info(f"  📈 Max batch delta: {stage_summary['max_batch_delta_mb']:.1f} MB")
        
        # Analyze memory patterns
        logger.info("=" * 80)
        logger.info("🎯 MEMORY USAGE PATTERN ANALYSIS")
        
        for snapshot in memory_snapshots:
            logger.info(f"📊 {snapshot['stage_name']}:")
            logger.info(f"    Items: {snapshot['items_processed']:,}, Records: {snapshot['total_records']:,}")
            logger.info(f"    Memory increase: {snapshot['memory_increase_mb']:.1f} MB")
            logger.info(f"    Memory per record: {snapshot['memory_per_record_kb']:.2f} KB")
            logger.info(f"    Max batch delta: {snapshot['max_batch_delta_mb']:.1f} MB")
        
        # Memory usage assertions
        final_memory = memory_snapshots[-1]['memory_end_mb']
        initial_memory = memory_snapshots[0]['memory_start_mb']
        total_memory_increase = final_memory - initial_memory
        
        # Should not use excessive memory (less than 1GB total increase)
        assert total_memory_increase < 1024, f"Memory usage too high: {total_memory_increase:.1f} MB"
        
        # Memory per record should be reasonable (less than 1KB per record)
        avg_memory_per_record = statistics.mean([s['memory_per_record_kb'] for s in memory_snapshots])
        assert avg_memory_per_record < 1.0, f"Memory per record too high: {avg_memory_per_record:.2f} KB"
        
        await manager.cleanup_performance_tables()
        await conn.close()
        
        logger.info("✅ Memory usage pattern test PASSED")
        return memory_snapshots
        
    except Exception as e:
        logger.error(f"❌ Memory usage pattern test FAILED: {e}")
        raise

async def test_database_performance_scaling():
    """Test database performance under increasing load"""
    logger.info("🚀 Starting database performance scaling test...")
    
    try:
        conn = await asyncpg.connect(
            host='postgres',
            port=5432,
            user='postgres',
            password='dev_password',
            database='dev_db'
        )
        
        monitor = PerformanceMonitor()
        monitor.start_monitoring()
        
        manager = PerformanceTestCheckpointManager(conn, monitor)
        await manager.setup_performance_tables()
        
        scaling_results = []
        connection_counts = [1, 5, 10, 20]  # Test with different connection pool sizes
        
        for conn_count in connection_counts:
            logger.info(f"📊 Testing with {conn_count} database connections...")
            
            # Create connection pool
            pool = await asyncpg.create_pool(
                host='postgres',
                port=5432,
                user='postgres',
                password='dev_password',
                database='dev_db',
                min_size=conn_count,
                max_size=conn_count
            )
            
            async def db_intensive_task(task_id: int, operations_per_task: int):
                """Run database-intensive operations"""
                async with pool.acquire() as conn:
                    task_manager = PerformanceTestCheckpointManager(conn, monitor)
                    
                    # Create job
                    job_id = await task_manager.create_job_run(f"db_scale_test_{task_id}", "scale_test", operations_per_task)
                    
                    # Generate and initialize items
                    items = [f"TASK_{task_id}_ITEM_{i}" for i in range(operations_per_task)]
                    await task_manager.bulk_initialize_items(job_id, items)
                    
                    # Perform rapid database operations
                    completed_items = [(item, 10, 5) for item in items]  # 10 records, 5ms processing time
                    await task_manager.batch_update_completed_items(job_id, completed_items)
                    
                    # Generate and store price data
                    price_data = []
                    for item in items:
                        price_data.extend([
                            {
                                'symbol': item,
                                'date': date.today() - timedelta(days=i),
                                'price': 100.0 + i,
                                'volume': 1000000
                            }
                            for i in range(10)
                        ])
                    
                    await task_manager.bulk_store_prices(job_id, f"scale_batch_{task_id}", price_data)
                    
                    return len(items), len(price_data)
            
            # Run concurrent database-intensive tasks
            operations_per_task = 100
            num_tasks = conn_count * 2  # 2 tasks per connection
            
            start_time = time.time()
            
            task_results = await asyncio.gather(*[
                db_intensive_task(i, operations_per_task)
                for i in range(num_tasks)
            ])
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Calculate metrics
            total_items = sum(result[0] for result in task_results)
            total_records = sum(result[1] for result in task_results)
            
            scaling_result = {
                'connection_count': conn_count,
                'concurrent_tasks': num_tasks,
                'total_items': total_items,
                'total_records': total_records,
                'duration_seconds': duration,
                'items_per_second': total_items / duration,
                'records_per_second': total_records / duration,
                'db_calls': monitor.db_calls
            }
            
            scaling_results.append(scaling_result)
            
            logger.info(f"  ⏱️  Duration: {duration:.2f}s")
            logger.info(f"  🚀 Items/sec: {scaling_result['items_per_second']:.1f}")
            logger.info(f"  📊 Records/sec: {scaling_result['records_per_second']:.1f}")
            
            await pool.close()
            
            # Reset monitor counters
            monitor.db_calls = 0
        
        # Analyze scaling performance
        logger.info("=" * 80)
        logger.info("🎯 DATABASE PERFORMANCE SCALING RESULTS")
        
        for result in scaling_results:
            logger.info(f"📊 {result['connection_count']} connections:")
            logger.info(f"    Items/sec: {result['items_per_second']:.1f}")
            logger.info(f"    Records/sec: {result['records_per_second']:.1f}")
            logger.info(f"    Duration: {result['duration_seconds']:.2f}s")
        
        # Verify scaling efficiency
        base_performance = scaling_results[0]['items_per_second']
        
        for result in scaling_results[1:]:
            scaling_factor = result['connection_count'] / scaling_results[0]['connection_count']
            expected_min_performance = base_performance * min(scaling_factor, 2.0)  # Should scale up to 2x
            
            assert result['items_per_second'] >= expected_min_performance * 0.7, \
                f"Poor scaling: {result['items_per_second']:.1f} < {expected_min_performance * 0.7:.1f} items/sec"
        
        await manager.cleanup_performance_tables()
        await conn.close()
        
        logger.info("✅ Database performance scaling test PASSED")
        return scaling_results
        
    except Exception as e:
        logger.error(f"❌ Database performance scaling test FAILED: {e}")
        raise

async def run_all_performance_tests():
    """Run comprehensive performance test suite"""
    logger.info("🚀 Starting Comprehensive Performance Test Suite")
    logger.info("=" * 80)
    
    test_results = []
    
    performance_tests = [
        ("High Volume Processing", test_high_volume_processing),
        ("Concurrent Job Performance", test_concurrent_job_performance),
        ("Memory Usage Patterns", test_memory_usage_patterns),
        ("Database Performance Scaling", test_database_performance_scaling),
    ]
    
    for test_name, test_func in performance_tests:
        logger.info(f"\n🧪 Running: {test_name}")
        logger.info("-" * 60)
        
        try:
            start_time = time.time()
            result = await test_func()
            duration = time.time() - start_time
            
            test_results.append({
                'name': test_name,
                'status': 'PASSED',
                'duration': duration,
                'result': result
            })
            
            logger.info(f"✅ {test_name} completed in {duration:.2f}s")
            
        except Exception as e:
            test_results.append({
                'name': test_name,
                'status': 'FAILED',
                'duration': 0,
                'error': str(e)
            })
            
            logger.error(f"❌ {test_name} FAILED: {e}")
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("📊 PERFORMANCE TEST SUITE RESULTS")
    logger.info("=" * 80)
    
    passed = sum(1 for result in test_results if result['status'] == 'PASSED')
    failed = len(test_results) - passed
    total_duration = sum(result.get('duration', 0) for result in test_results)
    
    for result in test_results:
        status_icon = "✅" if result['status'] == 'PASSED' else "❌"
        duration_str = f"({result['duration']:.2f}s)" if result.get('duration') else ""
        logger.info(f"{status_icon} {result['name']} {duration_str}")
        
        if result['status'] == 'FAILED':
            logger.info(f"    Error: {result.get('error', 'Unknown error')}")
    
    logger.info("-" * 80)
    logger.info(f"🎯 Summary: {passed} passed, {failed} failed")
    logger.info(f"⏱️  Total duration: {total_duration:.2f} seconds")
    
    if failed == 0:
        logger.info("🎉 All performance tests PASSED! System is ready for production load.")
        return True
    else:
        logger.error(f"❌ {failed} performance test(s) FAILED. Please review and optimize.")
        return False

if __name__ == "__main__":
    # Run performance test suite
    success = asyncio.run(run_all_performance_tests())
    exit(0 if success else 1)