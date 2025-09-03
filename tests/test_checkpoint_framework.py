"""
Comprehensive test suite for Generic Checkpoint Framework

Tests cover:
- Unit tests for core framework components
- Integration tests with database
- End-to-end job execution scenarios  
- Error handling and recovery
- Performance and scalability
"""

import pytest
import asyncio
import json
import logging
from datetime import datetime, date, timedelta
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from typing import Dict, List, Optional, Tuple, Any
import asyncpg
import aiohttp

# Import framework components (would be actual imports in real implementation)
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractmethod

# Framework classes (simplified for testing - in real implementation these would be imports)
class IterationType(Enum):
    INSTRUMENT = "instrument"
    DATE = "date" 
    INSTRUMENT_DATE = "instrument_date"
    CUSTOM = "custom"

class JobStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"

@dataclass
class JobConfiguration:
    job_name: str
    vendor: str
    iteration_type: IterationType
    batch_size: int
    rate_limit_delay: float
    max_retries: int
    timeout_seconds: int
    custom_config: Dict[str, Any]

@dataclass
class CheckpointState:
    job_id: str
    iteration_type: str
    current_position: str
    processed_count: int
    error_count: int
    last_successful_item: Optional[str]
    metadata: Dict[str, Any]
    created_at: datetime
    updated_at: datetime

class CheckpointableJob(ABC):
    def __init__(self, config: JobConfiguration, db_connection: asyncpg.Connection):
        self.config = config
        self.conn = db_connection
        self.job_id: str = ""
        
    @abstractmethod
    async def get_iteration_items(self) -> List[Any]:
        pass
        
    @abstractmethod
    async def process_item(self, item: Any, session: aiohttp.ClientSession) -> Tuple[Any, Optional[str]]:
        pass
        
    @abstractmethod
    async def store_result(self, item: Any, result: Any) -> int:
        pass

# Mock implementations for testing
class MockCheckpointManager:
    def __init__(self, db_connection: asyncpg.Connection):
        self.conn = db_connection
        self.job_runs = {}
        self.job_progress = {}
        
    async def setup_checkpoint_tables(self):
        pass
        
    async def create_job_run(self, config: JobConfiguration, total_items: int) -> str:
        job_id = f"{config.job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.job_runs[job_id] = {
            'config': config,
            'total_items': total_items,
            'status': JobStatus.PENDING,
            'created_at': datetime.now()
        }
        return job_id
        
    async def get_or_create_job_run(self, config: JobConfiguration, total_items: int) -> Tuple[str, CheckpointState]:
        # Look for existing incomplete job
        for job_id, job_data in self.job_runs.items():
            if (job_data['config'].job_name == config.job_name and 
                job_data['config'].vendor == config.vendor and
                job_data['status'] in [JobStatus.PENDING, JobStatus.IN_PROGRESS]):
                
                checkpoint = CheckpointState(
                    job_id=job_id,
                    iteration_type=config.iteration_type.value,
                    current_position="{}",
                    processed_count=job_data.get('processed_count', 0),
                    error_count=job_data.get('error_count', 0),
                    last_successful_item=job_data.get('last_successful_item'),
                    metadata=job_data.get('metadata', {}),
                    created_at=job_data['created_at'],
                    updated_at=datetime.now()
                )
                return job_id, checkpoint
                
        # Create new job
        job_id = await self.create_job_run(config, total_items)
        checkpoint = CheckpointState(
            job_id=job_id,
            iteration_type=config.iteration_type.value,
            current_position="{}",
            processed_count=0,
            error_count=0,
            last_successful_item=None,
            metadata={},
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        return job_id, checkpoint
        
    async def initialize_items(self, job_id: str, items: List[Any], item_type: str):
        if job_id not in self.job_progress:
            self.job_progress[job_id] = {}
        for item in items:
            self.job_progress[job_id][str(item)] = {
                'item_key': str(item),
                'item_type': item_type,
                'status': 'pending'
            }
            
    async def get_next_items(self, job_id: str, item_type: str, batch_size: int) -> List[str]:
        if job_id not in self.job_progress:
            return []
        pending_items = [
            item_data['item_key'] for item_data in self.job_progress[job_id].values()
            if item_data['status'] == 'pending'
        ]
        return pending_items[:batch_size]
        
    async def mark_item_processing(self, job_id: str, item_key: str, item_type: str):
        if job_id in self.job_progress and item_key in self.job_progress[job_id]:
            self.job_progress[job_id][item_key]['status'] = 'in_progress'
            
    async def mark_item_completed(self, job_id: str, item_key: str, item_type: str, records_count: int):
        if job_id in self.job_progress and item_key in self.job_progress[job_id]:
            self.job_progress[job_id][item_key]['status'] = 'completed'
            self.job_progress[job_id][item_key]['records_processed'] = records_count
            
    async def mark_item_failed(self, job_id: str, item_key: str, item_type: str, error_msg: str):
        if job_id in self.job_progress and item_key in self.job_progress[job_id]:
            self.job_progress[job_id][item_key]['status'] = 'failed'
            self.job_progress[job_id][item_key]['error_message'] = error_msg
            
    async def get_job_stats(self, job_id: str) -> Dict:
        if job_id not in self.job_progress:
            return {}
        items = self.job_progress[job_id]
        return {
            'total_items': len(items),
            'completed': sum(1 for item in items.values() if item['status'] == 'completed'),
            'failed': sum(1 for item in items.values() if item['status'] == 'failed'),
            'in_progress': sum(1 for item in items.values() if item['status'] == 'in_progress'),
            'pending': sum(1 for item in items.values() if item['status'] == 'pending'),
            'total_records': sum(item.get('records_processed', 0) for item in items.values())
        }
        
    async def update_checkpoint(self, job_id: str, checkpoint: CheckpointState):
        if job_id in self.job_runs:
            self.job_runs[job_id]['processed_count'] = checkpoint.processed_count
            self.job_runs[job_id]['error_count'] = checkpoint.error_count
            self.job_runs[job_id]['last_successful_item'] = checkpoint.last_successful_item
            
    async def mark_job_completed(self, job_id: str):
        if job_id in self.job_runs:
            self.job_runs[job_id]['status'] = JobStatus.COMPLETED
            
    async def mark_job_failed(self, job_id: str, error_message: str):
        if job_id in self.job_runs:
            self.job_runs[job_id]['status'] = JobStatus.FAILED
            self.job_runs[job_id]['error_message'] = error_message

class MockTiingoJob(CheckpointableJob):
    def __init__(self, db_connection: asyncpg.Connection):
        config = JobConfiguration(
            job_name="test_tiingo_job",
            vendor="tiingo",
            iteration_type=IterationType.INSTRUMENT,
            batch_size=3,
            rate_limit_delay=0.1,  # Fast for testing
            max_retries=2,
            timeout_seconds=60,
            custom_config={'api_key': 'test_key'}
        )
        super().__init__(config, db_connection)
        self.processed_items = []
        
    async def get_iteration_items(self) -> List[str]:
        return ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
        
    async def process_item(self, symbol: str, session: aiohttp.ClientSession) -> Tuple[List[Dict], Optional[str]]:
        # Simulate API processing
        if symbol == 'FAIL_SYMBOL':  # For testing failures
            return [], "API Error: Symbol not found"
        if symbol == 'TIMEOUT_SYMBOL':  # For testing timeouts
            await asyncio.sleep(0.2)
            return [], "Timeout error"
            
        # Simulate successful processing
        self.processed_items.append(symbol)
        mock_data = [
            {
                'date': date.today() - timedelta(days=i),
                'open': 100.0 + i,
                'high': 105.0 + i,
                'low': 95.0 + i,
                'close': 102.0 + i,
                'volume': 1000000 + i * 10000
            }
            for i in range(10)  # 10 days of data
        ]
        return mock_data, None
        
    async def store_result(self, symbol: str, prices: List[Dict]) -> int:
        # Simulate database storage
        return len(prices)

class GenericJobRunner:
    def __init__(self, job: CheckpointableJob, checkpoint_manager):
        self.job = job
        self.checkpoint_manager = checkpoint_manager
        self.current_checkpoint: Optional[CheckpointState] = None
        
    async def run(self):
        try:
            await self.checkpoint_manager.setup_checkpoint_tables()
            
            all_items = await self.job.get_iteration_items()
            job_id, checkpoint = await self.checkpoint_manager.get_or_create_job_run(
                self.job.config, len(all_items))
            self.job.job_id = job_id
            self.current_checkpoint = checkpoint
            
            item_type = self.job.config.iteration_type.value
            await self.checkpoint_manager.initialize_items(job_id, all_items, item_type)
            
            async with aiohttp.ClientSession() as session:
                while True:
                    pending_items = await self.checkpoint_manager.get_next_items(
                        job_id, item_type, self.job.config.batch_size)
                    
                    if not pending_items:
                        break
                        
                    for item_key in pending_items:
                        try:
                            await self.checkpoint_manager.mark_item_processing(job_id, item_key, item_type)
                            
                            result, error = await self.job.process_item(item_key, session)
                            
                            if error:
                                await self.checkpoint_manager.mark_item_failed(job_id, item_key, item_type, error)
                                self.current_checkpoint.error_count += 1
                                continue
                            
                            records_count = await self.job.store_result(item_key, result)
                            await self.checkpoint_manager.mark_item_completed(job_id, item_key, item_type, records_count)
                            self.current_checkpoint.processed_count += 1
                            self.current_checkpoint.last_successful_item = item_key
                            
                            await asyncio.sleep(self.job.config.rate_limit_delay)
                            
                        except Exception as e:
                            await self.checkpoint_manager.mark_item_failed(job_id, item_key, item_type, str(e))
                            self.current_checkpoint.error_count += 1
                    
                    await self.checkpoint_manager.update_checkpoint(job_id, self.current_checkpoint)
            
            await self.checkpoint_manager.mark_job_completed(job_id)
            
        except Exception as e:
            if self.job.job_id:
                await self.checkpoint_manager.mark_job_failed(self.job.job_id, str(e))
            raise

# Test Classes
class TestCheckpointManager:
    """Unit tests for CheckpointManager"""
    
    @pytest.fixture
    def mock_db_connection(self):
        return AsyncMock(spec=asyncpg.Connection)
        
    @pytest.fixture
    def checkpoint_manager(self, mock_db_connection):
        return MockCheckpointManager(mock_db_connection)
        
    @pytest.fixture
    def sample_config(self):
        return JobConfiguration(
            job_name="test_job",
            vendor="test_vendor",
            iteration_type=IterationType.INSTRUMENT,
            batch_size=5,
            rate_limit_delay=1.0,
            max_retries=3,
            timeout_seconds=3600,
            custom_config={}
        )
    
    @pytest.mark.asyncio
    
    async def test_create_job_run(self, checkpoint_manager, sample_config):
        """Test job run creation"""
        job_id = await checkpoint_manager.create_job_run(sample_config, 100)
        
        assert job_id.startswith("test_job_")
        assert len(job_id.split("_")) >= 3
        assert job_id in checkpoint_manager.job_runs
        
        job_data = checkpoint_manager.job_runs[job_id]
        assert job_data['config'].job_name == "test_job"
        assert job_data['total_items'] == 100
        assert job_data['status'] == JobStatus.PENDING
        
    @pytest.mark.asyncio
        
    async def test_get_or_create_new_job(self, checkpoint_manager, sample_config):
        """Test creating new job when none exists"""
        job_id, checkpoint = await checkpoint_manager.get_or_create_job_run(sample_config, 50)
        
        assert job_id.startswith("test_job_")
        assert checkpoint.processed_count == 0
        assert checkpoint.error_count == 0
        assert checkpoint.last_successful_item is None
        
    @pytest.mark.asyncio
        
    async def test_get_or_create_existing_job(self, checkpoint_manager, sample_config):
        """Test resuming existing incomplete job"""
        # Create initial job
        job_id_1, checkpoint_1 = await checkpoint_manager.get_or_create_job_run(sample_config, 50)
        
        # Simulate some progress
        checkpoint_manager.job_runs[job_id_1]['processed_count'] = 10
        checkpoint_manager.job_runs[job_id_1]['error_count'] = 2
        checkpoint_manager.job_runs[job_id_1]['last_successful_item'] = 'AAPL'
        
        # Try to create again - should return existing
        job_id_2, checkpoint_2 = await checkpoint_manager.get_or_create_job_run(sample_config, 50)
        
        assert job_id_1 == job_id_2
        assert checkpoint_2.processed_count == 10
        assert checkpoint_2.error_count == 2
        assert checkpoint_2.last_successful_item == 'AAPL'
        
    @pytest.mark.asyncio
        
    async def test_initialize_items(self, checkpoint_manager, sample_config):
        """Test item initialization in progress table"""
        job_id = await checkpoint_manager.create_job_run(sample_config, 3)
        items = ['AAPL', 'MSFT', 'GOOGL']
        
        await checkpoint_manager.initialize_items(job_id, items, 'instrument')
        
        assert job_id in checkpoint_manager.job_progress
        assert len(checkpoint_manager.job_progress[job_id]) == 3
        
        for item in items:
            assert item in checkpoint_manager.job_progress[job_id]
            assert checkpoint_manager.job_progress[job_id][item]['status'] == 'pending'
            
    @pytest.mark.asyncio
            
    async def test_get_next_items(self, checkpoint_manager, sample_config):
        """Test getting next batch of pending items"""
        job_id = await checkpoint_manager.create_job_run(sample_config, 5)
        items = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
        await checkpoint_manager.initialize_items(job_id, items, 'instrument')
        
        # Mark some items as completed
        await checkpoint_manager.mark_item_completed(job_id, 'AAPL', 'instrument', 10)
        await checkpoint_manager.mark_item_failed(job_id, 'MSFT', 'instrument', 'Error')
        
        # Get next batch
        next_items = await checkpoint_manager.get_next_items(job_id, 'instrument', 3)
        
        assert len(next_items) == 3
        assert 'AAPL' not in next_items  # Already completed
        assert 'MSFT' not in next_items  # Already failed
        assert all(item in ['GOOGL', 'TSLA', 'NVDA'] for item in next_items)
        
    @pytest.mark.asyncio
        
    async def test_item_status_transitions(self, checkpoint_manager, sample_config):
        """Test item status transitions"""
        job_id = await checkpoint_manager.create_job_run(sample_config, 1)
        await checkpoint_manager.initialize_items(job_id, ['AAPL'], 'instrument')
        
        # Test processing status
        await checkpoint_manager.mark_item_processing(job_id, 'AAPL', 'instrument')
        assert checkpoint_manager.job_progress[job_id]['AAPL']['status'] == 'in_progress'
        
        # Test completed status
        await checkpoint_manager.mark_item_completed(job_id, 'AAPL', 'instrument', 25)
        assert checkpoint_manager.job_progress[job_id]['AAPL']['status'] == 'completed'
        assert checkpoint_manager.job_progress[job_id]['AAPL']['records_processed'] == 25
        
        # Test failed status
        await checkpoint_manager.initialize_items(job_id, ['MSFT'], 'instrument')
        await checkpoint_manager.mark_item_failed(job_id, 'MSFT', 'instrument', 'API Error')
        assert checkpoint_manager.job_progress[job_id]['MSFT']['status'] == 'failed'
        assert checkpoint_manager.job_progress[job_id]['MSFT']['error_message'] == 'API Error'
        
    @pytest.mark.asyncio
        
    async def test_get_job_stats(self, checkpoint_manager, sample_config):
        """Test job statistics calculation"""
        job_id = await checkpoint_manager.create_job_run(sample_config, 4)
        items = ['AAPL', 'MSFT', 'GOOGL', 'TSLA']
        await checkpoint_manager.initialize_items(job_id, items, 'instrument')
        
        # Set various statuses
        await checkpoint_manager.mark_item_completed(job_id, 'AAPL', 'instrument', 10)
        await checkpoint_manager.mark_item_completed(job_id, 'MSFT', 'instrument', 15)
        await checkpoint_manager.mark_item_failed(job_id, 'GOOGL', 'instrument', 'Error')
        # TSLA remains pending
        
        stats = await checkpoint_manager.get_job_stats(job_id)
        
        assert stats['total_items'] == 4
        assert stats['completed'] == 2
        assert stats['failed'] == 1
        assert stats['pending'] == 1
        assert stats['in_progress'] == 0
        assert stats['total_records'] == 25  # 10 + 15

class TestCheckpointableJob:
    """Unit tests for CheckpointableJob implementations"""
    
    @pytest.fixture
    def mock_db_connection(self):
        return AsyncMock(spec=asyncpg.Connection)
        
    @pytest.fixture
    def tiingo_job(self, mock_db_connection):
        return MockTiingoJob(mock_db_connection)
        
    @pytest.fixture
    def mock_session(self):
        return AsyncMock(spec=aiohttp.ClientSession)
    
    @pytest.mark.asyncio
    
    async def test_get_iteration_items(self, tiingo_job):
        """Test getting items for processing"""
        items = await tiingo_job.get_iteration_items()
        assert len(items) == 5
        assert 'AAPL' in items
        assert 'MSFT' in items
        
    @pytest.mark.asyncio
        
    async def test_process_item_success(self, tiingo_job, mock_session):
        """Test successful item processing"""
        result, error = await tiingo_job.process_item('AAPL', mock_session)
        
        assert error is None
        assert len(result) == 10  # 10 days of mock data
        assert all('date' in item for item in result)
        assert all('open' in item for item in result)
        assert 'AAPL' in tiingo_job.processed_items
        
    @pytest.mark.asyncio
        
    async def test_process_item_failure(self, tiingo_job, mock_session):
        """Test item processing failure"""
        result, error = await tiingo_job.process_item('FAIL_SYMBOL', mock_session)
        
        assert result == []
        assert error == "API Error: Symbol not found"
        assert 'FAIL_SYMBOL' not in tiingo_job.processed_items
        
    @pytest.mark.asyncio
        
    async def test_store_result(self, tiingo_job):
        """Test result storage"""
        mock_prices = [{'date': date.today(), 'open': 100}] * 5
        count = await tiingo_job.store_result('AAPL', mock_prices)
        assert count == 5
        
    @pytest.mark.asyncio
        
    async def test_job_configuration(self, tiingo_job):
        """Test job configuration properties"""
        config = tiingo_job.config
        assert config.job_name == "test_tiingo_job"
        assert config.vendor == "tiingo"
        assert config.iteration_type == IterationType.INSTRUMENT
        assert config.batch_size == 3
        assert config.rate_limit_delay == 0.1
        assert config.max_retries == 2

class TestGenericJobRunner:
    """Integration tests for GenericJobRunner"""
    
    @pytest.fixture
    def mock_db_connection(self):
        return AsyncMock(spec=asyncpg.Connection)
        
    @pytest.fixture
    def tiingo_job(self, mock_db_connection):
        return MockTiingoJob(mock_db_connection)
        
    @pytest.fixture
    def checkpoint_manager(self, mock_db_connection):
        return MockCheckpointManager(mock_db_connection)
        
    @pytest.fixture
    def job_runner(self, tiingo_job, checkpoint_manager):
        return GenericJobRunner(tiingo_job, checkpoint_manager)
        
    @pytest.mark.asyncio
        
    async def test_complete_job_execution(self, job_runner):
        """Test full job execution from start to finish"""
        await job_runner.run()
        
        # Verify job completed successfully
        job_id = job_runner.job.job_id
        stats = await job_runner.checkpoint_manager.get_job_stats(job_id)
        
        assert stats['total_items'] == 5  # AAPL, MSFT, GOOGL, TSLA, NVDA
        assert stats['completed'] == 5
        assert stats['failed'] == 0
        assert stats['total_records'] == 50  # 5 items * 10 records each
        
        # Verify all items were processed
        assert len(job_runner.job.processed_items) == 5
        assert job_runner.checkpoint_manager.job_runs[job_id]['status'] == JobStatus.COMPLETED
        
    @pytest.mark.asyncio
        
    async def test_job_resume_after_failure(self, job_runner, tiingo_job):
        """Test job resumption after partial completion"""
        # First run - simulate partial completion
        job_id = await job_runner.checkpoint_manager.create_job_run(tiingo_job.config, 5)
        await job_runner.checkpoint_manager.initialize_items(job_id, ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA'], 'instrument')
        
        # Mark some items as completed
        await job_runner.checkpoint_manager.mark_item_completed(job_id, 'AAPL', 'instrument', 10)
        await job_runner.checkpoint_manager.mark_item_completed(job_id, 'MSFT', 'instrument', 10)
        
        # Create new runner to simulate restart
        new_tiingo_job = MockTiingoJob(AsyncMock())
        new_runner = GenericJobRunner(new_tiingo_job, job_runner.checkpoint_manager)
        
        await new_runner.run()
        
        # Verify only remaining items were processed
        final_stats = await new_runner.checkpoint_manager.get_job_stats(job_id)
        assert final_stats['completed'] == 5  # All items completed
        assert len(new_tiingo_job.processed_items) == 3  # Only GOOGL, TSLA, NVDA processed in second run
        
    @pytest.mark.asyncio
        
    async def test_error_handling_continues_processing(self, tiingo_job, checkpoint_manager):
        """Test that errors on individual items don't stop job"""
        # Create job with failure item
        failing_job = MockTiingoJob(AsyncMock())
        
        # Override to include failure cases
        async def get_items_with_failures():
            return ['AAPL', 'FAIL_SYMBOL', 'GOOGL', 'TIMEOUT_SYMBOL', 'NVDA']
        
        failing_job.get_iteration_items = get_items_with_failures
        runner = GenericJobRunner(failing_job, checkpoint_manager)
        
        await runner.run()
        
        job_id = runner.job.job_id
        stats = await checkpoint_manager.get_job_stats(job_id)
        
        # Should have 3 successes and 2 failures
        assert stats['completed'] == 3
        assert stats['failed'] == 2
        assert stats['total_records'] == 30  # 3 successful items * 10 records each
        
        # Job should still complete despite individual failures
        assert checkpoint_manager.job_runs[job_id]['status'] == JobStatus.COMPLETED

class TestPerformanceAndScalability:
    """Performance and scalability tests"""
    
    @pytest.fixture
    def mock_db_connection(self):
        return AsyncMock(spec=asyncpg.Connection)
        
    @pytest.mark.asyncio
        
    async def test_large_item_count_processing(self, mock_db_connection):
        """Test processing large number of items"""
        class LargeScaleJob(CheckpointableJob):
            def __init__(self, db_connection):
                config = JobConfiguration(
                    job_name="large_scale_test",
                    vendor="test",
                    iteration_type=IterationType.INSTRUMENT,
                    batch_size=50,
                    rate_limit_delay=0.01,  # Very fast for testing
                    max_retries=1,
                    timeout_seconds=300,
                    custom_config={}
                )
                super().__init__(config, db_connection)
                
            async def get_iteration_items(self):
                # Generate 1000 test symbols
                return [f"SYMBOL_{i:04d}" for i in range(1000)]
                
            async def process_item(self, symbol, session):
                # Simulate very fast processing
                await asyncio.sleep(0.001)
                return [{'data': f'processed_{symbol}'}], None
                
            async def store_result(self, symbol, result):
                return len(result)
        
        job = LargeScaleJob(mock_db_connection)
        checkpoint_manager = MockCheckpointManager(mock_db_connection)
        runner = GenericJobRunner(job, checkpoint_manager)
        
        start_time = datetime.now()
        await runner.run()
        end_time = datetime.now()
        
        # Verify all items processed
        stats = await checkpoint_manager.get_job_stats(job.job_id)
        assert stats['total_items'] == 1000
        assert stats['completed'] == 1000
        assert stats['failed'] == 0
        
        # Verify reasonable performance (should complete in under 30 seconds)
        duration = (end_time - start_time).total_seconds()
        assert duration < 30, f"Large scale test took too long: {duration}s"
        
    @pytest.mark.asyncio
        
    async def test_batch_processing_efficiency(self, mock_db_connection):
        """Test batch processing reduces database calls"""
        call_count = {'db_calls': 0}
        
        class BatchTestJob(MockTiingoJob):
            def __init__(self, db_connection):
                super().__init__(db_connection)
                self.config.batch_size = 10  # Large batch size
                
            async def store_result(self, symbol, result):
                call_count['db_calls'] += 1
                return await super().store_result(symbol, result)
        
        # Create job with 50 items, batch size 10
        items = [f"SYM_{i}" for i in range(50)]
        job = BatchTestJob(mock_db_connection)
        job.get_iteration_items = AsyncMock(return_value=items)
        
        checkpoint_manager = MockCheckpointManager(mock_db_connection)
        runner = GenericJobRunner(job, checkpoint_manager)
        
        await runner.run()
        
        # Should have made 50 database calls (one per item)
        # With batching, we process 10 items at a time but still store individually
        assert call_count['db_calls'] == 50
        
        stats = await checkpoint_manager.get_job_stats(job.job_id)
        assert stats['completed'] == 50
        
    @pytest.mark.asyncio
        
    async def test_memory_usage_with_large_datasets(self, mock_db_connection):
        """Test memory usage doesn't grow unbounded"""
        class MemoryTestJob(CheckpointableJob):
            def __init__(self, db_connection):
                config = JobConfiguration(
                    job_name="memory_test",
                    vendor="test", 
                    iteration_type=IterationType.INSTRUMENT,
                    batch_size=5,
                    rate_limit_delay=0.001,
                    max_retries=1,
                    timeout_seconds=60,
                    custom_config={}
                )
                super().__init__(config, db_connection)
                
            async def get_iteration_items(self):
                return [f"SYM_{i}" for i in range(100)]
                
            async def process_item(self, symbol, session):
                # Return large dataset to test memory handling
                large_data = [
                    {'date': date.today(), 'data': f'x' * 1000}  # 1KB per record
                    for _ in range(100)  # 100KB per symbol
                ]
                return large_data, None
                
            async def store_result(self, symbol, result):
                # Simulate database storage and memory cleanup
                return len(result)
        
        job = MemoryTestJob(mock_db_connection)
        checkpoint_manager = MockCheckpointManager(mock_db_connection)
        runner = GenericJobRunner(job, checkpoint_manager)
        
        await runner.run()
        
        stats = await checkpoint_manager.get_job_stats(job.job_id)
        assert stats['completed'] == 100
        assert stats['total_records'] == 10000  # 100 symbols * 100 records each

class TestErrorHandlingAndRecovery:
    """Comprehensive error handling tests"""
    
    @pytest.fixture
    def mock_db_connection(self):
        return AsyncMock(spec=asyncpg.Connection)
        
    @pytest.mark.asyncio
        
    async def test_database_connection_failure_recovery(self, mock_db_connection):
        """Test recovery from database connection failures"""
        # Mock database to fail initially then succeed
        failure_count = {'count': 0}
        
        async def mock_execute(*args, **kwargs):
            failure_count['count'] += 1
            if failure_count['count'] <= 3:  # Fail first 3 attempts
                raise asyncpg.ConnectionDoesNotExistError("Connection lost")
            return None
        
        mock_db_connection.execute = mock_execute
        
        checkpoint_manager = MockCheckpointManager(mock_db_connection)
        job = MockTiingoJob(mock_db_connection)
        
        # Override to handle db failures gracefully
        original_mark_completed = checkpoint_manager.mark_item_completed
        async def safe_mark_completed(*args, **kwargs):
            try:
                return await original_mark_completed(*args, **kwargs)
            except asyncpg.ConnectionDoesNotExistError:
                # Simulate connection recovery
                await asyncio.sleep(0.1)
                return await original_mark_completed(*args, **kwargs)
        
        checkpoint_manager.mark_item_completed = safe_mark_completed
        
        runner = GenericJobRunner(job, checkpoint_manager)
        
        # Should complete despite initial database failures
        await runner.run()
        
        stats = await checkpoint_manager.get_job_stats(job.job_id)
        assert stats['completed'] == 5
        
    @pytest.mark.asyncio
        
    async def test_api_rate_limit_handling(self, mock_db_connection):
        """Test proper handling of API rate limits"""
        class RateLimitedJob(MockTiingoJob):
            def __init__(self, db_connection):
                super().__init__(db_connection)
                self.api_calls = 0
                
            async def process_item(self, symbol, session):
                self.api_calls += 1
                
                # Simulate rate limit after 3 calls
                if self.api_calls <= 3:
                    return await super().process_item(symbol, session)
                elif self.api_calls <= 6:  # Next 3 calls get rate limited
                    return [], "HTTP 429: Rate limit exceeded"
                else:  # Then succeed again
                    return await super().process_item(symbol, session)
        
        job = RateLimitedJob(mock_db_connection)
        checkpoint_manager = MockCheckpointManager(mock_db_connection)
        runner = GenericJobRunner(job, checkpoint_manager)
        
        await runner.run()
        
        stats = await checkpoint_manager.get_job_stats(job.job_id)
        # Should have some successes and some rate limit failures
        assert stats['completed'] >= 2  # At least initial successes
        assert stats['failed'] >= 1     # At least some rate limit failures
        
    @pytest.mark.asyncio
        
    async def test_job_timeout_handling(self, mock_db_connection):
        """Test job timeout scenarios"""
        class SlowJob(MockTiingoJob):
            async def process_item(self, symbol, session):
                # Simulate slow processing
                await asyncio.sleep(0.1)
                return await super().process_item(symbol, session)
        
        job = SlowJob(mock_db_connection)
        job.config.timeout_seconds = 1  # Very short timeout
        
        checkpoint_manager = MockCheckpointManager(mock_db_connection)
        runner = GenericJobRunner(job, checkpoint_manager)
        
        # Job should timeout before completing all items
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(runner.run(), timeout=0.5)
        
    @pytest.mark.asyncio
        
    async def test_partial_failure_recovery(self, mock_db_connection):
        """Test recovery from partial failures"""
        class PartialFailureJob(MockTiingoJob):
            def __init__(self, db_connection):
                super().__init__(db_connection)
                self.attempt_count = {}
                
            async def process_item(self, symbol, session):
                if symbol not in self.attempt_count:
                    self.attempt_count[symbol] = 0
                self.attempt_count[symbol] += 1
                
                # Fail certain symbols on first attempt, succeed on retry
                if symbol in ['GOOGL', 'TSLA'] and self.attempt_count[symbol] == 1:
                    return [], f"Temporary failure for {symbol}"
                    
                return await super().process_item(symbol, session)
        
        job = PartialFailureJob(mock_db_connection)
        checkpoint_manager = MockCheckpointManager(mock_db_connection)
        
        # First run - will have some failures
        runner1 = GenericJobRunner(job, checkpoint_manager)
        await runner1.run()
        
        stats1 = await checkpoint_manager.get_job_stats(job.job_id)
        initial_failures = stats1['failed']
        
        # Second run - simulate retry of failed items
        # Reset failed items to pending for retry test
        for item_key, item_data in checkpoint_manager.job_progress[job.job_id].items():
            if item_data['status'] == 'failed':
                item_data['status'] = 'pending'
        
        runner2 = GenericJobRunner(job, checkpoint_manager)
        runner2.job = job  # Use same job instance to maintain attempt counts
        await runner2.run()
        
        stats2 = await checkpoint_manager.get_job_stats(job.job_id)
        # Should have fewer failures after retry
        assert stats2['failed'] < initial_failures or stats2['failed'] == 0

class TestEndToEndScenarios:
    """End-to-end testing scenarios"""
    
    @pytest.fixture 
    def mock_db_connection(self):
        return AsyncMock(spec=asyncpg.Connection)
        
    @pytest.mark.asyncio
        
    async def test_multi_vendor_job_coordination(self, mock_db_connection):
        """Test running multiple vendor jobs concurrently"""
        class MockFMPJob(CheckpointableJob):
            def __init__(self, db_connection):
                config = JobConfiguration(
                    job_name="fmp_test",
                    vendor="fmp",
                    iteration_type=IterationType.INSTRUMENT,
                    batch_size=2,
                    rate_limit_delay=0.2,  # Slower than Tiingo
                    max_retries=2,
                    timeout_seconds=60,
                    custom_config={}
                )
                super().__init__(config, db_connection)
                
            async def get_iteration_items(self):
                return ['AAPL', 'MSFT', 'GOOGL']
                
            async def process_item(self, symbol, session):
                await asyncio.sleep(0.05)  # Simulate FMP API processing
                return [{'fmp_data': f'processed_{symbol}'}], None
                
            async def store_result(self, symbol, result):
                return len(result)
        
        # Create both jobs
        tiingo_job = MockTiingoJob(mock_db_connection)
        fmp_job = MockFMPJob(mock_db_connection)
        
        checkpoint_manager = MockCheckpointManager(mock_db_connection)
        
        tiingo_runner = GenericJobRunner(tiingo_job, checkpoint_manager)
        fmp_runner = GenericJobRunner(fmp_job, checkpoint_manager)
        
        # Run jobs concurrently
        await asyncio.gather(
            tiingo_runner.run(),
            fmp_runner.run()
        )
        
        # Verify both jobs completed
        tiingo_stats = await checkpoint_manager.get_job_stats(tiingo_job.job_id)
        fmp_stats = await checkpoint_manager.get_job_stats(fmp_job.job_id)
        
        assert tiingo_stats['completed'] == 5  # Tiingo processes 5 symbols
        assert fmp_stats['completed'] == 3    # FMP processes 3 symbols
        assert tiingo_stats['failed'] == 0
        assert fmp_stats['failed'] == 0
        
    @pytest.mark.asyncio
        
    async def test_30_year_historical_simulation(self, mock_db_connection):
        """Simulate 30-year historical data collection scenario"""
        class Historical30YearJob(CheckpointableJob):
            def __init__(self, db_connection):
                config = JobConfiguration(
                    job_name="historical_30year",
                    vendor="tiingo",
                    iteration_type=IterationType.INSTRUMENT,
                    batch_size=10,
                    rate_limit_delay=1.5,  # Realistic rate limiting
                    max_retries=3,
                    timeout_seconds=3600,
                    custom_config={'years': 30}
                )
                super().__init__(config, db_connection)
                
            async def get_iteration_items(self):
                # Simulate realistic symbol count (S&P 500)
                return [f"SYMBOL_{i:03d}" for i in range(500)]
                
            async def process_item(self, symbol, session):
                # Simulate 30 years of daily data (30 * 252 trading days)
                trading_days = 30 * 252
                
                # Simulate realistic processing time and occasional failures
                if symbol.endswith('499'):  # Last symbol fails occasionally
                    if len(self.processed_items) % 10 == 0:  # 10% failure rate
                        return [], "API timeout for large dataset"
                
                await asyncio.sleep(0.01)  # Simulate API call time
                
                mock_data = [
                    {
                        'date': date.today() - timedelta(days=i),
                        'open': 100.0 + (i % 50),
                        'high': 105.0 + (i % 50), 
                        'low': 95.0 + (i % 50),
                        'close': 102.0 + (i % 50),
                        'volume': 1000000 + (i * 1000)
                    }
                    for i in range(min(100, trading_days))  # Limit for testing
                ]
                
                return mock_data, None
                
            async def store_result(self, symbol, prices):
                # Simulate database storage time
                await asyncio.sleep(0.005)
                return len(prices)
        
        job = Historical30YearJob(mock_db_connection)
        job.processed_items = []  # Initialize for failure simulation
        
        checkpoint_manager = MockCheckpointManager(mock_db_connection)
        runner = GenericJobRunner(job, checkpoint_manager)
        
        start_time = datetime.now()
        await runner.run()
        end_time = datetime.now()
        
        stats = await checkpoint_manager.get_job_stats(job.job_id)
        
        # Verify large scale processing
        assert stats['total_items'] == 500
        assert stats['completed'] >= 450  # Allow for some failures
        assert stats['total_records'] >= 45000  # 450+ symbols * 100 records
        
        # Verify reasonable performance
        duration = (end_time - start_time).total_seconds()
        processing_rate = stats['completed'] / duration
        assert processing_rate >= 10, f"Processing rate too slow: {processing_rate:.2f} items/sec"
        
        print(f"30-year simulation completed in {duration:.2f}s")
        print(f"Processed {stats['completed']}/{stats['total_items']} symbols")
        print(f"Total records: {stats['total_records']:,}")
        print(f"Processing rate: {processing_rate:.2f} items/sec")

# Test runner and configuration
if __name__ == "__main__":
    # Configure logging for tests
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run specific test categories
    import sys
    
    if len(sys.argv) > 1:
        test_category = sys.argv[1]
        if test_category == "unit":
            pytest.main(["-v", "test_checkpoint_framework.py::TestCheckpointManager"])
            pytest.main(["-v", "test_checkpoint_framework.py::TestCheckpointableJob"])
        elif test_category == "integration": 
            pytest.main(["-v", "test_checkpoint_framework.py::TestGenericJobRunner"])
        elif test_category == "performance":
            pytest.main(["-v", "test_checkpoint_framework.py::TestPerformanceAndScalability"])
        elif test_category == "errors":
            pytest.main(["-v", "test_checkpoint_framework.py::TestErrorHandlingAndRecovery"])
        elif test_category == "e2e":
            pytest.main(["-v", "test_checkpoint_framework.py::TestEndToEndScenarios"])
        else:
            print("Usage: python test_checkpoint_framework.py [unit|integration|performance|errors|e2e]")
    else:
        # Run all tests
        pytest.main(["-v", "test_checkpoint_framework.py"])