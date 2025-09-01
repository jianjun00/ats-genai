#!/usr/bin/env python3
"""
Base Frontfill Job for all data ingestion jobs.
Provides common functionality for checkpointing, duplicate detection, and error handling.
"""

import asyncio
import asyncpg
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass

from config.environment import Environment
from frontfill.checkpoint_manager import CheckpointManager, CheckpointType, JobStatus, Checkpoint

logger = logging.getLogger(__name__)


@dataclass
class FrontfillConfig:
    """Configuration for frontfill jobs."""
    job_name: str
    job_type: str  # instruments, daily_prices, news, economic_events
    vendor: str
    checkpoint_type: CheckpointType
    batch_size: int = 100
    max_retries: int = 3
    retry_delay: int = 5  # seconds
    duplicate_check_hours: int = 24
    error_threshold: int = 10  # Max errors before stopping
    rate_limit_delay: float = 0.1  # seconds between API calls


class BaseFrontfillJob(ABC):
    """Base class for all frontfill jobs."""
    
    def __init__(self, config: FrontfillConfig, connection_pool: asyncpg.Pool, env: Environment):
        self.config = config
        self.pool = connection_pool
        self.env = env
        self.checkpoint_manager = CheckpointManager(connection_pool, env)
        self.current_run_id: Optional[int] = None
        
        # Statistics
        self.stats = {
            "records_processed": 0,
            "records_inserted": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "error_count": 0,
            "api_calls": 0,
            "start_time": None,
            "end_time": None
        }
    
    async def initialize(self):
        """Initialize the job and checkpoint tables."""
        await self.checkpoint_manager.initialize_tables()
        logger.info(f"Initialized frontfill job: {self.config.job_name}")
    
    async def run_frontfill(self, start_checkpoint: Optional[str] = None,
                          end_checkpoint: Optional[str] = None,
                          dry_run: bool = False) -> Dict[str, Any]:
        """
        Run the frontfill operation.
        
        Args:
            start_checkpoint: Override start checkpoint
            end_checkpoint: Stop at this checkpoint
            dry_run: Don't actually insert data, just simulate
            
        Returns:
            Job execution statistics
        """
        self.stats["start_time"] = datetime.now()
        
        try:
            # Get or create starting checkpoint
            current_checkpoint = await self._get_starting_checkpoint(start_checkpoint)
            logger.info(f"Starting frontfill from checkpoint: {current_checkpoint}")
            
            # Start job run tracking
            self.current_run_id = await self.checkpoint_manager.start_job_run(
                job_name=self.config.job_name,
                job_type=self.config.job_type,
                vendor=self.config.vendor,
                checkpoint_start=current_checkpoint,
                metadata={"dry_run": dry_run, "config": self.config.__dict__}
            )
            
            # Run the main frontfill loop
            final_checkpoint = await self._run_frontfill_loop(
                current_checkpoint, end_checkpoint, dry_run
            )
            
            # Complete the job run
            await self.checkpoint_manager.complete_job_run(
                self.current_run_id, 
                JobStatus.COMPLETED if self.stats["error_count"] < self.config.error_threshold else JobStatus.FAILED,
                checkpoint_end=final_checkpoint
            )
            
            # Save final checkpoint
            if not dry_run and final_checkpoint:
                await self._save_checkpoint(final_checkpoint, JobStatus.COMPLETED)
            
            self.stats["end_time"] = datetime.now()
            duration = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
            
            logger.info(f"Frontfill completed in {duration:.1f}s: {self.stats}")
            return self.stats
            
        except Exception as e:
            logger.error(f"Frontfill job failed: {e}")
            self.stats["error_count"] += 1
            
            if self.current_run_id:
                await self.checkpoint_manager.complete_job_run(
                    self.current_run_id, JobStatus.FAILED, str(e)
                )
            
            raise
    
    async def _get_starting_checkpoint(self, override_checkpoint: Optional[str]) -> str:
        """Get the starting checkpoint for the job."""
        if override_checkpoint:
            return override_checkpoint
        
        # Check for existing checkpoint
        checkpoint = await self.checkpoint_manager.get_checkpoint(
            self.config.job_name, self.config.vendor
        )
        
        if checkpoint:
            return checkpoint.checkpoint_value
        
        # Generate default starting checkpoint
        return await self.get_default_starting_checkpoint()
    
    async def _save_checkpoint(self, checkpoint_value: str, status: JobStatus):
        """Save a checkpoint."""
        checkpoint = Checkpoint(
            job_name=self.config.job_name,
            job_type=self.config.job_type,
            vendor=self.config.vendor,
            checkpoint_type=self.config.checkpoint_type,
            checkpoint_value=checkpoint_value,
            metadata={
                "last_run_stats": self.stats,
                "last_update": datetime.now().isoformat()
            },
            created_at=datetime.now(),
            updated_at=datetime.now(),
            status=status
        )
        
        await self.checkpoint_manager.save_checkpoint(checkpoint)
    
    async def _run_frontfill_loop(self, start_checkpoint: str, 
                                end_checkpoint: Optional[str],
                                dry_run: bool) -> str:
        """Main frontfill loop."""
        current_checkpoint = start_checkpoint
        consecutive_errors = 0
        
        while True:
            try:
                # Fetch batch of data
                batch_data, next_checkpoint = await self.fetch_data_batch(
                    current_checkpoint, self.config.batch_size
                )
                
                self.stats["api_calls"] += 1
                
                if not batch_data:
                    logger.info("No more data to process")
                    break
                
                # Check for duplicates
                processed_keys = await self._check_duplicates(batch_data)
                
                # Filter out duplicates
                new_records = []
                for record in batch_data:
                    record_key = await self.checkpoint_manager.get_duplicate_detection_key(
                        self.config.job_type, self.config.vendor, record
                    )
                    if record_key not in processed_keys:
                        new_records.append(record)
                    else:
                        self.stats["records_skipped"] += 1
                
                logger.info(f"Batch: {len(batch_data)} fetched, {len(new_records)} new, "
                          f"{len(batch_data) - len(new_records)} duplicates")
                
                # Process the new records
                if new_records and not dry_run:
                    inserted, updated = await self.process_data_batch(new_records)
                    self.stats["records_inserted"] += inserted
                    self.stats["records_updated"] += updated
                
                self.stats["records_processed"] += len(batch_data)
                
                # Update job run statistics
                if self.current_run_id:
                    await self.checkpoint_manager.update_job_run(
                        self.current_run_id,
                        records_processed=len(batch_data),
                        records_inserted=len(new_records) if not dry_run else 0,
                        records_skipped=len(batch_data) - len(new_records),
                        checkpoint_end=next_checkpoint
                    )
                
                # Save checkpoint periodically
                if self.stats["records_processed"] % (self.config.batch_size * 10) == 0:
                    await self._save_checkpoint(next_checkpoint, JobStatus.RUNNING)
                
                # Check if we've reached the end checkpoint
                if end_checkpoint and next_checkpoint >= end_checkpoint:
                    logger.info(f"Reached end checkpoint: {end_checkpoint}")
                    break
                
                # Check error threshold
                if self.stats["error_count"] >= self.config.error_threshold:
                    logger.error(f"Error threshold exceeded: {self.stats['error_count']}")
                    break
                
                current_checkpoint = next_checkpoint
                consecutive_errors = 0
                
                # Rate limiting
                if self.config.rate_limit_delay > 0:
                    await asyncio.sleep(self.config.rate_limit_delay)
                
            except Exception as e:
                consecutive_errors += 1
                self.stats["error_count"] += 1
                
                logger.error(f"Error in frontfill loop (attempt {consecutive_errors}): {e}")
                
                if consecutive_errors >= self.config.max_retries:
                    logger.error("Max consecutive errors reached, stopping")
                    raise
                
                # Exponential backoff
                wait_time = self.config.retry_delay * (2 ** (consecutive_errors - 1))
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
        
        return current_checkpoint
    
    async def _check_duplicates(self, batch_data: List[Dict[str, Any]]) -> Set[str]:
        """Check for duplicate records."""
        record_keys = []
        for record in batch_data:
            key = await self.checkpoint_manager.get_duplicate_detection_key(
                self.config.job_type, self.config.vendor, record
            )
            record_keys.append(key)
        
        return await self.checkpoint_manager.check_processed_records(
            self.config.job_type, self.config.vendor, record_keys,
            self.config.duplicate_check_hours
        )
    
    # Abstract methods that subclasses must implement
    
    @abstractmethod
    async def get_default_starting_checkpoint(self) -> str:
        """Get the default starting checkpoint for this job type."""
    
    @abstractmethod
    async def fetch_data_batch(self, checkpoint: str, batch_size: int) -> Tuple[List[Dict[str, Any]], str]:
        """
        Fetch a batch of data starting from the given checkpoint.
        
        Returns:
            Tuple of (batch_data, next_checkpoint)
        """
    
    @abstractmethod
    async def process_data_batch(self, batch_data: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        Process a batch of data and store it in the database.
        
        Returns:
            Tuple of (inserted_count, updated_count)
        """
    
    # Utility methods for subclasses
    
    async def handle_rate_limit(self, vendor: str, wait_seconds: int = 60):
        """Handle rate limit by waiting."""
        logger.warning(f"{vendor} rate limit hit, waiting {wait_seconds} seconds")
        await asyncio.sleep(wait_seconds)
    
    async def log_progress(self, message: str):
        """Log progress with statistics."""
        elapsed = (datetime.now() - self.stats["start_time"]).total_seconds()
        rate = self.stats["records_processed"] / elapsed if elapsed > 0 else 0
        
        logger.info(f"{message} | Processed: {self.stats['records_processed']}, "
                   f"Rate: {rate:.1f} records/sec, Errors: {self.stats['error_count']}")
    
    async def get_job_statistics(self) -> Dict[str, Any]:
        """Get current job statistics."""
        stats = self.stats.copy()
        if self.stats["start_time"] and self.stats["end_time"]:
            stats["duration_seconds"] = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
        return stats