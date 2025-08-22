#!/usr/bin/env python3
"""
Checkpoint Management System for Frontfill Operations.
Handles checkpointing, duplicate detection, and recovery for data ingestion jobs.
"""

import asyncio
import asyncpg
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Set
from dataclasses import dataclass, asdict
from enum import Enum
import json

from config.environment import Environment

logger = logging.getLogger(__name__)


class CheckpointType(Enum):
    """Types of checkpoints supported."""
    TIMESTAMP = "timestamp"
    SEQUENCE_ID = "sequence_id"
    OFFSET = "offset"
    CURSOR = "cursor"


class JobStatus(Enum):
    """Job execution statuses."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


@dataclass
class Checkpoint:
    """Checkpoint data structure."""
    job_name: str
    job_type: str  # instruments, daily_prices, news, economic_events
    vendor: str
    checkpoint_type: CheckpointType
    checkpoint_value: str
    metadata: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    status: JobStatus = JobStatus.PENDING


@dataclass
class JobRun:
    """Job execution run information."""
    id: Optional[int]
    job_name: str
    job_type: str
    vendor: str
    start_time: datetime
    end_time: Optional[datetime]
    status: JobStatus
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    error_count: int = 0
    error_message: Optional[str] = None
    checkpoint_start: Optional[str] = None
    checkpoint_end: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class CheckpointManager:
    """Manages checkpoints for frontfill operations."""
    
    def __init__(self, connection_pool: asyncpg.Pool, env: Environment):
        self.pool = connection_pool
        self.env = env
    
    async def initialize_tables(self):
        """Initialize checkpoint management tables."""
        checkpoints_table = self.env.get_table_name("frontfill_checkpoints")
        job_runs_table = self.env.get_table_name("frontfill_job_runs")
        
        async with self.pool.acquire() as conn:
            # Create checkpoints table
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {checkpoints_table} (
                    job_name VARCHAR(255) NOT NULL,
                    job_type VARCHAR(100) NOT NULL,
                    vendor VARCHAR(100) NOT NULL,
                    checkpoint_type VARCHAR(50) NOT NULL,
                    checkpoint_value TEXT NOT NULL,
                    metadata JSONB DEFAULT '{{}}',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR(50) DEFAULT 'pending',
                    
                    PRIMARY KEY (job_name, vendor)
                )
            """)
            
            # Create job runs table
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {job_runs_table} (
                    id SERIAL PRIMARY KEY,
                    job_name VARCHAR(255) NOT NULL,
                    job_type VARCHAR(100) NOT NULL,
                    vendor VARCHAR(100) NOT NULL,
                    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    end_time TIMESTAMP WITH TIME ZONE,
                    status VARCHAR(50) NOT NULL,
                    records_processed INTEGER DEFAULT 0,
                    records_inserted INTEGER DEFAULT 0,
                    records_updated INTEGER DEFAULT 0,
                    records_skipped INTEGER DEFAULT 0,
                    error_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    checkpoint_start TEXT,
                    checkpoint_end TEXT,
                    metadata JSONB DEFAULT '{{}}'
                )
            """)
            
            # Create indexes
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_checkpoints_job_vendor 
                ON {checkpoints_table}(job_name, vendor)
            """)
            
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_job_runs_start_time 
                ON {job_runs_table}(start_time DESC)
            """)
            
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_job_runs_job_vendor 
                ON {job_runs_table}(job_name, vendor)
            """)
            
            logger.info("Checkpoint management tables initialized")
    
    async def get_checkpoint(self, job_name: str, vendor: str) -> Optional[Checkpoint]:
        """Get the latest checkpoint for a job and vendor."""
        table_name = self.env.get_table_name("frontfill_checkpoints")
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(f"""
                SELECT * FROM {table_name}
                WHERE job_name = $1 AND vendor = $2
            """, job_name, vendor)
            
            if row:
                return Checkpoint(
                    job_name=row["job_name"],
                    job_type=row["job_type"],
                    vendor=row["vendor"],
                    checkpoint_type=CheckpointType(row["checkpoint_type"]),
                    checkpoint_value=row["checkpoint_value"],
                    metadata=row["metadata"] or {},
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                    status=JobStatus(row["status"])
                )
            return None
    
    async def save_checkpoint(self, checkpoint: Checkpoint):
        """Save or update a checkpoint."""
        table_name = self.env.get_table_name("frontfill_checkpoints")
        
        async with self.pool.acquire() as conn:
            await conn.execute(f"""
                INSERT INTO {table_name}
                (job_name, job_type, vendor, checkpoint_type, checkpoint_value, 
                 metadata, status, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
                ON CONFLICT (job_name, vendor) DO UPDATE SET
                    checkpoint_type = EXCLUDED.checkpoint_type,
                    checkpoint_value = EXCLUDED.checkpoint_value,
                    metadata = EXCLUDED.metadata,
                    status = EXCLUDED.status,
                    updated_at = CURRENT_TIMESTAMP
            """, checkpoint.job_name, checkpoint.job_type, checkpoint.vendor,
                checkpoint.checkpoint_type.value, checkpoint.checkpoint_value,
                json.dumps(checkpoint.metadata), checkpoint.status.value)
        
        logger.info(f"Saved checkpoint for {checkpoint.job_name}:{checkpoint.vendor} = {checkpoint.checkpoint_value}")
    
    async def start_job_run(self, job_name: str, job_type: str, vendor: str,
                          checkpoint_start: Optional[str] = None,
                          metadata: Optional[Dict[str, Any]] = None) -> int:
        """Start a new job run and return the run ID."""
        table_name = self.env.get_table_name("frontfill_job_runs")
        
        async with self.pool.acquire() as conn:
            run_id = await conn.fetchval(f"""
                INSERT INTO {table_name}
                (job_name, job_type, vendor, start_time, status, checkpoint_start, metadata)
                VALUES ($1, $2, $3, CURRENT_TIMESTAMP, $4, $5, $6)
                RETURNING id
            """, job_name, job_type, vendor, JobStatus.RUNNING.value,
                checkpoint_start, json.dumps(metadata or {}))
        
        logger.info(f"Started job run {run_id} for {job_name}:{vendor}")
        return run_id
    
    async def update_job_run(self, run_id: int, 
                           records_processed: int = 0,
                           records_inserted: int = 0,
                           records_updated: int = 0,
                           records_skipped: int = 0,
                           error_count: int = 0,
                           checkpoint_end: Optional[str] = None):
        """Update job run statistics."""
        table_name = self.env.get_table_name("frontfill_job_runs")
        
        async with self.pool.acquire() as conn:
            await conn.execute(f"""
                UPDATE {table_name} SET
                    records_processed = records_processed + $2,
                    records_inserted = records_inserted + $3,
                    records_updated = records_updated + $4,
                    records_skipped = records_skipped + $5,
                    error_count = error_count + $6,
                    checkpoint_end = COALESCE($7, checkpoint_end)
                WHERE id = $1
            """, run_id, records_processed, records_inserted, records_updated,
                records_skipped, error_count, checkpoint_end)
    
    async def complete_job_run(self, run_id: int, status: JobStatus,
                             error_message: Optional[str] = None,
                             checkpoint_end: Optional[str] = None):
        """Complete a job run."""
        table_name = self.env.get_table_name("frontfill_job_runs")
        
        async with self.pool.acquire() as conn:
            await conn.execute(f"""
                UPDATE {table_name} SET
                    end_time = CURRENT_TIMESTAMP,
                    status = $2,
                    error_message = $3,
                    checkpoint_end = COALESCE($4, checkpoint_end)
                WHERE id = $1
            """, run_id, status.value, error_message, checkpoint_end)
        
        logger.info(f"Completed job run {run_id} with status {status.value}")
    
    async def get_recent_job_runs(self, job_name: Optional[str] = None,
                                vendor: Optional[str] = None,
                                limit: int = 50) -> List[JobRun]:
        """Get recent job runs."""
        table_name = self.env.get_table_name("frontfill_job_runs")
        
        where_conditions = []
        params = []
        
        if job_name:
            where_conditions.append(f"job_name = ${len(params) + 1}")
            params.append(job_name)
        
        if vendor:
            where_conditions.append(f"vendor = ${len(params) + 1}")
            params.append(vendor)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        params.append(limit)
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT * FROM {table_name}
                {where_clause}
                ORDER BY start_time DESC
                LIMIT ${len(params)}
            """, *params)
            
            return [JobRun(
                id=row["id"],
                job_name=row["job_name"],
                job_type=row["job_type"],
                vendor=row["vendor"],
                start_time=row["start_time"],
                end_time=row["end_time"],
                status=JobStatus(row["status"]),
                records_processed=row["records_processed"],
                records_inserted=row["records_inserted"],
                records_updated=row["records_updated"],
                records_skipped=row["records_skipped"],
                error_count=row["error_count"],
                error_message=row["error_message"],
                checkpoint_start=row["checkpoint_start"],
                checkpoint_end=row["checkpoint_end"],
                metadata=row["metadata"] or {}
            ) for row in rows]
    
    async def cleanup_old_job_runs(self, days_to_keep: int = 30):
        """Clean up old job run records."""
        table_name = self.env.get_table_name("frontfill_job_runs")
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        async with self.pool.acquire() as conn:
            deleted_count = await conn.fetchval(f"""
                DELETE FROM {table_name}
                WHERE start_time < $1
                RETURNING count(*)
            """, cutoff_date)
        
        logger.info(f"Cleaned up {deleted_count} old job run records")
        return deleted_count
    
    async def get_duplicate_detection_key(self, job_type: str, vendor: str,
                                        record_data: Dict[str, Any]) -> str:
        """Generate a unique key for duplicate detection."""
        if job_type == "instruments":
            return f"{vendor}:{record_data.get('symbol', '')}"
        elif job_type == "daily_prices":
            return f"{vendor}:{record_data.get('symbol', '')}:{record_data.get('date', '')}"
        elif job_type == "news":
            return f"{vendor}:{record_data.get('id', '')}:{record_data.get('publishedDate', '')}"
        elif job_type == "economic_events":
            return f"{vendor}:{record_data.get('name', '')}:{record_data.get('date', '')}"
        else:
            # Fallback to generic key
            return f"{vendor}:{hash(str(sorted(record_data.items())))}"
    
    async def check_processed_records(self, job_type: str, vendor: str,
                                    record_keys: List[str],
                                    lookback_hours: int = 24) -> Set[str]:
        """Check which records have been processed recently to avoid duplicates."""
        # This would check against actual data tables
        # Implementation depends on each table's structure
        
        processed_keys = set()
        lookback_time = datetime.now() - timedelta(hours=lookback_hours)
        
        if job_type == "instruments":
            table_name = self.env.get_table_name("instruments")
            async with self.pool.acquire() as conn:
                # Check for recently updated instruments
                rows = await conn.fetch(f"""
                    SELECT symbol FROM {table_name}
                    WHERE updated_at > $1 AND symbol = ANY($2)
                """, lookback_time, record_keys)
                processed_keys.update([f"{vendor}:{row['symbol']}" for row in rows])
        
        elif job_type == "daily_prices":
            # Check both polygon and tiingo tables
            for table_suffix in ["polygon", "tiingo"]:
                if vendor.lower() in table_suffix:
                    table_name = self.env.get_table_name(f"daily_prices_{table_suffix}")
                    async with self.pool.acquire() as conn:
                        # Extract symbol:date pairs from keys
                        symbol_date_pairs = []
                        for key in record_keys:
                            parts = key.split(":")
                            if len(parts) >= 3:
                                symbol_date_pairs.append((parts[1], parts[2]))
                        
                        if symbol_date_pairs:
                            placeholders = ",".join([f"(${i*2+1}, ${i*2+2})" for i in range(len(symbol_date_pairs))])
                            params = []
                            for symbol, date_str in symbol_date_pairs:
                                params.extend([symbol, date_str])
                            
                            rows = await conn.fetch(f"""
                                SELECT i.symbol, dp.date 
                                FROM {table_name} dp
                                JOIN {self.env.get_table_name("instruments")} i ON dp.instrument_id = i.id
                                WHERE (i.symbol, dp.date::text) IN (VALUES {placeholders})
                                AND dp.updated_at > ${len(params) + 1}
                            """, *params, lookback_time)
                            
                            processed_keys.update([f"{vendor}:{row['symbol']}:{row['date']}" for row in rows])
        
        return processed_keys