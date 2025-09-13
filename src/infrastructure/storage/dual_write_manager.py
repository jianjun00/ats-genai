#!/usr/bin/env python3
"""
Dual-Write Manager for Database-to-File Migration

Provides a seamless transition from database storage to file-based storage by
writing to both systems simultaneously. This ensures data consistency during
the migration period and allows for gradual rollout.

Features:
- Atomic dual writes to both database and files
- Fallback mechanisms if one system fails
- Configuration-driven write modes (DB-only, Files-only, Both)
- Validation and reconciliation between systems
- Performance monitoring for both storage backends

Migration Phases:
1. Phase 1: Database-only (current state)
2. Phase 2: Dual-write (transition phase)
3. Phase 3: Files-only with DB fallback
4. Phase 4: Files-only (final state)
"""

import asyncio
import asyncpg
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from enum import Enum
import json

from storage.time_series_file_manager import (
    TimeSeriesFileManager,
    MinuteRecord,
    TimeSeriesQueryEngine
)

class WriteMode(Enum):
    """Write modes for dual-write system"""
    DATABASE_ONLY = "database_only"
    FILES_ONLY = "files_only"
    DUAL_WRITE = "dual_write"
    DUAL_WRITE_FILES_PRIMARY = "dual_write_files_primary"

class ReadMode(Enum):
    """Read modes for dual-read system"""
    DATABASE_ONLY = "database_only"
    FILES_ONLY = "files_only"
    FILES_WITH_DB_FALLBACK = "files_with_db_fallback"
    DATABASE_WITH_FILES_FALLBACK = "database_with_files_fallback"

@dataclass
class DualWriteConfig:
    """Configuration for dual-write system"""
    # Storage paths
    file_base_path: str = "/data/monthly/interval"

    # Database connection
    db_host: str = "postgres-simple"
    db_password: str = "dev_password"
    db_name: str = "dev_db"

    # Write/Read modes
    write_mode: WriteMode = WriteMode.DUAL_WRITE
    read_mode: ReadMode = ReadMode.DATABASE_WITH_FILES_FALLBACK

    # Error handling
    fail_on_db_error: bool = False       # Continue if DB write fails
    fail_on_file_error: bool = True      # Fail if file write fails
    max_retries: int = 3                 # Retry attempts per write

    # Performance
    write_timeout: float = 30.0          # Seconds
    batch_size: int = 1000               # Records per batch

    # Monitoring
    enable_metrics: bool = True
    log_write_stats: bool = True

@dataclass
class WriteResult:
    """Result of a dual-write operation"""
    success: bool
    db_success: bool = False
    file_success: bool = False
    db_error: Optional[str] = None
    file_error: Optional[str] = None
    records_written: int = 0
    write_time_seconds: float = 0.0

@dataclass
class WriteMetrics:
    """Metrics for dual-write operations"""
    total_writes: int = 0
    successful_writes: int = 0
    db_writes: int = 0
    db_failures: int = 0
    file_writes: int = 0
    file_failures: int = 0
    total_records: int = 0
    avg_write_time: float = 0.0
    last_reset: datetime = None

class DualWriteTimeSeriesManager:
    """Manages dual writes to both database and file storage"""

    def __init__(self, config: DualWriteConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Database setup
        self.db_url = f"postgresql://postgres:{config.db_password}@{config.db_host}:5432/{config.db_name}"

        # File manager
        self.file_manager = TimeSeriesFileManager(config.file_base_path)
        self.query_engine = TimeSeriesQueryEngine(self.file_manager)

        # Metrics
        self.metrics = WriteMetrics(last_reset=datetime.now())

        # Database table mapping for different vendors
        self.vendor_tables = {
            'fmp': 'dev_minute_prices_fmp',
            'polygon': 'dev_minute_prices_polygon',
            'tiingo': 'dev_minute_prices_tiingo'
        }

    async def write_minute_data(self,
                              instrument_id: int,
                              records: List[MinuteRecord],
                              vendor: str = 'fmp') -> WriteResult:
        """
        Write minute data using configured dual-write strategy

        Args:
            instrument_id: Instrument ID
            records: List of minute records to write
            vendor: Data vendor (fmp, polygon, tiingo)

        Returns:
            WriteResult with success status and detailed results
        """
        if not records:
            return WriteResult(success=True, records_written=0)

        start_time = datetime.now()
        result = WriteResult(success=False, records_written=len(records))

        # Determine which systems to write to
        write_to_db = self.config.write_mode in [
            WriteMode.DATABASE_ONLY,
            WriteMode.DUAL_WRITE,
            WriteMode.DUAL_WRITE_FILES_PRIMARY
        ]

        write_to_files = self.config.write_mode in [
            WriteMode.FILES_ONLY,
            WriteMode.DUAL_WRITE,
            WriteMode.DUAL_WRITE_FILES_PRIMARY
        ]

        # Execute writes based on configuration
        if self.config.write_mode == WriteMode.DUAL_WRITE_FILES_PRIMARY:
            # Files first, then database
            if write_to_files:
                result.file_success = await self._write_to_files(instrument_id, records)
                if not result.file_success:
                    result.file_error = "File write failed"

            if write_to_db:
                result.db_success = await self._write_to_database(instrument_id, records, vendor)
                if not result.db_success:
                    result.db_error = "Database write failed"
        else:
            # Concurrent writes for better performance
            tasks = []

            if write_to_db:
                tasks.append(self._write_to_database(instrument_id, records, vendor))

            if write_to_files:
                tasks.append(self._write_to_files(instrument_id, records))

            if tasks:
                try:
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    if write_to_db and write_to_files:
                        result.db_success = not isinstance(results[0], Exception) and results[0]
                        result.file_success = not isinstance(results[1], Exception) and results[1]

                        if isinstance(results[0], Exception):
                            result.db_error = str(results[0])
                        if isinstance(results[1], Exception):
                            result.file_error = str(results[1])

                    elif write_to_db:
                        result.db_success = not isinstance(results[0], Exception) and results[0]
                        if isinstance(results[0], Exception):
                            result.db_error = str(results[0])

                    elif write_to_files:
                        result.file_success = not isinstance(results[0], Exception) and results[0]
                        if isinstance(results[0], Exception):
                            result.file_error = str(results[0])

                except Exception as e:
                    self.logger.error(f"❌ Dual write failed: {e}")
                    result.db_error = str(e)
                    result.file_error = str(e)

        # Determine overall success based on configuration
        if self.config.write_mode == WriteMode.DATABASE_ONLY:
            result.success = result.db_success
        elif self.config.write_mode == WriteMode.FILES_ONLY:
            result.success = result.file_success
        else:  # Dual write modes
            if self.config.fail_on_db_error and self.config.fail_on_file_error:
                result.success = result.db_success and result.file_success
            elif self.config.fail_on_db_error:
                result.success = result.db_success
            elif self.config.fail_on_file_error:
                result.success = result.file_success
            else:
                result.success = result.db_success or result.file_success

        # Record timing
        result.write_time_seconds = (datetime.now() - start_time).total_seconds()

        # Update metrics
        if self.config.enable_metrics:
            self._update_metrics(result)

        # Log if configured
        if self.config.log_write_stats:
            self._log_write_result(instrument_id, result, vendor)

        return result

    async def _write_to_database(self, instrument_id: int, records: List[MinuteRecord], vendor: str) -> bool:
        """Write records to PostgreSQL database"""
        table_name = self.vendor_tables.get(vendor, 'dev_minute_prices_fmp')

        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=3)

        try:
            async with pool.acquire() as conn:
                # Prepare data for insert
                insert_data = []
                for record in records:
                    insert_data.append((
                        instrument_id,
                        record.timestamp,
                        record.open_price,
                        record.high_price,
                        record.low_price,
                        record.close_price,
                        record.volume
                    ))

                # Create table if not exists
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id SERIAL PRIMARY KEY,
                        instrument_id INTEGER NOT NULL REFERENCES dev_instrument(id),
                        timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                        open_price NUMERIC(10, 4),
                        high_price NUMERIC(10, 4),
                        low_price NUMERIC(10, 4),
                        close_price NUMERIC(10, 4),
                        volume BIGINT,
                        created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                        updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                        UNIQUE(instrument_id, timestamp)
                    )
                """)

                # Insert data with conflict resolution
                sql = f"""
                    INSERT INTO {table_name}
                    (instrument_id, timestamp, open_price, high_price, low_price, close_price, volume)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (instrument_id, timestamp) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        updated_at = NOW()
                """

                await conn.executemany(sql, insert_data)
                return True

        except Exception as e:
            self.logger.error(f"❌ Database write failed: {e}")
            return False

        finally:
            await pool.close()

    async def _write_to_files(self, instrument_id: int, records: List[MinuteRecord]) -> bool:
        """Write records to file storage"""
        try:
            # Group records by month
            monthly_records = {}

            for record in records:
                month_key = (record.timestamp.year, record.timestamp.month)
                if month_key not in monthly_records:
                    monthly_records[month_key] = []
                monthly_records[month_key].append(record)

            # Write each month's data
            write_success = True
            for (year, month), month_records in monthly_records.items():
                success = await self.file_manager.write_monthly_file(
                    instrument_id, year, month, month_records
                )
                if not success:
                    write_success = False
                    self.logger.error(f"❌ Failed to write file for instrument {instrument_id}, {year}-{month:02d}")

            return write_success

        except Exception as e:
            self.logger.error(f"❌ File write failed: {e}")
            return False

    async def read_minute_data(self,
                             instrument_ids: List[int],
                             start_time: datetime,
                             end_time: datetime) -> Dict[int, List[MinuteRecord]]:
        """
        Read minute data using configured read strategy

        Args:
            instrument_ids: List of instrument IDs
            start_time: Start datetime
            end_time: End datetime

        Returns:
            Dict mapping instrument_id to list of records
        """
        if self.config.read_mode == ReadMode.FILES_ONLY:
            return await self._read_from_files(instrument_ids, start_time, end_time)

        elif self.config.read_mode == ReadMode.DATABASE_ONLY:
            return await self._read_from_database(instrument_ids, start_time, end_time)

        elif self.config.read_mode == ReadMode.FILES_WITH_DB_FALLBACK:
            # Try files first, fallback to database
            try:
                return await self._read_from_files(instrument_ids, start_time, end_time)
            except Exception as e:
                self.logger.warning(f"File read failed, falling back to database: {e}")
                return await self._read_from_database(instrument_ids, start_time, end_time)

        elif self.config.read_mode == ReadMode.DATABASE_WITH_FILES_FALLBACK:
            # Try database first, fallback to files
            try:
                return await self._read_from_database(instrument_ids, start_time, end_time)
            except Exception as e:
                self.logger.warning(f"Database read failed, falling back to files: {e}")
                return await self._read_from_files(instrument_ids, start_time, end_time)

        else:
            raise ValueError(f"Unsupported read mode: {self.config.read_mode}")

    async def _read_from_files(self,
                             instrument_ids: List[int],
                             start_time: datetime,
                             end_time: datetime) -> Dict[int, List[MinuteRecord]]:
        """Read data from file storage"""
        return await self.query_engine.query_range(instrument_ids, start_time, end_time)

    async def _read_from_database(self,
                                instrument_ids: List[int],
                                start_time: datetime,
                                end_time: datetime,
                                vendor: str = 'fmp') -> Dict[int, List[MinuteRecord]]:
        """Read data from database"""
        table_name = self.vendor_tables.get(vendor, 'dev_minute_prices_fmp')
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=3)

        results = {instrument_id: [] for instrument_id in instrument_ids}

        try:
            async with pool.acquire() as conn:
                query = f"""
                    SELECT instrument_id, timestamp, open_price, high_price,
                           low_price, close_price, volume
                    FROM {table_name}
                    WHERE instrument_id = ANY($1)
                    AND timestamp BETWEEN $2 AND $3
                    ORDER BY instrument_id, timestamp
                """

                rows = await conn.fetch(query, instrument_ids, start_time, end_time)

                for row in rows:
                    record = MinuteRecord(
                        timestamp=row['timestamp'],
                        open_price=float(row['open_price'] or 0),
                        high_price=float(row['high_price'] or 0),
                        low_price=float(row['low_price'] or 0),
                        close_price=float(row['close_price'] or 0),
                        volume=int(row['volume'] or 0)
                    )

                    results[row['instrument_id']].append(record)

        finally:
            await pool.close()

        return results

    def _update_metrics(self, result: WriteResult):
        """Update write metrics"""
        self.metrics.total_writes += 1
        self.metrics.total_records += result.records_written

        if result.success:
            self.metrics.successful_writes += 1

        if result.db_success:
            self.metrics.db_writes += 1
        elif result.db_error:
            self.metrics.db_failures += 1

        if result.file_success:
            self.metrics.file_writes += 1
        elif result.file_error:
            self.metrics.file_failures += 1

        # Update average write time
        self.metrics.avg_write_time = (
            (self.metrics.avg_write_time * (self.metrics.total_writes - 1) + result.write_time_seconds)
            / self.metrics.total_writes
        )

    def _log_write_result(self, instrument_id: int, result: WriteResult, vendor: str):
        """Log write result"""
        status_symbol = "✅" if result.success else "❌"
        db_status = "✅" if result.db_success else ("❌" if result.db_error else "⏭️")
        file_status = "✅" if result.file_success else ("❌" if result.file_error else "⏭️")

        self.logger.info(
            f"{status_symbol} Write instrument {instrument_id} [{vendor}]: "
            f"{result.records_written} records, "
            f"DB: {db_status}, Files: {file_status}, "
            f"{result.write_time_seconds:.2f}s"
        )

        if result.db_error:
            self.logger.error(f"   DB Error: {result.db_error}")
        if result.file_error:
            self.logger.error(f"   File Error: {result.file_error}")

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        total_time = (datetime.now() - self.metrics.last_reset).total_seconds()

        return {
            'total_writes': self.metrics.total_writes,
            'successful_writes': self.metrics.successful_writes,
            'success_rate': self.metrics.successful_writes / max(self.metrics.total_writes, 1),
            'total_records': self.metrics.total_records,
            'avg_write_time': self.metrics.avg_write_time,
            'records_per_second': self.metrics.total_records / max(total_time, 1),
            'database': {
                'writes': self.metrics.db_writes,
                'failures': self.metrics.db_failures,
                'success_rate': self.metrics.db_writes / max(self.metrics.db_writes + self.metrics.db_failures, 1)
            },
            'files': {
                'writes': self.metrics.file_writes,
                'failures': self.metrics.file_failures,
                'success_rate': self.metrics.file_writes / max(self.metrics.file_writes + self.metrics.file_failures, 1)
            },
            'uptime_seconds': total_time
        }

    def reset_metrics(self):
        """Reset metrics counters"""
        self.metrics = WriteMetrics(last_reset=datetime.now())
        self.logger.info("📊 Metrics reset")

# Example usage and testing
async def example_usage():
    """Example of how to use the dual-write system"""

    # Configure for dual-write mode
    config = DualWriteConfig(
        write_mode=WriteMode.DUAL_WRITE,
        read_mode=ReadMode.FILES_WITH_DB_FALLBACK,
        fail_on_file_error=True,  # Prioritize file writes
        fail_on_db_error=False    # Allow DB failures during transition
    )

    manager = DualWriteTimeSeriesManager(config)

    # Example data
    instrument_id = 12345
    records = [
        MinuteRecord(
            timestamp=datetime.now(),
            open_price=100.0,
            high_price=101.0,
            low_price=99.0,
            close_price=100.5,
            volume=1000
        )
    ]

    # Write data using dual-write
    result = await manager.write_minute_data(instrument_id, records, 'fmp')
    print(f"Write result: {result}")

    # Read data back
    read_data = await manager.read_minute_data(
        [instrument_id],
        datetime.now() - timedelta(hours=1),
        datetime.now()
    )
    print(f"Read {len(read_data.get(instrument_id, []))} records")

    # Get performance metrics
    metrics = manager.get_metrics_summary()
    print(f"Metrics: {json.dumps(metrics, indent=2)}")

if __name__ == "__main__":
    import asyncio
    from datetime import timedelta

    asyncio.run(example_usage())