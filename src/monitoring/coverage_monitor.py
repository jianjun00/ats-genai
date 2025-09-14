#!/usr/bin/env python3
"""
Data Coverage Monitoring System
Tracks daily prices and minute bar coverage, detects gaps, and prioritizes backfill operations.
"""

import asyncio
import asyncpg
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Set, Optional, NamedTuple, Tuple
from pathlib import Path
from dataclasses import dataclass
import pandas as pd
import json
import os
from collections import defaultdict

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class CoverageRecord:
    vendor: str
    data_type: str
    symbol: str
    trading_date: date
    coverage_status: str  # 'complete', 'partial', 'missing', 'stale'
    data_quality_score: Optional[float] = None
    record_count: Optional[int] = None
    file_path: Optional[str] = None
    file_size_bytes: Optional[int] = None

@dataclass
class CoverageGap:
    vendor: str
    data_type: str
    symbol: str
    gap_start_date: date
    gap_end_date: date
    gap_days: int
    priority_score: int
    estimated_effort_minutes: Optional[int] = None

class CoverageMonitor:
    """Main coverage monitoring system."""

    def __init__(self, db_config: Optional[Dict] = None):
        if db_config is None:
            self.db_config = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': int(os.getenv('DB_PORT', 4432)),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'intg_password'),
                'database': os.getenv('DB_NAME', 'intg_db'),
            }
        else:
            self.db_config = db_config
        self.db_pool = None

        # File paths for different vendors
        self.vendor_paths = {
            'firstrate': '/mnt/d/ats-data/minute-bars/firstrate',
            'polygon': '/mnt/d/ats-data/minute-bars/polygon',
            'tiingo': '/mnt/d/ats-data/minute-bars/tiingo',
            'eodhd': '/mnt/d/ats-data/minute-bars/eodhd'
        }

    async def initialize(self):
        """Initialize database connection pool."""
        self.db_pool = await asyncpg.create_pool(
            host=self.db_config['host'],
            port=self.db_config['port'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            min_size=2,
            max_size=10
        )
        logger.info(f"✅ Connected to database: {self.db_config['host']}:{self.db_config['port']}")

    async def close(self):
        """Close database connections."""
        if self.db_pool:
            await self.db_pool.close()

    def get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """Get list of trading days (excluding weekends)."""
        trading_days = []
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:  # Monday=0, Friday=4
                trading_days.append(current)
            current += timedelta(days=1)
        return trading_days

    async def scan_firstrate_coverage(self, lookback_days: int = 90) -> List[CoverageRecord]:
        """Scan FirstRate minute bar files for coverage analysis."""
        logger.info(f"🔍 Scanning FirstRate coverage for past {lookback_days} days")

        end_date = date.today()
        start_date = end_date - timedelta(days=lookback_days)
        trading_days = self.get_trading_days(start_date, end_date)

        base_path = Path(self.vendor_paths['firstrate'])
        if not base_path.exists():
            logger.error(f"FirstRate path not found: {base_path}")
            return []

        # Get all instruments
        instruments = set()
        for letter_dir in base_path.iterdir():
            if letter_dir.is_dir() and letter_dir.name not in ['2', '2000', '2020']:
                for symbol_dir in letter_dir.iterdir():
                    if symbol_dir.is_dir():
                        instruments.add(symbol_dir.name)

        logger.info(f"📋 Found {len(instruments)} FirstRate instruments")

        coverage_records = []

        # Calculate months to check
        months_to_check = set()
        for trading_day in trading_days:
            months_to_check.add((trading_day.year, trading_day.month))

        # Check each instrument for each month
        for instrument in sorted(instruments):
            first_letter = instrument[0]
            instrument_path = base_path / first_letter / instrument / "2025"

            for year, month in months_to_check:
                month_path = instrument_path / f"{month:02d}"
                file_path = month_path / f"{instrument}_2025_{month:02d}.parquet"

                # Get trading days for this month
                month_start = date(year, month, 1)
                if month == 12:
                    month_end = date(year + 1, 1, 1) - timedelta(days=1)
                else:
                    month_end = date(year, month + 1, 1) - timedelta(days=1)

                month_trading_days = [d for d in trading_days if month_start <= d <= month_end]

                if file_path.exists():
                    # File exists - check if it's recent enough
                    file_stat = file_path.stat()
                    file_size = file_stat.st_size
                    mod_time = datetime.fromtimestamp(file_stat.st_mtime)

                    # Determine quality based on recency and size
                    days_since_update = (datetime.now() - mod_time).days

                    if days_since_update <= 1 and file_size > 1000:  # Recent and reasonable size
                        status = 'complete'
                        quality = 95.0
                    elif days_since_update <= 7 and file_size > 500:
                        status = 'complete'
                        quality = 85.0
                    elif file_size < 500:
                        status = 'partial'
                        quality = 60.0
                    else:
                        status = 'stale'
                        quality = 40.0

                    # Create record for each trading day in this month
                    for trading_day in month_trading_days:
                        coverage_records.append(CoverageRecord(
                            vendor='firstrate',
                            data_type='minute_bars',
                            symbol=instrument,
                            trading_date=trading_day,
                            coverage_status=status,
                            data_quality_score=quality,
                            file_path=str(file_path),
                            file_size_bytes=file_size
                        ))
                else:
                    # File missing - create missing records for each trading day
                    for trading_day in month_trading_days:
                        coverage_records.append(CoverageRecord(
                            vendor='firstrate',
                            data_type='minute_bars',
                            symbol=instrument,
                            trading_date=trading_day,
                            coverage_status='missing',
                            data_quality_score=0.0
                        ))

        logger.info(f"📊 Generated {len(coverage_records)} coverage records")
        return coverage_records

    async def update_coverage_tracking(self, coverage_records: List[CoverageRecord]):
        """Update coverage tracking table with new data."""
        if not coverage_records:
            return

        logger.info(f"💾 Updating coverage tracking with {len(coverage_records)} records")

        async with self.db_pool.acquire() as conn:
            # Prepare bulk insert/update
            insert_query = """
                INSERT INTO dev_data_coverage_tracking
                (vendor, data_type, symbol, trading_date, coverage_status,
                 data_quality_score, record_count, file_path, file_size_bytes)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (vendor, data_type, symbol, trading_date)
                DO UPDATE SET
                    coverage_status = EXCLUDED.coverage_status,
                    data_quality_score = EXCLUDED.data_quality_score,
                    record_count = EXCLUDED.record_count,
                    file_path = EXCLUDED.file_path,
                    file_size_bytes = EXCLUDED.file_size_bytes,
                    last_updated = NOW()
            """

            # Batch insert
            batch_size = 1000
            for i in range(0, len(coverage_records), batch_size):
                batch = coverage_records[i:i + batch_size]
                values = [
                    (r.vendor, r.data_type, r.symbol, r.trading_date, r.coverage_status,
                     r.data_quality_score, r.record_count, r.file_path, r.file_size_bytes)
                    for r in batch
                ]

                await conn.executemany(insert_query, values)

                if i % 5000 == 0:
                    logger.info(f"  Processed {i}/{len(coverage_records)} records")

        logger.info("✅ Coverage tracking updated")

    async def detect_coverage_gaps(self, vendor: str, data_type: str, lookback_days: int = 90) -> List[CoverageGap]:
        """Detect coverage gaps and generate backfill tasks."""
        logger.info(f"🔍 Detecting coverage gaps for {vendor} {data_type}")

        end_date = date.today()
        start_date = end_date - timedelta(days=lookback_days)

        async with self.db_pool.acquire() as conn:
            # Find gaps by looking for consecutive missing dates per symbol
            gap_query = """
                WITH trading_days AS (
                    SELECT d::date as trading_date
                    FROM generate_series($1::date, $2::date, '1 day'::interval) d
                    WHERE extract(dow from d) NOT IN (0, 6)  -- Exclude weekends
                ),
                symbol_coverage AS (
                    SELECT DISTINCT symbol
                    FROM dev_data_coverage_tracking
                    WHERE vendor = $3 AND data_type = $4
                    AND trading_date >= $1
                ),
                expected_coverage AS (
                    SELECT sc.symbol, td.trading_date
                    FROM symbol_coverage sc
                    CROSS JOIN trading_days td
                ),
                actual_coverage AS (
                    SELECT symbol, trading_date, coverage_status
                    FROM dev_data_coverage_tracking
                    WHERE vendor = $3 AND data_type = $4
                    AND trading_date >= $1
                    AND coverage_status != 'missing'
                ),
                gaps AS (
                    SELECT
                        ec.symbol,
                        ec.trading_date,
                        CASE WHEN ac.symbol IS NULL THEN 1 ELSE 0 END as is_gap,
                        ROW_NUMBER() OVER (PARTITION BY ec.symbol ORDER BY ec.trading_date) -
                        ROW_NUMBER() OVER (PARTITION BY ec.symbol, CASE WHEN ac.symbol IS NULL THEN 1 ELSE 0 END ORDER BY ec.trading_date) as gap_group
                    FROM expected_coverage ec
                    LEFT JOIN actual_coverage ac ON ec.symbol = ac.symbol AND ec.trading_date = ac.trading_date
                )
                SELECT
                    symbol,
                    MIN(trading_date) as gap_start,
                    MAX(trading_date) as gap_end,
                    COUNT(*) as gap_days
                FROM gaps
                WHERE is_gap = 1
                GROUP BY symbol, gap_group
                HAVING COUNT(*) >= 1  -- At least 1 day gap
                ORDER BY COUNT(*) DESC, symbol
            """

            gap_rows = await conn.fetch(gap_query, start_date, end_date, vendor, data_type)

        # Convert to CoverageGap objects with priority scoring
        gaps = []
        for row in gap_rows:
            priority = await self.calculate_gap_priority(
                row['symbol'], row['gap_days'], row['gap_end']
            )

            gaps.append(CoverageGap(
                vendor=vendor,
                data_type=data_type,
                symbol=row['symbol'],
                gap_start_date=row['gap_start'],
                gap_end_date=row['gap_end'],
                gap_days=row['gap_days'],
                priority_score=priority,
                estimated_effort_minutes=self.estimate_backfill_effort(
                    vendor, data_type, row['gap_days']
                )
            ))

        logger.info(f"📊 Found {len(gaps)} coverage gaps")
        return gaps

    async def calculate_gap_priority(self, symbol: str, gap_days: int, gap_end_date: date) -> int:
        """Calculate priority score for a gap (1-10 scale)."""
        base_score = 1

        # Get symbol priority multiplier
        async with self.db_pool.acquire() as conn:
            priority_row = await conn.fetchrow(
                "SELECT priority_tier, priority_multiplier FROM dev_priority_symbols WHERE symbol = $1 AND active = true",
                symbol
            )

            if priority_row:
                multiplier = float(priority_row['priority_multiplier'])
                base_score *= multiplier

        # Recency multiplier
        days_ago = (date.today() - gap_end_date).days
        if days_ago <= 7:
            base_score *= 3  # Very recent
        elif days_ago <= 30:
            base_score *= 2  # Recent

        # Gap size multiplier
        if gap_days >= 10:
            base_score *= 2  # Large gaps
        elif gap_days >= 5:
            base_score *= 1.5  # Medium gaps

        return min(int(base_score), 10)  # Cap at 10

    def estimate_backfill_effort(self, vendor: str, data_type: str, gap_days: int) -> int:
        """Estimate effort in minutes to backfill a gap."""
        # Base effort per day
        effort_per_day = {
            ('firstrate', 'minute_bars'): 2,  # 2 minutes per day
            ('polygon', 'minute_bars'): 3,    # 3 minutes per day (API rate limits)
            ('polygon', 'daily_prices'): 1,   # 1 minute per day
            ('tiingo', 'minute_bars'): 3,
            ('tiingo', 'daily_prices'): 1,
        }

        base_effort = effort_per_day.get((vendor, data_type), 2)
        return gap_days * base_effort

    async def queue_backfill_gaps(self, gaps: List[CoverageGap]):
        """Queue gaps for backfill processing."""
        if not gaps:
            return

        logger.info(f"📋 Queueing {len(gaps)} gaps for backfill")

        async with self.db_pool.acquire() as conn:
            insert_query = """
                INSERT INTO dev_coverage_gaps
                (vendor, data_type, symbol, gap_start_date, gap_end_date, gap_days,
                 priority_score, estimated_effort_minutes)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT DO NOTHING
            """

            values = [
                (g.vendor, g.data_type, g.symbol, g.gap_start_date, g.gap_end_date,
                 g.gap_days, g.priority_score, g.estimated_effort_minutes)
                for g in gaps
            ]

            await conn.executemany(insert_query, values)

        logger.info("✅ Gaps queued for backfill")

    async def calculate_daily_metrics(self, vendor: str, data_type: str, metric_date: date = None):
        """Calculate and store daily coverage metrics."""
        if metric_date is None:
            metric_date = date.today()

        logger.info(f"📊 Calculating daily metrics for {vendor} {data_type} on {metric_date}")

        async with self.db_pool.acquire() as conn:
            # Calculate metrics from coverage tracking
            metrics_query = """
                WITH coverage_stats AS (
                    SELECT
                        COUNT(DISTINCT symbol) as total_instruments,
                        COUNT(DISTINCT CASE WHEN coverage_status != 'missing' THEN symbol END) as instruments_with_data,
                        COUNT(*) as total_expected_files,
                        COUNT(CASE WHEN coverage_status = 'complete' THEN 1 END) as files_complete,
                        COUNT(CASE WHEN coverage_status = 'missing' THEN 1 END) as files_missing,
                        COUNT(CASE WHEN coverage_status = 'stale' THEN 1 END) as files_stale,
                        AVG(data_quality_score) as avg_quality_score
                    FROM dev_data_coverage_tracking
                    WHERE vendor = $1 AND data_type = $2
                    AND trading_date >= $3::date - INTERVAL '30 days'
                    AND trading_date <= $3::date
                )
                SELECT
                    total_instruments,
                    instruments_with_data,
                    ROUND(instruments_with_data * 100.0 / NULLIF(total_instruments, 0), 2) as coverage_percentage,
                    total_expected_files,
                    files_complete as files_found,
                    files_missing,
                    files_stale,
                    ROUND(avg_quality_score, 2) as avg_quality_score
                FROM coverage_stats
            """

            metrics = await conn.fetchrow(metrics_query, vendor, data_type, metric_date)

            if metrics:
                # Insert/update daily metrics
                upsert_query = """
                    INSERT INTO dev_daily_coverage_metrics
                    (metric_date, vendor, data_type, total_expected_instruments, instruments_with_data,
                     coverage_percentage, total_expected_files, files_found, files_missing, files_stale, avg_quality_score)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (metric_date, vendor, data_type)
                    DO UPDATE SET
                        total_expected_instruments = EXCLUDED.total_expected_instruments,
                        instruments_with_data = EXCLUDED.instruments_with_data,
                        coverage_percentage = EXCLUDED.coverage_percentage,
                        total_expected_files = EXCLUDED.total_expected_files,
                        files_found = EXCLUDED.files_found,
                        files_missing = EXCLUDED.files_missing,
                        files_stale = EXCLUDED.files_stale,
                        avg_quality_score = EXCLUDED.avg_quality_score
                """

                await conn.execute(upsert_query,
                    metric_date, vendor, data_type,
                    metrics['total_instruments'], metrics['instruments_with_data'],
                    metrics['coverage_percentage'], metrics['total_expected_files'],
                    metrics['files_found'], metrics['files_missing'], metrics['files_stale'],
                    metrics['avg_quality_score']
                )

                logger.info(f"✅ Daily metrics stored: {metrics['coverage_percentage']:.1f}% coverage")

    async def run_daily_monitoring(self, vendors: List[str] = None, lookback_days: int = 90):
        """Run complete daily monitoring workflow."""
        if vendors is None:
            vendors = ['firstrate']  # Start with FirstRate, expand later

        logger.info("🚀 Starting daily coverage monitoring")
        logger.info("="*60)

        for vendor in vendors:
            logger.info(f"📊 Processing vendor: {vendor}")

            if vendor == 'firstrate':
                # Scan FirstRate minute bars
                coverage_records = await self.scan_firstrate_coverage(lookback_days)
                await self.update_coverage_tracking(coverage_records)

                # Detect and queue gaps
                gaps = await self.detect_coverage_gaps(vendor, 'minute_bars', lookback_days)
                await self.queue_backfill_gaps(gaps)

                # Calculate daily metrics
                await self.calculate_daily_metrics(vendor, 'minute_bars')

                logger.info(f"✅ {vendor}: {len(coverage_records)} records, {len(gaps)} gaps detected")

        logger.info("🏁 Daily monitoring complete")

async def main():
    """Main entry point for coverage monitoring."""
    # Database configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 4432)),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'intg_password'),
        'database': os.getenv('DB_NAME', 'intg_db')
    }

    monitor = CoverageMonitor(db_config)

    try:
        await monitor.initialize()
        await monitor.run_daily_monitoring()
    except Exception as e:
        logger.error(f"💥 Monitoring failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        await monitor.close()

if __name__ == "__main__":
    asyncio.run(main())