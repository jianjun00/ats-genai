#!/usr/bin/env python3
"""
Database-to-File Migration Script

Migrates existing minute-level time-series data from PostgreSQL tables to the new 
file-based storage format with monthly aggregation and binary compression.

This script handles the complete migration from the current database architecture to
the new file-based system optimized for massive scale (29.5B+ records).

Migration Process:
1. Read minute data from PostgreSQL tables (FMP, Polygon, Tiingo)
2. Group data by instrument and month
3. Convert to optimized binary format
4. Write to compressed monthly files with sharding
5. Validate migration integrity

Directory Structure Created:
/data/monthly/interval/<yyyy>/<mm>/<instrument_id % 100>/<instrument_id>_<yyyy>_<mm>.record

Performance Optimizations:
- Processes data in monthly chunks to manage memory
- Uses async I/O for concurrent file operations
- Implements progress checkpoints for resumability
- Validates data integrity throughout migration
"""

import asyncio
import asyncpg
import logging
import os
import json
import argparse
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass, asdict
from pathlib import Path
import sys
from collections import defaultdict
import calendar

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from storage.time_series_file_manager import (
    TimeSeriesFileManager, 
    MinuteRecord, 
    FileMetadata
)

@dataclass
class MigrationConfig:
    """Configuration for database-to-file migration"""
    # Database connection
    db_host: str = "postgres-simple"
    db_password: str = "dev_password"
    db_name: str = "dev_db"
    
    # File storage
    output_base_path: str = "/data/monthly/interval"
    
    # Performance tuning
    batch_size: int = 10000           # Records per batch
    max_concurrent_files: int = 20     # Concurrent file writes
    chunk_months: int = 6             # Process N months at a time
    
    # Source tables
    source_tables: List[str] = None
    
    # Migration control
    start_date: date = date(2005, 1, 1)
    end_date: date = date(2025, 12, 31)
    checkpoint_file: str = "/tmp/migration_checkpoint.json"
    
    # Validation
    validate_after_write: bool = True
    
    def __post_init__(self):
        if self.source_tables is None:
            self.source_tables = [
                'dev_minute_prices_fmp',
                'dev_minute_prices_polygon', 
                'dev_minute_prices_tiingo'
            ]

@dataclass
class MigrationProgress:
    """Tracks migration progress and statistics"""
    total_instruments: int = 0
    completed_instruments: int = 0
    total_months: int = 0
    completed_months: int = 0
    total_records_processed: int = 0
    total_files_created: int = 0
    total_size_bytes: int = 0
    
    failed_instruments: List[int] = None
    failed_months: List[str] = None
    validation_errors: List[str] = None
    
    start_time: Optional[datetime] = None
    last_checkpoint: Optional[datetime] = None
    
    # Source data statistics
    source_records_by_table: Dict[str, int] = None
    source_instruments_by_table: Dict[str, int] = None
    
    def __post_init__(self):
        if self.failed_instruments is None:
            self.failed_instruments = []
        if self.failed_months is None:
            self.failed_months = []
        if self.validation_errors is None:
            self.validation_errors = []
        if self.source_records_by_table is None:
            self.source_records_by_table = {}
        if self.source_instruments_by_table is None:
            self.source_instruments_by_table = {}

class DatabaseToFileMigrator:
    """Handles migration from PostgreSQL to file-based storage"""
    
    def __init__(self, config: MigrationConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Database connection
        self.db_url = f"postgresql://postgres:{config.db_password}@{config.db_host}:5432/{config.db_name}"
        
        # File manager for new format
        self.file_manager = TimeSeriesFileManager(config.output_base_path)
        
        # Migration tracking
        self.progress = MigrationProgress()
        
        # Instrument cache
        self.instrument_cache = {}  # symbol -> id mapping
        self.id_to_symbol_cache = {}  # id -> symbol mapping
    
    async def analyze_source_data(self) -> Dict[str, any]:
        """Analyze existing data in PostgreSQL tables"""
        self.logger.info("🔍 Analyzing source data...")
        
        pool = await asyncpg.create_pool(self.db_url, min_size=2, max_size=5)
        
        analysis = {
            'total_records': 0,
            'total_instruments': 0,
            'date_range': {},
            'table_statistics': {},
            'monthly_distribution': defaultdict(int)
        }
        
        try:
            async with pool.acquire() as conn:
                # Load instruments cache
                instruments = await conn.fetch("SELECT id, symbol FROM dev_instruments WHERE symbol IS NOT NULL")
                self.instrument_cache = {row['symbol']: row['id'] for row in instruments}
                self.id_to_symbol_cache = {row['id']: row['symbol'] for row in instruments}
                
                analysis['total_instruments'] = len(instruments)
                self.progress.total_instruments = len(instruments)
                
                # Analyze each source table
                for table in self.config.source_tables:
                    try:
                        # Check if table exists
                        table_exists = await conn.fetchval(
                            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = $1", 
                            table
                        )
                        
                        if not table_exists:
                            self.logger.warning(f"⚠️ Table {table} does not exist, skipping")
                            continue
                        
                        # Get table statistics
                        stats = await conn.fetchrow(f"""
                            SELECT 
                                COUNT(*) as total_records,
                                COUNT(DISTINCT instrument_id) as unique_instruments,
                                MIN(timestamp) as earliest_timestamp,
                                MAX(timestamp) as latest_timestamp
                            FROM {table}
                        """)
                        
                        if stats['total_records'] > 0:
                            analysis['table_statistics'][table] = {
                                'records': stats['total_records'],
                                'instruments': stats['unique_instruments'],
                                'date_range': {
                                    'start': stats['earliest_timestamp'],
                                    'end': stats['latest_timestamp']
                                }
                            }
                            
                            analysis['total_records'] += stats['total_records']
                            
                            self.progress.source_records_by_table[table] = stats['total_records']
                            self.progress.source_instruments_by_table[table] = stats['unique_instruments']
                            
                            # Get monthly distribution for this table
                            monthly_dist = await conn.fetch(f"""
                                SELECT 
                                    EXTRACT(YEAR FROM timestamp) as year,
                                    EXTRACT(MONTH FROM timestamp) as month,
                                    COUNT(*) as record_count
                                FROM {table}
                                WHERE timestamp BETWEEN $1 AND $2
                                GROUP BY EXTRACT(YEAR FROM timestamp), EXTRACT(MONTH FROM timestamp)
                                ORDER BY year, month
                            """, self.config.start_date, self.config.end_date)
                            
                            for row in monthly_dist:
                                month_key = f"{int(row['year'])}-{int(row['month']):02d}"
                                analysis['monthly_distribution'][month_key] += row['record_count']
                        
                        self.logger.info(f"✅ {table}: {stats['total_records']:,} records, {stats['unique_instruments']:,} instruments")
                    
                    except Exception as e:
                        self.logger.error(f"❌ Error analyzing table {table}: {e}")
                        continue
                
                # Calculate total months to process
                if analysis['monthly_distribution']:
                    self.progress.total_months = len(analysis['monthly_distribution'])
                
        finally:
            await pool.close()
        
        # Store analysis results
        analysis_file = Path(self.config.checkpoint_file).parent / "migration_analysis.json"
        analysis_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(analysis_file, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        
        self.logger.info(f"📊 Source data analysis complete:")
        self.logger.info(f"   Total records: {analysis['total_records']:,}")
        self.logger.info(f"   Total instruments: {analysis['total_instruments']:,}")
        self.logger.info(f"   Total months: {self.progress.total_months:,}")
        self.logger.info(f"   Analysis saved: {analysis_file}")
        
        return analysis
    
    async def migrate_monthly_data(self, year: int, month: int) -> Dict[str, int]:
        """Migrate data for a specific month across all tables"""
        month_key = f"{year}-{month:02d}"
        self.logger.info(f"📅 Migrating data for {month_key}")
        
        # Calculate month boundaries
        start_date = datetime(year, month, 1)
        _, last_day = calendar.monthrange(year, month)
        end_date = datetime(year, month, last_day, 23, 59, 59)
        
        pool = await asyncpg.create_pool(self.db_url, min_size=3, max_size=8)
        
        migration_stats = {
            'records_processed': 0,
            'files_created': 0,
            'instruments_processed': 0,
            'size_bytes': 0
        }
        
        # Group data by instrument across all source tables
        instrument_data = defaultdict(list)  # instrument_id -> [MinuteRecord, ...]
        
        try:
            async with pool.acquire() as conn:
                # Process each source table
                for table in self.config.source_tables:
                    try:
                        # Check if table exists
                        table_exists = await conn.fetchval(
                            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = $1", 
                            table
                        )
                        
                        if not table_exists:
                            continue
                        
                        # Query month's data from this table
                        query = f"""
                            SELECT 
                                instrument_id,
                                timestamp,
                                open_price,
                                high_price,
                                low_price,
                                close_price,
                                volume
                            FROM {table}
                            WHERE timestamp >= $1 AND timestamp <= $2
                            ORDER BY instrument_id, timestamp
                        """
                        
                        self.logger.debug(f"   Querying {table} for {month_key}")
                        
                        # Process in batches to manage memory
                        offset = 0
                        while True:
                            batch_query = f"{query} LIMIT {self.config.batch_size} OFFSET {offset}"
                            rows = await conn.fetch(batch_query, start_date, end_date)
                            
                            if not rows:
                                break
                            
                            for row in rows:
                                # Convert to MinuteRecord
                                record = MinuteRecord(
                                    timestamp=row['timestamp'],
                                    open_price=float(row['open_price'] or 0),
                                    high_price=float(row['high_price'] or 0),
                                    low_price=float(row['low_price'] or 0),
                                    close_price=float(row['close_price'] or 0),
                                    volume=int(row['volume'] or 0)
                                )
                                
                                instrument_data[row['instrument_id']].append(record)
                            
                            migration_stats['records_processed'] += len(rows)
                            offset += self.config.batch_size
                            
                            if len(rows) < self.config.batch_size:
                                break
                        
                        self.logger.debug(f"   ✅ {table}: {migration_stats['records_processed']:,} records")
                    
                    except Exception as e:
                        self.logger.error(f"❌ Error processing table {table} for {month_key}: {e}")
                        continue
            
            # Write monthly files for each instrument
            self.logger.info(f"💾 Writing {len(instrument_data)} monthly files for {month_key}")
            
            # Process instruments concurrently
            semaphore = asyncio.Semaphore(self.config.max_concurrent_files)
            
            async def write_instrument_file(instrument_id: int, records: List[MinuteRecord]):
                async with semaphore:
                    try:
                        if records:
                            success = await self.file_manager.write_monthly_file(
                                instrument_id, year, month, records
                            )
                            
                            if success:
                                migration_stats['files_created'] += 1
                                
                                # Estimate file size (compressed)
                                estimated_size = len(records) * 32 * 0.3  # ~30% compression ratio
                                migration_stats['size_bytes'] += estimated_size
                                
                                if self.config.validate_after_write:
                                    # Validate the written file
                                    read_records = await self.file_manager.read_monthly_file(
                                        instrument_id, year, month
                                    )
                                    
                                    if len(read_records) != len(records):
                                        error_msg = f"Validation failed for instrument {instrument_id}, {month_key}: expected {len(records)}, got {len(read_records)} records"
                                        self.logger.error(error_msg)
                                        self.progress.validation_errors.append(error_msg)
                            else:
                                self.progress.failed_months.append(f"{instrument_id}_{month_key}")
                    
                    except Exception as e:
                        error_msg = f"Error writing file for instrument {instrument_id}, {month_key}: {e}"
                        self.logger.error(error_msg)
                        self.progress.failed_instruments.append(instrument_id)
            
            # Execute all file writes concurrently
            tasks = [
                write_instrument_file(instrument_id, records)
                for instrument_id, records in instrument_data.items()
            ]
            
            await asyncio.gather(*tasks, return_exceptions=True)
            
            migration_stats['instruments_processed'] = len(instrument_data)
            
        finally:
            await pool.close()
        
        self.logger.info(f"✅ {month_key} migration complete: {migration_stats['records_processed']:,} records, {migration_stats['files_created']} files")
        
        return migration_stats
    
    def save_checkpoint(self):
        """Save migration progress checkpoint"""
        checkpoint_data = {
            'config': asdict(self.config),
            'progress': asdict(self.progress),
            'timestamp': datetime.now().isoformat()
        }
        
        checkpoint_path = Path(self.config.checkpoint_file)
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(checkpoint_path, 'w') as f:
            json.dump(checkpoint_data, f, indent=2, default=str)
        
        self.progress.last_checkpoint = datetime.now()
        self.logger.info(f"💾 Checkpoint saved: {self.progress.completed_months}/{self.progress.total_months} months, {self.progress.total_records_processed:,} records")
    
    def load_checkpoint(self) -> bool:
        """Load migration progress from checkpoint"""
        try:
            if os.path.exists(self.config.checkpoint_file):
                with open(self.config.checkpoint_file, 'r') as f:
                    data = json.load(f)
                
                progress_data = data.get('progress', {})
                
                # Restore progress fields
                for field, value in progress_data.items():
                    if hasattr(self.progress, field):
                        setattr(self.progress, field, value)
                
                self.logger.info(f"📂 Checkpoint loaded: {self.progress.completed_months}/{self.progress.total_months} months completed")
                return True
        
        except Exception as e:
            self.logger.warning(f"⚠️ Could not load checkpoint: {e}")
        
        return False
    
    async def run_migration(self) -> MigrationProgress:
        """Run the complete database-to-file migration"""
        self.progress.start_time = datetime.now()
        
        self.logger.info("🚀 Starting Database-to-File Migration")
        self.logger.info(f"📂 Output path: {self.config.output_base_path}")
        self.logger.info(f"📅 Date range: {self.config.start_date} to {self.config.end_date}")
        
        # Load existing checkpoint if available
        checkpoint_loaded = self.load_checkpoint()
        
        # Analyze source data if not resuming
        if not checkpoint_loaded:
            await self.analyze_source_data()
        
        # Generate list of months to process
        current_date = self.config.start_date.replace(day=1)
        end_date = self.config.end_date.replace(day=1)
        
        months_to_process = []
        while current_date <= end_date:
            months_to_process.append((current_date.year, current_date.month))
            
            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        
        self.logger.info(f"📊 Processing {len(months_to_process)} months")
        
        # Process months
        for year, month in months_to_process:
            month_key = f"{year}-{month:02d}"
            
            # Skip if already completed (checkpoint resume)
            if checkpoint_loaded and month_key in [f"{y}-{m:02d}" for y, m in months_to_process[:self.progress.completed_months]]:
                continue
            
            try:
                stats = await self.migrate_monthly_data(year, month)
                
                # Update progress
                self.progress.completed_months += 1
                self.progress.total_records_processed += stats['records_processed']
                self.progress.total_files_created += stats['files_created']
                self.progress.total_size_bytes += stats['size_bytes']
                
                # Save checkpoint every few months
                if self.progress.completed_months % 3 == 0:
                    self.save_checkpoint()
                
                # Progress report
                completion_pct = (self.progress.completed_months / len(months_to_process)) * 100
                self.logger.info(f"📈 Progress: {completion_pct:.1f}% ({self.progress.completed_months}/{len(months_to_process)} months)")
            
            except Exception as e:
                self.logger.error(f"❌ Failed to migrate {month_key}: {e}")
                self.progress.failed_months.append(month_key)
                continue
        
        # Final checkpoint
        self.save_checkpoint()
        
        # Summary
        elapsed = datetime.now() - self.progress.start_time
        
        self.logger.info("🎉 Migration Complete!")
        self.logger.info(f"⏱️  Total time: {elapsed}")
        self.logger.info(f"📊 Records processed: {self.progress.total_records_processed:,}")
        self.logger.info(f"📁 Files created: {self.progress.total_files_created:,}")
        self.logger.info(f"💾 Total size: {self.progress.total_size_bytes / (1024**3):.2f} GB")
        self.logger.info(f"❌ Failed months: {len(self.progress.failed_months)}")
        self.logger.info(f"⚠️ Validation errors: {len(self.progress.validation_errors)}")
        
        return self.progress

async def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Database-to-File Migration for Time-Series Data')
    parser.add_argument('--output-path', default='/data/monthly/interval', help='Output base path')
    parser.add_argument('--start-date', default='2005-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', default='2025-12-31', help='End date (YYYY-MM-DD)')
    parser.add_argument('--batch-size', type=int, default=10000, help='Records per batch')
    parser.add_argument('--max-concurrent', type=int, default=20, help='Max concurrent file writes')
    parser.add_argument('--checkpoint-file', help='Checkpoint file path')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint')
    parser.add_argument('--analyze-only', action='store_true', help='Only analyze source data')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('database_migration.log')
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        config = MigrationConfig(
            output_base_path=args.output_path,
            start_date=datetime.strptime(args.start_date, '%Y-%m-%d').date(),
            end_date=datetime.strptime(args.end_date, '%Y-%m-%d').date(),
            batch_size=args.batch_size,
            max_concurrent_files=args.max_concurrent,
            checkpoint_file=args.checkpoint_file or '/tmp/migration_checkpoint.json'
        )
        
        migrator = DatabaseToFileMigrator(config)
        
        if args.analyze_only:
            logger.info("🔍 Running analysis only...")
            await migrator.analyze_source_data()
        else:
            await migrator.run_migration()
        
    except KeyboardInterrupt:
        logger.info("🛑 Migration interrupted by user")
    except Exception as e:
        logger.error(f"💥 Migration failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())