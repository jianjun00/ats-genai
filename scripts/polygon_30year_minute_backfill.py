#!/usr/bin/env python3
"""
Polygon 30-Year Minute Bar Backfill Script with Enhanced Checkpointing

Fetches 1-minute OHLCV data from Polygon API for all US instruments over 30 years
using the existing enhanced checkpoint system for reliable resume capability.

Features:
- Uses existing enhanced_minute_backfill_orchestrator.py
- Fine-grained checkpointing at segment level  
- Intelligent resume from exact failure point
- 30-year historical coverage (1994-2024)
- All US instruments from dev_instruments table
- Storage on D: drive: /mnt/d/ats-data/polygon/minute-bars/
- Comprehensive progress tracking and ETA
"""

import os
import sys
import asyncio
import asyncpg
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from pathlib import Path
import json

# Add src to Python path
sys.path.insert(0, '/workspace/src')

from market_data.backfill.enhanced_minute_backfill_orchestrator import (
    EnhancedMinuteBackfillOrchestrator,
    EnhancedBackfillConfig,
    ReconciliationMethod
)
from storage.hybrid_minute_data_manager import StorageConfig
from config.environment import Environment, EnvironmentType
from config.database import Database

# Configure logging (Docker-compatible)
log_dir = Path('/logs' if Path('/logs').exists() else '/tmp/logs')
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'polygon_minute_backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("polygon_30year_minute_backfill")

class PolygonMinuteBackfillRunner:
    """
    Runner for Polygon 30-year minute bar backfill using enhanced checkpoint system.
    """
    
    def __init__(self, api_key: str, checkpoint_file: str = None):
        self.api_key = api_key
        self.checkpoint_file = checkpoint_file or "/data/checkpoints/polygon/minute_backfill_30year.json"
        
        # Database connection
        self.db_pool = None
        
        # Storage configuration (Docker-compatible paths)
        self.storage_config = StorageConfig(
            base_data_path="/data/polygon"
        )
        
        # Statistics
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_symbols': 0,
            'total_segments': 0,
            'completed_segments': 0,
            'failed_segments': 0,
            'total_bars_processed': 0,
            'resume_from_checkpoint': False
        }
    
    async def setup_database_connection(self):
        """Setup async database connection pool."""
        try:
            # Use development database connection (Docker-compatible)
            db_host = os.getenv('DB_HOST', 'postgres')
            db_port = int(os.getenv('DB_PORT', '5432'))
            
            # If DB_HOST is postgres (Docker service name), use internal port 5432
            if db_host == 'postgres':
                db_port = 5432
            
            db_config = {
                'host': db_host,
                'port': db_port,
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'dev_password'),
                'database': os.getenv('DB_NAME', 'dev_db'),
                'min_size': 5,
                'max_size': 20
            }
            
            self.db_pool = await asyncpg.create_pool(**db_config)
            logger.info("Database connection pool created successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup database connection: {e}")
            raise
    
    async def get_us_symbols(self, limit: int = None) -> List[str]:
        """Get list of active US symbols from dev_instruments table."""
        try:
            query = """
            SELECT DISTINCT symbol 
            FROM dev_instruments 
            WHERE active = true 
              AND symbol ~ '^[A-Z]{1,5}$'  -- US symbol pattern
              AND exchange IN ('NYSE', 'NASDAQ', 'NYSE ARCA', 'BATS')
            ORDER BY symbol
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(query)
                symbols = [row['symbol'] for row in rows]
            
            logger.info(f"Retrieved {len(symbols)} US symbols for backfill")
            return symbols
            
        except Exception as e:
            logger.error(f"Failed to get US symbols: {e}")
            raise
    
    def create_backfill_config(self, symbols: List[str]) -> EnhancedBackfillConfig:
        """Create enhanced backfill configuration for 30-year minute data."""
        
        # 30-year date range
        start_date = datetime(1994, 1, 1)
        end_date = datetime(2024, 12, 31)
        
        config = EnhancedBackfillConfig(
            # Date range
            start_date=start_date,
            end_date=end_date,
            
            # API credentials
            polygon_api_key=self.api_key,
            tiingo_api_key=None,  # Polygon only for this backfill
            
            # Symbol selection
            symbols=symbols,
            
            # Parallel processing (conservative for rate limits)
            max_concurrent_symbols=3,  # Polygon rate limits
            max_concurrent_date_ranges=2,
            max_total_workers=6,
            
            # Chunking (weekly chunks for minute data)
            chunk_size_days=7,
            min_chunk_size_days=1,
            
            # Storage
            storage_base_path="/data/polygon",
            
            # Reconciliation (Polygon only, no reconciliation needed)
            reconciliation_method=ReconciliationMethod.POLYGON_PRIORITY,
            require_both_vendors=False,
            
            # Error handling
            max_retries_per_segment=3,
            retry_delay_seconds=600,  # 10 minutes between retries
            continue_on_error=True,
            failure_threshold=0.05,  # Allow 5% segment failure
            
            # Checkpointing
            checkpoint_file=self.checkpoint_file,
            checkpoint_interval_minutes=2,  # Save every 2 minutes
            auto_checkpoint_segment_count=25,  # Checkpoint every 25 segments
            
            # Progress reporting
            progress_reporting_interval=50,
            detailed_logging=True
        )
        
        return config
    
    async def run_backfill(self, symbols: List[str] = None, limit: int = None) -> Dict[str, Any]:
        """Execute the 30-year Polygon minute bar backfill."""
        
        self.stats['start_time'] = datetime.now()
        logger.info(f"Starting Polygon 30-year minute bar backfill at {self.stats['start_time']}")
        
        try:
            # Setup database connection
            await self.setup_database_connection()
            
            # Get symbols to process
            if not symbols:
                symbols = await self.get_us_symbols(limit=limit)
            
            self.stats['total_symbols'] = len(symbols)
            logger.info(f"Processing {len(symbols)} symbols over 30 years (1994-2024)")
            
            # Create enhanced backfill configuration
            config = self.create_backfill_config(symbols)
            
            # Check if resuming from checkpoint
            if Path(self.checkpoint_file).exists():
                self.stats['resume_from_checkpoint'] = True
                logger.info(f"Resuming from existing checkpoint: {self.checkpoint_file}")
            
            # Execute backfill using enhanced orchestrator
            async with EnhancedMinuteBackfillOrchestrator(
                pool=self.db_pool,
                config=config,
                storage_config=self.storage_config
            ) as orchestrator:
                
                # Run the enhanced backfill
                results = await orchestrator.run_backfill()
                
                # Update statistics
                self.stats.update(results)
                
                return results
                
        except Exception as e:
            logger.error(f"Backfill failed: {e}")
            raise
        
        finally:
            if self.db_pool:
                await self.db_pool.close()
            
            self.stats['end_time'] = datetime.now()
            
            # Log final statistics
            self._log_final_statistics()
    
    def _log_final_statistics(self):
        """Log comprehensive final statistics."""
        if not self.stats['start_time']:
            return
        
        duration = self.stats['end_time'] - self.stats['start_time']
        duration_hours = duration.total_seconds() / 3600
        
        logger.info("=" * 80)
        logger.info("POLYGON 30-YEAR MINUTE BAR BACKFILL - FINAL STATISTICS")
        logger.info("=" * 80)
        logger.info(f"Duration: {duration} ({duration_hours:.1f} hours)")
        logger.info(f"Total Symbols: {self.stats.get('total_symbols', 0):,}")
        logger.info(f"Total Segments: {self.stats.get('total_segments', 0):,}")
        logger.info(f"Completed Segments: {self.stats.get('segments_completed', 0):,}")
        logger.info(f"Failed Segments: {self.stats.get('segments_failed', 0):,}")
        logger.info(f"Total Bars Processed: {self.stats.get('total_bars_stored', 0):,}")
        
        if self.stats.get('segments_completed', 0) > 0:
            success_rate = (self.stats.get('segments_completed', 0) / 
                          max(self.stats.get('total_segments', 1), 1)) * 100
            logger.info(f"Success Rate: {success_rate:.1f}%")
            
            bars_per_hour = self.stats.get('total_bars_stored', 0) / max(duration_hours, 0.01)
            logger.info(f"Processing Rate: {bars_per_hour:,.0f} bars/hour")
        
        logger.info(f"Resumed from Checkpoint: {self.stats.get('resume_from_checkpoint', False)}")
        logger.info(f"Checkpoint File: {self.checkpoint_file}")
        logger.info("=" * 80)

async def main():
    """Main entry point for Polygon minute bar backfill."""
    
    parser = argparse.ArgumentParser(
        description="Polygon 30-Year Minute Bar Backfill with Enhanced Checkpointing"
    )
    
    parser.add_argument(
        "--api-key", 
        default=os.getenv("POLYGON_API_KEY"),
        help="Polygon API key (default: from POLYGON_API_KEY env var)"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of symbols for testing (default: all US symbols)"
    )
    
    parser.add_argument(
        "--checkpoint-file",
        default="/data/checkpoints/polygon/minute_backfill_30year.json",
        help="Checkpoint file path for resume capability"
    )
    
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Specific symbols to process (default: all US symbols from dev_instruments)"
    )
    
    args = parser.parse_args()
    
    # Validate API key
    if not args.api_key:
        logger.error("POLYGON_API_KEY environment variable must be set or --api-key provided")
        return 1
    
    # Create checkpoint directory
    checkpoint_dir = Path(args.checkpoint_file).parent
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Initialize and run backfill
        runner = PolygonMinuteBackfillRunner(
            api_key=args.api_key,
            checkpoint_file=args.checkpoint_file
        )
        
        results = await runner.run_backfill(
            symbols=args.symbols,
            limit=args.limit
        )
        
        logger.info("Polygon 30-year minute bar backfill completed successfully!")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Backfill interrupted by user - progress saved to checkpoint")
        return 130
        
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)