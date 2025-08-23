#!/usr/bin/env python3
"""
Multi-Vendor 1-Minute Bars Population Script

Fetches 1-minute OHLCV data from multiple vendors (Polygon, Tiingo, FMP, EODHD)
for selected instruments over the past month. Supports both file-based storage
and database insertion with comprehensive data quality validation.

Usage:
    python populate_minute_bars_multi_vendor.py --symbols AAPL,MSFT,GOOGL --days 30
    python populate_minute_bars_multi_vendor.py --symbols AAPL --vendors polygon,tiingo
    python populate_minute_bars_multi_vendor.py --config symbols.txt --storage file
"""

import os
import sys
import asyncio
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path
import json

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

try:
    # Import market data adapters
    from market_data.agent.polygon_minute_adapter import PolygonMinuteAdapter, MinuteBar as PolygonBar
    from market_data.agent.tiingo_intraday_adapter import TiingoIntradayAdapter, TiingoMinuteBar
    from market_data.agent.fmp_minute_adapter import FMPMinuteAdapter, FMPMinuteBar
    from market_data.agent.eodhd_minute_adapter import EODHDMinuteAdapter, EODHDMinuteBar
    
    # Import storage systems
    from storage.file_based_minute_manager import FileBasedMinuteManager
    from config.environment import Environment
    
    # Import database components
    import asyncpg
    
except ImportError as e:
    print(f"❌ Failed to import required modules: {e}")
    print("Make sure you're running from the project root and src/ is in PYTHONPATH")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MultiVendorMinuteDataCollector:
    """
    Unified collector for 1-minute bars from multiple market data vendors.
    """
    
    def __init__(self):
        self.vendors = {
            'polygon': None,
            'tiingo': None, 
            'fmp': None,
            'eodhd': None
        }
        
        # Initialize file-based storage manager
        self.file_manager = None
        
        # Database connection
        self.db_pool = None
        
        # Statistics tracking
        self.stats = {
            'total_symbols': 0,
            'successful_symbols': 0,
            'failed_symbols': 0,
            'total_bars': 0,
            'vendor_stats': {vendor: {'bars': 0, 'symbols': 0} for vendor in self.vendors.keys()}
        }
    
    async def initialize(self, vendors: List[str], storage_type: str = 'file'):
        """Initialize the collector with specified vendors and storage."""
        logger.info(f"🚀 Initializing multi-vendor collector with vendors: {vendors}")
        
        # Initialize requested vendors
        for vendor in vendors:
            if vendor == 'polygon':
                try:
                    self.vendors['polygon'] = PolygonMinuteAdapter()
                    logger.info("✅ Polygon adapter initialized")
                except Exception as e:
                    logger.warning(f"⚠️  Failed to initialize Polygon adapter: {e}")
            
            elif vendor == 'tiingo':
                try:
                    self.vendors['tiingo'] = TiingoIntradayAdapter()
                    logger.info("✅ Tiingo adapter initialized")
                except Exception as e:
                    logger.warning(f"⚠️  Failed to initialize Tiingo adapter: {e}")
            
            elif vendor == 'fmp':
                try:
                    self.vendors['fmp'] = FMPMinuteAdapter()
                    logger.info("✅ FMP adapter initialized")
                except Exception as e:
                    logger.warning(f"⚠️  Failed to initialize FMP adapter: {e}")
            
            elif vendor == 'eodhd':
                try:
                    self.vendors['eodhd'] = EODHDMinuteAdapter()
                    logger.info("✅ EODHD adapter initialized")
                except Exception as e:
                    logger.warning(f"⚠️  Failed to initialize EODHD adapter: {e}")
        
        # Initialize storage
        if storage_type == 'file':
            try:
                self.file_manager = FileBasedMinuteManager()
                logger.info("✅ File-based storage manager initialized")
            except Exception as e:
                logger.error(f"❌ Failed to initialize file storage: {e}")
                raise
        
        elif storage_type == 'database':
            try:
                env = Environment()
                db_url = env.get_database_url()
                self.db_pool = await asyncpg.create_pool(db_url, min_size=2, max_size=10)
                logger.info("✅ Database connection pool initialized")
            except Exception as e:
                logger.error(f"❌ Failed to initialize database connection: {e}")
                raise
    
    async def collect_data(
        self, 
        symbols: List[str], 
        start_date: datetime, 
        end_date: datetime,
        storage_type: str = 'file'
    ) -> Dict[str, Any]:
        """
        Collect 1-minute data for all symbols from all available vendors.
        
        Args:
            symbols: List of stock symbols
            start_date: Start date for data collection
            end_date: End date for data collection
            storage_type: 'file' or 'database'
        
        Returns:
            Collection results summary
        """
        logger.info(f"📊 Starting data collection for {len(symbols)} symbols from {start_date.date()} to {end_date.date()}")
        self.stats['total_symbols'] = len(symbols)
        
        results = {}
        
        # Process each vendor
        active_vendors = {k: v for k, v in self.vendors.items() if v is not None}
        
        for vendor_name, adapter in active_vendors.items():
            logger.info(f"🔄 Processing vendor: {vendor_name.upper()}")
            
            try:
                # Collect data using async context manager
                if vendor_name == 'polygon':
                    async with adapter:
                        vendor_data = await adapter.fetch_multiple_symbols_async(
                            symbols, start_date, end_date, max_concurrent=3
                        )
                
                elif vendor_name == 'tiingo':
                    async with adapter:
                        vendor_data = await adapter.fetch_multiple_symbols_async(
                            symbols, start_date, end_date, max_concurrent=2
                        )
                
                elif vendor_name == 'fmp':
                    async with adapter:
                        vendor_data = await adapter.fetch_multiple_symbols_async(
                            symbols, start_date, end_date, max_concurrent=2
                        )
                
                elif vendor_name == 'eodhd':
                    async with adapter:
                        vendor_data = await adapter.fetch_multiple_symbols_async(
                            symbols, start_date, end_date, max_concurrent=1
                        )
                
                # Process and store the data
                for symbol, bars in vendor_data.items():
                    if not bars:
                        logger.warning(f"No data received for {symbol} from {vendor_name}")
                        continue
                    
                    # Validate data quality
                    quality_metrics = adapter.validate_data_quality(bars)
                    
                    if not quality_metrics.get('valid', False):
                        logger.warning(f"Poor data quality for {symbol} from {vendor_name}: {quality_metrics.get('reason', 'Unknown')}")
                    
                    # Store the data
                    if storage_type == 'file':
                        await self._store_data_to_file(symbol, vendor_name, bars)
                    elif storage_type == 'database':
                        await self._store_data_to_database(symbol, vendor_name, bars)
                    
                    # Update statistics
                    self.stats['vendor_stats'][vendor_name]['bars'] += len(bars)
                    self.stats['vendor_stats'][vendor_name]['symbols'] += 1
                    self.stats['total_bars'] += len(bars)
                    
                    logger.info(f"✅ Processed {len(bars)} bars for {symbol} from {vendor_name}")
                
                results[vendor_name] = {
                    'symbols_processed': len([s for s, bars in vendor_data.items() if bars]),
                    'total_bars': sum(len(bars) for bars in vendor_data.values()),
                    'status': 'completed'
                }
                
                self.stats['successful_symbols'] += len([s for s, bars in vendor_data.items() if bars])
                
            except Exception as e:
                logger.error(f"❌ Error processing {vendor_name}: {e}")
                results[vendor_name] = {
                    'status': 'failed',
                    'error': str(e)
                }
                self.stats['failed_symbols'] += len(symbols)
        
        return results
    
    async def _store_data_to_file(self, symbol: str, vendor: str, bars: List[Any]):
        """Store data using file-based storage."""
        if not self.file_manager:
            logger.error("File manager not initialized")
            return
        
        try:
            # Import the standardized MinuteBar from FileBasedMinuteManager
            from storage.file_based_minute_manager import MinuteBar
            
            # Convert vendor-specific bars to unified format
            unified_bars = []
            
            for bar in bars:
                # Create a standardized MinuteBar object
                unified_bar = MinuteBar(
                    symbol=symbol,
                    timestamp=bar.timestamp,
                    open=float(bar.open),
                    high=float(bar.high),
                    low=float(bar.low),
                    close=float(bar.close),
                    volume=int(bar.volume),
                    vwap=getattr(bar, 'vwap', None),
                    trade_count=getattr(bar, 'trade_count', None),
                    vendor=vendor,
                    quality_score=getattr(bar, 'quality_score', 1.0)
                )
                unified_bars.append(unified_bar)
            
            # Store with merge strategy to handle overlaps
            result = await self.file_manager.store_minute_data(
                symbol, unified_bars, overlap_strategy='merge'
            )
            
            logger.debug(f"Stored {len(unified_bars)} bars for {symbol} ({vendor}) to file: {result}")
            
        except Exception as e:
            logger.error(f"Error storing {symbol} data from {vendor} to file: {e}")
    
    async def _store_data_to_database(self, symbol: str, vendor: str, bars: List[Any]):
        """Store data to PostgreSQL database."""
        if not self.db_pool:
            logger.error("Database pool not initialized")
            return
        
        try:
            async with self.db_pool.acquire() as conn:
                # Prepare data for batch insert
                records = []
                
                for bar in bars:
                    record = (
                        symbol,
                        bar.timestamp,
                        float(bar.open),
                        float(bar.high),
                        float(bar.low),
                        float(bar.close),
                        int(bar.volume),
                        vendor,
                        datetime.now()  # created_at
                    )
                    records.append(record)
                
                # Batch insert with ON CONFLICT handling
                await conn.executemany("""
                    INSERT INTO minute_bars (
                        symbol, timestamp, open, high, low, close, volume, vendor, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (symbol, timestamp) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        vendor = EXCLUDED.vendor,
                        updated_at = CURRENT_TIMESTAMP
                """, records)
                
                logger.debug(f"Inserted {len(records)} bars for {symbol} ({vendor}) to database")
                
        except Exception as e:
            logger.error(f"Error storing {symbol} data from {vendor} to database: {e}")
    
    async def cleanup(self):
        """Clean up resources."""
        if self.file_manager:
            await self.file_manager.close()
        
        if self.db_pool:
            await self.db_pool.close()
        
        logger.info("🧹 Cleanup completed")
    
    def print_summary(self):
        """Print collection summary."""
        print("\n" + "="*60)
        print("📊 MULTI-VENDOR DATA COLLECTION SUMMARY")
        print("="*60)
        print(f"Total Symbols Requested: {self.stats['total_symbols']}")
        print(f"Successfully Processed: {self.stats['successful_symbols']}")
        print(f"Failed: {self.stats['failed_symbols']}")
        print(f"Total Bars Collected: {self.stats['total_bars']:,}")
        
        print(f"\n📈 Vendor Breakdown:")
        for vendor, stats in self.stats['vendor_stats'].items():
            if stats['bars'] > 0:
                print(f"  {vendor.upper():<10}: {stats['bars']:,} bars from {stats['symbols']} symbols")
        
        print("\n✅ Collection completed successfully!")


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Multi-Vendor 1-Minute Bars Population",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Collect AAPL data from all vendors for past 30 days
    python populate_minute_bars_multi_vendor.py --symbols AAPL --days 30

    # Collect multiple symbols from specific vendors
    python populate_minute_bars_multi_vendor.py --symbols AAPL,MSFT,GOOGL --vendors polygon,tiingo

    # Use file from symbols list and store in database
    python populate_minute_bars_multi_vendor.py --symbols-file symbols.txt --storage database

    # Collect data for custom date range
    python populate_minute_bars_multi_vendor.py --symbols AAPL --start-date 2024-01-01 --end-date 2024-01-31
        """
    )
    
    parser.add_argument(
        '--symbols', 
        type=str, 
        help='Comma-separated list of stock symbols (e.g., AAPL,MSFT,GOOGL)'
    )
    parser.add_argument(
        '--symbols-file', 
        type=str,
        help='Path to file containing symbols (one per line)'
    )
    parser.add_argument(
        '--vendors', 
        type=str, 
        default='polygon,tiingo,fmp,eodhd',
        help='Comma-separated list of vendors (default: all)'
    )
    parser.add_argument(
        '--days', 
        type=int, 
        default=30,
        help='Number of days back to collect data (default: 30)'
    )
    parser.add_argument(
        '--start-date', 
        type=str,
        help='Start date in YYYY-MM-DD format'
    )
    parser.add_argument(
        '--end-date', 
        type=str,
        help='End date in YYYY-MM-DD format'
    )
    parser.add_argument(
        '--storage', 
        type=str, 
        choices=['file', 'database'],
        default='file',
        help='Storage type: file or database (default: file)'
    )
    parser.add_argument(
        '--debug', 
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate arguments
    if not args.symbols and not args.symbols_file:
        parser.error("Either --symbols or --symbols-file must be provided")
    
    # Parse symbols
    symbols = []
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(',') if s.strip()]
    elif args.symbols_file:
        try:
            with open(args.symbols_file, 'r') as f:
                symbols = [line.strip().upper() for line in f if line.strip()]
        except FileNotFoundError:
            logger.error(f"Symbols file not found: {args.symbols_file}")
            return 1
    
    if not symbols:
        logger.error("No symbols provided")
        return 1
    
    # Parse vendors
    vendors = [v.strip().lower() for v in args.vendors.split(',') if v.strip()]
    
    # Parse dates
    if args.start_date and args.end_date:
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            return 1
    else:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)
    
    # Validate date range
    if start_date >= end_date:
        logger.error("Start date must be before end date")
        return 1
    
    if (end_date - start_date).days > 365:
        logger.warning("Date range is more than 1 year, this may take a very long time")
    
    logger.info(f"🎯 Target: {len(symbols)} symbols, {len(vendors)} vendors, {(end_date - start_date).days} days")
    logger.info(f"📅 Date range: {start_date.date()} to {end_date.date()}")
    logger.info(f"🏪 Vendors: {', '.join(vendors)}")
    logger.info(f"💾 Storage: {args.storage}")
    
    # Initialize collector
    collector = MultiVendorMinuteDataCollector()
    
    try:
        # Initialize with specified vendors and storage
        await collector.initialize(vendors, args.storage)
        
        # Collect data
        results = await collector.collect_data(symbols, start_date, end_date, args.storage)
        
        # Print results
        collector.print_summary()
        
        # Print detailed results if debug
        if args.debug:
            print(f"\n🔍 Detailed Results:")
            print(json.dumps(results, indent=2, default=str))
        
        return 0
    
    except KeyboardInterrupt:
        logger.info("⚠️  Operation cancelled by user")
        return 1
    
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        return 1
    
    finally:
        await collector.cleanup()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))