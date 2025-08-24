#!/usr/bin/env python3
"""
Enhanced Multi-Vendor 1-Minute Bars Collection

Demonstrates vendor-separated file storage and timezone normalization.
Each vendor gets its own directory structure: vendor/symbol/year/month/

Usage:
    python scripts/enhanced_minute_bars_collection.py --symbols AAPL --vendors polygon,tiingo,eodhd --days 1
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
import pandas as pd
import pytz

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

try:
    # Import market data adapters
    from market_data.agent.polygon_minute_adapter import PolygonMinuteAdapter, MinuteBar as PolygonBar
    from market_data.agent.tiingo_intraday_adapter import TiingoIntradayAdapter, TiingoMinuteBar
    from market_data.agent.fmp_minute_adapter import FMPMinuteAdapter, FMPMinuteBar
    from market_data.agent.eodhd_minute_adapter import EODHDMinuteAdapter, EODHDMinuteBar
    
    # Import storage systems
    from storage.file_based_minute_manager import MinuteBar
    from config.environment import Environment
    
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


class VendorSeparatedFileManager:
    """
    File manager with vendor-specific directory structure:
    /home/jianjun/ats-data/minute-files/
    ├── polygon/AAPL/2025/08/AAPL_2025_08.parquet
    ├── tiingo/AAPL/2025/08/AAPL_2025_08.parquet
    └── eodhd/AAPL/2025/08/AAPL_2025_08.parquet
    """
    
    def __init__(self, base_path: str = "/home/jianjun/ats-data/minute-files"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
    async def store_vendor_data(self, vendor: str, symbol: str, bars: List[MinuteBar]) -> Dict[str, Any]:
        """Store data in vendor-specific directory structure."""
        
        if not bars:
            return {"status": "no_data", "bars_stored": 0}
        
        try:
            # Normalize all timestamps to UTC
            normalized_bars = []
            for bar in bars:
                # Ensure timestamp is timezone-aware UTC
                if bar.timestamp.tzinfo is None:
                    # Assume naive timestamps are in US/Eastern (market time)
                    eastern = pytz.timezone('US/Eastern')
                    timestamp_utc = eastern.localize(bar.timestamp).astimezone(pytz.UTC)
                else:
                    timestamp_utc = bar.timestamp.astimezone(pytz.UTC)
                
                normalized_bar = MinuteBar(
                    symbol=symbol,
                    timestamp=timestamp_utc,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    vwap=bar.vwap,
                    trade_count=bar.trade_count,
                    vendor=vendor,
                    quality_score=bar.quality_score
                )
                normalized_bars.append(normalized_bar)
            
            # Group by month for file organization
            monthly_groups = {}
            for bar in normalized_bars:
                year_month = (bar.timestamp.year, bar.timestamp.month)
                if year_month not in monthly_groups:
                    monthly_groups[year_month] = []
                monthly_groups[year_month].append(bar)
            
            total_stored = 0
            files_created = []
            
            for (year, month), month_bars in monthly_groups.items():
                # Create vendor-specific directory structure
                vendor_dir = self.base_path / vendor / symbol / str(year) / f"{month:02d}"
                vendor_dir.mkdir(parents=True, exist_ok=True)
                
                # Create file path
                file_path = vendor_dir / f"{symbol}_{year}_{month:02d}.parquet"
                
                # Convert to DataFrame
                df_data = []
                for bar in month_bars:
                    df_data.append({
                        'timestamp': bar.timestamp,
                        'open': bar.open,
                        'high': bar.high,
                        'low': bar.low,
                        'close': bar.close,
                        'volume': bar.volume,
                        'vwap': bar.vwap,
                        'trade_count': bar.trade_count,
                        'vendor': bar.vendor,
                        'quality_score': bar.quality_score
                    })
                
                df = pd.DataFrame(df_data)
                df = df.sort_values('timestamp')
                df = df.drop_duplicates(subset=['timestamp'], keep='last')
                
                # Handle existing file
                if file_path.exists():
                    # Load existing data and merge
                    existing_df = pd.read_parquet(file_path)
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df = combined_df.sort_values('timestamp')
                    combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
                    df = combined_df
                
                # Save to file
                df.to_parquet(file_path, index=False)
                
                bars_in_file = len(df)
                total_stored += len(month_bars)
                files_created.append(str(file_path))
                
                logger.info(f"Stored {len(month_bars)} bars for {symbol} ({vendor}) in {file_path}")
                logger.debug(f"File contains {bars_in_file} total bars after merge")
            
            return {
                "status": "success",
                "bars_stored": total_stored,
                "files_created": files_created,
                "vendor": vendor,
                "symbol": symbol
            }
            
        except Exception as e:
            logger.error(f"Error storing {symbol} data from {vendor}: {e}")
            return {
                "status": "error",
                "error": str(e),
                "bars_stored": 0
            }


class EnhancedMultiVendorCollector:
    """Enhanced collector with vendor-separated storage and timezone normalization."""
    
    def __init__(self):
        self.vendors = {
            'polygon': None,
            'tiingo': None, 
            'fmp': None,
            'eodhd': None
        }
        
        self.file_manager = VendorSeparatedFileManager()
        
        # Statistics tracking
        self.stats = {
            'total_symbols': 0,
            'successful_symbols': 0,
            'failed_symbols': 0,
            'total_bars': 0,
            'vendor_stats': {vendor: {'bars': 0, 'symbols': 0, 'files': 0} for vendor in self.vendors.keys()}
        }
    
    async def initialize(self, vendors: List[str]):
        """Initialize the collector with specified vendors."""
        logger.info(f"🚀 Initializing enhanced multi-vendor collector with vendors: {vendors}")
        
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
    
    async def collect_data(
        self, 
        symbols: List[str], 
        start_date: datetime, 
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Collect 1-minute data for all symbols from all available vendors with vendor separation.
        """
        logger.info(f"📊 Starting vendor-separated data collection for {len(symbols)} symbols from {start_date.date()} to {end_date.date()}")
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
                
                # Process and store the data with vendor separation
                for symbol, bars in vendor_data.items():
                    if not bars:
                        logger.warning(f"No data received for {symbol} from {vendor_name}")
                        continue
                    
                    # Convert vendor-specific bars to unified format
                    unified_bars = []
                    for bar in bars:
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
                            vendor=vendor_name,
                            quality_score=getattr(bar, 'quality_score', 1.0)
                        )
                        unified_bars.append(unified_bar)
                    
                    # Store in vendor-specific directory
                    storage_result = await self.file_manager.store_vendor_data(
                        vendor_name, symbol, unified_bars
                    )
                    
                    # Update statistics
                    if storage_result['status'] == 'success':
                        bars_stored = storage_result['bars_stored']
                        files_created = len(storage_result.get('files_created', []))
                        
                        self.stats['vendor_stats'][vendor_name]['bars'] += bars_stored
                        self.stats['vendor_stats'][vendor_name]['symbols'] += 1
                        self.stats['vendor_stats'][vendor_name]['files'] += files_created
                        self.stats['total_bars'] += bars_stored
                        
                        logger.info(f"✅ Stored {bars_stored} bars for {symbol} from {vendor_name} in {files_created} files")
                    else:
                        logger.error(f"❌ Failed to store {symbol} data from {vendor_name}: {storage_result.get('error', 'Unknown error')}")
                
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
    
    def print_summary(self):
        """Print enhanced collection summary with vendor separation."""
        print("\n" + "="*70)
        print("📊 ENHANCED MULTI-VENDOR DATA COLLECTION SUMMARY")
        print("="*70)
        print(f"Total Symbols Requested: {self.stats['total_symbols']}")
        print(f"Successfully Processed: {self.stats['successful_symbols']}")
        print(f"Failed: {self.stats['failed_symbols']}")
        print(f"Total Bars Collected: {self.stats['total_bars']:,}")
        
        print(f"\n📈 Vendor Breakdown (with separate directories):")
        for vendor, stats in self.stats['vendor_stats'].items():
            if stats['bars'] > 0:
                print(f"  {vendor.upper():<10}: {stats['bars']:,} bars from {stats['symbols']} symbols in {stats['files']} files")
        
        print(f"\n📁 File Structure:")
        base_path = Path("/home/jianjun/ats-data/minute-files")
        for vendor_dir in sorted(base_path.iterdir()):
            if vendor_dir.is_dir() and vendor_dir.name != '.backups':
                print(f"  📂 {vendor_dir.name}/")
                for symbol_dir in sorted(vendor_dir.iterdir()):
                    if symbol_dir.is_dir():
                        file_count = sum(1 for _ in symbol_dir.rglob("*.parquet"))
                        print(f"    📂 {symbol_dir.name}/ ({file_count} files)")
        
        print("\n✅ Enhanced collection completed successfully!")


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Enhanced Multi-Vendor 1-Minute Bars Collection with Vendor Separation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Collect AAPL data from 3 vendors with vendor separation
    python scripts/enhanced_minute_bars_collection.py --symbols AAPL --vendors polygon,tiingo,eodhd --days 1

    # Collect multiple symbols from specific vendors
    python scripts/enhanced_minute_bars_collection.py --symbols AAPL,MSFT --vendors polygon,tiingo --days 2
        """
    )
    
    parser.add_argument(
        '--symbols', 
        type=str, 
        required=True,
        help='Comma-separated list of stock symbols (e.g., AAPL,MSFT,GOOGL)'
    )
    parser.add_argument(
        '--vendors', 
        type=str, 
        default='polygon,tiingo,eodhd',
        help='Comma-separated list of vendors (default: polygon,tiingo,eodhd)'
    )
    parser.add_argument(
        '--days', 
        type=int, 
        default=1,
        help='Number of days back to collect data (default: 1)'
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
    
    # Parse symbols
    symbols = [s.strip().upper() for s in args.symbols.split(',') if s.strip()]
    
    # Parse vendors
    vendors = [v.strip().lower() for v in args.vendors.split(',') if v.strip()]
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=args.days)
    
    logger.info(f"🎯 Target: {len(symbols)} symbols, {len(vendors)} vendors, {args.days} days")
    logger.info(f"📅 Date range: {start_date.date()} to {end_date.date()}")
    logger.info(f"🏪 Vendors: {', '.join(vendors)}")
    logger.info(f"📁 Storage: Vendor-separated file structure")
    
    # Initialize collector
    collector = EnhancedMultiVendorCollector()
    
    try:
        # Initialize with specified vendors
        await collector.initialize(vendors)
        
        # Collect data
        results = await collector.collect_data(symbols, start_date, end_date)
        
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


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))