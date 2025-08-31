#!/usr/bin/env python3
"""
FirstRate Minute Bar Backfill Processor

Converts FirstRate historical 1-minute OHLCV zip files into monthly Parquet files 
for the ATS trading platform with checkpoint-based resumable processing.

Usage:
    # Process all stock symbols (full backfill)
    PYTHONPATH=src python scripts/populate_firstrate_minute_bars.py --asset-type stock

    # Process specific symbols only  
    PYTHONPATH=src python scripts/populate_firstrate_minute_bars.py --symbols AAPL,MSFT,GOOGL

    # Resume from checkpoint
    PYTHONPATH=src python scripts/populate_firstrate_minute_bars.py --checkpoint-file production.json --resume

    # Debug mode with limited symbols
    PYTHONPATH=src python scripts/populate_firstrate_minute_bars.py --limit 10 --debug
"""

import os
import sys
import asyncio
import json
import argparse
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Set
from pathlib import Path
from collections import defaultdict
import time

# Add src to Python path 
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from market_data.agent.firstrate_adapter import FirstRateAdapter, Tick
from storage.file_based_minute_manager import FileBasedMinuteManager

logger = logging.getLogger(__name__)


class FirstRateBackfillProcessor:
    """Checkpoint-based FirstRate minute bar processing"""
    
    def __init__(
        self, 
        data_path: str = "/data/firstrate-data",  # Docker mount point
        output_path: str = "/data/minute-bars/firstrate",  # Docker mount point
        checkpoint_file: str = "firstrate_monthly_production.json"
    ):
        self.adapter = FirstRateAdapter(data_path)
        self.output_path = Path(output_path)
        self.checkpoint_file = Path(checkpoint_file)
        
        # Create output directory
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize minute manager
        self.minute_manager = FileBasedMinuteManager(str(self.output_path))
        
        # Load checkpoint
        self.checkpoint_data = self.load_checkpoint()
        
    def load_checkpoint(self) -> Dict:
        """Load processing checkpoint from file"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    
                    # Convert date strings back to date objects in inventory
                    if 'symbol_inventory' in data and data['symbol_inventory']:
                        for symbol, info in data['symbol_inventory'].items():
                            if 'min_date' in info and isinstance(info['min_date'], str):
                                info['min_date'] = datetime.fromisoformat(info['min_date']).date()
                            if 'max_date' in info and isinstance(info['max_date'], str):
                                info['max_date'] = datetime.fromisoformat(info['max_date']).date()
                    
                    logger.info(f"📝 Loaded checkpoint: {len(data.get('completed_months', {}))} symbols processed")
                    return data
            except Exception as e:
                logger.error(f"❌ Failed to load checkpoint: {e}")
                
        return {
            'completed_months': {},
            'failed_months': {},
            'last_processed': None,
            'total_symbols': 0,
            'processing_stats': {
                'symbols_processed': 0,
                'months_processed': 0,
                'records_written': 0,
                'errors': 0
            }
        }
    
    def save_checkpoint(self):
        """Save current processing state"""
        self.checkpoint_data['last_processed'] = datetime.now().isoformat()
        try:
            # Create a serializable copy of the data
            serializable_data = {}
            for key, value in self.checkpoint_data.items():
                if key == 'symbol_inventory' and value:
                    # Convert date objects to strings in inventory
                    serializable_inventory = {}
                    for symbol, info in value.items():
                        serializable_info = info.copy()
                        if 'min_date' in serializable_info and serializable_info['min_date']:
                            serializable_info['min_date'] = serializable_info['min_date'].isoformat()
                        if 'max_date' in serializable_info and serializable_info['max_date']:
                            serializable_info['max_date'] = serializable_info['max_date'].isoformat()
                        serializable_inventory[symbol] = serializable_info
                    serializable_data[key] = serializable_inventory
                else:
                    serializable_data[key] = value
            
            with open(self.checkpoint_file, 'w') as f:
                json.dump(serializable_data, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Failed to save checkpoint: {e}")
    
    def build_symbol_inventory(self, asset_type: str = 'stock') -> Dict[str, Dict]:
        """Build inventory of all available symbols from zip files"""
        logger.info("Building symbol inventory from zip files...")
        
        # Use the adapter's built-in method
        return self.adapter.get_symbol_inventory(asset_type)
    
    def get_monthly_ranges(self, start_date: date, end_date: date) -> List[tuple]:
        """Generate list of (year, month) tuples for date range"""
        ranges = []
        current = start_date.replace(day=1)  # Start of month
        
        while current <= end_date:
            # End of month
            if current.month == 12:
                month_end = current.replace(year=current.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                month_end = current.replace(month=current.month + 1, day=1) - timedelta(days=1)
            
            # Don't go past the actual end date
            month_end = min(month_end, end_date)
            
            ranges.append((current.year, current.month, current, month_end))
            
            # Move to next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
                
        return ranges
    
    async def process_symbol_month(
        self, 
        symbol: str, 
        year: int, 
        month: int, 
        zip_file_path: str
    ) -> int:
        """Process one month of data for a symbol"""
        month_key = f"{year}-{month:02d}"
        
        # Check if already completed
        if (symbol in self.checkpoint_data['completed_months'] and 
            month_key in self.checkpoint_data['completed_months'][symbol]):
            return 0  # Already processed
        
        try:
            # Get ticks for this month
            ticks = []
            month_start = date(year, month, 1)
            
            # Get next month for end boundary
            if month == 12:
                month_end = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                month_end = date(year, month + 1, 1) - timedelta(days=1)
            
            # Extract ticks from adapter
            zip_file = Path(zip_file_path)
            tick_generator = self.adapter.process_minute_data_from_zip(
                zip_file, symbol, month_start, month_end
            )
            
            for tick in tick_generator:
                ticks.append(tick)
            
            if not ticks:
                logger.debug(f"   No data for {symbol} {month_key}")
                return 0
            
            # Store using FileBasedMinuteManager  
            if ticks:
                from storage.file_based_minute_manager import MinuteBar
                
                minute_bars = []
                for tick in ticks:
                    bar = MinuteBar(
                        symbol=tick.symbol,
                        timestamp=tick.timestamp,
                        open=tick.open,
                        high=tick.high,
                        low=tick.low,
                        close=tick.close,
                        volume=tick.volume,
                        vendor="firstrate"
                    )
                    minute_bars.append(bar)
                
                await self.minute_manager.store_minute_data(minute_bars)
                records_written = len(minute_bars)
            
            # Mark month as completed
            if symbol not in self.checkpoint_data['completed_months']:
                self.checkpoint_data['completed_months'][symbol] = []
            self.checkpoint_data['completed_months'][symbol].append(month_key)
            
            # Update stats
            self.checkpoint_data['processing_stats']['months_processed'] += 1
            self.checkpoint_data['processing_stats']['records_written'] += records_written
            
            logger.info(f"✅ {symbol} {month_key}: {records_written:,} records")
            return records_written
            
        except Exception as e:
            logger.error(f"❌ {symbol} {month_key} failed: {e}")
            
            # Track failed month
            if symbol not in self.checkpoint_data['failed_months']:
                self.checkpoint_data['failed_months'][symbol] = []
            self.checkpoint_data['failed_months'][symbol].append(month_key)
            
            self.checkpoint_data['processing_stats']['errors'] += 1
            return 0
    
    async def process_symbol(self, symbol: str, symbol_info: Dict) -> int:
        """Process all months for a single symbol"""
        logger.info(f"Processing symbol: {symbol}")
        
        # Parse date range from symbol info
        start_date = symbol_info['min_date']
        end_date = symbol_info['max_date']
        
        if not start_date or not end_date:
            logger.warning(f"⚠️ {symbol}: Missing date range, skipping")
            return 0
        
        # Get monthly ranges
        monthly_ranges = self.get_monthly_ranges(start_date, end_date)
        
        logger.info(f"{symbol}: Processing {len(monthly_ranges)} months from {start_date} to {end_date}")
        
        total_records = 0
        for year, month, month_start, month_end in monthly_ranges:
            records = await self.process_symbol_month(
                symbol, year, month, symbol_info['zip_file']
            )
            total_records += records
            
            # Save checkpoint periodically
            if self.checkpoint_data['processing_stats']['months_processed'] % 10 == 0:
                self.save_checkpoint()
        
        # Mark symbol as completed
        self.checkpoint_data['processing_stats']['symbols_processed'] += 1
        logger.info(f"✅ {symbol} complete: {len(monthly_ranges)}/{len(monthly_ranges)} months, {total_records:,} records")
        
        return total_records
    
    async def run_backfill(
        self, 
        asset_type: str = 'stock', 
        symbols: List[str] = None,
        limit: int = None,
        resume: bool = False
    ) -> Dict:
        """Run the complete backfill process"""
        logger.info("🚀 Starting FirstRate minute bar backfill")
        logger.info(f"📊 Asset type: {asset_type}")
        logger.info(f"💾 Output path: {self.output_path}")
        logger.info(f"📝 Checkpoint file: {self.checkpoint_file}")
        
        # Build symbol inventory
        if not resume or not self.checkpoint_data.get('symbol_inventory'):
            symbol_inventory = self.build_symbol_inventory(asset_type)
            self.checkpoint_data['symbol_inventory'] = symbol_inventory
        else:
            symbol_inventory = self.checkpoint_data['symbol_inventory']
        
        # Filter symbols if specified
        if symbols:
            symbol_inventory = {
                sym: info for sym, info in symbol_inventory.items() 
                if sym in symbols
            }
            logger.info(f"🎯 Filtered to {len(symbol_inventory)} requested symbols")
        
        # Apply limit if specified
        if limit:
            symbol_inventory = dict(list(symbol_inventory.items())[:limit])
            logger.info(f"🔢 Limited to first {len(symbol_inventory)} symbols")
        
        self.checkpoint_data['total_symbols'] = len(symbol_inventory)
        
        # Process each symbol
        start_time = time.time()
        total_records = 0
        
        for i, (symbol, info) in enumerate(symbol_inventory.items(), 1):
            # Skip if already completed (for resume)
            if (resume and symbol in self.checkpoint_data['completed_months'] and 
                len(self.checkpoint_data['completed_months'][symbol]) > 0):
                logger.info(f"⏭️ Skipping {symbol} (already processed)")
                continue
            
            logger.info(f"🔄 Progress: {i}/{len(symbol_inventory)} symbols")
            
            try:
                records = await self.process_symbol(symbol, info)
                total_records += records
                
                # Save checkpoint after each symbol
                self.save_checkpoint()
                
            except Exception as e:
                logger.error(f"❌ Failed to process {symbol}: {e}")
                self.checkpoint_data['processing_stats']['errors'] += 1
                continue
        
        # Final statistics
        elapsed_time = time.time() - start_time
        stats = self.checkpoint_data['processing_stats']
        
        logger.info("🎉 FirstRate backfill completed!")
        logger.info(f"📊 Symbols processed: {stats['symbols_processed']}")
        logger.info(f"📅 Months processed: {stats['months_processed']}")
        logger.info(f"📝 Records written: {stats['records_written']:,}")
        logger.info(f"❌ Errors: {stats['errors']}")
        logger.info(f"⏱️ Total time: {elapsed_time:.1f} seconds")
        
        self.save_checkpoint()
        return self.checkpoint_data


def main():
    """Command line interface"""
    parser = argparse.ArgumentParser(description="FirstRate minute bar backfill processor")
    parser.add_argument("--asset-type", default="stock", 
                        choices=['stock', 'etf', 'fx', 'index'],
                        help="Asset type to process")
    parser.add_argument("--data-path", default="/data/firstrate-data",
                        help="Path to FirstRate data directory")
    parser.add_argument("--output-path", default="/data/minute-bars/firstrate",
                        help="Output directory for processed files")
    parser.add_argument("--checkpoint-file", default="firstrate_monthly_production.json",
                        help="Checkpoint file for resumable processing")
    parser.add_argument("--symbols", type=str,
                        help="Specific symbols to process (comma-separated)")
    parser.add_argument("--limit", type=int,
                        help="Limit number of symbols for testing")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from existing checkpoint")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'firstrate_backfill_{datetime.now().strftime("%Y%m%d")}.log')
        ]
    )
    
    # Parse symbols
    symbols = None
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',')]
    
    # Create processor
    processor = FirstRateBackfillProcessor(
        data_path=args.data_path,
        output_path=args.output_path,
        checkpoint_file=args.checkpoint_file
    )
    
    # Run backfill
    try:
        result = asyncio.run(processor.run_backfill(
            asset_type=args.asset_type,
            symbols=symbols,
            limit=args.limit,
            resume=args.resume
        ))
        
        print(f"\n✅ Backfill completed successfully!")
        print(f"📊 Final stats: {result['processing_stats']}")
        
    except KeyboardInterrupt:
        print("\n🛑 Backfill interrupted by user")
        processor.save_checkpoint()
        print("💾 Checkpoint saved - use --resume to continue")
        
    except Exception as e:
        print(f"\n❌ Backfill failed: {e}")
        processor.save_checkpoint()
        raise


if __name__ == "__main__":
    main()