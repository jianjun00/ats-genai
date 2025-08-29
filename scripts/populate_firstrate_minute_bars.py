#!/usr/bin/env python3
"""
FirstRate Monthly Minute Bar Backfill Script

Processes FirstRate historical minute data from zip files and stores them
in monthly Parquet files under /mnt/d/ats-data/minute-bars/firstrate/

Features:
- Processes data by month for memory efficiency
- EDT to UTC timezone conversion
- Checkpoint-based resumable processing
- Parallel processing of multiple symbols
- Progress tracking and logging
"""

import os
import sys
import argparse
import asyncio
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import logging
from dataclasses import asdict

from market_data.agent.firstrate_adapter import FirstRateAdapter
from storage.file_based_minute_manager import FileBasedMinuteManager, MinuteBar
from config.environment import Environment

logger = logging.getLogger(__name__)


class FirstRateBackfillProcessor:
    """Manages the monthly backfill process for FirstRate data."""
    
    def __init__(
        self, 
        data_path: str = "/mnt/d/ats-data/firstrate-data",
        output_path: str = "/mnt/d/ats-data/minute-bars/firstrate",
        checkpoint_file: str = "firstrate_backfill_checkpoint.json",
        asset_type: str = "stock"
    ):
        self.adapter = FirstRateAdapter(data_path)
        self.storage_manager = FileBasedMinuteManager(base_path=output_path)
        self.checkpoint_file = Path(checkpoint_file)
        self.asset_type = asset_type
        
        # Processing state
        self.checkpoint = self._load_checkpoint()
        self.stats = {
            'symbols_processed': 0,
            'months_processed': 0,
            'records_written': 0,
            'errors': 0,
            'start_time': None,
            'last_checkpoint_time': None
        }
        
    def _load_checkpoint(self) -> Dict:
        """Load processing checkpoint."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    logger.info(f"Loaded checkpoint: {len(checkpoint.get('completed_months', {}))} months completed")
                    return checkpoint
            except Exception as e:
                logger.error(f"Error loading checkpoint: {e}")
        
        return {
            'completed_months': {},  # {symbol: [list of completed YYYY-MM]}
            'failed_months': {},     # {symbol: [list of failed YYYY-MM]}
            'last_processed': None,
            'total_symbols': 0,
            'processing_stats': {}
        }
    
    def _save_checkpoint(self):
        """Save current processing state."""
        self.checkpoint['processing_stats'] = self.stats
        self.checkpoint['last_processed'] = datetime.now().isoformat()
        
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.checkpoint, f, indent=2)
            self.stats['last_checkpoint_time'] = datetime.now()
            logger.debug("Checkpoint saved")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")
    
    def get_symbol_inventory(self) -> Dict[str, Dict]:
        """Get inventory of all available symbols."""
        logger.info("Building symbol inventory from zip files...")
        inventory = self.adapter.get_symbol_inventory(self.asset_type)
        logger.info(f"Found {len(inventory)} symbols across all zip files")
        
        # Log some sample symbols and date ranges
        sample_symbols = list(inventory.items())[:5]
        for symbol, info in sample_symbols:
            logger.info(f"  {symbol}: {info['min_date']} to {info['max_date']} ({info['total_files']} files)")
        
        return inventory
    
    def generate_monthly_date_ranges(self, start_date: date, end_date: date) -> List[tuple[date, date]]:
        """Generate list of (month_start, month_end) tuples."""
        months = []
        current = start_date.replace(day=1)  # First day of month
        
        while current <= end_date:
            # Last day of current month
            if current.month == 12:
                next_month = current.replace(year=current.year + 1, month=1)
            else:
                next_month = current.replace(month=current.month + 1)
            
            month_end = next_month - timedelta(days=1)
            
            # Don't go beyond end_date
            if month_end > end_date:
                month_end = end_date
            
            months.append((current, month_end))
            current = next_month
            
            if current > end_date:
                break
                
        return months
    
    async def process_symbol_month(
        self,
        symbol: str,
        zip_files: List[str],
        month_start: date,
        month_end: date
    ) -> Dict:
        """Process one symbol for one month."""
        month_key = month_start.strftime('%Y-%m')
        
        try:
            logger.debug(f"Processing {symbol} for {month_key}")
            
            # Collect all ticks for this month from all relevant zip files
            all_ticks = []
            for zip_file in zip_files:
                zip_path = Path(zip_file)
                if not zip_path.exists():
                    continue
                
                ticks = list(self.adapter.process_minute_data_from_zip(
                    zip_path, symbol, month_start, month_end
                ))
                all_ticks.extend(ticks)
            
            if not all_ticks:
                logger.debug(f"No data for {symbol} in {month_key}")
                return {'success': True, 'records': 0, 'month': month_key}
            
            # Convert to MinuteBar objects
            minute_bars = []
            for tick in all_ticks:
                minute_bar = MinuteBar(
                    symbol=tick.symbol,
                    timestamp=tick.timestamp,
                    open=tick.open,
                    high=tick.high,
                    low=tick.low,
                    close=tick.close,
                    volume=tick.volume,
                    vendor=self.adapter.vendor_name
                )
                minute_bars.append(minute_bar)
            
            if not minute_bars:
                return {'success': True, 'records': 0, 'month': month_key}
            
            # Sort by timestamp
            minute_bars.sort(key=lambda x: x.timestamp)
            
            # Store using FileBasedMinuteManager
            result = await self.storage_manager.store_minute_data(
                symbol=symbol,
                bars=minute_bars,
                overlap_strategy='merge'
            )
            
            records_written = result.get('records_stored', len(minute_bars))
            
            logger.debug(f"Stored {records_written} records for {symbol} in {month_key}")
            
            return {
                'success': True,
                'records': records_written,
                'month': month_key,
                'date_range': f"{minute_bars[0].timestamp} to {minute_bars[-1].timestamp}"
            }
            
        except Exception as e:
            logger.error(f"Error processing {symbol} for {month_key}: {e}")
            return {
                'success': False,
                'error': str(e),
                'month': month_key
            }
    
    async def process_symbol(self, symbol: str, symbol_info: Dict) -> Dict:
        """Process all months for a single symbol."""
        logger.info(f"Processing symbol: {symbol}")
        
        # Get date range for this symbol
        symbol_start = symbol_info['min_date']
        symbol_end = symbol_info['max_date']
        
        if not symbol_start or not symbol_end:
            logger.warning(f"No date range available for {symbol}")
            return {'success': False, 'error': 'No date range available'}
        
        # Generate monthly ranges
        monthly_ranges = self.generate_monthly_date_ranges(symbol_start, symbol_end)
        logger.info(f"{symbol}: Processing {len(monthly_ranges)} months from {symbol_start} to {symbol_end}")
        
        symbol_stats = {
            'months_total': len(monthly_ranges),
            'months_completed': 0,
            'months_failed': 0,
            'total_records': 0
        }
        
        # Process each month
        for month_start, month_end in monthly_ranges:
            month_key = month_start.strftime('%Y-%m')
            
            # Check if already completed
            completed_months = self.checkpoint['completed_months'].get(symbol, [])
            if month_key in completed_months:
                logger.debug(f"Skipping {symbol} {month_key} (already completed)")
                symbol_stats['months_completed'] += 1
                continue
            
            # Process this month
            result = await self.process_symbol_month(
                symbol,
                symbol_info['zip_files'],
                month_start,
                month_end
            )
            
            if result['success']:
                # Mark as completed
                if symbol not in self.checkpoint['completed_months']:
                    self.checkpoint['completed_months'][symbol] = []
                self.checkpoint['completed_months'][symbol].append(month_key)
                
                symbol_stats['months_completed'] += 1
                symbol_stats['total_records'] += result['records']
                self.stats['records_written'] += result['records']
                
                logger.info(f"✅ {symbol} {month_key}: {result['records']} records")
                
            else:
                # Mark as failed
                if symbol not in self.checkpoint['failed_months']:
                    self.checkpoint['failed_months'][symbol] = []
                self.checkpoint['failed_months'][symbol].append(month_key)
                
                symbol_stats['months_failed'] += 1
                self.stats['errors'] += 1
                
                logger.error(f"❌ {symbol} {month_key}: {result.get('error', 'Unknown error')}")
        
        self.stats['months_processed'] += symbol_stats['months_completed']
        return symbol_stats
    
    async def run_backfill(
        self,
        symbols: Optional[List[str]] = None,
        limit: Optional[int] = None,
        resume: bool = True
    ):
        """Run the complete backfill process."""
        self.stats['start_time'] = datetime.now()
        run_id = f"firstrate_backfill_{self.stats['start_time'].strftime('%Y%m%d_%H%M%S')}"
        
        logger.info("🚀 Starting FirstRate minute bar backfill")
        logger.info(f"📊 Asset type: {self.asset_type}")
        logger.info(f"💾 Output path: {self.storage_manager.base_path}")
        logger.info(f"📝 Checkpoint file: {self.checkpoint_file}")
        
        # Get symbol inventory
        inventory = self.get_symbol_inventory()
        
        # Filter symbols if specified
        if symbols:
            inventory = {k: v for k, v in inventory.items() if k in symbols}
            logger.info(f"Filtering to {len(inventory)} specified symbols")
        
        # Apply limit
        if limit:
            inventory_items = list(inventory.items())[:limit]
            inventory = dict(inventory_items)
            logger.info(f"Limited to first {len(inventory)} symbols")
        
        self.checkpoint['total_symbols'] = len(inventory)
        
        # Process symbols
        for i, (symbol, symbol_info) in enumerate(inventory.items(), 1):
            logger.info(f"🔄 Progress: {i}/{len(inventory)} symbols")
            
            try:
                symbol_stats = await self.process_symbol(symbol, symbol_info)
                self.stats['symbols_processed'] += 1
                
                logger.info(f"✅ {symbol} complete: {symbol_stats['months_completed']}/{symbol_stats['months_total']} months, {symbol_stats['total_records']} records")
                
                # Save checkpoint periodically
                if i % 10 == 0:
                    self._save_checkpoint()
                    
            except Exception as e:
                logger.error(f"❌ Failed to process {symbol}: {e}")
                self.stats['errors'] += 1
        
        # Final checkpoint
        self._save_checkpoint()
        
        # Final statistics
        duration = datetime.now() - self.stats['start_time']
        logger.info("🎉 FirstRate backfill completed!")
        logger.info(f"📊 Symbols processed: {self.stats['symbols_processed']}")
        logger.info(f"📊 Months processed: {self.stats['months_processed']}")
        logger.info(f"📊 Records written: {self.stats['records_written']:,}")
        logger.info(f"📊 Errors: {self.stats['errors']}")
        logger.info(f"⏱️ Duration: {duration}")


def main():
    parser = argparse.ArgumentParser(description="FirstRate Minute Bar Backfill")
    parser.add_argument("--data-path", default="/mnt/d/ats-data/firstrate-data", 
                       help="Path to FirstRate data directory")
    parser.add_argument("--output-path", default="/mnt/d/ats-data/minute-bars/firstrate",
                       help="Output path for processed minute bars")
    parser.add_argument("--checkpoint-file", default="firstrate_backfill_checkpoint.json",
                       help="Checkpoint file for resumable processing")
    parser.add_argument("--asset-type", default="stock", choices=["stock", "etf", "fx", "index"],
                       help="Asset type to process")
    parser.add_argument("--symbols", nargs="+", help="Specific symbols to process")
    parser.add_argument("--limit", type=int, help="Limit number of symbols to process")
    parser.add_argument("--resume", action="store_true", default=True,
                       help="Resume from checkpoint")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create output directory
    output_path = Path(args.output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Create and run processor
    processor = FirstRateBackfillProcessor(
        data_path=args.data_path,
        output_path=args.output_path,
        checkpoint_file=args.checkpoint_file,
        asset_type=args.asset_type
    )
    
    try:
        asyncio.run(processor.run_backfill(
            symbols=args.symbols,
            limit=args.limit,
            resume=args.resume
        ))
    except KeyboardInterrupt:
        logger.info("⏸️ Backfill interrupted by user")
        processor._save_checkpoint()
    except Exception as e:
        logger.error(f"💥 Backfill failed: {e}")
        processor._save_checkpoint()
        raise


if __name__ == "__main__":
    main()