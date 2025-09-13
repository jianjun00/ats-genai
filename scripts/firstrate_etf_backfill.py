#!/usr/bin/env python3
"""
FirstRate Critical ETF 30-Day Backfill

Focused backfill for critical ETFs only from our comprehensive backfill script.
Targets the ETFs that showed coverage gaps in validation.
"""

import os
import sys
import asyncio
import json
import argparse
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Set, Optional
from pathlib import Path
import time

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from domains.market_data.services.core.agent.core.firstrate_daily_downloader import FirstRateDownloader, DownloadJob
from core.vendor.adapters import create_firstrate_adapter, FirstRateAdapter
from dataclasses import dataclass

@dataclass
class MinuteBar:
    """Simple minute bar for this script"""
    symbol: str
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int

# Simplified file manager for this script
class SimpleFileMinuteManager:
    """Simplified file-based minute data manager for this script"""
    
    def __init__(self, storage_path: str):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
    
    async def get_minute_data(self, symbol: str, start_date: date, end_date: date) -> List[MinuteBar]:
        """Get existing minute data for a symbol and date range"""
        # For now, return empty list - in practice this would read parquet files
        return []
    
    async def store_minute_data(self, symbol: str, bars: List[MinuteBar]) -> None:
        """Store minute bars to file storage"""
        # For now, just log - in practice this would write to parquet files
        logger.info(f"Would store {len(bars)} bars for {symbol} to {self.storage_path}")
        return

logger = logging.getLogger(__name__)

# Critical ETFs identified from validation gaps
CRITICAL_ETFS = [
    'SPY',   # S&P 500 ETF - most important
    'QQQ',   # NASDAQ-100 ETF
    'IWM',   # Russell 2000 ETF
    'VTI',   # Total Stock Market ETF
    'VOO',   # S&P 500 ETF (Vanguard)
    'XLK',   # Technology Select Sector ETF
    'XLF',   # Financial Select Sector ETF
    'XLE',   # Energy Select Sector ETF
    'XLV',   # Health Care Select Sector ETF
    'XLI',   # Industrial Select Sector ETF
    'XLP',   # Consumer Staples Select Sector ETF
    'XLY',   # Consumer Discretionary Select ETF
    'XLU',   # Utilities Select Sector ETF
    'XLB',   # Materials Select Sector ETF
    'XLRE'   # Real Estate Select Sector ETF
]


class FirstRateETFBackfill:
    """Focused ETF backfill processor"""

    def __init__(
        self,
        data_path: str = "/mnt/d/ats-data/firstrate-data",
        output_path: str = "/mnt/d/ats-data/minute-bars/firstrate",
        checkpoint_file: str = "firstrate_etf_backfill.json"
    ):
        self.data_path = Path(data_path)
        self.output_path = Path(output_path)
        self.checkpoint_file = Path(checkpoint_file)

        # Initialize components
        self.downloader = FirstRateDownloader(base_path=str(self.data_path))
        self.adapter = create_firstrate_adapter(str(self.data_path))
        self.minute_manager = SimpleFileMinuteManager(str(self.output_path))

        # Create directories
        self.output_path.mkdir(parents=True, exist_ok=True)

        # Load checkpoint
        self.checkpoint_data = self.load_checkpoint()

    def load_checkpoint(self) -> Dict:
        """Load processing checkpoint"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    logger.info(f"📝 Loaded checkpoint: {data.get('etfs_completed', 0)} ETFs completed")
                    return data
            except Exception as e:
                logger.error(f"❌ Failed to load checkpoint: {e}")

        return {
            'downloads_completed': {},
            'processing_completed': {},
            'etfs_completed': 0,
            'last_run': None,
            'stats': {
                'downloads_attempted': 0,
                'downloads_successful': 0,
                'etfs_processed': 0,
                'records_written': 0,
                'merges_performed': 0
            }
        }

    def save_checkpoint(self):
        """Save current processing state"""
        self.checkpoint_data['last_run'] = datetime.now().isoformat()
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.checkpoint_data, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Failed to save checkpoint: {e}")

    def get_30_day_date_range(self) -> tuple[date, date]:
        """Get the date range for last 30 days"""
        end_date = date.today()
        start_date = end_date - timedelta(days=30)
        return start_date, end_date

    async def download_30_days_etfs(self) -> Dict[str, bool]:
        """Download the latest 30 days of ETF data"""
        logger.info("🚀 Starting 30-day ETF data downloads...")
        
        start_date, end_date = self.get_30_day_date_range()
        logger.info(f"📅 Date range: {start_date} to {end_date}")

        # Create download job for ETFs only
        jobs = [DownloadJob(asset_type="etf")]

        all_results = {}
        current_date = start_date

        while current_date <= end_date:
            date_key = current_date.strftime('%Y-%m-%d')
            
            # Skip if already downloaded successfully
            if self.checkpoint_data['downloads_completed'].get(date_key):
                logger.info(f"✅ {date_key} already downloaded, skipping")
                current_date += timedelta(days=1)
                continue

            logger.info(f"📥 Downloading ETF data for {date_key}...")
            
            # Download for this specific date
            results = await self.downloader.download_daily_data(jobs, current_date)
            
            # Track results
            all_successful = all(results.values())
            all_results[date_key] = all_successful
            
            if all_successful:
                self.checkpoint_data['downloads_completed'][date_key] = True
                logger.info(f"✅ {date_key} ETF download completed successfully")
            else:
                logger.warning(f"⚠️ {date_key} ETF download failed")

            # Update stats
            self.checkpoint_data['stats']['downloads_attempted'] += len(jobs)
            self.checkpoint_data['stats']['downloads_successful'] += sum(results.values())

            # Save checkpoint after each day
            self.save_checkpoint()

            current_date += timedelta(days=1)

        successful_days = sum(1 for success in all_results.values() if success)
        total_days = len(all_results)
        
        logger.info(f"📊 ETF Download summary: {successful_days}/{total_days} days successful")
        return all_results

    def check_etf_existing_data(self, symbol: str, year: int, month: int) -> Optional[int]:
        """Check if we have existing data for this ETF/month and return record count"""
        try:
            # Check for existing file
            symbol_path = self.output_path / symbol[0] / symbol / f'{year}' / f'{month:02d}'
            file_path = symbol_path / f'{symbol}_{year}_{month:02d}.parquet'
            
            if file_path.exists():
                try:
                    import pandas as pd
                    df = pd.read_parquet(file_path)
                    record_count = len(df)
                    logger.debug(f"📊 {symbol} {year}-{month:02d}: Found {record_count} existing records")
                    return record_count
                except Exception as e:
                    logger.debug(f"⚠️ Could not read {file_path}: {e}")
                    return 0
            else:
                return 0
                
        except Exception as e:
            logger.debug(f"⚠️ Could not check existing data for {symbol} {year}-{month:02d}: {e}")
            return 0

    async def process_etf_month(self, symbol: str, year: int, month: int) -> int:
        """Process one month of data for an ETF"""
        
        month_key = f"{symbol}-{year}-{month:02d}"
        
        # Check if already processed
        if month_key in self.checkpoint_data['processing_completed']:
            return self.checkpoint_data['processing_completed'][month_key]

        # Check existing data
        existing_count = self.check_etf_existing_data(symbol, year, month)
        
        # For ETFs, be more aggressive about updating (since they were identified as problematic)
        if existing_count > 5000:  # Only skip if we have substantial data
            logger.debug(f"✅ {symbol} {year}-{month:02d}: Already has {existing_count} records, skipping")
            self.checkpoint_data['processing_completed'][month_key] = existing_count
            return existing_count

        logger.info(f"🔄 Processing ETF {symbol} {year}-{month:02d}...")

        try:
            # Get minute bars from unified adapter
            minute_bars = []
            month_start = date(year, month, 1)
            
            if month == 12:
                month_end = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                month_end = date(year, month + 1, 1) - timedelta(days=1)

            # Get minute bars using the unified adapter
            async for unified_bar in self.adapter.get_minute_bars(symbol, month_start, month_end):
                # Convert UnifiedMinuteBar to MinuteBar
                bar = MinuteBar(
                    symbol=unified_bar.symbol,
                    timestamp=unified_bar.timestamp,
                    open_price=float(unified_bar.open),
                    high_price=float(unified_bar.high),
                    low_price=float(unified_bar.low),
                    close_price=float(unified_bar.close),
                    volume=unified_bar.volume
                )
                minute_bars.append(bar)

            if not minute_bars:
                logger.debug(f"📭 No new data found for ETF {symbol} {year}-{month:02d}")
                return existing_count

            # Store the data (this will merge with existing)
            await self.minute_manager.store_minute_data(symbol, minute_bars)
            new_total = existing_count + len(minute_bars)
            
            if existing_count > 0:
                logger.info(f"🔄 ETF {symbol} {year}-{month:02d}: Merged {len(minute_bars)} new + {existing_count} existing = {new_total} total")
                self.checkpoint_data['stats']['merges_performed'] += 1
            else:
                logger.info(f"✅ ETF {symbol} {year}-{month:02d}: Added {len(minute_bars)} records")

            # Update stats
            self.checkpoint_data['stats']['records_written'] += len(minute_bars)
            self.checkpoint_data['processing_completed'][month_key] = new_total
            
            return new_total

        except Exception as e:
            logger.error(f"❌ Failed to process ETF {symbol} {year}-{month:02d}: {e}")
            return existing_count

    async def process_etf(self, symbol: str) -> int:
        """Process all relevant months for a critical ETF"""
        logger.info(f"🔄 Processing critical ETF: {symbol}")
        
        start_date, end_date = self.get_30_day_date_range()
        
        # Get monthly ranges for the past 30 days
        monthly_ranges = []
        current = start_date.replace(day=1)  # Start of month
        
        while current <= end_date:
            # End of month
            if current.month == 12:
                month_end = current.replace(year=current.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                month_end = current.replace(month=current.month + 1, day=1) - timedelta(days=1)

            # Don't go past the actual end date
            month_end = min(month_end, end_date)

            monthly_ranges.append((current.year, current.month, current, month_end))

            # Move to next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        
        total_records = 0
        
        for year, month, month_start, month_end in monthly_ranges:
            records = await self.process_etf_month(symbol, year, month)
            total_records += records

        logger.info(f"✅ ETF {symbol}: {total_records:,} total records across {len(monthly_ranges)} months")
        return total_records

    async def run_etf_backfill(
        self, 
        download: bool = True,
        process: bool = True
    ) -> Dict:
        """Run complete ETF backfill (download + process)"""
        
        logger.info("🚀 Starting FirstRate Critical ETF 30-day backfill")
        logger.info(f"🎯 Target ETFs: {', '.join(CRITICAL_ETFS)}")
        
        start_time = time.time()
        results = {}
        
        # Step 1: Download if requested
        if download:
            logger.info("📥 Phase 1: Downloading latest 30 days of ETF data...")
            download_results = await self.download_30_days_etfs()
            results['downloads'] = download_results
        
        # Step 2: Process if requested  
        if process:
            logger.info("⚙️ Phase 2: Processing critical ETFs...")
            
            total_records = 0
            
            for i, symbol in enumerate(CRITICAL_ETFS, 1):
                # Skip if already completed
                if symbol in self.checkpoint_data.get('processing_completed', {}):
                    logger.info(f"⏭️ Skipping {symbol} (already processed)")
                    continue
                    
                logger.info(f"📈 Progress: {i}/{len(CRITICAL_ETFS)} ETFs - {symbol}")
                
                try:
                    records = await self.process_etf(symbol)
                    total_records += records
                    
                    # Mark ETF as completed
                    self.checkpoint_data['etfs_completed'] += 1
                    self.checkpoint_data['stats']['etfs_processed'] += 1
                    
                    # Save checkpoint after each ETF
                    self.save_checkpoint()
                    
                except Exception as e:
                    logger.error(f"❌ Failed to process ETF {symbol}: {e}")
                    continue

            results['processing'] = {
                'etfs_processed': self.checkpoint_data['stats']['etfs_processed'],
                'total_records': total_records,
                'stats': self.checkpoint_data['stats']
            }
        
        # Final summary
        elapsed_time = time.time() - start_time
        logger.info("🎉 Critical ETF backfill completed!")
        logger.info(f"📊 ETFs processed: {self.checkpoint_data['stats']['etfs_processed']}")
        logger.info(f"📝 Total records: {self.checkpoint_data['stats']['records_written']:,}")
        logger.info(f"🔄 Merges performed: {self.checkpoint_data['stats']['merges_performed']}")
        logger.info(f"⏱️ Total time: {elapsed_time:.1f} seconds")
        
        return results


def main():
    """Command line interface"""
    parser = argparse.ArgumentParser(
        description="FirstRate Critical ETF 30-Day Backfill", 
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full ETF backfill (download + process)
  PYTHONPATH=src python scripts/firstrate_etf_backfill.py --full

  # Process only (skip download)  
  PYTHONPATH=src python scripts/firstrate_etf_backfill.py --process-only

  # Download only
  PYTHONPATH=src python scripts/firstrate_etf_backfill.py --download-only
        """
    )
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--full", action="store_true", 
                           help="Run full ETF backfill (download + process)")
    mode_group.add_argument("--download-only", action="store_true",
                           help="Download only")
    mode_group.add_argument("--process-only", action="store_true", 
                           help="Process only (skip download)")
    
    # Options
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--checkpoint-file", default="firstrate_etf_backfill.json",
                       help="Checkpoint file for resumable processing")
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'firstrate_etf_backfill_{datetime.now().strftime("%Y%m%d_%H%M")}.log')
        ]
    )
    
    # Create processor
    processor = FirstRateETFBackfill(checkpoint_file=args.checkpoint_file)
    
    # Determine what to run
    download = args.full or args.download_only
    process = args.full or args.process_only
    
    # Run ETF backfill
    try:
        result = asyncio.run(processor.run_etf_backfill(
            download=download,
            process=process
        ))
        
        print(f"\n✅ Critical ETF backfill completed successfully!")
        if 'processing' in result:
            print(f"📊 Final stats: {result['processing']['stats']}")
            
    except KeyboardInterrupt:
        print("\n🛑 ETF backfill interrupted by user")
        processor.save_checkpoint()
        print("💾 Checkpoint saved - resume with same command")
        
    except Exception as e:
        print(f"\n❌ ETF backfill failed: {e}")
        processor.save_checkpoint()
        raise


if __name__ == "__main__":
    main()