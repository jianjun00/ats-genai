#!/usr/bin/env python3
"""
FirstRate 30-Day Complete Backfill Script

Downloads and processes the latest 30 days of minute bar data for all stocks and critical ETFs.
Handles merging with incomplete previous month data and ensures current month coverage.

Features:
- Downloads latest 30 days from FirstRate API
- Processes all stocks and critical ETFs
- Merges incomplete previous month data with new downloads
- Populates current month data
- Checkpoint-based resumable processing

Usage:
    # Full 30-day backfill (download + process)
    PYTHONPATH=src python -m domains.workflow.firstrate.backfill.thirty_day_backfill --full

    # Process only (skip download)
    PYTHONPATH=src python -m domains.workflow.firstrate.backfill.thirty_day_backfill --process-only

    # Debug mode with limited symbols
    PYTHONPATH=src python -m domains.workflow.firstrate.backfill.thirty_day_backfill --full --limit 10 --debug
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

class SimpleFileMinuteManager:
    """File-based minute data manager with parquet storage"""
    
    def __init__(self, storage_path: str):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
    
    async def get_minute_data(self, symbol: str, start_date: date, end_date: date) -> List[MinuteBar]:
        """Get existing minute data for a symbol and date range"""
        import pandas as pd
        
        bars = []
        year = start_date.year
        month = start_date.month
        
        symbol_dir = self.storage_path / symbol[0] / symbol / str(year) / f"{month:02d}"
        parquet_path = symbol_dir / f"{symbol}_{year}_{month:02d}.parquet"
        
        if parquet_path.exists():
            try:
                df = pd.read_parquet(parquet_path)
                for _, row in df.iterrows():
                    bars.append(MinuteBar(
                        symbol=row['symbol'],
                        timestamp=pd.to_datetime(row['timestamp']),
                        open_price=float(row['open']),
                        high_price=float(row['high']),
                        low_price=float(row['low']),
                        close_price=float(row['close']),
                        volume=int(row['volume'])
                    ))
            except Exception as e:
                logger.debug(f"Could not read existing data for {symbol}: {e}")
        
        return bars
    
    async def store_minute_data(self, symbol: str, bars: List[MinuteBar]) -> None:
        """Store minute bars to parquet files"""
        if not bars:
            return
        
        import pandas as pd
        
        records = []
        for bar in bars:
            records.append({
                'symbol': bar.symbol,
                'timestamp': bar.timestamp,
                'open': float(bar.open_price),
                'high': float(bar.high_price),
                'low': float(bar.low_price),
                'close': float(bar.close_price),
                'volume': int(bar.volume),
                'vendor': 'firstrate'
            })
        
        df = pd.DataFrame(records)
        
        year = bars[0].timestamp.year
        month = bars[0].timestamp.month
        
        symbol_dir = self.storage_path / symbol[0] / symbol / str(year) / f"{month:02d}"
        symbol_dir.mkdir(parents=True, exist_ok=True)
        
        parquet_path = symbol_dir / f"{symbol}_{year}_{month:02d}.parquet"
        df.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)
        
        logger.debug(f"Stored {len(bars)} bars for {symbol} to {parquet_path}")

logger = logging.getLogger(__name__)

CRITICAL_ETFS = [
    'SPY', 'QQQ', 'IWM', 'VTI', 'VOO', 'VEA', 'VWO', 'AGG', 'BND',
    'GLD', 'SLV', 'TLT', 'HYG', 'LQD', 'EFA', 'EEM', 'VNQ', 'XLF',
    'XLE', 'XLK', 'XLV', 'XLI', 'XLP', 'XLU', 'XLB', 'XLY', 'XLRE'
]


class FirstRate30DayBackfill:
    """Complete 30-day backfill processor with download and merge capabilities"""

    def __init__(
        self,
        data_path: str = "/mnt/d/ats-data/firstrate-data",
        output_path: str = "/mnt/d/ats-data/minute-bars/firstrate",
        checkpoint_file: str = "firstrate_30day_backfill.json"
    ):
        self.data_path = Path(data_path)
        self.output_path = Path(output_path)
        self.checkpoint_file = Path(checkpoint_file)

        self.downloader = FirstRateDownloader(base_path=str(self.data_path))
        self.adapter = create_firstrate_adapter(str(self.data_path))
        self.minute_manager = SimpleFileMinuteManager(str(self.output_path))

        self.output_path.mkdir(parents=True, exist_ok=True)

        self.checkpoint_data = self.load_checkpoint()

    def load_checkpoint(self) -> Dict:
        """Load processing checkpoint"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    logger.info(f"📝 Loaded checkpoint: {data.get('symbols_completed', 0)} symbols completed")
                    return data
            except Exception as e:
                logger.error(f"❌ Failed to load checkpoint: {e}")

        return {
            'downloads_completed': {},
            'processing_completed': {},
            'symbols_completed': 0,
            'last_run': None,
            'stats': {
                'downloads_attempted': 0,
                'downloads_successful': 0,
                'symbols_processed': 0,
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

    def get_30_day_date_range(self) -> tuple:
        """Get the date range for last 30 days"""
        end_date = date.today()
        start_date = end_date - timedelta(days=30)
        return start_date, end_date

    def _get_symbols_from_zip(self, zip_file_path: Path) -> List[str]:
        """Extract symbol list from a zip file"""
        symbols = []
        try:
            import zipfile
            with zipfile.ZipFile(zip_file_path, 'r') as zf:
                for filename in zf.namelist():
                    if filename.endswith('.csv') or filename.endswith('.txt'):
                        symbol = Path(filename).stem.upper()
                        if 1 <= len(symbol) <= 5 and symbol.isalpha():
                            symbols.append(symbol)
        except Exception as e:
            logger.debug(f"Error reading zip file {zip_file_path}: {e}")
        
        return symbols

    def get_monthly_ranges(self, start_date: date, end_date: date) -> List[tuple]:
        """Generate list of (year, month, start, end) tuples for date range"""
        ranges = []
        current = start_date.replace(day=1)

        while current <= end_date:
            if current.month == 12:
                month_end = current.replace(year=current.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                month_end = current.replace(month=current.month + 1, day=1) - timedelta(days=1)

            month_end = min(month_end, end_date)

            ranges.append((current.year, current.month, current, month_end))

            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        return ranges

    async def download_30_days(self) -> Dict[str, bool]:
        """Download the latest 30 days of data for stocks and ETFs"""
        logger.info("🚀 Starting 30-day data downloads...")
        
        start_date, end_date = self.get_30_day_date_range()
        logger.info(f"📅 Date range: {start_date} to {end_date}")

        jobs = [
            DownloadJob(asset_type='stock'),
            DownloadJob(asset_type='etf')
        ]

        results = {}
        for job in jobs:
            if job.asset_type in self.checkpoint_data['downloads_completed']:
                logger.info(f"⏭️  {job.asset_type} download already completed, skipping")
                results[job.asset_type] = True
                continue

            logger.info(f"📥 Downloading {job.asset_type} data...")
            try:
                success = await self.downloader.download_daily_data([job], download_date=None)
                results[job.asset_type] = success[job.asset_type] if success else False
                
                if results[job.asset_type]:
                    self.checkpoint_data['downloads_completed'][job.asset_type] = datetime.now().isoformat()
                    self.checkpoint_data['stats']['downloads_successful'] += 1
                    logger.info(f"✅ {job.asset_type} download completed")
                else:
                    logger.error(f"❌ {job.asset_type} download failed")
                
                self.checkpoint_data['stats']['downloads_attempted'] += 1
                self.save_checkpoint()

            except Exception as e:
                logger.error(f"❌ Error downloading {job.asset_type}: {e}")
                results[job.asset_type] = False

        return results

    async def get_symbols_to_process(self, limit: Optional[int] = None) -> List[str]:
        """Get list of symbols from downloaded data"""
        logger.info("📋 Building symbol list from downloaded data...")

        symbols = set()

        daily_dir = self.data_path / 'daily'
        if not daily_dir.exists():
            logger.error(f"❌ Daily download directory not found: {daily_dir}")
            return []

        stock_files = list(daily_dir.glob('stock_*.zip'))
        etf_files = list(daily_dir.glob('etf_*.zip'))

        logger.info(f"📁 Found {len(stock_files)} stock files, {len(etf_files)} ETF files")

        for zip_file in stock_files + etf_files:
            file_symbols = self._get_symbols_from_zip(zip_file)
            symbols.update(file_symbols)

        symbols.update(CRITICAL_ETFS)

        symbol_list = sorted(list(symbols))
        
        if limit:
            symbol_list = symbol_list[:limit]
            logger.info(f"🔢 Limited to {limit} symbols")

        logger.info(f"📊 Total symbols to process: {len(symbol_list)}")
        return symbol_list

    async def process_symbol_month(
        self,
        symbol: str,
        year: int,
        month: int,
        start_date: date,
        end_date: date
    ) -> int:
        """Process one month of data for a symbol"""
        month_key = f"{year}-{month:02d}"
        
        if symbol in self.checkpoint_data.get('processing_completed', {}) and \
           month_key in self.checkpoint_data['processing_completed'][symbol]:
            return 0

        try:
            existing_data = await self.minute_manager.get_minute_data(symbol, start_date, end_date)
            
            new_bars = []
            async for bar in self.adapter.get_minute_bars(symbol, start_date, end_date):
                new_bars.append(MinuteBar(
                    symbol=bar.symbol,
                    timestamp=bar.timestamp,
                    open_price=bar.open,
                    high_price=bar.high,
                    low_price=bar.low,
                    close_price=bar.close,
                    volume=bar.volume
                ))

            if new_bars:
                all_bars = existing_data + new_bars
                all_bars.sort(key=lambda x: x.timestamp)
                
                await self.minute_manager.store_minute_data(symbol, all_bars)
                
                if symbol not in self.checkpoint_data['processing_completed']:
                    self.checkpoint_data['processing_completed'][symbol] = []
                self.checkpoint_data['processing_completed'][symbol].append(month_key)
                
                self.checkpoint_data['stats']['records_written'] += len(new_bars)
                if existing_data:
                    self.checkpoint_data['stats']['merges_performed'] += 1
                
                logger.info(f"✅ {symbol} {month_key}: {len(new_bars)} new records, {len(all_bars)} total")
                return len(new_bars)

            return 0

        except Exception as e:
            logger.error(f"❌ {symbol} {month_key} failed: {e}")
            return 0

    async def process_all_symbols(
        self,
        symbols: List[str],
        start_date: date,
        end_date: date
    ) -> Dict:
        """Process all symbols for the date range"""
        logger.info(f"🚀 Processing {len(symbols)} symbols...")

        monthly_ranges = self.get_monthly_ranges(start_date, end_date)
        logger.info(f"📅 Processing {len(monthly_ranges)} month ranges")

        total_records = 0
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"🔄 Processing {symbol} ({i}/{len(symbols)})")

            symbol_records = 0
            for year, month, month_start, month_end in monthly_ranges:
                records = await self.process_symbol_month(
                    symbol, year, month, month_start, month_end
                )
                symbol_records += records

            total_records += symbol_records
            self.checkpoint_data['stats']['symbols_processed'] += 1
            self.checkpoint_data['symbols_completed'] += 1

            if (i % 10) == 0:
                self.save_checkpoint()

        self.save_checkpoint()

        return {
            'symbols_processed': len(symbols),
            'total_records': total_records
        }

    async def run_full_backfill(
        self,
        download: bool = True,
        limit: Optional[int] = None
    ) -> Dict:
        """Run complete 30-day backfill"""
        logger.info("🚀 Starting FirstRate 30-day backfill")
        logger.info(f"📂 Data path: {self.data_path}")
        logger.info(f"💾 Output path: {self.output_path}")

        start_time = time.time()

        if download:
            download_results = await self.download_30_days()
            logger.info(f"📊 Download results: {download_results}")
        else:
            logger.info("⏭️  Skipping download step")

        symbols = await self.get_symbols_to_process(limit=limit)
        
        start_date, end_date = self.get_30_day_date_range()
        
        process_results = await self.process_all_symbols(symbols, start_date, end_date)

        elapsed_time = time.time() - start_time

        logger.info("🎉 30-day backfill completed!")
        logger.info(f"📊 Symbols processed: {process_results['symbols_processed']}")
        logger.info(f"📝 Records written: {process_results['total_records']:,}")
        logger.info(f"⏱️  Total time: {elapsed_time:.1f} seconds")

        return {
            'success': True,
            'stats': self.checkpoint_data['stats'],
            'elapsed_time': elapsed_time
        }


def main():
    """Command line interface"""
    parser = argparse.ArgumentParser(description="FirstRate 30-day backfill processor")
    parser.add_argument("--full", action="store_true",
                        help="Run full backfill (download + process)")
    parser.add_argument("--process-only", action="store_true",
                        help="Process only, skip download")
    parser.add_argument("--data-path", default="/mnt/d/ats-data/firstrate-data",
                        help="Path to FirstRate data directory")
    parser.add_argument("--output-path", default="/mnt/d/ats-data/minute-bars/firstrate",
                        help="Output directory for processed files")
    parser.add_argument("--checkpoint-file", default="firstrate_30day_backfill.json",
                        help="Checkpoint file for resumable processing")
    parser.add_argument("--limit", type=int,
                        help="Limit number of symbols for testing")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging")

    args = parser.parse_args()

    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'firstrate_30day_backfill_{datetime.now().strftime("%Y%m%d")}.log')
        ]
    )

    processor = FirstRate30DayBackfill(
        data_path=args.data_path,
        output_path=args.output_path,
        checkpoint_file=args.checkpoint_file
    )

    try:
        result = asyncio.run(processor.run_full_backfill(
            download=args.full or not args.process_only,
            limit=args.limit
        ))

        print(f"\n✅ Backfill completed successfully!")
        print(f"📊 Final stats: {result['stats']}")

    except KeyboardInterrupt:
        print("\n🛑 Backfill interrupted by user")
        processor.save_checkpoint()
        print("💾 Checkpoint saved")

    except Exception as e:
        print(f"\n❌ Backfill failed: {e}")
        processor.save_checkpoint()
        raise


if __name__ == "__main__":
    main()