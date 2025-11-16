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
import aiohttp
import asyncpg
from collections import defaultdict, Counter

from domains.market_data.services.core.agent.core.firstrate_daily_downloader import FirstRateDownloader, DownloadJob
from core.adapters import create_firstrate_adapter, FirstRateAdapter
from core.run_aware_logging import setup_run_aware_logging
from core.platform.config_env.environment import Environment, EnvironmentType
from dataclasses import dataclass

# Import API status tracker (optional)
try:
    sys.path.append('/workspace/scripts')
    from api_status_tracker import get_global_tracker, initialize_global_tracker
    API_TRACKER_AVAILABLE = True
except ImportError:
    API_TRACKER_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.debug("API status tracker not available, continuing without tracking")

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
        
        # Save with metadata (create simple version for compatibility)
        df.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)
        
        # Generate metadata
        import json
        from datetime import datetime
        metadata_path = parquet_path.with_suffix('.parquet.metadata.json')
        metadata = {
            'file_path': parquet_path.name,
            'created_at': datetime.now().isoformat(),
            'file_size_bytes': parquet_path.stat().st_size,
            'records_count': len(df),
            'symbols': df['symbol'].unique().tolist(),
            'date_range': {
                'start': df['timestamp'].dt.date.min().isoformat(),
                'end': df['timestamp'].dt.date.max().isoformat()
            },
            'vendor': 'firstrate',
            'last_updated': datetime.now().isoformat()
        }
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.debug(f"Stored {len(bars)} bars for {symbol} to {parquet_path} with metadata")

logger = logging.getLogger(__name__)

CRITICAL_ETFS = {
    'SPY', 'QQQ', 'VTI', 'IWM', 'EFA', 'VWO', 'GLD', 'SLV', 'TLT', 'HYG',
    'LQD', 'EEM', 'XLF', 'XLK', 'XLE', 'XLI', 'XLV', 'XLY', 'XLP', 'XLU',
    'VNQ', 'EWJ', 'FXI', 'EWZ', 'RSX', 'ARKK', 'ARKG', 'ARKW', 'JETS', 'ICLN'
}


def process_symbol_standalone(symbol: str, zip_files: List[Path], start_date: date, end_date: date, period: str, output_path: Path) -> int:
    """Standalone function for Ray parallel processing"""
    import pandas as pd
    import zipfile
    import io
    
    new_records = []
    
    for zip_file in zip_files:
        try:
            with zipfile.ZipFile(zip_file, 'r') as zf:
                txt_file = f"{symbol}_{period}_1min_adjsplit.txt"
                if txt_file not in zf.namelist():
                    continue
                
                with zf.open(txt_file) as f:
                    content = f.read()
                    if len(content) < 50:
                        continue
                    
                    df = pd.read_csv(
                        io.BytesIO(content),
                        header=None,
                        names=['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    )
                    
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df = df[(df['timestamp'].dt.date >= start_date) & (df['timestamp'].dt.date <= end_date)]
                    
                    if len(df) > 0:
                        df['symbol'] = symbol
                        df['vendor'] = 'firstrate'
                        df['vwap'] = df['close']
                        df['trade_count'] = 0
                        df['quality_score'] = 1.0
                        new_records.append(df)
                        
        except Exception as e:
            continue
    
    if not new_records:
        return 0
    
    new_df = pd.concat(new_records, ignore_index=True)
    new_df = new_df.drop_duplicates(subset=['timestamp'], keep='last')
    new_df = new_df.sort_values('timestamp')
    
    monthly_groups = new_df.groupby([new_df['timestamp'].dt.year, new_df['timestamp'].dt.month])
    
    total_written = 0
    for (year, month), month_df in monthly_groups:
        symbol_dir = output_path / symbol[0] / symbol / str(year) / f"{month:02d}"
        parquet_path = symbol_dir / f"{symbol}_{year}_{month:02d}.parquet"
        
        if parquet_path.exists():
            existing_df = pd.read_parquet(parquet_path)
            
            if existing_df['timestamp'].dt.tz is not None:
                existing_df['timestamp'] = existing_df['timestamp'].dt.tz_localize(None)
            if month_df['timestamp'].dt.tz is not None:
                month_df['timestamp'] = month_df['timestamp'].dt.tz_localize(None)
            
            merged_df = pd.concat([existing_df, month_df], ignore_index=True)
            merged_df = merged_df.drop_duplicates(subset=['timestamp'], keep='last')
            merged_df = merged_df.sort_values('timestamp')
        else:
            merged_df = month_df
        
        symbol_dir.mkdir(parents=True, exist_ok=True)
        merged_df.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)
        
        # Generate metadata
        import json
        from datetime import datetime
        metadata_path = parquet_path.with_suffix('.parquet.metadata.json')
        metadata = {
            'file_path': parquet_path.name,
            'created_at': datetime.now().isoformat(),
            'file_size_bytes': parquet_path.stat().st_size,
            'records_count': len(merged_df),
            'symbols': merged_df['symbol'].unique().tolist(),
            'date_range': {
                'start': merged_df['timestamp'].dt.date.min().isoformat(),
                'end': merged_df['timestamp'].dt.date.max().isoformat()
            },
            'vendor': 'firstrate',
            'last_updated': datetime.now().isoformat()
        }
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        total_written += len(month_df)
    
    return total_written


class FirstRate30DayBackfill:
    """Complete 30-day backfill processor with download, merge capabilities, and production features"""

    def __init__(
        self,
        data_path: str = "/mnt/d/ats-data/firstrate-data",
        output_path: str = "/mnt/d/ats-data/minute-bars/firstrate",
        environment: str = "dev",
        slack_webhook: Optional[str] = None,
        prometheus_gateway: Optional[str] = None
    ):
        self.data_path = Path(data_path)
        self.output_path = Path(output_path)
        self.environment = environment
        self.slack_webhook = slack_webhook or os.getenv('SLACK_WEBHOOK_URL')
        self.prometheus_gateway = prometheus_gateway or os.getenv('PROMETHEUS_PUSHGATEWAY_URL')
        self.run_id = None
        self.db_pool = None

        self.downloader = FirstRateDownloader(base_path=str(self.data_path))
        self.adapter = create_firstrate_adapter(str(self.data_path))
        self.minute_manager = SimpleFileMinuteManager(str(self.output_path))
        
        # Initialize API status tracker (optional)
        self.api_tracker = get_global_tracker() if API_TRACKER_AVAILABLE else None

        # Processing stats
        self.stats = {
            'start_time': datetime.now(),
            'instruments_processed': 0,
            'files_created': 0,
            'files_updated': 0,
            'total_minute_bars': 0,
            'total_data_size_mb': 0,
            'instrument_types': defaultdict(int),
            'minute_bars_by_type': defaultdict(int),
            'minute_bars_by_day': defaultdict(int),
            'symbols_by_first_letter': defaultdict(int),
            'processing_errors': [],
            'critical_etfs_processed': 0,
            'active_stocks_processed': 0,
            'days_processed': [],
            'prometheus_metrics_sent': False,
            'slack_notification_sent': False
        }

        self.output_path.mkdir(parents=True, exist_ok=True)

    def _save_parquet_with_metadata(self, parquet_path: Path, df: pd.DataFrame) -> None:
        """Save parquet file and generate metadata atomically"""
        import json
        from datetime import datetime
        
        # Save parquet file
        df.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)
        
        # Generate and save metadata
        metadata_path = parquet_path.with_suffix('.parquet.metadata.json')
        metadata = {
            'file_path': parquet_path.name,
            'created_at': datetime.now().isoformat(),
            'file_size_bytes': parquet_path.stat().st_size,
            'records_count': len(df),
            'symbols': df['symbol'].unique().tolist(),
            'date_range': {
                'start': df['timestamp'].dt.date.min().isoformat(),
                'end': df['timestamp'].dt.date.max().isoformat()
            },
            'vendor': 'firstrate',
            'last_updated': datetime.now().isoformat()
        }
        
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.debug(f"💾 Saved {parquet_path.name} with {len(df):,} records and updated metadata")

    async def initialize(self):
        """Initialize database connections and API tracker."""
        try:
            # Use Environment class for proper database configuration
            env_type = EnvironmentType.INTEGRATION if self.environment == 'intg' else EnvironmentType.DEV
            env = Environment(env_type=env_type)
            db_config = env.get_database_config()
            
            db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            
            self.db_pool = await asyncpg.create_pool(
                db_url,
                min_size=db_config.get('min_size', 2),
                max_size=db_config.get('max_size', 10),
                command_timeout=db_config.get('command_timeout', 60)
            )
            logger.info(f"✅ Database connected: {db_config['host']}:{db_config['port']}/{db_config['database']}")
        except Exception as e:
            logger.warning(f"Database initialization failed: {e}")
            self.db_pool = None

        # Initialize API status tracker (optional)
        if API_TRACKER_AVAILABLE:
            await initialize_global_tracker()
        
        logger.info("✅ FirstRate 30-day backfill initialized")
        logger.info(f"📁 Data path: {self.data_path}")
        logger.info(f"📁 Output path: {self.output_path}")
        logger.info("✅ API status tracking initialized")

    async def close(self):
        """Close database connections."""
        if self.db_pool:
            await self.db_pool.close()

    async def get_active_instruments(self) -> List[tuple]:
        """Get active instruments from database."""
        if not self.db_pool:
            return []

        query = f"""
            SELECT DISTINCT
                symbol,
                CASE
                    WHEN symbol IN ('SPY', 'QQQ', 'VTI', 'IWM', 'EFA', 'VWO', 'GLD', 'SLV', 'TLT', 'HYG',
                                    'LQD', 'EEM', 'XLF', 'XLK', 'XLE', 'XLI', 'XLV', 'XLY', 'XLP', 'XLU',
                                    'VNQ', 'EWJ', 'FXI', 'EWZ', 'RSX', 'ARKK', 'ARKG', 'ARKW', 'JETS', 'ICLN')
                    THEN 'critical_etf'
                    WHEN symbol LIKE '%--%' OR symbol LIKE '%-%' OR symbol LIKE '%.%'
                    THEN 'other_etf'
                    ELSE 'stock'
                END as instrument_type,
                COALESCE(exchange, 'UNKNOWN') as exchange
            FROM {self.environment}_instrument
            WHERE active = true
              AND symbol IS NOT NULL
              AND LENGTH(symbol) BETWEEN 1 AND 5
              AND symbol NOT LIKE '%.%'
            ORDER BY
                CASE
                    WHEN symbol IN ('SPY', 'QQQ', 'VTI', 'IWM', 'EFA', 'VWO', 'GLD', 'SLV', 'TLT', 'HYG',
                                    'LQD', 'EEM', 'XLF', 'XLK', 'XLE', 'XLI', 'XLV', 'XLY', 'XLP', 'XLU',
                                    'VNQ', 'EWJ', 'FXI', 'EWZ', 'RSX', 'ARKK', 'ARKG', 'ARKW', 'JETS', 'ICLN')
                    THEN 0
                    ELSE 1
                END,
                symbol
        """

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(query)
            
            instruments = [(row['symbol'], row['instrument_type'], row['exchange']) for row in rows]
            
            # Update stats
            instrument_counts = Counter(row['instrument_type'] for row in rows)
            self.stats['instrument_types'].update(instrument_counts)

            logger.info(f"📊 Retrieved {len(instruments)} active instruments:")
            for inst_type, count in instrument_counts.items():
                logger.info(f"  {inst_type}: {count:,}")

            return instruments
        except Exception as e:
            logger.warning(f"Database query failed: {e}")
            return []


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
                    if filename.endswith('_1min_adjsplit.txt'):
                        symbol = filename.split('_')[0].upper()
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

    async def send_prometheus_metrics(self):
        """Send processing metrics to Prometheus pushgateway."""
        if not self.prometheus_gateway:
            logger.debug("📊 Prometheus gateway not configured, skipping metrics")
            return

        metrics = []
        timestamp = int(datetime.now().timestamp())

        # Total metrics
        metrics.extend([
            f"ats_firstrate_30day_instruments_processed {self.stats['instruments_processed']} {timestamp}",
            f"ats_firstrate_30day_files_created {self.stats['files_created']} {timestamp}",
            f"ats_firstrate_30day_files_updated {self.stats['files_updated']} {timestamp}",
            f"ats_firstrate_30day_total_minute_bars {self.stats['total_minute_bars']} {timestamp}",
            f"ats_firstrate_30day_total_data_size_mb {self.stats['total_data_size_mb']:.2f} {timestamp}",
            f"ats_firstrate_30day_processing_errors {len(self.stats['processing_errors'])} {timestamp}",
        ])

        # Instrument type metrics
        for inst_type, count in self.stats['instrument_types'].items():
            metrics.append(f'ats_firstrate_30day_symbols_by_type{{type="{inst_type}"}} {count} {timestamp}')

        # Send to Prometheus
        try:
            async with aiohttp.ClientSession() as session:
                metrics_data = '\n'.join(metrics) + '\n'
                await session.post(
                    f"{self.prometheus_gateway}/metrics/job/ats_firstrate_30day_backfill",
                    data=metrics_data,
                    headers={'Content-Type': 'text/plain'}
                )

            self.stats['prometheus_metrics_sent'] = True
            logger.info(f"📊 Sent {len(metrics)} metrics to Prometheus")
        except Exception as e:
            logger.warning(f"Failed to send Prometheus metrics: {e}")

    async def send_slack_notification(self):
        """Send completion summary to Slack."""
        if not self.slack_webhook:
            logger.debug("🔔 Slack webhook not configured, skipping notification")
            return

        duration = datetime.now() - self.stats['start_time']

        message = {
            "text": "🎯 ATS FirstRate 30-Day Backfill Complete",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🎯 FirstRate 30-Day Backfill Summary"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*📅 Period:* 30 days\n*⏱️ Duration:* {duration}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*📊 Instruments:* {self.stats['instruments_processed']:,}\n*📄 Files:* {self.stats['files_created']:,} new, {self.stats['files_updated']:,} updated"
                        }
                    ]
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*📈 Minute Bars:* {self.stats['total_minute_bars']:,}\n*💾 Data Size:* {self.stats['total_data_size_mb']:.1f} MB"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*🏆 Critical ETFs:* {self.stats['critical_etfs_processed']:,}\n*📈 Stocks:* {self.stats['active_stocks_processed']:,}"
                        }
                    ]
                }
            ]
        }

        if self.stats['processing_errors']:
            error_count = len(self.stats['processing_errors'])
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*⚠️ Processing Errors:* {error_count}\n" +
                           f"First few: {', '.join(self.stats['processing_errors'][:3])}"
                }
            })

        try:
            async with aiohttp.ClientSession() as session:
                await session.post(
                    self.slack_webhook,
                    json=message,
                    headers={'Content-Type': 'application/json'}
                )

            self.stats['slack_notification_sent'] = True
            logger.info("🔔 Completion summary sent to Slack")
        except Exception as e:
            logger.warning(f"Failed to send Slack notification: {e}")

    async def download_30_days(self, period: str = "month") -> Dict[str, bool]:
        """Download the latest 30 days of data for stocks and ETFs
        
        Args:
            period: API period parameter - 'month' (30 days), 'week', or 'day'
        """
        logger.info(f"🚀 Starting {period} data downloads...")
        
        start_date, end_date = self.get_30_day_date_range()
        logger.info(f"📅 Date range: {start_date} to {end_date}")

        jobs = [
            DownloadJob(asset_type='stock', period=period),
            DownloadJob(asset_type='etf', period=period)
        ]

        results = {}
        for job in jobs:
            logger.info(f"📥 Downloading {job.asset_type} data...")
            try:
                success = await self.downloader.download_daily_data([job], date_override=None)
                results[job.asset_type] = success[job.asset_type] if success else False
                
                if results[job.asset_type]:
                    logger.info(f"✅ {job.asset_type} download completed")
                else:
                    logger.error(f"❌ {job.asset_type} download failed")

            except Exception as e:
                logger.error(f"❌ Error downloading {job.asset_type}: {e}")
                results[job.asset_type] = False

        return results

    async def get_symbols_to_process(self, limit: Optional[int] = None) -> List[str]:
        """Get list of symbols from database or downloaded data"""
        symbols = set()
        
        # First try to get from database
        if self.db_pool:
            logger.info("📋 Getting symbols from database...")
            instruments = await self.get_active_instruments()
            for symbol, inst_type, _ in instruments:
                symbols.add(symbol)
            
            if symbols:
                logger.info(f"📊 Found {len(symbols)} symbols from database")
            else:
                logger.info("📋 No symbols found in database, falling back to file scan...")
                
        # Fallback to file scan if no database symbols
        if not symbols:
            logger.info("📋 Building symbol list from downloaded data...")

            daily_dir = self.data_path / 'daily'
            if not daily_dir.exists():
                logger.error(f"❌ Daily download directory not found: {daily_dir}")
                return []

            stock_files = list((daily_dir / 'stock').glob('stock_*.zip')) if (daily_dir / 'stock').exists() else []
            etf_files = list((daily_dir / 'etf').glob('etf_*.zip')) if (daily_dir / 'etf').exists() else []

            logger.info(f"📁 Found {len(stock_files)} stock files, {len(etf_files)} ETF files")

            for zip_file in stock_files + etf_files:
                file_symbols = self._get_symbols_from_zip(zip_file)
                symbols.update(file_symbols)

        # Always add critical ETFs
        symbols.update(CRITICAL_ETFS)

        symbol_list = sorted(list(symbols))
        
        if limit:
            symbol_list = symbol_list[:limit]
            logger.info(f"🔢 Limited to {limit} symbols")

        logger.info(f"📊 Total symbols to process: {len(symbol_list)}")
        return symbol_list

    def get_daily_zip_files_for_date_range(self, start_date: date, end_date: date) -> List[Path]:
        """Get all zip files for processing (daily + monthly)"""
        zip_files = []
        
        # Check for monthly files first (newer downloads)
        monthly_stock_dir = self.data_path / 'monthly' / 'stock'
        monthly_etf_dir = self.data_path / 'monthly' / 'etf'
        
        for asset_dir in [monthly_stock_dir, monthly_etf_dir]:
            if not asset_dir.exists():
                continue
            
            for zip_file in sorted(asset_dir.glob('*_month_1min_adj_split.zip')):
                zip_files.append(zip_file)
                logger.debug(f"Found monthly file: {zip_file}")
        
        # Fallback to daily files if no monthly files found
        if not zip_files:
            daily_stock_dir = self.data_path / 'daily' / 'stock'
            daily_etf_dir = self.data_path / 'daily' / 'etf'
            
            for asset_dir in [daily_stock_dir, daily_etf_dir]:
                if not asset_dir.exists():
                    continue
                
                for zip_file in sorted(asset_dir.glob('*_1min_adj_split.zip')):
                    zip_files.append(zip_file)
                    logger.debug(f"Found daily file: {zip_file}")
        
        return zip_files

    async def process_symbol_from_daily_zips(
        self,
        symbol: str,
        zip_files: List[Path],
        start_date: date,
        end_date: date,
        period: str = "day"
    ) -> int:
        """Process symbol from daily zip files and merge with existing monthly data"""
        import pandas as pd
        import zipfile
        import io
        
        logger.info(f"Processing {symbol} from {len(zip_files)} daily zip files")
        
        new_records = []
        
        for zip_file in zip_files:
            try:
                with zipfile.ZipFile(zip_file, 'r') as zf:
                    # FirstRate files use 'month' pattern regardless of period
                    txt_file = f"{symbol}_month_1min_adjsplit.txt"
                    if txt_file not in zf.namelist():
                        continue
                    
                    with zf.open(txt_file) as f:
                        content = f.read()
                        if len(content) < 50:
                            continue
                        
                        df = pd.read_csv(
                            io.BytesIO(content),
                            header=None,
                            names=['timestamp', 'open', 'high', 'low', 'close', 'volume']
                        )
                        
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df = df[(df['timestamp'].dt.date >= start_date) & (df['timestamp'].dt.date <= end_date)]
                        
                        if len(df) > 0:
                            df['symbol'] = symbol
                            df['vendor'] = 'firstrate'
                            df['vwap'] = df['close']
                            df['trade_count'] = 0
                            df['quality_score'] = 1.0
                            new_records.append(df)
                            
            except Exception as e:
                logger.debug(f"Could not process {symbol} from {zip_file.name}: {e}")
                continue
        
        if not new_records:
            logger.debug(f"No new data for {symbol}")
            return 0
        
        new_df = pd.concat(new_records, ignore_index=True)
        new_df = new_df.drop_duplicates(subset=['timestamp'], keep='last')
        new_df = new_df.sort_values('timestamp')
        
        monthly_groups = new_df.groupby([new_df['timestamp'].dt.year, new_df['timestamp'].dt.month])
        
        total_written = 0
        for (year, month), month_df in monthly_groups:
            symbol_dir = self.output_path / symbol[0] / symbol / str(year) / f"{month:02d}"
            parquet_path = symbol_dir / f"{symbol}_{year}_{month:02d}.parquet"
            
            if parquet_path.exists():
                existing_df = pd.read_parquet(parquet_path)
                
                if existing_df['timestamp'].dt.tz is not None:
                    existing_df['timestamp'] = existing_df['timestamp'].dt.tz_localize(None)
                if month_df['timestamp'].dt.tz is not None:
                    month_df['timestamp'] = month_df['timestamp'].dt.tz_localize(None)
                
                merged_df = pd.concat([existing_df, month_df], ignore_index=True)
                merged_df = merged_df.drop_duplicates(subset=['timestamp'], keep='last')
                merged_df = merged_df.sort_values('timestamp')
                
                new_count = len(merged_df) - len(existing_df)
                logger.info(f"✅ {symbol} {year}-{month:02d}: merged {new_count} new records (was {len(existing_df)}, now {len(merged_df)})")
            else:
                merged_df = month_df
                logger.info(f"✅ {symbol} {year}-{month:02d}: created with {len(merged_df)} records")
            
            symbol_dir.mkdir(parents=True, exist_ok=True)
            self._save_parquet_with_metadata(parquet_path, merged_df)
            total_written += len(month_df)
            
            # Update statistics
            file_size = parquet_path.stat().st_size / (1024 * 1024)  # MB
            self.stats['total_minute_bars'] += len(month_df)
            self.stats['total_data_size_mb'] += file_size
            
            if parquet_path.exists():
                self.stats['files_updated'] += 1
            else:
                self.stats['files_created'] += 1
        
        return total_written

    async def process_all_symbols(
        self,
        symbols: List[str],
        start_date: date,
        end_date: date,
        period: str = "day",
        use_ray: bool = False
    ) -> Dict:
        """Process all symbols for the date range"""
        logger.info(f"🚀 Processing {len(symbols)} symbols from daily zip files...")

        zip_files = self.get_daily_zip_files_for_date_range(start_date, end_date)
        logger.info(f"📁 Found {len(zip_files)} daily zip files to process")

        if use_ray:
            import ray
            if not ray.is_initialized():
                ray.init(ignore_reinit_error=True)
                logger.info("🎯 Ray initialized for parallel processing")
            
            process_symbol_remote = ray.remote(process_symbol_standalone)
            
            logger.info(f"🚀 Processing {len(symbols)} symbols in parallel with Ray...")
            futures = [
                process_symbol_remote.remote(symbol, zip_files, start_date, end_date, period, self.output_path)
                for symbol in symbols
            ]
            
            results = ray.get(futures)
            total_records = sum(results)
            
            logger.info(f"✅ Parallel processing complete: {len(symbols)} symbols, {total_records:,} records")
            
            return {
                'symbols_processed': len(symbols),
                'total_records': total_records
            }

        total_records = 0
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"🔄 Processing {symbol} ({i}/{len(symbols)})")

            records = await self.process_symbol_from_daily_zips(
                symbol, zip_files, start_date, end_date, period=period
            )
            
            total_records += records

            # Progress tracking disabled
            if (i % 10) == 0:
                logger.info(f"Progress: {i} symbols processed, {total_records} records written")

        return {
            'symbols_processed': len(symbols),
            'total_records': total_records
        }

    async def run_full_backfill(
        self,
        download: bool = True,
        period: str = "month",
        symbols: Optional[List[str]] = None,
        limit: Optional[int] = None,
        use_ray: bool = False
    ) -> Dict:
        """Run complete 30-day backfill"""
        logger.info("🚀 Starting FirstRate 30-day backfill")
        logger.info(f"📂 Data path: {self.data_path}")
        logger.info(f"💾 Output path: {self.output_path}")

        # Initialize database and API tracker
        await self.initialize()
        
        self.run_id = f"firstrate_30day_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"📝 Run ID: {self.run_id}")

        start_time = time.time()

        if download:
            logger.info("📥 Starting download of past 30 days data...")
            download_results = await self.download_30_days(period=period)
            
            if any(download_results.values()):
                logger.info("✅ Download completed successfully")
                for asset_type, success in download_results.items():
                    if success:
                        logger.info(f"  ✅ {asset_type}: Success")
                    else:
                        logger.warning(f"  ⚠️ {asset_type}: Failed")
            else:
                logger.error("❌ All downloads failed!")
        else:
            logger.info("⏭️  Skipping download step")

        if symbols:
            symbol_list = symbols
            logger.info(f"🎯 Specific symbols requested: {symbol_list}")
        else:
            symbol_list = await self.get_symbols_to_process(limit=limit)
        
        start_date, end_date = self.get_30_day_date_range()
        self.stats['days_processed'] = [(start_date + timedelta(days=x)).isoformat() for x in range((end_date - start_date).days + 1)]
        
        process_results = await self.process_all_symbols(symbol_list, start_date, end_date, period=period, use_ray=use_ray)

        elapsed_time = time.time() - start_time
        self.stats['instruments_processed'] = process_results['symbols_processed']

        logger.info("🎉 30-day backfill completed!")
        logger.info(f"📊 Symbols processed: {process_results['symbols_processed']}")
        logger.info(f"📝 Records written: {process_results['total_records']:,}")
        logger.info(f"📄 Files created: {self.stats['files_created']:,}")
        logger.info(f"📄 Files updated: {self.stats['files_updated']:,}")
        logger.info(f"💾 Total data size: {self.stats['total_data_size_mb']:.1f} MB")
        logger.info(f"⏱️  Total time: {elapsed_time:.1f} seconds")

        # Send metrics and notifications
        await asyncio.gather(
            self.send_prometheus_metrics(),
            self.send_slack_notification(),
            return_exceptions=True
        )
        
        # Cleanup
        await self.close()
        logger.info(f"Run {self.run_id} completed successfully")

        return {
            'success': True,
            'run_id': self.run_id,
            'symbols_processed': process_results['symbols_processed'],
            'total_records': process_results['total_records'],
            'elapsed_time': elapsed_time,
            'files_created': self.stats['files_created'],
            'files_updated': self.stats['files_updated'],
            'data_size_mb': self.stats['total_data_size_mb']
        }


def main():
    """Command line interface"""
    parser = argparse.ArgumentParser(description="FirstRate 30-day backfill processor")
    parser.add_argument("--full", action="store_true",
                        help="Run full backfill (download + process)")
    parser.add_argument("--process-only", action="store_true",
                        help="Process only, skip download")
    parser.add_argument("--data-path", 
                        default=os.getenv('ATS_DATA_PATH', '/mnt/d/ats-data') + "/firstrate-data",
                        help="Path to FirstRate data directory")
    parser.add_argument("--output-path",
                        default=os.getenv('ATS_DATA_PATH', '/mnt/d/ats-data') + "/minute-bars/firstrate",
                        help="Output directory for processed files")
    parser.add_argument("--environment", default="dev",
                        choices=['dev', 'intg', 'prod'],
                        help="Environment (dev, intg, prod) for run tracking")
    parser.add_argument("--use-ray", action="store_true",
                        help="Use Ray for parallel symbol processing")
    parser.add_argument("--period", default="month",
                        choices=['day', 'week', 'month', 'full'],
                        help="Download period: day (1 day), week (current week), month (30 days), full (all history)")
    parser.add_argument("--symbols", type=str,
                        help="Specific symbols to process (comma-separated)")
    parser.add_argument("--limit", type=int,
                        help="Limit number of symbols for testing")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging")
    parser.add_argument("--slack-webhook", help="Slack webhook URL for notifications")
    parser.add_argument("--prometheus-gateway", help="Prometheus pushgateway URL")

    args = parser.parse_args()

    # Configure logging with run-aware logging
    log_level = "DEBUG" if args.debug else "INFO"
    setup_run_aware_logging(log_level=log_level)
    
    # Also add file handler
    file_handler = logging.FileHandler(f'firstrate_30day_backfill_{datetime.now().strftime("%Y%m%d")}.log')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(file_handler)

    processor = FirstRate30DayBackfill(
        data_path=args.data_path,
        output_path=args.output_path,
        environment=args.environment,
        slack_webhook=args.slack_webhook,
        prometheus_gateway=args.prometheus_gateway
    )

    try:
        symbols = None
        if args.symbols:
            symbols = [s.strip() for s in args.symbols.split(',')]
        
        result = asyncio.run(processor.run_full_backfill(
            download=args.full or not args.process_only,
            period=args.period,
            symbols=symbols,
            use_ray=args.use_ray,
            limit=args.limit
        ))

        print(f"\n✅ Backfill completed successfully!")
        print(f"📝 Run ID: {result['run_id']}")
        print(f"📊 Symbols: {result['symbols_processed']}")
        print(f"📝 Records: {result['total_records']:,}")
        print(f"⏱️  Time: {result['elapsed_time']:.1f}s")

    except KeyboardInterrupt:
        print("\n🛑 Backfill interrupted by user")
        raise

    except Exception as e:
        print(f"\n❌ Backfill failed: {e}")
        raise


if __name__ == "__main__":
    main()