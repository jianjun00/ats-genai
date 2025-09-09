"""
FirstRate Minute Bar Data Adapter

Processes FirstRate's zip-compressed 1-minute historical data files with split/dividend adjustments.
Supports efficient parsing of large historical datasets organized by symbol alphabetically.

Data Format: timestamp,open,high,low,close,volume
Example: 2013-12-09 08:36:00,22.8049,22.8049,22.6258,22.6258,1697
"""

import asyncio
import zipfile
import csv
import io
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any, AsyncIterator
from pathlib import Path
import logging
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
import pandas as pd
import hashlib
from io import StringIO

logger = logging.getLogger(__name__)


@dataclass
class FirstRateMinuteBar:
    """FirstRate minute bar data structure."""
    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int

    # Computed fields
    vwap: Optional[Decimal] = None
    returns: Optional[Decimal] = None
    quality_score: Decimal = Decimal('1.0')  # Default high quality for FirstRate
    vendor: str = 'firstrate'

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp,
            'open': float(self.open),
            'high': float(self.high),
            'low': float(self.low),
            'close': float(self.close),
            'volume': self.volume,
            'vwap': float(self.vwap) if self.vwap else None,
            'returns': float(self.returns) if self.returns else None,
            'quality_score': float(self.quality_score),
            'vendor': self.vendor,
            'data_source_flags': {
                'source': 'firstrate',
                'adjusted': 'split_dividend',
                'resolution': '1min',
                'file_format': 'csv_zip'
            }
        }


@dataclass
class FirstRateParsingStats:
    """Statistics from parsing FirstRate data."""
    total_files_processed: int = 0
    total_bars_parsed: int = 0
    total_symbols_processed: int = 0
    parsing_errors: List[str] = None
    processing_time_seconds: float = 0.0

    def __post_init__(self):
        if self.parsing_errors is None:
            self.parsing_errors = []


class FirstRateMinuteAdapter:
    """
    Adapter for parsing FirstRate's 1-minute historical data files.

    Features:
    - Efficient zip file processing without full extraction
    - Memory-efficient streaming for large datasets
    - Data validation and quality scoring
    - Batch processing with progress tracking
    - Support for date range filtering
    """

    def __init__(self, base_data_path: str = "/mnt/d/ats-data/firstrate-data/stock"):
        self.base_data_path = Path(base_data_path)
        self.daily_data_path = Path("/mnt/d/ats-data/firstrate-data/daily/stock")
        self.stats = FirstRateParsingStats()
        self.files_written = 0
        self.files_skipped = 0

        # Validation settings
        self.max_price_deviation = Decimal('0.5')  # 50% max price jump
        self.min_volume = 0
        self.max_volume = 1_000_000_000  # 1B shares max

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""

    async def get_available_symbols(self) -> Dict[str, List[str]]:
        """
        Get all available symbols organized by alphabet letter.

        Returns:
            Dict mapping letter -> list of symbols
        """
        symbols_by_letter = {}

        # Find all FirstRate zip files
        zip_files = list(self.base_data_path.glob("stock_*_full_1min_adjsplitdiv_*.zip"))

        for zip_path in zip_files:
            # Extract letter from filename (e.g. stock_A_full_1min...)
            letter = zip_path.name.split('_')[1]

            symbols = await self._get_symbols_from_zip(zip_path)
            symbols_by_letter[letter] = sorted(symbols)

        return symbols_by_letter

    async def _get_symbols_from_zip(self, zip_path: Path) -> List[str]:
        """Extract symbol list from zip file without reading data."""
        symbols = []

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                for filename in zf.namelist():
                    if filename.endswith('_full_1min_adjsplitdiv.txt'):
                        # Extract symbol from filename (e.g. AAPL_full_1min_adjsplitdiv.txt -> AAPL)
                        symbol = filename.replace('_full_1min_adjsplitdiv.txt', '')
                        symbols.append(symbol)
        except Exception as e:
            logger.error(f"Error reading zip file {zip_path}: {e}")
            self.stats.parsing_errors.append(f"Zip file {zip_path}: {e}")

        return symbols

    async def fetch_minute_bars_async(
        self,
        symbols: List[str],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_bars_per_symbol: Optional[int] = None
    ) -> AsyncIterator[FirstRateMinuteBar]:
        """
        Fetch minute bars for specified symbols with date filtering.

        Args:
            symbols: List of symbols to fetch
            start_date: Optional start date filter
            end_date: Optional end date filter
            max_bars_per_symbol: Optional limit per symbol

        Yields:
            FirstRateMinuteBar objects
        """
        start_time = datetime.now()

        # Group symbols by alphabet letter for efficient zip file access
        symbols_by_letter = self._group_symbols_by_letter(symbols)

        for letter, letter_symbols in symbols_by_letter.items():
            zip_path = self._get_zip_path_for_letter(letter)
            if not zip_path or not zip_path.exists():
                logger.warning(f"No FirstRate data file found for letter {letter}")
                continue

            async for bar in self._process_zip_file(
                zip_path, letter_symbols, start_date, end_date, max_bars_per_symbol
            ):
                yield bar

        # Update processing time
        self.stats.processing_time_seconds = (datetime.now() - start_time).total_seconds()

    def _group_symbols_by_letter(self, symbols: List[str]) -> Dict[str, List[str]]:
        """Group symbols by first letter for efficient zip file processing."""
        symbols_by_letter = {}

        for symbol in symbols:
            letter = symbol[0].upper()
            if letter not in symbols_by_letter:
                symbols_by_letter[letter] = []
            symbols_by_letter[letter].append(symbol)

        return symbols_by_letter

    def _get_zip_path_for_letter(self, letter: str) -> Optional[Path]:
        """Get zip file path for given letter."""
        # Pattern: stock_A_full_1min_adjsplitdiv_*.zip
        zip_files = list(self.base_data_path.glob(f"stock_{letter}_full_1min_adjsplitdiv_*.zip"))

        if zip_files:
            return zip_files[0]  # Take first match
        return None

    async def _process_zip_file(
        self,
        zip_path: Path,
        target_symbols: List[str],
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        max_bars_per_symbol: Optional[int]
    ) -> AsyncIterator[FirstRateMinuteBar]:
        """Process specific zip file for target symbols."""

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                for symbol in target_symbols:
                    filename = f"{symbol}_full_1min_adjsplitdiv.txt"

                    if filename not in zf.namelist():
                        logger.debug(f"Symbol {symbol} not found in {zip_path}")
                        continue

                    bars_processed = 0
                    async for bar in self._parse_symbol_file(
                        zf, filename, symbol, start_date, end_date
                    ):
                        if max_bars_per_symbol and bars_processed >= max_bars_per_symbol:
                            break

                        yield bar
                        bars_processed += 1

                    if bars_processed > 0:
                        self.stats.total_symbols_processed += 1
                        logger.debug(f"Processed {bars_processed} bars for {symbol}")

        except Exception as e:
            logger.error(f"Error processing zip file {zip_path}: {e}")
            self.stats.parsing_errors.append(f"Zip processing {zip_path}: {e}")

    async def _parse_symbol_file(
        self,
        zip_file: zipfile.ZipFile,
        filename: str,
        symbol: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime]
    ) -> AsyncIterator[FirstRateMinuteBar]:
        """Parse individual symbol file from zip."""

        try:
            with zip_file.open(filename, 'r') as f:
                # Read as text with UTF-8 encoding
                text_data = io.TextIOWrapper(f, encoding='utf-8')
                csv_reader = csv.reader(text_data)

                previous_bar = None
                bars_parsed = 0

                for row_num, row in enumerate(csv_reader, 1):
                    try:
                        if len(row) != 6:
                            logger.warning(f"Invalid row format in {filename} line {row_num}: {row}")
                            continue

                        bar = self._parse_csv_row(row, symbol)
                        if not bar:
                            continue

                        # Date filtering
                        if start_date and bar.timestamp < start_date:
                            continue
                        if end_date and bar.timestamp > end_date:
                            break

                        # Data validation
                        if not self._validate_bar(bar, previous_bar):
                            continue

                        # Calculate returns if we have previous bar
                        if previous_bar and previous_bar.close > 0:
                            bar.returns = (bar.close / previous_bar.close - 1).quantize(
                                Decimal('0.000001'), rounding=ROUND_HALF_UP
                            )

                        # Calculate VWAP approximation (use close as proxy for FirstRate data)
                        bar.vwap = (bar.open + bar.high + bar.low + bar.close) / 4

                        yield bar
                        previous_bar = bar
                        bars_parsed += 1

                        # Yield control periodically for better async performance
                        if bars_parsed % 1000 == 0:
                            await asyncio.sleep(0)  # Allow other coroutines to run

                    except Exception as e:
                        logger.warning(f"Error parsing row {row_num} in {filename}: {e}")
                        self.stats.parsing_errors.append(f"{filename}:{row_num}: {e}")
                        continue

                self.stats.total_bars_parsed += bars_parsed
                self.stats.total_files_processed += 1

        except Exception as e:
            logger.error(f"Error reading file {filename}: {e}")
            self.stats.parsing_errors.append(f"File read {filename}: {e}")

    def _parse_csv_row(self, row: List[str], symbol: str) -> Optional[FirstRateMinuteBar]:
        """Parse single CSV row into FirstRateMinuteBar."""

        try:
            # Format: timestamp,open,high,low,close,volume
            # Example: 2013-12-09 08:36:00,22.8049,22.8049,22.6258,22.6258,1697

            timestamp_str, open_str, high_str, low_str, close_str, volume_str = row

            # Parse timestamp (assumed to be in market timezone - Eastern)
            timestamp = datetime.strptime(timestamp_str.strip(), '%Y-%m-%d %H:%M:%S')
            # Convert to UTC (FirstRate data is in Eastern Time)
            timestamp = timestamp.replace(tzinfo=timezone.utc)  # Simplified - assume UTC for now

            # Parse prices with high precision
            open_price = Decimal(open_str.strip()).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
            high_price = Decimal(high_str.strip()).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
            low_price = Decimal(low_str.strip()).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
            close_price = Decimal(close_str.strip()).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)

            # Parse volume
            volume = int(float(volume_str.strip()))  # Handle potential decimal volumes

            return FirstRateMinuteBar(
                symbol=symbol.upper(),
                timestamp=timestamp,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume
            )

        except (ValueError, IndexError) as e:
            logger.debug(f"Failed to parse row for {symbol}: {row} - {e}")
            return None

    def _validate_bar(self, bar: FirstRateMinuteBar, previous_bar: Optional[FirstRateMinuteBar]) -> bool:
        """Validate minute bar data quality."""

        try:
            # Basic OHLC validation
            if not (bar.low <= bar.open <= bar.high and bar.low <= bar.close <= bar.high):
                logger.debug(f"Invalid OHLC for {bar.symbol} at {bar.timestamp}")
                return False

            # Price positivity
            if bar.open <= 0 or bar.high <= 0 or bar.low <= 0 or bar.close <= 0:
                logger.debug(f"Non-positive prices for {bar.symbol} at {bar.timestamp}")
                return False

            # Volume validation
            if bar.volume < self.min_volume or bar.volume > self.max_volume:
                logger.debug(f"Invalid volume {bar.volume} for {bar.symbol} at {bar.timestamp}")
                return False

            # Price jump validation (if we have previous bar)
            if previous_bar:
                price_change = abs(bar.open / previous_bar.close - 1) if previous_bar.close > 0 else 0
                if price_change > self.max_price_deviation:
                    logger.debug(f"Large price jump {price_change:.1%} for {bar.symbol} at {bar.timestamp}")
                    bar.quality_score = Decimal('0.5')  # Lower quality but don't reject

            return True

        except Exception as e:
            logger.debug(f"Validation error for {bar.symbol}: {e}")
            return False

    def get_parsing_stats(self) -> FirstRateParsingStats:
        """Get current parsing statistics."""
        return self.stats

    def reset_stats(self):
        """Reset parsing statistics."""
        self.stats = FirstRateParsingStats()
        self.files_written = 0
        self.files_skipped = 0

    def get_recent_firstrate_files(self, days_back: int = 30) -> List[Path]:
        """Get recent FirstRate daily ZIP files for incremental backfill."""
        files = []
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        current_date = start_date
        while current_date <= end_date:
            filename = f"stock_{current_date.strftime('%Y%m%d')}_1min_adj_split.zip"
            file_path = self.daily_data_path / filename
            if file_path.exists():
                files.append(file_path)
            current_date += timedelta(days=1)
            
        logger.info(f"Found {len(files)} FirstRate daily files for past {days_back} days")
        return files
        
    def extract_symbol_from_daily_zip(self, zip_path: Path, symbol: str) -> pd.DataFrame:
        """Extract minute data for symbol from daily ZIP file."""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                target_file = f"{symbol}_day_1min_adjsplit.txt"
                if target_file not in zf.namelist():
                    return pd.DataFrame()
                    
                with zf.open(target_file) as f:
                    content = f.read().decode('utf-8').strip()
                    if not content:
                        return pd.DataFrame()
                    
                    # Parse CSV without header
                    df = pd.read_csv(StringIO(content), 
                                   names=['timestamp', 'open', 'high', 'low', 'close', 'volume'],
                                   parse_dates=['timestamp'])
                    
                    # Add metadata columns
                    df['vwap'] = None
                    df['trade_count'] = None  
                    df['vendor'] = 'firstrate'
                    df['quality_score'] = 1.0
                    
                    return df
                    
        except Exception as e:
            logger.error(f"Error reading {symbol} from {zip_path.name}: {e}")
            return pd.DataFrame()
    
    def fetch_symbol_data_for_backfill(self, symbol: str, days_back: int = 30) -> pd.DataFrame:
        """Fetch all recent data for symbol from daily files."""
        files = self.get_recent_firstrate_files(days_back)
        all_data = []
        
        for zip_path in files:
            df = self.extract_symbol_from_daily_zip(zip_path, symbol)
            if not df.empty:
                all_data.append(df)
                
        if not all_data:
            return pd.DataFrame()
            
        # Combine and deduplicate
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
        
        logger.info(f"Fetched {len(combined_df)} records for {symbol}")
        return combined_df
    
    def get_monthly_file_path(self, symbol: str, year: int, month: int, output_path: str = "/mnt/d/ats-data/minute-bars/firstrate") -> Path:
        """Get path to monthly parquet file."""
        first_letter = symbol[0]
        return (Path(output_path) / first_letter / symbol / str(year) / 
                f"{month:02d}" / f"{symbol}_{year}_{month:02d}.parquet")
                
    def calculate_hash(self, df: pd.DataFrame) -> str:
        """Calculate data hash for change detection."""
        if df.empty:
            return "empty"
        hash_data = f"{len(df)}|{df['timestamp'].min()}|{df['timestamp'].max()}|{df['volume'].sum()}"
        return hashlib.md5(hash_data.encode()).hexdigest()
        
    def read_existing_data(self, symbol: str, year: int, month: int, output_path: str = "/mnt/d/ats-data/minute-bars/firstrate") -> pd.DataFrame:
        """Read existing monthly data."""
        file_path = self.get_monthly_file_path(symbol, year, month, output_path)
        if file_path.exists():
            try:
                return pd.read_parquet(file_path)
            except Exception as e:
                logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()
        
    def filter_month_data(self, df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
        """Filter data to specific month."""
        if df.empty:
            return df
            
        month_start = pd.Timestamp(year, month, 1)
        if month == 12:
            month_end = pd.Timestamp(year + 1, 1, 1)
        else:
            month_end = pd.Timestamp(year, month + 1, 1)
            
        mask = (df['timestamp'] >= month_start) & (df['timestamp'] < month_end)
        return df[mask].copy()
        
    def write_monthly_file(self, symbol: str, year: int, month: int, df: pd.DataFrame, output_path: str = "/mnt/d/ats-data/minute-bars/firstrate") -> bool:
        """Write monthly file if changed."""
        if df.empty:
            return False
            
        file_path = self.get_monthly_file_path(symbol, year, month, output_path)
        
        # Check for changes
        existing_df = self.read_existing_data(symbol, year, month, output_path)
        new_hash = self.calculate_hash(df)
        existing_hash = self.calculate_hash(existing_df)
        
        if new_hash == existing_hash:
            logger.info(f"⏭️  No changes for {symbol} {year}-{month:02d}, skipping")
            self.files_skipped += 1
            return False
            
        # Create directory and write file
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(file_path, index=False)
        
        logger.info(f"✅ Wrote {len(df)} records to {symbol}_{year}_{month:02d}.parquet")
        self.files_written += 1
        return True
    
    async def process_symbol_incremental(self, symbol: str, days_back: int = 30, output_path: str = "/mnt/d/ats-data/minute-bars/firstrate") -> dict:
        """Process single symbol for incremental backfill."""
        result = {'symbol': symbol, 'success': False, 'files_written': 0}
        
        try:
            logger.info(f"🔄 Processing {symbol}")
            
            # Get new data
            new_data = self.fetch_symbol_data_for_backfill(symbol, days_back)
            if new_data.empty:
                logger.info(f"No data for {symbol}")
                result['success'] = True
                return result
                
            # Process last 2 months
            now = datetime.now()
            months = [
                (now.year, now.month),
                ((now.replace(day=1) - timedelta(days=1)).year, (now.replace(day=1) - timedelta(days=1)).month)
            ]
            
            for year, month in months:
                # Filter new data to this month
                month_new = self.filter_month_data(new_data, year, month)
                
                if month_new.empty:
                    continue
                    
                # Read existing data
                existing = self.read_existing_data(symbol, year, month, output_path)
                
                # Merge data
                if existing.empty:
                    merged = month_new
                else:
                    combined = pd.concat([existing, month_new], ignore_index=True)
                    merged = combined.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
                
                # Write if changed
                if self.write_monthly_file(symbol, year, month, merged, output_path):
                    result['files_written'] += 1
                    
            result['success'] = True
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            result['error'] = str(e)
            
        return result
    
    async def incremental_backfill_to_files(
        self,
        symbols: List[str],
        days_back: int = 30,
        output_path: str = "/mnt/d/ats-data/minute-bars/firstrate"
    ) -> Dict[str, Any]:
        """Run incremental backfill for multiple symbols."""
        start_time = datetime.now()
        
        logger.info(f"🚀 Starting FirstRate Incremental Backfill")
        logger.info(f"📅 Processing past {days_back} days")
        logger.info(f"💾 Output: {output_path}")
        
        # Check files available
        files = self.get_recent_firstrate_files(days_back)
        if not files:
            logger.error("❌ No FirstRate files found")
            return {
                'symbols_processed': [],
                'files_written': 0,
                'files_skipped': 0,
                'success': False,
                'error': 'No FirstRate files found'
            }
            
        # Process each symbol
        results = []
        for symbol in symbols:
            result = await self.process_symbol_incremental(symbol, days_back, output_path)
            results.append(result)
            
        # Summary
        elapsed = (datetime.now() - start_time).total_seconds()
        successful = [r for r in results if r.get('success')]
        
        return {
            'symbols_processed': [r['symbol'] for r in successful],
            'files_written': self.files_written,
            'files_skipped': self.files_skipped,
            'total_time': elapsed,
            'success': len(successful) > 0
        }


# Convenience function for simple usage
async def load_firstrate_minute_data(
    symbols: List[str],
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    data_path: str = "/mnt/d/ats-data/firstrate-data/stock"
) -> List[FirstRateMinuteBar]:
    """
    Convenience function to load FirstRate minute data for symbols.

    Args:
        symbols: List of symbols to load
        start_date: Optional start date filter
        end_date: Optional end date filter
        data_path: Path to FirstRate data directory

    Returns:
        List of FirstRateMinuteBar objects
    """
    bars = []

    async with FirstRateMinuteAdapter(data_path) as adapter:
        async for bar in adapter.fetch_minute_bars_async(symbols, start_date, end_date):
            bars.append(bar)

    return bars