import zipfile
from datetime import datetime, date
from typing import List, Optional, Dict, Generator
from zoneinfo import ZoneInfo
from pathlib import Path
import logging
from .base_adapter import VendorAdapter
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Tick:
    """Simple tick data structure for FirstRate processing."""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vendor: str


class FirstRateAdapter(VendorAdapter):
    """
    Adapter for processing FirstRate historical minute data from zip files.
    Data format: timestamp,open,high,low,close,volume (EDT timezone)
    """
    vendor_name = "firstrate"

    def __init__(self, data_path: str = "/mnt/d/ats-data/firstrate-data"):
        self.data_path = Path(data_path)
        self.edt_tz = ZoneInfo("America/New_York")  # EDT/EST
        self.utc_tz = ZoneInfo("UTC")

        # Available data types
        self.data_types = {
            'stock': self.data_path / 'stock',
            'etf': self.data_path / 'etf',
            'fx': self.data_path / 'fx',
            'index': self.data_path / 'index'
        }

    def get_available_zip_files(self, asset_type: str = 'stock') -> List[Path]:
        """Get all zip files for a given asset type."""
        data_dir = self.data_types.get(asset_type)
        if not data_dir or not data_dir.exists():
            return []

        return list(data_dir.glob(f"{asset_type}_*.zip"))

    def extract_symbols_from_zip(self, zip_path: Path) -> List[str]:
        """Extract list of symbols available in a zip file."""
        symbols = []
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                for filename in zf.namelist():
                    if filename.endswith('_full_1min_adjsplitdiv.txt'):
                        symbol = filename.replace('_full_1min_adjsplitdiv.txt', '')
                        symbols.append(symbol)
        except Exception as e:
            logger.error(f"Error reading zip file {zip_path}: {e}")

        return symbols

    def process_minute_data_from_zip(
        self,
        zip_path: Path,
        symbol: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Generator[Tick, None, None]:
        """
        Process minute data for a specific symbol from zip file.
        Yields Tick objects with proper UTC conversion.
        """
        filename = f"{symbol}_full_1min_adjsplitdiv.txt"

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                if filename not in zf.namelist():
                    logger.warning(f"Symbol {symbol} not found in {zip_path}")
                    return

                with zf.open(filename) as file:
                    for line_bytes in file:
                        line = line_bytes.decode('utf-8').strip()
                        if not line:
                            continue

                        try:
                            parts = line.split(',')
                            if len(parts) != 6:
                                continue

                            # Parse EDT timestamp
                            timestamp_str = parts[0]
                            edt_dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                            edt_dt = edt_dt.replace(tzinfo=self.edt_tz)

                            # Convert to UTC
                            utc_dt = edt_dt.astimezone(self.utc_tz)

                            # Filter by date range if specified
                            if start_date and utc_dt.date() < start_date:
                                continue
                            if end_date and utc_dt.date() > end_date:
                                continue

                            # Parse OHLCV data
                            open_price = float(parts[1])
                            high_price = float(parts[2])
                            low_price = float(parts[3])
                            close_price = float(parts[4])
                            volume = int(parts[5])

                            yield Tick(
                                symbol=symbol,
                                timestamp=utc_dt,
                                open=open_price,
                                high=high_price,
                                low=low_price,
                                close=close_price,
                                volume=volume,
                                vendor=self.vendor_name
                            )

                        except (ValueError, IndexError) as e:
                            logger.warning(f"Error parsing line '{line}' for {symbol}: {e}")
                            continue

        except Exception as e:
            logger.error(f"Error processing {symbol} from {zip_path}: {e}")

    def get_date_range_for_symbol(self, zip_path: Path, symbol: str) -> tuple[Optional[date], Optional[date]]:
        """Get the date range available for a symbol in a zip file."""
        filename = f"{symbol}_full_1min_adjsplitdiv.txt"
        min_date = None
        max_date = None

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                if filename not in zf.namelist():
                    return None, None

                with zf.open(filename) as file:
                    # Read first line for min date
                    first_line = file.readline().decode('utf-8').strip()
                    if first_line:
                        timestamp_str = first_line.split(',')[0]
                        min_date = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S').date()

                    # Read last few lines to find max date
                    file.seek(-10000, 2)  # Seek near end
                    lines = file.read().decode('utf-8').strip().split('\n')
                    for line in reversed(lines):
                        if line.strip():
                            timestamp_str = line.split(',')[0]
                            max_date = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S').date()
                            break

        except Exception as e:
            logger.error(f"Error getting date range for {symbol} from {zip_path}: {e}")

        return min_date, max_date

    def get_symbol_inventory(self, asset_type: str = 'stock') -> Dict[str, Dict]:
        """Get inventory of all symbols and their date ranges across all zip files."""
        inventory = {}
        zip_files = self.get_available_zip_files(asset_type)

        for zip_path in zip_files:
            logger.info(f"Processing inventory for {zip_path.name}")
            symbols = self.extract_symbols_from_zip(zip_path)

            for symbol in symbols:
                min_date, max_date = self.get_date_range_for_symbol(zip_path, symbol)

                if symbol not in inventory:
                    inventory[symbol] = {
                        'zip_files': [],
                        'min_date': min_date,
                        'max_date': max_date,
                        'total_files': 0
                    }

                inventory[symbol]['zip_files'].append(str(zip_path))
                inventory[symbol]['total_files'] += 1

                # Update date range
                if min_date and (not inventory[symbol]['min_date'] or min_date < inventory[symbol]['min_date']):
                    inventory[symbol]['min_date'] = min_date
                if max_date and (not inventory[symbol]['max_date'] or max_date > inventory[symbol]['max_date']):
                    inventory[symbol]['max_date'] = max_date

        return inventory

    # Base adapter interface implementations (not used for file-based processing)
    def fetch_instruments(self):
        """Get list of available symbols from zip files."""
        inventory = self.get_symbol_inventory('stock')
        return list(inventory.keys())

    def fetch_eod(self, symbols, start_date, end_date):
        """Not implemented for FirstRate (minute data only)."""
        raise NotImplementedError("FirstRate adapter provides minute data only")

    def fetch_ticks(self, symbol, start_dt, end_dt):
        """Not implemented for FirstRate (use process_minute_data_from_zip)."""
        raise NotImplementedError("Use process_minute_data_from_zip for FirstRate data")

    def fetch_interval(self, symbol, interval, start_dt, end_dt):
        """Not implemented for FirstRate (use process_minute_data_from_zip)."""
        raise NotImplementedError("Use process_minute_data_from_zip for FirstRate data")