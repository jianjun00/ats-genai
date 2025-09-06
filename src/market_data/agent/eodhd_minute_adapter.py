"""
EODHD (End of Day Historical Data) Adapter for 1-Minute Interval Data

Enhanced EODHD adapter specifically designed for 1-minute OHLCV data ingestion
to support unified cross-vendor data collection.
"""

import os
import requests
import asyncio
import aiohttp
from datetime import datetime, timedelta, date
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
import logging
import pytz

from .base_adapter import VendorAdapter
from .models import InstrumentMetadata

logger = logging.getLogger(__name__)


@dataclass
class EODHDMinuteBar:
    """EODHD 1-minute OHLCV bar data structure."""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vendor: str = "eodhd"


class EODHDMinuteAdapter(VendorAdapter):
    """
    EODHD adapter for 1-minute interval data ingestion.
    
    Uses EODHD's intraday API to fetch 1-minute bars for cross-vendor validation.
    """
    
    vendor_name = "eodhd"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("EODHD_API_KEY")
        if not self.api_key:
            raise ValueError("EODHD_API_KEY environment variable must be set")
        
        # Rate limiting configuration
        self.rate_limit_calls = 20  # calls per minute for free tier
        self.rate_limit_premium = 100  # calls per minute for premium
        self.session = None
        
        # Base URL
        self.base_url = "https://eodhistoricaldata.com/api"
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    def get_minute_bars_url(self, symbol: str, start_timestamp: int, end_timestamp: int) -> str:
        """Construct URL for 1-minute intraday data using Unix timestamps."""
        return (
            f"{self.base_url}/intraday/{symbol}.US"
            f"?api_token={self.api_key}&interval=1m&from={start_timestamp}&to={end_timestamp}&fmt=json"
        )
    
    async def fetch_minute_bars_async(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[EODHDMinuteBar]:
        """
        Fetch 1-minute bars asynchronously.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            start_date: Start date for data
            end_date: End date for data
        
        Returns:
            List of EODHDMinuteBar objects
        """
        if not self.session:
            raise RuntimeError("Must use async context manager")
        
        bars = []
        current_date = start_date.date()
        end_date_only = end_date.date()
        
        # EODHD requires daily requests for minute data
        while current_date <= end_date_only:
            # Calculate Unix timestamps for start and end of day
            start_of_day = datetime.combine(current_date, datetime.min.time())
            end_of_day = datetime.combine(current_date, datetime.max.time())
            
            start_timestamp = int(start_of_day.timestamp())
            end_timestamp = int(end_of_day.timestamp())
            
            day_bars = await self._fetch_single_day(symbol, start_timestamp, end_timestamp, current_date)
            bars.extend(day_bars)
            
            current_date += timedelta(days=1)
            
            # Conservative rate limiting - 3 seconds between calls
            await asyncio.sleep(3.0)
        
        return bars
    
    async def _fetch_single_day(self, symbol: str, start_timestamp: int, end_timestamp: int, date: date) -> List[EODHDMinuteBar]:
        """Fetch data for a single day using Unix timestamps."""
        date_str = date.strftime("%Y-%m-%d")  # For logging only
        url = self.get_minute_bars_url(symbol, start_timestamp, end_timestamp)
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_minute_bars(symbol, data)
                elif response.status == 429:
                    # Rate limit exceeded
                    logger.warning(f"Rate limit exceeded for {symbol} on {date_str}")
                    await asyncio.sleep(60)  # Wait 1 minute
                    return await self._fetch_single_day(symbol, start_timestamp, end_timestamp, date)
                elif response.status == 404:
                    # No data available
                    logger.debug(f"No data for {symbol} on {date_str}")
                    return []
                else:
                    logger.error(f"EODHD API error for {symbol} on {date_str}: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching data for {symbol} on {date_str}: {e}")
            return []
    
    def _parse_minute_bars(self, symbol: str, data: List[Dict]) -> List[EODHDMinuteBar]:
        """Parse EODHD API response into EODHDMinuteBar objects."""
        bars = []
        
        if not data:
            return bars
        
        for item in data:
            try:
                # EODHD timestamp format: "2024-01-01 09:30:00"
                timestamp_str = f"{item['datetime']}"
                # Parse as naive datetime then localize to UTC (EODHD uses UTC timestamps)
                naive_timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                timestamp = pytz.UTC.localize(naive_timestamp)
                
                bar = EODHDMinuteBar(
                    symbol=symbol,
                    timestamp=timestamp,
                    open=float(item['open']),
                    high=float(item['high']),
                    low=float(item['low']),
                    close=float(item['close']),
                    volume=int(item.get('volume', 0))
                )
                bars.append(bar)
                
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Error parsing bar for {symbol}: {e}")
                continue
        
        logger.info(f"Parsed {len(bars)} minute bars for {symbol}")
        return bars
    
    def fetch_minute_bars_sync(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[EODHDMinuteBar]:
        """Synchronous version for backward compatibility."""
        bars = []
        current_date = start_date.date()
        end_date_only = end_date.date()
        
        while current_date <= end_date_only:
            # Calculate Unix timestamps for start and end of day
            start_of_day = datetime.combine(current_date, datetime.min.time())
            end_of_day = datetime.combine(current_date, datetime.max.time())
            
            start_timestamp = int(start_of_day.timestamp())
            end_timestamp = int(end_of_day.timestamp())
            
            date_str = current_date.strftime("%Y-%m-%d")  # For logging only
            url = self.get_minute_bars_url(symbol, start_timestamp, end_timestamp)
            
            try:
                response = requests.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    day_bars = self._parse_minute_bars(symbol, data)
                    bars.extend(day_bars)
                elif response.status_code == 429:
                    logger.warning(f"Rate limit exceeded for {symbol} on {date_str}")
                    import time
                    time.sleep(60)
                elif response.status_code == 404:
                    logger.debug(f"No data for {symbol} on {date_str}")
                else:
                    logger.error(f"EODHD API error for {symbol} on {date_str}: {response.status_code}")
                
                current_date += timedelta(days=1)
                
                # Rate limiting delay
                import time
                time.sleep(3.0)
                
            except Exception as e:
                logger.error(f"Error fetching data for {symbol} on {date_str}: {e}")
                current_date += timedelta(days=1)
                continue
        
        return bars
    
    async def fetch_multiple_symbols_async(
        self, 
        symbols: List[str], 
        start_date: datetime, 
        end_date: datetime,
        max_concurrent: int = 2  # Very conservative due to rate limits
    ) -> Dict[str, List[EODHDMinuteBar]]:
        """Fetch minute bars for multiple symbols concurrently."""
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(symbol: str) -> tuple[str, List[EODHDMinuteBar]]:
            async with semaphore:
                bars = await self.fetch_minute_bars_async(symbol, start_date, end_date)
                return symbol, bars
        
        tasks = [fetch_with_semaphore(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        symbol_data = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Exception in concurrent fetch: {result}")
                continue
            
            symbol, bars = result
            symbol_data[symbol] = bars
        
        return symbol_data
    
    def validate_data_quality(self, bars: List[EODHDMinuteBar]) -> Dict[str, Any]:
        """Validate data quality for EODHD minute bars."""
        if not bars:
            return {"valid": False, "reason": "No data"}
        
        # Check for gaps in time series
        timestamps = sorted([bar.timestamp for bar in bars])
        gaps = []
        
        for i in range(1, len(timestamps)):
            expected_next = timestamps[i-1] + timedelta(minutes=1)
            if timestamps[i] > expected_next + timedelta(seconds=30):
                gaps.append((timestamps[i-1], timestamps[i]))
        
        # Check for outliers in price data
        closes = [bar.close for bar in bars]
        price_changes = [abs(closes[i] - closes[i-1]) / closes[i-1] 
                        for i in range(1, len(closes))]
        
        outlier_threshold = 0.15  # 15% price change
        outliers = [i for i, change in enumerate(price_changes) 
                   if change > outlier_threshold]
        
        # Check volume consistency
        volumes = [bar.volume for bar in bars if bar.volume > 0]
        avg_volume = sum(volumes) / len(volumes) if volumes else 0
        zero_volume_count = sum(1 for bar in bars if bar.volume == 0)
        
        quality_metrics = {
            "valid": len(gaps) < len(bars) * 0.20,  # Less than 20% gaps (most lenient)
            "total_bars": len(bars),
            "time_gaps": len(gaps),
            "gap_details": gaps[:10],
            "price_outliers": len(outliers),
            "zero_volume_bars": zero_volume_count,
            "avg_volume": avg_volume,
            "data_completeness": (len(bars) - len(gaps)) / len(bars) if bars else 0,
            "vendor": "eodhd"
        }
        
        return quality_metrics
    
    # Implement required abstract methods
    def fetch_instruments(self) -> List[InstrumentMetadata]:
        """Fetch instrument metadata."""
        # EODHD has an exchange symbols endpoint
        url = f"{self.base_url}/exchange-symbol-list/US?api_token={self.api_key}&fmt=json"
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                instruments = []
                
                for item in data:
                    instruments.append(InstrumentMetadata(
                        instrument_id=item.get("Code"),
                        symbol=item.get("Code"),
                        name=item.get("Name"),
                        exchange=item.get("Exchange"),
                        sector=item.get("Sector"),
                        list_date=None,
                        delist_date=None,
                        vendor=self.vendor_name,
                        extra=item
                    ))
                
                return instruments[:1000]  # Limit to 1000 instruments
            else:
                logger.error(f"Error fetching instruments: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error fetching instruments: {e}")
            return []
    
    def fetch_eod(self, symbols: List[str], start_date, end_date):
        """Fetch EOD data - not implemented for minute adapter."""
        raise NotImplementedError("Use EODHD daily price adapter for EOD data")
    
    def fetch_ticks(self, symbol: str, start_dt, end_dt):
        """Fetch tick data - not implemented."""
        raise NotImplementedError("Tick data not implemented")
    
    def fetch_interval(self, symbol: str, interval: str, start_dt, end_dt):
        """Fetch interval data - implemented for 1-minute only."""
        if interval != "1min":
            raise ValueError("Only 1-minute intervals supported")
        
        return self.fetch_minute_bars_sync(symbol, start_dt, end_dt)


# Convenience functions
async def fetch_eodhd_minute_data(
    symbols: List[str],
    start_date: datetime,
    end_date: datetime,
    api_key: Optional[str] = None
) -> Dict[str, List[EODHDMinuteBar]]:
    """
    Convenience function to fetch 1-minute data from EODHD.
    
    Args:
        symbols: List of stock symbols
        start_date: Start date
        end_date: End date
        api_key: EODHD API key (optional)
    
    Returns:
        Dictionary mapping symbols to minute bars
    """
    async with EODHDMinuteAdapter(api_key) as adapter:
        return await adapter.fetch_multiple_symbols_async(
            symbols, start_date, end_date
        )


def backfill_eodhd_minute_data(
    symbol: str,
    days_back: int = 30,
    api_key: Optional[str] = None
) -> List[EODHDMinuteBar]:
    """
    Backfill minute data for a single symbol from EODHD.
    
    Args:
        symbol: Stock symbol
        days_back: Number of days to backfill
        api_key: EODHD API key (optional)
    
    Returns:
        List of minute bars
    """
    adapter = EODHDMinuteAdapter(api_key)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    
    return adapter.fetch_minute_bars_sync(
        symbol, 
        datetime.combine(start_date, datetime.min.time()),
        datetime.combine(end_date, datetime.min.time())
    )