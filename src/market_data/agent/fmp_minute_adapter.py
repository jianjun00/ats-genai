"""
Financial Modeling Prep (FMP) Adapter for 1-Minute Interval Data

Enhanced FMP adapter specifically designed for 1-minute OHLCV data ingestion
to support unified cross-vendor data collection.
"""

import os
import requests
import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
import logging

from .base_adapter import VendorAdapter
from .models import InstrumentMetadata

logger = logging.getLogger(__name__)


@dataclass
class FMPMinuteBar:
    """FMP 1-minute OHLCV bar data structure."""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vendor: str = "fmp"


class FMPMinuteAdapter(VendorAdapter):
    """
    Financial Modeling Prep adapter for 1-minute interval data ingestion.
    
    Supports intraday data fetching for cross-vendor validation.
    """
    
    vendor_name = "fmp"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("FMP_API_KEY")
        if not self.api_key:
            raise ValueError("FMP_API_KEY environment variable must be set")
        
        # Rate limiting configuration
        self.rate_limit_calls = 250  # calls per minute for free tier
        self.rate_limit_premium = 300  # calls per minute for premium
        self.session = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    def get_minute_bars_url(self, symbol: str, date_str: str) -> str:
        """Construct URL for 1-minute intraday data."""
        return (
            f"https://financialmodelingprep.com/api/v3/historical-chart/1min/{symbol}"
            f"?from={date_str}&to={date_str}&apikey={self.api_key}"
        )
    
    async def fetch_minute_bars_async(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[FMPMinuteBar]:
        """
        Fetch 1-minute bars asynchronously.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            start_date: Start date for data
            end_date: End date for data
        
        Returns:
            List of FMPMinuteBar objects
        """
        if not self.session:
            raise RuntimeError("Must use async context manager")
        
        bars = []
        current_date = start_date.date()
        end_date_only = end_date.date()
        
        # FMP requires daily requests for minute data
        while current_date <= end_date_only:
            date_str = current_date.strftime("%Y-%m-%d")
            day_bars = await self._fetch_single_day(symbol, date_str)
            bars.extend(day_bars)
            
            current_date += timedelta(days=1)
            
            # Rate limiting delay
            await asyncio.sleep(0.25)  # 4 calls per second = 240 calls per minute
        
        return bars
    
    async def _fetch_single_day(self, symbol: str, date_str: str) -> List[FMPMinuteBar]:
        """Fetch data for a single day."""
        url = self.get_minute_bars_url(symbol, date_str)
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_minute_bars(symbol, data)
                elif response.status == 429:
                    # Rate limit exceeded
                    logger.warning(f"Rate limit exceeded for {symbol} on {date_str}")
                    await asyncio.sleep(60)  # Wait 1 minute
                    return await self._fetch_single_day(symbol, date_str)
                elif response.status == 404:
                    # No data available
                    logger.debug(f"No data for {symbol} on {date_str}")
                    return []
                else:
                    logger.error(f"FMP API error for {symbol} on {date_str}: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching data for {symbol} on {date_str}: {e}")
            return []
    
    def _parse_minute_bars(self, symbol: str, data: List[Dict]) -> List[FMPMinuteBar]:
        """Parse FMP API response into FMPMinuteBar objects."""
        bars = []
        
        if not data:
            return bars
        
        for item in data:
            try:
                # FMP timestamp format: "2024-01-01 09:30:00"
                timestamp = datetime.strptime(item['date'], "%Y-%m-%d %H:%M:%S")
                
                bar = FMPMinuteBar(
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
    ) -> List[FMPMinuteBar]:
        """Synchronous version for backward compatibility."""
        bars = []
        current_date = start_date.date()
        end_date_only = end_date.date()
        
        while current_date <= end_date_only:
            date_str = current_date.strftime("%Y-%m-%d")
            url = self.get_minute_bars_url(symbol, date_str)
            
            try:
                response = requests.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    day_bars = self._parse_minute_bars(symbol, data)
                    bars.extend(day_bars)
                elif response.status_code == 429:
                    logger.warning(f"Rate limit exceeded for {symbol} on {date_str}")
                elif response.status_code == 404:
                    logger.debug(f"No data for {symbol} on {date_str}")
                else:
                    logger.error(f"FMP API error for {symbol} on {date_str}: {response.status_code}")
                
                current_date += timedelta(days=1)
                
                # Rate limiting delay
                import time
                time.sleep(0.25)
                
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
        max_concurrent: int = 3
    ) -> Dict[str, List[FMPMinuteBar]]:
        """Fetch minute bars for multiple symbols concurrently."""
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(symbol: str) -> tuple[str, List[FMPMinuteBar]]:
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
    
    def validate_data_quality(self, bars: List[FMPMinuteBar]) -> Dict[str, Any]:
        """Validate data quality for FMP minute bars."""
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
        
        outlier_threshold = 0.15  # 15% price change (more lenient than Polygon)
        outliers = [i for i, change in enumerate(price_changes) 
                   if change > outlier_threshold]
        
        # Check volume consistency
        volumes = [bar.volume for bar in bars if bar.volume > 0]
        avg_volume = sum(volumes) / len(volumes) if volumes else 0
        zero_volume_count = sum(1 for bar in bars if bar.volume == 0)
        
        quality_metrics = {
            "valid": len(gaps) < len(bars) * 0.15,  # Less than 15% gaps
            "total_bars": len(bars),
            "time_gaps": len(gaps),
            "gap_details": gaps[:10],
            "price_outliers": len(outliers),
            "zero_volume_bars": zero_volume_count,
            "avg_volume": avg_volume,
            "data_completeness": (len(bars) - len(gaps)) / len(bars) if bars else 0,
            "vendor": "fmp"
        }
        
        return quality_metrics
    
    # Implement required abstract methods
    def fetch_instruments(self) -> List[InstrumentMetadata]:
        """Fetch instrument metadata."""
        # FMP doesn't have a dedicated instruments endpoint, so we'll return empty
        # In practice, you'd maintain a symbol list or use the stock list endpoint
        return []
    
    def fetch_eod(self, symbols: List[str], start_date, end_date):
        """Fetch EOD data - delegate to main FMP adapter."""
        raise NotImplementedError("Use FMP daily price adapter for EOD data")
    
    def fetch_ticks(self, symbol: str, start_dt, end_dt):
        """Fetch tick data - not implemented."""
        raise NotImplementedError("Tick data not implemented")
    
    def fetch_interval(self, symbol: str, interval: str, start_dt, end_dt):
        """Fetch interval data - implemented for 1-minute only."""
        if interval != "1min":
            raise ValueError("Only 1-minute intervals supported")
        
        return self.fetch_minute_bars_sync(symbol, start_dt, end_dt)


# Convenience functions
async def fetch_fmp_minute_data(
    symbols: List[str],
    start_date: datetime,
    end_date: datetime,
    api_key: Optional[str] = None
) -> Dict[str, List[FMPMinuteBar]]:
    """
    Convenience function to fetch 1-minute data from FMP.
    
    Args:
        symbols: List of stock symbols
        start_date: Start date
        end_date: End date
        api_key: FMP API key (optional)
    
    Returns:
        Dictionary mapping symbols to minute bars
    """
    async with FMPMinuteAdapter(api_key) as adapter:
        return await adapter.fetch_multiple_symbols_async(
            symbols, start_date, end_date
        )


def backfill_fmp_minute_data(
    symbol: str,
    days_back: int = 30,
    api_key: Optional[str] = None
) -> List[FMPMinuteBar]:
    """
    Backfill minute data for a single symbol from FMP.
    
    Args:
        symbol: Stock symbol
        days_back: Number of days to backfill
        api_key: FMP API key (optional)
    
    Returns:
        List of minute bars
    """
    adapter = FMPMinuteAdapter(api_key)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    
    return adapter.fetch_minute_bars_sync(
        symbol, 
        datetime.combine(start_date, datetime.min.time()),
        datetime.combine(end_date, datetime.min.time())
    )