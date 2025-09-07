"""
Tiingo Intraday Data Adapter for 1-Minute Intervals

Enhanced adapter for Tiingo's IEX intraday data to support 1-minute bars
for unified cross-vendor data reconciliation with Polygon.
"""

import os
import asyncio
import aiohttp
import requests
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
import logging
import pandas as pd

from .base_adapter import VendorAdapter
from .models import InstrumentMetadata

logger = logging.getLogger(__name__)


@dataclass
class TiingoMinuteBar:
    """Tiingo 1-minute OHLCV bar data structure."""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vendor: str = "tiingo"


class TiingoIntradayAdapter(VendorAdapter):
    """
    Tiingo adapter for intraday data ingestion.

    Uses Tiingo's IEX intraday endpoint to fetch 1-minute equivalent data
    for cross-vendor validation with Polygon data.
    """

    vendor_name = "tiingo"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("TIINGO_API_KEY")
        if not self.api_key:
            raise ValueError("TIINGO_API_KEY environment variable must be set")

        # Rate limiting configuration
        self.rate_limit_calls = 500  # calls per day for free tier
        self.rate_limit_premium = 50000  # calls per day for premium
        self.session = None

        # Base URLs for different endpoints
        self.base_urls = {
            'iex_intraday': 'https://api.tiingo.com/iex',
            'daily': 'https://api.tiingo.com/tiingo/daily',
            'crypto': 'https://api.tiingo.com/tiingo/crypto/prices'
        }

    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()

    def get_intraday_url(self, symbol: str, start_date: str, end_date: str) -> str:
        """Construct URL for IEX intraday data."""
        return (
            f"{self.base_urls['iex_intraday']}/{symbol}/prices"
            f"?startDate={start_date}&endDate={end_date}"
            f"&resampleFreq=1min&token={self.api_key}"
        )

    async def fetch_minute_bars_async(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[TiingoMinuteBar]:
        """
        Fetch 1-minute equivalent bars from Tiingo IEX data.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            start_date: Start date for data
            end_date: End date for data

        Returns:
            List of TiingoMinuteBar objects
        """
        if not self.session:
            raise RuntimeError("Must use async context manager")

        # Tiingo uses different date format
        start_date.strftime("%Y-%m-%d")
        end_date.strftime("%Y-%m-%d")

        # Split into daily chunks for large date ranges
        bars = []
        current_date = start_date.date()
        end_date_only = end_date.date()

        while current_date <= end_date_only:
            # Process one day at a time to avoid rate limits
            day_str = current_date.strftime("%Y-%m-%d")
            day_bars = await self._fetch_single_day(symbol, day_str)
            bars.extend(day_bars)

            current_date += timedelta(days=1)

            # Small delay to respect rate limits
            await asyncio.sleep(0.1)

        return bars

    async def _fetch_single_day(self, symbol: str, date_str: str) -> List[TiingoMinuteBar]:
        """Fetch data for a single day."""
        url = self.get_intraday_url(symbol, date_str, date_str)

        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_intraday_data(symbol, data)
                elif response.status == 429:
                    # Rate limit exceeded
                    logger.warning(f"Rate limit exceeded for {symbol} on {date_str}")
                    await asyncio.sleep(60)  # Wait 1 minute
                    return await self._fetch_single_day(symbol, date_str)
                elif response.status == 404:
                    # No data for this date
                    logger.debug(f"No data for {symbol} on {date_str}")
                    return []
                else:
                    logger.error(f"API error for {symbol} on {date_str}: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching data for {symbol} on {date_str}: {e}")
            return []

    def _parse_intraday_data(self, symbol: str, data: List[Dict]) -> List[TiingoMinuteBar]:
        """Parse Tiingo IEX intraday response into TiingoMinuteBar objects."""
        bars = []

        if not data:
            return bars

        for item in data:
            try:
                # Tiingo IEX format: timestamp is in ISO format
                timestamp = pd.to_datetime(item['date'])

                bar = TiingoMinuteBar(
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
    ) -> List[TiingoMinuteBar]:
        """Synchronous version for backward compatibility."""
        # Use daily endpoint and resample to 1-minute
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        url = f"{self.base_urls['daily']}/{symbol}/prices"
        params = {
            'startDate': start_str,
            'endDate': end_str,
            'resampleFreq': '1min',
            'token': self.api_key
        }

        try:
            response = requests.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                return self._parse_daily_resampled(symbol, data)
            elif response.status_code == 429:
                logger.warning(f"Rate limit exceeded for {symbol}")
                return []
            else:
                logger.error(f"API error for {symbol}: {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"Error fetching minute bars for {symbol}: {e}")
            return []

    def _parse_daily_resampled(self, symbol: str, data: List[Dict]) -> List[TiingoMinuteBar]:
        """Parse daily data resampled to 1-minute intervals."""
        bars = []

        for item in data:
            try:
                timestamp = pd.to_datetime(item['date'])

                bar = TiingoMinuteBar(
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
                logger.warning(f"Error parsing resampled bar for {symbol}: {e}")
                continue

        return bars

    async def fetch_multiple_symbols_async(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        max_concurrent: int = 3  # Lower than Polygon due to rate limits
    ) -> Dict[str, List[TiingoMinuteBar]]:
        """Fetch minute bars for multiple symbols concurrently."""
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_with_semaphore(symbol: str) -> tuple[str, List[TiingoMinuteBar]]:
            async with semaphore:
                bars = await self.fetch_minute_bars_async(symbol, start_date, end_date)
                await asyncio.sleep(0.2)  # Longer delay for Tiingo rate limits
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

    def validate_data_quality(self, bars: List[TiingoMinuteBar]) -> Dict[str, Any]:
        """Validate data quality for cross-vendor comparison."""
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

        outlier_threshold = 0.1  # 10% price change
        outliers = [i for i, change in enumerate(price_changes)
                   if change > outlier_threshold]

        # Check volume consistency
        volumes = [bar.volume for bar in bars if bar.volume > 0]
        avg_volume = sum(volumes) / len(volumes) if volumes else 0
        zero_volume_count = sum(1 for bar in bars if bar.volume == 0)

        quality_metrics = {
            "valid": len(gaps) < len(bars) * 0.1,  # Less than 10% gaps (more lenient than Polygon)
            "total_bars": len(bars),
            "time_gaps": len(gaps),
            "gap_details": gaps[:10],
            "price_outliers": len(outliers),
            "zero_volume_bars": zero_volume_count,
            "avg_volume": avg_volume,
            "data_completeness": (len(bars) - len(gaps)) / len(bars) if bars else 0,
            "vendor": "tiingo"
        }

        return quality_metrics

    # Implement required abstract methods
    def fetch_instruments(self) -> List[InstrumentMetadata]:
        """Fetch instrument metadata."""
        # Use existing implementation from base TiingoAdapter
        url = f"https://api.tiingo.com/tiingo/supported-tickers?token={self.api_key}"
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()

        instruments = []
        for row in data:
            instruments.append(InstrumentMetadata(
                instrument_id=row.get("ticker"),
                symbol=row.get("ticker"),
                name=row.get("name"),
                exchange=row.get("exchange"),
                sector=None,
                list_date=None,
                delist_date=None,
                vendor=self.vendor_name,
                extra=row
            ))
        return instruments

    def fetch_eod(self, symbols: List[str], start_date, end_date):
        """Fetch EOD data - delegate to main TiingoAdapter."""
        raise NotImplementedError("Use TiingoAdapter for EOD data")

    def fetch_ticks(self, symbol: str, start_dt, end_dt):
        """Fetch tick data - not implemented."""
        raise NotImplementedError("Tick data not implemented")

    def fetch_interval(self, symbol: str, interval: str, start_dt, end_dt):
        """Fetch interval data - implemented for 1-minute only."""
        if interval != "1min":
            raise ValueError("Only 1-minute intervals supported")

        return self.fetch_minute_bars_sync(symbol, start_dt, end_dt)


# Convenience functions
async def fetch_tiingo_minute_data(
    symbols: List[str],
    start_date: datetime,
    end_date: datetime,
    api_key: Optional[str] = None
) -> Dict[str, List[TiingoMinuteBar]]:
    """
    Convenience function to fetch 1-minute data from Tiingo.

    Args:
        symbols: List of stock symbols
        start_date: Start date
        end_date: End date
        api_key: Tiingo API key (optional)

    Returns:
        Dictionary mapping symbols to minute bars
    """
    async with TiingoIntradayAdapter(api_key) as adapter:
        return await adapter.fetch_multiple_symbols_async(
            symbols, start_date, end_date
        )


def backfill_tiingo_minute_data(
    symbol: str,
    days_back: int = 30,
    api_key: Optional[str] = None
) -> List[TiingoMinuteBar]:
    """
    Backfill minute data for a single symbol from Tiingo.

    Args:
        symbol: Stock symbol
        days_back: Number of days to backfill
        api_key: Tiingo API key (optional)

    Returns:
        List of minute bars
    """
    adapter = TiingoIntradayAdapter(api_key)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)

    return adapter.fetch_minute_bars_sync(
        symbol,
        datetime.combine(start_date, datetime.min.time()),
        datetime.combine(end_date, datetime.min.time())
    )