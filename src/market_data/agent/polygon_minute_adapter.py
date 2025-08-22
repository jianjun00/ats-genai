"""
Polygon Adapter for 1-Minute Interval Data

Enhanced Polygon adapter specifically designed for 1-minute OHLCV data ingestion
to support TFT model training and real-time inference.
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
class MinuteBar:
    """1-minute OHLCV bar data structure."""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float] = None  # Volume weighted average price
    trade_count: Optional[int] = None
    vendor: str = "polygon"


class PolygonMinuteAdapter(VendorAdapter):
    """
    Polygon adapter for 1-minute interval data ingestion.
    
    Supports both historical backfill and real-time streaming for TFT models.
    """
    
    vendor_name = "polygon"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY environment variable must be set")
        
        # Rate limiting configuration
        self.rate_limit_calls = 5  # calls per minute for free tier
        self.rate_limit_premium = 100  # calls per minute for premium
        self.session = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    def get_minute_bars_url(self, symbol: str, start_date: str, end_date: str) -> str:
        """Construct URL for 1-minute aggregates."""
        return (
            f"https://api.polygon.io/v2/aggs/ticker/{symbol}/"
            f"range/1/minute/{start_date}/{end_date}"
            f"?adjusted=true&sort=asc&limit=50000&apiKey={self.api_key}"
        )
    
    async def fetch_minute_bars_async(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[MinuteBar]:
        """
        Fetch 1-minute bars asynchronously.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            start_date: Start date for data
            end_date: End date for data
        
        Returns:
            List of MinuteBar objects
        """
        if not self.session:
            raise RuntimeError("Must use async context manager")
        
        # Format dates for API
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        url = self.get_minute_bars_url(symbol, start_str, end_str)
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_minute_bars(symbol, data)
                elif response.status == 429:
                    # Rate limit exceeded
                    logger.warning(f"Rate limit exceeded for {symbol}, retrying...")
                    await asyncio.sleep(60)  # Wait 1 minute
                    return await self.fetch_minute_bars_async(symbol, start_date, end_date)
                else:
                    logger.error(f"API error for {symbol}: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching minute bars for {symbol}: {e}")
            return []
    
    def _parse_minute_bars(self, symbol: str, data: Dict[str, Any]) -> List[MinuteBar]:
        """Parse Polygon API response into MinuteBar objects."""
        bars = []
        
        if "results" not in data:
            logger.warning(f"No results in response for {symbol}")
            return bars
        
        for result in data["results"]:
            try:
                timestamp = datetime.utcfromtimestamp(result["t"] / 1000)
                
                bar = MinuteBar(
                    symbol=symbol,
                    timestamp=timestamp,
                    open=float(result["o"]),
                    high=float(result["h"]),
                    low=float(result["l"]),
                    close=float(result["c"]),
                    volume=int(result["v"]),
                    vwap=result.get("vw"),  # Volume weighted average price
                    trade_count=result.get("n")  # Number of trades
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
    ) -> List[MinuteBar]:
        """
        Synchronous version for backward compatibility.
        
        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date
        
        Returns:
            List of MinuteBar objects
        """
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        url = self.get_minute_bars_url(symbol, start_str, end_str)
        
        try:
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                return self._parse_minute_bars(symbol, data)
            elif response.status_code == 429:
                logger.warning(f"Rate limit exceeded for {symbol}")
                return []
            else:
                logger.error(f"API error for {symbol}: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching minute bars for {symbol}: {e}")
            return []
    
    async def fetch_multiple_symbols_async(
        self, 
        symbols: List[str], 
        start_date: datetime, 
        end_date: datetime,
        max_concurrent: int = 5
    ) -> Dict[str, List[MinuteBar]]:
        """
        Fetch minute bars for multiple symbols concurrently.
        
        Args:
            symbols: List of stock symbols
            start_date: Start date
            end_date: End date
            max_concurrent: Maximum concurrent requests
        
        Returns:
            Dictionary mapping symbols to their minute bars
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(symbol: str) -> tuple[str, List[MinuteBar]]:
            async with semaphore:
                bars = await self.fetch_minute_bars_async(symbol, start_date, end_date)
                await asyncio.sleep(0.1)  # Small delay to respect rate limits
                return symbol, bars
        
        tasks = [fetch_with_semaphore(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and handle exceptions
        symbol_data = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Exception in concurrent fetch: {result}")
                continue
            
            symbol, bars = result
            symbol_data[symbol] = bars
        
        return symbol_data
    
    def calculate_technical_indicators(self, bars: List[MinuteBar]) -> List[Dict[str, Any]]:
        """
        Calculate technical indicators for TFT model features.
        
        Args:
            bars: List of minute bars
        
        Returns:
            List of dictionaries with technical indicators
        """
        if len(bars) < 50:  # Need minimum data for indicators
            return []
        
        import pandas as pd
        import numpy as np
        
        # Convert to DataFrame for calculation
        df = pd.DataFrame([
            {
                'timestamp': bar.timestamp,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume
            }
            for bar in bars
        ])
        
        # Calculate returns
        df['returns'] = df['close'].pct_change()
        
        # Simple moving averages
        df['sma_5'] = df['close'].rolling(window=5).mean()
        df['sma_20'] = df['close'].rolling(window=20).mean()
        
        # Exponential moving averages
        df['ema_12'] = df['close'].ewm(span=12).mean()
        df['ema_26'] = df['close'].ewm(span=26).mean()
        
        # MACD
        df['macd'] = df['ema_12'] - df['ema_26']
        df['macd_signal'] = df['macd'].ewm(span=9).mean()
        
        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # Bollinger Bands
        df['bb_middle'] = df['close'].rolling(window=20).mean()
        bb_std = df['close'].rolling(window=20).std()
        df['bb_upper'] = df['bb_middle'] + (2 * bb_std)
        df['bb_lower'] = df['bb_middle'] - (2 * bb_std)
        
        # Volume indicators
        df['volume_sma'] = df['volume'].rolling(window=20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma']
        
        # Volatility
        df['volatility'] = df['returns'].rolling(window=20).std() * np.sqrt(252 * 390)  # Annualized
        
        return df.to_dict('records')
    
    def validate_data_quality(self, bars: List[MinuteBar]) -> Dict[str, Any]:
        """
        Validate data quality for TFT model consumption.
        
        Args:
            bars: List of minute bars
        
        Returns:
            Data quality metrics
        """
        if not bars:
            return {"valid": False, "reason": "No data"}
        
        # Check for gaps in time series
        timestamps = sorted([bar.timestamp for bar in bars])
        gaps = []
        
        for i in range(1, len(timestamps)):
            expected_next = timestamps[i-1] + timedelta(minutes=1)
            if timestamps[i] > expected_next + timedelta(seconds=30):  # Allow 30s tolerance
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
            "valid": len(gaps) < len(bars) * 0.05,  # Less than 5% gaps
            "total_bars": len(bars),
            "time_gaps": len(gaps),
            "gap_details": gaps[:10],  # First 10 gaps
            "price_outliers": len(outliers),
            "zero_volume_bars": zero_volume_count,
            "avg_volume": avg_volume,
            "data_completeness": (len(bars) - len(gaps)) / len(bars) if bars else 0
        }
        
        return quality_metrics
    
    # Implement required abstract methods
    def fetch_instruments(self) -> List[InstrumentMetadata]:
        """Fetch instrument metadata - not implemented for minute data."""
        raise NotImplementedError("Use PolygonAdapter for instrument metadata")
    
    def fetch_eod(self, symbols: List[str], start_date, end_date):
        """Fetch EOD data - not implemented for minute data."""
        raise NotImplementedError("Use PolygonAdapter for EOD data")
    
    def fetch_ticks(self, symbol: str, start_dt, end_dt):
        """Fetch tick data - not implemented."""
        raise NotImplementedError("Tick data not implemented")
    
    def fetch_interval(self, symbol: str, interval: str, start_dt, end_dt):
        """Fetch interval data - implemented for 1-minute only."""
        if interval != "1min":
            raise ValueError("Only 1-minute intervals supported")
        
        return self.fetch_minute_bars_sync(symbol, start_dt, end_dt)


# Convenience functions
async def fetch_minute_data_for_tft(
    symbols: List[str],
    start_date: datetime,
    end_date: datetime,
    api_key: Optional[str] = None
) -> Dict[str, List[MinuteBar]]:
    """
    Convenience function to fetch 1-minute data for TFT training.
    
    Args:
        symbols: List of stock symbols
        start_date: Start date
        end_date: End date
        api_key: Polygon API key (optional)
    
    Returns:
        Dictionary mapping symbols to minute bars
    """
    async with PolygonMinuteAdapter(api_key) as adapter:
        return await adapter.fetch_multiple_symbols_async(
            symbols, start_date, end_date
        )


def backfill_minute_data(
    symbol: str,
    days_back: int = 30,
    api_key: Optional[str] = None
) -> List[MinuteBar]:
    """
    Backfill minute data for a single symbol.
    
    Args:
        symbol: Stock symbol
        days_back: Number of days to backfill
        api_key: Polygon API key (optional)
    
    Returns:
        List of minute bars
    """
    adapter = PolygonMinuteAdapter(api_key)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    
    return adapter.fetch_minute_bars_sync(
        symbol, 
        datetime.combine(start_date, datetime.min.time()),
        datetime.combine(end_date, datetime.min.time())
    )