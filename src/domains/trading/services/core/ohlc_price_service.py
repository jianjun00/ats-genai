#!/usr/bin/env python3
"""
OHLC Price Service Backend

High-performance price data service for news visualization and analytics.
Provides REST API endpoints for OHLC data retrieval with caching and
integration with existing market data infrastructure.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

import asyncpg
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
import uvicorn

from src.core.config.database import Database

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Timeframe(str, Enum):
    """Supported OHLC timeframes"""
    HOUR = "1h"
    DAY = "1d"


@dataclass
class OHLCData:
    """OHLC data point"""
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int


@dataclass
class NewsEvent:
    """News event with metadata"""
    news_id: str
    ticker: str
    published_utc: datetime
    signal_type: str
    confidence: float
    sentiment: str
    title: str


class OHLCPriceService:
    """High-performance OHLC price data service"""

    def __init__(self):
        self.db = Database()
        self.pool: Optional[asyncpg.Pool] = None
        self.cache_ttl = timedelta(hours=1)  # Cache TTL for price data

    async def initialize(self):
        """Initialize database connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.db.host,
                port=self.db.port,
                user=self.db.user,
                password=self.db.password,
                database=self.db.database,
                min_size=5,
                max_size=20
            )
            logger.info("✅ OHLC Price Service initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize OHLC Price Service: {e}")
            raise

    async def close(self):
        """Close database connections"""
        if self.pool:
            await self.pool.close()

    async def get_ohlc_data(
        self,
        ticker: str,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime
    ) -> List[OHLCData]:
        """
        Retrieve OHLC data for a ticker and timeframe

        Args:
            ticker: Stock symbol
            timeframe: 1h or 1d
            start_date: Start date for data
            end_date: End date for data

        Returns:
            List of OHLC data points
        """
        # First try to get from cache
        cached_data = await self._get_cached_data(ticker, timeframe, start_date, end_date)
        if cached_data:
            logger.info(f"📊 Retrieved {len(cached_data)} cached OHLC records for {ticker}")
            return cached_data

        # If not cached, get from minute bars and aggregate
        ohlc_data = await self._aggregate_from_minute_bars(
            ticker, timeframe, start_date, end_date
        )

        # Cache the results
        if ohlc_data:
            await self._cache_ohlc_data(ticker, timeframe, ohlc_data)
            logger.info(f"📊 Generated and cached {len(ohlc_data)} OHLC records for {ticker}")

        return ohlc_data

    async def _get_cached_data(
        self,
        ticker: str,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime
    ) -> Optional[List[OHLCData]]:
        """Get OHLC data from cache if available and fresh"""
        if not self.pool:
            return None

        try:
            async with self.pool.acquire() as conn:
                query = """
                    SELECT timestamp, open_price, high_price, low_price, close_price, volume
                    FROM dev_ohlc_cache
                    WHERE ticker = $1
                    AND timeframe = $2
                    AND timestamp >= $3
                    AND timestamp <= $4
                    AND cached_at > $5
                    ORDER BY timestamp
                """

                cache_cutoff = datetime.now() - self.cache_ttl
                rows = await conn.fetch(query, ticker, timeframe.value,
                                      start_date, end_date, cache_cutoff)

                if not rows:
                    return None

                return [
                    OHLCData(
                        timestamp=row['timestamp'],
                        open_price=float(row['open_price']),
                        high_price=float(row['high_price']),
                        low_price=float(row['low_price']),
                        close_price=float(row['close_price']),
                        volume=int(row['volume'])
                    )
                    for row in rows
                ]

        except Exception as e:
            logger.error(f"❌ Error retrieving cached data: {e}")
            return None

    async def _aggregate_from_minute_bars(
        self,
        ticker: str,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime
    ) -> List[OHLCData]:
        """Aggregate OHLC data from minute bars"""
        if not self.pool:
            return []

        try:
            # Get minute bar data from file-based storage
            # This would integrate with FileBasedMinuteManager
            from storage.file_based_minute_manager import FileBasedMinuteManager

            minute_manager = FileBasedMinuteManager()

            # Get minute data for the date range
            minute_data = await minute_manager.get_minute_data(
                ticker, start_date, end_date
            )

            if not minute_data:
                logger.warning(f"⚠️ No minute data found for {ticker}")
                return []

            # Convert to DataFrame for aggregation
            df = pd.DataFrame([
                {
                    'timestamp': bar.timestamp,
                    'open': bar.open_price,
                    'high': bar.high_price,
                    'low': bar.low_price,
                    'close': bar.close_price,
                    'volume': bar.volume
                }
                for bar in minute_data
            ])

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')

            # Aggregate based on timeframe
            if timeframe == Timeframe.HOUR:
                aggregated = df.resample('1H').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna()
            else:  # Daily
                aggregated = df.resample('1D').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna()

            # Convert back to OHLCData objects
            ohlc_data = []
            for timestamp, row in aggregated.iterrows():
                ohlc_data.append(OHLCData(
                    timestamp=timestamp.to_pydatetime(),
                    open_price=float(row['open']),
                    high_price=float(row['high']),
                    low_price=float(row['low']),
                    close_price=float(row['close']),
                    volume=int(row['volume'])
                ))

            return ohlc_data

        except Exception as e:
            logger.error(f"❌ Error aggregating minute data: {e}")
            return []

    async def _cache_ohlc_data(
        self,
        ticker: str,
        timeframe: Timeframe,
        ohlc_data: List[OHLCData]
    ):
        """Cache OHLC data for future requests"""
        if not self.pool or not ohlc_data:
            return

        try:
            async with self.pool.acquire() as conn:
                # Clear existing cache for this ticker/timeframe
                await conn.execute(
                    "DELETE FROM dev_ohlc_cache WHERE ticker = $1 AND timeframe = $2",
                    ticker, timeframe.value
                )

                # Insert new cache data
                values = [
                    (ticker, timeframe.value, data.timestamp,
                     data.open_price, data.high_price, data.low_price,
                     data.close_price, data.volume)
                    for data in ohlc_data
                ]

                await conn.executemany("""
                    INSERT INTO dev_ohlc_cache
                    (ticker, timeframe, timestamp, open_price, high_price,
                     low_price, close_price, volume)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """, values)

                logger.info(f"📊 Cached {len(values)} OHLC records for {ticker}")

        except Exception as e:
            logger.error(f"❌ Error caching OHLC data: {e}")

    async def get_news_events(
        self,
        ticker: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[NewsEvent]:
        """Get news events with signals for visualization"""
        if not self.pool:
            return []

        try:
            async with self.pool.acquire() as conn:
                # Build dynamic query
                conditions = []
                params = []
                param_count = 0

                if ticker:
                    param_count += 1
                    conditions.append(f"ts.ticker = ${param_count}")
                    params.append(ticker)

                if start_date:
                    param_count += 1
                    conditions.append(f"ts.published_utc >= ${param_count}")
                    params.append(start_date)

                if end_date:
                    param_count += 1
                    conditions.append(f"ts.published_utc <= ${param_count}")
                    params.append(end_date)

                where_clause = " AND " + " AND ".join(conditions) if conditions else ""

                query = f"""
                    SELECT
                        ts.news_id,
                        ts.ticker,
                        ts.published_utc,
                        ts.signal_type,
                        ts.confidence,
                        ts.sentiment,
                        np.title
                    FROM dev_trading_signals ts
                    JOIN dev_news_polygon np ON ts.news_id = np.vendor_id
                    WHERE 1=1 {where_clause}
                    ORDER BY ts.published_utc DESC
                    LIMIT 100
                """

                rows = await conn.fetch(query, *params)

                return [
                    NewsEvent(
                        news_id=row['news_id'],
                        ticker=row['ticker'],
                        published_utc=row['published_utc'],
                        signal_type=row['signal_type'],
                        confidence=float(row['confidence']),
                        sentiment=row['sentiment'],
                        title=row['title']
                    )
                    for row in rows
                ]

        except Exception as e:
            logger.error(f"❌ Error retrieving news events: {e}")
            return []


# FastAPI application
app = FastAPI(title="OHLC Price Service", version="1.0.0")
ohlc_service = OHLCPriceService()


@app.on_event("startup")
async def startup():
    """Initialize service on startup"""
    await ohlc_service.initialize()


@app.on_event("shutdown")
async def shutdown():
    """Clean up on shutdown"""
    await ohlc_service.close()


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "OHLC Price Service"}


@app.get("/api/ohlc/{ticker}")
async def get_ohlc(
    ticker: str,
    timeframe: Timeframe = Query(Timeframe.DAY),
    start_date: datetime = Query(...),
    end_date: datetime = Query(...)
) -> Dict[str, Any]:
    """
    Get OHLC data for a ticker and timeframe

    Args:
        ticker: Stock symbol (e.g., AAPL, TSLA)
        timeframe: Data frequency (1h or 1d)
        start_date: Start date (ISO format)
        end_date: End date (ISO format)

    Returns:
        JSON response with OHLC data array
    """
    try:
        ohlc_data = await ohlc_service.get_ohlc_data(
            ticker, timeframe, start_date, end_date
        )

        return {
            "ticker": ticker,
            "timeframe": timeframe.value,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "count": len(ohlc_data),
            "data": [
                {
                    "timestamp": data.timestamp.isoformat(),
                    "open": data.open_price,
                    "high": data.high_price,
                    "low": data.low_price,
                    "close": data.close_price,
                    "volume": data.volume
                }
                for data in ohlc_data
            ]
        }

    except Exception as e:
        logger.error(f"❌ Error in get_ohlc endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/news/events")
async def get_news_events(
    ticker: Optional[str] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None)
) -> Dict[str, Any]:
    """
    Get news events with signals

    Args:
        ticker: Optional stock symbol filter
        start_date: Optional start date filter
        end_date: Optional end date filter

    Returns:
        JSON response with news events array
    """
    try:
        events = await ohlc_service.get_news_events(ticker, start_date, end_date)

        return {
            "count": len(events),
            "events": [
                {
                    "news_id": event.news_id,
                    "ticker": event.ticker,
                    "published_utc": event.published_utc.isoformat(),
                    "signal_type": event.signal_type,
                    "confidence": event.confidence,
                    "sentiment": event.sentiment,
                    "title": event.title
                }
                for event in events
            ]
        }

    except Exception as e:
        logger.error(f"❌ Error in get_news_events endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/news/ohlc/{ticker}")
async def get_news_centered_ohlc(
    ticker: str,
    news_date: datetime = Query(...),
    timeframe: Timeframe = Query(Timeframe.DAY)
) -> Dict[str, Any]:
    """
    Get OHLC data centered around a news event

    Args:
        ticker: Stock symbol
        news_date: Date/time of the news event
        timeframe: Data frequency (1h or 1d)

    Returns:
        JSON response with OHLC data ±10 days/hours around news
    """
    try:
        if timeframe == Timeframe.HOUR:
            # ±10 hours around news
            start_date = news_date - timedelta(hours=10)
            end_date = news_date + timedelta(hours=10)
        else:
            # ±10 days around news
            start_date = news_date - timedelta(days=10)
            end_date = news_date + timedelta(days=10)

        ohlc_data = await ohlc_service.get_ohlc_data(
            ticker, timeframe, start_date, end_date
        )

        return {
            "ticker": ticker,
            "news_date": news_date.isoformat(),
            "timeframe": timeframe.value,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "count": len(ohlc_data),
            "data": [
                {
                    "timestamp": data.timestamp.isoformat(),
                    "open": data.open_price,
                    "high": data.high_price,
                    "low": data.low_price,
                    "close": data.close_price,
                    "volume": data.volume
                }
                for data in ohlc_data
            ]
        }

    except Exception as e:
        logger.error(f"❌ Error in get_news_centered_ohlc endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(
        "ohlc_price_service:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    )