#!/usr/bin/env python3
"""
Real-Time Minute Bar Collector for ATS-INTG

Continuously collects 1-minute bar data from Polygon and Tiingo APIs
and stores in INTG database for real-time trading applications.

Usage:
    python3 scripts/realtime_minute_collector.py --symbols AAPL,TSLA,SPY
    python3 scripts/realtime_minute_collector.py --production  # All symbols
"""

import asyncio
import asyncpg
import logging
import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import aiohttp
import time

# Add src to Python path
sys.path.insert(0, '/workspace/src' if os.path.exists('/workspace/src') else 'src')

logger = logging.getLogger(__name__)

class RealTimeMinuteCollector:
    """Real-time minute bar collector for ATS-INTG."""

    def __init__(self, symbols: List[str], db_url: str):
        self.symbols = symbols
        self.db_url = db_url
        self.db_pool = None
        self.polygon_api_key = os.getenv('POLYGON_API_KEY')
        self.tiingo_api_key = os.getenv('TIINGO_API_KEY')
        self.running = False

    async def initialize(self):
        """Initialize database connections."""
        try:
            self.db_pool = await asyncpg.create_pool(
                self.db_url,
                min_size=2,
                max_size=10,
                command_timeout=60
            )
            logger.info("✅ Database connection pool created")

            # Ensure tables exist
            await self.create_tables()

        except Exception as e:
            logger.error(f"❌ Failed to initialize collector: {e}")
            raise

    async def create_tables(self):
        """Create minute bar tables if they don't exist."""
        polygon_table_sql = """
        CREATE TABLE IF NOT EXISTS intg_one_minute_live_polygon (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            open DECIMAL(12,4) NOT NULL,
            high DECIMAL(12,4) NOT NULL,
            low DECIMAL(12,4) NOT NULL,
            close DECIMAL(12,4) NOT NULL,
            volume BIGINT NOT NULL,
            vwap DECIMAL(12,4),
            trade_count INTEGER,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(symbol, timestamp)
        );
        """

        tiingo_table_sql = """
        CREATE TABLE IF NOT EXISTS intg_one_minute_live_tiingo (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            open DECIMAL(12,4) NOT NULL,
            high DECIMAL(12,4) NOT NULL,
            low DECIMAL(12,4) NOT NULL,
            close DECIMAL(12,4) NOT NULL,
            volume BIGINT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(symbol, timestamp)
        );
        """

        async with self.db_pool.acquire() as conn:
            await conn.execute(polygon_table_sql)
            await conn.execute(tiingo_table_sql)
            logger.info("✅ Database tables ready")

    async def collect_polygon_data(self, symbol: str) -> List[Dict]:
        """Collect current minute bar from Polygon."""
        if not self.polygon_api_key:
            logger.warning("Polygon API key not configured")
            return []

        try:
            # Get current minute (rounded down)
            now = datetime.utcnow()
            current_minute = now.replace(second=0, microsecond=0)
            prev_minute = current_minute - timedelta(minutes=1)

            # Polygon aggregates API for 1-minute bars
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{int(prev_minute.timestamp() * 1000)}/{int(current_minute.timestamp() * 1000)}"

            async with aiohttp.ClientSession() as session:
                params = {'apikey': self.polygon_api_key}
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = data.get('results', [])

                        minute_bars = []
                        for bar in results:
                            minute_bars.append({
                                'symbol': symbol,
                                'timestamp': datetime.fromtimestamp(bar['t'] / 1000),
                                'open': bar['o'],
                                'high': bar['h'],
                                'low': bar['l'],
                                'close': bar['c'],
                                'volume': bar['v'],
                                'vwap': bar.get('vw'),
                                'trade_count': bar.get('n')
                            })

                        return minute_bars
                    else:
                        logger.warning(f"Polygon API error for {symbol}: {response.status}")
                        return []

        except Exception as e:
            logger.error(f"Error collecting Polygon data for {symbol}: {e}")
            return []

    async def collect_tiingo_data(self, symbol: str) -> List[Dict]:
        """Collect current minute bar from Tiingo."""
        if not self.tiingo_api_key:
            logger.warning("Tiingo API key not configured")
            return []

        try:
            # Get current minute (rounded down)
            now = datetime.utcnow()
            current_minute = now.replace(second=0, microsecond=0)
            prev_minute = current_minute - timedelta(minutes=1)

            # Tiingo IEX intraday API
            url = f"https://api.tiingo.com/iex/{symbol}/prices"

            async with aiohttp.ClientSession() as session:
                params = {
                    'token': self.tiingo_api_key,
                    'startDate': prev_minute.strftime('%Y-%m-%d %H:%M:%S'),
                    'endDate': current_minute.strftime('%Y-%m-%d %H:%M:%S'),
                    'resampleFreq': '1min'
                }

                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        minute_bars = []
                        for bar in data:
                            timestamp = datetime.fromisoformat(bar['date'].replace('Z', '+00:00'))
                            minute_bars.append({
                                'symbol': symbol,
                                'timestamp': timestamp,
                                'open': bar['open'],
                                'high': bar['high'],
                                'low': bar['low'],
                                'close': bar['close'],
                                'volume': bar['volume']
                            })

                        return minute_bars
                    else:
                        logger.warning(f"Tiingo API error for {symbol}: {response.status}")
                        return []

        except Exception as e:
            logger.error(f"Error collecting Tiingo data for {symbol}: {e}")
            return []

    async def store_polygon_data(self, bars: List[Dict]):
        """Store Polygon minute bars in database."""
        if not bars:
            return

        insert_sql = """
        INSERT INTO intg_one_minute_live_polygon
        (symbol, timestamp, open, high, low, close, volume, vwap, trade_count)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (symbol, timestamp) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            vwap = EXCLUDED.vwap,
            trade_count = EXCLUDED.trade_count,
            created_at = NOW()
        """

        async with self.db_pool.acquire() as conn:
            for bar in bars:
                await conn.execute(
                    insert_sql,
                    bar['symbol'], bar['timestamp'], bar['open'], bar['high'],
                    bar['low'], bar['close'], bar['volume'],
                    bar.get('vwap'), bar.get('trade_count')
                )

        logger.info(f"📊 Stored {len(bars)} Polygon minute bars")

    async def store_tiingo_data(self, bars: List[Dict]):
        """Store Tiingo minute bars in database."""
        if not bars:
            return

        insert_sql = """
        INSERT INTO intg_one_minute_live_tiingo
        (symbol, timestamp, open, high, low, close, volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (symbol, timestamp) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            created_at = NOW()
        """

        async with self.db_pool.acquire() as conn:
            for bar in bars:
                await conn.execute(
                    insert_sql,
                    bar['symbol'], bar['timestamp'], bar['open'], bar['high'],
                    bar['low'], bar['close'], bar['volume']
                )

        logger.info(f"📊 Stored {len(bars)} Tiingo minute bars")

    async def collect_symbol_data(self, symbol: str):
        """Collect data for a single symbol from both vendors."""
        try:
            # Collect from both vendors concurrently
            polygon_task = self.collect_polygon_data(symbol)
            tiingo_task = self.collect_tiingo_data(symbol)

            polygon_bars, tiingo_bars = await asyncio.gather(
                polygon_task, tiingo_task, return_exceptions=True
            )

            # Handle exceptions
            if isinstance(polygon_bars, Exception):
                logger.error(f"Polygon collection failed for {symbol}: {polygon_bars}")
                polygon_bars = []

            if isinstance(tiingo_bars, Exception):
                logger.error(f"Tiingo collection failed for {symbol}: {tiingo_bars}")
                tiingo_bars = []

            # Store data
            if polygon_bars:
                await self.store_polygon_data(polygon_bars)

            if tiingo_bars:
                await self.store_tiingo_data(tiingo_bars)

            total_bars = len(polygon_bars) + len(tiingo_bars)
            if total_bars > 0:
                logger.info(f"✅ {symbol}: {total_bars} minute bars collected")

        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")

    async def run_collection_cycle(self):
        """Run a single collection cycle for all symbols."""
        start_time = time.time()

        # Process symbols concurrently (with rate limiting)
        semaphore = asyncio.Semaphore(5)  # Max 5 concurrent requests

        async def limited_collect(symbol):
            async with semaphore:
                await self.collect_symbol_data(symbol)

        tasks = [limited_collect(symbol) for symbol in self.symbols]
        await asyncio.gather(*tasks, return_exceptions=True)

        duration = time.time() - start_time
        logger.info(f"🔄 Collection cycle completed in {duration:.1f}s for {len(self.symbols)} symbols")

    async def run_continuous(self):
        """Run continuous real-time collection."""
        self.running = True
        logger.info(f"🚀 Starting real-time collection for {len(self.symbols)} symbols")

        try:
            while self.running:
                # Wait until the start of the next minute
                now = datetime.utcnow()
                next_minute = (now.replace(second=0, microsecond=0) + timedelta(minutes=1))
                wait_seconds = (next_minute - now).total_seconds()

                if wait_seconds > 0:
                    logger.info(f"⏱️  Waiting {wait_seconds:.1f}s until next minute...")
                    await asyncio.sleep(wait_seconds)

                # Run collection cycle
                await self.run_collection_cycle()

                # Brief pause between cycles
                await asyncio.sleep(1)

        except KeyboardInterrupt:
            logger.info("🛑 Stopping real-time collection...")
            self.running = False

    async def close(self):
        """Clean shutdown."""
        self.running = False
        if self.db_pool:
            await self.db_pool.close()

async def main():
    parser = argparse.ArgumentParser(description='Real-Time Minute Bar Collector')

    parser.add_argument('--symbols', help='Comma-separated symbols (e.g., AAPL,TSLA,SPY)')
    parser.add_argument('--production', action='store_true', help='Run with production symbol set')
    parser.add_argument('--test', action='store_true', help='Run test with limited symbols')
    parser.add_argument('--db-host', default='ats-intg-postgres', help='Database host')
    parser.add_argument('--db-port', default='5432', help='Database port')
    parser.add_argument('--db-user', default='postgres', help='Database user')
    parser.add_argument('--db-password', default='intg_password', help='Database password')
    parser.add_argument('--db-name', default='intg_db', help='Database name')

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Determine symbols to collect
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(',')]
    elif args.test:
        symbols = ['AAPL', 'TSLA', 'SPY', 'QQQ']  # Test symbols
    elif args.production:
        # Production symbol set - major stocks and ETFs
        symbols = [
            # Major ETFs
            'SPY', 'QQQ', 'IWM', 'VTI', 'VOO', 'EFA', 'VWO', 'GLD', 'SLV', 'TLT',
            # Tech giants
            'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX',
            # Major banks
            'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C',
            # Other major stocks
            'JNJ', 'PG', 'KO', 'PFE', 'WMT', 'HD', 'V', 'MA', 'UNH', 'DIS'
        ]
    else:
        symbols = ['AAPL', 'SPY']  # Default minimal set

    logger.info(f"📊 Collecting real-time data for {len(symbols)} symbols: {symbols}")

    # Build database URL
    db_url = f"postgresql://{args.db_user}:{args.db_password}@{args.db_host}:{args.db_port}/{args.db_name}"

    # Create and run collector
    collector = RealTimeMinuteCollector(symbols, db_url)

    try:
        await collector.initialize()
        await collector.run_continuous()
    finally:
        await collector.close()

if __name__ == "__main__":
    asyncio.run(main())