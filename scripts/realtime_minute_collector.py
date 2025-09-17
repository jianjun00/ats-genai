#!/usr/bin/env python3
"""
Real-Time Minute Bar Collector for ATS-INTG

Continuously collects 1-minute bar data from Polygon and Tiingo APIs
and stores in INTG database for real-time trading applications.

Usage:
    python3 scripts/realtime_minute_collector.py --symbols AAPL,TSLA,SPY
    python3 scripts/realtime_minute_collector.py --universe-id 2  # High volume large cap universe
    python3 scripts/realtime_minute_collector.py --production  # All symbols
    python3 scripts/realtime_minute_collector.py --test  # Test symbols only
"""

import asyncio
import asyncpg
import logging
import os
import sys
import json
import argparse
import contextlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import aiohttp
import time

# OpenTelemetry imports for SigNoz integration
try:
    from opentelemetry import trace, metrics
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    logging.warning("OpenTelemetry not available - SigNoz integration disabled")

# Add src to Python path
sys.path.insert(0, '/workspace/src' if os.path.exists('/workspace/src') else 'src')

# from infrastructure.vendor.eodhd.adapters.eodhd_minute_adapter import EODHDMinuteAdapter

logger = logging.getLogger(__name__)

def setup_observability(service_name: str = "realtime-minute-collector"):
    """Setup OpenTelemetry for SigNoz integration."""
    if not OTEL_AVAILABLE:
        # Setup simple HTTP logging to SigNoz when OpenTelemetry unavailable
        signoz_endpoint = os.getenv("SIGNOZ_ENDPOINT", "http://10.0.0.79:4000")
        if signoz_endpoint:
            logger.info(f"🔍 SigNoz monitoring endpoint configured: {signoz_endpoint}")
        return None, None

    # Resource with service information
    resource = Resource.create({
        "service.name": service_name,
        "service.version": "1.0.0",
        "environment": os.getenv("ENVIRONMENT", "intg")
    })

    # SigNoz OTEL collector endpoint (try multiple possible endpoints)
    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://10.0.0.79:4317")

    # Setup tracing
    trace_provider = TracerProvider(resource=resource)
    trace_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    span_processor = BatchSpanProcessor(trace_exporter)
    trace_provider.add_span_processor(span_processor)
    trace.set_tracer_provider(trace_provider)
    tracer = trace.get_tracer(__name__)

    # Setup metrics
    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True),
        export_interval_millis=10000
    )
    metric_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(metric_provider)
    meter = metrics.get_meter(__name__)

    return tracer, meter

async def send_metrics_to_signoz(metrics_data: dict):
    """Send metrics to SigNoz via HTTP (alternative when OpenTelemetry unavailable)."""
    signoz_endpoint = os.getenv("SIGNOZ_ENDPOINT")
    if not signoz_endpoint:
        return

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{signoz_endpoint}/api/metrics", json=metrics_data) as response:
                if response.status == 200:
                    logger.debug("📊 Metrics sent to SigNoz")
    except Exception as e:
        logger.debug(f"Failed to send metrics to SigNoz: {e}")

class RealTimeMinuteCollector:
    """Real-time minute bar collector for ATS-INTG."""

    def __init__(self, symbols: List[str], db_url: str, universe_id: Optional[int] = None):
        self.symbols = symbols
        self.db_url = db_url
        self.universe_id = universe_id
        self.db_pool = None
        self.polygon_api_key = os.getenv('POLYGON_API_KEY')
        self.tiingo_api_key = os.getenv('TIINGO_API_KEY')
        self.eodhd_api_key = os.getenv('EODHD_API_KEY')
        self.running = False

        # Initialize observability
        self.tracer, self.meter = setup_observability()
        self.setup_metrics()

    def setup_metrics(self):
        """Setup custom metrics for monitoring."""
        if not self.meter:
            # Set None defaults when metrics not available
            self.bars_collected = None
            self.api_errors = None
            self.collection_duration = None
            self.active_symbols = None
            return

        # Counter for collected minute bars
        self.bars_collected = self.meter.create_counter(
            "minute_bars_collected_total",
            description="Total number of minute bars collected"
        )

        # Counter for API errors
        self.api_errors = self.meter.create_counter(
            "api_errors_total",
            description="Total number of API errors"
        )

        # Histogram for collection duration
        self.collection_duration = self.meter.create_histogram(
            "collection_duration_seconds",
            description="Time taken for collection cycles"
        )

        # Gauge for active symbols
        self.active_symbols = self.meter.create_up_down_counter(
            "active_symbols",
            description="Number of active symbols being monitored"
        )

    async def load_symbols_from_universe(self) -> List[str]:
        """Load symbols from specified universe."""
        if not self.universe_id:
            return self.symbols

        async with self.db_pool.acquire() as conn:
            query = """
            SELECT DISTINCT i.symbol
            FROM intg_universe_membership um
            JOIN intg_instrument i ON um.instrument_id = i.id
            WHERE um.universe_id = $1
            AND (um.end_at IS NULL OR um.end_at > CURRENT_DATE)
            AND i.active = true
            AND i.symbol IS NOT NULL
            AND i.symbol != ''
            ORDER BY i.symbol
            """

            rows = await conn.fetch(query, self.universe_id)
            universe_symbols = [row['symbol'] for row in rows]

            logger.info(f"📊 Loaded {len(universe_symbols)} symbols from universe {self.universe_id}")
            return universe_symbols

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

            # Load symbols from universe if specified
            if self.universe_id:
                self.symbols = await self.load_symbols_from_universe()

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

        eodhd_table_sql = """
        CREATE TABLE IF NOT EXISTS intg_one_minute_live_eodhd (
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
            await conn.execute(eodhd_table_sql)
            logger.info("✅ Database tables ready")

    async def collect_polygon_data(self, symbol: str) -> List[Dict]:
        """Collect current minute bar from Polygon."""
        if not self.polygon_api_key:
            logger.warning("Polygon API key not configured")
            return []

        span_name = f"collect_polygon_data_{symbol}"
        with self.tracer.start_as_current_span(span_name) if self.tracer else contextlib.nullcontext():
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

                            # Track metrics
                            if self.bars_collected:
                                self.bars_collected.add(len(minute_bars), {"vendor": "polygon", "symbol": symbol})

                            return minute_bars
                        else:
                            logger.warning(f"Polygon API error for {symbol}: {response.status}")
                            if self.api_errors:
                                self.api_errors.add(1, {"vendor": "polygon", "symbol": symbol, "status": str(response.status)})
                            return []

            except Exception as e:
                logger.error(f"Error collecting Polygon data for {symbol}: {e}")
                if self.api_errors:
                    self.api_errors.add(1, {"vendor": "polygon", "symbol": symbol, "error": str(e)})
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

    async def collect_eodhd_data(self, symbol: str) -> List[Dict]:
        """Collect current minute bar from EODHD."""
        if not self.eodhd_api_key:
            logger.warning("EODHD API key not configured")
            return []

        try:
            # Get current minute (rounded down)
            now = datetime.utcnow()
            current_minute = now.replace(second=0, microsecond=0)
            prev_minute = current_minute - timedelta(minutes=1)

            # EODHD intraday API for 1-minute bars
            url = f"https://eodhistoricaldata.com/api/intraday/{symbol}.US"

            async with aiohttp.ClientSession() as session:
                params = {
                    'api_token': self.eodhd_api_key,
                    'interval': '1m',
                    'from': prev_minute.strftime('%Y-%m-%d %H:%M:%S'),
                    'to': current_minute.strftime('%Y-%m-%d %H:%M:%S'),
                    'fmt': 'json'
                }

                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        minute_bars = []
                        for bar in data:
                            timestamp = datetime.strptime(bar['datetime'], '%Y-%m-%d %H:%M:%S')
                            minute_bars.append({
                                'symbol': symbol,
                                'timestamp': timestamp,
                                'open': bar['open'],
                                'high': bar['high'],
                                'low': bar['low'],
                                'close': bar['close'],
                                'volume': bar.get('volume', 0)
                            })

                        return minute_bars
                    else:
                        logger.warning(f"EODHD API error for {symbol}: {response.status}")
                        return []

        except Exception as e:
            logger.error(f"Error collecting EODHD data for {symbol}: {e}")
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

    async def store_eodhd_data(self, bars: List[Dict]):
        """Store EODHD minute bars in database."""
        if not bars:
            return

        insert_sql = """
        INSERT INTO intg_one_minute_live_eodhd
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

        logger.info(f"📊 Stored {len(bars)} EODHD minute bars")

    async def collect_symbol_data(self, symbol: str):
        """Collect data for a single symbol from all vendors."""
        try:
            # Collect from all vendors concurrently
            polygon_task = self.collect_polygon_data(symbol)
            tiingo_task = self.collect_tiingo_data(symbol)
            eodhd_task = self.collect_eodhd_data(symbol)

            polygon_bars, tiingo_bars, eodhd_bars = await asyncio.gather(
                polygon_task, tiingo_task, eodhd_task, return_exceptions=True
            )

            # Handle exceptions
            if isinstance(polygon_bars, Exception):
                logger.error(f"Polygon collection failed for {symbol}: {polygon_bars}")
                polygon_bars = []

            if isinstance(tiingo_bars, Exception):
                logger.error(f"Tiingo collection failed for {symbol}: {tiingo_bars}")
                tiingo_bars = []

            if isinstance(eodhd_bars, Exception):
                logger.error(f"EODHD collection failed for {symbol}: {eodhd_bars}")
                eodhd_bars = []

            # Store data
            if polygon_bars:
                await self.store_polygon_data(polygon_bars)

            if tiingo_bars:
                await self.store_tiingo_data(tiingo_bars)

            if eodhd_bars:
                await self.store_eodhd_data(eodhd_bars)

            total_bars = len(polygon_bars) + len(tiingo_bars) + len(eodhd_bars)
            if total_bars > 0:
                logger.info(f"✅ {symbol}: {total_bars} minute bars collected (P:{len(polygon_bars)}, T:{len(tiingo_bars)}, E:{len(eodhd_bars)})")

        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")

    async def run_collection_cycle(self):
        """Run a single collection cycle for all symbols."""
        start_time = time.time()

        with self.tracer.start_as_current_span("collection_cycle") if self.tracer else contextlib.nullcontext():
            # Process symbols concurrently (with rate limiting)
            semaphore = asyncio.Semaphore(5)  # Max 5 concurrent requests

            async def limited_collect(symbol):
                async with semaphore:
                    await self.collect_symbol_data(symbol)

            tasks = [limited_collect(symbol) for symbol in self.symbols]
            await asyncio.gather(*tasks, return_exceptions=True)

            duration = time.time() - start_time

            # Track collection duration
            if self.collection_duration:
                self.collection_duration.record(duration)

            # Track active symbols
            if self.active_symbols:
                self.active_symbols.add(len(self.symbols))

            # Send basic metrics to SigNoz (alternative when OpenTelemetry unavailable)
            if os.getenv("SIGNOZ_ENDPOINT"):
                metrics_data = {
                    "service": "realtime-minute-collector",
                    "timestamp": datetime.utcnow().isoformat(),
                    "metrics": {
                        "collection_duration_seconds": duration,
                        "active_symbols": len(self.symbols),
                        "environment": os.getenv("ENVIRONMENT", "intg")
                    }
                }
                await send_metrics_to_signoz(metrics_data)

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
    parser.add_argument('--universe-id', type=int, help='Universe ID to collect symbols from (e.g., 2 for high_volume_large_cap)')
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
    if args.universe_id:
        # Symbols will be loaded from universe during initialization
        symbols = []  # Will be populated from universe
        logger.info(f"📊 Will collect symbols from universe ID {args.universe_id}")
    elif args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(',')]
        logger.info(f"📊 Collecting real-time data for {len(symbols)} symbols: {symbols}")
    elif args.test:
        symbols = ['AAPL', 'TSLA', 'SPY', 'QQQ']  # Test symbols
        logger.info(f"📊 Test mode: collecting real-time data for {len(symbols)} symbols: {symbols}")
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
        logger.info(f"📊 Production mode: collecting real-time data for {len(symbols)} symbols")
    else:
        symbols = ['AAPL', 'SPY']  # Default minimal set
        logger.info(f"📊 Default mode: collecting real-time data for {len(symbols)} symbols: {symbols}")

    # Build database URL
    db_url = f"postgresql://{args.db_user}:{args.db_password}@{args.db_host}:{args.db_port}/{args.db_name}"

    # Create and run collector
    collector = RealTimeMinuteCollector(symbols, db_url, universe_id=args.universe_id)

    try:
        await collector.initialize()
        await collector.run_continuous()
    finally:
        await collector.close()

if __name__ == "__main__":
    asyncio.run(main())