#!/usr/bin/env python3
"""
Real-Time Market Data Collector Orchestrator

Deploys and manages real-time 1-minute bar collectors for all vendors:
- Polygon WebSocket streaming
- Tiingo real-time API polling
- FMP real-time API polling

Features:
- Database storage with overlap handling
- Gap detection and backfill coordination
- Health monitoring and failover
- Market hours intelligence
"""

import asyncio
import logging
import json
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set
import pytz
from dataclasses import dataclass, asdict
import asyncpg
import aiohttp
import websockets
import requests
import signal
import sys
from aiohttp import web

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

@dataclass
class MinuteBar:
    """Standardized minute bar for all vendors."""
    symbol: str
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    vendor: str
    
    # Optional fields
    vwap: Optional[float] = None
    trade_count: Optional[int] = None
    adj_close_price: Optional[float] = None
    
    # Real-time metadata
    received_at: Optional[datetime] = None
    data_latency_ms: Optional[int] = None
    collection_method: str = 'unknown'
    quality_score: float = 0.8

@dataclass
class CollectorStatus:
    """Status tracking for each collector."""
    vendor: str
    status: str  # 'running', 'stopped', 'error'
    symbols_count: int = 0
    bars_collected_today: int = 0
    last_collection: Optional[datetime] = None
    error_count: int = 0
    last_error: Optional[str] = None
    health_score: float = 1.0

class MarketHours:
    """Market hours utility."""
    
    def __init__(self):
        self.eastern = pytz.timezone('US/Eastern')
        self.market_open = datetime.strptime('09:30', '%H:%M').time()
        self.market_close = datetime.strptime('16:00', '%H:%M').time()
    
    def is_market_open(self, dt: Optional[datetime] = None) -> bool:
        """Check if market is currently open."""
        if dt is None:
            dt = datetime.now(self.eastern)
        
        if dt.weekday() >= 5:  # Weekend
            return False
        
        current_time = dt.time()
        return self.market_open <= current_time <= self.market_close

class PolygonCollector:
    """Polygon WebSocket real-time collector."""
    
    def __init__(self, api_key: str, db_pool: asyncpg.Pool):
        self.api_key = api_key
        self.db_pool = db_pool
        self.logger = logging.getLogger('polygon')
        self.symbols: Set[str] = set()
        self.ws_connection = None
        self.is_running = False
        
    async def initialize(self):
        """Load symbols from database."""
        try:
            async with self.db_pool.acquire() as conn:
                query = """
                    SELECT DISTINCT i.symbol
                    FROM dev_instruments i
                    INNER JOIN dev_universe_membership um ON i.id = um.instrument_id
                    WHERE um.is_active = true
                      AND LENGTH(i.symbol) <= 5
                    ORDER BY i.symbol
                    LIMIT 100
                """
                rows = await conn.fetch(query)
                self.symbols = {row['symbol'] for row in rows}
                self.logger.info(f"Loaded {len(self.symbols)} symbols for Polygon")
        except Exception as e:
            self.logger.error(f"Failed to load symbols: {e}")
            # Fallback to popular symbols
            self.symbols = {'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA'}
    
    async def connect_websocket(self):
        """Connect to Polygon WebSocket."""
        try:
            ws_url = f"wss://socket.polygon.io/stocks"
            self.ws_connection = await websockets.connect(ws_url)
            
            # Authenticate
            auth_message = {"action": "auth", "params": self.api_key}
            await self.ws_connection.send(json.dumps(auth_message))
            
            # Subscribe to minute aggregates for all symbols
            for symbol in self.symbols:
                subscribe_msg = {
                    "action": "subscribe",
                    "params": f"AM.{symbol}"  # Aggregate minute bars
                }
                await self.ws_connection.send(json.dumps(subscribe_msg))
            
            self.logger.info(f"Connected to Polygon WebSocket for {len(self.symbols)} symbols")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Polygon WebSocket: {e}")
            return False
    
    async def process_message(self, message_data: dict) -> Optional[MinuteBar]:
        """Process incoming WebSocket message."""
        try:
            if message_data.get('ev') == 'AM':  # Aggregate minute
                symbol = message_data.get('sym', '')
                timestamp_ms = message_data.get('s')  # Start timestamp
                
                if not symbol or not timestamp_ms:
                    return None
                
                timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                
                return MinuteBar(
                    symbol=symbol,
                    timestamp=timestamp,
                    open_price=float(message_data.get('o', 0)),
                    high_price=float(message_data.get('h', 0)),
                    low_price=float(message_data.get('l', 0)),
                    close_price=float(message_data.get('c', 0)),
                    volume=int(message_data.get('v', 0)),
                    vendor='polygon',
                    vwap=message_data.get('vw'),
                    trade_count=message_data.get('n'),
                    received_at=datetime.now(timezone.utc),
                    collection_method='websocket'
                )
                
        except Exception as e:
            self.logger.error(f"Error processing Polygon message: {e}")
        
        return None
    
    async def listen_websocket(self, callback):
        """Listen for WebSocket messages."""
        try:
            while self.is_running and self.ws_connection:
                try:
                    message = await asyncio.wait_for(self.ws_connection.recv(), timeout=30.0)
                    data = json.loads(message)
                    
                    if isinstance(data, list):
                        for item in data:
                            bar = await self.process_message(item)
                            if bar:
                                await callback(bar)
                    else:
                        bar = await self.process_message(data)
                        if bar:
                            await callback(bar)
                            
                except asyncio.TimeoutError:
                    # Send ping to keep connection alive
                    await self.ws_connection.ping()
                except Exception as e:
                    self.logger.error(f"WebSocket error: {e}")
                    break
                    
        except Exception as e:
            self.logger.error(f"WebSocket listener error: {e}")
        finally:
            if self.ws_connection:
                await self.ws_connection.close()
                self.ws_connection = None
    
    async def start(self, callback):
        """Start the Polygon collector."""
        self.is_running = True
        await self.initialize()
        
        while self.is_running:
            if await self.connect_websocket():
                await self.listen_websocket(callback)
            
            if self.is_running:
                self.logger.warning("Reconnecting to Polygon in 10 seconds...")
                await asyncio.sleep(10)
    
    async def stop(self):
        """Stop the Polygon collector."""
        self.is_running = False
        if self.ws_connection:
            await self.ws_connection.close()

class TiingoCollector:
    """Tiingo real-time polling collector."""
    
    def __init__(self, api_key: str, db_pool: asyncpg.Pool):
        self.api_key = api_key
        self.db_pool = db_pool
        self.logger = logging.getLogger('tiingo')
        self.symbols: Set[str] = set()
        self.session = None
        self.is_running = False
    
    async def initialize(self):
        """Initialize Tiingo collector."""
        try:
            async with self.db_pool.acquire() as conn:
                query = """
                    SELECT DISTINCT i.symbol
                    FROM dev_instruments i
                    INNER JOIN dev_universe_membership um ON i.id = um.instrument_id
                    WHERE um.is_active = true
                      AND LENGTH(i.symbol) <= 5
                    ORDER BY i.symbol
                    LIMIT 50
                """
                rows = await conn.fetch(query)
                self.symbols = {row['symbol'] for row in rows}
                self.logger.info(f"Loaded {len(self.symbols)} symbols for Tiingo")
        except Exception as e:
            self.logger.error(f"Failed to load symbols: {e}")
            self.symbols = {'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'}
        
        self.session = aiohttp.ClientSession()
    
    async def fetch_latest_prices(self) -> List[MinuteBar]:
        """Fetch latest prices from Tiingo."""
        bars = []
        
        try:
            # Batch request for multiple symbols
            symbols_str = ','.join(list(self.symbols)[:10])  # Limit to 10 at a time
            url = f"https://api.tiingo.com/iex/{symbols_str}/prices"
            
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Token {self.api_key}'
            }
            
            params = {
                'resampleFreq': '1min',
                'columns': 'open,high,low,close,volume'
            }
            
            async with self.session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    for item in data:
                        symbol = item.get('ticker', '')
                        if symbol and 'priceData' in item:
                            for price_data in item['priceData'][-1:]:  # Get latest
                                timestamp = datetime.fromisoformat(
                                    price_data['date'].replace('Z', '+00:00')
                                )
                                
                                bar = MinuteBar(
                                    symbol=symbol,
                                    timestamp=timestamp,
                                    open_price=float(price_data.get('open', 0)),
                                    high_price=float(price_data.get('high', 0)),
                                    low_price=float(price_data.get('low', 0)),
                                    close_price=float(price_data.get('close', 0)),
                                    volume=int(price_data.get('volume', 0)),
                                    vendor='tiingo',
                                    received_at=datetime.now(timezone.utc),
                                    collection_method='polling'
                                )
                                bars.append(bar)
                
        except Exception as e:
            self.logger.error(f"Failed to fetch Tiingo data: {e}")
        
        return bars
    
    async def start(self, callback):
        """Start the Tiingo collector."""
        self.is_running = True
        await self.initialize()
        
        while self.is_running:
            try:
                bars = await self.fetch_latest_prices()
                for bar in bars:
                    await callback(bar)
                
                # Poll every 60 seconds for minute bars
                await asyncio.sleep(60)
                
            except Exception as e:
                self.logger.error(f"Tiingo collection error: {e}")
                await asyncio.sleep(30)
    
    async def stop(self):
        """Stop the Tiingo collector."""
        self.is_running = False
        if self.session:
            await self.session.close()

class FMPCollector:
    """FMP real-time polling collector."""
    
    def __init__(self, api_key: str, db_pool: asyncpg.Pool):
        self.api_key = api_key
        self.db_pool = db_pool
        self.logger = logging.getLogger('fmp')
        self.symbols: Set[str] = set()
        self.session = None
        self.is_running = False
    
    async def initialize(self):
        """Initialize FMP collector."""
        try:
            async with self.db_pool.acquire() as conn:
                query = """
                    SELECT DISTINCT i.symbol
                    FROM dev_instruments i
                    INNER JOIN dev_universe_membership um ON i.id = um.instrument_id
                    WHERE um.is_active = true
                      AND LENGTH(i.symbol) <= 5
                    ORDER BY i.symbol
                    LIMIT 50
                """
                rows = await conn.fetch(query)
                self.symbols = {row['symbol'] for row in rows}
                self.logger.info(f"Loaded {len(self.symbols)} symbols for FMP")
        except Exception as e:
            self.logger.error(f"Failed to load symbols: {e}")
            self.symbols = {'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'}
        
        self.session = aiohttp.ClientSession()
    
    async def fetch_latest_prices(self) -> List[MinuteBar]:
        """Fetch latest prices from FMP."""
        bars = []
        
        try:
            for symbol in list(self.symbols)[:10]:  # Limit concurrent requests
                url = f"https://financialmodelingprep.com/api/v3/historical-chart/1min/{symbol}"
                params = {
                    'apikey': self.api_key,
                    'from': (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S'),
                    'to': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Get the most recent bar
                        if data and isinstance(data, list) and len(data) > 0:
                            latest = data[0]  # FMP returns newest first
                            timestamp = datetime.fromisoformat(latest['date'])
                            
                            bar = MinuteBar(
                                symbol=symbol,
                                timestamp=timestamp,
                                open_price=float(latest.get('open', 0)),
                                high_price=float(latest.get('high', 0)),
                                low_price=float(latest.get('low', 0)),
                                close_price=float(latest.get('close', 0)),
                                volume=int(latest.get('volume', 0)),
                                vendor='fmp',
                                received_at=datetime.now(timezone.utc),
                                collection_method='polling'
                            )
                            bars.append(bar)
                
                # Rate limiting
                await asyncio.sleep(0.2)  # 5 requests per second limit
                
        except Exception as e:
            self.logger.error(f"Failed to fetch FMP data: {e}")
        
        return bars
    
    async def start(self, callback):
        """Start the FMP collector."""
        self.is_running = True
        await self.initialize()
        
        while self.is_running:
            try:
                bars = await self.fetch_latest_prices()
                for bar in bars:
                    await callback(bar)
                
                # Poll every 60 seconds for minute bars
                await asyncio.sleep(60)
                
            except Exception as e:
                self.logger.error(f"FMP collection error: {e}")
                await asyncio.sleep(30)
    
    async def stop(self):
        """Stop the FMP collector."""
        self.is_running = False
        if self.session:
            await self.session.close()

class RealtimeOrchestrator:
    """Main orchestrator for all real-time collectors."""
    
    def __init__(self):
        self.logger = logging.getLogger('orchestrator')
        self.db_pool: Optional[asyncpg.Pool] = None
        self.market_hours = MarketHours()
        
        # API keys
        self.polygon_key = os.getenv('POLYGON_API_KEY', '')
        self.tiingo_key = os.getenv('TIINGO_API_KEY', '')
        self.fmp_key = os.getenv('FMP_API_KEY', '')
        
        # Collectors
        self.collectors: Dict[str, object] = {}
        self.collector_tasks: Dict[str, asyncio.Task] = {}
        self.collector_status: Dict[str, CollectorStatus] = {}
        
        # Statistics
        self.total_bars_collected = 0
        self.start_time = None
        self.is_running = False
        
        # HTTP server for health checks
        self.http_app = None
        self.http_runner = None
    
    async def initialize_database(self):
        """Initialize database connection."""
        try:
            db_host = os.getenv('DB_HOST', 'localhost')
            db_port = os.getenv('DB_PORT', '5433')
            db_user = os.getenv('DB_USER', 'postgres')
            db_password = os.getenv('DB_PASSWORD', 'postgres')
            db_name = os.getenv('DB_NAME', 'dev_db')
            
            dsn = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            self.db_pool = await asyncpg.create_pool(
                dsn,
                min_size=5,
                max_size=20,
                command_timeout=30
            )
            
            # Test connection
            async with self.db_pool.acquire() as conn:
                version = await conn.fetchval("SELECT version()")
                self.logger.info(f"Connected to database: {version}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            return False
    
    async def store_minute_bar(self, bar: MinuteBar):
        """Store minute bar in appropriate vendor table."""
        try:
            table_name = f"dev_one_minute_live_{bar.vendor}"
            
            async with self.db_pool.acquire() as conn:
                # Get instrument_id
                instrument_query = "SELECT id FROM dev_instruments WHERE symbol = $1 LIMIT 1"
                instrument_row = await conn.fetchrow(instrument_query, bar.symbol)
                
                if not instrument_row:
                    self.logger.warning(f"No instrument found for {bar.symbol}")
                    return False
                
                instrument_id = instrument_row['id']
                
                # Calculate latency
                if bar.received_at and bar.timestamp:
                    latency_delta = bar.received_at - bar.timestamp
                    bar.data_latency_ms = int(latency_delta.total_seconds() * 1000)
                
                # Upsert minute bar (handle overlapping intervals)
                if bar.vendor == 'polygon':
                    upsert_query = f"""
                        INSERT INTO {table_name} (
                            instrument_id, symbol, timestamp, open_price, high_price, 
                            low_price, close_price, volume, vwap, trade_count,
                            received_at, data_latency_ms, collection_method, 
                            is_realtime, quality_score, validation_status
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                        ON CONFLICT (instrument_id, timestamp)
                        DO UPDATE SET
                            open_price = EXCLUDED.open_price,
                            high_price = EXCLUDED.high_price,
                            low_price = EXCLUDED.low_price,
                            close_price = EXCLUDED.close_price,
                            volume = EXCLUDED.volume,
                            vwap = EXCLUDED.vwap,
                            trade_count = EXCLUDED.trade_count,
                            received_at = EXCLUDED.received_at,
                            data_latency_ms = EXCLUDED.data_latency_ms,
                            quality_score = EXCLUDED.quality_score,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE {table_name}.received_at < EXCLUDED.received_at
                    """
                    
                    await conn.execute(
                        upsert_query,
                        instrument_id, bar.symbol, bar.timestamp, bar.open_price,
                        bar.high_price, bar.low_price, bar.close_price, bar.volume,
                        bar.vwap, bar.trade_count, bar.received_at, bar.data_latency_ms,
                        bar.collection_method, True, bar.quality_score, 'pending'
                    )
                else:
                    # Tiingo and FMP
                    upsert_query = f"""
                        INSERT INTO {table_name} (
                            instrument_id, symbol, timestamp, open_price, high_price, 
                            low_price, close_price, volume, adj_close_price,
                            received_at, data_latency_ms, collection_method, 
                            is_realtime, quality_score, validation_status
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                        ON CONFLICT (instrument_id, timestamp)
                        DO UPDATE SET
                            open_price = EXCLUDED.open_price,
                            high_price = EXCLUDED.high_price,
                            low_price = EXCLUDED.low_price,
                            close_price = EXCLUDED.close_price,
                            volume = EXCLUDED.volume,
                            adj_close_price = EXCLUDED.adj_close_price,
                            received_at = EXCLUDED.received_at,
                            data_latency_ms = EXCLUDED.data_latency_ms,
                            quality_score = EXCLUDED.quality_score,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE {table_name}.received_at < EXCLUDED.received_at
                    """
                    
                    await conn.execute(
                        upsert_query,
                        instrument_id, bar.symbol, bar.timestamp, bar.open_price,
                        bar.high_price, bar.low_price, bar.close_price, bar.volume,
                        bar.adj_close_price, bar.received_at, bar.data_latency_ms,
                        bar.collection_method, True, bar.quality_score, 'pending'
                    )
                
                self.total_bars_collected += 1
                
                # Update collector status
                if bar.vendor in self.collector_status:
                    self.collector_status[bar.vendor].bars_collected_today += 1
                    self.collector_status[bar.vendor].last_collection = datetime.now(timezone.utc)
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to store {bar.vendor} bar for {bar.symbol}: {e}")
            if bar.vendor in self.collector_status:
                self.collector_status[bar.vendor].error_count += 1
                self.collector_status[bar.vendor].last_error = str(e)
            return False
    
    async def initialize_collectors(self):
        """Initialize all collectors."""
        if self.polygon_key:
            self.collectors['polygon'] = PolygonCollector(self.polygon_key, self.db_pool)
            self.collector_status['polygon'] = CollectorStatus(vendor='polygon', status='initializing')
        
        if self.tiingo_key:
            self.collectors['tiingo'] = TiingoCollector(self.tiingo_key, self.db_pool)
            self.collector_status['tiingo'] = CollectorStatus(vendor='tiingo', status='initializing')
        
        if self.fmp_key:
            self.collectors['fmp'] = FMPCollector(self.fmp_key, self.db_pool)
            self.collector_status['fmp'] = CollectorStatus(vendor='fmp', status='initializing')
        
        self.logger.info(f"Initialized {len(self.collectors)} collectors: {list(self.collectors.keys())}")
    
    async def start_collector(self, vendor: str):
        """Start a specific collector."""
        try:
            collector = self.collectors[vendor]
            self.collector_status[vendor].status = 'running'
            
            # Start collector with callback
            self.collector_tasks[vendor] = asyncio.create_task(
                collector.start(self.store_minute_bar)
            )
            
            self.logger.info(f"✅ Started {vendor.upper()} collector")
            
        except Exception as e:
            self.logger.error(f"Failed to start {vendor} collector: {e}")
            self.collector_status[vendor].status = 'error'
            self.collector_status[vendor].last_error = str(e)
    
    async def stop_collector(self, vendor: str):
        """Stop a specific collector."""
        try:
            if vendor in self.collector_tasks:
                self.collector_tasks[vendor].cancel()
                try:
                    await self.collector_tasks[vendor]
                except asyncio.CancelledError:
                    pass
                del self.collector_tasks[vendor]
            
            if vendor in self.collectors:
                await self.collectors[vendor].stop()
            
            self.collector_status[vendor].status = 'stopped'
            self.logger.info(f"🛑 Stopped {vendor.upper()} collector")
            
        except Exception as e:
            self.logger.error(f"Error stopping {vendor} collector: {e}")
    
    async def start_all_collectors(self):
        """Start all available collectors."""
        self.is_running = True
        self.start_time = datetime.now(timezone.utc)
        
        for vendor in self.collectors.keys():
            await self.start_collector(vendor)
        
        self.logger.info("🚀 All real-time collectors started!")
    
    async def stop_all_collectors(self):
        """Stop all collectors."""
        self.is_running = False
        
        for vendor in list(self.collectors.keys()):
            await self.stop_collector(vendor)
        
        self.logger.info("🛑 All real-time collectors stopped!")
    
    async def health_handler(self, request):
        """Health check endpoint."""
        health_status = {
            'status': 'healthy' if self.is_running else 'unhealthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'collectors_running': len([c for c in self.collector_status.values() if c.status == 'running']),
            'database_connected': self.db_pool is not None
        }
        return web.json_response(health_status)
    
    async def ready_handler(self, request):
        """Readiness check endpoint."""
        ready = (self.is_running and 
                self.db_pool is not None and 
                any(c.status == 'running' for c in self.collector_status.values()))
        
        ready_status = {
            'ready': ready,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        return web.json_response(ready_status)
    
    async def status_handler(self, request):
        """Detailed status endpoint."""
        status = await self.get_status_report()
        return web.json_response(status, indent=2)
    
    async def metrics_handler(self, request):
        """Metrics endpoint."""
        metrics = {
            'total_bars_collected': self.total_bars_collected,
            'collectors_running': len([c for c in self.collector_status.values() if c.status == 'running']),
            'uptime_seconds': (datetime.now(timezone.utc) - self.start_time).total_seconds() if self.start_time else 0,
            'market_open': self.market_hours.is_market_open(),
            'collector_metrics': {
                vendor: {
                    'bars_today': status.bars_collected_today,
                    'error_count': status.error_count,
                    'health_score': status.health_score
                }
                for vendor, status in self.collector_status.items()
            }
        }
        return web.json_response(metrics)
    
    async def start_http_server(self):
        """Start HTTP server for health checks and status."""
        self.http_app = web.Application()
        
        # Add routes
        self.http_app.router.add_get('/health', self.health_handler)
        self.http_app.router.add_get('/ready', self.ready_handler)
        self.http_app.router.add_get('/status', self.status_handler)
        self.http_app.router.add_get('/metrics', self.metrics_handler)
        
        # Start server
        self.http_runner = web.AppRunner(self.http_app)
        await self.http_runner.setup()
        
        site = web.TCPSite(self.http_runner, '0.0.0.0', 8080)
        await site.start()
        
        self.logger.info("HTTP server started on port 8080")
    
    async def stop_http_server(self):
        """Stop HTTP server."""
        if self.http_runner:
            await self.http_runner.cleanup()
    
    async def get_status_report(self) -> dict:
        """Get comprehensive status report."""
        uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds() if self.start_time else 0
        
        return {
            'orchestrator': {
                'status': 'running' if self.is_running else 'stopped',
                'uptime_seconds': uptime,
                'total_bars_collected': self.total_bars_collected,
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'market_open': self.market_hours.is_market_open()
            },
            'collectors': {
                vendor: asdict(status) for vendor, status in self.collector_status.items()
            },
            'database': {
                'connected': self.db_pool is not None,
                'pool_size': len(self.db_pool._queue) if self.db_pool else 0
            }
        }
    
    async def monitor_health(self):
        """Health monitoring loop."""
        while self.is_running:
            try:
                # Check collector health
                for vendor, task in list(self.collector_tasks.items()):
                    if task.done() or task.cancelled():
                        self.logger.warning(f"{vendor} collector stopped unexpectedly, restarting...")
                        await self.start_collector(vendor)
                
                # Log status every 5 minutes
                status = await self.get_status_report()
                self.logger.info(f"Health check - Collectors: {len([c for c in status['collectors'].values() if c['status'] == 'running'])}/{len(status['collectors'])}, "
                               f"Total bars: {status['orchestrator']['total_bars_collected']}")
                
                await asyncio.sleep(300)  # 5 minutes
                
            except Exception as e:
                self.logger.error(f"Health monitor error: {e}")
                await asyncio.sleep(60)
    
    async def run(self):
        """Main run loop."""
        self.logger.info("🎯 Starting ATS Real-Time Data Collector Orchestrator")
        
        # Initialize database
        if not await self.initialize_database():
            self.logger.error("Failed to initialize database, exiting")
            return
        
        # Initialize collectors
        await self.initialize_collectors()
        
        if not self.collectors:
            self.logger.error("No collectors available (missing API keys?), exiting")
            return
        
        # Start HTTP server
        await self.start_http_server()
        
        # Start collectors
        await self.start_all_collectors()
        
        # Start health monitoring
        health_task = asyncio.create_task(self.monitor_health())
        
        try:
            # Keep running until interrupted
            await health_task
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
        finally:
            health_task.cancel()
            await self.stop_all_collectors()
            await self.stop_http_server()
            
            if self.db_pool:
                await self.db_pool.close()
            
            self.logger.info("✅ Real-Time Collector Orchestrator shutdown complete")

async def main():
    """Main entry point."""
    orchestrator = RealtimeOrchestrator()
    
    # Handle signals
    def signal_handler(sig, frame):
        asyncio.create_task(orchestrator.stop_all_collectors())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    await orchestrator.run()

if __name__ == "__main__":
    asyncio.run(main())