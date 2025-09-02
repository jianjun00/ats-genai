#!/usr/bin/env python3
"""
ATS Minute-Level Price Service
Extends existing framework for 1-minute intraday data collection
"""

import gin
import asyncio
from datetime import datetime
from typing import List
from fastapi import FastAPI, BackgroundTasks
from dataclasses import dataclass

# Reuse existing ATS framework
from config.environment import Environment
from dao.base.base_dao import BaseDAO
from core.logging.logger_config import get_logger
from market_data.eod.daily_price_tiingo import TIINGO_API_KEY
from market_data.eod.daily_price_fmp import *  # Reuse FMP patterns

# Configure Gin
try:
    gin.parse_config_file('config/app_dev.gin')
except Exception as e:
    print(f'[WARN] Could not parse gin config: {e}')

@gin.configurable
@dataclass
class MinuteServiceConfig:
    """Configuration for minute price service API"""
    title: str = "ATS Minute Price Service"
    description: str = "1-minute intraday price collection using existing ATS framework"
    version: str = "1.0.0"
    host: str = "0.0.0.0"
    port: int = 8081
    default_symbols: List[str] = None
    
    def __post_init__(self):
        if self.default_symbols is None:
            self.default_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA']

logger = get_logger(__name__)

@dataclass
class MinutePrice:
    """Standardized minute price data structure"""
    symbol: str
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    vendor: str
    quality_score: float = 1.0

class MinutePriceDAO(BaseDAO):
    """DAO for minute-level price data using existing patterns"""
    
    def __init__(self, vendor: str, env: Environment):
        table_name = f"minute_prices_{vendor}"
        super().__init__(table_name)
        self.env = env
        self.vendor = vendor
        self.logger = get_logger(f"{__name__}.{vendor}")
    
    async def create_table_if_not_exists(self):
        """Create minute price table using existing DAO patterns"""
        full_table_name = self.env.get_table_name(self.table_name)
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            id BIGSERIAL PRIMARY KEY,
            instrument_id INTEGER NOT NULL,
            symbol VARCHAR(10) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            open_price DECIMAL(20,6) NOT NULL,
            high_price DECIMAL(20,6) NOT NULL, 
            low_price DECIMAL(20,6) NOT NULL,
            close_price DECIMAL(20,6) NOT NULL,
            volume BIGINT NOT NULL DEFAULT 0,
            quality_score DECIMAL(5,4) DEFAULT 1.0,
            collected_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(instrument_id, timestamp)
        );
        
        CREATE INDEX IF NOT EXISTS idx_{self.vendor}_symbol_timestamp 
            ON {full_table_name}(symbol, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_{self.vendor}_timestamp 
            ON {full_table_name}(timestamp DESC);
        """
        
        await self.execute_raw_sql(create_sql)
        self.logger.info(f"Table {full_table_name} ready")
    
    async def insert_minute_prices(self, prices: List[MinutePrice]) -> int:
        """Insert minute prices using existing DAO patterns"""
        if not prices:
            return 0
        
        full_table_name = self.env.get_table_name(self.table_name)
        
        # Use existing instrument resolution
        from dao.instrument_xrefs_dao import InstrumentXrefsDAO
        instrument_dao = InstrumentXrefsDAO(self.env)
        
        inserted_count = 0
        for price in prices:
            try:
                # Get instrument_id using existing pattern
                instrument_id = await instrument_dao.get_instrument_id(price.symbol)
                if not instrument_id:
                    self.logger.warning(f"No instrument found for symbol {price.symbol}")
                    continue
                
                insert_sql = f"""
                INSERT INTO {full_table_name}
                (instrument_id, symbol, timestamp, open_price, high_price, low_price, 
                 close_price, volume, quality_score)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (instrument_id, timestamp) DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    quality_score = EXCLUDED.quality_score,
                    collected_at = EXCLUDED.collected_at
                """
                
                await self.execute_raw_sql(
                    insert_sql,
                    instrument_id, price.symbol, price.timestamp,
                    price.open_price, price.high_price, price.low_price, 
                    price.close_price, price.volume, price.quality_score
                )
                inserted_count += 1
                
            except Exception as e:
                self.logger.error(f"Error inserting price for {price.symbol}: {e}")
        
        return inserted_count

class VendorCollector:
    """Base class for vendor data collection - extends existing patterns"""
    
    def __init__(self, vendor: str, env: Environment):
        self.vendor = vendor
        self.env = env
        self.dao = MinutePriceDAO(vendor, env)
        self.logger = get_logger(f"{__name__}.{vendor}")
        
    async def initialize(self):
        """Initialize collector and create tables"""
        await self.dao.create_table_if_not_exists()
    
    async def collect_minute_data(self, symbols: List[str]) -> List[MinutePrice]:
        """Abstract method for data collection"""
        raise NotImplementedError

class PolygonMinuteCollector(VendorCollector):
    """Polygon minute data collector - reuses existing API patterns"""
    
    def __init__(self, env: Environment):
        super().__init__('polygon', env)
        from config.polygon import POLYGON_API_KEY
        self.api_key = POLYGON_API_KEY
    
    async def collect_minute_data(self, symbols: List[str]) -> List[MinutePrice]:
        """Collect minute data using existing Polygon patterns"""
        prices = []
        
        if not self.api_key:
            self.logger.warning("Polygon API key not configured")
            return prices
        
        import aiohttp
        async with aiohttp.ClientSession() as session:
            for symbol in symbols:
                try:
                    # Use Polygon aggregates API for minute data
                    today = datetime.now().strftime('%Y-%m-%d')
                    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{today}/{today}"
                    params = {
                        'apikey': self.api_key,
                        'adjusted': 'true',
                        'sort': 'desc',
                        'limit': 60  # Last 60 minutes
                    }
                    
                    async with session.get(url, params=params, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            if 'results' in data:
                                for item in data['results']:
                                    price = MinutePrice(
                                        symbol=symbol,
                                        timestamp=datetime.fromtimestamp(item['t'] / 1000),
                                        open_price=float(item['o']),
                                        high_price=float(item['h']),
                                        low_price=float(item['l']),
                                        close_price=float(item['c']),
                                        volume=int(item['v']),
                                        vendor='polygon'
                                    )
                                    prices.append(price)
                        else:
                            self.logger.warning(f"Polygon API error for {symbol}: {response.status}")
                    
                    # Rate limiting for Polygon API
                    await asyncio.sleep(12)  # 5 requests per minute
                    
                except Exception as e:
                    self.logger.error(f"Error collecting Polygon data for {symbol}: {e}")
        
        self.logger.info(f"Collected {len(prices)} minute prices from Polygon")
        return prices

class TiingoMinuteCollector(VendorCollector):
    """Tiingo minute data collector - reuses existing patterns"""
    
    def __init__(self, env: Environment):
        super().__init__('tiingo', env)
        self.api_key = TIINGO_API_KEY
    
    async def collect_minute_data(self, symbols: List[str]) -> List[MinutePrice]:
        """Collect minute data using existing Tiingo patterns"""
        prices = []
        
        if not self.api_key:
            self.logger.warning("Tiingo API key not configured")
            return prices
        
        import aiohttp
        async with aiohttp.ClientSession() as session:
            for symbol in symbols:
                try:
                    # Use Tiingo IEX data for minute bars
                    url = f"https://api.tiingo.com/iex/{symbol}/prices"
                    params = {
                        'token': self.api_key,
                        'startDate': datetime.now().strftime('%Y-%m-%d'),
                        'endDate': datetime.now().strftime('%Y-%m-%d'),
                        'resampleFreq': '1min'
                    }
                    
                    async with session.get(url, params=params, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            for item in data[-60:]:  # Last 60 minutes
                                price = MinutePrice(
                                    symbol=symbol,
                                    timestamp=datetime.fromisoformat(item['date'].replace('Z', '+00:00')),
                                    open_price=float(item['open']),
                                    high_price=float(item['high']),
                                    low_price=float(item['low']),
                                    close_price=float(item['close']),
                                    volume=int(item['volume']),
                                    vendor='tiingo'
                                )
                                prices.append(price)
                        else:
                            self.logger.warning(f"Tiingo API error for {symbol}: {response.status}")
                    
                    await asyncio.sleep(0.2)  # Tiingo rate limiting
                    
                except Exception as e:
                    self.logger.error(f"Error collecting Tiingo data for {symbol}: {e}")
        
        self.logger.info(f"Collected {len(prices)} minute prices from Tiingo")
        return prices

class FMPMinuteCollector(VendorCollector):
    """FMP minute data collector - reuses existing patterns"""
    
    def __init__(self, env: Environment):
        super().__init__('fmp', env)
        self.api_key = os.getenv('FMP_API_KEY', '')
    
    async def collect_minute_data(self, symbols: List[str]) -> List[MinutePrice]:
        """Collect minute data using FMP patterns"""
        prices = []
        
        if not self.api_key:
            self.logger.warning("FMP API key not configured")
            return prices
        
        import aiohttp
        async with aiohttp.ClientSession() as session:
            for symbol in symbols:
                try:
                    url = f"https://financialmodelingprep.com/api/v3/historical-chart/1min/{symbol}"
                    params = {'apikey': self.api_key}
                    
                    async with session.get(url, params=params, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            for item in data[:60]:  # Latest 60 minutes
                                price = MinutePrice(
                                    symbol=symbol,
                                    timestamp=datetime.fromisoformat(item['date']),
                                    open_price=float(item['open']),
                                    high_price=float(item['high']),
                                    low_price=float(item['low']),
                                    close_price=float(item['close']),
                                    volume=int(item['volume']),
                                    vendor='fmp'
                                )
                                prices.append(price)
                        else:
                            self.logger.warning(f"FMP API error for {symbol}: {response.status}")
                    
                    await asyncio.sleep(1)  # FMP rate limiting
                    
                except Exception as e:
                    self.logger.error(f"Error collecting FMP data for {symbol}: {e}")
        
        self.logger.info(f"Collected {len(prices)} minute prices from FMP")
        return prices

# Initialize configuration
service_config = MinuteServiceConfig()

# FastAPI Application - Integrates with existing patterns
app = FastAPI(
    title=service_config.title,
    description=service_config.description,
    version=service_config.version
)

class MinutePriceService:
    """Main service orchestrator using existing patterns"""
    
    def __init__(self):
        self.env = Environment(gin_config_path='config/app_dev.gin')
        self.collectors = {
            'polygon': PolygonMinuteCollector(self.env),
            'tiingo': TiingoMinuteCollector(self.env), 
            'fmp': FMPMinuteCollector(self.env)
        }
        self.symbols = service_config.default_symbols
        self.is_running = False
        self.metrics = {
            'total_collected': 0,
            'successful_inserts': 0,
            'errors': 0,
            'last_run': None
        }
        self.logger = get_logger(__name__)
    
    async def initialize(self):
        """Initialize all collectors"""
        for collector in self.collectors.values():
            await collector.initialize()
        self.logger.info("Minute price service initialized")
    
    async def run_collection_cycle(self):
        """Run data collection cycle for all vendors"""
        self.logger.info("Starting minute price collection cycle")
        
        # Collect from all vendors concurrently
        tasks = [
            collector.collect_minute_data(self.symbols)
            for collector in self.collectors.values()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        total_collected = 0
        for i, result in enumerate(results):
            vendor = list(self.collectors.keys())[i]
            
            if isinstance(result, Exception):
                self.logger.error(f"Collection failed for {vendor}: {result}")
                self.metrics['errors'] += 1
            else:
                # Store collected data
                inserted = await self.collectors[vendor].dao.insert_minute_prices(result)
                total_collected += len(result)
                self.metrics['successful_inserts'] += inserted
                self.logger.info(f"{vendor}: collected {len(result)}, inserted {inserted}")
        
        self.metrics['total_collected'] += total_collected
        self.metrics['last_run'] = datetime.now().isoformat()
        self.logger.info(f"Collection cycle complete: {total_collected} prices collected")
    
    async def start_continuous_collection(self):
        """Start continuous collection loop"""
        self.is_running = True
        
        while self.is_running:
            try:
                await self.run_collection_cycle()
                await asyncio.sleep(60)  # Collect every minute
            except Exception as e:
                self.logger.error(f"Collection loop error: {e}")
                self.metrics['errors'] += 1
                await asyncio.sleep(30)

# Global service instance
service = MinutePriceService()

@app.on_event("startup")
async def startup():
    """Service startup using existing patterns"""
    logger.info("Starting ATS Minute Price Service")
    await service.initialize()
    
    # Start collection in background
    asyncio.create_task(service.start_continuous_collection())
    logger.info("Service started successfully")

@app.on_event("shutdown") 
async def shutdown():
    """Service shutdown"""
    logger.info("Shutting down service")
    service.is_running = False

# API Endpoints following existing patterns
@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy" if service.is_running else "stopped",
        "environment": service.env.env_type.value,
        "vendors": list(service.collectors.keys()),
        "symbols": service.symbols,
        "metrics": service.metrics,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/metrics")
async def get_metrics():
    """Metrics endpoint"""
    return service.metrics

@app.post("/collect")
async def trigger_collection(background_tasks: BackgroundTasks):
    """Manual collection trigger"""
    background_tasks.add_task(service.run_collection_cycle)
    return {"message": "Collection triggered", "timestamp": datetime.now().isoformat()}

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "ATS Minute Price Service",
        "version": "1.0.0",
        "framework": "Reuses existing ATS patterns",
        "status": "running" if service.is_running else "stopped"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=service_config.host, port=service_config.port)