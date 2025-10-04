#!/usr/bin/env python3
"""
ATS 1-Minute Intraday Price Populator Service
Collects 1-minute OHLCV data from Tiingo, Polygon, and FMP
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
import aiohttp
import asyncpg
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Data models
@dataclass
class MinuteBar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vendor: str
    collected_at: datetime = None

    def __post_init__(self):
        if self.collected_at is None:
            self.collected_at = datetime.now(timezone.utc)

class HealthStatus(BaseModel):
    status: str
    timestamp: str
    database_connected: bool
    services_status: Dict[str, str]
    last_data_collection: Optional[str]
    metrics: Dict[str, Any]

class ValidationRequest(BaseModel):
    symbols: List[str] = ["AAPL", "MSFT", "GOOGL", "AMZN"]
    start_time: str
    end_time: str

# Main application
app = FastAPI(
    title="ATS Intraday Price Populator",
    description="1-minute OHLCV data collection from multiple vendors",
    version="1.0.0"
)

class IntradayPopulator:
    def __init__(self):
        self.db_pool: Optional[asyncpg.Pool] = None
        self.is_running = False
        self.metrics = {
            'total_bars_collected': 0,
            'successful_inserts': 0,
            'failed_inserts': 0,
            'api_calls': {
                'tiingo': 0,
                'polygon': 0,
                'fmp': 0
            },
            'last_collection_time': None,
            'error_count': 0,
            'uptime_start': datetime.now(timezone.utc)
        }

        # Configuration from environment
        self.db_config = {
            'host': os.getenv('DB_HOST', 'postgres-simple'),
            'port': int(os.getenv('DB_PORT', '5432')),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'dev_password'),
            'database': os.getenv('DB_NAME', 'dev_db')
        }

        self.api_keys = {
            'tiingo': os.getenv('TIINGO_API_KEY', ''),
            'polygon': os.getenv('POLYGON_API_KEY', ''),
            'fmp': os.getenv('FMP_API_KEY', '')
        }

        # Symbols to track
        self.symbols = os.getenv('SYMBOLS', 'AAPL,MSFT,GOOGL,AMZN,TSLA,META,NVDA,NFLX').split(',')

        logger.info(f"Initialized populator - DB: {self.db_config['host']}:{self.db_config['port']}")
        logger.info(f"Tracking symbols: {', '.join(self.symbols)}")

    async def initialize_db(self) -> bool:
        """Initialize database connection and create tables"""
        try:
            dsn = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.db_pool = await asyncpg.create_pool(
                dsn,
                min_size=2,
                max_size=10,
                command_timeout=30
            )

            # Test connection and create tables
            async with self.db_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
                await self._create_tables(conn)
                logger.info("Database connection established and tables created")

            return True
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            return False

    async def _create_tables(self, conn):
        """Create vendor-specific minute data tables"""
        vendors = ['tiingo', 'polygon', 'fmp']

        for vendor in vendors:
            table_name = f"dev_minute_prices_{vendor}"
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    open_price DECIMAL(20,6) NOT NULL,
                    high_price DECIMAL(20,6) NOT NULL,
                    low_price DECIMAL(20,6) NOT NULL,
                    close_price DECIMAL(20,6) NOT NULL,
                    volume BIGINT NOT NULL DEFAULT 0,
                    collected_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(symbol, timestamp)
                )
            """)

            # Create indexes for performance
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{vendor}_symbol_timestamp
                ON {table_name}(symbol, timestamp DESC)
            """)

            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{vendor}_timestamp
                ON {table_name}(timestamp DESC)
            """)

        # Create service health table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS dev_service_health_checks (
                id BIGSERIAL PRIMARY KEY,
                service_name VARCHAR(50) NOT NULL,
                check_time TIMESTAMPTZ DEFAULT NOW(),
                status VARCHAR(20) NOT NULL,
                response_time_ms INTEGER,
                error_message TEXT,
                metadata JSONB DEFAULT '{}'
            )
        """)

        logger.info("Database tables created/verified")

    async def collect_tiingo_data(self) -> List[MinuteBar]:
        """Collect 1-minute data from Tiingo API"""
        bars = []
        try:
            if not self.api_keys['tiingo']:
                logger.warning("Tiingo API key not configured")
                return bars

            async with aiohttp.ClientSession() as session:
                for symbol in self.symbols:
                    try:
                        # Get last 1 hour of 1-minute data
                        end_time = datetime.now(timezone.utc)
                        start_time = end_time - timedelta(hours=1)

                        url = f"https://api.tiingo.com/iex/{symbol}/prices"
                        params = {
                            'token': self.api_keys['tiingo'],
                            'startDate': start_time.strftime('%Y-%m-%d'),
                            'endDate': end_time.strftime('%Y-%m-%d'),
                            'resampleFreq': '1min'
                        }

                        async with session.get(url, params=params, timeout=30) as response:
                            self.metrics['api_calls']['tiingo'] += 1

                            if response.status == 200:
                                data = await response.json()
                                for item in data[-10:]:  # Last 10 minutes
                                    try:
                                        bar = MinuteBar(
                                            symbol=symbol,
                                            timestamp=datetime.fromisoformat(item['date'].replace('Z', '+00:00')),
                                            open=float(item['open']),
                                            high=float(item['high']),
                                            low=float(item['low']),
                                            close=float(item['close']),
                                            volume=int(item['volume']),
                                            vendor='tiingo'
                                        )
                                        bars.append(bar)
                                    except (KeyError, ValueError) as e:
                                        logger.warning(f"Invalid Tiingo data for {symbol}: {e}")
                            else:
                                logger.warning(f"Tiingo API error for {symbol}: {response.status}")

                    except Exception as e:
                        logger.error(f"Error collecting Tiingo data for {symbol}: {e}")
                        self.metrics['error_count'] += 1

                    # Rate limiting - Tiingo allows 500 requests/hour
                    await asyncio.sleep(0.1)

        except Exception as e:
            logger.error(f"Tiingo collection error: {e}")
            self.metrics['error_count'] += 1

        logger.info(f"Collected {len(bars)} bars from Tiingo")
        return bars

    async def collect_polygon_data(self) -> List[MinuteBar]:
        """Collect 1-minute data from Polygon API"""
        bars = []
        try:
            if not self.api_keys['polygon']:
                logger.warning("Polygon API key not configured")
                return bars

            async with aiohttp.ClientSession() as session:
                for symbol in self.symbols:
                    try:
                        # Get last 1 hour of 1-minute data
                        end_date = datetime.now(timezone.utc).date()

                        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{end_date}/{end_date}"
                        params = {
                            'apikey': self.api_keys['polygon'],
                            'adjusted': 'true',
                            'sort': 'desc',
                            'limit': 10
                        }

                        async with session.get(url, params=params, timeout=30) as response:
                            self.metrics['api_calls']['polygon'] += 1

                            if response.status == 200:
                                data = await response.json()
                                if 'results' in data:
                                    for item in data['results']:
                                        try:
                                            bar = MinuteBar(
                                                symbol=symbol,
                                                timestamp=datetime.fromtimestamp(item['t'] / 1000, tz=timezone.utc),
                                                open=float(item['o']),
                                                high=float(item['h']),
                                                low=float(item['l']),
                                                close=float(item['c']),
                                                volume=int(item['v']),
                                                vendor='polygon'
                                            )
                                            bars.append(bar)
                                        except (KeyError, ValueError) as e:
                                            logger.warning(f"Invalid Polygon data for {symbol}: {e}")
                            else:
                                logger.warning(f"Polygon API error for {symbol}: {response.status}")

                    except Exception as e:
                        logger.error(f"Error collecting Polygon data for {symbol}: {e}")
                        self.metrics['error_count'] += 1

                    # Rate limiting - Polygon allows 5 requests/minute for free tier
                    await asyncio.sleep(12)

        except Exception as e:
            logger.error(f"Polygon collection error: {e}")
            self.metrics['error_count'] += 1

        logger.info(f"Collected {len(bars)} bars from Polygon")
        return bars

    async def collect_fmp_data(self) -> List[MinuteBar]:
        """Collect 1-minute data from FMP API"""
        bars = []
        try:
            if not self.api_keys['fmp']:
                logger.warning("FMP API key not configured")
                return bars

            async with aiohttp.ClientSession() as session:
                for symbol in self.symbols:
                    try:
                        url = f"https://financialmodelingprep.com/api/v3/historical-chart/1min/{symbol}"
                        params = {
                            'apikey': self.api_keys['fmp']
                        }

                        async with session.get(url, params=params, timeout=30) as response:
                            self.metrics['api_calls']['fmp'] += 1

                            if response.status == 200:
                                data = await response.json()
                                for item in data[:10]:  # Last 10 minutes
                                    try:
                                        bar = MinuteBar(
                                            symbol=symbol,
                                            timestamp=datetime.fromisoformat(item['date']),
                                            open=float(item['open']),
                                            high=float(item['high']),
                                            low=float(item['low']),
                                            close=float(item['close']),
                                            volume=int(item['volume']),
                                            vendor='fmp'
                                        )
                                        bars.append(bar)
                                    except (KeyError, ValueError) as e:
                                        logger.warning(f"Invalid FMP data for {symbol}: {e}")
                            else:
                                logger.warning(f"FMP API error for {symbol}: {response.status}")

                    except Exception as e:
                        logger.error(f"Error collecting FMP data for {symbol}: {e}")
                        self.metrics['error_count'] += 1

                    # Rate limiting - FMP allows 250 requests/day for free tier
                    await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"FMP collection error: {e}")
            self.metrics['error_count'] += 1

        logger.info(f"Collected {len(bars)} bars from FMP")
        return bars

    async def store_bars(self, bars: List[MinuteBar]):
        """Store collected bars in vendor-specific tables"""
        if not bars:
            return

        try:
            async with self.db_pool.acquire() as conn:
                vendor_groups = {}
                for bar in bars:
                    if bar.vendor not in vendor_groups:
                        vendor_groups[bar.vendor] = []
                    vendor_groups[bar.vendor].append(bar)

                for vendor, vendor_bars in vendor_groups.items():
                    table_name = f"dev_minute_prices_{vendor}"

                    # Prepare batch insert
                    records = [
                        (
                            bar.symbol,
                            bar.timestamp,
                            bar.open,
                            bar.high,
                            bar.low,
                            bar.close,
                            bar.volume,
                            bar.collected_at
                        )
                        for bar in vendor_bars
                    ]

                    # Insert with ON CONFLICT handling
                    await conn.executemany(f"""
                        INSERT INTO {table_name}
                        (symbol, timestamp, open_price, high_price, low_price, close_price, volume, collected_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (symbol, timestamp) DO UPDATE SET
                            open_price = EXCLUDED.open_price,
                            high_price = EXCLUDED.high_price,
                            low_price = EXCLUDED.low_price,
                            close_price = EXCLUDED.close_price,
                            volume = EXCLUDED.volume,
                            collected_at = EXCLUDED.collected_at
                    """, records)

                    self.metrics['successful_inserts'] += len(records)
                    logger.info(f"Stored {len(records)} bars for {vendor}")

                self.metrics['total_bars_collected'] += len(bars)
                self.metrics['last_collection_time'] = datetime.now(timezone.utc).isoformat()

        except Exception as e:
            logger.error(f"Error storing bars: {e}")
            self.metrics['failed_inserts'] += len(bars)
            self.metrics['error_count'] += 1

    async def run_collection_cycle(self):
        """Run a single collection cycle for all vendors"""
        logger.info("Starting collection cycle")

        # Collect from all vendors concurrently
        tasks = [
            self.collect_tiingo_data(),
            self.collect_polygon_data(),
            self.collect_fmp_data()
        ]

        all_bars = []
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Collection task failed: {result}")
                self.metrics['error_count'] += 1
            elif isinstance(result, list):
                all_bars.extend(result)

        # Store collected data
        if all_bars:
            await self.store_bars(all_bars)
            logger.info(f"Collection cycle complete: {len(all_bars)} bars collected")
        else:
            logger.warning("No data collected in this cycle")

    async def start_collection_loop(self):
        """Start the continuous data collection loop"""
        self.is_running = True
        logger.info("Starting continuous data collection")

        while self.is_running:
            try:
                await self.run_collection_cycle()

                # Wait 1 minute between collections
                await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"Collection loop error: {e}")
                self.metrics['error_count'] += 1
                await asyncio.sleep(30)  # Wait before retry

    def stop_collection(self):
        """Stop the data collection loop"""
        self.is_running = False
        logger.info("Stopping data collection")

# Global populator instance
populator = IntradayPopulator()

@app.on_event("startup")
async def startup():
    """Application startup"""
    logger.info("Starting Intraday Price Populator Service")

    # Initialize database
    if not await populator.initialize_db():
        logger.error("Failed to initialize database")
        return

    # Start collection in background
    asyncio.create_task(populator.start_collection_loop())
    logger.info("Service started successfully")

@app.on_event("shutdown")
async def shutdown():
    """Application shutdown"""
    logger.info("Shutting down service")
    populator.stop_collection()
    if populator.db_pool:
        await populator.db_pool.close()

# API Endpoints

@app.get("/health", response_model=HealthStatus)
async def health_check():
    """Health check endpoint"""
    services_status = {
        "tiingo": "configured" if populator.api_keys['tiingo'] else "not_configured",
        "polygon": "configured" if populator.api_keys['polygon'] else "not_configured",
        "fmp": "configured" if populator.api_keys['fmp'] else "not_configured",
    }

    return HealthStatus(
        status="healthy" if populator.is_running else "stopped",
        timestamp=datetime.now(timezone.utc).isoformat(),
        database_connected=populator.db_pool is not None,
        services_status=services_status,
        last_data_collection=populator.metrics['last_collection_time'],
        metrics=populator.metrics
    )

@app.get("/ready")
async def readiness_check():
    """Kubernetes readiness probe"""
    if populator.db_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    return {"ready": True, "timestamp": datetime.now(timezone.utc).isoformat()}

@app.get("/metrics")
async def get_metrics():
    """Prometheus-style metrics endpoint"""
    uptime = (datetime.now(timezone.utc) - populator.metrics['uptime_start']).total_seconds()

    metrics = {
        **populator.metrics,
        "uptime_seconds": uptime,
        "collection_running": populator.is_running
    }
    return metrics

@app.post("/collect/manual")
async def manual_collection(background_tasks: BackgroundTasks):
    """Trigger manual data collection"""
    if not populator.is_running:
        raise HTTPException(status_code=503, detail="Service not running")

    background_tasks.add_task(populator.run_collection_cycle)
    return {"message": "Manual collection triggered", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.post("/validate")
async def validate_data(request: ValidationRequest):
    """Validate collected data quality"""
    try:
        async with populator.db_pool.acquire() as conn:
            validation_results = {}

            for vendor in ['tiingo', 'polygon', 'fmp']:
                table_name = f"dev_minute_prices_{vendor}"

                # Check data completeness
                count = await conn.fetchval(f"""
                    SELECT COUNT(*) FROM {table_name}
                    WHERE symbol = ANY($1)
                    AND timestamp >= $2::timestamptz
                    AND timestamp <= $3::timestamptz
                """, request.symbols, request.start_time, request.end_time)

                # Check for gaps
                gaps = await conn.fetch(f"""
                    SELECT symbol, COUNT(*) as records
                    FROM {table_name}
                    WHERE symbol = ANY($1)
                    AND timestamp >= $2::timestamptz
                    AND timestamp <= $3::timestamptz
                    GROUP BY symbol
                    ORDER BY records DESC
                """, request.symbols, request.start_time, request.end_time)

                validation_results[vendor] = {
                    "total_records": count,
                    "symbol_distribution": [{"symbol": gap["symbol"], "records": gap["records"]} for gap in gaps],
                    "status": "healthy" if count > 0 else "no_data"
                }

            return {
                "validation_time": datetime.now(timezone.utc).isoformat(),
                "request": request.dict(),
                "results": validation_results
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation error: {str(e)}")

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "ATS Intraday Price Populator",
        "version": "1.0.0",
        "status": "running" if populator.is_running else "stopped",
        "endpoints": {
            "health": "/health",
            "metrics": "/metrics",
            "manual_collection": "/collect/manual",
            "validation": "/validate"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)