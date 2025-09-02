#!/usr/bin/env python3
"""
AAPL and TSLA Real-time Minute Data Collector

Collects 1-minute OHLCV data from Tiingo and Polygon for AAPL and TSLA symbols
and stores it in real-time live tables.
"""

import asyncio
import asyncpg
import logging
import os
from datetime import datetime, timedelta
from typing import List, Optional
import aiohttp

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AAPLTSLARealtimeCollector:
    """Real-time collector for AAPL and TSLA minute data"""
    
    def __init__(self):
        # Configuration
        self.symbols = ['AAPL', 'TSLA']
        self.collection_interval = 60  # seconds
        
        # Database connection
        self.db_host = os.getenv('DB_HOST', 'ats-intg-postgres')
        self.db_port = int(os.getenv('DB_PORT', '5432'))
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', 'intg_password')
        self.db_name = os.getenv('DB_NAME', 'intg_db')
        
        # API keys
        self.tiingo_api_key = os.getenv('TIINGO_API_KEY')
        self.polygon_api_key = os.getenv('POLYGON_API_KEY')
        
        self.pool = None
        self.session = None
        self.running = False
        
        logger.info(f"Initialized collector for symbols: {self.symbols}")
        logger.info(f"Database: {self.db_host}:{self.db_port}/{self.db_name}")
        logger.info(f"Tiingo API configured: {bool(self.tiingo_api_key)}")
        logger.info(f"Polygon API configured: {bool(self.polygon_api_key)}")
    
    async def initialize(self):
        """Initialize database connection and HTTP session"""
        # Database connection pool
        dsn = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        self.pool = await asyncpg.create_pool(
            dsn,
            min_size=2,
            max_size=10,
            command_timeout=30
        )
        
        # HTTP session
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        
        logger.info("✅ Database and HTTP session initialized")
    
    async def collect_tiingo_minute_data(self, symbol: str) -> List[dict]:
        """Collect minute data from Tiingo IEX API"""
        if not self.tiingo_api_key:
            logger.warning("No Tiingo API key configured")
            return []
        
        # Get last 2 hours of minute data
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=2)
        
        url = f"https://api.tiingo.com/iex/{symbol}/prices"
        params = {
            'token': self.tiingo_api_key,
            'startDate': start_time.strftime('%Y-%m-%d'),
            'endDate': end_time.strftime('%Y-%m-%d'),
            'resampleFreq': '1min',
            'format': 'json'
        }
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    minute_bars = []
                    for item in data:
                        if not item:
                            continue
                            
                        # Parse timestamp
                        timestamp = datetime.fromisoformat(item['date'].replace('Z', '+00:00'))
                        
                        # Skip if older than 1 hour (avoid duplicates)
                        if timestamp < datetime.now().replace(tzinfo=timestamp.tzinfo) - timedelta(hours=1):
                            continue
                        
                        minute_bars.append({
                            'symbol': symbol,
                            'timestamp': timestamp,
                            'open_price': float(item['open']),
                            'high_price': float(item['high']),
                            'low_price': float(item['low']),
                            'close_price': float(item['close']),
                            'volume': int(item.get('volume', 0)),
                            'vendor': 'tiingo',
                            'quality_score': 0.9,  # Default quality score
                            'data_latency_ms': int((datetime.now().replace(tzinfo=timestamp.tzinfo) - timestamp).total_seconds() * 1000)
                        })
                    
                    logger.info(f"Collected {len(minute_bars)} Tiingo minute bars for {symbol}")
                    return minute_bars
                    
                elif response.status == 429:
                    logger.warning(f"Tiingo rate limit hit for {symbol}")
                    return []
                else:
                    logger.error(f"Tiingo API error for {symbol}: {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching Tiingo data for {symbol}: {e}")
            return []
    
    async def collect_polygon_minute_data(self, symbol: str) -> List[dict]:
        """Collect minute data from Polygon API"""
        if not self.polygon_api_key:
            logger.warning("No Polygon API key configured")
            return []
        
        # Get last 2 hours of minute data
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=2)
        
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start_time.strftime('%Y-%m-%d')}/{end_time.strftime('%Y-%m-%d')}"
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 1000,
            'apiKey': self.polygon_api_key
        }
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    results = data.get('results', [])
                    
                    minute_bars = []
                    for item in results:
                        # Parse timestamp (milliseconds)
                        timestamp = datetime.fromtimestamp(item['t'] / 1000)
                        
                        # Skip if older than 1 hour (avoid duplicates)
                        if timestamp < datetime.now() - timedelta(hours=1):
                            continue
                        
                        minute_bars.append({
                            'symbol': symbol,
                            'timestamp': timestamp,
                            'open_price': float(item['o']),
                            'high_price': float(item['h']),
                            'low_price': float(item['l']),
                            'close_price': float(item['c']),
                            'volume': int(item['v']),
                            'vwap': item.get('vw'),
                            'trade_count': item.get('n'),
                            'vendor': 'polygon',
                            'quality_score': 0.95,  # Polygon typically has high quality
                            'data_latency_ms': int((datetime.now() - timestamp).total_seconds() * 1000)
                        })
                    
                    logger.info(f"Collected {len(minute_bars)} Polygon minute bars for {symbol}")
                    return minute_bars
                    
                elif response.status == 429:
                    logger.warning(f"Polygon rate limit hit for {symbol}")
                    return []
                else:
                    logger.error(f"Polygon API error for {symbol}: {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching Polygon data for {symbol}: {e}")
            return []
    
    async def store_tiingo_data(self, bars: List[dict]) -> int:
        """Store Tiingo minute bars in database"""
        if not bars:
            return 0
        
        insert_query = """
        INSERT INTO intg_one_minute_live_tiingo (
            symbol, timestamp, open_price, high_price, low_price, close_price,
            volume, vendor, data_latency_ms, quality_score, received_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (symbol, timestamp) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            data_latency_ms = EXCLUDED.data_latency_ms,
            quality_score = EXCLUDED.quality_score,
            received_at = EXCLUDED.received_at
        """
        
        stored_count = 0
        async with self.pool.acquire() as conn:
            for bar in bars:
                try:
                    await conn.execute(
                        insert_query,
                        bar['symbol'], bar['timestamp'], bar['open_price'],
                        bar['high_price'], bar['low_price'], bar['close_price'],
                        bar['volume'], bar['vendor'], bar['data_latency_ms'],
                        bar['quality_score'], datetime.now()
                    )
                    stored_count += 1
                except Exception as e:
                    logger.error(f"Error storing Tiingo bar: {e}")
        
        return stored_count
    
    async def store_polygon_data(self, bars: List[dict]) -> int:
        """Store Polygon minute bars in database"""
        if not bars:
            return 0
        
        insert_query = """
        INSERT INTO intg_one_minute_live_polygon (
            symbol, timestamp, open_price, high_price, low_price, close_price,
            volume, vwap, trade_count, vendor, data_latency_ms, quality_score, received_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (symbol, timestamp) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            vwap = EXCLUDED.vwap,
            trade_count = EXCLUDED.trade_count,
            data_latency_ms = EXCLUDED.data_latency_ms,
            quality_score = EXCLUDED.quality_score,
            received_at = EXCLUDED.received_at
        """
        
        stored_count = 0
        async with self.pool.acquire() as conn:
            for bar in bars:
                try:
                    await conn.execute(
                        insert_query,
                        bar['symbol'], bar['timestamp'], bar['open_price'],
                        bar['high_price'], bar['low_price'], bar['close_price'],
                        bar['volume'], bar.get('vwap'), bar.get('trade_count'),
                        bar['vendor'], bar['data_latency_ms'],
                        bar['quality_score'], datetime.now()
                    )
                    stored_count += 1
                except Exception as e:
                    logger.error(f"Error storing Polygon bar: {e}")
        
        return stored_count
    
    async def collect_and_store_data(self):
        """Collect and store data for all symbols from both vendors"""
        total_stored = 0
        
        for symbol in self.symbols:
            try:
                # Collect from Tiingo
                tiingo_bars = await self.collect_tiingo_minute_data(symbol)
                tiingo_stored = await self.store_tiingo_data(tiingo_bars)
                
                # Collect from Polygon
                polygon_bars = await self.collect_polygon_minute_data(symbol)
                polygon_stored = await self.store_polygon_data(polygon_bars)
                
                symbol_total = tiingo_stored + polygon_stored
                total_stored += symbol_total
                
                logger.info(f"✅ {symbol}: Stored {tiingo_stored} Tiingo + {polygon_stored} Polygon = {symbol_total} total bars")
                
                # Small delay between symbols to avoid rate limits
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
        
        return total_stored
    
    async def start_collection(self):
        """Start continuous real-time data collection"""
        logger.info("🚀 Starting AAPL/TSLA real-time data collection...")
        self.running = True
        
        try:
            while self.running:
                start_time = datetime.now()
                
                # Collect and store data
                total_stored = await self.collect_and_store_data()
                
                collection_time = (datetime.now() - start_time).total_seconds()
                logger.info(f"📊 Collection cycle completed: {total_stored} bars stored in {collection_time:.1f}s")
                
                # Wait for next collection cycle
                sleep_time = max(0, self.collection_interval - collection_time)
                logger.info(f"⏱️ Waiting {sleep_time:.1f}s until next collection...")
                await asyncio.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("🛑 Collection stopped by user")
        except Exception as e:
            logger.error(f"💥 Collection error: {e}")
        finally:
            self.running = False
    
    async def shutdown(self):
        """Cleanup resources"""
        self.running = False
        
        if self.session:
            await self.session.close()
        
        if self.pool:
            await self.pool.close()
        
        logger.info("✅ Collector shutdown complete")

async def main():
    """Main entry point"""
    collector = AAPLTSLARealtimeCollector()
    
    try:
        await collector.initialize()
        await collector.start_collection()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        await collector.shutdown()

if __name__ == "__main__":
    asyncio.run(main())