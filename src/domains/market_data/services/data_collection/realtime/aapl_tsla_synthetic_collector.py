#!/usr/bin/env python3
"""
AAPL and TSLA Synthetic Real-time Minute Data Collector

Generates synthetic real-time minute data for AAPL and TSLA to demonstrate
the real-time collection infrastructure without requiring API keys.
"""

import asyncio
import asyncpg
import logging
import os
import random
from datetime import datetime, timedelta
from typing import List

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AAPLTSLASyntheticCollector:
    """Synthetic real-time collector for AAPL and TSLA minute data"""
    
    def __init__(self):
        # Configuration
        self.symbols = ['AAPL', 'TSLA']
        self.collection_interval = 60  # seconds
        
        # Base prices for realistic data generation (updated to current market prices)
        self.base_prices = {
            'AAPL': 225.0,  # Current AAPL price around $225
            'TSLA': 330.0   # Current TSLA price around $330
        }
        
        # Database connection
        self.db_host = os.getenv('DB_HOST', 'ats-intg-postgres')
        self.db_port = int(os.getenv('DB_PORT', '5432'))
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', 'intg_password')
        self.db_name = os.getenv('DB_NAME', 'intg_db')
        
        self.pool = None
        self.running = False
        
        logger.info(f"Initialized synthetic collector for symbols: {self.symbols}")
        logger.info(f"Database: {self.db_host}:{self.db_port}/{self.db_name}")
    
    async def initialize(self):
        """Initialize database connection"""
        dsn = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        self.pool = await asyncpg.create_pool(
            dsn,
            min_size=2,
            max_size=10,
            command_timeout=30
        )
        
        logger.info("✅ Database connection initialized")
    
    def generate_minute_bar(self, symbol: str, timestamp: datetime, vendor: str) -> dict:
        """Generate a realistic synthetic minute bar"""
        base_price = self.base_prices[symbol]
        
        # Add some realistic price movement (±2%)
        price_change = random.uniform(-0.02, 0.02)
        current_price = base_price * (1 + price_change)
        
        # Generate OHLC with realistic relationships
        spread = current_price * random.uniform(0.001, 0.005)  # 0.1% to 0.5% spread
        
        open_price = current_price + random.uniform(-spread/2, spread/2)
        close_price = current_price + random.uniform(-spread/2, spread/2)
        high_price = max(open_price, close_price) + random.uniform(0, spread)
        low_price = min(open_price, close_price) - random.uniform(0, spread)
        
        # Generate realistic volume
        base_volume = 50000 if symbol == 'AAPL' else 30000
        volume = int(base_volume * random.uniform(0.5, 2.0))
        
        # Calculate VWAP (for Polygon)
        vwap = (high_price + low_price + close_price) / 3
        
        bar_data = {
            'symbol': symbol,
            'timestamp': timestamp,
            'open_price': round(open_price, 2),
            'high_price': round(high_price, 2),
            'low_price': round(low_price, 2),
            'close_price': round(close_price, 2),
            'volume': volume,
            'vendor': vendor,
            'quality_score': round(random.uniform(0.85, 0.98), 3),
            'data_latency_ms': random.randint(100, 3000)  # 0.1s to 3s latency
        }
        
        if vendor == 'polygon':
            bar_data['vwap'] = round(vwap, 2)
            bar_data['trade_count'] = random.randint(100, 1000)
        
        return bar_data
    
    async def store_tiingo_data(self, bars: List[dict]) -> int:
        """Store synthetic Tiingo minute bars in database"""
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
        """Store synthetic Polygon minute bars in database"""
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
    
    async def generate_and_store_data(self):
        """Generate and store synthetic data for all symbols from both vendors"""
        total_stored = 0
        current_time = datetime.now().replace(second=0, microsecond=0)
        
        for symbol in self.symbols:
            try:
                # Generate Tiingo data
                tiingo_bars = [self.generate_minute_bar(symbol, current_time, 'tiingo')]
                tiingo_stored = await self.store_tiingo_data(tiingo_bars)
                
                # Generate Polygon data  
                polygon_bars = [self.generate_minute_bar(symbol, current_time, 'polygon')]
                polygon_stored = await self.store_polygon_data(polygon_bars)
                
                symbol_total = tiingo_stored + polygon_stored
                total_stored += symbol_total
                
                logger.info(f"✅ {symbol}: Generated {tiingo_stored} Tiingo + {polygon_stored} Polygon = {symbol_total} total bars")
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
        
        return total_stored
    
    async def start_collection(self):
        """Start continuous synthetic data generation"""
        logger.info("🚀 Starting AAPL/TSLA synthetic real-time data collection...")
        logger.info("📈 Generating realistic minute bars every 60 seconds...")
        self.running = True
        
        try:
            while self.running:
                start_time = datetime.now()
                
                # Generate and store data
                total_stored = await self.generate_and_store_data()
                
                collection_time = (datetime.now() - start_time).total_seconds()
                logger.info(f"📊 Collection cycle completed: {total_stored} synthetic bars stored in {collection_time:.1f}s")
                
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
    
    async def run_test_collection(self, cycles: int = 5):
        """Run a test collection for specified number of cycles"""
        logger.info(f"🧪 Running test collection for {cycles} cycles...")
        
        for i in range(cycles):
            logger.info(f"📊 Test cycle {i+1}/{cycles}")
            total_stored = await self.generate_and_store_data()
            logger.info(f"✅ Cycle {i+1}: Stored {total_stored} bars")
            
            if i < cycles - 1:  # Don't sleep on last cycle
                await asyncio.sleep(5)  # 5 second intervals for testing
    
    async def shutdown(self):
        """Cleanup resources"""
        self.running = False
        
        if self.pool:
            await self.pool.close()
        
        logger.info("✅ Collector shutdown complete")

async def main():
    """Main entry point"""
    import sys
    
    collector = AAPLTSLASyntheticCollector()
    
    try:
        await collector.initialize()
        
        # Run test mode if --test argument provided
        if len(sys.argv) > 1 and sys.argv[1] == '--test':
            await collector.run_test_collection(cycles=5)
        else:
            await collector.start_collection()
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        await collector.shutdown()

if __name__ == "__main__":
    asyncio.run(main())