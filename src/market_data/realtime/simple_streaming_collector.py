#!/usr/bin/env python3
"""
Simple Real-time Market Data Streaming Collector
Simplified version for initial deployment testing
"""

import asyncio
import logging
import os
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from dataclasses import dataclass
import aiohttp
from aiohttp import web
import asyncpg

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class MinuteBar:
    """Represents a minute bar of market data"""
    symbol: str
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    vendor: str = 'unknown'

class SimpleStreamingCollector:
    """Simplified real-time data collector for deployment testing"""
    
    def __init__(self):
        self.db_pool: Optional[asyncpg.Pool] = None
        self.is_running = False
        self.metrics = {
            'total_messages': 0,
            'successful_inserts': 0,
            'failed_inserts': 0,
            'last_update': None
        }
        
        # Environment configuration
        self.db_host = os.getenv('DB_HOST', 'postgres-simple')
        self.db_port = int(os.getenv('DB_PORT', '5432'))
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', 'dev_password')
        self.db_name = os.getenv('DB_NAME', 'dev_db')
        
        self.polygon_api_key = os.getenv('POLYGON_API_KEY', '')
        self.tiingo_api_key = os.getenv('TIINGO_API_KEY', '')
        
        logger.info(f"Initialized collector - DB: {self.db_host}:{self.db_port}/{self.db_name}")
    
    async def initialize_db(self) -> bool:
        """Initialize database connection pool"""
        try:
            dsn = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
            self.db_pool = await asyncpg.create_pool(
                dsn,
                min_size=2,
                max_size=10,
                command_timeout=30
            )
            
            # Test connection
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchval("SELECT version()")
                logger.info(f"Database connected: {result}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            return False
    
    async def create_test_tables(self):
        """Create test tables if they don't exist"""
        try:
            async with self.db_pool.acquire() as conn:
                # Create simple test table for minute bars
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS dev_realtime_test_bars (
                        id BIGSERIAL PRIMARY KEY,
                        symbol VARCHAR(10) NOT NULL,
                        timestamp TIMESTAMPTZ NOT NULL,
                        open_price DECIMAL(20,6),
                        high_price DECIMAL(20,6),
                        low_price DECIMAL(20,6),
                        close_price DECIMAL(20,6),
                        volume BIGINT,
                        vendor VARCHAR(20) DEFAULT 'test',
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                
                # Create index
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_realtime_test_symbol_timestamp 
                    ON dev_realtime_test_bars(symbol, timestamp)
                """)
                
                logger.info("Test tables created/verified")
                
        except Exception as e:
            logger.error(f"Failed to create test tables: {e}")
    
    async def simulate_data_collection(self):
        """Simulate real-time data collection"""
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        
        while self.is_running:
            try:
                # Simulate collecting data for each symbol
                current_time = datetime.now(timezone.utc)
                
                async with self.db_pool.acquire() as conn:
                    for symbol in symbols:
                        # Generate fake OHLCV data
                        base_price = 150.0 + hash(symbol) % 300
                        open_price = base_price + (hash(str(current_time)) % 100) / 100
                        high_price = open_price + abs(hash(symbol + str(current_time)) % 500) / 1000
                        low_price = open_price - abs(hash(str(current_time) + symbol) % 500) / 1000
                        close_price = (high_price + low_price) / 2 + (hash(symbol) % 200 - 100) / 1000
                        volume = abs(hash(symbol + str(current_time)) % 1000000) + 10000
                        
                        # Insert test data
                        await conn.execute("""
                            INSERT INTO dev_realtime_test_bars 
                            (symbol, timestamp, open_price, high_price, low_price, close_price, volume)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """, symbol, current_time, open_price, high_price, low_price, close_price, volume)
                        
                        self.metrics['successful_inserts'] += 1
                        
                    self.metrics['total_messages'] += len(symbols)
                    self.metrics['last_update'] = current_time.isoformat()
                    
                    logger.info(f"Processed {len(symbols)} symbols - Total: {self.metrics['total_messages']}")
                
                # Wait 60 seconds between collections (simulate minute bars)
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in data collection: {e}")
                self.metrics['failed_inserts'] += 1
                await asyncio.sleep(10)  # Wait before retry
    
    async def health_handler(self, request):
        """Health check endpoint"""
        health_status = {
            'status': 'healthy' if self.is_running else 'unhealthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'database': 'connected' if self.db_pool else 'disconnected',
            'uptime_seconds': (datetime.now(timezone.utc) - self.start_time).total_seconds() if hasattr(self, 'start_time') else 0
        }
        return web.json_response(health_status)
    
    async def ready_handler(self, request):
        """Readiness check endpoint"""
        ready_status = {
            'ready': self.is_running and self.db_pool is not None,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        return web.json_response(ready_status)
    
    async def metrics_handler(self, request):
        """Metrics endpoint"""
        return web.json_response(self.metrics)
    
    async def status_handler(self, request):
        """Status endpoint with detailed information"""
        status = {
            'collector': {
                'status': 'running' if self.is_running else 'stopped',
                'start_time': self.start_time.isoformat() if hasattr(self, 'start_time') else None,
            },
            'database': {
                'connected': self.db_pool is not None,
                'host': self.db_host,
                'port': self.db_port,
                'database': self.db_name
            },
            'metrics': self.metrics,
            'configuration': {
                'polygon_configured': bool(self.polygon_api_key),
                'tiingo_configured': bool(self.tiingo_api_key),
            }
        }
        return web.json_response(status, indent=2)
    
    async def start_http_server(self):
        """Start HTTP server for health checks and metrics"""
        app = web.Application()
        
        # Add routes
        app.router.add_get('/health', self.health_handler)
        app.router.add_get('/ready', self.ready_handler)
        app.router.add_get('/metrics', self.metrics_handler)
        app.router.add_get('/status', self.status_handler)
        
        # Start server
        runner = web.AppRunner(app)
        await runner.setup()
        
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()
        
        logger.info("HTTP server started on port 8080")
    
    async def start(self):
        """Start the collector"""
        self.start_time = datetime.now(timezone.utc)
        logger.info("Starting Simple Streaming Collector...")
        
        # Initialize database
        if not await self.initialize_db():
            logger.error("Failed to initialize database, exiting")
            return False
        
        # Create test tables
        await self.create_test_tables()
        
        # Start HTTP server
        await self.start_http_server()
        
        # Start data collection
        self.is_running = True
        logger.info("Simple Streaming Collector started successfully")
        
        # Run data collection in background
        collection_task = asyncio.create_task(self.simulate_data_collection())
        
        try:
            # Keep running
            await collection_task
        except asyncio.CancelledError:
            logger.info("Collection task cancelled")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.is_running = False
            if self.db_pool:
                await self.db_pool.close()
    
    async def stop(self):
        """Stop the collector"""
        logger.info("Stopping Simple Streaming Collector...")
        self.is_running = False
        if self.db_pool:
            await self.db_pool.close()

async def main():
    """Main entry point"""
    collector = SimpleStreamingCollector()
    
    try:
        await collector.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        await collector.stop()

# Alias for test compatibility
RealtimeStreamingCollector = SimpleStreamingCollector

if __name__ == "__main__":
    asyncio.run(main())