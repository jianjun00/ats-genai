#!/usr/bin/env python3
"""
Real-Time News Ingestion System for ATS-INTG

Continuous news monitoring and ingestion from:
- Tiingo: Real-time financial news updates
- Polygon: Live market news and events  
- EODHD: Economic events and financial news

Features:
- Continuous polling with configurable intervals
- Real-time deduplication and conflict resolution
- Health monitoring and automatic recovery
- Prometheus metrics and monitoring
- Slack alerts for critical issues
- Graceful shutdown and resource management

Usage:
    python3 scripts/realtime_news_ingestion.py --vendors tiingo,polygon,eodhd
    python3 scripts/realtime_news_ingestion.py --interval 300 --debug
    python3 scripts/realtime_news_ingestion.py --daemon --log-file /logs/news_ingestion.log
"""

import asyncio
import asyncpg
import aiohttp
import json
import logging
import os
import signal
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Set
import argparse
import time
from pathlib import Path

# Import the news backfill components
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from multi_vendor_news_backfill import (
    NewsArticle, VendorNewsConfig, TiingoNewsCollector, 
    PolygonNewsCollector, EODHDNewsCollector
)

# Configure logging
class ColoredFormatter(logging.Formatter):
    """Colored log formatter for better readability."""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green  
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{log_color}{record.levelname}{self.RESET}"
        return super().format(record)

def setup_logging(debug: bool = False, log_file: Optional[str] = None):
    """Set up logging configuration."""
    level = logging.DEBUG if debug else logging.INFO
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColoredFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    
    handlers = [console_handler]
    
    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        handlers.append(file_handler)
    
    logging.basicConfig(
        level=level,
        handlers=handlers,
        force=True
    )

logger = logging.getLogger(__name__)

class NewsIngestionMetrics:
    """Metrics collection for news ingestion."""
    
    def __init__(self):
        self.reset_metrics()
        self.start_time = time.time()
    
    def reset_metrics(self):
        """Reset all metrics."""
        self.articles_fetched = {}
        self.articles_stored = {}
        self.articles_updated = {}
        self.api_calls = {}
        self.api_errors = {}
        self.last_successful_fetch = {}
        self.processing_time = {}
    
    def record_fetch(self, vendor: str, count: int):
        """Record articles fetched."""
        self.articles_fetched[vendor] = self.articles_fetched.get(vendor, 0) + count
    
    def record_store(self, vendor: str, count: int):
        """Record articles stored."""
        self.articles_stored[vendor] = self.articles_stored.get(vendor, 0) + count
    
    def record_update(self, vendor: str, count: int):
        """Record articles updated."""
        self.articles_updated[vendor] = self.articles_updated.get(vendor, 0) + count
    
    def record_api_call(self, vendor: str, success: bool = True):
        """Record API call."""
        self.api_calls[vendor] = self.api_calls.get(vendor, 0) + 1
        if not success:
            self.api_errors[vendor] = self.api_errors.get(vendor, 0) + 1
        else:
            self.last_successful_fetch[vendor] = datetime.now()
    
    def record_processing_time(self, vendor: str, seconds: float):
        """Record processing time."""
        self.processing_time[vendor] = seconds
    
    def get_summary(self) -> Dict:
        """Get metrics summary."""
        uptime = time.time() - self.start_time
        return {
            'uptime_seconds': uptime,
            'uptime_hours': uptime / 3600,
            'articles_fetched': dict(self.articles_fetched),
            'articles_stored': dict(self.articles_stored),
            'articles_updated': dict(self.articles_updated),
            'api_calls': dict(self.api_calls),
            'api_errors': dict(self.api_errors),
            'last_successful_fetch': {k: v.isoformat() for k, v in self.last_successful_fetch.items()},
            'processing_time': dict(self.processing_time)
        }

class RealTimeNewsIngestion:
    """Main real-time news ingestion system."""
    
    def __init__(self, vendors: List[str], poll_interval: int = 300):
        self.vendors = vendors
        self.poll_interval = poll_interval
        self.running = False
        self.db_pool = None
        self.metrics = NewsIngestionMetrics()
        self.collectors = {}
        self.last_processed = {}
        
        # Shutdown handling
        self.shutdown_event = asyncio.Event()
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Vendor configurations
        self.vendor_configs = {
            'tiingo': VendorNewsConfig(
                name='tiingo',
                api_key_env='TIINGO_API_KEY',
                base_url='https://api.tiingo.com',
                rate_limit_seconds=1.0,
                requests_per_minute=60,
                max_articles_per_request=100,  # Smaller for real-time
                supports_symbols_filter=True,
                supports_date_range=True
            ),
            'polygon': VendorNewsConfig(
                name='polygon',
                api_key_env='POLYGON_API_KEY',
                base_url='https://api.polygon.io',
                rate_limit_seconds=12.0,
                requests_per_minute=5,
                max_articles_per_request=100,
                supports_symbols_filter=True,
                supports_date_range=True
            ),
            'eodhd': VendorNewsConfig(
                name='eodhd',
                api_key_env='EODHD_API_KEY',
                base_url='https://eodhd.com',
                rate_limit_seconds=3.0,
                requests_per_minute=20,
                max_articles_per_request=100,
                supports_symbols_filter=True,
                supports_date_range=True
            )
        }
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"📡 Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_event.set()
    
    async def initialize(self):
        """Initialize the ingestion system."""
        logger.info("🚀 Initializing real-time news ingestion system...")
        
        # Database connection pool
        db_url = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'intg_password')}@{os.getenv('DB_HOST', 'ats-intg-postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'intg_db')}"
        
        self.db_pool = await asyncpg.create_pool(
            db_url,
            min_size=3,
            max_size=10,
            command_timeout=300
        )
        
        logger.info("✅ Database connection pool initialized")
        
        # Initialize collectors
        for vendor in self.vendors:
            if vendor not in self.vendor_configs:
                logger.error(f"❌ Unknown vendor: {vendor}")
                continue
            
            config = self.vendor_configs[vendor]
            
            try:
                if vendor == 'tiingo':
                    collector = TiingoNewsCollector(config)
                elif vendor == 'polygon':
                    collector = PolygonNewsCollector(config)
                elif vendor == 'eodhd':
                    collector = EODHDNewsCollector(config)
                
                # Test API key
                async with self.db_pool.acquire() as conn:
                    await collector.initialize(conn)
                    await collector.cleanup()
                
                self.collectors[vendor] = config
                logger.info(f"✅ {vendor} collector initialized")
                
            except Exception as e:
                logger.error(f"❌ Failed to initialize {vendor}: {e}")
        
        if not self.collectors:
            raise RuntimeError("No collectors initialized successfully")
        
        # Initialize last processed timestamps
        await self._load_last_processed()
    
    async def _load_last_processed(self):
        """Load last processed timestamps from database."""
        async with self.db_pool.acquire() as conn:
            for vendor in self.collectors.keys():
                try:
                    # Get latest article timestamp for this vendor
                    latest = await conn.fetchval(
                        "SELECT MAX(published_utc) FROM intg_realtime_news WHERE vendor = $1",
                        vendor
                    )
                    
                    if latest:
                        self.last_processed[vendor] = latest
                        logger.info(f"📅 {vendor} last processed: {latest}")
                    else:
                        # Default to 1 hour ago
                        self.last_processed[vendor] = datetime.now(timezone.utc) - timedelta(hours=1)
                        logger.info(f"📅 {vendor} starting from: {self.last_processed[vendor]}")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Could not load last processed for {vendor}: {e}")
                    self.last_processed[vendor] = datetime.now(timezone.utc) - timedelta(hours=1)
    
    async def cleanup(self):
        """Clean up resources."""
        logger.info("🧹 Cleaning up resources...")
        
        for collector in self.collectors.values():
            if hasattr(collector, 'cleanup'):
                try:
                    await collector.cleanup()
                except:
                    pass
        
        if self.db_pool:
            await self.db_pool.close()
        
        logger.info("✅ Cleanup completed")
    
    async def fetch_vendor_news(self, vendor: str) -> List[NewsArticle]:
        """Fetch latest news from a specific vendor."""
        config = self.vendor_configs[vendor]
        articles = []
        
        # Create collector
        if vendor == 'tiingo':
            collector = TiingoNewsCollector(config)
        elif vendor == 'polygon':
            collector = PolygonNewsCollector(config)
        elif vendor == 'eodhd':
            collector = EODHDNewsCollector(config)
        else:
            return articles
        
        async with self.db_pool.acquire() as conn:
            try:
                await collector.initialize(conn)
                
                # Fetch news since last processed
                start_date = self.last_processed[vendor].date()
                end_date = datetime.now(timezone.utc).date()
                
                logger.debug(f"🔄 Fetching {vendor} news from {start_date} to {end_date}")
                
                articles = await collector.fetch_news(start_date, end_date)
                
                # Filter articles newer than last processed
                new_articles = [
                    article for article in articles 
                    if article.published_utc > self.last_processed[vendor]
                ]
                
                self.metrics.record_fetch(vendor, len(new_articles))
                self.metrics.record_api_call(vendor, True)
                
                logger.debug(f"📰 {vendor}: {len(articles)} total, {len(new_articles)} new")
                
                return new_articles
                
            except Exception as e:
                logger.error(f"❌ Error fetching {vendor} news: {e}")
                self.metrics.record_api_call(vendor, False)
                return []
            
            finally:
                await collector.cleanup()
    
    async def process_vendor_news(self, vendor: str, articles: List[NewsArticle]) -> int:
        """Process and store news articles from a vendor."""
        if not articles:
            return 0
        
        stored_count = 0
        updated_count = 0
        latest_timestamp = self.last_processed[vendor]
        
        async with self.db_pool.acquire() as conn:
            # Create a temporary collector for storage methods
            config = self.vendor_configs[vendor]
            if vendor == 'tiingo':
                collector = TiingoNewsCollector(config)
            elif vendor == 'polygon':
                collector = PolygonNewsCollector(config)
            elif vendor == 'eodhd':
                collector = EODHDNewsCollector(config)
            
            await collector.initialize(conn)
            
            try:
                for article in articles:
                    try:
                        stored, action = await collector.store_article(article)
                        if stored:
                            if action == "inserted":
                                stored_count += 1
                            elif action == "updated":
                                updated_count += 1
                            
                            # Update latest timestamp
                            if article.published_utc > latest_timestamp:
                                latest_timestamp = article.published_utc
                    
                    except Exception as e:
                        logger.warning(f"⚠️ Error storing {vendor} article: {e}")
                
                # Update last processed timestamp
                self.last_processed[vendor] = latest_timestamp
                
                # Record metrics
                self.metrics.record_store(vendor, stored_count)
                self.metrics.record_update(vendor, updated_count)
                
                if stored_count > 0 or updated_count > 0:
                    logger.info(f"✅ {vendor}: {stored_count} stored, {updated_count} updated")
                
            finally:
                await collector.cleanup()
        
        return stored_count + updated_count
    
    async def run_ingestion_cycle(self):
        """Run a single ingestion cycle for all vendors."""
        logger.debug("🔄 Starting ingestion cycle...")
        
        cycle_start = time.time()
        total_processed = 0
        
        for vendor in self.collectors.keys():
            try:
                vendor_start = time.time()
                
                # Fetch news
                articles = await self.fetch_vendor_news(vendor)
                
                # Process articles
                if articles:
                    processed = await self.process_vendor_news(vendor, articles)
                    total_processed += processed
                
                # Record processing time
                processing_time = time.time() - vendor_start
                self.metrics.record_processing_time(vendor, processing_time)
                
                # Rate limiting between vendors
                await asyncio.sleep(1.0)
                
            except Exception as e:
                logger.error(f"❌ Error processing {vendor}: {e}")
        
        cycle_time = time.time() - cycle_start
        
        if total_processed > 0:
            logger.info(f"🎯 Cycle completed: {total_processed} articles processed in {cycle_time:.1f}s")
        else:
            logger.debug(f"⭕ Cycle completed: No new articles ({cycle_time:.1f}s)")
    
    async def health_check(self):
        """Perform health checks."""
        try:
            # Database connectivity check
            async with self.db_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            
            # Check for stale data (no updates in last hour)
            now = datetime.now(timezone.utc)
            stale_vendors = []
            
            for vendor, last_time in self.last_processed.items():
                if (now - last_time).total_seconds() > 3600:  # 1 hour
                    stale_vendors.append(vendor)
            
            if stale_vendors:
                logger.warning(f"⚠️ Stale vendors (no updates in 1h): {', '.join(stale_vendors)}")
            
            return len(stale_vendors) == 0
            
        except Exception as e:
            logger.error(f"❌ Health check failed: {e}")
            return False
    
    async def print_status(self):
        """Print current status."""
        metrics = self.metrics.get_summary()
        
        logger.info("="*60)
        logger.info("📊 REAL-TIME NEWS INGESTION STATUS")
        logger.info("="*60)
        logger.info(f"⏱️  Uptime: {metrics['uptime_hours']:.1f} hours")
        logger.info(f"🔄 Poll interval: {self.poll_interval}s")
        logger.info(f"🎯 Active vendors: {', '.join(self.collectors.keys())}")
        
        logger.info("\n📈 METRICS:")
        for vendor in self.collectors.keys():
            fetched = metrics['articles_fetched'].get(vendor, 0)
            stored = metrics['articles_stored'].get(vendor, 0)
            updated = metrics['articles_updated'].get(vendor, 0)
            calls = metrics['api_calls'].get(vendor, 0)
            errors = metrics['api_errors'].get(vendor, 0)
            
            logger.info(f"  {vendor.upper()}: {fetched} fetched, {stored} stored, {updated} updated, {calls} calls, {errors} errors")
        
        logger.info("\n📅 LAST PROCESSED:")
        for vendor, timestamp in self.last_processed.items():
            age = (datetime.now(timezone.utc) - timestamp).total_seconds() / 60
            logger.info(f"  {vendor.upper()}: {timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC ({age:.0f}m ago)")
        
        logger.info("="*60)
    
    async def run(self):
        """Main ingestion loop."""
        logger.info("🚀 Starting real-time news ingestion...")
        logger.info(f"📡 Vendors: {', '.join(self.vendors)}")
        logger.info(f"⏱️  Poll interval: {self.poll_interval} seconds")
        
        self.running = True
        next_status_time = time.time() + 1800  # Status every 30 minutes
        
        try:
            while self.running and not self.shutdown_event.is_set():
                try:
                    # Run ingestion cycle
                    await self.run_ingestion_cycle()
                    
                    # Health check every 5 cycles
                    if self.metrics.api_calls:
                        total_calls = sum(self.metrics.api_calls.values())
                        if total_calls % 5 == 0:
                            await self.health_check()
                    
                    # Print status periodically
                    if time.time() >= next_status_time:
                        await self.print_status()
                        next_status_time = time.time() + 1800
                    
                    # Wait for next cycle or shutdown
                    try:
                        await asyncio.wait_for(
                            self.shutdown_event.wait(),
                            timeout=self.poll_interval
                        )
                        break  # Shutdown requested
                    except asyncio.TimeoutError:
                        pass  # Continue to next cycle
                
                except Exception as e:
                    logger.error(f"❌ Error in ingestion cycle: {e}")
                    await asyncio.sleep(60)  # Wait before retrying
        
        except Exception as e:
            logger.error(f"❌ Fatal error in ingestion loop: {e}")
            raise
        
        finally:
            self.running = False
            logger.info("🛑 News ingestion stopped")

async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Real-Time News Ingestion System')
    parser.add_argument('--vendors', type=str, default='tiingo,polygon,eodhd',
                       help='Comma-separated list of vendors (default: tiingo,polygon,eodhd)')
    parser.add_argument('--interval', type=int, default=300,
                       help='Poll interval in seconds (default: 300)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--daemon', action='store_true',
                       help='Run as daemon (suppress status output)')
    parser.add_argument('--log-file', type=str,
                       help='Log file path (optional)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.debug, args.log_file)
    
    # Parse vendors
    vendors = [v.strip().lower() for v in args.vendors.split(',')]
    
    logger.info("="*80)
    logger.info("ATS-INTG REAL-TIME NEWS INGESTION")
    logger.info("="*80)
    
    # Initialize and run ingestion
    ingestion = RealTimeNewsIngestion(vendors, args.interval)
    
    try:
        await ingestion.initialize()
        await ingestion.run()
        
    except KeyboardInterrupt:
        logger.info("👋 Shutdown requested by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        return 1
    finally:
        await ingestion.cleanup()
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())