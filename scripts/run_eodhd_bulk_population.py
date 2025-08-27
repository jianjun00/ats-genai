#!/usr/bin/env python3
"""
EODHD Bulk Instrument Population Script

CRITICAL: Addresses the massive EODHD instrument population gap where only 7,613 
out of 50,747 available US instruments were populated (85% missing).

This script performs comprehensive bulk population of all EODHD instruments
with proper error handling, rate limiting, and progress tracking.

Usage:
    python3 scripts/run_eodhd_bulk_population.py
    python3 scripts/run_eodhd_bulk_population.py --exchange US --batch-size 1000
    python3 scripts/run_eodhd_bulk_population.py --resume-from AAPL
"""

import os
import sys
import json
import time
import asyncio
import asyncpg
import requests
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config.database import Database
from config.environment import Environment, EnvironmentType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class EODHDConfig:
    """Configuration for EODHD API access"""
    api_key: str
    base_url: str = "https://eodhd.com/api"
    rate_limit_delay: float = 0.1  # 100ms between requests (10 req/sec)
    batch_size: int = 1000
    max_retries: int = 3
    timeout: int = 30

class EODHDBulkPopulator:
    """Bulk population of EODHD instruments with comprehensive error handling"""
    
    def __init__(self, config: EODHDConfig, env: Environment):
        self.config = config
        self.env = env
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ATS-Platform/1.0 (contact@akolo.ca)'
        })
        
        # Progress tracking
        self.total_processed = 0
        self.total_inserted = 0
        self.total_updated = 0
        self.total_errors = 0
        self.start_time = datetime.now()
        
    async def get_database_pool(self) -> asyncpg.Pool:
        """Create database connection pool"""
        try:
            logger.info("Creating database connection pool...")
            pool = await Database.create_connection_pool(
                env=self.env, 
                max_retries=3, 
                initial_delay=1.0, 
                timeout=10.0
            )
            logger.info("✅ Database connection established")
            return pool
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            raise
    
    def get_exchange_instruments(self, exchange: str = "US") -> List[Dict]:
        """Fetch all instruments for a given exchange"""
        url = f"{self.config.base_url}/exchange-symbol-list/{exchange}"
        params = {
            'api_token': self.config.api_key,
            'fmt': 'json'
        }
        
        logger.info(f"🔍 Fetching instrument list for exchange: {exchange}")
        
        for attempt in range(self.config.max_retries):
            try:
                response = self.session.get(
                    url, 
                    params=params, 
                    timeout=self.config.timeout
                )
                response.raise_for_status()
                
                instruments = response.json()
                logger.info(f"✅ Retrieved {len(instruments)} instruments from {exchange}")
                return instruments
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"⚠️ Attempt {attempt + 1} failed: {e}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"❌ Failed to fetch instruments after {self.config.max_retries} attempts")
                    raise
    
    def parse_ipo_date(self, date_str: Optional[str]) -> Optional[datetime.date]:
        """Parse IPO date with multiple format support"""
        if not date_str or date_str == 'null':
            return None
            
        # Common date formats from EODHD
        formats = [
            "%Y-%m-%d",
            "%Y/%m/%d", 
            "%m/%d/%Y",
            "%d/%m/%Y",
            "%Y-%m-%d %H:%M:%S"
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str[:10], fmt[:10]).date()
            except (ValueError, TypeError):
                continue
        
        logger.debug(f"Could not parse date: {date_str}")
        return None
    
    async def create_table_if_not_exists(self, pool: asyncpg.Pool):
        """Ensure the EODHD instruments table exists with proper schema"""
        table_name = self.env.get_table_name('instrument_eodhd')
        
        async with pool.acquire() as conn:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT UNIQUE NOT NULL,
                    name TEXT,
                    exchange TEXT,
                    asset_type TEXT,
                    currency TEXT,
                    ipo_date DATE,
                    raw JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Create indexes for performance
                CREATE INDEX IF NOT EXISTS idx_{table_name}_exchange ON {table_name}(exchange);
                CREATE INDEX IF NOT EXISTS idx_{table_name}_asset_type ON {table_name}(asset_type);
                CREATE INDEX IF NOT EXISTS idx_{table_name}_ipo_date ON {table_name}(ipo_date);
                CREATE INDEX IF NOT EXISTS idx_{table_name}_updated_at ON {table_name}(updated_at);
            """)
        
        logger.info(f"✅ Ensured table {table_name} exists with proper indexes")
    
    async def upsert_instrument(self, pool: asyncpg.Pool, instrument: Dict) -> Tuple[bool, str]:
        """
        Insert or update a single instrument
        
        Returns:
            Tuple[bool, str]: (success, action) where action is 'inserted', 'updated', or 'error'
        """
        table_name = self.env.get_table_name('instrument_eodhd')
        
        try:
            async with pool.acquire() as conn:
                # Check if instrument already exists
                existing = await conn.fetchrow(
                    f"SELECT id, updated_at FROM {table_name} WHERE symbol = $1",
                    instrument.get('Code')
                )
                
                # Prepare data
                symbol = instrument.get('Code')
                name = instrument.get('Name')
                exchange = instrument.get('Exchange') 
                asset_type = instrument.get('Type')
                currency = instrument.get('Currency')
                ipo_date = self.parse_ipo_date(instrument.get('IPODate'))
                raw_data = json.dumps(instrument)
                
                if existing:
                    # Update existing record
                    await conn.execute(f"""
                        UPDATE {table_name} 
                        SET 
                            name = $2,
                            exchange = $3,
                            asset_type = $4,
                            currency = $5,
                            ipo_date = $6,
                            raw = $7,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE symbol = $1
                    """, symbol, name, exchange, asset_type, currency, ipo_date, raw_data)
                    
                    self.total_updated += 1
                    return True, 'updated'
                else:
                    # Insert new record
                    await conn.execute(f"""
                        INSERT INTO {table_name} 
                        (symbol, name, exchange, asset_type, currency, ipo_date, raw)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """, symbol, name, exchange, asset_type, currency, ipo_date, raw_data)
                    
                    self.total_inserted += 1
                    return True, 'inserted'
                    
        except Exception as e:
            logger.error(f"❌ Error upserting {instrument.get('Code', 'UNKNOWN')}: {e}")
            self.total_errors += 1
            return False, 'error'
    
    async def process_batch(self, pool: asyncpg.Pool, instruments: List[Dict], batch_num: int, total_batches: int):
        """Process a batch of instruments"""
        batch_start = time.time()
        batch_inserted = 0
        batch_updated = 0
        batch_errors = 0
        
        logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(instruments)} instruments)")
        
        for i, instrument in enumerate(instruments):
            symbol = instrument.get('Code', f'UNKNOWN_{i}')
            
            try:
                success, action = await self.upsert_instrument(pool, instrument)
                
                if success:
                    if action == 'inserted':
                        batch_inserted += 1
                    elif action == 'updated':
                        batch_updated += 1
                    
                    if (i + 1) % 100 == 0:
                        logger.info(f"  📈 Progress: {i + 1}/{len(instruments)} instruments processed")
                else:
                    batch_errors += 1
                
                # Rate limiting
                await asyncio.sleep(self.config.rate_limit_delay)
                
            except Exception as e:
                logger.error(f"❌ Unexpected error processing {symbol}: {e}")
                batch_errors += 1
        
        batch_duration = time.time() - batch_start
        
        self.total_processed += len(instruments)
        
        logger.info(f"✅ Batch {batch_num} completed in {batch_duration:.2f}s:")
        logger.info(f"  📊 Inserted: {batch_inserted}, Updated: {batch_updated}, Errors: {batch_errors}")
        
        # Brief pause between batches
        await asyncio.sleep(1.0)
    
    def print_progress_summary(self):
        """Print comprehensive progress summary"""
        elapsed = datetime.now() - self.start_time
        rate = self.total_processed / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
        
        logger.info("=" * 60)
        logger.info("📊 EODHD BULK POPULATION PROGRESS SUMMARY")
        logger.info("=" * 60)
        logger.info(f"⏱️  Elapsed Time: {elapsed}")
        logger.info(f"📈 Total Processed: {self.total_processed:,}")
        logger.info(f"➕ Total Inserted: {self.total_inserted:,}")
        logger.info(f"🔄 Total Updated: {self.total_updated:,}")
        logger.info(f"❌ Total Errors: {self.total_errors:,}")
        logger.info(f"🚀 Processing Rate: {rate:.2f} instruments/sec")
        logger.info("=" * 60)
    
    async def run_bulk_population(self, exchange: str = "US", resume_from: Optional[str] = None):
        """Run the complete bulk population process"""
        logger.info("🚀 Starting EODHD Bulk Instrument Population")
        logger.info(f"📊 Target: Fix critical gap - populate all {exchange} instruments")
        
        try:
            # Get database connection
            pool = await self.get_database_pool()
            
            # Ensure table exists
            await self.create_table_if_not_exists(pool)
            
            # Get all instruments from EODHD API
            all_instruments = self.get_exchange_instruments(exchange)
            
            # Filter if resuming from specific symbol
            if resume_from:
                all_instruments = [inst for inst in all_instruments 
                                 if inst.get('Code', '').upper() >= resume_from.upper()]
                logger.info(f"🔄 Resuming from symbol: {resume_from} ({len(all_instruments)} remaining)")
            
            # Sort by symbol for consistent processing
            all_instruments.sort(key=lambda x: x.get('Code', ''))
            
            logger.info(f"📊 Total instruments to process: {len(all_instruments):,}")
            
            # Process in batches
            total_batches = (len(all_instruments) + self.config.batch_size - 1) // self.config.batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * self.config.batch_size
                end_idx = min(start_idx + self.config.batch_size, len(all_instruments))
                batch_instruments = all_instruments[start_idx:end_idx]
                
                await self.process_batch(pool, batch_instruments, batch_num + 1, total_batches)
                
                # Print progress every 10 batches
                if (batch_num + 1) % 10 == 0:
                    self.print_progress_summary()
            
            # Final summary
            await pool.close()
            self.print_progress_summary()
            
            # Verify results
            await self.verify_population_results(exchange)
            
            logger.info("🎉 EODHD Bulk Population Completed Successfully!")
            
        except Exception as e:
            logger.error(f"💥 Critical error in bulk population: {e}")
            raise
    
    async def verify_population_results(self, exchange: str = "US"):
        """Verify the population results against original data"""
        logger.info("🔍 Verifying population results...")
        
        try:
            # Get fresh database connection for verification
            pool = await self.get_database_pool()
            table_name = self.env.get_table_name('instrument_eodhd')
            
            async with pool.acquire() as conn:
                # Count total instruments in database
                db_count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                
                # Count by exchange
                exchange_counts = await conn.fetch(f"""
                    SELECT exchange, COUNT(*) as count
                    FROM {table_name}
                    GROUP BY exchange
                    ORDER BY count DESC
                """)
                
                # Recent additions
                recent_additions = await conn.fetchval(f"""
                    SELECT COUNT(*) FROM {table_name}
                    WHERE created_at >= NOW() - INTERVAL '1 hour'
                """)
                
                logger.info("📊 VERIFICATION RESULTS:")
                logger.info(f"📈 Total Instruments in DB: {db_count:,}")
                logger.info(f"🆕 Added in Last Hour: {recent_additions:,}")
                
                logger.info("📊 Instruments by Exchange:")
                for row in exchange_counts[:10]:  # Top 10 exchanges
                    logger.info(f"  {row['exchange']}: {row['count']:,}")
            
            await pool.close()
            
            # Compare with API data
            api_instruments = self.get_exchange_instruments(exchange)
            api_count = len(api_instruments)
            
            coverage = (db_count / api_count * 100) if api_count > 0 else 0
            
            logger.info(f"📊 COVERAGE ANALYSIS:")
            logger.info(f"🌐 Available from EODHD API: {api_count:,}")
            logger.info(f"💾 Stored in Database: {db_count:,}")
            logger.info(f"📈 Coverage Percentage: {coverage:.1f}%")
            
            if coverage >= 95:
                logger.info("🎉 Excellent coverage achieved!")
            elif coverage >= 85:
                logger.info("✅ Good coverage achieved!")
            else:
                logger.warning(f"⚠️ Coverage below 85% - may need investigation")
            
        except Exception as e:
            logger.error(f"❌ Error during verification: {e}")

def main():
    """Main entry point"""
    # Check if running with arguments or environment variables
    if len(sys.argv) > 1:
        # Command line argument mode
        import argparse
        
        parser = argparse.ArgumentParser(
            description="EODHD Bulk Instrument Population - Fix Critical Coverage Gap"
        )
        parser.add_argument(
            '--exchange', 
            type=str, 
            default='US',
            help='Exchange to populate (default: US)'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Batch size for processing (default: 1000)'
        )
        parser.add_argument(
            '--resume-from',
            type=str,
            help='Resume from specific symbol (useful for restarts)'
        )
        parser.add_argument(
            '--rate-limit-delay',
            type=float,
            default=0.1,
            help='Delay between API requests in seconds (default: 0.1)'
        )
        parser.add_argument(
            '--environment',
            type=str,
            default='dev',
            choices=['dev', 'intg', 'prod'],
            help='Environment to use (default: dev)'
        )
        
        args = parser.parse_args()
        exchange = args.exchange
        batch_size = args.batch_size
        resume_from = args.resume_from
        rate_limit_delay = args.rate_limit_delay
        environment = args.environment
    else:
        # Environment variable mode (for Docker/run_dev.py)
        exchange = os.getenv('EODHD_EXCHANGE', 'US')
        batch_size = int(os.getenv('EODHD_BATCH_SIZE', '1000'))
        resume_from = os.getenv('EODHD_RESUME_FROM')
        rate_limit_delay = float(os.getenv('EODHD_RATE_LIMIT_DELAY', '0.1'))
        environment = os.getenv('EODHD_ENVIRONMENT', 'dev')
    
    # Get API key
    api_key = os.getenv('EODHD_API_KEY')
    if not api_key:
        logger.error("❌ EODHD_API_KEY environment variable not set")
        logger.error("   Please set it in your .env file or environment")
        sys.exit(1)
    
    # Configure environment
    env_type_mapping = {
        'test': EnvironmentType.TEST,
        'dev': EnvironmentType.DEV,
        'intg': EnvironmentType.INTEGRATION,
        'integration': EnvironmentType.INTEGRATION,
        'prod': EnvironmentType.PRODUCTION,
        'production': EnvironmentType.PRODUCTION
    }
    env_type = env_type_mapping.get(environment.lower(), EnvironmentType.DEV)
    env = Environment(env_type)
    
    # Create configuration
    config = EODHDConfig(
        api_key=api_key,
        batch_size=batch_size,
        rate_limit_delay=rate_limit_delay
    )
    
    # Create and run populator
    populator = EODHDBulkPopulator(config, env)
    
    logger.info("🚀 EODHD Bulk Population Starting...")
    logger.info(f"📊 Configuration:")
    logger.info(f"  Exchange: {exchange}")
    logger.info(f"  Batch Size: {batch_size}")
    logger.info(f"  Rate Limit: {rate_limit_delay}s between requests")
    logger.info(f"  Environment: {environment}")
    if resume_from:
        logger.info(f"  Resume From: {resume_from}")
    
    try:
        asyncio.run(populator.run_bulk_population(exchange, resume_from))
    except KeyboardInterrupt:
        logger.info("🛑 Population interrupted by user")
        populator.print_progress_summary()
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Population failed: {e}")
        populator.print_progress_summary()
        sys.exit(1)

if __name__ == "__main__":
    main()