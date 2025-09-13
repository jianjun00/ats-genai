#!/usr/bin/env python3
"""
High-performance daily price backfill for massive parallel processing.
Target: 5000+ symbols with 5-year history in 1-2 days.
"""

import os
import asyncio
import argparse
import logging
from datetime import datetime, timedelta

# Configure Ray with optimizations
import ray
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@ray.remote
class PolygonWorker:
    """Ray actor for parallel Polygon API processing."""

    def __init__(self, api_key: str, env_config: dict):
        self.api_key = api_key
        self.env_config = env_config

    async def fetch_symbol_data(self, symbol: str, start_date: str, end_date: str, instrument_id: int) -> List[Dict[str, Any]]:
        """Fetch 5-year data for a single symbol with optimized batching."""
        import aiohttp
        from shared.utils.environment import Environment

        # Recreate environment in worker
        env = Environment()
        env.env_type = self.env_config['env_type']

        # Use larger date ranges (1 year per API call for efficiency)
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

        results = []
        current_date = start_dt

        async with aiohttp.ClientSession() as session:
            while current_date <= end_dt:
                # Process 1 year at a time for optimal API usage
                batch_end = min(current_date + timedelta(days=365), end_dt)

                url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{current_date}/{batch_end}"
                params = {
                    'adjusted': 'true',
                    'sort': 'asc',
                    'limit': 50000,  # Maximum limit
                    'apikey': self.api_key
                }

                try:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            if 'results' in data and data['results']:
                                for result in data['results']:
                                    results.append({
                                        'date': datetime.utcfromtimestamp(result['t']/1000).date(),
                                        'instrument_id': instrument_id,
                                        'open': result['o'],
                                        'high': result['h'],
                                        'low': result['l'],
                                        'close': result['c'],
                                        'volume': result['v']
                                    })
                        else:
                            logger.warning(f"API error for {symbol}: HTTP {response.status}")
                except Exception as e:
                    logger.error(f"Error fetching {symbol} {current_date}-{batch_end}: {e}")

                current_date = batch_end + timedelta(days=1)
                await asyncio.sleep(0.1)  # Minimal rate limiting for high throughput

        return results

@ray.remote
class TiingoWorker:
    """Ray actor for parallel Tiingo API processing."""

    def __init__(self, api_key: str, env_config: dict):
        self.api_key = api_key
        self.env_config = env_config

    async def fetch_symbol_data(self, symbol: str, start_date: str, end_date: str, instrument_id: int) -> List[Dict[str, Any]]:
        """Fetch 5-year data for a single symbol with optimized batching."""
        import aiohttp

        url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        params = {
            'startDate': start_date,
            'endDate': end_date,
            'format': 'json',
            'token': self.api_key
        }

        results = []

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        for item in data:
                            results.append({
                                'date': datetime.strptime(item['date'][:10], "%Y-%m-%d").date(),
                                'instrument_id': instrument_id,
                                'open': item.get('open'),
                                'high': item.get('high'),
                                'low': item.get('low'),
                                'close': item.get('close'),
                                'volume': item.get('volume')
                            })
                    else:
                        logger.warning(f"Tiingo API error for {symbol}: HTTP {response.status}")
        except Exception as e:
            logger.error(f"Error fetching Tiingo data for {symbol}: {e}")

        return results

@ray.remote
class DatabaseInserter:
    """Ray actor for high-performance batch database insertions."""

    def __init__(self, db_config: dict):
        self.db_config = db_config

    async def batch_insert_polygon(self, data: List[Dict[str, Any]]) -> int:
        """Batch insert Polygon data with optimized performance."""
        import asyncpg

        if not data:
            return 0

        # Create database connection with optimizations
        conn = await asyncpg.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            server_settings={'jit': 'off'}  # Disable JIT for batch inserts
        )

        try:
            # Use COPY for maximum insert performance
            await conn.copy_records_to_table(
                'dev_daily_price_polygon',
                records=[(
                    item['date'],
                    item['instrument_id'],
                    item['open'],
                    item['high'],
                    item['low'],
                    item['close'],
                    item['volume']
                ) for item in data],
                columns=['date', 'instrument_id', 'open', 'high', 'low', 'close', 'volume']
            )
            return len(data)
        except Exception as e:
            logger.error(f"Database insert error: {e}")
            return 0
        finally:
            await conn.close()

    async def batch_insert_tiingo(self, data: List[Dict[str, Any]]) -> int:
        """Batch insert Tiingo data with optimized performance."""
        import asyncpg

        if not data:
            return 0

        conn = await asyncpg.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            server_settings={'jit': 'off'}
        )

        try:
            await conn.copy_records_to_table(
                'dev_daily_price_tiingo',
                records=[(
                    item['date'],
                    item['instrument_id'],
                    item['open'],
                    item['high'],
                    item['low'],
                    item['close'],
                    item['volume']
                ) for item in data],
                columns=['date', 'instrument_id', 'open', 'high', 'low', 'close', 'volume']
            )
            return len(data)
        except Exception as e:
            logger.error(f"Database insert error: {e}")
            return 0
        finally:
            await conn.close()

async def main():
    parser = argparse.ArgumentParser(description="High-performance daily price backfill")
    parser.add_argument('--start_date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--environment', default='dev', choices=['test', 'intg', 'prod', 'dev'])
    parser.add_argument('--gin_config', default='config/app_dev.gin', help='Gin config file')
    parser.add_argument('--workers', type=int, default=100, help='Number of parallel workers')
    parser.add_argument('--batch_size', type=int, default=1000, help='Database batch insert size')
    args = parser.parse_args()

    # Initialize Ray with optimizations
    ray_config = {
        "excludes": [
            "db/snapshot/",
            ".git/objects/pack/",
            "*.sql",
            "*.pack",
            "__pycache__/"
        ]
    }

    if not ray.is_initialized():
        ray.init(
            runtime_env=ray_config,
            ignore_reinit_error=True,
            num_cpus=args.workers,
            object_store_memory=2000000000  # 2GB object store
        )

    # Setup environment
    from shared.utils.environment import Environment
    env = Environment(gin_config_path=args.gin_config)

    # Get API keys
    polygon_api_key = env.get_polygon_api_key() or os.getenv("POLYGON_API_KEY")
    tiingo_api_key = os.getenv("TIINGO_API_KEY")

    # Database config
    db_config = {
        'host': os.getenv("DB_HOST", "localhost"),
        'port': int(os.getenv("DB_PORT", "5433")),
        'user': os.getenv("DB_USER", "postgres"),
        'password': os.getenv("DB_PASSWORD", "postgres"),
        'database': os.getenv("DB_NAME", "dev_db")
    }

    # Get all symbols to process
    from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
    xrefs_dao = InstrumentXrefsDAO(env)

    # Get symbols with instrument_ids
    symbols_data = []
    all_symbols = await xrefs_dao.get_all_symbols()
    logger.info(f"Found {len(all_symbols)} symbols to process")

    for symbol in all_symbols:
        instrument_id = await xrefs_dao.resolve_instrument_id(symbol)
        if instrument_id:
            symbols_data.append((symbol, instrument_id))

    logger.info(f"Processing {len(symbols_data)} symbols with valid instrument_ids")

    # Create worker pools
    polygon_workers = [PolygonWorker.remote(polygon_api_key, {'env_type': env.env_type}) for _ in range(args.workers)]
    tiingo_workers = [TiingoWorker.remote(tiingo_api_key, {'env_type': env.env_type}) for _ in range(args.workers)]
    db_inserters = [DatabaseInserter.remote(db_config) for _ in range(10)]  # 10 DB workers

    # Process symbols in batches
    batch_size = args.workers
    polygon_futures = []
    tiingo_futures = []

    logger.info(f"Starting parallel processing with {args.workers} workers...")

    for i in range(0, len(symbols_data), batch_size):
        batch = symbols_data[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(symbols_data) + batch_size - 1)//batch_size}")

        # Submit Polygon tasks
        for j, (symbol, instrument_id) in enumerate(batch):
            worker = polygon_workers[j % len(polygon_workers)]
            future = worker.fetch_symbol_data.remote(symbol, args.start_date, args.end_date, instrument_id)
            polygon_futures.append(future)

        # Submit Tiingo tasks
        for j, (symbol, instrument_id) in enumerate(batch):
            worker = tiingo_workers[j % len(tiingo_workers)]
            future = worker.fetch_symbol_data.remote(symbol, args.start_date, args.end_date, instrument_id)
            tiingo_futures.append(future)

    logger.info("All tasks submitted. Processing results...")

    # Process results in batches for database insertion
    polygon_total = 0
    tiingo_total = 0

    # Process Polygon results
    for i in range(0, len(polygon_futures), args.batch_size):
        batch_futures = polygon_futures[i:i+args.batch_size]
        batch_results = ray.get(batch_futures)

        # Flatten and batch insert
        all_data = []
        for result in batch_results:
            all_data.extend(result)

        if all_data:
            inserter = db_inserters[i % len(db_inserters)]
            inserted = await inserter.batch_insert_polygon.remote(all_data)
            polygon_total += ray.get(inserted)
            logger.info(f"Polygon batch {i//args.batch_size + 1}: {len(all_data)} records inserted")

    # Process Tiingo results
    for i in range(0, len(tiingo_futures), args.batch_size):
        batch_futures = tiingo_futures[i:i+args.batch_size]
        batch_results = ray.get(batch_futures)

        all_data = []
        for result in batch_results:
            all_data.extend(result)

        if all_data:
            inserter = db_inserters[i % len(db_inserters)]
            inserted = await inserter.batch_insert_tiingo.remote(all_data)
            tiingo_total += ray.get(inserted)
            logger.info(f"Tiingo batch {i//args.batch_size + 1}: {len(all_data)} records inserted")

    logger.info(f"COMPLETE! Polygon: {polygon_total} records, Tiingo: {tiingo_total} records")
    ray.shutdown()

if __name__ == "__main__":
    asyncio.run(main())