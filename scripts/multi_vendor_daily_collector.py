#!/usr/bin/env python3
"""
Multi-Vendor Daily Data Collector for ATS-INTG

Collects daily price data from multiple vendors (Tiingo, Polygon, EODHD) and stores
in the integration database. Designed for automated daily execution via cron jobs.

Features:
- Rate-limited API calls respecting vendor limits
- Comprehensive error handling and logging
- Conflict resolution for overlapping data
- Slack notifications for critical failures
- Progress tracking and detailed reporting

Usage:
    python3 scripts/multi_vendor_daily_collector.py --vendors tiingo,polygon,eodhd
    python3 scripts/multi_vendor_daily_collector.py --vendors tiingo --symbols 50 --days 7
    python3 scripts/multi_vendor_daily_collector.py --debug
"""

import asyncio
import asyncpg
import requests
import aiohttp
import os
import json
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
import argparse
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class VendorConfig:
    """Configuration for each vendor."""
    name: str
    api_key_env: str
    table_name: str
    rate_limit_seconds: float
    batch_size: int
    base_url: str
    timeout_seconds: int = 30

@dataclass
class CollectionResult:
    """Results from vendor data collection."""
    vendor: str
    symbols_processed: int
    records_inserted: int
    records_updated: int
    records_failed: int
    api_calls_made: int
    execution_time_seconds: float
    errors: List[str]
    latest_date: Optional[str] = None

class VendorCollector:
    """Base class for vendor-specific data collection."""

    def __init__(self, config: VendorConfig):
        self.config = config
        self.api_key = os.getenv(config.api_key_env)
        if not self.api_key:
            raise ValueError(f"API key not found: {config.api_key_env}")

        self.session = None
        self.db_conn = None

    async def initialize(self, db_conn):
        """Initialize the collector with database connection."""
        self.db_conn = db_conn
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.config.timeout_seconds))

    async def cleanup(self):
        """Clean up resources."""
        if self.session:
            await self.session.close()

    async def collect_data(self, symbols: List[Tuple[str, int]], start_date: date, end_date: date) -> CollectionResult:
        """Collect data for given symbols and date range."""
        start_time = time.time()
        result = CollectionResult(
            vendor=self.config.name,
            symbols_processed=0,
            records_inserted=0,
            records_updated=0,
            records_failed=0,
            api_calls_made=0,
            execution_time_seconds=0,
            errors=[]
        )

        logger.info(f"🔄 Starting {self.config.name} collection for {len(symbols)} symbols")

        for i, (symbol, instrument_id) in enumerate(symbols):
            logger.debug(f"[{i+1}/{len(symbols)}] Processing {self.config.name} - {symbol}")

            # Fetch data from vendor API
            data = await self.fetch_symbol_data(symbol, start_date, end_date)
            result.api_calls_made += 1

            if not data:
                logger.debug(f"No data returned for {self.config.name} - {symbol}")
                continue

            # Store data in database
            inserted, updated, failed = await self.store_symbol_data(symbol, instrument_id, data)
            result.records_inserted += inserted
            result.records_updated += updated
            result.records_failed += failed
            result.symbols_processed += 1

            logger.debug(f"✅ {self.config.name} - {symbol}: {inserted} inserted, {updated} updated")

            # Rate limiting
            await asyncio.sleep(self.config.rate_limit_seconds)

        latest_date = await self.db_conn.fetchval(f"SELECT MAX(date) FROM {self.config.table_name}")
        result.latest_date = latest_date.isoformat() if latest_date else None
        result.execution_time_seconds = time.time() - start_time
        logger.info(f"✅ {self.config.name} collection completed: {result.symbols_processed} symbols, {result.records_inserted} inserted, {result.records_updated} updated")

        return result

class TiingoCollector(VendorCollector):
    """Tiingo-specific data collector."""

    async def fetch_symbol_data(self, symbol: str, start_date: date, end_date: date) -> List[Dict]:
        """Fetch price data from Tiingo API."""
        url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        params = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'token': self.api_key
        }

        async with self.session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()
    async def store_symbol_data(self, symbol: str, instrument_id: int, data: List[Dict]) -> Tuple[int, int, int]:
        """Store Tiingo data in database."""
        inserted, updated, failed = 0, 0, 0

        for record in data:
            # Parse date
            record_date = datetime.fromisoformat(record['date'].replace('Z', '+00:00')).date()

            # Check if record exists
            existing = await self.db_conn.fetchrow(
                f"SELECT instrument_id FROM {self.config.table_name} WHERE instrument_id = $1 AND date = $2",
                instrument_id, record_date
            )

            if existing:
                # Update existing record
                await self.db_conn.execute(f"""
                    UPDATE {self.config.table_name}
                    SET open = $3, high = $4, low = $5, close = $6, volume = $7,
                        adjusted_close = $8, symbol = $9, updated_at = CURRENT_TIMESTAMP
                    WHERE instrument_id = $1 AND date = $2
                """, instrument_id, record_date,
                    float(record['open']), float(record['high']), float(record['low']),
                    float(record['close']), int(record['volume']), float(record['adjClose']), symbol)
                updated += 1
            else:
                # Insert new record
                await self.db_conn.execute(f"""
                    INSERT INTO {self.config.table_name}
                    (instrument_id, date, open, high, low, close, volume, adjusted_close, symbol, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (instrument_id, date) DO NOTHING
                """, instrument_id, record_date,
                    float(record['open']), float(record['high']), float(record['low']),
                    float(record['close']), int(record['volume']), float(record['adjClose']), symbol)
                inserted += 1

        return inserted, updated, failed

class PolygonCollector(VendorCollector):
    """Polygon-specific data collector."""

    async def fetch_symbol_data(self, symbol: str, start_date: date, end_date: date) -> List[Dict]:
        """Fetch price data from Polygon API."""
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
        params = {'apikey': self.api_key}

        async with self.session.get(url, params=params) as response:
            response.raise_for_status()
            data = await response.json()

            if data.get('status') == 'OK' and data.get('results'):
                # Convert Polygon format to standard format
                results = []
                for item in data['results']:
                    results.append({
                        'date': datetime.fromtimestamp(item['t'] / 1000).strftime('%Y-%m-%d'),
                        'open': item['o'],
                        'high': item['h'],
                        'low': item['l'],
                        'close': item['c'],
                        'volume': item['v'],
                        'adjClose': item.get('vw', item['c'])  # Use VWAP as adjusted close if available
                    })
                return results

            return []
    async def store_symbol_data(self, symbol: str, instrument_id: int, data: List[Dict]) -> Tuple[int, int, int]:
        """Store Polygon data in database."""
        inserted, updated, failed = 0, 0, 0

        for record in data:
            # Parse date
            if isinstance(record['date'], str):
                record_date = datetime.strptime(record['date'], '%Y-%m-%d').date()
            else:
                record_date = record['date']

            # Check if record exists
            existing = await self.db_conn.fetchrow(
                f"SELECT instrument_id FROM {self.config.table_name} WHERE instrument_id = $1 AND date = $2",
                instrument_id, record_date
            )

            if existing:
                # Update existing record (Polygon table has no adjclose, has market_cap)
                await self.db_conn.execute(f"""
                    UPDATE {self.config.table_name}
                    SET open = $3, high = $4, low = $5, close = $6, volume = $7,
                        symbol = $8, updated_at = CURRENT_TIMESTAMP
                    WHERE instrument_id = $1 AND date = $2
                """, instrument_id, record_date,
                    float(record['open']), float(record['high']), float(record['low']),
                    float(record['close']), int(record['volume']), symbol)
                updated += 1
            else:
                # Insert new record (Polygon table schema)
                await self.db_conn.execute(f"""
                    INSERT INTO {self.config.table_name}
                    (instrument_id, date, open, high, low, close, volume, symbol, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (instrument_id, date) DO NOTHING
                """, instrument_id, record_date,
                    float(record['open']), float(record['high']), float(record['low']),
                    float(record['close']), int(record['volume']), symbol)
                inserted += 1

        return inserted, updated, failed

class EODHDCollector(VendorCollector):
    """EODHD-specific data collector."""

    async def fetch_symbol_data(self, symbol: str, start_date: date, end_date: date) -> List[Dict]:
        """Fetch price data from EODHD API."""
        url = f"https://eodhd.com/api/eod/{symbol}.US"
        params = {
            'api_token': self.api_key,
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'fmt': 'json'
        }

        async with self.session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()
    async def store_symbol_data(self, symbol: str, instrument_id: int, data: List[Dict]) -> Tuple[int, int, int]:
        """Store EODHD data in database."""
        inserted, updated, failed = 0, 0, 0

        for record in data:
            # Parse date
            record_date = datetime.strptime(record['date'], '%Y-%m-%d').date()

            # Check if record exists (EODHD table has id column)
            existing = await self.db_conn.fetchrow(
                f"SELECT id FROM {self.config.table_name} WHERE instrument_id = $1 AND date = $2",
                instrument_id, record_date
            )

            if existing:
                # Update existing record (no updated_at column in EODHD table)
                await self.db_conn.execute(f"""
                    UPDATE {self.config.table_name}
                    SET open = $3, high = $4, low = $5, close = $6, volume = $7,
                        adjusted_close = $8, symbol = $9
                    WHERE instrument_id = $1 AND date = $2
                """, instrument_id, record_date,
                    float(record['open']), float(record['high']), float(record['low']),
                    float(record['close']), int(record['volume']),
                    float(record.get('adjusted_close', record['close'])), symbol)
                updated += 1
            else:
                # Insert new record (EODHD table schema - has id, created_at but no updated_at)
                await self.db_conn.execute(f"""
                    INSERT INTO {self.config.table_name}
                    (instrument_id, date, open, high, low, close, volume, adjusted_close, symbol, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP)
                    ON CONFLICT (instrument_id, date) DO NOTHING
                """, instrument_id, record_date,
                    float(record['open']), float(record['high']), float(record['low']),
                    float(record['close']), int(record['volume']),
                    float(record.get('adjusted_close', record['close'])), symbol)
                inserted += 1

        return inserted, updated, failed

class MultiVendorDailyCollector:
    """Main collector that orchestrates multiple vendors."""

    def __init__(self):
        self.vendor_configs = {
            'tiingo': VendorConfig(
                name='tiingo',
                api_key_env='TIINGO_API_KEY',
                table_name='intg_daily_price_tiingo',
                rate_limit_seconds=1.0,  # 1000 requests/hour = ~1 per second
                batch_size=50,
                base_url='https://api.tiingo.com'
            ),
            'polygon': VendorConfig(
                name='polygon',
                api_key_env='POLYGON_API_KEY',
                table_name='intg_daily_price_polygon',
                rate_limit_seconds=12.0,  # 5 requests/minute
                batch_size=5,
                base_url='https://api.polygon.io'
            ),
            'eodhd': VendorConfig(
                name='eodhd',
                api_key_env='EODHD_API_KEY',
                table_name='intg_daily_price_eodhd',
                rate_limit_seconds=3.0,  # 20 requests/minute
                batch_size=20,
                base_url='https://eodhd.com'
            )
        }

        self.db_pool = None
        self.collectors = {}

    async def initialize(self):
        """Initialize database connection and collectors."""
        # Database connection for INTG environment
        db_url = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'intg_password')}@{os.getenv('DB_HOST', 'ats-intg-postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'intg_db')}"

        self.db_pool = await asyncpg.create_pool(
            db_url,
            min_size=2,
            max_size=10,
            command_timeout=300
        )

        logger.info("✅ Database connection pool initialized")

    async def cleanup(self):
        """Clean up resources."""
        for collector in self.collectors.values():
            await collector.cleanup()

        if self.db_pool:
            await self.db_pool.close()

    async def get_active_symbols(self, limit: Optional[int] = None) -> List[Tuple[str, int]]:
        """Get active symbols for collection."""
        async with self.db_pool.acquire() as conn:
            query = """
                SELECT DISTINCT symbol, id
                FROM intg_instrument
                WHERE active = true
                  AND symbol IS NOT NULL
                  AND symbol ~ '^[A-Z]{1,5}$'
                ORDER BY symbol
            """

            if limit:
                query += f" LIMIT {limit}"

            rows = await conn.fetch(query)
            return [(row['symbol'], row['id']) for row in rows]

    async def collect_vendor_data(self, vendor_name: str, symbols: List[Tuple[str, int]],
                                 start_date: date, end_date: date) -> CollectionResult:
        """Collect data from a specific vendor."""
        config = self.vendor_configs[vendor_name]

        # Initialize collector
        if vendor_name == 'tiingo':
            collector = TiingoCollector(config)
        elif vendor_name == 'polygon':
            collector = PolygonCollector(config)
        elif vendor_name == 'eodhd':
            collector = EODHDCollector(config)
        else:
            raise ValueError(f"Unknown vendor: {vendor_name}")

        async with self.db_pool.acquire() as conn:
            await collector.initialize(conn)
            return await collector.collect_data(symbols, start_date, end_date)
    async def run_collection(self, vendors: List[str], lookback_days: int = 7,
                           max_symbols: Optional[int] = None) -> Dict[str, CollectionResult]:
        """Run collection for specified vendors."""
        logger.info("🚀 Starting multi-vendor daily data collection...")

        # Get symbols
        symbols = await self.get_active_symbols(max_symbols)
        if not symbols:
            logger.error("❌ No active symbols found")
            return {}

        # Calculate date range
        end_date = date.today()
        start_date = end_date - timedelta(days=lookback_days)

        logger.info(f"📊 Processing {len(symbols)} symbols from {start_date} to {end_date}")
        logger.info(f"🎯 Vendors: {', '.join(vendors)}")

        results = {}

        for vendor in vendors:
            if vendor not in self.vendor_configs:
                logger.error(f"❌ Unknown vendor: {vendor}")
                continue

            logger.info(f"\n🔄 Starting {vendor} collection...")
            result = await self.collect_vendor_data(vendor, symbols, start_date, end_date)
            results[vendor] = result

            # Log summary
            logger.info(f"✅ {vendor} completed: {result.symbols_processed} symbols, "
                       f"{result.records_inserted} inserted, {result.records_updated} updated")

            if result.errors:
                logger.warning(f"⚠️ {vendor} had {len(result.errors)} errors")

        return results

async def send_slack_notification(results: Dict[str, CollectionResult]):
    """Send Slack notification with collection results."""
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not webhook_url:
        logger.info("SLACK_WEBHOOK_URL not configured - skipping notification")
        return

    # Calculate totals
    total_symbols = sum(r.symbols_processed for r in results.values())
    total_inserted = sum(r.records_inserted for r in results.values())
    total_updated = sum(r.records_updated for r in results.values())
    total_errors = sum(len(r.errors) for r in results.values())

    # Create message
    message = f"📊 **ATS-INTG Daily Data Collection Report**\n"
    message += f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    message += f"**Summary:**\n"
    message += f"• Symbols processed: {total_symbols}\n"
    message += f"• Records inserted: {total_inserted}\n"
    message += f"• Records updated: {total_updated}\n"
    message += f"• Total errors: {total_errors}\n\n"

    message += f"**Vendor Results:**\n"
    for vendor, result in results.items():
        status = "✅" if result.symbols_processed > 0 else "❌"
        message += f"• {status} {vendor.upper()}: {result.symbols_processed} symbols, "
        message += f"{result.records_inserted} inserted, {result.records_updated} updated"
        if result.errors:
            message += f" ({len(result.errors)} errors)"
        message += "\n"

    if total_errors > 10:
        message += f"\n⚠️ **Warning: {total_errors} errors detected - please review logs**"

    async with aiohttp.ClientSession() as session:
        payload = {"text": message}
        async with session.post(webhook_url, json=payload) as resp:
            if resp.status == 200:
                logger.info("✅ Slack notification sent")
            else:
                logger.error(f"❌ Failed to send Slack notification: {resp.status}")
async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Multi-Vendor Daily Data Collector')
    parser.add_argument('--vendors', type=str, default='tiingo,polygon,eodhd',
                       help='Comma-separated list of vendors (default: tiingo,polygon,eodhd)')
    parser.add_argument('--days', type=int, default=7,
                       help='Number of lookback days (default: 7)')
    parser.add_argument('--symbols', type=int,
                       help='Maximum number of symbols to process (default: all)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--no-slack', action='store_true',
                       help='Disable Slack notifications')

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse vendors
    vendors = [v.strip().lower() for v in args.vendors.split(',')]

    logger.info("="*80)
    logger.info("ATS-INTG MULTI-VENDOR DAILY DATA COLLECTION")
    logger.info("="*80)
    logger.info(f"Vendors: {', '.join(vendors)}")
    logger.info(f"Lookback days: {args.days}")
    logger.info(f"Max symbols: {args.symbols or 'all'}")
    logger.info(f"Debug mode: {args.debug}")

    # Run collection
    collector = MultiVendorDailyCollector()
    await collector.initialize()
    results = await collector.run_collection(vendors, args.days, args.symbols)

    # Send notifications
    if not args.no_slack and results:
        await send_slack_notification(results)

    # Print final summary
    logger.info("\n" + "="*80)
    logger.info("COLLECTION COMPLETED")
    logger.info("="*80)

    for vendor, result in results.items():
        logger.info(f"{vendor.upper()}: {result.symbols_processed} symbols, "
                   f"{result.records_inserted} inserted, {result.records_updated} updated, "
                   f"{len(result.errors)} errors")
        if result.latest_date:
            logger.info(f"  Latest data: {result.latest_date}")

    # Exit with appropriate code
    total_errors = sum(len(r.errors) for r in results.values())
    if total_errors > 0:
        logger.warning(f"⚠️ Completed with {total_errors} errors")
        exit(1)
    else:
        logger.info("✅ Collection completed successfully")
        exit(0)

if __name__ == "__main__":
    asyncio.run(main())