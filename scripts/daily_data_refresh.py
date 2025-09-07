#!/usr/bin/env python3
"""
ATS-INTG Daily Data Refresh Script

Collects daily price data from Tiingo, Polygon, and EODHD for the past 10 days
to ensure data integrity and detect any price discrepancies.

Features:
- 10-day lookback window for overlap validation
- Price discrepancy detection and alerting
- Multi-vendor data collection with rate limiting
- Comprehensive logging and error handling
- Slack notifications for anomalies

Usage:
    python3 scripts/daily_data_refresh.py
    python3 scripts/daily_data_refresh.py --vendors tiingo,polygon
    python3 scripts/daily_data_refresh.py --symbols AAPL,TSLA --debug
"""

import asyncio
import asyncpg
import logging
import os
import sys
import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Set
from pathlib import Path
import pandas as pd
import numpy as np
from dataclasses import dataclass, asdict
import aiohttp
import time

# Add src to path for imports
sys.path.insert(0, '/workspace/src')

from market_data.agent.tiingo_adapter import TiingoAdapter
from market_data.agent.polygon_adapter import PolygonAdapter
# Note: EODHD adapter not yet implemented - will be added later
from config.environment import Environment
from dao.instruments_dao import InstrumentsDAO
from dao.daily_prices_dao import DailyPricesDAO

logger = logging.getLogger(__name__)

@dataclass
class PriceDiscrepancy:
    """Represents a price discrepancy between vendors or time periods."""
    symbol: str
    date: date
    vendor1: str
    vendor2: str
    field: str  # 'open', 'high', 'low', 'close', 'volume'
    value1: float
    value2: float
    difference_pct: float
    severity: str  # 'minor', 'moderate', 'major'

@dataclass
class CollectionResult:
    """Results from daily data collection."""
    vendor: str
    symbols_processed: int
    records_inserted: int
    records_updated: int
    errors: List[str]
    discrepancies: List[PriceDiscrepancy]
    execution_time_seconds: float

class DailyDataRefresh:
    """Main class for daily price data collection and validation."""

    def __init__(self, lookback_days: int = 10):
        self.lookback_days = lookback_days
        self.db_pool = None
        self.instruments_dao = None
        self.daily_prices_dao = None

        # Initialize adapters with API keys
        self.tiingo_adapter = TiingoAdapter(os.getenv('TIINGO_API_KEY'))
        self.polygon_adapter = PolygonAdapter(os.getenv('POLYGON_API_KEY'))
        self.eodhd_adapter = None  # EODHD adapter not yet implemented

        # Collection results
        self.collection_results = []

    async def initialize(self):
        """Initialize database connections and DAOs."""
        try:
            # Database connection for INTG environment
            db_url = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'intg_password')}@{os.getenv('DB_HOST', 'ats-intg-postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'intg_db')}"

            self.db_pool = await asyncpg.create_pool(
                db_url,
                min_size=2,
                max_size=10,
                command_timeout=300
            )

            # Initialize DAOs
            env = Environment()
            self.instruments_dao = InstrumentsDAO(env)
            self.daily_prices_dao = DailyPricesDAO(env)

            logger.info("✅ Daily data refresh initialized successfully")

        except Exception as e:
            logger.error(f"❌ Failed to initialize daily data refresh: {e}")
            raise

    async def close(self):
        """Close database connections."""
        if self.db_pool:
            await self.db_pool.close()

    async def get_active_symbols(self, limit: int = None) -> List[str]:
        """Get list of active symbols for data collection."""
        async with self.db_pool.acquire() as conn:
            query = """
            SELECT DISTINCT symbol
            FROM intg_instruments
            WHERE active = true
              AND symbol IS NOT NULL
              AND symbol != ''
              AND symbol ~ '^[A-Z]{1,5}$'
            ORDER BY symbol
            """

            if limit:
                query += f" LIMIT {limit}"

            rows = await conn.fetch(query)
            symbols = [row['symbol'] for row in rows]

        logger.info(f"Retrieved {len(symbols)} active symbols for collection")
        return symbols

    async def collect_vendor_data(self, vendor: str, symbols: List[str]) -> CollectionResult:
        """Collect data from a specific vendor for the lookback period."""
        start_time = time.time()
        result = CollectionResult(
            vendor=vendor,
            symbols_processed=0,
            records_inserted=0,
            records_updated=0,
            errors=[],
            discrepancies=[],
            execution_time_seconds=0
        )

        try:
            # Calculate date range (past 10 days)
            end_date = date.today()
            start_date = end_date - timedelta(days=self.lookback_days)

            logger.info(f"🔄 Collecting {vendor} data for {len(symbols)} symbols from {start_date} to {end_date}")

            # Get adapter
            if vendor == 'tiingo':
                adapter = self.tiingo_adapter
                table_name = 'intg_daily_prices_tiingo'
            elif vendor == 'polygon':
                adapter = self.polygon_adapter
                table_name = 'intg_daily_prices_polygon'
            elif vendor == 'eodhd':
                if self.eodhd_adapter is None:
                    result.errors.append("EODHD adapter not yet implemented")
                    return result
                adapter = self.eodhd_adapter
                table_name = 'intg_daily_prices_eodhd'
            else:
                raise ValueError(f"Unknown vendor: {vendor}")

            # Process symbols in batches to respect rate limits
            batch_size = self._get_batch_size(vendor)

            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                logger.info(f"Processing {vendor} batch {i//batch_size + 1}: {len(batch)} symbols")

                try:
                    # Collect data for this batch
                    batch_result = await self._process_vendor_batch(
                        adapter, vendor, table_name, batch, start_date, end_date
                    )

                    result.symbols_processed += batch_result['symbols_processed']
                    result.records_inserted += batch_result['records_inserted']
                    result.records_updated += batch_result['records_updated']
                    result.errors.extend(batch_result['errors'])

                    # Rate limiting delay
                    await asyncio.sleep(self._get_rate_delay(vendor))

                except Exception as e:
                    error_msg = f"Batch processing error for {vendor} batch {i//batch_size + 1}: {e}"
                    logger.error(error_msg)
                    result.errors.append(error_msg)

            result.execution_time_seconds = time.time() - start_time
            logger.info(f"✅ {vendor} collection completed: {result.symbols_processed} symbols, {result.records_inserted} inserted, {result.records_updated} updated")

        except Exception as e:
            error_msg = f"Vendor collection failed for {vendor}: {e}"
            logger.error(error_msg)
            result.errors.append(error_msg)
            result.execution_time_seconds = time.time() - start_time

        return result

    async def _process_vendor_batch(self, adapter, vendor: str, table_name: str, symbols: List[str], start_date: date, end_date: date) -> Dict:
        """Process a batch of symbols for a specific vendor."""
        batch_result = {
            'symbols_processed': 0,
            'records_inserted': 0,
            'records_updated': 0,
            'errors': []
        }

        async with self.db_pool.acquire() as conn:
            for symbol in symbols:
                try:
                    # Get instrument ID
                    instrument_id = await self._get_instrument_id(conn, symbol)
                    if not instrument_id:
                        batch_result['errors'].append(f"Instrument not found: {symbol}")
                        continue

                    # Fetch price data from vendor
                    if vendor == 'tiingo':
                        price_data = await self._fetch_tiingo_data(adapter, symbol, start_date, end_date)
                    elif vendor == 'polygon':
                        price_data = await self._fetch_polygon_data(adapter, symbol, start_date, end_date)
                    elif vendor == 'eodhd':
                        price_data = await self._fetch_eodhd_data(adapter, symbol, start_date, end_date)
                    else:
                        continue

                    if not price_data:
                        continue

                    # Store data with conflict detection
                    insert_count, update_count = await self._store_price_data(
                        conn, table_name, instrument_id, price_data, symbol
                    )

                    batch_result['records_inserted'] += insert_count
                    batch_result['records_updated'] += update_count
                    batch_result['symbols_processed'] += 1

                except Exception as e:
                    error_msg = f"Error processing {vendor} {symbol}: {e}"
                    logger.warning(error_msg)
                    batch_result['errors'].append(error_msg)

        return batch_result

    async def _fetch_tiingo_data(self, adapter: TiingoAdapter, symbol: str, start_date: date, end_date: date) -> List[Dict]:
        """Fetch price data from Tiingo."""
        try:
            # Tiingo adapter expects a list of symbols
            data = adapter.fetch_eod([symbol], start_date, end_date)
            # Convert EODPrice objects to dictionaries
            if data:
                return [
                    {
                        'date': price.date,
                        'open': price.open,
                        'high': price.high,
                        'low': price.low,
                        'close': price.close,
                        'volume': price.volume,
                        'adjclose': price.adj_close if hasattr(price, 'adj_close') else price.close
                    }
                    for price in data  # All data should be for the requested symbol
                ]
            return []
        except Exception as e:
            logger.warning(f"Tiingo fetch error for {symbol}: {e}")
            return []

    async def _fetch_polygon_data(self, adapter: PolygonAdapter, symbol: str, start_date: date, end_date: date) -> List[Dict]:
        """Fetch price data from Polygon."""
        try:
            # Polygon adapter expects a list of symbols
            data = adapter.fetch_eod([symbol], start_date, end_date)
            # Convert EODPrice objects to dictionaries
            if data:
                return [
                    {
                        'date': price.date,
                        'open': price.open,
                        'high': price.high,
                        'low': price.low,
                        'close': price.close,
                        'volume': price.volume,
                        'adjclose': price.adj_close if hasattr(price, 'adj_close') else price.close
                    }
                    for price in data  # All data should be for the requested symbol
                ]
            return []
        except Exception as e:
            logger.warning(f"Polygon fetch error for {symbol}: {e}")
            return []

    async def _fetch_eodhd_data(self, adapter, symbol: str, start_date: date, end_date: date) -> List[Dict]:
        """Fetch price data from EODHD."""
        try:
            data = await adapter.fetch_eod(symbol, start_date, end_date)
            return data or []
        except Exception as e:
            logger.warning(f"EODHD fetch error for {symbol}: {e}")
            return []

    async def _store_price_data(self, conn, table_name: str, instrument_id: int, price_data: List[Dict], symbol: str) -> Tuple[int, int]:
        """Store price data with conflict detection and discrepancy tracking."""
        insert_count = 0
        update_count = 0

        for record in price_data:
            try:
                # Extract date and prices
                record_date = record.get('date')
                if isinstance(record_date, str):
                    record_date = datetime.fromisoformat(record_date.replace('Z', '+00:00')).date()
                elif isinstance(record_date, datetime):
                    record_date = record_date.date()

                # Check if record exists
                existing_query = f"""
                SELECT open, high, low, close, volume, adjclose
                FROM {table_name}
                WHERE instrument_id = $1 AND date = $2
                """
                existing = await conn.fetchrow(existing_query, instrument_id, record_date)

                if existing:
                    # Check for discrepancies
                    await self._check_price_discrepancies(
                        symbol, record_date, existing, record, table_name.split('_')[-1]
                    )

                    # Update record
                    update_query = f"""
                    UPDATE {table_name}
                    SET open = $3, high = $4, low = $5, close = $6, volume = $7, adjclose = $8,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE instrument_id = $1 AND date = $2
                    """
                    await conn.execute(
                        update_query,
                        instrument_id, record_date,
                        record.get('open'), record.get('high'), record.get('low'),
                        record.get('close'), record.get('volume'), record.get('adjclose')
                    )
                    update_count += 1
                else:
                    # Insert new record
                    insert_query = f"""
                    INSERT INTO {table_name} (instrument_id, date, open, high, low, close, volume, adjclose)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (instrument_id, date) DO NOTHING
                    """
                    await conn.execute(
                        insert_query,
                        instrument_id, record_date,
                        record.get('open'), record.get('high'), record.get('low'),
                        record.get('close'), record.get('volume'), record.get('adjclose')
                    )
                    insert_count += 1

            except Exception as e:
                logger.warning(f"Error storing {symbol} record {record}: {e}")

        return insert_count, update_count

    async def _check_price_discrepancies(self, symbol: str, record_date: date, existing: dict, new_record: dict, vendor: str):
        """Check for price discrepancies between existing and new data."""
        tolerance = 0.01  # 1% tolerance for minor differences

        price_fields = ['open', 'high', 'low', 'close']

        for field in price_fields:
            existing_val = float(existing[field] or 0)
            new_val = float(new_record.get(field) or 0)

            if existing_val > 0 and new_val > 0:
                diff_pct = abs((new_val - existing_val) / existing_val) * 100

                if diff_pct > tolerance:
                    severity = 'minor' if diff_pct < 5 else ('moderate' if diff_pct < 10 else 'major')

                    discrepancy = PriceDiscrepancy(
                        symbol=symbol,
                        date=record_date,
                        vendor1=vendor,
                        vendor2=f"{vendor}_existing",
                        field=field,
                        value1=new_val,
                        value2=existing_val,
                        difference_pct=diff_pct,
                        severity=severity
                    )

                    # Add to current collection result
                    if self.collection_results:
                        self.collection_results[-1].discrepancies.append(discrepancy)

                    logger.warning(f"Price discrepancy detected: {symbol} {record_date} {field} {existing_val} -> {new_val} ({diff_pct:.2f}%)")

    async def _get_instrument_id(self, conn, symbol: str) -> Optional[int]:
        """Get instrument ID for a symbol."""
        query = "SELECT id FROM intg_instruments WHERE symbol = $1 AND active = true"
        row = await conn.fetchrow(query, symbol)
        return row['id'] if row else None

    def _get_batch_size(self, vendor: str) -> int:
        """Get appropriate batch size for vendor rate limits."""
        return {
            'tiingo': 50,   # 1000 requests/hour
            'polygon': 5,   # 5 requests/minute
            'eodhd': 20     # 20 requests/minute
        }.get(vendor, 10)

    def _get_rate_delay(self, vendor: str) -> float:
        """Get rate limiting delay for vendor."""
        return {
            'tiingo': 1.0,    # 1 second delay
            'polygon': 12.0,  # 12 second delay (5 req/min)
            'eodhd': 3.0      # 3 second delay (20 req/min)
        }.get(vendor, 5.0)

    async def detect_cross_vendor_discrepancies(self, symbols: List[str] = None) -> List[PriceDiscrepancy]:
        """Detect discrepancies between different vendors for the same data."""
        discrepancies = []

        if not symbols:
            symbols = await self.get_active_symbols(limit=100)  # Limit for daily check

        logger.info(f"🔍 Checking cross-vendor discrepancies for {len(symbols)} symbols")

        async with self.db_pool.acquire() as conn:
            # Get last 5 days of data for comparison
            end_date = date.today()
            start_date = end_date - timedelta(days=5)

            for symbol in symbols[:50]:  # Limit to top 50 for daily check
                try:
                    instrument_id = await self._get_instrument_id(conn, symbol)
                    if not instrument_id:
                        continue

                    # Get data from all vendors for comparison
                    vendor_data = {}
                    for vendor in ['tiingo', 'polygon', 'eodhd']:
                        query = f"""
                        SELECT date, open, high, low, close, volume
                        FROM intg_daily_prices_{vendor}
                        WHERE instrument_id = $1 AND date >= $2 AND date <= $3
                        ORDER BY date
                        """
                        rows = await conn.fetch(query, instrument_id, start_date, end_date)
                        vendor_data[vendor] = {row['date']: row for row in rows}

                    # Compare between vendors
                    common_dates = set(vendor_data['tiingo'].keys()) & set(vendor_data['polygon'].keys())

                    for check_date in common_dates:
                        tiingo_data = vendor_data['tiingo'][check_date]
                        polygon_data = vendor_data['polygon'][check_date]

                        for field in ['close']:  # Focus on close price for cross-vendor comparison
                            tiingo_val = float(tiingo_data[field] or 0)
                            polygon_val = float(polygon_data[field] or 0)

                            if tiingo_val > 0 and polygon_val > 0:
                                diff_pct = abs((polygon_val - tiingo_val) / tiingo_val) * 100

                                if diff_pct > 1.0:  # 1% threshold for cross-vendor discrepancies
                                    severity = 'minor' if diff_pct < 3 else ('moderate' if diff_pct < 5 else 'major')

                                    discrepancy = PriceDiscrepancy(
                                        symbol=symbol,
                                        date=check_date,
                                        vendor1='tiingo',
                                        vendor2='polygon',
                                        field=field,
                                        value1=tiingo_val,
                                        value2=polygon_val,
                                        difference_pct=diff_pct,
                                        severity=severity
                                    )
                                    discrepancies.append(discrepancy)

                except Exception as e:
                    logger.warning(f"Error checking cross-vendor discrepancies for {symbol}: {e}")

        logger.info(f"Found {len(discrepancies)} cross-vendor discrepancies")
        return discrepancies

    async def send_alert_notification(self, discrepancies: List[PriceDiscrepancy]):
        """Send Slack notification for significant discrepancies."""
        if not discrepancies:
            return

        # Group discrepancies by severity
        major_discrepancies = [d for d in discrepancies if d.severity == 'major']
        moderate_discrepancies = [d for d in discrepancies if d.severity == 'moderate']

        if major_discrepancies or len(moderate_discrepancies) > 10:
            webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            if not webhook_url:
                logger.warning("SLACK_WEBHOOK_URL not configured - skipping alert")
                return

            # Create alert message
            alert_text = f"🚨 **Daily Price Data Alert**\n"
            alert_text += f"Found {len(discrepancies)} price discrepancies:\n"
            alert_text += f"• Major: {len(major_discrepancies)}\n"
            alert_text += f"• Moderate: {len(moderate_discrepancies)}\n"

            if major_discrepancies:
                alert_text += f"\n**Major Discrepancies:**\n"
                for d in major_discrepancies[:5]:  # Show first 5
                    alert_text += f"• {d.symbol} {d.date} {d.field}: {d.value1} vs {d.value2} ({d.difference_pct:.1f}%)\n"

            try:
                async with aiohttp.ClientSession() as session:
                    payload = {"text": alert_text}
                    async with session.post(webhook_url, json=payload) as resp:
                        if resp.status == 200:
                            logger.info("✅ Alert notification sent successfully")
                        else:
                            logger.error(f"❌ Failed to send alert notification: {resp.status}")
            except Exception as e:
                logger.error(f"❌ Error sending alert notification: {e}")

    async def generate_daily_report(self) -> Dict:
        """Generate comprehensive daily collection report."""
        report = {
            'collection_date': date.today().isoformat(),
            'collection_timestamp': datetime.now().isoformat(),
            'lookback_days': self.lookback_days,
            'vendor_results': [],
            'total_discrepancies': 0,
            'summary': {}
        }

        # Add vendor results
        for result in self.collection_results:
            report['vendor_results'].append(asdict(result))
            report['total_discrepancies'] += len(result.discrepancies)

        # Calculate summary statistics
        total_symbols = sum(r.symbols_processed for r in self.collection_results)
        total_inserted = sum(r.records_inserted for r in self.collection_results)
        total_updated = sum(r.records_updated for r in self.collection_results)
        total_errors = sum(len(r.errors) for r in self.collection_results)

        report['summary'] = {
            'vendors_processed': len(self.collection_results),
            'total_symbols_processed': total_symbols,
            'total_records_inserted': total_inserted,
            'total_records_updated': total_updated,
            'total_errors': total_errors,
            'success_rate': round((total_symbols - total_errors) / max(total_symbols, 1) * 100, 2),
            'data_freshness': 'Current' if total_inserted + total_updated > 0 else 'Stale'
        }

        return report

    async def run_daily_collection(self, vendors: List[str] = None, symbols: List[str] = None, max_symbols: int = None) -> Dict:
        """Run the complete daily data collection process."""
        start_time = time.time()
        logger.info("🚀 Starting daily price data collection...")

        try:
            # Get symbols to process
            if not symbols:
                symbols = await self.get_active_symbols(limit=max_symbols)

            if not symbols:
                logger.error("❌ No active symbols found for collection")
                return {'error': 'No symbols available'}

            # Default to all vendors
            if not vendors:
                vendors = ['tiingo', 'polygon', 'eodhd']

            logger.info(f"📊 Processing {len(symbols)} symbols across {len(vendors)} vendors")

            # Collect data from each vendor
            for vendor in vendors:
                logger.info(f"\n🔄 Starting {vendor} collection...")
                result = await self.collect_vendor_data(vendor, symbols)
                self.collection_results.append(result)

                if result.errors:
                    logger.warning(f"⚠️ {vendor} had {len(result.errors)} errors")

            # Check for cross-vendor discrepancies
            logger.info("\n🔍 Checking cross-vendor discrepancies...")
            cross_vendor_discrepancies = await self.detect_cross_vendor_discrepancies(symbols[:100])

            # Combine all discrepancies
            all_discrepancies = []
            for result in self.collection_results:
                all_discrepancies.extend(result.discrepancies)
            all_discrepancies.extend(cross_vendor_discrepancies)

            # Send alerts for significant discrepancies
            if all_discrepancies:
                await self.send_alert_notification(all_discrepancies)

            # Generate final report
            report = await self.generate_daily_report()
            report['cross_vendor_discrepancies'] = len(cross_vendor_discrepancies)
            report['execution_time_seconds'] = time.time() - start_time

            # Save report
            output_dir = Path("/logs")
            output_dir.mkdir(exist_ok=True)

            report_file = output_dir / f"daily_collection_report_{date.today().strftime('%Y%m%d')}.json"
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)

            logger.info(f"📋 Daily collection report saved: {report_file}")

            # Log summary
            logger.info(f"\n✅ Daily collection completed in {report['execution_time_seconds']:.1f} seconds")
            logger.info(f"📊 Summary: {report['summary']}")

            if all_discrepancies:
                major_count = len([d for d in all_discrepancies if d.severity == 'major'])
                logger.warning(f"⚠️ Found {len(all_discrepancies)} discrepancies ({major_count} major)")
            else:
                logger.info("✅ No price discrepancies detected")

            return report

        except Exception as e:
            logger.error(f"❌ Daily collection failed: {e}")
            import traceback
            traceback.print_exc()
            raise

async def main():
    """Main function for daily data refresh."""
    import argparse

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description='ATS-INTG Daily Data Refresh')
    parser.add_argument('--vendors', type=str, help='Comma-separated list of vendors (tiingo,polygon,eodhd)')
    parser.add_argument('--symbols', type=str, help='Comma-separated list of symbols to process')
    parser.add_argument('--max-symbols', type=int, help='Maximum number of symbols to process')
    parser.add_argument('--lookback-days', type=int, default=10, help='Number of days to look back for data collection')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse vendors
    vendors = None
    if args.vendors:
        vendors = [v.strip() for v in args.vendors.split(',')]

    # Parse symbols
    symbols = None
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(',')]

    logger.info("="*80)
    logger.info("ATS-INTG DAILY PRICE DATA COLLECTION")
    logger.info("="*80)
    logger.info(f"Lookback period: {args.lookback_days} days")
    logger.info(f"Vendors: {vendors or 'all'}")
    logger.info(f"Symbols: {symbols or 'all active'}")
    logger.info(f"Max symbols: {args.max_symbols or 'unlimited'}")

    # Initialize and run collection
    refresher = DailyDataRefresh(lookback_days=args.lookback_days)

    try:
        await refresher.initialize()

        report = await refresher.run_daily_collection(
            vendors=vendors,
            symbols=symbols,
            max_symbols=args.max_symbols
        )

        logger.info("\n🎯 DAILY COLLECTION COMPLETED SUCCESSFULLY")

    finally:
        await refresher.close()

if __name__ == "__main__":
    asyncio.run(main())