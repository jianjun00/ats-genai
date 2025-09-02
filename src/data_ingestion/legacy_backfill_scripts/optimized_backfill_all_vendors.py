#!/usr/bin/env python3
"""
Multi-Vendor Optimized Daily Price Backfill

Intelligent backfill for all vendors (Polygon, Tiingo, EODHD) that:
1. Identifies missing dates for each instrument per vendor
2. Skips weekends and market holidays
3. Makes targeted API calls for only missing trading days
4. Dramatically reduces API usage across all vendors
5. Runs vendors in parallel with proper rate limiting
"""

import sys
sys.path.append('/workspace/src')

import os
import asyncio
import asyncpg
import requests
import logging
from datetime import datetime, timedelta, date, timezone
import time
import json
import argparse
from typing import List, Set, Tuple, Dict
import concurrent.futures
from threading import Lock

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("optimized_backfill_all")

class SimpleTradingCalendar:
    """Simplified US trading calendar for recent years."""
    
    def __init__(self):
        # Key market holidays for 2020-2025
        self.market_holidays = {
            # 2020
            date(2020, 1, 1), date(2020, 1, 20), date(2020, 2, 17), date(2020, 4, 10),
            date(2020, 5, 25), date(2020, 7, 3), date(2020, 9, 7), date(2020, 11, 26),
            date(2020, 12, 25),
            # 2021
            date(2021, 1, 1), date(2021, 1, 18), date(2021, 2, 15), date(2021, 4, 2),
            date(2021, 5, 31), date(2021, 6, 18), date(2021, 7, 5), date(2021, 9, 6),
            date(2021, 11, 25), date(2021, 12, 24),
            # 2022
            date(2022, 1, 17), date(2022, 2, 21), date(2022, 4, 15), date(2022, 5, 30),
            date(2022, 6, 20), date(2022, 7, 4), date(2022, 9, 5), date(2022, 11, 24),
            date(2022, 12, 26),
            # 2023
            date(2023, 1, 2), date(2023, 1, 16), date(2023, 2, 20), date(2023, 4, 7),
            date(2023, 5, 29), date(2023, 6, 19), date(2023, 7, 4), date(2023, 9, 4),
            date(2023, 11, 23), date(2023, 12, 25),
            # 2024
            date(2024, 1, 1), date(2024, 1, 15), date(2024, 2, 19), date(2024, 3, 29),
            date(2024, 5, 27), date(2024, 6, 19), date(2024, 7, 4), date(2024, 9, 2),
            date(2024, 11, 28), date(2024, 12, 25),
            # 2025
            date(2025, 1, 1), date(2025, 1, 20), date(2025, 2, 17), date(2025, 4, 18),
            date(2025, 5, 26), date(2025, 6, 19), date(2025, 7, 4), date(2025, 9, 1),
            date(2025, 11, 27), date(2025, 12, 25)
        }
    
    def is_trading_day(self, check_date: date) -> bool:
        """Check if a date is a trading day (not weekend or holiday)."""
        # Check if weekend
        if check_date.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        
        # Check if holiday
        if check_date in self.market_holidays:
            return False
        
        return True
    
    def get_trading_days_in_range(self, start_date: date, end_date: date) -> List[date]:
        """Get all trading days in date range."""
        trading_days = []
        current_date = start_date
        
        while current_date <= end_date:
            if self.is_trading_day(current_date):
                trading_days.append(current_date)
            current_date += timedelta(days=1)
        
        return trading_days


class MultiVendorOptimizedBackfiller:
    """
    Optimized backfiller for multiple vendors with missing date detection.
    """
    
    def __init__(self):
        self.calendar = SimpleTradingCalendar()
        
        # API configurations
        self.vendors = {
            'polygon': {
                'api_key': os.environ.get("POLYGON_API_KEY"),
                'base_url': "https://api.polygon.io/v2/aggs/ticker",
                'rate_limit': 10.0,  # 10 seconds between requests
                'table_suffix': 'polygon'
            },
            'tiingo': {
                'api_key': os.environ.get("TIINGO_API_KEY"),
                'base_url': "https://api.tiingo.com/tiingo/daily",
                'rate_limit': 1.0,   # 1 second between requests
                'table_suffix': 'tiingo'
            },
            'eodhd': {
                'api_key': os.environ.get("EODHD_API_KEY"),
                'base_url': "https://eodhd.com/api/eod",
                'rate_limit': 3.0,   # 3 seconds between requests
                'table_suffix': 'eodhd'
            }
        }
        
        # Global statistics
        self.stats = {
            'total_instruments': 0,
            'total_vendors': len([v for v in self.vendors.values() if v['api_key']]),
            'vendor_stats': {}
        }
        
        # Initialize vendor stats
        for vendor_name, config in self.vendors.items():
            if config['api_key']:
                self.stats['vendor_stats'][vendor_name] = {
                    'processed_instruments': 0,
                    'skipped_instruments': 0,
                    'total_records': 0,
                    'api_calls': 0,
                    'missing_date_ranges': 0,
                    'errors': 0
                }
        
        logger.info(f"🎯 Multi-Vendor Optimized Backfiller initialized")
        logger.info(f"   Available vendors: {list(self.stats['vendor_stats'].keys())}")
        logger.info(f"   Trading calendar: 2020-2025 with {len(self.calendar.market_holidays)} holidays")

    async def get_database_connection(self):
        """Get database connection (Docker-compatible)."""
        env = os.getenv('ENV_TYPE', 'intg').lower()
        
        if env == 'intg':
            return await asyncpg.connect(
                host='ats-intg-postgres',
                port=5432,
                user='postgres',
                password='intg_password',
                database='intg_db'
            )
        else:
            return await asyncpg.connect(
                host='ats-dev-postgres',
                port=5432,
                user='postgres',
                password='dev_password',
                database='dev_db'
            )

    async def ensure_tables_exist(self, conn):
        """Ensure all vendor daily price tables exist."""
        env = os.getenv('ENV_TYPE', 'intg').lower()
        table_prefix = 'intg_' if env == 'intg' else 'dev_'
        
        for vendor_name, config in self.vendors.items():
            if not config['api_key']:
                continue
                
            table_name = f"{table_prefix}daily_prices_{config['table_suffix']}"
            
            try:
                result = await conn.fetchrow(f"SELECT to_regclass('{table_name}')")
                
                if result[0] is None:
                    await conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            date DATE NOT NULL,
                            symbol TEXT,
                            open DOUBLE PRECISION,
                            high DOUBLE PRECISION,
                            low DOUBLE PRECISION,
                            close DOUBLE PRECISION,
                            volume BIGINT,
                            market_cap DOUBLE PRECISION,
                            instrument_id INTEGER NOT NULL,
                            created_at TIMESTAMP WITH TIME ZONE,
                            updated_at TIMESTAMP WITH TIME ZONE,
                            PRIMARY KEY(date, instrument_id)
                        )
                    """)
                    logger.info(f"✅ Created table: {table_name}")
                else:
                    logger.info(f"✅ Table exists: {table_name}")
            except Exception as e:
                logger.error(f"❌ Failed to ensure table {table_name} exists: {e}")
                raise

    async def get_instruments_for_backfill(self, conn, limit=None):
        """Get active instruments from instruments table."""
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        env = os.getenv('ENV_TYPE', 'intg').lower()
        table_prefix = 'intg_' if env == 'intg' else 'dev_'
        
        instruments = await conn.fetch(f"""
            SELECT id, symbol, name, exchange, active
            FROM {table_prefix}instruments 
            WHERE active = true 
              AND symbol IS NOT NULL 
              AND symbol != ''
              AND exchange IN ('NASDAQ', 'NYSE', 'NYSE ARCA', 'BATS', 'XNYS', 'NYSE MKT', 'XNAS', 'AMEX', 'NYSE NAT')
            ORDER BY symbol
            {limit_clause}
        """)
        
        self.stats['total_instruments'] = len(instruments)
        logger.info(f"📊 Found {len(instruments)} instruments for multi-vendor backfill")
        return instruments

    async def get_missing_trading_days_vendor(self, conn, vendor_name: str, instrument_id: int, symbol: str, start_date: date, end_date: date) -> List[Tuple[date, date]]:
        """
        Get missing trading days for an instrument for specific vendor.
        Returns list of (start_date, end_date) tuples for missing ranges.
        """
        env = os.getenv('ENV_TYPE', 'intg').lower()
        table_prefix = 'intg_' if env == 'intg' else 'dev_'
        table_name = f"{table_prefix}daily_prices_{self.vendors[vendor_name]['table_suffix']}"
        
        # Get all existing dates for this instrument from this vendor
        existing_dates = await conn.fetch(f"""
            SELECT date FROM {table_name}
            WHERE instrument_id = $1 AND date BETWEEN $2 AND $3
            ORDER BY date
        """, instrument_id, start_date, end_date)
        
        existing_dates_set = {row['date'] for row in existing_dates}
        
        # Get all expected trading days
        expected_trading_days = self.calendar.get_trading_days_in_range(start_date, end_date)
        
        # Find missing trading days
        missing_dates = [d for d in expected_trading_days if d not in existing_dates_set]
        
        if not missing_dates:
            return []
        
        # Group consecutive missing dates into ranges (max 30 days per range for API efficiency)
        date_ranges = []
        range_start = missing_dates[0]
        range_end = missing_dates[0]
        
        for i in range(1, len(missing_dates)):
            current_date = missing_dates[i]
            days_diff = (current_date - range_end).days
            
            if days_diff <= 7 and (range_end - range_start).days < 30:
                # Extend current range (if gap is small and range not too long)
                range_end = current_date
            else:
                # Start new range
                date_ranges.append((range_start, range_end))
                range_start = current_date
                range_end = current_date
        
        # Add final range
        date_ranges.append((range_start, range_end))
        
        return date_ranges

    def download_polygon_data(self, symbol: str, start_date: date, end_date: date):
        """Download daily prices from Polygon API."""
        config = self.vendors['polygon']
        url = f"{config['base_url']}/{symbol}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000,
            'apikey': config['api_key']
        }
        
        try:
            response = requests.get(url, params=params)
            self.stats['vendor_stats']['polygon']['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') in ['OK', 'DELAYED'] and 'results' in data:
                    results = data['results']
                    # Convert Polygon format to standard format
                    standard_results = []
                    for item in results:
                        if 't' in item:
                            date_val = datetime.fromtimestamp(item['t']/1000, tz=timezone.utc).date()
                            standard_results.append({
                                'date': date_val,
                                'open': item.get('o'),
                                'high': item.get('h'),
                                'low': item.get('l'),
                                'close': item.get('c'),
                                'volume': item.get('v', 0)
                            })
                    return standard_results
                else:
                    return []
            else:
                self.stats['vendor_stats']['polygon']['errors'] += 1
                return []
                
        except Exception as e:
            logger.error(f"❌ Polygon error for {symbol}: {e}")
            self.stats['vendor_stats']['polygon']['errors'] += 1
            return []

    def download_tiingo_data(self, symbol: str, start_date: date, end_date: date):
        """Download daily prices from Tiingo API."""
        config = self.vendors['tiingo']
        url = f"{config['base_url']}/{symbol}/prices"
        params = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'token': config['api_key']
        }
        
        try:
            response = requests.get(url, params=params)
            self.stats['vendor_stats']['tiingo']['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                
                # Convert Tiingo format to standard format
                standard_results = []
                for item in data:
                    if 'date' in item:
                        date_val = datetime.fromisoformat(item['date'].replace('Z', '+00:00')).date()
                        standard_results.append({
                            'date': date_val,
                            'open': item.get('open'),
                            'high': item.get('high'),
                            'low': item.get('low'),
                            'close': item.get('close'),
                            'volume': item.get('volume', 0)
                        })
                return standard_results
            else:
                self.stats['vendor_stats']['tiingo']['errors'] += 1
                return []
                
        except Exception as e:
            logger.error(f"❌ Tiingo error for {symbol}: {e}")
            self.stats['vendor_stats']['tiingo']['errors'] += 1
            return []

    def download_eodhd_data(self, symbol: str, start_date: date, end_date: date):
        """Download daily prices from EODHD API."""
        config = self.vendors['eodhd']
        url = f"{config['base_url']}/{symbol}.US"
        params = {
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'period': 'd',
            'api_token': config['api_key'],
            'fmt': 'json'
        }
        
        try:
            response = requests.get(url, params=params)
            self.stats['vendor_stats']['eodhd']['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                
                # Convert EODHD format to standard format
                standard_results = []
                for item in data:
                    if 'date' in item:
                        date_val = datetime.strptime(item['date'], '%Y-%m-%d').date()
                        standard_results.append({
                            'date': date_val,
                            'open': item.get('open'),
                            'high': item.get('high'),
                            'low': item.get('low'),
                            'close': item.get('close'),
                            'volume': item.get('volume', 0)
                        })
                return standard_results
            else:
                self.stats['vendor_stats']['eodhd']['errors'] += 1
                return []
                
        except Exception as e:
            logger.error(f"❌ EODHD error for {symbol}: {e}")
            self.stats['vendor_stats']['eodhd']['errors'] += 1
            return []

    async def insert_vendor_data_idempotent(self, conn, vendor_name: str, instrument_id: int, symbol: str, data_list):
        """Insert vendor data with idempotent UPSERT operations."""
        if not data_list:
            return 0
        
        # Prepare data for insertion
        rows = []
        for data in data_list:
            try:
                rows.append((
                    data['date'],
                    symbol,
                    data.get('open'),
                    data.get('high'),
                    data.get('low'),
                    data.get('close'),
                    data.get('volume', 0),
                    instrument_id
                ))
            except Exception as e:
                logger.error(f"❌ Error processing {vendor_name} record for {symbol}: {e}")
                continue
        
        if not rows:
            return 0
        
        # Insert with idempotent UPSERT
        env = os.getenv('ENV_TYPE', 'intg').lower()
        table_prefix = 'intg_' if env == 'intg' else 'dev_'
        table_name = f"{table_prefix}daily_prices_{self.vendors[vendor_name]['table_suffix']}"
        
        try:
            result = await conn.executemany(f"""
                INSERT INTO {table_name}
                (date, symbol, open, high, low, close, volume, market_cap, instrument_id, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8, NOW(), NOW())
                ON CONFLICT (date, instrument_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    updated_at = NOW()
            """, rows)
            
            self.stats['vendor_stats'][vendor_name]['total_records'] += len(rows)
            return len(rows)
            
        except Exception as e:
            logger.error(f"❌ {vendor_name} database error for {symbol}: {e}")
            self.stats['vendor_stats'][vendor_name]['errors'] += 1
            return 0

    async def backfill_instrument_vendor(self, conn, vendor_name: str, instrument, start_date: date, end_date: date):
        """Backfill single instrument for specific vendor."""
        instrument_id = instrument['id']
        symbol = instrument['symbol']
        
        try:
            # Get missing trading days as date ranges for this vendor
            missing_ranges = await self.get_missing_trading_days_vendor(
                conn, vendor_name, instrument_id, symbol, start_date, end_date
            )
            
            if not missing_ranges:
                self.stats['vendor_stats'][vendor_name]['skipped_instruments'] += 1
                return 0
            
            self.stats['vendor_stats'][vendor_name]['missing_date_ranges'] += len(missing_ranges)
            
            # Download function mapping
            download_functions = {
                'polygon': self.download_polygon_data,
                'tiingo': self.download_tiingo_data,
                'eodhd': self.download_eodhd_data
            }
            
            total_inserted = 0
            
            # Process each missing date range
            for range_start, range_end in missing_ranges:
                # Download data for this specific range
                data_list = download_functions[vendor_name](symbol, range_start, range_end)
                
                if data_list:
                    # Insert data idempotently
                    inserted_count = await self.insert_vendor_data_idempotent(
                        conn, vendor_name, instrument_id, symbol, data_list
                    )
                    total_inserted += inserted_count
                
                # Rate limiting delay
                time.sleep(self.vendors[vendor_name]['rate_limit'])
            
            if total_inserted > 0:
                logger.info(f"✅ {vendor_name.upper()}-{symbol}: {total_inserted} records")
                self.stats['vendor_stats'][vendor_name]['processed_instruments'] += 1
            else:
                self.stats['vendor_stats'][vendor_name]['skipped_instruments'] += 1
            
            return total_inserted
            
        except Exception as e:
            logger.error(f"❌ {vendor_name.upper()}-{symbol}: {e}")
            self.stats['vendor_stats'][vendor_name]['errors'] += 1
            return 0

    async def run_multi_vendor_backfill(self, start_date: date, end_date: date, limit=None):
        """Run optimized backfill for all available vendors."""
        logger.info("🚀 Starting Multi-Vendor Optimized Backfill...")
        logger.info(f"📅 Date range: {start_date} to {end_date}")
        logger.info(f"🎯 Strategy: Missing trading days only (holidays excluded)")
        
        available_vendors = [name for name, config in self.vendors.items() if config['api_key']]
        logger.info(f"🔧 Available vendors: {available_vendors}")
        
        conn = await self.get_database_connection()
        
        try:
            # Ensure all tables exist
            await self.ensure_tables_exist(conn)
            
            # Get instruments to process
            instruments = await self.get_instruments_for_backfill(conn, limit)
            
            if not instruments:
                logger.warning("❌ No instruments found for backfill")
                return
            
            # Filter for specific symbols if TARGET_SYMBOLS is provided
            target_symbols = os.getenv('TARGET_SYMBOLS')
            if target_symbols:
                target_list = [s.strip().upper() for s in target_symbols.split(',')]
                instruments = [inst for inst in instruments if inst['symbol'].upper() in target_list]
                logger.info(f"🎯 Filtering to target symbols: {target_list}")
            
            logger.info(f"📊 Processing {len(instruments)} instruments across {len(available_vendors)} vendors")
            
            # Process each instrument across all vendors
            for i, instrument in enumerate(instruments, 1):
                try:
                    # Process each vendor for this instrument
                    for vendor_name in available_vendors:
                        await self.backfill_instrument_vendor(conn, vendor_name, instrument, start_date, end_date)
                    
                    # Progress logging
                    if i % 10 == 0 or i == len(instruments):
                        progress = (i / len(instruments)) * 100
                        total_records = sum(stats['total_records'] for stats in self.stats['vendor_stats'].values())
                        total_ranges = sum(stats['missing_date_ranges'] for stats in self.stats['vendor_stats'].values())
                        logger.info(f"📊 Progress: {i:,}/{len(instruments):,} ({progress:.1f}%) - "
                                  f"{total_records:,} records, {total_ranges} ranges")
                        
                except Exception as e:
                    logger.error(f"❌ Critical error processing {instrument.get('symbol', 'unknown')}: {e}")
                    continue
            
        finally:
            await conn.close()
    
    def log_final_summary(self):
        """Log comprehensive multi-vendor optimization summary."""
        logger.info("=" * 80)
        logger.info("🎉 MULTI-VENDOR OPTIMIZED BACKFILL COMPLETE")
        logger.info("=" * 80)
        logger.info(f"📊 OVERALL SUMMARY:")
        logger.info(f"  Total Instruments: {self.stats['total_instruments']:,}")
        logger.info(f"  Active Vendors: {self.stats['total_vendors']}")
        logger.info("")
        
        for vendor_name, stats in self.stats['vendor_stats'].items():
            logger.info(f"🔧 {vendor_name.upper()} VENDOR:")
            logger.info(f"  Processed Instruments: {stats['processed_instruments']:,}")
            logger.info(f"  Skipped Instruments: {stats['skipped_instruments']:,}")
            logger.info(f"  Total Records: {stats['total_records']:,}")
            logger.info(f"  API Calls: {stats['api_calls']:,}")
            logger.info(f"  Missing Ranges: {stats['missing_date_ranges']:,}")
            logger.info(f"  Errors: {stats['errors']:,}")
            
            if stats['processed_instruments'] > 0:
                success_rate = (stats['processed_instruments'] / 
                              (stats['processed_instruments'] + stats['skipped_instruments'])) * 100
                logger.info(f"  Success Rate: {success_rate:.1f}%")
            logger.info("")
        
        # Total across all vendors
        total_records = sum(stats['total_records'] for stats in self.stats['vendor_stats'].values())
        total_api_calls = sum(stats['api_calls'] for stats in self.stats['vendor_stats'].values())
        total_errors = sum(stats['errors'] for stats in self.stats['vendor_stats'].values())
        
        logger.info(f"🎯 OPTIMIZATION TOTALS:")
        logger.info(f"  Total Records Inserted: {total_records:,}")
        logger.info(f"  Total API Calls: {total_api_calls:,}")
        logger.info(f"  Total Errors: {total_errors:,}")
        logger.info("=" * 80)


async def main():
    parser = argparse.ArgumentParser(description="Multi-vendor optimized daily price backfill")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=int(os.getenv('LIMIT', '0')) or None, 
                       help='Limit number of instruments to process')
    parser.add_argument('--years', type=int, default=int(os.getenv('YEARS', '3')), 
                       help='Number of years of historical data to fetch (default: 3)')
    parser.add_argument('--start_date', type=str, default=None, 
                       help='Start date (YYYY-MM-DD), overrides --years')
    parser.add_argument('--end_date', type=str, default=None, 
                       help='End date (YYYY-MM-DD), defaults to today')
    
    args = parser.parse_args()
    
    # Set up logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    try:
        # Calculate date range (limit to 2020+ for API compatibility)
        if args.start_date:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        else:
            start_date = max(
                (datetime.now() - timedelta(days=365 * args.years)).date(),
                date(2020, 1, 1)  # Don't go before 2020
            )
        
        if args.end_date:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        else:
            end_date = datetime.now().date()
        
        logger.info(f"📅 Multi-vendor backfill from {start_date} to {end_date}")
        
        # Initialize multi-vendor backfiller
        backfiller = MultiVendorOptimizedBackfiller()
        
        # Check if at least one vendor API key is available
        available_vendors = [name for name, config in backfiller.vendors.items() if config['api_key']]
        if not available_vendors:
            logger.error("❌ No vendor API keys found. Please set POLYGON_API_KEY, TIINGO_API_KEY, or EODHD_API_KEY")
            sys.exit(1)
        
        # Run multi-vendor backfill
        await backfiller.run_multi_vendor_backfill(start_date, end_date, limit=args.limit)
        
        # Log final summary
        backfiller.log_final_summary()
        
        logger.info("✅ Multi-vendor optimized backfill complete")
        
    except Exception as e:
        logger.error(f"❌ Failed to run multi-vendor backfill: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())