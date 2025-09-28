#!/usr/bin/env python3
"""
Tiingo Data Collector for INTG Environment

Collects recent daily price data from Tiingo API and stores it in the integration database.
This script fixes the gap in Tiingo data ingestion by directly calling the Tiingo API.

Usage:
    python3 scripts/tiingo_data_collector_intg.py [--days DAYS] [--symbols SYMBOLS] [--debug]

Examples:
    python3 scripts/tiingo_data_collector_intg.py --days 7 --symbols 50
    python3 scripts/tiingo_data_collector_intg.py --debug
"""
import asyncio
import asyncpg
import requests
import os
import argparse
from datetime import datetime, date, timedelta
from typing import List, Dict, Tuple

async def collect_tiingo_data(lookback_days: int = 10, max_symbols: int = 100, debug: bool = False) -> Dict:
    """Collect Tiingo data and store in INTG database."""

    if debug:
        print("🔄 Starting Tiingo data collection (DEBUG MODE)...")

    # Configuration
    api_key = "5f40b4f36e171405746304ec0e5a6f3aa9ca77e5"
    db_url = "postgresql://postgres:intg_password@ats-intg-postgres:5432/intg_db"

    # Date range
    end_date = date.today()
    start_date = end_date - timedelta(days=lookback_days)

    if debug:
        print(f"📅 Collecting data from {start_date} to {end_date}")
        print(f"🎯 Max symbols: {max_symbols}")

    results = {
        'total_symbols_processed': 0,
        'total_records_inserted': 0,
        'total_records_updated': 0,
        'total_errors': 0,
        'symbols_with_data': [],
        'symbols_without_data': [],
        'error_symbols': []
    }

    conn = await asyncpg.connect(db_url)
    if debug:
        print("✅ Database connected")

    # Get active symbols
    symbols_result = await conn.fetch(f"""
        SELECT DISTINCT symbol, id
        FROM intg_instrument
        WHERE active = true
          AND symbol IS NOT NULL
          AND symbol ~ '^[A-Z]{{1,5}}$'
        ORDER BY symbol
        LIMIT {max_symbols}
    """)

    symbols = [(row['symbol'], row['id']) for row in symbols_result]
    if debug:
        print(f"📊 Found {len(symbols)} active symbols to process")

    for i, (symbol, instrument_id) in enumerate(symbols):
        if debug:
            print(f"🔄 [{i+1}/{len(symbols)}] Processing {symbol} (ID: {instrument_id})")

        # Fetch data from Tiingo API
        url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        params = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'token': api_key
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data:
            results['symbols_without_data'].append(symbol)
            if debug:
                print(f"⚠️ No data returned for {symbol}")
            continue

        results['symbols_with_data'].append(symbol)
        if debug:
            print(f"📈 Fetched {len(data)} records for {symbol}")

        # Process each price record
        symbol_inserted = 0
        symbol_updated = 0

        for record in data:
            # Parse date
            record_date = datetime.fromisoformat(record['date'].replace('Z', '+00:00')).date()

            # Check if record exists
            existing = await conn.fetchrow(
                "SELECT instrument_id FROM intg_daily_price_tiingo WHERE instrument_id = $1 AND date = $2",
                instrument_id, record_date
            )

            if existing:
                # Update existing record
                await conn.execute("""
                    UPDATE intg_daily_price_tiingo
                    SET open = $3, high = $4, low = $5, close = $6, volume = $7,
                        adjusted_close = $8, symbol = $9, updated_at = CURRENT_TIMESTAMP
                    WHERE instrument_id = $1 AND date = $2
                """, instrument_id, record_date,
                    float(record['open']), float(record['high']), float(record['low']),
                    float(record['close']), int(record['volume']), float(record['adjClose']), symbol)
                symbol_updated += 1
            else:
                # Insert new record
                await conn.execute("""
                    INSERT INTO intg_daily_price_tiingo
                    (instrument_id, date, open, high, low, close, volume, adjusted_close, symbol, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (instrument_id, date) DO NOTHING
                """, instrument_id, record_date,
                    float(record['open']), float(record['high']), float(record['low']),
                    float(record['close']), int(record['volume']), float(record['adjClose']), symbol)
                symbol_inserted += 1

        results['total_records_inserted'] += symbol_inserted
        results['total_records_updated'] += symbol_updated
        results['total_symbols_processed'] += 1

        if debug:
            print(f"✅ {symbol}: {symbol_inserted} inserted, {symbol_updated} updated")

        # Rate limiting - 1 request per second
        await asyncio.sleep(1.1)

    recent_count = await conn.fetchval("""
        SELECT COUNT(*)
        FROM intg_daily_price_tiingo
        WHERE date >= CURRENT_DATE - INTERVAL '7 days'
    """)

    latest_date = await conn.fetchval("""
        SELECT MAX(date)
        FROM intg_daily_price_tiingo
    """)

    results['recent_records_count'] = recent_count
    results['latest_data_date'] = latest_date.isoformat() if latest_date else None

    return results

def print_summary(results: Dict, debug: bool = False):
    """Print collection summary."""
    print(f"\n📊 TIINGO DATA COLLECTION SUMMARY")
    print(f"="*50)
    print(f"✅ Symbols processed: {results['total_symbols_processed']}")
    print(f"📈 Records inserted: {results['total_records_inserted']}")
    print(f"🔄 Records updated: {results['total_records_updated']}")
    print(f"❌ Errors: {results['total_errors']}")

    if 'recent_records_count' in results:
        print(f"📊 Recent records (7 days): {results['recent_records_count']}")

    if 'latest_data_date' in results and results['latest_data_date']:
        print(f"📅 Latest data date: {results['latest_data_date']}")

    if debug:
        print(f"\n📈 Symbols with data ({len(results['symbols_with_data'])}): {results['symbols_with_data'][:10]}")
        if len(results['symbols_with_data']) > 10:
            print(f"... and {len(results['symbols_with_data'])-10} more")

        if results['symbols_without_data']:
            print(f"⚠️ Symbols without data ({len(results['symbols_without_data'])}): {results['symbols_without_data'][:5]}")

        if results['error_symbols']:
            print(f"❌ Error symbols ({len(results['error_symbols'])}): {results['error_symbols'][:5]}")

    # Status message
    if results['total_records_inserted'] > 0 or results['total_records_updated'] > 0:
        print(f"\n✅ SUCCESS: Tiingo data collection completed successfully")
        print(f"🎯 The Grafana dashboard should now show updated Tiingo data")
    else:
        print(f"\n⚠️ WARNING: No records were inserted or updated")

async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Tiingo Data Collector for INTG Environment')
    parser.add_argument('--days', type=int, default=10, help='Number of days to look back (default: 10)')
    parser.add_argument('--symbols', type=int, default=100, help='Maximum number of symbols to process (default: 100)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    print("🚀 ATS-INTG TIINGO DATA COLLECTOR")
    print("="*50)
    print(f"Lookback days: {args.days}")
    print(f"Max symbols: {args.symbols}")
    print(f"Debug mode: {'ON' if args.debug else 'OFF'}")
    print()

    # Run collection
    results = await collect_tiingo_data(
        lookback_days=args.days,
        max_symbols=args.symbols,
        debug=args.debug
    )

    # Print summary
    print_summary(results, debug=args.debug)

if __name__ == "__main__":
    asyncio.run(main())