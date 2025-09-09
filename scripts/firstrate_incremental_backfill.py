#!/usr/bin/env python3
"""
FirstRate Incremental Backfill CLI

Uses the enhanced FirstRateMinuteAdapter for incremental backfills with file storage.
Consolidates functionality instead of duplicating code.
"""

import asyncio
import asyncpg
import argparse
import sys
from datetime import datetime

# Add src to path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from infrastructure.vendor.firstrate.adapters.firstrate_minute_adapter import FirstRateMinuteAdapter


DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 3432,
    'user': 'postgres', 
    'password': 'dev_password',
    'database': 'dev_db'
}

async def get_sample_instruments(limit: int = 5):
    """Get sample instruments from database."""
    conn = await asyncpg.connect(**DATABASE_CONFIG)
    try:
        rows = await conn.fetch(
            "SELECT symbol FROM dev_instruments WHERE symbol ~ '^[A-Z]+$' AND LENGTH(symbol) <= 4 ORDER BY symbol LIMIT $1", 
            limit
        )
        return [row['symbol'] for row in rows]
    finally:
        await conn.close()

async def main():
    parser = argparse.ArgumentParser(description='FirstRate Incremental Backfill CLI')
    parser.add_argument('--limit', type=int, default=5, help='Number of sample instruments')
    parser.add_argument('--symbol', type=str, help='Specific symbol to process')
    parser.add_argument('--days', type=int, default=30, help='Days back to fetch')
    parser.add_argument('--output', type=str, default='/mnt/d/ats-data/minute-bars/firstrate', help='Output directory')
    args = parser.parse_args()
    
    print("🚀 Starting FirstRate Incremental Backfill")
    start_time = datetime.now()
    
    # Get symbols
    if args.symbol:
        symbols = [args.symbol]
    else:
        symbols = await get_sample_instruments(args.limit)
    
    print(f"📋 Processing {len(symbols)} symbols: {', '.join(symbols)}")
    
    # Run backfill using enhanced adapter
    async with FirstRateMinuteAdapter() as adapter:
        results = await adapter.incremental_backfill_to_files(
            symbols=symbols,
            days_back=args.days,
            output_path=args.output
        )
    
    # Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n🎉 Backfill Complete!")
    print(f"⏱️  Time: {elapsed:.1f}s")
    print(f"✅ Symbols processed: {len(results['symbols_processed'])}")
    print(f"📄 Files written: {results['files_written']}")
    print(f"⏭️  Files skipped: {results['files_skipped']}")

if __name__ == "__main__":
    asyncio.run(main())