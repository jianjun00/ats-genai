#!/usr/bin/env python3
"""
Vendor Database Sync CLI

Uses the VendorDatabaseSync service for incremental database synchronization.
Supports EODHD, Tiingo, and other vendors.
Consolidates functionality instead of duplicating code.
"""

import asyncio
import argparse
import sys
from datetime import datetime

# Add src to path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from infrastructure.vendor.eodhd.services.eodhd_database_sync import sync_vendor_daily_price_polygon


async def main():
    parser = argparse.ArgumentParser(description='Vendor Database Sync CLI')
    parser.add_argument('--vendor', type=str, default='eodhd', choices=['eodhd', 'tiingo', 'polygon'], help='Vendor to sync (eodhd, tiingo, polygon)')
    parser.add_argument('--source-port', type=int, default=3432, help='Source database port')
    parser.add_argument('--target-port', type=int, default=4432, help='Target database port')
    parser.add_argument('--source-db', type=str, default='dev_db', help='Source database name')
    parser.add_argument('--target-db', type=str, default='intg_db', help='Target database name')
    args = parser.parse_args()

    print(f"🚀 Starting {args.vendor.upper()} Database Synchronization")
    start_time = datetime.now()

    # Configure database connections
    source_config = {
        'host': 'localhost',
        'port': args.source_port,
        'user': 'postgres',
        'password': 'dev_password' if args.source_port == 3432 else 'intg_password',
        'database': args.source_db
    }

    target_config = {
        'host': 'localhost',
        'port': args.target_port,
        'user': 'postgres',
        'password': 'intg_password' if args.target_port == 4432 else 'dev_password',
        'database': args.target_db
    }

    # Run sync using service
    results = await sync_vendor_daily_price_polygon(args.vendor, source_config, target_config)

    # Summary
    elapsed = (datetime.now() - start_time).total_seconds()

    if results['success']:
        print(f"\n🎉 Database Sync Complete!")
        print(f"⏱️  Total time: {results['total_time']:.1f} seconds")
        print(f"📊 Records processed: {results['records_processed']:,}")
        print(f"📊 Target before: {results['target_count_before']:,}")
        print(f"📊 Target after: {results['target_count_after']:,}")
        print(f"📊 Records added: {results['records_added']:,}")
        print(f"📊 Duplicates skipped: {results['duplicates_skipped']:,}")
        print(f"📊 Remaining gap: {results['remaining_gap']:,} records")
        if results['orphaned_records'] > 0:
            print(f"   (Includes {results['orphaned_records']:,} orphaned records that cannot be synced)")
        print(f"✅ Sync success rate: {results['sync_success_rate']:.1f}%")
        print(f"📈 Average rate: {results['average_rate']:.0f} records/second")
    else:
        print(f"\n❌ Database Sync Failed!")
        print(f"Error: {results.get('error', 'Unknown error')}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())