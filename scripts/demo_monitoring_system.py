#!/usr/bin/env python3
"""
Demonstration of ATS Daily Prices Backfill Monitoring System

Shows the complete monitoring workflow with Prometheus metrics collection
and Pushgateway integration for batch job tracking.
"""

import sys
import asyncio
import logging
from datetime import datetime
import time
import os

# Add src to path
sys.path.insert(0, 'src')

# Set up Prometheus gateway
os.environ['PROMETHEUS_GATEWAY'] = 'localhost:9091'

from infrastructure.vendor.eodhd.services.eodhd_database_sync import sync_vendor_daily_price_polygon

async def demo_monitoring():
    """Demonstrate the monitoring system with a limited sync operation."""

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    print("🎯 ATS Daily Prices Backfill Monitoring Demo")
    print("=" * 60)
    print()

    # Demo configuration
    source_config = {
        'host': 'localhost',
        'port': 3432,
        'user': 'postgres',
        'password': 'dev_password',
        'database': 'dev_db'
    }

    target_config = {
        'host': 'localhost',
        'port': 4432,
        'user': 'postgres',
        'password': 'intg_password',
        'database': 'intg_db'
    }

    print("📊 Starting monitored database sync...")
    print(f"   Source: DEV database (port {source_config['port']})")
    print(f"   Target: INTG database (port {target_config['port']})")
    print(f"   Metrics: Pushing to Pushgateway at {os.environ['PROMETHEUS_GATEWAY']}")
    print()

    start_time = time.time()

    try:
        # Run EODHD sync with monitoring
        result = await sync_vendor_daily_price_polygon('eodhd', source_config, target_config)

        duration = time.time() - start_time

        print("✅ SYNC COMPLETE!")
        print("=" * 60)
        print(f"🎯 Vendor: EODHD")
        print(f"📊 Records Processed: {result['records_processed']:,}")
        print(f"📈 Records Added: {result['records_added']:,}")
        print(f"🔄 Duplicates Skipped: {result['duplicates_skipped']:,}")
        print(f"🎯 Unique Symbols: {result.get('unique_symbols_processed', 'N/A')}")
        print(f"⚡ Success Rate: {result['sync_success_rate']:.1f}%")
        print(f"⏱️  Duration: {duration:.1f} seconds")
        print(f"📊 Average Rate: {result['average_rate']:,.0f} records/second")
        print()

        print("📡 Prometheus Metrics:")
        print("   ✅ ats_daily_price_polygon_sync_symbols_processed_total")
        print("   ✅ ats_daily_price_polygon_sync_prices_processed_total")
        print("   ✅ ats_daily_price_polygon_sync_duration_seconds")
        print("   ✅ ats_daily_price_polygon_sync_success_rate")
        print()

        print("🔗 Monitoring Endpoints:")
        print("   📊 Pushgateway: http://localhost:9091/metrics")
        print("   📈 Grafana Dashboard: config/grafana/ats-batch-jobs-dashboard.json")
        print()

        if result['success']:
            print("🎉 Monitoring demonstration completed successfully!")
        else:
            print(f"❌ Sync failed: {result.get('error', 'Unknown error')}")

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(demo_monitoring())