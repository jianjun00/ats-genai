#!/usr/bin/env python3
"""
Test Script for Batch Job Metrics

Tests Prometheus metrics collection for daily prices backfill jobs.
Runs a small sync operation and pushes metrics to test the monitoring pipeline.
"""

import sys
import asyncio
import logging
from datetime import datetime
import os

# Add src to path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from infrastructure.vendor.eodhd.services.eodhd_database_sync import sync_vendor_daily_price_polygon

async def test_sync_metrics():
    """Test database sync with metrics collection."""

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    logger.info("🧪 Testing batch job metrics collection...")

    # Set up test environment variables
    os.environ['PROMETHEUS_GATEWAY'] = 'localhost:9091'  # Prometheus Pushgateway

    # Test EODHD sync with a small subset
    logger.info("📊 Testing EODHD database sync with metrics...")

    # Run a limited sync operation for testing
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

    # Run sync for EODHD with metrics collection
    result = await sync_vendor_daily_price_polygon('eodhd', source_config, target_config)

    if result['success']:
        logger.info("✅ Sync completed successfully!")
        logger.info(f"   Records processed: {result['records_processed']:,}")
        logger.info(f"   Records added: {result['records_added']:,}")
        logger.info(f"   Unique symbols: {result.get('unique_symbols_processed', 'N/A')}")
        logger.info(f"   Success rate: {result['sync_success_rate']:.1f}%")
        logger.info(f"   Duration: {result['total_time']:.1f} seconds")
    else:
        logger.error(f"❌ Sync failed: {result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    asyncio.run(test_sync_metrics())