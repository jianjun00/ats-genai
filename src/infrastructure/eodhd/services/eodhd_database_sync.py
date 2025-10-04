#!/usr/bin/env python3
"""
Vendor Database Synchronization Service

Provides incremental synchronization of daily price data between databases.
Supports EODHD and Tiingo vendors.
Uses PostgreSQL ON CONFLICT DO NOTHING for safe incremental updates.
"""

import asyncpg
import time
import os
from typing import Dict, Any
import logging

# Prometheus metrics support
try:
    from prometheus_client import Counter, Gauge, Histogram, push_to_gateway
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logging.warning("prometheus_client not installed. Metrics will not be pushed to Prometheus.")

logger = logging.getLogger(__name__)


class VendorDatabaseSync:
    """Service for syncing vendor daily prices between databases (EODHD, Tiingo, etc.)."""

    def __init__(self, source_config: Dict[str, Any], target_config: Dict[str, Any]):
        self.source_config = source_config
        self.target_config = target_config
        self.batch_size = 10000

        # Initialize Prometheus metrics if available
        if PROMETHEUS_AVAILABLE:
            self.sync_symbols_processed = Counter(
                'ats_daily_prices_sync_symbols_processed_total',
                'Total number of symbols processed during daily prices sync',
                ['vendor', 'source_env', 'target_env']
            )
            self.sync_prices_processed = Counter(
                'ats_daily_prices_sync_prices_processed_total',
                'Total number of price records processed during sync',
                ['vendor', 'source_env', 'target_env']
            )
            self.sync_duration_seconds = Histogram(
                'ats_daily_prices_sync_duration_seconds',
                'Duration of daily prices sync operations in seconds',
                ['vendor', 'source_env', 'target_env']
            )
            self.sync_success_rate = Gauge(
                'ats_daily_prices_sync_success_rate',
                'Success rate of daily prices sync operations (0.0 to 1.0)',
                ['vendor', 'source_env', 'target_env']
            )
        else:
            self.sync_symbols_processed = None
            self.sync_prices_processed = None
            self.sync_duration_seconds = None
            self.sync_success_rate = None

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""

    async def sync_daily_prices_incremental(self, vendor: str = 'eodhd') -> Dict[str, Any]:
        """Incremental sync using direct INSERT with ON CONFLICT DO NOTHING."""

        logger.info(f"🚀 Starting incremental sync of daily_prices_{vendor}")
        logger.info("🛡️  SAFE MODE: Using ON CONFLICT DO NOTHING (no deletions)")
        start_time = time.time()

        # Table names based on vendor
        source_table = f"dev_daily_price_{vendor}"
        target_table = f"intg_daily_price_{vendor}"

        # Connect to both databases
        source_conn = await asyncpg.connect(**self.source_config)
        target_conn = await asyncpg.connect(**self.target_config)

        try:
            # Get initial counts
            source_count = await source_conn.fetchval(f"SELECT COUNT(*) FROM {source_table}")
            target_count_before = await target_conn.fetchval(f"SELECT COUNT(*) FROM {target_table}")

            logger.info(f"📊 Source (dev): {source_count:,} records")
            logger.info(f"📊 Target (intg) before: {target_count_before:,} records")
            logger.info(f"📊 Records to process: {source_count:,}")

            # Check for orphaned records
            orphaned_count = await source_conn.fetchval(f"""
                SELECT COUNT(*) FROM {source_table}
                WHERE instrument_id NOT IN (SELECT id FROM dev_instrument)
            """)

            logger.info(f"⚠️  Orphaned records (will be skipped): {orphaned_count:,}")
            logger.info(f"✅ Valid records to sync: {source_count - orphaned_count:,}")

            # Start batch processing
            logger.info(f"📦 Processing in batches of {self.batch_size:,}...")

            total_processed = 0
            total_inserted = 0
            offset = 0

            while True:
                # Fetch batch from source with valid instruments only
                if vendor == 'eodhd':
                    batch_data = await source_conn.fetch(f"""
                        SELECT date, symbol, open, high, low, close, adjusted_close, volume, instrument_id
                        FROM {source_table}
                        WHERE instrument_id IN (SELECT id FROM dev_instrument)
                        ORDER BY date, instrument_id
                        LIMIT $1 OFFSET $2
                    """, self.batch_size, offset)
                elif vendor == 'polygon':
                    batch_data = await source_conn.fetch(f"""
                        SELECT date, symbol, open, high, low, close, volume, market_cap, instrument_id
                        FROM {source_table}
                        WHERE instrument_id IN (SELECT id FROM dev_instrument)
                        ORDER BY date, instrument_id
                        LIMIT $1 OFFSET $2
                    """, self.batch_size, offset)
                else:  # tiingo and other vendors
                    batch_data = await source_conn.fetch(f"""
                        SELECT date, symbol, open, high, low, close, volume, instrument_id
                        FROM {source_table}
                        WHERE instrument_id IN (SELECT id FROM dev_instrument)
                        ORDER BY date, instrument_id
                        LIMIT $1 OFFSET $2
                    """, self.batch_size, offset)

                if not batch_data:
                    break

                # Insert batch with ON CONFLICT DO NOTHING (safe incremental)
                if vendor == 'eodhd':
                    result = await target_conn.executemany(f"""
                        INSERT INTO {target_table}
                        (date, symbol, open, high, low, close, adjusted_close, volume, instrument_id)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (date, instrument_id) DO NOTHING
                    """, [
                        (row['date'], row['symbol'], row['open'], row['high'],
                         row['low'], row['close'], row['adjusted_close'], row['volume'], row['instrument_id'])
                        for row in batch_data
                    ])
                elif vendor == 'polygon':
                    result = await target_conn.executemany(f"""
                        INSERT INTO {target_table}
                        (date, symbol, open, high, low, close, volume, market_cap, instrument_id)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (date, instrument_id) DO NOTHING
                    """, [
                        (row['date'], row['symbol'], row['open'], row['high'],
                         row['low'], row['close'], row['volume'], row['market_cap'], row['instrument_id'])
                        for row in batch_data
                    ])
                else:  # tiingo and other vendors
                    result = await target_conn.executemany(f"""
                        INSERT INTO {target_table}
                        (date, symbol, open, high, low, close, volume, instrument_id)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (date, instrument_id) DO NOTHING
                    """, [
                        (row['date'], row['symbol'], row['open'], row['high'],
                         row['low'], row['close'], row['volume'], row['instrument_id'])
                        for row in batch_data
                    ])

                total_processed += len(batch_data)
                offset += len(batch_data)
                elapsed = time.time() - start_time
                rate = total_processed / elapsed if elapsed > 0 else 0

                logger.info(f"✅ Batch {offset//self.batch_size}: {len(batch_data):,} records | "
                          f"Total: {total_processed:,} | "
                          f"Rate: {rate:.0f} rec/sec")

                if len(batch_data) < self.batch_size:
                    break

            # Final verification
            target_count_after = await target_conn.fetchval(f"SELECT COUNT(*) FROM {target_table}")
            records_added = target_count_after - target_count_before

            elapsed_time = time.time() - start_time

            # Calculate unique symbols processed
            symbols_query = f"""
                SELECT COUNT(DISTINCT symbol) FROM {source_table}
                WHERE instrument_id IN (SELECT id FROM dev_instrument)
            """
            unique_symbols = await source_conn.fetchval(symbols_query)

            # Calculate success rate
            success_rate = (records_added / max(1, source_count - target_count_before - orphaned_count))

            # Update Prometheus metrics if available
            if PROMETHEUS_AVAILABLE:
                source_env = 'dev' if self.source_config.get('port') == 3432 else 'intg'
                target_env = 'intg' if self.target_config.get('port') == 4432 else 'dev'

                self.sync_symbols_processed.labels(
                    vendor=vendor,
                    source_env=source_env,
                    target_env=target_env
                ).inc(unique_symbols)

                self.sync_prices_processed.labels(
                    vendor=vendor,
                    source_env=source_env,
                    target_env=target_env
                ).inc(records_added)

                self.sync_duration_seconds.labels(
                    vendor=vendor,
                    source_env=source_env,
                    target_env=target_env
                ).observe(elapsed_time)

                self.sync_success_rate.labels(
                    vendor=vendor,
                    source_env=source_env,
                    target_env=target_env
                ).set(success_rate)

                # Push metrics to Prometheus gateway if configured
                try:
                    gateway = os.getenv('PROMETHEUS_GATEWAY', 'localhost:9091')
                    job_name = f'daily-prices-sync-{vendor}'
                    push_to_gateway(gateway, job=job_name, registry=None,
                                  grouping_key={'vendor': vendor})
                    logger.info(f"📊 Pushed metrics to Prometheus gateway: {gateway}")
                except Exception as e:
                    logger.debug(f"Could not push to Prometheus gateway: {e}")

            # Return results
            return {
                'success': True,
                'records_processed': total_processed,
                'records_added': records_added,
                'duplicates_skipped': total_processed - records_added,
                'target_count_before': target_count_before,
                'target_count_after': target_count_after,
                'remaining_gap': source_count - target_count_after,
                'orphaned_records': orphaned_count,
                'unique_symbols_processed': unique_symbols,
                'total_time': elapsed_time,
                'average_rate': total_processed/elapsed_time if elapsed_time > 0 else 0,
                'sync_success_rate': success_rate * 100
            }

        except Exception as e:
            logger.error(f"❌ Error during sync: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        finally:
            await source_conn.close()
            await target_conn.close()


# Convenience functions for simple usage
async def sync_eodhd_daily_prices(
    source_config: Dict[str, Any] = None,
    target_config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Convenience function to sync EODHD daily prices.
    """
    return await sync_vendor_daily_prices('eodhd', source_config, target_config)

async def sync_tiingo_daily_prices(
    source_config: Dict[str, Any] = None,
    target_config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Convenience function to sync Tiingo daily prices.
    """
    return await sync_vendor_daily_prices('tiingo', source_config, target_config)

async def sync_polygon_daily_prices(
    source_config: Dict[str, Any] = None,
    target_config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Convenience function to sync Polygon daily prices.
    """
    return await sync_vendor_daily_prices('polygon', source_config, target_config)

async def sync_vendor_daily_prices(
    vendor: str,
    source_config: Dict[str, Any] = None,
    target_config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Convenience function to sync vendor daily prices.

    Args:
        vendor: Vendor name (eodhd, tiingo, etc.)
        source_config: Source database configuration (defaults to dev)
        target_config: Target database configuration (defaults to intg)

    Returns:
        Sync results dictionary
    """
    if not source_config:
        source_config = {
            'host': 'localhost',
            'port': 3432,
            'user': 'postgres',
            'password': 'dev_password',
            'database': 'dev_db'
        }

    if not target_config:
        target_config = {
            'host': 'localhost',
            'port': 4432,
            'user': 'postgres',
            'password': 'intg_password',
            'database': 'intg_db'
        }

    async with VendorDatabaseSync(source_config, target_config) as sync_service:
        return await sync_service.sync_daily_prices_incremental(vendor)