#!/usr/bin/env python3
"""
Detect Missing Minute Bars

Identifies gaps in minute bar collection across vendors and symbols.
Provides comprehensive analysis of missing data patterns.
"""

import asyncio
import asyncpg
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any

async def detect_missing_minute_bars():
    """Detect missing minute bars across all vendors."""

    # Database connection
    conn = await asyncpg.connect(
        host='localhost',
        port=4432,
        user='postgres',
        password='intg_password',
        database='intg_db'
    )

    print("🔍 MISSING MINUTE BAR DETECTION REPORT")
    print("=" * 60)

    vendors_tables = [
        ('Tiingo', 'intg_one_minute_live_tiingo'),
        ('Polygon', 'intg_one_minute_live_polygon')
    ]

    symbols = ['AAPL', 'TSLA']

    for vendor, table_name in vendors_tables:
        print(f"\n📊 {vendor} Missing Bar Analysis:")
        print("-" * 40)

        for symbol in symbols:
            # Check if table exists
            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = $1
                )
            """, table_name)

            if not table_exists:
                print(f"  ❌ {symbol}: Table {table_name} does not exist")
                continue

            # Get minute bar gaps for last 24 hours during market hours
            gap_query = f"""
            WITH market_minutes AS (
                -- Generate expected minute timestamps for market hours (9:30 AM - 4:00 PM EST)
                SELECT
                    generate_series(
                        DATE_TRUNC('day', NOW() - INTERVAL '1 day') + INTERVAL '13:30', -- 9:30 AM EST in UTC
                        DATE_TRUNC('day', NOW() - INTERVAL '1 day') + INTERVAL '20:00', -- 4:00 PM EST in UTC
                        INTERVAL '1 minute'
                    ) as expected_timestamp
                WHERE EXTRACT(DOW FROM (NOW() - INTERVAL '1 day')) BETWEEN 1 AND 5 -- Weekdays only
            ),
            actual_data AS (
                SELECT DISTINCT DATE_TRUNC('minute', timestamp) as minute_timestamp
                FROM {table_name}
                WHERE symbol = $1
                  AND timestamp >= NOW() - INTERVAL '1 day'
            )
            SELECT
                COUNT(*) as missing_minutes,
                MIN(expected_timestamp) as first_missing,
                MAX(expected_timestamp) as last_missing
            FROM market_minutes m
            LEFT JOIN actual_data a ON m.expected_timestamp = a.minute_timestamp
            WHERE a.minute_timestamp IS NULL
            """

            try:
                gap_result = await conn.fetchrow(gap_query, symbol)
                missing_count = gap_result['missing_minutes']

                if missing_count > 0:
                    print(f"  ⚠️  {symbol}: {missing_count:,} missing minutes")
                    print(f"      First gap: {gap_result['first_missing']}")
                    print(f"      Last gap: {gap_result['last_missing']}")
                else:
                    print(f"  ✅ {symbol}: No missing minutes detected")

                # Show data freshness
                freshness_query = f"""
                SELECT
                    COUNT(*) as total_records,
                    MAX(timestamp) as latest_timestamp,
                    EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))/60 as minutes_since_last
                FROM {table_name}
                WHERE symbol = $1 AND timestamp >= NOW() - INTERVAL '24 hours'
                """

                freshness = await conn.fetchrow(freshness_query, symbol)
                minutes_behind = freshness['minutes_since_last'] or 0

                if minutes_behind > 5:  # More than 5 minutes behind
                    print(f"      🚨 Data stale: {minutes_behind:.1f} minutes behind")
                else:
                    print(f"      📡 Data fresh: {minutes_behind:.1f} minutes behind")

            except Exception as e:
                print(f"  ❌ {symbol}: Error checking gaps - {e}")

    # Collection metrics analysis
    print(f"\n📈 COLLECTION PERFORMANCE ANALYSIS:")
    print("-" * 40)

    collection_analysis = await conn.fetch("""
    SELECT
        vendor,
        symbol,
        COUNT(*) as total_collections,
        COUNT(*) FILTER (WHERE collection_success = true) as successful_collections,
        AVG(records_collected) as avg_records_per_collection,
        MAX(collection_timestamp) as latest_collection,
        EXTRACT(EPOCH FROM (NOW() - MAX(collection_timestamp)))/60 as minutes_since_collection
    FROM intg_minute_bar_collection_metrics
    WHERE collection_timestamp >= NOW() - INTERVAL '24 hours'
    GROUP BY vendor, symbol
    ORDER BY vendor, symbol
    """)

    for row in collection_analysis:
        success_rate = (row['successful_collections'] / row['total_collections']) * 100
        status = "✅" if success_rate >= 95 else "⚠️" if success_rate >= 85 else "🚨"

        print(f"  {status} {row['vendor'].title()} {row['symbol']}: {success_rate:.1f}% success rate")
        print(f"      Collections: {row['total_collections']:,} ({row['avg_records_per_collection']:.1f} avg records)")
        print(f"      Last collection: {row['minutes_since_collection']:.1f} minutes ago")

    # Vendor comparison
    print(f"\n🏆 VENDOR COMPARISON (24h):")
    print("-" * 40)

    vendor_comparison = await conn.fetch("""
    SELECT
        vendor,
        COUNT(*) as total_api_calls,
        COUNT(*) FILTER (WHERE status_code BETWEEN 200 AND 299) as successful_calls,
        AVG(response_time_ms) as avg_response_time,
        COUNT(*) FILTER (WHERE status_code = 429) as rate_limit_hits
    FROM intg_api_calls
    WHERE request_timestamp >= NOW() - INTERVAL '24 hours'
    GROUP BY vendor
    ORDER BY successful_calls DESC
    """)

    for row in vendor_comparison:
        success_rate = (row['successful_calls'] / row['total_api_calls']) * 100
        print(f"  {row['vendor'].title()}: {success_rate:.1f}% API success ({row['successful_calls']:,}/{row['total_api_calls']:,} calls)")
        print(f"    Avg response: {row['avg_response_time']:.0f}ms, Rate limits: {row['rate_limit_hits']}")

    await conn.close()

    print(f"\n💡 RECOMMENDATIONS:")
    print("- Monitor vendors with <95% success rates")
    print("- Check API credentials if rate limit hits are high")
    print("- Investigate data staleness >5 minutes during market hours")
    print("- Set up alerting for missing minute bars during trading hours")

if __name__ == "__main__":
    asyncio.run(detect_missing_minute_bars())