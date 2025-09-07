#!/usr/bin/env python3
"""
Setup Minute Bar Alerting

Creates automated alerts for missing minute bars and collection failures.
Integrates with existing Slack notifications.
"""

import asyncio
import asyncpg
from datetime import datetime, time
import requests

SLACK_WEBHOOK = "https://hooks.slack.com/services/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"

async def check_and_alert():
    """Check for missing minute bars and send alerts."""

    # Only run during market hours (9:30 AM - 4:00 PM EST)
    now = datetime.now()
    market_start = time(13, 30)  # 9:30 AM EST in UTC
    market_end = time(20, 0)     # 4:00 PM EST in UTC

    if not (market_start <= now.time() <= market_end):
        print("Outside market hours, skipping check")
        return

    conn = await asyncpg.connect(
        host='localhost', port=4432, user='postgres',
        password='intg_password', database='intg_db'
    )

    alerts = []

    # Check for stale data (>5 minutes behind)
    stale_data_query = """
    SELECT
        'Tiingo' as vendor, symbol,
        EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))/60 as minutes_behind
    FROM intg_one_minute_live_tiingo
    WHERE timestamp >= NOW() - INTERVAL '1 hour'
    GROUP BY symbol
    HAVING EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))/60 > 5
    UNION ALL
    SELECT
        'Polygon' as vendor, symbol,
        EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))/60 as minutes_behind
    FROM intg_one_minute_live_polygon
    WHERE timestamp >= NOW() - INTERVAL '1 hour'
    GROUP BY symbol
    HAVING EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))/60 > 5
    """

    stale_data = await conn.fetch(stale_data_query)

    for row in stale_data:
        alerts.append(f"🚨 {row['vendor']} {row['symbol']}: Data stale ({row['minutes_behind']:.1f} minutes behind)")

    # Check for poor collection success rates
    poor_performance_query = """
    SELECT
        vendor, symbol,
        COUNT(*) as total_collections,
        COUNT(*) FILTER (WHERE collection_success = true) as successful_collections,
        ROUND(COUNT(*) FILTER (WHERE collection_success = true) * 100.0 / COUNT(*), 1) as success_rate
    FROM intg_minute_bar_collection_metrics
    WHERE collection_timestamp >= NOW() - INTERVAL '1 hour'
    GROUP BY vendor, symbol
    HAVING COUNT(*) FILTER (WHERE collection_success = true) * 100.0 / COUNT(*) < 90
    """

    poor_performance = await conn.fetch(poor_performance_query)

    for row in poor_performance:
        alerts.append(f"⚠️ {row['vendor'].title()} {row['symbol']}: Low success rate ({row['success_rate']}%)")

    await conn.close()

    # Send Slack alert if issues found
    if alerts:
        message = f"🚨 *Minute Bar Collection Issues Detected*\n\n" + "\n".join(alerts)
        message += f"\n\n📊 Dashboard: http://localhost:4002/d/cb0f07fd-9f56-486e-8cd6-7c9893e63116/ats-vendor-monitoring-dashboard-postgresql"

        try:
            response = requests.post(SLACK_WEBHOOK, json={"text": message}, timeout=10)
            if response.status_code == 200:
                print(f"✅ Alert sent to Slack: {len(alerts)} issues")
            else:
                print(f"❌ Failed to send Slack alert: {response.status_code}")
        except Exception as e:
            print(f"❌ Slack notification error: {e}")
    else:
        print("✅ No minute bar issues detected")

if __name__ == "__main__":
    asyncio.run(check_and_alert())