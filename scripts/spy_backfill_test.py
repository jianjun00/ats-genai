#!/usr/bin/env python3
"""
SPY Backfill Test

Test backfill specifically for SPY ETF to demonstrate working backfill system.
"""

import asyncio
import asyncpg
import requests
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("spy_backfill")

async def main():
    """Test backfill for SPY."""

    # API key
    tiingo_api_key = "5f40b4f36e171405746304ec0e5a6f3aa9ca77e5"

    # Database connection
    db_url = "postgresql://postgres:dev_password@ats-dev-postgres:5432/dev_db"

    logger.info("🔗 Connecting to database...")
    conn = await asyncpg.connect(db_url)

    # Get SPY instrument ID
    result = await conn.fetchrow("SELECT id, symbol FROM dev_instrument WHERE symbol = 'SPY'")

    if not result:
        logger.error("❌ SPY not found in instruments table")
        return

    instrument_id = result['id']
    symbol = result['symbol']

    logger.info(f"📊 Found {symbol} with ID {instrument_id}")

    # Check existing data
    existing_count = await conn.fetchval(
        "SELECT COUNT(*) FROM dev_daily_price_tiingo WHERE instrument_id = $1",
        instrument_id
    )
    logger.info(f"📈 Existing Tiingo records for {symbol}: {existing_count}")

    # Download recent data from Tiingo (last 90 days)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=90)

    logger.info(f"📡 Downloading Tiingo data for {symbol} from {start_date} to {end_date}...")

    url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
    params = {
        'startDate': start_date.strftime('%Y-%m-%d'),
        'endDate': end_date.strftime('%Y-%m-%d'),
        'format': 'json',
        'token': tiingo_api_key
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        logger.info(f"✅ Downloaded {len(data)} records for {symbol}")

        if data:
            # Show sample data
            sample = data[0] if data else {}
            logger.info(f"📊 Sample record: Date={sample.get('date', 'N/A')}, Close=${sample.get('close', 'N/A')}")

            # Insert data
            rows = []
            for price in data:
                date_val = datetime.strptime(price['date'][:10], '%Y-%m-%d').date()
                rows.append((
                    date_val, symbol, price.get('open'), price.get('high'),
                    price.get('low'), price.get('close'), price.get('volume', 0), instrument_id
                ))
            if rows:
                logger.info(f"💾 Inserting {len(rows)} records for {symbol}...")
                await conn.executemany("""
                    INSERT INTO dev_daily_price_tiingo
                    (date, symbol, open, high, low, close, volume, instrument_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (date, instrument_id) DO NOTHING
                """, rows)

                # Verify insertion
                final_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM dev_daily_price_tiingo WHERE instrument_id = $1",
                    instrument_id
                )

                new_records = final_count - existing_count
                logger.info(f"✅ Successfully inserted {new_records} new records")
                logger.info(f"📊 Total records for {symbol}: {final_count}")

    else:
        logger.warning(f"⚠️ Tiingo API returned {response.status_code} for {symbol}")

    await conn.close()
    logger.info("🎉 SPY backfill test completed successfully!")

if __name__ == "__main__":
    asyncio.run(main())