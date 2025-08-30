#!/usr/bin/env python3
"""
Quick test to backfill MSFT from Polygon
"""

import asyncio
import asyncpg
import requests
from datetime import date
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_msft_backfill():
    """Test MSFT backfill."""
    
    # Get MSFT instrument ID
    conn = await asyncpg.connect(
        host="ats-dev-postgres",
        port=5432,
        user="postgres",
        password="dev_password", 
        database="dev_db"
    )
    
    # Get MSFT instrument
    msft = await conn.fetchrow("SELECT id, symbol, name FROM dev_instruments WHERE symbol = 'MSFT' LIMIT 1")
    if not msft:
        logger.error("MSFT not found!")
        return
        
    logger.info(f"Found MSFT: {msft['name']}")
    
    # Test Polygon API call
    url = "https://api.polygon.io/v2/aggs/ticker/MSFT/range/1/day/2024-01-01/2024-08-30"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 500,
        "apikey": "wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"
    }
    
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    
    logger.info(f"API Response: {data.get('status')}")
    
    if data.get("status") == "OK" and "results" in data:
        results = data["results"]
        logger.info(f"Got {len(results)} records for MSFT")
        
        # Insert some data
        records = []
        for item in results[:10]:  # Just first 10 for testing
            date_val = date.fromtimestamp(item['t'] / 1000)
            record = (
                date_val, 'MSFT', float(item['o']), float(item['h']), 
                float(item['l']), float(item['c']), int(item['v']), None, msft['id']
            )
            records.append(record)
            
        # Insert
        insert_query = """
        INSERT INTO dev_daily_prices_polygon 
        (date, symbol, open, high, low, close, volume, market_cap, instrument_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (date, instrument_id) DO NOTHING
        """
        
        await conn.executemany(insert_query, records)
        logger.info(f"✅ Inserted {len(records)} MSFT records")
        
    await conn.close()

if __name__ == "__main__":
    asyncio.run(test_msft_backfill())