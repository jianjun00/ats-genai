import asyncpg
import asyncio
import os

DB_URL = os.environ.get("SEED_DB_URL") or "postgresql://postgres:postgres@localhost:5432/intg_db"

TSLA_SYMBOL = "TSLA"
AAPL_SYMBOL = "AAPL"
UNIVERSE_ID = 1
VENDOR_ID = 1
from datetime import datetime
START_AT = datetime.strptime("2025-01-01", "%Y-%m-%d").date()
ASOF_DATE = datetime.strptime("2022-12-31", "%Y-%m-%d").date()

async def seed_membership():
    pool = await asyncpg.create_pool(DB_URL)
    async with pool.acquire() as conn:
        # Ensure TSLA exists in instruments
        row = await conn.fetchrow("SELECT id FROM intg_instruments WHERE symbol = $1", TSLA_SYMBOL)
        if row:
            tsla_id = row["id"]
        else:
            insert_row = await conn.fetchrow(
                "INSERT INTO intg_instruments (symbol, name) VALUES ($1, $2) RETURNING id", TSLA_SYMBOL, "Tesla Inc.")
            tsla_id = insert_row["id"]
        
        # Ensure AAPL exists in instruments
        row = await conn.fetchrow("SELECT id FROM intg_instruments WHERE symbol = $1", AAPL_SYMBOL)
        if row:
            aapl_id = row["id"]
        else:
            insert_row = await conn.fetchrow(
                "INSERT INTO intg_instruments (symbol, name) VALUES ($1, $2) RETURNING id", AAPL_SYMBOL, "Apple Inc.")
            aapl_id = insert_row["id"]
        
        # Insert universe membership
        await conn.execute(
            "INSERT INTO intg_universe_membership (universe_id, instrument_id, symbol, start_at, end_at) "
            "VALUES ($1, $2, $3, $4, NULL) ON CONFLICT DO NOTHING",
            UNIVERSE_ID, tsla_id, TSLA_SYMBOL, START_AT
        )
        await conn.execute(
            "INSERT INTO intg_universe_membership (universe_id, instrument_id, symbol, start_at, end_at) "
            "VALUES ($1, $2, $3, $4, NULL) ON CONFLICT DO NOTHING",
            UNIVERSE_ID, aapl_id, AAPL_SYMBOL, START_AT
        )
        
        # Insert instrument xrefs
        await conn.execute(
            "INSERT INTO intg_instrument_xrefs (instrument_id, vendor_id, vendor_symbol, asof_date) "
            "VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
            tsla_id, VENDOR_ID, TSLA_SYMBOL, ASOF_DATE
        )
        await conn.execute(
            "INSERT INTO intg_instrument_xrefs (instrument_id, vendor_id, vendor_symbol, asof_date) "
            "VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
            aapl_id, VENDOR_ID, AAPL_SYMBOL, ASOF_DATE
        )
        
        print(f"Seeded TSLA membership: instrument_id={tsla_id}, universe_id={UNIVERSE_ID}, start_at={START_AT}")
        print(f"Seeded AAPL membership: instrument_id={aapl_id}, universe_id={UNIVERSE_ID}, start_at={START_AT}")
    await pool.close()

if __name__ == "__main__":
    asyncio.run(seed_membership())
