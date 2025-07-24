import asyncpg
import os
from datetime import date
from config.environment import get_environment, set_environment, EnvironmentType

def get_table_name(table, env):
    return env.get_table_name(table)

async def backup_table(conn, table, env):
    rows = await conn.fetch(f'SELECT * FROM {get_table_name(table, env)}')
    return rows

async def restore_table(conn, table, backup_rows, env):
    await conn.execute(f'DELETE FROM {get_table_name(table, env)}')
    if not backup_rows:
        return
    columns = list(backup_rows[0].keys())
    for row in backup_rows:
        cols = ','.join(columns)
        vals = ','.join([f'${i+1}' for i in range(len(columns))])
        await conn.execute(f'INSERT INTO {get_table_name(table, env)} ({cols}) VALUES ({vals})', *[row[c] for c in columns])

async def setup_test_universe(conn, universe_name, symbols, env):
    # Always include AAPL and TSLA for integration test setup
    required_symbols = {"AAPL", "TSLA"}
    symbols = list(set(symbols).union(required_symbols))
    # Ensure meta column exists in universe_membership
    await conn.execute(f"ALTER TABLE {get_table_name('universe_membership', env)} ADD COLUMN IF NOT EXISTS meta JSONB")
    # Ensure sector column exists in instrument_polygon and instruments
    await conn.execute(f"ALTER TABLE {get_table_name('instrument_polygon', env)} ADD COLUMN IF NOT EXISTS sector TEXT")
    await conn.execute(f"ALTER TABLE {get_table_name('instruments', env)} ADD COLUMN IF NOT EXISTS sector TEXT")
    # Ensure daily_prices table exists and has required columns
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {get_table_name('daily_prices', env)} (
            date DATE NOT NULL,
            symbol TEXT NOT NULL,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume BIGINT,
            PRIMARY KEY (date, symbol)
        )
    """)
    await conn.execute(f"ALTER TABLE {get_table_name('daily_prices', env)} ADD COLUMN IF NOT EXISTS market_cap DOUBLE PRECISION")
    # Create universe
    await conn.execute(f"""
        INSERT INTO {get_table_name('universe', env)} (name, description)
        VALUES ($1, $2)
        ON CONFLICT (name) DO NOTHING
    """, universe_name, f"Test universe {universe_name}")
    universe_id = await conn.fetchval(f"SELECT id FROM {get_table_name('universe', env)} WHERE name = $1", universe_name)
    # Insert instruments (both tables)
    for sym in symbols:
        await conn.execute(f"""
            INSERT INTO {get_table_name('instrument_polygon', env)} 
                (symbol, name, exchange, type, currency, figi, isin, cusip, composite_figi, active, list_date, delist_date, sector, raw, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, now(), now())
            ON CONFLICT (symbol) DO NOTHING
        """, sym, sym, 'TEST', 'EQUITY', 'USD', None, None, None, None, True, date(2020,1,1), None, 'TECH', '{}')
        await conn.execute(f"""
            INSERT INTO {get_table_name('instruments', env)} 
                (symbol, name, exchange, type, currency, figi, isin, cusip, composite_figi, active, list_date, delist_date, sector, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, now(), now())
            ON CONFLICT (symbol) DO NOTHING
        """, sym, sym, 'TEST', 'EQUITY', 'USD', None, None, None, None, True, date(2020,1,1), None, 'TECH')
        # Insert minimal daily_prices row for test date
        await conn.execute(f"""
            INSERT INTO {get_table_name('daily_prices', env)} 
                (date, symbol, open, high, low, close, volume, market_cap)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (date, symbol) DO NOTHING
        """, date(2025,1,2), sym, 100.0, 101.0, 99.0, 100.5, 1000000, 1_000_000_000)
    # Insert universe membership
    for sym in symbols:
        await conn.execute(f"""
            INSERT INTO {get_table_name('universe_membership', env)} (universe_id, symbol, start_at, end_at, meta)
            VALUES ($1, $2, $3, NULL, $4)
            ON CONFLICT (universe_id, symbol, start_at) DO NOTHING
        """, universe_id, sym, date(2025,1,2), '{}')
    return universe_id

async def cleanup_test_universe(conn, universe_id, symbols, env):
    # First delete membership (FK to universe), then universe, then instruments
    await conn.execute(f'DELETE FROM {get_table_name("universe_membership", env)} WHERE universe_id = $1', universe_id)
    await conn.execute(f'DELETE FROM {get_table_name("universe", env)} WHERE id = $1', universe_id)
    for sym in symbols:
        await conn.execute(f'DELETE FROM {get_table_name("instrument_polygon", env)} WHERE symbol = $1', sym)
        await conn.execute(f'DELETE FROM {get_table_name("instruments", env)} WHERE symbol = $1', sym)
