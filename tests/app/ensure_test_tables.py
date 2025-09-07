"""
Helper script to ensure all required test tables exist with correct prefixes.
This can be imported by test files to ensure proper test database setup.
"""
import asyncio
import asyncpg
import logging
from shared.utils.environment import Environment, EnvironmentType

logger = logging.getLogger(__name__)

async def ensure_test_tables(db_url):
    """
    Ensure all required test tables exist with correct prefixes.
    """
    logger.info(f"Ensuring test tables exist in database: {db_url}")
    env = Environment(env_type=EnvironmentType.TEST, db_url=db_url)
    conn = await asyncpg.connect(db_url)

    try:
        # List all existing tables
        tables = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        existing_tables = [t['table_name'] for t in tables]
        logger.info(f"Existing tables: {existing_tables}")

        # Check and create vendors table if needed
        vendors_table = env.get_table_name('vendors')
        if vendors_table not in existing_tables:
            logger.info(f"Creating {vendors_table} table")
            await conn.execute(f"""
                CREATE TABLE {vendors_table} (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    description TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                )
            """)
        else:
            # Check if description column exists and add it if not
            description_exists = await conn.fetchval(f"""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = '{vendors_table.replace("'", "''")}' AND column_name = 'description'
            """)
            if not description_exists:
                logger.info(f"Adding description column to {vendors_table}")
                await conn.execute(f"ALTER TABLE {vendors_table} ADD COLUMN description TEXT;")


        # Check and create instruments table if needed
        instruments_table = env.get_table_name('instruments')
        if instruments_table not in existing_tables:
            logger.info(f"Creating {instruments_table} table")
            await conn.execute(f"""
                CREATE TABLE {instruments_table} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    name TEXT,
                    type TEXT,
                    list_date DATE,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE (symbol)
                )
            """)

        # Check and create instrument_xrefs table if needed
        instrument_xrefs_table = env.get_table_name('instrument_xrefs')
        if instrument_xrefs_table not in existing_tables:
            logger.info(f"Creating {instrument_xrefs_table} table")
            await conn.execute(f"""
                CREATE TABLE {instrument_xrefs_table} (
                    id SERIAL PRIMARY KEY,
                    instrument_id INTEGER NOT NULL REFERENCES {instruments_table}(id) ON DELETE CASCADE,
                    vendor_id INTEGER NOT NULL REFERENCES {vendors_table}(id) ON DELETE CASCADE,
                    symbol TEXT NOT NULL,
                    type TEXT,
                    start_at TIMESTAMP NOT NULL DEFAULT now(),
                    end_at TIMESTAMP,
                    active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE (instrument_id, vendor_id)
                )
            """)

        # Check and create universe_membership table if needed
        universe_membership_table = env.get_table_name('universe_membership')
        if universe_membership_table not in existing_tables:
            logger.info(f"Creating {universe_membership_table} table")
            await conn.execute(f"""
                CREATE TABLE {universe_membership_table} (
                    id SERIAL PRIMARY KEY,
                    universe_id INTEGER NOT NULL,
                    instrument_id INTEGER NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE (universe_id, instrument_id, start_date)
                )
            """)

        # Check and create daily_prices table if needed
        daily_prices_table = env.get_table_name('daily_prices')
        if daily_prices_table not in existing_tables:
            logger.info(f"Creating {daily_prices_table} table")
            await conn.execute(f"""
                CREATE TABLE {daily_prices_table} (
                    id SERIAL PRIMARY KEY,
                    instrument_id INTEGER NOT NULL,
                    symbol TEXT,
                    date DATE NOT NULL,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume NUMERIC,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE (instrument_id, date)
                )
            """)
        else:
            # Check if symbol column exists and add it if not
            symbol_exists = await conn.fetchval(f"""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = '{daily_prices_table.replace("'", "''")}' AND column_name = 'symbol'
            """)
            if not symbol_exists:
                logger.info(f"Adding symbol column to {daily_prices_table}")
                await conn.execute(f"ALTER TABLE {daily_prices_table} ADD COLUMN symbol TEXT;")


        # Create daily_market_cap table if needed
        daily_market_cap_table = env.get_table_name('daily_market_cap')
        if daily_market_cap_table not in existing_tables:
            logger.info(f"Creating {daily_market_cap_table} table")
            await conn.execute(f"""
                CREATE TABLE {daily_market_cap_table} (
                    id SERIAL PRIMARY KEY,
                    instrument_id INTEGER NOT NULL,
                    symbol TEXT,
                    date DATE NOT NULL,
                    market_cap NUMERIC,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE (instrument_id, date)
                )
            """)

        # Create universe_membership_changes table if needed
        universe_membership_changes_table = env.get_table_name('universe_membership_changes')
        if universe_membership_changes_table not in existing_tables:
            logger.info(f"Creating {universe_membership_changes_table} table")
            await conn.execute(f"""
                CREATE TABLE {universe_membership_changes_table} (
                    id SERIAL PRIMARY KEY,
                    universe_id INTEGER NOT NULL,
                    instrument_id INTEGER NOT NULL,
                    symbol TEXT,
                    change_type TEXT NOT NULL,
                    action TEXT,
                    reason TEXT,
                    effective_date DATE NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                )
            """)

        # List tables after creation
        updated_tables = await conn.fetch("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        updated_tables = [t['table_name'] for t in updated_tables]
        logger.info(f"Updated tables: {updated_tables}")

    finally:
        await conn.close()

    return env

if __name__ == "__main__":
    # This can be run as a standalone script for testing
    logging.basicConfig(level=logging.INFO)
    db_url = "postgresql://test_user:test_password@localhost:5432/test_db"
    asyncio.run(ensure_test_tables(db_url))
