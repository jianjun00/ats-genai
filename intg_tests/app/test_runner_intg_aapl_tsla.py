import pytest
import asyncpg
import pandas as pd
from datetime import date
from src.app.runner import Runner
from src.universe.universe_manager import UniverseManager
from src.state.universe_state_manager import UniverseStateManager
from config.environment import Environment, EnvironmentType

# Test config: use intg_ DB prefix and robust test isolation
TEST_START_DATE = "2025-07-01"
TEST_END_DATE = "2025-07-03"
UNIVERSE_SYMBOLS = ["AAPL", "TSLA"]
UNIVERSE_ID = 9999  # Arbitrary test universe ID

@pytest.mark.asyncio
async def test_runner_with_aapl_tsla(monkeypatch):
    """
    Integration test: create a universe with AAPL and TSLA, run runner from 2025-07-01 to 2025-07-03,
    and verify correct processing.
    """
    env = Environment(EnvironmentType.INTEGRATION)
    pool = await asyncpg.create_pool(env.get_database_url())

    # --- Backup and restore for test isolation ---
    tables = [
        env.get_table_name('universe_membership_changes'),
        env.get_table_name('universes'),
        env.get_table_name('instruments'),
        env.get_table_name('daily_prices'),
        env.get_table_name('universe_state'),
    ]
    backups = {}
    async with pool.acquire() as conn:
        for table in tables:
            rows = await conn.fetch(f'SELECT * FROM {table}')
            backups[table] = [dict(row) for row in rows]

    try:
        # --- Setup test universe ---
        async with pool.acquire() as conn:
            # Clean tables
            for table in tables:
                await conn.execute(f'DELETE FROM {table}')
            # Insert instruments
            await conn.execute(f"""
                INSERT INTO {env.get_table_name('instruments')} (symbol, name, exchange) VALUES
                ('AAPL', 'Apple Inc.', 'NASDAQ'),
                ('TSLA', 'Tesla Inc.', 'NASDAQ')
            """)
            # Insert daily prices for AAPL and TSLA for 2025-07-01 to 2025-07-03
            for symbol in UNIVERSE_SYMBOLS:
                for d in pd.date_range(TEST_START_DATE, TEST_END_DATE):
                    await conn.execute(f"""
                        INSERT INTO {env.get_table_name('daily_prices')} (symbol, date, close, volume, market_cap) VALUES
                        ($1, $2, 100.0, 1000000, 2000000000.0)
                    """, symbol, d.date())
            # Insert universe
            await conn.execute(f"""
                INSERT INTO {env.get_table_name('universes')} (id, name, created_at) VALUES
                ($1, 'Test Universe', NOW())
            """, UNIVERSE_ID)
            # Insert membership changes (add both symbols effective 2025-07-01)
            for symbol in UNIVERSE_SYMBOLS:
                await conn.execute(f"""
                    INSERT INTO {env.get_table_name('universe_membership_changes')} (symbol, action, effective_date, reason, created_at) VALUES
                    ($1, 'add', $2, 'test', NOW())
                """, symbol, TEST_START_DATE)

        # --- Run the runner for the test universe and dates ---
        # Patch environment to use intg_test
        monkeypatch.setenv("ENV", "intg_test")
        runner = Runner(
            start_date=TEST_START_DATE,
            end_date=TEST_END_DATE,
            environment=env,
            universe_id=UNIVERSE_ID
        )
        runner.run()

        # --- Validate universe state for each date ---
        universe_state_mgr = UniverseStateManager(env)
        for d in pd.date_range(TEST_START_DATE, TEST_END_DATE):
            state = await universe_state_mgr.get_universe_state(UNIVERSE_ID, d.date())
            assert set(state['symbol']) == set(UNIVERSE_SYMBOLS), f"Universe state for {d.date()} does not match expected symbols"
    finally:
        # --- Restore tables ---
        async with pool.acquire() as conn:
            for table, rows in backups.items():
                await conn.execute(f'DELETE FROM {table}')
                if rows:
                    # Get columns
                    cols = rows[0].keys()
                    for row in rows:
                        colnames = ', '.join(cols)
                        placeholders = ', '.join([f'${i+1}' for i in range(len(cols))])
                        values = [row[c] for c in cols]
                        await conn.execute(f"INSERT INTO {table} ({colnames}) VALUES ({placeholders})", *values)
        await pool.close()
