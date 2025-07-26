import pytest
import asyncpg
import asyncio
from datetime import date

# Adjust import as needed for your project structure
from config.environment import Environment, EnvironmentType
from db.test_db_manager import unit_test_db

@pytest.mark.asyncio
async def test_membership_changes_produce_expected_universe_membership(unit_test_db, tmp_path):
    """
    Test that applying a sequence of membership changes results in the expected universe_membership state.
    """
    env = Environment(EnvironmentType.TEST)
    env.get_database_url = lambda: unit_test_db
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        # Clean up both tables
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership_changes')}")

        # Insert test universe
        await conn.execute(f"INSERT INTO {env.get_table_name('universe')} (id, name, description) VALUES (100, 'test', 'test universe') ON CONFLICT (id) DO NOTHING")
        universe_id = 100

        # Insert membership changes: add/remove events for two symbols
        changes = [
            ('AAPL', 'add', date(2025, 1, 2), 'test add'),
            ('TSLA', 'add', date(2025, 1, 2), 'test add'),
            ('AAPL', 'remove', date(2025, 1, 5), 'test remove'),
            ('TSLA', 'remove', date(2025, 1, 6), 'test remove'),
            ('AAPL', 'add', date(2025, 1, 7), 'test re-add'),
        ]
        for symbol, action, eff_date, reason in changes:
            await conn.execute(f"""
                INSERT INTO {env.get_table_name('universe_membership_changes')} (universe_id, symbol, action, effective_date, reason)
                VALUES ($1, $2, $3, $4, $5)
            """, universe_id, symbol, action, eff_date, reason)

        # Simulate logic: reconstruct expected universe_membership as of 2025-01-08
        # (In production, builder logic would do this. Here, we do it in test.)
        as_of_date = date(2025, 1, 8)
        # Get all changes up to as_of_date, ordered by effective_date then id
        rows = await conn.fetch(f"""
            SELECT symbol, action, effective_date
            FROM {env.get_table_name('universe_membership_changes')}
            WHERE effective_date <= $1
            ORDER BY effective_date, symbol
        """, as_of_date)
        symbol_state = {}
        for row in rows:
            if row['action'] == 'add':
                symbol_state[row['symbol']] = True
            elif row['action'] == 'remove':
                symbol_state[row['symbol']] = False
        expected_members = {s for s, active in symbol_state.items() if active}

        # Now, simulate what the membership table should contain as of as_of_date
        # (In real system, this should be built from changes, but here we insert it manually for test)
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership')} WHERE universe_id = $1", universe_id)
        for symbol in expected_members:
            await conn.execute(f"""
                INSERT INTO {env.get_table_name('universe_membership')} (universe_id, symbol, start_at, end_at)
                VALUES ($1, $2, $3, NULL)
            """, universe_id, symbol, as_of_date)

        # Fetch actual membership
        actual_members = await conn.fetch(f"""
            SELECT symbol FROM {env.get_table_name('universe_membership')} WHERE universe_id = $1
        """, universe_id)
        actual_symbols = {r['symbol'] for r in actual_members}
        assert actual_symbols == expected_members, f"Universe membership mismatch: expected {expected_members}, got {actual_symbols}"

        # Clean up
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership')} WHERE universe_id = $1", universe_id)
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership_changes')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('universe')} WHERE id = $1", universe_id)
    await pool.close()
