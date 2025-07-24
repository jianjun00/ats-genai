import os
import sys
import subprocess
from pathlib import Path
import pandas as pd
import pytest
import asyncio
import asyncpg

import pytest
import asyncio
import asyncpg
import os
import sys
from pathlib import Path
from intg_tests.universe import db_test_utils

@pytest.mark.asyncio
async def test_universe_state_manager_cli_build_and_inspect_aapl_tsla(tmp_path):
    """
    Integration test: build universe state for a test universe (AAPL, TSLA) from 2025-01-02 using the CLI.
    Ensures DB is isolated and minimal universe/instruments exist for real builder.
    """
    universe_name = "test_universe"
    symbols = ["AAPL", "TSLA"]
    from config.environment import get_environment, set_environment, EnvironmentType
    set_environment(EnvironmentType.INTEGRATION)
    env = get_environment()
    pool = await asyncpg.create_pool(env.get_database_url())
    async with pool.acquire() as conn:
        # Backup tables
        universe_backup = await db_test_utils.backup_table(conn, "universe", env)
        membership_backup = await db_test_utils.backup_table(conn, "universe_membership", env)
        inst_backup = await db_test_utils.backup_table(conn, "instrument_polygon", env)
        # Setup minimal test data
        universe_id = await db_test_utils.setup_test_universe(conn, universe_name, symbols, env)
    # Debug: print schema for instrument_polygon and instruments before build
    async with pool.acquire() as conn:
        for table in ["instrument_polygon", "instruments"]:
            tn = env.get_table_name(table)
            schema = await conn.fetch(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", tn)
            print(f"DEBUG: {tn} columns:", schema)
            # Print all columns for first row if present
            row = await conn.fetchrow(f"SELECT * FROM {tn} LIMIT 1")
            if row:
                print(f"DEBUG: {tn} sample row:", dict(row))
            else:
                print(f"DEBUG: {tn} sample row: <empty>")
    await pool.close()
    try:
        proc_env = os.environ.copy()
        proc_env["PYTHONPATH"] = f"{os.getcwd()}/src" + (":" + proc_env["PYTHONPATH"] if "PYTHONPATH" in proc_env else "")
        proc_env["ENVIRONMENT"] = "intg"
        cli_path = Path(os.getcwd()) / "src/state/universe_state_manager.py"

        # Build universe state for date range
        build_cmd = [
            sys.executable, str(cli_path),
            "--start_date", "2025-01-02",
            "--end_date", "2025-01-02",
            "--universe_id", universe_name,
            "--action", "build",
            "--saved_dir", str(tmp_path)
        ]

        print(f"DEBUG: build CLI command: {' '.join(str(x) for x in build_cmd)}")
        result = await asyncio.create_subprocess_exec(*build_cmd, env=proc_env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await result.communicate()
        print("DEBUG: build CLI stdout:", stdout.decode())
        print("DEBUG: build CLI stderr:", stderr.decode())
        assert result.returncode == 0, f"Build CLI failed: {stderr.decode()}"

        # Find all expected state files
        states_dir = tmp_path / "states"
        files = sorted(states_dir.glob("universe_state_*.parquet"))
        assert files, "No universe state files were created."

        # Inspect for AAPL and TSLA on a sample date
        sample_date = "2025-01-02"
        for instrument, label in [(1, "AAPL"), (2, "TSLA")]:
            inspect_cmd = [
                sys.executable, str(cli_path),
                "--start_date", sample_date,
                "--end_date", sample_date,
                "--universe_id", universe_name,
                "--action", "inspect",
                "--instrument_id", str(instrument),
                "--mode", "print",
                "--fields", "low", "high", "close", "pldot", "oneonedot", "etop", "ebot",
                "--saved_dir", str(tmp_path)
            ]
            print(f"DEBUG: inspect CLI command for {instrument}: {' '.join(str(x) for x in inspect_cmd)}")
            inspect = await asyncio.create_subprocess_exec(*inspect_cmd, env=proc_env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            out, err = await inspect.communicate()
            print(f"DEBUG: inspect CLI stdout for {instrument}:", out.decode())
            print(f"DEBUG: inspect CLI stderr for {instrument}:", err.decode())
            assert inspect.returncode == 0, f"Inspect CLI failed for {instrument}!\nSTDOUT:\n{out.decode()}\nSTDERR:\n{err.decode()}"
            # Do not assert specific signal values (real builder)
    finally:
        pool = await asyncpg.create_pool(env.get_database_url())
        async with pool.acquire() as conn:
            await db_test_utils.cleanup_test_universe(conn, universe_id, symbols, env)
            # Restore in FK-safe order: universe, then membership, then instrument_polygon
            # Restore in FK-safe order: instrument_polygon, then universe, then membership
            await db_test_utils.restore_table(conn, "instrument_polygon", inst_backup, env)
            # Ensure no memberships reference universes before restore
            await conn.execute(f'DELETE FROM {env.get_table_name("universe_membership")}')
            await db_test_utils.restore_table(conn, "universe", universe_backup, env)
            await db_test_utils.restore_table(conn, "universe_membership", membership_backup, env)
        await pool.close()
