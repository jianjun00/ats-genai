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
from src.db.test_db_manager import IntegrationTestSession

@pytest.mark.asyncio
async def test_universe_state_manager_cli_build_and_inspect_aapl_tsla(tmp_path):
    """
    Integration test: build universe state for a test universe (AAPL, TSLA) from 2025-01-02 using the CLI.
    Ensures DB is isolated and minimal universe/instruments exist for real builder.
    """
    universe_name = "test_universe"
    symbols = ["AAPL", "TSLA"]
    from config.environment import Environment, EnvironmentType
    from intg_tests.db.test_intg_db_base_intg import get_test_db_url
    env = Environment(env_type=EnvironmentType.INTEGRATION, db_url=get_test_db_url())
    import tempfile
    import subprocess
    db_url = env.get_database_url()
    # Parse DB name, user, host, port from db_url (assume postgres://user:pass@host:port/dbname)
    import re
    m = re.match(r"postgresql?://([^:]+):([^@]+)@([^:/]+)(?::(\d+))?/([^?]+)", db_url)
    if not m:
        raise RuntimeError(f"Could not parse DB URL: {db_url}")
    db_user, db_pass, db_host, db_port, db_name = m.groups()
    db_port = db_port or "5432"
    # Use a temp file for the backup
    backup_file = tempfile.NamedTemporaryFile(delete=False)
    backup_file.close()
    # Set PGPASSWORD for subprocess
    env_vars = os.environ.copy()
    env_vars["PGPASSWORD"] = db_pass
    # Dump the whole DB before test
    subprocess.run([
        "pg_dump", "-h", db_host, "-p", db_port, "-U", db_user, "-F", "c", "-f", backup_file.name, db_name
    ], check=True, env=env_vars)
    pool = await asyncpg.create_pool(env.get_database_url())
    async with pool.acquire() as conn:
        # Setup minimal test data: insert universe and instruments
        universe_table = env.get_table_name("universe")
        membership_table = env.get_table_name("universe_membership")
        inst_table = env.get_table_name("instrument_polygon")
        daily_prices_polygon_table = env.get_table_name("daily_prices_polygon")
        await conn.execute(f"INSERT INTO {universe_table} (name, description) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING", universe_name, "Test Universe")
        universe_id_row = await conn.fetchrow(f"SELECT id FROM {universe_table} WHERE name = $1", universe_name)
        universe_id = universe_id_row["id"] if universe_id_row else None
        # Insert instruments and instrument_polygon rows for AAPL, TSLA
        for symbol in symbols:
            await conn.execute(f"INSERT INTO {env.get_table_name('instruments')} (symbol) VALUES ($1) ON CONFLICT (symbol) DO NOTHING", symbol)
            inst_row = await conn.fetchrow(f"SELECT id FROM {env.get_table_name('instruments')} WHERE symbol = $1", symbol)
            inst_id = inst_row["id"] if inst_row else None
            import datetime
            await conn.execute(f"INSERT INTO {inst_table} (symbol, list_date) VALUES ($1, $2) ON CONFLICT (symbol) DO NOTHING", symbol, datetime.date(2020, 1, 1))
            # Add membership
            await conn.execute(f"INSERT INTO {membership_table} (universe_id, instrument_id, symbol, start_at) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING", universe_id, inst_id, symbol, datetime.date(2025, 1, 2))
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
        # Restore the DB from backup (drop schema, recreate, restore from dump)
        # Use env_vars from above for PGPASSWORD
        # Drop all connections to DB (optional, best effort)
        subprocess.run([
            "psql", "-h", db_host, "-p", db_port, "-U", db_user, "-d", db_name, "-c", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid();"
        ], check=False, env=env_vars)
        # Drop schema cascade (clean slate)
        subprocess.run([
            "psql", "-h", db_host, "-p", db_port, "-U", db_user, "-d", db_name, "-c", "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
        ], check=True, env=env_vars)
        # Restore from backup
        subprocess.run([
            "pg_restore", "-h", db_host, "-p", db_port, "-U", db_user, "-d", db_name, "-F", "c", backup_file.name
        ], check=True, env=env_vars)
        os.unlink(backup_file.name)
