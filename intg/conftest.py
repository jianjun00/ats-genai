import pytest_asyncio
from intg_tests.db.test_intg_db_base_intg import get_test_db_url

@pytest_asyncio.fixture(scope="session")
async def integration_test_db():
    """Async fixture that provides the integration test database URL for all integration tests."""
    yield get_test_db_url()

import pytest
import asyncpg
import os
import re
from contextlib import asynccontextmanager

@asynccontextmanager
async def _clone_intg_db(base_url, unique_db_name):
    # Parse base_url for host/port/user/password
    match = re.match(r"postgresql://([^:]+):([^@]+)@([^:/]+)(?::(\d+))?/(\w+)", base_url)
    if not match:
        raise ValueError(f"Invalid DB URL: {base_url}")
    user, password, host, port, base_db = match.groups()
    port = port or "5432"
    admin_url = f"postgresql://{user}:{password}@{host}:{port}/postgres"
    unique_url = f"postgresql://{user}:{password}@{host}:{port}/{unique_db_name}"
    conn = await asyncpg.connect(admin_url)
    try:
        # Drop if exists
        await conn.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{unique_db_name}'")
        await conn.execute(f"DROP DATABASE IF EXISTS {unique_db_name}")
        await conn.execute(f"CREATE DATABASE {unique_db_name} TEMPLATE {base_db}")
    finally:
        await conn.close()
    try:
        yield unique_url
    finally:
        conn = await asyncpg.connect(admin_url)
        try:
            await conn.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{unique_db_name}'")
            await conn.execute(f"DROP DATABASE IF EXISTS {unique_db_name}")
        finally:
            await conn.close()

import pytest_asyncio
@pytest_asyncio.fixture
async def intg_test_db(request):
    """Fixture: per-test integration DB, cloned from intg_db, dropped after test."""
    base_url = get_test_db_url()
    test_file = os.path.splitext(os.path.basename(request.fspath))[0]
    test_name = request.node.name
    unique_db_name = f"intg_db_{test_file}_{test_name}".replace("-", "_").replace(".", "_")
    async with _clone_intg_db(base_url, unique_db_name) as db_url:
        yield db_url
