import os
import tempfile
import asyncpg
import pytest
import asyncio
import uuid

TSDB_URL_TEMPLATE = "postgresql://{user}:{password}@{host}:{port}/{dbname}"

# You may want to set these from your environment or test config
def get_test_db_url(dbname):
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "password")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    return TSDB_URL_TEMPLATE.format(user=user, password=password, host=host, port=port, dbname=dbname)

class AsyncPGTestDBBase:
    """
    Base class for asyncpg-based PostgreSQL test databases.
    Set DROP_TEST_DB = False to keep the test DB after test run (for debugging).
    By default, the test DB is dropped after each test.
    """
    DROP_TEST_DB = True
    import pytest_asyncio
    @pytest_asyncio.fixture(autouse=True, scope="function")
    async def _setup_and_teardown_db(self, request):
        # Create a unique DB for this test
        self._test_db_name = f"test_db_{uuid.uuid4().hex[:8]}"
        self._admin_url = get_test_db_url("postgres")
        self._db_url = get_test_db_url(self._test_db_name)
        self._loop = asyncio.get_event_loop()
        # Create DB
        conn = await asyncpg.connect(self._admin_url)
        await conn.execute(f'CREATE DATABASE "{self._test_db_name}"')
        await conn.close()
        # Set env var for DB URL
        os.environ["TSDB_URL"] = self._db_url
        # Run test
        try:
            await self._init_schema()
            yield
        finally:
            # Drop DB after test if enabled
            if self.DROP_TEST_DB:
                conn = await asyncpg.connect(self._admin_url)
                await conn.execute(f'DROP DATABASE IF EXISTS "{self._test_db_name}"')
                await conn.close()
            os.environ.pop("TSDB_URL", None)

    async def _init_schema(self):
        # Load unified schema from SQL file
        import os
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
        sql_path = os.path.join(project_root, "src/db/schema.sql")
        with open(sql_path, "r") as f:
            schema_sql = f.read()
        pool = await asyncpg.create_pool(os.environ["TSDB_URL"])
        async with pool.acquire() as conn:
            # Remove comment lines before splitting
            sql_lines = schema_sql.splitlines()
            non_comment_lines = [line for line in sql_lines if not line.strip().startswith("--")]
            sql_no_comments = "\n".join(non_comment_lines)
            for statement in sql_no_comments.split(";"):
                stripped = statement.strip()
                if not stripped:
                    print(f"Skipping empty statement.")
                    continue
                try:
                    await conn.execute(statement)
                except Exception as e:
                    import traceback
                    print(f"Error executing statement: {statement}\nError: {e}")
                    traceback.print_exc()
                    raise
        await pool.close()

    @pytest.mark.asyncio
    async def test_daily_prices_table(self):
        pool = await asyncpg.create_pool(os.environ["TSDB_URL"])
        async with pool.acquire() as conn:
            # Insert a row and check count
            await conn.execute("INSERT INTO daily_prices (date, symbol, open, high, low, close, volume, adjusted_price) VALUES ('2025-07-21', 'AAPL', 100, 110, 99, 108, 1000000, 108)")
            count = await conn.fetchval("SELECT COUNT(*) FROM daily_prices WHERE symbol='AAPL'")
        await pool.close()
        assert count == 1
