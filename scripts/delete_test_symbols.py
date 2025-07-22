import asyncio
import asyncpg
from config.environment import get_environment

# List all tables that might have a symbol column referencing test data
TABLES_WITH_SYMBOL = [
    "daily_prices",
    "stock_splits",
    "dividends",
    "daily_adjusted_prices",
    # Add more tables as needed
]

async def delete_test_symbols():
    env = get_environment()
    db_url = env.get_database_url()
    pool = await asyncpg.create_pool(db_url)
    async with pool.acquire() as conn:
        for table in TABLES_WITH_SYMBOL:
            table_name = env.get_table_name(table)
            try:
                result = await conn.execute(f"DELETE FROM {table_name} WHERE symbol LIKE 'TEST%'")
                print(f"Deleted from {table_name}: {result}")
            except Exception as e:
                print(f"Error deleting from {table_name}: {e}")
    await pool.close()

if __name__ == "__main__":
    asyncio.run(delete_test_symbols())
