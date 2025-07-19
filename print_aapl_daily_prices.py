import asyncpg
import asyncio
import os

TSDB_URL = os.getenv('TSDB_URL', 'postgresql://localhost:5432/trading_db')

QUERY = """
SELECT date, open, high, low, close, volume
FROM daily_prices
WHERE symbol = 'AAPL'
ORDER BY date;
"""

async def main():
    pool = await asyncpg.create_pool(TSDB_URL, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        rows = await conn.fetch(QUERY)
        print(f"date       open      high      low       close     volume")
        for row in rows:
            print(f"{row['date']}  {row['open']:.2f}  {row['high']:.2f}  {row['low']:.2f}  {row['close']:.2f}  {row['volume']}")
    await pool.close()

if __name__ == '__main__':
    asyncio.run(main())
