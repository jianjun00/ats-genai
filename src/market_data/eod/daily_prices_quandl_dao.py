from typing import List, Dict, Any
from core.config.environment import Environment

class DailyPricesQuandlDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table = self.env.get_table_name('daily_prices_quandl')
        self.pool = None  # Set externally or use context

    def set_pool(self, pool):
        self.pool = pool

    async def batch_insert_prices(self, prices: List[Dict[str, Any]], ticker: str):
        if not self.pool:
            raise RuntimeError("Connection pool not set for DailyPricesQuandlDAO")
        async with self.pool.acquire() as conn:
            await conn.executemany(
                f"""
                INSERT INTO {self.table} (date, symbol, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (date, symbol) DO NOTHING
                """,
                [(
                    p['date'],
                    ticker,
                    p.get('open'),
                    p.get('high'),
                    p.get('low'),
                    p.get('close'),
                    p.get('volume')
                ) for p in prices]
            )

    async def get_prices(self, ticker: str, start_date, end_date) -> List[Dict[str, Any]]:
        if not self.pool:
            raise RuntimeError("Connection pool not set for DailyPricesQuandlDAO")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT date, open, high, low, close, volume FROM {self.table}
                WHERE symbol = $1 AND date BETWEEN $2 AND $3
                ORDER BY date ASC
                """,
                ticker, start_date, end_date
            )
            return [dict(row) for row in rows]
