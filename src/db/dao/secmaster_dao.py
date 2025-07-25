from config.environment import Environment
import asyncpg
from typing import List, Optional, Dict, Any

class SecMasterDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.universe_membership_table = self.env.get_table_name('universe_membership')
        self.daily_prices_table = self.env.get_table_name('daily_prices')
        self.db_url = self.env.get_database_url()

    async def get_spy_membership_events(self) -> List[Dict[str, Any]]:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT m.symbol, m.start_at AS start_date, m.end_at AS end_date
                    FROM {self.universe_membership_table} m
                    JOIN {self.env.get_table_name('universe')} u ON m.universe_id = u.id
                    WHERE u.name = 'S&P 500'
                    ORDER BY m.start_at
                    """
                )
                return [dict(row) for row in rows]
        finally:
            await pool.close()

    async def batch_last_close_prices(self, as_of_date, symbols: List[str]) -> Dict[str, float]:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT symbol, close FROM {self.daily_prices_table}
                    WHERE date = $1 AND symbol = ANY($2)
                    """, as_of_date, symbols
                )
                return {row['symbol']: row['close'] for row in rows}
        finally:
            await pool.close()

    async def batch_market_caps(self, as_of_date, symbols: List[str]) -> Dict[str, float]:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT symbol, market_cap FROM {self.daily_prices_table}
                    WHERE date = $1 AND symbol = ANY($2)
                    """, as_of_date, symbols
                )
                return {row['symbol']: row['market_cap'] for row in rows}
        finally:
            await pool.close()

    async def get_last_close_price(self, symbol: str, as_of_date) -> Optional[float]:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                price = await conn.fetchval(
                    f"""
                    SELECT close FROM {self.daily_prices_table}
                    WHERE symbol = $1 AND date <= $2
                    ORDER BY date DESC LIMIT 1
                    """, symbol, as_of_date
                )
                return price
        finally:
            await pool.close()

    async def get_market_cap(self, symbol: str, as_of_date) -> Optional[float]:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                mc = await conn.fetchval(
                    f"""
                    SELECT market_cap FROM {self.daily_prices_table}
                    WHERE symbol = $1 AND date <= $2
                    ORDER BY date DESC LIMIT 1
                    """, symbol, as_of_date
                )
                return mc
        finally:
            await pool.close()

    async def get_average_dollar_volume(self, symbol: str, as_of_date, window: int = 30) -> Optional[float]:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                avg_dv = await conn.fetchval(
                    f"""
                    SELECT AVG(close * volume) FROM (
                        SELECT close, volume FROM {self.daily_prices_table}
                        WHERE symbol = $1 AND date <= $2
                        ORDER BY date DESC LIMIT $3
                    ) sub
                    """, symbol, as_of_date, window
                )
                return avg_dv
        finally:
            await pool.close()
