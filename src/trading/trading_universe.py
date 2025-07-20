import asyncpg
from datetime import date, timedelta
from typing import List, Dict, Optional

class TradingUniverse:
    """
    Maintains the list of stocks eligible for trading based on dynamic criteria.
    Criteria:
      - price > 5
      - 30-day average trading dollar value > 1,000,000
      - market cap > 500,000,000
    Universe membership can change daily.
    """
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.current_universe: List[str] = []
        self.last_update: Optional[date] = None

    async def update_for_end_of_day(self, as_of_date: date):
        """
        Update the trading universe for the given date.
        """
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            # Get all stocks with price, adv, and market cap as of as_of_date
            rows = await conn.fetch('''
                SELECT symbol, close, adv, market_cap
                FROM daily_prices
                WHERE date = $1
            ''', as_of_date)
            eligible = [
                row['symbol'] for row in rows
                if row['close'] is not None and row['close'] > 5 and
                   row['adv'] is not None and row['adv'] > 1_000_000 and
                   row['market_cap'] is not None and row['market_cap'] > 500_000_000
            ]
            self.current_universe = eligible
            self.last_update = as_of_date
        await pool.close()

    def get_current_universe(self) -> List[str]:
        return self.current_universe

class SecurityMaster:
    """
    Provides security-level info as of a given date.
    """
    def __init__(self, db_url: str):
        self.db_url = db_url

    async def get_security_info(self, symbol: str, as_of_date: date) -> Optional[Dict]:
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT dap.symbol, dap.close AS adjusted_price, dp.close, dp.adv, dp.market_cap
                FROM daily_adjusted_prices dap
                JOIN daily_prices dp ON dap.symbol = dp.symbol AND dap.date = dp.date
                WHERE dap.symbol = $1 AND dap.date = $2
            ''', symbol, as_of_date)
        await pool.close()
        if row:
            return dict(row)
        return None

    async def get_multiple_securities_info(self, symbols: List[str], as_of_date: date) -> Dict[str, Dict]:
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT dap.symbol, dap.close AS adjusted_price, dp.close, dp.adv, dp.market_cap
                FROM daily_adjusted_prices dap
                JOIN daily_prices dp ON dap.symbol = dp.symbol AND dap.date = dp.date
                WHERE dap.symbol = ANY($1::text[]) AND dap.date = $2
            ''', symbols, as_of_date)
        await pool.close()
        return {row['symbol']: dict(row) for row in rows}
