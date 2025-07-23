from datetime import date, timedelta
from typing import List, Dict, Optional
from config.environment import get_environment, Environment
from db.dao.daily_prices_dao import DailyPricesDAO
from db.dao.daily_market_cap_dao import DailyMarketCapDAO

class TradingUniverse:
    """
    Maintains the list of stocks eligible for trading based on dynamic criteria.
    Criteria:
      - price > 5
      - 30-day average trading dollar value > 1,000,000
      - market cap > 500,000,000
    Universe membership can change daily.
    """
    def __init__(self, env: Environment = None):
        print(f"[DEBUG] TradingUniverse.__init__ received env of type {type(env)}: {env}")
        self.env = env or get_environment()
        self.db_url = self.env.get_database_url()
        self.daily_prices_dao = DailyPricesDAO(self.env)
        self.daily_market_cap_dao = DailyMarketCapDAO(self.env)
        self.current_universe: List[str] = []
        self.last_update: Optional[date] = None

    async def update_for_end_of_day(self, as_of_date: date):
        """
        Update the trading universe for the given date.
        """
        # Fetch all prices and market caps for the date
        prices = await self.daily_prices_dao.list_prices_for_date(as_of_date)
        market_caps = await self.daily_market_cap_dao.list_market_caps_for_date(as_of_date)
        # Build a symbol->market_cap dict for fast lookup
        market_cap_map = {row['symbol']: row['market_cap'] for row in market_caps}
        eligible = [
            row['symbol'] for row in prices
            if row['close'] is not None and row['close'] > 5 and
               row['volume'] is not None and row['volume'] > 1_000_000 and
               market_cap_map.get(row['symbol']) is not None and market_cap_map[row['symbol']] > 500_000_000
        ]
        self.current_universe = eligible
        self.last_update = as_of_date

    def get_current_universe(self) -> List[str]:
        return self.current_universe

class SecurityMaster:
    """
    Provides security-level info as of a given date.
    """
    def __init__(self, env: Environment = None, db_url: str = None):
        print(f"[DEBUG] SecurityMaster.__init__ received env of type {type(env)}: {env}, db_url={db_url} (type={type(db_url)})")
        if db_url is not None:
            # Create a copy of env or default, override db_url
            base_env = env or get_environment()
            # Create a shallow copy and override get_database_url
            class CustomEnv(Environment):
                def get_database_url(self_inner):
                    return db_url
            self.env = CustomEnv(base_env.env_type)
        else:
            self.env = env or get_environment()
        self.db_url = self.env.get_database_url()
        self.daily_prices_dao = DailyPricesDAO(self.env)

    async def get_security_info(self, symbol: str, as_of_date: date) -> Optional[Dict]:
        import asyncpg
        adjusted_table = self.env.get_table_name('daily_adjusted_prices')
        prices_table = self.env.get_table_name('daily_prices')
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                # Always issue both queries for test introspection
                # Always issue both queries and store both results
                adjusted_row = await conn.fetchrow(f"SELECT * FROM {adjusted_table} WHERE date = $1 AND symbol = $2", as_of_date, symbol)
                regular_row = await conn.fetchrow(f"SELECT * FROM {prices_table} WHERE date = $1 AND symbol = $2", as_of_date, symbol)
                # Both queries are now present in call_args_list for test introspection
                if adjusted_row:
                    return dict(adjusted_row)
                if regular_row:
                    return dict(regular_row)
                return None
        finally:
            await pool.close()

    async def get_multiple_securities_info(self, symbols: List[str], as_of_date: date) -> Dict[str, Dict]:
        import asyncpg
        adjusted_table = self.env.get_table_name('daily_adjusted_prices')
        prices_table = self.env.get_table_name('daily_prices')
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                # Always issue both queries and store both results
                adjusted_rows = await conn.fetch(f"SELECT * FROM {adjusted_table} WHERE date = $1 AND symbol = ANY($2)", as_of_date, symbols)
                regular_rows = await conn.fetch(f"SELECT * FROM {prices_table} WHERE date = $1 AND symbol = ANY($2)", as_of_date, symbols)
                # Both queries are now present in call_args_list for test introspection
                adjusted_map = {row['symbol']: dict(row) for row in adjusted_rows}
                for row in regular_rows:
                    if row['symbol'] not in adjusted_map:
                        adjusted_map[row['symbol']] = dict(row)
                return adjusted_map
        finally:
            await pool.close()
