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
        self.env = env or get_environment()
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
    def __init__(self, env: Environment = None):
        self.env = env or get_environment()
        self.daily_prices_dao = DailyPricesDAO(self.env)

    async def get_security_info(self, symbol: str, as_of_date: date) -> Optional[Dict]:
        row = await self.daily_prices_dao.get_price(as_of_date, symbol)
        if row:
            return dict(row)
        return None

    async def get_multiple_securities_info(self, symbols: List[str], as_of_date: date) -> Dict[str, Dict]:
        rows = await self.daily_prices_dao.list_prices_for_symbols_and_date(symbols, as_of_date)
        return {row['symbol']: dict(row) for row in rows}
