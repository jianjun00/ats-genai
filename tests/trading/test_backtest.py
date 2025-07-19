import pytest
import asyncio
from datetime import date, timedelta
import numpy as np
from unittest.mock import AsyncMock, patch

# Dummy TradingUniverse and SecurityMaster for isolated test
class DummyTradingUniverse:
    def __init__(self, universe_by_date):
        self.universe_by_date = universe_by_date
        self.current_date = None
    async def update_for_end_of_day(self, d):
        self.current_date = d
    def get_current_universe(self):
        return self.universe_by_date.get(self.current_date, [])

class DummySecurityMaster:
    def __init__(self, price_data):
        self.price_data = price_data
    async def get_multiple_securities_info(self, tickers, d):
        return {s: {'adjusted_price': self.price_data.get((s, d), None)} for s in tickers}
    async def get_security_info(self, ticker, d):
        val = self.price_data.get((ticker, d), None)
        return {'adjusted_price': val} if val is not None else None

def log_return(p0, p1):
    import math
    if p0 > 0 and p1 > 0:
        return math.log(p1/p0)
    else:
        return 0.0

@pytest.mark.asyncio
async def test_backtest_membership_and_returns():
    # Simulate a 2-day test for 2 stocks
    start_date = date(2024, 1, 2)
    end_date = date(2024, 1, 3)
    data_start = date(2024, 1, 1)
    dates = [data_start, start_date, end_date]
    universe_by_date = {
        start_date: ['AAA', 'BBB'],
        end_date: ['AAA', 'BBB'],
    }
    # Prices: (ticker, date) -> adjusted_price
    price_data = {
        ('AAA', date(2024,1,1)): 100,
        ('AAA', date(2024,1,2)): 110,
        ('AAA', date(2024,1,3)): 121,
        ('BBB', date(2024,1,1)): 200,
        ('BBB', date(2024,1,2)): 180,
        ('BBB', date(2024,1,3)): 198,
    }
    trading_universe = DummyTradingUniverse(universe_by_date)
    security_master = DummySecurityMaster(price_data)
    portfolio = {s: 100 for s in trading_universe.get_current_universe()}
    portfolio_value_history = []
    cum_log_return = 0.0
    for i, d in enumerate(dates):
        if d < start_date:
            continue
        await trading_universe.update_for_end_of_day(d)
        universe = trading_universe.get_current_universe()
        # Remove stocks no longer eligible
        portfolio = {s: portfolio.get(s, 100) for s in universe}
        # Get adjusted prices
        prices = await security_master.get_multiple_securities_info(list(portfolio.keys()), d)
        # Compute daily log returns
        day_returns = []
        for s in portfolio:
            prev_price = await security_master.get_security_info(s, d-timedelta(days=1))
            curr_price = prices.get(s)
            if prev_price and curr_price and prev_price['adjusted_price'] and curr_price['adjusted_price']:
                r = log_return(prev_price['adjusted_price'], curr_price['adjusted_price'])
                day_returns.append(r)
            else:
                day_returns.append(0.0)
        # Portfolio log return = mean of all log returns (equal weight)
        if day_returns:
            portfolio_log_return = np.mean(day_returns)
        else:
            portfolio_log_return = 0.0
        cum_log_return += portfolio_log_return
        portfolio_value_history.append((d, cum_log_return))
    # --- Assertions ---
    # Membership
    assert trading_universe.get_current_universe() == ['AAA', 'BBB']
    # Individual returns
    # Day 1 (2024-01-02): AAA: log(110/100), BBB: log(180/200)
    r_aaa = log_return(100, 110)
    r_bbb = log_return(200, 180)
    # Day 2 (2024-01-03): AAA: log(121/110), BBB: log(198/180)
    r_aaa2 = log_return(110, 121)
    r_bbb2 = log_return(180, 198)
    # Aggregate returns match mean of individual returns per day
    assert np.isclose(portfolio_value_history[0][1], np.mean([r_aaa, r_bbb]))
    assert np.isclose(portfolio_value_history[1][1]-portfolio_value_history[0][1], np.mean([r_aaa2, r_bbb2]))
    # Check final cumulative return
    expected_cum = np.mean([r_aaa, r_bbb]) + np.mean([r_aaa2, r_bbb2])
    assert np.isclose(portfolio_value_history[-1][1], expected_cum)
