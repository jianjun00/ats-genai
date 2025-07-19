import argparse
import asyncio
from datetime import datetime, timedelta
import numpy as np
import asyncpg
from trading.trading_universe import TradingUniverse, SecurityMaster
import math

async def fetch_spy_members(as_of_date, db_url):
    pool = await asyncpg.create_pool(db_url)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT added FROM spy_membership_change WHERE event_date <= $1
            AND (removed IS NULL OR event_date > $1)
        """, as_of_date)
    await pool.close()
    return [row['added'] for row in rows if row['added']]

def log_return(p0, p1):
    if p0 > 0 and p1 > 0:
        return math.log(p1/p0)
    else:
        return 0.0

async def run_backtest(args):
    db_url = args.db_url
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    data_start = start_date - timedelta(days=args.data_start_days)

    trading_universe = TradingUniverse(db_url)
    security_master = SecurityMaster(db_url)

    # Warm up: build state up to start_date
    # (No trades, just for rolling calculations)
    # At start_date, determine eligible universe
    await trading_universe.update_for_end_of_day(start_date)
    portfolio = {s: 100 for s in trading_universe.get_current_universe()}
    portfolio_value_history = []
    cum_log_return = 0.0
    dates = [data_start + timedelta(days=i) for i in range((end_date-data_start).days+1)]

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
            # Use adjusted price for return calculation
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
    # Calculate stats
    returns = np.array([x[1] for x in portfolio_value_history])
    daily_returns = np.diff(returns)
    n_years = (end_date - start_date).days / 365.25
    avg_yearly_return = returns[-1] / n_years if n_years > 0 else 0.0
    max_drawdown = np.min(returns - np.maximum.accumulate(returns))
    avg_vol = np.std(daily_returns) * np.sqrt(252)
    sharpe = avg_yearly_return / avg_vol if avg_vol > 0 else 0.0
    print(f"Average yearly return: {avg_yearly_return:.4f}")
    print(f"Max drawdown: {max_drawdown:.4f}")
    print(f"Average yearly volatility: {avg_vol:.4f}")
    print(f"Sharpe ratio: {sharpe:.4f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", type=str, required=True)
    parser.add_argument("--end_date", type=str, required=True)
    parser.add_argument("--data_start_days", type=int, default=30)
    parser.add_argument("--db_url", type=str, default="postgresql://postgres:postgres@localhost:5432/trading_db")
    args = parser.parse_args()
    asyncio.run(run_backtest(args))
