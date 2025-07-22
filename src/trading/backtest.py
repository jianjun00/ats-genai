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

from universe.universe_db import UniverseDB

async def run_backtest(args):
    db_url = args.db_url
    universe_name = getattr(args, 'universe_name', 'TEST_UNIVERSE')
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    data_start = start_date - timedelta(days=args.data_start_days)

    security_master = SecurityMaster(db_url)
    universe_db = UniverseDB(db_url)
    universe_id = await universe_db.get_universe_id(universe_name)
    if universe_id is None:
        raise ValueError(f"Universe '{universe_name}' not found in DB.")

    portfolio_value_history = []
    cum_log_return = 0.0
    dates = [data_start + timedelta(days=i) for i in range((end_date-data_start).days+1)]
    portfolio = None

    from trading.market_data_manager import MarketDataManager
    from state.universe_state_builder import UniverseStateBuilder
    from state.universe_interval import UniverseInterval
    from state.instrument_interval import InstrumentInterval
    from trading.indicator import UniverseState
    
    # Stubs for ModelManager, Optimizer, ExecutionManager
    class ModelManager:
        def forecast(self, universe_state):
            # Return dicts: instrument_id -> forecasted_return/vol/volume
            return {}, {}, {}
    class Optimizer:
        def optimize(self, forecasts, current_portfolio):
            # Return next portfolio (instrument_id -> position)
            return current_portfolio.copy()
    class ExecutionManager:
        def execute(self, current_portfolio, next_portfolio, open_prices):
            # Fill trades at open + cost, return new portfolio
            cost = 0.001  # 0.1% transaction cost
            trades = {iid: next_portfolio.get(iid,0) - current_portfolio.get(iid,0) for iid in set(current_portfolio)|set(next_portfolio)}
            return next_portfolio.copy(), cost*sum(abs(qty) for qty in trades.values())

    market_data = MarketDataManager()
    # Create a temporary universe for the state builder
    from trading.universe import Universe
    temp_universe = Universe(current_date=start_date, instrument_ids=[])
    state_builder = UniverseStateBuilder(temp_universe)
    model_manager = ModelManager()
    optimizer = Optimizer()
    execution_manager = ExecutionManager()

    # Simulation loop
    portfolio = {}
    daily_returns = []
    for d in dates:
        # 1. Get universe for the day
        members = await universe_db.get_universe_members(universe_id, d)
        if not members:
            continue
        # 2. Update market data manager with OHLC for each instrument
        ohlc_batch = market_data.get_ohlc_batch(members, d, d)
        # 3. Build universe state for this day
        instrument_intervals = {}
        for iid in members:
            ohlc = ohlc_batch.get(iid) or {'open':0,'high':0,'low':0,'close':0}
            instrument_intervals[iid] = InstrumentInterval(
                instrument_id=iid,
                start_date_time=d,
                end_date_time=d,
                open=ohlc['open'], high=ohlc['high'], low=ohlc['low'], close=ohlc['close'],
                traded_volume=0, traded_dollar=0, status=None)
        universe_interval = UniverseInterval(
            start_date_time=d,
            end_date_time=d,
            instrument_intervals=instrument_intervals
        )
        state_builder.add_interval(universe_interval)
        universe_state = state_builder.build()
        # 4. ModelManager generates forecasts
        ret_f, vol_f, volu_f = model_manager.forecast(universe_state)
        # 5. Optimizer generates next portfolio
        next_portfolio = optimizer.optimize({'return': ret_f, 'vol': vol_f, 'volume': volu_f}, portfolio)
        # 6. ExecutionManager fills trades at next open + cost
        open_prices = {iid: instrument_intervals[iid].open for iid in next_portfolio}
        portfolio, tx_cost = execution_manager.execute(portfolio, next_portfolio, open_prices)
        # 7. Update prices for splits/dividends (stub)
        # (implement split/dividend logic here if needed)
        # 8. Compute portfolio return
        prev_value = sum(portfolio.get(iid,0) * instrument_intervals[iid].open for iid in portfolio)
        curr_value = sum(portfolio.get(iid,0) * instrument_intervals[iid].close for iid in portfolio)
        daily_ret = (curr_value - prev_value - tx_cost) / (prev_value + 1e-8) if prev_value > 0 else 0.0
        daily_returns.append(daily_ret)
        print(f"{d}: Portfolio return={daily_ret:.5f}, tx_cost={tx_cost:.5f}")
    # Summary
    avg_return = sum(daily_returns)/len(daily_returns) if daily_returns else 0.0
    print(f"Average daily return: {avg_return:.5f}")
    if daily_returns:
        portfolio_log_return = np.mean(daily_returns)
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
    parser.add_argument("--universe_name", type=str, default="TEST_UNIVERSE", help="Universe name to backtest (from universe table)")
    args = parser.parse_args()
    asyncio.run(run_backtest(args))
