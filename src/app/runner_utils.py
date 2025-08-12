import os
from typing import List, Optional
import pandas as pd
from pathlib import Path
from config.environment import Environment, EnvironmentType
from state.universe_state_manager import UniverseStateManager
from state.universe_state_builder import UniverseStateIntervalBuilder
from market_data.eod.file_daily_price_market_data_manager import FileDailyPriceMarketDataManager
from app.runner import Runner

async def run_file_daily_price_ohlcv(
    vendors_dirs: dict,
    instrument_ids: List[int],
    start_date: str,
    end_date: str,
    env,
    universe_id: int = 1,
    output_dir: Optional[str] = None,
    indicator_config=None,
    print_ohlcv: bool = True,
    required_indicators: Optional[List[str]] = None,
):
    """
    Run the file-based daily price runner and print OHLCV for each symbol/date.
    """
    # Use provided environment
    if indicator_config is not None:
        env.get_indicator_config = lambda: indicator_config

    market_data_manager = await FileDailyPriceMarketDataManager.create_async(vendors_dirs, env)
    universe_state_manager = UniverseStateManager(env=env, base_path=output_dir)
    builder = UniverseStateIntervalBuilder(
        env=env,
        base_duration='1d',
        target_durations='1d'
    )
    builder.universe_state_manager = universe_state_manager
    runner = Runner(
        start_date=start_date,
        end_date=end_date,
        environment=env,
        universe_id=universe_id,
        callbacks=[builder],
        base_duration='1d'
    )
    runner.market_data_manager = market_data_manager
    runner.universe_manager.instrument_ids = instrument_ids
    runner.universe_state_manager = universe_state_manager
    await runner.run()

    # Fetch universe state intervals from DB using DAO
    from dao.universe_state_interval_dao import UniverseStateIntervalDAO
    from datetime import datetime
    dao = UniverseStateIntervalDAO(env)
    universe_id = env.get_universe_id()
    
    # Debug: Print the query parameters
    print(f"[DEBUG][run_file_daily_price_ohlcv] Fetching intervals for universe_id={universe_id}, start_date={start_date}, end_date={end_date}")
    
    intervals = await dao.list(
        universe_id=universe_id,
        start_date_time=start_date,
        end_date_time=end_date
    )
    
    print(f"[DEBUG][run_file_daily_price_ohlcv] Found {len(intervals)} intervals")
    for i, interval in enumerate(intervals):
        print(f"[DEBUG][run_file_daily_price_ohlcv] Interval {i}: {interval}")
    
    if not intervals:
        raise RuntimeError('No universe state intervals found in DB.')
    # Convert intervals to DataFrame (assuming to_dataframe exists or build manually)
    dfs = []
    for idx, interval in enumerate(intervals):
        print(f"[runner_utils] interval idx={idx}, type={type(interval)}")
        assert hasattr(interval, 'to_dataframe'), (
            f"[runner_utils] interval idx={idx} type={type(interval)} does not have .to_dataframe(). Value: {interval}")
        dfs.append(interval.to_dataframe())
    import pandas as pd
    df = pd.concat(dfs, ignore_index=True)
    if df.empty:
        raise RuntimeError('Universe state DataFrame is empty.')

    # Guarantee all requested dates are present for each instrument_id
    from datetime import datetime, timedelta
    all_dates = pd.date_range(start=start_date, end=end_date).date
    instrument_ids_unique = df['instrument_id'].unique()
    # Build full index for all (date, instrument_id) pairs
    full_index = pd.MultiIndex.from_product([all_dates, instrument_ids_unique], names=['start_date_time', 'instrument_id'])
    # If indicator_name exists, include all indicators as well
    if 'indicator_name' in df.columns:
        indicators = df['indicator_name'].unique()
        full_index = pd.MultiIndex.from_product([all_dates, instrument_ids_unique, indicators], names=['start_date_time', 'instrument_id', 'indicator_name'])
        df = df.set_index(['start_date_time', 'instrument_id', 'indicator_name'])
    else:
        df = df.set_index(['start_date_time', 'instrument_id'])
    df = df.reindex(full_index).reset_index()

    # Print OHLCV (and indicators if required)
    ohlc_cols = ['start_date_time', 'instrument_id', 'open', 'high', 'low', 'close', 'volume']
    # If 'volume' is missing, fill with 0
    if 'volume' not in df.columns:
        df['volume'] = 0
    base_df = df[ohlc_cols].drop_duplicates()
    if print_ohlcv:
        for idx, row in base_df.iterrows():
            date = row['start_date_time']
            instrument_id = row['instrument_id']
            open_ = row['open']
            high = row['high']
            low = row['low']
            close = row['close']
            volume = row['volume']
            out = f"date: {date}, instrument_id: {instrument_id}, open: {open_}, high: {high}, low: {low}, close: {close}, volume: {volume}"
            if required_indicators and 'indicator_name' in df.columns:
                indicator_vals = {}
                for ind in required_indicators:
                    val = df[(df['start_date_time'] == date) & (df['instrument_id'] == instrument_id) & (df['indicator_name'] == ind)]['indicator_value']
                    indicator_vals[ind] = val.iloc[0] if not val.empty else None
                ind_str = ', '.join(f"{k}: {v}" for k, v in indicator_vals.items())
                out += f", {ind_str}"
            print(out)
    return df
