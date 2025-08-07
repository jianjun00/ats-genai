import os
from typing import List, Optional
import pandas as pd
from pathlib import Path
from config.environment import Environment, EnvironmentType
from state.universe_state_manager import UniverseStateManager
from state.universe_state_builder import UniverseStateBuilder
from market_data.eod.file_daily_price_market_data_manager import FileDailyPriceMarketDataManager
from app.runner import Runner

async def run_file_daily_price_ohlcv(
    vendors_dirs: dict,
    instrument_ids: List[int],
    start_date: str,
    end_date: str,
    db_url: Optional[str] = None,
    universe_id: int = 1,
    output_dir: Optional[str] = None,
    indicator_config=None,
    print_ohlcv: bool = True,
    required_indicators: Optional[List[str]] = None,
):
    """
    Run the file-based daily price runner and print OHLCV for each symbol/date.
    """
    # Setup environment
    env = Environment(env_type=EnvironmentType.TEST, db_url=db_url)
    env.get_table_name = lambda table: f"test_{table}"
    if indicator_config is not None:
        env.get_indicator_config = lambda: indicator_config

    market_data_manager = FileDailyPriceMarketDataManager(vendors_dirs)
    universe_state_manager = UniverseStateManager(env=env, base_path=output_dir)
    builder = UniverseStateBuilder(
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

    # Gather state files
    states_dir = os.path.join(output_dir, 'states')
    state_files = []
    for root, dirs, files in os.walk(states_dir):
        for file in files:
            if file.startswith('universe_state_') and file.endswith('.parquet'):
                state_files.append(os.path.join(root, file))
    if not state_files:
        raise RuntimeError('No universe state files created.')
    dfs = [pd.read_parquet(f) for f in sorted(state_files)]
    df = pd.concat(dfs, ignore_index=True)
    if df.empty:
        raise RuntimeError('Universe state DataFrame is empty.')
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
            if required_indicators:
                indicator_vals = {}
                for ind in required_indicators:
                    val = df[(df['start_date_time'] == date) & (df['instrument_id'] == instrument_id) & (df['indicator_name'] == ind)]['indicator_value']
                    indicator_vals[ind] = val.iloc[0] if not val.empty else None
                ind_str = ', '.join(f"{k}: {v}" for k, v in indicator_vals.items())
                out += f", {ind_str}"
            print(out)
    return df
