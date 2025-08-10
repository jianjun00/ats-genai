"""
Train Data Generator using IndicatorRunner
- Leverages IndicatorRunner to produce a DataFrame of OHLCV + technical indicators for all instruments and dates
- Converts DataFrame to PyTorch tensors for multi-instrument, multi-step forecasting
- Produces X: [batch, lag_steps, num_instruments, features], y: [batch, lead_steps, num_instruments, 1]
- Handles missing data with masking
- Intended for integration with pytorch_multi_instrument_train.py
"""
import torch
import numpy as np
import pandas as pd
from app.indicator_runner import IndicatorRunner
from config.environment import Environment, EnvironmentType

# --- CONFIG ---
LAG_STEPS = 30
LEAD_STEPS = 7
FEATURE_COLS = ['open', 'high', 'low', 'close', 'etop', 'ebot', 'pldot']
TARGET_COL = 'close'

# --- DATA GENERATOR ---
async def generate_train_data_async(start_date, end_date, environment, universe_id, symbols=None, vendor='polygon', output_path="train_data.pt"):
    from examples.train_data_callback import TrainDataCallback
    from src.dao.universe_membership_dao import UniverseMembershipDAO
    from market_data.eod.unified_db_daily_price_market_data_manager import UnifiedDBDailyPriceMarketDataManager
    callback = TrainDataCallback(
        lag_steps=LAG_STEPS,
        lead_steps=LEAD_STEPS,
        feature_cols=FEATURE_COLS,
        target_col=TARGET_COL,
        output_path=output_path,
    )
    # Resolve instrument_ids for the universe
    from datetime import datetime, date
    membership_dao = UniverseMembershipDAO(environment)
    if isinstance(start_date, str):
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    else:
        start_date_dt = start_date
    active_memberships = await membership_dao.get_active_memberships(universe_id, start_date_dt)
    print(f"[DEBUG][generate_train_data_async] active_memberships at {start_date_dt}: {active_memberships}")
    instrument_ids = [row['instrument_id'] for row in active_memberships if row.get('instrument_id') is not None]
    symbols_from_memberships = [row['symbol'] for row in active_memberships if row.get('symbol')]
    # Create unified DB daily price market data manager (async) and inject into runner
    manager = await UnifiedDBDailyPriceMarketDataManager.create_async(environment, symbols=symbols_from_memberships or symbols)
    runner = IndicatorRunner(
        start_date=start_date,
        end_date=end_date,
        environment=environment,
        vendor=vendor,
        indicator_config=None,
        callbacks=[callback],
        base_duration='1d',
        market_data_manager=manager,
    )
    runner.instrument_ids = instrument_ids
    await runner.run()
    import torch, os
    print(f"[DEBUG][generate_train_data_async] CWD: {os.getcwd()}")
    print(f"[DEBUG][generate_train_data_async] About to load output_path: {output_path} (abs: {os.path.abspath(output_path)})")
    print(f"[DEBUG][generate_train_data_async] File exists before load: {os.path.exists(output_path)}")
    data = torch.load(output_path)
    print(f"[DEBUG][generate_train_data_async] Successfully loaded file: {output_path}")
    return data['X'], data['y'], data['mask']


async def generate_train_data(*args, **kwargs):
    return await generate_train_data_async(*args, **kwargs)



# --- Example usage ---
if __name__ == "__main__":
    # Example: test environment, universe_id=1
    env = Environment(None, EnvironmentType.TEST, db_url=None)
    X, y, mask = generate_train_data(
        start_date="2024-01-01",
        end_date="2024-03-31",
        environment=env,
        universe_id=1,
        symbols=None,
        vendor='polygon',
    )
    print(f"X shape: {X.shape}, y shape: {y.shape}, mask shape: {mask.shape}")
