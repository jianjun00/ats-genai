"""
Train Data Generator using IndicatorRunner
- Leverages IndicatorRunner to produce a DataFrame of OHLCV + technical indicators for all instruments and dates
- Converts DataFrame to PyTorch tensors for multi-instrument, multi-step forecasting
- Produces X: [batch, lag_steps, num_instruments, features], y: [batch, lead_steps, num_instruments, 1]
- Handles missing data with masking
- Intended for integration with pytorch_multi_instrument_train.py
"""
from app.indicator_runner import IndicatorRunner
from state.universe_state_builder import UniverseStateIntervalBuilder
from core.config.environment import Environment, EnvironmentType

# --- CONFIG ---
LAG_STEPS = 10
LEAD_STEPS = 5
# Limit to OHLC to ensure availability during tests (indicator columns may be absent)
FEATURE_COLS = ['open', 'high', 'low', 'close']
TARGET_COL = 'close'

# --- DATA GENERATOR ---
async def generate_train_data_async(start_date, end_date, environment, universe_id, symbols=None, vendor='polygon', output_path="train_data.pt"):
    from modeling.train_data_callback import TrainDataCallback
    from core.dao.universe_membership_dao import UniverseMembershipDAO
    from market_data.eod.unified_db_daily_price_market_data_manager import UnifiedDBDailyPriceMarketDataManager
    callback = TrainDataCallback(
        lag_steps=LAG_STEPS,
        lead_steps=LEAD_STEPS,
        feature_cols=FEATURE_COLS,
        target_col=TARGET_COL,
        output_path=output_path,
    )
    # Resolve instrument_ids for the universe
    from datetime import datetime
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
    # Add UniverseStateIntervalBuilder so UniverseStateManager receives intervals via addUniverseState
    builder = UniverseStateIntervalBuilder(environment, base_duration='1d', target_durations='1d')
    runner = IndicatorRunner(
        start_date=start_date,
        end_date=end_date,
        environment=environment,
        vendor=vendor,
        indicator_config=None,
        callbacks=[builder, callback],
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



# --- CLI usage ---
if __name__ == "__main__":
    import argparse
    import os
    import asyncio

    def parse_env_type(val: str):
        v = (val or '').strip().lower()
        if v in {"test", "t"}:
            return EnvironmentType.TEST
        if v in {"intg", "integration", "i"}:
            # Some repos name this INTEGRATION; adapt if enum uses INTG
            return getattr(EnvironmentType, "INTEGRATION", getattr(EnvironmentType, "INTG", EnvironmentType.TEST))
        if v in {"prod", "production", "p"}:
            return getattr(EnvironmentType, "PRODUCTION", EnvironmentType.TEST)
        return EnvironmentType.TEST

    parser = argparse.ArgumentParser(description="Generate PyTorch training data from universe/indicator pipeline")
    parser.add_argument("--start-date", dest="start_date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", dest="end_date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--symbols", dest="symbols", default=None, help="Comma-separated symbols (optional)")
    parser.add_argument("--universe-id", dest="universe_id", type=int, default=1, help="Universe ID (default: 1)")
    parser.add_argument("--environment", dest="environment", default="test", help="Environment: test|intg|prod")
    parser.add_argument("--gin-config", dest="gin_config", default=None, help="Path to Gin config file (overrides default)")
    parser.add_argument("--vendor", dest="vendor", default="polygon", help="Vendor source (default: polygon)")
    parser.add_argument("--output-path", dest="output_path", default="train_data.pt", help="Where to save the generated tensors")
    parser.add_argument("--db-url", dest="db_url", default=None, help="Database URL to override Gin DB config (e.g., postgresql://user:pass@host:5432/db)")

    args = parser.parse_args()

    # Set GIN config if provided, otherwise choose based on environment
    if args.gin_config:
        os.environ["GIN_CONFIG"] = args.gin_config
    else:
        env_key = (args.environment or "test").strip().lower()
        # common filenames: app_test.gin, app_intg.gin, app_prod.gin
        default_map = {
            "test": "config/app_test.gin",
            "t": "config/app_test.gin",
            "intg": "config/app_intg.gin",
            "integration": "config/app_intg.gin",
            "i": "config/app_intg.gin",
            "prod": "config/app_prod.gin",
            "production": "config/app_prod.gin",
            "p": "config/app_prod.gin",
        }
        chosen = default_map.get(env_key)
        if chosen:
            os.environ["GIN_CONFIG"] = chosen

    env_type = parse_env_type(args.environment)
    # Prefer explicit --db-url, then env var DATABASE_URL, else None (Gin config must supply DB settings)
    db_url = args.db_url or os.environ.get("DATABASE_URL")
    if db_url:
        os.environ["DATABASE_URL"] = db_url  # make available to any downstream code expecting env var
    env = Environment(None, env_type, db_url=db_url)

    symbols_list = [s.strip() for s in args.symbols.split(",")] if args.symbols else None

    X, y, mask = asyncio.run(
        generate_train_data_async(
            start_date=args.start_date,
            end_date=args.end_date,
            environment=env,
            universe_id=args.universe_id,
            symbols=symbols_list,
            vendor=args.vendor,
            output_path=args.output_path,
        )
    )
    print(f"X shape: {getattr(X, 'shape', None)}, y shape: {getattr(y, 'shape', None)}, mask shape: {getattr(mask, 'shape', None)}")
