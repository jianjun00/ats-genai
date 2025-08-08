import os
import argparse
import asyncio
from pathlib import Path
from signals.indicator_config import IndicatorConfig
from signals.indicator import ETop, EBot, PL
from datetime import datetime
from app.runner_utils import run_file_daily_price_ohlcv
from market_data.eod.file_daily_price_market_data_manager import FileDailyPriceMarketDataManager


def parse_args():
    parser = argparse.ArgumentParser(description="Run indicator runner for given symbols and date range.")
    parser.add_argument('--symbols', nargs='+', required=True, help='List of symbols (e.g. AAPL MSFT)')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--vendor', default='polygon', choices=['polygon', 'tiingo'], help='Vendor to use for prices')
    parser.add_argument('--data-dir', default=None, help='Base data directory (defaults to ../data/)')
    parser.add_argument('--environment', default='test', choices=['test', 'intg', 'prod'], help='Environment type')
    parser.add_argument('--db-url', default=None, help='Database URL (overrides environment default)')
    parser.add_argument('--gin_config', default=None, help='Path to Gin config file (optional)')
    return parser.parse_args()


def main():
    args = parse_args()
    base_data_dir = args.data_dir or os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data'))
    vendors_dirs = {
        'polygon': os.path.join(base_data_dir, 'daily_prices_polygon'),
        'tiingo': os.path.join(base_data_dir, 'daily_prices_tiingo'),
    }
    # Only use the selected vendor for now
    vendors_dirs = {args.vendor: vendors_dirs[args.vendor]}

    indicator_config = IndicatorConfig(indicators={
        'ETop': ETop,
        'EBot': EBot,
        'PL': PL
    })
    output_dir = os.path.join(os.getcwd(), 'indicator_runner_output')
    os.makedirs(output_dir, exist_ok=True)
    # Setup environment
    from config.environment import Environment, EnvironmentType
    # Robust mapping from CLI arg to EnvironmentType
    env_map = {
        'test': EnvironmentType.TEST,
        'intg': EnvironmentType.INTEGRATION,
        'integration': EnvironmentType.INTEGRATION,
        'prod': EnvironmentType.PRODUCTION,
        'production': EnvironmentType.PRODUCTION
    }
    env_key = args.environment.lower()
    if env_key not in env_map:
        print(f"[ERROR] Unknown environment: {args.environment}. Supported: test, intg, prod")
        return
    env_type = env_map[env_key]
    # Determine correct db_url based on environment, unless overridden by --db_url
    if args.db_url:
        db_url = args.db_url
    elif env_type == EnvironmentType.INTEGRATION:
        db_url = "postgresql://postgres:postgres@localhost:5432/intg_db"  # Integration DB
    elif env_type == EnvironmentType.TEST:
        db_url = "postgresql://postgres:postgres@localhost:5432/test_db"  # Test DB
    else:
        db_url = None
    env = Environment(args.gin_config, env_type, db_url)
    env.get_table_name = lambda table: f"{args.environment}_" + table
    # Run
    from datetime import datetime, date
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    assert isinstance(start_date, date)
    assert isinstance(end_date, date)
    # Fetch instrument_ids from universe_membership
    from dao.universe_membership_dao import UniverseMembershipDAO
    universe_id = 1  # hardcoded or from args if needed
    membership_dao = UniverseMembershipDAO(env)
    import asyncio
    active_memberships = asyncio.run(membership_dao.get_active_memberships(universe_id, start_date))
    instrument_ids = [row['instrument_id'] for row in active_memberships if row.get('instrument_id') is not None]
    print(f"[DEBUG] instrument_ids from universe_membership: {instrument_ids}")
    if not instrument_ids:
        print(f"No instrument IDs found in universe_membership for universe_id={universe_id} as_of={start_date}")
        return
    asyncio.run(run_file_daily_price_ohlcv(
        vendors_dirs=vendors_dirs,
        instrument_ids=instrument_ids,
        start_date=start_date,
        end_date=end_date,
        env=env,
        universe_id=universe_id,
        output_dir=output_dir,
        indicator_config=indicator_config,
        print_ohlcv=True,
        required_indicators=['ETop', 'EBot', 'PL']
    ))

if __name__ == "__main__":
    main()
