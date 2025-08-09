import os
import argparse
import asyncio
import asyncio
from pathlib import Path
from signals.indicator_config import IndicatorConfig
from signals.indicator import ETop, EBot, PL
from datetime import datetime, timedelta
from app.runner_utils import run_file_daily_price_ohlcv
from market_data.eod.db_daily_price_market_data_manager import DBDailyPriceMarketDataManager


def parse_args():
    parser = argparse.ArgumentParser(description="Run indicator runner for given symbols and date range.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--symbols', nargs='+', help='List of symbols (e.g. AAPL MSFT)')
    group.add_argument('--universe-id', type=int, help='Universe ID to fetch all instrument_ids from universe memberships')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--vendor', default='polygon', choices=['polygon', 'tiingo'], help='Vendor to use for prices')
    parser.add_argument('--data-dir', default=None, help='Base data directory (defaults to ../data/)')
    parser.add_argument('--environment', default='test', choices=['test', 'intg', 'prod'], help='Environment type')
    parser.add_argument('--db-url', default=None, help='Database URL (overrides environment default)')
    parser.add_argument('--gin_config', default=None, help='Path to Gin config file (optional)')
    return parser.parse_args()


from app.runner import Runner

class IndicatorRunner(Runner):
    def __init__(self, start_date, end_date, environment, universe_id=None, symbols=None, vendor='polygon', indicator_config=None, callbacks=None, base_duration='1d'):
        # Parse dates
        from datetime import datetime, date
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        self.symbols = symbols
        self.vendor = vendor
        self.indicator_config = indicator_config
        self.instrument_ids = None
        self.symbol_to_id = None
        self.env = environment
        self.universe_id = universe_id
        self.start_date = start_date
        self.end_date = end_date
        # Resolve instrument_ids
        if universe_id is not None:
            from dao.universe_membership_dao import UniverseMembershipDAO
            membership_dao = UniverseMembershipDAO(environment)
            import asyncio
            active_memberships = asyncio.run(membership_dao.get_active_memberships(universe_id, start_date))
            self.instrument_ids = [row['instrument_id'] for row in active_memberships if row.get('instrument_id') is not None]
            print(f"[DEBUG] instrument_ids from universe_membership: {self.instrument_ids}")
        elif symbols:
            from dao.instrument_xrefs_dao import InstrumentXrefsDAO
            xrefs_dao = InstrumentXrefsDAO(environment)
            import asyncio
            async def resolve_ids():
                mapping = {}
                for s in symbols:
                    iid = await xrefs_dao.resolve_instrument_id_by_symbol(s)
                    mapping[s] = iid
                return mapping
            self.symbol_to_id = asyncio.run(resolve_ids())
            print(f"[DEBUG] symbol_to_id mapping: {self.symbol_to_id}")
            self.instrument_ids = [iid for iid in self.symbol_to_id.values() if iid is not None]
        else:
            self.instrument_ids = []

        # Compose callbacks if needed
        if callbacks is None:
            callbacks = []
        super().__init__(
            start_date=start_date,
            end_date=end_date,
            environment=environment,
            universe_id=universe_id if universe_id is not None else 1,
            callbacks=callbacks,
            base_duration=base_duration,
        )

    async def run_indicators(self):
        # Use Runner's event loop, but print OHLC for each instrument/date
        from datetime import datetime, timedelta
        from market_data.eod.db_daily_price_market_data_manager import DBDailyPriceMarketDataManager
        manager = await DBDailyPriceMarketDataManager.create_async(self.env, symbols=self.symbols)
        if not self.instrument_ids:
            print("[DEBUG] No instrument_ids to process, exiting.")
            return
        print(f"[INFO] Ready to run indicators for instrument_ids: {self.instrument_ids}")
        print(f"[DEBUG] Date range: {self.start_date} to {self.end_date}")
        for instrument_id in self.instrument_ids:
            print(f"[DEBUG] Looping instrument_id: {instrument_id}")
            for single_date in range((self.end_date - self.start_date).days + 1):
                d = self.start_date + timedelta(days=single_date)
                print(f"[DEBUG] Fetching OHLC for instrument_id={instrument_id} on {d}")
                ohlc = await manager.get_ohlc(instrument_id, datetime.combine(d, datetime.min.time()), datetime.combine(d, datetime.min.time()))
                print(f"{d} | instrument_id={instrument_id}: {ohlc}")

if __name__ == "__main__":
    args = parse_args()
    indicator_config = IndicatorConfig(indicators={
        'ETop': ETop,
        'EBot': EBot,
        'PL': PL
    })
    from config.environment import Environment, EnvironmentType
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
        exit(1)
    env_type = env_map[env_key]
    if args.db_url:
        db_url = args.db_url
    elif env_type == EnvironmentType.INTEGRATION:
        db_url = "postgresql://postgres:postgres@localhost:5432/intg_db"
    elif env_type == EnvironmentType.TEST:
        db_url = "postgresql://postgres:postgres@localhost:5432/test_db"
    else:
        db_url = None
    env = Environment(args.gin_config, env_type, db_url)
    env.get_table_name = lambda table: f"{args.environment}_" + table
    runner = IndicatorRunner(
        start_date=args.start_date,
        end_date=args.end_date,
        environment=env,
        universe_id=args.universe_id,
        symbols=args.symbols,
        vendor=args.vendor,
        indicator_config=indicator_config,
        callbacks=[],
        base_duration='1d',
    )
    import asyncio
    asyncio.run(runner.run_indicators())
