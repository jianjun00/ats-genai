import os
import argparse
import asyncio
import asyncio
from pathlib import Path
from signals.indicator_config import IndicatorConfig
from signals.indicator import ETop, EBot, PL
from datetime import datetime, timedelta
from app.runner_utils import run_file_daily_price_ohlcv


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
    parser.add_argument('--output-format', default='df', choices=['df', 'chart'], help='Output format: df (default) or chart')
    parser.add_argument('--output-chart-path', default=None, help='If set, save chart PNG to this path (default: indicator_chart_<symbol>.png)')
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

    async def run_indicators(self, output_format='df', output_chart_path=None):
        import pandas as pd
        import matplotlib.pyplot as plt
        import mplfinance as mpf
        from datetime import datetime, timedelta
        from market_data.eod.unified_db_daily_price_market_data_manager import UnifiedDBDailyPriceMarketDataManager
        manager = await UnifiedDBDailyPriceMarketDataManager.create_async(self.env, symbols=self.symbols)
        if not self.instrument_ids:
            print("[DEBUG] No instrument_ids to process, exiting.")
            return
        print(f"[INFO] Ready to run indicators for instrument_ids: {self.instrument_ids}")
        print(f"[DEBUG] Date range: {self.start_date} to {self.end_date}")
        all_rows = []
        # For each instrument, collect OHLC and indicators
        for instrument_id in self.instrument_ids:
            symbol = None
            if self.symbol_to_id:
                for s, iid in self.symbol_to_id.items():
                    if iid == instrument_id:
                        symbol = s
                        break
            if symbol is None:
                symbol = str(instrument_id)
            for single_date in range((self.end_date - self.start_date).days + 1):
                d = self.start_date + timedelta(days=single_date)
                ohlc = await manager.get_ohlc(instrument_id, datetime.combine(d, datetime.min.time()), datetime.combine(d, datetime.min.time()))
                if ohlc is None:
                    continue
                # Compute indicators
                row = {
                    'date': d,
                    'symbol': symbol,
                    **ohlc
                }
                # Compute ETop, EBot, PL using indicator_config
                # These are expected to be functions/classes in indicator_config.indicators
                for ind_name, ind_cls in (self.indicator_config.indicators.items() if self.indicator_config else []):
                    try:
                        # Expect indicator to be callable: ind_cls(ohlc)
                        row[ind_name] = ind_cls(ohlc) if callable(ind_cls) else None
                    except Exception as e:
                        row[ind_name] = None
                all_rows.append(row)
        if not all_rows:
            print("[WARN] No data collected for the given range and instruments.")
            return
        df = pd.DataFrame(all_rows)
        # Output as DataFrame
        if output_format == 'df':
            print(df.to_string(index=False))
            return
        # Output as chart
        # For each symbol, plot OHLC with overlays
        for symbol, sdf in df.groupby('symbol'):
            sdf = sdf.sort_values('date')
            sdf = sdf.set_index('date')
            if sdf.empty:
                print(f"[WARN] No data for symbol {symbol}, skipping chart.")
                continue
            # Prepare OHLC DataFrame for mplfinance
            mpf_df = sdf[['open', 'high', 'low', 'close']].copy()
            mpf_df.index = pd.DatetimeIndex(mpf_df.index)
            if mpf_df.empty:
                print(f"[WARN] No OHLC data for symbol {symbol}, skipping chart.")
                continue
            # Prepare overlays
            addplots = []
            for ind in ['ETop', 'EBot', 'PL']:
                if ind in sdf.columns:
                    addplots.append(mpf.make_addplot(sdf[ind], panel=0, color={'ETop':'g','EBot':'r','PL':'b'}.get(ind,'k'), width=1.0))
            # Chart file name
            chart_path = output_chart_path or f"indicator_chart_{symbol}.png"
            fig, axlist = mpf.plot(mpf_df, type='candle', style='charles', title=f"{symbol} OHLC with Indicators", addplot=addplots, ylabel='Price', returnfig=True)
            if chart_path:
                try:
                    print(f"[DEBUG] Attempting to save chart to {chart_path}")
                    fig.savefig(chart_path)
                    print(f"[INFO] Chart saved to {chart_path}")
                except Exception as e:
                    print(f"[ERROR] Failed to save chart to {chart_path}: {e}")
            plt.show()

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
    # Pass output_format and output_chart_path to runner
    asyncio.run(runner.run_indicators(output_format=args.output_format, output_chart_path=args.output_chart_path))
