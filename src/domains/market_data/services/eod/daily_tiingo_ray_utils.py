import ray
from domains.market_data.services.eod.daily_price_tiingo import fetch_and_insert_symbol, get_instrument_dates
from domains.market_data.repositories.daily_prices_tiingo_dao import DailyPricesTiingoDAO
import aiohttp

@ray.remote
def ray_ingest_instrument(env_dict, instrument_id, symbol, start_date, end_date, ok_status_id, no_data_status_id):
    import asyncio
    return asyncio.run(_ray_ingest_instrument(env_dict, instrument_id, symbol, start_date, end_date, ok_status_id, no_data_status_id))

async def _ray_ingest_instrument(env_dict, instrument_id, symbol, start_date, end_date, ok_status_id, no_data_status_id):
    # Reconstruct Environment and DAO inside Ray task
    from shared.utils.environment import Environment
    env = Environment(
        gin_config_path=env_dict.get('gin_config_path'),
        env_type=env_dict.get('env_type'),
        db_url=env_dict.get('db_url')
    )
    dao = DailyPricesTiingoDAO(env)
    # Ensure any asyncpg pool in DAO uses min_size=1, max_size=1 to avoid TooManyConnectionsError
    async with aiohttp.ClientSession() as session:
        list_date, delist_date = await get_instrument_dates(env, instrument_id)
        if not list_date:
            print(f"[DEBUG] Ray: Skipping {symbol} (no list_date)")
            return symbol, 0
        import datetime
        start_date_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        effective_start = max(list_date, start_date_dt)
        effective_end = delist_date if delist_date else end_date_dt
        if effective_start > effective_end:
            print(f"[DEBUG] Ray: Skipping {symbol} (effective_start {effective_start} > effective_end {effective_end})")
            return symbol, 0
        print(f"[INFO][Ray] Processing {symbol} from {effective_start} to {effective_end}")
        # Throttle: sleep between every symbol ingestion to avoid rate limit
        import asyncio
        await fetch_and_insert_symbol(dao, session, instrument_id, symbol, effective_start, effective_end, ok_status_id, no_data_status_id)
        await asyncio.sleep(2.0)  # 2 seconds between symbols (adjust as needed)
        return symbol, 1
