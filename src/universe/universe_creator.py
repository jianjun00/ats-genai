import argparse
import asyncpg
import pandas as pd
from datetime import datetime, timedelta
import os
from config import get_environment, Environment, EnvironmentType

ADV_WINDOW = 20  # days for average daily volume

async def get_all_symbols(pool, env):
    async with pool.acquire() as conn:
        table = env.get_table_name('instrument_polygon')
        rows = await conn.fetch(f"SELECT symbol, delist_date FROM {table} WHERE list_date IS NOT NULL")
        return {row['symbol']: row['delist_date'] for row in rows}

async def get_daily_metrics(pool, symbol, start_date, end_date, env):
    async with pool.acquire() as conn:
        table = env.get_table_name('daily_prices_tiingo')
        rows = await conn.fetch(
            f"SELECT date, close, volume FROM {table} "
            f"WHERE symbol = $1 AND date >= $2 AND date <= $3 AND close IS NOT NULL AND volume IS NOT NULL "
            f"ORDER BY date",
            symbol, start_date, end_date)
        return pd.DataFrame(rows, columns=['date', 'close', 'volume'])

async def create_universe_membership(
    start_date,
    end_date,
    min_adv,
    min_price,
    universe_name='default',
    env=None,
    pool=None,
):
    """
    Main business logic for creating/updating universe membership.
    Parameters:
        start_date (date): Start date for universe calculation
        end_date (date): End date for universe calculation
        min_adv (float): Minimum average daily volume
        min_price (float): Minimum price
        universe_name (str): Universe name
        env (Environment): Environment object (if None, auto-detect)
        pool (asyncpg.Pool): Database pool (if None, create from TSDB_URL)
    """
    if env is None:
        env = get_environment()
    if pool is None:
        pool = await asyncpg.create_pool(os.environ['TSDB_URL'])
        close_pool = True
    else:
        close_pool = False

    # Look up or create universe_id for the given universe_name
    async with pool.acquire() as conn:
        universe_table = env.get_table_name('universe')
        rec = await conn.fetchrow(f"SELECT id FROM {universe_table} WHERE name=$1", universe_name)
        if rec:
            universe_id = rec['id']
        else:
            # Insert new universe row
            result = await conn.fetchrow(
                f"INSERT INTO {universe_table} (name, description) VALUES ($1, $2) RETURNING id",
                universe_name, f"Universe {universe_name}"
            )
            universe_id = result['id']
    print(f"[DEBUG] Using universe_id={universe_id} for universe_name={universe_name}")

    # Use get_all_symbols with env
    symbols_dict = await get_all_symbols(pool, env)
    symbols = list(symbols_dict.keys())
    universe = set()
    today = start_date
    adv_cache = {}

    # Prepare output table (not used in test, but kept for compatibility)
    async with pool.acquire() as conn:
        universe_table = env.get_table_name('universe')
        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {universe_table} (
            date DATE NOT NULL,
            symbol TEXT NOT NULL,
            PRIMARY KEY (date, symbol)
        )
        """)

    while today <= end_date:
        print(f"[INFO] Processing universe for {today}")
        newly_added = set()
        to_remove = set()
        for symbol in symbols:
            delist_date = symbols_dict[symbol]
            if delist_date and today > delist_date:
                if symbol in universe:
                    to_remove.add(symbol)
                continue
            # Get ADV window
            adv_start = today - timedelta(days=ADV_WINDOW*2)  # buffer for missing days
            # Use get_daily_metrics with env
            df = await get_daily_metrics(pool, symbol, adv_start, today, env)
            if df.empty or today not in set(df['date']):
                if symbol in universe:
                    to_remove.add(symbol)
                continue
            df = df.sort_values('date')
            # Calculate ADV over last ADV_WINDOW days up to today
            recent = df[df['date'] <= today].tail(ADV_WINDOW)
            print(f"[DEBUG] {symbol}: recent={recent}")
            if len(recent) < ADV_WINDOW:
                if symbol in universe:
                    to_remove.add(symbol)
                continue
            adv = recent['volume'].mean()
            close = recent.iloc[-1]['close']
            if close >= min_price and adv >= min_adv:
                if symbol not in universe:
                    newly_added.add(symbol)
            else:
                if symbol in universe:
                    to_remove.add(symbol)
        # Debugging output
        print(f"[DEBUG] {today}: newly_added={newly_added}, to_remove={to_remove}")
        print(f"[DEBUG] {today}: universe before update={universe}")
        universe.update(newly_added)
        universe.difference_update(to_remove)
        print(f"[DEBUG] {today}: universe after update={universe}")
        # Save to DB using start_at/end_at interval logic
        async with pool.acquire() as conn:
            membership_table = env.get_table_name('universe_membership')
            # Insert new memberships (added today)
            for symbol in newly_added:
                try:
                    await conn.execute(
                        f"INSERT INTO {membership_table} (universe_id, symbol, start_at, end_at) VALUES ($1, $2, $3, NULL)",
                        universe_id, symbol, today
                    )
                    print(f"[DEBUG] Inserted membership: universe_id={universe_id}, symbol={symbol}, start_at={today}")
                except Exception as e:
                    print(f"[ERROR] Failed to insert membership for {symbol} on {today}: {e}")
            # Update end_at for removed memberships
            for symbol in to_remove:
                try:
                    await conn.execute(
                        f"UPDATE {membership_table} SET end_at=$1 WHERE universe_id=$2 AND symbol=$3 AND end_at IS NULL",
                        today, universe_id, symbol
                    )
                    print(f"[DEBUG] Updated end_at for universe_id={universe_id}, symbol={symbol} to {today}")
                except Exception as e:
                    print(f"[ERROR] Failed to update end_at for {symbol} on {today}: {e}")
        today += timedelta(days=1)
    if close_pool:
        await pool.close()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, required=True)
    parser.add_argument('--end_date', type=str, required=True)
    parser.add_argument('--min_adv', type=float, required=True)
    parser.add_argument('--min_price', type=float, required=True)
    parser.add_argument('--universe_name', type=str, default='default')
    parser.add_argument('--environment', type=str, default=None, help='Environment: prod, intg, or test (for table prefixing)')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    min_adv = args.min_adv
    min_price = args.min_price
    universe_name = args.universe_name
    env_type = args.environment.lower() if args.environment else None
    env = Environment(EnvironmentType(env_type)) if env_type else get_environment()

    await create_universe_membership(
        start_date=start_date,
        end_date=end_date,
        min_adv=min_adv,
        min_price=min_price,
        universe_name=universe_name,
        env=env,
        pool=None,
    )


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
