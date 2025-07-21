import argparse
import asyncpg
import pandas as pd
from datetime import datetime, timedelta
import os

ADV_WINDOW = 20  # days for average daily volume

async def get_all_symbols(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT symbol, delist_date FROM instrument_polygon WHERE list_date IS NOT NULL")
        return {row['symbol']: row['delist_date'] for row in rows}

async def get_daily_metrics(pool, symbol, start_date, end_date):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT date, close, volume FROM daily_prices_tiingo
            WHERE symbol = $1 AND date >= $2 AND date <= $3 AND close IS NOT NULL AND volume IS NOT NULL
            ORDER BY date
            """, symbol, start_date, end_date)
        return pd.DataFrame(rows, columns=['date', 'close', 'volume'])

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, required=True)
    parser.add_argument('--end_date', type=str, required=True)
    parser.add_argument('--min_adv', type=float, required=True)
    parser.add_argument('--min_price', type=float, required=True)

    parser.add_argument('--universe_name', type=str, default='default')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    min_adv = args.min_adv
    min_price = args.min_price

    universe_name = args.universe_name

    pool = await asyncpg.create_pool(os.environ['TSDB_URL'])
    # Look up or create universe_id for the given universe_name
    async with pool.acquire() as conn:
        rec = await conn.fetchrow("SELECT id FROM universe WHERE name=$1", universe_name)
        if rec:
            universe_id = rec['id']
        else:
            # Insert new universe row
            result = await conn.fetchrow(
                "INSERT INTO universe (name, description) VALUES ($1, $2) RETURNING id",
                universe_name, f"Universe {universe_name}"
            )
            universe_id = result['id']
    print(f"[DEBUG] Using universe_id={universe_id} for universe_name={universe_name}")
    symbols_dict = await get_all_symbols(pool)
    symbols = list(symbols_dict.keys())
    universe = set()
    today = start_date
    adv_cache = {}

    # Prepare output table
    async with pool.acquire() as conn:
        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS universe (
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
            df = await get_daily_metrics(pool, symbol, adv_start, today)
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
            # Insert new memberships (added today)
            for symbol in newly_added:
                try:
                    await conn.execute(
                        "INSERT INTO universe_membership (universe_id, symbol, start_at, end_at) VALUES ($1, $2, $3, NULL)",
                        universe_id, symbol, today
                    )
                    print(f"[DEBUG] Inserted membership: universe_id={universe_id}, symbol={symbol}, start_at={today}")
                except Exception as e:
                    print(f"[ERROR] Failed to insert membership for {symbol} on {today}: {e}")
            # Update end_at for removed memberships
            for symbol in to_remove:
                try:
                    await conn.execute(
                        "UPDATE universe_membership SET end_at=$1 WHERE universe_id=$2 AND symbol=$3 AND end_at IS NULL",
                        today, universe_id, symbol
                    )
                    print(f"[DEBUG] Updated end_at for universe_id={universe_id}, symbol={symbol} to {today}")
                except Exception as e:
                    print(f"[ERROR] Failed to update end_at for {symbol} on {today}: {e}")
        today += timedelta(days=1)
    await pool.close()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
