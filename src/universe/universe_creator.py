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
    parser.add_argument('--universe_table', type=str, default='universe_membership')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    min_price = args.min_price
    min_adv = args.min_adv
    universe_table = args.universe_table

    pool = await asyncpg.create_pool(os.environ['TSDB_URL'])
    symbols_dict = await get_all_symbols(pool)
    symbols = list(symbols_dict.keys())
    universe = set()
    today = start_date
    adv_cache = {}

    # Prepare output table
    async with pool.acquire() as conn:
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
                        f"INSERT INTO {universe_table} (symbol, start_at, end_at) VALUES ($1, $2, NULL)",
                        symbol, today
                    )
                    print(f"[DEBUG] Inserted membership: symbol={symbol}, start_at={today}")
                except Exception as e:
                    print(f"[ERROR] Failed to insert membership for {symbol} on {today}: {e}")
            # Update end_at for removed memberships
            for symbol in to_remove:
                try:
                    await conn.execute(
                        f"UPDATE {universe_table} SET end_at=$1 WHERE symbol=$2 AND end_at IS NULL",
                        today, symbol
                    )
                    print(f"[DEBUG] Updated end_at for symbol={symbol} to {today}")
                except Exception as e:
                    print(f"[ERROR] Failed to update end_at for {symbol} on {today}: {e}")
        today += timedelta(days=1)
    await pool.close()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
