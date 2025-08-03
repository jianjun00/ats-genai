import argparse
import asyncio
import requests
from datetime import datetime, date
from config.environment import set_environment, EnvironmentType
from dao.stock_splits_tiingo_dao import StockSplitsTiingoDAO
import asyncpg

def parse_date(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return datetime.strptime(val, "%Y-%m-%d").date()

def fetch_tiingo_splits(symbol, api_key):
    url = f"https://api.tiingo.com/tiingo/daily/{symbol}/splits?token={api_key}"
    print(f"[DEBUG] Requesting Tiingo splits: {url}")
    resp = requests.get(url)
    print(f"[DEBUG] Response status: {resp.status_code}")
    print(f"[DEBUG] Response headers: {dict(resp.headers)}")
    print(f"[DEBUG] Response body (first 500 chars): {resp.text[:500]}")
    if resp.status_code != 200:
        print(f"Failed to fetch splits for {symbol}: {resp.status_code} {resp.text}")
        return []
    return resp.json()

def map_tiingo_split(split):
    def pd(val):
        return parse_date(split.get(val))
    return {
        'symbol': split.get('ticker'),
        'execution_date': pd('executionDate'),
        'split_from': split.get('fromFactor'),
        'split_to': split.get('toFactor'),
        'cash_amount': split.get('cashAmount'),
        'declaration_date': pd('declarationDate'),
        'payment_date': pd('payDate'),
        'record_date': pd('recordDate'),
        'description': split.get('description'),
        'refid': split.get('id'),
    }

async def get_symbols_from_stock_splits_polygon(env, start_date, end_date):
    db_url = env.get_database_url()
    table_name = env.get_table_name('stock_splits_polygon')
    start = parse_date(start_date)
    end = parse_date(end_date)
    pool = await asyncpg.create_pool(db_url)
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"SELECT DISTINCT symbol FROM {table_name} WHERE execution_date >= $1 AND execution_date <= $2", start, end)
    await pool.close()
    return [row['symbol'] for row in rows]

async def insert_splits_tiingo(splits, dao):
    if not splits:
        print("No splits to insert.")
        return
    inserted = 0
    for split in splits:
        mapped = map_tiingo_split(split)
        if mapped['symbol'] and mapped['execution_date'] and mapped['split_from'] is not None and mapped['split_to'] is not None:
            await dao.insert_split(mapped)
            inserted += 1
    print(f"Inserted {inserted} splits.")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod'], help='Environment to use (test, intg, prod)')
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    set_environment(EnvironmentType(args.environment))
    env = Environment()
    api_key = env.get_api_key('tiingo')
    if not api_key:
        raise Exception("Please set your TIINGO_API_KEY in your environment or config.")
    symbols = await get_symbols_from_stock_splits_polygon(env, args.start_date, args.end_date)
    print(f"Found {len(symbols)} symbols with splits in stock_splits_polygon between {args.start_date} and {args.end_date}")
    splits_dao = StockSplitsTiingoDAO(env)
    for symbol in symbols:
        tiingo_splits = fetch_tiingo_splits(symbol, api_key)
        await insert_splits_tiingo(tiingo_splits, splits_dao)

if __name__ == "__main__":
    asyncio.run(main())
