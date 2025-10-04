import argparse
import asyncio
import requests
from datetime import datetime, date
from core.platform.config_env.environment import Environment, EnvironmentType
from infrastructure.database.repositories.stock_splits_polygon_dao import StockSplitsPolygonDAO

# Table structure reference from migration:
# stock_splits_polygon (
#     id SERIAL PRIMARY KEY,
#     symbol TEXT NOT NULL,
#     execution_date DATE NOT NULL,
#     split_from INTEGER NOT NULL,
#     split_to INTEGER NOT NULL,
#     cash_amount DOUBLE PRECISION,
#     declaration_date DATE,
#     payment_date DATE,
#     record_date DATE,
#     description TEXT,
#     refid TEXT,
#     created_at TIMESTAMPTZ DEFAULT now(),
#     UNIQUE (symbol, execution_date, refid)
# )


def fetch_splits_polygon(start_date, end_date, api_key):
    url = (
        f"https://api.polygon.io/v3/reference/splits?execution_date.gte={start_date}" \
        f"&execution_date.lte={end_date}&apiKey={api_key}"
    )
    print(f"[DEBUG] Requesting Polygon splits: {url}")
    resp = requests.get(url)
    print(f"[DEBUG] Response status: {resp.status_code}")
    print(f"[DEBUG] Response headers: {dict(resp.headers)}")
    print(f"[DEBUG] Response body (first 500 chars): {resp.text[:500]}")
    if resp.status_code != 200:
        print(f"Failed to fetch splits: {resp.status_code} {resp.text}")
        return []
    data = resp.json()
    return data.get('results', [])


def parse_date(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return datetime.strptime(val, "%Y-%m-%d").date()


async def insert_splits_polygon(splits, dao):
    if not splits:
        print("No splits to insert.")
        return
    inserted = 0
    for split in splits:
        split_row = {
            'symbol': split.get('ticker'),
            'execution_date': parse_date(split.get('execution_date')),
            'split_from': int(split.get('split_from')) if split.get('split_from') else None,
            'split_to': int(split.get('split_to')) if split.get('split_to') else None,
            'cash_amount': float(split.get('cash_amount')) if split.get('cash_amount') is not None else None,
            'declaration_date': parse_date(split.get('declaration_date')),
            'payment_date': parse_date(split.get('payment_date')),
            'record_date': parse_date(split.get('record_date')),
            'description': split.get('description'),
            'refid': split.get('refid'),
        }
        # Only insert if required fields are present
        if split_row['symbol'] and split_row['execution_date'] and split_row['split_from'] is not None and split_row['split_to'] is not None:
            await dao.insert_split(split_row)
            inserted += 1
    print(f"Inserted {inserted} splits.")

from datetime import timedelta

def date_chunks(start_date, end_date, chunk_days=5):
    start = parse_date(start_date)
    end = parse_date(end_date)
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days-1), end)
        yield current.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')
        current = chunk_end + timedelta(days=1)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod'], help='Environment to use (test, intg, prod)')
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    env = Environment(env_type=EnvironmentType(args.environment))
    api_key = env.get_api_key('polygon')
    if not api_key:
        raise Exception("Please set your POLYGON_API_KEY in your environment or config.")
    all_splits = []
    for chunk_start, chunk_end in date_chunks(args.start_date, args.end_date, chunk_days=5):
        print(f"[DEBUG] Fetching splits for {chunk_start} to {chunk_end}")
        chunk_splits = fetch_splits_polygon(chunk_start, chunk_end, api_key)
        print(f"[DEBUG] Fetched {len(chunk_splits)} splits for {chunk_start} to {chunk_end}")
        all_splits.extend(chunk_splits)
    print(f"Fetched {len(all_splits)} total splits from Polygon API.")
    splits_dao = StockSplitsPolygonDAO(env)
    await insert_splits_polygon(all_splits, splits_dao)


if __name__ == "__main__":
    asyncio.run(main())
