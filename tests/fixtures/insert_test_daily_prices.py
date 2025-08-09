import json
import os
from datetime import datetime, date
import asyncio

from src.dao.daily_prices_dao import DailyPricesDAO
from config.environment import Environment, EnvironmentType

async def insert_test_daily_prices(json_path, symbol, instrument_id, unit_test_db):
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    dao = DailyPricesDAO(env=env)
    with open(json_path, 'r') as f:
        data = json.load(f)
    for row in data['results']:
        row_date = datetime.utcfromtimestamp(row['t'] // 1000).date()
        await dao.insert_price(
            date=row_date,
            instrument_id=instrument_id,
            open_=row['o'],
            high=row['h'],
            low=row['l'],
            close=row['c'],
            volume=row['v']
        )

async def main(unit_test_db):
    base = os.path.dirname(__file__)
    data_dir = os.path.join(base, '../data/daily_prices_polygon')
    aapl_path = os.path.join(data_dir, 'polygon_aapl_response.json')
    tsla_path = os.path.join(data_dir, 'polygon_tsla_response.json')
    await insert_test_daily_prices(aapl_path, 'AAPL', 1, unit_test_db)
    await insert_test_daily_prices(tsla_path, 'TSLA', 2, unit_test_db)

if __name__ == "__main__":
    import sys
    db_url = sys.argv[1]
    asyncio.run(main(db_url))
