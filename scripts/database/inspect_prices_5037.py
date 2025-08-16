import asyncio
from dao.daily_prices_dao import DailyPricesDAO
from config.environment import Environment

env = Environment()
dao = DailyPricesDAO(env)

async def main():
    rows = await dao.list_prices(5037)
    print(f"Found {len(rows)} rows for instrument_id=5037:")
    for row in rows:
        print(dict(row))

if __name__ == "__main__":
    asyncio.run(main())
