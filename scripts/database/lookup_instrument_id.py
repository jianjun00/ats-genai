import asyncio
from config.environment import get_environment
from dao.instrument_xrefs_dao import InstrumentXrefsDAO

async def main():
    env = get_environment()
    dao = InstrumentXrefsDAO(env)
    for symbol in ["AAPL", "TSLA"]:
        iid = await dao.resolve_instrument_id(symbol)
        print(f"{symbol} instrument_id: {iid}")

if __name__ == "__main__":
    asyncio.run(main())
