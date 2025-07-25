import argparse
import asyncio
from config.environment import get_environment, set_environment, EnvironmentType
from dao.instrument_polygon_dao import InstrumentPolygonDAO
from dao.instruments_dao import InstrumentsDAO
from dao.universe_membership_dao import UniverseMembershipDAO

async def main(environment: str, tickers=None, universe_id=None):
    set_environment(EnvironmentType(environment))
    env = get_environment()
    polygon_dao = InstrumentPolygonDAO(env)
    instruments_dao = InstrumentsDAO(env)
    membership_dao = UniverseMembershipDAO(env)

    # Determine which tickers to copy
    tickers_to_copy = set()
    if tickers:
        tickers_to_copy.update(tickers)
    if universe_id:
        memberships = await membership_dao.get_memberships_by_universe(universe_id)
        tickers_to_copy.update([row['symbol'] for row in memberships])

    if not tickers_to_copy:
        print("[WARN] No tickers specified or found in universe. Nothing to copy.")
        return

    # Copy each instrument from polygon to instruments
    for symbol in tickers_to_copy:
        instrument = await polygon_dao.get_instrument(symbol)
        if not instrument:
            print(f"[WARN] Instrument {symbol} not found in polygon table.")
            continue
        # Insert or update in instruments table
        await instruments_dao.create_instrument(
            symbol=instrument['symbol'],
            name=instrument.get('name'),
            exchange=instrument.get('exchange'),
            type_=instrument.get('type'),
            currency=instrument.get('currency')
        )
        print(f"[INFO] Copied {symbol} to instruments table.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy instruments from <env>_instrument_polygon to <env>_instruments.")
    parser.add_argument('--environment', required=True, help='Environment (test, intg, prod)')
    parser.add_argument('--tickers', help='Comma-separated list of tickers to copy')
    parser.add_argument('--universe_id', type=int, help='Universe ID to include all tickers in that universe')
    args = parser.parse_args()
    tickers = args.tickers.split(',') if args.tickers else None
    asyncio.run(main(args.environment, tickers, args.universe_id))
