import argparse
import asyncio
from config.environment import get_environment, set_environment, EnvironmentType
from dao.instrument_polygon_dao import InstrumentPolygonDAO
from dao.instruments_dao import InstrumentsDAO
from dao.universe_membership_dao import UniverseMembershipDAO
from dao.instrument_xrefs_dao import InstrumentXrefsDAO
from dao.vendors_dao import VendorsDAO

async def main(environment: str, tickers=None):
    set_environment(EnvironmentType(environment))
    env = get_environment()
    polygon_dao = InstrumentPolygonDAO(env)
    instruments_dao = InstrumentsDAO(env)
    membership_dao = UniverseMembershipDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    vendors_dao = VendorsDAO(env)

    # Lookup vendor_id for Polygon
    ticker_vendor = await vendors_dao.get_vendor_by_name('ticker')
    if not ticker_vendor:
        raise RuntimeError("Polygon vendor not found in vendors table.")
    ticker_vendor_id = ticker_vendor['id']

    # Determine which tickers to copy
    tickers_to_copy = set()
    if tickers:
        tickers_to_copy.update(tickers)

    # If no tickers specified or found in universe, get all symbols from InstrumentPolygonDAO
    if not tickers_to_copy:
        print("[INFO] No tickers or universe provided. Fetching all symbols from InstrumentPolygonDAO.")
        all_symbols = await polygon_dao.get_all_symbols()
        tickers_to_copy.update(all_symbols)

    if not tickers_to_copy:
        print("[WARN] No tickers specified or found in universe or Polygon table. Nothing to copy.")
        return

    # Copy each instrument from polygon to instruments
    for symbol in tickers_to_copy:
        # Check if xref already exists for this vendor and symbol
        xref = await xrefs_dao.find_xref(ticker_vendor_id, symbol)
        if xref:
            print(f"[SKIP] Xref already exists for {symbol} (vendor: Ticker), skipping instrument/xref creation.")
            continue
        instrument = await polygon_dao.get_instrument(symbol)
        if not instrument:
            print(f"[WARN] Instrument {symbol} not found in polygon table.")
            continue
        # Create instrument
        instrument_id = await instruments_dao.create_instrument(
            symbol=instrument['symbol'],
            name=instrument.get('name'),
            exchange=instrument.get('exchange'),
            type_=instrument.get('type'),
            currency=instrument.get('currency'),
            list_date=instrument.get('list_date'),
            delist_date=instrument.get('delist_date')
        )
        # Create xref
        await xrefs_dao.create_xref(
            instrument_id=instrument_id,
            vendor_id=ticker_vendor_id,
            symbol=symbol,
            type=instrument.get('type')
        )
        print(f"[INFO] Created instrument and xref for {symbol} (vendor: Polygon).")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy instruments from <env>_instrument_polygon to <env>_instruments.")
    parser.add_argument('--environment', required=True, help='Environment (test, intg, prod)')
    parser.add_argument('--tickers', help='Comma-separated list of tickers to copy')
    args = parser.parse_args()
    tickers = args.tickers.split(',') if args.tickers else None
    asyncio.run(main(args.environment, tickers))
