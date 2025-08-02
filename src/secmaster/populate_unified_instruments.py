import argparse
import asyncio
from config.environment import get_environment, set_environment, EnvironmentType
from dao.instrument_polygon_dao import InstrumentPolygonDAO
from dao.instruments_dao import InstrumentsDAO
from dao.universe_membership_dao import UniverseMembershipDAO
from dao.instrument_xrefs_dao import InstrumentXrefsDAO
from dao.vendors_dao import VendorsDAO

from datetime import datetime, date

# Only parse valid date strings; if value is None or blank, return None. Never substitute the current date.
def parse_date(val):
    if val is None or val == '' or isinstance(val, date):
        return None if val == '' else val
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except Exception:
        return None

async def populate_unified_instruments(polygon_dao, instruments_dao, xrefs_dao, vendors_dao, tickers=None, debug=False):
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
        if debug:
            print("[INFO] No tickers or universe provided. Fetching all symbols from InstrumentPolygonDAO.")
        all_symbols = await polygon_dao.get_all_symbols()
        tickers_to_copy.update(all_symbols)

    if not tickers_to_copy:
        if debug:
            print("[WARN] No tickers specified or found in universe or Polygon table. Nothing to copy.")
        return

    # Copy each instrument from polygon to instruments
    for symbol in tickers_to_copy:
        try:
            await process_symbol(symbol, polygon_dao, instruments_dao, xrefs_dao, ticker_vendor_id, debug)
        except Exception as e:
            # Gather as much context as possible
            try:
                polygon_instrument = await polygon_dao.get_instrument_by_symbol(symbol)
            except Exception as e2:
                polygon_instrument = f"[ERROR retrieving instrument: {e2}]"
            print(f"[EXCEPTION] Error processing symbol: {symbol}")
            print(f"  polygon_instrument: {polygon_instrument}")
            if isinstance(polygon_instrument, dict):
                raw_list_date = polygon_instrument.get('list_date')
                raw_delist_date = polygon_instrument.get('delist_date')
                print(f"  raw_list_date: {raw_list_date} ({type(raw_list_date)})")
                print(f"  raw_delist_date: {raw_delist_date} ({type(raw_delist_date)})")
                from .populate_unified_instruments import parse_date
                try:
                    start_at = parse_date(raw_list_date)
                    end_at = parse_date(raw_delist_date)
                    print(f"  parsed start_at: {start_at} ({type(start_at)})")
                    print(f"  parsed end_at: {end_at} ({type(end_at)})")
                except Exception as e3:
                    print(f"  [ERROR parsing dates: {e3}]")
            print(f"  Exception: {e}")
            continue

async def process_symbol(symbol, polygon_dao, instruments_dao, xrefs_dao, ticker_vendor_id, debug=False):
    # Check if xref already exists for this vendor and symbol
    xref = await xrefs_dao.find_xref(ticker_vendor_id, symbol)
    if xref:
        if debug:
            print(f"[SKIP] Xref already exists for {symbol} (vendor: Ticker), skipping instrument/xref creation.")
        return
    polygon_instrument = await polygon_dao.get_instrument_by_symbol(symbol)
    if not polygon_instrument:
        if debug:
            print(f"[WARN] Instrument {symbol} not found in polygon table.")
        return
    if not polygon_instrument.get('list_date'):
        print(f"instrument {symbol} does not have valid list date")
        return
    instrument_id = await instruments_dao.create_instrument(
        symbol=polygon_instrument['symbol'],
        name=polygon_instrument.get('name'),
        exchange=polygon_instrument.get('exchange'),
        type_=polygon_instrument.get('type'),
        currency=polygon_instrument.get('currency'),
        list_date=polygon_instrument.get('list_date'),
        delist_date=polygon_instrument.get('delist_date')
    )
    raw_list_date = polygon_instrument.get('list_date')
    raw_delist_date = polygon_instrument.get('delist_date')
    start_at = parse_date(raw_list_date)
    end_at = parse_date(raw_delist_date)
    # Defensive: ensure correct type
    from datetime import date
    if not (start_at is None or isinstance(start_at, date)):
        if debug:
            print(f"[ERROR] start_at for {symbol} is not a date: {start_at} ({type(start_at)}) - skipping xref creation!")
        return
    if not (end_at is None or isinstance(end_at, date)):
        if debug:
            print(f"[ERROR] end_at for {symbol} is not a date: {end_at} ({type(end_at)}) - skipping xref creation!")
        return
    if debug:
        print(f"[DEBUG] Symbol: {symbol}, instrument_id: {instrument_id}")
        print(f"[DEBUG] Raw list_date: {raw_list_date} ({type(raw_list_date)}), Parsed start_at: {start_at} ({type(start_at)})")
        print(f"[DEBUG] Raw delist_date: {raw_delist_date} ({type(raw_delist_date)}), Parsed end_at: {end_at} ({type(end_at)})")
    await xrefs_dao.create_xref(
        instrument_id=instrument_id,
        vendor_id=ticker_vendor_id,
        symbol=symbol,
        start_at=list_date,  # Always use instrument's list_date for xref
        end_at=end_at
    )
    if debug:
        print(f"[INFO] Created instrument and xref for {symbol} (vendor: Polygon).")

async def main(environment: str, tickers=None, debug=True):
    set_environment(EnvironmentType(environment))
    env = get_environment()
    polygon_dao = InstrumentPolygonDAO(env)
    instruments_dao = InstrumentsDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    vendors_dao = VendorsDAO(env)
    await populate_unified_instruments(polygon_dao, instruments_dao, xrefs_dao, vendors_dao, tickers, debug=debug)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy instruments from <env>_instrument_polygon to <env>_instruments.")
    parser.add_argument('--environment', required=True, help='Environment (test, intg, prod)')
    parser.add_argument('--tickers', help='Comma-separated list of tickers to copy')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    tickers = args.tickers.split(',') if args.tickers else None
    asyncio.run(main(args.environment, tickers, debug=args.debug))
