import argparse
import asyncio
from shared.utils.environment import Environment, EnvironmentType
from infrastructure.vendor.polygon.dao.instrument_polygon_dao import InstrumentPolygonDAO
from domains.instruments.repositories.instruments_dao import InstrumentsDAO
from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
from domains.instruments.repositories.vendors_dao import VendorsDAO
from datetime import datetime, date
import ray

BATCH_SIZE = 100
RAY_NUM_WORKERS = 4

# Only parse valid date strings; if value is None or blank, return None. Never substitute the current date.
def parse_date(val):
    if val is None or val == '' or isinstance(val, date):
        return None if val == '' else val
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except Exception:
        return None

# Expose batch_logic for direct unit testing (bypassing Ray)
async def batch_logic_for_test(batch, polygon_dao, instruments_dao, xrefs_dao, vendors_dao, debug=False):
    ticker_vendor = await vendors_dao.get_vendor_by_name('ticker')
    ticker_vendor_id = ticker_vendor['id']
    # Fetch all polygon instruments for batch
    polygon_instruments = {}
    for symbol in batch:
        polygon_instruments[symbol] = await polygon_dao.get_instrument_by_symbol(symbol)
    # Filter out missing or invalid instruments
    valid_instruments = []
    for symbol, inst in polygon_instruments.items():
        if not inst:
            if debug:
                print(f"[WARN] Instrument {symbol} not found in polygon table.")
            continue
        if not inst.get('list_date'):
            if debug:
                print(f"[WARN] Instrument {symbol} missing list_date.")
            continue
        valid_instruments.append({
            'symbol': inst['symbol'],
            'name': inst.get('name'),
            'exchange': inst.get('exchange'),
            'type_': inst.get('type'),
            'currency': inst.get('currency'),
            'list_date': inst.get('list_date'),
            'delist_date': inst.get('delist_date')
        })

    # Step 1: Categorize instruments into existing vs new, and check for missing xrefs
    symbol_to_id = {}
    new_instruments = []
    missing_xref_instruments = []

    for inst in valid_instruments:
        symbol = inst['symbol']

        # First check if xref already exists (fastest check)
        instrument_id = await xrefs_dao.resolve_instrument_id(symbol, vendor_id=ticker_vendor_id)
        if instrument_id:
            # Xref exists, instrument is already fully configured
            symbol_to_id[symbol] = instrument_id
            if debug:
                print(f"[SKIP] Instrument {symbol} already has xref, skipping.")
            continue

        # No xref found, check if instrument exists in instruments table
        existing_instrument = await instruments_dao.get_instrument_by_symbol(symbol)
        if existing_instrument:
            # Instrument exists but missing xref
            symbol_to_id[symbol] = existing_instrument['id']
            missing_xref_instruments.append(inst)
            if debug:
                print(f"[XREF] Instrument {symbol} exists but missing xref, will create xref.")
        else:
            # Completely new instrument
            new_instruments.append(inst)
            if debug:
                print(f"[NEW] Instrument {symbol} is completely new, will create instrument and xref.")

    # Step 2: Batch insert new instruments
    if new_instruments:
        await instruments_dao.create_instruments_batch(new_instruments, pool_min_size=1, pool_max_size=1)
        # Update symbol_to_id mapping for new instruments
        for inst in new_instruments:
            row = await instruments_dao.get_instrument_by_symbol(inst['symbol'])
            if not row:
                raise RuntimeError(f"Instrument {inst['symbol']} not found after insert.")
            symbol_to_id[inst['symbol']] = row['id']

    # Step 3: Prepare xref inserts for both new instruments AND existing instruments with missing xrefs
    xref_inserts = []
    instruments_needing_xrefs = new_instruments + missing_xref_instruments

    for inst in instruments_needing_xrefs:
        symbol = inst['symbol']
        instrument_id = symbol_to_id[symbol]
        start_at = parse_date(inst['list_date'])
        end_at = parse_date(inst['delist_date'])
        xref_inserts.append({
            'instrument_id': instrument_id,
            'vendor_id': ticker_vendor_id,
            'symbol': symbol,
            'type': inst.get('type_'),
            'start_at': start_at,
            'end_at': end_at
        })

    # Step 4: Batch insert xrefs for both new instruments and existing instruments with missing xrefs
    if xref_inserts:
        await xrefs_dao.create_xrefs_batch(xref_inserts, pool_min_size=1, pool_max_size=1)

    if debug:
        print(f"[INFO] Batch processed: {len(new_instruments)} new instruments, {len(missing_xref_instruments)} missing xrefs, {len(xref_inserts)} total xrefs created")

    return len(valid_instruments)

async def populate_unified_instruments(polygon_dao, instruments_dao, xrefs_dao, vendors_dao, tickers=None, debug=False):
    # Lookup vendor_id for Polygon
    ticker_vendor = await vendors_dao.get_vendor_by_name('ticker')
    if not ticker_vendor:
        raise RuntimeError("Ticker vendor not found in vendors table.")
    ticker_vendor['id']

    # Determine which tickers to copy
    tickers_to_copy = set()
    if tickers:
        tickers_to_copy.update(tickers)

    if not tickers_to_copy:
        if debug:
            print("[INFO] No tickers or universe provided. Fetching all symbols from InstrumentPolygonDAO.")
        all_symbols = await polygon_dao.get_all_symbols()
        tickers_to_copy.update(all_symbols)

    if not tickers_to_copy:
        if debug:
            print("[WARN] No tickers specified or found in universe or Polygon table. Nothing to copy.")
        return

    # Chunk tickers into batches
    tickers_list = list(tickers_to_copy)
    batches = [tickers_list[i:i+BATCH_SIZE] for i in range(0, len(tickers_list), BATCH_SIZE)]

    # Ray: Define remote function for batch processing
    @ray.remote
    def process_batch(batch, env_args, debug):
        import asyncio
        import nest_asyncio
        nest_asyncio.apply()
        from shared.utils.environment import Environment
        from infrastructure.vendor.polygon.dao.instrument_polygon_dao import InstrumentPolygonDAO
        from domains.instruments.repositories.instruments_dao import InstrumentsDAO
        from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
        from domains.instruments.repositories.vendors_dao import VendorsDAO
        # Recreate DAOs in Ray worker
        env = Environment(*env_args)
        polygon_dao = InstrumentPolygonDAO(env)
        instruments_dao = InstrumentsDAO(env)
        xrefs_dao = InstrumentXrefsDAO(env)
        vendors_dao = VendorsDAO(env)
        async def batch_logic():
            ticker_vendor = await vendors_dao.get_vendor_by_name('ticker')
            ticker_vendor_id = ticker_vendor['id']
            # Fetch all polygon instruments for batch
            polygon_instruments = {}
            for symbol in batch:
                polygon_instruments[symbol] = await polygon_dao.get_instrument_by_symbol(symbol)
            # Filter out missing or invalid instruments
            valid_instruments = []
            for symbol, inst in polygon_instruments.items():
                if not inst:
                    if debug:
                        print(f"[WARN] Instrument {symbol} not found in polygon table.")
                    continue
                if not inst.get('list_date'):
                    if debug:
                        print(f"[WARN] Instrument {symbol} missing list_date.")
                    continue
                valid_instruments.append({
                    'symbol': inst['symbol'],
                    'name': inst.get('name'),
                    'exchange': inst.get('exchange'),
                    'type_': inst.get('type'),
                    'currency': inst.get('currency'),
                    'list_date': inst.get('list_date'),
                    'delist_date': inst.get('delist_date')
                })

            # Step 1: Categorize instruments into existing vs new, and check for missing xrefs
            symbol_to_id = {}
            new_instruments = []
            missing_xref_instruments = []

            for inst in valid_instruments:
                symbol = inst['symbol']

                # First check if xref already exists (fastest check)
                instrument_id = await xrefs_dao.resolve_instrument_id(symbol, vendor_id=ticker_vendor_id)
                if instrument_id:
                    # Xref exists, instrument is already fully configured
                    symbol_to_id[symbol] = instrument_id
                    if debug:
                        print(f"[SKIP] Instrument {symbol} already has xref, skipping.")
                    continue

                # No xref found, check if instrument exists in instruments table
                existing_instrument = await instruments_dao.get_instrument_by_symbol(symbol)
                if existing_instrument:
                    # Instrument exists but missing xref
                    symbol_to_id[symbol] = existing_instrument['id']
                    missing_xref_instruments.append(inst)
                    if debug:
                        print(f"[XREF] Instrument {symbol} exists but missing xref, will create xref.")
                else:
                    # Completely new instrument
                    new_instruments.append(inst)
                    if debug:
                        print(f"[NEW] Instrument {symbol} is completely new, will create instrument and xref.")

            # Step 2: Batch insert new instruments
            if new_instruments:
                await instruments_dao.create_instruments_batch(new_instruments, pool_min_size=1, pool_max_size=1)
                # Update symbol_to_id mapping for new instruments
                for inst in new_instruments:
                    row = await instruments_dao.get_instrument_by_symbol(inst['symbol'])
                    if not row:
                        raise RuntimeError(f"Instrument {inst['symbol']} not found after insert.")
                    symbol_to_id[inst['symbol']] = row['id']

            # Step 3: Prepare xref inserts for both new instruments AND existing instruments with missing xrefs
            xref_inserts = []
            instruments_needing_xrefs = new_instruments + missing_xref_instruments

            for inst in instruments_needing_xrefs:
                symbol = inst['symbol']
                instrument_id = symbol_to_id[symbol]
                start_at = parse_date(inst['list_date'])
                end_at = parse_date(inst['delist_date'])
                xref_inserts.append({
                    'instrument_id': instrument_id,
                    'vendor_id': ticker_vendor_id,
                    'symbol': symbol,
                    'type': inst.get('type_'),
                    'start_at': start_at,
                    'end_at': end_at
                })

            # Step 4: Batch insert xrefs for both new instruments and existing instruments with missing xrefs
            if xref_inserts:
                await xrefs_dao.create_xrefs_batch(xref_inserts, pool_min_size=1, pool_max_size=1)

            if debug:
                print(f"[INFO] Batch processed: {len(new_instruments)} new instruments, {len(missing_xref_instruments)} missing xrefs, {len(xref_inserts)} total xrefs created")

            return len(valid_instruments)
        return asyncio.run(batch_logic())

    # Ray: Initialize and run batches
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True, num_cpus=RAY_NUM_WORKERS)
    env_args = (polygon_dao.env.gin_config_path, polygon_dao.env.env_type)
    # Only RAY_NUM_WORKERS batches in flight at once
    import time
    results = []
    i = 0
    import time
    while i < len(batches):
        batch_futures = []
        for _ in range(min(RAY_NUM_WORKERS, len(batches)-i)):
            batch_futures.append(process_batch.remote(batches[i], env_args, debug))
            i += 1
        # Robust DB overload handling
        retry_wait = 5  # seconds
        max_retries = 10
        for attempt in range(max_retries):
            try:
                results.extend(ray.get(batch_futures))
                break
            except Exception as e:
                # Detect asyncpg TooManyConnectionsError in Ray remote error chain
                err_str = str(e)
                if "TooManyConnectionsError" in err_str or "too many clients already" in err_str:
                    print(f"[WARN] DB overloaded (TooManyConnectionsError). Waiting {retry_wait}s before retrying batch (attempt {attempt+1}/{max_retries})...")
                    time.sleep(retry_wait)
                else:
                    raise
        else:
            print("[ERROR] TooManyConnectionsError persists after retries. Exiting batch processing.")
            raise RuntimeError("Database overloaded: TooManyConnectionsError after retries")
        time.sleep(0.1)
    print(f"[INFO] Completed {len(results)} batches, total inserted: {sum(results)}")

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
        start_at=start_at,  # Always use parsed start_at for xref
        end_at=end_at
    )
    if debug:
        print(f"[INFO] Created instrument and xref for {symbol} (vendor: Polygon).")

async def main(environment: str, tickers=None, debug=True, gin_config=None):
    # Environment expects gin_config_path as first argument, env_type as second
    env = Environment(gin_config, EnvironmentType(environment))
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
    parser.add_argument('--gin_config', help='Path to gin config file', default=None)
    args = parser.parse_args()
    tickers = args.tickers.split(',') if args.tickers else None
    asyncio.run(main(args.environment, tickers, debug=args.debug, gin_config=args.gin_config))
