import os
import asyncpg
import aiohttp
import argparse
from bs4 import BeautifulSoup
import datetime
import re

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")
SPY_UNIVERSE_NAME = "SPY"
SPY_UNIVERSE_DESC = "S&P 500 membership events scraped from Wikipedia."
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

async def fetch_spy_events(tickers=None):
    async with aiohttp.ClientSession() as session:
        async with session.get(WIKI_URL) as resp:
            html = await resp.text()
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")

    # Build company name -> ticker mapping from the constituents table
    constituents_table = soup.find("table", {"id": "constituents"})
    name_to_ticker = {}
    if constituents_table:
        for row in constituents_table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if not cols or len(cols) < 2:
                continue
            ticker = cols[0].text.strip().replace(".", "-")
            name = cols[1].text.strip()
            name_to_ticker[name] = ticker

    # Find the changes table
    changes_table = None
    for t in tables:
        caption = t.find("caption")
        if caption and ("changes" in caption.text.lower() or "addition" in caption.text.lower()):
            changes_table = t
            break
    if changes_table is None:
        changes_table = tables[1]
    events = []
    # Find the header row and determine column indices for 'Added' and 'Removed'
    header_row = changes_table.find('tr')
    header_cols = [th.text.strip().lower() for th in header_row.find_all(['th', 'td'])]
    try:
        added_idx = header_cols.index('added')
    except ValueError:
        added_idx = 1
    try:
        removed_idx = header_cols.index('removed')+1
    except ValueError:
        removed_idx = 3
    for row in changes_table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if not cols:
            continue
        # Only log debug for matching tickers (if provided)
        log_this_row = True
        ticker_set = set(t.upper() for t in tickers.split(",")) if tickers else None
        if ticker_set:
            add_val = cols[added_idx].text.strip().replace('.', '-') if len(cols) > added_idx else ''
            rem_val = cols[removed_idx].text.strip().replace('.', '-') if len(cols) > removed_idx else ''
            def extract_ticker_for_log(val):
                if not val:
                    return ''
                val = val.replace('.', '-')
                m = re.match(r"^([A-Z0-9\.-]+) ", val)
                if m:
                    return m.group(1)
                m2 = re.match(r"^([A-Z0-9\.-]+) \((.+)\)$", val)
                if m2:
                    return m2.group(1)
                return val
            add_ticker = extract_ticker_for_log(add_val).upper()
            rem_ticker = extract_ticker_for_log(rem_val).upper()
            log_this_row = (add_ticker in ticker_set or rem_ticker in ticker_set)
        if log_this_row:
            print('[DEBUG] Row values:', [c.text.strip() for c in cols])
        if len(cols) > max(added_idx, removed_idx, 0):
            date_str = cols[0].text.strip()
            added = cols[added_idx].text.strip() if len(cols) > added_idx else ''
            removed = cols[removed_idx].text.strip() if len(cols) > removed_idx else ''
            reason = cols[4].text.strip() if len(cols) > 4 else ''
        else:
            continue
        try:
            date = datetime.datetime.strptime(date_str, "%B %d, %Y").date()
        except Exception:
            try:
                date = datetime.datetime.strptime(date_str, "%b %d, %Y").date()
            except Exception:
                continue
        # Always extract tickers from both columns using regex
        def extract_ticker(val):
            if not val:
                return None
            val = val.replace('.', '-')
            # Ticker: all caps, <=5 chars, no spaces
            if re.fullmatch(r'[A-Z0-9\.-]{1,6}', val):
                return val
            # Pattern: TICKER (Company Name)
            m = re.match(r"^([A-Z0-9\.-]{1,6}) \((.+)\)$", val)
            if m:
                return m.group(1)
            # Try mapping by company name
            ticker = name_to_ticker.get(val)
            if ticker:
                return ticker
            # Try extracting ticker from parenthesis
            m2 = re.match(r"^(.+) \(([A-Z0-9\.-]{1,6})\)$", val)
            if m2:
                return m2.group(2)
            print(f"[WARN] Could not extract ticker from value: '{val}'")
            return val
        # Debug: print raw and extracted values
        print(f"[DEBUG] Row: date={date_str}, added='{added}', removed='{removed}'")
        added_ticker = extract_ticker(added)
        removed_ticker = extract_ticker(removed)
        print(f"[DEBUG] Extracted: added_ticker='{added_ticker}', removed_ticker='{removed_ticker}'")
        # For each row, generate at most two events: one for added symbol, one for removed symbol
        # The ticker under Added is the added symbol, under Removed is the removed symbol
        if added_ticker:
            # Use the reason column from the table if available, else empty string
            event_add = {"type": "add", "symbol": added_ticker, "date": date, "reason": reason or ""}
            print(f"[DEBUG] Event add: {event_add}")
            events.append(event_add)
        if removed_ticker:
            event_remove = {"type": "remove", "symbol": removed_ticker, "date": date, "reason": reason or ""}
            print(f"[DEBUG] Event remove: {event_remove}")
            events.append(event_remove)

        # Only generate events if the symbol is present; if only one is present, only that event is generated.
    print("[DEBUG] First 10 events:", events[:10])
    print("[DEBUG] Last 10 events:", events[-10:])
    return events

async def get_or_create_universe(pool, name, description):
    from config.environment import get_environment
    env = get_environment()
    universe_table = env.get_table_name('universe')
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT id FROM {universe_table} WHERE name = $1", name)
        if row:
            print(f"[DEBUG] Universe '{name}' already exists with id {row['id']}")
            return row['id']
        row = await conn.fetchrow(
            f"INSERT INTO {universe_table} (name, description) VALUES ($1, $2) RETURNING id",
            name, description)
        print(f"[DEBUG] Inserted new universe '{name}' with id {row['id']}")
        return row['id']

async def apply_events_to_membership(pool, universe_id, events, env=None):
    """
    Apply add/remove events to universe_membership and also log to universe_membership_changes.
    If env is provided, use it to prefix the membership_changes table.
    """
    from config.environment import get_environment
    if env is None:
        env = get_environment()
    universe_membership_table = env.get_table_name('universe_membership')
    membership_changes_table = env.get_table_name('universe_membership_changes')
    async with pool.acquire() as conn:
        for event in events:
            # Insert into membership_changes
            await conn.execute(
                f"""
                INSERT INTO {membership_changes_table} (symbol, action, effective_date, reason)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (symbol, action, effective_date) DO NOTHING
                """,
                event['symbol'], event['type'], event['date'], event.get('reason', None)
            )
            if event['type'] == 'add':
                import json
                meta_str = json.dumps({"reason": event['reason']})
                result = await conn.execute(
                    f"""
                    INSERT INTO {universe_membership_table} (universe_id, symbol, start_at, end_at, meta)
                    VALUES ($1, $2, $3, NULL, $4::jsonb)
                    ON CONFLICT (universe_id, symbol, start_at) DO NOTHING
                    """,
                    universe_id, event['symbol'], event['date'], meta_str)
                print(f"[DEBUG] ADD {event['symbol']} at {event['date']} ({result})")
            elif event['type'] == 'remove':
                import json
                meta_str = json.dumps({"remove_reason": event['reason']})
                result = await conn.execute(
                    f"""
                    UPDATE {universe_membership_table}
                    SET end_at = $1, meta = COALESCE(meta, '{{}}'::jsonb) || $2::jsonb
                    WHERE universe_id = $3 AND symbol = $4 AND end_at IS NULL
                    """,
                    event['date'], meta_str, universe_id, event['symbol'])
                print(f"[DEBUG] REMOVE {event['symbol']} at {event['date']} ({result})")


async def remove_all_universe_membership(pool, universe_id):
    from config.environment import get_environment
    env = get_environment()
    universe_membership_table = env.get_table_name('universe_membership')
    async with pool.acquire() as conn:
        result = await conn.execute(f"DELETE FROM {universe_membership_table} WHERE universe_id = $1", universe_id)
        print(f"[DEBUG] Removed all {universe_membership_table} for universe_id={universe_id} ({result})")

async def remove_all_universe_membership_changes(pool, universe_name, env=None):
    """
    Remove all membership_changes for the given universe_name (by symbol set if needed).
    If env is provided, use it to prefix the table name.
    """
    async with pool.acquire() as conn:
        if env is not None:
            membership_changes_table = env.get_table_name('universe_membership_changes')
        else:
            membership_changes_table = 'universe_membership_changes'
        # For integration, just delete all changes (since we don't have universe_id linkage)
        result = await conn.execute(f"DELETE FROM {membership_changes_table}")
        print(f"[DEBUG] Removed all universe_membership_changes ({result})")

async def populate_spy_universe_events(universe_name, tickers=None):
    """
    Populate both universe_membership and universe_membership_changes with SPY events.
    """
    from config.environment import get_environment
    db_url = get_environment().get_database_url()
    pool = await asyncpg.create_pool(db_url, min_size=1, max_size=4)
    events = await fetch_spy_events(tickers)
    print(f"[DEBUG] Fetched {len(events)} SPY membership events from Wikipedia.")
    universe_id = await get_or_create_universe(pool, universe_name, SPY_UNIVERSE_DESC)
    await remove_all_universe_membership(pool, universe_id)
    # Get env for prefixed table names
    env = get_environment()
    await remove_all_universe_membership_changes(pool, universe_name, env=env)
    if tickers:
        ticker_set = set(t.upper() for t in tickers.split(",") if t)
        events = [e for e in events if (e['symbol'] and e['symbol'].upper() in ticker_set) or (e.get('reason') and isinstance(e.get('reason'), str) and e['reason'].upper() in ticker_set)]
        print(f"[DEBUG] Filtered events for tickers {ticker_set}: {len(events)} events")
    await apply_events_to_membership(pool, universe_id, events, env=env)
    await pool.close()


async def main(universe_name=None, tickers=None, environment=None):  # environment is now required by CLI

    import argparse
    import sys
    from config.environment import set_environment, EnvironmentType
    if universe_name is None or tickers is None or environment is None:
        parser = argparse.ArgumentParser()
        parser.add_argument('--universe_name', type=str, default='S&P 500')
        parser.add_argument('--tickers', type=str, default=None, help='Only update/insert universe_membership for these comma-delimited tickers (optional)')
        parser.add_argument('--environment', type=str, required=True, help='Environment type: test, intg, prod (required)')
        args = parser.parse_args()
        universe_name = args.universe_name
        tickers = args.tickers
        environment = args.environment
    if environment:
        try:
            set_environment(EnvironmentType(environment))
            print(f"[INFO] Set environment to {environment}")
        except Exception as e:
            print(f"[WARN] Invalid environment '{environment}': {e}. Using auto-detected environment.")
    from config.environment import get_environment
    db_url = get_environment().get_database_url()
    print(f"db_url:{db_url}")
    await populate_spy_universe_events(universe_name, tickers)
    print("Done populating SPY universe membership events.")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
