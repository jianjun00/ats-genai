import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import asyncio
import asyncpg
import re

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")

CREATE_SPY_MEMBERSHIP_CHANGE_SQL = """
CREATE TABLE IF NOT EXISTS spy_membership_change (
    id SERIAL PRIMARY KEY,
    change_date DATE NOT NULL,
    added TEXT,
    removed TEXT
);
"""

INSERT_CHANGE_SQL = """
INSERT INTO spy_membership_change (change_date, added, removed)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING;
"""

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# Fetch and parse the S&P 500 table from Wikipedia (current membership)
def fetch_sp500_symbols(soup=None):
    if soup is None:
        resp = requests.get(WIKI_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"id": "constituents"})
    if not table:
        raise Exception("Could not find S&P 500 table on Wikipedia page!")
    symbols = []
    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if cols:
            symbol = cols[0].text.strip().upper()
            symbols.append(symbol)
    return symbols

# Parse the 'Changes' table for historic adds/removes
def fetch_sp500_changes(soup=None):
    if soup is None:
        resp = requests.get(WIKI_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
    changes_table = None
    for table in soup.find_all("table", {"class": "wikitable"}):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if any("date" in h for h in headers) and any("add" in h for h in headers) and any("remove" in h for h in headers):
            changes_table = table
            break
    if not changes_table:
        print("Could not find S&P 500 changes table. Table headers found:")
        for table in soup.find_all("table", {"class": "wikitable"}):
            print([th.get_text(strip=True) for th in table.find_all("th")])
        raise Exception("Could not find S&P 500 changes table on Wikipedia page!")
    changes = []
    for i, row in enumerate(changes_table.find_all("tr")[1:]):
        cols = row.find_all("td")
        if len(cols) < 4:
            continue  # skip malformed or note rows
        date_str = cols[0].text.strip()
        # Debug: Print the raw row being parsed
        if i < 5:
            print(f"Row {i}: {[c.text.strip() for c in cols]}")
        dt = None
        for fmt in ("%Y-%m-%d", "%B %d, %Y"):
            try:
                dt = datetime.strptime(date_str, fmt).date()
                break
            except Exception:
                continue
        if not dt:
            print(f"Skipping row with invalid date: {date_str}")
            continue
        added_raw = cols[1].text.strip()
        removed_raw = cols[3].text.strip()
        added = [added_raw.strip().upper()] if added_raw.strip() else []
        removed = [removed_raw.strip().upper()] if removed_raw.strip() else []
        if not added and not removed:
            continue
        # For each added symbol, create a row with removed=None
        for symbol in added:
            changes.append((dt, symbol, None))
        # For each removed symbol, create a row with added=None
        for symbol in removed:
            changes.append((dt, None, symbol))
    print(f"Total parsed change events: {len(changes)}")
    print("First 5 DB rows to insert:", changes[:5])
    return changes


async def create_table_and_insert(changes):
    pool = await asyncpg.create_pool(TSDB_URL, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_SPY_MEMBERSHIP_CHANGE_SQL)
        await conn.executemany(INSERT_CHANGE_SQL, changes)
    await pool.close()
    print(f"Inserted {len(changes)} S&P 500 membership change events into spy_membership_change table.")

def main():
    resp = requests.get(WIKI_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    changes = fetch_sp500_changes(soup)
    # 'changes' is already a list of (change_date, added, removed) tuples
    asyncio.run(create_table_and_insert(changes))

if __name__ == "__main__":
    main()


async def create_table_and_insert(rows):
    pool = await asyncpg.create_pool(TSDB_URL, min_size=1, max_size=2)
