# moved from project root
import psycopg2
import os

# Connection parameters for postgres superuser (to create database)
PG_SUPER_URL = os.getenv('PG_SUPER_URL', 'postgresql://postgres@localhost:5432/postgres')
TRADING_DB = 'trading_db'

# SQL for required tables
CREATE_DAILY_PRICES = """
CREATE TABLE IF NOT EXISTS daily_prices (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (date, symbol)
);
"""

CREATE_DAILY_MARKET_CAP = """
CREATE TABLE IF NOT EXISTS daily_market_cap (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    market_cap DOUBLE PRECISION,
    PRIMARY KEY (date, symbol)
);
"""

CREATE_SIGNAL_TABLE = """
CREATE TABLE IF NOT EXISTS signals (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    signal JSONB NOT NULL,
    PRIMARY KEY (time, symbol)
);
"""

CREATE_EXTENSION = "CREATE EXTENSION IF NOT EXISTS timescaledb;"
CREATE_HYPERTABLE_DAILY_PRICES = "SELECT create_hypertable('daily_prices', 'date', if_not_exists => TRUE);"
CREATE_HYPERTABLE_MARKET_CAP = "SELECT create_hypertable('daily_market_cap', 'date', if_not_exists => TRUE);"


def create_database(force=False):
    # Connect to postgres maintenance DB as superuser
    conn = psycopg2.connect(PG_SUPER_URL)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{TRADING_DB}'")
    exists = cur.fetchone() is not None
    if exists and force:
        cur.execute(f"DROP DATABASE {TRADING_DB}")
        cur.execute(f"CREATE DATABASE {TRADING_DB}")
        print(f"Database '{TRADING_DB}' dropped and recreated.")
    elif not exists:
        cur.execute(f"CREATE DATABASE {TRADING_DB}")
        print(f"Database '{TRADING_DB}' created.")
    else:
        print(f"Database '{TRADING_DB}' already exists. Skipping drop/create.")
    cur.close()
    conn.close()

CREATE_FUNDAMENTALS = """
CREATE TABLE IF NOT EXISTS fundamentals (
    ticker TEXT NOT NULL,
    date DATE NOT NULL,
    market_cap DOUBLE PRECISION,
    PRIMARY KEY (ticker, date)
);
"""

def setup_tables():
    # Connect to trading_db
    db_url = os.getenv('TSDB_URL', f'postgresql://localhost:5432/{TRADING_DB}')
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()
    # Enable TimescaleDB
    try:
        cur.execute(CREATE_EXTENSION)
    except Exception as e:
        print(f"Extension creation skipped or failed: {e}")
    # Create tables
    cur.execute(CREATE_DAILY_PRICES)
    cur.execute(CREATE_DAILY_MARKET_CAP)
    cur.execute(CREATE_SIGNAL_TABLE)
    cur.execute(CREATE_FUNDAMENTALS)

    # --- Additional tables ---
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_prices_tiingo (
        date DATE NOT NULL,
        symbol TEXT NOT NULL,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        adjClose DOUBLE PRECISION,
        volume BIGINT,
        PRIMARY KEY (date, symbol)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_adjusted_prices (
        date DATE NOT NULL,
        symbol TEXT NOT NULL,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        volume BIGINT,
        market_cap DOUBLE PRECISION,
        original_open DOUBLE PRECISION,
        original_high DOUBLE PRECISION,
        original_low DOUBLE PRECISION,
        original_close DOUBLE PRECISION,
        split_numerator DOUBLE PRECISION,
        split_denominator DOUBLE PRECISION,
        dividend_amount DOUBLE PRECISION,
        adjustment_factor DOUBLE PRECISION,
        PRIMARY KEY (date, symbol)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_prices_polygon (
        date DATE NOT NULL,
        symbol TEXT NOT NULL,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        volume BIGINT,
        market_cap DOUBLE PRECISION,
        PRIMARY KEY (date, symbol)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_prices_norgate (
        date DATE NOT NULL,
        symbol TEXT NOT NULL,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        volume BIGINT,
        PRIMARY KEY (date, symbol)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_prices_quandl (
        date DATE NOT NULL,
        symbol TEXT NOT NULL,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        volume BIGINT,
        PRIMARY KEY (date, symbol)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS spy_membership (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        effective_date DATE NOT NULL,
        removal_date DATE
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS spy_membership_change (
        id SERIAL PRIMARY KEY,
        change_date DATE NOT NULL,
        added TEXT,
        removed TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id SERIAL PRIMARY KEY,
        event_type TEXT NOT NULL,
        symbol TEXT,
        event_time TIMESTAMPTZ NOT NULL,
        reported_time TIMESTAMPTZ,
        source TEXT,
        data JSONB NOT NULL,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_symbol_time ON events(symbol, event_time);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(event_type, event_time);")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dividends (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        ex_date DATE NOT NULL,
        amount DOUBLE PRECISION NOT NULL
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stock_splits (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        split_date DATE NOT NULL,
        numerator DOUBLE PRECISION NOT NULL,
        denominator DOUBLE PRECISION NOT NULL,
        split_ratio DOUBLE PRECISION GENERATED ALWAYS AS (numerator/denominator) STORED
    );
    """)

    # --- Universe tables ---
    cur.execute('''
    CREATE TABLE IF NOT EXISTS universe (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        description TEXT,
        created_at TIMESTAMP DEFAULT now()
    );
    ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS universe_membership (
        id SERIAL PRIMARY KEY,
        universe_id INTEGER NOT NULL REFERENCES universe(id),
        symbol TEXT NOT NULL,
        start_at DATE NOT NULL,
        end_at DATE,
        meta JSONB,
        created_at TIMESTAMP DEFAULT now(),
        UNIQUE (universe_id, symbol, start_at)
    );
    ''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_universe_membership_universe_date ON universe_membership (universe_id, start_at, end_at);''')

    # Insert test universe and memberships if not exist
    cur.execute("SELECT id FROM universe WHERE name = %s", ('TEST_UNIVERSE',))
    row = cur.fetchone()
    if row is None:
        cur.execute("INSERT INTO universe (name, description) VALUES (%s, %s) RETURNING id", ('TEST_UNIVERSE', 'Test universe for integration tests'))
        universe_id = cur.fetchone()[0]
        cur.execute("""
            INSERT INTO universe_membership (universe_id, symbol, start_at, end_at)
            VALUES (%s, %s, %s, NULL),
                   (%s, %s, %s, NULL)
        """, (universe_id, 'AAPL', '2020-01-01', universe_id, 'TSLA', '2020-01-01'))
        print("Inserted TEST_UNIVERSE with AAPL and TSLA memberships.")
    else:
        print("TEST_UNIVERSE already exists.")

    # Convert to hypertables
    try:
        cur.execute(CREATE_HYPERTABLE_DAILY_PRICES)
    except Exception as e:
        print(f"Hypertable creation for daily_prices failed: {e}")
    try:
        cur.execute(CREATE_HYPERTABLE_MARKET_CAP)
    except Exception as e:
        print(f"Hypertable creation for daily_market_cap failed: {e}")
    cur.close()
    conn.close()
    print("All tables created and hypertables set up (if TimescaleDB is enabled).")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--force', action='store_true', help='Drop and recreate trading_db')
    args = parser.parse_args()
    if args.force:
        print('Dropping and recreating trading_db (--force specified)')
    create_database(force=args.force)
    setup_tables()
