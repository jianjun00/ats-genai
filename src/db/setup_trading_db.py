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
    # Load and execute unified schema
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r") as f:
        schema_sql = f.read()
    for statement in schema_sql.split(';'):
        stripped = statement.strip()
        if not stripped or stripped.startswith('--'):
            continue
        try:
            cur.execute(statement)
        except Exception as e:
            print(f"Error executing statement: {statement}\nError: {e}")
            raise

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
