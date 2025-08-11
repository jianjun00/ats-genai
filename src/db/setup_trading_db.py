# Database setup script
import os
import sys
import psycopg2
from psycopg2 import OperationalError

# Connection parameters for postgres superuser (to create database)
PG_SUPER_URL = os.getenv('PG_SUPER_URL', 'postgresql://postgres@localhost:5432/postgres')
TRADING_DB = os.getenv('POSTGRES_DB', 'trading_db')
SKIP_DB_SETUP = os.getenv('SKIP_DB_SETUP', 'false').lower() == 'true'

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
    """Create the trading database and required tables."""
    if SKIP_DB_SETUP:
        print("Skipping database setup as SKIP_DB_SETUP is true")
        return True
        
    try:
        # Connect to postgres database
        conn = psycopg2.connect(PG_SUPER_URL)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Drop database if force is True
        if force:
            try:
                cursor.execute(f"DROP DATABASE IF EXISTS {TRADING_DB}")
                print(f"Dropped database {TRADING_DB}")
            except Exception as e:
                print(f"Error dropping database: {e}")
        
        # Create database
        try:
            cursor.execute(f"CREATE DATABASE {TRADING_DB}")
            print(f"Created database {TRADING_DB}")
        except Exception as e:
            print(f"Database {TRADING_DB} already exists or could not be created: {e}")
        
        cursor.close()
        conn.close()
        return True
        
    except OperationalError as e:
        print(f"Warning: Could not connect to database at {PG_SUPER_URL}")
        print("Database setup will be skipped. Set SKIP_DB_SETUP=true to suppress this warning.")
        return False
    except Exception as e:
        print(f"Error setting up database: {e}")
        return False

CREATE_FUNDAMENTALS = """
CREATE TABLE IF NOT EXISTS fundamentals (
    ticker TEXT NOT NULL,
    date DATE NOT NULL,
    market_cap DOUBLE PRECISION,
    PRIMARY KEY (ticker, date)
);
"""

def setup_tables():
    if SKIP_DB_SETUP:
        print("Skipping table setup as SKIP_DB_SETUP is true")
        return True
        
    try:
        # Connect to trading_db
        db_url = os.getenv('TSDB_URL', f'postgresql://localhost:5432/{TRADING_DB}')
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # Create tables
        cur.execute(CREATE_DAILY_PRICES)
        cur.execute(CREATE_FUNDAMENTALS)
        
        # Commit changes and close connection
        conn.commit()
        cur.close()
        conn.close()
        print("Created tables: daily_prices, fundamentals")
        return True
        
    except OperationalError as e:
        print(f"Warning: Could not connect to database at {db_url} to set up tables")
        print("Table setup will be skipped. Set SKIP_DB_SETUP=true to suppress this warning.")
        return False
    except Exception as e:
        print(f"Error setting up tables: {e}")
        return False

    try:
        # Connect to trading_db
        db_url = os.getenv('TSDB_URL', f'postgresql://localhost:5432/{TRADING_DB}')
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # Enable TimescaleDB
        try:
            cur.execute(CREATE_EXTENSION)
        except Exception as e:
            print(f"Extension creation skipped or failed: {e}")
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
    except OperationalError as e:
        print(f"Warning: Could not connect to database at {db_url} to set up hypertables")
        print("Hypertable setup will be skipped. Set SKIP_DB_SETUP=true to suppress this warning.")
        return False
    except Exception as e:
        print(f"Error setting up hypertables: {e}")
        return False
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
