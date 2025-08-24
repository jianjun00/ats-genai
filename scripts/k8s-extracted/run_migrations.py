#!/usr/bin/env python3

"""
Run complete database migrations to ensure proper schema
"""
import asyncio
import asyncpg
import logging
from pathlib import Path

async def main():
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_url = "postgresql://postgres:dev_password@postgres-simple:5432/dev_db"

# Wait for database
max_retries = 10
for attempt in range(max_retries):
try:
conn = await asyncpg.connect(db_url)
logger.info("✅ Connected to database")
break
except Exception as e:
if attempt < max_retries - 1:
await asyncio.sleep(2)
else:
raise Exception(f"Could not connect after {max_retries} attempts: {e}")

try:
logger.info("🔧 Running database migrations...")

# Enable TimescaleDB extension
await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
logger.info("✅ TimescaleDB extension enabled")

# Create all core tables with proper schema

# 1. Universe management tables
await conn.execute("""
CREATE TABLE IF NOT EXISTS dev_universe (
id SERIAL PRIMARY KEY,
name TEXT UNIQUE NOT NULL,
description TEXT,
created_at TIMESTAMP DEFAULT NOW(),
updated_at TIMESTAMP DEFAULT NOW()
);
""")

await conn.execute("""
CREATE TABLE IF NOT EXISTS dev_universe_membership (
id SERIAL PRIMARY KEY,
universe_id INTEGER NOT NULL REFERENCES dev_universe(id) ON DELETE CASCADE,
symbol TEXT NOT NULL,
start_at DATE NOT NULL,
end_at DATE,
created_at TIMESTAMP DEFAULT NOW(),
UNIQUE(universe_id, symbol, start_at)
);
""")

# 2. Instrument and reference data tables
await conn.execute("""
CREATE TABLE IF NOT EXISTS dev_instruments (
id SERIAL PRIMARY KEY,
symbol TEXT UNIQUE NOT NULL,
name TEXT,
exchange TEXT,
currency TEXT DEFAULT 'USD',
created_at TIMESTAMP DEFAULT NOW(),
updated_at TIMESTAMP DEFAULT NOW()
);
""")

await conn.execute("""
CREATE TABLE IF NOT EXISTS dev_vendors (
id SERIAL PRIMARY KEY,
name TEXT UNIQUE NOT NULL,
description TEXT,
created_at TIMESTAMP DEFAULT NOW()
);
""")

await conn.execute("""
INSERT INTO dev_vendors (id, name, description) VALUES 
(1, 'polygon', 'Polygon.io market data'),
(2, 'tiingo', 'Tiingo market data API'),
(3, 'ticker', 'Standard ticker symbols'),
(4, 'finnhub', 'Finnhub market data'),
(5, 'alpha_vantage', 'Alpha Vantage API')
ON CONFLICT (name) DO NOTHING;
""")

await conn.execute("""
CREATE TABLE IF NOT EXISTS dev_instrument_xrefs (
id SERIAL PRIMARY KEY,
instrument_id INTEGER NOT NULL REFERENCES dev_instruments(id) ON DELETE CASCADE,
vendor_id INTEGER NOT NULL REFERENCES dev_vendors(id),
vendor_symbol TEXT NOT NULL,
created_at TIMESTAMP DEFAULT NOW(),
UNIQUE(instrument_id, vendor_id)
);
""")

# 3. Price data tables (time-series optimized)
await conn.execute("""
CREATE TABLE IF NOT EXISTS dev_daily_prices_polygon (
id SERIAL PRIMARY KEY,
instrument_id INTEGER NOT NULL REFERENCES dev_instruments(id),
date DATE NOT NULL,
open_price DECIMAL,
high_price DECIMAL,
low_price DECIMAL,
close DECIMAL NOT NULL,
volume BIGINT NOT NULL,
vwap DECIMAL,
transactions INTEGER,
created_at TIMESTAMP DEFAULT NOW(),
updated_at TIMESTAMP DEFAULT NOW(),
UNIQUE(instrument_id, date)
);
""")

# Convert to hypertable for time-series optimization
try:
await conn.execute("""
SELECT create_hypertable('dev_daily_prices_polygon', 'date', 
if_not_exists => TRUE);
""")
logger.info("✅ Created hypertable for price data")
except Exception as e:
logger.warning(f"Hypertable creation warning (may already exist): {e}")

# 4. Market cap data table
await conn.execute("""
CREATE TABLE IF NOT EXISTS dev_daily_market_cap (
id SERIAL PRIMARY KEY,
instrument_id INTEGER NOT NULL REFERENCES dev_instruments(id),
date DATE NOT NULL,
market_cap BIGINT,
shares_outstanding BIGINT,
created_at TIMESTAMP DEFAULT NOW(),
UNIQUE(instrument_id, date)
);
""")

# 5. Additional useful tables
await conn.execute("""
CREATE TABLE IF NOT EXISTS dev_stock_splits (
id SERIAL PRIMARY KEY,
instrument_id INTEGER NOT NULL REFERENCES dev_instruments(id),
ex_date DATE NOT NULL,
split_ratio DECIMAL NOT NULL,
created_at TIMESTAMP DEFAULT NOW(),
UNIQUE(instrument_id, ex_date)
);
""")

await conn.execute("""
CREATE TABLE IF NOT EXISTS dev_dividends (
id SERIAL PRIMARY KEY,
instrument_id INTEGER NOT NULL REFERENCES dev_instruments(id),
ex_date DATE NOT NULL,
pay_date DATE,
amount DECIMAL NOT NULL,
currency TEXT DEFAULT 'USD',
created_at TIMESTAMP DEFAULT NOW(),
UNIQUE(instrument_id, ex_date)
);
""")

# 6. Create useful indexes
await conn.execute("""
CREATE INDEX IF NOT EXISTS idx_daily_prices_date 
ON dev_daily_prices_polygon(date);
""")

await conn.execute("""
CREATE INDEX IF NOT EXISTS idx_daily_prices_instrument_date 
ON dev_daily_prices_polygon(instrument_id, date);
""")

await conn.execute("""
CREATE INDEX IF NOT EXISTS idx_universe_membership_universe_id 
ON dev_universe_membership(universe_id);
""")

await conn.execute("""
CREATE INDEX IF NOT EXISTS idx_instrument_xrefs_vendor 
ON dev_instrument_xrefs(vendor_id, vendor_symbol);
""")

# 7. Create useful views
await conn.execute("""
CREATE OR REPLACE VIEW universe_volume_analysis AS
WITH recent_metrics AS (
SELECT 
xr.vendor_symbol as symbol,
i.name as instrument_name,
COUNT(*) as trading_days,
AVG(dp.close) as avg_price,
AVG(dp.volume) as avg_volume,
AVG(dp.close * dp.volume) as avg_dollar_volume,
MIN(dp.date) as first_date,
MAX(dp.date) as last_date
FROM dev_daily_prices_polygon dp
JOIN dev_instrument_xrefs xr ON dp.instrument_id = xr.instrument_id
JOIN dev_instruments i ON dp.instrument_id = i.id
WHERE xr.vendor_id = 3  -- ticker symbols
AND dp.date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY xr.vendor_symbol, i.name
)
SELECT 
symbol,
instrument_name,
trading_days,
ROUND(avg_price, 2) as avg_price,
avg_volume,
ROUND(avg_dollar_volume) as avg_dollar_volume,
CASE 
WHEN avg_dollar_volume >= 1000000000 THEN 'Tier 1 (>$1B)'
WHEN avg_dollar_volume >= 100000000 THEN 'Tier 2 ($100M-$1B)'  
WHEN avg_dollar_volume >= 10000000 THEN 'Tier 3 ($10M-$100M)'
ELSE 'Below $10M'
END as volume_tier,
first_date,
last_date
FROM recent_metrics
ORDER BY avg_dollar_volume DESC;
""")

# Grant proper permissions
await conn.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;")
await conn.execute("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;")

logger.info("✅ All database migrations completed successfully")

# Verify schema
table_check = await conn.fetch("""
SELECT tablename 
FROM pg_tables 
WHERE schemaname = 'public' 
AND tablename LIKE 'dev_%'
ORDER BY tablename;
""")

logger.info(f"📋 Created tables:")
for table in table_check:
logger.info(f"  ✓ {table['tablename']}")

# Check if we have any existing data
data_summary = await conn.fetchrow("""
SELECT 
(SELECT COUNT(*) FROM dev_universe) as universes,
(SELECT COUNT(*) FROM dev_instruments) as instruments,
(SELECT COUNT(*) FROM dev_daily_prices_polygon) as price_records,
(SELECT COUNT(*) FROM dev_universe_membership) as memberships
""")

logger.info(f"📊 Current data summary:")
logger.info(f"  Universes: {data_summary['universes']}")
logger.info(f"  Instruments: {data_summary['instruments']}")
logger.info(f"  Price records: {data_summary['price_records']}")
logger.info(f"  Universe memberships: {data_summary['memberships']}")

print("🎉 DATABASE MIGRATION COMPLETED SUCCESSFULLY!")
print(f"Created {len(table_check)} tables with proper schema")
print("Database is ready for universe creation")

finally:
await conn.close()

if __name__ == "__main__":
asyncio.run(main())
