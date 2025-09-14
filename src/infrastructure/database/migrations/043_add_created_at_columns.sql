-- Migration 043: Add created_at columns to tables missing them
-- This ensures all tables have consistent timestamp tracking

-- Add created_at to dev_daily_market_cap
ALTER TABLE dev_daily_market_cap 
ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Add created_at to dev_daily_prices
ALTER TABLE dev_daily_prices 
ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Add created_at to dev_daily_prices_polygon
ALTER TABLE dev_daily_prices_polygon 
ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Add created_at to dev_daily_prices_tiingo
ALTER TABLE dev_daily_prices_tiingo 
ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Add created_at to dev_fundamentals
ALTER TABLE dev_fundamentals 
ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Add created_at to dev_fundamentals_checkpoint
ALTER TABLE dev_fundamentals_checkpoint 
ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Add created_at to dev_instrument_aliases
ALTER TABLE dev_instrument_aliases 
ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Add created_at to dev_instrument_tiingo_end_date_backup
ALTER TABLE dev_instrument_tiingo_end_date_backup 
ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Add created_at to dev_realtime_collection_status
ALTER TABLE dev_realtime_collection_status 
ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Add created_at to dev_realtime_gaps
ALTER TABLE dev_realtime_gaps 
ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Add created_at to dev_status_code
ALTER TABLE dev_status_code 
ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Add created_at to dev_universe
ALTER TABLE dev_universe 
ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Add created_at to dev_universe_membership
ALTER TABLE dev_universe_membership 
ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Create triggers to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Add triggers for automatic updated_at maintenance
CREATE TRIGGER update_dev_daily_market_cap_updated_at 
    BEFORE UPDATE ON dev_daily_market_cap 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dev_daily_prices_updated_at 
    BEFORE UPDATE ON dev_daily_prices 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dev_daily_prices_polygon_updated_at 
    BEFORE UPDATE ON dev_daily_prices_polygon 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dev_daily_prices_tiingo_updated_at 
    BEFORE UPDATE ON dev_daily_prices_tiingo 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dev_fundamentals_updated_at 
    BEFORE UPDATE ON dev_fundamentals 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dev_fundamentals_checkpoint_updated_at 
    BEFORE UPDATE ON dev_fundamentals_checkpoint 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dev_instrument_aliases_updated_at 
    BEFORE UPDATE ON dev_instrument_aliases 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dev_instrument_tiingo_end_date_backup_updated_at 
    BEFORE UPDATE ON dev_instrument_tiingo_end_date_backup 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dev_realtime_collection_status_updated_at 
    BEFORE UPDATE ON dev_realtime_collection_status 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dev_realtime_gaps_updated_at 
    BEFORE UPDATE ON dev_realtime_gaps 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dev_status_code_updated_at 
    BEFORE UPDATE ON dev_status_code 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dev_universe_updated_at 
    BEFORE UPDATE ON dev_universe 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dev_universe_membership_updated_at 
    BEFORE UPDATE ON dev_universe_membership 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add index on created_at columns for performance
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_daily_market_cap_created_at ON dev_daily_market_cap(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_daily_prices_created_at ON dev_daily_prices(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_daily_prices_polygon_created_at ON dev_daily_prices_polygon(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_daily_prices_tiingo_created_at ON dev_daily_prices_tiingo(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_fundamentals_created_at ON dev_fundamentals(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_fundamentals_checkpoint_created_at ON dev_fundamentals_checkpoint(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_instrument_aliases_created_at ON dev_instrument_aliases(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_instrument_tiingo_end_date_backup_created_at ON dev_instrument_tiingo_end_date_backup(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_realtime_collection_status_created_at ON dev_realtime_collection_status(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_realtime_gaps_created_at ON dev_realtime_gaps(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_status_code_created_at ON dev_status_code(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_universe_created_at ON dev_universe(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_universe_membership_created_at ON dev_universe_membership(created_at);

-- Comment for documentation
COMMENT ON FUNCTION update_updated_at_column() IS 'Trigger function to automatically update updated_at timestamp';