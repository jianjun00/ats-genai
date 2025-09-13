-- Migration 091: Drop old daily_prices_* tables after renaming to daily_price_*
-- This migration removes the old table names that used plural naming convention
-- The new singular naming convention is now standardized across the codebase

-- Drop old daily_prices_polygon table if it exists
DROP TABLE IF EXISTS daily_prices_polygon CASCADE;

-- Drop old daily_prices_tiingo table if it exists  
DROP TABLE IF EXISTS daily_prices_tiingo CASCADE;

-- Drop old daily_prices_eodhd table if it exists
DROP TABLE IF EXISTS daily_prices_eodhd CASCADE;

-- Drop old daily_prices_polygon_30year table if it exists (legacy naming)
DROP TABLE IF EXISTS daily_prices_polygon_30year CASCADE;

-- Drop old daily_prices_tiingo_30year table if it exists (legacy naming)
DROP TABLE IF EXISTS daily_prices_tiingo_30year CASCADE;

-- Drop old daily_prices_eodhd_30year table if it exists (legacy naming)
DROP TABLE IF EXISTS daily_prices_eodhd_30year CASCADE;

-- Drop any associated audit tables for the old naming convention
DROP TABLE IF EXISTS daily_prices_polygon_audit CASCADE;
DROP TABLE IF EXISTS daily_prices_tiingo_audit CASCADE;  
DROP TABLE IF EXISTS daily_prices_eodhd_audit CASCADE;

-- Drop any associated indexes that might still reference old table names
-- Note: Most indexes should be dropped automatically with CASCADE, but being explicit

-- Log successful completion
SELECT 'Migration 091: Successfully dropped old daily_prices_* tables' AS migration_status;