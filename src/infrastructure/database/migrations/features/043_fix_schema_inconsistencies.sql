-- Fix schema inconsistencies identified in production logs
-- Migration: 043_fix_schema_inconsistencies.sql

-- Create missing instrument tables if they don't exist
CREATE TABLE IF NOT EXISTS dev_instruments_tiingo (
    instrument_id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    name TEXT,
    exchange VARCHAR(50),
    sector VARCHAR(100),
    list_date DATE,
    delist_date DATE,
    vendor VARCHAR(20) DEFAULT 'tiingo',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    active BOOLEAN DEFAULT true,
    extra JSONB
);

CREATE TABLE IF NOT EXISTS dev_instruments_polygon (
    instrument_id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    name TEXT,
    exchange VARCHAR(50),
    sector VARCHAR(100),
    list_date DATE,
    delist_date DATE,
    vendor VARCHAR(20) DEFAULT 'polygon',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    active BOOLEAN DEFAULT true,
    extra JSONB
);

CREATE TABLE IF NOT EXISTS dev_instruments_eodhd (
    instrument_id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    name TEXT,
    exchange VARCHAR(50),
    sector VARCHAR(100),
    list_date DATE,
    delist_date DATE,
    vendor VARCHAR(20) DEFAULT 'eodhd',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    active BOOLEAN DEFAULT true,
    extra JSONB
);

-- Add created_at and updated_at columns to tables that need them (if they don't exist)
DO $$
BEGIN
    -- Fix realtime_collection_status table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_realtime_collection_status') THEN
        -- Add created_at if it doesn't exist
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name = 'dev_realtime_collection_status' AND column_name = 'created_at') THEN
            ALTER TABLE dev_realtime_collection_status 
            ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
        END IF;
        
        -- Add updated_at if it doesn't exist  
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name = 'dev_realtime_collection_status' AND column_name = 'updated_at') THEN
            ALTER TABLE dev_realtime_collection_status 
            ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
        END IF;
    END IF;

    -- Fix realtime_gaps table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_realtime_gaps') THEN
        -- Add created_at if it doesn't exist
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name = 'dev_realtime_gaps' AND column_name = 'created_at') THEN
            ALTER TABLE dev_realtime_gaps 
            ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
        END IF;
        
        -- Add updated_at if it doesn't exist
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name = 'dev_realtime_gaps' AND column_name = 'updated_at') THEN
            ALTER TABLE dev_realtime_gaps 
            ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
        END IF;
    END IF;

    -- Fix db_version table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_db_version') THEN
        -- Add created_at if it doesn't exist
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name = 'dev_db_version' AND column_name = 'created_at') THEN
            ALTER TABLE dev_db_version 
            ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
        END IF;
    END IF;
END
$$;

-- Create indexes safely (only if columns exist)
DO $$
BEGIN
    -- Index for dev_realtime_collection_status
    IF EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name = 'dev_realtime_collection_status' AND column_name = 'created_at') THEN
        -- Use regular CREATE INDEX instead of CONCURRENTLY within a function
        CREATE INDEX IF NOT EXISTS idx_dev_realtime_collection_status_created_at 
        ON dev_realtime_collection_status(created_at);
    END IF;

    -- Index for dev_realtime_gaps  
    IF EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name = 'dev_realtime_gaps' AND column_name = 'created_at') THEN
        CREATE INDEX IF NOT EXISTS idx_dev_realtime_gaps_created_at 
        ON dev_realtime_gaps(created_at);
    END IF;

    -- Index for dev_db_version
    IF EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name = 'dev_db_version' AND column_name = 'created_at') THEN
        CREATE INDEX IF NOT EXISTS idx_dev_db_version_created_at 
        ON dev_db_version(created_at);
    END IF;
END
$$;

-- Create indexes for instrument tables
CREATE INDEX IF NOT EXISTS idx_dev_instruments_tiingo_symbol ON dev_instruments_tiingo(symbol);
CREATE INDEX IF NOT EXISTS idx_dev_instruments_tiingo_active ON dev_instruments_tiingo(active);
CREATE INDEX IF NOT EXISTS idx_dev_instruments_tiingo_vendor ON dev_instruments_tiingo(vendor);

CREATE INDEX IF NOT EXISTS idx_dev_instruments_polygon_symbol ON dev_instruments_polygon(symbol);
CREATE INDEX IF NOT EXISTS idx_dev_instruments_polygon_active ON dev_instruments_polygon(active);  
CREATE INDEX IF NOT EXISTS idx_dev_instruments_polygon_vendor ON dev_instruments_polygon(vendor);

CREATE INDEX IF NOT EXISTS idx_dev_instruments_eodhd_symbol ON dev_instruments_eodhd(symbol);
CREATE INDEX IF NOT EXISTS idx_dev_instruments_eodhd_active ON dev_instruments_eodhd(active);
CREATE INDEX IF NOT EXISTS idx_dev_instruments_eodhd_vendor ON dev_instruments_eodhd(vendor);

-- Add missing columns to daily price tables if they exist
DO $$
BEGIN
    -- Fix daily prices tables to include created_at if missing
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_daily_price_eodhd') THEN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name = 'dev_daily_price_eodhd' AND column_name = 'created_at') THEN
            ALTER TABLE dev_daily_price_eodhd 
            ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
        END IF;
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_daily_price_polygon') THEN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name = 'dev_daily_price_polygon' AND column_name = 'created_at') THEN
            ALTER TABLE dev_daily_price_polygon 
            ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
        END IF;
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_daily_price_tiingo') THEN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name = 'dev_daily_price_tiingo' AND column_name = 'created_at') THEN
            ALTER TABLE dev_daily_price_tiingo 
            ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
        END IF;
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_fundamentals_comprehensive') THEN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name = 'dev_fundamentals_comprehensive' AND column_name = 'created_at') THEN
            ALTER TABLE dev_fundamentals_comprehensive 
            ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
        END IF;
    END IF;
END
$$;