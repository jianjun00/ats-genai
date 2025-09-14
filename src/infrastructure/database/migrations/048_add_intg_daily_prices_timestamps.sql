-- Migration 048: Add created_at and updated_at columns to intg_daily_prices tables
-- This ensures intg environment has same schema as dev environment for data sync

-- Add created_at and updated_at to intg_daily_prices_polygon (if exists)
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_daily_prices_polygon') THEN
        -- Check if columns already exist before adding
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name = 'intg_daily_prices_polygon' 
                      AND column_name = 'created_at') THEN
            ALTER TABLE intg_daily_prices_polygon 
            ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
        END IF;
    END IF;
END $$;

-- Add created_at and updated_at to intg_daily_prices_tiingo (if exists)
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_daily_prices_tiingo') THEN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name = 'intg_daily_prices_tiingo' 
                      AND column_name = 'created_at') THEN
            ALTER TABLE intg_daily_prices_tiingo 
            ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
        END IF;
    END IF;
END $$;

-- Add created_at and updated_at to intg_daily_prices_eodhd (if exists)
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_daily_prices_eodhd') THEN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name = 'intg_daily_prices_eodhd' 
                      AND column_name = 'created_at') THEN
            ALTER TABLE intg_daily_prices_eodhd 
            ADD COLUMN created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
        END IF;
    END IF;
END $$;

-- Create or replace the update trigger function (if not already exists)
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Add triggers for automatic updated_at maintenance (if tables exist)
DO $$
BEGIN
    -- Trigger for intg_daily_prices_polygon
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_daily_prices_polygon') THEN
        IF NOT EXISTS (SELECT 1 FROM information_schema.triggers 
                      WHERE trigger_name = 'update_intg_daily_prices_polygon_updated_at') THEN
            EXECUTE 'CREATE TRIGGER update_intg_daily_prices_polygon_updated_at 
                     BEFORE UPDATE ON intg_daily_prices_polygon 
                     FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()';
        END IF;
    END IF;
    
    -- Trigger for intg_daily_prices_tiingo
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_daily_prices_tiingo') THEN
        IF NOT EXISTS (SELECT 1 FROM information_schema.triggers 
                      WHERE trigger_name = 'update_intg_daily_prices_tiingo_updated_at') THEN
            EXECUTE 'CREATE TRIGGER update_intg_daily_prices_tiingo_updated_at 
                     BEFORE UPDATE ON intg_daily_prices_tiingo 
                     FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()';
        END IF;
    END IF;
    
    -- Trigger for intg_daily_prices_eodhd
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_daily_prices_eodhd') THEN
        IF NOT EXISTS (SELECT 1 FROM information_schema.triggers 
                      WHERE trigger_name = 'update_intg_daily_prices_eodhd_updated_at') THEN
            EXECUTE 'CREATE TRIGGER update_intg_daily_prices_eodhd_updated_at 
                     BEFORE UPDATE ON intg_daily_prices_eodhd 
                     FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()';
        END IF;
    END IF;
END $$;

-- Add performance indexes on created_at columns (if tables exist)
DO $$
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_daily_prices_polygon') THEN
        IF NOT EXISTS (SELECT 1 FROM pg_indexes 
                      WHERE tablename = 'intg_daily_prices_polygon' 
                      AND indexname = 'idx_intg_daily_prices_polygon_created_at') THEN
            CREATE INDEX CONCURRENTLY idx_intg_daily_prices_polygon_created_at 
            ON intg_daily_prices_polygon(created_at);
        END IF;
    END IF;
    
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_daily_prices_tiingo') THEN
        IF NOT EXISTS (SELECT 1 FROM pg_indexes 
                      WHERE tablename = 'intg_daily_prices_tiingo' 
                      AND indexname = 'idx_intg_daily_prices_tiingo_created_at') THEN
            CREATE INDEX CONCURRENTLY idx_intg_daily_prices_tiingo_created_at 
            ON intg_daily_prices_tiingo(created_at);
        END IF;
    END IF;
    
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_daily_prices_eodhd') THEN
        IF NOT EXISTS (SELECT 1 FROM pg_indexes 
                      WHERE tablename = 'intg_daily_prices_eodhd' 
                      AND indexname = 'idx_intg_daily_prices_eodhd_created_at') THEN
            CREATE INDEX CONCURRENTLY idx_intg_daily_prices_eodhd_created_at 
            ON intg_daily_prices_eodhd(created_at);
        END IF;
    END IF;
END $$;

-- Comment for documentation
COMMENT ON FUNCTION update_updated_at_column() IS 'Trigger function to automatically update updated_at timestamp';