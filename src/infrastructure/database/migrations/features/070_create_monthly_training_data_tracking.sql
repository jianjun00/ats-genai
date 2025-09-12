-- Migration: Create monthly training data tracking table
-- Date: 2025-09-11
-- Purpose: Add granular monthly tracking for training data with timeframe file paths

-- Create monthly training data tracking tables for all environments
CREATE TABLE IF NOT EXISTS dev_monthly_training_data (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    instrument_id INTEGER,
    year_month DATE NOT NULL, -- First day of the month (e.g., '2025-07-01')
    
    -- Timeframe file paths as JSONB for flexible storage
    timeframe_paths JSONB DEFAULT '{}', -- e.g., {"5m": "/path/to/5m.arrayrecord", "15m": "/path/to/15m.arrayrecord", ...}
    
    -- Metadata for quick filtering and sorting
    total_records INTEGER DEFAULT 0, -- Number of records in this month
    file_size_mb FLOAT DEFAULT 0.0, -- Total size of all timeframe files for this month
    data_quality_score FLOAT DEFAULT 0.0, -- Quality score for this month's data
    
    -- Status tracking
    status VARCHAR(50) DEFAULT 'created', -- created, processing, completed, failed
    error_message TEXT DEFAULT '',
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT unique_dev_monthly_training_run_symbol_month UNIQUE(run_id, symbol, year_month)
);

CREATE TABLE IF NOT EXISTS intg_monthly_training_data (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    instrument_id INTEGER,
    year_month DATE NOT NULL, -- First day of the month (e.g., '2025-07-01')
    
    -- Timeframe file paths as JSONB for flexible storage
    timeframe_paths JSONB DEFAULT '{}', -- e.g., {"5m": "/path/to/5m.arrayrecord", "15m": "/path/to/15m.arrayrecord", ...}
    
    -- Metadata for quick filtering and sorting
    total_records INTEGER DEFAULT 0, -- Number of records in this month
    file_size_mb FLOAT DEFAULT 0.0, -- Total size of all timeframe files for this month
    data_quality_score FLOAT DEFAULT 0.0, -- Quality score for this month's data
    
    -- Status tracking
    status VARCHAR(50) DEFAULT 'created', -- created, processing, completed, failed
    error_message TEXT DEFAULT '',
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT unique_intg_monthly_training_run_symbol_month UNIQUE(run_id, symbol, year_month)
);

-- Add test table conditionally
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'test_runs') THEN
        CREATE TABLE IF NOT EXISTS test_monthly_training_data (
            id SERIAL PRIMARY KEY,
            run_id INTEGER NOT NULL,
            symbol VARCHAR(10) NOT NULL,
            instrument_id INTEGER,
            year_month DATE NOT NULL,
            timeframe_paths JSONB DEFAULT '{}',
            total_records INTEGER DEFAULT 0,
            file_size_mb FLOAT DEFAULT 0.0,
            data_quality_score FLOAT DEFAULT 0.0,
            status VARCHAR(50) DEFAULT 'created',
            error_message TEXT DEFAULT '',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            CONSTRAINT unique_test_monthly_training_run_symbol_month UNIQUE(run_id, symbol, year_month)
        );
    END IF;
END $$;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_dev_monthly_training_run_id ON dev_monthly_training_data(run_id);
CREATE INDEX IF NOT EXISTS idx_dev_monthly_training_symbol ON dev_monthly_training_data(symbol);
CREATE INDEX IF NOT EXISTS idx_dev_monthly_training_year_month ON dev_monthly_training_data(year_month);
CREATE INDEX IF NOT EXISTS idx_dev_monthly_training_status ON dev_monthly_training_data(status);
CREATE INDEX IF NOT EXISTS idx_dev_monthly_training_created_at ON dev_monthly_training_data(created_at DESC);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_dev_monthly_training_symbol_month ON dev_monthly_training_data(symbol, year_month);
CREATE INDEX IF NOT EXISTS idx_dev_monthly_training_run_symbol ON dev_monthly_training_data(run_id, symbol);

-- Same indexes for intg environment
CREATE INDEX IF NOT EXISTS idx_intg_monthly_training_run_id ON intg_monthly_training_data(run_id);
CREATE INDEX IF NOT EXISTS idx_intg_monthly_training_symbol ON intg_monthly_training_data(symbol);
CREATE INDEX IF NOT EXISTS idx_intg_monthly_training_year_month ON intg_monthly_training_data(year_month);
CREATE INDEX IF NOT EXISTS idx_intg_monthly_training_status ON intg_monthly_training_data(status);
CREATE INDEX IF NOT EXISTS idx_intg_monthly_training_created_at ON intg_monthly_training_data(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_intg_monthly_training_symbol_month ON intg_monthly_training_data(symbol, year_month);
CREATE INDEX IF NOT EXISTS idx_intg_monthly_training_run_symbol ON intg_monthly_training_data(run_id, symbol);

-- Add foreign key constraints if runs tables exist
DO $$ 
BEGIN
    -- Link to runs table if it exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_runs') THEN
        ALTER TABLE dev_monthly_training_data 
        ADD CONSTRAINT fk_dev_monthly_training_run_id 
        FOREIGN KEY (run_id) REFERENCES dev_runs(id) ON DELETE CASCADE;
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'intg_runs') THEN
        ALTER TABLE intg_monthly_training_data 
        ADD CONSTRAINT fk_intg_monthly_training_run_id 
        FOREIGN KEY (run_id) REFERENCES intg_runs(id) ON DELETE CASCADE;
    END IF;
    
    -- Link to instruments table if it exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_instruments') THEN
        ALTER TABLE dev_monthly_training_data 
        ADD CONSTRAINT fk_dev_monthly_training_instrument_id 
        FOREIGN KEY (instrument_id) REFERENCES dev_instruments(id) ON DELETE SET NULL;
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'intg_instruments') THEN
        ALTER TABLE intg_monthly_training_data 
        ADD CONSTRAINT fk_intg_monthly_training_instrument_id 
        FOREIGN KEY (instrument_id) REFERENCES intg_instruments(id) ON DELETE SET NULL;
    END IF;
END $$;

-- Add column comments for documentation
COMMENT ON TABLE dev_monthly_training_data IS 'Monthly granular tracking of training data files with timeframe paths for EDA navigation';
COMMENT ON COLUMN dev_monthly_training_data.timeframe_paths IS 'JSONB mapping of timeframe names to ArrayRecord file paths';
COMMENT ON COLUMN dev_monthly_training_data.year_month IS 'First day of the month for this training data (e.g., 2025-07-01 for July 2025)';
COMMENT ON COLUMN dev_monthly_training_data.total_records IS 'Number of training records in this month for this symbol';
COMMENT ON COLUMN dev_monthly_training_data.data_quality_score IS 'Data quality score for this month (0.0-1.0)';

COMMENT ON TABLE intg_monthly_training_data IS 'Monthly granular tracking of training data files with timeframe paths for EDA navigation';
COMMENT ON COLUMN intg_monthly_training_data.timeframe_paths IS 'JSONB mapping of timeframe names to ArrayRecord file paths';
COMMENT ON COLUMN intg_monthly_training_data.year_month IS 'First day of the month for this training data (e.g., 2025-07-01 for July 2025)';
COMMENT ON COLUMN intg_monthly_training_data.total_records IS 'Number of training records in this month for this symbol';
COMMENT ON COLUMN intg_monthly_training_data.data_quality_score IS 'Data quality score for this month (0.0-1.0)';

-- Create view for easy querying with instrument details
-- Handle cases where market_cap column may not exist in instruments table
DO $$ 
BEGIN
    -- Create dev view with conditional market_cap column
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'dev_instruments' AND column_name = 'market_cap') THEN
        EXECUTE 'CREATE OR REPLACE VIEW dev_monthly_training_data_with_instruments AS
        SELECT 
            mtd.*,
            i.name as instrument_name,
            i.exchange,
            i.sector,
            i.market_cap
        FROM dev_monthly_training_data mtd
        LEFT JOIN dev_instruments i ON mtd.instrument_id = i.id';
    ELSE
        EXECUTE 'CREATE OR REPLACE VIEW dev_monthly_training_data_with_instruments AS
        SELECT 
            mtd.*,
            i.name as instrument_name,
            i.exchange,
            i.sector
        FROM dev_monthly_training_data mtd
        LEFT JOIN dev_instruments i ON mtd.instrument_id = i.id';
    END IF;
    
    -- Create intg view with conditional market_cap column
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'intg_instruments' AND column_name = 'market_cap') THEN
        EXECUTE 'CREATE OR REPLACE VIEW intg_monthly_training_data_with_instruments AS
        SELECT 
            mtd.*,
            i.name as instrument_name,
            i.exchange,
            i.sector,
            i.market_cap
        FROM intg_monthly_training_data mtd
        LEFT JOIN intg_instruments i ON mtd.instrument_id = i.id';
    ELSE
        EXECUTE 'CREATE OR REPLACE VIEW intg_monthly_training_data_with_instruments AS
        SELECT 
            mtd.*,
            i.name as instrument_name,
            i.exchange,
            i.sector
        FROM intg_monthly_training_data mtd
        LEFT JOIN intg_instruments i ON mtd.instrument_id = i.id';
    END IF;
END $$;

-- Sample query examples for documentation
/*
-- Example usage queries:

-- 1. Get all monthly records for a specific symbol
SELECT * FROM dev_monthly_training_data 
WHERE symbol = 'AAPL' 
ORDER BY year_month DESC;

-- 2. Get timeframe paths for specific month and symbol
SELECT 
    symbol, 
    year_month,
    timeframe_paths->>'5m' as path_5m,
    timeframe_paths->>'15m' as path_15m,
    timeframe_paths->>'1h' as path_1h,
    timeframe_paths->>'1d' as path_1d
FROM dev_monthly_training_data 
WHERE symbol = 'TSLA' AND year_month = '2025-07-01';

-- 3. Get all months with data for EDA table view
SELECT DISTINCT 
    symbol,
    year_month,
    total_records,
    file_size_mb,
    data_quality_score,
    status
FROM dev_monthly_training_data 
WHERE status = 'completed'
ORDER BY symbol, year_month DESC;

-- 4. Filter by date range and multiple symbols
SELECT * FROM dev_monthly_training_data_with_instruments
WHERE symbol IN ('AAPL', 'TSLA', 'MSFT')
  AND year_month BETWEEN '2025-01-01' AND '2025-12-01'
  AND status = 'completed'
ORDER BY symbol, year_month;

-- 5. Get summary statistics by symbol
SELECT 
    symbol,
    COUNT(*) as total_months,
    SUM(total_records) as total_records_all_months,
    AVG(data_quality_score) as avg_quality_score,
    SUM(file_size_mb) as total_size_mb
FROM dev_monthly_training_data
WHERE status = 'completed'
GROUP BY symbol
ORDER BY total_records_all_months DESC;
*/