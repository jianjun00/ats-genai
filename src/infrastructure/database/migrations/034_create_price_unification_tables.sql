-- Migration 034: Create price unification tables

-- Runs table to track price unification job executions
CREATE TABLE IF NOT EXISTS runs (
    id SERIAL PRIMARY KEY,
    run_type TEXT NOT NULL, -- 'daily_price_unification', 'backfill', etc.
    start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
    end_time TIMESTAMP WITHOUT TIME ZONE,
    status TEXT NOT NULL DEFAULT 'running', -- 'running', 'completed', 'failed', 'cancelled'
    command_line TEXT,
    git_commit_hash TEXT,
    git_branch TEXT,
    environment TEXT,
    parameters JSONB,
    results JSONB,
    error_message TEXT,
    created_by TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);

-- Index for efficient querying
CREATE INDEX IF NOT EXISTS idx_runs_run_type ON runs(run_type);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_start_time ON runs(start_time);

-- Validation status codes for price validation
CREATE TABLE IF NOT EXISTS price_validation_status (
    id SERIAL PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);

-- Insert standard validation status codes
INSERT INTO price_validation_status (code, description) VALUES 
    ('valid', 'Price passed all validation checks'),
    ('outlier_statistical', 'Price rejected due to statistical outlier (>4-6 sigma deviation)'),
    ('outlier_vendor_disagreement', 'Price rejected due to significant vendor disagreement'),
    ('missing_vendor_data', 'Insufficient vendor data for validation'),
    ('manual_review', 'Price flagged for manual review'),
    ('holiday_excluded', 'Trading date excluded due to market holiday'),
    ('corporate_action', 'Price adjusted due to detected corporate action'),
    ('data_quality_issue', 'Price rejected due to data quality concerns')
ON CONFLICT (code) DO NOTHING;

-- Unified daily prices table
CREATE TABLE IF NOT EXISTS daily_prices (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instruments(id),
    date DATE NOT NULL,
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    close NUMERIC NOT NULL,
    adj_close NUMERIC,
    volume BIGINT NOT NULL DEFAULT 0,
    dollar_volume BIGINT GENERATED ALWAYS AS (ROUND(close * volume::numeric)) STORED,
    
    -- Validation fields
    validation_status_id INTEGER NOT NULL REFERENCES price_validation_status(id),
    run_id INTEGER NOT NULL REFERENCES runs(id),
    
    -- Source tracking
    primary_vendor TEXT, -- 'polygon', 'tiingo', 'fmp', 'alphavantage', 'yfinance'
    secondary_vendors TEXT[], -- List of other vendors that provided data
    vendor_count INTEGER DEFAULT 1, -- Number of vendors that provided this price
    
    -- Price validation metadata
    price_variance NUMERIC, -- Variance across vendor prices
    statistical_score NUMERIC, -- Z-score or similar statistical measure
    confidence_score NUMERIC DEFAULT 1.0, -- 0.0 to 1.0, confidence in the price
    
    -- Vendor-specific prices for audit trail
    polygon_price NUMERIC,
    tiingo_price NUMERIC,
    fmp_price NUMERIC,
    alphavantage_price NUMERIC,
    yfinance_price NUMERIC,
    
    -- Validation notes
    validation_notes TEXT,
    rejection_reason TEXT,
    
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    
    -- Constraints
    CONSTRAINT daily_prices_instrument_date_unique UNIQUE (instrument_id, date),
    CONSTRAINT daily_prices_positive_prices CHECK (close > 0 AND (adj_close IS NULL OR adj_close > 0)),
    CONSTRAINT daily_prices_positive_volume CHECK (volume >= 0),
    CONSTRAINT daily_prices_confidence_range CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_daily_prices_instrument_id ON daily_prices(instrument_id);
CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON daily_prices(date);
CREATE INDEX IF NOT EXISTS idx_daily_prices_instrument_date ON daily_prices(instrument_id, date);
CREATE INDEX IF NOT EXISTS idx_daily_prices_run_id ON daily_prices(run_id);
CREATE INDEX IF NOT EXISTS idx_daily_prices_validation_status ON daily_prices(validation_status_id);
CREATE INDEX IF NOT EXISTS idx_daily_prices_primary_vendor ON daily_prices(primary_vendor);
CREATE INDEX IF NOT EXISTS idx_daily_prices_confidence ON daily_prices(confidence_score);

-- Price validation details table for storing individual vendor validations
CREATE TABLE IF NOT EXISTS price_validation_details (
    id SERIAL PRIMARY KEY,
    daily_price_id INTEGER NOT NULL REFERENCES daily_prices(id) ON DELETE CASCADE,
    vendor TEXT NOT NULL,
    vendor_price NUMERIC,
    vendor_volume BIGINT,
    validation_passed BOOLEAN NOT NULL,
    validation_notes TEXT,
    statistical_score NUMERIC,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    
    CONSTRAINT price_validation_details_unique UNIQUE (daily_price_id, vendor)
);

CREATE INDEX IF NOT EXISTS idx_price_validation_details_daily_price ON price_validation_details(daily_price_id);
CREATE INDEX IF NOT EXISTS idx_price_validation_details_vendor ON price_validation_details(vendor);

-- Update trigger for updated_at columns
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_runs_updated_at BEFORE UPDATE ON runs 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_daily_prices_updated_at BEFORE UPDATE ON daily_prices 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();