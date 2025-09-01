-- Migration 044: Enhanced Fundamental Data Schema
-- Add comprehensive fundamental data tables with multi-vendor support

-- Create comprehensive fundamental data table
CREATE TABLE IF NOT EXISTS dev_fundamental_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    vendor VARCHAR(20) NOT NULL,
    fiscal_period VARCHAR(10) NOT NULL,  -- FY, Q1, Q2, Q3, Q4
    
    -- Income Statement Data
    revenue BIGINT,
    gross_profit BIGINT,
    operating_income BIGINT,
    net_income BIGINT,
    ebitda BIGINT,
    eps DECIMAL(10, 4),
    
    -- Balance Sheet Data
    total_assets BIGINT,
    total_liabilities BIGINT,
    shareholders_equity BIGINT,
    current_assets BIGINT,
    current_liabilities BIGINT,
    total_debt BIGINT,
    
    -- Cash Flow Data
    operating_cash_flow BIGINT,
    investing_cash_flow BIGINT,
    financing_cash_flow BIGINT,
    free_cash_flow BIGINT,
    
    -- Market Data
    market_cap BIGINT,
    
    -- Financial Ratios
    pe_ratio DECIMAL(10, 4),
    pb_ratio DECIMAL(10, 4),
    debt_to_equity DECIMAL(10, 4),
    roe DECIMAL(10, 4),
    roa DECIMAL(10, 4),
    current_ratio DECIMAL(10, 4),
    quick_ratio DECIMAL(10, 4),
    
    -- Quality and metadata
    quality_score DECIMAL(4, 3) DEFAULT 1.0,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(symbol, date, vendor, fiscal_period),
    CHECK (quality_score >= 0 AND quality_score <= 1)
);

-- Create index for efficient querying
CREATE INDEX IF NOT EXISTS idx_fundamental_data_symbol_date ON dev_fundamental_data(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_fundamental_data_vendor ON dev_fundamental_data(vendor);
CREATE INDEX IF NOT EXISTS idx_fundamental_data_fiscal_period ON dev_fundamental_data(fiscal_period);
CREATE INDEX IF NOT EXISTS idx_fundamental_data_symbol_vendor ON dev_fundamental_data(symbol, vendor);
CREATE INDEX IF NOT EXISTS idx_fundamental_data_date ON dev_fundamental_data(date DESC);

-- Create fundamental data quality metrics table
CREATE TABLE IF NOT EXISTS dev_fundamental_quality_metrics (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    vendor VARCHAR(20) NOT NULL,
    date_range_start DATE NOT NULL,
    date_range_end DATE NOT NULL,
    
    -- Quality metrics
    total_records INTEGER DEFAULT 0,
    complete_records INTEGER DEFAULT 0,
    missing_revenue INTEGER DEFAULT 0,
    missing_net_income INTEGER DEFAULT 0,
    missing_total_assets INTEGER DEFAULT 0,
    quality_score DECIMAL(4, 3),
    
    -- Data coverage
    annual_records INTEGER DEFAULT 0,
    quarterly_records INTEGER DEFAULT 0,
    years_covered INTEGER DEFAULT 0,
    
    -- Metadata
    first_record_date DATE,
    last_record_date DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(symbol, vendor, date_range_start, date_range_end),
    CHECK (quality_score >= 0 AND quality_score <= 1),
    CHECK (complete_records <= total_records)
);

-- Create cross-vendor reconciliation table
CREATE TABLE IF NOT EXISTS dev_fundamental_reconciliation (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    fiscal_period VARCHAR(10) NOT NULL,
    
    -- Cross-vendor comparison metrics
    vendor_count INTEGER DEFAULT 0,
    vendors TEXT[],
    
    -- Revenue reconciliation
    revenue_values BIGINT[],
    revenue_variance DECIMAL(10, 4),
    revenue_consensus BIGINT,
    
    -- Net income reconciliation
    net_income_values BIGINT[],
    net_income_variance DECIMAL(10, 4),
    net_income_consensus BIGINT,
    
    -- Quality flags
    high_variance BOOLEAN DEFAULT FALSE,
    data_quality VARCHAR(20) DEFAULT 'unknown',  -- high, medium, low, unknown
    reconciliation_notes TEXT,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(symbol, date, fiscal_period),
    CHECK (vendor_count >= 0),
    CHECK (data_quality IN ('high', 'medium', 'low', 'unknown'))
);

-- Create index for reconciliation table
CREATE INDEX IF NOT EXISTS idx_fundamental_reconciliation_symbol_date ON dev_fundamental_reconciliation(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_fundamental_reconciliation_quality ON dev_fundamental_reconciliation(data_quality);
CREATE INDEX IF NOT EXISTS idx_fundamental_reconciliation_variance ON dev_fundamental_reconciliation(high_variance);

-- Create population tracking table
CREATE TABLE IF NOT EXISTS dev_fundamental_population_log (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    vendor VARCHAR(20),
    start_date DATE,
    end_date DATE,
    
    -- Population results
    records_fetched INTEGER DEFAULT 0,
    records_stored INTEGER DEFAULT 0,
    api_calls INTEGER DEFAULT 0,
    processing_time_seconds INTEGER DEFAULT 0,
    
    -- Status
    status VARCHAR(20) DEFAULT 'pending',  -- pending, running, completed, failed
    error_message TEXT,
    quality_score DECIMAL(4, 3),
    
    -- Timestamps
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    CHECK (quality_score >= 0 AND quality_score <= 1),
    CHECK (records_stored <= records_fetched)
);

-- Create index for population tracking
CREATE INDEX IF NOT EXISTS idx_fundamental_population_symbol ON dev_fundamental_population_log(symbol);
CREATE INDEX IF NOT EXISTS idx_fundamental_population_vendor ON dev_fundamental_population_log(vendor);
CREATE INDEX IF NOT EXISTS idx_fundamental_population_status ON dev_fundamental_population_log(status);
CREATE INDEX IF NOT EXISTS idx_fundamental_population_date ON dev_fundamental_population_log(created_at DESC);

-- Create updated_at trigger function if it doesn't exist
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add updated_at triggers
CREATE TRIGGER update_fundamental_data_updated_at 
    BEFORE UPDATE ON dev_fundamental_data 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_fundamental_quality_metrics_updated_at 
    BEFORE UPDATE ON dev_fundamental_quality_metrics 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_fundamental_reconciliation_updated_at 
    BEFORE UPDATE ON dev_fundamental_reconciliation 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Add comments for documentation
COMMENT ON TABLE dev_fundamental_data IS 'Comprehensive fundamental data from multiple vendors with quality scoring';
COMMENT ON TABLE dev_fundamental_quality_metrics IS 'Quality metrics and data coverage statistics per symbol/vendor';
COMMENT ON TABLE dev_fundamental_reconciliation IS 'Cross-vendor data reconciliation and consensus values';
COMMENT ON TABLE dev_fundamental_population_log IS 'Tracking and monitoring of fundamental data population processes';

-- Create views for common queries
CREATE OR REPLACE VIEW dev_fundamental_summary AS
SELECT 
    symbol,
    vendor,
    COUNT(*) as total_records,
    COUNT(CASE WHEN revenue IS NOT NULL THEN 1 END) as records_with_revenue,
    COUNT(CASE WHEN fiscal_period = 'FY' THEN 1 END) as annual_records,
    COUNT(CASE WHEN fiscal_period LIKE 'Q%' THEN 1 END) as quarterly_records,
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    AVG(quality_score) as avg_quality_score,
    COUNT(DISTINCT EXTRACT(YEAR FROM date)) as years_covered
FROM dev_fundamental_data 
GROUP BY symbol, vendor;

COMMENT ON VIEW dev_fundamental_summary IS 'Summary statistics of fundamental data coverage per symbol and vendor';

-- Grant appropriate permissions (if needed)
-- GRANT SELECT, INSERT, UPDATE ON dev_fundamental_data TO your_app_user;
-- GRANT SELECT, INSERT, UPDATE ON dev_fundamental_quality_metrics TO your_app_user;
-- GRANT SELECT, INSERT, UPDATE ON dev_fundamental_reconciliation TO your_app_user;
-- GRANT SELECT, INSERT, UPDATE ON dev_fundamental_population_log TO your_app_user;