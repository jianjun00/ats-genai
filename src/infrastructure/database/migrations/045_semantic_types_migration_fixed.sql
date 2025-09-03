-- Semantic Types Schema Migration (Fixed Version)
-- Part 2: Indexes and final setup (run after main migration)

-- ================================================
-- CREATE INDEXES ON SEMANTIC TYPES
-- ================================================

-- Indexes for better query performance on ENUMs
CREATE INDEX IF NOT EXISTS idx_dev_instruments_instrument_type 
    ON dev_instruments(instrument_type) WHERE instrument_type IS NOT NULL;
    
CREATE INDEX IF NOT EXISTS idx_dev_instruments_exchange_code 
    ON dev_instruments(exchange_code) WHERE exchange_code IS NOT NULL;
    
CREATE INDEX IF NOT EXISTS idx_dev_instruments_currency_code 
    ON dev_instruments(currency_code) WHERE currency_code IS NOT NULL;

-- Composite indexes for common filtering patterns
CREATE INDEX IF NOT EXISTS idx_dev_instruments_type_exchange 
    ON dev_instruments(instrument_type, exchange_code) WHERE instrument_type IS NOT NULL AND exchange_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dev_instruments_active_type
    ON dev_instruments(active, instrument_type) WHERE active IS NOT NULL AND instrument_type IS NOT NULL;

-- ================================================
-- CREATE BUSINESS LOGIC CONSTRAINTS
-- ================================================

-- Business rule constraints
ALTER TABLE dev_instruments 
  ADD CONSTRAINT chk_list_before_delist 
    CHECK (delist_date IS NULL OR list_date IS NULL OR list_date <= delist_date);

ALTER TABLE dev_instruments
  ADD CONSTRAINT chk_active_consistent_with_delist
    CHECK (delist_date IS NULL OR active = false OR delist_date > CURRENT_DATE);

-- ================================================
-- CREATE SEMANTIC TYPE VIEWS FOR EDA
-- ================================================

-- Create view with semantic type information for EDA system
CREATE OR REPLACE VIEW dev_instruments_semantic AS
SELECT 
    id,
    symbol,
    name,
    instrument_type,
    exchange_code, 
    currency_code,
    sector_type,
    active,
    list_date,
    delist_date,
    -- Computed semantic fields
    CASE 
        WHEN delist_date IS NOT NULL AND delist_date <= CURRENT_DATE THEN 'Delisted'
        WHEN active = false THEN 'Inactive' 
        ELSE 'Active'
    END AS listing_status,
    
    CASE
        WHEN instrument_type = 'Stock' THEN 'Equity'
        WHEN instrument_type = 'ETF' THEN 'Fund'
        WHEN instrument_type IN ('WARRANT', 'RIGHT') THEN 'Derivative'
        ELSE 'Other'
    END AS asset_class,
    
    -- Age calculations
    CURRENT_DATE - list_date AS days_since_listing,
    
    created_at,
    updated_at
FROM dev_instruments
WHERE instrument_type IS NOT NULL;

COMMENT ON VIEW dev_instruments_semantic IS 'Semantic view of instruments with computed business logic fields for EDA';

-- ================================================
-- UPDATE ANALYTICS SERVICE TYPE MAPPING
-- ================================================

-- Create mapping table for analytics service to understand semantic types
CREATE TABLE IF NOT EXISTS dev_column_semantic_types (
    table_name text NOT NULL,
    column_name text NOT NULL,
    semantic_type text NOT NULL,
    enum_values text[], -- For categorical types
    business_meaning text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name)
);

-- Insert semantic type mappings
INSERT INTO dev_column_semantic_types (table_name, column_name, semantic_type, enum_values, business_meaning) VALUES
('dev_instruments', 'instrument_type', 'categorical', ARRAY['Stock', 'ETF', 'PFD', 'WARRANT', 'CS', 'SP', 'UNIT', 'ADRC', 'RIGHT'], 'Type of financial instrument'),
('dev_instruments', 'exchange_code', 'categorical', ARRAY['NASDAQ', 'NYSE', 'NYSE_ARCA', 'BATS', 'XNYS', 'NYSE_MKT'], 'Exchange where instrument trades'),
('dev_instruments', 'currency_code', 'categorical', ARRAY['USD', 'CAD', 'EUR', 'GBP'], 'Base currency for instrument pricing'),
('dev_instruments', 'active', 'boolean', NULL, 'Whether instrument is currently active for trading'),
('dev_instruments', 'list_date', 'date', NULL, 'Date when instrument was first listed on exchange'),
('dev_instruments', 'delist_date', 'date', NULL, 'Date when instrument was delisted (null if still listed)'),
('dev_instruments', 'symbol', 'identifier', NULL, 'Unique ticker symbol for instrument'),
('dev_financial_events', 'event_type', 'categorical', ARRAY['earnings', 'analyst_rating', 'corporate_action', 'announcement'], 'Type of financial event'),
('dev_financial_events', 'sentiment', 'categorical', ARRAY['positive', 'negative', 'neutral'], 'Market sentiment impact'),
('dev_financial_events', 'importance_level', 'categorical', ARRAY['high', 'medium', 'low'], 'Market importance level'),
('dev_financial_events', 'event_datetime', 'datetime', NULL, 'When the financial event occurred')
ON CONFLICT (table_name, column_name) DO UPDATE SET
    semantic_type = EXCLUDED.semantic_type,
    enum_values = EXCLUDED.enum_values,
    business_meaning = EXCLUDED.business_meaning;

COMMENT ON TABLE dev_column_semantic_types IS 'Metadata table mapping database columns to semantic types for EDA system';