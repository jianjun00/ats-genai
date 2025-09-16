-- Semantic Types Schema Migration
-- Adds proper PostgreSQL ENUMs and semantic types for all ATS dev tables
-- This improves type safety, EDA categorization, and performance

BEGIN;

-- ================================================
-- 1. INSTRUMENT SEMANTIC TYPES
-- ================================================

-- Create instrument type ENUM (normalize case differences)
CREATE TYPE instrument_type_enum AS ENUM (
    'Stock',      -- Common stocks (normalize 'stock' → 'Stock')
    'ETF',        -- Exchange Traded Funds
    'PFD',        -- Preferred Stock
    'WARRANT',    -- Warrants
    'CS',         -- Common Stock (alternative)
    'SP',         -- Special Purpose
    'UNIT',       -- Unit Investment Trust
    'ADRC',       -- American Depositary Receipt
    'RIGHT',      -- Rights
    'REIT',       -- Real Estate Investment Trust
    'FUND',       -- Mutual Funds
    'INDEX'       -- Index Securities
);

-- Create exchange code ENUM
CREATE TYPE exchange_code_enum AS ENUM (
    'NASDAQ',     -- NASDAQ Global Market
    'NYSE',       -- New York Stock Exchange  
    'NYSE_ARCA',  -- NYSE Arca (normalize 'NYSE ARCA')
    'BATS',       -- BATS Exchange
    'XNYS',       -- NYSE (alternative code)
    'NYSE_MKT',   -- NYSE MKT (normalize 'NYSE MKT')
    'XNAS',       -- NASDAQ (alternative code)
    'AMEX',       -- American Stock Exchange
    'XASE',       -- AMEX (alternative code)
    'NYSE_NAT',   -- NYSE National (normalize 'NYSE NAT')
    'CBOE',       -- CBOE Exchange
    'IEX',        -- Investors Exchange
    'EDGX',       -- EDGX Exchange
    'EDGA'        -- EDGA Exchange
);

-- Create currency code ENUM (ISO 4217 standard)
CREATE TYPE currency_code_enum AS ENUM (
    'USD',        -- US Dollar (normalize 'usd' → 'USD')
    'CAD',        -- Canadian Dollar
    'EUR',        -- Euro
    'GBP',        -- British Pound
    'JPY',        -- Japanese Yen
    'CHF',        -- Swiss Franc
    'AUD',        -- Australian Dollar
    'NZD'         -- New Zealand Dollar
);

-- Create sector ENUM (GICS sectors)
CREATE TYPE sector_enum AS ENUM (
    'Technology',
    'Healthcare',
    'Financials',
    'Consumer_Discretionary',
    'Communication_Services',
    'Industrials',
    'Consumer_Staples',
    'Energy',
    'Utilities',
    'Real_Estate',
    'Materials'
);

-- ================================================
-- 2. MARKET DATA SEMANTIC TYPES  
-- ================================================

-- Create status code ENUM for market data quality
CREATE TYPE market_data_status_enum AS ENUM (
    'VALID',      -- Valid data point
    'SUSPECT',    -- Questionable data
    'INVALID',    -- Invalid data
    'MISSING',    -- Missing data
    'HALTED',     -- Trading halted
    'ADJUSTED',   -- Corporate action adjusted
    'ESTIMATED'   -- Estimated value
);

-- Create vendor ENUM
CREATE TYPE vendor_enum AS ENUM (
    'POLYGON',
    'TIINGO', 
    'EODHD',
    'QUANDL',
    'FMP',
    'ALPHA_VANTAGE',
    'IEX',
    'YAHOO',
    'BLOOMBERG',
    'REFINITIV'
);

-- ================================================
-- 3. TRADING & BACKTEST SEMANTIC TYPES
-- ================================================

-- Create trade action ENUM
CREATE TYPE trade_action_enum AS ENUM (
    'BUY',
    'SELL',
    'SHORT',
    'COVER',
    'HOLD'
);

-- Create trade status ENUM  
CREATE TYPE trade_status_enum AS ENUM (
    'OPEN',
    'CLOSED',
    'CANCELLED',
    'PARTIAL',
    'PENDING'
);

-- Create backtest status ENUM
CREATE TYPE backtest_status_enum AS ENUM (
    'RUNNING',
    'COMPLETED',
    'FAILED',
    'CANCELLED',
    'PAUSED'
);

-- ================================================
-- 4. APPLY SEMANTIC TYPES TO TABLES
-- ================================================

-- 4.1 Update dev_instruments table
COMMENT ON TABLE dev_instruments IS 'Financial instruments with semantic typing for better EDA and validation';

-- Normalize instrument types (Stock/stock → Stock, usd → USD)
UPDATE dev_instruments SET type = 'Stock' WHERE LOWER(type) = 'stock';
UPDATE dev_instruments SET currency = 'USD' WHERE LOWER(currency) = 'usd';
UPDATE dev_instruments SET exchange = 'NYSE_ARCA' WHERE exchange = 'NYSE ARCA';
UPDATE dev_instruments SET exchange = 'NYSE_MKT' WHERE exchange = 'NYSE MKT';  
UPDATE dev_instruments SET exchange = 'NYSE_NAT' WHERE exchange = 'NYSE NAT';

-- Add semantic type columns with proper ENUMs
ALTER TABLE dev_instruments 
  ADD COLUMN instrument_type instrument_type_enum,
  ADD COLUMN exchange_code exchange_code_enum,
  ADD COLUMN currency_code currency_code_enum,
  ADD COLUMN sector_type sector_enum;

-- Populate semantic type columns from existing text columns
UPDATE dev_instruments SET instrument_type = type::instrument_type_enum WHERE type IS NOT NULL;
UPDATE dev_instruments SET exchange_code = exchange::exchange_code_enum WHERE exchange IS NOT NULL;
UPDATE dev_instruments SET currency_code = currency::currency_code_enum WHERE currency IS NOT NULL;

-- Add column comments for documentation
COMMENT ON COLUMN dev_instruments.instrument_type IS 'Semantic instrument type (Stock, ETF, WARRANT, etc.)';
COMMENT ON COLUMN dev_instruments.exchange_code IS 'Exchange where instrument is listed (NASDAQ, NYSE, etc.)';  
COMMENT ON COLUMN dev_instruments.currency_code IS 'Base currency (USD, CAD, EUR, etc.)';
COMMENT ON COLUMN dev_instruments.list_date IS 'Date when instrument was first listed';
COMMENT ON COLUMN dev_instruments.delist_date IS 'Date when instrument was delisted (if applicable)';
COMMENT ON COLUMN dev_instruments.active IS 'Whether instrument is currently active for trading';

-- 4.2 Update daily prices tables with semantic types
COMMENT ON TABLE dev_daily_price_tiingo IS 'Daily OHLCV data from Tiingo with semantic vendor typing';
COMMENT ON TABLE dev_daily_price_eodhd IS 'Daily OHLCV data from EODHD with semantic vendor typing';
COMMENT ON TABLE dev_daily_price_polygon IS 'Daily OHLCV data from Polygon with semantic vendor typing';

-- Add vendor column to price tables
ALTER TABLE dev_daily_price_tiingo ADD COLUMN vendor_type vendor_enum DEFAULT 'TIINGO';
ALTER TABLE dev_daily_price_eodhd ADD COLUMN vendor_type vendor_enum DEFAULT 'EODHD'; 
ALTER TABLE dev_daily_price_polygon ADD COLUMN vendor_type vendor_enum DEFAULT 'POLYGON';

-- Add data quality status
ALTER TABLE dev_daily_price_tiingo ADD COLUMN data_status market_data_status_enum DEFAULT 'VALID';
ALTER TABLE dev_daily_price_eodhd ADD COLUMN data_status market_data_status_enum DEFAULT 'VALID';
ALTER TABLE dev_daily_price_polygon ADD COLUMN data_status market_data_status_enum DEFAULT 'VALID';

-- 4.3 Update financial events (already has some ENUMs)
COMMENT ON TABLE dev_financial_events IS 'Corporate and market events with semantic importance and sentiment typing';
COMMENT ON COLUMN dev_financial_events.event_type IS 'Type of financial event (earnings, rating, corporate action, etc.)';
COMMENT ON COLUMN dev_financial_events.sentiment IS 'Market sentiment impact (positive, negative, neutral)';
COMMENT ON COLUMN dev_financial_events.importance_level IS 'Market importance level (high, medium, low)';

-- 4.4 Update backtest tables  
COMMENT ON TABLE dev_backtest_trades IS 'Individual trade records from backtesting with semantic action types';

-- Check if backtest trades table exists and has the expected columns
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_backtest_trades') THEN
        -- Add semantic columns if they don't exist
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_backtest_trades' AND column_name = 'action_type') THEN
            ALTER TABLE dev_backtest_trades ADD COLUMN action_type trade_action_enum;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_backtest_trades' AND column_name = 'trade_status') THEN
            ALTER TABLE dev_backtest_trades ADD COLUMN trade_status trade_status_enum DEFAULT 'CLOSED';
        END IF;
    END IF;
END $$;

-- ================================================
-- 5. CREATE INDEXES ON SEMANTIC TYPES
-- ================================================

-- Indexes for better query performance on ENUMs
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_instruments_instrument_type 
    ON dev_instruments(instrument_type) WHERE instrument_type IS NOT NULL;
    
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_instruments_exchange_code 
    ON dev_instruments(exchange_code) WHERE exchange_code IS NOT NULL;
    
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_instruments_currency_code 
    ON dev_instruments(currency_code) WHERE currency_code IS NOT NULL;

-- Composite indexes for common filtering patterns
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_instruments_type_exchange 
    ON dev_instruments(instrument_type, exchange_code) WHERE instrument_type IS NOT NULL AND exchange_code IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_instruments_active_type
    ON dev_instruments(active, instrument_type) WHERE active IS NOT NULL AND instrument_type IS NOT NULL;

-- ================================================
-- 6. CREATE BUSINESS LOGIC CONSTRAINTS
-- ================================================

-- Business rule constraints
ALTER TABLE dev_instruments 
  ADD CONSTRAINT chk_list_before_delist 
    CHECK (delist_date IS NULL OR list_date IS NULL OR list_date <= delist_date);

ALTER TABLE dev_instruments
  ADD CONSTRAINT chk_active_consistent_with_delist
    CHECK (delist_date IS NULL OR active = false OR delist_date > CURRENT_DATE);

-- ================================================
-- 7. CREATE SEMANTIC TYPE VIEWS FOR EDA
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
-- 8. UPDATE ANALYTICS SERVICE TYPE MAPPING
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

COMMIT;

-- ================================================
-- 9. VERIFICATION QUERIES
-- ================================================

-- Verify semantic types are applied correctly
SELECT 'Semantic Types Migration Completed' AS status;

-- Show instrument type distribution
SELECT 
    instrument_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM dev_instruments 
WHERE instrument_type IS NOT NULL
GROUP BY instrument_type 
ORDER BY count DESC;

-- Show exchange distribution  
SELECT 
    exchange_code,
    COUNT(*) as count
FROM dev_instruments
WHERE exchange_code IS NOT NULL  
GROUP BY exchange_code
ORDER BY count DESC;

-- Verify constraints work
SELECT 
    COUNT(*) as total_instruments,
    COUNT(*) FILTER (WHERE instrument_type IS NOT NULL) as with_semantic_type,
    COUNT(*) FILTER (WHERE exchange_code IS NOT NULL) as with_semantic_exchange
FROM dev_instruments;