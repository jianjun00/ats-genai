-- ATS-INTG Database Initialization
-- Tables required for daily refresh jobs

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Integration Instruments Table
CREATE TABLE IF NOT EXISTS intg_instruments (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(255),
    exchange VARCHAR(50),
    sector VARCHAR(100),
    industry VARCHAR(100),
    market_cap BIGINT,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for active symbols
CREATE INDEX IF NOT EXISTS idx_intg_instruments_active ON intg_instruments(active, symbol);

-- Integration Daily Prices Table
CREATE TABLE IF NOT EXISTS intg_daily_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    vendor VARCHAR(50) NOT NULL,
    open_price DECIMAL(12,4),
    high_price DECIMAL(12,4), 
    low_price DECIMAL(12,4),
    close_price DECIMAL(12,4),
    volume BIGINT,
    adjusted_close DECIMAL(12,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(symbol, date, vendor)
);

-- Convert to TimescaleDB hypertable
SELECT create_hypertable('intg_daily_prices', 'date', if_not_exists => TRUE);

-- Create indexes for daily prices
CREATE INDEX IF NOT EXISTS idx_intg_daily_prices_symbol_date ON intg_daily_prices(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_intg_daily_prices_vendor_date ON intg_daily_prices(vendor, date DESC);

-- Integration Fundamentals Comprehensive Table
CREATE TABLE IF NOT EXISTS intg_fundamentals_comprehensive (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    vendor VARCHAR(50) NOT NULL,
    fiscal_period VARCHAR(10) NOT NULL DEFAULT 'FY',
    
    -- Income Statement
    revenue BIGINT,
    gross_profit BIGINT,
    operating_income BIGINT,
    net_income BIGINT,
    ebitda BIGINT,
    eps DECIMAL(8,4),
    
    -- Balance Sheet
    total_assets BIGINT,
    total_liabilities BIGINT,
    shareholders_equity BIGINT,
    current_assets BIGINT,
    current_liabilities BIGINT,
    total_debt BIGINT,
    cash_and_equivalents BIGINT,
    
    -- Cash Flow
    operating_cash_flow BIGINT,
    investing_cash_flow BIGINT,
    financing_cash_flow BIGINT,
    free_cash_flow BIGINT,
    
    -- Calculated Ratios
    current_ratio DECIMAL(8,4),
    debt_to_equity DECIMAL(8,4),
    return_on_assets DECIMAL(8,4),
    return_on_equity DECIMAL(8,4),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(symbol, date, vendor, fiscal_period)
);

-- Create indexes for fundamentals
CREATE INDEX IF NOT EXISTS idx_intg_fundamentals_symbol_date ON intg_fundamentals_comprehensive(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_intg_fundamentals_vendor_date ON intg_fundamentals_comprehensive(vendor, date DESC);

-- Integration News Table  
CREATE TABLE IF NOT EXISTS intg_news (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    vendor VARCHAR(50) NOT NULL,
    news_id VARCHAR(255) NOT NULL,
    title TEXT NOT NULL,
    summary TEXT,
    content TEXT,
    published_at TIMESTAMP NOT NULL,
    url TEXT,
    sentiment_score DECIMAL(5,3),
    author VARCHAR(255),
    source VARCHAR(255),
    keywords TEXT[],
    relevance_score DECIMAL(5,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(symbol, vendor, news_id)
);

-- Create indexes for news
CREATE INDEX IF NOT EXISTS idx_intg_news_symbol_date ON intg_news(symbol, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_intg_news_vendor_date ON intg_news(vendor, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_intg_news_published ON intg_news(published_at DESC);

-- Daily Price Checkpoint Table
CREATE TABLE IF NOT EXISTS intg_daily_price_checkpoint (
    id SERIAL PRIMARY KEY,
    job_date DATE NOT NULL,
    vendor VARCHAR(50) NOT NULL,
    symbols_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    last_symbol VARCHAR(20),
    status VARCHAR(20) DEFAULT 'running',
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    
    UNIQUE(job_date, vendor)
);

-- Fundamentals Checkpoint Table
CREATE TABLE IF NOT EXISTS intg_fundamentals_checkpoint (
    id SERIAL PRIMARY KEY,
    job_date DATE NOT NULL,
    vendor VARCHAR(50) NOT NULL,
    symbols_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    last_symbol VARCHAR(20),
    status VARCHAR(20) DEFAULT 'running',
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    
    UNIQUE(job_date, vendor)
);

-- News Checkpoint Table
CREATE TABLE IF NOT EXISTS intg_news_checkpoint (
    id SERIAL PRIMARY KEY,
    job_date DATE NOT NULL,
    vendor VARCHAR(50) NOT NULL,
    symbols_processed INTEGER DEFAULT 0,
    news_items_inserted INTEGER DEFAULT 0,
    last_symbol VARCHAR(20),
    status VARCHAR(20) DEFAULT 'running',
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    
    UNIQUE(job_date, vendor)
);

-- Insert sample instruments for testing
INSERT INTO intg_instruments (symbol, name, exchange, sector, active) VALUES
('AAPL', 'Apple Inc.', 'NASDAQ', 'Technology', true),
('MSFT', 'Microsoft Corporation', 'NASDAQ', 'Technology', true),
('GOOGL', 'Alphabet Inc.', 'NASDAQ', 'Technology', true),
('AMZN', 'Amazon.com Inc.', 'NASDAQ', 'Consumer Discretionary', true),
('TSLA', 'Tesla Inc.', 'NASDAQ', 'Consumer Discretionary', true),
('META', 'Meta Platforms Inc.', 'NASDAQ', 'Technology', true),
('NVDA', 'NVIDIA Corporation', 'NASDAQ', 'Technology', true),
('NFLX', 'Netflix Inc.', 'NASDAQ', 'Communication Services', true),
('JPM', 'JPMorgan Chase & Co.', 'NYSE', 'Financials', true),
('V', 'Visa Inc.', 'NYSE', 'Financials', true)
ON CONFLICT (symbol) DO NOTHING;

-- Create materialized views for performance
CREATE MATERIALIZED VIEW IF NOT EXISTS intg_daily_summary AS
SELECT 
    date,
    COUNT(DISTINCT symbol) as symbols_with_data,
    COUNT(DISTINCT vendor) as active_vendors,
    COUNT(*) as total_price_records
FROM intg_daily_prices 
GROUP BY date
ORDER BY date DESC;

-- Refresh materialized view function
CREATE OR REPLACE FUNCTION refresh_daily_summary()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW intg_daily_summary;
END;
$$ LANGUAGE plpgsql;

-- Create index on materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_intg_daily_summary_date ON intg_daily_summary(date);

-- Performance monitoring view
CREATE OR REPLACE VIEW intg_job_performance AS
SELECT 
    'prices' as job_type,
    job_date,
    vendor,
    symbols_processed,
    records_inserted as items_processed,
    status,
    EXTRACT(EPOCH FROM (completed_at - started_at)) / 60 as duration_minutes
FROM intg_daily_price_checkpoint
WHERE job_date >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL

SELECT 
    'fundamentals' as job_type,
    job_date,
    vendor,
    symbols_processed,
    records_inserted as items_processed,
    status,
    EXTRACT(EPOCH FROM (completed_at - started_at)) / 60 as duration_minutes
FROM intg_fundamentals_checkpoint
WHERE job_date >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL

SELECT 
    'news' as job_type,
    job_date,
    vendor,
    symbols_processed,
    news_items_inserted as items_processed,
    status,
    EXTRACT(EPOCH FROM (completed_at - started_at)) / 60 as duration_minutes
FROM intg_news_checkpoint
WHERE job_date >= CURRENT_DATE - INTERVAL '7 days'

ORDER BY job_date DESC, job_type, vendor;

-- Data quality monitoring view
CREATE OR REPLACE VIEW intg_data_quality AS
SELECT 
    'prices' as data_type,
    DATE(created_at) as data_date,
    vendor,
    COUNT(*) as records_count,
    COUNT(DISTINCT symbol) as unique_symbols,
    AVG(CASE WHEN close_price > 0 THEN 1 ELSE 0 END) * 100 as valid_price_percentage
FROM intg_daily_prices
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(created_at), vendor

UNION ALL

SELECT 
    'fundamentals' as data_type,
    DATE(created_at) as data_date,
    vendor,
    COUNT(*) as records_count,
    COUNT(DISTINCT symbol) as unique_symbols,
    AVG(CASE WHEN revenue > 0 OR net_income IS NOT NULL THEN 1 ELSE 0 END) * 100 as valid_price_percentage
FROM intg_fundamentals_comprehensive
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(created_at), vendor

UNION ALL

SELECT 
    'news' as data_type,
    DATE(created_at) as data_date,
    vendor,
    COUNT(*) as records_count,
    COUNT(DISTINCT symbol) as unique_symbols,
    AVG(CASE WHEN title IS NOT NULL AND title != '' THEN 1 ELSE 0 END) * 100 as valid_price_percentage
FROM intg_news
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(created_at), vendor

ORDER BY data_date DESC, data_type, vendor;

-- Grant permissions for application user
-- Note: Adjust these based on your actual application user
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'ats_app_user') THEN
        CREATE ROLE ats_app_user WITH LOGIN PASSWORD 'ats_app_password';
    END IF;
END
$$;

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO ats_app_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO ats_app_user;

-- Completion message
DO $$
BEGIN
    RAISE NOTICE '✅ ATS-INTG database tables initialized successfully';
    RAISE NOTICE '📊 Tables created: intg_instruments, intg_daily_prices, intg_fundamentals_comprehensive, intg_news';
    RAISE NOTICE '🔍 Checkpoint tables: intg_*_checkpoint for job tracking';
    RAISE NOTICE '📈 Views created: intg_job_performance, intg_data_quality';
    RAISE NOTICE '🚀 Ready for daily refresh jobs';
END
$$;