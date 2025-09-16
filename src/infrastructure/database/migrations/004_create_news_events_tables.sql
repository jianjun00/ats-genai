-- Migration 004: Create news and events tables from intg schema
-- Generated from current intg database schema on 2025-09-15

-- Update db_version
INSERT INTO db_version (version, description) VALUES 
(4, 'News and events tables - earnings, economic events, news')
ON CONFLICT (version) DO NOTHING;

-- Economic events tables
CREATE TABLE IF NOT EXISTS economic_event_types (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    importance INTEGER DEFAULT 0, -- 0=low, 1=medium, 2=high
    category TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS economic_events (
    id SERIAL PRIMARY KEY,
    event_type_id INTEGER REFERENCES economic_event_types(id),
    event_date TIMESTAMP WITH TIME ZONE NOT NULL,
    country TEXT,
    description TEXT,
    previous_value DOUBLE PRECISION,
    forecast_value DOUBLE PRECISION,
    actual_value DOUBLE PRECISION,
    importance INTEGER DEFAULT 0,
    currency TEXT DEFAULT 'USD',
    source TEXT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Vendor-specific economic events tables
CREATE TABLE IF NOT EXISTS economic_events_fred (
    id SERIAL PRIMARY KEY,
    series_id TEXT NOT NULL,
    event_date DATE NOT NULL,
    value DOUBLE PRECISION,
    title TEXT,
    units TEXT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(series_id, event_date)
);

CREATE TABLE IF NOT EXISTS economic_events_alpha_vantage (
    id SERIAL PRIMARY KEY,
    event_name TEXT NOT NULL,
    event_date TIMESTAMP WITH TIME ZONE NOT NULL,
    country TEXT,
    actual_value TEXT,
    previous_value TEXT,
    forecast_value TEXT,
    importance TEXT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS economic_events_polygon (
    id SERIAL PRIMARY KEY,
    event_name TEXT NOT NULL,
    event_date TIMESTAMP WITH TIME ZONE NOT NULL,
    country TEXT,
    actual_value DOUBLE PRECISION,
    previous_value DOUBLE PRECISION,
    forecast_value DOUBLE PRECISION,
    importance INTEGER,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS economic_events_tiingo (
    id SERIAL PRIMARY KEY,
    event_name TEXT NOT NULL,
    event_date TIMESTAMP WITH TIME ZONE NOT NULL,
    country TEXT,
    actual_value TEXT,
    previous_value TEXT,
    forecast_value TEXT,
    importance TEXT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS economic_events_eodhd (
    id SERIAL PRIMARY KEY,
    event_name TEXT NOT NULL,
    event_date TIMESTAMP WITH TIME ZONE NOT NULL,
    country TEXT,
    actual_value TEXT,
    previous_value TEXT,
    forecast_value TEXT,
    importance TEXT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Earnings events table
CREATE TABLE IF NOT EXISTS earnings_events (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER REFERENCES instrument(id),
    symbol TEXT NOT NULL,
    earnings_date DATE NOT NULL,
    report_date DATE,
    fiscal_period TEXT, -- 'Q1', 'Q2', 'Q3', 'Q4'
    fiscal_year INTEGER,
    eps_estimate DOUBLE PRECISION,
    eps_actual DOUBLE PRECISION,
    revenue_estimate DOUBLE PRECISION,
    revenue_actual DOUBLE PRECISION,
    surprise_percent DOUBLE PRECISION,
    time_of_day TEXT, -- 'before_market', 'after_market'
    source TEXT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(symbol, earnings_date, fiscal_period, fiscal_year)
);

-- Financial events table (generic events)
CREATE TABLE IF NOT EXISTS financial_events (
    id SERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_date TIMESTAMP WITH TIME ZONE NOT NULL,
    instrument_id INTEGER REFERENCES instrument(id),
    symbol TEXT,
    description TEXT,
    impact TEXT, -- 'low', 'medium', 'high'
    category TEXT,
    source TEXT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- News tables
CREATE TABLE IF NOT EXISTS news (
    id SERIAL PRIMARY KEY,
    headline TEXT NOT NULL,
    summary TEXT,
    content TEXT,
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
    source TEXT,
    url TEXT,
    sentiment_score DOUBLE PRECISION,
    sentiment_label TEXT, -- 'positive', 'negative', 'neutral'
    symbols TEXT[], -- Array of related symbols
    categories TEXT[], -- Array of news categories
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS news_polygon (
    id SERIAL PRIMARY KEY,
    headline TEXT NOT NULL,
    summary TEXT,
    content TEXT,
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
    author TEXT,
    url TEXT,
    symbols TEXT[],
    keywords TEXT[],
    insights JSONB,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS realtime_news (
    id SERIAL PRIMARY KEY,
    headline TEXT NOT NULL,
    summary TEXT,
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
    source TEXT,
    url TEXT,
    symbols TEXT[],
    sentiment_score DOUBLE PRECISION,
    relevance_score DOUBLE PRECISION,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    raw_data JSONB
);

-- News and events indexes
CREATE INDEX IF NOT EXISTS idx_economic_events_event_date ON economic_events(event_date);
CREATE INDEX IF NOT EXISTS idx_economic_events_country ON economic_events(country);
CREATE INDEX IF NOT EXISTS idx_economic_events_importance ON economic_events(importance);
CREATE INDEX IF NOT EXISTS idx_earnings_events_earnings_date ON earnings_events(earnings_date);
CREATE INDEX IF NOT EXISTS idx_earnings_events_symbol ON earnings_events(symbol);
CREATE INDEX IF NOT EXISTS idx_earnings_events_fiscal ON earnings_events(fiscal_year, fiscal_period);
CREATE INDEX IF NOT EXISTS idx_financial_events_event_date ON financial_events(event_date);
CREATE INDEX IF NOT EXISTS idx_financial_events_symbol ON financial_events(symbol);
CREATE INDEX IF NOT EXISTS idx_financial_events_event_type ON financial_events(event_type);
CREATE INDEX IF NOT EXISTS idx_news_published_at ON news(published_at);
CREATE INDEX IF NOT EXISTS idx_news_symbols ON news USING GIN(symbols);
CREATE INDEX IF NOT EXISTS idx_news_sentiment_score ON news(sentiment_score);
CREATE INDEX IF NOT EXISTS idx_realtime_news_published_at ON realtime_news(published_at);
CREATE INDEX IF NOT EXISTS idx_realtime_news_symbols ON realtime_news USING GIN(symbols);

-- Insert basic economic event types
INSERT INTO economic_event_types (name, description, importance, category) VALUES 
    ('FOMC Rate Decision', 'Federal Open Market Committee Interest Rate Decision', 2, 'monetary_policy'),
    ('GDP Growth', 'Gross Domestic Product Growth Rate', 2, 'economic_growth'),
    ('Unemployment Rate', 'National Unemployment Rate', 2, 'employment'),
    ('CPI', 'Consumer Price Index (Inflation)', 2, 'inflation'),
    ('PPI', 'Producer Price Index', 1, 'inflation'),
    ('Retail Sales', 'Monthly Retail Sales Report', 1, 'consumption'),
    ('Industrial Production', 'Monthly Industrial Production Index', 1, 'production'),
    ('PMI Manufacturing', 'Purchasing Managers Index - Manufacturing', 1, 'business_sentiment'),
    ('PMI Services', 'Purchasing Managers Index - Services', 1, 'business_sentiment'),
    ('Initial Jobless Claims', 'Weekly Initial Jobless Claims', 1, 'employment')
ON CONFLICT (name) DO NOTHING;