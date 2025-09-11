-- Migration 071: Create economic events tables
-- Description: Tables for storing economic events from multiple sources (Polygon, Tiingo, Alpha Vantage, FRED)

-- Economic event types lookup table
CREATE TABLE IF NOT EXISTS {table_prefix}economic_event_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    category VARCHAR(100),
    country VARCHAR(3), -- ISO 3166-1 alpha-3 country code
    importance_level INTEGER CHECK (importance_level BETWEEN 1 AND 5), -- 1=low, 5=high
    frequency VARCHAR(50), -- daily, weekly, monthly, quarterly, yearly
    typical_release_time TIME,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Economic events main table
CREATE TABLE IF NOT EXISTS {table_prefix}economic_events (
    id SERIAL PRIMARY KEY,
    event_type_id INTEGER NOT NULL REFERENCES {table_prefix}economic_event_types(id),
    date DATE NOT NULL,
    release_time TIMESTAMP WITH TIME ZONE,
    estimate DECIMAL(20,6),
    actual DECIMAL(20,6),
    previous DECIMAL(20,6),
    revised DECIMAL(20,6),
    unit VARCHAR(50), -- percentage, billions, millions, index, etc.
    currency VARCHAR(3), -- USD, EUR, etc.
    source_vendor VARCHAR(50) NOT NULL, -- polygon, tiingo, alpha_vantage, fred
    source_event_id VARCHAR(255), -- vendor's event ID
    is_preliminary BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_economic_event UNIQUE (event_type_id, date, source_vendor)
);

-- Economic events from Polygon
CREATE TABLE IF NOT EXISTS {table_prefix}economic_events_polygon (
    id SERIAL PRIMARY KEY,
    economic_event_id INTEGER NOT NULL REFERENCES {table_prefix}economic_events(id),
    polygon_event_id VARCHAR(255),
    name VARCHAR(500),
    country VARCHAR(3),
    importance INTEGER,
    actual_change_percent DECIMAL(10,4),
    estimated_change_percent DECIMAL(10,4),
    previous_change_percent DECIMAL(10,4),
    raw_data JSONB, -- Store original response for debugging
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Economic events from Tiingo
CREATE TABLE IF NOT EXISTS {table_prefix}economic_events_tiingo (
    id SERIAL PRIMARY KEY,
    economic_event_id INTEGER NOT NULL REFERENCES {table_prefix}economic_events(id),
    tiingo_event_id VARCHAR(255),
    description TEXT,
    source_url VARCHAR(1000),
    tags TEXT[],
    raw_data JSONB, -- Store original response for debugging
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Economic events from Alpha Vantage
CREATE TABLE IF NOT EXISTS {table_prefix}economic_events_alpha_vantage (
    id SERIAL PRIMARY KEY,
    economic_event_id INTEGER NOT NULL REFERENCES {table_prefix}economic_events(id),
    alpha_vantage_event_id VARCHAR(255),
    function_name VARCHAR(100), -- ECONOMIC_INDICATORS, etc.
    interval_period VARCHAR(50), -- monthly, quarterly, etc.
    raw_data JSONB, -- Store original response for debugging
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Economic events from EODHD
CREATE TABLE IF NOT EXISTS {table_prefix}economic_events_eodhd (
    id SERIAL PRIMARY KEY,
    economic_event_id INTEGER NOT NULL REFERENCES {table_prefix}economic_events(id),
    eodhd_event_id VARCHAR(255),
    event_name VARCHAR(500),
    country VARCHAR(3),
    importance VARCHAR(50),
    period VARCHAR(100),
    reference VARCHAR(255),
    source VARCHAR(255),
    raw_data JSONB, -- Store original response for debugging
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Economic events from FRED (Federal Reserve Economic Data)
CREATE TABLE IF NOT EXISTS {table_prefix}economic_events_fred (
    id SERIAL PRIMARY KEY,
    economic_event_id INTEGER NOT NULL REFERENCES {table_prefix}economic_events(id),
    fred_series_id VARCHAR(255),
    fred_observation_date DATE,
    series_title VARCHAR(500),
    series_units VARCHAR(100),
    seasonal_adjustment VARCHAR(100),
    frequency VARCHAR(50),
    raw_data JSONB, -- Store original response for debugging
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_economic_events_date ON {table_prefix}economic_events(date);
CREATE INDEX IF NOT EXISTS idx_economic_events_event_type ON {table_prefix}economic_events(event_type_id);
CREATE INDEX IF NOT EXISTS idx_economic_events_source_vendor ON {table_prefix}economic_events(source_vendor);
CREATE INDEX IF NOT EXISTS idx_economic_events_release_time ON {table_prefix}economic_events(release_time);
CREATE INDEX IF NOT EXISTS idx_economic_events_importance ON {table_prefix}economic_event_types(importance_level);
CREATE INDEX IF NOT EXISTS idx_economic_events_country ON {table_prefix}economic_event_types(country);

-- Create indexes on vendor-specific tables
CREATE INDEX IF NOT EXISTS idx_economic_events_polygon_event_id ON {table_prefix}economic_events_polygon(economic_event_id);
CREATE INDEX IF NOT EXISTS idx_economic_events_tiingo_event_id ON {table_prefix}economic_events_tiingo(economic_event_id);
CREATE INDEX IF NOT EXISTS idx_economic_events_alpha_vantage_event_id ON {table_prefix}economic_events_alpha_vantage(economic_event_id);
CREATE INDEX IF NOT EXISTS idx_economic_events_eodhd_event_id ON {table_prefix}economic_events_eodhd(economic_event_id);
CREATE INDEX IF NOT EXISTS idx_economic_events_fred_event_id ON {table_prefix}economic_events_fred(economic_event_id);

-- Create function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at columns
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_economic_event_types_updated_at') THEN
        CREATE TRIGGER update_economic_event_types_updated_at 
            BEFORE UPDATE ON {table_prefix}economic_event_types 
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_economic_events_updated_at') THEN
        CREATE TRIGGER update_economic_events_updated_at 
            BEFORE UPDATE ON {table_prefix}economic_events 
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

-- Insert common economic event types
INSERT INTO {table_prefix}economic_event_types (name, description, category, country, importance_level, frequency) VALUES
-- US Economic Indicators
('Non-Farm Payrolls', 'Monthly employment change excluding farm workers', 'Employment', 'USA', 5, 'monthly'),
('Unemployment Rate', 'Percentage of labor force that is unemployed', 'Employment', 'USA', 5, 'monthly'),
('Consumer Price Index', 'Measure of inflation in consumer goods and services', 'Inflation', 'USA', 5, 'monthly'),
('Federal Funds Rate', 'Interest rate at which banks lend to each other overnight', 'Interest Rates', 'USA', 5, 'irregular'),
('Gross Domestic Product', 'Total value of goods and services produced', 'Growth', 'USA', 5, 'quarterly'),
('Consumer Confidence Index', 'Measure of consumer optimism about economic conditions', 'Sentiment', 'USA', 4, 'monthly'),
('Retail Sales', 'Total sales at retail stores', 'Consumption', 'USA', 4, 'monthly'),
('Industrial Production', 'Output from manufacturing, mining, and utilities', 'Production', 'USA', 4, 'monthly'),
('Producer Price Index', 'Measure of wholesale price changes', 'Inflation', 'USA', 4, 'monthly'),
('Housing Starts', 'Number of new residential construction projects', 'Housing', 'USA', 3, 'monthly'),

-- Global Economic Indicators
('ECB Interest Rate Decision', 'European Central Bank monetary policy rate', 'Interest Rates', 'EUR', 5, 'irregular'),
('UK Interest Rate Decision', 'Bank of England base rate decision', 'Interest Rates', 'GBR', 5, 'irregular'),
('BOJ Interest Rate Decision', 'Bank of Japan policy rate decision', 'Interest Rates', 'JPN', 5, 'irregular'),
('China GDP', 'Chinese gross domestic product growth', 'Growth', 'CHN', 5, 'quarterly'),
('Eurozone CPI', 'Eurozone consumer price inflation', 'Inflation', 'EUR', 5, 'monthly'),

-- Market Indicators
('VIX Index', 'CBOE Volatility Index - fear gauge', 'Market', 'USA', 4, 'daily'),
('Oil Inventories', 'Weekly petroleum inventory levels', 'Commodities', 'USA', 3, 'weekly'),
('Gold Reserves', 'Central bank gold holdings', 'Commodities', 'USA', 2, 'monthly')

ON CONFLICT (name) DO NOTHING;