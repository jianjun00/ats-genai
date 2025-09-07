-- Migration 030: Create news tables for Polygon and Tiingo news events

-- Create unified events table
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    instrument_id INTEGER REFERENCES instruments(id),
    symbol VARCHAR(50),
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    reported_time TIMESTAMP WITH TIME ZONE,
    source VARCHAR(50) NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create Polygon news table
CREATE TABLE news_polygon (
    id BIGSERIAL PRIMARY KEY,
    polygon_id VARCHAR(255) UNIQUE NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    author VARCHAR(255),
    published_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    article_url TEXT,
    image_url TEXT,
    publisher_name VARCHAR(255),
    publisher_homepage_url TEXT,
    publisher_logo_url TEXT,
    publisher_favicon_url TEXT,
    keywords TEXT[],
    tickers TEXT[],
    insights JSONB,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create Tiingo news table 
CREATE TABLE news_tiingo (
    id BIGSERIAL PRIMARY KEY,
    tiingo_id INTEGER UNIQUE NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    published_date TIMESTAMP WITH TIME ZONE NOT NULL,
    crawl_date TIMESTAMP WITH TIME ZONE NOT NULL,
    url TEXT,
    source VARCHAR(255),
    tags TEXT[],
    tickers TEXT[],
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_events_instrument_id ON events(instrument_id);
CREATE INDEX idx_events_event_time ON events(event_time);
CREATE INDEX idx_events_source ON events(source);
CREATE INDEX idx_events_event_type ON events(event_type);
CREATE INDEX idx_events_symbol ON events(symbol);

CREATE INDEX idx_news_polygon_published_utc ON news_polygon(published_utc);
CREATE INDEX idx_news_polygon_tickers ON news_polygon USING GIN(tickers);
CREATE INDEX idx_news_polygon_keywords ON news_polygon USING GIN(keywords);
CREATE INDEX idx_news_polygon_polygon_id ON news_polygon(polygon_id);

CREATE INDEX idx_news_tiingo_published_date ON news_tiingo(published_date);
CREATE INDEX idx_news_tiingo_tickers ON news_tiingo USING GIN(tickers);
CREATE INDEX idx_news_tiingo_tags ON news_tiingo USING GIN(tags);
CREATE INDEX idx_news_tiingo_tiingo_id ON news_tiingo(tiingo_id);
CREATE INDEX idx_news_tiingo_source ON news_tiingo(source);

-- Create composite indexes for common queries
CREATE INDEX idx_events_symbol_time ON events(symbol, event_time DESC);
-- Note: PostgreSQL doesn't support mixing GIN with other column types, so create separate indexes