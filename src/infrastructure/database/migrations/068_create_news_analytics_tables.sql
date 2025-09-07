-- Migration: Create news analytics and OHLC cache tables
-- This migration creates the necessary tables for news analytics dashboard,
-- OHLC price caching, and news event training dataset metadata.

-- OHLC data cache for news visualization performance
CREATE TABLE IF NOT EXISTS dev_ohlc_cache (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    timeframe VARCHAR(2) NOT NULL CHECK (timeframe IN ('1h', '1d')),
    timestamp TIMESTAMPTZ NOT NULL,
    open_price DECIMAL(12,4) NOT NULL,
    high_price DECIMAL(12,4) NOT NULL, 
    low_price DECIMAL(12,4) NOT NULL,
    close_price DECIMAL(12,4) NOT NULL,
    volume BIGINT DEFAULT 0,
    cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(ticker, timeframe, timestamp)
);

-- News event training datasets metadata
CREATE TABLE IF NOT EXISTS dev_news_training_datasets (
    id SERIAL PRIMARY KEY,
    news_id VARCHAR(255) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    dataset_path VARCHAR(500) NOT NULL,
    news_date TIMESTAMPTZ NOT NULL,
    start_date TIMESTAMPTZ NOT NULL,
    end_date TIMESTAMPTZ NOT NULL,
    daily_records INTEGER DEFAULT 0,
    hourly_records INTEGER DEFAULT 0,
    dataset_size_mb DECIMAL(10,2),
    signal_type VARCHAR(10),
    confidence DECIMAL(4,3),
    sentiment VARCHAR(20),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Performance indexes for OHLC cache
CREATE INDEX IF NOT EXISTS idx_ohlc_cache_ticker_timeframe ON dev_ohlc_cache(ticker, timeframe);
CREATE INDEX IF NOT EXISTS idx_ohlc_cache_timestamp ON dev_ohlc_cache(timestamp);
CREATE INDEX IF NOT EXISTS idx_ohlc_cache_ticker_timestamp ON dev_ohlc_cache(ticker, timestamp);
CREATE INDEX IF NOT EXISTS idx_ohlc_cache_cached_at ON dev_ohlc_cache(cached_at);

-- Performance indexes for training datasets
CREATE INDEX IF NOT EXISTS idx_news_datasets_ticker ON dev_news_training_datasets(ticker);
CREATE INDEX IF NOT EXISTS idx_news_datasets_news_id ON dev_news_training_datasets(news_id);
CREATE INDEX IF NOT EXISTS idx_news_datasets_news_date ON dev_news_training_datasets(news_date);
CREATE INDEX IF NOT EXISTS idx_news_datasets_signal_type ON dev_news_training_datasets(signal_type);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_ohlc_cache_ticker_timeframe_range ON dev_ohlc_cache(ticker, timeframe, timestamp);
CREATE INDEX IF NOT EXISTS idx_news_datasets_ticker_date_range ON dev_news_training_datasets(ticker, news_date);

-- Add trigger to automatically update updated_at timestamps
CREATE TRIGGER update_ohlc_cache_updated_at 
    BEFORE UPDATE ON dev_ohlc_cache 
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_news_datasets_updated_at 
    BEFORE UPDATE ON dev_news_training_datasets 
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

-- Add comments for documentation
COMMENT ON TABLE dev_ohlc_cache IS 'Cached OHLC data for news visualization performance optimization';
COMMENT ON TABLE dev_news_training_datasets IS 'Metadata for news event training datasets used in ML model training';

COMMENT ON COLUMN dev_ohlc_cache.timeframe IS 'Data frequency: 1h (hourly) or 1d (daily)';
COMMENT ON COLUMN dev_ohlc_cache.cached_at IS 'When this data was cached for TTL expiration';

COMMENT ON COLUMN dev_news_training_datasets.dataset_path IS 'File path to the training dataset in /mnt/d/ats-data/news/training_data/';
COMMENT ON COLUMN dev_news_training_datasets.news_date IS 'Original news event timestamp';
COMMENT ON COLUMN dev_news_training_datasets.start_date IS 'Start date of training data (news_date - 10 days/hours)';
COMMENT ON COLUMN dev_news_training_datasets.end_date IS 'End date of training data (news_date + 10 days/hours)';
COMMENT ON COLUMN dev_news_training_datasets.daily_records IS 'Number of daily OHLC records in dataset';
COMMENT ON COLUMN dev_news_training_datasets.hourly_records IS 'Number of hourly OHLC records in dataset';