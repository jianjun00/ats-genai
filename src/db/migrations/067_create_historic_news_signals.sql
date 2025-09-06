-- Migration: Create historic news signal extraction tables
-- This migration creates the necessary tables for storing enhanced trading signals
-- extracted from historic news data using local LLM models.

-- Trading signals extracted from news articles
CREATE TABLE IF NOT EXISTS dev_trading_signals (
    id SERIAL PRIMARY KEY,
    news_id VARCHAR(255) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    signal_type VARCHAR(10) NOT NULL CHECK (signal_type IN ('BUY', 'SELL', 'HOLD', 'WATCH')),
    confidence DECIMAL(4,3) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    sentiment VARCHAR(20) NOT NULL CHECK (sentiment IN ('positive', 'negative', 'neutral')),
    sentiment_score DECIMAL(4,3) NOT NULL CHECK (sentiment_score >= -1 AND sentiment_score <= 1),
    impact_timeframe VARCHAR(20) NOT NULL CHECK (impact_timeframe IN ('immediate', 'short_term', 'medium_term', 'long_term')),
    key_factors JSONB,
    risk_level VARCHAR(10) NOT NULL CHECK (risk_level IN ('low', 'medium', 'high')),
    target_price_change DECIMAL(6,3),
    published_utc TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    model_version VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(news_id, ticker)
);

-- Enhanced analysis of news articles with market impact metrics
CREATE TABLE IF NOT EXISTS dev_enhanced_news_analysis (
    id SERIAL PRIMARY KEY,
    news_id VARCHAR(255) NOT NULL UNIQUE,
    title TEXT NOT NULL,
    description TEXT,
    original_insights JSONB,
    market_impact_score DECIMAL(4,3) NOT NULL CHECK (market_impact_score >= 0 AND market_impact_score <= 1),
    volatility_indicator DECIMAL(4,3) NOT NULL CHECK (volatility_indicator >= 0 AND volatility_indicator <= 1),
    sector_impact JSONB,
    entity_mentions JSONB,
    processing_time_seconds DECIMAL(8,3) NOT NULL,
    model_performance_metrics JSONB,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Processing checkpoints for batch operations and recovery
CREATE TABLE IF NOT EXISTS dev_news_processing_checkpoints (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL UNIQUE,
    last_processed_news_id VARCHAR(255),
    processed_count INTEGER NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- Performance indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_trading_signals_ticker_date ON dev_trading_signals(ticker, published_utc);
CREATE INDEX IF NOT EXISTS idx_trading_signals_signal_type ON dev_trading_signals(signal_type);
CREATE INDEX IF NOT EXISTS idx_trading_signals_confidence ON dev_trading_signals(confidence DESC);
CREATE INDEX IF NOT EXISTS idx_trading_signals_sentiment_score ON dev_trading_signals(sentiment_score);
CREATE INDEX IF NOT EXISTS idx_trading_signals_published_utc ON dev_trading_signals(published_utc);

CREATE INDEX IF NOT EXISTS idx_enhanced_analysis_impact ON dev_enhanced_news_analysis(market_impact_score DESC);
CREATE INDEX IF NOT EXISTS idx_enhanced_analysis_volatility ON dev_enhanced_news_analysis(volatility_indicator DESC);
CREATE INDEX IF NOT EXISTS idx_enhanced_analysis_processed_at ON dev_enhanced_news_analysis(processed_at);

-- Composite index for backtesting queries
CREATE INDEX IF NOT EXISTS idx_trading_signals_ticker_date_signal ON dev_trading_signals(ticker, published_utc, signal_type);

-- Index for checkpoint management
CREATE INDEX IF NOT EXISTS idx_checkpoints_batch_id ON dev_news_processing_checkpoints(batch_id);
CREATE INDEX IF NOT EXISTS idx_checkpoints_started_at ON dev_news_processing_checkpoints(started_at);

-- Add trigger to automatically update updated_at timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_trading_signals_updated_at 
    BEFORE UPDATE ON dev_trading_signals 
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_enhanced_analysis_updated_at 
    BEFORE UPDATE ON dev_enhanced_news_analysis 
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_checkpoints_updated_at 
    BEFORE UPDATE ON dev_news_processing_checkpoints 
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

-- Add comments for documentation
COMMENT ON TABLE dev_trading_signals IS 'Trading signals extracted from news articles using LLM analysis';
COMMENT ON TABLE dev_enhanced_news_analysis IS 'Enhanced analysis of news articles with market impact metrics';
COMMENT ON TABLE dev_news_processing_checkpoints IS 'Processing checkpoints for batch operations and recovery';

COMMENT ON COLUMN dev_trading_signals.signal_type IS 'Trading signal type: BUY, SELL, HOLD, WATCH';
COMMENT ON COLUMN dev_trading_signals.confidence IS 'Confidence score from 0.0 to 1.0';
COMMENT ON COLUMN dev_trading_signals.sentiment_score IS 'Sentiment score from -1.0 (negative) to 1.0 (positive)';
COMMENT ON COLUMN dev_trading_signals.impact_timeframe IS 'Expected impact timeframe: immediate, short_term, medium_term, long_term';
COMMENT ON COLUMN dev_trading_signals.target_price_change IS 'Expected price change percentage (e.g., 0.05 for 5% increase)';

COMMENT ON COLUMN dev_enhanced_news_analysis.market_impact_score IS 'Overall market impact score from 0.0 to 1.0';
COMMENT ON COLUMN dev_enhanced_news_analysis.volatility_indicator IS 'Volatility expectation from 0.0 to 1.0';
COMMENT ON COLUMN dev_enhanced_news_analysis.sector_impact IS 'Array of impacted sectors';
COMMENT ON COLUMN dev_enhanced_news_analysis.entity_mentions IS 'Extracted entities: companies, people, events';