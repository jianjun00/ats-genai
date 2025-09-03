-- Create sentiment analysis tables for news and social media sentiment
-- Migration 032: Add comprehensive sentiment analysis infrastructure

-- News sentiment analysis table
CREATE TABLE IF NOT EXISTS news_sentiment_analysis (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT UNIQUE NOT NULL,
    source VARCHAR(100) NOT NULL,
    published_date TIMESTAMPTZ NOT NULL,
    symbols TEXT[] NOT NULL,
    sentiment_score NUMERIC(5,4) NOT NULL, -- -1.0000 to 1.0000
    confidence NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    relevance_score NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    content TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for news sentiment analysis
CREATE INDEX IF NOT EXISTS idx_news_sentiment_symbols ON news_sentiment_analysis USING GIN(symbols);
CREATE INDEX IF NOT EXISTS idx_news_sentiment_published_date ON news_sentiment_analysis (published_date DESC);
CREATE INDEX IF NOT EXISTS idx_news_sentiment_source ON news_sentiment_analysis (source);
CREATE INDEX IF NOT EXISTS idx_news_sentiment_score ON news_sentiment_analysis (sentiment_score DESC);

-- News sentiment signals aggregated by symbol
CREATE TABLE IF NOT EXISTS sentiment_signals (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    signal_strength NUMERIC(5,4) NOT NULL, -- -1.0000 to 1.0000
    signal_direction VARCHAR(20) NOT NULL, -- 'bullish', 'bearish', 'neutral'
    confidence NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    time_horizon VARCHAR(20) NOT NULL, -- 'short', 'medium', 'long'
    sentiment_momentum NUMERIC(5,4) NOT NULL, -- -1.0000 to 1.0000
    volume_weighted_sentiment NUMERIC(5,4) NOT NULL, -- -1.0000 to 1.0000
    article_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for sentiment signals
CREATE INDEX IF NOT EXISTS idx_sentiment_signals_symbol ON sentiment_signals (symbol);
CREATE INDEX IF NOT EXISTS idx_sentiment_signals_created_at ON sentiment_signals (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sentiment_signals_strength ON sentiment_signals (signal_strength DESC);

-- Social media posts table
CREATE TABLE IF NOT EXISTS social_media_posts (
    id SERIAL PRIMARY KEY,
    post_id VARCHAR(255) UNIQUE NOT NULL,
    platform VARCHAR(50) NOT NULL, -- 'twitter', 'reddit', 'stocktwits', etc.
    content TEXT NOT NULL,
    author VARCHAR(255) NOT NULL,
    followers_count INTEGER NOT NULL DEFAULT 0,
    likes_count INTEGER NOT NULL DEFAULT 0,
    retweets_count INTEGER NOT NULL DEFAULT 0,
    timestamp TIMESTAMPTZ NOT NULL,
    symbols TEXT[] NOT NULL,
    hashtags TEXT[] NOT NULL DEFAULT '{}',
    sentiment_score NUMERIC(5,4) NOT NULL, -- -1.0000 to 1.0000
    engagement_score NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    author_influence_score NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for social media posts
CREATE INDEX IF NOT EXISTS idx_social_posts_symbols ON social_media_posts USING GIN(symbols);
CREATE INDEX IF NOT EXISTS idx_social_posts_timestamp ON social_media_posts (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_social_posts_platform ON social_media_posts (platform);
CREATE INDEX IF NOT EXISTS idx_social_posts_author ON social_media_posts (author);
CREATE INDEX IF NOT EXISTS idx_social_posts_hashtags ON social_media_posts USING GIN(hashtags);

-- Social media trading signals
CREATE TABLE IF NOT EXISTS social_trading_signals (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    signal_type VARCHAR(50) NOT NULL, -- 'momentum', 'contrarian', 'trending', 'influencer'
    signal_strength NUMERIC(5,4) NOT NULL, -- -1.0000 to 1.0000
    confidence NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    time_horizon VARCHAR(20) NOT NULL, -- 'intraday', 'short', 'medium'
    total_posts INTEGER NOT NULL DEFAULT 0,
    average_sentiment NUMERIC(5,4) NOT NULL, -- -1.0000 to 1.0000
    bullish_ratio NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    bearish_ratio NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    trending_score NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    momentum_score NUMERIC(5,4) NOT NULL, -- -1.0000 to 1.0000
    risk_factors TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for social trading signals
CREATE INDEX IF NOT EXISTS idx_social_signals_symbol ON social_trading_signals (symbol);
CREATE INDEX IF NOT EXISTS idx_social_signals_created_at ON social_trading_signals (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_social_signals_type ON social_trading_signals (signal_type);
CREATE INDEX IF NOT EXISTS idx_social_signals_strength ON social_trading_signals (signal_strength DESC);

-- Unified sentiment signals (combining news and social)
CREATE TABLE IF NOT EXISTS unified_sentiment_signals (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    overall_sentiment_score NUMERIC(5,4) NOT NULL, -- -1.0000 to 1.0000
    overall_confidence NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    signal_strength NUMERIC(5,4) NOT NULL, -- -1.0000 to 1.0000
    signal_direction VARCHAR(20) NOT NULL, -- 'bullish', 'bearish', 'neutral'
    time_horizon VARCHAR(20) NOT NULL, -- 'short', 'medium', 'long'
    risk_score NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    volume_indicator NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    consensus_score NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    divergence_score NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    total_news_articles INTEGER NOT NULL DEFAULT 0,
    total_social_posts INTEGER NOT NULL DEFAULT 0,
    key_themes TEXT[] NOT NULL DEFAULT '{}',
    risk_factors TEXT[] NOT NULL DEFAULT '{}',
    sentiment_features JSONB NOT NULL DEFAULT '{}', -- ML features as JSON
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for unified sentiment signals
CREATE INDEX IF NOT EXISTS idx_unified_signals_symbol ON unified_sentiment_signals (symbol);
CREATE INDEX IF NOT EXISTS idx_unified_signals_created_at ON unified_sentiment_signals (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_unified_signals_direction ON unified_sentiment_signals (signal_direction);
CREATE INDEX IF NOT EXISTS idx_unified_signals_strength ON unified_sentiment_signals (signal_strength DESC);
CREATE INDEX IF NOT EXISTS idx_unified_signals_risk ON unified_sentiment_signals (risk_score);

-- GIN index for sentiment features JSON search
CREATE INDEX IF NOT EXISTS idx_unified_signals_features ON unified_sentiment_signals USING GIN(sentiment_features);

-- Sentiment-enhanced predictions table
CREATE TABLE IF NOT EXISTS sentiment_enhanced_predictions (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    prediction_date TIMESTAMPTZ NOT NULL,
    horizon_days INTEGER NOT NULL,
    base_residual_return NUMERIC(8,6) NOT NULL,
    sentiment_adjusted_return NUMERIC(8,6) NOT NULL,
    base_confidence NUMERIC(5,4) NOT NULL,
    final_confidence NUMERIC(5,4) NOT NULL,
    sentiment_score NUMERIC(5,4) NOT NULL,
    sentiment_confidence NUMERIC(5,4) NOT NULL,
    news_contribution NUMERIC(5,4) NOT NULL,
    social_contribution NUMERIC(5,4) NOT NULL,
    risk_adjustment NUMERIC(5,4) NOT NULL,
    explanation TEXT NOT NULL,
    unified_sentiment_signal_id INTEGER REFERENCES unified_sentiment_signals(id),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for sentiment-enhanced predictions
CREATE INDEX IF NOT EXISTS idx_sentiment_predictions_symbol ON sentiment_enhanced_predictions (symbol);
CREATE INDEX IF NOT EXISTS idx_sentiment_predictions_date ON sentiment_enhanced_predictions (prediction_date DESC);
CREATE INDEX IF NOT EXISTS idx_sentiment_predictions_horizon ON sentiment_enhanced_predictions (horizon_days);
CREATE INDEX IF NOT EXISTS idx_sentiment_predictions_performance ON sentiment_enhanced_predictions (sentiment_adjusted_return DESC);

-- Sentiment analysis performance tracking
CREATE TABLE IF NOT EXISTS sentiment_analysis_performance (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    analysis_date TIMESTAMPTZ NOT NULL,
    prediction_horizon_days INTEGER NOT NULL,
    predicted_sentiment_impact NUMERIC(8,6) NOT NULL,
    actual_price_change NUMERIC(8,6),
    sentiment_accuracy_score NUMERIC(5,4), -- How accurate was sentiment prediction
    news_signal_accuracy NUMERIC(5,4), -- News component accuracy
    social_signal_accuracy NUMERIC(5,4), -- Social component accuracy
    combined_signal_accuracy NUMERIC(5,4), -- Combined signal accuracy
    evaluation_date TIMESTAMPTZ, -- When actual results were evaluated
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance tracking
CREATE INDEX IF NOT EXISTS idx_sentiment_performance_symbol ON sentiment_analysis_performance (symbol);
CREATE INDEX IF NOT EXISTS idx_sentiment_performance_date ON sentiment_analysis_performance (analysis_date DESC);
CREATE INDEX IF NOT EXISTS idx_sentiment_performance_accuracy ON sentiment_analysis_performance (sentiment_accuracy_score DESC);

-- Sentiment model metadata and versioning
CREATE TABLE IF NOT EXISTS sentiment_model_metadata (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    model_type VARCHAR(50) NOT NULL, -- 'finbert', 'vader', 'ensemble', etc.
    configuration JSONB NOT NULL DEFAULT '{}',
    performance_metrics JSONB NOT NULL DEFAULT '{}',
    deployment_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Index for model metadata
CREATE INDEX IF NOT EXISTS idx_sentiment_models_active ON sentiment_model_metadata (is_active, deployment_date DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sentiment_models_unique ON sentiment_model_metadata (model_name, model_version);

-- Create views for easy querying

-- Latest sentiment signals by symbol
CREATE OR REPLACE VIEW latest_sentiment_signals AS
SELECT DISTINCT ON (symbol)
    symbol,
    overall_sentiment_score,
    overall_confidence,
    signal_direction,
    time_horizon,
    risk_score,
    key_themes,
    created_at
FROM unified_sentiment_signals
ORDER BY symbol, created_at DESC;

-- Sentiment performance summary
CREATE OR REPLACE VIEW sentiment_performance_summary AS
SELECT
    symbol,
    COUNT(*) as total_predictions,
    AVG(sentiment_accuracy_score) as avg_accuracy,
    AVG(news_signal_accuracy) as avg_news_accuracy,
    AVG(social_signal_accuracy) as avg_social_accuracy,
    AVG(combined_signal_accuracy) as avg_combined_accuracy,
    STDDEV(sentiment_accuracy_score) as accuracy_std,
    MIN(analysis_date) as first_prediction,
    MAX(analysis_date) as latest_prediction
FROM sentiment_analysis_performance
WHERE sentiment_accuracy_score IS NOT NULL
GROUP BY symbol;

-- Daily sentiment volume metrics
CREATE OR REPLACE VIEW daily_sentiment_volume AS
SELECT
    DATE(created_at) as analysis_date,
    COUNT(DISTINCT symbol) as symbols_analyzed,
    COUNT(*) as total_signals,
    AVG(overall_confidence) as avg_confidence,
    AVG(risk_score) as avg_risk_score,
    COUNT(CASE WHEN signal_direction = 'bullish' THEN 1 END) as bullish_signals,
    COUNT(CASE WHEN signal_direction = 'bearish' THEN 1 END) as bearish_signals,
    COUNT(CASE WHEN signal_direction = 'neutral' THEN 1 END) as neutral_signals
FROM unified_sentiment_signals
GROUP BY DATE(created_at)
ORDER BY analysis_date DESC;

-- Add constraints
ALTER TABLE sentiment_signals ADD CONSTRAINT sentiment_signals_signal_strength_check CHECK (signal_strength >= -1.0 AND signal_strength <= 1.0);
ALTER TABLE sentiment_signals ADD CONSTRAINT sentiment_signals_confidence_check CHECK (confidence >= 0.0 AND confidence <= 1.0);
ALTER TABLE sentiment_signals ADD CONSTRAINT sentiment_signals_sentiment_momentum_check CHECK (sentiment_momentum >= -1.0 AND sentiment_momentum <= 1.0);
ALTER TABLE sentiment_signals ADD CONSTRAINT sentiment_signals_volume_weighted_sentiment_check CHECK (volume_weighted_sentiment >= -1.0 AND volume_weighted_sentiment <= 1.0);

ALTER TABLE social_trading_signals ADD CONSTRAINT social_signals_signal_strength_check CHECK (signal_strength >= -1.0 AND signal_strength <= 1.0);
ALTER TABLE social_trading_signals ADD CONSTRAINT social_signals_confidence_check CHECK (confidence >= 0.0 AND confidence <= 1.0);
ALTER TABLE social_trading_signals ADD CONSTRAINT social_signals_average_sentiment_check CHECK (average_sentiment >= -1.0 AND average_sentiment <= 1.0);
ALTER TABLE social_trading_signals ADD CONSTRAINT social_signals_bullish_ratio_check CHECK (bullish_ratio >= 0.0 AND bullish_ratio <= 1.0);
ALTER TABLE social_trading_signals ADD CONSTRAINT social_signals_bearish_ratio_check CHECK (bearish_ratio >= 0.0 AND bearish_ratio <= 1.0);
ALTER TABLE social_trading_signals ADD CONSTRAINT social_signals_trending_score_check CHECK (trending_score >= 0.0 AND trending_score <= 1.0);
ALTER TABLE social_trading_signals ADD CONSTRAINT social_signals_momentum_score_check CHECK (momentum_score >= -1.0 AND momentum_score <= 1.0);

ALTER TABLE unified_sentiment_signals ADD CONSTRAINT unified_signals_overall_sentiment_check CHECK (overall_sentiment_score >= -1.0 AND overall_sentiment_score <= 1.0);
ALTER TABLE unified_sentiment_signals ADD CONSTRAINT unified_signals_overall_confidence_check CHECK (overall_confidence >= 0.0 AND overall_confidence <= 1.0);
ALTER TABLE unified_sentiment_signals ADD CONSTRAINT unified_signals_signal_strength_check CHECK (signal_strength >= -1.0 AND signal_strength <= 1.0);
ALTER TABLE unified_sentiment_signals ADD CONSTRAINT unified_signals_risk_score_check CHECK (risk_score >= 0.0 AND risk_score <= 1.0);
ALTER TABLE unified_sentiment_signals ADD CONSTRAINT unified_signals_volume_indicator_check CHECK (volume_indicator >= 0.0 AND volume_indicator <= 1.0);
ALTER TABLE unified_sentiment_signals ADD CONSTRAINT unified_signals_consensus_score_check CHECK (consensus_score >= 0.0 AND consensus_score <= 1.0);
ALTER TABLE unified_sentiment_signals ADD CONSTRAINT unified_signals_divergence_score_check CHECK (divergence_score >= 0.0 AND divergence_score <= 1.0);

-- Add comments for documentation
COMMENT ON TABLE news_sentiment_analysis IS 'Stores news articles with sentiment analysis for financial markets';
COMMENT ON TABLE sentiment_signals IS 'Aggregated news sentiment signals by symbol for trading decisions';
COMMENT ON TABLE social_media_posts IS 'Social media posts with sentiment analysis and engagement metrics';
COMMENT ON TABLE social_trading_signals IS 'Trading signals derived from social media sentiment analysis';
COMMENT ON TABLE unified_sentiment_signals IS 'Combined sentiment signals from news and social media sources';
COMMENT ON TABLE sentiment_enhanced_predictions IS 'Residual return predictions enhanced with sentiment analysis';
COMMENT ON TABLE sentiment_analysis_performance IS 'Performance tracking for sentiment-based predictions';
COMMENT ON TABLE sentiment_model_metadata IS 'Metadata and versioning for sentiment analysis models';

COMMENT ON COLUMN news_sentiment_analysis.sentiment_score IS 'Sentiment score from -1.0 (very negative) to 1.0 (very positive)';
COMMENT ON COLUMN news_sentiment_analysis.confidence IS 'Confidence in sentiment analysis from 0.0 to 1.0';
COMMENT ON COLUMN news_sentiment_analysis.relevance_score IS 'Relevance of article to symbols from 0.0 to 1.0';

COMMENT ON COLUMN unified_sentiment_signals.sentiment_features IS 'JSON object containing 23+ ML features extracted from sentiment analysis';
COMMENT ON COLUMN unified_sentiment_signals.consensus_score IS 'Agreement between news and social sentiment (0.0 = complete disagreement, 1.0 = perfect agreement)';
COMMENT ON COLUMN unified_sentiment_signals.divergence_score IS 'Disagreement between news and social sentiment sources';