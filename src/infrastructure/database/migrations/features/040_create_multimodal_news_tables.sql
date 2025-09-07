-- Migration 040: Create enhanced news tables for multi-modal prediction system
-- Extends existing news infrastructure with economic events and training data

-- Enhanced economic events classification table
CREATE TABLE dev_economic_events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    event_subtype VARCHAR(50),
    event_category VARCHAR(30) NOT NULL CHECK (event_category IN ('macro', 'earnings', 'corporate', 'fed', 'employment', 'inflation', 'growth')),
    severity INTEGER NOT NULL CHECK (severity BETWEEN 1 AND 10),
    confidence_score DECIMAL(5,3) NOT NULL CHECK (confidence_score BETWEEN 0 AND 1),
    
    -- Affected entities
    affected_symbols TEXT[] DEFAULT '{}',
    affected_sectors TEXT[] DEFAULT '{}',
    affected_regions TEXT[] DEFAULT '{}',
    
    -- Timing
    event_date TIMESTAMP WITH TIME ZONE NOT NULL,
    announcement_date TIMESTAMP WITH TIME ZONE,
    market_open_date TIMESTAMP WITH TIME ZONE, -- Next market open after event
    
    -- Impact analysis
    predicted_impact_score DECIMAL(7,4), -- -1 to 1, predicted market impact
    actual_impact_score DECIMAL(7,4), -- Measured post-event impact
    impact_duration_days INTEGER, -- How long effect lasted
    
    -- Event details
    title TEXT NOT NULL,
    description TEXT,
    source_url TEXT,
    data JSONB NOT NULL, -- Full structured event data
    
    -- Metadata
    data_vendor VARCHAR(30) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- News-to-economic events mapping table
CREATE TABLE dev_news_economic_events (
    id BIGSERIAL PRIMARY KEY,
    news_id BIGINT NOT NULL,
    news_source VARCHAR(20) NOT NULL CHECK (news_source IN ('polygon', 'tiingo', 'alpha_vantage', 'fmp', 'benzinga')),
    event_id BIGINT NOT NULL REFERENCES dev_economic_events(id),
    relevance_score DECIMAL(5,3) NOT NULL CHECK (relevance_score BETWEEN 0 AND 1),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(news_id, news_source, event_id)
);

-- Enhanced news tables for additional sources
CREATE TABLE dev_news_alpha_vantage (
    id BIGSERIAL PRIMARY KEY,
    alpha_vantage_id VARCHAR(255) UNIQUE NOT NULL,
    title TEXT NOT NULL,
    summary TEXT,
    url TEXT,
    time_published TIMESTAMP WITH TIME ZONE NOT NULL,
    authors TEXT[],
    topics TEXT[],
    tickers TEXT[],
    overall_sentiment_score DECIMAL(7,4),
    overall_sentiment_label VARCHAR(20),
    ticker_sentiment JSONB, -- Ticker-specific sentiment data
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE dev_news_fmp (
    id BIGSERIAL PRIMARY KEY,
    fmp_id VARCHAR(255) UNIQUE NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    url TEXT,
    publishedDate TIMESTAMP WITH TIME ZONE NOT NULL,
    site VARCHAR(255),
    symbol VARCHAR(20),
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE dev_news_benzinga (
    id BIGSERIAL PRIMARY KEY,
    benzinga_id VARCHAR(255) UNIQUE NOT NULL,
    title TEXT NOT NULL,
    body TEXT,
    url TEXT,
    created TIMESTAMP WITH TIME ZONE NOT NULL,
    updated TIMESTAMP WITH TIME ZONE,
    author VARCHAR(255),
    tickers TEXT[],
    channels TEXT[],
    tags TEXT[],
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Multi-modal training samples table
CREATE TABLE dev_multimodal_training_samples (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    sample_date DATE NOT NULL,
    prediction_horizon INTEGER NOT NULL CHECK (prediction_horizon IN (1, 5, 10, 20)),
    
    -- News sentiment features (lookback: 1, 3, 7 days)
    news_sentiment_1d DECIMAL(7,4) DEFAULT 0, -- Average sentiment last 1 day
    news_sentiment_3d DECIMAL(7,4) DEFAULT 0, -- Average sentiment last 3 days  
    news_sentiment_7d DECIMAL(7,4) DEFAULT 0, -- Average sentiment last 7 days
    news_volume_1d INTEGER DEFAULT 0, -- News article count
    news_volume_3d INTEGER DEFAULT 0,
    news_volume_7d INTEGER DEFAULT 0,
    news_momentum_3d DECIMAL(7,4) DEFAULT 0, -- Sentiment change over 3 days
    news_momentum_7d DECIMAL(7,4) DEFAULT 0, -- Sentiment change over 7 days
    
    -- Economic event features
    economic_event_impact_1d DECIMAL(7,4) DEFAULT 0, -- Economic events last 1 day
    economic_event_impact_3d DECIMAL(7,4) DEFAULT 0, -- Economic events last 3 days
    economic_event_impact_7d DECIMAL(7,4) DEFAULT 0, -- Economic events last 7 days
    earnings_impact_score DECIMAL(7,4) DEFAULT 0, -- Earnings-specific impact
    macro_event_impact DECIMAL(7,4) DEFAULT 0, -- Macro economic impact
    fed_event_impact DECIMAL(7,4) DEFAULT 0, -- Federal Reserve impact
    
    -- Technical market features (stored as JSONB for flexibility)
    price_features JSONB NOT NULL DEFAULT '{}', -- SMA, EMA, RSI, MACD, Bollinger, ATR, etc.
    volume_features JSONB NOT NULL DEFAULT '{}', -- Volume SMA, relative volume, OBV, etc.
    market_microstructure JSONB DEFAULT '{}', -- Bid-ask spread, order imbalance, etc.
    
    -- Cross-asset features
    sector_correlation DECIMAL(7,4), -- Correlation with sector ETF
    market_correlation DECIMAL(7,4), -- Correlation with SPY
    vix_level DECIMAL(7,4), -- VIX at sample date
    yield_curve_10y2y DECIMAL(7,4), -- 10Y-2Y yield spread
    dxy_level DECIMAL(7,4), -- Dollar strength index
    
    -- Target variables (actual future performance)
    target_return_1d DECIMAL(8,5),
    target_return_5d DECIMAL(8,5), 
    target_return_10d DECIMAL(8,5),
    target_return_20d DECIMAL(8,5),
    target_volatility_5d DECIMAL(8,5), -- 5-day realized volatility
    target_volatility_20d DECIMAL(8,5), -- 20-day realized volatility
    target_max_drawdown DECIMAL(8,5), -- Maximum drawdown in horizon
    target_sharpe_ratio DECIMAL(8,5), -- Risk-adjusted return
    
    -- Classification targets
    target_direction_1d INTEGER CHECK (target_direction_1d IN (-1, 0, 1)), -- down, flat, up
    target_direction_5d INTEGER CHECK (target_direction_5d IN (-1, 0, 1)),
    target_direction_10d INTEGER CHECK (target_direction_10d IN (-1, 0, 1)),
    target_direction_20d INTEGER CHECK (target_direction_20d IN (-1, 0, 1)),
    target_volatility_regime INTEGER CHECK (target_volatility_regime IN (1, 2, 3)), -- low, medium, high vol
    
    -- Sample metadata
    sample_quality_score DECIMAL(5,3) DEFAULT 1.0 CHECK (sample_quality_score BETWEEN 0 AND 1), -- Data quality indicator
    sample_weight DECIMAL(7,4) DEFAULT 1.0, -- Training weight
    is_outlier BOOLEAN DEFAULT FALSE, -- Statistical outlier detection
    market_regime VARCHAR(20) CHECK (market_regime IN ('bull', 'bear', 'sideways', 'crisis')), -- Market regime
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (symbol, sample_date, prediction_horizon)
);

-- Performance indexes for economic events
CREATE INDEX idx_economic_events_event_date ON dev_economic_events(event_date DESC);
CREATE INDEX idx_economic_events_type_category ON dev_economic_events(event_type, event_category);
CREATE INDEX idx_economic_events_symbols ON dev_economic_events USING GIN(affected_symbols);
CREATE INDEX idx_economic_events_sectors ON dev_economic_events USING GIN(affected_sectors);
CREATE INDEX idx_economic_events_severity ON dev_economic_events(severity DESC, event_date DESC);
CREATE INDEX idx_economic_events_impact ON dev_economic_events(predicted_impact_score DESC, event_date DESC);

-- Indexes for news-event mapping
CREATE INDEX idx_news_economic_events_news ON dev_news_economic_events(news_id, news_source);
CREATE INDEX idx_news_economic_events_event ON dev_news_economic_events(event_id);
CREATE INDEX idx_news_economic_events_relevance ON dev_news_economic_events(relevance_score DESC);

-- Performance indexes for Alpha Vantage news
CREATE INDEX idx_news_alpha_vantage_published ON dev_news_alpha_vantage(time_published DESC);
CREATE INDEX idx_news_alpha_vantage_tickers ON dev_news_alpha_vantage USING GIN(tickers);
CREATE INDEX idx_news_alpha_vantage_topics ON dev_news_alpha_vantage USING GIN(topics);
CREATE INDEX idx_news_alpha_vantage_sentiment ON dev_news_alpha_vantage(overall_sentiment_score DESC);

-- Performance indexes for FMP news
CREATE INDEX idx_news_fmp_published ON dev_news_fmp(publishedDate DESC);
CREATE INDEX idx_news_fmp_symbol ON dev_news_fmp(symbol);
CREATE INDEX idx_news_fmp_site ON dev_news_fmp(site);

-- Performance indexes for Benzinga news  
CREATE INDEX idx_news_benzinga_created ON dev_news_benzinga(created DESC);
CREATE INDEX idx_news_benzinga_tickers ON dev_news_benzinga USING GIN(tickers);
CREATE INDEX idx_news_benzinga_channels ON dev_news_benzinga USING GIN(channels);
CREATE INDEX idx_news_benzinga_tags ON dev_news_benzinga USING GIN(tags);

-- Indexes for efficient training data access
CREATE INDEX idx_multimodal_samples_symbol_date ON dev_multimodal_training_samples(symbol, sample_date DESC);
CREATE INDEX idx_multimodal_samples_horizon ON dev_multimodal_training_samples(prediction_horizon, sample_date DESC);
CREATE INDEX idx_multimodal_samples_quality ON dev_multimodal_training_samples(sample_quality_score DESC, is_outlier, sample_date DESC);
CREATE INDEX idx_multimodal_samples_regime ON dev_multimodal_training_samples(market_regime, sample_date DESC);

-- Composite indexes for common queries
CREATE INDEX idx_multimodal_samples_symbol_horizon_date ON dev_multimodal_training_samples(symbol, prediction_horizon, sample_date DESC);
CREATE INDEX idx_economic_events_category_date ON dev_economic_events(event_category, event_date DESC);

-- Views for simplified querying
CREATE VIEW dev_recent_economic_events AS
SELECT 
    id,
    event_type,
    event_subtype, 
    event_category,
    severity,
    affected_symbols,
    affected_sectors,
    event_date,
    predicted_impact_score,
    title,
    data_vendor
FROM dev_economic_events 
WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY event_date DESC, severity DESC;

CREATE VIEW dev_high_impact_news AS
SELECT 
    'polygon' as source,
    p.id,
    p.title,
    p.published_utc as published_date,
    p.tickers,
    NULL as sentiment_score
FROM dev_news_polygon p
WHERE p.published_utc >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL

SELECT 
    'tiingo' as source,
    t.id,
    t.title, 
    t.published_date,
    t.tickers,
    NULL as sentiment_score
FROM dev_news_tiingo t
WHERE t.published_date >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL

SELECT 
    'alpha_vantage' as source,
    av.id,
    av.title,
    av.time_published as published_date,
    av.tickers,
    av.overall_sentiment_score as sentiment_score
FROM dev_news_alpha_vantage av
WHERE av.time_published >= CURRENT_DATE - INTERVAL '7 days'

ORDER BY published_date DESC;

-- Grant permissions to application user
GRANT SELECT, INSERT, UPDATE, DELETE ON dev_economic_events TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON dev_news_economic_events TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON dev_news_alpha_vantage TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON dev_news_fmp TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON dev_news_benzinga TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON dev_multimodal_training_samples TO postgres;

GRANT USAGE ON SEQUENCE dev_economic_events_id_seq TO postgres;
GRANT USAGE ON SEQUENCE dev_news_economic_events_id_seq TO postgres;
GRANT USAGE ON SEQUENCE dev_news_alpha_vantage_id_seq TO postgres;
GRANT USAGE ON SEQUENCE dev_news_fmp_id_seq TO postgres;
GRANT USAGE ON SEQUENCE dev_news_benzinga_id_seq TO postgres;
GRANT USAGE ON SEQUENCE dev_multimodal_training_samples_id_seq TO postgres;

GRANT SELECT ON dev_recent_economic_events TO postgres;
GRANT SELECT ON dev_high_impact_news TO postgres;