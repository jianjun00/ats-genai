-- Create 1-minute bars table for TFT model training
-- Migration 033: Add minute-level OHLCV data support

-- Minute bars table using TimescaleDB for time-series optimization
CREATE TABLE IF NOT EXISTS minute_bars (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open NUMERIC(12,4) NOT NULL,
    high NUMERIC(12,4) NOT NULL,
    low NUMERIC(12,4) NOT NULL,
    close NUMERIC(12,4) NOT NULL,
    volume BIGINT NOT NULL DEFAULT 0,
    vwap NUMERIC(12,4), -- Volume weighted average price
    trade_count INTEGER, -- Number of trades in the minute
    vendor VARCHAR(50) NOT NULL DEFAULT 'polygon',
    
    -- Technical indicators (calculated fields)
    returns NUMERIC(8,6), -- Minute-over-minute returns
    sma_5 NUMERIC(12,4), -- 5-minute simple moving average
    sma_20 NUMERIC(12,4), -- 20-minute simple moving average
    ema_12 NUMERIC(12,4), -- 12-minute exponential moving average
    ema_26 NUMERIC(12,4), -- 26-minute exponential moving average
    macd NUMERIC(8,6), -- MACD indicator
    macd_signal NUMERIC(8,6), -- MACD signal line
    rsi NUMERIC(5,2), -- Relative Strength Index (0-100)
    bb_upper NUMERIC(12,4), -- Bollinger Band upper
    bb_middle NUMERIC(12,4), -- Bollinger Band middle
    bb_lower NUMERIC(12,4), -- Bollinger Band lower
    volume_sma NUMERIC(15,2), -- 20-minute volume moving average
    volume_ratio NUMERIC(6,3), -- Current volume / volume SMA
    volatility NUMERIC(8,6), -- Rolling volatility
    
    -- Data quality fields
    quality_score NUMERIC(3,2), -- 0.0 to 1.0 quality score
    is_validated BOOLEAN DEFAULT FALSE,
    data_source_flags JSONB DEFAULT '{}', -- Additional metadata
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure unique constraint on symbol + timestamp
    UNIQUE(symbol, timestamp)
);

-- Convert to TimescaleDB hypertable for time-series optimization
SELECT create_hypertable('minute_bars', 'timestamp', if_not_exists => TRUE);

-- Indexes for performance optimization
CREATE INDEX IF NOT EXISTS idx_minute_bars_symbol_time ON minute_bars (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_minute_bars_timestamp ON minute_bars (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_minute_bars_symbol ON minute_bars (symbol);
CREATE INDEX IF NOT EXISTS idx_minute_bars_vendor ON minute_bars (vendor);
CREATE INDEX IF NOT EXISTS idx_minute_bars_quality ON minute_bars (quality_score DESC) WHERE quality_score IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_minute_bars_volume ON minute_bars (volume DESC) WHERE volume > 0;

-- Partial indexes for TFT model training queries
CREATE INDEX IF NOT EXISTS idx_minute_bars_tft_features ON minute_bars (symbol, timestamp DESC) 
    WHERE returns IS NOT NULL AND rsi IS NOT NULL AND volume > 0;

-- GIN index for data source flags JSONB
CREATE INDEX IF NOT EXISTS idx_minute_bars_source_flags ON minute_bars USING GIN(data_source_flags);

-- Data retention policy (keep 2 years of minute data)
SELECT add_retention_policy('minute_bars', INTERVAL '2 years', if_not_exists => TRUE);

-- Compression policy (compress data older than 7 days)
SELECT add_compression_policy('minute_bars', INTERVAL '7 days', if_not_exists => TRUE);

-- Minute bars aggregated view for TFT model consumption
CREATE OR REPLACE VIEW minute_bars_tft AS
SELECT 
    symbol,
    timestamp,
    open,
    high,
    low,
    close,
    volume,
    returns,
    
    -- Technical indicators for TFT features
    COALESCE(sma_5, close) as sma_5,
    COALESCE(sma_20, close) as sma_20,
    COALESCE(ema_12, close) as ema_12,
    COALESCE(ema_26, close) as ema_26,
    COALESCE(macd, 0) as macd,
    COALESCE(macd_signal, 0) as macd_signal,
    COALESCE(rsi, 50) as rsi,
    COALESCE(bb_upper, high) as bb_upper,
    COALESCE(bb_middle, close) as bb_middle,
    COALESCE(bb_lower, low) as bb_lower,
    COALESCE(volume_ratio, 1.0) as volume_ratio,
    COALESCE(volatility, 0) as volatility,
    
    -- Additional computed features
    (high - low) / NULLIF(close, 0) as price_range_ratio,
    (close - open) / NULLIF(open, 0) as intraday_return,
    CASE 
        WHEN volume > 0 THEN (vwap - close) / NULLIF(close, 0)
        ELSE 0 
    END as vwap_deviation,
    
    -- Data quality indicators
    quality_score,
    is_validated,
    vendor,
    created_at

FROM minute_bars
WHERE 
    quality_score IS NULL OR quality_score >= 0.7  -- Filter low quality data
ORDER BY symbol, timestamp;

-- Materialized view for TFT training data (updated hourly)
CREATE MATERIALIZED VIEW IF NOT EXISTS minute_bars_tft_training AS
SELECT 
    symbol,
    timestamp,
    open,
    high,
    low,
    close,
    volume,
    returns,
    sma_5,
    sma_20,
    ema_12,
    ema_26,
    macd,
    macd_signal,
    rsi,
    bb_upper,
    bb_middle,
    bb_lower,
    volume_ratio,
    volatility,
    price_range_ratio,
    intraday_return,
    vwap_deviation,
    
    -- Lag features for TFT
    LAG(close, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as close_lag1,
    LAG(close, 5) OVER (PARTITION BY symbol ORDER BY timestamp) as close_lag5,
    LAG(volume, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as volume_lag1,
    LAG(returns, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as returns_lag1,
    
    -- Forward looking targets (for supervised learning)
    LEAD(close, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as close_lead1,
    LEAD(close, 5) OVER (PARTITION BY symbol ORDER BY timestamp) as close_lead5,
    LEAD(close, 15) OVER (PARTITION BY symbol ORDER BY timestamp) as close_lead15,
    LEAD(close, 30) OVER (PARTITION BY symbol ORDER BY timestamp) as close_lead30

FROM minute_bars_tft
WHERE 
    timestamp >= CURRENT_DATE - INTERVAL '1 year'  -- Last year for training
    AND is_validated = TRUE
ORDER BY symbol, timestamp;

-- Index on materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_minute_bars_tft_training_symbol_time 
    ON minute_bars_tft_training (symbol, timestamp);

-- Data quality summary view
CREATE OR REPLACE VIEW minute_bars_quality_summary AS
SELECT 
    symbol,
    DATE(timestamp) as date,
    COUNT(*) as total_bars,
    COUNT(*) FILTER (WHERE volume > 0) as bars_with_volume,
    COUNT(*) FILTER (WHERE quality_score >= 0.8) as high_quality_bars,
    AVG(quality_score) as avg_quality_score,
    MIN(timestamp) as first_bar,
    MAX(timestamp) as last_bar,
    
    -- Gap analysis
    COUNT(*) FILTER (WHERE 
        LAG(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp) < timestamp - INTERVAL '2 minutes'
    ) as time_gaps,
    
    -- Price continuity
    COUNT(*) FILTER (WHERE 
        ABS((close - LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp)) / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp), 0)) > 0.1
    ) as price_outliers,
    
    -- Volume analysis
    AVG(volume) as avg_volume,
    STDDEV(volume) as volume_std,
    
    vendor
    
FROM minute_bars
WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY symbol, DATE(timestamp), vendor
ORDER BY symbol, date DESC;

-- Constraints for data integrity
ALTER TABLE minute_bars ADD CONSTRAINT minute_bars_ohlc_check 
    CHECK (high >= low AND high >= open AND high >= close AND low <= open AND low <= close);

ALTER TABLE minute_bars ADD CONSTRAINT minute_bars_volume_check 
    CHECK (volume >= 0);

ALTER TABLE minute_bars ADD CONSTRAINT minute_bars_quality_check 
    CHECK (quality_score IS NULL OR (quality_score >= 0.0 AND quality_score <= 1.0));

ALTER TABLE minute_bars ADD CONSTRAINT minute_bars_rsi_check 
    CHECK (rsi IS NULL OR (rsi >= 0 AND rsi <= 100));

ALTER TABLE minute_bars ADD CONSTRAINT minute_bars_positive_prices_check 
    CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0);

-- Update trigger for updated_at timestamp
CREATE OR REPLACE FUNCTION update_minute_bars_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER trigger_minute_bars_updated_at
    BEFORE UPDATE ON minute_bars
    FOR EACH ROW
    EXECUTE FUNCTION update_minute_bars_updated_at();

-- Comments for documentation
COMMENT ON TABLE minute_bars IS 'High-frequency 1-minute OHLCV bars optimized for TFT model training';
COMMENT ON COLUMN minute_bars.vwap IS 'Volume weighted average price for the minute';
COMMENT ON COLUMN minute_bars.trade_count IS 'Number of individual trades aggregated into this minute bar';
COMMENT ON COLUMN minute_bars.quality_score IS 'Data quality score from 0.0 (poor) to 1.0 (excellent)';
COMMENT ON COLUMN minute_bars.returns IS 'Minute-over-minute log returns for TFT targets';
COMMENT ON COLUMN minute_bars.data_source_flags IS 'JSON metadata about data source, gaps, interpolations, etc.';

COMMENT ON VIEW minute_bars_tft IS 'Cleaned minute bars view with computed features for TFT model consumption';
COMMENT ON MATERIALIZED VIEW minute_bars_tft_training IS 'Pre-computed training dataset with lag/lead features for TFT models';
COMMENT ON VIEW minute_bars_quality_summary IS 'Daily summary of data quality metrics for monitoring';