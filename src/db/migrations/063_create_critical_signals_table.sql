-- Migration 063: Create critical news signals table
-- This migration creates the core table for storing real-time trading signals generated from news analysis

CREATE TABLE dev_critical_news_signals (
    id BIGSERIAL PRIMARY KEY,
    
    -- Signal Identity
    symbol VARCHAR(10) NOT NULL,
    signal_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    signal_uuid UUID DEFAULT gen_random_uuid() UNIQUE,
    
    -- Signal Classification
    signal_type VARCHAR(50) NOT NULL, -- 'earnings_surprise', 'ma_announcement', 'regulatory_change', etc.
    signal_category VARCHAR(30) NOT NULL CHECK (signal_category IN ('bullish', 'bearish', 'neutral', 'risk', 'opportunity')),
    urgency_level INTEGER NOT NULL CHECK (urgency_level BETWEEN 1 AND 10),
    market_session VARCHAR(20) CHECK (market_session IN ('pre_market', 'market_hours', 'after_hours', 'closed')),
    
    -- Signal Strength & Confidence
    signal_strength DECIMAL(7,4) NOT NULL CHECK (signal_strength BETWEEN -1.0 AND 1.0),
    signal_confidence DECIMAL(5,3) NOT NULL CHECK (signal_confidence BETWEEN 0.0 AND 1.0),
    signal_uncertainty DECIMAL(5,3) DEFAULT 0.0 CHECK (signal_uncertainty BETWEEN 0.0 AND 1.0),
    
    -- Supporting Analysis References
    news_llm_analysis_ids BIGINT[] NOT NULL, -- References to supporting analyses
    multi_agent_analysis_ids BIGINT[] NOT NULL, -- References to agent analyses
    supporting_news_count INTEGER DEFAULT 0,
    
    -- Market Impact Predictions
    predicted_price_impact_1h DECIMAL(8,5),
    predicted_price_impact_1d DECIMAL(8,5),
    predicted_price_impact_5d DECIMAL(8,5),
    predicted_price_impact_20d DECIMAL(8,5),
    predicted_volatility_spike DECIMAL(8,5),
    predicted_volume_impact DECIMAL(8,5),
    
    -- Risk Assessment
    risk_score DECIMAL(5,3) NOT NULL DEFAULT 0.0 CHECK (risk_score BETWEEN 0.0 AND 1.0),
    risk_factors TEXT[] DEFAULT '{}',
    uncertainty_score DECIMAL(5,3) DEFAULT 0.0 CHECK (uncertainty_score BETWEEN 0.0 AND 1.0),
    false_positive_probability DECIMAL(5,3) CHECK (false_positive_probability BETWEEN 0.0 AND 1.0),
    model_consensus_strength DECIMAL(5,3), -- How much models agree
    
    -- Trading Recommendations
    recommended_action VARCHAR(20) CHECK (recommended_action IN ('strong_buy', 'buy', 'hold', 'sell', 'strong_sell', 'hedge', 'wait')),
    position_sizing_recommendation DECIMAL(5,3) CHECK (position_sizing_recommendation BETWEEN 0.0 AND 1.0),
    time_horizon VARCHAR(20) CHECK (time_horizon IN ('intraday', 'short', 'medium', 'long')),
    stop_loss_recommendation DECIMAL(8,5),
    take_profit_recommendation DECIMAL(8,5),
    
    -- Signal Context
    key_entities JSONB DEFAULT '{}',
    key_themes TEXT[],
    market_conditions JSONB DEFAULT '{}',
    sector_impact TEXT[],
    correlated_symbols TEXT[],
    
    -- Performance Tracking (populated after signal execution)
    signal_performance_1h DECIMAL(8,5), -- Actual performance after 1h
    signal_performance_1d DECIMAL(8,5), -- Actual performance after 1d
    signal_performance_5d DECIMAL(8,5), -- Actual performance after 5d
    signal_performance_20d DECIMAL(8,5), -- Actual performance after 20d
    performance_evaluation_date TIMESTAMP WITH TIME ZONE,
    signal_accuracy_score DECIMAL(5,3), -- Post-evaluation accuracy (0.0 to 1.0)
    
    -- Signal Attribution
    contributing_factors JSONB DEFAULT '{}', -- What factors contributed most to signal
    model_attribution JSONB DEFAULT '{}', -- Which models/agents contributed most
    news_attribution JSONB DEFAULT '{}', -- Which news articles were most influential
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by VARCHAR(50) DEFAULT 'llm_signal_system',
    signal_version VARCHAR(20) DEFAULT '1.0'
);

-- Performance indexes for critical signals
CREATE INDEX idx_critical_signals_symbol_time ON dev_critical_news_signals(symbol, signal_timestamp DESC);
CREATE INDEX idx_critical_signals_urgency ON dev_critical_news_signals(urgency_level DESC, signal_timestamp DESC);
CREATE INDEX idx_critical_signals_strength ON dev_critical_news_signals(signal_strength DESC, signal_confidence DESC);
CREATE INDEX idx_critical_signals_type ON dev_critical_news_signals(signal_type, signal_timestamp DESC);
CREATE INDEX idx_critical_signals_category ON dev_critical_news_signals(signal_category, signal_timestamp DESC);
CREATE INDEX idx_critical_signals_performance ON dev_critical_news_signals(signal_accuracy_score DESC NULLS LAST);
CREATE INDEX idx_critical_signals_risk ON dev_critical_news_signals(risk_score ASC, signal_timestamp DESC);
CREATE INDEX idx_critical_signals_recommendation ON dev_critical_news_signals(recommended_action, signal_timestamp DESC);

-- Composite indexes for common queries
CREATE INDEX idx_critical_signals_symbol_strength_time ON dev_critical_news_signals(symbol, signal_strength DESC, signal_timestamp DESC);
CREATE INDEX idx_critical_signals_urgency_confidence ON dev_critical_news_signals(urgency_level DESC, signal_confidence DESC);
CREATE INDEX idx_critical_signals_session_category ON dev_critical_news_signals(market_session, signal_category);

-- GIN indexes for array and JSONB fields
CREATE INDEX idx_critical_signals_themes ON dev_critical_news_signals USING GIN(key_themes);
CREATE INDEX idx_critical_signals_entities ON dev_critical_news_signals USING GIN(key_entities);
CREATE INDEX idx_critical_signals_conditions ON dev_critical_news_signals USING GIN(market_conditions);
CREATE INDEX idx_critical_signals_llm_analysis_ids ON dev_critical_news_signals USING GIN(news_llm_analysis_ids);

-- Comments for documentation
COMMENT ON TABLE dev_critical_news_signals IS 'Real-time trading signals generated from LLM analysis of financial news';
COMMENT ON COLUMN dev_critical_news_signals.signal_strength IS 'Signal strength from -1.0 (strong bearish) to 1.0 (strong bullish)';
COMMENT ON COLUMN dev_critical_news_signals.urgency_level IS 'Signal urgency from 1 (low) to 10 (critical, immediate action required)';
COMMENT ON COLUMN dev_critical_news_signals.signal_confidence IS 'Confidence in signal accuracy from 0.0 (no confidence) to 1.0 (very confident)';
COMMENT ON COLUMN dev_critical_news_signals.risk_score IS 'Risk assessment from 0.0 (low risk) to 1.0 (high risk)';
COMMENT ON COLUMN dev_critical_news_signals.recommended_action IS 'Specific trading action recommended based on signal analysis';
COMMENT ON COLUMN dev_critical_news_signals.position_sizing_recommendation IS 'Recommended position size as fraction of portfolio (0.0 to 1.0)';
COMMENT ON COLUMN dev_critical_news_signals.signal_accuracy_score IS 'Post-evaluation accuracy score populated after signal outcome is known';

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON dev_critical_news_signals TO postgres;
GRANT USAGE ON SEQUENCE dev_critical_news_signals_id_seq TO postgres;

-- Create function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_critical_signals_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update the updated_at field
CREATE TRIGGER trigger_update_critical_signals_updated_at
    BEFORE UPDATE ON dev_critical_news_signals
    FOR EACH ROW
    EXECUTE FUNCTION update_critical_signals_updated_at();

-- Create view for active high-priority signals
CREATE VIEW dev_active_critical_signals AS
SELECT 
    id,
    symbol,
    signal_timestamp,
    signal_type,
    signal_category,
    urgency_level,
    signal_strength,
    signal_confidence,
    recommended_action,
    time_horizon,
    key_themes,
    risk_score
FROM dev_critical_news_signals 
WHERE signal_timestamp >= CURRENT_TIMESTAMP - INTERVAL '4 hours'
    AND urgency_level >= 6
    AND signal_confidence >= 0.6
    AND recommended_action IN ('strong_buy', 'buy', 'sell', 'strong_sell')
ORDER BY urgency_level DESC, signal_confidence DESC, signal_timestamp DESC;

GRANT SELECT ON dev_active_critical_signals TO postgres;

-- Create view for recent signal performance
CREATE VIEW dev_recent_signal_performance AS
SELECT 
    symbol,
    signal_type,
    COUNT(*) as total_signals,
    AVG(signal_accuracy_score) as avg_accuracy,
    AVG(signal_confidence) as avg_confidence,
    AVG(ABS(signal_performance_1d)) as avg_1d_impact,
    COUNT(CASE WHEN signal_accuracy_score >= 0.7 THEN 1 END) as successful_signals,
    MAX(signal_timestamp) as latest_signal
FROM dev_critical_news_signals 
WHERE signal_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
    AND signal_accuracy_score IS NOT NULL
GROUP BY symbol, signal_type
HAVING COUNT(*) >= 3
ORDER BY avg_accuracy DESC, total_signals DESC;

GRANT SELECT ON dev_recent_signal_performance TO postgres;

-- Create view for signal quality metrics
CREATE VIEW dev_signal_quality_metrics AS
SELECT 
    DATE(signal_timestamp) as signal_date,
    COUNT(*) as total_signals,
    AVG(signal_confidence) as avg_confidence,
    AVG(signal_uncertainty) as avg_uncertainty,
    AVG(risk_score) as avg_risk_score,
    COUNT(CASE WHEN urgency_level >= 8 THEN 1 END) as critical_signals,
    COUNT(CASE WHEN signal_accuracy_score >= 0.8 THEN 1 END) as high_accuracy_signals,
    AVG(CASE WHEN signal_accuracy_score IS NOT NULL THEN signal_accuracy_score END) as avg_accuracy
FROM dev_critical_news_signals
WHERE signal_timestamp >= CURRENT_TIMESTAMP - INTERVAL '90 days'
GROUP BY DATE(signal_timestamp)
ORDER BY signal_date DESC;

GRANT SELECT ON dev_signal_quality_metrics TO postgres;