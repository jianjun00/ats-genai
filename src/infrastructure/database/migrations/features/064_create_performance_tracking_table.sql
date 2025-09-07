-- Migration 064: Create signal performance tracking table
-- This migration creates the table for detailed signal performance tracking and evaluation

CREATE TABLE dev_signal_performance_tracking (
    id BIGSERIAL PRIMARY KEY,
    signal_id BIGINT NOT NULL REFERENCES dev_critical_news_signals(id) ON DELETE CASCADE,
    
    -- Performance Evaluation Details
    evaluation_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    evaluation_horizon VARCHAR(20) NOT NULL CHECK (evaluation_horizon IN ('1h', '1d', '5d', '20d')),
    evaluation_method VARCHAR(30) DEFAULT 'price_movement' CHECK (evaluation_method IN ('price_movement', 'risk_adjusted', 'relative_performance')),
    
    -- Price Performance Metrics
    actual_price_change DECIMAL(8,5), -- Actual price change during horizon
    predicted_price_change DECIMAL(8,5), -- What was predicted
    price_prediction_error DECIMAL(8,5), -- Absolute error between predicted and actual
    price_prediction_accuracy DECIMAL(5,3), -- Accuracy score (0.0 to 1.0)
    direction_accuracy BOOLEAN, -- Did we get the direction right?
    
    -- Volatility Performance
    actual_volatility_change DECIMAL(8,5), -- Actual volatility during horizon
    predicted_volatility_change DECIMAL(8,5), -- Predicted volatility
    volatility_prediction_accuracy DECIMAL(5,3),
    volatility_spike_detected BOOLEAN, -- Was predicted volatility spike accurate?
    
    -- Volume Performance
    actual_volume_impact DECIMAL(8,5), -- Actual volume change
    predicted_volume_impact DECIMAL(8,5), -- Predicted volume impact
    volume_prediction_accuracy DECIMAL(5,3),
    unusual_volume_detected BOOLEAN,
    
    -- Risk-Adjusted Performance
    risk_adjusted_return DECIMAL(8,5), -- Risk-adjusted performance
    sharpe_ratio DECIMAL(8,5), -- Sharpe ratio for the position
    max_drawdown DECIMAL(8,5), -- Maximum drawdown during horizon
    value_at_risk DECIMAL(8,5), -- VaR calculation
    
    -- Signal Quality Metrics
    signal_hit_rate DECIMAL(5,3), -- Did signal predict direction correctly (0.0 to 1.0)
    signal_magnitude_accuracy DECIMAL(5,3), -- How accurate was magnitude prediction
    signal_timing_accuracy DECIMAL(5,3), -- How accurate was timing
    overall_signal_score DECIMAL(5,3), -- Composite score (0.0 to 1.0)
    confidence_calibration DECIMAL(5,3), -- How well calibrated was confidence score
    
    -- Market Context at Evaluation
    market_regime VARCHAR(20) CHECK (market_regime IN ('bull', 'bear', 'sideways', 'crisis', 'recovery')),
    market_volatility_percentile INTEGER CHECK (market_volatility_percentile BETWEEN 0 AND 100),
    sector_performance DECIMAL(8,5), -- How did the sector perform
    market_performance DECIMAL(8,5), -- How did overall market perform (SPY)
    relative_performance DECIMAL(8,5), -- Performance relative to market
    
    -- Attribution Analysis
    news_contribution DECIMAL(5,3), -- How much news vs other factors contributed
    model_attribution JSONB DEFAULT '{}', -- Which models/agents contributed most to accuracy
    error_attribution JSONB DEFAULT '{}', -- What caused prediction errors
    external_factors JSONB DEFAULT '{}', -- External factors that affected performance
    
    -- Trading Execution Analysis (if signal was traded)
    trade_executed BOOLEAN DEFAULT FALSE,
    execution_price DECIMAL(12,5), -- Price at which trade was executed
    execution_slippage DECIMAL(8,5), -- Slippage vs expected execution
    execution_delay_ms INTEGER, -- Delay between signal and execution
    position_size_actual DECIMAL(8,5), -- Actual position size taken
    commission_costs DECIMAL(8,5), -- Trading costs
    net_pnl DECIMAL(12,5), -- Net P&L from trade
    
    -- Benchmark Comparisons
    benchmark_return DECIMAL(8,5), -- Benchmark return (e.g., SPY)
    alpha_generated DECIMAL(8,5), -- Alpha vs benchmark
    information_ratio DECIMAL(8,5), -- Information ratio
    tracking_error DECIMAL(8,5), -- Tracking error vs benchmark
    
    -- Model Performance Attribution
    llm_accuracy_contribution DECIMAL(5,3), -- How much LLM analysis contributed
    agent_accuracy_contribution JSONB DEFAULT '{}', -- Individual agent contributions
    ensemble_accuracy_boost DECIMAL(5,3), -- Boost from ensemble vs individual models
    rag_context_contribution DECIMAL(5,3), -- How much RAG context helped
    
    -- Learning and Improvement
    false_positive_analysis TEXT, -- Analysis if signal was false positive
    false_negative_missed TEXT, -- Analysis if we missed a signal we should have caught
    improvement_suggestions TEXT[], -- Suggestions for model improvement
    edge_case_identified BOOLEAN DEFAULT FALSE, -- Was this an edge case?
    
    -- Metadata
    evaluator_version VARCHAR(20) DEFAULT '1.0',
    evaluation_data_sources TEXT[], -- What data sources were used for evaluation
    evaluation_confidence DECIMAL(5,3) DEFAULT 1.0, -- Confidence in evaluation accuracy
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance tracking
CREATE INDEX idx_signal_performance_signal_id ON dev_signal_performance_tracking(signal_id);
CREATE INDEX idx_signal_performance_evaluation_time ON dev_signal_performance_tracking(evaluation_timestamp DESC);
CREATE INDEX idx_signal_performance_horizon ON dev_signal_performance_tracking(evaluation_horizon, evaluation_timestamp DESC);
CREATE INDEX idx_signal_performance_accuracy ON dev_signal_performance_tracking(overall_signal_score DESC NULLS LAST);
CREATE INDEX idx_signal_performance_hit_rate ON dev_signal_performance_tracking(signal_hit_rate DESC NULLS LAST);
CREATE INDEX idx_signal_performance_alpha ON dev_signal_performance_tracking(alpha_generated DESC NULLS LAST);
CREATE INDEX idx_signal_performance_regime ON dev_signal_performance_tracking(market_regime, evaluation_timestamp DESC);

-- Composite indexes for analytics
CREATE INDEX idx_signal_performance_horizon_accuracy ON dev_signal_performance_tracking(evaluation_horizon, overall_signal_score DESC);
CREATE INDEX idx_signal_performance_executed_pnl ON dev_signal_performance_tracking(trade_executed, net_pnl DESC NULLS LAST);

-- GIN indexes for JSONB fields
CREATE INDEX idx_signal_performance_model_attribution ON dev_signal_performance_tracking USING GIN(model_attribution);
CREATE INDEX idx_signal_performance_external_factors ON dev_signal_performance_tracking USING GIN(external_factors);

-- Add constraints for data validation
ALTER TABLE dev_signal_performance_tracking ADD CONSTRAINT performance_accuracy_check CHECK (price_prediction_accuracy >= 0.0 AND price_prediction_accuracy <= 1.0);
ALTER TABLE dev_signal_performance_tracking ADD CONSTRAINT performance_signal_score_check CHECK (overall_signal_score >= 0.0 AND overall_signal_score <= 1.0);
ALTER TABLE dev_signal_performance_tracking ADD CONSTRAINT performance_hit_rate_check CHECK (signal_hit_rate >= 0.0 AND signal_hit_rate <= 1.0);
ALTER TABLE dev_signal_performance_tracking ADD CONSTRAINT performance_confidence_check CHECK (evaluation_confidence >= 0.0 AND evaluation_confidence <= 1.0);

-- Comments for documentation
COMMENT ON TABLE dev_signal_performance_tracking IS 'Detailed performance tracking and evaluation for trading signals';
COMMENT ON COLUMN dev_signal_performance_tracking.overall_signal_score IS 'Composite performance score combining accuracy, timing, and magnitude';
COMMENT ON COLUMN dev_signal_performance_tracking.confidence_calibration IS 'How well the original confidence score matched actual performance';
COMMENT ON COLUMN dev_signal_performance_tracking.alpha_generated IS 'Alpha generated vs benchmark (risk-adjusted excess return)';
COMMENT ON COLUMN dev_signal_performance_tracking.model_attribution IS 'JSON showing which models/agents contributed to accuracy/errors';
COMMENT ON COLUMN dev_signal_performance_tracking.news_contribution IS 'Estimated contribution of news vs other market factors';

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON dev_signal_performance_tracking TO postgres;
GRANT USAGE ON SEQUENCE dev_signal_performance_tracking_id_seq TO postgres;

-- Create materialized view for signal performance analytics
CREATE MATERIALIZED VIEW dev_signal_performance_analytics AS
SELECT 
    -- Signal characteristics
    s.symbol,
    s.signal_type,
    s.signal_category,
    s.urgency_level,
    DATE_TRUNC('day', s.signal_timestamp) as signal_date,
    
    -- Performance metrics by horizon
    AVG(CASE WHEN p.evaluation_horizon = '1h' THEN p.overall_signal_score END) as avg_1h_score,
    AVG(CASE WHEN p.evaluation_horizon = '1d' THEN p.overall_signal_score END) as avg_1d_score,
    AVG(CASE WHEN p.evaluation_horizon = '5d' THEN p.overall_signal_score END) as avg_5d_score,
    AVG(CASE WHEN p.evaluation_horizon = '20d' THEN p.overall_signal_score END) as avg_20d_score,
    
    -- Hit rates by horizon
    AVG(CASE WHEN p.evaluation_horizon = '1h' THEN p.signal_hit_rate END) as hit_rate_1h,
    AVG(CASE WHEN p.evaluation_horizon = '1d' THEN p.signal_hit_rate END) as hit_rate_1d,
    AVG(CASE WHEN p.evaluation_horizon = '5d' THEN p.signal_hit_rate END) as hit_rate_5d,
    
    -- Alpha generation
    AVG(CASE WHEN p.evaluation_horizon = '1d' THEN p.alpha_generated END) as avg_alpha_1d,
    AVG(CASE WHEN p.evaluation_horizon = '5d' THEN p.alpha_generated END) as avg_alpha_5d,
    
    -- Risk metrics
    AVG(p.max_drawdown) as avg_max_drawdown,
    AVG(p.sharpe_ratio) as avg_sharpe_ratio,
    
    -- Trading metrics (for executed signals)
    COUNT(CASE WHEN p.trade_executed THEN 1 END) as executed_count,
    AVG(CASE WHEN p.trade_executed THEN p.net_pnl END) as avg_net_pnl,
    SUM(CASE WHEN p.trade_executed THEN p.net_pnl END) as total_pnl,
    
    -- Signal counts and metadata
    COUNT(DISTINCT s.id) as total_signals,
    COUNT(DISTINCT p.id) as total_evaluations,
    MAX(p.evaluation_timestamp) as latest_evaluation
    
FROM dev_critical_news_signals s
LEFT JOIN dev_signal_performance_tracking p ON s.id = p.signal_id
WHERE s.signal_timestamp >= CURRENT_TIMESTAMP - INTERVAL '90 days'
GROUP BY s.symbol, s.signal_type, s.signal_category, s.urgency_level, DATE_TRUNC('day', s.signal_timestamp)
ORDER BY signal_date DESC, avg_1d_score DESC NULLS LAST;

-- Create unique index on materialized view
CREATE UNIQUE INDEX idx_signal_performance_analytics_unique ON dev_signal_performance_analytics(symbol, signal_type, signal_category, urgency_level, signal_date);

-- Grant permissions on materialized view
GRANT SELECT ON dev_signal_performance_analytics TO postgres;

-- Create function to refresh the materialized view
CREATE OR REPLACE FUNCTION refresh_signal_performance_analytics()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW dev_signal_performance_analytics;
END;
$$ LANGUAGE plpgsql;

-- Create view for model performance comparison
CREATE VIEW dev_model_performance_comparison AS
SELECT 
    jsonb_object_keys(model_attribution) as model_name,
    COUNT(*) as total_evaluations,
    AVG(overall_signal_score) as avg_performance_score,
    AVG((model_attribution ->> jsonb_object_keys(model_attribution))::decimal) as avg_model_contribution,
    AVG(signal_hit_rate) as avg_hit_rate,
    AVG(alpha_generated) as avg_alpha,
    COUNT(CASE WHEN overall_signal_score >= 0.8 THEN 1 END) as high_performance_count
FROM dev_signal_performance_tracking
WHERE model_attribution IS NOT NULL 
    AND model_attribution != '{}'::jsonb
    AND evaluation_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
GROUP BY jsonb_object_keys(model_attribution)
HAVING COUNT(*) >= 10
ORDER BY avg_performance_score DESC;

GRANT SELECT ON dev_model_performance_comparison TO postgres;