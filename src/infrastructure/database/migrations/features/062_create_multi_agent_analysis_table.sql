-- Migration 062: Create multi-agent analysis results table
-- This migration creates the table for storing results from the multi-agent analysis framework

CREATE TABLE dev_multi_agent_analysis (
    id BIGSERIAL PRIMARY KEY,
    news_llm_analysis_id BIGINT NOT NULL REFERENCES dev_news_llm_analysis(id) ON DELETE CASCADE,
    
    -- Individual Agent Results
    sentiment_agent_score DECIMAL(7,4), -- -1.0 to 1.0
    sentiment_agent_confidence DECIMAL(5,3), -- 0.0 to 1.0
    sentiment_agent_reasoning TEXT,
    sentiment_agent_key_factors TEXT[],
    sentiment_agent_risk_factors TEXT[],
    
    technical_agent_score DECIMAL(7,4),
    technical_agent_confidence DECIMAL(5,3),
    technical_agent_reasoning TEXT,
    technical_agent_key_factors TEXT[],
    technical_agent_risk_factors TEXT[],
    
    fundamental_agent_score DECIMAL(7,4),
    fundamental_agent_confidence DECIMAL(5,3),
    fundamental_agent_reasoning TEXT,
    fundamental_agent_key_factors TEXT[],
    fundamental_agent_risk_factors TEXT[],
    
    risk_agent_score DECIMAL(7,4),
    risk_agent_confidence DECIMAL(5,3),
    risk_agent_reasoning TEXT,
    risk_agent_key_factors TEXT[],
    risk_agent_risk_factors TEXT[],
    
    macro_agent_score DECIMAL(7,4),
    macro_agent_confidence DECIMAL(5,3),
    macro_agent_reasoning TEXT,
    macro_agent_key_factors TEXT[],
    macro_agent_risk_factors TEXT[],
    
    microstructure_agent_score DECIMAL(7,4),
    microstructure_agent_confidence DECIMAL(5,3),
    microstructure_agent_reasoning TEXT,
    microstructure_agent_key_factors TEXT[],
    microstructure_agent_risk_factors TEXT[],
    
    -- Consensus Results
    consensus_signal DECIMAL(7,4) NOT NULL, -- -1.0 to 1.0
    consensus_confidence DECIMAL(5,3) NOT NULL, -- 0.0 to 1.0
    consensus_method VARCHAR(50) DEFAULT 'weighted_average',
    agent_agreement_score DECIMAL(5,3), -- How much agents agree (0.0 to 1.0)
    agent_disagreement_score DECIMAL(5,3), -- Level of disagreement
    outlier_agents TEXT[], -- Agents with significantly different scores
    
    -- Consensus Reasoning and Attribution
    consensus_explanation TEXT,
    consensus_key_factors TEXT[], -- Most important factors across agents
    consensus_risk_factors TEXT[], -- Key risk factors identified
    uncertainty_factors TEXT[], -- Factors contributing to uncertainty
    
    -- Agent Performance Weights (dynamic based on historical accuracy)
    agent_weights JSONB DEFAULT '{}', -- Dynamic weights used for this analysis
    weight_adjustment_reason TEXT, -- Why weights were adjusted
    
    -- Processing Metadata
    analysis_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    analysis_duration_ms INTEGER, -- Time taken for multi-agent analysis
    model_versions JSONB DEFAULT '{}', -- Model versions used by each agent
    
    -- Quality Metrics
    consensus_quality_score DECIMAL(5,3), -- Quality of the consensus (0.0 to 1.0)
    agent_diversity_score DECIMAL(5,3), -- How diverse agent opinions were
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_multi_agent_analysis_news_id ON dev_multi_agent_analysis(news_llm_analysis_id);
CREATE INDEX idx_multi_agent_analysis_consensus ON dev_multi_agent_analysis(consensus_signal DESC, consensus_confidence DESC);
CREATE INDEX idx_multi_agent_analysis_timestamp ON dev_multi_agent_analysis(analysis_timestamp DESC);
CREATE INDEX idx_multi_agent_analysis_agreement ON dev_multi_agent_analysis(agent_agreement_score DESC);
CREATE INDEX idx_multi_agent_analysis_quality ON dev_multi_agent_analysis(consensus_quality_score DESC);

-- Composite indexes for common queries
CREATE INDEX idx_multi_agent_analysis_signal_time ON dev_multi_agent_analysis(consensus_signal DESC, analysis_timestamp DESC);
CREATE INDEX idx_multi_agent_analysis_confidence_quality ON dev_multi_agent_analysis(consensus_confidence DESC, consensus_quality_score DESC);

-- Add constraints to ensure data integrity
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT multi_agent_consensus_signal_check CHECK (consensus_signal >= -1.0 AND consensus_signal <= 1.0);
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT multi_agent_consensus_confidence_check CHECK (consensus_confidence >= 0.0 AND consensus_confidence <= 1.0);
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT multi_agent_agreement_score_check CHECK (agent_agreement_score >= 0.0 AND agent_agreement_score <= 1.0);
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT multi_agent_quality_score_check CHECK (consensus_quality_score >= 0.0 AND consensus_quality_score <= 1.0);

-- Individual agent score constraints
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT sentiment_agent_score_check CHECK (sentiment_agent_score >= -1.0 AND sentiment_agent_score <= 1.0);
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT technical_agent_score_check CHECK (technical_agent_score >= -1.0 AND technical_agent_score <= 1.0);
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT fundamental_agent_score_check CHECK (fundamental_agent_score >= -1.0 AND fundamental_agent_score <= 1.0);
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT risk_agent_score_check CHECK (risk_agent_score >= -1.0 AND risk_agent_score <= 1.0);
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT macro_agent_score_check CHECK (macro_agent_score >= -1.0 AND macro_agent_score <= 1.0);
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT microstructure_agent_score_check CHECK (microstructure_agent_score >= -1.0 AND microstructure_agent_score <= 1.0);

-- Individual agent confidence constraints
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT sentiment_agent_confidence_check CHECK (sentiment_agent_confidence >= 0.0 AND sentiment_agent_confidence <= 1.0);
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT technical_agent_confidence_check CHECK (technical_agent_confidence >= 0.0 AND technical_agent_confidence <= 1.0);
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT fundamental_agent_confidence_check CHECK (fundamental_agent_confidence >= 0.0 AND fundamental_agent_confidence <= 1.0);
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT risk_agent_confidence_check CHECK (risk_agent_confidence >= 0.0 AND risk_agent_confidence <= 1.0);
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT macro_agent_confidence_check CHECK (macro_agent_confidence >= 0.0 AND macro_agent_confidence <= 1.0);
ALTER TABLE dev_multi_agent_analysis ADD CONSTRAINT microstructure_agent_confidence_check CHECK (microstructure_agent_confidence >= 0.0 AND microstructure_agent_confidence <= 1.0);

-- Comments for documentation
COMMENT ON TABLE dev_multi_agent_analysis IS 'Results from multi-agent analysis framework including individual agent scores and consensus mechanism';
COMMENT ON COLUMN dev_multi_agent_analysis.consensus_signal IS 'Final consensus signal from all agents (-1.0 bearish to 1.0 bullish)';
COMMENT ON COLUMN dev_multi_agent_analysis.agent_agreement_score IS 'Measure of how much agents agree (1.0 = perfect agreement)';
COMMENT ON COLUMN dev_multi_agent_analysis.outlier_agents IS 'List of agent names that had significantly different opinions from consensus';
COMMENT ON COLUMN dev_multi_agent_analysis.agent_weights IS 'JSON object with dynamic weights used for each agent in consensus calculation';
COMMENT ON COLUMN dev_multi_agent_analysis.consensus_quality_score IS 'Overall quality score of the consensus based on agent confidence and agreement';

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON dev_multi_agent_analysis TO postgres;
GRANT USAGE ON SEQUENCE dev_multi_agent_analysis_id_seq TO postgres;

-- Create view for high-quality consensus results
CREATE VIEW dev_high_quality_consensus AS
SELECT 
    maa.id,
    maa.news_llm_analysis_id,
    nla.news_id,
    nla.news_source,
    maa.consensus_signal,
    maa.consensus_confidence,
    maa.agent_agreement_score,
    maa.consensus_quality_score,
    maa.consensus_key_factors,
    maa.analysis_timestamp
FROM dev_multi_agent_analysis maa
JOIN dev_news_llm_analysis nla ON maa.news_llm_analysis_id = nla.id
WHERE maa.consensus_confidence >= 0.7
    AND maa.agent_agreement_score >= 0.6
    AND maa.consensus_quality_score >= 0.7
    AND maa.analysis_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY maa.consensus_confidence DESC, maa.agent_agreement_score DESC;

GRANT SELECT ON dev_high_quality_consensus TO postgres;

-- Create view for agent performance tracking
CREATE VIEW dev_agent_performance_summary AS
SELECT 
    'sentiment' as agent_name,
    COUNT(*) as total_analyses,
    AVG(sentiment_agent_score) as avg_score,
    AVG(sentiment_agent_confidence) as avg_confidence,
    STDDEV(sentiment_agent_score) as score_std_dev,
    COUNT(CASE WHEN 'sentiment' = ANY(outlier_agents) THEN 1 END) as outlier_count
FROM dev_multi_agent_analysis
WHERE analysis_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'

UNION ALL

SELECT 
    'technical' as agent_name,
    COUNT(*) as total_analyses,
    AVG(technical_agent_score) as avg_score,
    AVG(technical_agent_confidence) as avg_confidence,
    STDDEV(technical_agent_score) as score_std_dev,
    COUNT(CASE WHEN 'technical' = ANY(outlier_agents) THEN 1 END) as outlier_count
FROM dev_multi_agent_analysis
WHERE analysis_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'

UNION ALL

SELECT 
    'fundamental' as agent_name,
    COUNT(*) as total_analyses,
    AVG(fundamental_agent_score) as avg_score,
    AVG(fundamental_agent_confidence) as avg_confidence,
    STDDEV(fundamental_agent_score) as score_std_dev,
    COUNT(CASE WHEN 'fundamental' = ANY(outlier_agents) THEN 1 END) as outlier_count
FROM dev_multi_agent_analysis
WHERE analysis_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'

UNION ALL

SELECT 
    'risk' as agent_name,
    COUNT(*) as total_analyses,
    AVG(risk_agent_score) as avg_score,
    AVG(risk_agent_confidence) as avg_confidence,
    STDDEV(risk_agent_score) as score_std_dev,
    COUNT(CASE WHEN 'risk' = ANY(outlier_agents) THEN 1 END) as outlier_count
FROM dev_multi_agent_analysis
WHERE analysis_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'

UNION ALL

SELECT 
    'macro' as agent_name,
    COUNT(*) as total_analyses,
    AVG(macro_agent_score) as avg_score,
    AVG(macro_agent_confidence) as avg_confidence,
    STDDEV(macro_agent_score) as score_std_dev,
    COUNT(CASE WHEN 'macro' = ANY(outlier_agents) THEN 1 END) as outlier_count
FROM dev_multi_agent_analysis
WHERE analysis_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'

UNION ALL

SELECT 
    'microstructure' as agent_name,
    COUNT(*) as total_analyses,
    AVG(microstructure_agent_score) as avg_score,
    AVG(microstructure_agent_confidence) as avg_confidence,
    STDDEV(microstructure_agent_score) as score_std_dev,
    COUNT(CASE WHEN 'microstructure' = ANY(outlier_agents) THEN 1 END) as outlier_count
FROM dev_multi_agent_analysis
WHERE analysis_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days';

GRANT SELECT ON dev_agent_performance_summary TO postgres;