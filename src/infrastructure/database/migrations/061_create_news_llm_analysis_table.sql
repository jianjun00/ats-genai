-- Migration 061: Create enhanced database schema for LLM news analysis
-- This migration implements the core database schema for the LLM-powered news signal extraction system

-- Enhanced news LLM analysis table
CREATE TABLE dev_news_llm_analysis (
    -- Primary Key & References
    id BIGSERIAL PRIMARY KEY,
    news_id BIGINT NOT NULL,
    news_source VARCHAR(20) NOT NULL CHECK (news_source IN ('polygon', 'tiingo', 'alpha_vantage', 'fmp', 'benzinga', 'reuters', 'bloomberg')),
    
    -- Named Entity Recognition Results
    extracted_entities JSONB NOT NULL DEFAULT '{}', -- All extracted entities by category
    financial_entities JSONB DEFAULT '{}', -- Companies, tickers, financial instruments
    people_entities JSONB DEFAULT '{}', -- CEOs, analysts, officials, executives
    amount_entities JSONB DEFAULT '{}', -- Dollar amounts, percentages, quantities
    date_entities JSONB DEFAULT '{}', -- Dates, deadlines, announcement dates
    location_entities JSONB DEFAULT '{}', -- Countries, cities, exchanges
    
    -- Event Extraction Results
    detected_events JSONB DEFAULT '{}', -- Structured financial events
    event_types TEXT[] DEFAULT '{}', -- earnings, m&a, regulatory, layoffs, etc.
    event_urgency INTEGER CHECK (event_urgency BETWEEN 1 AND 10),
    event_scope VARCHAR(20) CHECK (event_scope IN ('company', 'sector', 'market', 'global')),
    
    -- Causal Analysis
    causal_relationships JSONB DEFAULT '{}', -- Cause-effect chains
    causal_confidence DECIMAL(5,3) DEFAULT 0,
    impact_timeline JSONB DEFAULT '{}', -- Expected timeline of effects
    
    -- Market Impact Predictions
    predicted_price_impact_1h DECIMAL(8,5),
    predicted_price_impact_1d DECIMAL(8,5),
    predicted_price_impact_5d DECIMAL(8,5),
    predicted_volatility_impact DECIMAL(8,5),
    impact_confidence DECIMAL(5,3),
    
    -- Enhanced Sentiment Analysis
    sentiment_scores JSONB DEFAULT '{}', -- Multi-model sentiment scores
    sentiment_finbert DECIMAL(7,4), -- FinBERT score
    sentiment_finllama DECIMAL(7,4), -- FinLlama score
    sentiment_bloomberggpt DECIMAL(7,4), -- BloombergGPT score
    sentiment_ensemble DECIMAL(7,4), -- Weighted ensemble score
    sentiment_confidence DECIMAL(5,3),
    sentiment_uncertainty DECIMAL(5,3), -- Uncertainty quantification
    
    -- RAG-Based Context Analysis
    historical_precedents JSONB DEFAULT '{}', -- Similar historical events
    market_context JSONB DEFAULT '{}', -- Current market conditions context
    company_context JSONB DEFAULT '{}', -- Company-specific context
    sector_context JSONB DEFAULT '{}', -- Sector-specific context
    rag_confidence DECIMAL(5,3),
    
    -- Processing Metadata
    processing_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processing_latency_ms INTEGER,
    model_versions JSONB DEFAULT '{}', -- Version info for all models used
    processing_node VARCHAR(50), -- Which processing node handled this
    
    -- Quality Metrics
    data_quality_score DECIMAL(5,3) DEFAULT 1.0,
    analysis_completeness DECIMAL(5,3) DEFAULT 1.0,
    
    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Performance indexes for dev_news_llm_analysis
CREATE INDEX idx_news_llm_analysis_news_id ON dev_news_llm_analysis(news_id, news_source);
CREATE INDEX idx_news_llm_analysis_timestamp ON dev_news_llm_analysis(processing_timestamp DESC);
CREATE INDEX idx_news_llm_analysis_events ON dev_news_llm_analysis USING GIN(event_types);
CREATE INDEX idx_news_llm_analysis_entities ON dev_news_llm_analysis USING GIN(extracted_entities);
CREATE INDEX idx_news_llm_analysis_sentiment ON dev_news_llm_analysis(sentiment_ensemble DESC);
CREATE INDEX idx_news_llm_analysis_impact ON dev_news_llm_analysis(predicted_price_impact_1d DESC NULLS LAST);
CREATE INDEX idx_news_llm_analysis_quality ON dev_news_llm_analysis(data_quality_score DESC, analysis_completeness DESC);

-- Comments for documentation
COMMENT ON TABLE dev_news_llm_analysis IS 'LLM-based analysis results for news articles including NER, events, sentiment, and RAG context';
COMMENT ON COLUMN dev_news_llm_analysis.extracted_entities IS 'JSON object containing all extracted entities categorized by type (COMPANY, PERSON, FINANCIAL_METRIC, etc.)';
COMMENT ON COLUMN dev_news_llm_analysis.detected_events IS 'JSON array of structured financial events extracted from the news article';
COMMENT ON COLUMN dev_news_llm_analysis.causal_relationships IS 'JSON object mapping cause-effect relationships between events';
COMMENT ON COLUMN dev_news_llm_analysis.sentiment_ensemble IS 'Weighted ensemble sentiment score from multiple models (-1.0 to 1.0)';
COMMENT ON COLUMN dev_news_llm_analysis.sentiment_uncertainty IS 'Uncertainty quantification for sentiment analysis (0.0 to 1.0)';
COMMENT ON COLUMN dev_news_llm_analysis.historical_precedents IS 'JSON array of similar historical events retrieved via RAG';
COMMENT ON COLUMN dev_news_llm_analysis.processing_latency_ms IS 'Total processing time in milliseconds for performance monitoring';

-- Grant permissions to application users
GRANT SELECT, INSERT, UPDATE, DELETE ON dev_news_llm_analysis TO postgres;
GRANT USAGE ON SEQUENCE dev_news_llm_analysis_id_seq TO postgres;

-- Create function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_news_llm_analysis_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update the updated_at field
CREATE TRIGGER trigger_update_news_llm_analysis_updated_at
    BEFORE UPDATE ON dev_news_llm_analysis
    FOR EACH ROW
    EXECUTE FUNCTION update_news_llm_analysis_updated_at();

-- Create view for recent high-quality analysis
CREATE VIEW dev_recent_news_llm_analysis AS
SELECT 
    id,
    news_id,
    news_source,
    event_types,
    sentiment_ensemble,
    sentiment_confidence,
    predicted_price_impact_1d,
    impact_confidence,
    data_quality_score,
    processing_timestamp
FROM dev_news_llm_analysis 
WHERE processing_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
    AND data_quality_score >= 0.7
    AND analysis_completeness >= 0.8
ORDER BY processing_timestamp DESC, sentiment_confidence DESC;

GRANT SELECT ON dev_recent_news_llm_analysis TO postgres;