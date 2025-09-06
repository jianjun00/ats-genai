-- Migration 065: Create real-time news ingestion table
-- This migration creates the table for storing real-time news articles with processing metadata

CREATE TABLE dev_news_realtime (
    id BIGSERIAL PRIMARY KEY,
    
    -- Article Identity
    article_id VARCHAR(255) NOT NULL,
    vendor VARCHAR(50) NOT NULL CHECK (vendor IN ('polygon', 'tiingo', 'alpha_vantage', 'fmp', 'benzinga', 'finnhub')),
    
    -- Content
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    summary TEXT,
    url TEXT,
    author VARCHAR(255),
    
    -- Timing
    published_date TIMESTAMP WITH TIME ZONE NOT NULL,
    discovered_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Classification
    urgency VARCHAR(20) NOT NULL DEFAULT 'low' CHECK (urgency IN ('critical', 'high', 'medium', 'low')),
    market_session VARCHAR(20) NOT NULL CHECK (market_session IN ('pre_market', 'market_hours', 'after_hours', 'closed')),
    
    -- Related Assets
    tickers TEXT[] DEFAULT '{}',
    keywords TEXT[] DEFAULT '{}',
    
    -- Processing Metadata
    processing_latency_ms INTEGER DEFAULT 0,
    content_hash VARCHAR(32),
    similarity_score DECIMAL(5,3) DEFAULT 0.0,
    
    -- LLM Processing Status
    llm_processing_triggered BOOLEAN DEFAULT FALSE,
    llm_processing_completed BOOLEAN DEFAULT FALSE,
    llm_analysis_id BIGINT REFERENCES dev_news_llm_analysis(id),
    signal_generated BOOLEAN DEFAULT FALSE,
    signal_ids BIGINT[],
    
    -- Quality Metrics
    duplicate_check_status VARCHAR(20) DEFAULT 'unique' CHECK (duplicate_check_status IN ('unique', 'duplicate_title', 'duplicate_content', 'duplicate_similarity')),
    quality_score DECIMAL(5,3) DEFAULT 1.0 CHECK (quality_score BETWEEN 0.0 AND 1.0),
    
    -- Raw Data
    raw_data JSONB NOT NULL,
    
    -- Standard Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Unique constraint on article per vendor
    UNIQUE(article_id, vendor)
);

-- Performance indexes for real-time news
CREATE INDEX idx_realtime_news_discovered_date ON dev_news_realtime(discovered_date DESC);
CREATE INDEX idx_realtime_news_published_date ON dev_news_realtime(published_date DESC);
CREATE INDEX idx_realtime_news_urgency ON dev_news_realtime(urgency, discovered_date DESC);
CREATE INDEX idx_realtime_news_market_session ON dev_news_realtime(market_session, discovered_date DESC);
CREATE INDEX idx_realtime_news_vendor ON dev_news_realtime(vendor, discovered_date DESC);

-- Composite indexes for common real-time queries
CREATE INDEX idx_realtime_news_urgency_session ON dev_news_realtime(urgency, market_session, discovered_date DESC);
CREATE INDEX idx_realtime_news_processing_status ON dev_news_realtime(llm_processing_triggered, signal_generated, discovered_date DESC);
CREATE INDEX idx_realtime_news_content_hash ON dev_news_realtime(content_hash) WHERE content_hash IS NOT NULL;

-- GIN indexes for array and JSONB fields
CREATE INDEX idx_realtime_news_tickers ON dev_news_realtime USING GIN(tickers);
CREATE INDEX idx_realtime_news_keywords ON dev_news_realtime USING GIN(keywords);
CREATE INDEX idx_realtime_news_raw_data ON dev_news_realtime USING GIN(raw_data);
CREATE INDEX idx_realtime_news_signal_ids ON dev_news_realtime USING GIN(signal_ids);

-- Partial indexes for active processing
CREATE INDEX idx_realtime_news_pending_llm ON dev_news_realtime(discovered_date DESC) 
    WHERE llm_processing_triggered = FALSE AND urgency IN ('critical', 'high');
CREATE INDEX idx_realtime_news_llm_processing ON dev_news_realtime(discovered_date DESC) 
    WHERE llm_processing_triggered = TRUE AND llm_processing_completed = FALSE;

-- Comments for documentation
COMMENT ON TABLE dev_news_realtime IS 'Real-time news articles with processing status and metadata';
COMMENT ON COLUMN dev_news_realtime.urgency IS 'News urgency: critical (earnings/M&A), high (ratings), medium, low';
COMMENT ON COLUMN dev_news_realtime.market_session IS 'Market session when news was discovered';
COMMENT ON COLUMN dev_news_realtime.processing_latency_ms IS 'Time taken to process article from discovery to database storage';
COMMENT ON COLUMN dev_news_realtime.content_hash IS 'Hash of normalized content for deduplication';
COMMENT ON COLUMN dev_news_realtime.similarity_score IS 'Highest similarity score with recent articles (0.0-1.0)';
COMMENT ON COLUMN dev_news_realtime.duplicate_check_status IS 'Result of deduplication analysis';
COMMENT ON COLUMN dev_news_realtime.quality_score IS 'Overall content quality score (0.0-1.0)';

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON dev_news_realtime TO postgres;
GRANT USAGE ON SEQUENCE dev_news_realtime_id_seq TO postgres;

-- Create function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_realtime_news_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update the updated_at field
CREATE TRIGGER trigger_update_realtime_news_updated_at
    BEFORE UPDATE ON dev_news_realtime
    FOR EACH ROW
    EXECUTE FUNCTION update_realtime_news_updated_at();

-- Create view for high-priority recent articles
CREATE VIEW dev_realtime_news_priority AS
SELECT 
    id,
    article_id,
    vendor,
    title,
    LEFT(content, 200) as content_preview,
    url,
    published_date,
    discovered_date,
    urgency,
    market_session,
    tickers,
    keywords,
    processing_latency_ms,
    llm_processing_triggered,
    signal_generated,
    quality_score
FROM dev_news_realtime 
WHERE discovered_date >= CURRENT_TIMESTAMP - INTERVAL '2 hours'
    AND urgency IN ('critical', 'high')
    AND duplicate_check_status = 'unique'
ORDER BY urgency DESC, discovered_date DESC;

GRANT SELECT ON dev_realtime_news_priority TO postgres;

-- Create view for LLM processing queue
CREATE VIEW dev_realtime_news_llm_queue AS
SELECT 
    id,
    article_id,
    vendor,
    title,
    content,
    urgency,
    market_session,
    tickers,
    discovered_date,
    processing_latency_ms
FROM dev_news_realtime 
WHERE llm_processing_triggered = FALSE
    AND duplicate_check_status = 'unique'
    AND quality_score >= 0.7
    AND discovered_date >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
ORDER BY 
    CASE urgency 
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2  
        WHEN 'medium' THEN 3
        ELSE 4 
    END,
    discovered_date DESC;

GRANT SELECT ON dev_realtime_news_llm_queue TO postgres;

-- Create materialized view for real-time news analytics
CREATE MATERIALIZED VIEW dev_realtime_news_analytics AS
SELECT 
    -- Time buckets
    DATE_TRUNC('hour', discovered_date) as hour_bucket,
    DATE_TRUNC('day', discovered_date) as day_bucket,
    
    -- Processing metrics
    vendor,
    urgency,
    market_session,
    COUNT(*) as article_count,
    COUNT(CASE WHEN duplicate_check_status = 'unique' THEN 1 END) as unique_articles,
    COUNT(CASE WHEN llm_processing_triggered THEN 1 END) as llm_processed,
    COUNT(CASE WHEN signal_generated THEN 1 END) as signals_generated,
    
    -- Performance metrics
    AVG(processing_latency_ms) as avg_processing_latency,
    MAX(processing_latency_ms) as max_processing_latency,
    AVG(quality_score) as avg_quality_score,
    AVG(similarity_score) as avg_similarity_score,
    
    -- Content metrics
    COUNT(DISTINCT UNNEST(tickers)) as unique_tickers_mentioned,
    AVG(ARRAY_LENGTH(tickers, 1)) as avg_tickers_per_article,
    AVG(LENGTH(content)) as avg_content_length,
    
    -- Deduplication metrics
    COUNT(CASE WHEN duplicate_check_status = 'duplicate_content' THEN 1 END) as content_duplicates,
    COUNT(CASE WHEN duplicate_check_status = 'duplicate_similarity' THEN 1 END) as similarity_duplicates,
    
    -- Latest processing timestamp
    MAX(discovered_date) as latest_article_time
    
FROM dev_news_realtime
WHERE discovered_date >= CURRENT_TIMESTAMP - INTERVAL '7 days'
GROUP BY 
    DATE_TRUNC('hour', discovered_date),
    DATE_TRUNC('day', discovered_date),
    vendor,
    urgency,
    market_session
ORDER BY hour_bucket DESC;

-- Create unique index for materialized view
CREATE UNIQUE INDEX idx_realtime_news_analytics_unique 
ON dev_realtime_news_analytics(hour_bucket, vendor, urgency, market_session);

-- Grant permissions on materialized view
GRANT SELECT ON dev_realtime_news_analytics TO postgres;

-- Create function to refresh the materialized view
CREATE OR REPLACE FUNCTION refresh_realtime_news_analytics()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW dev_realtime_news_analytics;
END;
$$ LANGUAGE plpgsql;

-- Create view for vendor performance comparison
CREATE VIEW dev_realtime_news_vendor_performance AS
SELECT 
    vendor,
    COUNT(*) as total_articles,
    COUNT(CASE WHEN duplicate_check_status = 'unique' THEN 1 END) as unique_articles,
    ROUND(
        100.0 * COUNT(CASE WHEN duplicate_check_status = 'unique' THEN 1 END) / NULLIF(COUNT(*), 0), 
        2
    ) as uniqueness_rate_pct,
    
    -- Processing performance
    AVG(processing_latency_ms) as avg_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY processing_latency_ms) as p95_latency_ms,
    
    -- Content quality
    AVG(quality_score) as avg_quality_score,
    COUNT(CASE WHEN quality_score >= 0.8 THEN 1 END) as high_quality_articles,
    
    -- LLM processing results
    COUNT(CASE WHEN llm_processing_triggered THEN 1 END) as llm_processed,
    COUNT(CASE WHEN signal_generated THEN 1 END) as signals_generated,
    ROUND(
        100.0 * COUNT(CASE WHEN signal_generated THEN 1 END) / NULLIF(COUNT(CASE WHEN llm_processing_triggered THEN 1 END), 0),
        2
    ) as signal_generation_rate_pct,
    
    -- Urgency distribution
    COUNT(CASE WHEN urgency = 'critical' THEN 1 END) as critical_articles,
    COUNT(CASE WHEN urgency = 'high' THEN 1 END) as high_articles,
    COUNT(CASE WHEN urgency = 'medium' THEN 1 END) as medium_articles,
    COUNT(CASE WHEN urgency = 'low' THEN 1 END) as low_articles,
    
    -- Time coverage
    MIN(discovered_date) as first_article,
    MAX(discovered_date) as latest_article,
    COUNT(DISTINCT DATE(discovered_date)) as active_days
    
FROM dev_news_realtime 
WHERE discovered_date >= CURRENT_TIMESTAMP - INTERVAL '30 days'
GROUP BY vendor
ORDER BY total_articles DESC;

GRANT SELECT ON dev_realtime_news_vendor_performance TO postgres;

-- Create view for signal generation effectiveness
CREATE VIEW dev_realtime_news_signal_effectiveness AS
SELECT 
    urgency,
    market_session,
    COUNT(*) as total_articles,
    COUNT(CASE WHEN llm_processing_triggered THEN 1 END) as llm_processed,
    COUNT(CASE WHEN signal_generated THEN 1 END) as signals_generated,
    
    -- Effectiveness rates
    ROUND(
        100.0 * COUNT(CASE WHEN llm_processing_triggered THEN 1 END) / NULLIF(COUNT(*), 0),
        2
    ) as llm_trigger_rate_pct,
    ROUND(
        100.0 * COUNT(CASE WHEN signal_generated THEN 1 END) / NULLIF(COUNT(CASE WHEN llm_processing_triggered THEN 1 END), 0),
        2
    ) as signal_conversion_rate_pct,
    
    -- Performance metrics
    AVG(processing_latency_ms) as avg_processing_latency,
    AVG(quality_score) as avg_quality_score,
    
    -- Top tickers in this category
    (SELECT ARRAY_AGG(ticker ORDER BY ticker_count DESC) FROM (
        SELECT UNNEST(tickers) as ticker, COUNT(*) as ticker_count
        FROM dev_news_realtime n2 
        WHERE n2.urgency = dev_news_realtime.urgency 
            AND n2.market_session = dev_news_realtime.market_session
            AND n2.discovered_date >= CURRENT_TIMESTAMP - INTERVAL '7 days'
        GROUP BY UNNEST(tickers)
        ORDER BY ticker_count DESC
        LIMIT 5
    ) top_tickers) as top_tickers
    
FROM dev_news_realtime
WHERE discovered_date >= CURRENT_TIMESTAMP - INTERVAL '7 days'
    AND duplicate_check_status = 'unique'
GROUP BY urgency, market_session
ORDER BY 
    CASE urgency 
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'medium' THEN 3  
        ELSE 4
    END,
    signals_generated DESC;

GRANT SELECT ON dev_realtime_news_signal_effectiveness TO postgres;