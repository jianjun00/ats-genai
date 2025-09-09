-- Migration: Create Price Gap Events Schema
-- Description: Comprehensive database schema for tracking price gaps between sessions,
--              gap fills, and gap-related events with full Protocol Buffer support

-- =============================================
-- ENUMS AND TYPES
-- =============================================

-- Gap direction type
DO $$ BEGIN
    CREATE TYPE gap_direction AS ENUM ('gap_up', 'gap_down');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Gap size classification type  
DO $$ BEGIN
    CREATE TYPE gap_size_class AS ENUM (
        'micro',        -- 0.2% - 0.5% (normal market noise)
        'small',        -- 0.5% - 1.0% (minor news/sentiment)
        'medium',       -- 1.0% - 2.5% (significant news/events)
        'large',        -- 2.5% - 5.0% (major news/earnings)
        'extreme'       -- >5.0% (major fundamental events)
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Gap context classification
DO $$ BEGIN
    CREATE TYPE gap_context AS ENUM (
        'earnings',     -- Around earnings announcement dates
        'news',         -- Response to fundamental news
        'market',       -- Broad market movement gaps
        'continuation', -- In direction of existing trend
        'reversal'      -- Counter to existing trend
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Gap fill type
DO $$ BEGIN
    CREATE TYPE gap_fill_type AS ENUM (
        'full',         -- 100% gap fill (price returned to gap level)
        'partial',      -- 50-99% retracement to gap level
        'none'          -- Gap unfilled within tracking period
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- =============================================
-- CORE TABLES
-- =============================================

-- Price Gap Events Master Table
CREATE TABLE IF NOT EXISTS gap_events (
    -- Primary identification
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(150) UNIQUE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    
    -- Gap timing
    gap_date DATE NOT NULL,
    gap_datetime TIMESTAMPTZ NOT NULL,
    
    -- Gap metrics
    gap_points DECIMAL(10,4) NOT NULL,
    gap_percentage DECIMAL(8,4) NOT NULL,
    gap_size_class gap_size_class NOT NULL,
    direction gap_direction NOT NULL,
    
    -- Market context
    prev_close DECIMAL(10,4) NOT NULL,
    open_price DECIMAL(10,4) NOT NULL,
    volume BIGINT NOT NULL,
    avg_volume BIGINT,
    volume_confirmed BOOLEAN DEFAULT FALSE,
    significance_score DECIMAL(8,4) NOT NULL DEFAULT 0.0,
    
    -- Gap classification
    gap_context gap_context NOT NULL DEFAULT 'market',
    
    -- Gap fill tracking
    fill_date DATE,
    days_to_fill INTEGER,
    fill_percentage DECIMAL(6,2),
    fill_type gap_fill_type,
    
    -- Protocol Buffer event data
    event_data BYTEA,
    
    -- Status and processing
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    processing_notes TEXT,
    
    -- Audit trail
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT chk_gap_percentage_valid CHECK (abs(gap_percentage) >= 0.2),
    CONSTRAINT chk_significance_positive CHECK (significance_score >= 0.0),
    CONSTRAINT chk_fill_percentage_valid CHECK (fill_percentage IS NULL OR (fill_percentage >= 0.0 AND fill_percentage <= 100.0)),
    CONSTRAINT chk_days_to_fill_positive CHECK (days_to_fill IS NULL OR days_to_fill >= 0)
);

-- Indexes for gap_events table
CREATE INDEX IF NOT EXISTS idx_gap_events_symbol ON gap_events(symbol);
CREATE INDEX IF NOT EXISTS idx_gap_events_date ON gap_events(gap_date DESC);
CREATE INDEX IF NOT EXISTS idx_gap_events_datetime ON gap_events(gap_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_gap_events_direction ON gap_events(direction);
CREATE INDEX IF NOT EXISTS idx_gap_events_size_class ON gap_events(gap_size_class);
CREATE INDEX IF NOT EXISTS idx_gap_events_significance ON gap_events(significance_score DESC);
CREATE INDEX IF NOT EXISTS idx_gap_events_fill_type ON gap_events(fill_type);
CREATE INDEX IF NOT EXISTS idx_gap_events_context ON gap_events(gap_context);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_gap_events_symbol_date ON gap_events(symbol, gap_date DESC);
CREATE INDEX IF NOT EXISTS idx_gap_events_symbol_direction ON gap_events(symbol, direction, gap_date DESC);
CREATE INDEX IF NOT EXISTS idx_gap_events_size_significance ON gap_events(gap_size_class, significance_score DESC);
CREATE INDEX IF NOT EXISTS idx_gap_events_fill_tracking ON gap_events(fill_type, days_to_fill) WHERE fill_type IS NOT NULL;

-- =============================================
-- ANALYTICAL VIEWS
-- =============================================

-- Gap Analytics by Symbol
CREATE OR REPLACE VIEW vw_gap_analytics AS
SELECT 
    symbol,
    
    -- Gap counts by direction
    COUNT(*) as total_gaps,
    COUNT(CASE WHEN direction = 'gap_up' THEN 1 END) as gap_ups,
    COUNT(CASE WHEN direction = 'gap_down' THEN 1 END) as gap_downs,
    
    -- Gap size distribution
    COUNT(CASE WHEN gap_size_class = 'extreme' THEN 1 END) as extreme_gaps,
    COUNT(CASE WHEN gap_size_class = 'large' THEN 1 END) as large_gaps,
    COUNT(CASE WHEN gap_size_class = 'medium' THEN 1 END) as medium_gaps,
    COUNT(CASE WHEN gap_size_class = 'small' THEN 1 END) as small_gaps,
    COUNT(CASE WHEN gap_size_class = 'micro' THEN 1 END) as micro_gaps,
    
    -- Gap magnitude statistics
    ROUND(AVG(abs(gap_percentage)), 3) as avg_gap_pct,
    ROUND(MAX(abs(gap_percentage)), 3) as max_gap_pct,
    ROUND(MIN(abs(gap_percentage)), 3) as min_gap_pct,
    ROUND(AVG(significance_score), 3) as avg_significance,
    
    -- Fill statistics
    COUNT(CASE WHEN fill_type = 'full' THEN 1 END) as full_fills,
    COUNT(CASE WHEN fill_type = 'partial' THEN 1 END) as partial_fills,
    COUNT(CASE WHEN fill_type = 'none' THEN 1 END) as unfilled,
    COUNT(CASE WHEN fill_type IS NULL THEN 1 END) as pending_fill_analysis,
    
    -- Fill rate percentage
    CASE 
        WHEN COUNT(CASE WHEN fill_type IS NOT NULL THEN 1 END) > 0 THEN
            ROUND(
                COUNT(CASE WHEN fill_type IN ('full', 'partial') THEN 1 END)::DECIMAL / 
                COUNT(CASE WHEN fill_type IS NOT NULL THEN 1 END)::DECIMAL * 100, 1
            )
        ELSE 0.0
    END as fill_rate_pct,
    
    -- Average days to fill
    ROUND(AVG(days_to_fill), 1) as avg_days_to_fill,
    
    -- Context distribution
    COUNT(CASE WHEN gap_context = 'earnings' THEN 1 END) as earnings_gaps,
    COUNT(CASE WHEN gap_context = 'news' THEN 1 END) as news_gaps,
    COUNT(CASE WHEN gap_context = 'market' THEN 1 END) as market_gaps,
    
    -- Time range
    MIN(gap_date) as earliest_gap,
    MAX(gap_date) as latest_gap,
    
    -- Gap frequency (gaps per month)
    CASE 
        WHEN MIN(gap_date) != MAX(gap_date) THEN
            ROUND(
                COUNT(*)::DECIMAL / 
                GREATEST(
                    EXTRACT(DAYS FROM MAX(gap_date) - MIN(gap_date))::DECIMAL / 30.0,
                    1.0
                ), 2
            )
        ELSE 0.0
    END as gaps_per_month

FROM gap_events
GROUP BY symbol;

-- Recent Gap Events Summary
CREATE OR REPLACE VIEW vw_recent_gap_events AS
SELECT 
    symbol,
    gap_date,
    gap_datetime,
    direction,
    gap_percentage,
    gap_size_class,
    significance_score,
    prev_close,
    open_price,
    volume_confirmed,
    gap_context,
    fill_type,
    days_to_fill,
    fill_percentage,
    created_at
    
FROM gap_events
WHERE gap_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY gap_datetime DESC;

-- Gap Fill Performance Analysis
CREATE OR REPLACE VIEW vw_gap_fill_performance AS
SELECT 
    gap_size_class,
    direction,
    
    -- Fill statistics by gap size and direction
    COUNT(*) as total_gaps,
    COUNT(CASE WHEN fill_type = 'full' THEN 1 END) as full_fills,
    COUNT(CASE WHEN fill_type = 'partial' THEN 1 END) as partial_fills,
    COUNT(CASE WHEN fill_type = 'none' THEN 1 END) as unfilled,
    
    -- Fill rates
    CASE 
        WHEN COUNT(CASE WHEN fill_type IS NOT NULL THEN 1 END) > 0 THEN
            ROUND(
                COUNT(CASE WHEN fill_type = 'full' THEN 1 END)::DECIMAL / 
                COUNT(CASE WHEN fill_type IS NOT NULL THEN 1 END)::DECIMAL * 100, 1
            )
        ELSE 0.0
    END as full_fill_rate_pct,
    
    CASE 
        WHEN COUNT(CASE WHEN fill_type IS NOT NULL THEN 1 END) > 0 THEN
            ROUND(
                COUNT(CASE WHEN fill_type IN ('full', 'partial') THEN 1 END)::DECIMAL / 
                COUNT(CASE WHEN fill_type IS NOT NULL THEN 1 END)::DECIMAL * 100, 1
            )
        ELSE 0.0
    END as any_fill_rate_pct,
    
    -- Timing statistics
    ROUND(AVG(days_to_fill), 1) as avg_days_to_fill,
    MIN(days_to_fill) as min_days_to_fill,
    MAX(days_to_fill) as max_days_to_fill,
    
    -- Average gap size for this category
    ROUND(AVG(abs(gap_percentage)), 2) as avg_gap_size_pct

FROM gap_events
WHERE fill_type IS NOT NULL
GROUP BY gap_size_class, direction
ORDER BY 
    CASE gap_size_class 
        WHEN 'extreme' THEN 1
        WHEN 'large' THEN 2
        WHEN 'medium' THEN 3
        WHEN 'small' THEN 4
        WHEN 'micro' THEN 5
    END,
    direction;

-- =============================================
-- TRIGGERS AND FUNCTIONS
-- =============================================

-- Update timestamps automatically
CREATE OR REPLACE FUNCTION update_gap_timestamps()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply timestamp trigger to gap_events table
DROP TRIGGER IF EXISTS trigger_gap_events_updated_at ON gap_events;
CREATE TRIGGER trigger_gap_events_updated_at
    BEFORE UPDATE ON gap_events
    FOR EACH ROW
    EXECUTE FUNCTION update_gap_timestamps();

-- =============================================
-- HELPER FUNCTIONS
-- =============================================

-- Function to get gaps for symbol within date range
CREATE OR REPLACE FUNCTION get_symbol_gaps(
    p_symbol VARCHAR(20),
    p_start_date DATE DEFAULT CURRENT_DATE - INTERVAL '30 days',
    p_end_date DATE DEFAULT CURRENT_DATE,
    p_min_size_pct DECIMAL(8,4) DEFAULT 0.5
)
RETURNS TABLE (
    gap_date DATE,
    gap_percentage DECIMAL(8,4),
    direction gap_direction,
    gap_size_class gap_size_class,
    significance_score DECIMAL(8,4),
    fill_type gap_fill_type,
    days_to_fill INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        g.gap_date,
        g.gap_percentage,
        g.direction,
        g.gap_size_class,
        g.significance_score,
        g.fill_type,
        g.days_to_fill
    FROM gap_events g
    WHERE g.symbol = p_symbol
      AND g.gap_date BETWEEN p_start_date AND p_end_date
      AND abs(g.gap_percentage) >= p_min_size_pct
    ORDER BY g.gap_date DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to get unfilled gaps for monitoring
CREATE OR REPLACE FUNCTION get_unfilled_gaps(
    p_symbol VARCHAR(20) DEFAULT NULL,
    p_days_old INTEGER DEFAULT 10,
    p_min_size_pct DECIMAL(8,4) DEFAULT 1.0
)
RETURNS TABLE (
    symbol VARCHAR(20),
    gap_date DATE,
    gap_percentage DECIMAL(8,4),
    direction gap_direction,
    gap_size_class gap_size_class,
    days_since_gap INTEGER,
    prev_close DECIMAL(10,4),
    open_price DECIMAL(10,4)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        g.symbol,
        g.gap_date,
        g.gap_percentage,
        g.direction,
        g.gap_size_class,
        EXTRACT(DAYS FROM CURRENT_DATE - g.gap_date)::INTEGER as days_since_gap,
        g.prev_close,
        g.open_price
    FROM gap_events g
    WHERE (p_symbol IS NULL OR g.symbol = p_symbol)
      AND g.gap_date <= CURRENT_DATE - INTERVAL '1 day' * p_days_old
      AND g.fill_type IS NULL
      AND abs(g.gap_percentage) >= p_min_size_pct
    ORDER BY g.gap_date DESC, abs(g.gap_percentage) DESC;
END;
$$ LANGUAGE plpgsql;

-- =============================================
-- TABLE COMMENTS AND DOCUMENTATION
-- =============================================

COMMENT ON TABLE gap_events IS 'Price gap events tracking opening price discontinuities with Protocol Buffer support';

COMMENT ON COLUMN gap_events.gap_points IS 'Gap size in price points (open - prev_close)';
COMMENT ON COLUMN gap_events.gap_percentage IS 'Gap size as percentage of previous close';
COMMENT ON COLUMN gap_events.significance_score IS 'Gap significance score combining size and volume factors';
COMMENT ON COLUMN gap_events.volume_confirmed IS 'Whether gap occurred with above-average volume';
COMMENT ON COLUMN gap_events.event_data IS 'Serialized Protocol Buffer event data';
COMMENT ON COLUMN gap_events.fill_percentage IS 'Percentage of gap that was filled (0-100)';

-- Performance indexes for large datasets
CREATE INDEX IF NOT EXISTS idx_gap_events_large_gaps ON gap_events(symbol, gap_date DESC) 
    WHERE gap_size_class IN ('large', 'extreme');
CREATE INDEX IF NOT EXISTS idx_gap_events_unfilled ON gap_events(symbol, gap_date DESC) 
    WHERE fill_type IS NULL AND gap_size_class IN ('medium', 'large', 'extreme');

SELECT 'Gap events schema created successfully' as result;