-- Migration: Create Support/Resistance Event System Schema
-- Description: Comprehensive database schema for tracking support/resistance levels,
--              tests, and events across multiple timeframes with full audit trail

-- =============================================
-- ENUMS AND TYPES
-- =============================================

-- Support/Resistance type
DO $$ BEGIN
    CREATE TYPE sr_type AS ENUM ('support', 'resistance');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Level classification type
DO $$ BEGIN
    CREATE TYPE sr_level_type AS ENUM (
        'pivot_point',      -- Based on swing highs/lows
        'psychological',    -- Round numbers, major levels  
        'volume_profile',   -- High volume concentration
        'historical',       -- Previous significant levels
        'dynamic',          -- Moving averages, trendlines
        'confluence'        -- Multiple factors converge
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Test outcome classification
DO $$ BEGIN
    CREATE TYPE sr_test_outcome AS ENUM (
        'hold_strong',      -- Bounced with conviction
        'hold_weak',        -- Bounced but weakly
        'break_clean',      -- Clean break through
        'break_false',      -- Brief break then return
        'penetration',      -- Minor penetration but held
        'pending'           -- Test in progress
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Analysis timeframe
DO $$ BEGIN
    CREATE TYPE sr_timeframe AS ENUM (
        '1m', '5m', '15m', '1h',    -- Intraday
        '1d', '1w', '1M',           -- Daily, Weekly, Monthly
        '3M', '1Y'                  -- Quarterly, Yearly
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- =============================================
-- CORE TABLES
-- =============================================

-- Support/Resistance Levels Master Table
CREATE TABLE IF NOT EXISTS dev_sr_levels (
    -- Primary identification
    id BIGSERIAL PRIMARY KEY,
    level_id VARCHAR(100) UNIQUE NOT NULL, -- Composite ID for deduplication
    symbol VARCHAR(20) NOT NULL,
    
    -- Level characteristics  
    price DECIMAL(15,6) NOT NULL,
    sr_type sr_type NOT NULL,
    level_type sr_level_type NOT NULL,
    timeframe sr_timeframe NOT NULL,
    
    -- Strength and confidence metrics
    strength DECIMAL(4,3) NOT NULL CHECK (strength >= 0.0 AND strength <= 1.0),
    confidence DECIMAL(4,3) NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    
    -- Historical tracking
    first_established TIMESTAMPTZ NOT NULL,
    last_tested TIMESTAMPTZ NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Test statistics
    test_count INTEGER NOT NULL DEFAULT 1,
    hold_count INTEGER NOT NULL DEFAULT 0,  
    break_count INTEGER NOT NULL DEFAULT 0,
    
    -- Volume confirmation
    volume_confirmation BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Status and lifecycle
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    deactivated_at TIMESTAMPTZ NULL,
    deactivation_reason VARCHAR(100) NULL,
    
    -- Rich metadata storage
    metadata JSONB NOT NULL DEFAULT '{}',
    
    -- Audit trail
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for dev_sr_levels
CREATE INDEX IF NOT EXISTS idx_sr_levels_symbol ON dev_sr_levels(symbol);
CREATE INDEX IF NOT EXISTS idx_sr_levels_price ON dev_sr_levels(price);
CREATE INDEX IF NOT EXISTS idx_sr_levels_type ON dev_sr_levels(sr_type, level_type);
CREATE INDEX IF NOT EXISTS idx_sr_levels_timeframe ON dev_sr_levels(timeframe);
CREATE INDEX IF NOT EXISTS idx_sr_levels_strength ON dev_sr_levels(strength DESC);
CREATE INDEX IF NOT EXISTS idx_sr_levels_active ON dev_sr_levels(is_active, symbol, timeframe);
CREATE INDEX IF NOT EXISTS idx_sr_levels_last_tested ON dev_sr_levels(last_tested DESC);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_sr_levels_active_symbol_timeframe ON dev_sr_levels(symbol, timeframe, is_active, strength DESC);
CREATE INDEX IF NOT EXISTS idx_sr_levels_price_range ON dev_sr_levels(symbol, timeframe, price, is_active);

-- JSON indexes for metadata queries
CREATE INDEX IF NOT EXISTS idx_sr_levels_metadata_gin ON dev_sr_levels USING GIN(metadata);

-- Support/Resistance Level Tests
CREATE TABLE IF NOT EXISTS dev_sr_tests (
    -- Primary identification
    id BIGSERIAL PRIMARY KEY,
    test_id VARCHAR(150) UNIQUE NOT NULL, -- Composite ID for deduplication
    level_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    
    -- Foreign key relationship
    sr_level_id BIGINT NOT NULL REFERENCES dev_sr_levels(id) ON DELETE CASCADE,
    
    -- Test characteristics
    test_datetime TIMESTAMPTZ NOT NULL,
    test_price DECIMAL(15,6) NOT NULL,
    approach_direction VARCHAR(20) NOT NULL CHECK (approach_direction IN ('from_above', 'from_below', 'unknown')),
    timeframe sr_timeframe NOT NULL,
    
    -- Test measurements
    max_penetration DECIMAL(8,6) NOT NULL DEFAULT 0.0, -- Percentage penetration
    hold_duration INTERVAL NOT NULL DEFAULT '0 minutes',
    volume_spike DECIMAL(6,2) NOT NULL DEFAULT 1.0, -- Multiple of average volume
    
    -- Test outcome and confidence
    outcome sr_test_outcome NOT NULL DEFAULT 'pending',
    outcome_confidence DECIMAL(4,3) NOT NULL DEFAULT 0.5,
    
    -- Analysis metadata
    metadata JSONB NOT NULL DEFAULT '{}',
    
    -- Audit trail  
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for dev_sr_tests
CREATE INDEX IF NOT EXISTS idx_sr_tests_level_id ON dev_sr_tests(sr_level_id);
CREATE INDEX IF NOT EXISTS idx_sr_tests_symbol ON dev_sr_tests(symbol);
CREATE INDEX IF NOT EXISTS idx_sr_tests_datetime ON dev_sr_tests(test_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_sr_tests_outcome ON dev_sr_tests(outcome);
CREATE INDEX IF NOT EXISTS idx_sr_tests_timeframe ON dev_sr_tests(timeframe);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_sr_tests_level_outcome ON dev_sr_tests(sr_level_id, outcome, test_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_sr_tests_symbol_timeframe ON dev_sr_tests(symbol, timeframe, test_datetime DESC);

-- JSON indexes
CREATE INDEX IF NOT EXISTS idx_sr_tests_metadata_gin ON dev_sr_tests USING GIN(metadata);

-- Support/Resistance Events (Integration with main events system)
CREATE TABLE IF NOT EXISTS dev_sr_events (
    -- Primary identification
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(150) UNIQUE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    
    -- Links to S/R system
    sr_level_id BIGINT NOT NULL REFERENCES dev_sr_levels(id) ON DELETE CASCADE,
    sr_test_id BIGINT NOT NULL REFERENCES dev_sr_tests(id) ON DELETE CASCADE,
    
    -- Event classification
    event_type VARCHAR(50) NOT NULL DEFAULT 'support_resistance',
    event_subtype VARCHAR(50) NOT NULL, -- 'level_established', 'level_tested', 'level_broken', etc.
    
    -- Timing information
    event_datetime TIMESTAMPTZ NOT NULL,
    market_datetime TIMESTAMPTZ NOT NULL,
    timeframe sr_timeframe NOT NULL,
    
    -- Event significance
    significance_score DECIMAL(4,3) NOT NULL DEFAULT 0.5,
    impact_score DECIMAL(4,3) NOT NULL DEFAULT 0.5,
    
    -- Market context
    price_at_event DECIMAL(15,6) NOT NULL,
    volume_at_event BIGINT NULL,
    market_condition VARCHAR(50) NULL, -- 'trending_up', 'trending_down', 'consolidating'
    
    -- Rich event data
    event_data JSONB NOT NULL DEFAULT '{}',
    
    -- Integration with financial events system  
    financial_event_id BIGINT NULL,
    
    -- Status and processing
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    processing_notes TEXT NULL,
    
    -- Audit trail
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for dev_sr_events
CREATE INDEX IF NOT EXISTS idx_sr_events_symbol ON dev_sr_events(symbol);
CREATE INDEX IF NOT EXISTS idx_sr_events_datetime ON dev_sr_events(event_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_sr_events_type ON dev_sr_events(event_type, event_subtype);
CREATE INDEX IF NOT EXISTS idx_sr_events_timeframe ON dev_sr_events(timeframe);
CREATE INDEX IF NOT EXISTS idx_sr_events_significance ON dev_sr_events(significance_score DESC);
CREATE INDEX IF NOT EXISTS idx_sr_events_level_id ON dev_sr_events(sr_level_id);
CREATE INDEX IF NOT EXISTS idx_sr_events_test_id ON dev_sr_events(sr_test_id);

-- Composite indexes for common analytical queries
CREATE INDEX IF NOT EXISTS idx_sr_events_symbol_timeframe_date ON dev_sr_events(symbol, timeframe, event_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_sr_events_significance_date ON dev_sr_events(significance_score DESC, event_datetime DESC);

-- JSON indexes
CREATE INDEX IF NOT EXISTS idx_sr_events_data_gin ON dev_sr_events USING GIN(event_data);

-- =============================================
-- ANALYTICAL VIEWS  
-- =============================================

-- Active S/R Levels with Latest Test Information
CREATE OR REPLACE VIEW vw_active_sr_levels AS
SELECT 
    l.id,
    l.level_id,
    l.symbol,
    l.price,
    l.sr_type,
    l.level_type,
    l.timeframe,
    l.strength,
    l.confidence,
    l.first_established,
    l.last_tested,
    l.test_count,
    l.hold_count,
    l.break_count,
    l.volume_confirmation,
    
    -- Latest test information
    lt.test_datetime as latest_test_datetime,
    lt.outcome as latest_test_outcome,
    lt.outcome_confidence as latest_test_confidence,
    
    -- Success metrics
    CASE 
        WHEN l.test_count > 0 THEN 
            ROUND(l.hold_count::DECIMAL / l.test_count::DECIMAL, 3)
        ELSE 0.0 
    END as success_rate,
    
    -- Age metrics
    EXTRACT(DAYS FROM NOW() - l.last_tested) as days_since_last_test,
    EXTRACT(DAYS FROM NOW() - l.first_established) as level_age_days,
    
    l.metadata,
    l.created_at,
    l.updated_at
    
FROM dev_sr_levels l
LEFT JOIN LATERAL (
    SELECT test_datetime, outcome, outcome_confidence
    FROM dev_sr_tests t 
    WHERE t.sr_level_id = l.id 
    ORDER BY t.test_datetime DESC 
    LIMIT 1
) lt ON true
WHERE l.is_active = true;

-- S/R Level Performance Analytics
CREATE OR REPLACE VIEW vw_sr_level_analytics AS
SELECT 
    l.symbol,
    l.timeframe,
    l.sr_type,
    l.level_type,
    
    -- Level counts
    COUNT(*) as total_levels,
    COUNT(CASE WHEN l.is_active THEN 1 END) as active_levels,
    
    -- Strength distribution
    ROUND(AVG(l.strength), 3) as avg_strength,
    ROUND(MIN(l.strength), 3) as min_strength,
    ROUND(MAX(l.strength), 3) as max_strength,
    
    -- Test statistics
    SUM(l.test_count) as total_tests,
    SUM(l.hold_count) as total_holds,
    SUM(l.break_count) as total_breaks,
    
    -- Success rates
    CASE 
        WHEN SUM(l.test_count) > 0 THEN 
            ROUND(SUM(l.hold_count)::DECIMAL / SUM(l.test_count)::DECIMAL, 3)
        ELSE 0.0 
    END as overall_success_rate,
    
    -- Timing analytics
    MIN(l.first_established) as earliest_level,
    MAX(l.last_tested) as latest_test,
    ROUND(AVG(EXTRACT(DAYS FROM NOW() - l.first_established)), 1) as avg_level_age_days
    
FROM dev_sr_levels l
GROUP BY l.symbol, l.timeframe, l.sr_type, l.level_type;

-- Recent S/R Events Summary
CREATE OR REPLACE VIEW vw_recent_sr_events AS
SELECT 
    e.symbol,
    e.event_datetime,
    e.event_subtype,
    e.timeframe,
    e.significance_score,
    e.price_at_event,
    
    -- Level information
    l.price as level_price,
    l.sr_type,
    l.level_type,
    l.strength as level_strength,
    
    -- Test information  
    t.outcome as test_outcome,
    t.max_penetration,
    t.volume_spike,
    
    e.event_data,
    e.created_at
    
FROM dev_sr_events e
JOIN dev_sr_levels l ON e.sr_level_id = l.id
JOIN dev_sr_tests t ON e.sr_test_id = t.id
WHERE e.event_datetime >= NOW() - INTERVAL '7 days'
ORDER BY e.event_datetime DESC;

-- =============================================
-- TRIGGERS AND FUNCTIONS
-- =============================================

-- Update timestamps automatically
CREATE OR REPLACE FUNCTION update_sr_timestamps()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply timestamp trigger to all S/R tables
DROP TRIGGER IF EXISTS trigger_sr_levels_updated_at ON dev_sr_levels;
CREATE TRIGGER trigger_sr_levels_updated_at
    BEFORE UPDATE ON dev_sr_levels
    FOR EACH ROW
    EXECUTE FUNCTION update_sr_timestamps();

DROP TRIGGER IF EXISTS trigger_sr_tests_updated_at ON dev_sr_tests;  
CREATE TRIGGER trigger_sr_tests_updated_at
    BEFORE UPDATE ON dev_sr_tests
    FOR EACH ROW
    EXECUTE FUNCTION update_sr_timestamps();

DROP TRIGGER IF EXISTS trigger_sr_events_updated_at ON dev_sr_events;
CREATE TRIGGER trigger_sr_events_updated_at
    BEFORE UPDATE ON dev_sr_events
    FOR EACH ROW
    EXECUTE FUNCTION update_sr_timestamps();

-- Function to update level statistics when tests are added/updated
CREATE OR REPLACE FUNCTION update_sr_level_stats()
RETURNS TRIGGER AS $$
BEGIN
    -- Update the associated level's test statistics
    IF TG_OP = 'INSERT' THEN
        UPDATE dev_sr_levels 
        SET 
            test_count = test_count + 1,
            hold_count = hold_count + CASE WHEN NEW.outcome IN ('hold_strong', 'hold_weak', 'penetration') THEN 1 ELSE 0 END,
            break_count = break_count + CASE WHEN NEW.outcome IN ('break_clean', 'break_false') THEN 1 ELSE 0 END,
            last_tested = NEW.test_datetime,
            updated_at = NOW()
        WHERE id = NEW.sr_level_id;
        
    ELSIF TG_OP = 'UPDATE' AND OLD.outcome != NEW.outcome THEN
        -- Recalculate stats when outcome changes
        UPDATE dev_sr_levels 
        SET 
            hold_count = (
                SELECT COUNT(*) 
                FROM dev_sr_tests 
                WHERE sr_level_id = NEW.sr_level_id 
                AND outcome IN ('hold_strong', 'hold_weak', 'penetration')
            ),
            break_count = (
                SELECT COUNT(*)
                FROM dev_sr_tests 
                WHERE sr_level_id = NEW.sr_level_id 
                AND outcome IN ('break_clean', 'break_false')
            ),
            updated_at = NOW()
        WHERE id = NEW.sr_level_id;
    END IF;
    
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Apply stats update trigger
DROP TRIGGER IF EXISTS trigger_update_sr_level_stats ON dev_sr_tests;
CREATE TRIGGER trigger_update_sr_level_stats
    AFTER INSERT OR UPDATE OF outcome ON dev_sr_tests
    FOR EACH ROW
    EXECUTE FUNCTION update_sr_level_stats();

-- =============================================
-- HELPER FUNCTIONS
-- =============================================

-- Function to get active levels within price range
CREATE OR REPLACE FUNCTION get_sr_levels_in_range(
    p_symbol VARCHAR(20),
    p_timeframe sr_timeframe,
    p_min_price DECIMAL(15,6),
    p_max_price DECIMAL(15,6),
    p_min_strength DECIMAL(4,3) DEFAULT 0.3
)
RETURNS TABLE (
    level_id VARCHAR(100),
    price DECIMAL(15,6),
    sr_type sr_type,
    level_type sr_level_type,
    strength DECIMAL(4,3),
    test_count INTEGER,
    success_rate DECIMAL(4,3)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        l.level_id,
        l.price,
        l.sr_type,
        l.level_type, 
        l.strength,
        l.test_count,
        CASE 
            WHEN l.test_count > 0 THEN 
                ROUND(l.hold_count::DECIMAL / l.test_count::DECIMAL, 3)
            ELSE 0.0 
        END as success_rate
    FROM dev_sr_levels l
    WHERE l.symbol = p_symbol
      AND l.timeframe = p_timeframe
      AND l.is_active = true
      AND l.price BETWEEN p_min_price AND p_max_price
      AND l.strength >= p_min_strength
    ORDER BY l.strength DESC, l.test_count DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to get recent tests for a level
CREATE OR REPLACE FUNCTION get_recent_sr_tests(
    p_level_id VARCHAR(100),
    p_days INTEGER DEFAULT 30
)
RETURNS TABLE (
    test_datetime TIMESTAMPTZ,
    test_price DECIMAL(15,6),
    outcome sr_test_outcome,
    outcome_confidence DECIMAL(4,3),
    max_penetration DECIMAL(8,6),
    volume_spike DECIMAL(6,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.test_datetime,
        t.test_price,
        t.outcome,
        t.outcome_confidence,
        t.max_penetration,
        t.volume_spike
    FROM dev_sr_tests t
    WHERE t.level_id = p_level_id
      AND t.test_datetime >= NOW() - (p_days || ' days')::INTERVAL
    ORDER BY t.test_datetime DESC;
END;
$$ LANGUAGE plpgsql;

-- =============================================
-- INITIAL DATA AND COMMENTS
-- =============================================

-- Add table comments for documentation
COMMENT ON TABLE dev_sr_levels IS 'Master table for support/resistance levels with strength scoring and test statistics';
COMMENT ON TABLE dev_sr_tests IS 'Individual tests of S/R levels with outcome classification and measurements';
COMMENT ON TABLE dev_sr_events IS 'S/R events for integration with main event system and alerting';

COMMENT ON COLUMN dev_sr_levels.strength IS 'Level strength score (0.0-1.0) based on test history and confluence';
COMMENT ON COLUMN dev_sr_levels.confidence IS 'Statistical confidence in level validity (0.0-1.0)';
COMMENT ON COLUMN dev_sr_tests.max_penetration IS 'Maximum penetration beyond level as percentage of price';
COMMENT ON COLUMN dev_sr_tests.volume_spike IS 'Volume during test as multiple of average volume';

-- Create initial indexes for performance
CREATE INDEX IF NOT EXISTS idx_sr_levels_symbol_price_active ON dev_sr_levels(symbol, price, is_active) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_sr_tests_recent ON dev_sr_tests(test_datetime DESC, sr_level_id) WHERE test_datetime >= NOW() - INTERVAL '90 days';

-- Grant appropriate permissions (adjust as needed for your environment)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON dev_sr_levels TO ats_app_role;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON dev_sr_tests TO ats_app_role;  
-- GRANT SELECT, INSERT, UPDATE, DELETE ON dev_sr_events TO ats_app_role;
-- GRANT USAGE ON SEQUENCE dev_sr_levels_id_seq TO ats_app_role;
-- GRANT USAGE ON SEQUENCE dev_sr_tests_id_seq TO ats_app_role;
-- GRANT USAGE ON SEQUENCE dev_sr_events_id_seq TO ats_app_role;

SELECT 'Support/Resistance schema created successfully' as result;