-- Financial Events Database Schema
-- Professional-grade schema following Bloomberg/Refinitiv patterns
-- Created: 2025-08-27

-- ==========================================
-- ENUMS FOR TYPE SAFETY
-- ==========================================

-- Event Types
CREATE TYPE event_type_enum AS ENUM (
    'earnings', 
    'analyst_rating', 
    'corporate_action', 
    'announcement', 
    'economic', 
    'regulatory',
    'insider_trading',
    'sec_filing'
);

-- Sentiment Analysis
CREATE TYPE sentiment_enum AS ENUM ('positive', 'negative', 'neutral');

-- Importance Levels
CREATE TYPE importance_enum AS ENUM ('low', 'medium', 'high', 'critical');

-- Analyst Ratings
CREATE TYPE rating_enum AS ENUM (
    'strong_buy', 
    'buy', 
    'hold', 
    'sell', 
    'strong_sell',
    'outperform',
    'underperform',
    'neutral'
);

-- Rating Changes  
CREATE TYPE rating_change_enum AS ENUM (
    'upgrade', 
    'downgrade', 
    'initiated', 
    'reiterated',
    'suspended',
    'resumed'
);

-- Corporate Actions
CREATE TYPE corporate_action_enum AS ENUM (
    'dividend', 
    'split', 
    'merger', 
    'acquisition', 
    'spinoff', 
    'rights_offering', 
    'special_dividend', 
    'stock_dividend',
    'stock_buyback',
    'delisting',
    'symbol_change'
);

-- Earnings Report Types
CREATE TYPE earnings_type_enum AS ENUM (
    'preliminary',
    'final', 
    'amended',
    'guidance_update',
    'preannouncement'
);

-- ==========================================
-- CORE TABLES
-- ==========================================

-- 1. Core Financial Events Table
CREATE TABLE dev_financial_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT UNIQUE NOT NULL,  -- Vendor-specific unique identifier
    instrument_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    
    -- Event Classification
    event_type event_type_enum NOT NULL,
    event_subtype TEXT,  -- More specific categorization
    
    -- Timing (Critical for trading systems)
    event_datetime TIMESTAMPTZ NOT NULL,  -- Actual event time with timezone
    market_datetime TIMESTAMPTZ,  -- Market close time for event day
    announcement_datetime TIMESTAMPTZ,  -- When event was announced
    fiscal_period TEXT,  -- Q1, Q2, Q3, Q4, FY
    fiscal_year INTEGER,
    
    -- Event Content
    title TEXT NOT NULL,
    description TEXT,
    summary TEXT,  -- Concise version for APIs
    
    -- Impact Analysis (Key differentiator)
    sentiment sentiment_enum,
    impact_score DECIMAL(3,2),  -- -1.00 to +1.00
    importance_level importance_enum,
    market_moving BOOLEAN DEFAULT FALSE,
    
    -- Expectations vs Reality (Crucial for earnings)
    expected_value DECIMAL(15,4),
    actual_value DECIMAL(15,4),
    variance_pct DECIMAL(8,4),  -- (actual - expected) / expected * 100
    
    -- Source Attribution
    vendor TEXT NOT NULL,  -- polygon, tiingo, alpha_vantage, etc.
    source_url TEXT,
    confidence_score DECIMAL(3,2) DEFAULT 1.0,  -- Data quality score
    
    -- Metadata
    tags TEXT[],
    raw_data JSONB,  -- Store original vendor response
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT fk_financial_events_instrument 
        FOREIGN KEY (instrument_id) REFERENCES dev_instruments(id),
    CONSTRAINT chk_impact_score 
        CHECK (impact_score >= -1.00 AND impact_score <= 1.00),
    CONSTRAINT chk_confidence_score 
        CHECK (confidence_score >= 0.00 AND confidence_score <= 1.00)
);

-- 2. Earnings-Specific Events
CREATE TABLE dev_earnings_events (
    id BIGSERIAL PRIMARY KEY,
    financial_event_id BIGINT NOT NULL REFERENCES dev_financial_events(id) ON DELETE CASCADE,
    instrument_id INTEGER NOT NULL REFERENCES dev_instruments(id),
    
    -- Earnings Specifics
    report_period DATE NOT NULL,
    report_type earnings_type_enum NOT NULL DEFAULT 'final',
    
    -- Financial Metrics (using BIGINT for precision, stored in cents)
    eps_actual_cents BIGINT,  -- EPS * 10000 (4 decimal places)
    eps_estimated_cents BIGINT,
    eps_surprise_pct DECIMAL(8,4),
    
    revenue_actual_cents BIGINT,  -- Revenue in cents
    revenue_estimated_cents BIGINT,
    revenue_surprise_pct DECIMAL(8,4),
    
    -- Additional Metrics
    net_income_cents BIGINT,
    operating_income_cents BIGINT,
    gross_margin_pct DECIMAL(8,4),
    operating_margin_pct DECIMAL(8,4),
    
    -- Call Information
    earnings_call_datetime TIMESTAMPTZ,
    earnings_call_url TEXT,
    transcript_available BOOLEAN DEFAULT FALSE,
    webcast_url TEXT,
    
    -- Guidance
    forward_guidance JSONB,  -- Store next quarter guidance
    guidance_raised BOOLEAN DEFAULT FALSE,
    guidance_lowered BOOLEAN DEFAULT FALSE,
    guidance_maintained BOOLEAN DEFAULT FALSE,
    
    -- Beat/Miss Analysis
    earnings_beat BOOLEAN,  -- TRUE if beat estimates
    revenue_beat BOOLEAN,   -- TRUE if beat revenue estimates
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_report_period 
        CHECK (report_period >= '1990-01-01'),
    CONSTRAINT chk_surprise_pct 
        CHECK (eps_surprise_pct >= -100.0 AND eps_surprise_pct <= 1000.0)
);

-- 3. Analyst Ratings Events
CREATE TABLE dev_analyst_ratings (
    id BIGSERIAL PRIMARY KEY,
    financial_event_id BIGINT NOT NULL REFERENCES dev_financial_events(id) ON DELETE CASCADE,
    instrument_id INTEGER NOT NULL REFERENCES dev_instruments(id),
    
    -- Rating Details
    analyst_firm TEXT NOT NULL,
    analyst_name TEXT,
    analyst_id TEXT,  -- Unique analyst identifier
    
    -- Rating Change
    previous_rating rating_enum,
    new_rating rating_enum NOT NULL,
    rating_change rating_change_enum,
    
    -- Price Targets (in cents for precision)
    previous_price_target_cents BIGINT,
    new_price_target_cents BIGINT,
    current_price_cents BIGINT,  -- Price at time of rating
    upside_potential_pct DECIMAL(8,4),
    
    -- Reasoning
    reasoning TEXT,
    key_factors TEXT[],
    recommendation_summary TEXT,
    
    -- Research Report
    report_url TEXT,
    report_title TEXT,
    report_published_date DATE,
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_price_targets_positive 
        CHECK (previous_price_target_cents > 0 OR previous_price_target_cents IS NULL),
    CONSTRAINT chk_current_price_positive 
        CHECK (current_price_cents > 0 OR current_price_cents IS NULL)
);

-- 4. Corporate Actions Events
CREATE TABLE dev_corporate_actions (
    id BIGSERIAL PRIMARY KEY,
    financial_event_id BIGINT NOT NULL REFERENCES dev_financial_events(id) ON DELETE CASCADE,
    instrument_id INTEGER NOT NULL REFERENCES dev_instruments(id),
    
    -- Action Details
    action_type corporate_action_enum NOT NULL,
    action_description TEXT,
    
    -- Critical Dates (all required for trading systems)
    announcement_date DATE NOT NULL,
    ex_date DATE,  -- Ex-dividend/ex-split date
    record_date DATE,
    payment_date DATE,
    effective_date DATE,
    
    -- Financial Details (in cents for precision)
    cash_amount_cents BIGINT,  -- For dividends, stored in cents
    currency_code TEXT DEFAULT 'USD',
    
    -- Stock Split/Stock Dividend Details
    ratio_from INTEGER,  -- For 2:1 split, this is 2
    ratio_to INTEGER,    -- For 2:1 split, this is 1
    
    -- Additional Details
    is_special BOOLEAN DEFAULT FALSE,  -- Special dividend/action
    is_recurring BOOLEAN DEFAULT FALSE, -- Regular dividend
    
    -- Complex Action Data
    action_details JSONB,  -- Flexible storage for complex actions
    
    -- Tax Implications
    qualified_dividend BOOLEAN,  -- For tax reporting
    return_of_capital_pct DECIMAL(5,2),  -- Percentage that's return of capital
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_announcement_date 
        CHECK (announcement_date >= '1990-01-01'),
    CONSTRAINT chk_split_ratio 
        CHECK ((ratio_from IS NULL AND ratio_to IS NULL) OR 
               (ratio_from > 0 AND ratio_to > 0)),
    CONSTRAINT chk_return_of_capital 
        CHECK (return_of_capital_pct >= 0.0 AND return_of_capital_pct <= 100.0)
);

-- 5. Event Impact Analysis (for ML/backtesting)
CREATE TABLE dev_event_impacts (
    id BIGSERIAL PRIMARY KEY,
    financial_event_id BIGINT NOT NULL REFERENCES dev_financial_events(id) ON DELETE CASCADE,
    instrument_id INTEGER NOT NULL REFERENCES dev_instruments(id),
    
    -- Price Impact Analysis
    price_1d_before_cents BIGINT,
    price_1d_after_cents BIGINT,
    price_5d_after_cents BIGINT,
    price_30d_after_cents BIGINT,
    
    -- Volume Impact
    volume_1d_before BIGINT,
    volume_1d_after BIGINT,
    avg_volume_20d BIGINT,
    
    -- Calculated Metrics
    price_impact_1d_pct DECIMAL(8,4),
    price_impact_5d_pct DECIMAL(8,4),
    price_impact_30d_pct DECIMAL(8,4),
    volume_spike_factor DECIMAL(8,4),  -- volume_1d_after / avg_volume_20d
    
    -- Market Context
    market_return_1d_pct DECIMAL(8,4),  -- SPY return for context
    sector_return_1d_pct DECIMAL(8,4),
    
    calculated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- PERFORMANCE INDEXES
-- ==========================================

-- Core event queries (most important)
CREATE INDEX idx_financial_events_symbol_datetime 
    ON dev_financial_events (symbol, event_datetime DESC);

CREATE INDEX idx_financial_events_type_datetime 
    ON dev_financial_events (event_type, event_datetime DESC);

CREATE INDEX idx_financial_events_instrument_datetime 
    ON dev_financial_events (instrument_id, event_datetime DESC);

CREATE INDEX idx_financial_events_importance 
    ON dev_financial_events (importance_level, event_datetime DESC);

CREATE INDEX idx_financial_events_market_moving 
    ON dev_financial_events (market_moving, event_datetime DESC) 
    WHERE market_moving = TRUE;

-- Earnings-specific indexes
CREATE INDEX idx_earnings_events_period 
    ON dev_earnings_events (report_period DESC);

CREATE INDEX idx_earnings_events_instrument_period 
    ON dev_earnings_events (instrument_id, report_period DESC);

CREATE INDEX idx_earnings_surprise 
    ON dev_earnings_events (eps_surprise_pct DESC NULLS LAST) 
    WHERE eps_surprise_pct IS NOT NULL;

CREATE INDEX idx_earnings_beat_miss 
    ON dev_earnings_events (earnings_beat, revenue_beat, report_period DESC);

-- Analyst ratings indexes
CREATE INDEX idx_analyst_ratings_firm_date 
    ON dev_analyst_ratings (analyst_firm, created_at DESC);

CREATE INDEX idx_analyst_ratings_change 
    ON dev_analyst_ratings (rating_change, created_at DESC);

CREATE INDEX idx_analyst_ratings_instrument 
    ON dev_analyst_ratings (instrument_id, created_at DESC);

-- Corporate actions indexes
CREATE INDEX idx_corporate_actions_type_date 
    ON dev_corporate_actions (action_type, ex_date DESC NULLS LAST);

CREATE INDEX idx_corporate_actions_instrument_date 
    ON dev_corporate_actions (instrument_id, announcement_date DESC);

CREATE INDEX idx_corporate_actions_ex_date 
    ON dev_corporate_actions (ex_date DESC NULLS LAST) 
    WHERE ex_date IS NOT NULL;

-- Event impacts indexes (for backtesting queries)
CREATE INDEX idx_event_impacts_instrument 
    ON dev_event_impacts (instrument_id, calculated_at DESC);

CREATE INDEX idx_event_impacts_price_impact 
    ON dev_event_impacts (price_impact_1d_pct DESC NULLS LAST) 
    WHERE price_impact_1d_pct IS NOT NULL;

-- ==========================================
-- VIEWS FOR COMMON QUERIES
-- ==========================================

-- Recent earnings with surprise analysis
CREATE VIEW v_recent_earnings AS
SELECT 
    fe.symbol,
    fe.event_datetime,
    ee.report_period,
    ee.eps_actual_cents::DECIMAL/10000 AS eps_actual,
    ee.eps_estimated_cents::DECIMAL/10000 AS eps_estimated,
    ee.eps_surprise_pct,
    ee.revenue_actual_cents::DECIMAL/100 AS revenue_actual,
    ee.revenue_estimated_cents::DECIMAL/100 AS revenue_estimated,
    ee.revenue_surprise_pct,
    ee.earnings_beat,
    ee.revenue_beat,
    fe.importance_level
FROM dev_financial_events fe
JOIN dev_earnings_events ee ON fe.id = ee.financial_event_id
WHERE fe.event_type = 'earnings'
    AND fe.event_datetime >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY fe.event_datetime DESC;

-- Analyst consensus view
CREATE VIEW v_analyst_consensus AS
WITH latest_ratings AS (
    SELECT 
        instrument_id,
        symbol,
        analyst_firm,
        new_rating,
        new_price_target_cents,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY instrument_id, analyst_firm 
            ORDER BY created_at DESC
        ) as rn
    FROM dev_analyst_ratings ar
    JOIN dev_financial_events fe ON ar.financial_event_id = fe.id
    WHERE created_at >= CURRENT_DATE - INTERVAL '365 days'
)
SELECT 
    instrument_id,
    symbol,
    COUNT(*) as total_ratings,
    COUNT(CASE WHEN new_rating IN ('strong_buy', 'buy') THEN 1 END) as buy_ratings,
    COUNT(CASE WHEN new_rating = 'hold' THEN 1 END) as hold_ratings,
    COUNT(CASE WHEN new_rating IN ('sell', 'strong_sell') THEN 1 END) as sell_ratings,
    AVG(new_price_target_cents::DECIMAL/100) as avg_price_target,
    MIN(created_at) as oldest_rating_date,
    MAX(created_at) as newest_rating_date
FROM latest_ratings
WHERE rn = 1  -- Only latest rating per firm
    AND new_price_target_cents IS NOT NULL
GROUP BY instrument_id, symbol
HAVING COUNT(*) >= 2;  -- At least 2 analysts

-- ==========================================
-- COMMENTS FOR DOCUMENTATION
-- ==========================================

COMMENT ON TABLE dev_financial_events IS 
'Core financial events table following Bloomberg Terminal standards. Stores all types of market-moving events with sentiment analysis and impact scoring.';

COMMENT ON TABLE dev_earnings_events IS 
'Earnings-specific data with beat/miss analysis and guidance tracking. Financial metrics stored in cents for precision.';

COMMENT ON TABLE dev_analyst_ratings IS 
'Analyst ratings and price target changes with full attribution and reasoning.';

COMMENT ON TABLE dev_corporate_actions IS 
'Corporate actions with precise date tracking for trading systems. Handles dividends, splits, mergers, etc.';

COMMENT ON TABLE dev_event_impacts IS 
'Calculated price and volume impacts for machine learning and backtesting analysis.';

-- Schema creation completed
SELECT 'Financial Events Database Schema Created Successfully!' as status;