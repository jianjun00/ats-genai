-- Unified Data Quality Schema Migration
-- Consolidates coverage monitoring + validation + agent workflow tracking
-- Replaces fragmented monitoring with unified data quality framework

-- =====================================================================
-- UNIFIED DATA QUALITY ISSUES TABLE
-- Consolidates: dev_coverage_gaps + quality_issues + validation_errors
-- =====================================================================

CREATE TABLE IF NOT EXISTS dev_data_quality_issues (
    id SERIAL PRIMARY KEY,
    
    -- Issue Classification
    issue_type VARCHAR(50) NOT NULL,        -- 'coverage_gap', 'missing_data', 'stale_data', 'extreme_value', 'validation_error'
    issue_category VARCHAR(20) NOT NULL,    -- 'coverage', 'validation', 'consistency', 'timeliness'
    
    -- Data Identification
    vendor VARCHAR(20) NOT NULL,            -- 'polygon', 'tiingo', 'firstrate', 'eodhd'
    data_type VARCHAR(20) NOT NULL,         -- 'daily_prices', 'minute_bars'
    symbol VARCHAR(20) NOT NULL,
    affected_date_start DATE NOT NULL,
    affected_date_end DATE NOT NULL,
    
    -- Severity and Status
    severity VARCHAR(20) NOT NULL DEFAULT 'medium',  -- 'critical', 'high', 'medium', 'low'
    status VARCHAR(20) NOT NULL DEFAULT 'pending',   -- 'pending', 'in_progress', 'resolved', 'escalated', 'cancelled'
    
    -- Agent Classification
    complexity VARCHAR(20),                 -- 'simple', 'medium', 'complex'
    priority_score INTEGER NOT NULL DEFAULT 5,
    estimated_effort_minutes INTEGER,
    confidence_score DECIMAL(5,2),          -- Agent confidence in classification
    
    -- Resolution Tracking
    resolution_strategy VARCHAR(30),        -- 'auto_resolve', 'human_assisted', 'escalate', 'monitor'
    assigned_agent VARCHAR(50),             -- Agent ID handling the issue
    workflow_id UUID,                       -- Workflow tracking ID
    
    -- Metadata (JSON for flexibility)
    issue_metadata JSONB,                   -- Issue-specific details (gap_days, affected_records, etc.)
    resolution_metadata JSONB,              -- Resolution details (backfill_job_id, fix_applied, etc.)
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMP,
    first_detected_at TIMESTAMP DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT valid_severity CHECK (severity IN ('critical', 'high', 'medium', 'low')),
    CONSTRAINT valid_status CHECK (status IN ('pending', 'in_progress', 'resolved', 'escalated', 'cancelled')),
    CONSTRAINT valid_category CHECK (issue_category IN ('coverage', 'validation', 'consistency', 'timeliness')),
    CONSTRAINT valid_date_range CHECK (affected_date_end >= affected_date_start)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_dqi_category_status ON dev_data_quality_issues(issue_category, status);
CREATE INDEX IF NOT EXISTS idx_dqi_vendor_data_type ON dev_data_quality_issues(vendor, data_type);
CREATE INDEX IF NOT EXISTS idx_dqi_symbol_dates ON dev_data_quality_issues(symbol, affected_date_start, affected_date_end);
CREATE INDEX IF NOT EXISTS idx_dqi_priority_created ON dev_data_quality_issues(priority_score DESC, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dqi_workflow ON dev_data_quality_issues(workflow_id) WHERE workflow_id IS NOT NULL;

-- =====================================================================
-- UNIFIED DATA QUALITY METRICS TABLE
-- Consolidates: dev_daily_coverage_metrics + validation_metrics + quality_scores
-- =====================================================================

CREATE TABLE IF NOT EXISTS dev_data_quality_metrics (
    id SERIAL PRIMARY KEY,
    
    -- Metric Identification
    metric_date DATE NOT NULL,
    vendor VARCHAR(20) NOT NULL,
    data_type VARCHAR(20) NOT NULL,
    metric_category VARCHAR(30) NOT NULL,   -- 'coverage', 'validation', 'consistency', 'timeliness'
    metric_name VARCHAR(50) NOT NULL,       -- 'coverage_percentage', 'completeness_score', 'consistency_score'
    
    -- Core Metrics (unified across categories)
    total_expected INTEGER,                 -- Total expected records/files
    total_actual INTEGER,                   -- Actual records/files found
    metric_value DECIMAL(8,2) NOT NULL,     -- Primary metric value (percentage, score, count)
    threshold_value DECIMAL(8,2),           -- Threshold for this metric
    
    -- Quality Scores (0-100 scale)
    quality_score DECIMAL(5,2),             -- Overall quality score for this metric
    coverage_percentage DECIMAL(5,2),       -- Coverage-specific percentage
    completeness_score DECIMAL(5,2),        -- Validation-specific completeness
    consistency_score DECIMAL(5,2),         -- Validation-specific consistency
    timeliness_score DECIMAL(5,2),          -- Timeliness-specific score
    
    -- Issue Tracking
    issues_detected INTEGER DEFAULT 0,
    issues_critical INTEGER DEFAULT 0,
    issues_auto_resolved INTEGER DEFAULT 0,
    issues_escalated INTEGER DEFAULT 0,
    
    -- Performance Tracking
    scan_duration_seconds DECIMAL(8,3),     -- Time taken to collect this metric
    data_points_analyzed INTEGER,           -- Number of data points analyzed
    
    -- Status and Metadata
    metric_status VARCHAR(20) DEFAULT 'unknown',  -- 'healthy', 'warning', 'critical', 'unknown'
    metadata JSONB,                         -- Metric-specific details
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT valid_metric_status CHECK (metric_status IN ('healthy', 'warning', 'critical', 'unknown')),
    CONSTRAINT valid_metric_category CHECK (metric_category IN ('coverage', 'validation', 'consistency', 'timeliness')),
    CONSTRAINT positive_metric_value CHECK (metric_value >= 0),
    
    -- Unique constraint for metric identity
    UNIQUE(metric_date, vendor, data_type, metric_category, metric_name)
);

-- Indexes for trending and analysis
CREATE INDEX IF NOT EXISTS idx_dqm_trending ON dev_data_quality_metrics(vendor, data_type, metric_category, metric_date);
CREATE INDEX IF NOT EXISTS idx_dqm_status_date ON dev_data_quality_metrics(metric_status, metric_date);
CREATE INDEX IF NOT EXISTS idx_dqm_category_score ON dev_data_quality_metrics(metric_category, quality_score);

-- =====================================================================
-- DATA QUALITY AGENT OPERATIONS LOG
-- Tracks all agent operations, decisions, and learning patterns
-- =====================================================================

CREATE TABLE IF NOT EXISTS dev_data_quality_agent_operations (
    id SERIAL PRIMARY KEY,
    
    -- Operation Identification
    operation_type VARCHAR(50) NOT NULL,    -- 'scan', 'classify', 'resolve', 'escalate', 'monitor'
    agent_id VARCHAR(100) NOT NULL,         -- Unique agent identifier
    operation_subtype VARCHAR(50),          -- 'coverage_scan', 'validation_scan', 'gap_detection', etc.
    
    -- Related Issue (if applicable)
    issue_id INTEGER REFERENCES dev_data_quality_issues(id),
    related_workflow_id UUID,
    
    -- Operation Input/Output
    operation_input JSONB,                  -- Input parameters and configuration
    operation_output JSONB,                 -- Results, decisions, and data produced
    operation_status VARCHAR(20) NOT NULL,  -- 'success', 'failure', 'partial', 'skipped'
    
    -- Performance and Quality Metrics
    execution_time_ms INTEGER,              -- Execution time in milliseconds
    memory_usage_mb DECIMAL(10,2),          -- Memory usage during operation
    confidence_score DECIMAL(5,2),          -- Agent confidence in operation result
    
    -- Learning and Decision Tracking
    decision_reasoning TEXT,                -- Agent's reasoning for decisions made
    alternatives_considered JSONB,          -- Alternative actions considered
    learning_applied JSONB,                 -- Historical patterns used in decision
    
    -- Error Handling
    error_message TEXT,                     -- Error details if operation failed
    retry_count INTEGER DEFAULT 0,          -- Number of retries attempted
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    
    -- Constraints
    CONSTRAINT valid_operation_status CHECK (operation_status IN ('success', 'failure', 'partial', 'skipped')),
    CONSTRAINT positive_execution_time CHECK (execution_time_ms >= 0),
    CONSTRAINT valid_confidence CHECK (confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 1))
);

-- Indexes for performance analysis and debugging
CREATE INDEX IF NOT EXISTS idx_dqao_agent_operation ON dev_data_quality_agent_operations(agent_id, operation_type, created_at);
CREATE INDEX IF NOT EXISTS idx_dqao_issue_workflow ON dev_data_quality_agent_operations(issue_id, related_workflow_id);
CREATE INDEX IF NOT EXISTS idx_dqao_performance ON dev_data_quality_agent_operations(operation_type, execution_time_ms, created_at);
CREATE INDEX IF NOT EXISTS idx_dqao_learning ON dev_data_quality_agent_operations(operation_type, confidence_score) WHERE confidence_score IS NOT NULL;

-- =====================================================================
-- UNIFIED ALERT CONFIGURATION TABLE
-- Centralizes alert thresholds and notification preferences
-- =====================================================================

CREATE TABLE IF NOT EXISTS dev_data_quality_alert_config (
    id SERIAL PRIMARY KEY,
    
    -- Alert Identification
    alert_name VARCHAR(100) NOT NULL UNIQUE,
    alert_category VARCHAR(30) NOT NULL,    -- 'coverage', 'validation', 'agent', 'system'
    alert_type VARCHAR(50) NOT NULL,        -- 'threshold_breach', 'trend_degradation', 'agent_failure'
    
    -- Scope (what triggers this alert)
    vendor_filter VARCHAR(20),              -- NULL = all vendors
    data_type_filter VARCHAR(20),           -- NULL = all data types
    severity_filter VARCHAR(20),            -- Minimum severity to trigger
    
    -- Threshold Configuration
    threshold_config JSONB NOT NULL,        -- JSON configuration for thresholds
    evaluation_window_minutes INTEGER DEFAULT 60,  -- Time window for evaluation
    
    -- Notification Configuration
    notification_channels JSONB,            -- Slack, email, etc. configuration
    notification_frequency VARCHAR(20) DEFAULT 'immediate',  -- 'immediate', 'hourly', 'daily'
    escalation_config JSONB,                -- Escalation rules and timelines
    
    -- Status and Control
    active BOOLEAN DEFAULT true,
    suppress_until TIMESTAMP,               -- Temporary suppression
    last_triggered TIMESTAMP,
    trigger_count INTEGER DEFAULT 0,
    
    -- Metadata
    description TEXT,
    created_by VARCHAR(100) DEFAULT 'system',
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT valid_alert_category CHECK (alert_category IN ('coverage', 'validation', 'agent', 'system')),
    CONSTRAINT valid_notification_frequency CHECK (notification_frequency IN ('immediate', 'hourly', 'daily'))
);

-- Index for alert evaluation
CREATE INDEX IF NOT EXISTS idx_dqac_active_category ON dev_data_quality_alert_config(active, alert_category);
CREATE INDEX IF NOT EXISTS idx_dqac_evaluation ON dev_data_quality_alert_config(active, alert_type, vendor_filter, data_type_filter);

-- =====================================================================
-- VIEWS FOR COMMON QUERIES
-- Replaces multiple view definitions with unified perspectives
-- =====================================================================

-- Current Quality Status by Category
CREATE OR REPLACE VIEW v_current_data_quality_status AS
SELECT 
    issue_category,
    vendor,
    data_type,
    COUNT(*) as total_issues,
    COUNT(CASE WHEN severity = 'critical' THEN 1 END) as critical_issues,
    COUNT(CASE WHEN severity = 'high' THEN 1 END) as high_issues,
    COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved_issues,
    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_issues,
    AVG(priority_score) as avg_priority_score,
    MAX(created_at) as latest_issue_time
FROM dev_data_quality_issues 
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY issue_category, vendor, data_type
ORDER BY critical_issues DESC, high_issues DESC;

-- Quality Metrics Trending
CREATE OR REPLACE VIEW v_data_quality_metrics_trending AS
SELECT 
    metric_date,
    metric_category,
    vendor,
    data_type,
    AVG(quality_score) as avg_quality_score,
    COUNT(*) as metrics_count,
    COUNT(CASE WHEN metric_status = 'critical' THEN 1 END) as critical_metrics,
    LAG(AVG(quality_score)) OVER (
        PARTITION BY metric_category, vendor, data_type 
        ORDER BY metric_date
    ) as prev_day_score,
    AVG(quality_score) - LAG(AVG(quality_score)) OVER (
        PARTITION BY metric_category, vendor, data_type 
        ORDER BY metric_date
    ) as score_change
FROM dev_data_quality_metrics
WHERE metric_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY metric_date, metric_category, vendor, data_type
ORDER BY metric_date DESC, metric_category, vendor, data_type;

-- Active Issues Requiring Attention
CREATE OR REPLACE VIEW v_active_quality_issues AS
SELECT 
    i.*,
    CASE 
        WHEN i.severity = 'critical' AND i.created_at < NOW() - INTERVAL '1 hour' THEN 'overdue'
        WHEN i.severity = 'high' AND i.created_at < NOW() - INTERVAL '4 hours' THEN 'overdue'
        WHEN i.severity = 'medium' AND i.created_at < NOW() - INTERVAL '24 hours' THEN 'overdue'
        ELSE 'on_time'
    END as resolution_timeliness,
    EXTRACT(EPOCH FROM NOW() - i.created_at) / 3600 as hours_open,
    COUNT(ao.id) as agent_operations_count,
    MAX(ao.completed_at) as last_agent_operation
FROM dev_data_quality_issues i
LEFT JOIN dev_data_quality_agent_operations ao ON i.id = ao.issue_id
WHERE i.status IN ('pending', 'in_progress')
GROUP BY i.id
ORDER BY 
    CASE i.severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END,
    i.priority_score DESC,
    i.created_at ASC;

-- Agent Performance Summary
CREATE OR REPLACE VIEW v_agent_performance_summary AS
SELECT 
    agent_id,
    operation_type,
    DATE(created_at) as operation_date,
    COUNT(*) as total_operations,
    COUNT(CASE WHEN operation_status = 'success' THEN 1 END) as successful_operations,
    COUNT(CASE WHEN operation_status = 'failure' THEN 1 END) as failed_operations,
    AVG(execution_time_ms) as avg_execution_time_ms,
    AVG(confidence_score) as avg_confidence_score,
    COUNT(DISTINCT issue_id) as unique_issues_handled
FROM dev_data_quality_agent_operations
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY agent_id, operation_type, DATE(created_at)
ORDER BY operation_date DESC, agent_id, operation_type;

-- =====================================================================
-- INSERT DEFAULT ALERT CONFIGURATIONS
-- Replaces scattered alert configurations with unified setup
-- =====================================================================

INSERT INTO dev_data_quality_alert_config (alert_name, alert_category, alert_type, threshold_config, notification_channels, description) VALUES

-- Coverage Alerts
('critical_coverage_gaps', 'coverage', 'threshold_breach', 
 '{"coverage_threshold": 0.95, "gap_days_threshold": 1, "critical_symbols": ["SPY", "QQQ", "AAPL", "MSFT"]}',
 '{"slack": {"enabled": true, "channel": "#data-quality-alerts"}, "email": {"enabled": false}}',
 'Alert when critical symbols have coverage gaps'),

('coverage_degradation_trend', 'coverage', 'trend_degradation',
 '{"min_coverage_pct": 85, "trending_window_days": 7, "degradation_threshold": 0.05}',
 '{"slack": {"enabled": true, "channel": "#data-quality-alerts"}}',
 'Alert when coverage shows degradation trend over time'),

-- Validation Alerts  
('validation_completeness_breach', 'validation', 'threshold_breach',
 '{"completeness_threshold": 0.98, "consistency_threshold": 0.95}',
 '{"slack": {"enabled": true, "channel": "#data-quality-alerts"}}',
 'Alert when validation scores breach thresholds'),

('extreme_value_detection', 'validation', 'threshold_breach',
 '{"extreme_move_threshold": 0.5, "volume_outlier_threshold": 10}',
 '{"slack": {"enabled": true, "channel": "#data-quality-alerts"}}',
 'Alert when extreme values detected in market data'),

-- Agent Alerts
('agent_failure_rate', 'agent', 'threshold_breach',
 '{"max_failure_rate": 0.1, "evaluation_window_minutes": 60}',
 '{"slack": {"enabled": true, "channel": "#data-quality-alerts"}}',
 'Alert when agent failure rate exceeds threshold'),

('agent_processing_delays', 'agent', 'threshold_breach',
 '{"max_processing_time_minutes": 30, "backlog_threshold": 50}',
 '{"slack": {"enabled": true, "channel": "#data-quality-alerts"}}',
 'Alert when agent processing delays exceed thresholds')

ON CONFLICT (alert_name) DO UPDATE SET
    threshold_config = EXCLUDED.threshold_config,
    notification_channels = EXCLUDED.notification_channels,
    description = EXCLUDED.description,
    updated_at = NOW();

-- =====================================================================
-- COMMENTS FOR DOCUMENTATION
-- =====================================================================

COMMENT ON TABLE dev_data_quality_issues IS 'Unified table for all data quality issues: coverage gaps, validation errors, consistency problems';
COMMENT ON TABLE dev_data_quality_metrics IS 'Consolidated metrics for coverage, validation, and quality scoring across all vendors';
COMMENT ON TABLE dev_data_quality_agent_operations IS 'Complete log of agent operations, decisions, and learning patterns';
COMMENT ON TABLE dev_data_quality_alert_config IS 'Centralized alert configuration for all data quality monitoring';

COMMENT ON VIEW v_current_data_quality_status IS 'Real-time quality status across all categories and vendors';
COMMENT ON VIEW v_data_quality_metrics_trending IS 'Quality metrics trending with change detection';
COMMENT ON VIEW v_active_quality_issues IS 'Active issues with resolution timeliness and agent activity';
COMMENT ON VIEW v_agent_performance_summary IS 'Agent performance metrics and success rates';

-- =====================================================================
-- MIGRATION COMPLETION LOG
-- =====================================================================

INSERT INTO dev_schema_migration_log (migration_name, migration_type, description, executed_at) VALUES
('090_unified_data_quality_schema', 'feature', 'Consolidated data quality framework: issues + metrics + agent operations + alert config', NOW())
ON CONFLICT (migration_name) DO UPDATE SET
    executed_at = NOW(),
    description = EXCLUDED.description;