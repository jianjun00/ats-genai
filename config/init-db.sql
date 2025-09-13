-- ATS Data Quality Agent Database Initialization
-- This script creates all necessary tables and indexes for the agent system

-- Create extensions
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create agent issues table
CREATE TABLE IF NOT EXISTS agent_issues (
    issue_id VARCHAR(50) PRIMARY KEY,
    severity VARCHAR(10) NOT NULL CHECK (severity IN ('critical', 'high', 'medium', 'low')),
    status VARCHAR(20) NOT NULL CHECK (status IN ('open', 'in_progress', 'resolved', 'closed')),
    issue_type VARCHAR(50) NOT NULL,
    symbol VARCHAR(10),
    date DATE,
    description TEXT NOT NULL,
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    resolved_at TIMESTAMP WITH TIME ZONE,
    vendor VARCHAR(20),
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create agent workflows table
CREATE TABLE IF NOT EXISTS agent_workflows (
    workflow_id VARCHAR(50) PRIMARY KEY,
    issue_id VARCHAR(50) REFERENCES agent_issues(issue_id) ON DELETE CASCADE,
    state VARCHAR(20) NOT NULL CHECK (state IN ('pending', 'running', 'paused', 'completed', 'failed', 'cancelled')),
    tool_name VARCHAR(50) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    progress INTEGER DEFAULT 0 CHECK (progress >= 0 AND progress <= 100),
    execution_log JSONB DEFAULT '[]'::jsonb,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create agent alerts table
CREATE TABLE IF NOT EXISTS agent_alerts (
    alert_id VARCHAR(50) PRIMARY KEY,
    severity VARCHAR(10) NOT NULL CHECK (severity IN ('critical', 'high', 'medium', 'low')),
    type VARCHAR(20) NOT NULL,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_at TIMESTAMP WITH TIME ZONE,
    acknowledged_by VARCHAR(100),
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolved_by VARCHAR(100),
    metadata JSONB,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create agent configuration table
CREATE TABLE IF NOT EXISTS agent_config (
    config_key VARCHAR(100) PRIMARY KEY,
    config_value JSONB NOT NULL,
    config_type VARCHAR(50) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create agent metrics table (for performance tracking)
CREATE TABLE IF NOT EXISTS agent_metrics (
    metric_id SERIAL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    tags JSONB,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (metric_id, timestamp)
);

-- Create hypertable for metrics (TimescaleDB)
SELECT create_hypertable('agent_metrics', 'timestamp', if_not_exists => TRUE);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_agent_issues_severity_status ON agent_issues(severity, status);
CREATE INDEX IF NOT EXISTS idx_agent_issues_detected_at ON agent_issues(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_issues_symbol_date ON agent_issues(symbol, date);
CREATE INDEX IF NOT EXISTS idx_agent_issues_vendor ON agent_issues(vendor);
CREATE INDEX IF NOT EXISTS idx_agent_issues_type ON agent_issues(issue_type);

CREATE INDEX IF NOT EXISTS idx_agent_workflows_state ON agent_workflows(state);
CREATE INDEX IF NOT EXISTS idx_agent_workflows_issue_id ON agent_workflows(issue_id);
CREATE INDEX IF NOT EXISTS idx_agent_workflows_tool_name ON agent_workflows(tool_name);
CREATE INDEX IF NOT EXISTS idx_agent_workflows_started_at ON agent_workflows(started_at DESC);

CREATE INDEX IF NOT EXISTS idx_agent_alerts_severity_resolved ON agent_alerts(severity, resolved);
CREATE INDEX IF NOT EXISTS idx_agent_alerts_created_at ON agent_alerts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_alerts_type ON agent_alerts(type);
CREATE INDEX IF NOT EXISTS idx_agent_alerts_acknowledged ON agent_alerts(acknowledged);

CREATE INDEX IF NOT EXISTS idx_agent_config_type ON agent_config(config_type);

CREATE INDEX IF NOT EXISTS idx_agent_metrics_name_timestamp ON agent_metrics(metric_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_agent_metrics_type ON agent_metrics(metric_type);

-- Create trigger functions for updated_at timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at
CREATE TRIGGER update_agent_issues_updated_at BEFORE UPDATE ON agent_issues
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_agent_workflows_updated_at BEFORE UPDATE ON agent_workflows
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_agent_alerts_updated_at BEFORE UPDATE ON agent_alerts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_agent_config_updated_at BEFORE UPDATE ON agent_config
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert default configuration
INSERT INTO agent_config (config_key, config_value, config_type, description) 
VALUES 
    ('monitoring', '{"cycle_interval_seconds": 300, "max_concurrent_workflows": 20, "enable_automatic_resolution": true, "enable_automatic_scanning": true}', 'monitoring', 'Core monitoring configuration')
ON CONFLICT (config_key) DO NOTHING;

INSERT INTO agent_config (config_key, config_value, config_type, description)
VALUES 
    ('issue_thresholds', '{"quality_score_critical_threshold": 50, "quality_score_warning_threshold": 75, "extreme_volume_multiplier": 50.0, "data_freshness_hours": 24}', 'thresholds', 'Issue detection thresholds')
ON CONFLICT (config_key) DO NOTHING;

INSERT INTO agent_config (config_key, config_value, config_type, description)
VALUES 
    ('notifications', '{"enable_email_notifications": true, "enable_slack_notifications": true, "max_notifications_per_hour": 20, "alert_cooldown_minutes": 30}', 'notifications', 'Notification configuration')
ON CONFLICT (config_key) DO NOTHING;

INSERT INTO agent_config (config_key, config_value, config_type, description)
VALUES 
    ('system_health', '{"cpu_warning_threshold": 70, "cpu_critical_threshold": 85, "memory_warning_threshold": 80, "memory_critical_threshold": 90, "disk_warning_threshold": 85, "disk_critical_threshold": 95}', 'system', 'System health monitoring thresholds')
ON CONFLICT (config_key) DO NOTHING;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
GRANT USAGE ON SCHEMA public TO postgres;

-- Create read-only user for monitoring (optional)
-- CREATE USER ats_monitor WITH PASSWORD 'monitor_password';
-- GRANT CONNECT ON DATABASE prod_db TO ats_monitor;
-- GRANT USAGE ON SCHEMA public TO ats_monitor;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO ats_monitor;

COMMENT ON TABLE agent_issues IS 'Data quality issues detected by the agent';
COMMENT ON TABLE agent_workflows IS 'Workflow executions for issue resolution';
COMMENT ON TABLE agent_alerts IS 'System alerts and notifications';
COMMENT ON TABLE agent_config IS 'Agent configuration storage';
COMMENT ON TABLE agent_metrics IS 'Performance and operational metrics';

-- Create views for common queries
CREATE OR REPLACE VIEW agent_active_issues AS
SELECT 
    issue_id,
    severity,
    status,
    issue_type,
    symbol,
    date,
    description,
    detected_at,
    vendor,
    EXTRACT(EPOCH FROM (NOW() - detected_at))/3600 AS age_hours
FROM agent_issues 
WHERE status IN ('open', 'in_progress')
ORDER BY 
    CASE severity 
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'medium' THEN 3
        WHEN 'low' THEN 4
    END,
    detected_at DESC;

CREATE OR REPLACE VIEW agent_active_workflows AS
SELECT 
    w.workflow_id,
    w.issue_id,
    w.state,
    w.tool_name,
    w.started_at,
    w.progress,
    i.severity,
    i.symbol,
    EXTRACT(EPOCH FROM (NOW() - w.started_at))/60 AS runtime_minutes
FROM agent_workflows w
JOIN agent_issues i ON w.issue_id = i.issue_id
WHERE w.state IN ('pending', 'running', 'paused')
ORDER BY w.started_at DESC;

CREATE OR REPLACE VIEW agent_recent_alerts AS
SELECT 
    alert_id,
    severity,
    type,
    title,
    description,
    created_at,
    acknowledged,
    resolved,
    EXTRACT(EPOCH FROM (NOW() - created_at))/60 AS age_minutes
FROM agent_alerts 
WHERE created_at >= NOW() - INTERVAL '24 hours'
ORDER BY 
    CASE severity 
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'medium' THEN 3
        WHEN 'low' THEN 4
    END,
    created_at DESC;

-- Create function to get system health summary
CREATE OR REPLACE FUNCTION get_agent_health_summary()
RETURNS TABLE (
    total_issues INTEGER,
    critical_issues INTEGER,
    high_priority_issues INTEGER,
    active_workflows INTEGER,
    unresolved_alerts INTEGER,
    avg_resolution_time_hours NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        (SELECT COUNT(*)::INTEGER FROM agent_issues WHERE status IN ('open', 'in_progress')),
        (SELECT COUNT(*)::INTEGER FROM agent_issues WHERE severity = 'critical' AND status IN ('open', 'in_progress')),
        (SELECT COUNT(*)::INTEGER FROM agent_issues WHERE severity = 'high' AND status IN ('open', 'in_progress')),
        (SELECT COUNT(*)::INTEGER FROM agent_workflows WHERE state IN ('pending', 'running', 'paused')),
        (SELECT COUNT(*)::INTEGER FROM agent_alerts WHERE resolved = FALSE),
        (SELECT ROUND(AVG(EXTRACT(EPOCH FROM (resolved_at - detected_at))/3600), 2) 
         FROM agent_issues 
         WHERE status = 'resolved' AND resolved_at IS NOT NULL AND detected_at >= NOW() - INTERVAL '7 days');
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_agent_health_summary() IS 'Returns summary statistics for agent health dashboard';

-- Add sample data for testing (optional - remove in production)
-- INSERT INTO agent_issues (issue_id, severity, status, issue_type, symbol, date, description, vendor)
-- VALUES ('test_001', 'medium', 'open', 'missing_data', 'AAPL', CURRENT_DATE - 1, 'Test issue for validation', 'polygon');

-- Vacuum and analyze tables
VACUUM ANALYZE agent_issues;
VACUUM ANALYZE agent_workflows;
VACUUM ANALYZE agent_alerts;
VACUUM ANALYZE agent_config;
VACUUM ANALYZE agent_metrics;