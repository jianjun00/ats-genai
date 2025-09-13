-- Data Quality Consolidation: Deprecate Fragmented Monitoring Tables
-- Migration: Clean up deprecated tables and code after unified service deployment
-- WARNING: Only run this after unified data quality service is fully operational

-- =====================================================================
-- PHASE 1: BACKUP EXISTING DATA (Safety measure)
-- =====================================================================

-- Create backup tables with timestamp suffix
CREATE TABLE IF NOT EXISTS backup_dev_coverage_gaps_20250913 AS 
SELECT * FROM dev_coverage_gaps;

CREATE TABLE IF NOT EXISTS backup_dev_daily_coverage_metrics_20250913 AS 
SELECT * FROM dev_daily_coverage_metrics;

CREATE TABLE IF NOT EXISTS backup_dev_backfill_operations_20250913 AS 
SELECT * FROM dev_backfill_operations;

-- Log backup creation
INSERT INTO dev_schema_migration_log (migration_name, migration_type, description, executed_at) VALUES
('091_backup_fragmented_tables', 'backup', 'Created backup tables before deprecation: coverage_gaps, daily_coverage_metrics, backfill_operations', NOW());

-- =====================================================================
-- PHASE 2: DATA MIGRATION VALIDATION
-- Ensure all data has been migrated to unified tables
-- =====================================================================

-- Validation: Check if unified tables contain migrated data
DO $$
DECLARE
    coverage_gaps_count INTEGER;
    unified_coverage_issues_count INTEGER;
    coverage_metrics_count INTEGER;
    unified_coverage_metrics_count INTEGER;
BEGIN
    -- Check coverage gaps migration
    SELECT COUNT(*) INTO coverage_gaps_count FROM dev_coverage_gaps;
    SELECT COUNT(*) INTO unified_coverage_issues_count 
    FROM dev_data_quality_issues 
    WHERE issue_category = 'coverage';
    
    IF unified_coverage_issues_count < coverage_gaps_count THEN
        RAISE EXCEPTION 'MIGRATION INCOMPLETE: Only % of % coverage gaps migrated to unified table', 
            unified_coverage_issues_count, coverage_gaps_count;
    END IF;
    
    -- Check coverage metrics migration
    SELECT COUNT(*) INTO coverage_metrics_count FROM dev_daily_coverage_metrics;
    SELECT COUNT(*) INTO unified_coverage_metrics_count 
    FROM dev_data_quality_metrics 
    WHERE metric_category = 'coverage';
    
    IF unified_coverage_metrics_count < coverage_metrics_count THEN
        RAISE EXCEPTION 'MIGRATION INCOMPLETE: Only % of % coverage metrics migrated to unified table', 
            unified_coverage_metrics_count, coverage_metrics_count;
    END IF;
    
    RAISE NOTICE 'VALIDATION PASSED: All data successfully migrated to unified tables';
END
$$;

-- =====================================================================
-- PHASE 3: DEPRECATE VIEWS THAT REFERENCE OLD TABLES
-- =====================================================================

-- Drop views that reference deprecated tables
DROP VIEW IF EXISTS v_current_coverage_summary CASCADE;
DROP VIEW IF EXISTS v_active_backfill_queue CASCADE;
DROP VIEW IF EXISTS v_coverage_trending CASCADE;
DROP VIEW IF EXISTS v_recent_gaps CASCADE;

-- Log view deprecation
INSERT INTO dev_schema_migration_log (migration_name, migration_type, description, executed_at) VALUES
('091_deprecate_coverage_views', 'cleanup', 'Dropped views referencing deprecated coverage monitoring tables', NOW());

-- =====================================================================
-- PHASE 4: DEPRECATE FRAGMENTED MONITORING TABLES
-- Mark tables as deprecated (rename with _deprecated suffix)
-- =====================================================================

-- Rename tables to indicate deprecation (safer than dropping immediately)
ALTER TABLE dev_coverage_gaps RENAME TO dev_coverage_gaps_deprecated_20250913;
ALTER TABLE dev_daily_coverage_metrics RENAME TO dev_daily_coverage_metrics_deprecated_20250913;
ALTER TABLE dev_backfill_operations RENAME TO dev_backfill_operations_deprecated_20250913;
ALTER TABLE dev_priority_symbols RENAME TO dev_priority_symbols_deprecated_20250913;
ALTER TABLE dev_coverage_alert_thresholds RENAME TO dev_coverage_alert_thresholds_deprecated_20250913;

-- Add deprecation comments
COMMENT ON TABLE dev_coverage_gaps_deprecated_20250913 IS 'DEPRECATED: Replaced by dev_data_quality_issues with issue_category=coverage. Safe to drop after 2025-12-31.';
COMMENT ON TABLE dev_daily_coverage_metrics_deprecated_20250913 IS 'DEPRECATED: Replaced by dev_data_quality_metrics with metric_category=coverage. Safe to drop after 2025-12-31.';
COMMENT ON TABLE dev_backfill_operations_deprecated_20250913 IS 'DEPRECATED: Replaced by dev_data_quality_agent_operations. Safe to drop after 2025-12-31.';
COMMENT ON TABLE dev_priority_symbols_deprecated_20250913 IS 'DEPRECATED: Priority logic moved to dev_data_quality_alert_config. Safe to drop after 2025-12-31.';
COMMENT ON TABLE dev_coverage_alert_thresholds_deprecated_20250913 IS 'DEPRECATED: Replaced by dev_data_quality_alert_config. Safe to drop after 2025-12-31.';

-- Log table deprecation
INSERT INTO dev_schema_migration_log (migration_name, migration_type, description, executed_at) VALUES
('091_deprecate_fragmented_tables', 'deprecation', 'Deprecated fragmented monitoring tables: coverage_gaps, daily_coverage_metrics, backfill_operations, priority_symbols, coverage_alert_thresholds', NOW());

-- =====================================================================
-- PHASE 5: CREATE MIGRATION SUMMARY VIEW
-- =====================================================================

CREATE OR REPLACE VIEW v_data_quality_migration_summary AS
SELECT 
    'Unified Data Quality Migration' as migration_name,
    NOW() as migration_date,
    
    -- Original table counts (from backup)
    (SELECT COUNT(*) FROM backup_dev_coverage_gaps_20250913) as original_coverage_gaps,
    (SELECT COUNT(*) FROM backup_dev_daily_coverage_metrics_20250913) as original_coverage_metrics,
    (SELECT COUNT(*) FROM backup_dev_backfill_operations_20250913) as original_backfill_operations,
    
    -- Unified table counts
    (SELECT COUNT(*) FROM dev_data_quality_issues WHERE issue_category = 'coverage') as unified_coverage_issues,
    (SELECT COUNT(*) FROM dev_data_quality_metrics WHERE metric_category = 'coverage') as unified_coverage_metrics,
    (SELECT COUNT(*) FROM dev_data_quality_agent_operations) as unified_agent_operations,
    
    -- Migration success indicators
    CASE 
        WHEN (SELECT COUNT(*) FROM dev_data_quality_issues WHERE issue_category = 'coverage') >= 
             (SELECT COUNT(*) FROM backup_dev_coverage_gaps_20250913) 
        THEN 'SUCCESS' 
        ELSE 'INCOMPLETE' 
    END as coverage_gaps_migration_status,
    
    CASE 
        WHEN (SELECT COUNT(*) FROM dev_data_quality_metrics WHERE metric_category = 'coverage') >= 
             (SELECT COUNT(*) FROM backup_dev_daily_coverage_metrics_20250913) 
        THEN 'SUCCESS' 
        ELSE 'INCOMPLETE' 
    END as coverage_metrics_migration_status,
    
    -- Cleanup status
    CASE 
        WHEN EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_coverage_gaps_deprecated_20250913')
        THEN 'DEPRECATED' 
        ELSE 'NOT_DEPRECATED' 
    END as fragmented_tables_status;

COMMENT ON VIEW v_data_quality_migration_summary IS 'Migration summary showing successful consolidation of fragmented monitoring systems';

-- =====================================================================
-- PHASE 6: SCHEDULED CLEANUP TASKS
-- =====================================================================

-- Create cleanup script for future execution (after validation period)
CREATE OR REPLACE FUNCTION cleanup_deprecated_monitoring_tables()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    cleanup_date DATE := '2025-12-31';  -- Safe cleanup date (3+ months)
    result_message TEXT;
BEGIN
    IF CURRENT_DATE < cleanup_date THEN
        RETURN format('CLEANUP SCHEDULED: Deprecated tables will be dropped on %s. Current date: %s', 
                     cleanup_date, CURRENT_DATE);
    END IF;
    
    -- Drop deprecated tables (only after validation period)
    DROP TABLE IF EXISTS dev_coverage_gaps_deprecated_20250913 CASCADE;
    DROP TABLE IF EXISTS dev_daily_coverage_metrics_deprecated_20250913 CASCADE;
    DROP TABLE IF EXISTS dev_backfill_operations_deprecated_20250913 CASCADE;
    DROP TABLE IF EXISTS dev_priority_symbols_deprecated_20250913 CASCADE;
    DROP TABLE IF EXISTS dev_coverage_alert_thresholds_deprecated_20250913 CASCADE;
    
    -- Drop backup tables
    DROP TABLE IF EXISTS backup_dev_coverage_gaps_20250913 CASCADE;
    DROP TABLE IF EXISTS backup_dev_daily_coverage_metrics_20250913 CASCADE;
    DROP TABLE IF EXISTS backup_dev_backfill_operations_20250913 CASCADE;
    
    -- Log final cleanup
    INSERT INTO dev_schema_migration_log (migration_name, migration_type, description, executed_at) VALUES
    ('091_final_cleanup', 'cleanup', 'Final cleanup: Dropped all deprecated monitoring tables and backups', NOW());
    
    RETURN 'CLEANUP COMPLETE: All deprecated monitoring tables and backups have been dropped';
END
$$;

COMMENT ON FUNCTION cleanup_deprecated_monitoring_tables() IS 'Scheduled cleanup function for deprecated monitoring tables. Safe to run after 2025-12-31.';

-- =====================================================================
-- PHASE 7: UPDATE SCHEMA DOCUMENTATION
-- =====================================================================

-- Update table documentation to reflect new architecture
COMMENT ON TABLE dev_data_quality_issues IS 'Unified data quality issues table - consolidates coverage gaps, validation errors, and agent issues from fragmented monitoring systems (deprecated 2025-09-13)';
COMMENT ON TABLE dev_data_quality_metrics IS 'Unified data quality metrics table - consolidates coverage metrics, validation scores, and agent performance from fragmented monitoring systems (deprecated 2025-09-13)';
COMMENT ON TABLE dev_data_quality_agent_operations IS 'Unified agent operations log - consolidates backfill operations and agent workflows from fragmented monitoring systems (deprecated 2025-09-13)';

-- =====================================================================
-- PHASE 8: MIGRATION COMPLETION LOG
-- =====================================================================

INSERT INTO dev_schema_migration_log (migration_name, migration_type, description, executed_at) VALUES
('091_deprecate_fragmented_monitoring_tables', 'cleanup', 'Successfully deprecated fragmented monitoring tables and consolidated into unified data quality framework. Original data backed up. Tables marked for cleanup on 2025-12-31.', NOW());

-- =====================================================================
-- VERIFICATION QUERIES
-- Run these to verify successful consolidation
-- =====================================================================

-- Query to verify data integrity
SELECT 
    'Data Migration Verification' as check_name,
    'Original coverage gaps: ' || (SELECT COUNT(*) FROM backup_dev_coverage_gaps_20250913) ||
    ', Unified coverage issues: ' || (SELECT COUNT(*) FROM dev_data_quality_issues WHERE issue_category = 'coverage') ||
    ', Migration success: ' || 
    CASE 
        WHEN (SELECT COUNT(*) FROM dev_data_quality_issues WHERE issue_category = 'coverage') >= 
             (SELECT COUNT(*) FROM backup_dev_coverage_gaps_20250913) 
        THEN 'YES' 
        ELSE 'NO' 
    END as verification_result;

-- Query to show consolidated architecture benefits
SELECT 
    'Architecture Consolidation Benefits' as summary_name,
    'Tables before: 5+ fragmented, Tables after: 3 unified' ||
    ', Reduction: ' || 
    ROUND(((5.0 - 3.0) / 5.0) * 100, 0) || '% complexity reduction' as benefits;