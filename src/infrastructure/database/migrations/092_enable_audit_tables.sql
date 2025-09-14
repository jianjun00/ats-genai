-- Migration 055: Enable Auditing for All Existing Tables
-- Executes the audit system setup for all current tables

-- Create audit tables for all existing tables
DO $$
DECLARE
    table_record RECORD;
    table_count INTEGER := 0;
BEGIN
    -- Loop through all user tables (excluding audit tables and system tables)
    FOR table_record IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename NOT LIKE '%_audit'
        AND tablename NOT LIKE 'pg_%'
        AND tablename NOT LIKE 'information_schema%'
        ORDER BY tablename
    LOOP
        PERFORM create_audit_table_for(table_record.tablename);
        table_count := table_count + 1;
    END LOOP;
    
    RAISE NOTICE 'Enabled auditing for % tables', table_count;
END;
$$;

-- Verify audit system is working
DO $$
DECLARE
    audit_tables_count INTEGER;
    triggers_count INTEGER;
BEGIN
    -- Count audit tables created
    SELECT COUNT(*)
    INTO audit_tables_count
    FROM pg_tables
    WHERE schemaname = 'public'
    AND tablename LIKE '%_audit';
    
    -- Count audit triggers created
    SELECT COUNT(*)
    INTO triggers_count
    FROM pg_trigger t
    JOIN pg_class c ON t.tgrelid = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = 'public'
    AND t.tgname LIKE '%_audit_trigger';
    
    RAISE NOTICE 'Audit system initialized: % audit tables, % triggers created', 
                 audit_tables_count, triggers_count;
END;
$$;