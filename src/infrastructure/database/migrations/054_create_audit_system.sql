-- Migration 054: Create Historic Audit System
-- Creates audit tables and triggers for complete database auditing

-- Create audit function that works for any table
CREATE OR REPLACE FUNCTION audit_trigger_function()
RETURNS TRIGGER AS $$
DECLARE
    audit_table_name TEXT;
    insert_query TEXT;
BEGIN
    audit_table_name := TG_TABLE_NAME || '_audit';
    
    IF TG_OP = 'DELETE' THEN
        insert_query := format('INSERT INTO %I (audit_action, audit_timestamp, audit_user, audit_session_id, original_data) VALUES ($1, $2, $3, $4, $5)',
                              audit_table_name);
        EXECUTE insert_query USING 'DELETE', now(), current_user, to_hex(pg_backend_pid()), row_to_json(OLD);
        RETURN OLD;
        
    ELSIF TG_OP = 'UPDATE' THEN
        insert_query := format('INSERT INTO %I (audit_action, audit_timestamp, audit_user, audit_session_id, original_data, new_data) VALUES ($1, $2, $3, $4, $5, $6)',
                              audit_table_name);
        EXECUTE insert_query USING 'UPDATE', now(), current_user, to_hex(pg_backend_pid()), row_to_json(OLD), row_to_json(NEW);
        RETURN NEW;
        
    ELSIF TG_OP = 'INSERT' THEN
        insert_query := format('INSERT INTO %I (audit_action, audit_timestamp, audit_user, audit_session_id, new_data) VALUES ($1, $2, $3, $4, $5)',
                              audit_table_name);
        EXECUTE insert_query USING 'INSERT', now(), current_user, to_hex(pg_backend_pid()), row_to_json(NEW);
        RETURN NEW;
    END IF;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create function to create audit table for any given table
CREATE OR REPLACE FUNCTION create_audit_table_for(table_name TEXT)
RETURNS VOID AS $$
DECLARE
    audit_table_name TEXT;
    create_sql TEXT;
    trigger_sql TEXT;
BEGIN
    audit_table_name := table_name || '_audit';
    
    -- Create audit table with JSONB columns for flexibility
    create_sql := format('CREATE TABLE IF NOT EXISTS %I (
        audit_id BIGSERIAL PRIMARY KEY,
        audit_action TEXT NOT NULL CHECK (audit_action IN (''INSERT'', ''UPDATE'', ''DELETE'')),
        audit_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
        audit_user TEXT NOT NULL DEFAULT current_user,
        audit_session_id TEXT NOT NULL DEFAULT to_hex(pg_backend_pid()),
        original_data JSONB,
        new_data JSONB
    )', audit_table_name);
    
    EXECUTE create_sql;
    
    -- Create indexes for efficient querying
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_timestamp ON %I (audit_timestamp)',
                   audit_table_name, audit_table_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_action ON %I (audit_action)',
                   audit_table_name, audit_table_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_user ON %I (audit_user)',
                   audit_table_name, audit_table_name);
    
    -- Create trigger
    trigger_sql := format('CREATE TRIGGER %I_audit_trigger
        AFTER INSERT OR UPDATE OR DELETE ON %I
        FOR EACH ROW EXECUTE FUNCTION audit_trigger_function()',
        table_name, table_name);
    
    EXECUTE trigger_sql;
    
    RAISE NOTICE 'Created audit table % with trigger for %', audit_table_name, table_name;
END;
$$ LANGUAGE plpgsql;