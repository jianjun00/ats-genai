-- Migration 014: Remove NOT NULL constraint from symbol column in all tables where it exists

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT table_name
             FROM information_schema.columns
             WHERE column_name = 'symbol'
               AND is_nullable = 'NO'
               AND table_schema = 'public'
               AND table_name NOT IN ('instrument_polygon')
    LOOP
        EXECUTE format('ALTER TABLE %I ALTER COLUMN symbol DROP NOT NULL', r.table_name);
    END LOOP;
END $$;
