-- Migration 017: Add id column to vendors, set as PK, drop vendor_id PK and NOT NULL

-- 1. Add id column if missing
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS id SERIAL;

-- 2. Drop NOT NULL from vendor_id
ALTER TABLE vendors ALTER COLUMN vendor_id DROP NOT NULL;

-- 3. Drop old PK constraint on vendor_id
DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT tc.constraint_name INTO constraint_name
    FROM information_schema.table_constraints tc
    WHERE tc.table_name = 'vendors' AND tc.constraint_type = 'PRIMARY KEY';
    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE vendors DROP CONSTRAINT %I CASCADE', constraint_name);
    END IF;
END $$;

-- 4. Set id as new PK
ALTER TABLE vendors ADD PRIMARY KEY (id);
