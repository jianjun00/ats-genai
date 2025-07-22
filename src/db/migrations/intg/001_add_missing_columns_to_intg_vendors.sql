-- Integration-only migration: Ensure intg_vendors has all columns required for integration tests
DO $$
BEGIN
    BEGIN
        ALTER TABLE intg_vendors ADD COLUMN website TEXT;
    EXCEPTION
        WHEN duplicate_column THEN NULL;
    END;
    BEGIN
        ALTER TABLE intg_vendors ADD COLUMN api_key_env_var TEXT;
    EXCEPTION
        WHEN duplicate_column THEN NULL;
    END;
END $$;
