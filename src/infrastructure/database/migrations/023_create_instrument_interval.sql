-- Migration 022b: Create instrument_interval table only

-- Ensure PK exists on universe_state_interval for FK reference
ALTER TABLE universe_state_interval
    ADD COLUMN IF NOT EXISTS id SERIAL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'universe_state_interval'::regclass
          AND contype = 'p'
    ) THEN
        ALTER TABLE universe_state_interval
            ADD CONSTRAINT universe_state_interval_pkey PRIMARY KEY (id);
    ELSE
        -- If a primary key already exists (likely composite), ensure 'id' is UNIQUE so it can be referenced by FKs
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE c.contype = 'u'
              AND t.relname = 'universe_state_interval'
              AND c.conkey = ARRAY[
                  (SELECT attnum FROM pg_attribute WHERE attrelid = 'universe_state_interval'::regclass AND attname = 'id')
              ]
        ) THEN
            ALTER TABLE universe_state_interval
                ADD CONSTRAINT universe_state_interval_id_key UNIQUE (id);
        END IF;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS instrument_interval (
    id SERIAL PRIMARY KEY,
    universe_state_interval_id INTEGER NOT NULL REFERENCES universe_state_interval(id) ON DELETE CASCADE,
    instrument_id INTEGER NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    traded_volume DOUBLE PRECISION,
    traded_dollar DOUBLE PRECISION,
    status VARCHAR(16),
    market_cap DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (universe_state_interval_id, instrument_id)
);
