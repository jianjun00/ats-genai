-- Migration 013: Add instrument_id if missing, and change PK from symbol to instrument_id if needed

-- 1. Daily prices
ALTER TABLE daily_prices ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT tc.constraint_name INTO constraint_name
    FROM information_schema.table_constraints tc
    WHERE tc.table_name = 'daily_prices' AND tc.constraint_type = 'PRIMARY KEY';
    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE daily_prices DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;
ALTER TABLE daily_prices ADD CONSTRAINT dp_pk PRIMARY KEY (date, instrument_id);

-- 2. Daily prices tiingo
ALTER TABLE daily_prices_tiingo ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT tc.constraint_name INTO constraint_name
    FROM information_schema.table_constraints tc
    WHERE tc.table_name = 'daily_prices_tiingo' AND tc.constraint_type = 'PRIMARY KEY';
    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE daily_prices_tiingo DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;
ALTER TABLE daily_prices_tiingo ADD CONSTRAINT dpt_pk PRIMARY KEY (date, instrument_id);

-- 3. Daily prices polygon
ALTER TABLE daily_prices_polygon ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT tc.constraint_name INTO constraint_name
    FROM information_schema.table_constraints tc
    WHERE tc.table_name = 'daily_prices_polygon' AND tc.constraint_type = 'PRIMARY KEY';
    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE daily_prices_polygon DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;
ALTER TABLE daily_prices_polygon ADD CONSTRAINT dpp_pk PRIMARY KEY (date, instrument_id);

-- 4. Stock splits
ALTER TABLE stock_splits ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT tc.constraint_name INTO constraint_name
    FROM information_schema.table_constraints tc
    WHERE tc.table_name = 'stock_splits' AND tc.constraint_type = 'PRIMARY KEY';
    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE stock_splits DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;
ALTER TABLE stock_splits ADD CONSTRAINT ss_pk PRIMARY KEY (ex_date, instrument_id);

-- 5. Dividends
ALTER TABLE dividends ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT tc.constraint_name INTO constraint_name
    FROM information_schema.table_constraints tc
    WHERE tc.table_name = 'dividends' AND tc.constraint_type = 'PRIMARY KEY';
    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE dividends DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;
ALTER TABLE dividends ADD CONSTRAINT d_pk PRIMARY KEY (ex_date, instrument_id);

-- 6. Fundamentals
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT tc.constraint_name INTO constraint_name
    FROM information_schema.table_constraints tc
    WHERE tc.table_name = 'fundamentals' AND tc.constraint_type = 'PRIMARY KEY';
    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE fundamentals DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;
ALTER TABLE fundamentals ADD CONSTRAINT fund_pk PRIMARY KEY (instrument_id, date);

-- 7. Daily market cap
ALTER TABLE daily_market_cap ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT tc.constraint_name INTO constraint_name
    FROM information_schema.table_constraints tc
    WHERE tc.table_name = 'daily_market_cap' AND tc.constraint_type = 'PRIMARY KEY';
    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE daily_market_cap DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;
ALTER TABLE daily_market_cap ADD CONSTRAINT dmc_pk PRIMARY KEY (instrument_id, date);

-- 8. Universe membership
ALTER TABLE universe_membership ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT tc.constraint_name INTO constraint_name
    FROM information_schema.table_constraints tc
    WHERE tc.table_name = 'universe_membership' AND tc.constraint_type = 'PRIMARY KEY';
    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE universe_membership DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;
ALTER TABLE universe_membership ADD CONSTRAINT um_pk PRIMARY KEY (universe_id, instrument_id, start_at);

-- 9. Universe membership changes
ALTER TABLE universe_membership_changes ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT tc.constraint_name INTO constraint_name
    FROM information_schema.table_constraints tc
    WHERE tc.table_name = 'universe_membership_changes' AND tc.constraint_type = 'PRIMARY KEY';
    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE universe_membership_changes DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;
ALTER TABLE universe_membership_changes ADD CONSTRAINT umc_pk PRIMARY KEY (universe_id, instrument_id, action, effective_date);
