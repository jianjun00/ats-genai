-- Migration 022: Create normalized universe state tables

-- 1. UniverseStateInterval: top-level interval per universe, duration, and time
CREATE TABLE IF NOT EXISTS universe_state_interval (
    id SERIAL PRIMARY KEY,
    universe_id INTEGER NOT NULL,
    duration VARCHAR(16) NOT NULL,
    start_date_time TIMESTAMP NOT NULL,
    end_date_time TIMESTAMP NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (universe_id, duration, start_date_time)
);

-- 2. InstrumentInterval: OHLCV for each instrument in a universe state interval
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

-- 3. InstrumentIndicatorInterval: indicators per instrument per interval
CREATE TABLE IF NOT EXISTS instrument_indicator_interval (
    id SERIAL PRIMARY KEY,
    instrument_interval_id INTEGER NOT NULL REFERENCES instrument_interval(id) ON DELETE CASCADE,
    indicator_name VARCHAR(64) NOT NULL,
    indicator_value DOUBLE PRECISION,
    indicator_status VARCHAR(16),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (instrument_interval_id, indicator_name)
);

-- 4. FactorInterval: factor data per universe state interval
CREATE TABLE IF NOT EXISTS factor_interval (
    id SERIAL PRIMARY KEY,
    universe_state_interval_id INTEGER NOT NULL REFERENCES universe_state_interval(id) ON DELETE CASCADE,
    factor_name VARCHAR(64) NOT NULL,
    factor_value DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (universe_state_interval_id, factor_name)
);
