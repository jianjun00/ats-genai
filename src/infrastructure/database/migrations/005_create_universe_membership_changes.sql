-- Migration 005: Create universe_membership_changes table for tracking universe membership events

CREATE TABLE IF NOT EXISTS universe_membership_changes (
    universe_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    action TEXT NOT NULL,
    effective_date DATE NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (universe_id, symbol, action, effective_date)
);

-- For environment-specific tables (integration, test, prod), the migration runner should apply prefixing automatically.
