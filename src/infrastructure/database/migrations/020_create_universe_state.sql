-- Migration 020: Create universe_state table for TimescaleDB persistence of universe state

CREATE EXTENSION IF NOT EXISTS timescaledb;
