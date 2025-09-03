-- Migration 016: Drop instrument_aliases and instrument_metadata tables

DROP TABLE IF EXISTS instrument_aliases CASCADE;
DROP TABLE IF EXISTS instrument_metadata CASCADE;
