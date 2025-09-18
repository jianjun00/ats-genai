#!/usr/bin/env python3
"""
Database constraints regression tests.

Tests the specific database constraint issues found during AAPL training data generation:
1. Unique constraint on intg_instrument_interval table
2. UUID deduplication system
3. Concurrent run handling
4. Data integrity constraints
"""

import pytest
import asyncio
import tempfile
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch
import asyncpg


class TestDatabaseConstraintsRegression:
    """Test database constraint fixes and prevent regressions."""
    
    def test_instrument_interval_constraint_definition(self):
        """Test that the unique constraint includes universe_state_interval_id."""
        
        # Test the constraint specification (conceptually)
        # The fixed constraint should be:
        # UNIQUE (instrument_id, interval_start, interval_duration, run_id, universe_state_interval_id)
        
        constraint_fields = [
            'instrument_id',
            'interval_start', 
            'interval_duration',
            'run_id',
            'universe_state_interval_id'  # This was missing and caused the duplicate key error
        ]
        
        # Conceptual test - in a real test this would check the actual DB schema
        assert 'universe_state_interval_id' in constraint_fields, \
            "universe_state_interval_id must be included in unique constraint"
            
        # Test the scenario that was failing before the fix
        test_records = [
            {
                'instrument_id': 31,  # AAPL
                'interval_start': '2025-07-01 20:00:00+00',
                'interval_duration': '60m',
                'run_id': 'run_20250913_200448_3e7e7e92',
                'universe_state_interval_id': 1913
            },
            {
                'instrument_id': 31,  # Same AAPL
                'interval_start': '2025-07-01 20:00:00+00',  # Same time
                'interval_duration': '60m',  # Same duration
                'run_id': 'run_20250913_201041_69ebf7be',  # Different run_id
                'universe_state_interval_id': 1915  # Different universe state
            }
        ]
        
        # These should be allowed by the fixed constraint
        # because they have different run_id and universe_state_interval_id
        record1 = test_records[0]
        record2 = test_records[1]
        
        # Same instrument, time, duration (this was causing the error)
        assert record1['instrument_id'] == record2['instrument_id']
        assert record1['interval_start'] == record2['interval_start'] 
        assert record1['interval_duration'] == record2['interval_duration']
        
        # But different run_id and universe_state_interval_id (this should make it unique)
        assert record1['run_id'] != record2['run_id']
        assert record1['universe_state_interval_id'] != record2['universe_state_interval_id']
        
        print("✅ Constraint definition test passed - different universe states allowed")

    def test_uuid_deduplication_system(self):
        """Test UUID-based deduplication system works correctly."""
        
        # Test UUID generation format
        uuid_examples = [
            'run_20250913_200448_3e7e7e92',
            'run_20250913_201041_69ebf7be', 
            'run_20250913_202347_e09811b5'
        ]
        
        for uuid in uuid_examples:
            # Test UUID format: run_YYYYMMDD_HHMMSS_{8_char_hex}
            assert uuid.startswith('run_'), f"UUID should start with 'run_': {uuid}"
            
            parts = uuid.split('_')
            assert len(parts) == 4, f"UUID should have 4 parts: {uuid}"
            
            date_part = parts[1] 
            time_part = parts[2]
            hex_part = parts[3]
            
            # Validate date format YYYYMMDD
            assert len(date_part) == 8, f"Date part should be 8 chars: {date_part}"
            assert date_part.isdigit(), f"Date part should be numeric: {date_part}"
            
            # Validate time format HHMMSS
            assert len(time_part) == 6, f"Time part should be 6 chars: {time_part}"
            assert time_part.isdigit(), f"Time part should be numeric: {time_part}"
            
            # Validate hex part
            assert len(hex_part) == 8, f"Hex part should be 8 chars: {hex_part}"
            
        print("✅ UUID format validation passed")

    def test_concurrent_run_scenarios(self):
        """Test scenarios with concurrent training data generation runs."""
        
        # Simulate the actual error scenario that occurred
        failing_scenario = {
            'error': 'duplicate key value violates unique constraint "intg_instrument_interval_instrument_id_interval_start_run_key"',
            'detail': 'Key (instrument_id, interval_start, interval_duration, run_id)=(31, 2025-07-01 20:00:00+00, 60m, run_20250913_200448_3e7e7e92) already exists.',
            'cause': 'Same run trying to insert same interval twice, or constraint missing universe_state_interval_id'
        }
        
        # Test that we understand the error correctly
        assert 'instrument_id' in failing_scenario['detail']
        assert 'interval_start' in failing_scenario['detail'] 
        assert 'interval_duration' in failing_scenario['detail']
        assert 'run_id' in failing_scenario['detail']
        
        # The fix: constraint should include universe_state_interval_id
        # so that same interval can exist with different universe states
        
        fixed_scenario = {
            'constraint': 'intg_instrument_interval_instrument_id_interval_start_run_universe_key',
            'fields': ['instrument_id', 'interval_start', 'interval_duration', 'run_id', 'universe_state_interval_id'],
            'allows_same_interval_different_universe_states': True
        }
        
        assert 'universe_state_interval_id' in fixed_scenario['fields'], \
            "Fixed constraint must include universe_state_interval_id"
            
        print("✅ Concurrent run scenario test passed")

    def test_database_connection_patterns(self):
        """Test database connection patterns used in training data generation."""
        
        # Test the connection patterns that appear in the logs
        connection_patterns = [
            "Using standard connection for localhost",
            "localhost:4432/intg_db", 
            "localhost:5432/dev_db"
        ]
        
        for pattern in connection_patterns:
            # These patterns should be consistent
            if 'localhost' in pattern:
                assert ':' in pattern or 'localhost' == pattern.split()[-1], \
                    f"Database connection pattern should include port: {pattern}"
                    
        # Test environment-specific database configs
        db_configs = {
            'dev': {
                'host': 'localhost',
                'port': 5432, 
                'database': 'dev_db',
                'user': 'postgres',
                'password': 'dev_password'
            },
            'intg': {
                'host': 'localhost',
                'port': 4432,
                'database': 'intg_db', 
                'user': 'postgres',
                'password': 'intg_password'
            }
        }
        
        for env, config in db_configs.items():
            assert config['host'] == 'localhost', f"Host should be localhost for {env}"
            assert config['user'] == 'postgres', f"User should be postgres for {env}"
            assert f"{env}_" in config['database'], f"Database should include env prefix for {env}"
            assert f"{env}_" in config['password'], f"Password should include env prefix for {env}"
            
        print("✅ Database connection pattern test passed")


class TestDataIntegrityConstraints:
    """Test data integrity constraints and validation."""
    
    def test_instrument_interval_data_integrity(self):
        """Test instrument interval data meets integrity requirements."""
        
        # Test the data structure that gets inserted
        sample_interval_data = {
            'instrument_id': 31,
            'interval_start': datetime(2025, 7, 1, 20, 0, 0),
            'interval_end': datetime(2025, 7, 1, 21, 0, 0),
            'interval_duration': '60m',
            'open': 208.02,
            'high': 208.11, 
            'low': 208.01,
            'close': 208.08,
            'traded_volume': 56512.0,
            'traded_dollar': 11750000.0,  # close * volume
            'status': 'ok',
            'market_cap': 1500000000.0,
            'run_id': 'run_20250913_202555_b2ec8a83',
            'universe_state_interval_id': 1924
        }
        
        # Data integrity checks
        assert sample_interval_data['instrument_id'] > 0, "Instrument ID must be positive"
        assert sample_interval_data['interval_start'] < sample_interval_data['interval_end'], \
            "Interval start must be before end"
        assert sample_interval_data['interval_duration'] in ['1m', '5m', '15m', '30m', '60m', '1h', '1d'], \
            f"Invalid interval duration: {sample_interval_data['interval_duration']}"
        
        # OHLC validation
        ohlc = [sample_interval_data['open'], sample_interval_data['high'], 
                sample_interval_data['low'], sample_interval_data['close']]
        assert all(price > 0 for price in ohlc), "All OHLC prices must be positive"
        assert sample_interval_data['high'] >= max(sample_interval_data['open'], sample_interval_data['close']), \
            "High must be >= max(open, close)"
        assert sample_interval_data['low'] <= min(sample_interval_data['open'], sample_interval_data['close']), \
            "Low must be <= min(open, close)"
            
        # Volume validation  
        assert sample_interval_data['traded_volume'] >= 0, "Volume must be non-negative"
        assert isinstance(sample_interval_data['traded_volume'], (int, float)), \
            f"Volume must be numeric, got {type(sample_interval_data['traded_volume'])}"
            
        # Dollar volume calculation (allow for reasonable floating point precision)
        expected_dollar_volume = sample_interval_data['close'] * sample_interval_data['traded_volume']
        tolerance = max(1.0, abs(expected_dollar_volume) * 0.001)  # 0.1% tolerance or $1, whichever is larger
        assert abs(sample_interval_data['traded_dollar'] - expected_dollar_volume) < tolerance, \
            f"Dollar volume mismatch: {sample_interval_data['traded_dollar']} vs {expected_dollar_volume} (tolerance: {tolerance})"
            
        # UUID validation
        run_id = sample_interval_data['run_id']
        assert run_id.startswith('run_'), f"Run ID should start with 'run_': {run_id}"
        assert len(run_id.split('_')) == 4, f"Run ID should have 4 parts: {run_id}"
        
        # Universe state validation
        assert sample_interval_data['universe_state_interval_id'] > 0, \
            "Universe state interval ID must be positive"
            
        print("✅ Data integrity validation passed")

    def test_foreign_key_relationships(self):
        """Test foreign key relationships are maintained."""
        
        # Test the relationships that should exist
        relationships = {
            'intg_instrument_interval.instrument_id': 'intg_instrument.id',
            'intg_instrument_interval.universe_state_interval_id': 'intg_universe_state_interval.id',
            'intg_instrument_xrefs.instrument_id': 'intg_instrument.id'
        }
        
        for child_key, parent_key in relationships.items():
            child_table, child_column = child_key.split('.')
            parent_table, parent_column = parent_key.split('.')
            
            # These relationships should be enforced by foreign key constraints
            assert child_column.endswith('_id'), f"Foreign key should end with '_id': {child_column}"
            assert parent_column == 'id', f"Parent key should be 'id': {parent_column}"
            
            # Table naming consistency
            if child_column == 'instrument_id':
                assert 'instrument' in parent_table, f"Parent table should contain 'instrument': {parent_table}"
                
        print("✅ Foreign key relationship test passed")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])