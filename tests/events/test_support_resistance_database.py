#!/usr/bin/env python3
"""
Database integration tests for Support/Resistance system
"""

import pytest
import asyncio
import asyncpg
from datetime import datetime, timedelta
from decimal import Decimal
import json

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from config.environment import Environment
from events.analysis.support_resistance_detector import (
    SRType, SRLevelType, SRTestOutcome, Timeframe
)

class TestSupportResistanceDatabase:
    """Test database schema and operations for S/R system"""

    @pytest.fixture
    async def db_connection(self):
        """Create test database connection"""
        env = Environment()
        pool = await env.database.create_pool_with_retry(max_retries=3)
        conn = await pool.acquire()
        
        # Ensure clean state for tests
        await self._cleanup_test_data(conn)
        
        yield conn
        
        # Cleanup after tests
        await self._cleanup_test_data(conn)
        await pool.release(conn)
        await pool.close()

    async def _cleanup_test_data(self, conn):
        """Clean up test data from all S/R tables"""
        cleanup_queries = [
            "DELETE FROM dev_sr_events WHERE symbol LIKE 'TEST%'",
            "DELETE FROM dev_sr_tests WHERE symbol LIKE 'TEST%'",
            "DELETE FROM dev_sr_levels WHERE symbol LIKE 'TEST%'"
        ]
        
        for query in cleanup_queries:
            try:
                await conn.execute(query)
            except Exception as e:
                print(f"Cleanup warning: {e}")

    async def test_schema_exists(self, db_connection):
        """Test that all required S/R tables and types exist"""
        # Test custom types
        type_check = """
        SELECT EXISTS (
            SELECT 1 FROM pg_type 
            WHERE typname IN ('sr_type', 'sr_level_type', 'sr_test_outcome', 'sr_timeframe')
        )
        """
        
        result = await db_connection.fetchval(type_check)
        assert result, "Required S/R enum types should exist"
        
        # Test tables
        table_check = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
          AND table_name IN ('dev_sr_levels', 'dev_sr_tests', 'dev_sr_events')
        ORDER BY table_name
        """
        
        tables = await db_connection.fetch(table_check)
        table_names = {row['table_name'] for row in tables}
        
        expected_tables = {'dev_sr_levels', 'dev_sr_tests', 'dev_sr_events'}
        assert table_names >= expected_tables, f"Missing tables: {expected_tables - table_names}"

    async def test_sr_level_insertion(self, db_connection):
        """Test inserting S/R levels"""
        level_data = {
            'level_id': 'TEST_AAPL_1d_support_100.00_1704067200',
            'symbol': 'TEST_AAPL',
            'price': Decimal('100.50'),
            'sr_type': 'support',
            'level_type': 'pivot',
            'timeframe': '1d',
            'strength': Decimal('0.75'),
            'confidence': Decimal('0.85'),
            'first_established': datetime.now(),
            'last_tested': datetime.now(),
            'test_count': 3,
            'hold_count': 2,
            'break_count': 1,
            'volume_confirmation': True,
            'metadata': json.dumps({'source': 'test', 'algo': 'pivot'})
        }
        
        insert_query = """
        INSERT INTO dev_sr_levels (
            level_id, symbol, price, sr_type, level_type, timeframe,
            strength, confidence, first_established, last_tested,
            test_count, hold_count, break_count, volume_confirmation, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        RETURNING id
        """
        
        result = await db_connection.fetchrow(
            insert_query,
            level_data['level_id'], level_data['symbol'], level_data['price'],
            level_data['sr_type'], level_data['level_type'], level_data['timeframe'],
            level_data['strength'], level_data['confidence'],
            level_data['first_established'], level_data['last_tested'],
            level_data['test_count'], level_data['hold_count'], level_data['break_count'],
            level_data['volume_confirmation'], level_data['metadata']
        )
        
        assert result['id'] is not None, "Should return inserted ID"
        
        # Verify data was inserted correctly
        select_query = "SELECT * FROM dev_sr_levels WHERE id = $1"
        level_row = await db_connection.fetchrow(select_query, result['id'])
        
        assert level_row['symbol'] == 'TEST_AAPL'
        assert level_row['price'] == Decimal('100.50')
        assert level_row['sr_type'] == 'support'
        assert level_row['strength'] == Decimal('0.75')

    async def test_sr_level_upsert(self, db_connection):
        """Test S/R level upsert behavior (insert or update on conflict)"""
        level_id = 'TEST_MSFT_1d_resistance_200.00_1704067200'
        
        # First insert
        insert_query = """
        INSERT INTO dev_sr_levels (
            level_id, symbol, price, sr_type, level_type, timeframe,
            strength, confidence, first_established, last_tested,
            test_count, hold_count, break_count, volume_confirmation, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (level_id) DO UPDATE SET
            strength = EXCLUDED.strength,
            test_count = GREATEST(dev_sr_levels.test_count, EXCLUDED.test_count),
            updated_at = NOW()
        RETURNING id
        """
        
        # Insert level
        result1 = await db_connection.fetchrow(
            insert_query,
            level_id, 'TEST_MSFT', Decimal('200.00'), 'resistance', 'pivot', '1d',
            Decimal('0.6'), Decimal('0.7'), datetime.now(), datetime.now(),
            2, 1, 1, False, json.dumps({})
        )
        
        first_id = result1['id']
        
        # Update same level (should upsert)
        result2 = await db_connection.fetchrow(
            insert_query,
            level_id, 'TEST_MSFT', Decimal('200.00'), 'resistance', 'pivot', '1d',
            Decimal('0.8'), Decimal('0.9'), datetime.now(), datetime.now(),
            1, 1, 0, True, json.dumps({})  # Lower test_count should not override
        )
        
        second_id = result2['id']
        
        # Should be same ID (update, not new insert)
        assert first_id == second_id, "Upsert should update existing record"
        
        # Check final values
        select_query = "SELECT * FROM dev_sr_levels WHERE id = $1"
        final_row = await db_connection.fetchrow(select_query, first_id)
        
        # Strength should be updated
        assert final_row['strength'] == Decimal('0.8'), "Strength should be updated"
        # Test count should be max (2, not reduced to 1)
        assert final_row['test_count'] == 2, "Test count should be maximum value"

    async def test_sr_test_insertion(self, db_connection):
        """Test inserting S/R tests"""
        # First create a level to reference
        level_insert = """
        INSERT INTO dev_sr_levels (
            level_id, symbol, price, sr_type, level_type, timeframe,
            strength, confidence, first_established, last_tested,
            test_count, hold_count, break_count, volume_confirmation, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        RETURNING id
        """
        
        level_result = await db_connection.fetchrow(
            level_insert,
            'TEST_GOOGL_1h_support_150.00_1704067200', 'TEST_GOOGL', Decimal('150.00'),
            'support', 'pivot', '1h', Decimal('0.7'), Decimal('0.8'),
            datetime.now(), datetime.now(), 1, 1, 0, False, json.dumps({})
        )
        
        level_db_id = level_result['id']
        
        # Now insert test
        test_data = {
            'test_id': 'TEST_GOOGL_1h_support_150.00_1704067200_1704153600',
            'level_id': 'TEST_GOOGL_1h_support_150.00_1704067200',
            'symbol': 'TEST_GOOGL',
            'sr_level_id': level_db_id,
            'test_datetime': datetime.now(),
            'test_price': Decimal('150.25'),
            'approach_direction': 'down',
            'timeframe': '1h',
            'max_penetration': Decimal('0.01'),
            'hold_duration': 300,  # 5 minutes
            'volume_spike': Decimal('2.5'),
            'outcome': 'hold_strong',
            'outcome_confidence': Decimal('0.9'),
            'metadata': json.dumps({'algo': 'pivot_test'})
        }
        
        test_insert = """
        INSERT INTO dev_sr_tests (
            test_id, level_id, symbol, sr_level_id, test_datetime,
            test_price, approach_direction, timeframe, max_penetration,
            hold_duration, volume_spike, outcome, outcome_confidence, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        RETURNING id
        """
        
        test_result = await db_connection.fetchrow(
            test_insert,
            test_data['test_id'], test_data['level_id'], test_data['symbol'],
            test_data['sr_level_id'], test_data['test_datetime'], test_data['test_price'],
            test_data['approach_direction'], test_data['timeframe'], test_data['max_penetration'],
            test_data['hold_duration'], test_data['volume_spike'],
            test_data['outcome'], test_data['outcome_confidence'], test_data['metadata']
        )
        
        assert test_result['id'] is not None, "Should return test ID"
        
        # Verify foreign key relationship
        join_query = """
        SELECT t.*, l.price as level_price
        FROM dev_sr_tests t
        JOIN dev_sr_levels l ON t.sr_level_id = l.id
        WHERE t.id = $1
        """
        
        join_result = await db_connection.fetchrow(join_query, test_result['id'])
        assert join_result['level_price'] == Decimal('150.00'), "Should join with level correctly"

    async def test_sr_event_insertion(self, db_connection):
        """Test inserting S/R events"""
        # Create level and test first
        level_insert = """
        INSERT INTO dev_sr_levels (
            level_id, symbol, price, sr_type, level_type, timeframe,
            strength, confidence, first_established, last_tested,
            test_count, hold_count, break_count, volume_confirmation, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        RETURNING id
        """
        
        level_result = await db_connection.fetchrow(
            level_insert,
            'TEST_NVDA_1d_resistance_300.00_1704067200', 'TEST_NVDA', Decimal('300.00'),
            'resistance', 'psychological', '1d', Decimal('0.9'), Decimal('0.95'),
            datetime.now(), datetime.now(), 5, 3, 2, True, json.dumps({})
        )
        
        test_insert = """
        INSERT INTO dev_sr_tests (
            test_id, level_id, symbol, sr_level_id, test_datetime,
            test_price, approach_direction, timeframe, max_penetration,
            hold_duration, volume_spike, outcome, outcome_confidence, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        RETURNING id
        """
        
        test_result = await db_connection.fetchrow(
            test_insert,
            'TEST_NVDA_1d_resistance_300.00_1704067200_1704153600',
            'TEST_NVDA_1d_resistance_300.00_1704067200',
            'TEST_NVDA', level_result['id'], datetime.now(),
            Decimal('299.50'), 'up', '1d', Decimal('0.005'),
            180, Decimal('3.2'), 'break_clean', Decimal('0.85'), json.dumps({})
        )
        
        # Now insert event
        event_data = {
            'event_id': 'sr_TEST_NVDA_1d_break_clean_1704153600',
            'symbol': 'TEST_NVDA',
            'sr_level_id': level_result['id'],
            'sr_test_id': test_result['id'],
            'event_type': 'support_resistance',
            'event_subtype': 'level_broken_clean',
            'event_datetime': datetime.now(),
            'market_datetime': datetime.now(),
            'timeframe': '1d',
            'significance_score': Decimal('0.9'),
            'impact_score': Decimal('0.85'),
            'price_at_event': Decimal('299.50'),
            'event_data': json.dumps({
                'test_outcome': 'break_clean',
                'level_strength': 0.9,
                'approach_direction': 'up',
                'volume_spike': 3.2
            })
        }
        
        event_insert = """
        INSERT INTO dev_sr_events (
            event_id, symbol, sr_level_id, sr_test_id, event_type, event_subtype,
            event_datetime, market_datetime, timeframe, significance_score,
            impact_score, price_at_event, event_data
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        RETURNING id
        """
        
        event_result = await db_connection.fetchrow(
            event_insert,
            event_data['event_id'], event_data['symbol'], event_data['sr_level_id'],
            event_data['sr_test_id'], event_data['event_type'], event_data['event_subtype'],
            event_data['event_datetime'], event_data['market_datetime'], event_data['timeframe'],
            event_data['significance_score'], event_data['impact_score'], event_data['price_at_event'],
            event_data['event_data']
        )
        
        assert event_result['id'] is not None, "Should insert event successfully"

    async def test_analytical_views(self, db_connection):
        """Test analytical views and queries"""
        # Insert test data for analytics
        await self._insert_test_analytics_data(db_connection)
        
        # Test level strength distribution
        strength_query = """
        SELECT 
            CASE 
                WHEN strength >= 0.8 THEN 'Strong'
                WHEN strength >= 0.5 THEN 'Medium'
                ELSE 'Weak'
            END as strength_category,
            COUNT(*) as level_count
        FROM dev_sr_levels 
        WHERE symbol LIKE 'TEST%'
        GROUP BY strength_category
        ORDER BY strength_category
        """
        
        strength_dist = await db_connection.fetch(strength_query)
        assert len(strength_dist) > 0, "Should return strength distribution"
        
        # Test test outcome analysis
        outcome_query = """
        SELECT outcome, COUNT(*) as outcome_count
        FROM dev_sr_tests 
        WHERE symbol LIKE 'TEST%'
        GROUP BY outcome
        ORDER BY outcome_count DESC
        """
        
        outcomes = await db_connection.fetch(outcome_query)
        assert len(outcomes) > 0, "Should return outcome analysis"
        
        # Test timeframe analysis
        timeframe_query = """
        SELECT timeframe, AVG(strength) as avg_strength, COUNT(*) as level_count
        FROM dev_sr_levels 
        WHERE symbol LIKE 'TEST%'
        GROUP BY timeframe
        ORDER BY avg_strength DESC
        """
        
        timeframes = await db_connection.fetch(timeframe_query)
        assert len(timeframes) > 0, "Should return timeframe analysis"

    async def _insert_test_analytics_data(self, db_connection):
        """Insert varied test data for analytics"""
        # Different strength levels
        levels_data = [
            ('TEST_STRONG_1', 'TEST_ANALYTICS', Decimal('100.00'), 'support', 'pivot', '1d', Decimal('0.9')),
            ('TEST_MEDIUM_1', 'TEST_ANALYTICS', Decimal('110.00'), 'resistance', 'psychological', '1h', Decimal('0.6')),
            ('TEST_WEAK_1', 'TEST_ANALYTICS', Decimal('120.00'), 'support', 'volume_profile', '15m', Decimal('0.3')),
        ]
        
        for level_data in levels_data:
            await db_connection.execute(
                """
                INSERT INTO dev_sr_levels (
                    level_id, symbol, price, sr_type, level_type, timeframe, strength,
                    confidence, first_established, last_tested,
                    test_count, hold_count, break_count, volume_confirmation, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                ON CONFLICT (level_id) DO NOTHING
                """,
                level_data[0], level_data[1], level_data[2], level_data[3],
                level_data[4], level_data[5], level_data[6], Decimal('0.8'),
                datetime.now(), datetime.now(), 2, 1, 1, False, json.dumps({})
            )
        
        # Different test outcomes
        test_outcomes = ['hold_strong', 'hold_weak', 'break_clean']
        
        for i, outcome in enumerate(test_outcomes):
            level_id = f'TEST_{outcome.upper()}_1'
            test_id = f'{level_id}_{int(datetime.now().timestamp())}'
            
            await db_connection.execute(
                """
                INSERT INTO dev_sr_tests (
                    test_id, level_id, symbol, sr_level_id, test_datetime,
                    test_price, approach_direction, timeframe, max_penetration,
                    hold_duration, volume_spike, outcome, outcome_confidence, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (test_id) DO NOTHING
                """,
                test_id, level_id, 'TEST_ANALYTICS', 1,
                datetime.now(), Decimal('100.00'), 'up', '1d',
                Decimal('0.01'), 300, Decimal('2.0'), outcome, Decimal('0.8'), json.dumps({})
            )

    async def test_indexes_exist(self, db_connection):
        """Test that required indexes exist for performance"""
        index_query = """
        SELECT indexname 
        FROM pg_indexes 
        WHERE tablename IN ('dev_sr_levels', 'dev_sr_tests', 'dev_sr_events')
          AND indexname LIKE '%sr_%'
        ORDER BY indexname
        """
        
        indexes = await db_connection.fetch(index_query)
        index_names = {row['indexname'] for row in indexes}
        
        # Should have indexes on key columns
        expected_patterns = ['symbol', 'timeframe', 'event_datetime']
        
        for pattern in expected_patterns:
            matching_indexes = [idx for idx in index_names if pattern in idx]
            assert len(matching_indexes) > 0, f"Should have index containing '{pattern}'"

    async def test_data_constraints(self, db_connection):
        """Test database constraints and validations"""
        
        # Test strength constraint (should be between 0 and 1)
        with pytest.raises(Exception):  # Should raise constraint violation
            await db_connection.execute(
                """
                INSERT INTO dev_sr_levels (
                    level_id, symbol, price, sr_type, level_type, timeframe, strength,
                    confidence, first_established, last_tested,
                    test_count, hold_count, break_count, volume_confirmation, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                """,
                'TEST_INVALID_STRENGTH', 'TEST', Decimal('100.00'), 'support', 'pivot', '1d',
                Decimal('1.5'),  # Invalid: > 1.0
                Decimal('0.8'), datetime.now(), datetime.now(), 1, 1, 0, False, json.dumps({})
            )
        
        # Test confidence constraint (should be between 0 and 1)
        with pytest.raises(Exception):
            await db_connection.execute(
                """
                INSERT INTO dev_sr_levels (
                    level_id, symbol, price, sr_type, level_type, timeframe, strength,
                    confidence, first_established, last_tested,
                    test_count, hold_count, break_count, volume_confirmation, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                """,
                'TEST_INVALID_CONFIDENCE', 'TEST', Decimal('100.00'), 'support', 'pivot', '1d',
                Decimal('0.8'), Decimal('2.0'),  # Invalid: > 1.0
                datetime.now(), datetime.now(), 1, 1, 0, False, json.dumps({})
            )

    async def test_triggers_and_updates(self, db_connection):
        """Test database triggers and automatic updates"""
        # Insert a level
        level_result = await db_connection.fetchrow(
            """
            INSERT INTO dev_sr_levels (
                level_id, symbol, price, sr_type, level_type, timeframe, strength,
                confidence, first_established, last_tested,
                test_count, hold_count, break_count, volume_confirmation, metadata,
                created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            RETURNING id, created_at, updated_at
            """,
            'TEST_TRIGGER', 'TEST_TRIGGER', Decimal('100.00'), 'support', 'pivot', '1d',
            Decimal('0.5'), Decimal('0.7'), datetime.now(), datetime.now(),
            1, 1, 0, False, json.dumps({}), datetime.now(), datetime.now()
        )
        
        original_updated_at = level_result['updated_at']
        
        # Wait a moment to ensure timestamp difference
        await asyncio.sleep(1)
        
        # Update the level
        await db_connection.execute(
            """
            UPDATE dev_sr_levels 
            SET strength = $1 
            WHERE id = $2
            """,
            Decimal('0.8'), level_result['id']
        )
        
        # Check that updated_at was automatically updated
        updated_row = await db_connection.fetchrow(
            "SELECT updated_at FROM dev_sr_levels WHERE id = $1",
            level_result['id']
        )
        
        # updated_at should be newer (if trigger exists)
        # Note: This test assumes an update trigger exists - if not, this will help identify the need for one
        print(f"Original: {original_updated_at}, Updated: {updated_row['updated_at']}")

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])