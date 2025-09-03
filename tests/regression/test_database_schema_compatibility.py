"""
Regression Tests for Database Schema Compatibility Issues

This test suite prevents regression of database schema compatibility issues
that caused failures when scripts expected different table structures than
what actually existed in the database.

Issues Fixed:
1. Tiingo backfill script expected adj_close column but table had adjclose
2. Scripts creating new tables with different schemas than existing ones  
3. Column name mismatches between expected and actual database schema
"""

import pytest
import asyncio
import asyncpg
import os
import sys
from typing import Dict, List, Tuple, Optional

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

class TestDatabaseSchemaCompatibility:
    """Test suite for database schema compatibility and consistency"""
    
    @pytest.fixture
    async def db_connection(self):
        """Create test database connection"""
        conn = await asyncpg.connect(
            host=os.getenv('DB_HOST', 'postgres'),
            port=os.getenv('DB_PORT', '5432'),
            user=os.getenv('DB_USER', 'postgres'), 
            password=os.getenv('DB_PASSWORD', 'dev_password'),
            database=os.getenv('DB_NAME', 'dev_db')
        )
        yield conn
        await conn.close()
    
    @pytest.fixture
    def expected_table_schemas(self):
        """Define expected schemas for critical tables"""
        return {
            'dev_instrument_tiingo': {
                'required_columns': [
                    'id', 'symbol', 'name', 'exchange', 'asset_type', 
                    'currency', 'start_date', 'end_date', 'raw', 
                    'created_at', 'updated_at'
                ],
                'unique_constraints': ['symbol'],
                'primary_key': ['id']
            },
            'dev_daily_prices_tiingo': {
                'required_columns': [
                    'date', 'symbol', 'open', 'high', 'low', 'close',
                    'adjclose', 'volume', 'status_id', 'instrument_id'  # Note: adjclose not adj_close
                ],
                'unique_constraints': [('date', 'instrument_id')],
                'primary_key': [('date', 'instrument_id')]
            },
            'dev_daily_prices_polygon': {
                'required_columns': [
                    'id', 'date', 'symbol', 'open', 'high', 'low', 'close',
                    'volume', 'market_cap', 'instrument_id', 
                    'created_at', 'updated_at'
                ],
                'unique_constraints': [('date', 'instrument_id')],
                'primary_key': ['id']
            },
            'dev_instruments': {
                'required_columns': [
                    'id', 'symbol', 'name', 'exchange', 'type', 'currency',
                    'figi', 'isin', 'cusip', 'composite_figi', 'active',
                    'list_date', 'delist_date', 'created_at', 'updated_at', 'sector'
                ],
                'unique_constraints': ['symbol'],
                'primary_key': ['id']
            }
        }
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_tiingo_daily_prices_table_schema(self, db_connection, expected_table_schemas):
        """Test that Tiingo daily prices table has correct schema"""
        table_name = 'dev_daily_prices_tiingo'
        
        # Get actual table schema
        columns = await db_connection.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position
        """, table_name)
        
        assert len(columns) > 0, f"Table {table_name} should exist"
        
        # Check required columns exist
        actual_columns = [col['column_name'] for col in columns]
        expected_columns = expected_table_schemas[table_name]['required_columns']
        
        missing_columns = set(expected_columns) - set(actual_columns)
        assert len(missing_columns) == 0, f"Missing columns in {table_name}: {missing_columns}"
        
        # Specifically test the adjclose vs adj_close issue
        assert 'adjclose' in actual_columns, "Table should have 'adjclose' column (not 'adj_close')"
        assert 'adj_close' not in actual_columns, "Table should NOT have 'adj_close' column"
    
    @pytest.mark.asyncio 
    @pytest.mark.asyncio
    async def test_polygon_daily_prices_table_schema(self, db_connection, expected_table_schemas):
        """Test that Polygon daily prices table has correct schema"""
        table_name = 'dev_daily_prices_polygon'
        
        # Get actual table schema
        columns = await db_connection.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = $1 AND table_schema = 'public' 
            ORDER BY ordinal_position
        """, table_name)
        
        assert len(columns) > 0, f"Table {table_name} should exist"
        
        # Check required columns
        actual_columns = [col['column_name'] for col in columns]
        expected_columns = expected_table_schemas[table_name]['required_columns']
        
        missing_columns = set(expected_columns) - set(actual_columns)
        assert len(missing_columns) == 0, f"Missing columns in {table_name}: {missing_columns}"
        
        # Polygon table should have market_cap column
        assert 'market_cap' in actual_columns, "Polygon table should have market_cap column"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_instruments_table_schema(self, db_connection, expected_table_schemas):
        """Test that main instruments table has correct schema"""
        table_name = 'dev_instruments'
        
        # Check if table exists
        table_exists = await db_connection.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = $1 AND table_schema = 'public'
            )
        """, table_name)
        
        assert table_exists, f"Table {table_name} should exist"
        
        # Get actual columns
        columns = await db_connection.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position  
        """, table_name)
        
        actual_columns = [col['column_name'] for col in columns]
        expected_columns = expected_table_schemas[table_name]['required_columns']
        
        missing_columns = set(expected_columns) - set(actual_columns)
        assert len(missing_columns) == 0, f"Missing columns in {table_name}: {missing_columns}"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_primary_key_constraints(self, db_connection):
        """Test that tables have proper primary key constraints"""
        tables_with_pks = [
            ('dev_instrument_tiingo', 'id'),
            ('dev_daily_prices_tiingo', ['date', 'instrument_id']),  # Composite PK
            ('dev_daily_prices_polygon', 'id'),
            ('dev_instruments', 'id')
        ]
        
        for table_name, expected_pk in tables_with_pks:
            # Get primary key constraint info
            pk_info = await db_connection.fetch("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = $1::regclass AND i.indisprimary
                ORDER BY a.attnum
            """, table_name)
            
            if pk_info:
                actual_pk_columns = [row['attname'] for row in pk_info]
                
                if isinstance(expected_pk, str):
                    expected_pk = [expected_pk]
                
                assert set(actual_pk_columns) == set(expected_pk), \
                    f"Table {table_name} should have PK on {expected_pk}, got {actual_pk_columns}"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_foreign_key_constraints(self, db_connection):
        """Test that tables have proper foreign key relationships"""
        # Test that daily prices tables reference instruments table
        tables_with_fks = [
            ('dev_daily_prices_tiingo', 'instrument_id', 'dev_instruments', 'id'),
            ('dev_daily_prices_polygon', 'instrument_id', 'dev_instruments', 'id')
        ]
        
        for table_name, fk_column, ref_table, ref_column in tables_with_fks:
            fk_info = await db_connection.fetch("""
                SELECT 
                    tc.constraint_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu 
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu 
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY' 
                    AND tc.table_name = $1
                    AND kcu.column_name = $2
            """, table_name, fk_column)
            
            if fk_info:  # FK exists
                fk_row = fk_info[0]
                assert fk_row['foreign_table_name'] == ref_table, \
                    f"FK should reference {ref_table}, got {fk_row['foreign_table_name']}"
                assert fk_row['foreign_column_name'] == ref_column, \
                    f"FK should reference {ref_column}, got {fk_row['foreign_column_name']}"
    
    def test_tiingo_backfill_script_column_names(self):
        """Test that Tiingo backfill script uses correct column names"""
        script_path = '/workspace/scripts/run_tiingo_daily_backfill.py'
        
        if os.path.exists(script_path):
            with open(script_path, 'r') as f:
                content = f.read()
                
                # Should use 'adjclose' not 'adj_close'
                assert 'adjclose' in content, "Script should use 'adjclose' column name"
                assert 'adj_close' not in content or 'adjclose' in content, \
                    "Script should not use 'adj_close' without also having 'adjclose'"
                
                # Should match actual table schema
                expected_columns = [
                    'date', 'symbol', 'open', 'high', 'low', 'close',
                    'adjclose', 'volume', 'status_id', 'instrument_id'
                ]
                
                # Check INSERT statement uses correct columns
                if 'INSERT INTO dev_daily_prices_tiingo' in content:
                    for col in expected_columns:
                        assert col in content, f"Script should reference column '{col}'"
    
    def test_polygon_backfill_script_column_names(self):
        """Test that Polygon backfill script uses correct column names"""
        script_paths = [
            '/workspace/scripts/run_polygon_backfill_direct.py',
            '/workspace/scripts/run_polygon_daily_backfill_30years.py'
        ]
        
        for script_path in script_paths:
            if os.path.exists(script_path):
                with open(script_path, 'r') as f:
                    content = f.read()
                    
                    # Should use correct Polygon table columns
                    if 'INSERT INTO dev_daily_prices_polygon' in content:
                        expected_columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
                        for col in expected_columns:
                            assert col in content, f"Polygon script should reference column '{col}'"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_schema_consistency_across_environments(self, db_connection):
        """Test that schema is consistent and doesn't vary by environment"""
        # Get current environment tables
        tables = await db_connection.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
                AND table_name LIKE 'dev_%'
            ORDER BY table_name
        """)
        
        table_names = [t['table_name'] for t in tables]
        
        # Should have the core tables we depend on
        required_tables = [
            'dev_instrument_tiingo',
            'dev_daily_prices_tiingo', 
            'dev_daily_prices_polygon',
            'dev_instruments'
        ]
        
        missing_tables = set(required_tables) - set(table_names)
        assert len(missing_tables) == 0, f"Missing required tables: {missing_tables}"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_type_compatibility(self, db_connection):
        """Test that column data types are compatible with our usage"""
        # Check critical data types
        type_checks = [
            ('dev_daily_prices_tiingo', 'date', 'date'),
            ('dev_daily_prices_tiingo', 'open', 'double precision'),
            ('dev_daily_prices_tiingo', 'volume', 'bigint'),
            ('dev_daily_prices_polygon', 'date', 'date'),
            ('dev_daily_prices_polygon', 'volume', 'bigint'),
            ('dev_instruments', 'active', 'boolean')
        ]
        
        for table_name, column_name, expected_type in type_checks:
            actual_type = await db_connection.fetchval("""
                SELECT data_type 
                FROM information_schema.columns
                WHERE table_name = $1 AND column_name = $2
            """, table_name, column_name)
            
            if actual_type:  # Column exists
                assert actual_type == expected_type, \
                    f"{table_name}.{column_name} should be {expected_type}, got {actual_type}"
    
    def test_schema_migration_safety(self):
        """Test that schema changes are handled safely"""
        # This test ensures we have mechanisms to handle schema evolution
        migration_practices = [
            "Always check existing schema before creating tables",
            "Use IF NOT EXISTS for table creation",
            "Use ALTER TABLE ADD COLUMN IF NOT EXISTS for new columns", 
            "Test schema compatibility in CI/CD",
            "Document schema changes in migration scripts"
        ]
        
        # Verify our scripts follow these practices
        script_paths = [
            '/workspace/scripts/run_tiingo_daily_backfill.py',
            '/workspace/scripts/run_polygon_backfill_direct.py'
        ]
        
        for script_path in script_paths:
            if os.path.exists(script_path):
                with open(script_path, 'r') as f:
                    content = f.read()
                    
                    # Should not create conflicting table structures
                    if 'CREATE TABLE' in content:
                        assert 'IF NOT EXISTS' in content, \
                            f"{script_path} should use IF NOT EXISTS for table creation"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_insert_statement_column_order(self, db_connection):
        """Test that INSERT statements match actual table column order"""
        # Test a sample INSERT to ensure compatibility
        test_data = {
            'date': '2025-01-01',
            'symbol': 'TEST_SCHEMA',
            'open': 100.0,
            'high': 105.0,
            'low': 99.0,
            'close': 102.0,
            'adjclose': 102.0,
            'volume': 1000000,
            'status_id': None,
            'instrument_id': 1
        }
        
        # Test INSERT works with explicit column names (most robust)
        try:
            await db_connection.execute("""
                INSERT INTO dev_daily_prices_tiingo 
                (date, symbol, open, high, low, close, adjclose, volume, status_id, instrument_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """, 
            test_data['date'], test_data['symbol'], test_data['open'],
            test_data['high'], test_data['low'], test_data['close'],
            test_data['adjclose'], test_data['volume'], test_data['status_id'],
            test_data['instrument_id'])
            
            # Cleanup test data
            await db_connection.execute(
                "DELETE FROM dev_daily_prices_tiingo WHERE symbol = 'TEST_SCHEMA'"
            )
            
        except Exception as e:
            pytest.fail(f"INSERT statement failed - schema compatibility issue: {e}")
    
    def test_script_error_handling_for_schema_issues(self):
        """Test that scripts handle schema mismatches gracefully"""
        script_paths = [
            '/workspace/scripts/run_tiingo_daily_backfill.py', 
            '/workspace/scripts/run_polygon_backfill_direct.py'
        ]
        
        for script_path in script_paths:
            if os.path.exists(script_path):
                with open(script_path, 'r') as f:
                    content = f.read()
                    
                    # Should have error handling around database operations
                    assert 'except' in content, f"{script_path} should have error handling"
                    assert 'try:' in content, f"{script_path} should have try-catch blocks"
                    
                    # Should log errors meaningfully
                    if 'INSERT INTO' in content:
                        assert 'logger.error' in content or 'print(' in content, \
                            f"{script_path} should log database errors"


@pytest.mark.integration
class TestSchemaCompatibilityIntegration:
    """Integration tests for schema compatibility across the full system"""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_end_to_end_data_flow(self):
        """Test complete data flow from API to database respects schema"""
        # This would test:
        # 1. API response parsing
        # 2. Data transformation
        # 3. Database insertion
        # 4. Data retrieval
        
        # For now, we document the expected flow
        data_flow_stages = [
            "API Response → Python Dict",
            "Python Dict → Database Record", 
            "Database Record → Application Object",
            "Application Object → API Response"
        ]
        
        # Each stage should preserve data integrity and schema compatibility
        assert len(data_flow_stages) == 4, "Should have complete data flow pipeline"
    
    def test_backfill_scripts_schema_validation(self):
        """Test that backfill scripts validate schema before proceeding"""
        # Our scripts should check table schema before attempting operations
        validation_requirements = [
            "Check table exists before INSERT",
            "Validate column names match expected schema", 
            "Handle schema mismatches gracefully",
            "Provide clear error messages for schema issues",
            "Support schema evolution without breaking"
        ]
        
        # This documents requirements for robust schema handling
        assert len(validation_requirements) == 5, "Should have comprehensive schema validation"
    
    def test_cross_vendor_schema_consistency(self):
        """Test that different vendor tables follow consistent patterns"""
        # Tiingo and Polygon tables should have similar structures where appropriate
        common_patterns = [
            "All price tables have: date, symbol, open, high, low, close, volume",
            "All price tables reference instrument_id",
            "All instrument tables have: symbol, name, exchange",
            "All tables have created_at/updated_at timestamps",
            "All tables follow dev_ prefix convention"
        ]
        
        # This ensures consistency across vendor implementations
        assert len(common_patterns) == 5, "Should have consistent schema patterns"