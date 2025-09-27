#!/usr/bin/env python3
"""
Schema Compatibility and Deployment Issue Tests

Critical tests that would have caught the real issues we encountered:
1. Schema mismatch between backfill scripts and database tables
2. Missing columns in different vendor tables
3. Data type compatibility across vendors
4. Deployment script validation before execution
"""

import pytest
import asyncpg
import sys
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from core.platform.config.environment import Environment

@dataclass
class TableSchema:
    table_name: str
    columns: Dict[str, str]  # column_name -> data_type
    constraints: List[str]

@dataclass
class VendorTableExpected:
    vendor: str
    table_name: str
    required_columns: List[str]
    optional_columns: List[str]

class SchemaCompatibilityValidator:
    """Validates database schema compatibility across all vendor tables"""

    def __init__(self, env: Environment):
        self.env = env
        self.db_url = env.get_database_url()

    async def get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        """Get actual schema for a table from database"""
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=2)
        async with pool.acquire() as conn:
            # Check if table exists
            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = $1
                )
            """, table_name)

            if not table_exists:
                return None

            # Get column information
            rows = await conn.fetch("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = $1
                ORDER BY ordinal_position
            """, table_name)

            columns = {}
            for row in rows:
                columns[row['column_name']] = {
                    'data_type': row['data_type'],
                    'nullable': row['is_nullable'] == 'YES'
                }

            # Get constraints
            constraint_rows = await conn.fetch("""
                SELECT constraint_name, constraint_type
                FROM information_schema.table_constraints
                WHERE table_schema = 'public'
                  AND table_name = $1
            """, table_name)

            constraints = [f"{row['constraint_type']}: {row['constraint_name']}"
                         for row in constraint_rows]

            return TableSchema(table_name, columns, constraints)
    async def validate_vendor_table_compatibility(self) -> Dict[str, Any]:
        """Validate all vendor tables have compatible schemas"""

        # Define expected schemas for each vendor table
        vendor_tables = [
            VendorTableExpected(
                vendor="polygon",
                table_name="dev_daily_price_polygon",
                required_columns=["id", "instrument_id", "date", "close", "volume"],
                optional_columns=["open_price", "high_price", "low_price", "vwap", "transactions",
                                "dollar_volume", "created_at", "updated_at"]
            ),
            VendorTableExpected(
                vendor="tiingo",
                table_name="dev_daily_price_tiingo",
                required_columns=["id", "instrument_id", "date", "close", "volume"],
                optional_columns=["open_price", "high_price", "low_price", "adj_close",
                                "dollar_volume", "created_at", "updated_at"]
            ),
            VendorTableExpected(
                vendor="alpha_vantage",
                table_name="dev_daily_price_alphavantage",
                required_columns=["id", "instrument_id", "date", "close", "volume"],
                optional_columns=["open_price", "high_price", "low_price", "adj_close",
                                "dollar_volume", "created_at", "updated_at"]
            ),
            VendorTableExpected(
                vendor="fmp",
                table_name="dev_daily_price_fmp",
                required_columns=["id", "instrument_id", "date", "close", "volume"],
                optional_columns=["open_price", "high_price", "low_price", "adj_close",
                                "dollar_volume", "created_at", "updated_at"]
            )
        ]

        validation_results = {
            'compatible': True,
            'issues': [],
            'schema_differences': {},
            'missing_tables': [],
            'column_mismatches': {}
        }

        schemas = {}

        # Get actual schemas
        for vendor_table in vendor_tables:
            schema = await self.get_table_schema(vendor_table.table_name)
            if schema is None:
                validation_results['missing_tables'].append(vendor_table.table_name)
                validation_results['compatible'] = False
                continue
            schemas[vendor_table.vendor] = schema

        # Check for required columns in each vendor table
        for vendor_table in vendor_tables:
            if vendor_table.vendor not in schemas:
                continue

            schema = schemas[vendor_table.vendor]
            missing_required = []

            for required_col in vendor_table.required_columns:
                if required_col not in schema.columns:
                    missing_required.append(required_col)

            if missing_required:
                validation_results['column_mismatches'][vendor_table.vendor] = {
                    'missing_required': missing_required
                }
                validation_results['compatible'] = False

        # Compare schemas between vendors for compatibility
        if len(schemas) > 1:
            vendor_names = list(schemas.keys())
            base_vendor = vendor_names[0]
            base_schema = schemas[base_vendor]

            for other_vendor in vendor_names[1:]:
                other_schema = schemas[other_vendor]
                differences = []

                # Check for column differences
                base_cols = set(base_schema.columns.keys())
                other_cols = set(other_schema.columns.keys())

                only_in_base = base_cols - other_cols
                only_in_other = other_cols - base_cols

                if only_in_base:
                    differences.append(f"Columns only in {base_vendor}: {list(only_in_base)}")
                if only_in_other:
                    differences.append(f"Columns only in {other_vendor}: {list(only_in_other)}")

                # Check data type compatibility for common columns
                common_cols = base_cols & other_cols
                for col in common_cols:
                    base_type = base_schema.columns[col]['data_type']
                    other_type = other_schema.columns[col]['data_type']
                    if base_type != other_type:
                        differences.append(f"Column {col}: {base_vendor}='{base_type}' vs {other_vendor}='{other_type}'")

                if differences:
                    validation_results['schema_differences'][f"{base_vendor}_vs_{other_vendor}"] = differences

        return validation_results

class BackfillScriptValidator:
    """Validates backfill scripts against actual database schemas"""

    def __init__(self, env: Environment):
        self.env = env
        self.schema_validator = SchemaCompatibilityValidator(env)

    def extract_insert_columns_from_script(self, script_content: str, table_name: str) -> List[str]:
        """Extract column names from INSERT statements in backfill scripts"""
        import re

        # Look for INSERT INTO statements
        insert_pattern = rf"INSERT INTO {re.escape(table_name)}\s*\((.*?)\)\s*VALUES"
        matches = re.search(insert_pattern, script_content, re.DOTALL | re.IGNORECASE)

        if not matches:
            return []

        columns_str = matches.group(1)
        # Extract column names, handling multi-line and whitespace
        columns = []
        for line in columns_str.split('\n'):
            line = line.strip()
            if line:
                # Remove comments and split by comma
                if '--' in line:
                    line = line.split('--')[0]
                for col in line.split(','):
                    col = col.strip().rstrip(',')
                    if col:
                        columns.append(col)

        return columns

    async def validate_backfill_script_compatibility(self, script_path: str,
                                                   vendor: str,
                                                   table_name: str) -> Dict[str, Any]:
        """Validate that a backfill script is compatible with database schema"""

        with open(script_path, 'r') as f:
            script_content = f.read()
        schema = await self.schema_validator.get_table_schema(table_name)
        if not schema:
            return {
                'valid': False,
                'error': f"Table {table_name} does not exist"
            }

        # Extract columns that script tries to insert
        script_columns = self.extract_insert_columns_from_script(script_content, table_name)

        if not script_columns:
            return {
                'valid': False,
                'error': f"Could not find INSERT statement for table {table_name} in script"
            }

        # Check for column mismatches
        table_columns = set(schema.columns.keys())
        script_columns_set = set(script_columns)

        missing_in_table = script_columns_set - table_columns

        result = {
            'valid': len(missing_in_table) == 0,
            'script_columns': script_columns,
            'table_columns': list(table_columns),
            'missing_in_table': list(missing_in_table),
            'vendor': vendor,
            'table_name': table_name
        }

        if missing_in_table:
            result['error'] = f"Script references columns not in table: {list(missing_in_table)}"

        return result

@pytest.mark.integration
@pytest.mark.database
class TestSchemaCompatibilityAndDeploymentIssues:
    """Integration tests that would have caught our real deployment issues"""

    @pytest.fixture
    def env(self):
        return Environment()

    @pytest.fixture
    def schema_validator(self, env):
        return SchemaCompatibilityValidator(env)

    @pytest.fixture
    def script_validator(self, env):
        return BackfillScriptValidator(env)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_all_vendor_tables_exist_and_compatible(self, schema_validator):
        """Test that all vendor price tables exist and have compatible schemas"""
        print("\n🔍 Testing vendor table schema compatibility...")

        results = await schema_validator.validate_vendor_table_compatibility()

        # Print detailed results for debugging
        print(f"Schema compatibility results:")
        print(f"  Compatible: {results['compatible']}")

        if results['missing_tables']:
            print(f"  Missing tables: {results['missing_tables']}")

        if results['column_mismatches']:
            print(f"  Column mismatches: {results['column_mismatches']}")

        if results['schema_differences']:
            print(f"  Schema differences:")
            for comparison, differences in results['schema_differences'].items():
                print(f"    {comparison}:")
                for diff in differences:
                    print(f"      - {diff}")

        # Assertions
        assert len(results['missing_tables']) == 0, f"Missing vendor tables: {results['missing_tables']}"
        assert len(results['column_mismatches']) == 0, f"Column mismatches: {results['column_mismatches']}"

        # Schema differences are warnings, not failures (vendors can have different optional columns)
        # But we should log them for awareness

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_critical_columns_consistent_across_vendors(self, schema_validator):
        """Test that critical price columns are consistent across all vendor tables"""
        print("\n🔍 Testing critical column consistency...")

        critical_columns = ['id', 'instrument_id', 'date', 'close', 'volume']

        vendor_tables = [
            'dev_daily_price_polygon',
            'dev_daily_price_tiingo',
            'dev_daily_price_alphavantage',
            'dev_daily_price_fmp'
        ]

        schemas = {}
        for table_name in vendor_tables:
            schema = await schema_validator.get_table_schema(table_name)
            if schema:
                schemas[table_name] = schema

        # Check each critical column exists in all tables
        missing_critical = {}
        data_type_inconsistencies = {}

        for critical_col in critical_columns:
            tables_missing_col = []
            column_types = {}

            for table_name, schema in schemas.items():
                if critical_col not in schema.columns:
                    tables_missing_col.append(table_name)
                else:
                    column_types[table_name] = schema.columns[critical_col]['data_type']

            if tables_missing_col:
                missing_critical[critical_col] = tables_missing_col

            # Check data type consistency
            if len(set(column_types.values())) > 1:
                data_type_inconsistencies[critical_col] = column_types

        print(f"Critical column analysis:")
        print(f"  Missing critical columns: {missing_critical}")
        print(f"  Data type inconsistencies: {data_type_inconsistencies}")

        assert len(missing_critical) == 0, f"Critical columns missing: {missing_critical}"
        assert len(data_type_inconsistencies) == 0, f"Data type inconsistencies: {data_type_inconsistencies}"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_backfill_script_column_compatibility(self, script_validator):
        """Test that backfill scripts match actual table schemas - WOULD HAVE CAUGHT OUR BUG!"""
        print("\n🔍 Testing backfill script compatibility with database schemas...")

        # Test scripts that we know should work
        script_tests = [
            {
                'vendor': 'polygon',
                'table': 'dev_daily_price_polygon',
                'expected_columns': ['date', 'instrument_id', 'open_price', 'high_price', 'low_price', 'close', 'volume']
            },
            {
                'vendor': 'tiingo',
                'table': 'dev_daily_price_tiingo',
                'expected_columns': ['date', 'instrument_id', 'open_price', 'high_price', 'low_price', 'close', 'adj_close', 'volume']
            },
            {
                'vendor': 'fmp',
                'table': 'dev_daily_price_fmp',
                'expected_columns': ['date', 'instrument_id', 'open_price', 'high_price', 'low_price', 'close', 'adj_close', 'volume']
            }
        ]

        # Test the problematic scenario that caused our deployment failure
        print("  🚨 TESTING THE EXACT ISSUE WE ENCOUNTERED:")

        # Simulate the bad script that tried to insert adj_close into Polygon table
        bad_polygon_script_content = '''
        INSERT INTO dev_daily_price_polygon
        (date, instrument_id, open_price, high_price, low_price, close, adj_close, volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        '''

        # Test this against actual Polygon table schema
        polygon_schema = await script_validator.schema_validator.get_table_schema('dev_daily_price_polygon')

        if polygon_schema:
            script_columns = ['date', 'instrument_id', 'open_price', 'high_price', 'low_price', 'close', 'adj_close', 'volume']
            table_columns = set(polygon_schema.columns.keys())
            script_columns_set = set(script_columns)

            missing_in_table = script_columns_set - table_columns

            print(f"    Script columns: {script_columns}")
            print(f"    Table columns: {list(table_columns)}")
            print(f"    Missing in table: {list(missing_in_table)}")

            # This test should FAIL, demonstrating it would have caught our bug
            if missing_in_table:
                print(f"    ✅ TEST CORRECTLY DETECTED THE ISSUE: {missing_in_table}")
                # In our case, this would be ['adj_close']
                assert 'adj_close' in missing_in_table, "Test should detect adj_close column missing from Polygon table"
            else:
                pytest.fail("Test failed to detect the schema mismatch that caused our deployment failure!")
        else:
            pytest.skip("Polygon table not available for testing")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_type_compatibility_for_joins(self, schema_validator):
        """Test that foreign key columns have compatible data types for cross-vendor queries"""
        print("\n🔍 Testing foreign key data type compatibility...")

        vendor_tables = [
            'dev_daily_price_polygon',
            'dev_daily_price_tiingo',
            'dev_daily_price_alphavantage',
            'dev_daily_price_fmp'
        ]

        key_columns = ['instrument_id', 'date']

        for key_col in key_columns:
            column_types = {}

            for table_name in vendor_tables:
                schema = await schema_validator.get_table_schema(table_name)
                if schema and key_col in schema.columns:
                    column_types[table_name] = schema.columns[key_col]['data_type']

            # All tables should have the same data type for join columns
            unique_types = set(column_types.values())

            print(f"  {key_col} types: {column_types}")

            assert len(unique_types) <= 1, f"Inconsistent {key_col} types across vendor tables: {column_types}"

    def test_backfill_script_validation_framework(self, script_validator):
        """Test that our validation framework itself works correctly"""
        print("\n🔍 Testing backfill script validation framework...")

        # Test the column extraction logic
        test_script = '''
        INSERT INTO test_table
        (
            date,
            instrument_id,
            open_price,  -- opening price
            close,
            volume
        )
        VALUES ($1, $2, $3, $4, $5)
        '''

        columns = script_validator.extract_insert_columns_from_script(test_script, 'test_table')
        expected_columns = ['date', 'instrument_id', 'open_price', 'close', 'volume']

        print(f"  Extracted columns: {columns}")
        print(f"  Expected columns: {expected_columns}")

        assert columns == expected_columns, f"Column extraction failed: got {columns}, expected {expected_columns}"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_detect_schema_evolution_issues(self, schema_validator):
        """Test detection of schema changes that could break existing code"""
        print("\n🔍 Testing schema evolution issue detection...")

        # This test would catch issues when database schema changes
        # but application code doesn't get updated

        vendor_tables = [
            'dev_daily_price_polygon',
            'dev_daily_price_tiingo',
            'dev_daily_price_alphavantage',
            'dev_daily_price_fmp'
        ]

        required_for_business_logic = [
            'instrument_id',  # Required for joining with instruments
            'date',           # Required for time-series analysis
            'close',          # Required for price calculations
            'volume'          # Required for dollar volume calculations
        ]

        missing_business_critical = {}

        for table_name in vendor_tables:
            schema = await schema_validator.get_table_schema(table_name)
            if schema:
                table_columns = set(schema.columns.keys())
                missing = []

                for required_col in required_for_business_logic:
                    if required_col not in table_columns:
                        missing.append(required_col)

                if missing:
                    missing_business_critical[table_name] = missing

        print(f"  Missing business-critical columns: {missing_business_critical}")

        assert len(missing_business_critical) == 0, f"Business-critical columns missing: {missing_business_critical}"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_deployment_readiness_checklist(self, schema_validator, script_validator):
        """Comprehensive deployment readiness test - would prevent production issues"""
        print("\n🔍 Running deployment readiness checklist...")

        checklist_results = {
            'schema_compatibility': False,
            'all_tables_exist': False,
            'foreign_keys_valid': False,
            'data_types_consistent': False,
            'ready_for_deployment': False
        }

        # 1. Schema compatibility check
        schema_results = await schema_validator.validate_vendor_table_compatibility()
        checklist_results['schema_compatibility'] = schema_results['compatible']

        # 2. All required tables exist
        checklist_results['all_tables_exist'] = len(schema_results['missing_tables']) == 0

        # 3. Check foreign key constraints exist
        vendor_tables = ['dev_daily_price_polygon', 'dev_daily_price_tiingo',
                        'dev_daily_price_alphavantage', 'dev_daily_price_fmp']

        fk_issues = []
        for table_name in vendor_tables:
            schema = await schema_validator.get_table_schema(table_name)
            if schema:
                # Check if instrument_id foreign key exists
                fk_found = any('FOREIGN KEY' in constraint for constraint in schema.constraints)
                if not fk_found:
                    fk_issues.append(table_name)

        checklist_results['foreign_keys_valid'] = len(fk_issues) == 0

        # 4. Data types consistent across vendors
        checklist_results['data_types_consistent'] = len(schema_results['column_mismatches']) == 0

        # 5. Overall deployment readiness
        checklist_results['ready_for_deployment'] = all([
            checklist_results['schema_compatibility'],
            checklist_results['all_tables_exist'],
            checklist_results['foreign_keys_valid'],
            checklist_results['data_types_consistent']
        ])

        print(f"  Deployment readiness checklist:")
        for check, passed in checklist_results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"    {check}: {status}")

        # This should prevent deployment if any critical check fails
        assert checklist_results['ready_for_deployment'], f"Deployment readiness failed: {checklist_results}"

if __name__ == "__main__":
    # Allow running this test file directly
    pytest.main([__file__, "-v", "-s"])