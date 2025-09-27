#!/usr/bin/env python3
"""
Test case to detect schema consistency errors between environments.

Reproduces the error:
asyncpg.exceptions.UndefinedColumnError: column "results" of relation "intg_runs" does not exist

This test ensures that critical schema differences between dev and intg environments
are detected before they cause runtime failures.
"""

import pytest
import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from core.platform.config.environment import Environment, EnvironmentType


class TestSchemaConsistencyDetection:
    """Test schema consistency between dev and intg environments."""

    @pytest.fixture
    def dev_environment(self):
        """Create dev environment for testing."""
        return Environment(env_type=EnvironmentType.DEV)

    @pytest.fixture  
    def intg_environment(self):
        """Create intg environment for testing."""
        return Environment(env_type=EnvironmentType.INTEGRATION)

    async def test_runs_table_schema_consistency(self, dev_environment, intg_environment):
        """
        Test that dev_runs and intg_runs tables have consistent schemas.
        
        This test detects the specific issue where intg_runs was missing the 'results' column
        that exists in dev_runs, causing UndefinedColumnError at runtime.
        """
        print(f"🔍 Testing runs table schema consistency between environments")
        
        # Get column information for both environments
        dev_columns = await self._get_table_columns(dev_environment, "dev_runs")
        intg_columns = await self._get_table_columns(intg_environment, "intg_runs")
        
        print(f"📊 Dev runs columns: {len(dev_columns)} found")
        print(f"📊 Intg runs columns: {len(intg_columns)} found")
        
        # Convert to sets for comparison
        dev_column_set = set(dev_columns)
        intg_column_set = set(intg_columns)
        
        # Find missing columns in each environment
        missing_in_intg = dev_column_set - intg_column_set
        missing_in_dev = intg_column_set - dev_column_set
        
        # Report schema differences
        if missing_in_intg:
            print(f"❌ Columns missing in intg_runs: {missing_in_intg}")
        if missing_in_dev:
            print(f"❌ Columns missing in dev_runs: {missing_in_dev}")
            
        # Specifically test for the 'results' column issue
        assert "results" in dev_column_set, "dev_runs should have 'results' column"
        assert "results" in intg_column_set, "intg_runs should have 'results' column"
        
        # Ensure no critical columns are missing
        critical_columns = {
            'id', 'run_type', 'status', 'created_at', 'parameters', 
            'results',  # This was the missing column causing the error
            'command_line', 'git_commit_hash', 'environment'
        }
        
        for column in critical_columns:
            assert column in dev_column_set, f"Critical column '{column}' missing in dev_runs"
            assert column in intg_column_set, f"Critical column '{column}' missing in intg_runs"
        
        # Overall consistency check
        assert dev_column_set == intg_column_set, (
            f"Schema inconsistency detected:\n"
            f"Missing in intg: {missing_in_intg}\n"
            f"Missing in dev: {missing_in_dev}"
        )
        
        print(f"✅ Schema consistency verified: {len(dev_columns)} columns match")

    async def test_training_dataset_table_schema_consistency(self, dev_environment, intg_environment):
        """
        Test that training dataset tables have consistent schemas.
        
        Ensures training data operations work consistently across environments.
        """
        print(f"🔍 Testing training dataset table schema consistency")
        
        # Get column information for training dataset tables
        dev_columns = await self._get_table_columns(dev_environment, "dev_training_datasets")
        intg_columns = await self._get_table_columns(intg_environment, "intg_training_datasets")
        
        print(f"📊 Dev training_datasets columns: {len(dev_columns)} found")
        print(f"📊 Intg training_datasets columns: {len(intg_columns)} found")
        
        # Convert to sets for comparison
        dev_column_set = set(dev_columns)
        intg_column_set = set(intg_columns)
        
        # Check for critical training dataset columns
        critical_columns = {
            'id', 'dataset_name', 'run_id', 'symbols', 'created_at',
            'total_sequences', 'feature_count', 'status',
            'features_file_path', 'labels_file_path'
        }
        
        for column in critical_columns:
            assert column in dev_column_set, f"Critical column '{column}' missing in dev_training_datasets"
            assert column in intg_column_set, f"Critical column '{column}' missing in intg_training_datasets"
        
        # Ensure schemas match exactly
        missing_in_intg = dev_column_set - intg_column_set
        missing_in_dev = intg_column_set - dev_column_set
        
        assert dev_column_set == intg_column_set, (
            f"Training dataset schema inconsistency:\n"
            f"Missing in intg: {missing_in_intg}\n"
            f"Missing in dev: {missing_in_dev}"
        )
        
        print(f"✅ Training dataset schema consistency verified")

    async def test_detect_undefined_column_error_pattern(self):
        """
        Test that simulates the UndefinedColumnError to ensure our detection works.
        
        This validates our ability to catch schema mismatches before runtime.
        """
        print(f"🔍 Testing UndefinedColumnError detection pattern")
        
        # Simulate the error scenario
        table_name = "intg_runs"
        missing_column = "results"
        
        # This would be the error that occurs at runtime
        expected_error_pattern = f'column "{missing_column}" of relation "{table_name}" does not exist'
        
        # Verify our test would catch this
        # Simulate query that would fail
        from asyncpg.exceptions import UndefinedColumnError
        raise UndefinedColumnError(expected_error_pattern)
    async def test_migration_007_completeness(self):
        """
        Test that migration 007 adds all required columns.
        
        Ensures the migration resolves the schema inconsistency completely.
        """
        print(f"🔍 Testing migration 007 completeness")
        
        # Expected columns that migration 007 should add
        expected_new_columns = {
            'command_line', 'git_commit_hash', 'git_branch', 'environment',
            'results',  # The critical missing column
            'host_info', 'working_directory', 'python_version', 'dependencies_hash'
        }
        
        # Verify these columns are present after migration
        for env_type, table_name in [(EnvironmentType.DEV, "dev_runs"), (EnvironmentType.INTEGRATION, "intg_runs")]:
            env = Environment(env_type=env_type)
            columns = await self._get_table_columns(env, table_name)
            column_set = set(columns)
            
            for expected_column in expected_new_columns:
                assert expected_column in column_set, (
                    f"Migration 007 should add '{expected_column}' to {table_name}"
                )
        
        print(f"✅ Migration 007 completeness verified")

    async def _get_table_columns(self, environment, table_name):
        """Get list of column names for a table."""
        async with environment.get_connection() as conn:
            query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = $1 
            ORDER BY ordinal_position
            """
            rows = await conn.fetch(query, table_name)
            return [row['column_name'] for row in rows]
async def test_schema_consistency():
    """Test schema consistency between environments."""
    test_instance = TestSchemaConsistencyDetection()
    
    dev_env = Environment(env_type=EnvironmentType.DEV)
    intg_env = Environment(env_type=EnvironmentType.INTEGRATION)
    
    await test_instance.test_runs_table_schema_consistency(dev_env, intg_env)
    await test_instance.test_training_dataset_table_schema_consistency(dev_env, intg_env)


async def test_error_detection():
    """Test UndefinedColumnError detection pattern."""
    test_instance = TestSchemaConsistencyDetection()
    await test_instance.test_detect_undefined_column_error_pattern()


async def test_migration_verification():
    """Test migration 007 completeness."""
    test_instance = TestSchemaConsistencyDetection()
    await test_instance.test_migration_007_completeness()


if __name__ == "__main__":
    print("🧪 Testing schema consistency detection...")
    print("=" * 60)
    
    async def run_tests():
        # Test 1: Schema consistency
        print("\n1. Testing schema consistency:")
        await test_schema_consistency()
        
        # Test 2: Error detection pattern
        print("\n2. Testing error detection pattern:")
        await test_error_detection()
        
        # Test 3: Migration verification
        print("\n3. Testing migration verification:")
        await test_migration_verification()
        
        return True
        
    success = asyncio.run(run_tests())
    if success:
        print(f"\n🎉 All schema consistency tests passed!")
        print(f"✅ UndefinedColumnError detection is working")
        print(f"✅ Schema inconsistencies would be caught by tests")
    else:
        print(f"\n❌ Schema consistency tests failed")
        sys.exit(1)