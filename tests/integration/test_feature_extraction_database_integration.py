#!/usr/bin/env python3
"""
Integration tests for feature extraction database system.

Tests the complete database integration including:
1. Migration execution and table creation
2. Feature catalog population
3. Pattern matching system
4. DAO database operations
5. Real database connectivity and transactions
"""

import pytest
import asyncio
from datetime import date, datetime
from typing import Dict, Any, List

from core.platform.config_env.environment import Environment, EnvironmentType
from domains.ml.services.training_data.dao.feature_extraction_dao import (
    FeatureExtractionDAO, 
    FeatureGroup, 
    FeatureCatalog, 
    FeaturePattern, 
    FeatureMappingResult,
    FeatureExtractionRun
)


class TestFeatureExtractionDatabaseIntegration:
    """Integration tests using real database connections."""

    @pytest.fixture(scope="class")
    async def test_environment(self):
        """Create test environment with real database connection."""
        # Use test database configuration
        environment = Environment(
            env_type=EnvironmentType.TEST,
            db_host="localhost",
            db_port=5432,
            db_user="test_user", 
            db_password="test_password",
            db_name="postgres"
        )
        
        yield environment
        
        # Cleanup after tests
        async with environment.get_connection() as conn:
            # Clean up test tables
            await conn.execute("DROP TABLE IF EXISTS test_feature_patterns CASCADE")
            await conn.execute("DROP TABLE IF EXISTS test_feature_catalog CASCADE") 
            await conn.execute("DROP TABLE IF EXISTS test_feature_groups CASCADE")
            await conn.execute("DROP TABLE IF EXISTS test_feature_extraction_runs CASCADE")

    @pytest.fixture
    async def feature_dao(self, test_environment):
        """Create FeatureExtractionDAO with real database."""
        return FeatureExtractionDAO(test_environment)

    async def test_database_migration_creates_tables(self, test_environment):
        """Test that migration creates all required tables."""
        async with test_environment.get_connection() as conn:
            # Check that required tables exist
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
              AND table_name LIKE 'test_feature_%'
            ORDER BY table_name
            """
            
            rows = await conn.fetch(tables_query)
            table_names = [row['table_name'] for row in rows]
            
            expected_tables = [
                'test_feature_groups',
                'test_feature_catalog', 
                'test_feature_patterns',
                'test_feature_extraction_runs'
            ]
            
            for expected_table in expected_tables:
                assert expected_table in table_names, f"Missing table: {expected_table}"

    async def test_feature_groups_population(self, feature_dao, test_environment):
        """Test that feature groups are properly populated."""
        # Insert test feature groups
        async with test_environment.get_connection() as conn:
            await conn.execute("""
                INSERT INTO test_feature_groups (group_name, display_name, description, category, update_frequency) 
                VALUES 
                    ('ohlcv_basic', 'Basic OHLCV Features', 'Core price and volume features', 'basic', 'daily'),
                    ('technical_momentum', 'Technical Momentum', 'Momentum-based technical indicators', 'technical', 'daily'),
                    ('technical_volatility', 'Technical Volatility', 'Volatility-based technical indicators', 'technical', 'daily')
                ON CONFLICT (group_name) DO NOTHING
            """)

        # Test retrieval
        feature_groups = await feature_dao.get_feature_groups()
        
        # Verify groups were created
        assert len(feature_groups) >= 3
        
        group_names = {fg.group_name for fg in feature_groups}
        expected_groups = {"ohlcv_basic", "technical_momentum", "technical_volatility"}
        assert expected_groups.issubset(group_names)
        
        # Verify group properties
        ohlcv_group = next(fg for fg in feature_groups if fg.group_name == "ohlcv_basic")
        assert ohlcv_group.category == "basic"
        assert ohlcv_group.update_frequency == "daily"
        assert ohlcv_group.is_active == True

    async def test_feature_catalog_population(self, feature_dao, test_environment):
        """Test that feature catalog is properly populated."""
        # Get feature groups for foreign key references
        feature_groups = await feature_dao.get_feature_groups()
        ohlcv_group = next(fg for fg in feature_groups if fg.group_name == "ohlcv_basic")
        momentum_group = next(fg for fg in feature_groups if fg.group_name == "technical_momentum")

        # Insert test feature catalog entries
        async with test_environment.get_connection() as conn:
            await conn.execute("""
                INSERT INTO test_feature_catalog (feature_name, feature_group_id, data_type, column_position, description) 
                VALUES 
                    ('open', $1, 'FLOAT64', 2, 'Opening price'),
                    ('close', $1, 'FLOAT64', 5, 'Closing price'),
                    ('volume', $1, 'INT64', 6, 'Trading volume'),
                    ('sma_20', $2, 'FLOAT64', 2, '20-period simple moving average'),
                    ('rsi_14', $2, 'FLOAT64', 4, '14-period relative strength index')
                ON CONFLICT (feature_name) DO NOTHING
            """, ohlcv_group.id, momentum_group.id)

        # Test retrieval
        ohlcv_catalog = await feature_dao.get_feature_catalog_by_group("ohlcv_basic")
        momentum_catalog = await feature_dao.get_feature_catalog_by_group("technical_momentum")
        
        # Verify catalog entries
        assert len(ohlcv_catalog) >= 3
        assert len(momentum_catalog) >= 2
        
        # Verify specific features
        ohlcv_features = {fc.feature_name for fc in ohlcv_catalog}
        expected_ohlcv = {"open", "close", "volume"}
        assert expected_ohlcv.issubset(ohlcv_features)
        
        momentum_features = {fc.feature_name for fc in momentum_catalog}
        expected_momentum = {"sma_20", "rsi_14"}
        assert expected_momentum.issubset(momentum_features)

    async def test_feature_patterns_system(self, feature_dao, test_environment):
        """Test that feature patterns work correctly."""
        # Get feature groups for foreign key references
        feature_groups = await feature_dao.get_feature_groups()
        momentum_group = next(fg for fg in feature_groups if fg.group_name == "technical_momentum")
        volatility_group = next(fg for fg in feature_groups if fg.group_name == "technical_volatility")

        # Insert test patterns
        async with test_environment.get_connection() as conn:
            await conn.execute("""
                INSERT INTO test_feature_patterns (pattern, feature_group_id, pattern_type, priority, description) 
                VALUES 
                    ('sma_', $1, 'starts_with', 100, 'Simple moving averages'),
                    ('ema_', $1, 'starts_with', 101, 'Exponential moving averages'),
                    ('bb_', $2, 'starts_with', 200, 'Bollinger Bands'),
                    ('.*_vol.*', $2, 'regex', 300, 'Volatility indicators')
            """, momentum_group.id, volatility_group.id)

        # Test pattern matching through DAO
        test_features = ["sma_50", "ema_12", "bb_upper", "realized_vol_20d", "unknown_feature"]
        results = await feature_dao.get_feature_mappings_batch(test_features)
        
        # Verify pattern matches
        results_dict = {r.feature_name: r for r in results}
        
        # SMA should match momentum group via pattern
        sma_result = results_dict["sma_50"]
        assert sma_result.feature_group_name == "technical_momentum"
        assert sma_result.match_type in ["pattern", "exact"]  # Could be exact if in catalog
        
        # EMA should match momentum group via pattern
        ema_result = results_dict["ema_12"]
        assert ema_result.feature_group_name == "technical_momentum"
        
        # BB should match volatility group via pattern
        bb_result = results_dict["bb_upper"]
        assert bb_result.feature_group_name == "technical_volatility"
        
        # Volatility regex should match
        vol_result = results_dict["realized_vol_20d"]
        assert vol_result.feature_group_name == "technical_volatility"
        assert vol_result.match_type == "pattern"

    async def test_feature_extraction_run_tracking(self, feature_dao, test_environment):
        """Test feature extraction run tracking functionality."""
        # Create test run
        test_run = FeatureExtractionRun(
            run_id="test_run_12345",
            run_type="feature_extraction",
            status="running",
            feature_groups=["ohlcv_basic", "technical_momentum"],
            date_range_start=date(2025, 7, 1),
            date_range_end=date(2025, 7, 31),
            total_instruments=10,
            command_line="python test_script.py",
            git_commit_hash="abc123def456",
            environment="test"
        )
        
        # Insert run
        run_id = await feature_dao.create_feature_extraction_run(test_run)
        assert run_id is not None
        assert isinstance(run_id, int)
        
        # Update run status
        await feature_dao.update_feature_extraction_run_status(
            run_id, 
            "completed", 
            results={"total_features": 1500, "files_created": 20}
        )
        
        # Verify run was tracked
        recent_runs = await feature_dao.get_recent_extraction_runs(limit=5)
        assert len(recent_runs) >= 1
        
        # Find our test run
        test_run_result = next((r for r in recent_runs if r.run_id == "test_run_12345"), None)
        assert test_run_result is not None
        assert test_run_result.status == "completed"
        assert test_run_result.results["total_features"] == 1500

    async def test_feature_availability_tracking(self, feature_dao, test_environment):
        """Test feature availability tracking for dataset discovery."""
        # Get feature groups
        feature_groups = await feature_dao.get_feature_groups()
        ohlcv_group = next(fg for fg in feature_groups if fg.group_name == "ohlcv_basic")
        
        # Create test availability record
        from domains.ml.services.training_data.dao.feature_extraction_dao import FeatureAvailability
        
        availability = FeatureAvailability(
            feature_group_id=ohlcv_group.id,
            instrument_id=1,
            symbol="AAPL",
            year_month=date(2025, 7, 1),
            file_path="/data/test/AAPL_2025_07_ohlcv_basic.arrayrecord",
            file_size_bytes=1024000,
            record_count=8640,  # 30 days * 288 5-minute intervals
            date_range_start=datetime(2025, 7, 1, 9, 30),
            date_range_end=datetime(2025, 7, 31, 16, 0),
            quality_score=0.95,
            validation_status="passed"
        )
        
        # Insert availability record
        availability_id = await feature_dao.create_feature_availability(availability)
        assert availability_id is not None
        
        # Test coverage query
        coverage = await feature_dao.get_feature_availability_coverage(
            symbols=["AAPL"],
            feature_groups=["ohlcv_basic"],
            start_date=date(2025, 7, 1),
            end_date=date(2025, 7, 31)
        )
        
        # Verify coverage results
        assert len(coverage) >= 1
        aapl_coverage = next((c for c in coverage if c.symbol == "AAPL"), None)
        assert aapl_coverage is not None
        assert aapl_coverage.quality_score == 0.95
        assert aapl_coverage.validation_status == "passed"
        assert aapl_coverage.record_count == 8640

    async def test_database_performance_with_real_data(self, feature_dao, test_environment):
        """Test database performance with realistic data volumes."""
        # Create larger dataset for performance testing
        feature_groups = await feature_dao.get_feature_groups()
        
        # Insert 100 feature catalog entries
        async with test_environment.get_connection() as conn:
            for i in range(100):
                group_id = feature_groups[i % len(feature_groups)].id
                await conn.execute("""
                    INSERT INTO test_feature_catalog (feature_name, feature_group_id, data_type, column_position) 
                    VALUES ($1, $2, 'FLOAT64', $3)
                    ON CONFLICT (feature_name) DO NOTHING
                """, f"perf_feature_{i:03d}", group_id, i)
        
        # Test batch mapping performance
        import time
        feature_names = [f"perf_feature_{i:03d}" for i in range(100)]
        
        start_time = time.time()
        results = await feature_dao.get_feature_mappings_batch(feature_names)
        execution_time = time.time() - start_time
        
        # Verify results
        assert len(results) == 100
        
        # Performance assertion - should complete quickly
        assert execution_time < 2.0  # Should complete within 2 seconds
        
        # Verify exact matches
        exact_matches = [r for r in results if r.match_type == "exact"]
        assert len(exact_matches) == 100

    async def test_cache_persistence_across_dao_instances(self, test_environment):
        """Test that cache works correctly across multiple DAO instances."""
        # Create first DAO instance and load cache
        dao1 = FeatureExtractionDAO(test_environment)
        results1 = await dao1.get_feature_mappings_batch(["open", "close", "sma_20"])
        
        # Create second DAO instance (fresh cache)
        dao2 = FeatureExtractionDAO(test_environment)
        results2 = await dao2.get_feature_mappings_batch(["open", "close", "sma_20"])
        
        # Results should be identical
        assert len(results1) == len(results2)
        for r1, r2 in zip(results1, results2):
            assert r1.feature_name == r2.feature_name
            assert r1.feature_group_name == r2.feature_group_name
            assert r1.match_type == r2.match_type

    async def test_transaction_rollback_on_error(self, feature_dao, test_environment):
        """Test that database transactions rollback properly on errors."""
        # Attempt to create run with invalid data
        invalid_run = FeatureExtractionRun(
            run_id="",  # Invalid empty run_id
            run_type="feature_extraction",
            status="running",
            feature_groups=["ohlcv_basic"],
            date_range_start=date(2025, 7, 1),
            date_range_end=date(2025, 7, 31),
            total_instruments=0
        )
        
        # This should fail and not leave partial data
        with pytest.raises(Exception):
            await feature_dao.create_feature_extraction_run(invalid_run)
        
        # Verify no partial data was left
        recent_runs = await feature_dao.get_recent_extraction_runs(limit=10)
        empty_run_ids = [r for r in recent_runs if r.run_id == ""]
        assert len(empty_run_ids) == 0

    async def test_database_schema_validation(self, test_environment):
        """Test that database schema matches expected structure."""
        async with test_environment.get_connection() as conn:
            # Check feature_groups table structure
            groups_columns = await conn.fetch("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'test_feature_groups'
                ORDER BY ordinal_position
            """)
            
            groups_column_names = [col['column_name'] for col in groups_columns]
            expected_groups_columns = [
                'id', 'group_name', 'display_name', 'description', 'category',
                'update_frequency', 'computation_lag_minutes', 'dependencies',
                'storage_format', 'retention_months', 'is_active', 'created_at', 'updated_at'
            ]
            
            for expected_col in expected_groups_columns:
                assert expected_col in groups_column_names, f"Missing column: {expected_col}"

            # Check feature_catalog table structure  
            catalog_columns = await conn.fetch("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'test_feature_catalog'
                ORDER BY ordinal_position
            """)
            
            catalog_column_names = [col['column_name'] for col in catalog_columns]
            expected_catalog_columns = [
                'feature_id', 'feature_name', 'feature_group_id', 'data_type',
                'column_position', 'description', 'computation_method', 'dependencies',
                'validation_rules', 'is_active', 'created_at', 'updated_at'
            ]
            
            for expected_col in expected_catalog_columns:
                assert expected_col in catalog_column_names, f"Missing column: {expected_col}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])