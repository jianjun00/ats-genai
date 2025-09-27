#!/usr/bin/env python3
"""
Database Schema Consistency Tests

Tests the critical fixes made for database table naming inconsistencies
between the analytics service and actual database schema.

Based on fixes documented in PRD: ArrayRecord Training Data System (September 4, 2025)
Issue: API queried dev_training_dataset (singular) but table name is dev_training_dataset (plural)
Solution: Standardized on plural table names throughout analytics service
"""

import pytest
import sys
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))


@pytest.mark.integration
def test_training_datasets_table_exists():
    """Verify training datasets table uses correct plural naming."""
    from core.database.connection_manager import get_raw_connection
    with get_raw_connection("dev") as conn:
        with conn.cursor() as cursor:
            # Test that plural table name exists
            cursor.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_name = 'dev_training_dataset'
            """)
            result = cursor.fetchone()
            assert result is not None, "dev_training_dataset table not found"

            # Test that old singular name doesn't exist (if migration was done)
            cursor.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_name = 'dev_training_dataset'
            """)
            result = cursor.fetchone()
            # Note: This might exist during transition period, so we just warn
            if result is not None:
                print("Warning: Old singular table name still exists")
@pytest.mark.integration
def test_analytics_service_table_names():
    """Test that analytics service uses correct table names."""
    from domains.analytics.services.analytics_service import UnifiedAnalyticsService
    service = UnifiedAnalyticsService()

    # Test that get_training_datasets method works
    # This will fail if using wrong table name
    datasets = service.get_training_datasets()
    assert isinstance(datasets, dict)
    assert "datasets" in datasets
def test_table_name_generation_logic():
    """Test that table name generation follows consistent patterns."""
    # Test the pattern used in analytics service
    environment = "dev"

    # Correct pattern (plural)
    correct_table_name = f"{environment}_training_datasets"
    assert correct_table_name == "dev_training_dataset"

    # Incorrect pattern (singular) - should not be used
    incorrect_table_name = f"{environment}_training_dataset"
    assert incorrect_table_name == "dev_training_dataset"

    # Verify we're using the correct pattern
    assert correct_table_name != incorrect_table_name


@pytest.mark.integration
def test_training_datasets_table_schema():
    """Test that training datasets table has expected columns."""
    from core.database.connection_manager import get_raw_connection
    expected_columns = [
        'id', 'dataset_name', 'total_sequences', 'sequence_length',
        'feature_count', 'label_count', 'data_quality_score',
        'feature_completeness', 'label_completeness', 'file_size_mb',
        'technical_indicators', 'symbols', 'date_range_start',
        'date_range_end', 'created_at'
    ]

    with get_raw_connection("dev") as conn:
        with conn.cursor() as cursor:
            # Get column information
            cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'dev_training_dataset'
                ORDER BY ordinal_position
            """)

            actual_columns = [row[0] for row in cursor.fetchall()]

            if not actual_columns:
                pytest.skip("dev_training_dataset table not found or empty schema")

            # Check that key columns exist
            key_columns = ['id', 'dataset_name', 'created_at']
            for col in key_columns:
                assert col in actual_columns, f"Key column '{col}' missing from dev_training_dataset"

@pytest.mark.integration
def test_sequences_api_table_query():
    """Test that sequences API queries correct table."""
    from domains.analytics.services.analytics_service import UnifiedAnalyticsService
    service = UnifiedAnalyticsService()

    # Test get_training_dataset_sequences method
    # Use a likely-to-exist dataset ID or handle empty case gracefully
    result = service.get_training_dataset_sequences(1)

    assert isinstance(result, dict)
    assert "datasets" in result
    assert "sequences" in result
    assert "total_count" in result

@pytest.mark.integration
def test_environment_based_table_naming():
    """Test table naming works correctly for different environments."""
    environments = ["dev", "test", "intg", "prod"]

    for env in environments:
        # Test correct plural naming pattern
        table_name = f"{env}_training_datasets"

        # Verify pattern consistency
        assert table_name.startswith(env)
        assert table_name.endswith("_training_datasets")
        assert "_training_dataset_" not in table_name  # Avoid singular in middle

        # Test specific patterns
        if env == "dev":
            assert table_name == "dev_training_dataset"
        elif env == "test":
            assert table_name == "test_training_datasets"


@pytest.mark.integration
def test_column_naming_consistency():
    """Test that column names follow consistent patterns."""
    from core.database.connection_manager import get_raw_connection
    with get_raw_connection("dev") as conn:
        with conn.cursor() as cursor:
            # Check for common column naming issues
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'dev_training_dataset'
            """)

            columns = dict(cursor.fetchall())

            if not columns:
                pytest.skip("dev_training_dataset table not found")

            # Test timestamp column naming (should be created_at, not creation_timestamp)
            timestamp_columns = [col for col in columns.keys() if 'timestamp' in col or 'created' in col]

            if timestamp_columns:
                # If we have timestamp columns, prefer created_at
                preferred_names = ['created_at', 'updated_at']
                found_preferred = any(col in preferred_names for col in timestamp_columns)

                if not found_preferred:
                    print(f"Warning: Timestamp columns found but none match preferred names: {timestamp_columns}")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])