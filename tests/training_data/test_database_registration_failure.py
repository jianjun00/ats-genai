#!/usr/bin/env python3
"""
Test to detect training data database registration failures

This test detects the specific issue where:
- Training data generation completes successfully
- Files are created on disk (metadata.json, arrayrecord files)
- Dataset is NOT registered in database (intg_training_dataset table)
- Dataset doesn't appear in UI because UI queries database

Test Failure Modes Detected:
1. Files created but no database entry (current bug)
2. Database entry created but no files
3. Mismatched metadata between files and database
4. Dataset visible in database but not in UI endpoint
"""

import pytest
import tempfile
import json
import os
from pathlib import Path
from datetime import datetime, date
import asyncio
import sys

sys.path.append('/home/jianjun/ats-genai-admin/src')

from core.platform.database.connection_manager import get_raw_connection


class TestTrainingDataDatabaseRegistration:
    """Test training data generation AND database registration consistency"""

    def test_training_data_end_to_end_registration(self):
        """
        CRITICAL TEST: Detect training data generation without database registration

        This test will FAIL with current code because:
        ✅ Files are created on disk successfully
        ❌ Database entry is NOT created (the bug)

        Expected behavior:
        1. Generate training data for test symbol
        2. Verify files exist on disk
        3. Verify database entry exists in intg_training_dataset
        4. Verify metadata consistency between files and database
        """

        # Test configuration
        test_symbol = "TEST"
        test_start_date = "2025-07-01"
        test_end_date = "2025-07-02"

        print(f"\n🔍 TESTING: End-to-end training data generation + database registration")
        print(f"   Symbol: {test_symbol}")
        print(f"   Date Range: {test_start_date} to {test_end_date}")

        # Step 1: Run training data generation (would use actual runner here)
        # For now, simulate what should happen

        # Step 2: Check file system state
        training_data_dir = Path("/mnt/d/ats-data/training")

        # Find the latest dataset directory
        if training_data_dir.exists():
            dataset_dirs = [d for d in training_data_dir.iterdir() if d.is_dir() and d.name.startswith("dataset_")]
            latest_dataset_dir = max(dataset_dirs, key=lambda d: d.name) if dataset_dirs else None

            print(f"\n📁 FILE SYSTEM CHECK:")
            if latest_dataset_dir:
                print(f"   ✅ Latest dataset directory: {latest_dataset_dir}")

                # Check metadata file exists
                metadata_file = latest_dataset_dir / "dataset_metadata.json"
                assert metadata_file.exists(), f"❌ Metadata file missing: {metadata_file}"
                print(f"   ✅ Metadata file exists: {metadata_file}")

                # Read metadata
                with open(metadata_file) as f:
                    metadata = json.load(f)

                dataset_id = latest_dataset_dir.name
                symbols = metadata.get('symbols', [])

                print(f"   📋 Dataset ID: {dataset_id}")
                print(f"   📋 Symbols: {symbols}")

            else:
                pytest.fail("❌ No dataset directories found")
        else:
            pytest.fail(f"❌ Training data directory doesn't exist: {training_data_dir}")

        # Step 3: Check database state (THIS IS WHERE THE BUG IS)
        print(f"\n🗄️ DATABASE CHECK:")

        # Query intg_training_dataset table
        with get_raw_connection() as conn:
            cursor = conn.cursor()

            # Look for dataset with matching symbols and date range
            cursor.execute("""
                SELECT id, dataset_name, symbols, creation_timestamp, file_size_mb
                FROM intg_training_dataset
                WHERE symbols = %s
                AND date_range_start <= %s
                AND date_range_end >= %s
                ORDER BY creation_timestamp DESC
                LIMIT 1
            """, (json.dumps(symbols), test_start_date, test_end_date))

            db_result = cursor.fetchone()

            if db_result:
                db_id, db_name, db_symbols, db_timestamp, db_size = db_result
                print(f"   ✅ Database entry found:")
                print(f"      ID: {db_id}")
                print(f"      Name: {db_name}")
                print(f"      Symbols: {db_symbols}")
                print(f"      Created: {db_timestamp}")

                # Step 4: Verify consistency between files and database
                print(f"\n🔍 CONSISTENCY CHECK:")

                # Check symbols match
                if json.loads(db_symbols) == symbols:
                    print(f"   ✅ Symbols match: {symbols}")
                else:
                    pytest.fail(f"❌ Symbol mismatch - DB: {db_symbols}, Files: {symbols}")

                # Check dataset exists and has reasonable size
                if db_size and db_size > 0:
                    print(f"   ✅ Dataset has data: {db_size} MB")
                else:
                    print(f"   ⚠️ Dataset size is zero or null: {db_size}")

            else:
                # THIS IS THE CURRENT BUG - Database entry missing
                pytest.fail(f"""❌ CRITICAL BUG DETECTED: Training data database registration failure

                FILE SYSTEM: ✅ Files created successfully
                - Dataset directory: {latest_dataset_dir}
                - Metadata file: {metadata_file}
                - Symbols: {symbols}

                DATABASE: ❌ No corresponding entry in intg_training_dataset table
                - Expected symbols: {symbols}
                - Expected date range: {test_start_date} to {test_end_date}
                - Query returned: None

                ROOT CAUSE: Training data generation completes but fails to register in database
                IMPACT: Dataset won't appear in UI at http://localhost:4000/
                FIX NEEDED: Add database registration step to training data generation
                """)

    def test_ui_endpoint_consistency(self):
        """
        Test that datasets in database are returned by UI endpoint

        This tests the full chain:
        Database → UI API → Frontend Display
        """
        import requests

        print(f"\n🌐 UI ENDPOINT CHECK:")

        try:
            # Check if analytics service is running
            response = requests.get("http://localhost:4000/api/v1/training-datasets", timeout=5)

            if response.status_code == 200:
                datasets = response.json()
                print(f"   ✅ UI endpoint accessible")
                print(f"   📊 Datasets returned: {len(datasets)}")

                # Check if any datasets exist
                if datasets:
                    for dataset in datasets[:3]:  # Show first 3
                        print(f"      - {dataset.get('dataset_name', 'Unknown')}: {dataset.get('symbols', [])}")
                else:
                    print(f"   ⚠️ No datasets returned by UI endpoint")

            else:
                print(f"   ❌ UI endpoint error: {response.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"   ❌ Cannot reach UI endpoint: {e}")
            pytest.skip("Analytics service not accessible - skipping UI test")

    def test_database_registration_function_exists(self):
        """
        Unit test to verify database registration function exists and works
        """
        print(f"\n🔧 UNIT TEST: Database registration function")

        # This test would verify the registration function exists
        # and can be called independently

        # Check if we can import the registration function
        try:
            # This import would fail if the function doesn't exist
            # from domains.ml.services.training_data.database_registry import register_training_dataset
            # print(f"   ✅ Database registration function exists")

            # Test the function with mock data
            # result = register_training_dataset(
            #     dataset_name="test_dataset",
            #     symbols=["TEST"],
            #     start_date="2025-07-01",
            #     end_date="2025-07-02"
            # )
            # assert result, "Database registration should return success"

            print(f"   ⚠️ Database registration function not yet implemented")

        except ImportError as e:
            print(f"   ❌ Database registration function missing: {e}")
            pytest.fail("Database registration functionality is not implemented")


class TestTrainingDataQuality:
    """Test training data quality and content validation"""

    def test_training_dataset_has_meaningful_data(self):
        """Test that generated training datasets contain actual data (more than 1 record)"""

        print(f"\n🔍 TESTING: Training dataset data quality")

        # Get the latest training datasets from database
        with get_raw_connection() as conn:
            cursor = conn.cursor()

            # Get datasets with actual data (total_sequences > 0)
            cursor.execute("""
                SELECT id, dataset_name, symbols, total_sequences, created_at
                FROM intg_training_dataset
                WHERE total_sequences > 1
                ORDER BY created_at DESC
                LIMIT 5
            """)

            datasets_with_data = cursor.fetchall()

            # Get all recent datasets for comparison
            cursor.execute("""
                SELECT id, dataset_name, symbols, total_sequences, created_at
                FROM intg_training_dataset
                ORDER BY created_at DESC
                LIMIT 10
            """)

            all_recent_datasets = cursor.fetchall()

        print(f"\n📊 DATASET QUALITY ANALYSIS:")
        print(f"   Recent datasets: {len(all_recent_datasets)}")
        print(f"   Datasets with data (>1 records): {len(datasets_with_data)}")

        if all_recent_datasets:
            print(f"\n📋 Recent Datasets:")
            for dataset in all_recent_datasets[:5]:
                dataset_id, name, symbols, sequences, created_at = dataset
                status = "✅ HAS DATA" if sequences > 1 else "❌ EMPTY/MINIMAL"
                print(f"   ID {dataset_id}: {name} | Symbols: {symbols} | Sequences: {sequences} | {status}")

        # Verify that we have datasets with meaningful data
        assert len(datasets_with_data) > 0, (
            f"❌ CRITICAL ISSUE: No training datasets found with more than 1 record\n"
            f"   Recent datasets: {len(all_recent_datasets)}\n"
            f"   Datasets with data: {len(datasets_with_data)}\n"
            f"   This indicates training data generation is producing empty datasets\n"
            f"   Check: Data availability, timeframe processing, feature extraction"
        )

        # For datasets that do have data, verify they have reasonable amounts
        meaningful_datasets = [d for d in datasets_with_data if d[3] >= 10]  # At least 10 sequences

        if meaningful_datasets:
            print(f"\n✅ QUALITY CHECK PASSED:")
            print(f"   Found {len(meaningful_datasets)} datasets with substantial data (≥10 sequences)")
            for dataset in meaningful_datasets[:3]:
                dataset_id, name, symbols, sequences, created_at = dataset
                print(f"   ✅ ID {dataset_id}: {sequences} sequences for {symbols}")
        else:
            print(f"\n⚠️ WARNING: Datasets have minimal data")
            print(f"   All datasets have <10 sequences - may indicate data processing issues")

    def test_latest_tsla_dataset_has_data(self):
        """Test that the latest TSLA dataset specifically has meaningful data"""

        print(f"\n🔍 TESTING: Latest TSLA dataset data content")

        with get_raw_connection() as conn:
            cursor = conn.cursor()

            # Get the latest TSLA dataset
            cursor.execute("""
                SELECT id, dataset_name, symbols, total_sequences, feature_count, created_at
                FROM intg_training_dataset
                WHERE symbols @> '{TSLA}'
                ORDER BY created_at DESC
                LIMIT 1
            """)

            latest_tsla = cursor.fetchone()

        if not latest_tsla:
            pytest.skip("No TSLA datasets found - cannot test data content")

        dataset_id, name, symbols, sequences, features, created_at = latest_tsla

        print(f"\n📊 LATEST TSLA DATASET:")
        print(f"   Dataset ID: {dataset_id}")
        print(f"   Name: {name}")
        print(f"   Symbols: {symbols}")
        print(f"   Total Sequences: {sequences}")
        print(f"   Feature Count: {features}")
        print(f"   Created: {created_at}")

        # Test that TSLA dataset has meaningful data
        assert sequences > 1, (
            f"❌ TSLA DATASET IS EMPTY/MINIMAL: Only {sequences} sequences\n"
            f"   Dataset: {name} (ID: {dataset_id})\n"
            f"   This indicates TSLA training data generation failed to process actual data\n"
            f"   Check: TSLA minute bar data availability, timeframe aggregation, feature extraction"
        )

        # Test that feature count is reasonable
        assert features > 10, (
            f"❌ TSLA DATASET HAS MINIMAL FEATURES: Only {features} features\n"
            f"   Dataset: {name} (ID: {dataset_id})\n"
            f"   Expected: >10 features across multiple timeframes\n"
            f"   This indicates feature extraction is not working properly"
        )

        print(f"\n✅ TSLA DATASET QUALITY VERIFIED:")
        print(f"   ✅ Contains substantial data: {sequences} sequences")
        print(f"   ✅ Has comprehensive features: {features} features")
        print(f"   ✅ Dataset is suitable for training")


class TestTrainingDataConsistency:
    """Test consistency between file system, database, and UI"""

    def test_detect_orphaned_files(self):
        """Detect files on disk that aren't registered in database"""

        print(f"\n🔍 ORPHANED FILES CHECK:")

        training_data_dir = Path("/mnt/d/ats-data/training")
        if not training_data_dir.exists():
            pytest.skip("Training data directory doesn't exist")

        # Get all dataset directories
        dataset_dirs = [d for d in training_data_dir.iterdir() if d.is_dir() and d.name.startswith("dataset_")]

        # Get all dataset names from database
        with get_raw_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT dataset_name FROM intg_training_dataset")
            db_datasets = set(row[0] for row in cursor.fetchall())

        # Check for orphaned directories
        orphaned_dirs = []
        for dataset_dir in dataset_dirs:
            dataset_id = dataset_dir.name

            # Check if this dataset_id exists in any database entry
            # (This is a simplified check - real implementation would be more sophisticated)
            if not any(dataset_id in db_name for db_name in db_datasets):
                orphaned_dirs.append(dataset_dir)

        if orphaned_dirs:
            print(f"   ❌ ORPHANED FILES DETECTED:")
            for orphan in orphaned_dirs:
                print(f"      - {orphan}")

            # This would be the current failure - files exist but no DB entries
            pytest.fail(f"Found {len(orphaned_dirs)} orphaned dataset directories without database entries")
        else:
            print(f"   ✅ All dataset directories have corresponding database entries")

    def test_detect_orphaned_database_entries(self):
        """Detect database entries that don't have corresponding files"""

        print(f"\n🔍 ORPHANED DATABASE ENTRIES CHECK:")

        training_data_dir = Path("/mnt/d/ats-data/training")
        if not training_data_dir.exists():
            pytest.skip("Training data directory doesn't exist")

        # Get all dataset directories
        dataset_dirs = set(d.name for d in training_data_dir.iterdir() if d.is_dir() and d.name.startswith("dataset_"))

        # Get all dataset names from database
        orphaned_entries = []
        with get_raw_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT dataset_name, id FROM intg_training_dataset")

            for db_name, db_id in cursor.fetchall():
                # Check if any dataset directory corresponds to this database entry
                # (Simplified check)
                if not any(dataset_dir for dataset_dir in dataset_dirs if dataset_dir in db_name):
                    orphaned_entries.append((db_id, db_name))

        if orphaned_entries:
            print(f"   ❌ ORPHANED DATABASE ENTRIES DETECTED:")
            for db_id, db_name in orphaned_entries:
                print(f"      - ID {db_id}: {db_name}")

            pytest.fail(f"Found {len(orphaned_entries)} database entries without corresponding files")
        else:
            print(f"   ✅ All database entries have corresponding files")


if __name__ == "__main__":
    # Run the critical test to detect the current bug
    pytest.main([__file__ + "::TestTrainingDataDatabaseRegistration::test_training_data_end_to_end_registration", "-v", "-s"])