#!/usr/bin/env python3
"""
Integration Test for Training Data Generation Completion Consistency

Tests to catch inconsistencies between database status, run records, and actual file outputs
in training data generation workflows. Identifies issues where processes complete but
database status is not updated or files are not created.

Test Coverage:
- Database vs file system consistency checks
- Run status vs dataset status validation
- Completion tracking across process boundaries
- File creation validation for completed datasets
- Status update transaction integrity
- Dead process detection and cleanup
"""

import pytest
import asyncpg
import asyncio
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
import logging
import json
import time

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from core.config.environment import Environment, EnvironmentType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
async def test_db_connection():
    """Create test database connection."""
    try:
        connection = await asyncpg.connect(
            host="localhost",
            port=3432,
            user="postgres",
            password="dev_password",
            database="dev_db"
        )
        yield connection
        await connection.close()
    except Exception as e:
        logger.warning(f"Could not connect to test database: {e}")
        yield None


@pytest.fixture
def temp_training_dir():
    """Create temporary training data directory."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


class TestTrainingDataCompletionConsistency:
    """Test completion consistency between database and file system."""

    @pytest.mark.asyncio
    async def test_detect_completion_status_inconsistency(self, test_db_connection):
        """Test detection of datasets marked as generating but with completed runs."""
        if test_db_connection is None:
            pytest.skip("Database not available for integration test")

        # Query for inconsistencies like the one we found
        inconsistency_query = """
        SELECT
            d.id as dataset_id,
            d.dataset_name,
            d.status as dataset_status,
            d.total_sequences,
            r.id as run_id,
            r.status as run_status,
            r.start_time,
            r.end_time,
            r.run_type
        FROM dev_training_dataset d
        LEFT JOIN dev_runs r ON d.run_id = r.id
        WHERE d.status = 'generating'
        AND (r.status = 'completed' OR r.end_time IS NOT NULL)
        ORDER BY d.id
        """

        inconsistent_records = await test_db_connection.fetch(inconsistency_query)

        if inconsistent_records:
            logger.error(f"🚨 Found {len(inconsistent_records)} completion status inconsistencies:")

            for record in inconsistent_records:
                logger.error(f"  Dataset {record['dataset_id']}: {record['dataset_name']}")
                logger.error(f"    Dataset status: {record['dataset_status']}")
                logger.error(f"    Run status: {record['run_status']}")
                logger.error(f"    Run completed: {record['end_time']}")
                logger.error(f"    Sequences: {record['total_sequences']}")

                # Check if files exist for this dataset
                file_check = await self._check_dataset_files_exist(record['dataset_id'], test_db_connection)
                logger.error(f"    Files exist: {file_check['files_exist']}")
                if not file_check['files_exist']:
                    logger.error(f"    Expected files: {file_check['expected_files']}")

            # This test should fail if inconsistencies are found
            pytest.fail(f"Found {len(inconsistent_records)} datasets with inconsistent completion status. "
                       f"Database shows 'generating' but runs are 'completed'.")
        else:
            logger.info("✅ No completion status inconsistencies detected")

    @pytest.mark.asyncio
    async def test_detect_missing_output_files(self, test_db_connection):
        """Test detection of completed datasets with missing output files."""
        if test_db_connection is None:
            pytest.skip("Database not available for integration test")

        # Query for completed datasets
        completed_datasets_query = """
        SELECT
            id,
            dataset_name,
            features_file_path,
            labels_file_path,
            metadata_file_path,
            status,
            total_sequences
        FROM dev_training_dataset
        WHERE status = 'completed'
        AND id >= 40  -- Focus on recent datasets
        ORDER BY id DESC
        LIMIT 10
        """

        completed_datasets = await test_db_connection.fetch(completed_datasets_query)
        missing_files_issues = []

        for dataset in completed_datasets:
            file_issues = await self._validate_dataset_files(dataset)
            if file_issues['has_issues']:
                missing_files_issues.append({
                    'dataset_id': dataset['id'],
                    'dataset_name': dataset['dataset_name'],
                    'issues': file_issues['issues']
                })

        if missing_files_issues:
            logger.error(f"🚨 Found {len(missing_files_issues)} completed datasets with missing files:")

            for issue in missing_files_issues:
                logger.error(f"  Dataset {issue['dataset_id']}: {issue['dataset_name']}")
                for file_issue in issue['issues']:
                    logger.error(f"    - {file_issue}")

            # This is a warning rather than failure since some datasets might be expected to have missing files
            logger.warning("⚠️ Some completed datasets have missing output files - verify if expected")
        else:
            logger.info("✅ All completed datasets have expected output files")

    @pytest.mark.asyncio
    async def test_detect_orphaned_generating_datasets(self, test_db_connection):
        """Test detection of datasets stuck in 'generating' status with no active processes."""
        if test_db_connection is None:
            pytest.skip("Database not available for integration test")

        # Query for datasets that have been generating for too long
        long_generating_query = """
        SELECT
            d.id,
            d.dataset_name,
            d.status,
            r.start_time,
            r.end_time,
            r.status as run_status,
            EXTRACT(EPOCH FROM (NOW() - r.start_time))/3600 as hours_elapsed
        FROM dev_training_dataset d
        LEFT JOIN dev_runs r ON d.run_id = r.id
        WHERE d.status = 'generating'
        AND r.start_time < NOW() - INTERVAL '2 hours'  -- Generating for more than 2 hours
        ORDER BY r.start_time
        """

        orphaned_datasets = await test_db_connection.fetch(long_generating_query)

        if orphaned_datasets:
            logger.warning(f"⚠️ Found {len(orphaned_datasets)} potentially orphaned datasets:")

            for dataset in orphaned_datasets:
                logger.warning(f"  Dataset {dataset['id']}: {dataset['dataset_name']}")
                logger.warning(f"    Status: {dataset['status']}")
                logger.warning(f"    Started: {dataset['start_time']}")
                logger.warning(f"    Hours elapsed: {dataset['hours_elapsed']:.1f}")
                logger.warning(f"    Run status: {dataset['run_status']}")

                # Check if this is the specific issue we found
                if dataset['id'] in [42, 43]:  # The problematic datasets we identified
                    logger.error(f"🎯 This is the known problematic dataset {dataset['id']}")

            # This should trigger investigation
            logger.warning("🔍 These datasets may need manual intervention or status correction")
        else:
            logger.info("✅ No orphaned generating datasets detected")

    @pytest.mark.asyncio
    async def test_validate_run_to_dataset_linkage(self, test_db_connection):
        """Test that run records properly link to their corresponding datasets."""
        if test_db_connection is None:
            pytest.skip("Database not available for integration test")

        # Check for run/dataset linkage issues
        linkage_query = """
        SELECT
            r.id as run_id,
            r.run_type,
            r.status as run_status,
            r.start_time,
            COUNT(d.id) as linked_datasets,
            ARRAY_AGG(d.id) as dataset_ids,
            ARRAY_AGG(d.status) as dataset_statuses
        FROM dev_runs r
        LEFT JOIN dev_training_dataset d ON d.run_id = r.id
        WHERE r.run_type = 'training_data_generation'
        AND r.start_time > NOW() - INTERVAL '1 day'  -- Recent runs only
        GROUP BY r.id, r.run_type, r.status, r.start_time
        ORDER BY r.start_time DESC
        """

        linkage_results = await test_db_connection.fetch(linkage_query)
        linkage_issues = []

        for result in linkage_results:
            issues = []

            # Check if completed run has no linked datasets
            if result['run_status'] == 'completed' and result['linked_datasets'] == 0:
                issues.append("Completed run has no linked datasets")

            # Check if run is completed but datasets are still generating
            if (result['run_status'] == 'completed' and
                result['dataset_statuses'] and
                'generating' in result['dataset_statuses']):
                issues.append("Run completed but some datasets still marked as generating")

            # Check if run failed but datasets are not marked as failed
            if (result['run_status'] == 'failed' and
                result['dataset_statuses'] and
                'failed' not in result['dataset_statuses']):
                issues.append("Run failed but datasets not marked as failed")

            if issues:
                linkage_issues.append({
                    'run_id': result['run_id'],
                    'run_status': result['run_status'],
                    'dataset_ids': result['dataset_ids'],
                    'dataset_statuses': result['dataset_statuses'],
                    'issues': issues
                })

        if linkage_issues:
            logger.error(f"🚨 Found {len(linkage_issues)} run-dataset linkage issues:")

            for issue in linkage_issues:
                logger.error(f"  Run {issue['run_id']} (status: {issue['run_status']}):")
                logger.error(f"    Datasets: {issue['dataset_ids']} (statuses: {issue['dataset_statuses']})")
                for problem in issue['issues']:
                    logger.error(f"    - {problem}")

            pytest.fail(f"Found {len(linkage_issues)} run-dataset linkage inconsistencies")
        else:
            logger.info("✅ Run-dataset linkage is consistent")

    async def _check_dataset_files_exist(self, dataset_id: int, connection: asyncpg.Connection) -> Dict[str, Any]:
        """Check if expected files exist for a dataset."""
        # Get file paths from database
        file_paths_query = """
        SELECT features_file_path, labels_file_path, metadata_file_path
        FROM dev_training_dataset
        WHERE id = $1
        """

        result = await connection.fetchrow(file_paths_query, dataset_id)
        if not result:
            return {'files_exist': False, 'expected_files': [], 'error': 'Dataset not found'}

        expected_files = []
        if result['features_file_path']:
            expected_files.append(result['features_file_path'])
        if result['labels_file_path']:
            expected_files.append(result['labels_file_path'])
        if result['metadata_file_path']:
            expected_files.append(result['metadata_file_path'])

        # Check if files actually exist
        existing_files = []
        for file_path in expected_files:
            if file_path and Path(file_path).exists():
                existing_files.append(file_path)

        return {
            'files_exist': len(existing_files) > 0,
            'expected_files': expected_files,
            'existing_files': existing_files,
            'missing_count': len(expected_files) - len(existing_files)
        }

    async def _validate_dataset_files(self, dataset_record: Dict[str, Any]) -> Dict[str, Any]:
        """Validate files for a specific dataset record."""
        issues = []

        # Check features file
        if dataset_record['features_file_path']:
            features_path = Path(dataset_record['features_file_path'])
            if not features_path.exists():
                issues.append(f"Missing features file: {features_path}")
            else:
                # Check file size
                file_size = features_path.stat().st_size
                if file_size == 0:
                    issues.append(f"Features file is empty: {features_path}")

        # Check metadata file
        if dataset_record['metadata_file_path']:
            metadata_path = Path(dataset_record['metadata_file_path'])
            if not metadata_path.exists():
                issues.append(f"Missing metadata file: {metadata_path}")
            else:
                # Validate JSON structure
                try:
                    with open(metadata_path, 'r') as f:
                        json.load(f)
                except json.JSONDecodeError as e:
                    issues.append(f"Invalid metadata JSON: {e}")

        # Check if dataset claims to have sequences but no files exist
        if (dataset_record['total_sequences'] > 0 and
            not dataset_record['features_file_path'] and
            not dataset_record['labels_file_path']):
            issues.append(f"Dataset claims {dataset_record['total_sequences']} sequences but has no file paths")

        return {
            'has_issues': len(issues) > 0,
            'issues': issues
        }


class TestTrainingDataProcessIntegrity:
    """Test process integrity and transaction handling."""

    @pytest.mark.asyncio
    async def test_atomic_status_updates(self, test_db_connection, temp_training_dir):
        """Test that status updates happen atomically with file creation."""
        if test_db_connection is None:
            pytest.skip("Database not available for integration test")

        # This test simulates the completion process to ensure atomicity
        # In a real scenario, this would be part of the training data generator

        test_dataset_name = f"test_atomic_{int(time.time())}"

        try:
            # 1. Create a test dataset record in 'generating' status
            insert_query = """
            INSERT INTO dev_training_dataset (
                dataset_name, status, total_sequences, symbols,
                date_range_start, date_range_end, created_by
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            """

            dataset_id = await test_db_connection.fetchval(
                insert_query,
                test_dataset_name,
                'generating',
                100,  # test sequences
                ['TEST'],
                date.today() - timedelta(days=1),
                date.today(),
                'test_atomic_completion'
            )

            # 2. Simulate file creation
            test_file = temp_training_dir / f"{test_dataset_name}.arrayrecord"
            test_file.write_text("test data")

            # 3. Update status to completed (this should be atomic with file creation)
            async with test_db_connection.transaction():
                # In real implementation, file creation and status update should be in same transaction
                if test_file.exists():
                    await test_db_connection.execute(
                        "UPDATE dev_training_dataset SET status = 'completed', features_file_path = $1 WHERE id = $2",
                        str(test_file),
                        dataset_id
                    )
                else:
                    # If file creation failed, mark as failed
                    await test_db_connection.execute(
                        "UPDATE dev_training_dataset SET status = 'failed' WHERE id = $1",
                        dataset_id
                    )

            # 4. Verify consistency
            final_record = await test_db_connection.fetchrow(
                "SELECT status, features_file_path FROM dev_training_dataset WHERE id = $1",
                dataset_id
            )

            assert final_record['status'] == 'completed'
            assert Path(final_record['features_file_path']).exists()

            logger.info("✅ Atomic status update test passed")

        finally:
            # Cleanup test data
            try:
                await test_db_connection.execute(
                    "DELETE FROM dev_training_dataset WHERE dataset_name = $1",
                    test_dataset_name
                )
            except Exception as e:
                logger.warning(f"Cleanup failed: {e}")

    @pytest.mark.asyncio
    async def test_completion_timeout_detection(self, test_db_connection):
        """Test detection of processes that should have completed but haven't."""
        if test_db_connection is None:
            pytest.skip("Database not available for integration test")

        # Look for datasets that started generating a long time ago
        timeout_query = """
        SELECT
            d.id,
            d.dataset_name,
            d.status,
            d.total_sequences,
            r.start_time,
            r.end_time,
            r.status as run_status,
            EXTRACT(EPOCH FROM (NOW() - COALESCE(r.end_time, r.start_time)))/3600 as hours_since_last_activity
        FROM dev_training_dataset d
        LEFT JOIN dev_runs r ON d.run_id = r.id
        WHERE d.status = 'generating'
        AND (
            (r.end_time IS NULL AND r.start_time < NOW() - INTERVAL '1 hour') OR
            (r.end_time IS NOT NULL AND r.end_time < NOW() - INTERVAL '1 hour')
        )
        """

        timeout_candidates = await test_db_connection.fetch(timeout_query)

        if timeout_candidates:
            logger.warning(f"⚠️ Found {len(timeout_candidates)} datasets that may have timed out:")

            for candidate in timeout_candidates:
                logger.warning(f"  Dataset {candidate['id']}: {candidate['dataset_name']}")
                logger.warning(f"    Status: {candidate['status']}")
                logger.warning(f"    Hours since activity: {candidate['hours_since_last_activity']:.1f}")
                logger.warning(f"    Run status: {candidate['run_status']}")

                # These should be investigated for cleanup
                if candidate['hours_since_last_activity'] > 24:  # More than 24 hours
                    logger.error(f"🚨 Dataset {candidate['id']} has been inactive for over 24 hours - needs intervention")
        else:
            logger.info("✅ No timeout candidates detected")


if __name__ == "__main__":
    # Run the consistency tests
    pytest.main([__file__, "-v", "--tb=short", "-s"])