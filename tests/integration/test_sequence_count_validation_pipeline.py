#!/usr/bin/env python3
"""
Sequence Count Validation Pipeline Test Suite

CRITICAL: These tests validate that the sequence count reported by the API
matches the actual training data generated and stored in ArrayRecord files.

The bug was: Database claimed 3,216 sequences, but API returned only 1 sequence.
These tests ensure sequence count consistency across the entire pipeline:
Training Data Generation → Database Storage → API Response → UI Display
"""

import pytest
import asyncio
import asyncpg
import tempfile
import shutil
import json
import requests
from pathlib import Path
from datetime import datetime
from unittest.mock import patch
from typing import Dict, List, Any

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestSequenceCountValidationPipeline:
    """Comprehensive sequence count validation across the entire data pipeline."""

    @pytest.fixture
    async def db_connection(self):
        """Database connection for testing."""
        conn = await asyncpg.connect(
            host="localhost",
            port=3432,
            user="postgres",
            password="dev_password",
            database="dev_db"
        )
        yield conn
        await conn.close()

    @pytest.fixture
    def temp_training_dir(self):
        """Create temporary training data directory."""
        temp_dir = tempfile.mkdtemp()
        training_data_dir = Path(temp_dir) / "training_data"
        training_data_dir.mkdir()
        yield training_data_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    async def clean_test_data(self, db_connection):
        """Clean test data before and after."""
        await db_connection.execute("""
            DELETE FROM dev_training_datasets
            WHERE dataset_name LIKE 'test_sequence_count_%'
        """)
        yield
        await db_connection.execute("""
            DELETE FROM dev_training_datasets
            WHERE dataset_name LIKE 'test_sequence_count_%'
        """)

    def create_training_data_with_known_sequences(
        self, training_dir: Path, run_id: int, symbol: str,
        sequences_per_timeframe: int, timeframes: List[str] = None
    ) -> Dict[str, Any]:
        """
        Create training data files with precisely known sequence counts.

        Returns metadata about what was created for validation.
        """
        if timeframes is None:
            timeframes = ["5m", "15m", "1h", "1d", "1w"]

        run_dir = training_dir / str(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)

        total_sequences = 0
        created_files = []
        sequence_distribution = {}

        for timeframe in timeframes:
            timeframe_dir = run_dir / timeframe
            timeframe_dir.mkdir(exist_ok=True)

            # Create ArrayRecord file with exact number of sequences
            arrayrecord_file = timeframe_dir / f"{symbol}_20250701_000000_20250906_000000.arrayrecord"

            # Create sequences as JSON records (mock ArrayRecord format)
            sequences = []
            for i in range(sequences_per_timeframe):
                sequence = {
                    "sequence_id": i,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "run_id": run_id,
                    "ohlc_data": [
                        {"open": 100 + i, "high": 105 + i, "low": 95 + i, "close": 102 + i}
                        for j in range(21)  # 21-bar sequences
                    ]
                }
                sequences.append(sequence)

            # Write sequences to file (mock ArrayRecord format)
            file_content = "\n".join(json.dumps(seq) for seq in sequences)
            arrayrecord_file.write_text(file_content)

            # Create metadata file
            metadata_file = timeframe_dir / f"{symbol}_20250701_000000_20250906_000000_metadata.json"
            metadata = {
                "symbol": symbol,
                "timeframe": timeframe,
                "example_count": sequences_per_timeframe,
                "run_id": run_id,
                "total_sequences": sequences_per_timeframe
            }
            metadata_file.write_text(json.dumps(metadata, indent=2))

            created_files.append(arrayrecord_file)
            sequence_distribution[timeframe] = sequences_per_timeframe
            total_sequences += sequences_per_timeframe

        return {
            "run_id": run_id,
            "symbol": symbol,
            "total_sequences": total_sequences,
            "sequences_per_timeframe": sequences_per_timeframe,
            "timeframes": timeframes,
            "sequence_distribution": sequence_distribution,
            "created_files": created_files
        }

    async def create_database_dataset(
        self, db_connection, dataset_name: str, run_id: int,
        symbols: List[str], total_sequences: int
    ) -> int:
        """Create dataset record in database and return dataset_id."""

        await db_connection.execute("""
            INSERT INTO dev_training_datasets (
                dataset_name, run_id, symbols, total_sequences,
                sequence_length, feature_count, creation_timestamp,
                status, data_quality_score
            ) VALUES ($1, $2, $3, $4, 21, 50, $5, 'completed', 1.0)
        """, dataset_name, run_id, symbols, total_sequences, datetime.now())

        dataset_id = await db_connection.fetchval("""
            SELECT id FROM dev_training_datasets
            WHERE dataset_name = $1
        """, dataset_name)

        return dataset_id

    @pytest.mark.asyncio
    async def test_single_symbol_sequence_count_accuracy(
        self, db_connection, temp_training_dir, clean_test_data
    ):
        """
        CRITICAL TEST: Verify API returns exact sequence count for single symbol.

        This test would have caught the 3,216 → 1 sequence bug.
        """
        print("\n🧪 CRITICAL TEST: Single symbol sequence count accuracy")

        # Step 1: Create training data with known sequence count
        sequences_per_timeframe = 100
        expected_total = sequences_per_timeframe * 5  # 5 timeframes = 500 total

        training_metadata = self.create_training_data_with_known_sequences(
            temp_training_dir, run_id=123, symbol="SEQTEST",
            sequences_per_timeframe=sequences_per_timeframe
        )

        print(f"📁 Created training data: {expected_total} total sequences")
        print(f"📁 Distribution: {training_metadata['sequence_distribution']}")

        # Step 2: Store in database with exact sequence count
        dataset_id = await self.create_database_dataset(
            db_connection, "test_sequence_count_single_symbol",
            run_id=123, symbols=["SEQTEST"], total_sequences=expected_total
        )

        print(f"📊 Created dataset {dataset_id} with {expected_total} sequences in DB")

        # Step 3: Verify database has correct count
        db_sequence_count = await db_connection.fetchval("""
            SELECT total_sequences FROM dev_training_datasets WHERE id = $1
        """, dataset_id)
        assert db_sequence_count == expected_total, (
            f"Database sequence count mismatch: {db_sequence_count} != {expected_total}"
        )

        # Step 4: Test API returns correct sequence count
        with patch('src.services.analytics_service.training_base_paths', [temp_training_dir]):
            response = requests.get(
                f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences",
                timeout=30
            )
            assert response.status_code == 200, f"API failed: {response.status_code}"

            api_data = response.json()
            api_sequence_count = api_data['total_count']

            print(f"🔍 Database says: {db_sequence_count} sequences")
            print(f"🔍 API returns: {api_sequence_count} sequences")

            # CRITICAL ASSERTION: API must match database and training data
            assert api_sequence_count == expected_total, (
                f"SEQUENCE COUNT BUG DETECTED: "
                f"Expected {expected_total} sequences, but API returned {api_sequence_count}. "
                f"This is the exact bug that caused 'No sequence data available'."
            )

            # Verify sequence structure
            sequences = api_data.get('sequences', [])
            assert len(sequences) > 0, "API returned no sequence objects"

        print(f"✅ CRITICAL TEST PASSED: API correctly returned {api_sequence_count} sequences")

    @pytest.mark.asyncio
    async def test_multi_symbol_sequence_count_distribution(
        self, db_connection, temp_training_dir, clean_test_data
    ):
        """Test sequence count accuracy with multiple symbols."""
        print("\n🧪 TEST: Multi-symbol sequence count distribution")

        # Create training data for multiple symbols with different sequence counts
        symbol_configs = [
            ("MULTI_A", 150),  # 150 sequences per timeframe
            ("MULTI_B", 200),  # 200 sequences per timeframe
        ]

        total_expected = 0
        run_id = 124

        for symbol, sequences_per_timeframe in symbol_configs:
            training_metadata = self.create_training_data_with_known_sequences(
                temp_training_dir, run_id=run_id, symbol=symbol,
                sequences_per_timeframe=sequences_per_timeframe
            )
            symbol_total = training_metadata['total_sequences']
            total_expected += symbol_total
            print(f"📁 {symbol}: {symbol_total} sequences ({sequences_per_timeframe} per timeframe)")

        print(f"📊 Total expected across all symbols: {total_expected}")

        # Store in database
        symbols = [config[0] for config in symbol_configs]
        dataset_id = await self.create_database_dataset(
            db_connection, "test_sequence_count_multi_symbol",
            run_id=run_id, symbols=symbols, total_sequences=total_expected
        )

        # Test API accuracy
        with patch('src.services.analytics_service.training_base_paths', [temp_training_dir]):
            response = requests.get(
                f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences"
            )

            api_data = response.json()
            api_count = api_data['total_count']

            assert api_count == total_expected, (
                f"Multi-symbol sequence count error: Expected {total_expected}, got {api_count}"
            )

        print(f"✅ Multi-symbol test passed: {api_count} sequences")

    @pytest.mark.asyncio
    async def test_sequence_count_with_missing_timeframes(
        self, db_connection, temp_training_dir, clean_test_data
    ):
        """Test sequence count when some timeframes are missing."""
        print("\n🧪 TEST: Sequence count with missing timeframes")

        # Create data for only some timeframes (simulate incomplete generation)
        available_timeframes = ["1h", "1d"]  # Missing 5m, 15m, 1w
        sequences_per_timeframe = 80
        expected_total = sequences_per_timeframe * len(available_timeframes)  # 160

        training_metadata = self.create_training_data_with_known_sequences(
            temp_training_dir, run_id=125, symbol="PARTIAL",
            sequences_per_timeframe=sequences_per_timeframe,
            timeframes=available_timeframes
        )

        dataset_id = await self.create_database_dataset(
            db_connection, "test_sequence_count_partial_timeframes",
            run_id=125, symbols=["PARTIAL"], total_sequences=expected_total
        )

        with patch('src.services.analytics_service.training_base_paths', [temp_training_dir]):
            response = requests.get(
                f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences"
            )

            api_data = response.json()
            api_count = api_data['total_count']

            assert api_count == expected_total, (
                f"Partial timeframes count error: Expected {expected_total}, got {api_count}"
            )

        print(f"✅ Partial timeframes test passed: {api_count} sequences")

    @pytest.mark.asyncio
    async def test_sequence_count_edge_cases(
        self, db_connection, temp_training_dir, clean_test_data
    ):
        """Test sequence count edge cases (empty, single, large numbers)."""
        print("\n🧪 TEST: Sequence count edge cases")

        edge_cases = [
            ("EMPTY", 0, "zero sequences"),
            ("SINGLE", 1, "single sequence"),
            ("LARGE", 1000, "large sequence count")
        ]

        run_id = 126

        for symbol, sequences_per_timeframe, description in edge_cases:
            print(f"  Testing {description}...")

            if sequences_per_timeframe > 0:
                training_metadata = self.create_training_data_with_known_sequences(
                    temp_training_dir, run_id=run_id, symbol=symbol,
                    sequences_per_timeframe=sequences_per_timeframe
                )
                expected_total = training_metadata['total_sequences']
            else:
                # Create empty dataset
                expected_total = 0

            dataset_id = await self.create_database_dataset(
                db_connection, f"test_sequence_count_edge_{symbol.lower()}",
                run_id=run_id, symbols=[symbol], total_sequences=expected_total
            )

            with patch('src.services.analytics_service.training_base_paths', [temp_training_dir]):
                response = requests.get(
                    f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences"
                )

                api_data = response.json()
                api_count = api_data['total_count']

                assert api_count == expected_total, (
                    f"Edge case {description} failed: Expected {expected_total}, got {api_count}"
                )

            print(f"    ✅ {description}: {api_count} sequences")
            run_id += 1  # Use different run_id for each test

        print("✅ All edge cases passed")

    @pytest.mark.asyncio
    async def test_sequence_count_regression_protection(
        self, db_connection, temp_training_dir, clean_test_data
    ):
        """
        Regression test: Ensure the specific 3,216 → 1 bug doesn't recur.

        Creates the exact scenario that caused the original bug.
        """
        print("\n🧪 REGRESSION TEST: Protect against 3,216 → 1 sequence bug")

        # Simulate the original bug scenario
        # Run 60: AAPL with 50 sequences per timeframe (250 total)
        run_60_metadata = self.create_training_data_with_known_sequences(
            temp_training_dir, run_id=60, symbol="AAPL", sequences_per_timeframe=50
        )

        # Run 76: AAPL + TSLA with 643.2 sequences per timeframe (≈ 3,216 total)
        sequences_per_timeframe_76 = 643  # Close to original
        run_76_metadata = self.create_training_data_with_known_sequences(
            temp_training_dir, run_id=76, symbol="AAPL", sequences_per_timeframe=sequences_per_timeframe_76
        )
        # Also create TSLA
        tsla_76_metadata = self.create_training_data_with_known_sequences(
            temp_training_dir, run_id=76, symbol="TSLA", sequences_per_timeframe=sequences_per_timeframe_76
        )

        expected_run_76_total = run_76_metadata['total_sequences'] + tsla_76_metadata['total_sequences']

        print(f"📁 Run 60: {run_60_metadata['total_sequences']} AAPL sequences")
        print(f"📁 Run 76: {expected_run_76_total} AAPL+TSLA sequences")

        # Create datasets pointing to specific runs (this is where the bug occurred)
        dataset_60_id = await self.create_database_dataset(
            db_connection, "test_sequence_count_regression_run_60",
            run_id=60, symbols=["AAPL"], total_sequences=run_60_metadata['total_sequences']
        )

        dataset_76_id = await self.create_database_dataset(
            db_connection, "test_sequence_count_regression_run_76",
            run_id=76, symbols=["AAPL", "TSLA"], total_sequences=expected_run_76_total
        )

        with patch('src.services.analytics_service.training_base_paths', [temp_training_dir]):

            # Test dataset 60 gets run 60 count
            response_60 = requests.get(
                f"http://localhost:3000/api/v1/training-datasets/{dataset_60_id}/sequences"
            )
            data_60 = response_60.json()
            count_60 = data_60['total_count']

            # Test dataset 76 gets run 76 count (THIS WAS THE BUG)
            response_76 = requests.get(
                f"http://localhost:3000/api/v1/training-datasets/{dataset_76_id}/sequences"
            )
            data_76 = response_76.json()
            count_76 = data_76['total_count']

            print(f"🔍 Dataset 60 API result: {count_60} sequences")
            print(f"🔍 Dataset 76 API result: {count_76} sequences")

            # CRITICAL REGRESSION ASSERTIONS
            assert count_60 == run_60_metadata['total_sequences'], (
                f"Dataset 60 regression: Expected {run_60_metadata['total_sequences']}, got {count_60}"
            )

            assert count_76 == expected_run_76_total, (
                f"Dataset 76 regression: Expected {expected_run_76_total}, got {count_76}. "
                f"This is the original 3,216 → 1 bug recurring!"
            )

            # Ensure no cross-contamination
            assert count_60 != count_76, "Datasets returning identical counts (cross-contamination)"

        print(f"✅ REGRESSION TEST PASSED: No cross-contamination between runs")
        print(f"   Dataset 60: {count_60} sequences ✓")
        print(f"   Dataset 76: {count_76} sequences ✓")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])