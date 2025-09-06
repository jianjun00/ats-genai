#!/usr/bin/env python3
"""
Multi-Run Training Dataset Path Resolution Test Suite

CRITICAL: Tests designed to catch the path resolution bug where analytics service
returns wrong run's data due to incorrect file discovery algorithm.

This test suite creates multiple training runs and verifies that each dataset
gets data from its correct run_id, not cross-contaminated data.
"""

import pytest
import asyncio
import asyncpg
import tempfile
import shutil
import json
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch
import requests

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.config.environment import Environment

class TestMultiRunTrainingDatasetPathResolution:
    """Test suite to catch multi-run path resolution bugs."""
    
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
        """Create temporary training data directory structure."""
        temp_dir = tempfile.mkdtemp()
        training_data_dir = Path(temp_dir) / "training_data"
        training_data_dir.mkdir()
        yield training_data_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    async def clean_test_data(self, db_connection):
        """Clean test data before and after."""
        # Clean datasets
        await db_connection.execute("""
            DELETE FROM dev_training_datasets 
            WHERE dataset_name LIKE 'test_multi_run_%'
        """)
        yield
        await db_connection.execute("""
            DELETE FROM dev_training_datasets 
            WHERE dataset_name LIKE 'test_multi_run_%'
        """)

    def create_mock_arrayrecord_files(self, training_dir: Path, run_id: int, 
                                    symbols: list, sequences_per_symbol: int = 100):
        """Create mock ArrayRecord files for a specific run."""
        run_dir = training_dir / str(run_id)
        timeframes = ["5m", "15m", "1h", "1d", "1w"]
        
        created_files = []
        for timeframe in timeframes:
            timeframe_dir = run_dir / timeframe
            timeframe_dir.mkdir(parents=True, exist_ok=True)
            
            for symbol in symbols:
                # Create mock ArrayRecord file
                file_name = f"{symbol}_20250701_000000_20250906_000000.arrayrecord"
                file_path = timeframe_dir / file_name
                
                # Write mock data - different for each run to detect cross-contamination
                mock_data = json.dumps({
                    "run_id": run_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "sequences": sequences_per_symbol,
                    "test_marker": f"run_{run_id}_{symbol}_{timeframe}"
                }).encode()
                
                file_path.write_bytes(mock_data * 10)  # Make it reasonably sized
                created_files.append(file_path)
                
                # Create metadata file
                metadata_path = timeframe_dir / f"{file_name.replace('.arrayrecord', '_metadata.json')}"
                metadata = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "example_count": sequences_per_symbol,
                    "run_id": run_id
                }
                metadata_path.write_text(json.dumps(metadata, indent=2))
                
        return created_files

    async def create_test_dataset(self, db_connection, dataset_name: str, 
                                run_id: int, symbols: list, total_sequences: int):
        """Create test dataset in database."""
        await db_connection.execute("""
            INSERT INTO dev_training_datasets (
                dataset_name, run_id, symbols, total_sequences,
                sequence_length, feature_count, creation_timestamp,
                status
            ) VALUES ($1, $2, $3, $4, 60, 100, $5, 'completed')
        """, dataset_name, run_id, symbols, total_sequences, datetime.now())

    @pytest.mark.asyncio 
    async def test_multi_run_path_resolution_isolation(
        self, db_connection, temp_training_dir, clean_test_data
    ):
        """
        CRITICAL TEST: Verify each dataset gets data from its correct run_id.
        
        This is the exact test that would have caught the path resolution bug.
        """
        print("\n🧪 CRITICAL TEST: Multi-run path resolution isolation")
        
        # Step 1: Create multiple training runs with overlapping symbols
        run_60_files = self.create_mock_arrayrecord_files(
            temp_training_dir, run_id=60, symbols=["AAPL"], sequences_per_symbol=50
        )
        run_76_files = self.create_mock_arrayrecord_files(  
            temp_training_dir, run_id=76, symbols=["AAPL", "TSLA"], sequences_per_symbol=200
        )
        
        print(f"📁 Created {len(run_60_files)} files for run 60")
        print(f"📁 Created {len(run_76_files)} files for run 76")
        
        # Step 2: Create datasets in database pointing to different runs
        await self.create_test_dataset(
            db_connection, "test_multi_run_dataset_60", run_id=60, 
            symbols=["AAPL"], total_sequences=250  # 50 sequences * 5 timeframes
        )
        dataset_60_id = await db_connection.fetchval("""
            SELECT id FROM dev_training_datasets 
            WHERE dataset_name = 'test_multi_run_dataset_60'
        """)
        
        await self.create_test_dataset(
            db_connection, "test_multi_run_dataset_76", run_id=76,
            symbols=["AAPL", "TSLA"], total_sequences=2000  # 200 sequences * 5 timeframes * 2 symbols
        )
        dataset_76_id = await db_connection.fetchval("""
            SELECT id FROM dev_training_datasets
            WHERE dataset_name = 'test_multi_run_dataset_76' 
        """)
        
        print(f"📊 Created dataset {dataset_60_id} → run 60")
        print(f"📊 Created dataset {dataset_76_id} → run 76")
        
        # Step 3: Mock the analytics service to use our temp directory
        with patch('src.services.analytics_service.training_base_paths', [temp_training_dir]):
            
            # Step 4: Test dataset 60 gets run 60 data (CRITICAL TEST)
            response_60 = requests.get(
                f"http://localhost:3000/api/v1/training-datasets/{dataset_60_id}/sequences"
            )
            assert response_60.status_code == 200
            data_60 = response_60.json()
            
            print(f"🔍 Dataset 60 sequences: {data_60['total_count']}")
            print(f"🔍 Expected from run 60: 250 sequences")
            
            # CRITICAL ASSERTION: Should get run 60's sequence count, not run 76's
            assert data_60['total_count'] == 250, (
                f"Dataset 60 should return 250 sequences from run 60, "
                f"got {data_60['total_count']} (probably from wrong run)"
            )
            
            # Step 5: Test dataset 76 gets run 76 data (CRITICAL TEST) 
            response_76 = requests.get(
                f"http://localhost:3000/api/v1/training-datasets/{dataset_76_id}/sequences"
            )
            assert response_76.status_code == 200
            data_76 = response_76.json()
            
            print(f"🔍 Dataset 76 sequences: {data_76['total_count']}")
            print(f"🔍 Expected from run 76: 2000 sequences")
            
            # CRITICAL ASSERTION: Should get run 76's sequence count, not run 60's
            assert data_76['total_count'] == 2000, (
                f"Dataset 76 should return 2000 sequences from run 76, "
                f"got {data_76['total_count']} (probably from wrong run)"
            )
            
            # Step 6: Verify no cross-contamination by checking actual sequence data
            seq_response_60 = requests.get(
                f"http://localhost:3000/api/v1/training-datasets/{dataset_60_id}/sequences/0/data"
            )
            if seq_response_60.status_code == 200:
                seq_data_60 = seq_response_60.json()
                # Should contain run 60 marker, not run 76
                assert "run_60" in str(seq_data_60), "Dataset 60 contains run 76 data (cross-contamination)"
                assert "run_76" not in str(seq_data_60), "Dataset 60 contaminated with run 76 data"
            
        print("✅ CRITICAL TEST PASSED: Each dataset gets data from correct run_id")

    @pytest.mark.asyncio
    async def test_sequence_count_accuracy_validation(
        self, db_connection, temp_training_dir, clean_test_data
    ):
        """Test that API-reported sequence counts match actual training data."""
        print("\n🧪 TEST: Sequence count accuracy validation")
        
        # Create training data with known sequence counts
        expected_sequences = 150
        run_id = 88
        
        self.create_mock_arrayrecord_files(
            temp_training_dir, run_id=run_id, symbols=["TEST"], 
            sequences_per_symbol=expected_sequences
        )
        
        await self.create_test_dataset(
            db_connection, "test_sequence_count_validation", run_id=run_id,
            symbols=["TEST"], total_sequences=expected_sequences * 5  # 5 timeframes
        )
        
        dataset_id = await db_connection.fetchval("""
            SELECT id FROM dev_training_datasets 
            WHERE dataset_name = 'test_sequence_count_validation'
        """)
        
        with patch('src.services.analytics_service.training_base_paths', [temp_training_dir]):
            response = requests.get(
                f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences"
            )
            
            data = response.json()
            actual_sequences = data['total_count']
            expected_from_db = expected_sequences * 5
            
            assert actual_sequences == expected_from_db, (
                f"Sequence count mismatch: Expected {expected_from_db}, got {actual_sequences}"
            )
            
        print(f"✅ Sequence count validation passed: {actual_sequences} sequences")

    @pytest.mark.asyncio 
    async def test_database_filesystem_linkage_validation(
        self, db_connection, temp_training_dir, clean_test_data
    ):
        """Test that database run_id correctly maps to filesystem structure."""
        print("\n🧪 TEST: Database-filesystem linkage validation")
        
        # Create specific run structure
        run_id = 99
        symbols = ["LINKTEST"]
        
        created_files = self.create_mock_arrayrecord_files(
            temp_training_dir, run_id=run_id, symbols=symbols, sequences_per_symbol=75
        )
        
        await self.create_test_dataset(
            db_connection, "test_db_filesystem_linkage", run_id=run_id,
            symbols=symbols, total_sequences=375  # 75 * 5 timeframes
        )
        
        dataset_id = await db_connection.fetchval("""
            SELECT id FROM dev_training_datasets
            WHERE dataset_name = 'test_db_filesystem_linkage'
        """)
        
        # Verify database has correct run_id
        db_run_id = await db_connection.fetchval("""
            SELECT run_id FROM dev_training_datasets WHERE id = $1
        """, dataset_id)
        assert db_run_id == run_id, f"Database run_id mismatch: {db_run_id} != {run_id}"
        
        # Verify files exist in correct run directory  
        run_dir = temp_training_dir / str(run_id)
        assert run_dir.exists(), f"Run directory {run_dir} doesn't exist"
        
        arrayrecord_files = list(run_dir.rglob("*.arrayrecord"))
        assert len(arrayrecord_files) > 0, "No ArrayRecord files found in run directory"
        
        # Verify analytics service uses correct run directory
        with patch('src.services.analytics_service.training_base_paths', [temp_training_dir]):
            response = requests.get(
                f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences"
            )
            
            data = response.json()
            assert data['total_count'] == 375, "Analytics service not using correct run files"
            
        print(f"✅ Database-filesystem linkage validated for run {run_id}")

    @pytest.mark.asyncio
    async def test_wrong_run_detection(
        self, db_connection, temp_training_dir, clean_test_data  
    ):
        """Test that detects when analytics service uses wrong run's data."""
        print("\n🧪 TEST: Wrong run detection (regression test)")
        
        # Create "decoy" run with different sequence count
        decoy_run = 100
        target_run = 101
        
        # Decoy run: 30 sequences per symbol
        self.create_mock_arrayrecord_files(
            temp_training_dir, run_id=decoy_run, symbols=["DECOY"], 
            sequences_per_symbol=30
        )
        
        # Target run: 80 sequences per symbol
        self.create_mock_arrayrecord_files(
            temp_training_dir, run_id=target_run, symbols=["TARGET"],
            sequences_per_symbol=80
        )
        
        # Create dataset pointing to target run
        await self.create_test_dataset(
            db_connection, "test_wrong_run_detection", run_id=target_run,
            symbols=["TARGET"], total_sequences=400  # 80 * 5 timeframes
        )
        
        dataset_id = await db_connection.fetchval("""
            SELECT id FROM dev_training_datasets
            WHERE dataset_name = 'test_wrong_run_detection'
        """)
        
        with patch('src.services.analytics_service.training_base_paths', [temp_training_dir]):
            response = requests.get(
                f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences"
            )
            
            data = response.json()
            actual_count = data['total_count']
            
            # Should get target run's count (400), not decoy run's count (150)
            assert actual_count == 400, (
                f"Analytics service using wrong run's data: "
                f"Expected 400 sequences from target run {target_run}, "
                f"got {actual_count} (possibly from decoy run {decoy_run})"
            )
            
        print(f"✅ Wrong run detection passed: Got {actual_count} from correct run")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])