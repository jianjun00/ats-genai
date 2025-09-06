#!/usr/bin/env python3
"""
ArrayRecord Reading Compatibility Test Suite

CRITICAL: Tests ArrayRecord file reading in analytics service to catch
binary data parsing issues like UTF-8 decode errors.

The current bug: ArrayRecord files contain binary data but analytics service
tries to decode them as UTF-8 text, causing UnicodeDecodeError.
"""

import pytest
import tempfile
import shutil
import json
from pathlib import Path
import requests
import asyncpg
from datetime import datetime
from unittest.mock import patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestArrayRecordReadingCompatibility:
    """Test ArrayRecord file reading compatibility in analytics service."""
    
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
            WHERE dataset_name LIKE 'test_arrayrecord_%'
        """)
        yield
        await db_connection.execute("""
            DELETE FROM dev_training_datasets
            WHERE dataset_name LIKE 'test_arrayrecord_%'
        """)

    def create_real_arrayrecord_file(self, file_path: Path, symbol: str, sequences: int = 10):
        """Create a real ArrayRecord file using the ArrayRecordWriter."""
        try:
            from array_record.python.array_record_module import ArrayRecordWriter
            
            # Create sample training data
            sample_data = []
            for i in range(sequences):
                record = {
                    "symbol": symbol,
                    "sequence_id": i,
                    "datetime": f"2025-07-{i+1:02d}T10:00:00",
                    "open": 100.0 + i,
                    "high": 105.0 + i, 
                    "low": 95.0 + i,
                    "close": 102.0 + i,
                    "volume": 1000000 + i * 1000,
                    "envelope_top": 110.0 + i,
                    "envelope_bot": 90.0 + i,
                    "pldot": 0.5 + (i % 2) * 0.1
                }
                sample_data.append(record)
            
            # Write using ArrayRecordWriter (creates binary format)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with ArrayRecordWriter(str(file_path), 'group_size:1') as writer:
                for record in sample_data:
                    # Convert to JSON bytes for writing
                    record_bytes = json.dumps(record).encode('utf-8')
                    writer.write(record_bytes)
            
            return len(sample_data)
            
        except ImportError:
            # Fallback: Create a mock binary file that will trigger the UTF-8 decode error
            print("⚠️  ArrayRecord not available, creating mock binary file")
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write binary data that will cause UTF-8 decode error
            binary_data = b'\xd1\xd2\xd3\xd4\xd5'  # Invalid UTF-8 bytes
            file_path.write_bytes(binary_data)
            
            return sequences

    async def create_test_dataset(self, db_connection, dataset_name: str, 
                                run_id: int, symbols: list, total_sequences: int):
        """Create test dataset in database."""
        await db_connection.execute("""
            INSERT INTO dev_training_datasets (
                dataset_name, run_id, symbols, total_sequences,
                sequence_length, feature_count, creation_timestamp,
                status
            ) VALUES ($1, $2, $3, $4, 21, 50, $5, 'completed')
        """, dataset_name, run_id, symbols, total_sequences, datetime.now())
        
        return await db_connection.fetchval("""
            SELECT id FROM dev_training_datasets 
            WHERE dataset_name = $1
        """, dataset_name)

    @pytest.mark.asyncio
    async def test_arrayrecord_binary_reading_compatibility(
        self, db_connection, temp_training_dir, clean_test_data
    ):
        """
        CRITICAL TEST: Verify analytics service can read real ArrayRecord files.
        
        This test would have caught the UTF-8 decode error.
        """
        print("\n🧪 CRITICAL TEST: ArrayRecord binary reading compatibility")
        
        # Step 1: Create real ArrayRecord file (binary format)
        run_id = 200
        symbol = "BINTEST"
        sequences_per_file = 25
        
        arrayrecord_file = (temp_training_dir / str(run_id) / "1h" / 
                          f"{symbol}_20250701_000000_20250906_000000.arrayrecord")
        
        actual_sequences = self.create_real_arrayrecord_file(
            arrayrecord_file, symbol, sequences_per_file
        )
        
        print(f"📁 Created ArrayRecord file: {arrayrecord_file}")
        print(f"📁 File size: {arrayrecord_file.stat().st_size} bytes")
        print(f"📁 Expected sequences: {actual_sequences}")
        
        # Verify file is actually binary (not text)
        file_content = arrayrecord_file.read_bytes()
        try:
            file_content.decode('utf-8')
            print("⚠️  File appears to be text, not binary ArrayRecord")
        except UnicodeDecodeError:
            print("✅ File is binary (will trigger UTF-8 decode error if handled incorrectly)")
        
        # Step 2: Create dataset in database
        dataset_id = await self.create_test_dataset(
            db_connection, "test_arrayrecord_binary_reading", 
            run_id=run_id, symbols=[symbol], total_sequences=actual_sequences
        )
        
        # Step 3: Test analytics service can read the file
        with patch('src.services.analytics_service.training_base_paths', [temp_training_dir]):
            
            print(f"🔍 Testing sequences endpoint for dataset {dataset_id}...")
            
            try:
                response = requests.get(
                    f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences",
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    returned_sequences = data['total_count']
                    
                    print(f"✅ API Response: {returned_sequences} sequences")
                    
                    # CRITICAL ASSERTION: Should not get 0 due to reading errors
                    assert returned_sequences > 0, (
                        f"ArrayRecord reading failed: Expected {actual_sequences} sequences, "
                        f"got {returned_sequences} (likely UTF-8 decode error)"
                    )
                    
                    assert returned_sequences == actual_sequences, (
                        f"Sequence count mismatch: Expected {actual_sequences}, got {returned_sequences}"
                    )
                    
                else:
                    pytest.fail(f"API request failed: {response.status_code} - {response.text}")
                    
            except requests.exceptions.RequestException as e:
                pytest.fail(f"API request exception: {e}")
        
        print("✅ CRITICAL TEST PASSED: ArrayRecord binary reading works")

    @pytest.mark.asyncio
    async def test_arrayrecord_sequence_data_reading(
        self, db_connection, temp_training_dir, clean_test_data
    ):
        """Test that sequence data endpoint can read ArrayRecord content."""
        print("\n🧪 TEST: ArrayRecord sequence data reading")
        
        run_id = 201
        symbol = "DATATEST"
        sequences = 5
        
        arrayrecord_file = (temp_training_dir / str(run_id) / "1h" / 
                          f"{symbol}_20250701_000000_20250906_000000.arrayrecord")
        
        self.create_real_arrayrecord_file(arrayrecord_file, symbol, sequences)
        
        dataset_id = await self.create_test_dataset(
            db_connection, "test_arrayrecord_data_reading",
            run_id=run_id, symbols=[symbol], total_sequences=sequences
        )
        
        with patch('src.services.analytics_service.training_base_paths', [temp_training_dir]):
            
            # Test sequence data endpoint (this reads actual ArrayRecord content)
            response = requests.get(
                f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences/0/data",
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Should contain actual OHLC data, not error messages
                assert 'error' not in data, f"Sequence data reading failed: {data}"
                assert 'data' in data or 'ohlc_data' in data, "No OHLC data returned"
                
                print(f"✅ Sequence data endpoint returned: {list(data.keys())}")
                
            else:
                # Check if it's the UTF-8 decode error
                if "utf-8" in response.text.lower() or "decode" in response.text.lower():
                    pytest.fail(f"UTF-8 decode error detected in sequence data reading: {response.text}")
                else:
                    pytest.fail(f"Sequence data endpoint failed: {response.status_code} - {response.text}")
        
        print("✅ ArrayRecord sequence data reading works")

    @pytest.mark.asyncio
    async def test_arrayrecord_library_availability(
        self, db_connection, temp_training_dir, clean_test_data
    ):
        """Test that ArrayRecord library is properly available in the system."""
        print("\n🧪 TEST: ArrayRecord library availability")
        
        # Test 1: Check if ArrayRecord can be imported
        try:
            from array_record.python.array_record_module import ArrayRecordWriter, ArrayRecordReader
            print("✅ ArrayRecord classes can be imported")
            
            # Test 2: Check if ArrayRecord writer works
            test_file = temp_training_dir / "test_write.arrayrecord"
            test_data = b'{"test": "data"}'
            
            with ArrayRecordWriter(str(test_file), 'group_size:1') as writer:
                writer.write(test_data)
            
            print("✅ ArrayRecord writer works")
            
            # Test 3: Check if ArrayRecord reader works
            with ArrayRecordReader(str(test_file)) as reader:
                record = reader.read()
                assert record == test_data
                
            print("✅ ArrayRecord reader works")
            
        except ImportError as e:
            pytest.fail(f"ArrayRecord library not available: {e}")
        except Exception as e:
            pytest.fail(f"ArrayRecord library error: {e}")

    @pytest.mark.asyncio 
    async def test_utf8_decode_error_regression(
        self, db_connection, temp_training_dir, clean_test_data
    ):
        """
        Regression test: Ensure UTF-8 decode errors don't occur with ArrayRecord files.
        
        This recreates the exact error: 'utf-8' codec can't decode byte 0xd1
        """
        print("\n🧪 REGRESSION TEST: UTF-8 decode error prevention")
        
        run_id = 202
        symbol = "UTF8TEST"
        
        # Create file with problematic binary data (like real ArrayRecord files)
        arrayrecord_file = (temp_training_dir / str(run_id) / "1h" / 
                          f"{symbol}_20250701_000000_20250906_000000.arrayrecord")
        arrayrecord_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Write binary data that triggers the exact error we saw
        problematic_bytes = b'\xd1\xd2invalid utf8\xd3\xd4'
        arrayrecord_file.write_bytes(problematic_bytes)
        
        dataset_id = await self.create_test_dataset(
            db_connection, "test_utf8_decode_error_regression",
            run_id=run_id, symbols=[symbol], total_sequences=10
        )
        
        with patch('src.services.analytics_service.training_base_paths', [temp_training_dir]):
            
            response = requests.get(
                f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences",
                timeout=30
            )
            
            # Should NOT get UTF-8 decode error
            if response.status_code != 200:
                error_text = response.text.lower()
                if "utf-8" in error_text and ("decode" in error_text or "codec" in error_text):
                    pytest.fail(
                        f"UTF-8 decode error regression detected: {response.text}. "
                        f"Analytics service is trying to decode binary ArrayRecord as UTF-8 text."
                    )
            
            # Even if file is unreadable, should handle gracefully, not crash with UTF-8 error
            print("✅ No UTF-8 decode error on binary file")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])