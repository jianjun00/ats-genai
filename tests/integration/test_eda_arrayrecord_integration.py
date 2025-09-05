#!/usr/bin/env python3
"""
EDA ArrayRecord Integration Tests

Tests the complete end-to-end workflow from training data generation
to EDA visualization, ensuring all ArrayRecord fixes work together.

Based on fixes documented in PRD: ArrayRecord Training Data System (September 4, 2025)
Tests integration of:
- ArrayRecord API compatibility fixes
- JSON datetime serialization fixes  
- Database schema consistency fixes
- API endpoint pattern fixes
- TSLA data path resolution fixes
"""

import pytest
import requests
import subprocess
import tempfile
import time
import sys
from pathlib import Path
from datetime import datetime

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))


@pytest.mark.integration
@pytest.mark.slow
def test_complete_eda_arrayrecord_workflow():
    """Test complete workflow from training data generation to EDA visualization."""
    
    # Verify analytics service is running
    base_url = "http://localhost:3000"
    try:
        health_response = requests.get(f"{base_url}/health", timeout=5)
        if health_response.status_code != 200:
            pytest.skip("Analytics service not running")
    except requests.ConnectionError:
        pytest.skip("Analytics service not accessible")
    
    # Step 1: Verify training datasets are visible in API
    datasets_response = requests.get(f"{base_url}/api/v1/training-datasets", timeout=10)
    assert datasets_response.status_code == 200, "Training datasets API should be accessible"
    
    datasets_data = datasets_response.json()
    assert "datasets" in datasets_data, "Response should contain datasets"
    
    if not datasets_data["datasets"]:
        pytest.skip("No training datasets available - run training data generation first")
    
    # Find most recent dataset (likely to have ArrayRecord format)
    datasets = sorted(datasets_data["datasets"], key=lambda x: x.get("created_at", ""), reverse=True)
    latest_dataset = datasets[0]
    dataset_id = latest_dataset["id"]
    
    print(f"Testing with dataset: {latest_dataset.get('dataset_name', 'Unknown')} (ID: {dataset_id})")
    
    # Step 2: Verify sequences endpoint returns ArrayRecord files  
    sequences_response = requests.get(f"{base_url}/api/v1/training-datasets/{dataset_id}/sequences", timeout=10)
    assert sequences_response.status_code == 200, f"Sequences endpoint should work for dataset {dataset_id}"
    
    sequences_data = sequences_response.json()
    assert "sequences" in sequences_data, "Sequences response should contain sequences"
    assert "total_count" in sequences_data, "Sequences response should contain total_count"
    assert "datasets" in sequences_data, "Sequences response should contain datasets info"
    
    if sequences_data["total_count"] == 0:
        pytest.skip("No sequences found in dataset - may not have ArrayRecord files")
    
    # Verify ArrayRecord files are found
    sequences = sequences_data["sequences"]
    arrayrecord_sequences = [seq for seq in sequences if seq.get("filename", "").endswith(".arrayrecord")]
    
    if not arrayrecord_sequences:
        pytest.skip("No ArrayRecord files found - dataset may use different format")
    
    print(f"Found {len(arrayrecord_sequences)} ArrayRecord sequences")
    
    # Step 3: Verify ArrayRecord file structure
    for sequence in arrayrecord_sequences[:2]:  # Test first 2 sequences
        assert "filename" in sequence, "Sequence should have filename"
        assert "path" in sequence, "Sequence should have path"
        assert "symbol" in sequence, "Sequence should have symbol"
        
        # Verify path format
        path = sequence["path"]
        assert "/data/training_data/" in path, f"Incorrect path format: {path}"
        assert path.endswith(".arrayrecord"), f"Path should end with .arrayrecord: {path}"
        
        print(f"Verified sequence: {sequence['symbol']} - {sequence['filename']}")
    
    # Step 4: Verify EDA page loads without errors
    try:
        eda_response = requests.get(f"{base_url}/eda", timeout=10)
        assert eda_response.status_code == 200, "EDA page should be accessible"
        
        eda_content = eda_response.text
        assert "Select Sequence" in eda_content, "EDA page should contain sequence selector"
        assert "training-datasets" in eda_content, "EDA page should reference training datasets API"
        
        print("✅ EDA page loads successfully")
        
    except requests.RequestException as e:
        pytest.skip(f"Cannot verify EDA page: {e}")
    
    # Step 5: Test dataset metadata consistency
    dataset_info = sequences_data["datasets"][0]
    assert dataset_info.get("dataset_name"), "Dataset should have name"
    assert dataset_info.get("symbols"), "Dataset should have symbols list"
    
    # Verify symbols match sequences
    dataset_symbols = set(dataset_info.get("symbols", []))
    sequence_symbols = set(seq.get("symbol", "") for seq in sequences)
    
    if dataset_symbols and sequence_symbols:
        # Should have some overlap
        assert dataset_symbols.intersection(sequence_symbols), "Dataset symbols should match sequence symbols"
    
    print("✅ Complete EDA ArrayRecord workflow verified")


@pytest.mark.integration
def test_arrayrecord_file_accessibility():
    """Test that ArrayRecord files are accessible and readable."""
    
    # Find ArrayRecord files in training data directory
    training_data_path = Path("/mnt/d/ats-data/training_data")
    if not training_data_path.exists():
        pytest.skip("Training data directory not found")
    
    arrayrecord_files = list(training_data_path.rglob("*.arrayrecord"))
    if not arrayrecord_files:
        pytest.skip("No ArrayRecord files found in training data directory")
    
    print(f"Found {len(arrayrecord_files)} ArrayRecord files")
    
    # Test first few files
    for file_path in arrayrecord_files[:3]:
        # Verify file exists and has content
        assert file_path.exists(), f"ArrayRecord file should exist: {file_path}"
        assert file_path.stat().st_size > 0, f"ArrayRecord file should not be empty: {file_path}"
        
        # Try to read with ArrayRecord (if available)
        try:
            pytest.importorskip("array_record")
            from array_record.python.array_record_module import ArrayRecordReader
            
            with ArrayRecordReader(str(file_path)) as reader:
                records = list(reader)
                assert len(records) > 0, f"ArrayRecord file should contain records: {file_path}"
                
                # Verify record format
                first_record = records[0]
                assert isinstance(first_record, bytes), "ArrayRecord records should be bytes"
                
                # Try to parse as JSON
                import json
                record_data = json.loads(first_record.decode())
                assert isinstance(record_data, dict), "Record should be JSON dictionary"
                
                print(f"✅ Verified ArrayRecord file: {file_path.name} ({len(records)} records)")
                
        except ImportError:
            print(f"⚠️  ArrayRecord package not available, skipping file content verification")
            continue


@pytest.mark.integration
def test_database_to_eda_consistency():
    """Test consistency between database records and EDA display."""
    
    base_url = "http://localhost:3000"
    
    try:
        # Get database connection to verify table consistency
        from core.database.connection_manager import get_raw_connection
        
        with get_raw_connection("dev") as conn:
            with conn.cursor() as cursor:
                # Get training datasets from database directly
                cursor.execute("""
                    SELECT id, dataset_name, total_sequences, symbols, created_at
                    FROM dev_training_datasets 
                    ORDER BY created_at DESC
                    LIMIT 5
                """)
                
                db_datasets = cursor.fetchall()
                if not db_datasets:
                    pytest.skip("No training datasets in database")
                
        # Get same data via API
        api_response = requests.get(f"{base_url}/api/v1/training-datasets", timeout=10)
        assert api_response.status_code == 200, "API should work when database has data"
        
        api_data = api_response.json()
        api_datasets = api_data["datasets"]
        
        # Find matching datasets
        db_dataset = db_datasets[0]  # Most recent from DB
        db_id, db_name, db_sequences, db_symbols, db_created = db_dataset
        
        # Find matching dataset in API response
        api_dataset = next((ds for ds in api_datasets if ds["id"] == db_id), None)
        assert api_dataset is not None, f"Database dataset {db_id} not found in API"
        
        # Verify consistency
        assert api_dataset["dataset_name"] == db_name, "Dataset name should match between DB and API"
        assert api_dataset["total_sequences"] == db_sequences, "Sequence count should match between DB and API"
        
        print(f"✅ Database-API consistency verified for dataset {db_name}")
        
    except ImportError:
        pytest.skip("Database connection manager not available")
    except Exception as e:
        if "could not connect" in str(e).lower():
            pytest.skip("Database not available")
        raise


@pytest.mark.integration
def test_json_datetime_in_api_responses():
    """Test that API responses handle datetime serialization correctly."""
    
    base_url = "http://localhost:3000"
    
    datasets_response = requests.get(f"{base_url}/api/v1/training-datasets", timeout=10)
    if datasets_response.status_code != 200:
        pytest.skip("Training datasets API not available")
    
    data = datasets_response.json()
    datasets = data.get("datasets", [])
    
    if not datasets:
        pytest.skip("No datasets available for datetime testing")
    
    # Check that datetime fields are properly serialized
    for dataset in datasets[:3]:  # Check first 3 datasets
        created_at = dataset.get("created_at")
        if created_at:
            # Should be ISO format string, not datetime object
            assert isinstance(created_at, str), f"created_at should be string, got {type(created_at)}"
            
            # Should be parseable as datetime
            try:
                parsed_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                assert isinstance(parsed_dt, datetime), "Should parse as datetime"
                print(f"✅ Datetime serialization verified: {created_at}")
            except ValueError as e:
                pytest.fail(f"Invalid datetime format in API response: {created_at} - {e}")


@pytest.mark.integration
def test_error_handling_graceful_degradation():
    """Test that system handles errors gracefully without crashing."""
    
    base_url = "http://localhost:3000"
    
    # Test 1: Non-existent dataset ID
    response = requests.get(f"{base_url}/api/v1/training-datasets/99999/sequences", timeout=5)
    assert response.status_code in [200, 404], "Should handle non-existent dataset gracefully"
    
    if response.status_code == 200:
        data = response.json()
        assert "sequences" in data, "Should return structured response even for non-existent dataset"
        assert data.get("total_count", 0) == 0, "Should return zero sequences for non-existent dataset"
    
    # Test 2: Invalid dataset ID format
    response = requests.get(f"{base_url}/api/v1/training-datasets/invalid/sequences", timeout=5)
    # Should not crash server (no 500 error)
    assert response.status_code != 500, "Server should not crash on invalid dataset ID"
    
    # Test 3: EDA page with no datasets
    try:
        eda_response = requests.get(f"{base_url}/eda", timeout=10)
        assert eda_response.status_code == 200, "EDA page should load even without datasets"
        
        content = eda_response.text
        # Should have basic structure even without data
        assert "Select Sequence" in content, "EDA page should have sequence selector even without data"
        
    except requests.RequestException:
        pytest.skip("Cannot test EDA error handling")
    
    print("✅ Error handling works gracefully")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])