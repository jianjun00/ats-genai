#!/usr/bin/env python3
"""
Comprehensive end-to-end testing for training data visualization.
Tests each component systematically to identify issues quickly.
"""
import os
import sys
import json
import requests
import subprocess
from pathlib import Path
import time

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

class TrainingDataE2ETester:
    def __init__(self):
        self.base_url = "http://localhost:3000"
        self.test_results = {}
        self.failures = []
    
    def log_test(self, test_name, result, details=""):
        """Log test result with details."""
        self.test_results[test_name] = {
            "result": result,
            "details": details,
            "timestamp": time.time()
        }
        
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if details:
            print(f"    {details}")
        
        if not result:
            self.failures.append(f"{test_name}: {details}")
    
    def test_1_database_connection(self):
        """Test database connectivity and basic queries."""
        try:
            result = subprocess.run([
                "python3", "scripts/run_dev.py", "query", "--query", "SELECT version();"
            ], capture_output=True, text=True, timeout=10)
            
            success = result.returncode == 0
            details = f"Return code: {result.returncode}, Output: {result.stdout[:100]}"
            self.log_test("Database Connection", success, details)
            return success
        except Exception as e:
            self.log_test("Database Connection", False, str(e))
            return False
    
    def test_2_training_datasets_table(self):
        """Test training datasets table structure and access."""
        try:
            result = subprocess.run([
                "python3", "scripts/run_dev.py", "query", "--query", 
                "SELECT COUNT(*) FROM dev_training_datasets;"
            ], capture_output=True, text=True, timeout=10)
            
            success = result.returncode == 0
            if success:
                count = result.stdout.strip().split('\n')[-1].strip()
                details = f"Found {count} datasets in table"
            else:
                details = f"Error: {result.stderr}"
            
            self.log_test("Training Datasets Table", success, details)
            return success
        except Exception as e:
            self.log_test("Training Datasets Table", False, str(e))
            return False
    
    def test_3_file_system_access(self):
        """Test file system access to training data directory."""
        try:
            # Check host directory
            host_path = Path("/mnt/d/ats-data/training")
            host_exists = host_path.exists()
            
            # Check container directory
            result = subprocess.run([
                "docker", "exec", "ats-dev-analytics", "ls", "-la", "/data/training"
            ], capture_output=True, text=True, timeout=10)
            
            container_access = result.returncode == 0
            
            details = f"Host path exists: {host_exists}, Container access: {container_access}"
            if container_access:
                details += f", Container contents: {result.stdout.count('drwx')} dirs"
            
            success = host_exists and container_access
            self.log_test("File System Access", success, details)
            return success
        except Exception as e:
            self.log_test("File System Access", False, str(e))
            return False
    
    def test_4_training_data_files(self):
        """Test that training data files exist and are readable."""
        try:
            # List all training files
            result = subprocess.run([
                "docker", "exec", "ats-dev-analytics", "find", "/data/training", 
                "-name", "*.arrayrecord", "-o", "-name", "*.riegeli"
            ], capture_output=True, text=True, timeout=10)
            
            files_found = result.stdout.strip().split('\n') if result.stdout.strip() else []
            files_found = [f for f in files_found if f]  # Remove empty strings
            
            details = f"Found {len(files_found)} training files: {files_found[:3]}..."
            success = len(files_found) > 0
            
            self.log_test("Training Data Files Exist", success, details)
            
            # Test file readability
            if files_found:
                test_file = files_found[0]
                stat_result = subprocess.run([
                    "docker", "exec", "ats-dev-analytics", "stat", test_file
                ], capture_output=True, text=True, timeout=5)
                
                readable = stat_result.returncode == 0
                details_read = f"First file {test_file} readable: {readable}"
                self.log_test("Training Files Readable", readable, details_read)
                return success and readable
            
            return success
        except Exception as e:
            self.log_test("Training Data Files", False, str(e))
            return False
    
    def test_5_arrayrecord_library(self):
        """Test ArrayRecord library availability in container."""
        try:
            result = subprocess.run([
                "docker", "exec", "ats-dev-analytics", "python3", "-c",
                "from array_record.python.array_record_module import ArrayRecordReader; print('ArrayRecord available')"
            ], capture_output=True, text=True, timeout=10)
            
            success = result.returncode == 0
            details = f"Import result: {result.stdout.strip() if success else result.stderr.strip()}"
            
            self.log_test("ArrayRecord Library", success, details)
            return success
        except Exception as e:
            self.log_test("ArrayRecord Library", False, str(e))
            return False
    
    def test_6_training_datasets_api(self):
        """Test training datasets listing API."""
        try:
            response = requests.get(f"{self.base_url}/api/v1/training-datasets", timeout=10)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                count = len(data.get("datasets", []))
                details = f"API returned {count} datasets"
                
                # Check structure
                if count > 0:
                    sample = data["datasets"][0]
                    has_required_fields = all(field in sample for field in ["id", "dataset_name", "symbols"])
                    details += f", has required fields: {has_required_fields}"
                    success = success and has_required_fields
            else:
                details = f"HTTP {response.status_code}: {response.text[:100]}"
            
            self.log_test("Training Datasets API", success, details)
            return success
        except Exception as e:
            self.log_test("Training Datasets API", False, str(e))
            return False
    
    def test_7_specific_dataset_api(self, dataset_id=40):
        """Test specific dataset visualization API."""
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/training-datasets/{dataset_id}/visualization-data", 
                timeout=10
            )
            success = response.status_code == 200
            
            if success:
                data = response.json()
                has_error = "error" in data
                has_data = len(data.get("data", [])) > 0
                
                details = f"Dataset {dataset_id}: error={has_error}, data_count={len(data.get('data', []))}"
                if has_error:
                    details += f", error: {data['error'][:50]}..."
                
                # Success means no error or has actual data
                success = not has_error or has_data
            else:
                details = f"HTTP {response.status_code}: {response.text[:100]}"
            
            self.log_test(f"Dataset {dataset_id} API", success, details)
            return success, data if success else None
        except Exception as e:
            self.log_test(f"Dataset {dataset_id} API", False, str(e))
            return False, None
    
    def test_8_file_discovery_logic(self):
        """Test the file discovery logic specifically."""
        try:
            # Get a dataset to test with
            response = requests.get(f"{self.base_url}/api/v1/training-datasets", timeout=10)
            if response.status_code != 200:
                self.log_test("File Discovery Logic", False, "Cannot get datasets list")
                return False
            
            datasets = response.json().get("datasets", [])
            if not datasets:
                self.log_test("File Discovery Logic", False, "No datasets to test with")
                return False
            
            test_dataset = datasets[0]
            dataset_id = test_dataset["id"]
            symbols = test_dataset.get("symbols", [])
            
            if not symbols:
                self.log_test("File Discovery Logic", False, f"Dataset {dataset_id} has no symbols")
                return False
            
            target_symbol = symbols[0]
            
            # Manually test file discovery
            result = subprocess.run([
                "docker", "exec", "ats-dev-analytics", "find", "/data/training", 
                "-type", "f", "\\(", "-name", "*.arrayrecord", "-o", "-name", "*.riegeli", "\\)",
                "-exec", "basename", "{}", "\\;"
            ], capture_output=True, text=True, timeout=10)
            
            all_files = result.stdout.strip().split('\n') if result.stdout.strip() else []
            matching_files = [f for f in all_files if target_symbol.lower() in f.lower()]
            
            details = f"Symbol: {target_symbol}, All files: {len(all_files)}, Matching: {len(matching_files)}"
            if matching_files:
                details += f", Matches: {matching_files[:2]}"
            
            success = len(matching_files) > 0
            self.log_test("File Discovery Logic", success, details)
            return success
        except Exception as e:
            self.log_test("File Discovery Logic", False, str(e))
            return False
    
    def test_9_arrayrecord_reading(self):
        """Test actual ArrayRecord file reading."""
        try:
            # Find an ArrayRecord file to test
            result = subprocess.run([
                "docker", "exec", "ats-dev-analytics", "find", "/data/training", 
                "-name", "*.arrayrecord", "-type", "f"
            ], capture_output=True, text=True, timeout=10)
            
            files = result.stdout.strip().split('\n') if result.stdout.strip() else []
            files = [f for f in files if f]  # Remove empty
            
            if not files:
                self.log_test("ArrayRecord Reading", False, "No ArrayRecord files found")
                return False
            
            test_file = files[0]
            
            # Test reading the file
            read_script = f'''
import json
from array_record.python.array_record_module import ArrayRecordReader

try:
    with ArrayRecordReader("{test_file}") as reader:
        records = list(reader)
        print(f"SUCCESS: Read {{len(records)}} records")
        if records:
            first_record = json.loads(records[0].decode())
            print(f"SAMPLE: {{list(first_record.keys())[:5]}}")
except Exception as e:
    print(f"ERROR: {{e}}")
'''
            
            result = subprocess.run([
                "docker", "exec", "ats-dev-analytics", "python3", "-c", read_script
            ], capture_output=True, text=True, timeout=15)
            
            success = "SUCCESS" in result.stdout
            details = f"File: {test_file.split('/')[-1]}, Output: {result.stdout.strip()}"
            if not success:
                details += f", Error: {result.stderr.strip()}"
            
            self.log_test("ArrayRecord Reading", success, details)
            return success
        except Exception as e:
            self.log_test("ArrayRecord Reading", False, str(e))
            return False
    
    def test_10_eda_frontend(self):
        """Test EDA frontend loads and has required elements."""
        try:
            response = requests.get(f"{self.base_url}/eda", timeout=10)
            success = response.status_code == 200
            
            if success:
                content = response.text
                required_elements = [
                    "dataset-selector",
                    "sequence-selector", 
                    "loadTrainingDatasets",
                    "createSequenceTable"
                ]
                
                missing = [elem for elem in required_elements if elem not in content]
                details = f"Page loaded, missing elements: {missing}"
                success = len(missing) == 0
            else:
                details = f"HTTP {response.status_code}"
            
            self.log_test("EDA Frontend", success, details)
            return success
        except Exception as e:
            self.log_test("EDA Frontend", False, str(e))
            return False
    
    def run_full_test_suite(self):
        """Run complete test suite and provide summary."""
        print("🧪 Starting Comprehensive Training Data E2E Test Suite")
        print("=" * 70)
        
        # Run tests in logical order
        tests = [
            self.test_1_database_connection,
            self.test_2_training_datasets_table,
            self.test_3_file_system_access,
            self.test_4_training_data_files,
            self.test_5_arrayrecord_library,
            self.test_6_training_datasets_api,
            lambda: self.test_7_specific_dataset_api(40),
            lambda: self.test_7_specific_dataset_api(41),
            self.test_8_file_discovery_logic,
            self.test_9_arrayrecord_reading,
            self.test_10_eda_frontend
        ]
        
        passed = 0
        total = len(tests)
        
        for i, test in enumerate(tests, 1):
            print(f"\n--- Test {i}/{total} ---")
            try:
                result = test()
                # Handle tuple returns from some tests
                if isinstance(result, tuple):
                    result = result[0]
                if result:
                    passed += 1
            except Exception as e:
                print(f"❌ Test {i} crashed: {e}")
        
        print("\n" + "=" * 70)
        print(f"📊 Test Results: {passed}/{total} passed")
        
        if self.failures:
            print(f"\n❌ Failures ({len(self.failures)}):")
            for i, failure in enumerate(self.failures, 1):
                print(f"  {i}. {failure}")
        
        if passed == total:
            print("\n🎉 ALL TESTS PASSED - System is working correctly!")
        else:
            print(f"\n⚠️  {total - passed} tests failed - Issues identified:")
            print("Fix the failing tests above to resolve the visualization issue.")
        
        return passed == total

if __name__ == "__main__":
    tester = TrainingDataE2ETester()
    success = tester.run_full_test_suite()
    exit(0 if success else 1)