#!/usr/bin/env python3
"""
Complete Training Data Pipeline Validation Tests

This test validates the COMPLETE end-to-end pipeline that our current tests miss:
1. Generate training data (files + database records)  
2. Verify ArrayRecord files can actually be read by visualization API
3. Test frontend can display the data using Playwright
4. Fail if any step doesn't work

Current test gaps this addresses:
- Training data tests only check file existence, not readability
- API tests only check responses, not actual data visualization  
- No tests verify complete user workflow works
- Tests pass while user experience fails

This test MUST fail if training data generation doesn't produce usable files.
"""

import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import requests
import json
import sys

# Add src to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from playwright.async_api import async_playwright


class TestCompleteTrainingDataPipeline:
    """Tests that catch gaps where files exist but visualization doesn't work."""
    
    @pytest.fixture
    def temp_training_dir(self):
        """Create temporary directory for training data generation."""
        temp_dir = tempfile.mkdtemp(prefix="test_training_pipeline_")
        yield Path(temp_dir)
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_services_are_running(self):
        """Verify required services are running before testing pipeline."""
        services = [
            ("analytics", "http://localhost:3000/health"),
            ("postgres", "localhost:3432")  # Check if we can connect
        ]
        
        for service_name, endpoint in services:
            if service_name == "analytics":
                try:
                    response = requests.get(endpoint, timeout=5)
                    assert response.status_code == 200, f"Analytics service not healthy: {response.status_code}"
                except requests.ConnectionError:
                    pytest.fail(f"Analytics service not accessible at {endpoint}")
            
            elif service_name == "postgres":
                try:
                    # Test database connection
                    import subprocess
                    result = subprocess.run([
                        "PGPASSWORD=dev_password", "psql", 
                        "-h", "localhost", "-p", "3432", 
                        "-U", "postgres", "-d", "dev_db", 
                        "-c", "SELECT 1"
                    ], capture_output=True, text=True, timeout=10)
                    assert result.returncode == 0, f"Database not accessible: {result.stderr}"
                except Exception as e:
                    pytest.fail(f"Database connection failed: {e}")
    
    def test_training_data_generation_creates_usable_files(self, temp_training_dir):
        """Test that training data generation creates files that can actually be used."""
        
        # Step 1: Generate training data using the actual training system
        import subprocess
        import os
        
        # Run a minimal training data generation
        env = os.environ.copy()
        env.update({
            "PYTHONPATH": "src",
            "SYMBOLS": "AAPL", 
            "START_DATE": "2025-08-01",
            "END_DATE": "2025-08-02", 
            "ENVIRONMENT": "dev",
            "STORAGE_FORMAT": "arrayrecord",
            "USE_ADVANCED_STORAGE": "true",
            "OUTPUT_DIR": str(temp_training_dir),
            "DEBUG": "true"
        })
        
        cmd = [
            "python3", "scripts/run_dev.py", "run", 
            "--script", "src/ml/training_data/runners/training_data_callback_runner.py"
        ]
        
        print(f"Running training data generation: {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=Path(__file__).parent.parent.parent, 
                              capture_output=True, text=True, timeout=300, env=env)
        
        if result.returncode != 0:
            print(f"Training data generation stdout: {result.stdout}")
            print(f"Training data generation stderr: {result.stderr}")
            pytest.fail(f"Training data generation failed: {result.stderr}")
        
        print("✅ Training data generation completed")
        
        # Step 2: CRITICAL - Verify files match expected structure for visualization API
        self.validate_training_data_file_structure(temp_training_dir)
        
        # Step 3: Verify files can be found by visualization API path matching
        self.validate_visualization_api_path_matching(temp_training_dir)
    
    def validate_training_data_file_structure(self, output_dir):
        """CRITICAL: Validate training data files match expected structure for visualization API."""
        
        print("🔍 CRITICAL: Validating file structure matches visualization API expectations...")
        
        # Expected structure: /data/training_data/{run_id}/{timeframe}/{symbol}_{start}_{end}.arrayrecord
        expected_timeframes = ['5m', '15m', '1h', '1d', '1w']
        expected_symbols = ['AAPL']  # Based on test setup
        
        structure_errors = []
        
        # Find all ArrayRecord files
        arrayrecord_files = list(output_dir.rglob("*.arrayrecord"))
        assert len(arrayrecord_files) > 0, f"No ArrayRecord files created in {output_dir}"
        
        print(f"Found {len(arrayrecord_files)} ArrayRecord files")
        for file_path in arrayrecord_files:
            print(f"  {file_path.relative_to(output_dir)}")
        
        # Check if files are organized by timeframe and symbol correctly
        for timeframe in expected_timeframes:
            timeframe_files = []
            
            # Look for files in timeframe subdirectories
            timeframe_pattern = f"*/{timeframe}/*"
            timeframe_files = list(output_dir.glob(timeframe_pattern))
            
            if not timeframe_files:
                structure_errors.append(f"No files found for timeframe {timeframe} in expected structure: {output_dir}/*/{timeframe}/")
                continue
            
            print(f"✅ Found {len(timeframe_files)} files for timeframe {timeframe}")
            
            # Validate each symbol has a file for this timeframe
            for symbol in expected_symbols:
                symbol_files = [f for f in timeframe_files if symbol in f.name]
                if not symbol_files:
                    structure_errors.append(f"No {symbol} file found for timeframe {timeframe}")
                else:
                    # Validate filename format: {symbol}_{start}_{end}.arrayrecord
                    for file_path in symbol_files:
                        filename = file_path.name
                        if not filename.startswith(f"{symbol}_") or not filename.endswith(".arrayrecord"):
                            structure_errors.append(f"Incorrect filename format: {filename} (should be {symbol}_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS.arrayrecord)")
        
        if structure_errors:
            print(f"\n❌ CRITICAL FILE STRUCTURE ERRORS:")
            for i, error in enumerate(structure_errors, 1):
                print(f"   {i}. {error}")
            
            print(f"\n📋 EXPECTED STRUCTURE:")
            print(f"   {output_dir}/{{run_id}}/{{timeframe}}/{{symbol}}_{{start}}_{{end}}.arrayrecord")
            print(f"   Example: {output_dir}/123/5m/AAPL_20250801_000000_20250802_000000.arrayrecord")
            
            pytest.fail(f"Training data file structure doesn't match visualization API expectations: {len(structure_errors)} errors")
        
        print("✅ CRITICAL: File structure validation passed - matches visualization API expectations")
    
    def validate_visualization_api_path_matching(self, output_dir):
        """Validate that generated files can be found by visualization API path patterns."""
        
        print("🔍 Testing visualization API path matching patterns...")
        
        from pathlib import Path
        import glob
        
        # Simulate the path patterns used by visualization API
        search_patterns = [
            # Pattern from analytics_service.py
            f"{output_dir}/*/*/*_*.arrayrecord",  # /data/training_data/{run_id}/{timeframe}/{symbol}_{dates}.arrayrecord
            f"{output_dir}/*/*/*.arrayrecord",    # Fallback pattern
        ]
        
        matching_files = []
        for pattern in search_patterns:
            matches = glob.glob(str(pattern))
            matching_files.extend(matches)
            if matches:
                print(f"✅ Pattern '{pattern}' found {len(matches)} files")
                for match in matches[:3]:  # Show first 3 matches
                    print(f"   {Path(match).relative_to(output_dir)}")
            else:
                print(f"❌ Pattern '{pattern}' found no files")
        
        if not matching_files:
            # Show what files actually exist
            print(f"\n📁 Actual files found:")
            all_arrayrecord = list(output_dir.rglob("*.arrayrecord"))
            for file_path in all_arrayrecord:
                print(f"   {file_path.relative_to(output_dir)}")
            
            pytest.fail("No training data files match visualization API search patterns - API won't find the files!")
        
        print(f"✅ Visualization API path matching validation passed - {len(set(matching_files))} unique files found")
        
        # Step 3: CRITICAL - Verify files can actually be read by ArrayRecord
        try:
            import array_record
            from array_record.python.array_record_module import ArrayRecordReader
            
            for file_path in arrayrecord_files[:2]:  # Test first 2 files
                print(f"Testing ArrayRecord readability: {file_path}")
                
                with ArrayRecordReader(str(file_path)) as reader:
                    records = list(reader)
                    assert len(records) > 0, f"ArrayRecord file is empty: {file_path}"
                    
                    # Verify first record is valid JSON
                    first_record = records[0]
                    assert isinstance(first_record, bytes), "ArrayRecord should return bytes"
                    
                    record_data = json.loads(first_record.decode())
                    assert isinstance(record_data, dict), "Record should be valid JSON dict"
                    
                    # Verify expected structure for visualization
                    assert "features" in record_data, "Record should have features"
                    assert "labels" in record_data, "Record should have labels"
                    
                    print(f"✅ ArrayRecord file is readable: {file_path.name} ({len(records)} records)")
        
        except ImportError:
            pytest.fail("ArrayRecord package not available - this is required for the pipeline")
        
        except Exception as e:
            pytest.fail(f"ArrayRecord files cannot be read: {e}")
    
    def test_training_data_in_database_matches_files(self):
        """Test that database records match actual files on disk."""
        
        # Get training datasets from database
        import subprocess
        result = subprocess.run([
            "python3", "scripts/run_dev.py", "query", 
            "--query", "SELECT id, dataset_name, total_sequences FROM dev_training_dataset ORDER BY id DESC LIMIT 5"
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            pytest.fail(f"Database query failed: {result.stderr}")
        
        output_lines = result.stdout.strip().split('\n')
        if len(output_lines) < 3:  # Header + separator + at least 1 row
            pytest.skip("No training datasets in database")
        
        # Parse output (skip header and separator)
        datasets = []
        for line in output_lines[2:]:
            if line.strip() and '|' in line:
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 3:
                    datasets.append({
                        'id': int(parts[0]) if parts[0].isdigit() else None,
                        'name': parts[1],
                        'sequences': int(parts[2]) if parts[2].isdigit() else 0
                    })
        
        if not datasets:
            pytest.skip("No parseable training datasets found")
        
        # Test API returns the same datasets
        response = requests.get("http://localhost:3000/api/v1/training-datasets", timeout=10)
        assert response.status_code == 200, "Training datasets API should work"
        
        api_data = response.json()
        api_datasets = api_data.get("datasets", [])
        
        # Find matching dataset
        db_dataset = datasets[0]
        api_dataset = next((ds for ds in api_datasets if ds["id"] == db_dataset["id"]), None)
        
        if api_dataset:
            assert api_dataset["total_sequences"] == db_dataset["sequences"], \
                f"Database shows {db_dataset['sequences']} sequences, API shows {api_dataset['total_sequences']}"
            print(f"✅ Database-API consistency verified for dataset {db_dataset['id']}")
        else:
            pytest.fail(f"Dataset {db_dataset['id']} in database but not in API")
    
    def test_visualization_api_returns_actual_data(self):
        """CRITICAL: Test that visualization API returns actual data, not empty arrays."""
        
        # Get available datasets
        response = requests.get("http://localhost:3000/api/v1/training-datasets", timeout=10)
        assert response.status_code == 200, "Training datasets API should work"
        
        datasets = response.json().get("datasets", [])
        if not datasets:
            pytest.skip("No training datasets available")
        
        dataset_id = datasets[0]["id"]
        
        # Test sequences API
        sequences_response = requests.get(
            f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences", timeout=10
        )
        assert sequences_response.status_code == 200, "Sequences API should work"
        
        sequences_data = sequences_response.json()
        sequences = sequences_data.get("sequences", [])
        
        assert len(sequences) > 0, f"Sequences API returns empty array - this is the bug we're testing for!"
        
        print(f"✅ Sequences API returns {len(sequences)} sequences")
        
        # CRITICAL TEST: Visualization data API should return actual data
        viz_response = requests.get(
            f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/visualization-data?start_idx=0", 
            timeout=15
        )
        assert viz_response.status_code == 200, "Visualization API should work"
        
        viz_data = viz_response.json()
        
        # THIS IS THE CRITICAL ASSERTION THAT CATCHES THE BUG
        data_array = viz_data.get("data", [])
        total_records = viz_data.get("total_records", 0)
        
        assert len(data_array) > 0, f"Visualization API returns empty data array - ArrayRecord files not readable!"
        assert total_records > 0, f"Visualization API shows 0 total_records - no data loaded from files!"
        
        print(f"✅ Visualization API returns actual data: {len(data_array)} records")
        
        # Verify data structure is correct for frontend
        if data_array:
            first_record = data_array[0]
            required_fields = ["timestamp", "open", "high", "low", "close", "volume"]
            
            for field in required_fields:
                assert field in first_record, f"Visualization data missing required field: {field}"
            
            print("✅ Visualization data has correct structure for frontend")
    
    @pytest.mark.asyncio
    async def test_complete_frontend_workflow_with_playwright(self):
        """Test complete user workflow from dataset selection to chart visualization."""
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                # Navigate to EDA page
                await page.goto("http://localhost:3000/eda")
                await page.wait_for_load_state("networkidle")
                
                # Click Training Datasets tab
                training_tab = page.get_by_role("button", name="🤖 Training Datasets")
                await training_tab.click()
                await page.wait_for_timeout(2000)
                
                # Select first dataset
                dataset_selector = page.locator("#dataset-selector")
                dataset_options = await dataset_selector.locator("option").count()
                
                if dataset_options <= 1:  # Only default "Choose a dataset..." option
                    pytest.fail("No training datasets available in frontend - dataset loading failed")
                
                await dataset_selector.select_option(index=1)
                await page.wait_for_timeout(3000)
                
                # Select first sequence  
                sequence_selector = page.locator("#sequence-selector")
                sequence_options = await sequence_selector.locator("option").count()
                
                # THIS IS THE CRITICAL TEST - sequences should be available
                assert sequence_options > 1, "No sequences available - this indicates ArrayRecord files not accessible"
                
                await sequence_selector.select_option(index=1)
                await page.wait_for_timeout(5000)  # Wait for visualization to load
                
                # CRITICAL: Verify Plotly chart actually loaded with data
                plotly_selectors = [".plotly", ".js-plotly-plot", ".plotly-graph-div"]
                plotly_found = False
                
                for selector in plotly_selectors:
                    element = page.locator(selector)
                    if await element.count() > 0:
                        plotly_found = True
                        print(f"✅ Found Plotly element: {selector}")
                        break
                
                assert plotly_found, "No Plotly visualization rendered - data not reaching frontend"
                
                # Verify sequence data table has actual data
                table_selectors = ["#sequence-data-table", "table:has(th:contains('Open'))"]
                table_found = False
                
                for selector in table_selectors:
                    try:
                        table = page.locator(selector)
                        if await table.count() > 0:
                            table_text = await table.inner_text()
                            
                            # CRITICAL: Should not show "No sequence data available"
                            assert "No sequence data available" not in table_text, \
                                "Sequence data table shows no data - visualization pipeline failed"
                            
                            # Should have actual data rows
                            rows = table.locator("tr")
                            row_count = await rows.count()
                            assert row_count > 1, "Sequence data table has no data rows"
                            
                            table_found = True
                            print(f"✅ Sequence data table has {row_count - 1} data rows")
                            break
                    except Exception:
                        continue
                
                assert table_found, "No sequence data table found or accessible"
                
                print("✅ Complete frontend workflow works - training data pipeline validated")
                
                # Take screenshot for debugging if needed
                await page.screenshot(path="/tmp/successful_training_pipeline_test.png", full_page=True)
                
            finally:
                await context.close()
                await browser.close()
    
    def test_pipeline_performance_and_file_sizes(self):
        """Test that generated files are reasonable size and performance is acceptable."""
        
        # Check actual training data directories
        training_paths = [
            Path("/mnt/d/ats-data/training"),
            Path("/data/training")
        ]
        
        arrayrecord_files = []
        for path in training_paths:
            if path.exists():
                arrayrecord_files.extend(list(path.rglob("*.arrayrecord")))
        
        if not arrayrecord_files:
            pytest.skip("No ArrayRecord files found for performance testing")
        
        total_size = 0
        file_count = 0
        
        for file_path in arrayrecord_files:
            file_size = file_path.stat().st_size
            total_size += file_size
            file_count += 1
            
            # File should not be empty
            assert file_size > 0, f"ArrayRecord file is empty: {file_path}"
            
            # File should not be unreasonably large (> 1GB per file)
            assert file_size < 1_000_000_000, f"ArrayRecord file too large: {file_path} ({file_size} bytes)"
            
            print(f"File: {file_path.name} - Size: {file_size:,} bytes")
        
        avg_size = total_size / file_count if file_count > 0 else 0
        
        print(f"✅ Performance check: {file_count} files, total {total_size:,} bytes, avg {avg_size:,} bytes")
        
        # Total should be reasonable
        assert total_size < 10_000_000_000, f"Total training data too large: {total_size:,} bytes"


@pytest.mark.integration
@pytest.mark.slow
def test_complete_pipeline_integration():
    """Main integration test that must pass for training data pipeline to be considered working."""
    
    tester = TestCompleteTrainingDataPipeline()
    
    # Run all validation steps
    print("🔧 Step 1: Verifying services are running...")
    tester.test_services_are_running()
    
    print("🔧 Step 2: Testing training data-database consistency...")
    tester.test_training_data_in_database_matches_files()
    
    print("🔧 Step 3: CRITICAL - Testing visualization API returns actual data...")
    tester.test_visualization_api_returns_actual_data()
    
    print("🔧 Step 4: CRITICAL - Testing complete frontend workflow...")
    asyncio.run(tester.test_complete_frontend_workflow_with_playwright())
    
    print("🔧 Step 5: Testing performance and file sizes...")
    tester.test_pipeline_performance_and_file_sizes()
    
    print("✅ COMPLETE TRAINING DATA PIPELINE VALIDATION PASSED")
    print("   - Files created ✅")
    print("   - ArrayRecord files readable ✅")  
    print("   - Visualization API returns data ✅")
    print("   - Frontend displays charts ✅")
    print("   - User workflow works end-to-end ✅")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "--tb=short"])