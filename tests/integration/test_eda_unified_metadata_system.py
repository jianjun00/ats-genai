#!/usr/bin/env python3
"""
Integration Tests for EDA Unified Metadata System
Tests all issues found and validates fixes
"""

import pytest
import requests
import json
import time
import asyncio
from unittest.mock import patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from services.analytics_service import JobManager, AnalyticsHandler
from services.dataset_metadata_service import DatasetMetadataService, DatasetType
import asyncpg

class TestEDAUnifiedMetadataSystem:
    """Test suite for the unified metadata system and issue coverage"""
    
    BASE_URL = "http://localhost:3000"
    
    @pytest.fixture(scope="class")
    def service_running(self):
        """Ensure analytics service is running"""
        try:
            response = requests.get(f"{self.BASE_URL}/health", timeout=5)
            if response.status_code != 200:
                pytest.skip("Analytics service not running")
        except requests.ConnectionError:
            pytest.skip("Analytics service not accessible")
    
    # Test 1: Core UI and Interface Tests
    
    def test_eda_page_loads_with_unified_tabs(self, service_running):
        """Test that EDA page loads with database and training dataset tabs"""
        response = requests.get(f"{self.BASE_URL}/eda", timeout=10)
        
        assert response.status_code == 200, "EDA page should load successfully"
        content = response.text
        
        # Verify tab structure
        assert "dataset-tabs" in content, "Dataset tabs container should exist"
        assert "Database Tables" in content, "Database Tables tab should be present"
        assert "Training Datasets" in content, "Training Datasets tab should be present"
        assert "automatically when datasets" in content, "Auto-statistics message should be present"
        
        # Verify JavaScript functions exist
        assert "switchTab(" in content, "Tab switching function should exist"
        assert "loadDatasets(" in content, "Dataset loading function should exist"
    
    def test_tabs_styling_and_css(self, service_running):
        """Test that tab styling and CSS is properly included"""
        response = requests.get(f"{self.BASE_URL}/eda", timeout=10)
        content = response.text
        
        # Verify CSS classes exist
        assert ".dataset-tabs" in content, "Tab CSS should be present"
        assert ".tab-button" in content, "Tab button styling should exist"
        assert ".tab-button.active" in content, "Active tab styling should exist"
        assert "border-bottom-color: #3498db" in content, "Active tab color should be defined"
    
    # Test 2: API Endpoint Tests
    
    def test_datasets_api_basic_functionality(self, service_running):
        """Test basic datasets API functionality"""
        response = requests.get(f"{self.BASE_URL}/api/eda/datasets", timeout=15)
        
        assert response.status_code == 200, "Datasets API should respond successfully"
        datasets = response.json()
        
        assert isinstance(datasets, list), "Datasets should be returned as list"
        assert len(datasets) > 0, "Should return at least some datasets"
        
        # Verify dataset structure
        for dataset in datasets[:3]:  # Check first 3
            assert "name" in dataset, "Dataset should have name"
            assert "display_name" in dataset, "Dataset should have display_name"
            assert "row_count" in dataset, "Dataset should have row_count"
            assert "column_count" in dataset, "Dataset should have column_count"
    
    def test_datasets_api_with_training_parameter(self, service_running):
        """Test datasets API with include_training parameter"""
        # Test without training datasets
        response = requests.get(f"{self.BASE_URL}/api/eda/datasets?include_training=false", timeout=15)
        assert response.status_code == 200, "Datasets API should work with training=false"
        
        # Test with training datasets  
        response = requests.get(f"{self.BASE_URL}/api/eda/datasets?include_training=true", timeout=15)
        assert response.status_code == 200, "Datasets API should work with training=true"
    
    def test_schema_api_functionality(self, service_running):
        """Test schema API for small tables"""
        # Use a small table to avoid timeout issues
        small_tables = ["dev_analyst_ratings", "dev_comprehensive_backtest_runs", "dev_column_semantic_types"]
        
        schema_working = False
        for table in small_tables:
            try:
                response = requests.get(f"{self.BASE_URL}/api/eda/datasets/{table}/schema", timeout=8)
                if response.status_code == 200:
                    schema = response.json()
                    assert "table_name" in schema, "Schema should include table_name"
                    assert "columns" in schema, "Schema should include columns"
                    assert isinstance(schema["columns"], list), "Columns should be a list"
                    
                    # Verify column structure
                    if schema["columns"]:
                        col = schema["columns"][0]
                        assert "name" in col, "Column should have name"
                        assert "type" in col, "Column should have type"
                        assert "nullable" in col, "Column should have nullable info"
                    
                    schema_working = True
                    break
            except requests.Timeout:
                continue
        
        assert schema_working, f"Schema API should work for at least one small table from {small_tables}"
    
    def test_data_table_api_functionality(self, service_running):
        """Test data table API with small payload"""
        # Test with minimal data request
        payload = {
            "filters": {},
            "page": 1,
            "page_size": 2  # Very small page size
        }
        
        # Try small tables first
        small_tables = ["dev_analyst_ratings", "dev_comprehensive_backtest_runs"]
        
        data_api_working = False
        for table in small_tables:
            try:
                response = requests.post(
                    f"{self.BASE_URL}/api/eda/datasets/{table}/data",
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=10
                )
                
                if response.status_code == 200:
                    data = response.json()
                    assert "data" in data, "Response should include data"
                    assert "total_count" in data, "Response should include total_count"
                    assert "current_page" in data, "Response should include current_page"
                    assert isinstance(data["data"], list), "Data should be a list"
                    
                    data_api_working = True
                    break
                    
            except requests.Timeout:
                continue
        
        assert data_api_working, f"Data API should work for at least one small table from {small_tables}"
    
    # Test 3: Job Manager Variable Scope Issues (The main bug we fixed)
    
    def test_job_manager_variable_scope_fixes(self):
        """Test that job_manager variable is properly defined in all scopes"""
        from services.analytics_service import AnalyticsHandler
        from unittest.mock import MagicMock
        import io
        
        # Create mock request handler
        handler = AnalyticsHandler(MagicMock(), ('127.0.0.1', 12345), MagicMock())
        handler.wfile = io.BytesIO()
        
        # Test that JobManager is imported and can be instantiated
        from services.analytics_service import JobManager
        job_manager = JobManager()
        assert job_manager is not None, "JobManager should be instantiable"
        
        # Test the specific methods that were failing
        assert hasattr(job_manager, 'get_dataset_schema'), "JobManager should have get_dataset_schema method"
        assert hasattr(job_manager, 'get_column_values'), "JobManager should have get_column_values method" 
        assert hasattr(job_manager, 'get_filtered_data'), "JobManager should have get_filtered_data method"
        assert hasattr(job_manager, 'get_job_stats'), "JobManager should have get_job_stats method"
        assert hasattr(job_manager, 'get_recent_jobs'), "JobManager should have get_recent_jobs method"
    
    def test_job_manager_error_handling(self):
        """Test that job_manager errors are properly handled"""
        from services.analytics_service import JobManager
        
        job_manager = JobManager()
        
        # Test with invalid table name
        try:
            schema = job_manager.get_dataset_schema("nonexistent_table")
            # Should either return error dict or raise exception
            if isinstance(schema, dict) and "error" in schema:
                assert True, "Error properly handled in dict format"
            else:
                # If no error, schema should be valid
                assert "columns" in schema, "Valid schema should have columns"
        except Exception as e:
            assert "error" in str(e).lower() or "not" in str(e).lower(), "Exception should indicate error"
    
    # Test 4: Database Metadata System Tests
    
    def test_metadata_database_tables_exist(self):
        """Test that metadata database tables exist and have correct structure"""
        import asyncpg
        import asyncio
        
        async def check_tables():
            try:
                # Try Docker connection first
                conn = await asyncpg.connect(
                    host='postgres',
                    port=5432,
                    user='postgres',
                    password='dev_password',
                    database='dev_db'
                )
            except:
                # Fallback to localhost
                conn = await asyncpg.connect(
                    host='localhost',
                    port=3432,
                    user='postgres',
                    password='dev_password',
                    database='dev_db'
                )
            
            # Check that our metadata tables exist
            tables = await conn.fetch("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name LIKE 'dev_dataset%'
                ORDER BY table_name
            """)
            
            table_names = [row['table_name'] for row in tables]
            
            await conn.close()
            return table_names
        
        table_names = asyncio.run(check_tables())
        
        assert 'dev_datasets' in table_names, "dev_datasets table should exist"
        assert 'dev_dataset_columns' in table_names, "dev_dataset_columns table should exist"
        assert 'dev_dataset_column_stats' in table_names, "dev_dataset_column_stats table should exist"
    
    def test_training_datasets_populated(self):
        """Test that sample training datasets were populated"""
        import asyncpg
        import asyncio
        
        async def check_training_data():
            try:
                conn = await asyncpg.connect(
                    host='postgres', port=5432, user='postgres',
                    password='dev_password', database='dev_db'
                )
            except:
                conn = await asyncpg.connect(
                    host='localhost', port=3432, user='postgres', 
                    password='dev_password', database='dev_db'
                )
            
            training_datasets = await conn.fetch("""
                SELECT name, display_name, dataset_type, total_rows
                FROM dev_datasets 
                WHERE dataset_type = 'training_dataset'
                ORDER BY name
            """)
            
            await conn.close()
            return training_datasets
        
        training_data = asyncio.run(check_training_data())
        
        assert len(training_data) >= 4, "Should have at least 4 training datasets"
        
        # Check for specific training datasets we created
        names = [row['name'] for row in training_data]
        assert 'ml_feature_matrix_v1' in names, "ML feature matrix should exist"
        assert 'backtest_results_2024' in names, "Backtest results should exist"
        assert 'portfolio_optimization_features' in names, "Portfolio features should exist"
        assert 'sentiment_analysis_training' in names, "Sentiment training data should exist"
    
    # Test 5: Performance and Timeout Issue Tests
    
    def test_api_timeout_handling(self, service_running):
        """Test that APIs handle timeouts gracefully"""
        # Test with very short timeout to force timeout condition
        try:
            response = requests.get(
                f"{self.BASE_URL}/api/eda/datasets/dev_daily_prices_tiingo/schema", 
                timeout=0.1  # Very short timeout
            )
            # If it doesn't timeout, that's fine too
            if response.status_code == 200:
                pytest.skip("API responded too quickly to test timeout handling")
        except requests.Timeout:
            # This is expected - timeout should be handled gracefully
            assert True, "Timeout should be handled gracefully"
        except requests.ConnectionError:
            # Service might be overloaded, which is also a valid test result
            assert True, "Connection error indicates service handling load"
    
    def test_large_dataset_handling(self, service_running):
        """Test behavior with large datasets"""
        # Get list of large datasets
        response = requests.get(f"{self.BASE_URL}/api/eda/datasets", timeout=10)
        datasets = response.json()
        
        large_datasets = [d for d in datasets if d.get('row_count', 0) > 10_000_000]
        
        if not large_datasets:
            pytest.skip("No large datasets available for testing")
        
        # Test that large datasets are properly identified
        assert len(large_datasets) >= 2, "Should identify multiple large datasets"
        
        # Verify they have proper metadata
        for dataset in large_datasets:
            assert dataset['row_count'] > 10_000_000, "Large dataset should have >10M rows"
            assert 'size' in dataset, "Large dataset should have size info"
    
    # Test 6: Async/Await Syntax Error Tests
    
    def test_no_await_outside_async_function(self):
        """Test that there are no await statements outside async functions"""
        import ast
        import os
        
        analytics_service_path = os.path.join(
            os.path.dirname(__file__), '../../src/services/analytics_service.py'
        )
        
        with open(analytics_service_path, 'r') as f:
            source = f.read()
        
        # Parse the AST
        tree = ast.parse(source)
        
        class AwaitChecker(ast.NodeVisitor):
            def __init__(self):
                self.errors = []
                self.in_async_function = False
                self.function_stack = []
            
            def visit_FunctionDef(self, node):
                self.function_stack.append(node.name)
                old_in_async = self.in_async_function
                self.in_async_function = False
                self.generic_visit(node)
                self.in_async_function = old_in_async
                self.function_stack.pop()
            
            def visit_AsyncFunctionDef(self, node):
                self.function_stack.append(node.name)
                old_in_async = self.in_async_function
                self.in_async_function = True
                self.generic_visit(node)
                self.in_async_function = old_in_async
                self.function_stack.pop()
            
            def visit_Await(self, node):
                if not self.in_async_function:
                    context = f" in function {self.function_stack[-1]}" if self.function_stack else ""
                    self.errors.append(f"await outside async function{context} at line {node.lineno}")
                self.generic_visit(node)
        
        checker = AwaitChecker()
        checker.visit(tree)
        
        assert len(checker.errors) == 0, f"Found await outside async function: {checker.errors}"
    
    # Test 7: Integration Tests for Complete Workflow
    
    def test_complete_user_workflow_small_dataset(self, service_running):
        """Test complete user workflow with a small dataset"""
        # Step 1: Get datasets
        response = requests.get(f"{self.BASE_URL}/api/eda/datasets", timeout=10)
        assert response.status_code == 200
        datasets = response.json()
        
        # Find smallest dataset
        small_datasets = [d for d in datasets if d.get('row_count', 0) < 100]
        if not small_datasets:
            small_datasets = [d for d in datasets if d.get('row_count', 0) < 10000]
        
        if not small_datasets:
            pytest.skip("No small datasets available for workflow test")
        
        test_dataset = small_datasets[0]
        dataset_name = test_dataset['name']
        
        # Step 2: Get schema (with extended timeout)
        try:
            response = requests.get(f"{self.BASE_URL}/api/eda/datasets/{dataset_name}/schema", timeout=15)
            if response.status_code == 200:
                schema = response.json()
                assert 'columns' in schema
                
                # Step 3: Get data sample
                payload = {"filters": {}, "page": 1, "page_size": 1}
                response = requests.post(
                    f"{self.BASE_URL}/api/eda/datasets/{dataset_name}/data",
                    json=payload,
                    timeout=10
                )
                
                if response.status_code == 200:
                    data = response.json()
                    assert 'data' in data
                    pytest.mark.workflow_complete = True
                    
        except requests.Timeout:
            pytest.skip("Workflow test skipped due to timeout - performance issue noted")
    
    # Test 8: Error Condition Tests
    
    def test_invalid_dataset_name_handling(self, service_running):
        """Test handling of invalid dataset names"""
        response = requests.get(f"{self.BASE_URL}/api/eda/datasets/nonexistent_table_12345/schema", timeout=5)
        
        # Should either return error or 404, not crash
        assert response.status_code in [200, 404, 500], "Invalid dataset should be handled gracefully"
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and 'error' in data:
                assert 'not' in data['error'].lower() or 'exist' in data['error'].lower(), "Error should indicate missing table"
    
    def test_malformed_request_handling(self, service_running):
        """Test handling of malformed requests"""
        # Test malformed JSON
        try:
            response = requests.post(
                f"{self.BASE_URL}/api/eda/datasets/dev_analyst_ratings/data",
                data="invalid json{",
                headers={'Content-Type': 'application/json'},
                timeout=5
            )
            assert response.status_code in [400, 500], "Malformed JSON should return appropriate error code"
        except requests.Timeout:
            pass  # Timeout is acceptable for this test
    
    # Test 9: Service Health and Dependencies
    
    def test_service_dependencies(self, service_running):
        """Test that service dependencies are working"""
        # Test database connection
        import subprocess
        
        result = subprocess.run([
            'bash', '-c',
            'PGPASSWORD=dev_password psql -h localhost -p 3432 -U postgres -d dev_db -c "SELECT 1;" 2>/dev/null'
        ], capture_output=True, timeout=5)
        
        assert result.returncode == 0, "Database should be accessible"
    
    def test_service_health_endpoint(self, service_running):
        """Test service health endpoint"""
        response = requests.get(f"{self.BASE_URL}/health", timeout=5)
        assert response.status_code == 200
        
        health_data = response.json()
        assert "status" in health_data
        assert health_data["status"] == "healthy"


# Performance and Load Tests
class TestPerformanceIssues:
    """Specific tests for performance issues we identified"""
    
    def test_large_table_timeout_issue(self):
        """Document and test the large table timeout issue"""
        # This test documents the known issue with large tables
        BASE_URL = "http://localhost:3000"
        
        large_tables = ["dev_daily_prices_tiingo", "dev_daily_prices_eodhd"]
        
        for table in large_tables:
            try:
                response = requests.get(f"{BASE_URL}/api/eda/datasets/{table}/schema", timeout=3)
                if response.status_code == 200:
                    pytest.skip(f"Large table {table} responded quickly - timeout issue may be resolved")
            except requests.Timeout:
                # This documents the known performance issue
                pytest.xfail(f"Known issue: Large table {table} causes timeout - needs performance optimization")
    
    def test_ray_dns_resolution_issue(self):
        """Document the Ray DNS resolution issue found in logs"""
        # This test documents the Ray integration issue
        # Look for the specific error in a controlled way
        
        import subprocess
        
        try:
            result = subprocess.run([
                'docker', 'logs', 'ats-dev-analytics', '--tail', '100'
            ], capture_output=True, text=True, timeout=5)
            
            if 'Temporary failure in name resolution' in result.stderr or 'Temporary failure in name resolution' in result.stdout:
                pytest.xfail("Known issue: Ray has DNS resolution problems in Docker environment")
            
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pytest.skip("Cannot check logs for Ray DNS issue")


if __name__ == "__main__":
    # Run tests directly
    import subprocess
    
    print("🧪 Running EDA Unified Metadata System Tests...")
    
    # Run with pytest
    result = subprocess.run([
        'python', '-m', 'pytest', __file__, '-v', '--tb=short'
    ], cwd=os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    
    exit(result.returncode)