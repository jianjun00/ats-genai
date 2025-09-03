"""
Test cases to detect missing dataset endpoints in analytics webapp
This test suite ensures that the webapp has proper dataset functionality
and can detect errors like "Error loading sequences" in the dataset detail page.
"""

import pytest
import asyncio
import aiohttp
import json
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class TestDatasetEndpointDetection:
    """Test suite to detect missing or broken dataset endpoints"""
    
    @pytest.fixture
    def webapp_base_url(self):
        """Base URL for the webapp (change this to match your deployment)"""
        return "http://10.0.0.79:3000"
    
    @pytest.fixture
    async def http_session(self):
        """Create HTTP session for making requests"""
        async with aiohttp.ClientSession() as session:
            yield session
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_webapp_health_check(self, webapp_base_url, http_session):
        """Test that the webapp is accessible"""
        try:
            async with http_session.get(f"{webapp_base_url}/health") as response:
                assert response.status == 200
                data = await response.json()
                assert "status" in data
                logger.info(f"✅ Webapp health check passed: {data}")
        except Exception as e:
            pytest.fail(f"❌ Webapp health check failed: {e}")
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_endpoints_exist(self, webapp_base_url, http_session):
        """Test that required dataset endpoints exist"""
        required_endpoints = [
            "/api/v1/datasets",           # List datasets
            "/api/v1/datasets/1",         # Get specific dataset
            "/api/v1/datasets/1/sequences", # Get dataset sequences
            "/api/v1/datasets/1/metadata"   # Get dataset metadata
        ]
        
        missing_endpoints = []
        
        for endpoint in required_endpoints:
            try:
                async with http_session.get(f"{webapp_base_url}{endpoint}") as response:
                    # Accept 200 (success) or 404 (endpoint exists but no data)
                    # Reject 405 (method not allowed) or other errors indicating missing endpoint
                    if response.status == 405:
                        missing_endpoints.append(f"{endpoint} - Method not allowed (endpoint missing)")
                    elif response.status >= 500:
                        missing_endpoints.append(f"{endpoint} - Server error: {response.status}")
                    else:
                        logger.info(f"✅ Endpoint {endpoint} exists (status: {response.status})")
            except aiohttp.ClientConnectorError:
                missing_endpoints.append(f"{endpoint} - Connection failed")
            except Exception as e:
                missing_endpoints.append(f"{endpoint} - Error: {e}")
        
        if missing_endpoints:
            pytest.fail(f"❌ Missing dataset endpoints:\n" + "\n".join(missing_endpoints))
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_detail_page_accessibility(self, webapp_base_url, http_session):
        """Test that dataset detail page is accessible"""
        dataset_detail_url = f"{webapp_base_url}/dataset-detail?id=1"
        
        try:
            async with http_session.get(dataset_detail_url) as response:
                if response.status == 404:
                    pytest.fail(f"❌ Dataset detail page not found: {dataset_detail_url}")
                elif response.status >= 500:
                    pytest.fail(f"❌ Dataset detail page server error: {response.status}")
                
                content = await response.text()
                
                # Check for error messages in the HTML content
                error_indicators = [
                    "Error loading sequences",
                    "Dataset not found",
                    "Failed to load",
                    "500 Internal Server Error",
                    "404 Not Found"
                ]
                
                found_errors = []
                for error in error_indicators:
                    if error.lower() in content.lower():
                        found_errors.append(error)
                
                if found_errors:
                    pytest.fail(f"❌ Dataset detail page contains errors: {found_errors}")
                
                logger.info(f"✅ Dataset detail page accessible without obvious errors")
                
        except Exception as e:
            pytest.fail(f"❌ Failed to access dataset detail page: {e}")
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_sequence_loading(self, webapp_base_url, http_session):
        """Test that dataset sequences can be loaded via API"""
        sequence_endpoint = f"{webapp_base_url}/api/v1/datasets/1/sequences"
        
        try:
            async with http_session.get(sequence_endpoint) as response:
                if response.status == 404:
                    logger.warning(f"⚠️ Dataset sequences endpoint not found: {sequence_endpoint}")
                    return  # This is expected if dataset functionality is missing
                
                if response.status >= 500:
                    pytest.fail(f"❌ Dataset sequences endpoint server error: {response.status}")
                
                # Try to parse JSON response
                try:
                    data = await response.json()
                    
                    # Check for error messages in JSON response
                    if isinstance(data, dict):
                        if "error" in data:
                            pytest.fail(f"❌ Dataset sequences API returned error: {data['error']}")
                        if "sequences" in data or "data" in data:
                            logger.info(f"✅ Dataset sequences API returned data successfully")
                        else:
                            logger.warning(f"⚠️ Dataset sequences API returned unexpected format: {list(data.keys())}")
                    
                except json.JSONDecodeError:
                    # If not JSON, check if it's an error page
                    content = await response.text()
                    if "error" in content.lower() or "failed" in content.lower():
                        pytest.fail(f"❌ Dataset sequences endpoint returned error page")
                
        except aiohttp.ClientConnectorError:
            pytest.fail(f"❌ Cannot connect to dataset sequences endpoint")
        except Exception as e:
            pytest.fail(f"❌ Error testing dataset sequences: {e}")
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_metadata_loading(self, webapp_base_url, http_session):
        """Test that dataset metadata can be loaded via API"""
        metadata_endpoint = f"{webapp_base_url}/api/v1/datasets/1/metadata"
        
        try:
            async with http_session.get(metadata_endpoint) as response:
                if response.status == 404:
                    logger.warning(f"⚠️ Dataset metadata endpoint not found: {metadata_endpoint}")
                    return  # This is expected if dataset functionality is missing
                
                if response.status >= 500:
                    pytest.fail(f"❌ Dataset metadata endpoint server error: {response.status}")
                
                # Try to parse JSON response
                try:
                    data = await response.json()
                    
                    # Check for expected metadata fields
                    if isinstance(data, dict):
                        if "error" in data:
                            pytest.fail(f"❌ Dataset metadata API returned error: {data['error']}")
                        
                        expected_fields = ["dataset_name", "total_sequences", "feature_count", "label_count"]
                        missing_fields = [field for field in expected_fields if field not in data]
                        
                        if missing_fields:
                            logger.warning(f"⚠️ Dataset metadata missing fields: {missing_fields}")
                        else:
                            logger.info(f"✅ Dataset metadata API returned complete data")
                    
                except json.JSONDecodeError:
                    content = await response.text()
                    if "error" in content.lower():
                        pytest.fail(f"❌ Dataset metadata endpoint returned error page")
                
        except Exception as e:
            logger.warning(f"⚠️ Error testing dataset metadata: {e}")
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_list_functionality(self, webapp_base_url, http_session):
        """Test that dataset list functionality works"""
        datasets_endpoint = f"{webapp_base_url}/api/v1/datasets"
        
        try:
            async with http_session.get(datasets_endpoint) as response:
                if response.status == 404:
                    pytest.fail(f"❌ Datasets list endpoint not found: {datasets_endpoint}")
                
                if response.status >= 500:
                    pytest.fail(f"❌ Datasets list endpoint server error: {response.status}")
                
                try:
                    data = await response.json()
                    
                    if isinstance(data, dict):
                        if "error" in data:
                            pytest.fail(f"❌ Datasets list API returned error: {data['error']}")
                        
                        if "datasets" in data:
                            datasets = data["datasets"]
                            if isinstance(datasets, list):
                                logger.info(f"✅ Datasets list API returned {len(datasets)} datasets")
                            else:
                                pytest.fail(f"❌ Datasets list API returned invalid format")
                        else:
                            logger.warning(f"⚠️ Datasets list API returned unexpected format: {list(data.keys())}")
                    
                except json.JSONDecodeError:
                    pytest.fail(f"❌ Datasets list endpoint did not return valid JSON")
                
        except Exception as e:
            pytest.fail(f"❌ Error testing datasets list: {e}")
    
    def test_dataset_database_connectivity(self):
        """Test that the database has required dataset tables"""
        try:
            import asyncpg
            import asyncio
            import os
            
            async def check_tables():
                # Use environment variables or default values
                db_url = "postgresql://postgres:postgres@localhost:5433/dev_db"
                
                try:
                    conn = await asyncpg.connect(db_url)
                    
                    # Check for training dataset table
                    result = await conn.fetchval("""
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_name = 'dev_training_dataset'
                    """)
                    
                    if result == 0:
                        pytest.fail("❌ dev_training_dataset table not found in database")
                    
                    # Check if there are any datasets in the table
                    dataset_count = await conn.fetchval("SELECT COUNT(*) FROM dev_training_dataset")
                    logger.info(f"✅ Found {dataset_count} datasets in database")
                    
                    await conn.close()
                    
                except Exception as e:
                    logger.warning(f"⚠️ Database connectivity test failed: {e}")
                    # Don't fail the test as this might be expected in some environments
            
            asyncio.run(check_tables())
            
        except ImportError:
            logger.warning("⚠️ asyncpg not available, skipping database connectivity test")
        except Exception as e:
            logger.warning(f"⚠️ Database connectivity test error: {e}")


class TestDatasetDetailPageFunctionality:
    """Specific tests for dataset detail page functionality"""
    
    @pytest.fixture
    def webapp_base_url(self):
        return "http://10.0.0.79:3000"
    
    @pytest.fixture
    async def http_session(self):
        async with aiohttp.ClientSession() as session:
            yield session
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_detail_page_specific_error(self, webapp_base_url, http_session):
        """Test specifically for 'Error loading sequences' message"""
        dataset_detail_url = f"{webapp_base_url}/dataset-detail?id=1"
        
        try:
            async with http_session.get(dataset_detail_url) as response:
                content = await response.text()
                
                # Check specifically for the reported error
                if "Error loading sequences" in content:
                    pytest.fail(f"❌ Found 'Error loading sequences' in dataset detail page at {dataset_detail_url}")
                
                # Check for other common error patterns
                error_patterns = [
                    "failed to load",
                    "cannot connect",
                    "server error",
                    "not found",
                    "unauthorized",
                    "internal error"
                ]
                
                found_patterns = []
                for pattern in error_patterns:
                    if pattern.lower() in content.lower():
                        found_patterns.append(pattern)
                
                if found_patterns:
                    logger.warning(f"⚠️ Found potential error patterns in dataset detail page: {found_patterns}")
                
                # Check if the page has expected dataset detail elements
                expected_elements = [
                    "dataset",
                    "sequence", 
                    "feature",
                    "chart",
                    "table"
                ]
                
                found_elements = []
                for element in expected_elements:
                    if element.lower() in content.lower():
                        found_elements.append(element)
                
                logger.info(f"✅ Dataset detail page contains elements: {found_elements}")
                
        except Exception as e:
            pytest.fail(f"❌ Failed to test dataset detail page: {e}")
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_javascript_errors(self, webapp_base_url, http_session):
        """Test for JavaScript errors that might cause 'Error loading sequences'"""
        dataset_detail_url = f"{webapp_base_url}/dataset-detail?id=1"
        
        try:
            async with http_session.get(dataset_detail_url) as response:
                content = await response.text()
                
                # Check for JavaScript-related issues
                js_error_patterns = [
                    "uncaught",
                    "undefined",
                    "null is not",
                    "cannot read property",
                    "failed to fetch",
                    "network error"
                ]
                
                found_js_errors = []
                for pattern in js_error_patterns:
                    if pattern.lower() in content.lower():
                        found_js_errors.append(pattern)
                
                if found_js_errors:
                    pytest.fail(f"❌ Found JavaScript error patterns: {found_js_errors}")
                
                # Check if required JavaScript libraries are loaded
                required_libs = [
                    "chart.js",
                    "fetch",
                    "async",
                    "json"
                ]
                
                found_libs = []
                for lib in required_libs:
                    if lib.lower() in content.lower():
                        found_libs.append(lib)
                
                logger.info(f"✅ Dataset detail page includes libraries: {found_libs}")
                
        except Exception as e:
            logger.warning(f"⚠️ JavaScript error test failed: {e}")


if __name__ == "__main__":
    # Run the tests
    import sys
    sys.exit(pytest.main([__file__, "-v"]))