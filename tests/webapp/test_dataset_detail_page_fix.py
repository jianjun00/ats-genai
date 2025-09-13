#!/usr/bin/env python3
"""
Test Suite: Dataset Detail Page Fix Verification
Ensures that the dataset detail page issue (returning 404) is resolved and prevented.

This test verifies:
1. Dataset detail page HTML endpoint accessibility
2. Dataset API endpoints functionality
3. Dataset metadata endpoints functionality
4. Real data integration (no mock data)
5. Proper error handling for invalid dataset IDs
"""

import pytest
import aiohttp

class TestDatasetDetailPageFix:
    """Test dataset detail page accessibility and functionality"""

    @pytest.fixture
    def webapp_base_url(self):
        """Base URL for the analytics webapp"""
        return "http://10.0.0.79:3000"

    @pytest.fixture
    async def http_session(self):
        """HTTP session for making requests"""
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:
            yield session

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_detail_page_accessibility(self, webapp_base_url, http_session):
        """Test that dataset detail page is accessible and returns HTML"""
        # Test with a known dataset ID (5)
        async with http_session.get(f"{webapp_base_url}/dataset-detail?id=5") as response:
            assert response.status == 200, f"Dataset detail page should be accessible, got {response.status}"

            content_type = response.headers.get('content-type', '')
            assert 'text/html' in content_type, f"Should return HTML content, got {content_type}"

            html_content = await response.text()

            # Verify essential HTML elements
            assert "Dataset Detail - Analytics Platform" in html_content, "Should contain page title"
            assert "REAL DATABASE" in html_content, "Should show real database badge"
            assert "FILE ACCESS" in html_content, "Should show file access badge"
            assert "dataset-meta" in html_content, "Should contain dataset metadata section"
            assert "sequences-content" in html_content, "Should contain sequences section"

            # Verify JavaScript is present for dynamic loading
            assert "fetch(`/api/v1/datasets/${datasetId}`)" in html_content, "Should contain dataset API call"
            assert "fetch('/api/v1/training/files')" in html_content, "Should contain training files API call"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_api_endpoint_functionality(self, webapp_base_url, http_session):
        """Test that dataset API endpoints return real data"""
        # Test multiple dataset IDs to ensure consistency
        test_dataset_ids = [1, 2, 3, 4, 5]

        for dataset_id in test_dataset_ids:
            async with http_session.get(f"{webapp_base_url}/api/v1/datasets/{dataset_id}") as response:
                if response.status == 404:
                    # Dataset doesn't exist, which is acceptable
                    continue

                assert response.status == 200, f"Dataset API should work for ID {dataset_id}, got {response.status}"

                data = await response.json()

                # Verify essential dataset fields are present
                required_fields = ['id', 'dataset_name', 'symbols', 'total_sequences', 'feature_count', 'creation_timestamp']
                for field in required_fields:
                    assert field in data, f"Dataset {dataset_id} should have field '{field}'"

                # Verify data types and reasonable values
                assert isinstance(data['id'], int), "Dataset ID should be integer"
                assert isinstance(data['dataset_name'], str), "Dataset name should be string"
                assert isinstance(data['symbols'], list), "Symbols should be a list"
                assert isinstance(data['total_sequences'], int), "Total sequences should be integer"
                assert isinstance(data['feature_count'], int), "Feature count should be integer"
                assert data['total_sequences'] > 0, "Should have positive number of sequences"
                assert data['feature_count'] > 0, "Should have positive number of features"

                # Verify no mock data indicators
                assert "mock" not in data['dataset_name'].lower(), "Should not contain mock data"
                assert data.get('status') != 'mock', "Status should not be mock"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_metadata_endpoint(self, webapp_base_url, http_session):
        """Test dataset metadata endpoint functionality"""
        async with http_session.get(f"{webapp_base_url}/api/v1/datasets/5/metadata") as response:
            assert response.status == 200, f"Dataset metadata endpoint should work, got {response.status}"

            data = await response.json()

            # Verify metadata structure
            required_fields = ['dataset_name', 'total_sequences', 'feature_count', 'symbols', 'creation_timestamp', 'status']
            for field in required_fields:
                assert field in data, f"Metadata should have field '{field}'"

            # Verify real data status
            assert data['status'] == 'real_database_record', "Should indicate real database record"
            assert 'real file system access' in data['note'], "Should mention real file system access"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_detail_page_error_handling(self, webapp_base_url, http_session):
        """Test error handling for invalid dataset IDs"""
        # Test with non-existent dataset ID
        async with http_session.get(f"{webapp_base_url}/dataset-detail?id=999999") as response:
            # Page should still load (HTML), but will show error in JavaScript
            assert response.status == 200, "Dataset detail page should load even for invalid ID"

            content_type = response.headers.get('content-type', '')
            assert 'text/html' in content_type, "Should still return HTML content"

        # Test API endpoint with invalid ID
        async with http_session.get(f"{webapp_base_url}/api/v1/datasets/999999") as response:
            assert response.status == 404, "API should return 404 for non-existent dataset"

            data = await response.json()
            assert 'detail' in data, "Should return error detail"
            assert 'not found' in data['detail'].lower(), "Should indicate dataset not found"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_training_files_integration(self, webapp_base_url, http_session):
        """Test that training files endpoint works (used by dataset detail page)"""
        async with http_session.get(f"{webapp_base_url}/api/v1/training/files") as response:
            assert response.status == 200, f"Training files endpoint should work, got {response.status}"

            data = await response.json()

            # Verify training files response structure
            assert 'files' in data, "Should contain files list"
            assert 'total_files' in data, "Should contain total files count"
            assert 'status' in data, "Should contain status"

            # Verify real files access
            assert data['status'] == 'real_files_accessed', "Should indicate real files accessed"
            assert data['total_files'] > 0, "Should have found some training files"

            # Verify file information structure
            if data['files']:
                first_file = data['files'][0]
                required_file_fields = ['path', 'size_mb', 'type', 'modified']
                for field in required_file_fields:
                    assert field in first_file, f"File info should have field '{field}'"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_all_dataset_pages_accessible(self, webapp_base_url, http_session):
        """Test that all existing datasets have accessible detail pages"""
        # First get list of all datasets
        async with http_session.get(f"{webapp_base_url}/api/v1/datasets") as response:
            assert response.status == 200, "Datasets list endpoint should work"

            data = await response.json()
            datasets = data.get('datasets', [])

            assert len(datasets) > 0, "Should have at least one dataset"

            # Test each dataset's detail page
            for dataset in datasets:
                dataset_id = dataset['id']

                # Test HTML page
                async with http_session.get(f"{webapp_base_url}/dataset-detail?id={dataset_id}") as page_response:
                    assert page_response.status == 200, f"Dataset detail page should be accessible for ID {dataset_id}"

                # Test API endpoint
                async with http_session.get(f"{webapp_base_url}/api/v1/datasets/{dataset_id}") as api_response:
                    assert api_response.status == 200, f"Dataset API should work for ID {dataset_id}"

                # Test metadata endpoint
                async with http_session.get(f"{webapp_base_url}/api/v1/datasets/{dataset_id}/metadata") as meta_response:
                    assert meta_response.status == 200, f"Dataset metadata should work for ID {dataset_id}"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_health_endpoint_shows_dataset_features(self, webapp_base_url, http_session):
        """Test that health endpoint confirms dataset detail functionality"""
        async with http_session.get(f"{webapp_base_url}/health") as response:
            assert response.status == 200, "Health endpoint should be accessible"

            data = await response.json()

            # Verify health status
            assert data['status'] == 'healthy', "System should be healthy"
            assert data['database'] == 'connected', "Database should be connected"

            # Verify dataset features are listed
            features = data.get('features', [])
            assert 'datasets' in features, "Should list datasets as a feature"
            assert 'dataset_details' in features, "Should list dataset_details as a feature"
            assert 'real_training_files' in features, "Should list real_training_files as a feature"

            # Verify no mock data
            assert 'mock' not in str(data).lower(), "Health response should not mention mock data"

class TestDatasetDetailRegression:
    """Regression tests to prevent the dataset detail page issue from reoccurring"""

    @pytest.fixture
    def webapp_base_url(self):
        return "http://10.0.0.79:3000"

    @pytest.fixture
    async def http_session(self):
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:
            yield session

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_detail_not_404(self, webapp_base_url, http_session):
        """Regression test: Ensure dataset detail page doesn't return 404"""
        test_cases = [
            {"id": 1, "name": "dataset_1"},
            {"id": 5, "name": "dataset_5"},
            {"id": "invalid", "name": "invalid_id"}
        ]

        for case in test_cases:
            dataset_id = case['id']
            async with http_session.get(f"{webapp_base_url}/dataset-detail?id={dataset_id}") as response:
                assert response.status != 404, f"Dataset detail page should not return 404 for ID {dataset_id}"
                assert response.status == 200, f"Dataset detail page should return 200 for ID {dataset_id}"

                # Verify it's actually HTML, not JSON error
                content_type = response.headers.get('content-type', '')
                assert 'text/html' in content_type, f"Should return HTML, not JSON error for ID {dataset_id}"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_no_detail_not_found_json_responses(self, webapp_base_url, http_session):
        """Regression test: Ensure we don't get {"detail":"Not Found"} responses for pages"""
        endpoints_to_test = [
            "/dataset-detail?id=1",
            "/dataset-detail?id=5",
            "/datasets",
            "/training-data",
            "/jobs"
        ]

        for endpoint in endpoints_to_test:
            async with http_session.get(f"{webapp_base_url}{endpoint}") as response:
                content_type = response.headers.get('content-type', '')

                if 'application/json' in content_type:
                    data = await response.json()
                    # If it's JSON, it shouldn't be a "Not Found" error for these pages
                    if 'detail' in data:
                        assert data['detail'] != 'Not Found', f"Page {endpoint} should not return 'Not Found' error"
                else:
                    # Should be HTML for page endpoints
                    assert 'text/html' in content_type, f"Page {endpoint} should return HTML content"

if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v", "--tb=short"])