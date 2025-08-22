#!/usr/bin/env python3
"""
Regression Test: Dataset Detail Page 404 Issue
Specifically tests the exact issue reported: "http://10.0.0.79:3000/dataset-detail?id=5 returns {"detail":"Not Found"}"

This test ensures:
1. The exact URL that was failing now works
2. The response is HTML, not JSON error 
3. The page contains expected dataset detail content
4. API endpoints supporting the page work correctly
"""

import pytest
import asyncio
import aiohttp
import json

class TestDatasetDetail404Regression:
    """Regression test for the specific dataset detail page 404 issue"""
    
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
    async def test_original_failing_url_now_works(self, webapp_base_url, http_session):
        """Test the exact URL that was failing: http://10.0.0.79:3000/dataset-detail?id=5"""
        failing_url = f"{webapp_base_url}/dataset-detail?id=5"
        
        async with http_session.get(failing_url) as response:
            # CRITICAL: Should NOT be 404
            assert response.status != 404, "Original failing URL should not return 404"
            assert response.status == 200, f"Should return 200, got {response.status}"
            
            # CRITICAL: Should NOT return JSON error
            content_type = response.headers.get('content-type', '')
            assert 'text/html' in content_type, f"Should return HTML, not JSON. Got: {content_type}"
            
            content = await response.text()
            
            # CRITICAL: Should NOT be the JSON error {"detail":"Not Found"}
            assert content != '{"detail":"Not Found"}', "Should not return the original JSON error"
            assert '{"detail":"Not Found"}' not in content, "Should not contain JSON error anywhere"
            
            # Should be actual HTML page
            assert "<!DOCTYPE html>" in content, "Should be valid HTML document"
            assert "Dataset Detail" in content, "Should contain dataset detail page content"
    
    @pytest.mark.asyncio
    async def test_dataset_5_api_endpoint_works(self, webapp_base_url, http_session):
        """Test that the API endpoint for dataset 5 works (supports the page)"""
        api_url = f"{webapp_base_url}/api/v1/datasets/5"
        
        async with http_session.get(api_url) as response:
            # This endpoint should work for the page to load properly
            assert response.status == 200, f"Dataset 5 API should work, got {response.status}"
            
            data = await response.json()
            
            # Should contain real dataset information
            assert 'id' in data, "Should have dataset ID"
            assert data['id'] == 5, "Should be dataset 5"
            assert 'dataset_name' in data, "Should have dataset name"
            assert 'symbols' in data, "Should have symbols"
            
            # Should be real data, not mock
            assert 'mock' not in data['dataset_name'].lower(), "Should not be mock data"
    
    @pytest.mark.asyncio
    async def test_no_json_detail_not_found_responses(self, webapp_base_url, http_session):
        """Ensure no endpoints return the problematic JSON response"""
        test_urls = [
            f"{webapp_base_url}/dataset-detail?id=1",
            f"{webapp_base_url}/dataset-detail?id=2", 
            f"{webapp_base_url}/dataset-detail?id=3",
            f"{webapp_base_url}/dataset-detail?id=4",
            f"{webapp_base_url}/dataset-detail?id=5"  # The original failing URL
        ]
        
        for url in test_urls:
            async with http_session.get(url) as response:
                content = await response.text()
                
                # CRITICAL: Should never return the problematic JSON response
                assert content != '{"detail":"Not Found"}', f"URL {url} should not return JSON error"
                assert '{"detail":"Not Found"}' not in content, f"URL {url} should not contain JSON error"
                
                # Should be HTML pages
                content_type = response.headers.get('content-type', '')
                assert 'text/html' in content_type, f"URL {url} should return HTML"
    
    @pytest.mark.asyncio
    async def test_page_loads_with_real_dataset_content(self, webapp_base_url, http_session):
        """Test that the dataset detail page loads with real content from database"""
        url = f"{webapp_base_url}/dataset-detail?id=5"
        
        async with http_session.get(url) as response:
            assert response.status == 200, "Page should load successfully"
            
            content = await response.text()
            
            # Should contain elements that prove it's working
            assert "dataset-meta" in content, "Should have dataset metadata section"
            assert "sequences-content" in content, "Should have sequences content section"  
            assert "REAL DATABASE" in content, "Should show real database badge"
            assert "FILE ACCESS" in content, "Should show file access badge"
            
            # Should contain JavaScript that loads real data
            assert "fetch(`/api/v1/datasets/${datasetId}`)" in content, "Should fetch real dataset data"
            assert "fetch('/api/v1/training/files')" in content, "Should fetch real training files"
            
            # Most importantly: Should not be the original JSON error
            assert content != '{"detail":"Not Found"}', "Should not be the original JSON error"
            assert '{"detail":"Not Found"}' not in content, "Should not contain JSON error anywhere"
    
    @pytest.mark.asyncio
    async def test_all_supporting_apis_work(self, webapp_base_url, http_session):
        """Test all API endpoints that the dataset detail page depends on"""
        # Test dataset API
        async with http_session.get(f"{webapp_base_url}/api/v1/datasets/5") as response:
            assert response.status == 200, "Dataset API should work"
            data = await response.json()
            assert data['id'] == 5, "Should return dataset 5"
        
        # Test dataset metadata API
        async with http_session.get(f"{webapp_base_url}/api/v1/datasets/5/metadata") as response:
            assert response.status == 200, "Dataset metadata API should work"
            data = await response.json()
            assert 'dataset_name' in data, "Should return metadata"
        
        # Test training files API (used by dataset detail page)
        async with http_session.get(f"{webapp_base_url}/api/v1/training/files") as response:
            assert response.status == 200, "Training files API should work"
            data = await response.json()
            assert 'files' in data, "Should return training files"
            assert data['status'] == 'real_files_accessed', "Should access real files"

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])