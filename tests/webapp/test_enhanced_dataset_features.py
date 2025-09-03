#!/usr/bin/env python3
"""
Test Suite: Enhanced Dataset Detail Features
Tests the new enhanced dataset detail page with filterable tables, OHLC charts, and feature distributions
"""

import pytest
import asyncio
import aiohttp
import json

class TestEnhancedDatasetFeatures:
    """Test enhanced dataset detail page functionality"""
    
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
    async def test_enhanced_dataset_detail_page_accessibility(self, webapp_base_url, http_session):
        """Test that enhanced dataset detail page is accessible and contains new features"""
        async with http_session.get(f"{webapp_base_url}/dataset-detail?id=5") as response:
            assert response.status == 200, f"Enhanced dataset detail page should be accessible, got {response.status}"
            
            content_type = response.headers.get('content-type', '')
            assert 'text/html' in content_type, f"Should return HTML content, got {content_type}"
            
            html_content = await response.text()
            
            # Verify enhanced page elements
            assert "Enhanced Dataset Detail" in html_content, "Should contain enhanced page title"
            assert "ENHANCED ANALYSIS" in html_content, "Should show enhanced analysis badge"
            assert "FILTERABLE TABLES" in html_content, "Should show filterable tables badge"
            assert "OHLC CHARTS" in html_content, "Should show OHLC charts badge"
            
            # Verify data table functionality
            assert "data-table-container" in html_content, "Should contain data table container"
            assert "filterInput" in html_content, "Should contain filter input"
            assert "sortSelect" in html_content, "Should contain sort select"
            assert "createMiniOHLCChart" in html_content, "Should contain OHLC chart function"
            
            # Verify feature distribution charts
            assert "Feature Distribution Analysis" in html_content, "Should contain feature distribution section"
            assert "Close Price Distribution" in html_content, "Should contain close price distribution"
            assert "ETOP Distribution" in html_content, "Should contain ETOP distribution"
            assert "EBOT Distribution" in html_content, "Should contain EBOT distribution"
            assert "Volume Distribution" in html_content, "Should contain volume distribution"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_enhanced_dataset_api_endpoint(self, webapp_base_url, http_session):
        """Test the enhanced dataset API that provides sequence data"""
        async with http_session.get(f"{webapp_base_url}/api/v1/datasets/5/enhanced-data") as response:
            assert response.status == 200, f"Enhanced dataset API should work, got {response.status}"
            
            data = await response.json()
            
            # Verify enhanced data structure
            assert 'sequences' in data, "Should contain sequences data"
            assert 'total_sequences' in data, "Should contain total sequences count"
            assert 'note' in data, "Should contain note about enhanced data"
            
            # Verify sequence data contains all required fields
            if data['sequences']:
                first_sequence = data['sequences'][0]
                required_fields = ['sequence_id', 'date', 'open', 'high', 'low', 'close', 'volume', 'etop', 'ebot', 'pldot', 'oneonedot']
                for field in required_fields:
                    assert field in first_sequence, f"Sequence should have field '{field}'"
                
                # Verify data types
                assert isinstance(first_sequence['sequence_id'], int), "Sequence ID should be integer"
                assert isinstance(first_sequence['open'], (int, float)), "Open should be numeric"
                assert isinstance(first_sequence['high'], (int, float)), "High should be numeric"
                assert isinstance(first_sequence['low'], (int, float)), "Low should be numeric"
                assert isinstance(first_sequence['close'], (int, float)), "Close should be numeric"
                assert isinstance(first_sequence['volume'], int), "Volume should be integer"
                
                # Verify technical indicators are present (even if 0)
                assert 'etop' in first_sequence, "Should have ETOP indicator"
                assert 'ebot' in first_sequence, "Should have EBOT indicator"
                assert 'pldot' in first_sequence, "Should have PLDOT indicator"
                assert 'oneonedot' in first_sequence, "Should have ONEONEDOT indicator"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_enhanced_data_contains_real_indicators(self, webapp_base_url, http_session):
        """Test that the enhanced API returns real technical indicator values"""
        async with http_session.get(f"{webapp_base_url}/api/v1/datasets/5/enhanced-data") as response:
            assert response.status == 200, "Enhanced dataset API should work"
            
            data = await response.json()
            sequences = data['sequences']
            
            # Find sequences with non-zero indicators
            etop_values = [seq['etop'] for seq in sequences if seq['etop'] != 0]
            ebot_values = [seq['ebot'] for seq in sequences if seq['ebot'] != 0]
            pldot_values = [seq['pldot'] for seq in sequences if seq['pldot'] != 0]
            
            assert len(etop_values) > 0, "Should have some non-zero ETOP values"
            assert len(ebot_values) > 0, "Should have some non-zero EBOT values"
            assert len(pldot_values) > 0, "Should have some non-zero PLDOT values"
            
            # Verify indicator values are reasonable
            assert all(etop > 0 for etop in etop_values), "ETOP values should be positive when non-zero"
            assert all(ebot > 0 for ebot in ebot_values), "EBOT values should be positive when non-zero"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_chart_js_library_inclusion(self, webapp_base_url, http_session):
        """Test that Chart.js library is properly included for visualizations"""
        async with http_session.get(f"{webapp_base_url}/dataset-detail?id=5") as response:
            assert response.status == 200, "Dataset detail page should be accessible"
            
            html_content = await response.text()
            
            # Verify Chart.js library inclusion
            assert "chart.js" in html_content, "Should include Chart.js library"
            assert "chartjs-adapter-date-fns" in html_content, "Should include Chart.js date adapter"
            
            # Verify chart creation functions
            assert "createHistogram" in html_content, "Should contain histogram creation function"
            assert "createMiniOHLCChart" in html_content, "Should contain mini OHLC chart function"
            assert "createDistributionCharts" in html_content, "Should contain distribution charts function"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_filtering_and_sorting_functionality(self, webapp_base_url, http_session):
        """Test that the filtering and sorting functionality is present"""
        async with http_session.get(f"{webapp_base_url}/dataset-detail?id=5") as response:
            assert response.status == 200, "Dataset detail page should be accessible"
            
            html_content = await response.text()
            
            # Verify filtering functionality
            assert "filterTable()" in html_content, "Should contain filter table function"
            assert "filter-input" in html_content, "Should contain filter input class"
            assert "onkeyup=\"filterTable()\"" in html_content, "Should have filter event handler"
            
            # Verify sorting functionality
            assert "sortTable()" in html_content, "Should contain sort table function"
            assert "sortColumn(" in html_content, "Should contain sort column function"
            assert "sort-select" in html_content, "Should contain sort select class"
            
            # Verify table interaction
            assert "onclick=\"sortColumn(" in html_content, "Should have clickable column headers"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_enhanced_page_does_not_return_404(self, webapp_base_url, http_session):
        """Regression test: Ensure enhanced dataset detail page doesn't return 404"""
        test_ids = [1, 2, 3, 4, 5]
        
        for dataset_id in test_ids:
            async with http_session.get(f"{webapp_base_url}/dataset-detail?id={dataset_id}") as response:
                assert response.status != 404, f"Dataset detail page should not return 404 for ID {dataset_id}"
                assert response.status == 200, f"Dataset detail page should return 200 for ID {dataset_id}"
                
                # Verify it's actually HTML, not JSON error
                content_type = response.headers.get('content-type', '')
                assert 'text/html' in content_type, f"Should return HTML, not JSON error for ID {dataset_id}"
                
                content = await response.text()
                assert content != '{"detail":"Not Found"}', f"Should not return JSON error for ID {dataset_id}"

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])