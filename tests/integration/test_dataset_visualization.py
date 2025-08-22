#!/usr/bin/env python3
"""
Comprehensive Test Suite for Enhanced Dataset Visualization Features

Tests all aspects of the detailed dataset visualization system:
1. Dataset Detail Dashboard - Comprehensive overview with statistics
2. Feature Distribution Analysis - Interactive histograms and statistical plots  
3. Sample Data Table - Paginated, filterable, sortable sample browsing
4. Individual Sample Visualization - Detailed feature analysis
5. Advanced Filtering and Search - Complex multi-criteria filtering
6. Export and Navigation - Data export and seamless navigation

Follows TDD principles with failing tests first, then implementation.
"""

import pytest
import asyncio
import uuid
import json
import numpy as np
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List
from unittest.mock import AsyncMock, patch, MagicMock

import asyncpg
from fastapi.testclient import TestClient
from httpx import AsyncClient

# Import the enhanced application components
from enhanced_dataset_visualization_platform import (
    EnhancedDatasetVisualizationEngine,
    create_enhanced_dataset_visualization_app,
    Environment,
    DatasetDetailResponse, DistributionsResponse, SamplePage, SampleDetailResponse
)

class TestDatasetDetailDashboard:
    """Test comprehensive dataset detail dashboard functionality."""
    
    @pytest.fixture
    def client(self):
        """Create test client with enhanced features."""
        app = create_enhanced_dataset_visualization_app()
        return TestClient(app)
    
    def test_dataset_details_endpoint_comprehensive(self, client):
        """Test dataset details returns comprehensive information."""
        dataset_id = str(uuid.uuid4())
        
        response = client.get(f"/api/v1/datasets/{dataset_id}/details")
        assert response.status_code == 200
        
        data = response.json()
        
        # Test metadata section
        assert "metadata" in data
        metadata = data["metadata"]
        assert "name" in metadata
        assert "symbols" in metadata
        assert "date_range" in metadata
        assert "total_sequences" in metadata
        assert "feature_count" in metadata
        assert "file_size" in metadata
        
        # Test statistics section
        assert "statistics" in data
        statistics = data["statistics"]
        assert "numerical_features" in statistics
        assert "categorical_features" in statistics
        assert "missing_values" in statistics
        assert "data_quality_score" in statistics
        assert "outlier_percentage" in statistics
        
        # Verify data quality score is reasonable
        assert 0.0 <= statistics["data_quality_score"] <= 1.0
        assert 0.0 <= statistics["outlier_percentage"] <= 100.0
        
        # Test features section
        assert "features" in data
        features = data["features"]
        assert len(features) > 0
        
        for feature in features:
            assert "name" in feature
            assert "type" in feature
            assert "statistics" in feature
            
            feature_stats = feature["statistics"]
            assert "mean" in feature_stats
            assert "std" in feature_stats
            assert "min" in feature_stats
            assert "max" in feature_stats
            assert "quartiles" in feature_stats
            assert len(feature_stats["quartiles"]) == 3  # Q1, Q2, Q3
    
    def test_dataset_overview_summary_cards(self, client):
        """Test dataset overview displays key summary information."""
        dataset_id = str(uuid.uuid4())
        
        response = client.get(f"/api/v1/datasets/{dataset_id}/overview")
        assert response.status_code == 200
        
        data = response.json()
        
        # Test summary cards data
        assert "summary_cards" in data
        cards = data["summary_cards"]
        
        expected_cards = [
            "total_sequences", "feature_count", "data_quality_score",
            "date_coverage", "file_size", "last_updated"
        ]
        
        for card_type in expected_cards:
            assert card_type in cards
            card_data = cards[card_type]
            assert "value" in card_data
            assert "display_text" in card_data
            assert "status" in card_data  # good, warning, error

class TestFeatureDistributions:
    """Test interactive feature distribution analysis."""
    
    @pytest.fixture
    def client(self):
        app = create_enhanced_dataset_visualization_app()
        return TestClient(app)
    
    def test_feature_distributions_endpoint(self, client):
        """Test feature distributions API returns proper histogram data."""
        dataset_id = str(uuid.uuid4())
        
        # Test with specific features and bins
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/distributions",
            params={"features": "open,high,low,close", "bins": 30}
        )
        assert response.status_code == 200
        
        data = response.json()
        
        # Test distributions structure
        assert "distributions" in data
        distributions = data["distributions"]
        
        expected_features = ["open", "high", "low", "close"]
        for feature in expected_features:
            assert feature in distributions
            feature_dist = distributions[feature]
            
            # Test histogram data
            assert "histogram" in feature_dist
            histogram = feature_dist["histogram"]
            assert "bins" in histogram
            assert "counts" in histogram
            assert "density" in histogram
            
            # Verify bin count
            assert len(histogram["bins"]) == 30
            assert len(histogram["counts"]) == 30
            assert len(histogram["density"]) == 30
            
            # Test statistical data
            assert "statistics" in feature_dist
            stats = feature_dist["statistics"]
            assert "mean" in stats
            assert "std" in stats
            assert "skewness" in stats
            assert "kurtosis" in stats
    
    def test_correlation_matrix_calculation(self, client):
        """Test correlation matrix for feature relationships."""
        dataset_id = str(uuid.uuid4())
        
        response = client.get(f"/api/v1/datasets/{dataset_id}/correlations")
        assert response.status_code == 200
        
        data = response.json()
        
        assert "correlations" in data
        correlations = data["correlations"]
        
        assert "matrix" in correlations
        assert "feature_names" in correlations
        
        matrix = correlations["matrix"]
        feature_names = correlations["feature_names"]
        
        # Verify matrix is square and matches feature count
        assert len(matrix) == len(feature_names)
        for row in matrix:
            assert len(row) == len(feature_names)
        
        # Verify diagonal elements are 1.0 (self-correlation)
        for i in range(len(matrix)):
            assert abs(matrix[i][i] - 1.0) < 0.001
    
    def test_distribution_plot_configuration(self, client):
        """Test different plot configurations for distributions."""
        dataset_id = str(uuid.uuid4())
        
        # Test different bin sizes
        for bins in [10, 25, 50, 100]:
            response = client.get(
                f"/api/v1/datasets/{dataset_id}/distributions",
                params={"features": "open", "bins": bins}
            )
            assert response.status_code == 200
            
            data = response.json()
            histogram = data["distributions"]["open"]["histogram"]
            assert len(histogram["bins"]) == bins
        
        # Test outlier inclusion/exclusion
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/distributions",
            params={"features": "open", "exclude_outliers": "true"}
        )
        assert response.status_code == 200

class TestSampleDataTable:
    """Test interactive sample data table with filtering and pagination."""
    
    @pytest.fixture
    def client(self):
        app = create_enhanced_dataset_visualization_app()
        return TestClient(app)
    
    def test_sample_data_pagination(self, client):
        """Test sample data pagination works correctly."""
        dataset_id = str(uuid.uuid4())
        
        # Test first page
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples",
            params={"page": 1, "limit": 50}
        )
        assert response.status_code == 200
        
        data = response.json()
        
        # Test response structure
        assert "samples" in data
        assert "pagination" in data
        assert "aggregations" in data
        
        samples = data["samples"]
        pagination = data["pagination"]
        
        # Test pagination info
        assert pagination["page"] == 1
        assert pagination["limit"] == 50
        assert "total" in pagination
        assert "pages" in pagination
        
        # Test sample structure
        if samples:  # If we have samples
            sample = samples[0]
            assert "index" in sample
            assert "data" in sample
            assert "metadata" in sample
            
            # Test sample data contains expected features
            sample_data = sample["data"]
            expected_features = ["open", "high", "low", "close", "volume"]
            for feature in expected_features:
                assert feature in sample_data
    
    def test_numerical_range_filtering(self, client):
        """Test filtering by numerical feature ranges."""
        dataset_id = str(uuid.uuid4())
        
        # Test single feature range filter
        filter_criteria = {
            "feature_ranges": {
                "open": {"min": 100, "max": 200},
                "volume": {"min": 1000000}
            }
        }
        
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples",
            params={
                "filter": json.dumps(filter_criteria),
                "limit": 100
            }
        )
        assert response.status_code == 200
        
        data = response.json()
        samples = data["samples"]
        
        # Verify all samples meet filter criteria
        for sample in samples:
            sample_data = sample["data"]
            
            # Check open price range
            assert 100 <= sample_data["open"] <= 200
            
            # Check minimum volume
            assert sample_data["volume"] >= 1000000
    
    def test_date_range_filtering(self, client):
        """Test filtering by date ranges."""
        dataset_id = str(uuid.uuid4())
        
        filter_criteria = {
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-06-30"
            }
        }
        
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples",
            params={"filter": json.dumps(filter_criteria)}
        )
        assert response.status_code == 200
        
        data = response.json()
        
        # Verify aggregations show filter impact
        assert "aggregations" in data
        aggregations = data["aggregations"]
        assert "filtered_count" in aggregations
        assert "date_range_applied" in aggregations
    
    def test_quality_score_filtering(self, client):
        """Test filtering by data quality scores."""
        dataset_id = str(uuid.uuid4())
        
        filter_criteria = {
            "quality_threshold": 0.8,
            "exclude_outliers": True
        }
        
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples",
            params={"filter": json.dumps(filter_criteria)}
        )
        assert response.status_code == 200
        
        data = response.json()
        samples = data["samples"]
        
        # Verify quality filtering
        for sample in samples:
            metadata = sample["metadata"]
            assert metadata["quality_score"] >= 0.8
            assert metadata["is_outlier"] == False
    
    def test_multi_column_sorting(self, client):
        """Test sorting by different columns."""
        dataset_id = str(uuid.uuid4())
        
        # Test sorting by different fields
        sort_fields = ["index", "quality_score", "timestamp", "open"]
        
        for sort_field in sort_fields:
            for direction in ["asc", "desc"]:
                response = client.get(
                    f"/api/v1/datasets/{dataset_id}/samples",
                    params={"sort": f"{sort_field}:{direction}"}
                )
                assert response.status_code == 200
                
                data = response.json()
                samples = data["samples"]
                
                # Verify sorting (basic check - first few items should be ordered)
                if len(samples) >= 2:
                    if sort_field == "index":
                        if direction == "asc":
                            assert samples[0]["index"] <= samples[1]["index"]
                        else:
                            assert samples[0]["index"] >= samples[1]["index"]
    
    def test_text_search_functionality(self, client):
        """Test text search across sample data."""
        dataset_id = str(uuid.uuid4())
        
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples",
            params={"search": "AAPL", "limit": 20}
        )
        assert response.status_code == 200
        
        data = response.json()
        
        # Search should return relevant results
        assert "samples" in data
        assert "aggregations" in data
        
        # Check that search was applied
        aggregations = data["aggregations"]
        assert "search_applied" in aggregations
        assert aggregations["search_applied"] == True

class TestIndividualSampleVisualization:
    """Test detailed individual sample visualization."""
    
    @pytest.fixture
    def client(self):
        app = create_enhanced_dataset_visualization_app()
        return TestClient(app)
    
    def test_sample_detail_endpoint(self, client):
        """Test individual sample detail retrieval."""
        dataset_id = str(uuid.uuid4())
        sample_index = 42
        
        response = client.get(f"/api/v1/datasets/{dataset_id}/sample/{sample_index}")
        assert response.status_code == 200
        
        data = response.json()
        
        # Test sample structure
        assert "sample" in data
        sample = data["sample"]
        
        assert "index" in sample
        assert sample["index"] == sample_index
        
        # Test features section
        assert "features" in sample
        features = sample["features"]
        assert "raw_data" in features
        assert "technical_indicators" in features
        assert "derived_features" in features
        
        # Test metadata
        assert "metadata" in sample
        metadata = sample["metadata"]
        assert "timestamp" in metadata
        assert "symbol" in metadata
        assert "sequence_position" in metadata
        
        # Test analysis section
        assert "analysis" in sample
        analysis = sample["analysis"]
        assert "quality_score" in analysis
        assert "anomaly_scores" in analysis
        assert "feature_importance" in analysis
        assert "nearest_neighbors" in analysis
        
        # Test context for navigation
        assert "context" in data
        context = data["context"]
        assert "previous_sample" in context
        assert "next_sample" in context
    
    def test_sample_feature_importance(self, client):
        """Test feature importance calculation for individual samples."""
        dataset_id = str(uuid.uuid4())
        sample_index = 0
        
        response = client.get(f"/api/v1/datasets/{dataset_id}/sample/{sample_index}")
        assert response.status_code == 200
        
        data = response.json()
        analysis = data["sample"]["analysis"]
        
        # Test feature importance scores
        feature_importance = analysis["feature_importance"]
        assert isinstance(feature_importance, dict)
        
        # All importance scores should be between 0 and 1
        for feature, importance in feature_importance.items():
            assert 0.0 <= importance <= 1.0
    
    def test_sample_anomaly_detection(self, client):
        """Test anomaly detection for individual samples."""
        dataset_id = str(uuid.uuid4())
        sample_index = 10
        
        response = client.get(f"/api/v1/datasets/{dataset_id}/sample/{sample_index}")
        assert response.status_code == 200
        
        data = response.json()
        analysis = data["sample"]["analysis"]
        
        # Test anomaly scores
        anomaly_scores = analysis["anomaly_scores"]
        assert isinstance(anomaly_scores, dict)
        
        # Check for expected anomaly types
        expected_anomaly_types = ["isolation_forest", "local_outlier_factor", "statistical"]
        for anomaly_type in expected_anomaly_types:
            if anomaly_type in anomaly_scores:
                score = anomaly_scores[anomaly_type]
                assert isinstance(score, (int, float))
    
    def test_nearest_neighbors_calculation(self, client):
        """Test nearest neighbor calculation for sample similarity."""
        dataset_id = str(uuid.uuid4())
        sample_index = 5
        
        response = client.get(f"/api/v1/datasets/{dataset_id}/sample/{sample_index}")
        assert response.status_code == 200
        
        data = response.json()
        analysis = data["sample"]["analysis"]
        
        # Test nearest neighbors
        nearest_neighbors = analysis["nearest_neighbors"]
        assert isinstance(nearest_neighbors, list)
        assert len(nearest_neighbors) <= 10  # Should return reasonable number
        
        # All neighbor indices should be valid
        for neighbor_idx in nearest_neighbors:
            assert isinstance(neighbor_idx, int)
            assert neighbor_idx >= 0

class TestAdvancedFilteringAndSearch:
    """Test advanced filtering capabilities."""
    
    @pytest.fixture
    def client(self):
        app = create_enhanced_dataset_visualization_app()
        return TestClient(app)
    
    def test_complex_multi_criteria_filtering(self, client):
        """Test complex filtering with multiple criteria."""
        dataset_id = str(uuid.uuid4())
        
        # Complex filter with multiple conditions
        complex_filter = {
            "feature_ranges": {
                "open": {"min": 50, "max": 300},
                "volume": {"min": 500000, "max": 10000000},
                "close": {"min": 45}
            },
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-08-21"
            },
            "symbols": ["AAPL", "TSLA"],
            "quality_threshold": 0.7,
            "exclude_outliers": True,
            "technical_indicators": {
                "etop": {"min": -0.5, "max": 0.5},
                "ebot": {"max": 0.3}
            }
        }
        
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples",
            params={
                "filter": json.dumps(complex_filter),
                "limit": 100
            }
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "samples" in data
        assert "aggregations" in data
        
        # Verify filter impact is reported
        aggregations = data["aggregations"]
        assert "filters_applied" in aggregations
        assert len(aggregations["filters_applied"]) > 0
    
    def test_filter_combination_logic(self, client):
        """Test AND/OR logic in filter combinations."""
        dataset_id = str(uuid.uuid4())
        
        # Test AND logic (default)
        and_filter = {
            "logic": "AND",
            "conditions": [
                {"feature": "open", "operator": ">", "value": 100},
                {"feature": "volume", "operator": ">", "value": 1000000}
            ]
        }
        
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples/advanced-filter",
            json=and_filter
        )
        assert response.status_code == 200
        
        # Test OR logic
        or_filter = {
            "logic": "OR",
            "conditions": [
                {"feature": "open", "operator": "<", "value": 50},
                {"feature": "open", "operator": ">", "value": 500}
            ]
        }
        
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples/advanced-filter",
            json=or_filter
        )
        assert response.status_code == 200
    
    def test_saved_filters_functionality(self, client):
        """Test saving and loading filter configurations."""
        dataset_id = str(uuid.uuid4())
        
        # Save a filter configuration
        filter_config = {
            "name": "High Volume AAPL",
            "description": "AAPL stocks with high volume",
            "filter": {
                "symbols": ["AAPL"],
                "feature_ranges": {"volume": {"min": 5000000}}
            }
        }
        
        response = client.post(
            f"/api/v1/datasets/{dataset_id}/saved-filters",
            json=filter_config
        )
        assert response.status_code == 200
        
        saved_filter = response.json()
        assert "filter_id" in saved_filter
        filter_id = saved_filter["filter_id"]
        
        # Load saved filter
        response = client.get(f"/api/v1/datasets/{dataset_id}/saved-filters/{filter_id}")
        assert response.status_code == 200
        
        loaded_filter = response.json()
        assert loaded_filter["name"] == filter_config["name"]
        assert loaded_filter["filter"] == filter_config["filter"]
        
        # Apply saved filter
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples",
            params={"saved_filter_id": filter_id}
        )
        assert response.status_code == 200

class TestDataExportFunctionality:
    """Test data export capabilities."""
    
    @pytest.fixture
    def client(self):
        app = create_enhanced_dataset_visualization_app()
        return TestClient(app)
    
    def test_csv_export(self, client):
        """Test CSV export of filtered sample data."""
        dataset_id = str(uuid.uuid4())
        
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/export",
            params={
                "format": "csv",
                "limit": 100,
                "features": "open,high,low,close,volume"
            }
        )
        assert response.status_code == 200
        assert response.headers["content-type"] == "text/csv"
        
        # Basic CSV validation
        csv_content = response.content.decode("utf-8")
        lines = csv_content.strip().split("\n")
        assert len(lines) > 1  # Header + data rows
        
        # Check header contains expected features
        header = lines[0]
        assert "open" in header
        assert "high" in header
        assert "close" in header
    
    def test_json_export(self, client):
        """Test JSON export of sample data."""
        dataset_id = str(uuid.uuid4())
        
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/export",
            params={"format": "json", "limit": 50}
        )
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"
        
        data = response.json()
        assert "samples" in data
        assert "metadata" in data
        assert "export_timestamp" in data
    
    def test_distribution_plot_export(self, client):
        """Test export of distribution plots as images."""
        dataset_id = str(uuid.uuid4())
        
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/distributions/export",
            params={
                "features": "open,close",
                "format": "png",
                "width": 800,
                "height": 600
            }
        )
        assert response.status_code == 200
        assert response.headers["content-type"] == "image/png"
        
        # Basic PNG validation
        png_content = response.content
        assert png_content.startswith(b'\x89PNG\r\n\x1a\n')  # PNG signature

class TestPerformanceAndScalability:
    """Test performance and scalability aspects."""
    
    @pytest.fixture
    def client(self):
        app = create_enhanced_dataset_visualization_app()
        return TestClient(app)
    
    def test_large_dataset_handling(self, client):
        """Test handling of large datasets with many samples."""
        dataset_id = str(uuid.uuid4())
        
        # Test with large limit
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples",
            params={"limit": 1000}
        )
        assert response.status_code == 200
        
        data = response.json()
        samples = data["samples"]
        
        # Should handle large requests efficiently
        assert len(samples) <= 1000  # Respect limit
    
    def test_response_time_requirements(self, client):
        """Test that responses meet performance requirements."""
        import time
        
        dataset_id = str(uuid.uuid4())
        
        # Test dataset details endpoint
        start_time = time.time()
        response = client.get(f"/api/v1/datasets/{dataset_id}/details")
        details_time = time.time() - start_time
        
        assert response.status_code == 200
        assert details_time < 3.0  # Should respond within 3 seconds
        
        # Test sample data endpoint
        start_time = time.time()
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples",
            params={"limit": 100}
        )
        samples_time = time.time() - start_time
        
        assert response.status_code == 200
        assert samples_time < 2.0  # Should respond within 2 seconds
        
        # Test individual sample endpoint
        start_time = time.time()
        response = client.get(f"/api/v1/datasets/{dataset_id}/sample/0")
        sample_time = time.time() - start_time
        
        assert response.status_code == 200
        assert sample_time < 1.0  # Should respond within 1 second
    
    def test_concurrent_requests_handling(self, client):
        """Test handling of concurrent requests."""
        import threading
        
        dataset_id = str(uuid.uuid4())
        results = []
        
        def make_request():
            response = client.get(f"/api/v1/datasets/{dataset_id}/samples")
            results.append(response.status_code)
        
        # Launch 10 concurrent requests
        threads = []
        for i in range(10):
            thread = threading.Thread(target=make_request)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # All requests should succeed
        assert all(status_code == 200 for status_code in results)
        assert len(results) == 10

class TestErrorHandlingAndValidation:
    """Test error handling and input validation."""
    
    @pytest.fixture
    def client(self):
        app = create_enhanced_dataset_visualization_app()
        return TestClient(app)
    
    def test_invalid_dataset_id_handling(self, client):
        """Test handling of invalid dataset IDs."""
        invalid_id = "nonexistent_dataset"
        
        response = client.get(f"/api/v1/datasets/{invalid_id}/details")
        # In demo mode, should return demo data or proper error
        assert response.status_code in [200, 404]
    
    def test_invalid_filter_parameters(self, client):
        """Test validation of filter parameters."""
        dataset_id = str(uuid.uuid4())
        
        # Test invalid filter JSON
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples",
            params={"filter": "invalid_json"}
        )
        assert response.status_code in [200, 400]  # Demo mode may return 200
        
        # Test invalid feature ranges
        invalid_filter = {
            "feature_ranges": {
                "open": {"min": "not_a_number", "max": 200}
            }
        }
        
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples",
            params={"filter": json.dumps(invalid_filter)}
        )
        # Should handle gracefully
        assert response.status_code in [200, 400]
    
    def test_pagination_boundary_conditions(self, client):
        """Test pagination with boundary conditions."""
        dataset_id = str(uuid.uuid4())
        
        # Test zero limit
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples",
            params={"limit": 0}
        )
        assert response.status_code == 200
        
        # Test negative page
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples",
            params={"page": -1}
        )
        assert response.status_code in [200, 400]
        
        # Test very large page number
        response = client.get(
            f"/api/v1/datasets/{dataset_id}/samples",
            params={"page": 99999}
        )
        assert response.status_code == 200

# ===== Manual Testing Instructions =====

def print_dataset_visualization_test_instructions():
    """Print instructions for manual testing of dataset visualization features."""
    
    instructions = """
    🔍 DATASET VISUALIZATION MANUAL TESTING INSTRUCTIONS
    
    After running automated tests, perform these manual verifications:
    
    1. 🚀 START ENHANCED APPLICATION:
       python enhanced_analytics_platform.py
       
    2. 📊 VERIFY DATASET DETAIL DASHBOARD:
       - Navigate to http://localhost:9000/datasets/{dataset_id}/details
       - Check comprehensive overview with summary cards
       - Verify feature statistics and quality metrics display
       - Test responsive design on different screen sizes
       
    3. 📈 TEST FEATURE DISTRIBUTIONS:
       - Click "Distributions" tab in dataset detail view
       - Verify interactive histograms for all features
       - Test bin size adjustment controls
       - Check correlation heatmap functionality
       - Test feature selection dropdown
       
    4. 📋 TEST SAMPLE DATA TABLE:
       - Click "Samples" tab in dataset detail view
       - Verify paginated table with sortable columns
       - Test filtering by feature ranges
       - Check date range filtering
       - Test text search functionality
       - Verify row selection and highlighting
       
    5. 🔍 TEST INDIVIDUAL SAMPLE VISUALIZATION:
       - Click on any row in the sample table
       - Verify detailed sample information modal
       - Check feature importance visualization
       - Test anomaly detection indicators
       - Verify navigation to previous/next samples
       
    6. 🎛️ TEST ADVANCED FILTERING:
       - Test complex multi-criteria filtering
       - Check filter combination logic (AND/OR)
       - Test saved filter functionality
       - Verify filter state preservation
       
    7. 📤 TEST EXPORT FUNCTIONALITY:
       - Test CSV export of filtered data
       - Test JSON export with metadata
       - Test plot export as PNG/SVG
       - Verify downloaded file integrity
       
    8. 🔗 TEST NAVIGATION AND INTEGRATION:
       - Test navigation from dataset catalog to detail view
       - Verify back button functionality
       - Test deep linking to specific views
       - Check integration with existing job management
       
    9. 📱 TEST MOBILE RESPONSIVENESS:
       - Test on mobile device or browser dev tools
       - Verify collapsible sidebar functionality
       - Check swipeable tabs on mobile
       - Test touch interactions for plots
       
    10. ⚡ TEST PERFORMANCE:
        - Load dataset with 10k+ samples
        - Test filtering response time
        - Check plot rendering speed
        - Verify memory usage stays reasonable
    
    ✅ SUCCESS CRITERIA:
    - Dataset detail dashboard loads comprehensive information
    - Feature distributions display interactive histograms
    - Sample table supports filtering, sorting, pagination
    - Individual samples show detailed analysis
    - Advanced filtering works with complex criteria
    - Export functionality produces correct files
    - All features work responsively across devices
    - Performance meets specified requirements
    
    🚨 KNOWN ISSUES TO VERIFY:
    - Large datasets (>100k samples) should use pagination
    - Plot rendering should not freeze browser
    - Filter state should persist across page refreshes
    - Export should respect applied filters
    """
    
    print(instructions)

if __name__ == "__main__":
    print_dataset_visualization_test_instructions()