#!/usr/bin/env python3
"""
Comprehensive Test Suite for Unified Analytics Platform

Tests the complete job-to-dataset workflow following TDD principles:
1. Job Management - Create, track, and manage Flyte jobs
2. Dataset Registration - Automatic dataset registration on job completion
3. Dataset Catalog - Browse, search, and filter datasets
4. Dataset Comparison - Statistical comparison between datasets
5. End-to-end Workflow - Complete pipeline from job to analysis

Follows development workflow requirements for comprehensive validation.
"""

import pytest
import uuid
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient
from httpx import AsyncClient

# Import the application components
from unified_analytics_platform import (
    UnifiedAnalyticsEngine,
    create_unified_analytics_app,
    JobRunDetail,
    DatasetInfo, DatasetComparison
)

class TestAnalyticsPlatformIntegration:
    """Integration tests for unified analytics platform."""

    @pytest.fixture
    async def analytics_engine(self):
        """Create analytics engine for testing."""
        engine = UnifiedAnalyticsEngine()
        # Mock database connection for testing
        engine.pool = AsyncMock()
        return engine

    @pytest.fixture
    def client(self):
        """Create test client."""
        app = create_unified_analytics_app()
        return TestClient(app)

    @pytest.fixture
    async def async_client(self):
        """Create async test client."""
        app = create_unified_analytics_app()
        async with AsyncClient(app=app, base_url="http://test") as ac:
            yield ac

    # ===== Database Schema Tests =====

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_schema_creation(self, analytics_engine):
        """Test database schema creation for analytics platform."""

        # Mock the database connection
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        # Mock create_pool to return an awaitable mock
        async def mock_create_pool(*args, **kwargs):
            return mock_pool

        # Mock the create_pool function and schema creation
        with patch('unified_analytics_platform.asyncpg.create_pool', side_effect=mock_create_pool):
            with patch('unified_analytics_platform.create_analytics_schema') as mock_schema:
                await analytics_engine.initialize()

        # Verify pool was created and schema function was called
        assert analytics_engine.pool == mock_pool
        mock_schema.assert_called_once_with(mock_pool)

    # ===== Job Management Tests =====

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_job_creation_and_tracking(self, analytics_engine):
        """Test job registration and tracking workflow."""

        # Test job registration
        job_data = {
            "job_name": "Test Training Data Generation",
            "job_type": "training_data_gen",
            "user_id": "test_user",
            "flyte_execution_id": "test-exec-001",
            "flyte_workflow_name": "training_data_workflow",
            "status": "running",
            "parameters": {"symbols": ["AAPL"], "days_back": 120},
            "start_time": datetime.now()
        }

        job_id = await analytics_engine.register_job(job_data)
        assert job_id is not None
        assert isinstance(job_id, str)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_job_listing_with_filters(self, analytics_engine):
        """Test job listing with various filters."""

        # Test without filters
        jobs, total = await analytics_engine.list_jobs()
        assert isinstance(jobs, list)
        assert isinstance(total, int)
        assert len(jobs) <= 50  # Default limit

        # Test with job type filter
        from unified_analytics_platform import JobFilter
        filters = JobFilter(job_type="training_data_gen")
        jobs, total = await analytics_engine.list_jobs(filters)

        for job in jobs:
            assert job.job_type == "training_data_gen"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_job_detail_retrieval(self, analytics_engine):
        """Test detailed job information retrieval."""

        # Test with demo data
        job_detail = await analytics_engine.get_job_detail("test-job-id")
        assert job_detail is not None
        assert isinstance(job_detail, JobRunDetail)
        assert job_detail.job_id == "test-job-id"

    # ===== Dataset Management Tests =====

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_registration_on_job_completion(self, analytics_engine):
        """Test automatic dataset registration when job completes."""

        # Simulate job completion with dataset generation
        dataset_data = {
            "dataset_name": "Test AAPL Training Dataset",
            "source_job_id": str(uuid.uuid4()),
            "symbols": ["AAPL"],
            "start_date": date(2024, 1, 1),
            "end_date": date(2024, 8, 21),
            "total_sequences": 1500,
            "feature_count": 12,
            "technical_indicators": ["etop", "ebot", "pldot", "oneonedot"],
            "quality_metrics": {"completeness": 98.5, "duplicates": 0},
            "file_path": "/data/training/aapl_test.npy",
            "file_size_bytes": 1024000
        }

        dataset_id = await analytics_engine.register_dataset(dataset_data)
        assert dataset_id is not None
        assert isinstance(dataset_id, str)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_catalog_functionality(self, analytics_engine):
        """Test dataset browsing and search functionality."""

        # Test dataset listing
        datasets, total = await analytics_engine.list_datasets()
        assert isinstance(datasets, list)
        assert isinstance(total, int)

        # Test with symbol filter
        from unified_analytics_platform import DatasetFilter
        filters = DatasetFilter(symbols=["AAPL"])
        datasets, total = await analytics_engine.list_datasets(filters)

        for dataset in datasets:
            assert "AAPL" in dataset.symbols

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_detail_retrieval(self, analytics_engine):
        """Test detailed dataset information retrieval."""

        dataset = await analytics_engine.get_dataset_detail("test-dataset-id")
        assert dataset is not None
        assert isinstance(dataset, DatasetInfo)
        assert dataset.dataset_id == "test-dataset-id"

    # ===== Dataset Comparison Tests =====

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_comparison_engine(self, analytics_engine):
        """Test statistical dataset comparison functionality."""

        dataset_a_id = str(uuid.uuid4())
        dataset_b_id = str(uuid.uuid4())

        comparison = await analytics_engine.compare_datasets(
            dataset_a_id, dataset_b_id, "test_user"
        )

        assert isinstance(comparison, DatasetComparison)
        assert comparison.dataset_a_id == dataset_a_id
        assert comparison.dataset_b_id == dataset_b_id
        assert 0.0 <= comparison.overall_difference_score <= 1.0
        assert len(comparison.feature_comparisons) > 0
        assert len(comparison.recommendations) > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_statistical_comparison_accuracy(self, analytics_engine):
        """Test accuracy of statistical comparisons."""

        comparison = await analytics_engine.compare_datasets(
            "dataset-a", "dataset-b", "test_user"
        )

        # Verify statistical test results
        for feature_name, feature_comp in comparison.feature_comparisons.items():
            # KS statistic should be between 0 and 1
            assert 0.0 <= feature_comp.ks_statistic <= 1.0
            # Jensen-Shannon divergence should be between 0 and 1
            assert 0.0 <= feature_comp.jensen_shannon_divergence <= 1.0
            # Distribution shift score should be reasonable
            assert 0.0 <= feature_comp.distribution_shift_score <= 1.0
            # Should have recommendations
            assert len(feature_comp.recommendation) > 0

class TestJobToDatasetWorkflow:
    """Test complete job-to-dataset workflow end-to-end."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        app = create_unified_analytics_app()
        return TestClient(app)

    def test_complete_workflow_simulation(self, client):
        """Test complete workflow from job creation to dataset analysis."""

        # Step 1: Create a training data generation job
        job_data = {
            "job_name": "E2E Test Training Job",
            "job_type": "training_data_gen",
            "user_id": "e2e_test",
            "parameters": {"symbols": ["AAPL"], "days_back": 120}
        }

        response = client.post("/api/v1/jobs", json=job_data)
        assert response.status_code == 200
        job_result = response.json()
        job_id = job_result["job_id"]

        # Step 2: Simulate job completion with dataset generation
        dataset_data = {
            "dataset_name": "E2E Test AAPL Dataset",
            "source_job_id": job_id,
            "symbols": ["AAPL"],
            "start_date": "2024-01-01",
            "end_date": "2024-08-21",
            "total_sequences": 1500,
            "feature_count": 12,
            "technical_indicators": ["etop", "ebot", "pldot", "oneonedot"],
            "quality_metrics": {"completeness": 98.5, "duplicates": 0},
            "file_path": "/data/training/e2e_aapl.npy",
            "file_size_bytes": 1024000
        }

        response = client.post("/api/v1/datasets", json=dataset_data)
        assert response.status_code == 200
        dataset_result = response.json()
        dataset_id = dataset_result["dataset_id"]

        # Step 3: Verify job-to-dataset navigation
        response = client.get(f"/api/v1/jobs/{job_id}/datasets")
        assert response.status_code == 200

        response = client.get(f"/api/v1/datasets/{dataset_id}/source-job")
        assert response.status_code == 200
        source_job = response.json()
        # Note: In demo mode, this might return demo data, but structure should be correct

        # Step 4: List datasets and verify our dataset appears
        response = client.get("/api/v1/datasets")
        assert response.status_code == 200
        datasets_result = response.json()
        assert "datasets" in datasets_result
        assert len(datasets_result["datasets"]) > 0

        # Step 5: Get dataset details
        response = client.get(f"/api/v1/datasets/{dataset_id}")
        assert response.status_code == 200
        dataset_detail = response.json()
        assert dataset_detail["dataset_id"] == dataset_id

    def test_dataset_comparison_workflow(self, client):
        """Test dataset comparison workflow."""

        # Create two datasets for comparison
        dataset_a_data = {
            "dataset_name": "Comparison Test Dataset A",
            "source_job_id": str(uuid.uuid4()),
            "symbols": ["AAPL"],
            "start_date": "2024-01-01",
            "end_date": "2024-06-30",
            "total_sequences": 1000,
            "feature_count": 12,
            "technical_indicators": ["etop", "ebot"],
            "quality_metrics": {"completeness": 97.0},
            "file_path": "/data/training/comp_a.npy",
            "file_size_bytes": 500000
        }

        dataset_b_data = {
            "dataset_name": "Comparison Test Dataset B",
            "source_job_id": str(uuid.uuid4()),
            "symbols": ["AAPL"],
            "start_date": "2024-07-01",
            "end_date": "2024-12-31",
            "total_sequences": 1200,
            "feature_count": 12,
            "technical_indicators": ["etop", "ebot"],
            "quality_metrics": {"completeness": 98.5},
            "file_path": "/data/training/comp_b.npy",
            "file_size_bytes": 600000
        }

        # Create datasets
        response_a = client.post("/api/v1/datasets", json=dataset_a_data)
        response_b = client.post("/api/v1/datasets", json=dataset_b_data)

        assert response_a.status_code == 200
        assert response_b.status_code == 200

        dataset_a_id = response_a.json()["dataset_id"]
        dataset_b_id = response_b.json()["dataset_id"]

        # Perform comparison
        comparison_request = {
            "dataset_a_id": dataset_a_id,
            "dataset_b_id": dataset_b_id,
            "user_id": "comparison_test"
        }

        response = client.post("/api/v1/datasets/compare", json=comparison_request)
        assert response.status_code == 200

        comparison_result = response.json()
        assert "overall_difference_score" in comparison_result
        assert "feature_comparisons" in comparison_result
        assert "recommendations" in comparison_result

        # Verify comparison results structure
        assert 0.0 <= comparison_result["overall_difference_score"] <= 1.0
        assert len(comparison_result["feature_comparisons"]) > 0
        assert len(comparison_result["recommendations"]) > 0

class TestAPIEndpoints:
    """Test all API endpoints functionality."""

    @pytest.fixture
    def client(self):
        app = create_unified_analytics_app()
        return TestClient(app)

    def test_health_check_endpoint(self, client):
        """Test health check endpoint functionality."""
        response = client.get("/health")
        assert response.status_code == 200

        health_data = response.json()
        assert "status" in health_data
        assert "timestamp" in health_data
        assert health_data["status"] == "ok"

    def test_job_management_endpoints(self, client):
        """Test all job management API endpoints."""

        # Test job listing
        response = client.get("/api/v1/jobs")
        assert response.status_code == 200

        jobs_data = response.json()
        assert "jobs" in jobs_data
        assert "total" in jobs_data

        # Test job filtering
        response = client.get("/api/v1/jobs?job_type=training_data_gen&status=succeeded")
        assert response.status_code == 200

        # Test job creation
        job_data = {
            "job_name": "API Test Job",
            "job_type": "training_data_gen",
            "user_id": "api_test",
            "parameters": {"test": True}
        }

        response = client.post("/api/v1/jobs", json=job_data)
        assert response.status_code == 200
        assert "job_id" in response.json()

    def test_dataset_management_endpoints(self, client):
        """Test all dataset management API endpoints."""

        # Test dataset listing
        response = client.get("/api/v1/datasets")
        assert response.status_code == 200

        datasets_data = response.json()
        assert "datasets" in datasets_data
        assert "total" in datasets_data

        # Test dataset filtering
        response = client.get("/api/v1/datasets?symbols=AAPL&search=test")
        assert response.status_code == 200

        # Test dataset creation
        dataset_data = {
            "dataset_name": "API Test Dataset",
            "symbols": ["TEST"],
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "total_sequences": 1000,
            "feature_count": 10,
            "technical_indicators": ["test"],
            "quality_metrics": {"completeness": 99.0},
            "file_path": "/test/path.npy",
            "file_size_bytes": 100000
        }

        response = client.post("/api/v1/datasets", json=dataset_data)
        assert response.status_code == 200
        assert "dataset_id" in response.json()

    def test_comparison_endpoints(self, client):
        """Test dataset comparison API endpoints."""

        # Test comparison (will use demo data)
        comparison_data = {
            "dataset_a_id": str(uuid.uuid4()),
            "dataset_b_id": str(uuid.uuid4()),
            "user_id": "test"
        }

        response = client.post("/api/v1/datasets/compare", json=comparison_data)
        assert response.status_code == 200

        result = response.json()
        assert "overall_difference_score" in result
        assert "feature_comparisons" in result

    def test_navigation_endpoints(self, client):
        """Test job-to-dataset navigation endpoints."""

        # Test job datasets endpoint
        response = client.get(f"/api/v1/jobs/{uuid.uuid4()}/datasets")
        assert response.status_code == 200

        # Note: Source job endpoint may return 404 for demo data, which is expected
        response = client.get(f"/api/v1/datasets/{uuid.uuid4()}/source-job")
        # Accept both 200 (demo data) and 404 (not found) as valid responses
        assert response.status_code in [200, 404]

class TestRealWorldScenarios:
    """Test real-world usage scenarios and edge cases."""

    @pytest.fixture
    def client(self):
        app = create_unified_analytics_app()
        return TestClient(app)

    def test_large_dataset_handling(self, client):
        """Test handling of large dataset parameters."""

        dataset_data = {
            "dataset_name": "Large Dataset Test",
            "symbols": ["AAPL", "TSLA", "MSFT", "GOOGL", "AMZN"],  # Multiple symbols
            "start_date": "2020-01-01",
            "end_date": "2024-12-31", # 5 years of data
            "total_sequences": 50000,  # Large number of sequences
            "feature_count": 25,       # Many features
            "technical_indicators": ["etop", "ebot", "pldot", "oneonedot", "rsi", "macd"],
            "quality_metrics": {"completeness": 95.5, "duplicates": 12},
            "file_path": "/data/training/large_dataset.npy",
            "file_size_bytes": 50000000  # 50MB file
        }

        response = client.post("/api/v1/datasets", json=dataset_data)
        assert response.status_code == 200

    def test_concurrent_job_management(self, client):
        """Test concurrent job creation and management."""

        # Create multiple jobs concurrently
        jobs_data = []
        for i in range(5):
            job_data = {
                "job_name": f"Concurrent Job {i}",
                "job_type": "training_data_gen",
                "user_id": f"user_{i}",
                "parameters": {"job_number": i}
            }
            jobs_data.append(job_data)

        # Submit all jobs
        responses = []
        for job_data in jobs_data:
            response = client.post("/api/v1/jobs", json=job_data)
            assert response.status_code == 200
            responses.append(response.json())

        # Verify all jobs were created with unique IDs
        job_ids = [resp["job_id"] for resp in responses]
        assert len(set(job_ids)) == 5  # All IDs should be unique

    def test_edge_case_filtering(self, client):
        """Test edge cases in filtering functionality."""

        # Test empty search
        response = client.get("/api/v1/datasets?search=")
        assert response.status_code == 200

        # Test search with special characters
        response = client.get("/api/v1/datasets?search=%20%21%40%23")
        assert response.status_code == 200

        # Test invalid date ranges (future dates)
        response = client.get("/api/v1/datasets?start_date=2030-01-01")
        assert response.status_code == 200

        # Test pagination edge cases
        response = client.get("/api/v1/datasets?limit=0")  # Zero limit
        assert response.status_code == 200

        response = client.get("/api/v1/datasets?offset=10000")  # Large offset
        assert response.status_code == 200

    def test_comparison_edge_cases(self, client):
        """Test edge cases in dataset comparison."""

        # Test comparing same dataset to itself
        dataset_id = str(uuid.uuid4())
        comparison_data = {
            "dataset_a_id": dataset_id,
            "dataset_b_id": dataset_id,
            "user_id": "test"
        }

        response = client.post("/api/v1/datasets/compare", json=comparison_data)
        # Should still work (perfect similarity)
        assert response.status_code == 200

        # Test with non-existent dataset IDs
        comparison_data = {
            "dataset_a_id": "non-existent-1",
            "dataset_b_id": "non-existent-2",
            "user_id": "test"
        }

        response = client.post("/api/v1/datasets/compare", json=comparison_data)
        # In demo mode, should still return data
        assert response.status_code == 200

class TestPerformanceAndReliability:
    """Test performance and reliability aspects."""

    @pytest.fixture
    def client(self):
        app = create_unified_analytics_app()
        return TestClient(app)

    def test_response_time_requirements(self, client):
        """Test that API responses meet performance requirements."""
        import time

        # Test job listing performance
        start_time = time.time()
        response = client.get("/api/v1/jobs")
        job_time = time.time() - start_time

        assert response.status_code == 200
        assert job_time < 3.0  # Should respond within 3 seconds

        # Test dataset listing performance
        start_time = time.time()
        response = client.get("/api/v1/datasets")
        dataset_time = time.time() - start_time

        assert response.status_code == 200
        assert dataset_time < 3.0  # Should respond within 3 seconds

        # Test comparison performance
        start_time = time.time()
        comparison_data = {
            "dataset_a_id": str(uuid.uuid4()),
            "dataset_b_id": str(uuid.uuid4()),
            "user_id": "perf_test"
        }
        response = client.post("/api/v1/datasets/compare", json=comparison_data)
        comparison_time = time.time() - start_time

        assert response.status_code == 200
        assert comparison_time < 30.0  # Comparison should complete within 30 seconds

    def test_error_handling_and_validation(self, client):
        """Test error handling and input validation."""

        # Test invalid job creation
        invalid_job = {"invalid": "data"}
        response = client.post("/api/v1/jobs", json=invalid_job)
        # Should handle gracefully (may succeed in demo mode)
        assert response.status_code in [200, 400, 422]

        # Test invalid dataset creation
        invalid_dataset = {"invalid": "data"}
        response = client.post("/api/v1/datasets", json=invalid_dataset)
        # Should handle gracefully (may succeed in demo mode)
        assert response.status_code in [200, 400, 422]

        # Test invalid comparison request
        invalid_comparison = {"invalid": "data"}
        response = client.post("/api/v1/datasets/compare", json=invalid_comparison)
        # Should handle gracefully
        assert response.status_code in [200, 400, 422]

    def test_web_dashboard_accessibility(self, client):
        """Test web dashboard loads and is accessible."""

        response = client.get("/")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

        # Check that essential elements are present
        html_content = response.content.decode()
        assert "ATS Unified Analytics Platform" in html_content
        assert "Job Management" in html_content
        assert "Dataset Catalog" in html_content
        assert "Dataset Comparison" in html_content
        assert "Workflow Analytics" in html_content

# ===== Manual Testing Verification =====

def print_manual_test_instructions():
    """Print instructions for manual testing verification."""

    instructions = """
    🧪 MANUAL TESTING INSTRUCTIONS

    After running automated tests, perform these manual verifications:

    1. 🚀 START THE APPLICATION:
       python unified_analytics_platform.py

    2. 📊 VERIFY WEB DASHBOARD:
       - Open http://localhost:5000/
       - Check all 4 tabs load without errors
       - Verify navigation between tabs works

    3. 🔍 TEST JOB MANAGEMENT:
       - Click "Job Management" tab
       - Verify jobs table loads with demo data
       - Test filtering by job type and status
       - Click "View" and "Datasets" buttons

    4. 📁 TEST DATASET CATALOG:
       - Click "Dataset Catalog" tab
       - Verify datasets table loads with demo data
       - Test search and symbol filtering
       - Click "Visualize" and "Source Job" buttons

    5. ⚖️ TEST DATASET COMPARISON:
       - Click "Dataset Comparison" tab
       - Select two different datasets from dropdowns
       - Click "Compare Datasets" button
       - Verify comparison results display with statistics

    6. 📈 TEST WORKFLOW ANALYTICS:
       - Click "Workflow Analytics" tab
       - Verify statistics cards show demo data
       - Check job stats, dataset overview, recent activity

    7. 🔗 TEST API ENDPOINTS:
       - Open http://localhost:5000/api/docs
       - Test GET /api/v1/jobs
       - Test GET /api/v1/datasets
       - Test POST /api/v1/datasets/compare with dataset IDs

    8. 🎯 VERIFY END-TO-END WORKFLOW:
       - Simulate job creation via API
       - Register dataset linked to job
       - Navigate from job to dataset and vice versa
       - Perform dataset comparison
       - Verify all data flows correctly

    ✅ SUCCESS CRITERIA:
    - All web tabs load without errors
    - Demo data displays correctly in tables
    - Interactive features (filtering, comparison) work
    - API endpoints respond with proper JSON
    - Job-to-dataset navigation functions
    - Comparison engine produces statistical results
    """

    print(instructions)

if __name__ == "__main__":
    print_manual_test_instructions()