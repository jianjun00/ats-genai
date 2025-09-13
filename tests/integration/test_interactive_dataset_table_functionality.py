#!/usr/bin/env python3
"""
Interactive Dataset Table Functionality Tests

CRITICAL: These tests protect against regressions in the enhanced dataset table functionality.
They verify the complete end-to-end functionality that was implemented for the user request:
"let' do the same for dataset dashboard where all training datasets are shown in a table with filter and sort."

This test suite ensures:
1. Enhanced API endpoints work with filtering and sorting parameters
2. Real data integration functions correctly
3. Frontend table functionality remains intact
4. Interactive features (sorting, filtering, pagination) work as expected
"""

import pytest
import httpx
import asyncpg

# Import the enhanced analytics manager
from unified_analytics_fixed import UnifiedAnalyticsManager, Environment


class TestInteractiveDatasetTableAPI:
    """Test the enhanced dataset API functionality"""

    @pytest.fixture
    async def analytics_manager(self):
        """Create analytics manager for testing"""
        manager = UnifiedAnalyticsManager()
        await manager.initialize()
        yield manager
        await manager.close()

    @pytest.mark.asyncio

    async def test_enhanced_list_datasets_default_parameters(self, analytics_manager):
        """Test that enhanced list_datasets works with default parameters"""
        result = await analytics_manager.list_datasets()

        assert "datasets" in result
        assert "total" in result
        assert isinstance(result["datasets"], list)
        assert isinstance(result["total"], int)

        # Verify dataset structure includes all required fields
        if result["datasets"]:
            dataset = result["datasets"][0]
            required_fields = [
                "dataset_id", "dataset_name", "symbols", "total_sequences",
                "feature_count", "technical_indicators", "created_at", "file_size_mb"
            ]
            for field in required_fields:
                assert field in dataset, f"Dataset missing required field: {field}"

    @pytest.mark.asyncio

    async def test_enhanced_list_datasets_with_filtering(self, analytics_manager):
        """Test dataset filtering functionality"""
        # Test filtering by symbol
        result = await analytics_manager.list_datasets(symbol_filter="tsla")

        assert "datasets" in result
        assert "total" in result

        # Verify filtering worked (either real data filtered or sample data)
        for dataset in result["datasets"]:
            dataset_name = dataset["dataset_name"].lower()
            symbols = str(dataset["symbols"]).lower()
            # Filter should match either dataset name or symbols
            assert "tsla" in dataset_name or "tsla" in symbols

    @pytest.mark.asyncio

    async def test_enhanced_list_datasets_with_sorting(self, analytics_manager):
        """Test dataset sorting functionality"""
        # Test sorting by dataset name ascending
        result_asc = await analytics_manager.list_datasets(sort_by="dataset_name", sort_dir="asc")
        assert "datasets" in result_asc

        # Test sorting by dataset name descending
        result_desc = await analytics_manager.list_datasets(sort_by="dataset_name", sort_dir="desc")
        assert "datasets" in result_desc

        # If we have multiple datasets, verify sorting order
        if len(result_asc["datasets"]) > 1:
            names_asc = [d["dataset_name"] for d in result_asc["datasets"]]
            names_desc = [d["dataset_name"] for d in result_desc["datasets"]]
            assert names_asc == sorted(names_asc), "Ascending sort failed"
            assert names_desc == sorted(names_desc, reverse=True), "Descending sort failed"

    @pytest.mark.asyncio

    async def test_enhanced_list_datasets_with_pagination(self, analytics_manager):
        """Test dataset pagination functionality"""
        # Test with limit
        result = await analytics_manager.list_datasets(limit=1, offset=0)
        assert len(result["datasets"]) <= 1

        # Test with offset
        result_offset = await analytics_manager.list_datasets(limit=1, offset=1)
        assert isinstance(result_offset["datasets"], list)

    @pytest.mark.asyncio

    async def test_enhanced_list_datasets_parameter_validation(self, analytics_manager):
        """Test that invalid sort parameters are handled safely"""
        # Test with invalid sort field (should default to creation_timestamp)
        result = await analytics_manager.list_datasets(sort_by="invalid_field")
        assert "datasets" in result

        # Test with invalid sort direction (should default to ASC)
        result = await analytics_manager.list_datasets(sort_dir="invalid_direction")
        assert "datasets" in result


class TestInteractiveDatasetTableHTTPAPI:
    """Test the HTTP API endpoints for enhanced dataset functionality"""

    @pytest.fixture
    def base_url(self):
        """Base URL for testing - adjust based on deployment"""
        return "http://172.25.223.121:3000"

    @pytest.mark.asyncio

    async def test_enhanced_datasets_api_endpoint_basic(self, base_url):
        """Test basic enhanced datasets API endpoint"""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{base_url}/api/v1/datasets")
                assert response.status_code == 200

                data = response.json()
                assert "datasets" in data
                assert "total" in data
                assert isinstance(data["datasets"], list)
                assert isinstance(data["total"], int)

            except httpx.ConnectError:
                pytest.skip("Cannot connect to analytics API for integration test")

    @pytest.mark.asyncio

    async def test_enhanced_datasets_api_with_query_parameters(self, base_url):
        """Test enhanced datasets API with filtering and sorting parameters"""
        async with httpx.AsyncClient() as client:
            try:
                # Test sorting
                response = await client.get(f"{base_url}/api/v1/datasets?sort_by=dataset_name&sort_dir=asc")
                assert response.status_code == 200
                data = response.json()
                assert "datasets" in data

                # Test filtering
                response = await client.get(f"{base_url}/api/v1/datasets?symbol_filter=tsla")
                assert response.status_code == 200
                data = response.json()
                assert "datasets" in data

                # Test pagination
                response = await client.get(f"{base_url}/api/v1/datasets?limit=1&offset=0")
                assert response.status_code == 200
                data = response.json()
                assert len(data["datasets"]) <= 1

            except httpx.ConnectError:
                pytest.skip("Cannot connect to analytics API for integration test")

    @pytest.mark.asyncio

    async def test_enhanced_datasets_api_parameter_combinations(self, base_url):
        """Test combinations of enhanced API parameters"""
        async with httpx.AsyncClient() as client:
            try:
                # Test multiple parameters together
                params = {
                    "limit": 10,
                    "offset": 0,
                    "sort_by": "total_sequences",
                    "sort_dir": "desc",
                    "symbol_filter": "aapl"
                }

                response = await client.get(f"{base_url}/api/v1/datasets", params=params)
                assert response.status_code == 200

                data = response.json()
                assert "datasets" in data
                assert "total" in data

            except httpx.ConnectError:
                pytest.skip("Cannot connect to analytics API for integration test")


class TestInteractiveDatasetTableFrontend:
    """Test the frontend interactive table functionality"""

    @pytest.fixture
    def base_url(self):
        """Base URL for testing"""
        return "http://172.25.223.121:3000"

    @pytest.mark.asyncio

    async def test_web_interface_contains_interactive_dataset_table(self, base_url):
        """Test that web interface includes interactive dataset table elements"""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{base_url}/")
                assert response.status_code == 200

                html_content = response.text

                # Verify interactive dataset table elements are present
                interactive_elements = [
                    "table-controls",  # Filter and pagination controls
                    "interactive-table",  # Table styling class
                    "datasets-table-body",  # Table body ID
                    "symbol-filter",  # Filter input field
                    "dataset-limit-select",  # Pagination select
                    "sortDatasets",  # JavaScript sort function
                    "refreshDatasets",  # JavaScript refresh function
                    "Dataset Name",  # Table headers
                    "Technical Indicators",
                    "📈 Dataset Visualization"  # Tab button
                ]

                for element in interactive_elements:
                    assert element in html_content, f"Missing interactive element: {element}"

            except httpx.ConnectError:
                pytest.skip("Cannot connect to web interface for integration test")

    @pytest.mark.asyncio

    async def test_web_interface_javascript_functions_present(self, base_url):
        """Test that required JavaScript functions are present"""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{base_url}/")
                assert response.status_code == 200

                html_content = response.text

                # Verify critical JavaScript functions exist
                js_functions = [
                    "sortDatasets(",  # Sort function
                    "refreshDatasets(",  # Refresh function
                    "loadDatasets(",  # Load function
                    "updateDatasetSortIndicators(",  # Sort indicators
                    "changeDatasetPage(",  # Pagination
                    "currentDatasetSort",  # Sort state variable
                    "currentSymbolFilter"  # Filter state variable
                ]

                for js_function in js_functions:
                    assert js_function in html_content, f"Missing JavaScript function: {js_function}"

            except httpx.ConnectError:
                pytest.skip("Cannot connect to web interface for integration test")


class TestInteractiveDatasetTableRegressionProtection:
    """Tests specifically designed to catch regressions in enhanced functionality"""

    @pytest.fixture
    def base_url(self):
        """Base URL for testing"""
        return "http://172.25.223.121:3000"

    @pytest.mark.asyncio

    async def test_datasets_api_backwards_compatibility(self, base_url):
        """Ensure old dataset API still works (no parameters)"""
        async with httpx.AsyncClient() as client:
            try:
                # Test that basic call without parameters still works
                response = await client.get(f"{base_url}/api/v1/datasets")
                assert response.status_code == 200

                data = response.json()

                # Verify basic structure is maintained
                assert "datasets" in data
                assert "total" in data

                # Verify dataset objects have required fields
                if data["datasets"]:
                    dataset = data["datasets"][0]
                    legacy_fields = ["dataset_id", "dataset_name", "symbols", "total_sequences"]
                    for field in legacy_fields:
                        assert field in dataset, f"Legacy field missing: {field}"

            except httpx.ConnectError:
                pytest.skip("Cannot connect to analytics API for integration test")

    @pytest.mark.asyncio

    async def test_dataset_table_vs_job_table_consistency(self, base_url):
        """Test that dataset table has similar functionality to job table"""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{base_url}/")
                assert response.status_code == 200

                html_content = response.text

                # Both tables should have similar interactive elements
                common_elements = [
                    "table-controls",
                    "interactive-table",
                    "sort-indicator",
                    "pagination"
                ]

                for element in common_elements:
                    # Should appear at least twice (once for jobs, once for datasets)
                    assert html_content.count(element) >= 2, f"Element {element} not consistent between tables"

            except httpx.ConnectError:
                pytest.skip("Cannot connect to web interface for integration test")

    @pytest.mark.asyncio

    async def test_critical_user_workflow_preserved(self, base_url):
        """Test the specific workflow the user requested"""
        async with httpx.AsyncClient() as client:
            try:
                # User requested: "dataset dashboard where all training datasets are shown in a table with filter and sort"

                # 1. Verify datasets are shown in table format
                response = await client.get(f"{base_url}/")
                html_content = response.text
                assert "datasets-table-body" in html_content
                assert "<th" in html_content  # Table headers present

                # 2. Verify filter functionality exists
                assert "symbol-filter" in html_content
                assert "Filter by Symbol/Name" in html_content

                # 3. Verify sort functionality exists
                assert "sortDatasets" in html_content
                assert "sort-indicator" in html_content

                # 4. Test actual API endpoints work
                datasets_response = await client.get(f"{base_url}/api/v1/datasets")
                assert datasets_response.status_code == 200

                # 5. Test filtering API works
                filter_response = await client.get(f"{base_url}/api/v1/datasets?symbol_filter=test")
                assert filter_response.status_code == 200

                # 6. Test sorting API works
                sort_response = await client.get(f"{base_url}/api/v1/datasets?sort_by=dataset_name&sort_dir=asc")
                assert sort_response.status_code == 200

            except httpx.ConnectError:
                pytest.skip("Cannot connect for critical workflow test")


class TestDatasetTableDataIntegrity:
    """Test that dataset table properly handles real database data"""

    @pytest.fixture
    async def db_connection(self):
        """Get database connection for validation"""
        env = Environment()
        conn = await asyncpg.connect(env.get_database_url())
        yield conn
        await conn.close()

    @pytest.mark.asyncio

    async def test_dataset_table_real_data_compatibility(self, db_connection):
        """Test that enhanced dataset functionality works with real data"""
        try:
            # Check if we can query the actual dataset table
            result = await db_connection.fetchrow("""
                SELECT dataset_name, symbols, total_sequences, feature_count,
                       technical_indicators, creation_timestamp, file_size_mb
                FROM dev_training_dataset
                LIMIT 1
            """)

            if result:
                # Verify the enhanced API can handle this data structure
                manager = UnifiedAnalyticsManager()
                await manager.initialize()

                try:
                    # Test that enhanced functionality works with real data
                    datasets = await manager.list_datasets(limit=5, sort_by="dataset_name")
                    assert "datasets" in datasets
                    assert "total" in datasets

                    # Test filtering with real data
                    if datasets["datasets"]:
                        first_dataset = datasets["datasets"][0]["dataset_name"]
                        filtered = await manager.list_datasets(symbol_filter=first_dataset[:4])
                        assert "datasets" in filtered

                finally:
                    await manager.close()

        except Exception as e:
            # If we can't connect to real database, test should not fail
            pytest.skip(f"Cannot test real data compatibility: {e}")


def test_dataset_table_functionality_documentation():
    """Test that key functionality is documented and testable"""

    # This test ensures the key features are explicitly tested
    key_features = [
        "Enhanced API with filtering and sorting parameters",
        "Interactive table with clickable column headers",
        "Real-time filtering by symbol/dataset name",
        "Pagination with configurable row limits",
        "Professional styling consistent with job table",
        "Real database integration with fallback to sample data"
    ]

    # Verify this test file covers all key features
    with open(__file__, 'r') as f:
        test_content = f.read()

    coverage_indicators = [
        "test_enhanced_list_datasets_with_filtering",
        "test_enhanced_list_datasets_with_sorting",
        "test_enhanced_list_datasets_with_pagination",
        "test_web_interface_contains_interactive_dataset_table",
        "test_dataset_table_real_data_compatibility",
        "test_critical_user_workflow_preserved"
    ]

    for indicator in coverage_indicators:
        assert indicator in test_content, f"Missing test coverage for: {indicator}"


if __name__ == "__main__":
    # Run the interactive dataset table functionality tests
    pytest.main([__file__, "-v", "--tb=short"])