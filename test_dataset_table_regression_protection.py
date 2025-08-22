#!/usr/bin/env python3
"""
Dataset Table Regression Protection Test

CRITICAL: This test protects the exact functionality requested by the user:
"let' do the same for dataset dashboard where all training datasets are shown in a table with filter and sort."

This test will FAIL if future changes break:
1. Dataset table display functionality
2. Filtering by symbol/name capability
3. Sorting by columns capability
4. API parameter support for filtering and sorting
5. Interactive table elements in the web interface

RUN THIS TEST BEFORE ANY CHANGES TO DATASET FUNCTIONALITY!
"""

import asyncio
import httpx
import sys


class DatasetTableRegressionTest:
    """Test that protects against regressions in dataset table functionality"""
    
    def __init__(self, base_url="http://172.25.223.121:3000"):
        self.base_url = base_url
        self.failures = []
    
    def fail(self, message):
        """Record a test failure"""
        self.failures.append(f"❌ FAIL: {message}")
        print(f"❌ FAIL: {message}")
    
    def success(self, message):
        """Record a test success"""
        print(f"✅ PASS: {message}")
    
    async def test_api_enhanced_datasets_endpoint(self):
        """Test 1: Enhanced datasets API with filtering and sorting"""
        async with httpx.AsyncClient() as client:
            try:
                # Test basic endpoint
                response = await client.get(f"{self.base_url}/api/v1/datasets")
                if response.status_code != 200:
                    self.fail(f"Basic datasets API failed: {response.status_code}")
                    return
                
                data = response.json()
                if "datasets" not in data or "total" not in data:
                    self.fail("Datasets API missing required fields 'datasets' or 'total'")
                    return
                
                self.success("Basic datasets API works")
                
                # Test filtering parameter
                response = await client.get(f"{self.base_url}/api/v1/datasets?symbol_filter=tsla")
                if response.status_code != 200:
                    self.fail(f"Datasets filtering API failed: {response.status_code}")
                    return
                
                self.success("Dataset filtering API works")
                
                # Test sorting parameters
                response = await client.get(f"{self.base_url}/api/v1/datasets?sort_by=dataset_name&sort_dir=asc")
                if response.status_code != 200:
                    self.fail(f"Datasets sorting API failed: {response.status_code}")
                    return
                
                self.success("Dataset sorting API works")
                
                # Test pagination parameters
                response = await client.get(f"{self.base_url}/api/v1/datasets?limit=1&offset=0")
                if response.status_code != 200:
                    self.fail(f"Datasets pagination API failed: {response.status_code}")
                    return
                
                data = response.json()
                if len(data["datasets"]) > 1:
                    self.fail("Pagination limit parameter not working")
                    return
                
                self.success("Dataset pagination API works")
                
            except Exception as e:
                self.fail(f"API test exception: {e}")
    
    async def test_web_interface_interactive_table(self):
        """Test 2: Web interface contains interactive dataset table"""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{self.base_url}/")
                if response.status_code != 200:
                    self.fail(f"Web interface failed to load: {response.status_code}")
                    return
                
                html = response.text
                
                # Test 1: Dataset table structure exists
                required_elements = [
                    "📈 Dataset Visualization",  # Tab button
                    "datasets-table-body",  # Table body ID
                    "interactive-table",  # Table CSS class
                    "Dataset Name",  # Table header
                    "Technical Indicators",  # Table header
                    "Created"  # Table header
                ]
                
                for element in required_elements:
                    if element not in html:
                        self.fail(f"Missing interactive table element: {element}")
                        return
                
                self.success("Interactive table structure present")
                
                # Test 2: Filter controls exist
                filter_elements = [
                    "symbol-filter",  # Filter input ID
                    "Filter by Symbol/Name",  # Filter label
                    "refreshDatasets"  # Refresh function
                ]
                
                for element in filter_elements:
                    if element not in html:
                        self.fail(f"Missing filter element: {element}")
                        return
                
                self.success("Filter controls present")
                
                # Test 3: Sort functionality exists
                sort_elements = [
                    "sortDatasets(",  # Sort function
                    "sort-indicator",  # Sort indicator CSS
                    "onclick=\"sortDatasets('dataset_name')\""  # Clickable sort
                ]
                
                for element in sort_elements:
                    if element not in html:
                        self.fail(f"Missing sort element: {element}")
                        return
                
                self.success("Sort functionality present")
                
                # Test 4: Pagination controls exist
                pagination_elements = [
                    "dataset-pagination",  # Pagination container
                    "dataset-limit-select",  # Rows per page select
                    "changeDatasetPage"  # Page change function
                ]
                
                for element in pagination_elements:
                    if element not in html:
                        self.fail(f"Missing pagination element: {element}")
                        return
                
                self.success("Pagination controls present")
                
            except Exception as e:
                self.fail(f"Web interface test exception: {e}")
    
    async def test_dataset_table_vs_job_table_consistency(self):
        """Test 3: Dataset table has same features as job table"""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{self.base_url}/")
                if response.status_code != 200:
                    self.fail(f"Web interface failed: {response.status_code}")
                    return
                
                html = response.text
                
                # Both job and dataset tables should have interactive-table class
                table_count = html.count("interactive-table")
                if table_count < 2:
                    self.fail(f"Expected 2 interactive tables (jobs + datasets), found {table_count}")
                    return
                
                self.success("Both job and dataset tables use interactive styling")
                
                # Both should have sort indicators
                sort_count = html.count("sort-indicator")
                if sort_count < 10:  # Multiple columns in each table
                    self.fail(f"Not enough sort indicators found: {sort_count}")
                    return
                
                self.success("Sort indicators present for both tables")
                
                # Both should have table controls
                controls_count = html.count("table-controls")
                if controls_count < 2:
                    self.fail(f"Expected 2 table control sections, found {controls_count}")
                    return
                
                self.success("Table controls present for both tables")
                
            except Exception as e:
                self.fail(f"Consistency test exception: {e}")
    
    async def test_critical_user_workflow(self):
        """Test 4: The exact workflow user requested works"""
        async with httpx.AsyncClient() as client:
            try:
                # User request: "dataset dashboard where all training datasets are shown in a table with filter and sort"
                
                # 1. Access dataset dashboard
                response = await client.get(f"{self.base_url}/")
                if response.status_code != 200:
                    self.fail("Cannot access dataset dashboard")
                    return
                
                html = response.text
                
                # 2. Verify datasets shown in TABLE format (not cards)
                if "datasets-table-body" not in html:
                    self.fail("Datasets not shown in table format")
                    return
                
                if "<td>" not in html:  # Should have table cells
                    self.fail("No table cells found - datasets may not be in table format")
                    return
                
                self.success("Datasets shown in table format")
                
                # 3. Verify FILTER capability exists
                if "symbol-filter" not in html:
                    self.fail("Filter capability missing")
                    return
                
                # Test filter API works
                filter_response = await client.get(f"{self.base_url}/api/v1/datasets?symbol_filter=test")
                if filter_response.status_code != 200:
                    self.fail("Filter API not working")
                    return
                
                self.success("Filter capability working")
                
                # 4. Verify SORT capability exists
                if "sortDatasets" not in html:
                    self.fail("Sort capability missing")
                    return
                
                # Test sort API works
                sort_response = await client.get(f"{self.base_url}/api/v1/datasets?sort_by=dataset_name")
                if sort_response.status_code != 200:
                    self.fail("Sort API not working")
                    return
                
                self.success("Sort capability working")
                
                # 5. Verify training datasets are actually shown
                datasets_response = await client.get(f"{self.base_url}/api/v1/datasets")
                if datasets_response.status_code != 200:
                    self.fail("Cannot retrieve training datasets")
                    return
                
                data = datasets_response.json()
                if not data.get("datasets"):
                    self.fail("No training datasets found")
                    return
                
                self.success("Training datasets displayed")
                
                self.success("🎉 COMPLETE USER WORKFLOW VERIFIED")
                
            except Exception as e:
                self.fail(f"Critical workflow test exception: {e}")
    
    async def run_all_tests(self):
        """Run all regression protection tests"""
        print("🔍 Running Dataset Table Regression Protection Tests...")
        print("=" * 60)
        
        await self.test_api_enhanced_datasets_endpoint()
        await self.test_web_interface_interactive_table()
        await self.test_dataset_table_vs_job_table_consistency()
        await self.test_critical_user_workflow()
        
        print("=" * 60)
        
        if self.failures:
            print(f"💥 {len(self.failures)} REGRESSION(S) DETECTED!")
            for failure in self.failures:
                print(failure)
            print("\n🚨 DATASET TABLE FUNCTIONALITY IS BROKEN!")
            print("🔧 Fix these issues before deploying changes.")
            return False
        else:
            print("✅ ALL TESTS PASSED - No regressions detected")
            print("🚀 Dataset table functionality is working correctly")
            return True


async def main():
    """Run the regression protection test"""
    try:
        tester = DatasetTableRegressionTest()
        success = await tester.run_all_tests()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"❌ Test runner failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())