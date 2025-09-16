"""
Test cases to detect and fix dataset filesystem path issues
This test validates that dataset endpoints properly handle file access errors
and provides appropriate fallbacks or error messages.
"""

import pytest
import aiohttp
import json
import logging

logger = logging.getLogger(__name__)

class TestDatasetFilesystemFix:
    """Test suite to detect and fix dataset filesystem issues"""

    @pytest.fixture
    def webapp_base_url(self):
        return "http://10.0.0.79:3000"

    @pytest.fixture
    async def http_session(self):
        async with aiohttp.ClientSession() as session:
            yield session

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_sequences_error_detection(self, webapp_base_url, http_session):
        """Test that dataset sequences endpoint properly handles file not found errors"""
        try:
            # Test multiple datasets to find the pattern
            for dataset_id in [1, 2, 3, 4, 5]:
                async with http_session.get(f"{webapp_base_url}/api/v1/datasets/{dataset_id}/sequences") as response:
                    content = await response.text()

                    if response.status == 404:
                        logger.info(f"✅ Dataset {dataset_id}: Properly returns 404 for not found")
                        continue

                    try:
                        data = await response.json()

                        if "detail" in data and "No such file or directory" in data["detail"]:
                            logger.error(f"❌ Dataset {dataset_id}: File path error - {data['detail']}")
                            # This indicates the webapp is looking for files that don't exist
                            pytest.fail(f"Dataset {dataset_id} has filesystem path issue: {data['detail']}")

                        elif "sequences" in data:
                            logger.info(f"✅ Dataset {dataset_id}: Successfully returned sequences")

                        else:
                            logger.warning(f"⚠️ Dataset {dataset_id}: Unexpected response format")

                    except json.JSONDecodeError:
                        if "Error loading sequences" in content:
                            pytest.fail(f"Dataset {dataset_id} frontend shows 'Error loading sequences'")

        except Exception as e:
            pytest.fail(f"Failed to test dataset sequences: {e}")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_metadata_error_detection(self, webapp_base_url, http_session):
        """Test that dataset metadata endpoint handles file errors properly"""
        for dataset_id in [1, 2, 3]:
            async with http_session.get(f"{webapp_base_url}/api/v1/datasets/{dataset_id}/metadata") as response:
                try:
                    data = await response.json()

                    if "detail" in data and "No such file or directory" in data["detail"]:
                        logger.error(f"❌ Dataset {dataset_id} metadata: File path error - {data['detail']}")
                        # Extract the expected file path from the error
                        error_msg = data["detail"]
                        if "training_data_output/" in error_msg:
                            expected_path = error_msg.split("'")[1]
                            logger.error(f"   Expected file: {expected_path}")
                            pytest.fail(f"Dataset {dataset_id} metadata filesystem issue: {expected_path}")

                    elif "dataset_name" in data:
                        logger.info(f"✅ Dataset {dataset_id}: Metadata loaded successfully")

                except json.JSONDecodeError:
                    pass  # Non-JSON response is acceptable for some error cases

    def test_local_training_files_exist(self):
        """Test that training files exist locally (for comparison)"""
        from pathlib import Path

        training_dir = Path("training_data_output")
        if not training_dir.exists():
            pytest.skip("No local training_data_output directory")

        files = list(training_dir.glob("*"))
        logger.info(f"✅ Found {len(files)} local training files")

        # Check for aapl_tsla files which should work
        aapl_tsla_files = list(training_dir.glob("aapl_tsla*"))
        if aapl_tsla_files:
            logger.info(f"✅ Found aapl_tsla files: {[f.name for f in aapl_tsla_files]}")
        else:
            logger.warning("⚠️ No aapl_tsla files found locally")

        # List first 10 files for debugging
        for file in files[:10]:
            logger.info(f"   Local file: {file.name}")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_detail_page_error_source(self, webapp_base_url, http_session):
        """Test specifically what causes 'Error loading sequences' in the detail page"""
        dataset_detail_url = f"{webapp_base_url}/dataset-detail?id=1"

        async with http_session.get(dataset_detail_url) as response:
            content = await response.text()

            # Check if page loads
            if response.status != 200:
                pytest.fail(f"Dataset detail page not accessible: {response.status}")

            # Check JavaScript console errors in the HTML
            if "Error loading sequences" in content:
                logger.error("❌ Found 'Error loading sequences' in page content")

                # Try to determine the root cause by checking the API endpoint directly
                try:
                    async with http_session.get(f"{webapp_base_url}/api/v1/datasets/1/sequences") as api_response:
                        api_content = await api_response.text()

                        if api_response.status != 200:
                            logger.error(f"   Root cause: API endpoint returns {api_response.status}")
                            logger.error(f"   API response: {api_content}")

                        try:
                            api_data = await api_response.json()
                            if "detail" in api_data:
                                logger.error(f"   Root cause: API error - {api_data['detail']}")

                                # Extract file path issue
                                if "No such file or directory" in api_data["detail"]:
                                    missing_file = api_data["detail"].split("'")[1]
                                    logger.error(f"   Missing file: {missing_file}")

                                    pytest.fail(f"Dataset detail page fails because API cannot find file: {missing_file}")

                        except json.JSONDecodeError:
                            logger.error(f"   Root cause: API returned non-JSON response")

                except Exception as e:
                    logger.error(f"   Error checking API endpoint: {e}")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dataset_file_path_investigation(self, webapp_base_url, http_session):
        """Investigate exact file paths the webapp is looking for"""

        # Check all dataset IDs to see what files they expect
        expected_files = []

        for dataset_id in range(1, 6):
            try:
                async with http_session.get(f"{webapp_base_url}/api/v1/datasets/{dataset_id}/sequences") as response:
                    try:
                        data = await response.json()
                        if "detail" in data and "No such file or directory" in data["detail"]:
                            error_msg = data["detail"]
                            if "'" in error_msg:
                                missing_file = error_msg.split("'")[1]
                                expected_files.append(f"Dataset {dataset_id}: {missing_file}")
                    except:
                        pass
            except:
                pass

        if expected_files:
            logger.error("❌ Webapp expects these files but cannot find them:")
            for file_info in expected_files:
                logger.error(f"   {file_info}")

            pytest.fail(f"Webapp filesystem path issues detected for {len(expected_files)} datasets")
        else:
            logger.info("✅ No obvious file path issues detected")


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "-s"]))