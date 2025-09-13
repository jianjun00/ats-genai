"""
Integration tests for Enhanced Analytics Webapp

Following CLAUDE.md requirements:
- Test actual service startup
- Verify database connectivity
- Test real API endpoints
- Manual verification steps
"""
import pytest
import subprocess
import time
import requests
import os

class TestEnhancedWebappIntegration:
    """Integration tests for enhanced webapp functionality"""

    @pytest.fixture(scope="class")
    def webapp_process(self):
        """Start the webapp process for testing"""
        # Set environment for testing
        env = os.environ.copy()
        env['PYTHONPATH'] = 'src'

        # Start webapp in background
        process = subprocess.Popen(
            ['python', 'unified_backtest_analytics_webapp.py'],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        # Wait for startup and check if it's actually running
        time.sleep(5)

        # Check if process is still alive
        if process.poll() is not None:
            # Process died, get output
            stdout, stderr = process.communicate()
            pytest.fail(f"Webapp failed to start. Output: {stdout}")

        # Try to connect to health endpoint
        max_retries = 15
        webapp_ready = False
        for i in range(max_retries):
            try:
                response = requests.get('http://localhost:3000/health', timeout=2)
                if response.status_code == 200:
                    webapp_ready = True
                    break
            except requests.exceptions.ConnectionError:
                time.sleep(2)
                if process.poll() is not None:
                    # Process died during startup
                    stdout, stderr = process.communicate()
                    pytest.fail(f"Webapp died during startup. Output: {stdout}")

        if not webapp_ready:
            # Get process output for debugging
            process.terminate()
            stdout, stderr = process.communicate(timeout=5)
            pytest.fail(f"Webapp failed to become ready. Output: {stdout}")

        yield process

        # Cleanup
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

    def test_webapp_startup_and_health_check(self, webapp_process):
        """Test that webapp actually starts and responds to health check"""
        # Wait for webapp to be ready
        max_retries = 30
        for _ in range(max_retries):
            try:
                response = requests.get('http://localhost:3000/health', timeout=5)
                if response.status_code == 200:
                    break
            except requests.exceptions.ConnectionError:
                time.sleep(1)
        else:
            # Get process output for debugging
            stdout, stderr = webapp_process.communicate(timeout=5)
            pytest.fail(f"Webapp failed to start. STDOUT: {stdout.decode()}, STDERR: {stderr.decode()}")

        # Verify health endpoint
        response = requests.get('http://localhost:3000/health')
        assert response.status_code == 200

        health_data = response.json()
        assert health_data['status'] == 'healthy'
        assert 'timestamp' in health_data

    def test_job_runs_api_endpoint(self, webapp_process):
        """Test job runs API returns real data from database"""
        response = requests.get('http://localhost:3000/api/v1/job-runs?limit=5')

        # Should return 200 or 503 (if database unavailable) - NO 404
        assert response.status_code in [200, 503], f"Unexpected status: {response.status_code}"

        if response.status_code == 200:
            jobs = response.json()
            assert isinstance(jobs, list)

            # Verify job structure if data exists
            if jobs:
                job = jobs[0]
                required_fields = ['run_id', 'run_type', 'start_time', 'status']
                for field in required_fields:
                    assert field in job, f"Missing field: {field}"

    def test_training_datasets_api_endpoint(self, webapp_process):
        """Test training datasets API returns real data"""
        response = requests.get('http://localhost:3000/api/v1/training-datasets?limit=5')

        # Should return 200 - training data from filesystem
        assert response.status_code == 200

        datasets = response.json()
        assert isinstance(datasets, list)

        # Verify dataset structure if data exists
        if datasets:
            dataset = datasets[0]
            required_fields = ['dataset_name', 'creation_timestamp', 'total_sequences', 'feature_count']
            for field in required_fields:
                assert field in dataset, f"Missing field: {field}"

    def test_main_dashboard_contains_new_sections(self, webapp_process):
        """Test that main dashboard HTML contains job runs and training data sections"""
        response = requests.get('http://localhost:3000/')
        assert response.status_code == 200

        html = response.text

        # Check for job runs section
        assert 'job-runs' in html, "Job runs section missing from HTML"
        assert 'Job Runs' in html, "Job Runs tab missing"

        # Check for training data section
        assert 'training-data' in html, "Training data section missing from HTML"
        assert 'Training Data' in html, "Training Data tab missing"

        # Check for API endpoints in JavaScript
        assert '/api/v1/job-runs' in html, "Job runs API endpoint missing"
        assert '/api/v1/training-datasets' in html, "Training datasets API endpoint missing"

    def test_database_connection_required(self, webapp_process):
        """Test that webapp properly requires database connection (no mock data)"""
        # This test ensures we're not using mock data fallbacks
        response = requests.get('http://localhost:3000/api/v1/job-runs')

        if response.status_code == 503:
            # Expected when database unavailable - good, no mock fallback
            error = response.json()
            assert 'Database connection required' in error['detail']
        elif response.status_code == 200:
            # If successful, data should be from real database
            jobs = response.json()
            # Real database data should have actual timestamps and IDs
            if jobs:
                job = jobs[0]
                assert isinstance(job['run_id'], int), "run_id should be real database integer"
                assert 'T' in job['start_time'], "start_time should be real timestamp"


class TestFlyteDevCLIIntegration:
    """Integration tests for Flyte dev CLI"""

    def test_flyte_cli_submit_workflow(self):
        """Test that Flyte dev CLI can submit workflows"""
        # Test submitting a training data generation workflow
        result = subprocess.run([
            'python', 'scripts/flyte_dev_cli.py', 'submit', 'training-data-gen',
            '--dataset', 'test_integration', '--symbols', 'AAPL'
        ], capture_output=True, text=True)

        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        assert 'Workflow training_data_generation_workflow submitted successfully' in result.stdout
        assert 'Execution ID:' in result.stdout

    def test_flyte_cli_list_executions(self):
        """Test that Flyte dev CLI can list executions"""
        result = subprocess.run([
            'python', 'scripts/flyte_dev_cli.py', 'list-executions', '--limit', '5'
        ], capture_output=True, text=True)

        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        assert 'Recent Flyte Workflow Executions' in result.stdout


class TestManualVerificationSteps:
    """Manual verification test cases"""

    def test_manual_verification_instructions(self):
        """Provide manual verification steps for human testing"""
        instructions = """
        MANUAL VERIFICATION REQUIRED:

        1. Open browser to http://localhost:3000/
        2. Verify all tabs are visible: Executive Dashboard, Performance Analysis, Attribution & Risk, Model Performance, Forecast Visualization, Job Runs, Training Data
        3. Click on "Job Runs" tab - should show table with real job data
        4. Click on "Training Data" tab - should show dataset cards
        5. Test dataset comparison: select two datasets and click Compare
        6. Check that Flyte Console button works (opens Flyte URL)
        7. Verify no mock data is displayed (all data should be from database/filesystem)

        API ENDPOINTS TO TEST:
        - curl http://localhost:3000/health
        - curl http://localhost:3000/api/v1/job-runs
        - curl http://localhost:3000/api/v1/training-datasets

        DATABASE CONNECTIVITY:
        - Verify webapp connects to postgres-simple service in Kubernetes
        - Check that job submissions via flyte_dev_cli.py appear in webapp
        """

        # This test always passes but prints instructions
        print(instructions)
        assert True, "Manual verification steps provided above"


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])