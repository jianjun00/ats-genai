"""
Integration Tests for Backtest Webapp Deployment

Tests the complete deployment pipeline including:
- Kubernetes deployment
- ConfigMap mounting
- Service accessibility
- Port forwarding
- End-to-end web access
"""

import pytest
import requests
import subprocess
import time
import json
from pathlib import Path

class TestBacktestWebappKubernetesIntegration:
    """Test Kubernetes deployment integration"""

    @pytest.fixture(scope="class")
    def kubernetes_resources(self):
        """Deploy Kubernetes resources for testing"""
        # This would be handled by the actual deployment
        return {
            "namespace": "ats-dev",
            "deployment": "backtest-webapp",
            "service": "backtest-webapp-service",
            "configmap": "backtest-webapp-config"
        }

    def test_configmap_exists(self, kubernetes_resources):
        """Test that ConfigMap is properly created"""
        result = subprocess.run([
            "kubectl", "get", "configmap",
            kubernetes_resources["configmap"],
            "-n", kubernetes_resources["namespace"]
        ], capture_output=True, text=True)

        assert result.returncode == 0, "ConfigMap should exist"
        assert "backtest-webapp-config" in result.stdout

    def test_deployment_exists_and_ready(self, kubernetes_resources):
        """Test that deployment exists and is ready"""
        result = subprocess.run([
            "kubectl", "get", "deployment",
            kubernetes_resources["deployment"],
            "-n", kubernetes_resources["namespace"],
            "-o", "json"
        ], capture_output=True, text=True)

        assert result.returncode == 0, "Deployment should exist"

        deployment_data = json.loads(result.stdout)
        status = deployment_data.get("status", {})

        # Check that deployment is available
        conditions = status.get("conditions", [])
        available_condition = next(
            (c for c in conditions if c.get("type") == "Available"),
            None
        )

        if available_condition:
            assert available_condition.get("status") == "True", "Deployment should be available"

    def test_service_exists(self, kubernetes_resources):
        """Test that service exists and has correct configuration"""
        result = subprocess.run([
            "kubectl", "get", "service",
            kubernetes_resources["service"],
            "-n", kubernetes_resources["namespace"],
            "-o", "json"
        ], capture_output=True, text=True)

        assert result.returncode == 0, "Service should exist"

        service_data = json.loads(result.stdout)
        spec = service_data.get("spec", {})

        # Check service type and ports
        assert spec.get("type") == "NodePort"

        ports = spec.get("ports", [])
        assert len(ports) > 0, "Service should have ports configured"

        port_8000 = next((p for p in ports if p.get("port") == 8000), None)
        assert port_8000 is not None, "Service should expose port 8000"

    def test_pod_is_running(self, kubernetes_resources):
        """Test that pod is running successfully"""
        result = subprocess.run([
            "kubectl", "get", "pods",
            "-l", f"app={kubernetes_resources['deployment']}",
            "-n", kubernetes_resources["namespace"],
            "-o", "json"
        ], capture_output=True, text=True)

        assert result.returncode == 0, "Should be able to get pods"

        pods_data = json.loads(result.stdout)
        pods = pods_data.get("items", [])

        assert len(pods) > 0, "Should have at least one pod"

        pod = pods[0]
        status = pod.get("status", {})
        phase = status.get("phase")

        assert phase == "Running", f"Pod should be running, but is {phase}"

        # Check container status
        container_statuses = status.get("containerStatuses", [])
        assert len(container_statuses) > 0, "Should have container status"

        container_status = container_statuses[0]
        assert container_status.get("ready") is True, "Container should be ready"

class TestBacktestWebappEndToEnd:
    """End-to-end tests for webapp functionality"""

    def test_webapp_health_endpoint_accessible(self):
        """Test that webapp health endpoint is accessible via port forward"""
        try:
            # Test with current port forwarding
            response = requests.get("http://localhost:8001/health", timeout=5)
            assert response.status_code == 200

            data = response.json()
            assert data["status"] == "healthy"
            assert data["service"] == "backtest_dashboard"

        except requests.ConnectionError:
            pytest.skip("Port forwarding not active - this is expected in CI")

    def test_webapp_dashboard_endpoint_accessible(self):
        """Test that webapp dashboard is accessible and returns HTML"""
        try:
            # Test with current port forwarding
            response = requests.get("http://localhost:8001/", timeout=10)
            assert response.status_code == 200

            # Check content type
            assert "text/html" in response.headers.get("content-type", "")

            # Check HTML content
            html_content = response.text
            assert "<!DOCTYPE html>" in html_content
            assert "Backtest Results Dashboard" in html_content
            assert "Portfolio Strategy Performance Analysis" in html_content

            # Check for strategy data
            assert "Adaptive Support/Resistance Strategy" in html_content
            assert "Enhanced Momentum Strategy" in html_content
            assert "Statistical Mean Reversion" in html_content
            assert "SPY Buy & Hold Benchmark" in html_content

        except requests.ConnectionError:
            pytest.skip("Port forwarding not active - this is expected in CI")

    def test_webapp_performance_under_load(self):
        """Test webapp performance under multiple concurrent requests"""
        try:
            import concurrent.futures
            import time

            def make_request():
                start_time = time.time()
                response = requests.get("http://localhost:8001/", timeout=10)
                end_time = time.time()
                return {
                    "status_code": response.status_code,
                    "response_time": end_time - start_time,
                    "content_length": len(response.text)
                }

            # Make 5 concurrent requests
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(make_request) for _ in range(5)]
                results = [future.result() for future in concurrent.futures.as_completed(futures)]

            # All requests should succeed
            assert len(results) == 5
            for result in results:
                assert result["status_code"] == 200
                assert result["response_time"] < 5.0  # Should respond within 5 seconds
                assert result["content_length"] > 1000  # Should return substantial content

        except requests.ConnectionError:
            pytest.skip("Port forwarding not active - this is expected in CI")

class TestDeploymentFiles:
    """Test deployment configuration files"""

    def test_configmap_yaml_structure(self):
        """Test that ConfigMap YAML is properly structured"""
        configmap_file = Path("/home/jianjun/ats-genai/backtest_webapp_configmap.yaml")
        assert configmap_file.exists(), "ConfigMap YAML file should exist"

        content = configmap_file.read_text()

        # Check for required Kubernetes resources
        assert "apiVersion: v1" in content
        assert "kind: ConfigMap" in content
        assert "kind: Deployment" in content
        assert "kind: Service" in content

        # Check for namespace
        assert "namespace: ats-dev" in content

        # Check for Python webapp code
        assert "simple_backtest_webapp.py" in content
        assert "from fastapi import FastAPI" in content
        assert "BACKTEST_DATA" in content

    def test_webapp_python_file_structure(self):
        """Test that webapp Python file is properly structured"""
        webapp_file = Path("/home/jianjun/ats-genai/simple_backtest_webapp.py")
        assert webapp_file.exists(), "Webapp Python file should exist"

        content = webapp_file.read_text()

        # Check for required imports
        assert "from fastapi import FastAPI" in content
        assert "from fastapi.responses import HTMLResponse" in content

        # Check for app creation
        assert "app = FastAPI" in content

        # Check for required endpoints
        assert "@app.get(\"/\"" in content
        assert "@app.get(\"/health\"" in content

        # Check for data structure
        assert "BACKTEST_DATA" in content

        # Check for HTML generation
        assert "HTMLResponse(content=html)" in content

if __name__ == "__main__":
    # Run a quick integration test
    try:
        import requests
        response = requests.get("http://localhost:8001/health", timeout=5)
        print(f"✅ Health check: {response.json()}")

        response = requests.get("http://localhost:8001/", timeout=5)
        print(f"✅ Dashboard accessible: {len(response.text)} bytes of HTML")

    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        print("Note: This is expected if port forwarding is not active")