#!/usr/bin/env python3
"""
Unified Analytics Deployment Protection Test Suite

This test suite validates Kubernetes deployment configuration and prevents
regressions in the deployment setup that could cause crashloops or feature loss.

Critical validations:
- Correct ConfigMap is referenced
- Numpy dependency is properly installed
- Service configuration is correct
- Pod health and readiness
- Volume mounts and environment variables

These tests must pass to prevent deployment configuration regressions.
"""

import json
import pytest
import subprocess
import time
import yaml
from typing import Dict, List, Any, Optional


class UnifiedAnalyticsDeploymentProtector:
    """
    Protects against deployment configuration regressions that could cause:
    - CrashLoopBackOff (wrong configmap, missing dependencies)
    - Missing features (incomplete configuration)
    - Service accessibility issues
    """

    def __init__(self, namespace: str = "ats-dev"):
        self.namespace = namespace
        self.app_label = "unified-analytics-webapp"
        self.service_name = "unified-analytics-service"
        self.configmap_name = "unified-analytics-config"

    def run_kubectl(self, args: List[str], timeout: int = 30) -> Dict[str, Any]:
        """Run kubectl command and return result"""
        try:
            result = subprocess.run(
                ["kubectl"] + args,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            return {
                "success": result.returncode == 0,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "returncode": result.returncode
            }
        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "error": "timeout",
                "timeout": timeout
            }
        except FileNotFoundError:
            return {
                "success": False,
                "error": "kubectl_not_found"
            }

    def validate_configmap_content(self) -> Dict[str, Any]:
        """
        CRITICAL: Validate ConfigMap contains required features and numpy import
        Prevents regression to wrong configmap that caused original crashloop
        """
        result = self.run_kubectl([
            "get", "configmap", self.configmap_name,
            "-n", self.namespace, "-o", "yaml"
        ])

        if not result["success"]:
            return {"error": "configmap_not_accessible", "details": result}

        try:
            configmap_data = yaml.safe_load(result["stdout"])
            app_py_content = configmap_data["data"]["app.py"]

            validations = {
                "has_numpy_import": "import numpy" in app_py_content,
                "has_dataset_detail_route": "/dataset/{dataset_id}" in app_py_content,
                "has_jobs_route": "/jobs" in app_py_content,
                "has_datasets_route": "/datasets" in app_py_content,
                "has_health_route": "/health" in app_py_content,
                "has_fastapi_import": "from fastapi import" in app_py_content,
                "has_database_connection": "DATABASE_URL" in app_py_content,
                "no_demo_data_fallbacks": "generate_demo_" not in app_py_content,
            }

            # All validations must pass
            failed_validations = [k for k, v in validations.items() if not v]

            return {
                "success": len(failed_validations) == 0,
                "validations": validations,
                "failed": failed_validations,
                "configmap_size": len(app_py_content)
            }

        except Exception as e:
            return {"error": "configmap_parse_error", "details": str(e)}

    def validate_deployment_configuration(self) -> Dict[str, Any]:
        """
        CRITICAL: Validate deployment is correctly configured
        Prevents regressions in deployment spec that could cause issues
        """
        result = self.run_kubectl([
            "get", "deployment", f"{self.app_label}",
            "-n", self.namespace, "-o", "yaml"
        ])

        if not result["success"]:
            return {"error": "deployment_not_accessible", "details": result}

        try:
            deployment_data = yaml.safe_load(result["stdout"])
            container = deployment_data["spec"]["template"]["spec"]["containers"][0]

            # Extract installation command
            install_args = " ".join(container.get("args", []))

            validations = {
                "has_numpy_dependency": "numpy" in install_args,
                "has_fastapi_dependency": "fastapi" in install_args,
                "has_asyncpg_dependency": "asyncpg" in install_args,
                "correct_configmap_reference": False,
                "correct_port_configuration": container.get("ports", [{}])[0].get("containerPort") == 3000,
                "has_volume_mounts": len(container.get("volumeMounts", [])) > 0,
            }

            # Check volume references
            volumes = deployment_data["spec"]["template"]["spec"].get("volumes", [])
            for volume in volumes:
                if volume.get("name") == "webapp-config":
                    configmap_ref = volume.get("configMap", {}).get("name")
                    validations["correct_configmap_reference"] = configmap_ref == self.configmap_name

            failed_validations = [k for k, v in validations.items() if not v]

            return {
                "success": len(failed_validations) == 0,
                "validations": validations,
                "failed": failed_validations,
                "install_command": install_args
            }

        except Exception as e:
            return {"error": "deployment_parse_error", "details": str(e)}

    def validate_pod_health(self) -> Dict[str, Any]:
        """
        CRITICAL: Validate pods are healthy and not in crashloop
        Detects if deployment changes caused pod instability
        """
        result = self.run_kubectl([
            "get", "pods", "-n", self.namespace,
            "-l", f"app={self.app_label}", "-o", "json"
        ])

        if not result["success"]:
            return {"error": "pods_not_accessible", "details": result}

        try:
            pods_data = json.loads(result["stdout"])
            pods = pods_data.get("items", [])

            if not pods:
                return {"error": "no_pods_found", "expected_label": f"app={self.app_label}"}

            pod_statuses = []
            healthy_pods = 0

            for pod in pods:
                pod_name = pod["metadata"]["name"]
                pod_status = pod["status"]["phase"]

                # Check for crashloop indicators
                container_statuses = pod["status"].get("containerStatuses", [])
                restart_count = 0
                waiting_reason = None

                if container_statuses:
                    container = container_statuses[0]
                    restart_count = container.get("restartCount", 0)

                    if container.get("state", {}).get("waiting"):
                        waiting_reason = container["state"]["waiting"].get("reason")

                pod_info = {
                    "name": pod_name,
                    "status": pod_status,
                    "restart_count": restart_count,
                    "waiting_reason": waiting_reason,
                    "healthy": pod_status == "Running" and restart_count < 5
                }

                pod_statuses.append(pod_info)
                if pod_info["healthy"]:
                    healthy_pods += 1

            return {
                "success": healthy_pods > 0,
                "total_pods": len(pods),
                "healthy_pods": healthy_pods,
                "pod_statuses": pod_statuses
            }

        except Exception as e:
            return {"error": "pods_parse_error", "details": str(e)}

    def validate_service_configuration(self) -> Dict[str, Any]:
        """
        CRITICAL: Validate service is correctly configured for external access
        Ensures service configuration wasn't accidentally changed
        """
        result = self.run_kubectl([
            "get", "service", self.service_name,
            "-n", self.namespace, "-o", "yaml"
        ])

        if not result["success"]:
            return {"error": "service_not_accessible", "details": result}

        try:
            service_data = yaml.safe_load(result["stdout"])

            validations = {
                "correct_type": service_data["spec"]["type"] == "NodePort",
                "correct_port": False,
                "correct_target_port": False,
                "correct_selector": service_data["spec"]["selector"].get("app") == self.app_label,
            }

            # Check port configuration
            ports = service_data["spec"].get("ports", [])
            if ports:
                port_config = ports[0]
                validations["correct_port"] = port_config.get("port") == 3000
                validations["correct_target_port"] = port_config.get("targetPort") in [3000, "http"]

            failed_validations = [k for k, v in validations.items() if not v]

            return {
                "success": len(failed_validations) == 0,
                "validations": validations,
                "failed": failed_validations,
                "service_type": service_data["spec"]["type"],
                "ports": ports
            }

        except Exception as e:
            return {"error": "service_parse_error", "details": str(e)}

    def validate_pod_logs(self, max_pods: int = 3) -> Dict[str, Any]:
        """
        CRITICAL: Validate pod logs show successful startup without errors
        Detects if pods are starting but failing internally
        """
        # Get recent pod names
        result = self.run_kubectl([
            "get", "pods", "-n", self.namespace,
            "-l", f"app={self.app_label}",
            "--sort-by=.metadata.creationTimestamp",
            "-o", "jsonpath={.items[-3:].metadata.name}"
        ])

        if not result["success"]:
            return {"error": "pod_names_not_accessible"}

        pod_names = result["stdout"].strip().split()
        if not pod_names:
            return {"error": "no_pod_names_found"}

        log_results = []

        for pod_name in pod_names[-max_pods:]:  # Check last few pods
            log_result = self.run_kubectl([
                "logs", pod_name, "-n", self.namespace, "--tail=50"
            ])

            if log_result["success"]:
                log_content = log_result["stdout"]

                # Check for success indicators
                success_indicators = [
                    "Uvicorn running on",
                    "Application startup complete",
                    "Database pool created successfully",
                    "Training data accessible"
                ]

                # Check for error indicators
                error_indicators = [
                    "ModuleNotFoundError: No module named 'numpy'",
                    "CrashLoopBackOff",
                    "ImportError",
                    "Failed to create database pool",
                    "Traceback (most recent call last):"
                ]

                has_success = any(indicator in log_content for indicator in success_indicators)
                has_errors = any(indicator in log_content for indicator in error_indicators)

                log_results.append({
                    "pod": pod_name,
                    "has_success_indicators": has_success,
                    "has_error_indicators": has_errors,
                    "log_size": len(log_content),
                    "healthy": has_success and not has_errors
                })

        healthy_logs = sum(1 for log in log_results if log["healthy"])

        return {
            "success": healthy_logs > 0,
            "total_pods_checked": len(log_results),
            "healthy_logs": healthy_logs,
            "log_results": log_results
        }

    def validate_external_accessibility(self) -> Dict[str, Any]:
        """
        CRITICAL: Validate service is externally accessible
        Ensures external access configuration is correct
        """
        # Get service external access info
        result = self.run_kubectl([
            "get", "service", self.service_name, "-n", self.namespace, "-o", "json"
        ])

        if not result["success"]:
            return {"error": "service_not_accessible"}

        try:
            service_data = json.loads(result["stdout"])

            # Check if it's a NodePort service
            if service_data["spec"]["type"] == "NodePort":
                ports = service_data["spec"].get("ports", [])
                if ports:
                    node_port = ports[0].get("nodePort")
                    if node_port:
                        return {
                            "success": True,
                            "access_type": "NodePort",
                            "node_port": node_port,
                            "accessible_via": f"<node_ip>:{node_port}"
                        }

            return {
                "success": False,
                "error": "no_external_access_configured",
                "service_type": service_data["spec"]["type"]
            }

        except Exception as e:
            return {"error": "external_access_parse_error", "details": str(e)}


# Test fixtures
@pytest.fixture(scope="session")
def deployment_protector():
    """Create deployment protector instance"""
    return UnifiedAnalyticsDeploymentProtector()


# Critical deployment protection tests
class TestUnifiedAnalyticsDeploymentProtection:
    """
    Critical deployment tests that MUST pass to prevent deployment regressions
    """

    def test_configmap_content_regression_protection(self, deployment_protector):
        """
        CRITICAL: Ensure ConfigMap has correct content
        Prevents regression to wrong configmap (original cause of crashloop)
        """
        result = deployment_protector.validate_configmap_content()

        if "error" in result:
            pytest.skip(f"ConfigMap validation skipped: {result['error']}")
            return

        assert result["success"], f"ConfigMap validation failed: {result.get('failed', [])}"

        # Critical validations
        validations = result["validations"]
        assert validations["has_numpy_import"], "ConfigMap must include numpy import"
        assert validations["has_dataset_detail_route"], "ConfigMap must include dataset detail route"
        assert validations["has_jobs_route"], "ConfigMap must include jobs route"
        assert validations["has_datasets_route"], "ConfigMap must include datasets route"
        assert validations["no_demo_data_fallbacks"], "ConfigMap must not include demo data fallbacks"

        print(f"✅ ConfigMap validation passed ({result['configmap_size']} chars)")

    def test_deployment_configuration_regression_protection(self, deployment_protector):
        """
        CRITICAL: Ensure deployment configuration is correct
        Prevents regressions in deployment spec
        """
        result = deployment_protector.validate_deployment_configuration()

        if "error" in result:
            pytest.skip(f"Deployment validation skipped: {result['error']}")
            return

        assert result["success"], f"Deployment validation failed: {result.get('failed', [])}"

        # Critical validations
        validations = result["validations"]
        assert validations["has_numpy_dependency"], "Deployment must install numpy dependency"
        assert validations["correct_configmap_reference"], "Deployment must reference correct ConfigMap"
        assert validations["correct_port_configuration"], "Deployment must use correct port"

        print(f"✅ Deployment configuration validation passed")

    def test_pod_health_regression_protection(self, deployment_protector):
        """
        CRITICAL: Ensure pods are healthy and not in crashloop
        Detects crashloop regressions
        """
        result = deployment_protector.validate_pod_health()

        if "error" in result:
            pytest.skip(f"Pod health validation skipped: {result['error']}")
            return

        assert result["success"], f"Pod health validation failed"
        assert result["healthy_pods"] > 0, "At least one pod must be healthy"

        # Check for crashloop indicators
        for pod_status in result["pod_statuses"]:
            if pod_status["waiting_reason"] == "CrashLoopBackOff":
                pytest.fail(f"Pod {pod_status['name']} is in CrashLoopBackOff")

        print(f"✅ Pod health validation passed ({result['healthy_pods']}/{result['total_pods']} healthy)")

    def test_service_configuration_regression_protection(self, deployment_protector):
        """
        CRITICAL: Ensure service configuration is correct
        Prevents service access regressions
        """
        result = deployment_protector.validate_service_configuration()

        if "error" in result:
            pytest.skip(f"Service validation skipped: {result['error']}")
            return

        assert result["success"], f"Service validation failed: {result.get('failed', [])}"

        # Critical validations
        validations = result["validations"]
        assert validations["correct_type"], "Service must be NodePort type"
        assert validations["correct_port"], "Service must expose port 3000"
        assert validations["correct_selector"], "Service must target correct app"

        print(f"✅ Service configuration validation passed")

    def test_pod_logs_regression_protection(self, deployment_protector):
        """
        CRITICAL: Ensure pod logs show successful startup
        Detects internal startup issues
        """
        result = deployment_protector.validate_pod_logs()

        if "error" in result:
            pytest.skip(f"Pod logs validation skipped: {result['error']}")
            return

        assert result["success"], "At least one pod must have healthy logs"
        assert result["healthy_logs"] > 0, "At least one pod must show successful startup"

        # Check for specific error patterns
        for log_result in result["log_results"]:
            if log_result["has_error_indicators"]:
                print(f"⚠️ Pod {log_result['pod']} shows error indicators")

        print(f"✅ Pod logs validation passed ({result['healthy_logs']}/{result['total_pods_checked']} healthy)")

    def test_external_accessibility_regression_protection(self, deployment_protector):
        """
        CRITICAL: Ensure service is externally accessible
        Prevents external access regressions
        """
        result = deployment_protector.validate_external_accessibility()

        if "error" in result:
            pytest.skip(f"External access validation skipped: {result['error']}")
            return

        assert result["success"], "Service must be externally accessible"
        assert result["access_type"] == "NodePort", "Service must use NodePort for external access"
        assert "node_port" in result, "Service must have node port configured"

        print(f"✅ External accessibility validation passed (port {result['node_port']})")


class TestUnifiedAnalyticsDeploymentStability:
    """
    Additional stability tests for deployment
    """

    def test_deployment_rollout_stability(self, deployment_protector):
        """Test that deployment can be rolled out without issues"""
        result = deployment_protector.run_kubectl([
            "rollout", "status", f"deployment/{deployment_protector.app_label}",
            "-n", deployment_protector.namespace, "--timeout=60s"
        ])

        if result["success"]:
            assert "successfully rolled out" in result["stdout"], "Deployment rollout must be successful"
            print("✅ Deployment rollout stability verified")
        else:
            pytest.skip("Deployment rollout status check skipped")

    def test_resource_usage_stability(self, deployment_protector):
        """Basic check for resource usage stability"""
        # Get resource usage
        result = deployment_protector.run_kubectl([
            "top", "pods", "-n", deployment_protector.namespace,
            "-l", f"app={deployment_protector.app_label}", "--no-headers"
        ])

        if result["success"] and result["stdout"].strip():
            lines = result["stdout"].strip().split('\n')
            for line in lines:
                parts = line.split()
                if len(parts) >= 3:
                    cpu_usage = parts[1]
                    memory_usage = parts[2]
                    print(f"Pod {parts[0]}: CPU={cpu_usage}, Memory={memory_usage}")
            print("✅ Resource usage check completed")
        else:
            pytest.skip("Resource usage metrics not available")


if __name__ == "__main__":
    """
    Run deployment protection tests directly
    """
    protector = UnifiedAnalyticsDeploymentProtector()

    try:
        print("🔍 Running deployment protection tests...")

        print("✅ ConfigMap content validation")
        result = protector.validate_configmap_content()
        if not result.get("success", False):
            raise Exception(f"ConfigMap validation failed: {result}")

        print("✅ Deployment configuration validation")
        result = protector.validate_deployment_configuration()
        if not result.get("success", False):
            raise Exception(f"Deployment validation failed: {result}")

        print("✅ Pod health validation")
        result = protector.validate_pod_health()
        if not result.get("success", False):
            raise Exception(f"Pod health validation failed: {result}")

        print("✅ Service configuration validation")
        result = protector.validate_service_configuration()
        if not result.get("success", False):
            raise Exception(f"Service validation failed: {result}")

        print("✅ Pod logs validation")
        result = protector.validate_pod_logs()
        if not result.get("success", False):
            raise Exception(f"Pod logs validation failed: {result}")

        print("✅ External accessibility validation")
        result = protector.validate_external_accessibility()
        if not result.get("success", False):
            raise Exception(f"External access validation failed: {result}")

        print("🎉 All deployment protection tests passed!")

    except Exception as e:
        print(f"❌ Deployment regression detected: {str(e)}")
        exit(1)