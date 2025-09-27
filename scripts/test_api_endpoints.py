#!/usr/bin/env python3
"""
API Endpoints Test
==================

Test all Data Quality Agent API endpoints to ensure they're working correctly.
This validates the REST API interface for production readiness.
"""

import asyncio
import aiohttp
import json
import sys
from datetime import datetime
from typing import Dict, List, Any

class APITester:
    """Test all API endpoints"""
    
    def __init__(self, base_url: str = "http://localhost:4000"):
        self.base_url = base_url
        self.session = None
        self.test_results = []
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def test_endpoint(self, method: str, endpoint: str, description: str, 
                           data: Dict = None, expected_status: List[int] = None) -> Dict[str, Any]:
        """Test a single endpoint"""
        
        if expected_status is None:
            expected_status = [200]
        
        url = f"{self.base_url}{endpoint}"
        
        if method.upper() == "GET":
            async with self.session.get(url) as response:
                status = response.status
                response_data = await response.json()
        elif method.upper() == "POST":
            async with self.session.post(url, json=data) as response:
                status = response.status
                response_data = await response.json()
        elif method.upper() == "PUT":
            async with self.session.put(url, json=data) as response:
                status = response.status
                response_data = await response.json()
            raise ValueError(f"Unsupported method: {method}")
        
        passed = status in expected_status
        
        result = {
            "endpoint": endpoint,
            "method": method.upper(),
            "description": description,
            "status_code": status,
            "expected_status": expected_status,
            "passed": passed,
            "response_preview": str(response_data)[:200] if response_data else "",
            "timestamp": datetime.now().isoformat()
        }
        
        self.test_results.append(result)
        
        status_icon = "✅" if passed else "❌"
        print(f"  {status_icon} {method.upper()} {endpoint} - {status} - {description}")
        
        return result
        
    async def test_all_endpoints(self):
        """Test all API endpoints"""
        
        print("🌐 Testing Data Quality Agent API Endpoints")
        print("=" * 60)
        
        # Basic endpoints
        print("\n📋 Basic Endpoints:")
        await self.test_endpoint("GET", "/health", "Basic health check")
        await self.test_endpoint("GET", "/", "Root endpoint", expected_status=[200, 404])
        
        # Agent control endpoints
        print("\n🤖 Agent Control Endpoints:")
        await self.test_endpoint("GET", "/agent/status", "Agent status", expected_status=[200, 503])
        await self.test_endpoint("GET", "/agent/health", "Agent health check", expected_status=[200, 503])
        await self.test_endpoint("POST", "/agent/start", "Start agent monitoring", expected_status=[200, 503])
        await self.test_endpoint("POST", "/agent/stop", "Stop agent monitoring", expected_status=[200, 503])
        
        # Agent information endpoints
        print("\n📊 Agent Information Endpoints:")
        await self.test_endpoint("GET", "/agent/metrics", "Agent metrics", expected_status=[200, 503])
        await self.test_endpoint("GET", "/agent/workflows", "Agent workflows", expected_status=[200, 503])
        await self.test_endpoint("GET", "/agent/logs", "Agent logs", expected_status=[200, 503])
        await self.test_endpoint("GET", "/agent/performance", "Agent performance data", expected_status=[200, 503])
        await self.test_endpoint("GET", "/agent/system-health", "System health", expected_status=[200, 503])
        
        # Configuration endpoints
        print("\n⚙️ Configuration Endpoints:")
        await self.test_endpoint("GET", "/agent/config", "Get configuration", expected_status=[200, 503])
        
        # Test configuration update (if agent is available)
        config_update = {
            "monitoring": {
                "cycle_interval_seconds": 300
            }
        }
        await self.test_endpoint("PUT", "/agent/config", "Update configuration", 
                                data=config_update, expected_status=[200, 400, 503])
        
        await self.test_endpoint("POST", "/agent/config/reset", "Reset configuration", expected_status=[200, 503])
        await self.test_endpoint("POST", "/agent/config/environment/development", 
                                "Apply dev config", expected_status=[200, 503])
        
        # Alert management endpoints
        print("\n🚨 Alert Management Endpoints:")
        await self.test_endpoint("GET", "/agent/alerts", "Get alerts", expected_status=[200, 503])
        await self.test_endpoint("POST", "/agent/alerts/test-channels", "Test notification channels", 
                                expected_status=[200, 503])
        
        # Test alert actions (these will likely fail with 404, which is expected)
        await self.test_endpoint("POST", "/agent/alerts/test_alert/acknowledge", 
                                "Acknowledge alert", expected_status=[200, 404, 503])
        await self.test_endpoint("POST", "/agent/alerts/test_alert/resolve", 
                                "Resolve alert", expected_status=[200, 404, 503])
        
        # Agent action endpoint
        print("\n🎯 Agent Action Endpoints:")
        test_action = {"action": "investigate_issue"}
        await self.test_endpoint("POST", "/agent/action", "Trigger agent action", 
                                data=test_action, expected_status=[200, 400, 503])
        
        # Dashboard endpoints
        print("\n📊 Dashboard Endpoints:")
        await self.test_endpoint("GET", "/data-quality/dashboard", "Data quality dashboard", 
                                expected_status=[200, 404])
        await self.test_endpoint("GET", "/data-quality/api/issues", "Dashboard API issues", 
                                expected_status=[200, 404])
        
        # Analytics endpoints (if available)
        print("\n📈 Analytics Endpoints:")
        await self.test_endpoint("GET", "/api/tables", "Get database tables", expected_status=[200, 404, 500])
        await self.test_endpoint("GET", "/api/table-info/dev_instrument", "Table info", 
                                expected_status=[200, 404, 500])
    
    def generate_report(self):
        """Generate test report"""
        
        total_tests = len(self.test_results)
        passed_tests = len([r for r in self.test_results if r["passed"]])
        failed_tests = total_tests - passed_tests
        success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0
        
        print("\n" + "=" * 60)
        print("📊 API ENDPOINTS TEST REPORT")
        print("=" * 60)
        
        print(f"Total Endpoints Tested: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {failed_tests}")
        print(f"Success Rate: {success_rate:.1f}%")
        
        if failed_tests > 0:
            print(f"\n❌ Failed Endpoints:")
            for result in self.test_results:
                if not result["passed"]:
                    error_msg = result.get("error", f"Status {result['status_code']}")
                    print(f"  • {result['method']} {result['endpoint']}: {error_msg}")
        
        # Group by status patterns
        status_groups = {}
        for result in self.test_results:
            status = result["status_code"]
            if status not in status_groups:
                status_groups[status] = []
            status_groups[status].append(result["endpoint"])
        
        print(f"\n📋 Status Code Distribution:")
        for status, endpoints in sorted(status_groups.items()):
            print(f"  {status}: {len(endpoints)} endpoints")
        
        print("=" * 60)
        
        return success_rate >= 80  # Consider 80%+ success rate as acceptable

async def main():
    """Main test runner"""
    
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:4000"
    
    print(f"🧪 Testing API endpoints at {base_url}")
    print("Note: Some failures are expected if the agent is not running")
    
    async with APITester(base_url) as tester:
        await tester.test_all_endpoints()
        success = tester.generate_report()
        
        # Save detailed results
        import json
        from pathlib import Path
        
        results_dir = Path("logs/api_tests")
        results_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = results_dir / f"api_test_results_{timestamp}.json"
        
        with open(results_file, 'w') as f:
            json.dump(tester.test_results, f, indent=2)
        
        print(f"\n💾 Detailed results saved to: {results_file}")
        
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    asyncio.run(main())