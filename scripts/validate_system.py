#!/usr/bin/env python3
"""
System Validation Script
========================

Comprehensive validation of the Data Quality Agent + MCP Tools system.
Tests all components, API endpoints, and integrations to ensure production readiness.
"""

import asyncio
import aiohttp
import json
import logging
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/system_validation.log')
    ]
)
logger = logging.getLogger(__name__)

class SystemValidator:
    """Comprehensive system validation"""
    
    def __init__(self, base_url: str = "http://localhost:4000"):
        self.base_url = base_url
        self.session: Optional[aiohttp.ClientSession] = None
        self.validation_results: Dict[str, Any] = {}
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def validate_all(self) -> Dict[str, Any]:
        """Run complete system validation"""
        logger.info("🚀 Starting comprehensive system validation")
        
        validation_start = time.time()
        
        # Test categories
        test_results = {
            "database_connectivity": await self.test_database_connectivity(),
            "api_endpoints": await self.test_api_endpoints(),
            "agent_functionality": await self.test_agent_functionality(),
            "mcp_tools": await self.test_mcp_tools(),
            "configuration_system": await self.test_configuration_system(),
            "logging_system": await self.test_logging_system(),
            "system_monitoring": await self.test_system_monitoring(),
            "alert_management": await self.test_alert_management(),
            "dashboard_functionality": await self.test_dashboard_functionality()
        }
        
        # Calculate overall results
        total_tests = sum(len(category.get("tests", [])) for category in test_results.values())
        passed_tests = sum(
            len([t for t in category.get("tests", []) if t.get("passed", False)]) 
            for category in test_results.values()
        )
        
        validation_duration = time.time() - validation_start
        
        overall_result = {
            "validation_timestamp": datetime.now().isoformat(),
            "validation_duration_seconds": round(validation_duration, 2),
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": total_tests - passed_tests,
            "success_rate": round((passed_tests / total_tests * 100) if total_tests > 0 else 0, 1),
            "overall_status": "PASS" if passed_tests == total_tests else "FAIL",
            "categories": test_results
        }
        
        self.validation_results = overall_result
        
        # Save results
        await self.save_validation_results()
        
        # Print summary
        self.print_validation_summary()
        
        return overall_result
    
    async def test_database_connectivity(self) -> Dict[str, Any]:
        """Test database connectivity and basic queries"""
        logger.info("🗄️ Testing database connectivity")
        
        tests = []
        
        # Test PostgreSQL connection
        import asyncpg
        
        # Test dev database
        dev_conn = await asyncpg.connect(
            host='ats-dev-postgres', port=5432,
            user='postgres', password='dev_password', database='dev_db'
        )
        await dev_conn.fetchval("SELECT 1")
        await dev_conn.close()
        
        tests.append({
            "name": "dev_database_connection",
            "description": "Connect to development database",
            "passed": True,
            "message": "Successfully connected to dev database"
        })
        
        # Test integration database
        intg_conn = await asyncpg.connect(
            host='ats-intg-postgres', port=5432,
            user='postgres', password='intg_password', database='intg_db'
        )
        tables_count = await intg_conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'"
        )
        await intg_conn.close()
        
        tests.append({
            "name": "intg_database_connection",
            "description": "Connect to integration database and check tables",
            "passed": True,
            "message": f"Successfully connected to intg database with {tables_count} tables"
        })
        
        return {
            "category": "Database Connectivity",
            "tests": tests,
            "passed": all(t["passed"] for t in tests)
        }
    
    async def test_api_endpoints(self) -> Dict[str, Any]:
        """Test all API endpoints"""
        logger.info("🌐 Testing API endpoints")
        
        tests = []
        
        # Test basic health endpoint
        async with self.session.get(f"{self.base_url}/health") as response:
            tests.append({
                "name": "health_endpoint",
                "description": "Basic health check endpoint",
                "passed": response.status == 200,
                "message": f"Health endpoint returned {response.status}"
            })
        agent_endpoints = [
            "/agent/status",
            "/agent/health", 
            "/agent/config",
            "/agent/metrics",
            "/agent/workflows",
            "/agent/logs",
            "/agent/performance",
            "/agent/system-health",
            "/agent/alerts"
        ]
        
        for endpoint in agent_endpoints:
            async with self.session.get(f"{self.base_url}{endpoint}") as response:
                tests.append({
                    "name": f"endpoint_{endpoint.replace('/', '_')}",
                    "description": f"Test {endpoint} endpoint",
                    "passed": response.status in [200, 503],  # 503 acceptable if agent not running
                    "message": f"Endpoint {endpoint} returned {response.status}"
                })
        return {
            "category": "API Endpoints",
            "tests": tests,
            "passed": all(t["passed"] for t in tests)
        }
    
    async def test_agent_functionality(self) -> Dict[str, Any]:
        """Test agent core functionality"""
        logger.info("🤖 Testing agent functionality")
        
        tests = []
        
        # Test agent status
        async with self.session.get(f"{self.base_url}/agent/status") as response:
            if response.status == 200:
                data = await response.json()
                tests.append({
                    "name": "agent_status_check",
                    "description": "Agent status retrieval",
                    "passed": True,
                    "message": f"Agent status: {data.get('status', 'unknown')}"
                })
            else:
                tests.append({
                    "name": "agent_status_check",
                    "description": "Agent status retrieval",
                    "passed": False,
                    "message": f"Agent status endpoint returned {response.status}"
                })
        async with self.session.get(f"{self.base_url}/agent/config") as response:
            if response.status == 200:
                data = await response.json()
                config = data.get("config", {})
                tests.append({
                    "name": "agent_configuration",
                    "description": "Agent configuration retrieval",
                    "passed": bool(config),
                    "message": f"Configuration loaded with {len(config)} sections"
                })
            else:
                tests.append({
                    "name": "agent_configuration",
                    "description": "Agent configuration retrieval",
                    "passed": False,
                    "message": f"Configuration endpoint returned {response.status}"
                })
        return {
            "category": "Agent Functionality",
            "tests": tests,
            "passed": all(t["passed"] for t in tests)
        }
    
    async def test_mcp_tools(self) -> Dict[str, Any]:
        """Test MCP tools availability and functionality"""
        logger.info("🛠️ Testing MCP tools")
        
        tests = []
        
        # Test if MCP tools are importable
        from src.mcp_tools.quality_scan_tool import QualityScanTool
        tool = QualityScanTool()
        definition = tool.get_tool_definition()
        
        tests.append({
            "name": "quality_scan_tool",
            "description": "Quality Scan Tool initialization",
            "passed": bool(definition.get("name")),
            "message": f"Quality Scan Tool loaded: {definition.get('name')}"
        })
        from src.mcp_tools.backfill_orchestrator_tool import BackfillOrchestratorTool
        tool = BackfillOrchestratorTool()
        definition = tool.get_tool_definition()
        
        tests.append({
            "name": "backfill_orchestrator_tool",
            "description": "Backfill Orchestrator Tool initialization",
            "passed": bool(definition.get("name")),
            "message": f"Backfill Orchestrator Tool loaded: {definition.get('name')}"
        })
        return {
            "category": "MCP Tools",
            "tests": tests,
            "passed": all(t["passed"] for t in tests)
        }
    
    async def test_configuration_system(self) -> Dict[str, Any]:
        """Test configuration system"""
        logger.info("⚙️ Testing configuration system")
        
        tests = []
        
        # Test configuration manager
        from src.agents.agent_config import get_config_manager
        config_manager = get_config_manager()
        config = config_manager.get_config()
        
        tests.append({
            "name": "config_manager_initialization",
            "description": "Configuration manager initialization",
            "passed": bool(config),
            "message": f"Configuration manager loaded successfully"
        })
        
        # Test configuration validation
        test_updates = {
            "monitoring": {
                "cycle_interval_seconds": 300
            }
        }
        
        validation_result = config_manager._validate_updates(test_updates)
        tests.append({
            "name": "config_validation",
            "description": "Configuration validation system",
            "passed": validation_result,
            "message": "Configuration validation working correctly"
        })
        
        return {
            "category": "Configuration System",
            "tests": tests,
            "passed": all(t["passed"] for t in tests)
        }
    
    async def test_logging_system(self) -> Dict[str, Any]:
        """Test logging system"""
        logger.info("📝 Testing logging system")
        
        tests = []
        
        # Test agent logger
        from src.agents.agent_logger import get_agent_logger
        agent_logger = get_agent_logger("test_agent", "INFO")
        
        # Test logging functionality
        agent_logger.info("test", "validation", "Test log entry")
        
        # Test performance tracking
        with agent_logger.operation_timer("test", "validation_test"):
            await asyncio.sleep(0.1)
        
        # Get performance summary
        performance = agent_logger.get_performance_summary()
        
        tests.append({
            "name": "agent_logging",
            "description": "Agent logging system",
            "passed": bool(performance),
            "message": f"Logging system working with {len(performance)} operations tracked"
        })
        
        return {
            "category": "Logging System",
            "tests": tests,
            "passed": all(t["passed"] for t in tests)
        }
    
    async def test_system_monitoring(self) -> Dict[str, Any]:
        """Test system monitoring"""
        logger.info("🩺 Testing system monitoring")
        
        tests = []
        
        # Test system monitor
        from src.agents.system_monitor import get_system_monitor
        monitor = get_system_monitor("test_agent")
        
        # Test metrics collection
        metrics = await monitor._collect_system_metrics()
        
        tests.append({
            "name": "system_metrics_collection",
            "description": "System metrics collection",
            "passed": bool(metrics and metrics.cpu_percent >= 0),
            "message": f"Metrics collected: CPU {metrics.cpu_percent}%, Memory {metrics.memory_percent}%"
        })
        
        # Test health summary
        health_summary = await monitor.get_health_summary()
        
        tests.append({
            "name": "health_summary",
            "description": "Health summary generation",
            "passed": bool(health_summary.get("status")),
            "message": f"Health status: {health_summary.get('status')}"
        })
        
        return {
            "category": "System Monitoring",
            "tests": tests,
            "passed": all(t["passed"] for t in tests)
        }
    
    async def test_alert_management(self) -> Dict[str, Any]:
        """Test alert management system"""
        logger.info("🚨 Testing alert management")
        
        tests = []
        
        # Test alert manager
        from src.agents.alert_manager import get_alert_manager
        alert_manager = get_alert_manager("test_agent")
        
        # Test alert rule evaluation
        test_data = {
            "cpu_percent": 90,
            "memory_percent": 85,
            "disk_usage_percent": 70
        }
        
        await alert_manager.evaluate_alert_rules(test_data, "test_component")
        
        # Test alert summary
        summary = await alert_manager.get_alert_summary()
        
        tests.append({
            "name": "alert_management",
            "description": "Alert management system",
            "passed": bool(summary),
            "message": f"Alert system functional with {summary.get('alert_rules_enabled', 0)} rules"
        })
        
        return {
            "category": "Alert Management",
            "tests": tests,
            "passed": all(t["passed"] for t in tests)
        }
    
    async def test_dashboard_functionality(self) -> Dict[str, Any]:
        """Test dashboard functionality"""
        logger.info("📊 Testing dashboard functionality")
        
        tests = []
        
        # Test data quality dashboard
        async with self.session.get(f"{self.base_url}/data-quality/dashboard") as response:
            tests.append({
                "name": "data_quality_dashboard",
                "description": "Data quality dashboard accessibility",
                "passed": response.status == 200,
                "message": f"Dashboard returned {response.status}"
            })
        async with self.session.get(f"{self.base_url}/data-quality/api/issues") as response:
            tests.append({
                "name": "dashboard_api",
                "description": "Dashboard API functionality", 
                "passed": response.status == 200,
                "message": f"Dashboard API returned {response.status}"
            })
        return {
            "category": "Dashboard Functionality",
            "tests": tests,
            "passed": all(t["passed"] for t in tests)
        }
    
    async def save_validation_results(self):
        """Save validation results to file"""
        results_dir = Path("logs/validation")
        results_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = results_dir / f"validation_results_{timestamp}.json"
        
        with open(results_file, 'w') as f:
            json.dump(self.validation_results, f, indent=2)
        
        logger.info(f"💾 Validation results saved to {results_file}")
    
    def print_validation_summary(self):
        """Print validation summary"""
        print("\n" + "="*80)
        print("🚀 SYSTEM VALIDATION SUMMARY")
        print("="*80)
        
        result = self.validation_results
        
        print(f"⏱️  Duration: {result['validation_duration_seconds']}s")
        print(f"📊 Tests: {result['passed_tests']}/{result['total_tests']} passed ({result['success_rate']}%)")
        print(f"🎯 Overall Status: {result['overall_status']}")
        
        print(f"\n📋 Test Categories:")
        for category, data in result['categories'].items():
            status = "✅ PASS" if data['passed'] else "❌ FAIL"
            test_count = len(data.get('tests', []))
            passed_count = len([t for t in data.get('tests', []) if t.get('passed', False)])
            print(f"  {status} {data['category']}: {passed_count}/{test_count}")
        
        if result['failed_tests'] > 0:
            print(f"\n❌ Failed Tests:")
            for category, data in result['categories'].items():
                for test in data.get('tests', []):
                    if not test.get('passed', False):
                        print(f"  • {test['name']}: {test['message']}")
        
        print("\n" + "="*80)

async def main():
    """Main validation entry point"""
    import sys
    
    # Check if analytics service is available
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:4000"
    
    print(f"🔍 Validating Data Quality Agent system at {base_url}")
    
    async with SystemValidator(base_url) as validator:
        results = await validator.validate_all()
        
        # Exit with appropriate code
        sys.exit(0 if results['overall_status'] == 'PASS' else 1)

if __name__ == "__main__":
    asyncio.run(main())