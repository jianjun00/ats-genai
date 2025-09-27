#!/usr/bin/env python3
"""
Quick Health Check
==================

Rapid validation of core system components to ensure basic functionality.
Use this for quick smoke tests before full validation.
"""

import asyncio
import sys
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

async def quick_health_check():
    """Perform quick health check of core components"""
    
    print("🔍 Quick Health Check - Data Quality Agent System")
    print("=" * 60)
    
    checks_passed = 0
    total_checks = 0
    
    # 1. Test imports
    print("📦 Testing core imports...")
    total_checks += 1
    # Test agent imports
    from src.agents.data_quality_agent import DataQualityAgent
    from src.agents.agent_config import get_config_manager
    from src.agents.agent_logger import get_agent_logger
    from src.agents.system_monitor import get_system_monitor
    from src.agents.alert_manager import get_alert_manager
    
    # Test MCP tools imports
    from src.mcp_tools.quality_scan_tool import QualityScanTool
    from src.mcp_tools.backfill_orchestrator_tool import BackfillOrchestratorTool
    
    print("  ✅ All core imports successful")
    checks_passed += 1
    print("⚙️ Testing configuration system...")
    total_checks += 1
    config_manager = get_config_manager()
    config = config_manager.get_config()
    
    if config and hasattr(config, 'monitoring'):
        print(f"  ✅ Configuration loaded successfully")
        checks_passed += 1
    else:
        print("  ❌ Configuration invalid")
    print("🛠️ Testing MCP tools...")
    total_checks += 1
    quality_tool = QualityScanTool()
    backfill_tool = BackfillOrchestratorTool()
    
    quality_def = quality_tool.get_tool_definition()
    backfill_def = backfill_tool.get_tool_definition()
    
    if quality_def.get('name') and backfill_def.get('name'):
        print(f"  ✅ MCP tools initialized: {quality_def['name']}, {backfill_def['name']}")
        checks_passed += 1
    else:
        print("  ❌ MCP tools initialization failed")
    print("🤖 Testing agent initialization...")
    total_checks += 1
    # Create agent (don't start monitoring)
    agent = DataQualityAgent()
    
    if agent.agent_id and agent.mcp_tools and agent.agent_config:
        print(f"  ✅ Agent initialized: {agent.agent_id}")
        print(f"     Tools: {list(agent.mcp_tools.keys())}")
        checks_passed += 1
    else:
        print("  ❌ Agent initialization incomplete")
    print("📝 Testing logging system...")
    total_checks += 1
    logger_instance = get_agent_logger("health_check", "INFO")
    logger_instance.info("test", "health_check", "Test log message")
    
    # Test performance tracking
    with logger_instance.operation_timer("test", "health_check_timer"):
        await asyncio.sleep(0.01)
    
    performance = logger_instance.get_performance_summary()
    
    if performance and "health_check_timer" in performance:
        print("  ✅ Logging system working")
        checks_passed += 1
    else:
        print("  ❌ Logging system incomplete")
    print("🩺 Testing system monitoring...")
    total_checks += 1
    monitor = get_system_monitor("health_check")
    metrics = await monitor._collect_system_metrics()
    
    if metrics and metrics.cpu_percent >= 0:
        print(f"  ✅ System monitoring working (CPU: {metrics.cpu_percent}%)")
        checks_passed += 1
    else:
        print("  ❌ System monitoring failed")
    print("🚨 Testing alert management...")
    total_checks += 1
    alert_manager = get_alert_manager("health_check")
    
    # Test alert evaluation
    test_data = {"cpu_percent": 50, "memory_percent": 60}
    await alert_manager.evaluate_alert_rules(test_data, "health_check")
    
    summary = await alert_manager.get_alert_summary()
    
    if summary and 'alert_rules_enabled' in summary:
        print(f"  ✅ Alert management working ({summary['alert_rules_enabled']} rules)")
        checks_passed += 1
    else:
        print("  ❌ Alert management incomplete")
    print("🗄️ Testing database connectivity...")
    total_checks += 1
    import asyncpg
    
    # Try to connect to integration database
    conn = await asyncpg.connect(
        host='ats-intg-postgres', port=5432,
        user='postgres', password='intg_password', database='intg_db'
    )
    
    # Simple test query
    result = await conn.fetchval("SELECT 1")
    await conn.close()
    
    if result == 1:
        print("  ✅ Database connectivity working")
        checks_passed += 1
    else:
        print("  ❌ Database query failed")
    print("\n" + "=" * 60)
    print("📊 QUICK HEALTH CHECK SUMMARY")
    print("=" * 60)
    
    success_rate = (checks_passed / total_checks * 100) if total_checks > 0 else 0
    status = "✅ HEALTHY" if checks_passed == total_checks else "⚠️  ISSUES DETECTED"
    
    print(f"Status: {status}")
    print(f"Checks: {checks_passed}/{total_checks} passed ({success_rate:.1f}%)")
    
    if checks_passed == total_checks:
        print("\n🎉 System is ready for production deployment!")
    else:
        print(f"\n⚠️  {total_checks - checks_passed} issues need attention before deployment")
    
    print("=" * 60)
    
    return checks_passed == total_checks

def main():
    """Main entry point"""
    result = asyncio.run(quick_health_check())
    sys.exit(0 if result else 1)

if __name__ == "__main__":
    main()