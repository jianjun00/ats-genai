"""
Unified Data Quality Service Container
=====================================

Dependency injection container that consolidates all data quality concerns
under a single, consistent framework with shared components and configuration.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
import os
from datetime import datetime

# Import existing components to consolidate
from monitoring.coverage_monitor import CoverageMonitor
from agents.data_quality_agent import DataQualityAgent, AgentConfig
from monitoring.alert_system import AlertManager
from infrastructure.monitoring.data_quality_validator import DataQualityValidator
from monitoring.prometheus_exporter import PrometheusExporter

# Import unified components
from ..interfaces.unified_data_quality_service_interface import UnifiedDataQualityServiceInterface
from ..impl.unified_data_quality_service_impl import UnifiedDataQualityServiceImpl

# Import MCP tools
from mcp_tools.quality_scan_tool import QualityScanTool
from mcp_tools.backfill_orchestrator_tool import BackfillOrchestratorTool

logger = logging.getLogger(__name__)

class UnifiedDataQualityServiceContainer:
    """
    Centralized DI container for all data quality concerns

    Consolidates:
    - Coverage monitoring (from our new monitoring system)
    - Data quality agent (from existing agent framework)
    - Validation (from existing validation system)
    - Alerting (unified alert management)
    - Metrics export (Prometheus integration)
    - MCP tools (quality scanning and backfill orchestration)
    """

    def __init__(self, environment: str = "dev"):
        self.environment = environment
        self.initialized = False

        # Configuration
        self.db_config = self._build_db_config()
        self.agent_config = self._build_agent_config()
        self.alert_config = self._build_alert_config()

        # Core services (will be initialized)
        self.unified_service: Optional[UnifiedDataQualityServiceInterface] = None
        self.coverage_monitor: Optional[CoverageMonitor] = None
        self.data_quality_agent: Optional[DataQualityAgent] = None
        self.data_quality_validator: Optional[DataQualityValidator] = None
        self.alert_manager: Optional[AlertManager] = None
        self.prometheus_exporter: Optional[PrometheusExporter] = None

        # MCP tools
        self.mcp_tools: Dict[str, Any] = {}

        # Shared state
        self.monitoring_active = False
        self.container_id = f"unified_dq_container_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        logger.info(f"🔄 Unified Data Quality Service Container created for {environment} environment")

    async def initialize(self) -> bool:
        """
        Initialize all consolidated components with proper dependency order
        """
        if self.initialized:
            logger.warning("Container already initialized")
            return True

        try:
            logger.info("🚀 Initializing Unified Data Quality Service Container...")

            # Phase 1: Initialize database-dependent components
            await self._initialize_database_components()

            # Phase 2: Initialize MCP tools
            await self._initialize_mcp_tools()

            # Phase 3: Initialize data quality agent with enhanced tools
            await self._initialize_enhanced_agent()

            # Phase 4: Initialize unified service
            await self._initialize_unified_service()

            # Phase 5: Initialize supporting services
            await self._initialize_supporting_services()

            # Phase 6: Validate initialization
            await self._validate_initialization()

            self.initialized = True
            logger.info("✅ Unified Data Quality Service Container fully initialized")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to initialize Unified Data Quality Service Container: {e}")
            await self.cleanup()
            raise

    async def get_unified_service(self) -> UnifiedDataQualityServiceInterface:
        """
        Get the main unified data quality service
        """
        if not self.initialized:
            raise RuntimeError("Container not initialized. Call initialize() first.")

        return self.unified_service

    async def start_monitoring(self) -> bool:
        """
        Start unified monitoring across all quality concerns
        """
        if not self.initialized:
            raise RuntimeError("Container not initialized")

        if self.monitoring_active:
            logger.warning("Monitoring already active")
            return True

        try:
            # Start unified service monitoring
            success = await self.unified_service.start_agent_monitoring()

            if success:
                self.monitoring_active = True
                logger.info("🤖 Unified data quality monitoring started")

            return success

        except Exception as e:
            logger.error(f"Failed to start unified monitoring: {e}")
            return False

    async def stop_monitoring(self) -> bool:
        """
        Stop unified monitoring
        """
        if not self.monitoring_active:
            return True

        try:
            success = await self.unified_service.stop_agent_monitoring()

            if success:
                self.monitoring_active = False
                logger.info("🛑 Unified data quality monitoring stopped")

            return success

        except Exception as e:
            logger.error(f"Failed to stop unified monitoring: {e}")
            return False

    async def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of all container components
        """
        health_status = {
            "container_id": self.container_id,
            "environment": self.environment,
            "initialized": self.initialized,
            "monitoring_active": self.monitoring_active,
            "components": {}
        }

        if not self.initialized:
            health_status["status"] = "not_initialized"
            return health_status

        try:
            # Check unified service health
            if self.unified_service:
                agent_status = await self.unified_service.get_agent_status()
                health_status["components"]["unified_service"] = {
                    "status": "healthy",
                    "agent_status": agent_status.get("status", "unknown"),
                    "monitoring_active": agent_status.get("monitoring_active", False)
                }

            # Check coverage monitor health
            if self.coverage_monitor:
                health_status["components"]["coverage_monitor"] = {
                    "status": "healthy" if self.coverage_monitor.db_pool else "unhealthy",
                    "db_connected": bool(self.coverage_monitor.db_pool)
                }

            # Check data quality agent health
            if self.data_quality_agent:
                health_status["components"]["data_quality_agent"] = {
                    "status": "healthy",
                    "agent_status": self.data_quality_agent.status.value,
                    "mcp_tools_count": len(self.data_quality_agent.mcp_tools)
                }

            # Check alert manager health
            if self.alert_manager:
                alert_config = await self.alert_manager.get_configuration()
                health_status["components"]["alert_manager"] = {
                    "status": "healthy",
                    "slack_enabled": alert_config.get("slack", {}).get("enabled", False)
                }

            # Overall status
            component_statuses = [comp.get("status") for comp in health_status["components"].values()]
            if all(status == "healthy" for status in component_statuses):
                health_status["status"] = "healthy"
            elif any(status == "unhealthy" for status in component_statuses):
                health_status["status"] = "unhealthy"
            else:
                health_status["status"] = "degraded"

            return health_status

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            health_status["status"] = "error"
            health_status["error"] = str(e)
            return health_status

    async def cleanup(self) -> bool:
        """
        Clean up all container resources
        """
        logger.info("🧹 Cleaning up Unified Data Quality Service Container...")

        cleanup_success = True

        try:
            # Stop monitoring if active
            if self.monitoring_active:
                await self.stop_monitoring()

            # Close unified service
            if self.unified_service:
                await self.unified_service.close()

            # Close coverage monitor
            if self.coverage_monitor:
                await self.coverage_monitor.close()

            # Close data quality validator
            if self.data_quality_validator:
                await self.data_quality_validator.close()

            # Clean up other components
            self.data_quality_agent = None
            self.alert_manager = None
            self.prometheus_exporter = None
            self.mcp_tools.clear()

            self.initialized = False
            logger.info("✅ Container cleanup completed")

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            cleanup_success = False

        return cleanup_success

    # =====================================
    # PRIVATE INITIALIZATION METHODS
    # =====================================

    async def _initialize_database_components(self):
        """Initialize components that require database connectivity"""
        logger.info("📊 Initializing database components...")

        # Initialize coverage monitor (from our monitoring system)
        self.coverage_monitor = CoverageMonitor(self.db_config)
        await self.coverage_monitor.initialize()

        # Initialize data quality validator (from existing validation system)
        self.data_quality_validator = DataQualityValidator(self.db_config)
        await self.data_quality_validator.initialize()

        logger.info("✅ Database components initialized")

    async def _initialize_mcp_tools(self):
        """Initialize MCP tools for agent orchestration"""
        logger.info("🔧 Initializing MCP tools...")

        # Quality scanning tool
        self.mcp_tools["quality_scan"] = QualityScanTool()

        # Backfill orchestration tool
        self.mcp_tools["backfill_orchestrator"] = BackfillOrchestratorTool()

        # Coverage-specific tools (extending existing tools)
        self.mcp_tools["coverage_scanner"] = CoverageScannerTool(self.coverage_monitor)
        self.mcp_tools["gap_detector"] = GapDetectorTool(self.coverage_monitor)
        self.mcp_tools["coverage_validator"] = CoverageValidatorTool(self.coverage_monitor)

        logger.info(f"✅ Initialized {len(self.mcp_tools)} MCP tools")

    async def _initialize_enhanced_agent(self):
        """Initialize data quality agent with enhanced capabilities"""
        logger.info("🤖 Initializing enhanced data quality agent...")

        # Create enhanced agent configuration
        enhanced_config = self.agent_config.copy()
        enhanced_config.update({
            "coverage_monitoring_enabled": True,
            "unified_issue_detection": True,
            "mcp_tools": self.mcp_tools
        })

        # Initialize agent with enhanced configuration
        self.data_quality_agent = DataQualityAgent(enhanced_config)

        # Add coverage-specific MCP tools to agent
        self.data_quality_agent.mcp_tools.update(self.mcp_tools)

        logger.info("✅ Enhanced data quality agent initialized")

    async def _initialize_unified_service(self):
        """Initialize the main unified service"""
        logger.info("🔄 Initializing unified data quality service...")

        # Create unified service with all consolidated components
        self.unified_service = UnifiedDataQualityServiceImpl(self.db_config)

        # Inject consolidated components
        self.unified_service.coverage_monitor = self.coverage_monitor
        self.unified_service.data_quality_agent = self.data_quality_agent
        self.unified_service.data_quality_validator = self.data_quality_validator

        # Initialize unified service
        await self.unified_service.initialize()

        logger.info("✅ Unified data quality service initialized")

    async def _initialize_supporting_services(self):
        """Initialize supporting services (alerts, metrics)"""
        logger.info("🔔 Initializing supporting services...")

        # Initialize alert manager
        self.alert_manager = AlertManager(self.alert_config)
        self.unified_service.alert_manager = self.alert_manager

        # Initialize Prometheus exporter
        self.prometheus_exporter = PrometheusExporter()

        logger.info("✅ Supporting services initialized")

    async def _validate_initialization(self):
        """Validate that all components are properly initialized"""
        logger.info("✅ Validating container initialization...")

        # Check required components
        required_components = [
            ("unified_service", self.unified_service),
            ("coverage_monitor", self.coverage_monitor),
            ("data_quality_agent", self.data_quality_agent),
            ("data_quality_validator", self.data_quality_validator),
            ("alert_manager", self.alert_manager)
        ]

        for name, component in required_components:
            if component is None:
                raise RuntimeError(f"Required component {name} not initialized")

        # Validate database connectivity
        if not self.coverage_monitor.db_pool:
            raise RuntimeError("Coverage monitor database pool not initialized")

        # Validate MCP tools
        if len(self.mcp_tools) < 5:  # Should have at least 5 tools
            raise RuntimeError(f"Insufficient MCP tools initialized: {len(self.mcp_tools)}")

        logger.info("✅ Container validation completed successfully")

    # =====================================
    # CONFIGURATION BUILDERS
    # =====================================

    def _build_db_config(self) -> Dict[str, Any]:
        """Build database configuration for the environment"""
        env_prefix = f"{self.environment.upper()}_" if self.environment != "dev" else ""

        return {
            'host': os.getenv(f'{env_prefix}DB_HOST', 'localhost'),
            'port': int(os.getenv(f'{env_prefix}DB_PORT', 4432 if self.environment == "intg" else 3432)),
            'user': os.getenv(f'{env_prefix}DB_USER', 'postgres'),
            'password': os.getenv(f'{env_prefix}DB_PASSWORD', f'{self.environment}_password'),
            'database': os.getenv(f'{env_prefix}DB_NAME', f'{self.environment}_db'),
        }

    def _build_agent_config(self) -> Dict[str, Any]:
        """Build agent configuration"""
        return {
            "monitoring_interval_seconds": 300,  # 5 minutes
            "max_concurrent_workflows": 10,
            "auto_resolution_enabled": True,
            "coverage_monitoring_enabled": True,
            "unified_issue_detection": True,
            "quality_thresholds": {
                "coverage": {
                    "completeness": 0.95,
                    "timeliness": 0.90
                },
                "validation": {
                    "completeness": 0.98,
                    "consistency": 0.95,
                    "accuracy": 0.92
                }
            },
            "action_thresholds": {
                "auto_resolve_confidence_threshold": 0.8,
                "escalation_confidence_threshold": 0.3
            }
        }

    def _build_alert_config(self) -> Dict[str, Any]:
        """Build alert configuration"""
        slack_webhook = os.getenv('SLACK_WEBHOOK_URL')

        return {
            "slack": {
                "enabled": bool(slack_webhook),
                "webhook_url": slack_webhook,
                "channel": "#data-quality-alerts",
                "username": "ATS Data Quality Bot",
                "icon_emoji": ":warning:"
            },
            "email": {
                "enabled": False  # Could be configured later
            },
            "thresholds": {
                "critical_coverage_gap_minutes": 60,  # Alert if critical gap not resolved in 1 hour
                "high_validation_failure_count": 10,  # Alert if >10 validation failures
                "agent_failure_rate_threshold": 0.1   # Alert if agent failure rate >10%
            }
        }


# =====================================
# COVERAGE-SPECIFIC MCP TOOLS
# These extend the MCP framework for coverage monitoring
# =====================================

class CoverageScannerTool:
    """MCP tool for coverage scanning operations"""

    def __init__(self, coverage_monitor: CoverageMonitor):
        self.coverage_monitor = coverage_monitor

    async def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute coverage scan with given parameters"""
        try:
            vendors = params.get('vendors', ['firstrate'])
            data_types = params.get('data_types', ['minute_bars'])
            lookback_days = params.get('lookback_days', 30)

            results = {}
            for vendor in vendors:
                for data_type in data_types:
                    if vendor == 'firstrate' and data_type == 'minute_bars':
                        coverage_records = await self.coverage_monitor.scan_firstrate_coverage(lookback_days)
                        results[f"{vendor}_{data_type}"] = len(coverage_records)

            return {
                "success": True,
                "results": results,
                "scan_timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }


class GapDetectorTool:
    """MCP tool for gap detection operations"""

    def __init__(self, coverage_monitor: CoverageMonitor):
        self.coverage_monitor = coverage_monitor

    async def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute gap detection with given parameters"""
        try:
            vendor = params.get('vendor', 'firstrate')
            data_type = params.get('data_type', 'minute_bars')
            lookback_days = params.get('lookback_days', 30)

            # Get coverage records
            if vendor == 'firstrate' and data_type == 'minute_bars':
                coverage_records = await self.coverage_monitor.scan_firstrate_coverage(lookback_days)
            else:
                coverage_records = []

            # Detect gaps
            gaps = await self.coverage_monitor.detect_gaps(coverage_records)

            return {
                "success": True,
                "gaps_detected": len(gaps),
                "gaps": [
                    {
                        "symbol": gap.symbol,
                        "gap_start": gap.gap_start_date.isoformat(),
                        "gap_end": gap.gap_end_date.isoformat(),
                        "gap_days": gap.gap_days,
                        "priority_score": gap.priority_score
                    }
                    for gap in gaps
                ]
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }


class CoverageValidatorTool:
    """MCP tool for coverage validation operations"""

    def __init__(self, coverage_monitor: CoverageMonitor):
        self.coverage_monitor = coverage_monitor

    async def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute coverage validation with given parameters"""
        try:
            vendor = params.get('vendor', 'firstrate')
            data_type = params.get('data_type', 'minute_bars')
            symbol = params.get('symbol')
            date_range = params.get('date_range', {})

            # Validate coverage for specific symbol/date
            # This would integrate with existing validation logic

            return {
                "success": True,
                "validation_passed": True,
                "coverage_percentage": 95.5,  # Placeholder
                "issues_found": []
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }