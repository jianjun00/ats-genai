"""
Agent Configuration System
=========================

Provides configuration management for the Data Quality Agent.
Supports runtime configuration updates and persistent settings.
"""

import json
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict, field
from datetime import timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

@dataclass
class MonitoringConfig:
    """Configuration for agent monitoring behavior"""
    cycle_interval_seconds: int = 300  # 5 minutes
    stall_threshold_minutes: int = 60
    max_concurrent_workflows: int = 10
    health_check_interval_seconds: int = 60
    cleanup_old_workflows_days: int = 30

@dataclass
class IssueThresholds:
    """Thresholds for issue detection and classification"""
    extreme_volume_multiplier: float = 50.0  # 50x average volume
    extreme_price_change_percent: float = 20.0  # 20% price change
    data_staleness_hours: int = 24
    max_missing_consecutive_days: int = 3
    quality_score_critical_threshold: int = 50
    quality_score_warning_threshold: int = 75

@dataclass
class ActionThresholds:
    """Thresholds for automated action decisions"""
    auto_resolve_confidence_threshold: float = 0.85
    escalation_confidence_threshold: float = 0.3
    backfill_auto_trigger_threshold: int = 5  # Missing days before auto-backfill
    cross_validation_vendor_count: int = 2
    max_retry_attempts: int = 3

@dataclass
class VendorConfig:
    """Configuration for vendor-specific behavior"""
    primary_vendors: List[str] = field(default_factory=lambda: ["polygon", "tiingo", "eodhd"])
    secondary_vendors: List[str] = field(default_factory=lambda: ["eodhd", "tiingo"])
    vendor_priorities: Dict[str, int] = field(default_factory=lambda: {
        "polygon": 1, "tiingo": 2, "eodhd": 3
    })
    rate_limits: Dict[str, Dict[str, int]] = field(default_factory=lambda: {
        "polygon": {"requests_per_minute": 5, "requests_per_day": 100},
        "tiingo": {"requests_per_minute": 10, "requests_per_day": 500},
        "eodhd": {"requests_per_minute": 20, "requests_per_day": 1000}
    })

@dataclass
class NotificationConfig:
    """Configuration for alerts and notifications"""
    enable_email_notifications: bool = False
    enable_slack_notifications: bool = False
    email_recipients: List[str] = field(default_factory=list)
    slack_webhook_url: Optional[str] = None
    notification_severity_threshold: str = "high"  # critical, high, medium, low
    max_notifications_per_hour: int = 10

@dataclass
class AgentConfig:
    """Complete agent configuration"""
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    issue_thresholds: IssueThresholds = field(default_factory=IssueThresholds)
    action_thresholds: ActionThresholds = field(default_factory=ActionThresholds)
    vendor_config: VendorConfig = field(default_factory=VendorConfig)
    notifications: NotificationConfig = field(default_factory=NotificationConfig)

    # Agent behavior settings
    enable_autonomous_mode: bool = True
    enable_learning_mode: bool = True
    enable_reflection: bool = True
    log_level: str = "INFO"

    # Performance settings
    max_memory_mb: int = 1024
    max_cpu_percent: float = 50.0
    enable_metrics_collection: bool = True

class AgentConfigManager:
    """Manages agent configuration with persistence and validation"""

    def __init__(self, config_file_path: Optional[str] = None):
        self.config_file_path = Path(config_file_path) if config_file_path else Path("config/agent_config.json")
        self.config = AgentConfig()
        self._load_config()

    def _load_config(self):
        """Load configuration from file if it exists"""
        try:
            if self.config_file_path.exists():
                with open(self.config_file_path, 'r') as f:
                    config_data = json.load(f)
                self.config = self._deserialize_config(config_data)
                logger.info(f"Loaded agent configuration from {self.config_file_path}")
            else:
                logger.info("No configuration file found, using defaults")
                self.save_config()  # Create default config file
        except Exception as e:
            logger.error(f"Error loading configuration: {e}", exc_info=True)
            logger.info("Using default configuration")

    def save_config(self):
        """Save current configuration to file"""
        try:
            self.config_file_path.parent.mkdir(parents=True, exist_ok=True)
            config_data = self._serialize_config(self.config)

            with open(self.config_file_path, 'w') as f:
                json.dump(config_data, f, indent=2)

            logger.info(f"Saved agent configuration to {self.config_file_path}")
        except Exception as e:
            logger.error(f"Error saving configuration: {e}", exc_info=True)

    def _serialize_config(self, config: AgentConfig) -> Dict[str, Any]:
        """Convert configuration to serializable dictionary"""
        return {
            "monitoring": asdict(config.monitoring),
            "issue_thresholds": asdict(config.issue_thresholds),
            "action_thresholds": asdict(config.action_thresholds),
            "vendor_config": asdict(config.vendor_config),
            "notifications": asdict(config.notifications),
            "enable_autonomous_mode": config.enable_autonomous_mode,
            "enable_learning_mode": config.enable_learning_mode,
            "enable_reflection": config.enable_reflection,
            "log_level": config.log_level,
            "max_memory_mb": config.max_memory_mb,
            "max_cpu_percent": config.max_cpu_percent,
            "enable_metrics_collection": config.enable_metrics_collection
        }

    def _deserialize_config(self, config_data: Dict[str, Any]) -> AgentConfig:
        """Convert dictionary to configuration object"""
        return AgentConfig(
            monitoring=MonitoringConfig(**config_data.get("monitoring", {})),
            issue_thresholds=IssueThresholds(**config_data.get("issue_thresholds", {})),
            action_thresholds=ActionThresholds(**config_data.get("action_thresholds", {})),
            vendor_config=VendorConfig(**config_data.get("vendor_config", {})),
            notifications=NotificationConfig(**config_data.get("notifications", {})),
            enable_autonomous_mode=config_data.get("enable_autonomous_mode", True),
            enable_learning_mode=config_data.get("enable_learning_mode", True),
            enable_reflection=config_data.get("enable_reflection", True),
            log_level=config_data.get("log_level", "INFO"),
            max_memory_mb=config_data.get("max_memory_mb", 1024),
            max_cpu_percent=config_data.get("max_cpu_percent", 50.0),
            enable_metrics_collection=config_data.get("enable_metrics_collection", True)
        )

    def update_config(self, updates: Dict[str, Any]) -> bool:
        """Update configuration with new values"""
        try:
            # Validate updates
            if not self._validate_updates(updates):
                return False

            # Apply updates to appropriate sections
            for section_name, section_updates in updates.items():
                if hasattr(self.config, section_name):
                    section = getattr(self.config, section_name)
                    if hasattr(section, '__dict__'):  # It's a dataclass
                        for key, value in section_updates.items():
                            if hasattr(section, key):
                                setattr(section, key, value)
                            else:
                                logger.warning(f"Unknown configuration key: {section_name}.{key}")
                    else:  # It's a direct attribute
                        setattr(self.config, section_name, section_updates)
                else:
                    logger.warning(f"Unknown configuration section: {section_name}")

            # Save updated configuration
            self.save_config()
            logger.info(f"Configuration updated successfully")
            return True

        except Exception as e:
            logger.error(f"Error updating configuration: {e}", exc_info=True)
            return False

    def _validate_updates(self, updates: Dict[str, Any]) -> bool:
        """Validate configuration updates"""
        # Basic validation rules
        validation_rules = {
            "monitoring": {
                "cycle_interval_seconds": lambda x: isinstance(x, int) and 30 <= x <= 3600,
                "max_concurrent_workflows": lambda x: isinstance(x, int) and 1 <= x <= 100,
            },
            "issue_thresholds": {
                "extreme_volume_multiplier": lambda x: isinstance(x, (int, float)) and x > 0,
                "extreme_price_change_percent": lambda x: isinstance(x, (int, float)) and 0 < x <= 100,
            },
            "action_thresholds": {
                "auto_resolve_confidence_threshold": lambda x: isinstance(x, (int, float)) and 0 <= x <= 1,
                "max_retry_attempts": lambda x: isinstance(x, int) and 1 <= x <= 10,
            }
        }

        for section_name, section_updates in updates.items():
            if section_name in validation_rules:
                section_rules = validation_rules[section_name]
                for key, value in section_updates.items():
                    if key in section_rules:
                        if not section_rules[key](value):
                            logger.error(f"Invalid value for {section_name}.{key}: {value}")
                            return False

        return True

    def get_config(self) -> AgentConfig:
        """Get current configuration"""
        return self.config

    def get_config_dict(self) -> Dict[str, Any]:
        """Get current configuration as dictionary"""
        return self._serialize_config(self.config)

    def reset_to_defaults(self):
        """Reset configuration to default values"""
        self.config = AgentConfig()
        self.save_config()
        logger.info("Configuration reset to defaults")

    def export_config(self, export_path: str) -> bool:
        """Export configuration to specified file"""
        try:
            export_file = Path(export_path)
            export_file.parent.mkdir(parents=True, exist_ok=True)

            config_data = self._serialize_config(self.config)
            with open(export_file, 'w') as f:
                json.dump(config_data, f, indent=2)

            logger.info(f"Configuration exported to {export_path}")
            return True
        except Exception as e:
            logger.error(f"Error exporting configuration: {e}", exc_info=True)
            return False

    def import_config(self, import_path: str) -> bool:
        """Import configuration from specified file"""
        try:
            import_file = Path(import_path)
            if not import_file.exists():
                logger.error(f"Import file does not exist: {import_path}")
                return False

            with open(import_file, 'r') as f:
                config_data = json.load(f)

            # Validate imported configuration
            imported_config = self._deserialize_config(config_data)
            self.config = imported_config
            self.save_config()

            logger.info(f"Configuration imported from {import_path}")
            return True
        except Exception as e:
            logger.error(f"Error importing configuration: {e}", exc_info=True)
            return False

    def get_monitoring_interval(self) -> timedelta:
        """Get monitoring cycle interval as timedelta"""
        return timedelta(seconds=self.config.monitoring.cycle_interval_seconds)

    def should_auto_resolve(self, confidence: float) -> bool:
        """Check if issue should be auto-resolved based on confidence"""
        return confidence >= self.config.action_thresholds.auto_resolve_confidence_threshold

    def should_escalate(self, confidence: float) -> bool:
        """Check if issue should be escalated based on confidence"""
        return confidence <= self.config.action_thresholds.escalation_confidence_threshold

    def get_vendor_priority(self, vendor_name: str) -> int:
        """Get priority for specified vendor"""
        return self.config.vendor_config.vendor_priorities.get(vendor_name, 999)

    def is_extreme_volume(self, volume: int, avg_volume: int) -> bool:
        """Check if volume is considered extreme"""
        if avg_volume == 0:
            return False
        return volume > (avg_volume * self.config.issue_thresholds.extreme_volume_multiplier)

    def is_extreme_price_change(self, price_change_percent: float) -> bool:
        """Check if price change is considered extreme"""
        return abs(price_change_percent) > self.config.issue_thresholds.extreme_price_change_percent

# Global configuration manager instance
_config_manager: Optional[AgentConfigManager] = None

def get_config_manager() -> AgentConfigManager:
    """Get global configuration manager instance"""
    global _config_manager
    if _config_manager is None:
        _config_manager = AgentConfigManager()
    return _config_manager

def get_agent_config() -> AgentConfig:
    """Get current agent configuration"""
    return get_config_manager().get_config()

# Configuration presets for different environments
DEVELOPMENT_CONFIG_OVERRIDES = {
    "monitoring": {
        "cycle_interval_seconds": 60,  # More frequent in dev
        "health_check_interval_seconds": 30
    },
    "issue_thresholds": {
        "quality_score_critical_threshold": 60,  # More lenient in dev
        "max_missing_consecutive_days": 5
    },
    "log_level": "DEBUG"
}

PRODUCTION_CONFIG_OVERRIDES = {
    "monitoring": {
        "cycle_interval_seconds": 300,  # Standard 5 minutes
        "max_concurrent_workflows": 20
    },
    "issue_thresholds": {
        "quality_score_critical_threshold": 50,  # Stricter in production
        "max_missing_consecutive_days": 2
    },
    "notifications": {
        "enable_email_notifications": True,
        "notification_severity_threshold": "high"
    },
    "log_level": "INFO"
}

def apply_environment_config(environment: str = "development"):
    """Apply environment-specific configuration overrides"""
    config_manager = get_config_manager()

    if environment.lower() == "development":
        config_manager.update_config(DEVELOPMENT_CONFIG_OVERRIDES)
    elif environment.lower() == "production":
        config_manager.update_config(PRODUCTION_CONFIG_OVERRIDES)
    else:
        logger.warning(f"Unknown environment: {environment}, using default configuration")