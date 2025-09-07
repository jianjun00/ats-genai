#!/usr/bin/env python3
"""
Feature Flag Configuration System

Controls activation of advanced features across the ATS-GenAI platform.
Enables gradual rollout and A/B testing of new capabilities.

Key Features:
- Environment-based feature control
- Runtime feature toggling
- Performance impact isolation
- Safe feature rollbacks
"""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class FeatureStage(Enum):
    """Feature development stage."""
    EXPERIMENTAL = "experimental"  # Early development, may be unstable
    BETA = "beta"                 # Feature complete, testing phase
    STABLE = "stable"             # Production ready
    DEPRECATED = "deprecated"     # Being phased out


@dataclass
class FeatureFlag:
    """Individual feature flag configuration."""
    name: str
    enabled: bool = False
    stage: FeatureStage = FeatureStage.EXPERIMENTAL
    description: str = ""
    dependencies: list = field(default_factory=list)
    performance_impact: str = "low"  # "low", "medium", "high"
    rollout_percentage: float = 0.0  # 0-100 percentage for gradual rollout
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_available(self) -> bool:
        """Check if feature is available (considers rollout percentage)."""
        if not self.enabled:
            return False

        # Simple rollout logic - in production would use proper user hashing
        import random
        return random.random() * 100 <= self.rollout_percentage


@dataclass
class ModelFeatureFlags:
    """Feature flags for advanced model capabilities."""

    # Phase 2: Agent Interaction Networks
    enable_agent_networks: FeatureFlag = field(default_factory=lambda: FeatureFlag(
        name="enable_agent_networks",
        enabled=os.getenv("ENABLE_AGENT_NETWORKS", "false").lower() == "true",
        stage=FeatureStage.BETA,
        description="Multi-agent interaction networks for stock modeling",
        performance_impact="high",
        rollout_percentage=float(os.getenv("AGENT_NETWORKS_ROLLOUT", "0.0")),
        metadata={
            "min_agents": 3,
            "max_agents": 50,
            "default_interaction_radius": 0.1,
            "enable_graph_attention": True
        }
    ))

    enable_portfolio_agents: FeatureFlag = field(default_factory=lambda: FeatureFlag(
        name="enable_portfolio_agents",
        enabled=os.getenv("ENABLE_PORTFOLIO_AGENTS", "false").lower() == "true",
        stage=FeatureStage.BETA,
        description="Agent-based portfolio optimization",
        dependencies=["enable_agent_networks"],
        performance_impact="high",
        rollout_percentage=float(os.getenv("PORTFOLIO_AGENTS_ROLLOUT", "0.0"))
    ))

    enable_market_graph: FeatureFlag = field(default_factory=lambda: FeatureFlag(
        name="enable_market_graph",
        enabled=os.getenv("ENABLE_MARKET_GRAPH", "false").lower() == "true",
        stage=FeatureStage.BETA,
        description="Graph neural networks for market structure modeling",
        performance_impact="high",
        rollout_percentage=float(os.getenv("MARKET_GRAPH_ROLLOUT", "0.0"))
    ))

    # Phase 3: LLM-Based Event Analysis
    enable_llm_events: FeatureFlag = field(default_factory=lambda: FeatureFlag(
        name="enable_llm_events",
        enabled=os.getenv("ENABLE_LLM_EVENTS", "false").lower() == "true",
        stage=FeatureStage.EXPERIMENTAL,
        description="LLM-based event analysis with reflection",
        performance_impact="high",
        rollout_percentage=float(os.getenv("LLM_EVENTS_ROLLOUT", "0.0")),
        metadata={
            "llm_model": os.getenv("LLM_MODEL", "gpt-4"),
            "max_context_length": int(os.getenv("LLM_MAX_CONTEXT", "8192")),
            "enable_reflection": os.getenv("ENABLE_LLM_REFLECTION", "true").lower() == "true",
            "cache_embeddings": True
        }
    ))

    enable_adaptive_selection: FeatureFlag = field(default_factory=lambda: FeatureFlag(
        name="enable_adaptive_selection",
        enabled=os.getenv("ENABLE_ADAPTIVE_SELECTION", "false").lower() == "true",
        stage=FeatureStage.EXPERIMENTAL,
        description="Adaptive model selection framework",
        dependencies=["enable_llm_events"],
        performance_impact="medium",
        rollout_percentage=float(os.getenv("ADAPTIVE_SELECTION_ROLLOUT", "0.0"))
    ))

    enable_event_reflection: FeatureFlag = field(default_factory=lambda: FeatureFlag(
        name="enable_event_reflection",
        enabled=os.getenv("ENABLE_EVENT_REFLECTION", "false").lower() == "true",
        stage=FeatureStage.EXPERIMENTAL,
        description="Self-reflective event analysis",
        dependencies=["enable_llm_events"],
        performance_impact="high",
        rollout_percentage=float(os.getenv("EVENT_REFLECTION_ROLLOUT", "0.0"))
    ))

    def get_all_flags(self) -> Dict[str, FeatureFlag]:
        """Get all feature flags as a dictionary."""
        return {
            field_name: getattr(self, field_name)
            for field_name in self.__dataclass_fields__
            if isinstance(getattr(self, field_name), FeatureFlag)
        }

    def check_dependencies(self, flag_name: str) -> bool:
        """Check if all dependencies for a feature are satisfied."""
        flags = self.get_all_flags()
        if flag_name not in flags:
            return False

        flag = flags[flag_name]
        for dep in flag.dependencies:
            if dep not in flags or not flags[dep].is_available():
                logger.warning(f"Feature {flag_name} dependency {dep} not satisfied")
                return False

        return True

    def is_enabled(self, flag_name: str) -> bool:
        """Check if a specific feature is enabled and available."""
        flags = self.get_all_flags()
        if flag_name not in flags:
            return False

        flag = flags[flag_name]
        return flag.is_available() and self.check_dependencies(flag_name)

    def get_feature_summary(self) -> Dict[str, Dict[str, Any]]:
        """Get summary of all features and their status."""
        flags = self.get_all_flags()
        summary = {}

        for name, flag in flags.items():
            summary[name] = {
                "enabled": flag.enabled,
                "available": flag.is_available(),
                "stage": flag.stage.value,
                "dependencies_satisfied": self.check_dependencies(name),
                "performance_impact": flag.performance_impact,
                "rollout_percentage": flag.rollout_percentage,
                "description": flag.description
            }

        return summary


class FeatureManager:
    """Central feature flag manager."""

    def __init__(self):
        self.model_flags = ModelFeatureFlags()
        logger.info("Feature flags initialized")
        self._log_feature_status()

    def _log_feature_status(self):
        """Log current feature flag status."""
        summary = self.model_flags.get_feature_summary()
        enabled_features = [name for name, info in summary.items() if info["available"]]

        if enabled_features:
            logger.info(f"Enabled features: {', '.join(enabled_features)}")
        else:
            logger.info("No advanced features currently enabled")

    def is_enabled(self, feature_name: str) -> bool:
        """Check if a feature is enabled."""
        return self.model_flags.is_enabled(feature_name)

    def get_flag(self, feature_name: str) -> Optional[FeatureFlag]:
        """Get specific feature flag."""
        flags = self.model_flags.get_all_flags()
        return flags.get(feature_name)

    def override_flag(self, feature_name: str, enabled: bool):
        """Runtime override of feature flag (for testing)."""
        flags = self.model_flags.get_all_flags()
        if feature_name in flags:
            flags[feature_name].enabled = enabled
            logger.info(f"Feature {feature_name} {'enabled' if enabled else 'disabled'} via runtime override")

    def get_performance_budget(self) -> Dict[str, int]:
        """Get performance budget based on enabled features."""
        summary = self.model_flags.get_feature_summary()
        budget = {"high": 0, "medium": 0, "low": 0}

        for name, info in summary.items():
            if info["available"]:
                budget[info["performance_impact"]] += 1

        return budget


# Global feature manager instance
feature_manager = FeatureManager()


def is_enabled(feature_name: str) -> bool:
    """Convenience function to check feature status."""
    return feature_manager.is_enabled(feature_name)


def require_feature(feature_name: str):
    """Decorator to require a feature flag for function execution."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            if not is_enabled(feature_name):
                raise RuntimeError(f"Feature {feature_name} is not enabled")
            return func(*args, **kwargs)
        return wrapper
    return decorator


def feature_gate(feature_name: str, fallback=None):
    """Decorator to conditionally execute function based on feature flag."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            if is_enabled(feature_name):
                return func(*args, **kwargs)
            elif fallback is not None:
                return fallback(*args, **kwargs) if callable(fallback) else fallback
            else:
                logger.debug(f"Feature {feature_name} disabled, skipping {func.__name__}")
                return None
        return wrapper
    return decorator