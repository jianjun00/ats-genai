#!/usr/bin/env python3
"""
Hybrid LLM Client - Integration of Local and Cloud Models

This module provides a unified interface that seamlessly integrates:
- Self-hosted local models (FinGPT, Llama 3.1)
- Cloud API providers (OpenAI, Anthropic, Google)

Features:
- Intelligent routing based on task type and model availability
- Cost optimization by preferring local models
- Automatic failover from local to cloud models
- Load balancing across local models
- Performance monitoring and optimization
"""

import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from enum import Enum
from dataclasses import dataclass

from src.infrastructure.llm.multi_provider_client import (
    MultiProviderLLMClient, LLMResponse
)
from src.infrastructure.llm.local_model_client import (
    LocalModelOrchestrator, create_fingpt_config, create_llama_8b_config,
    create_llama_70b_config
)

logger = logging.getLogger(__name__)


class TaskType(Enum):
    """Types of tasks for intelligent model routing."""
    SENTIMENT_ANALYSIS = "sentiment"
    ENTITY_RECOGNITION = "entity"
    EVENT_DETECTION = "event"
    RISK_ASSESSMENT = "risk"
    MARKET_IMPACT = "impact"
    SIGNAL_GENERATION = "signal"
    GENERAL_ANALYSIS = "general"


class ModelTier(Enum):
    """Model performance tiers."""
    LOCAL_SPECIALIZED = "local_specialized"  # FinGPT for financial tasks
    LOCAL_GENERAL = "local_general"          # Llama 3.1 8B/70B
    CLOUD_FAST = "cloud_fast"                # GPT-4o-mini, Claude Haiku
    CLOUD_PREMIUM = "cloud_premium"          # GPT-4o, Claude Sonnet


@dataclass
class RoutingStrategy:
    """Configuration for intelligent model routing."""
    primary_tier: ModelTier
    alternative_tiers: List[ModelTier]
    task_type: TaskType
    max_latency_ms: Optional[int] = None
    max_cost_usd: Optional[float] = None
    min_confidence_threshold: float = 0.7


class HybridLLMClient:
    """Hybrid client combining local and cloud LLM providers."""

    def __init__(self, cloud_config: Optional[Dict[str, Dict]] = None):
        # Initialize local model orchestrator
        self.local_orchestrator = LocalModelOrchestrator()

        # Initialize cloud client if config provided
        self.cloud_client = None
        if cloud_config:
            self.cloud_client = MultiProviderLLMClient(cloud_config)

        # Routing strategies for different task types
        self.routing_strategies = self._create_default_routing_strategies()

        # Performance tracking
        self.routing_metrics = {
            'requests_by_tier': {tier.value: 0 for tier in ModelTier},
            'failures_by_tier': {tier.value: 0 for tier in ModelTier},
            'avg_latency_by_tier': {tier.value: 0.0 for tier in ModelTier},
            'total_cost_saved_usd': 0.0,
            'local_model_uptime': {}
        }

        self._initialized = False

    def _create_default_routing_strategies(self) -> Dict[TaskType, RoutingStrategy]:
        """Create default routing strategies optimized for each task type."""
        return {
            TaskType.SENTIMENT_ANALYSIS: RoutingStrategy(
                primary_tier=ModelTier.LOCAL_SPECIALIZED,
                alternative_tiers=[ModelTier.LOCAL_GENERAL, ModelTier.CLOUD_FAST, ModelTier.CLOUD_PREMIUM],
                task_type=TaskType.SENTIMENT_ANALYSIS,
                max_latency_ms=5000,
                max_cost_usd=0.01
            ),
            TaskType.ENTITY_RECOGNITION: RoutingStrategy(
                primary_tier=ModelTier.LOCAL_GENERAL,
                alternative_tiers=[ModelTier.LOCAL_SPECIALIZED, ModelTier.CLOUD_FAST, ModelTier.CLOUD_PREMIUM],
                task_type=TaskType.ENTITY_RECOGNITION,
                max_latency_ms=8000,
                max_cost_usd=0.015
            ),
            TaskType.EVENT_DETECTION: RoutingStrategy(
                primary_tier=ModelTier.LOCAL_GENERAL,
                alternative_tiers=[ModelTier.CLOUD_FAST, ModelTier.CLOUD_PREMIUM],
                task_type=TaskType.EVENT_DETECTION,
                max_latency_ms=10000,
                max_cost_usd=0.02
            ),
            TaskType.RISK_ASSESSMENT: RoutingStrategy(
                primary_tier=ModelTier.LOCAL_GENERAL,
                alternative_tiers=[ModelTier.CLOUD_PREMIUM, ModelTier.CLOUD_FAST],
                task_type=TaskType.RISK_ASSESSMENT,
                max_latency_ms=15000,
                max_cost_usd=0.03
            ),
            TaskType.MARKET_IMPACT: RoutingStrategy(
                primary_tier=ModelTier.LOCAL_GENERAL,
                alternative_tiers=[ModelTier.CLOUD_PREMIUM, ModelTier.CLOUD_FAST],
                task_type=TaskType.MARKET_IMPACT,
                max_latency_ms=15000,
                max_cost_usd=0.03
            ),
            TaskType.SIGNAL_GENERATION: RoutingStrategy(
                primary_tier=ModelTier.LOCAL_GENERAL,
                alternative_tiers=[ModelTier.CLOUD_PREMIUM, ModelTier.LOCAL_SPECIALIZED, ModelTier.CLOUD_FAST],
                task_type=TaskType.SIGNAL_GENERATION,
                max_latency_ms=20000,
                max_cost_usd=0.05
            ),
            TaskType.GENERAL_ANALYSIS: RoutingStrategy(
                primary_tier=ModelTier.LOCAL_GENERAL,
                alternative_tiers=[ModelTier.CLOUD_FAST, ModelTier.CLOUD_PREMIUM],
                task_type=TaskType.GENERAL_ANALYSIS,
                max_latency_ms=12000,
                max_cost_usd=0.025
            )
        }

    async def initialize(self):
        """Initialize both local and cloud models."""
        if self._initialized:
            return

        logger.info("Initializing Hybrid LLM Client...")

        try:
            # Add local models based on available hardware
            await self._setup_local_models()

            # Initialize local models
            await self.local_orchestrator.initialize_all()

            # Initialize cloud client if available
            if self.cloud_client:
                await self.cloud_client.initialize()

            self._initialized = True
            logger.info("Hybrid LLM Client initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Hybrid LLM Client: {e}")
            raise

    async def _setup_local_models(self):
        """Setup local models based on available hardware."""
        import torch
        import GPUtil

        if not torch.cuda.is_available():
            logger.warning("CUDA not available, skipping local GPU models")
            return

        gpus = GPUtil.getGPUs()
        if not gpus:
            logger.warning("No GPUs detected, skipping local models")
            return

        total_vram = sum(gpu.memoryTotal for gpu in gpus)
        logger.info(f"Detected {len(gpus)} GPUs with total {total_vram}MB VRAM")

        # Setup models based on available VRAM
        if total_vram >= 140000:  # 140GB+ for Llama 70B
            logger.info("Setting up Llama 3.1 70B (high-end configuration)")
            llama_70b_config = create_llama_70b_config(precision="fp16", enable_quantization=False)
            self.local_orchestrator.add_model("llama-70b", llama_70b_config, is_default=True)

            # Add FinGPT as specialized model
            fingpt_config = create_fingpt_config(precision="fp16")
            self.local_orchestrator.add_model("fingpt-sentiment", fingpt_config)

        elif total_vram >= 48000:  # 48GB+ for Llama 70B quantized
            logger.info("Setting up Llama 3.1 70B INT4 quantized")
            llama_70b_config = create_llama_70b_config(precision="int4", enable_quantization=True)
            self.local_orchestrator.add_model("llama-70b-quant", llama_70b_config, is_default=True)

            fingpt_config = create_fingpt_config(precision="fp16")
            self.local_orchestrator.add_model("fingpt-sentiment", fingpt_config)

        elif total_vram >= 20000:  # 20GB+ for Llama 8B or FinGPT
            logger.info("Setting up Llama 3.1 8B and FinGPT")
            llama_8b_config = create_llama_8b_config(precision="fp16")
            self.local_orchestrator.add_model("llama-8b", llama_8b_config, is_default=True)

            fingpt_config = create_fingpt_config(precision="fp16")
            self.local_orchestrator.add_model("fingpt-sentiment", fingpt_config)

        elif total_vram >= 12000:  # 12GB+ for quantized models
            logger.info("Setting up quantized models for limited VRAM")
            llama_8b_config = create_llama_8b_config(precision="int8", enable_quantization=True)
            self.local_orchestrator.add_model("llama-8b-quant", llama_8b_config, is_default=True)

        else:
            logger.warning("Insufficient VRAM for local models, using cloud-only mode")

    async def generate_response(
        self,
        prompt: str,
        task_type: TaskType = TaskType.GENERAL_ANALYSIS,
        **kwargs
    ) -> LLMResponse:
        """Generate response using intelligent model routing."""
        if not self._initialized:
            await self.initialize()

        strategy = self.routing_strategies.get(task_type, self.routing_strategies[TaskType.GENERAL_ANALYSIS])

        # Try each tier in order
        tiers_to_try = [strategy.primary_tier] + strategy.alternative_tiers

        for tier in tiers_to_try:
            try:
                start_time = time.time()

                # Route to appropriate model tier
                response = await self._route_to_tier(tier, prompt, task_type, **kwargs)

                # Record successful metrics
                latency_ms = (time.time() - start_time) * 1000
                self._record_success_metrics(tier, latency_ms, response.cost_usd)

                # Add routing metadata
                response.metadata = response.metadata or {}
                response.metadata.update({
                    'routing_tier': tier.value,
                    'task_type': task_type.value,
                    'attempted_tiers': [t.value for t in tiers_to_try[:tiers_to_try.index(tier) + 1]]
                })

                return response

            # Let all LLM tier exceptions propagate - fail fast on model access errors

        raise Exception(f"All model tiers failed for task type {task_type.value}")

    async def _route_to_tier(
        self,
        tier: ModelTier,
        prompt: str,
        task_type: TaskType,
        **kwargs
    ) -> LLMResponse:
        """Route request to specific model tier."""

        if tier == ModelTier.LOCAL_SPECIALIZED:
            # Route to FinGPT for financial tasks
            if task_type == TaskType.SENTIMENT_ANALYSIS:
                return await self.local_orchestrator.generate_response(
                    prompt, model_name="fingpt-sentiment", **kwargs
                )
            else:
                # Alternative: use general local model
                return await self._route_to_tier(ModelTier.LOCAL_GENERAL, prompt, task_type, **kwargs)

        elif tier == ModelTier.LOCAL_GENERAL:
            # Route to best available Llama model
            models = list(self.local_orchestrator.models.keys())
            llama_models = [m for m in models if 'llama' in m.lower()]

            if llama_models:
                # Prefer larger models if available
                preferred_order = ['llama-70b', 'llama-70b-quant', 'llama-8b', 'llama-8b-quant']
                selected_model = None

                for preferred in preferred_order:
                    if preferred in llama_models:
                        selected_model = preferred
                        break

                if not selected_model:
                    selected_model = llama_models[0]

                return await self.local_orchestrator.generate_response(
                    prompt, model_name=selected_model, **kwargs
                )
            else:
                raise Exception("No local general models available")

        elif tier == ModelTier.CLOUD_FAST:
            if not self.cloud_client:
                raise Exception("Cloud client not available")

            # Use fast, cost-effective cloud models
            kwargs.setdefault('model_preference', 'fast')
            return await self.cloud_client.generate_response(prompt, **kwargs)

        elif tier == ModelTier.CLOUD_PREMIUM:
            if not self.cloud_client:
                raise Exception("Cloud client not available")

            # Use premium cloud models
            kwargs.setdefault('model_preference', 'quality')
            return await self.cloud_client.generate_response(prompt, **kwargs)

        else:
            raise ValueError(f"Unknown model tier: {tier}")

    def _record_success_metrics(self, tier: ModelTier, latency_ms: float, cost_usd: float):
        """Record successful request metrics."""
        tier_key = tier.value
        self.routing_metrics['requests_by_tier'][tier_key] += 1

        # Update average latency
        current_avg = self.routing_metrics['avg_latency_by_tier'][tier_key]
        request_count = self.routing_metrics['requests_by_tier'][tier_key]
        new_avg = ((current_avg * (request_count - 1)) + latency_ms) / request_count
        self.routing_metrics['avg_latency_by_tier'][tier_key] = new_avg

        # Track cost savings for local models
        if tier in [ModelTier.LOCAL_SPECIALIZED, ModelTier.LOCAL_GENERAL]:
            # Estimate cost savings vs cloud
            estimated_cloud_cost = latency_ms / 1000 * 0.002  # Rough estimate
            self.routing_metrics['total_cost_saved_usd'] += estimated_cloud_cost

    def _record_failure_metrics(self, tier: ModelTier):
        """Record failed request metrics."""
        tier_key = tier.value
        self.routing_metrics['failures_by_tier'][tier_key] += 1

    async def health_check(self) -> Dict[str, Any]:
        """Check health of all available models."""
        health_status = {
            'hybrid_client_status': 'healthy',
            'local_models': {},
            'cloud_models': {},
            'routing_metrics': self.routing_metrics,
            'timestamp': datetime.now().isoformat()
        }

        # Check local models
        try:
            local_health = await self.local_orchestrator.health_check()
            health_status['local_models']['overall_healthy'] = local_health

            # Individual model health
            for model_name in self.local_orchestrator.models:
                try:
                    model_healthy = await self.local_orchestrator.health_check(model_name)
                    health_status['local_models'][model_name] = 'healthy' if model_healthy else 'unhealthy'
                except Exception as e:
                    health_status['local_models'][model_name] = f'error: {str(e)}'

        except Exception as e:
            health_status['local_models']['error'] = str(e)

        # Check cloud models
        if self.cloud_client:
            try:
                cloud_healthy = await self.cloud_client.health_check()
                health_status['cloud_models']['overall_healthy'] = cloud_healthy
            except Exception as e:
                health_status['cloud_models']['error'] = str(e)
        else:
            health_status['cloud_models']['status'] = 'not_configured'

        # Determine overall health
        local_ok = health_status['local_models'].get('overall_healthy', False)
        cloud_ok = health_status['cloud_models'].get('overall_healthy', False)

        if not local_ok and not cloud_ok:
            health_status['hybrid_client_status'] = 'critical'
        elif not local_ok or not cloud_ok:
            health_status['hybrid_client_status'] = 'degraded'

        return health_status

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get comprehensive performance metrics."""
        metrics = {
            'routing_metrics': self.routing_metrics.copy(),
            'local_models': {},
            'cloud_models': {},
            'cost_analysis': self._calculate_cost_analysis()
        }

        # Local model metrics
        if self.local_orchestrator.models:
            metrics['local_models'] = self.local_orchestrator.get_performance_metrics()

        # Cloud model metrics
        if self.cloud_client:
            try:
                metrics['cloud_models'] = self.cloud_client.get_cost_tracking()
            except Exception as e:
                metrics['cloud_models']['error'] = str(e)

        return metrics

    def _calculate_cost_analysis(self) -> Dict[str, Any]:
        """Calculate cost analysis and savings."""
        total_requests = sum(self.routing_metrics['requests_by_tier'].values())
        local_requests = (
            self.routing_metrics['requests_by_tier'][ModelTier.LOCAL_SPECIALIZED.value] +
            self.routing_metrics['requests_by_tier'][ModelTier.LOCAL_GENERAL.value]
        )

        if total_requests == 0:
            return {'status': 'no_requests_yet'}

        local_usage_percentage = (local_requests / total_requests) * 100

        return {
            'total_requests': total_requests,
            'local_requests': local_requests,
            'local_usage_percentage': local_usage_percentage,
            'estimated_cost_savings_usd': self.routing_metrics['total_cost_saved_usd'],
            'average_cost_per_request': self.routing_metrics['total_cost_saved_usd'] / total_requests if total_requests > 0 else 0
        }

    async def close(self):
        """Clean up all resources."""
        logger.info("Shutting down Hybrid LLM Client...")

        # Close local models
        await self.local_orchestrator.close_all()

        # Close cloud client
        if self.cloud_client:
            await self.cloud_client.close()

        self._initialized = False
        logger.info("Hybrid LLM Client shutdown complete")


# Factory function for easy setup
async def create_hybrid_llm_client(
    enable_cloud: bool = True,
    cloud_config: Optional[Dict[str, Dict]] = None,
    custom_routing: Optional[Dict[TaskType, RoutingStrategy]] = None
) -> HybridLLMClient:
    """Create and initialize a hybrid LLM client."""

    # Default cloud config if enabled but not provided
    if enable_cloud and not cloud_config:
        import os
        cloud_config = {}

        if os.getenv('OPENAI_API_KEY'):
            cloud_config['openai'] = {
                'api_key': os.getenv('OPENAI_API_KEY'),
                'model': 'gpt-4o-mini',
                'max_tokens': 1000,
                'temperature': 0.1
            }

        if os.getenv('ANTHROPIC_API_KEY'):
            cloud_config['anthropic'] = {
                'api_key': os.getenv('ANTHROPIC_API_KEY'),
                'model': 'claude-3-haiku-20240307',
                'max_tokens': 1000,
                'temperature': 0.1
            }

    # Create client
    client = HybridLLMClient(cloud_config if enable_cloud else None)

    # Apply custom routing if provided
    if custom_routing:
        client.routing_strategies.update(custom_routing)

    # Initialize
    await client.initialize()

    return client