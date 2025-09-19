#!/usr/bin/env python3
"""
LLM-Based Event Analysis with Reflection

Advanced event analysis using Large Language Models for financial market events.
Implements self-reflective analysis, contextual understanding, and impact assessment.

Key Features:
- LLM-powered event interpretation
- Self-reflective analysis for improved accuracy
- Multi-modal event processing (text, numerical, temporal)
- Adaptive model selection based on event complexity
- Feature-flag controlled activation
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from abc import ABC, abstractmethod
import hashlib

from core.config.feature_flags import require_feature, feature_gate, is_enabled

logger = logging.getLogger(__name__)


@dataclass
class EventAnalysisRequest:
    """Request for LLM event analysis."""
    event_id: str
    event_type: str
    content: str
    timestamp: datetime
    symbol: str
    context_data: Dict[str, Any] = field(default_factory=dict)
    analysis_depth: str = "standard"  # "quick", "standard", "deep"
    enable_reflection: bool = True
    cache_results: bool = True


@dataclass
class EventAnalysisResult:
    """Result from LLM event analysis."""
    event_id: str
    sentiment_score: float  # -1.0 to 1.0
    importance_score: float  # 0.0 to 1.0
    impact_category: str  # "high", "medium", "low"
    impact_timeframe: str  # "immediate", "short_term", "medium_term", "long_term"
    key_insights: List[str]
    confidence_score: float
    reasoning_chain: List[str]
    reflection_notes: Optional[str] = None
    processing_time: float = 0.0
    model_used: str = "unknown"
    metadata: Dict[str, Any] = field(default_factory=dict)


class LLMInterface(ABC):
    """Abstract interface for LLM providers."""

    @abstractmethod
    async def analyze_event(
        self,
        request: EventAnalysisRequest,
        system_prompt: str,
        user_prompt: str
    ) -> Dict[str, Any]:
        """Analyze event using LLM."""

    @abstractmethod
    def get_model_info(self) -> Dict[str, str]:
        """Get model information."""


class MockLLMInterface(LLMInterface):
    """Mock LLM interface for testing and development."""

    def __init__(self, model_name: str = "mock-gpt-4"):
        self.model_name = model_name

    async def analyze_event(
        self,
        request: EventAnalysisRequest,
        system_prompt: str,
        user_prompt: str
    ) -> Dict[str, Any]:
        """Mock analysis with realistic outputs."""
        await asyncio.sleep(0.1)  # Simulate API latency

        # Generate deterministic but realistic mock responses
        event_hash = hashlib.md5(f"{request.event_id}{request.content}".encode()).hexdigest()
        hash_int = int(event_hash[:8], 16)

        # Simulate sentiment analysis
        sentiment_score = (hash_int % 200 - 100) / 100.0  # -1.0 to 1.0
        importance_score = (hash_int % 100) / 100.0  # 0.0 to 1.0

        # Determine impact category
        if importance_score > 0.7:
            impact_category = "high"
        elif importance_score > 0.4:
            impact_category = "medium"
        else:
            impact_category = "low"

        # Mock key insights
        insights = [
            f"Event shows {'positive' if sentiment_score > 0 else 'negative'} sentiment",
            f"Market impact expected to be {impact_category}",
            f"Affects {request.symbol} directly"
        ]

        return {
            "sentiment_score": sentiment_score,
            "importance_score": importance_score,
            "impact_category": impact_category,
            "impact_timeframe": "short_term",
            "key_insights": insights,
            "confidence_score": 0.8,
            "reasoning_chain": [
                "Analyzed event content",
                "Assessed market context",
                "Generated impact assessment"
            ]
        }

    def get_model_info(self) -> Dict[str, str]:
        """Get mock model information."""
        return {
            "model": self.model_name,
            "provider": "mock",
            "version": "1.0.0"
        }


class OpenAIInterface(LLMInterface):
    """OpenAI GPT interface (placeholder - requires actual API integration)."""

    def __init__(self, model_name: str = "gpt-4", api_key: Optional[str] = None):
        self.model_name = model_name
        self.api_key = api_key
        logger.info(f"OpenAI interface initialized (placeholder) with {model_name}")

    async def analyze_event(
        self,
        request: EventAnalysisRequest,
        system_prompt: str,
        user_prompt: str
    ) -> Dict[str, Any]:
        """Placeholder for OpenAI API integration."""
        # In real implementation, this would call OpenAI API
        # For now, fall back to mock implementation
        mock_interface = MockLLMInterface(f"mock-{self.model_name}")
        return await mock_interface.analyze_event(request, system_prompt, user_prompt)

    def get_model_info(self) -> Dict[str, str]:
        return {
            "model": self.model_name,
            "provider": "openai",
            "version": "api"
        }


class EventAnalysisCache:
    """Cache for event analysis results."""

    def __init__(self, max_size: int = 10000):
        self.cache: Dict[str, EventAnalysisResult] = {}
        self.access_times: Dict[str, datetime] = {}
        self.max_size = max_size

    def _get_cache_key(self, request: EventAnalysisRequest) -> str:
        """Generate cache key for request."""
        key_data = f"{request.event_id}_{request.analysis_depth}_{request.enable_reflection}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def get(self, request: EventAnalysisRequest) -> Optional[EventAnalysisResult]:
        """Get cached result."""
        if not request.cache_results:
            return None

        key = self._get_cache_key(request)
        if key in self.cache:
            self.access_times[key] = datetime.now()
            return self.cache[key]
        return None

    def put(self, request: EventAnalysisRequest, result: EventAnalysisResult):
        """Cache analysis result."""
        if not request.cache_results:
            return

        key = self._get_cache_key(request)

        # Evict oldest entries if cache is full
        if len(self.cache) >= self.max_size:
            oldest_key = min(self.access_times.keys(), key=lambda k: self.access_times[k])
            del self.cache[oldest_key]
            del self.access_times[oldest_key]

        self.cache[key] = result
        self.access_times[key] = datetime.now()

    def clear(self):
        """Clear cache."""
        self.cache.clear()
        self.access_times.clear()


@require_feature("enable_llm_events")
class LLMEventAnalyzer:
    """LLM-based event analyzer with reflection capabilities."""

    def __init__(
        self,
        llm_interface: Optional[LLMInterface] = None,
        enable_caching: bool = True,
        max_cache_size: int = 10000
    ):
        self.llm_interface = llm_interface or MockLLMInterface()
        self.cache = EventAnalysisCache(max_cache_size) if enable_caching else None

        # Analysis prompts
        self.system_prompt = self._build_system_prompt()
        self.reflection_prompt = self._build_reflection_prompt()

        logger.info("LLM Event Analyzer initialized")

    def _build_system_prompt(self) -> str:
        """Build system prompt for event analysis."""
        return """You are an expert financial analyst specializing in market event analysis.

Your task is to analyze financial market events and assess their potential impact.
For each event, provide:

1. Sentiment Score (-1.0 to 1.0): Overall sentiment (negative to positive)
2. Importance Score (0.0 to 1.0): How significant this event is
3. Impact Category (high/medium/low): Expected market impact level
4. Impact Timeframe (immediate/short_term/medium_term/long_term): When impact will occur
5. Key Insights: List of 2-4 specific insights about the event
6. Confidence Score (0.0 to 1.0): How confident you are in your analysis
7. Reasoning Chain: Step-by-step reasoning process

Be precise, objective, and consider both direct and indirect market effects.
Focus on actionable insights that would be valuable for trading decisions."""

    def _build_reflection_prompt(self) -> str:
        """Build prompt for self-reflection."""
        return """Review your previous analysis and provide reflection:

1. Are there any potential biases in your analysis?
2. What alternative interpretations could be valid?
3. What additional context would improve the analysis?
4. How could the confidence score be adjusted?
5. Are there any contradictions in your reasoning?

Provide a brief reflection that could improve the analysis quality."""

    def _build_user_prompt(self, request: EventAnalysisRequest) -> str:
        """Build user prompt for specific event."""
        context_str = ""
        if request.context_data:
            context_str = f"\nAdditional Context: {json.dumps(request.context_data, indent=2)}"

        return f"""Analyze the following financial market event:

Event ID: {request.event_id}
Event Type: {request.event_type}
Symbol: {request.symbol}
Timestamp: {request.timestamp}
Content: {request.content}{context_str}

Analysis Depth: {request.analysis_depth}

Provide your analysis in JSON format with the exact keys specified in the system prompt."""

    async def analyze_event(self, request: EventAnalysisRequest) -> EventAnalysisResult:
        """Analyze event using LLM."""
        start_time = datetime.now()

        # Check cache first
        if self.cache:
            cached_result = self.cache.get(request)
            if cached_result:
                logger.debug(f"Cache hit for event {request.event_id}")
                return cached_result

        # Perform LLM analysis
        user_prompt = self._build_user_prompt(request)

        try:
            analysis_response = await self.llm_interface.analyze_event(
                request, self.system_prompt, user_prompt
            )

            # Create base result
            result = EventAnalysisResult(
                event_id=request.event_id,
                sentiment_score=analysis_response.get("sentiment_score", 0.0),
                importance_score=analysis_response.get("importance_score", 0.5),
                impact_category=analysis_response.get("impact_category", "medium"),
                impact_timeframe=analysis_response.get("impact_timeframe", "short_term"),
                key_insights=analysis_response.get("key_insights", []),
                confidence_score=analysis_response.get("confidence_score", 0.5),
                reasoning_chain=analysis_response.get("reasoning_chain", []),
                model_used=self.llm_interface.get_model_info()["model"],
                processing_time=(datetime.now() - start_time).total_seconds()
            )

            # Add reflection if enabled
            if request.enable_reflection and is_enabled("enable_event_reflection"):
                reflection = await self._perform_reflection(request, result)
                result.reflection_notes = reflection

            # Cache result
            if self.cache:
                self.cache.put(request, result)

            return result

        except Exception as e:
            logger.error(f"Error analyzing event {request.event_id}: {str(e)}")
            # Return fallback result
            return EventAnalysisResult(
                event_id=request.event_id,
                sentiment_score=0.0,
                importance_score=0.5,
                impact_category="medium",
                impact_timeframe="short_term",
                key_insights=["Analysis failed - using fallback"],
                confidence_score=0.1,
                reasoning_chain=["Fallback due to analysis error"],
                processing_time=(datetime.now() - start_time).total_seconds(),
                model_used="fallback"
            )

    @require_feature("enable_event_reflection")
    async def _perform_reflection(
        self,
        request: EventAnalysisRequest,
        initial_result: EventAnalysisResult
    ) -> str:
        """Perform self-reflection on analysis."""
        reflection_prompt = f"""
        Original Analysis:
        - Sentiment: {initial_result.sentiment_score}
        - Importance: {initial_result.importance_score}
        - Impact: {initial_result.impact_category}
        - Insights: {initial_result.key_insights}
        - Reasoning: {initial_result.reasoning_chain}

        {self.reflection_prompt}
        """

        try:
            reflection_response = await self.llm_interface.analyze_event(
                request, "You are reflecting on a financial analysis.", reflection_prompt
            )
            return reflection_response.get("reflection", "No reflection provided")
        except Exception as e:
            logger.warning(f"Reflection failed for event {request.event_id}: {str(e)}")
            return "Reflection unavailable due to error"

    async def analyze_batch(
        self,
        requests: List[EventAnalysisRequest],
        max_concurrent: int = 5
    ) -> List[EventAnalysisResult]:
        """Analyze multiple events concurrently."""
        semaphore = asyncio.Semaphore(max_concurrent)

        async def analyze_with_semaphore(request):
            async with semaphore:
                return await self.analyze_event(request)

        tasks = [analyze_with_semaphore(req) for req in requests]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and log them
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Batch analysis failed for request {i}: {result}")
            else:
                valid_results.append(result)

        return valid_results


@require_feature("enable_adaptive_selection")
class AdaptiveModelSelector:
    """Adaptive model selection based on event complexity."""

    def __init__(self):
        self.model_interfaces = {
            "quick": MockLLMInterface("mock-gpt-3.5-turbo"),
            "standard": MockLLMInterface("mock-gpt-4"),
            "deep": MockLLMInterface("mock-gpt-4-turbo")
        }

        self.complexity_thresholds = {
            "quick": 100,      # Character count threshold
            "standard": 500,   # Medium complexity
            "deep": float('inf')  # High complexity
        }

        logger.info("Adaptive model selector initialized")

    def select_model(self, request: EventAnalysisRequest) -> str:
        """Select appropriate model based on event complexity."""
        content_length = len(request.content)
        has_context = bool(request.context_data)

        # Override if explicitly requested
        if request.analysis_depth in self.model_interfaces:
            return request.analysis_depth

        # Automatic selection
        if content_length < self.complexity_thresholds["quick"] and not has_context:
            return "quick"
        elif content_length < self.complexity_thresholds["standard"]:
            return "standard"
        else:
            return "deep"

    def get_analyzer(self, model_type: str) -> LLMEventAnalyzer:
        """Get analyzer with selected model."""
        if model_type not in self.model_interfaces:
            model_type = "standard"

        return LLMEventAnalyzer(self.model_interfaces[model_type])


# Feature-gated factory functions
@feature_gate("enable_llm_events")
def create_event_analyzer(
    model_name: str = "gpt-4",
    enable_reflection: bool = True,
    enable_caching: bool = True
) -> Optional[LLMEventAnalyzer]:
    """Factory function to create LLM event analyzer."""
    if not is_enabled("enable_llm_events"):
        logger.warning("LLM events feature is disabled")
        return None

    # Select appropriate interface
    if model_name.startswith("gpt"):
        interface = OpenAIInterface(model_name)
    else:
        interface = MockLLMInterface(model_name)

    return LLMEventAnalyzer(interface, enable_caching)


@feature_gate("enable_adaptive_selection")
def create_adaptive_analyzer() -> Optional[AdaptiveModelSelector]:
    """Factory function to create adaptive model selector."""
    if not is_enabled("enable_adaptive_selection"):
        logger.warning("Adaptive selection feature is disabled")
        return None

    return AdaptiveModelSelector()


# Convenience functions for common use cases
async def quick_event_analysis(
    event_content: str,
    symbol: str,
    event_type: str = "news"
) -> Optional[EventAnalysisResult]:
    """Quick event analysis with minimal setup."""
    if not is_enabled("enable_llm_events"):
        return None

    analyzer = create_event_analyzer()
    if not analyzer:
        return None

    request = EventAnalysisRequest(
        event_id=hashlib.md5(event_content.encode()).hexdigest()[:8],
        event_type=event_type,
        content=event_content,
        timestamp=datetime.now(),
        symbol=symbol,
        analysis_depth="quick",
        enable_reflection=False
    )

    return await analyzer.analyze_event(request)


async def deep_event_analysis(
    event_content: str,
    symbol: str,
    context_data: Optional[Dict[str, Any]] = None,
    event_type: str = "news"
) -> Optional[EventAnalysisResult]:
    """Deep event analysis with reflection and context."""
    if not is_enabled("enable_llm_events"):
        return None

    analyzer = create_event_analyzer(enable_reflection=True)
    if not analyzer:
        return None

    request = EventAnalysisRequest(
        event_id=hashlib.md5(f"{event_content}{symbol}".encode()).hexdigest()[:8],
        event_type=event_type,
        content=event_content,
        timestamp=datetime.now(),
        symbol=symbol,
        context_data=context_data or {},
        analysis_depth="deep",
        enable_reflection=True
    )

    return await analyzer.analyze_event(request)