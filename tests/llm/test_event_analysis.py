#!/usr/bin/env python3
"""
Comprehensive Tests for LLM-Based Event Analysis

Tests advanced event analysis capabilities including:
- LLM interface implementations and abstractions
- Event analysis request/response processing
- Self-reflective analysis capabilities
- Adaptive model selection based on complexity
- Caching and performance optimization
- Feature flag integration and graceful degradation
"""

import pytest
import asyncio
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import tempfile
import os

# Test imports
import sys
sys.path.insert(0, 'src')

from shared.utils.feature_flags import FeatureManager, feature_manager
from llm.event_analysis import (
    EventAnalysisRequest, EventAnalysisResult, LLMInterface,
    MockLLMInterface, OpenAIInterface, EventAnalysisCache,
    LLMEventAnalyzer, AdaptiveModelSelector, create_event_analyzer,
    create_adaptive_analyzer, quick_event_analysis, deep_event_analysis
)


class TestEventAnalysisRequest:
    """Test event analysis request data structure."""
    
    def test_request_creation_basic(self):
        """Test basic request creation."""
        request = EventAnalysisRequest(
            event_id="test_001",
            event_type="earnings",
            content="Apple reports Q4 earnings beat",
            timestamp=datetime(2024, 1, 15, 16, 30),
            symbol="AAPL"
        )
        
        assert request.event_id == "test_001"
        assert request.event_type == "earnings"
        assert request.content == "Apple reports Q4 earnings beat"
        assert request.symbol == "AAPL"
        assert request.analysis_depth == "standard"  # Default
        assert request.enable_reflection == True  # Default
        assert request.cache_results == True  # Default
    
    def test_request_creation_with_context(self):
        """Test request creation with context data."""
        context = {
            "previous_earnings": {"eps": 1.25, "revenue": "81.8B"},
            "market_cap": "2.8T",
            "analyst_consensus": {"eps": 1.20, "revenue": "82.0B"}
        }
        
        request = EventAnalysisRequest(
            event_id="test_002",
            event_type="earnings",
            content="Apple beats earnings expectations",
            timestamp=datetime.now(),
            symbol="AAPL",
            context_data=context,
            analysis_depth="deep",
            enable_reflection=True
        )
        
        assert request.context_data == context
        assert request.analysis_depth == "deep"
        assert request.enable_reflection == True
    
    def test_request_defaults(self):
        """Test request default values."""
        request = EventAnalysisRequest(
            event_id="test_003",
            event_type="news", 
            content="Test content",
            timestamp=datetime.now(),
            symbol="MSFT"
        )
        
        assert request.context_data == {}
        assert request.analysis_depth == "standard"
        assert request.enable_reflection == True
        assert request.cache_results == True


class TestEventAnalysisResult:
    """Test event analysis result data structure."""
    
    def test_result_creation(self):
        """Test result creation."""
        result = EventAnalysisResult(
            event_id="test_001",
            sentiment_score=0.7,
            importance_score=0.8,
            impact_category="high",
            impact_timeframe="short_term",
            key_insights=["Strong earnings beat", "Revenue growth"],
            confidence_score=0.85,
            reasoning_chain=["Analyzed earnings data", "Compared to consensus"]
        )
        
        assert result.event_id == "test_001"
        assert result.sentiment_score == 0.7
        assert result.importance_score == 0.8
        assert result.impact_category == "high"
        assert result.impact_timeframe == "short_term"
        assert len(result.key_insights) == 2
        assert result.confidence_score == 0.85
        assert len(result.reasoning_chain) == 2
        assert result.reflection_notes is None  # Default
    
    def test_result_with_reflection(self):
        """Test result with reflection notes."""
        result = EventAnalysisResult(
            event_id="test_002",
            sentiment_score=0.3,
            importance_score=0.6,
            impact_category="medium",
            impact_timeframe="medium_term",
            key_insights=["Mixed signals"],
            confidence_score=0.6,
            reasoning_chain=["Initial analysis"],
            reflection_notes="Consider alternative interpretations",
            processing_time=1.5,
            model_used="gpt-4"
        )
        
        assert result.reflection_notes == "Consider alternative interpretations"
        assert result.processing_time == 1.5
        assert result.model_used == "gpt-4"


class TestMockLLMInterface:
    """Test mock LLM interface."""
    
    @pytest.fixture
    def mock_llm(self):
        """Create mock LLM interface."""
        return MockLLMInterface("mock-gpt-4")
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_mock_analysis(self, mock_llm):
        """Test mock analysis."""
        request = EventAnalysisRequest(
            event_id="test_001",
            event_type="news",
            content="Test news content",
            timestamp=datetime.now(),
            symbol="AAPL"
        )
        
        result = await mock_llm.analyze_event(request, "system prompt", "user prompt")
        
        assert "sentiment_score" in result
        assert "importance_score" in result
        assert "impact_category" in result
        assert "key_insights" in result
        assert "confidence_score" in result
        assert "reasoning_chain" in result
        
        # Verify value ranges
        assert -1.0 <= result["sentiment_score"] <= 1.0
        assert 0.0 <= result["importance_score"] <= 1.0
        assert result["impact_category"] in ["high", "medium", "low"]
    
    def test_mock_model_info(self, mock_llm):
        """Test mock model information."""
        info = mock_llm.get_model_info()
        
        assert info["model"] == "mock-gpt-4"
        assert info["provider"] == "mock"
        assert info["version"] == "1.0.0"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_mock_deterministic_output(self, mock_llm):
        """Test that mock produces deterministic output for same input."""
        request1 = EventAnalysisRequest(
            event_id="test_det",
            event_type="news",
            content="Identical content",
            timestamp=datetime(2024, 1, 1),
            symbol="AAPL"
        )
        
        request2 = EventAnalysisRequest(
            event_id="test_det",
            event_type="news",
            content="Identical content",
            timestamp=datetime(2024, 1, 1),
            symbol="AAPL"
        )
        
        result1 = await mock_llm.analyze_event(request1, "system", "user")
        result2 = await mock_llm.analyze_event(request2, "system", "user")
        
        assert result1["sentiment_score"] == result2["sentiment_score"]
        assert result1["importance_score"] == result2["importance_score"]


class TestOpenAIInterface:
    """Test OpenAI interface (placeholder implementation)."""
    
    def test_openai_initialization(self):
        """Test OpenAI interface initialization."""
        interface = OpenAIInterface("gpt-4", "test-api-key")
        
        assert interface.model_name == "gpt-4"
        assert interface.api_key == "test-api-key"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_openai_fallback_to_mock(self):
        """Test OpenAI fallback to mock implementation."""
        interface = OpenAIInterface("gpt-4")
        
        request = EventAnalysisRequest(
            event_id="test_openai",
            event_type="news",
            content="Test content",
            timestamp=datetime.now(),
            symbol="MSFT"
        )
        
        # Should fall back to mock implementation
        result = await interface.analyze_event(request, "system", "user")
        
        assert "sentiment_score" in result
        assert "importance_score" in result
    
    def test_openai_model_info(self):
        """Test OpenAI model information."""
        interface = OpenAIInterface("gpt-4")
        info = interface.get_model_info()
        
        assert info["model"] == "gpt-4"
        assert info["provider"] == "openai"
        assert info["version"] == "api"


class TestEventAnalysisCache:
    """Test event analysis caching."""
    
    @pytest.fixture
    def cache(self):
        """Create test cache."""
        return EventAnalysisCache(max_size=3)  # Small cache for testing
    
    def test_cache_initialization(self, cache):
        """Test cache initialization."""
        assert cache.max_size == 3
        assert len(cache.cache) == 0
        assert len(cache.access_times) == 0
    
    def test_cache_key_generation(self, cache):
        """Test cache key generation."""
        request1 = EventAnalysisRequest(
            event_id="test_001",
            event_type="news",
            content="content",
            timestamp=datetime.now(),
            symbol="AAPL",
            analysis_depth="standard",
            enable_reflection=True
        )
        
        request2 = EventAnalysisRequest(
            event_id="test_001",
            event_type="news",
            content="content",
            timestamp=datetime.now(),
            symbol="AAPL",
            analysis_depth="deep",  # Different depth
            enable_reflection=True
        )
        
        key1 = cache._get_cache_key(request1)
        key2 = cache._get_cache_key(request2)
        
        assert key1 != key2  # Different depths should have different keys
    
    def test_cache_put_get(self, cache):
        """Test cache put and get operations."""
        request = EventAnalysisRequest(
            event_id="test_001",
            event_type="news",
            content="content",
            timestamp=datetime.now(),
            symbol="AAPL"
        )
        
        result = EventAnalysisResult(
            event_id="test_001",
            sentiment_score=0.5,
            importance_score=0.7,
            impact_category="medium",
            impact_timeframe="short_term",
            key_insights=["Test insight"],
            confidence_score=0.8,
            reasoning_chain=["Test reasoning"]
        )
        
        # Initially not in cache
        assert cache.get(request) is None
        
        # Put in cache
        cache.put(request, result)
        
        # Should be retrievable
        cached_result = cache.get(request)
        assert cached_result is not None
        assert cached_result.event_id == result.event_id
        assert cached_result.sentiment_score == result.sentiment_score
    
    def test_cache_eviction(self, cache):
        """Test cache eviction when max size exceeded."""
        # Fill cache to capacity
        for i in range(3):
            request = EventAnalysisRequest(
                event_id=f"test_{i:03d}",
                event_type="news",
                content=f"content {i}",
                timestamp=datetime.now(),
                symbol="AAPL"
            )
            
            result = EventAnalysisResult(
                event_id=f"test_{i:03d}",
                sentiment_score=0.5,
                importance_score=0.7,
                impact_category="medium",
                impact_timeframe="short_term",
                key_insights=[f"Insight {i}"],
                confidence_score=0.8,
                reasoning_chain=[f"Reasoning {i}"]
            )
            
            cache.put(request, result)
        
        assert len(cache.cache) == 3
        
        # Add one more (should evict oldest)
        new_request = EventAnalysisRequest(
            event_id="test_new",
            event_type="news",
            content="new content",
            timestamp=datetime.now(),
            symbol="AAPL"
        )
        
        new_result = EventAnalysisResult(
            event_id="test_new",
            sentiment_score=0.6,
            importance_score=0.8,
            impact_category="high",
            impact_timeframe="immediate",
            key_insights=["New insight"],
            confidence_score=0.9,
            reasoning_chain=["New reasoning"]
        )
        
        cache.put(new_request, new_result)
        
        # Cache should still be at max size
        assert len(cache.cache) == 3
        
        # New item should be cached
        assert cache.get(new_request) is not None
    
    def test_cache_no_caching(self, cache):
        """Test cache with caching disabled."""
        request = EventAnalysisRequest(
            event_id="test_001",
            event_type="news",
            content="content",
            timestamp=datetime.now(),
            symbol="AAPL",
            cache_results=False  # Disable caching
        )
        
        result = EventAnalysisResult(
            event_id="test_001",
            sentiment_score=0.5,
            importance_score=0.7,
            impact_category="medium",
            impact_timeframe="short_term",
            key_insights=["Test"],
            confidence_score=0.8,
            reasoning_chain=["Test"]
        )
        
        cache.put(request, result)
        
        # Should not be cached
        assert len(cache.cache) == 0
        assert cache.get(request) is None


class TestLLMEventAnalyzer:
    """Test LLM event analyzer."""
    
    @pytest.fixture
    def mock_feature_flags(self):
        """Mock feature flags to enable LLM events."""
        with patch.object(feature_manager, 'is_enabled') as mock_is_enabled:
            def side_effect(flag_name):
                return flag_name in ["enable_llm_events", "enable_event_reflection"]
            mock_is_enabled.side_effect = side_effect
            yield mock_is_enabled
    
    @pytest.fixture
    def analyzer(self, mock_feature_flags):
        """Create test analyzer."""
        return LLMEventAnalyzer(MockLLMInterface("test-model"))
    
    def test_analyzer_initialization(self, analyzer):
        """Test analyzer initialization."""
        assert analyzer.llm_interface is not None
        assert analyzer.cache is not None
        assert hasattr(analyzer, 'system_prompt')
        assert hasattr(analyzer, 'reflection_prompt')
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_basic_event_analysis(self, analyzer):
        """Test basic event analysis."""
        request = EventAnalysisRequest(
            event_id="test_basic",
            event_type="earnings",
            content="Company reports strong Q4 results",
            timestamp=datetime.now(),
            symbol="AAPL",
            enable_reflection=False  # Disable for basic test
        )
        
        result = await analyzer.analyze_event(request)
        
        assert result.event_id == request.event_id
        assert isinstance(result.sentiment_score, float)
        assert isinstance(result.importance_score, float)
        assert result.impact_category in ["high", "medium", "low"]
        assert result.impact_timeframe in ["immediate", "short_term", "medium_term", "long_term"]
        assert isinstance(result.key_insights, list)
        assert isinstance(result.confidence_score, float)
        assert isinstance(result.reasoning_chain, list)
        assert result.processing_time > 0
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_event_analysis_with_reflection(self, analyzer, mock_feature_flags):
        """Test event analysis with reflection."""
        request = EventAnalysisRequest(
            event_id="test_reflection",
            event_type="news",
            content="Market volatility increases amid uncertainty",
            timestamp=datetime.now(),
            symbol="SPY",
            enable_reflection=True
        )
        
        result = await analyzer.analyze_event(request)
        
        assert result.reflection_notes is not None
        assert isinstance(result.reflection_notes, str)
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_event_analysis_with_context(self, analyzer):
        """Test event analysis with context data."""
        context = {
            "market_conditions": "volatile",
            "sector": "technology",
            "previous_performance": {"return_1m": 0.05, "return_3m": 0.12}
        }
        
        request = EventAnalysisRequest(
            event_id="test_context",
            event_type="upgrade",
            content="Analyst upgrades stock to buy",
            timestamp=datetime.now(),
            symbol="MSFT",
            context_data=context,
            enable_reflection=False
        )
        
        result = await analyzer.analyze_event(request)
        
        # Should successfully analyze with context
        assert result.event_id == request.event_id
        assert result.confidence_score > 0
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_cache_functionality(self, analyzer):
        """Test caching functionality."""
        request = EventAnalysisRequest(
            event_id="test_cache",
            event_type="news",
            content="Cached analysis test",
            timestamp=datetime.now(),
            symbol="GOOGL",
            cache_results=True
        )
        
        # First analysis (should be computed)
        result1 = await analyzer.analyze_event(request)
        
        # Second analysis (should be cached)
        result2 = await analyzer.analyze_event(request)
        
        assert result1.event_id == result2.event_id
        assert result1.sentiment_score == result2.sentiment_score
        assert result1.importance_score == result2.importance_score
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_batch_analysis(self, analyzer):
        """Test batch analysis of multiple events."""
        requests = [
            EventAnalysisRequest(
                event_id=f"batch_{i}",
                event_type="news",
                content=f"Batch news item {i}",
                timestamp=datetime.now(),
                symbol="AAPL",
                enable_reflection=False
            )
            for i in range(5)
        ]
        
        results = await analyzer.analyze_batch(requests, max_concurrent=3)
        
        assert len(results) == 5
        
        for i, result in enumerate(results):
            assert result.event_id == f"batch_{i}"
            assert isinstance(result.sentiment_score, float)
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_error_handling(self, mock_feature_flags):
        """Test error handling in analysis."""
        # Create analyzer with failing LLM interface
        class FailingLLMInterface(MockLLMInterface):
            async def analyze_event(self, request, system_prompt, user_prompt):
                raise Exception("Simulated API failure")
        
        analyzer = LLMEventAnalyzer(FailingLLMInterface())
        
        request = EventAnalysisRequest(
            event_id="test_error",
            event_type="news",
            content="This should fail",
            timestamp=datetime.now(),
            symbol="FAIL"
        )
        
        result = await analyzer.analyze_event(request)
        
        # Should return fallback result
        assert result.event_id == request.event_id
        assert result.model_used == "fallback"
        assert result.confidence_score == 0.1
        assert "fallback" in result.reasoning_chain[0].lower()


class TestAdaptiveModelSelector:
    """Test adaptive model selection."""
    
    @pytest.fixture
    def mock_feature_flags(self):
        """Mock feature flags to enable adaptive selection."""
        with patch.object(feature_manager, 'is_enabled', return_value=True):
            yield
    
    @pytest.fixture
    def selector(self, mock_feature_flags):
        """Create test selector."""
        return AdaptiveModelSelector()
    
    def test_selector_initialization(self, selector):
        """Test selector initialization."""
        assert len(selector.model_interfaces) == 3
        assert "quick" in selector.model_interfaces
        assert "standard" in selector.model_interfaces
        assert "deep" in selector.model_interfaces
    
    def test_model_selection_quick(self, selector):
        """Test quick model selection for simple events."""
        request = EventAnalysisRequest(
            event_id="test_quick",
            event_type="news",
            content="Short news",  # Short content
            timestamp=datetime.now(),
            symbol="AAPL"
        )
        
        model_type = selector.select_model(request)
        assert model_type == "quick"
    
    def test_model_selection_standard(self, selector):
        """Test standard model selection for medium complexity."""
        request = EventAnalysisRequest(
            event_id="test_standard",
            event_type="earnings",
            content="A" * 300,  # Medium length content
            timestamp=datetime.now(),
            symbol="MSFT"
        )
        
        model_type = selector.select_model(request)
        assert model_type == "standard"
    
    def test_model_selection_deep(self, selector):
        """Test deep model selection for complex events."""
        request = EventAnalysisRequest(
            event_id="test_deep",
            event_type="news",
            content="A" * 1000,  # Long content
            timestamp=datetime.now(),
            symbol="GOOGL",
            context_data={"complexity": "high"}  # Has context
        )
        
        model_type = selector.select_model(request)
        assert model_type == "deep"
    
    def test_explicit_depth_override(self, selector):
        """Test explicit analysis depth override."""
        request = EventAnalysisRequest(
            event_id="test_override",
            event_type="news",
            content="Short",  # Would normally be quick
            timestamp=datetime.now(),
            symbol="AAPL",
            analysis_depth="deep"  # Explicit override
        )
        
        model_type = selector.select_model(request)
        assert model_type == "deep"
    
    def test_get_analyzer(self, selector):
        """Test getting analyzer for selected model."""
        analyzer = selector.get_analyzer("standard")
        
        assert isinstance(analyzer, LLMEventAnalyzer)
        assert analyzer.llm_interface.model_name == "mock-gpt-4"


class TestFeatureFlagIntegration:
    """Test feature flag integration."""
    
    def test_analyzer_creation_disabled(self):
        """Test analyzer creation when feature is disabled."""
        with patch.object(feature_manager, 'is_enabled', return_value=False):
            analyzer = create_event_analyzer()
            assert analyzer is None
    
    def test_analyzer_creation_enabled(self):
        """Test analyzer creation when feature is enabled."""
        with patch.object(feature_manager, 'is_enabled', return_value=True):
            analyzer = create_event_analyzer()
            assert analyzer is not None
            assert isinstance(analyzer, LLMEventAnalyzer)
    
    def test_adaptive_selector_creation_disabled(self):
        """Test adaptive selector creation when disabled."""
        with patch.object(feature_manager, 'is_enabled', return_value=False):
            selector = create_adaptive_analyzer()
            assert selector is None
    
    def test_adaptive_selector_creation_enabled(self):
        """Test adaptive selector creation when enabled."""
        with patch.object(feature_manager, 'is_enabled', return_value=True):
            selector = create_adaptive_analyzer()
            assert selector is not None
            assert isinstance(selector, AdaptiveModelSelector)
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_quick_analysis_disabled(self):
        """Test quick analysis when feature is disabled."""
        with patch.object(feature_manager, 'is_enabled', return_value=False):
            result = await quick_event_analysis("test content", "AAPL")
            assert result is None
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_quick_analysis_enabled(self):
        """Test quick analysis when feature is enabled."""
        with patch.object(feature_manager, 'is_enabled', return_value=True):
            result = await quick_event_analysis("Strong earnings report", "AAPL")
            assert result is not None
            assert isinstance(result, EventAnalysisResult)
            assert result.symbol == "AAPL"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_deep_analysis_enabled(self):
        """Test deep analysis when feature is enabled."""
        with patch.object(feature_manager, 'is_enabled', return_value=True):
            context = {"market_cap": "2.8T", "sector": "technology"}
            result = await deep_event_analysis(
                "Comprehensive earnings analysis needed",
                "MSFT",
                context
            )
            assert result is not None
            assert isinstance(result, EventAnalysisResult)
            assert result.symbol == "MSFT"


class TestPerformanceBenchmarks:
    """Performance benchmarks for LLM event analysis."""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_single_analysis_performance(self):
        """Test single event analysis performance."""
        with patch.object(feature_manager, 'is_enabled', return_value=True):
            analyzer = create_event_analyzer(enable_reflection=False)  # Faster without reflection
            
            request = EventAnalysisRequest(
                event_id="perf_test",
                event_type="news",
                content="Performance test content",
                timestamp=datetime.now(),
                symbol="AAPL",
                enable_reflection=False
            )
            
            import time
            start_time = time.time()
            
            result = await analyzer.analyze_event(request)
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            assert result is not None
            assert processing_time < 1.0  # Should complete quickly with mock
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_batch_analysis_performance(self):
        """Test batch analysis performance."""
        with patch.object(feature_manager, 'is_enabled', return_value=True):
            analyzer = create_event_analyzer(enable_reflection=False)
            
            # Create batch of requests
            requests = [
                EventAnalysisRequest(
                    event_id=f"batch_perf_{i}",
                    event_type="news",
                    content=f"Batch performance test {i}",
                    timestamp=datetime.now(),
                    symbol=f"STOCK{i}",
                    enable_reflection=False
                )
                for i in range(20)
            ]
            
            import time
            start_time = time.time()
            
            results = await analyzer.analyze_batch(requests, max_concurrent=10)
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            assert len(results) == 20
            assert processing_time < 5.0  # Should complete reasonably fast
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_cache_performance_benefit(self):
        """Test caching performance benefit."""
        with patch.object(feature_manager, 'is_enabled', return_value=True):
            analyzer = create_event_analyzer(enable_caching=True)
            
            request = EventAnalysisRequest(
                event_id="cache_perf",
                event_type="news",
                content="Cache performance test",
                timestamp=datetime.now(),
                symbol="AAPL",
                cache_results=True
            )
            
            # First analysis (not cached)
            import time
            start_time = time.time()
            result1 = await analyzer.analyze_event(request)
            first_time = time.time() - start_time
            
            # Second analysis (cached)
            start_time = time.time()
            result2 = await analyzer.analyze_event(request)
            second_time = time.time() - start_time
            
            assert result1.sentiment_score == result2.sentiment_score
            # Cached should be significantly faster (though with mock it's minimal)
            assert second_time <= first_time


if __name__ == "__main__":
    pytest.main([__file__, "-v"])