"""
Tests for LLM-enhanced pattern recognition system.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
import json
import aiohttp

from domains.ml.services.llm_pattern_recognition import (
    LLMProvider,
    PatternAnalysis,
    MarketRegimeAnalysis,
    LLMPatternRecognizer,
    analyze_stock_pattern,
    enhance_features_with_llm,
    generate_training_data_with_llm
)


@pytest.fixture
def sample_price_data():
    """Sample price data for testing."""
    dates = pd.date_range('2024-01-01', '2024-01-20', freq='D')
    np.random.seed(42)  # For reproducible tests

    base_price = 100
    prices = [base_price]

    for i in range(len(dates) - 1):
        change = np.random.normal(0, 0.02)
        prices.append(prices[-1] * (1 + change))

    return pd.DataFrame({
        'open': [p * (1 + np.random.normal(0, 0.005)) for p in prices],
        'high': [p * (1 + abs(np.random.normal(0, 0.01))) for p in prices],
        'low': [p * (1 - abs(np.random.normal(0, 0.01))) for p in prices],
        'close': prices,
        'volume': [np.random.randint(100000, 1000000) for _ in prices]
    }, index=dates)


@pytest.fixture
def sample_pattern_analysis():
    """Sample pattern analysis for testing."""
    return PatternAnalysis(
        pattern_type="ascending_triangle",
        confidence=0.85,
        description="Strong ascending triangle pattern with breakout potential",
        technical_indicators={
            "trend_direction": "up",
            "momentum": "strong",
            "volume_confirmation": "confirmed"
        },
        predicted_direction="bullish",
        support_resistance={
            "support": 98.50,
            "resistance": 105.25
        },
        volume_analysis="Increasing volume confirms pattern strength",
        risk_assessment="Low risk with clear stop loss at support level",
        timeframe_relevance=["short_term", "medium_term"]
    )


@pytest.fixture
def mock_llm_response():
    """Mock LLM API response."""
    return {
        "choices": [{
            "message": {
                "content": """
                {
                    "pattern_type": "ascending_triangle",
                    "confidence": 0.85,
                    "description": "Strong ascending triangle pattern forming",
                    "technical_indicators": {
                        "trend_direction": "up",
                        "momentum": "strong",
                        "volume_confirmation": "confirmed"
                    },
                    "predicted_direction": "bullish",
                    "support_resistance": {
                        "support": 98.50,
                        "resistance": 105.25
                    },
                    "volume_analysis": "Volume increasing on approach to resistance",
                    "risk_assessment": "Low risk with clear levels",
                    "timeframe_relevance": ["short_term", "medium_term"]
                }
                """
            }
        }]
    }


class TestLLMProvider:
    """Test LLMProvider enum."""

    def test_llm_provider_values(self):
        """Test LLMProvider enum values."""
        assert LLMProvider.DEEPSEEK.value == "deepseek"
        assert LLMProvider.OPENAI.value == "openai"
        assert LLMProvider.LOCAL.value == "local"


class TestPatternAnalysis:
    """Test PatternAnalysis dataclass."""

    def test_pattern_analysis_creation(self, sample_pattern_analysis):
        """Test PatternAnalysis creation and properties."""
        analysis = sample_pattern_analysis

        assert analysis.pattern_type == "ascending_triangle"
        assert analysis.confidence == 0.85
        assert analysis.predicted_direction == "bullish"
        assert "support" in analysis.support_resistance
        assert "resistance" in analysis.support_resistance
        assert len(analysis.timeframe_relevance) == 2


class TestMarketRegimeAnalysis:
    """Test MarketRegimeAnalysis dataclass."""

    def test_market_regime_analysis_creation(self):
        """Test MarketRegimeAnalysis creation."""
        regime = MarketRegimeAnalysis(
            regime_type="trending_bull",
            confidence=0.9,
            characteristics=["strong momentum", "low volatility"],
            typical_duration="4-6 weeks",
            trading_implications=["favor momentum strategies", "avoid contrarian plays"],
            risk_factors=["potential reversal", "momentum exhaustion"]
        )

        assert regime.regime_type == "trending_bull"
        assert regime.confidence == 0.9
        assert len(regime.characteristics) == 2
        assert len(regime.trading_implications) == 2
        assert len(regime.risk_factors) == 2


class TestLLMPatternRecognizer:
    """Test LLMPatternRecognizer functionality."""

    def test_init_deepseek_provider(self):
        """Test initialization with DeepSeek provider."""
        recognizer = LLMPatternRecognizer(
            provider=LLMProvider.DEEPSEEK,
            api_key="test_key",
            model_name="deepseek-chat"
        )

        assert recognizer.provider == LLMProvider.DEEPSEEK
        assert recognizer.api_key == "test_key"
        assert recognizer.model_name == "deepseek-chat"
        assert recognizer.base_url == "https://api.deepseek.com/v1"

    def test_init_openai_provider(self):
        """Test initialization with OpenAI provider."""
        recognizer = LLMPatternRecognizer(
            provider=LLMProvider.OPENAI,
            api_key="test_key"
        )

        assert recognizer.provider == LLMProvider.OPENAI
        assert recognizer.model_name == "gpt-4-turbo-preview"
        assert recognizer.base_url == "https://api.openai.com/v1"

    def test_init_local_provider(self):
        """Test initialization with local provider."""
        recognizer = LLMPatternRecognizer(
            provider=LLMProvider.LOCAL,
            model_name="local-model"
        )

        assert recognizer.provider == LLMProvider.LOCAL
        assert recognizer.base_url == "http://localhost:8000/v1"

    def test_create_pattern_signature(self, sample_price_data):
        """Test pattern signature creation for caching."""
        recognizer = LLMPatternRecognizer()

        signature = recognizer._create_pattern_signature(sample_price_data)

        assert isinstance(signature, str)
        assert len(signature) > 0
        # Should be deterministic for same data
        signature2 = recognizer._create_pattern_signature(sample_price_data)
        assert signature == signature2

    def test_format_price_sequence(self, sample_price_data):
        """Test price sequence formatting for LLM."""
        recognizer = LLMPatternRecognizer()

        sequence = recognizer._format_price_sequence(sample_price_data)

        assert isinstance(sequence, str)
        assert "Price sequence" in sequence
        assert "Open" in sequence or "High" in sequence
        # Should limit to reasonable length
        lines = sequence.split('\n')
        assert len(lines) <= 35  # Header + max 30 data points

    def test_create_pattern_analysis_prompt(self):
        """Test pattern analysis prompt creation."""
        recognizer = LLMPatternRecognizer()

        price_sequence = "Price sequence: 100.0, 102.0, 101.0"
        prompt = recognizer._create_pattern_analysis_prompt(
            price_sequence, "AAPL", "daily"
        )

        assert isinstance(prompt, str)
        assert "AAPL" in prompt
        assert "daily" in prompt
        assert "JSON" in prompt
        assert "pattern_type" in prompt
        assert "confidence" in prompt

    def test_parse_pattern_analysis_valid_json(self, mock_llm_response):
        """Test parsing valid JSON response."""
        recognizer = LLMPatternRecognizer()

        response_content = mock_llm_response["choices"][0]["message"]["content"]
        analysis = recognizer._parse_pattern_analysis(response_content)

        assert isinstance(analysis, PatternAnalysis)
        assert analysis.pattern_type == "ascending_triangle"
        assert analysis.confidence == 0.85
        assert analysis.predicted_direction == "bullish"

    def test_parse_pattern_analysis_invalid_json(self):
        """Test parsing invalid JSON response."""
        recognizer = LLMPatternRecognizer()

        invalid_response = "This is not JSON"
        analysis = recognizer._parse_pattern_analysis(invalid_response)

        # Should return fallback analysis
        assert isinstance(analysis, PatternAnalysis)
        assert analysis.pattern_type == "unknown"
        assert analysis.confidence == 0.5

    def test_create_fallback_analysis(self):
        """Test fallback analysis creation."""
        recognizer = LLMPatternRecognizer()

        analysis = recognizer._create_fallback_analysis()

        assert isinstance(analysis, PatternAnalysis)
        assert analysis.pattern_type == "unknown"
        assert analysis.confidence == 0.5
        assert analysis.predicted_direction == "neutral"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_price_pattern_cached(self, sample_price_data):
        """Test price pattern analysis with caching."""
        recognizer = LLMPatternRecognizer()

        # Mock the LLM call to avoid actual API calls
        with patch.object(recognizer, '_call_llm') as mock_call:
            mock_call.return_value = """
            {
                "pattern_type": "test_pattern",
                "confidence": 0.8,
                "description": "Test pattern",
                "technical_indicators": {},
                "predicted_direction": "bullish",
                "support_resistance": {},
                "volume_analysis": "",
                "risk_assessment": "",
                "timeframe_relevance": []
            }
            """

            # First call
            analysis1 = await recognizer.analyze_price_pattern(sample_price_data, "AAPL", "daily")

            # Second call should use cache
            analysis2 = await recognizer.analyze_price_pattern(sample_price_data, "AAPL", "daily")

            # Should only call LLM once due to caching
            assert mock_call.call_count == 1
            assert analysis1.pattern_type == analysis2.pattern_type

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_price_pattern_error_handling(self, sample_price_data):
        """Test error handling in price pattern analysis."""
        recognizer = LLMPatternRecognizer()

        # Mock LLM call to raise exception
        with patch.object(recognizer, '_call_llm') as mock_call:
            mock_call.side_effect = Exception("API Error")

            analysis = await recognizer.analyze_price_pattern(sample_price_data, "AAPL", "daily")

            # Should return fallback analysis
            assert isinstance(analysis, PatternAnalysis)
            assert analysis.pattern_type == "unknown"

    def test_create_market_summary(self, sample_price_data):
        """Test market summary creation."""
        recognizer = LLMPatternRecognizer()

        # Use close prices as market data
        market_data = sample_price_data[['close']].copy()
        summary = recognizer._create_market_summary(market_data)

        assert isinstance(summary, str)
        assert "Market Summary" in summary
        assert "trading days" in summary
        assert "return" in summary
        assert "Volatility" in summary

    def test_calculate_max_drawdown(self):
        """Test maximum drawdown calculation."""
        recognizer = LLMPatternRecognizer()

        # Create prices with known drawdown
        prices = pd.Series([100, 110, 120, 90, 95, 105])  # 25% drawdown from 120 to 90

        max_dd = recognizer._calculate_max_drawdown(prices)

        assert isinstance(max_dd, float)
        assert max_dd < 0  # Drawdown should be negative
        assert abs(max_dd) > 0.2  # Should detect significant drawdown

    def test_calculate_pattern_strength(self, sample_pattern_analysis):
        """Test pattern strength calculation."""
        recognizer = LLMPatternRecognizer()

        strength = recognizer._calculate_pattern_strength(sample_pattern_analysis)

        assert isinstance(strength, float)
        assert 0 <= strength <= 1
        # Should be higher than base confidence due to strong momentum
        assert strength >= sample_pattern_analysis.confidence

    def test_calculate_fractal_dimension(self):
        """Test fractal dimension calculation."""
        recognizer = LLMPatternRecognizer()

        # Create trending price series
        trending_prices = pd.Series([100, 101, 102, 103, 104, 105, 106, 107, 108, 109])

        fractal_dim = recognizer._calculate_fractal_dimension(trending_prices)

        assert isinstance(fractal_dim, float)
        assert 1.0 <= fractal_dim <= 2.0

    def test_calculate_hurst_exponent(self):
        """Test Hurst exponent calculation."""
        recognizer = LLMPatternRecognizer()

        # Create trending price series
        trending_prices = pd.Series([100, 101, 102, 103, 104, 105, 106, 107, 108, 109])

        hurst = recognizer._calculate_hurst_exponent(trending_prices)

        assert isinstance(hurst, float)
        assert 0.0 <= hurst <= 1.0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_enhance_feature_engineering(self, sample_price_data):
        """Test feature enhancement with LLM."""
        recognizer = LLMPatternRecognizer()

        existing_features = {
            'price_momentum': 0.05,
            'volatility': 0.02,
            'volume_trend': 1.2
        }

        # Mock pattern analysis
        with patch.object(recognizer, 'analyze_price_pattern') as mock_analyze:
            mock_analyze.return_value = PatternAnalysis(
                pattern_type="test",
                confidence=0.8,
                description="",
                technical_indicators={},
                predicted_direction="bullish",
                support_resistance={"support": 95.0, "resistance": 105.0},
                volume_analysis="",
                risk_assessment="",
                timeframe_relevance=[]
            )

            enhanced_features = await recognizer.enhance_feature_engineering(
                sample_price_data, existing_features
            )

        assert isinstance(enhanced_features, dict)
        # Should include original features
        assert 'price_momentum' in enhanced_features
        # Should include LLM features
        assert 'llm_pattern_confidence' in enhanced_features
        assert 'llm_bullish_score' in enhanced_features
        assert 'llm_distance_to_support' in enhanced_features

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_llm_mathematical_features(self, sample_price_data):
        """Test LLM mathematical feature generation."""
        recognizer = LLMPatternRecognizer()

        features = await recognizer._get_llm_mathematical_features(sample_price_data)

        assert isinstance(features, dict)
        assert 'llm_fractal_dimension' in features
        assert 'llm_hurst_exponent' in features
        assert 'llm_price_acceleration' in features

        # Features should be numeric
        for key, value in features.items():
            assert isinstance(value, (int, float))

    def test_generate_single_synthetic_pattern(self):
        """Test single synthetic pattern generation."""
        recognizer = LLMPatternRecognizer()

        characteristics = {
            "typical_duration_range": [10, 30],
            "return_characteristics": {"min": -0.05, "max": 0.15, "typical": 0.03}
        }

        pattern = recognizer._generate_single_synthetic_pattern(
            characteristics, "bullish_reversal"
        )

        assert pattern is not None
        assert isinstance(pattern, pd.DataFrame)
        assert 'open' in pattern.columns
        assert 'high' in pattern.columns
        assert 'low' in pattern.columns
        assert 'close' in pattern.columns
        assert 'volume' in pattern.columns

        # Check OHLC consistency
        assert (pattern['high'] >= pattern[['open', 'close']].max(axis=1)).all()
        assert (pattern['low'] <= pattern[['open', 'close']].min(axis=1)).all()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_call_llm_success(self, mock_llm_response):
        """Test successful LLM API call."""
        recognizer = LLMPatternRecognizer(api_key="test_key")

        with patch('aiohttp.ClientSession.post') as mock_post:
            # Mock successful response
            mock_response = Mock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=mock_llm_response)
            mock_post.return_value.__aenter__.return_value = mock_response

            result = await recognizer._call_llm("test prompt")

            assert isinstance(result, str)
            assert "ascending_triangle" in result

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_call_llm_error(self):
        """Test LLM API call error handling."""
        recognizer = LLMPatternRecognizer(api_key="test_key")

        with patch('aiohttp.ClientSession.post') as mock_post:
            # Mock error response
            mock_response = Mock()
            mock_response.status = 500
            mock_post.return_value.__aenter__.return_value = mock_response

            result = await recognizer._call_llm("test prompt")

            assert result == ""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_call_llm_exception(self):
        """Test LLM API call exception handling."""
        recognizer = LLMPatternRecognizer(api_key="test_key")

        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_post.side_effect = Exception("Network error")

            result = await recognizer._call_llm("test prompt")

            assert result == ""


class TestConvenienceFunctions:
    """Test convenience functions."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_stock_pattern(self, sample_price_data, mock_llm_response):
        """Test analyze_stock_pattern convenience function."""
        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_response = Mock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=mock_llm_response)
            mock_post.return_value.__aenter__.return_value = mock_response

            analysis = await analyze_stock_pattern(
                sample_price_data, "AAPL", "test_key", LLMProvider.DEEPSEEK
            )

            assert isinstance(analysis, PatternAnalysis)
            assert analysis.pattern_type == "ascending_triangle"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_enhance_features_with_llm_function(self, sample_price_data):
        """Test enhance_features_with_llm convenience function."""
        existing_features = {'momentum': 0.05}

        with patch('modeling.llm_pattern_recognition.LLMPatternRecognizer') as mock_class:
            mock_recognizer = Mock()
            mock_recognizer.enhance_feature_engineering = AsyncMock(
                return_value={'momentum': 0.05, 'llm_feature': 0.8}
            )
            mock_class.return_value = mock_recognizer

            enhanced = await enhance_features_with_llm(
                sample_price_data, existing_features, "test_key"
            )

            assert 'momentum' in enhanced
            assert 'llm_feature' in enhanced

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_training_data_with_llm_function(self, sample_price_data):
        """Test generate_training_data_with_llm convenience function."""
        existing_patterns = [sample_price_data]

        with patch('modeling.llm_pattern_recognition.LLMPatternRecognizer') as mock_class:
            mock_recognizer = Mock()
            mock_recognizer.generate_synthetic_patterns = AsyncMock(
                return_value=[sample_price_data, sample_price_data]
            )
            mock_class.return_value = mock_recognizer

            synthetic_data = await generate_training_data_with_llm(
                existing_patterns, 2, "bullish_pattern", "test_key"
            )

            assert len(synthetic_data) == 2
            assert all(isinstance(df, pd.DataFrame) for df in synthetic_data)


class TestEdgeCasesAndErrorHandling:
    """Test edge cases and error handling."""

    def test_empty_price_data_handling(self):
        """Test handling of empty price data."""
        recognizer = LLMPatternRecognizer()

        empty_data = pd.DataFrame()
        signature = recognizer._create_pattern_signature(empty_data)

        # Should handle gracefully
        assert isinstance(signature, str)

    def test_insufficient_price_data(self):
        """Test handling of insufficient price data."""
        recognizer = LLMPatternRecognizer()

        # Very small dataset
        small_data = pd.DataFrame({
            'close': [100, 101]
        })

        sequence = recognizer._format_price_sequence(small_data)
        assert isinstance(sequence, str)

        # Should still work with mathematical features
        features = asyncio.run(recognizer._get_llm_mathematical_features(small_data))
        assert isinstance(features, dict)

    def test_extreme_price_values(self):
        """Test handling of extreme price values."""
        recognizer = LLMPatternRecognizer()

        # Extreme values
        extreme_data = pd.DataFrame({
            'close': [1e-10, 1e10, 0.001, 1000000]
        })

        fractal_dim = recognizer._calculate_fractal_dimension(extreme_data)
        assert 1.0 <= fractal_dim <= 2.0

        hurst = recognizer._calculate_hurst_exponent(extreme_data)
        assert 0.0 <= hurst <= 1.0

    def test_constant_price_values(self):
        """Test handling of constant price values."""
        recognizer = LLMPatternRecognizer()

        # All same prices
        constant_data = pd.DataFrame({
            'close': [100, 100, 100, 100, 100]
        })

        fractal_dim = recognizer._calculate_fractal_dimension(constant_data)
        assert fractal_dim == 1.5  # Should return default

        hurst = recognizer._calculate_hurst_exponent(constant_data)
        assert hurst == 0.5  # Should return default

    def test_pattern_analysis_with_missing_fields(self):
        """Test pattern analysis parsing with missing fields."""
        recognizer = LLMPatternRecognizer()

        incomplete_json = """
        {
            "pattern_type": "triangle",
            "confidence": 0.7
        }
        """

        analysis = recognizer._parse_pattern_analysis(incomplete_json)

        assert isinstance(analysis, PatternAnalysis)
        assert analysis.pattern_type == "triangle"
        assert analysis.confidence == 0.7
        assert analysis.predicted_direction == "neutral"  # Default value

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_synthetic_patterns_error_handling(self, sample_price_data):
        """Test error handling in synthetic pattern generation."""
        recognizer = LLMPatternRecognizer()

        # Mock analysis to raise exception
        with patch.object(recognizer, '_analyze_pattern_characteristics') as mock_analyze:
            mock_analyze.side_effect = Exception("Analysis failed")

            patterns = await recognizer.generate_synthetic_patterns(
                [sample_price_data], 5, "test_pattern"
            )

            # Should handle gracefully and return empty list
            assert isinstance(patterns, list)
            assert len(patterns) == 0

    def test_market_summary_with_insufficient_data(self):
        """Test market summary with insufficient data."""
        recognizer = LLMPatternRecognizer()

        # Single data point
        single_point = pd.DataFrame({'close': [100]})

        summary = recognizer._create_market_summary(single_point)

        assert isinstance(summary, str)
        assert "Market Summary" in summary

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_rate_limiting_semaphore(self):
        """Test rate limiting with semaphore."""
        recognizer = LLMPatternRecognizer(max_concurrent_requests=1)

        call_count = 0

        async def mock_api_call(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)  # Simulate API delay
            return ""

        with patch.object(recognizer, '_call_llm', side_effect=mock_api_call):
            # Start multiple concurrent calls
            tasks = [
                recognizer.analyze_price_pattern(pd.DataFrame({'close': [100]}), "TEST")
                for _ in range(3)
            ]

            await asyncio.gather(*tasks)

            # All calls should complete despite rate limiting
            assert call_count == 3


@pytest.mark.integration
class TestIntegrationScenarios:
    """Test integration scenarios."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_complete_pattern_analysis_workflow(self, sample_price_data):
        """Test complete pattern analysis workflow."""
        recognizer = LLMPatternRecognizer(api_key="test_key")

        # Mock successful LLM call
        with patch.object(recognizer, '_call_llm') as mock_call:
            mock_call.return_value = """
            {
                "pattern_type": "ascending_triangle",
                "confidence": 0.85,
                "description": "Clear ascending triangle with breakout",
                "technical_indicators": {
                    "trend_direction": "up",
                    "momentum": "strong"
                },
                "predicted_direction": "bullish",
                "support_resistance": {
                    "support": 98.0,
                    "resistance": 105.0
                },
                "volume_analysis": "Increasing volume",
                "risk_assessment": "Low risk",
                "timeframe_relevance": ["short_term"]
            }
            """

            # Analyze pattern
            analysis = await recognizer.analyze_price_pattern(
                sample_price_data, "AAPL", "daily"
            )

            # Enhance features
            base_features = {'momentum': 0.05, 'volatility': 0.02}
            enhanced = await recognizer.enhance_feature_engineering(
                sample_price_data, base_features
            )

            # Validate results
            assert isinstance(analysis, PatternAnalysis)
            assert analysis.confidence == 0.85
            assert analysis.predicted_direction == "bullish"

            assert isinstance(enhanced, dict)
            assert 'momentum' in enhanced  # Original feature preserved
            assert 'llm_pattern_confidence' in enhanced  # LLM feature added
            assert enhanced['llm_pattern_confidence'] == 0.85


if __name__ == "__main__":
    pytest.main([__file__])