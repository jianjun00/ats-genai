"""
Tests for interpretability framework for residual return predictions.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock

from domains.ml.services.interpretability_framework import (
    PredictionExplanation,
    ResidualReturnInterpreter
)


@pytest.fixture
def sample_prediction_data():
    """Sample prediction data."""
    return {
        'predicted_residual_return': 0.025,
        'prediction_confidence': 0.85,
        'prediction_horizon': 5,
        'features': {
            'ema_20': 105.0,
            'rsi_14': 75.0,
            'atr_14': 2.5,
            'volume_trend': 1.3,
            'event_proximity_score': 0.8,
            'sector_return_1d': 0.015,
            'quarterly_trend_direction': 1.0,
            'mtf_trend_strength': 0.7
        },
        'model_feature_importance': {
            'ema_20': 0.15,
            'rsi_14': 0.12,
            'volume_trend': 0.10,
            'event_proximity_score': 0.08
        }
    }


class TestPredictionExplanation:
    """Test PredictionExplanation dataclass."""

    def test_prediction_explanation_creation(self):
        """Test PredictionExplanation creation."""
        explanation = PredictionExplanation(
            instrument_id=123,
            symbol="AAPL",
            prediction_date=datetime(2024, 1, 15),
            predicted_return=0.025,
            confidence=0.85,
            executive_summary="Strong bullish signal",
            technical_analysis="RSI overbought, momentum strong",
            event_analysis="Earnings approaching",
            multi_timeframe_analysis="Aligned across timeframes",
            risk_assessment="Low risk, clear trend",
            factor_attribution="Market factor positive",
            confidence_analysis="High confidence prediction"
        )

        assert explanation.instrument_id == 123
        assert explanation.symbol == "AAPL"
        assert explanation.predicted_return == 0.025
        assert explanation.confidence == 0.85
        assert "bullish" in explanation.executive_summary


class TestResidualReturnInterpreter:
    """Test ResidualReturnInterpreter functionality."""

    def test_interpreter_initialization(self):
        """Test interpreter initialization."""
        interpreter = ResidualReturnInterpreter()

        assert interpreter is not None
        assert hasattr(interpreter, 'create_prediction_explanation')

    def test_create_prediction_explanation(self, sample_prediction_data):
        """Test prediction explanation creation."""
        interpreter = ResidualReturnInterpreter()

        explanation = interpreter.create_prediction_explanation(
            instrument_id=123,
            symbol="AAPL",
            prediction_date=datetime(2024, 1, 15),
            **sample_prediction_data
        )

        assert isinstance(explanation, PredictionExplanation)
        assert explanation.instrument_id == 123
        assert explanation.symbol == "AAPL"
        assert explanation.predicted_return == 0.025
        assert explanation.confidence == 0.85

        # Should have all explanation components
        assert len(explanation.executive_summary) > 0
        assert len(explanation.technical_analysis) > 0
        assert len(explanation.event_analysis) > 0
        assert len(explanation.multi_timeframe_analysis) > 0
        assert len(explanation.risk_assessment) > 0
        assert len(explanation.factor_attribution) > 0
        assert len(explanation.confidence_analysis) > 0

    def test_generate_executive_summary(self, sample_prediction_data):
        """Test executive summary generation."""
        interpreter = ResidualReturnInterpreter()

        summary = interpreter._generate_executive_summary(
            sample_prediction_data['predicted_residual_return'],
            sample_prediction_data['prediction_confidence'],
            sample_prediction_data['prediction_horizon'],
            sample_prediction_data['features']
        )

        assert isinstance(summary, str)
        assert len(summary) > 50  # Should be substantial
        assert "bullish" in summary.lower() or "positive" in summary.lower()
        assert "confidence" in summary.lower()
        assert str(sample_prediction_data['prediction_horizon']) in summary

    def test_generate_technical_analysis(self, sample_prediction_data):
        """Test technical analysis generation."""
        interpreter = ResidualReturnInterpreter()

        analysis = interpreter._generate_technical_analysis(
            sample_prediction_data['features'],
            sample_prediction_data['model_feature_importance']
        )

        assert isinstance(analysis, str)
        assert len(analysis) > 50
        # Should mention key technical indicators
        assert "ema" in analysis.lower() or "rsi" in analysis.lower()

    def test_generate_event_analysis(self, sample_prediction_data):
        """Test event analysis generation."""
        interpreter = ResidualReturnInterpreter()

        analysis = interpreter._generate_event_analysis(
            sample_prediction_data['features']
        )

        assert isinstance(analysis, str)
        assert len(analysis) > 20
        # Should reference events if present
        if sample_prediction_data['features']['event_proximity_score'] > 0:
            assert "event" in analysis.lower()

    def test_generate_multi_timeframe_analysis(self, sample_prediction_data):
        """Test multi-timeframe analysis generation."""
        interpreter = ResidualReturnInterpreter()

        analysis = interpreter._generate_multi_timeframe_analysis(
            sample_prediction_data['features']
        )

        assert isinstance(analysis, str)
        assert len(analysis) > 30
        assert "timeframe" in analysis.lower() or "trend" in analysis.lower()

    def test_generate_risk_assessment(self, sample_prediction_data):
        """Test risk assessment generation."""
        interpreter = ResidualReturnInterpreter()

        assessment = interpreter._generate_risk_assessment(
            sample_prediction_data['predicted_residual_return'],
            sample_prediction_data['prediction_confidence'],
            sample_prediction_data['features']
        )

        assert isinstance(assessment, str)
        assert len(assessment) > 30
        assert "risk" in assessment.lower()

    def test_generate_factor_attribution(self, sample_prediction_data):
        """Test factor attribution generation."""
        interpreter = ResidualReturnInterpreter()

        attribution = interpreter._generate_factor_attribution(
            sample_prediction_data['features'],
            sample_prediction_data['model_feature_importance']
        )

        assert isinstance(attribution, str)
        assert len(attribution) > 30
        # Should mention important factors
        assert "factor" in attribution.lower() or "driver" in attribution.lower()

    def test_generate_confidence_analysis(self, sample_prediction_data):
        """Test confidence analysis generation."""
        interpreter = ResidualReturnInterpreter()

        analysis = interpreter._generate_confidence_analysis(
            sample_prediction_data['prediction_confidence'],
            sample_prediction_data['features']
        )

        assert isinstance(analysis, str)
        assert len(analysis) > 30
        assert "confidence" in analysis.lower()

    def test_classify_prediction_strength(self):
        """Test prediction strength classification."""
        interpreter = ResidualReturnInterpreter()

        # Test different return levels
        assert interpreter._classify_prediction_strength(0.001) == "weak"
        assert interpreter._classify_prediction_strength(0.015) == "moderate"
        assert interpreter._classify_prediction_strength(0.035) == "strong"
        assert interpreter._classify_prediction_strength(-0.025) == "strong"  # Magnitude

    def test_assess_risk_level(self, sample_prediction_data):
        """Test risk level assessment."""
        interpreter = ResidualReturnInterpreter()

        # High confidence, strong indicators
        risk = interpreter._assess_risk_level(
            0.85, sample_prediction_data['features']
        )
        assert risk in ["low", "medium", "high"]

        # Low confidence
        risk = interpreter._assess_risk_level(
            0.45, sample_prediction_data['features']
        )
        assert risk in ["medium", "high"]

    def test_negative_prediction_explanation(self):
        """Test explanation for negative predictions."""
        interpreter = ResidualReturnInterpreter()

        negative_data = {
            'predicted_residual_return': -0.020,
            'prediction_confidence': 0.75,
            'prediction_horizon': 3,
            'features': {
                'rsi_14': 25.0,  # Oversold
                'ema_20': 95.0,
                'volume_trend': 0.8,
                'quarterly_trend_direction': -1.0
            },
            'model_feature_importance': {
                'rsi_14': 0.20,
                'quarterly_trend_direction': 0.15
            }
        }

        explanation = interpreter.create_prediction_explanation(
            instrument_id=456,
            symbol="XYZ",
            prediction_date=datetime(2024, 1, 15),
            **negative_data
        )

        assert explanation.predicted_return == -0.020
        assert "bearish" in explanation.executive_summary.lower() or "negative" in explanation.executive_summary.lower()

    def test_low_confidence_explanation(self):
        """Test explanation for low confidence predictions."""
        interpreter = ResidualReturnInterpreter()

        low_conf_data = {
            'predicted_residual_return': 0.005,
            'prediction_confidence': 0.45,
            'prediction_horizon': 2,
            'features': {
                'rsi_14': 50.0,
                'ema_20': 100.0,
                'volume_trend': 1.0
            },
            'model_feature_importance': {
                'rsi_14': 0.10
            }
        }

        explanation = interpreter.create_prediction_explanation(
            instrument_id=789,
            symbol="ABC",
            prediction_date=datetime(2024, 1, 15),
            **low_conf_data
        )

        assert "uncertain" in explanation.confidence_analysis.lower() or "low" in explanation.confidence_analysis.lower()

    def test_missing_features_handling(self):
        """Test handling of missing features."""
        interpreter = ResidualReturnInterpreter()

        minimal_data = {
            'predicted_residual_return': 0.015,
            'prediction_confidence': 0.70,
            'prediction_horizon': 1,
            'features': {},  # No features
            'model_feature_importance': {}
        }

        explanation = interpreter.create_prediction_explanation(
            instrument_id=999,
            symbol="TEST",
            prediction_date=datetime(2024, 1, 15),
            **minimal_data
        )

        # Should still create explanation
        assert isinstance(explanation, PredictionExplanation)
        assert len(explanation.executive_summary) > 0


class TestConvenienceFunction:
    """Test convenience function."""

    def test_create_prediction_explanation_for_model(self, sample_prediction_data):
        """Test convenience function."""
        with patch('modeling.interpretability_framework.ResidualReturnInterpreter') as mock_class:
            mock_interpreter = Mock()
            mock_explanation = PredictionExplanation(
                instrument_id=123, symbol="AAPL", prediction_date=datetime(2024, 1, 15),
                predicted_return=0.025, confidence=0.85,
                executive_summary="Test summary",
                technical_analysis="Test technical",
                event_analysis="Test events",
                multi_timeframe_analysis="Test timeframes",
                risk_assessment="Test risk",
                factor_attribution="Test factors",
                confidence_analysis="Test confidence"
            )
            mock_interpreter.create_prediction_explanation.return_value = mock_explanation
            mock_class.return_value = mock_interpreter

            result = create_prediction_explanation_for_model(
                instrument_id=123,
                symbol="AAPL",
                prediction_date=datetime(2024, 1, 15),
                **sample_prediction_data
            )

            assert isinstance(result, PredictionExplanation)
            assert result.symbol == "AAPL"


class TestEdgeCasesAndErrorHandling:
    """Test edge cases and error handling."""

    def test_extreme_prediction_values(self):
        """Test handling of extreme prediction values."""
        interpreter = ResidualReturnInterpreter()

        extreme_data = {
            'predicted_residual_return': 0.15,  # 15% return
            'prediction_confidence': 0.95,
            'prediction_horizon': 1,
            'features': {},
            'model_feature_importance': {}
        }

        explanation = interpreter.create_prediction_explanation(
            instrument_id=123,
            symbol="EXTREME",
            prediction_date=datetime(2024, 1, 15),
            **extreme_data
        )

        assert "strong" in explanation.executive_summary.lower()
        assert "high" in explanation.risk_assessment.lower()  # Should flag high magnitude as risky

    def test_zero_prediction(self):
        """Test handling of zero/neutral predictions."""
        interpreter = ResidualReturnInterpreter()

        neutral_data = {
            'predicted_residual_return': 0.0,
            'prediction_confidence': 0.60,
            'prediction_horizon': 3,
            'features': {},
            'model_feature_importance': {}
        }

        explanation = interpreter.create_prediction_explanation(
            instrument_id=456,
            symbol="NEUTRAL",
            prediction_date=datetime(2024, 1, 15),
            **neutral_data
        )

        assert "neutral" in explanation.executive_summary.lower() or "flat" in explanation.executive_summary.lower()

    def test_feature_importance_missing(self, sample_prediction_data):
        """Test handling when feature importance is missing."""
        interpreter = ResidualReturnInterpreter()

        data_no_importance = sample_prediction_data.copy()
        data_no_importance['model_feature_importance'] = {}

        explanation = interpreter.create_prediction_explanation(
            instrument_id=789,
            symbol="NO_IMP",
            prediction_date=datetime(2024, 1, 15),
            **data_no_importance
        )

        # Should still create explanation
        assert isinstance(explanation, PredictionExplanation)
        assert len(explanation.technical_analysis) > 0

    def test_very_short_horizon(self):
        """Test very short prediction horizons."""
        interpreter = ResidualReturnInterpreter()

        short_horizon_data = {
            'predicted_residual_return': 0.01,
            'prediction_confidence': 0.80,
            'prediction_horizon': 1,
            'features': {},
            'model_feature_importance': {}
        }

        explanation = interpreter.create_prediction_explanation(
            instrument_id=111,
            symbol="SHORT",
            prediction_date=datetime(2024, 1, 15),
            **short_horizon_data
        )

        assert "1 day" in explanation.executive_summary or "short" in explanation.executive_summary.lower()

    def test_long_horizon(self):
        """Test long prediction horizons."""
        interpreter = ResidualReturnInterpreter()

        long_horizon_data = {
            'predicted_residual_return': 0.02,
            'prediction_confidence': 0.70,
            'prediction_horizon': 10,
            'features': {},
            'model_feature_importance': {}
        }

        explanation = interpreter.create_prediction_explanation(
            instrument_id=222,
            symbol="LONG",
            prediction_date=datetime(2024, 1, 15),
            **long_horizon_data
        )

        assert "10 day" in explanation.executive_summary or str(long_horizon_data['prediction_horizon']) in explanation.executive_summary


if __name__ == "__main__":
    pytest.main([__file__])