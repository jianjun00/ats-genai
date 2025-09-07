"""
Interpretability Framework for Residual Return Prediction Models.
Provides comprehensive explanations for model predictions using multiple interpretation methods.
"""

import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import logging

from modeling.llm_pattern_recognition import LLMPatternRecognizer, LLMProvider
from modeling.multi_timeframe_analyzer import MultiTimeFrameFeatures
from modeling.event_features import EventFeatures

logger = logging.getLogger(__name__)


class ExplanationType(Enum):
    """Types of explanations available."""
    EXECUTIVE_SUMMARY = "executive_summary"
    TECHNICAL_ANALYSIS = "technical_analysis"
    EVENT_DRIVEN = "event_driven"
    MULTI_TIMEFRAME = "multi_timeframe"
    RISK_ASSESSMENT = "risk_assessment"
    FACTOR_ATTRIBUTION = "factor_attribution"
    CONFIDENCE_ANALYSIS = "confidence_analysis"


@dataclass
class FeatureImportance:
    """Feature importance with explanation."""
    feature_name: str
    importance_score: float
    contribution: float  # Actual contribution to prediction
    description: str
    category: str  # 'technical', 'event', 'market', 'factor'
    timeframe: Optional[str] = None


@dataclass
class PredictionExplanation:
    """Comprehensive explanation of a prediction."""
    symbol: str
    prediction_date: datetime
    prediction_horizons: List[int]
    predictions: Dict[str, Dict[str, float]]  # {horizon: {quantile: value}}

    # Different types of explanations
    executive_summary: str
    technical_explanation: Dict[str, Any]
    event_explanation: Dict[str, Any]
    timeframe_explanation: Dict[str, Any]
    risk_explanation: Dict[str, Any]
    factor_explanation: Dict[str, Any]
    confidence_explanation: Dict[str, Any]

    # Feature importance
    top_features: List[FeatureImportance]
    feature_contributions: Dict[str, float]

    # Supporting data
    market_regime: str
    volatility_environment: str
    trading_recommendations: List[str]
    warnings: List[str]


class ResidualReturnInterpreter:
    """Main class for generating model explanations."""

    def __init__(self,
                 model_name: str = "ResidualReturnPredictor",
                 llm_recognizer: Optional[LLMPatternRecognizer] = None):
        self.model_name = model_name
        self.llm_recognizer = llm_recognizer

        # Feature categorization
        self.feature_categories = {
            'technical': ['ema_', 'rsi_', 'atr_', 'vwap_', 'volume_', 'ma_', 'trend_', 'volatility'],
            'event': ['event_', 'earnings_', 'economic_', 'options_', 'quarter_', 'month_'],
            'market': ['market_', 'sector_', 'day_of_', 'is_month_', 'is_quarter_'],
            'factor': ['market_beta', 'size_factor', 'value_factor', 'momentum_factor', 'loading'],
            'timeframe': ['daily_', 'weekly_', 'monthly_', 'quarterly_', 'mtf_'],
            'llm': ['llm_', 'pattern_']
        }

        # Importance thresholds
        self.importance_thresholds = {
            'high': 0.1,
            'medium': 0.05,
            'low': 0.01
        }

    def generate_comprehensive_explanation(self,
                                         symbol: str,
                                         prediction_date: datetime,
                                         predictions: Dict[str, Dict[str, float]],
                                         feature_values: Dict[str, Any],
                                         feature_importance: Dict[str, float],
                                         mtf_features: Optional[MultiTimeFrameFeatures] = None,
                                         event_features: Optional[EventFeatures] = None,
                                         model_metadata: Optional[Dict[str, Any]] = None) -> PredictionExplanation:
        """
        Generate comprehensive explanation for a prediction.

        Args:
            symbol: Stock symbol
            prediction_date: Date of prediction
            predictions: Model predictions {horizon: {quantile: value}}
            feature_values: Input feature values
            feature_importance: Feature importance scores
            mtf_features: Multi-timeframe analysis results
            event_features: Event analysis results
            model_metadata: Additional model information

        Returns:
            Complete prediction explanation
        """
        logger.debug(f"Generating explanation for {symbol} on {prediction_date}")

        try:
            # Extract prediction horizons
            horizons = list(predictions.keys())
            horizons_int = [int(h.replace('d', '')) for h in horizons if 'd' in h]

            # Generate different types of explanations
            executive_summary = self._generate_executive_summary(
                symbol, predictions, feature_values, mtf_features
            )

            technical_explanation = self._generate_technical_explanation(
                feature_values, feature_importance, mtf_features
            )

            event_explanation = self._generate_event_explanation(
                feature_values, feature_importance, event_features
            )

            timeframe_explanation = self._generate_timeframe_explanation(
                mtf_features, feature_importance
            )

            risk_explanation = self._generate_risk_explanation(
                predictions, feature_values, model_metadata
            )

            factor_explanation = self._generate_factor_explanation(
                feature_values, feature_importance
            )

            confidence_explanation = self._generate_confidence_explanation(
                predictions, feature_importance, model_metadata
            )

            # Extract top features
            top_features = self._extract_top_features(
                feature_importance, feature_values
            )

            # Calculate feature contributions
            feature_contributions = self._calculate_feature_contributions(
                feature_importance, feature_values, predictions
            )

            # Determine market regime and environment
            market_regime = self._determine_market_regime(feature_values, mtf_features)
            volatility_environment = self._determine_volatility_environment(feature_values)

            # Generate trading recommendations
            trading_recommendations = self._generate_trading_recommendations(
                predictions, technical_explanation, risk_explanation
            )

            # Generate warnings
            warnings = self._generate_warnings(
                predictions, feature_values, model_metadata
            )

            return PredictionExplanation(
                symbol=symbol,
                prediction_date=prediction_date,
                prediction_horizons=horizons_int,
                predictions=predictions,
                executive_summary=executive_summary,
                technical_explanation=technical_explanation,
                event_explanation=event_explanation,
                timeframe_explanation=timeframe_explanation,
                risk_explanation=risk_explanation,
                factor_explanation=factor_explanation,
                confidence_explanation=confidence_explanation,
                top_features=top_features,
                feature_contributions=feature_contributions,
                market_regime=market_regime,
                volatility_environment=volatility_environment,
                trading_recommendations=trading_recommendations,
                warnings=warnings
            )

        except Exception as e:
            logger.error(f"Failed to generate explanation for {symbol}: {e}")
            raise RuntimeError(f"Failed to generate prediction explanation for {symbol}: {e}")

    def _generate_executive_summary(self,
                                  symbol: str,
                                  predictions: Dict[str, Dict[str, float]],
                                  feature_values: Dict[str, Any],
                                  mtf_features: Optional[MultiTimeFrameFeatures]) -> str:
        """Generate executive summary of the prediction."""
        try:
            # Get key prediction metrics
            if '1d' in predictions:
                day1_median = predictions['1d'].get('quantile_0.5', 0)
                day1_q05 = predictions['1d'].get('quantile_0.05', 0)
                day1_q95 = predictions['1d'].get('quantile_0.95', 0)
            else:
                day1_median = day1_q05 = day1_q95 = 0

            if '5d' in predictions:
                day5_median = predictions['5d'].get('quantile_0.5', 0)
            else:
                day5_median = 0

            # Determine direction and confidence
            direction = self._classify_direction(day1_median)
            confidence_level = self._assess_prediction_confidence(day1_q05, day1_q95, day1_median)

            # Get dominant trend from multi-timeframe analysis
            dominant_trend = "neutral"
            trend_strength = 0.0
            if mtf_features:
                dominant_trend = mtf_features.dominant_trend
                trend_strength = mtf_features.trend_strength

            # Build summary
            summary_parts = []

            # Main prediction
            summary_parts.append(
                f"**{symbol} Residual Return Forecast**: The model predicts a "
                f"**{direction}** move with **{confidence_level}** confidence over the next 1-5 days."
            )

            # Specific numbers
            summary_parts.append(
                f"• **1-day outlook**: {day1_median:+.2%} (90% range: {day1_q05:+.2%} to {day1_q95:+.2%})"
            )

            if abs(day5_median) > 0.001:
                summary_parts.append(
                    f"• **5-day outlook**: {day5_median:+.2%}"
                )

            # Multi-timeframe context
            if dominant_trend != "neutral":
                summary_parts.append(
                    f"• **Trend context**: {dominant_trend.title()} trend across multiple timeframes "
                    f"(strength: {trend_strength:.0%})"
                )

            # Key drivers preview
            top_driver = self._get_top_driver_description(feature_values)
            if top_driver:
                summary_parts.append(f"• **Key driver**: {top_driver}")

            return "\n".join(summary_parts)

        except Exception as e:
            logger.warning(f"Failed to generate executive summary: {e}")
            return f"Prediction generated for {symbol} - detailed analysis below."

    def _generate_technical_explanation(self,
                                      feature_values: Dict[str, Any],
                                      feature_importance: Dict[str, float],
                                      mtf_features: Optional[MultiTimeFrameFeatures]) -> Dict[str, Any]:
        """Generate technical analysis explanation."""
        explanation = {
            'trend_analysis': {},
            'momentum_indicators': {},
            'volatility_analysis': {},
            'volume_analysis': {},
            'support_resistance': {},
            'key_signals': []
        }

        try:
            # Trend analysis
            trend_features = {k: v for k, v in feature_values.items() if 'trend' in k.lower()}
            if trend_features:
                explanation['trend_analysis'] = self._analyze_trend_signals(trend_features, feature_importance)

            # Momentum indicators
            momentum_features = {k: v for k, v in feature_values.items()
                               if any(indicator in k.lower() for indicator in ['rsi', 'momentum', 'macd'])}
            if momentum_features:
                explanation['momentum_indicators'] = self._analyze_momentum_signals(momentum_features)

            # Volatility analysis
            vol_features = {k: v for k, v in feature_values.items()
                          if any(vol_term in k.lower() for vol_term in ['atr', 'volatility', 'vol'])}
            if vol_features:
                explanation['volatility_analysis'] = self._analyze_volatility_signals(vol_features)

            # Volume analysis
            volume_features = {k: v for k, v in feature_values.items() if 'volume' in k.lower()}
            if volume_features:
                explanation['volume_analysis'] = self._analyze_volume_signals(volume_features)

            # Support/Resistance from LLM or technical analysis
            sr_features = {k: v for k, v in feature_values.items()
                         if any(sr in k.lower() for sr in ['support', 'resistance', 'sr_'])}
            if sr_features:
                explanation['support_resistance'] = self._analyze_support_resistance(sr_features)

            # Generate key technical signals
            explanation['key_signals'] = self._generate_technical_signals(
                feature_values, feature_importance
            )

        except Exception as e:
            logger.warning(f"Failed to generate technical explanation: {e}")

        return explanation

    def _generate_event_explanation(self,
                                  feature_values: Dict[str, Any],
                                  feature_importance: Dict[str, float],
                                  event_features: Optional[EventFeatures]) -> Dict[str, Any]:
        """Generate event-driven explanation."""
        explanation = {
            'upcoming_events': [],
            'event_impact_analysis': {},
            'historical_patterns': {},
            'event_risk_factors': []
        }

        try:
            # Extract event-related features
            event_feature_values = {k: v for k, v in feature_values.items() if 'event' in k.lower()}

            if event_features:
                # Upcoming events
                for event in event_features.upcoming_events:
                    days_until = (event['event_date'] - event_features.date.date()).days
                    explanation['upcoming_events'].append({
                        'type': event['type'],
                        'name': event['event_name'],
                        'days_until': days_until,
                        'importance': event.get('importance', 'Medium'),
                        'expected_impact': self._estimate_event_impact(event, event_features)
                    })

                # Historical patterns
                for event_type, pattern in event_features.historical_patterns.items():
                    explanation['historical_patterns'][event_type] = {
                        'avg_reaction': f"{pattern.avg_reaction:+.2%}",
                        'volatility_spike': f"{pattern.volatility_spike:.1f}x",
                        'confidence': f"{pattern.confidence:.0%}",
                        'sample_size': pattern.sample_size
                    }

            # Event impact analysis from feature importance
            important_event_features = {
                k: v for k, v in feature_importance.items()
                if 'event' in k.lower() and abs(v) > self.importance_thresholds['low']
            }

            if important_event_features:
                explanation['event_impact_analysis'] = self._analyze_event_impacts(
                    important_event_features, feature_values
                )

            # Event risk factors
            explanation['event_risk_factors'] = self._identify_event_risks(
                event_feature_values, feature_importance
            )

        except Exception as e:
            logger.warning(f"Failed to generate event explanation: {e}")

        return explanation

    def _generate_timeframe_explanation(self,
                                      mtf_features: Optional[MultiTimeFrameFeatures],
                                      feature_importance: Dict[str, float]) -> Dict[str, Any]:
        """Generate multi-timeframe explanation."""
        explanation = {
            'timeframe_summary': {},
            'trend_alignment': {},
            'timeframe_conflicts': [],
            'dominant_drivers': {}
        }

        try:
            if not mtf_features:
                explanation['timeframe_summary'] = {'note': 'Multi-timeframe analysis not available'}
                return explanation

            # Timeframe summary
            timeframes = ['daily', 'weekly', 'monthly', 'quarterly']
            for tf in timeframes:
                tf_features = getattr(mtf_features, f'{tf}_features', {})
                if tf_features:
                    explanation['timeframe_summary'][tf] = self._summarize_timeframe(tf, tf_features)

            # Trend alignment analysis
            explanation['trend_alignment'] = {
                'dominant_trend': mtf_features.dominant_trend,
                'trend_strength': f"{mtf_features.trend_strength:.0%}",
                'alignment_scores': mtf_features.timeframe_alignment
            }

            # Identify timeframe conflicts
            explanation['timeframe_conflicts'] = self._identify_timeframe_conflicts(mtf_features)

            # Dominant drivers by timeframe
            for tf in timeframes:
                tf_importance = {k: v for k, v in feature_importance.items() if k.startswith(tf)}
                if tf_importance:
                    top_driver = max(tf_importance.items(), key=lambda x: abs(x[1]))
                    explanation['dominant_drivers'][tf] = {
                        'feature': top_driver[0],
                        'importance': top_driver[1],
                        'description': self._describe_feature(top_driver[0])
                    }

        except Exception as e:
            logger.warning(f"Failed to generate timeframe explanation: {e}")

        return explanation

    def _generate_risk_explanation(self,
                                 predictions: Dict[str, Dict[str, float]],
                                 feature_values: Dict[str, Any],
                                 model_metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate risk assessment explanation."""
        explanation = {
            'prediction_uncertainty': {},
            'tail_risks': {},
            'model_confidence': {},
            'risk_factors': [],
            'scenario_analysis': {}
        }

        try:
            # Prediction uncertainty
            for horizon, pred_quantiles in predictions.items():
                if 'quantile_0.05' in pred_quantiles and 'quantile_0.95' in pred_quantiles:
                    q05 = pred_quantiles['quantile_0.05']
                    q95 = pred_quantiles['quantile_0.95']
                    uncertainty = q95 - q05

                    explanation['prediction_uncertainty'][horizon] = {
                        'range': f"{q05:+.2%} to {q95:+.2%}",
                        'uncertainty_width': f"{uncertainty:.2%}",
                        'risk_level': 'High' if uncertainty > 0.1 else 'Medium' if uncertainty > 0.05 else 'Low'
                    }

            # Tail risks
            explanation['tail_risks'] = self._assess_tail_risks(predictions)

            # Model confidence
            if model_metadata:
                explanation['model_confidence'] = {
                    'r_squared': model_metadata.get('r_squared', 'N/A'),
                    'prediction_std': model_metadata.get('prediction_std', 'N/A'),
                    'feature_coverage': model_metadata.get('feature_coverage', 'N/A')
                }

            # Risk factors
            explanation['risk_factors'] = self._identify_risk_factors(feature_values)

            # Scenario analysis
            explanation['scenario_analysis'] = self._generate_scenario_analysis(predictions, feature_values)

        except Exception as e:
            logger.warning(f"Failed to generate risk explanation: {e}")

        return explanation

    def _generate_factor_explanation(self,
                                   feature_values: Dict[str, Any],
                                   feature_importance: Dict[str, float]) -> Dict[str, Any]:
        """Generate factor attribution explanation."""
        explanation = {
            'factor_exposures': {},
            'factor_contributions': {},
            'factor_analysis': {},
            'attribution_summary': ''
        }

        try:
            # Extract factor-related features
            factor_features = {k: v for k, v in feature_values.items()
                             if any(factor in k.lower() for factor in ['beta', 'factor', 'loading'])}

            if factor_features:
                # Factor exposures
                explanation['factor_exposures'] = {
                    k: f"{v:.3f}" for k, v in factor_features.items()
                    if 'loading' in k or 'beta' in k
                }

                # Factor contributions (importance * exposure)
                factor_contributions = {}
                for feature, value in factor_features.items():
                    importance = feature_importance.get(feature, 0)
                    contribution = importance * value
                    factor_contributions[feature] = contribution

                explanation['factor_contributions'] = factor_contributions

                # Factor analysis
                explanation['factor_analysis'] = self._analyze_factor_exposure(factor_features)

                # Attribution summary
                explanation['attribution_summary'] = self._create_attribution_summary(factor_contributions)

        except Exception as e:
            logger.warning(f"Failed to generate factor explanation: {e}")

        return explanation

    def _generate_confidence_explanation(self,
                                       predictions: Dict[str, Dict[str, float]],
                                       feature_importance: Dict[str, float],
                                       model_metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate confidence analysis explanation."""
        explanation = {
            'overall_confidence': 'Medium',
            'confidence_factors': {},
            'reliability_indicators': {},
            'prediction_stability': {}
        }

        try:
            # Calculate overall confidence
            confidence_score = self._calculate_overall_confidence(
                predictions, feature_importance, model_metadata
            )

            if confidence_score > 0.7:
                explanation['overall_confidence'] = 'High'
            elif confidence_score > 0.4:
                explanation['overall_confidence'] = 'Medium'
            else:
                explanation['overall_confidence'] = 'Low'

            # Confidence factors
            explanation['confidence_factors'] = {
                'feature_importance_concentration': self._calculate_importance_concentration(feature_importance),
                'prediction_consistency': self._calculate_prediction_consistency(predictions),
                'model_quality': model_metadata.get('r_squared', 0.5) if model_metadata else 0.5
            }

            # Reliability indicators
            explanation['reliability_indicators'] = {
                'number_of_strong_features': sum(1 for imp in feature_importance.values() if abs(imp) > 0.1),
                'feature_diversity': len(set(self._categorize_feature(f) for f in feature_importance.keys())),
                'prediction_range_reasonableness': self._assess_prediction_reasonableness(predictions)
            }

            # Prediction stability
            explanation['prediction_stability'] = self._assess_prediction_stability(predictions)

        except Exception as e:
            logger.warning(f"Failed to generate confidence explanation: {e}")

        return explanation

    def _extract_top_features(self,
                            feature_importance: Dict[str, float],
                            feature_values: Dict[str, Any],
                            top_n: int = 10) -> List[FeatureImportance]:
        """Extract top features with explanations."""
        top_features = []

        try:
            # Sort features by absolute importance
            sorted_features = sorted(
                feature_importance.items(),
                key=lambda x: abs(x[1]),
                reverse=True
            )

            for feature_name, importance in sorted_features[:top_n]:
                if abs(importance) < self.importance_thresholds['low']:
                    break

                # Get feature value
                feature_value = feature_values.get(feature_name, 0)

                # Calculate contribution (importance * value)
                contribution = importance * feature_value if isinstance(feature_value, (int, float)) else importance

                # Generate description
                description = self._describe_feature(feature_name, feature_value)

                # Categorize feature
                category = self._categorize_feature(feature_name)

                # Extract timeframe if applicable
                timeframe = self._extract_timeframe(feature_name)

                top_features.append(FeatureImportance(
                    feature_name=feature_name,
                    importance_score=importance,
                    contribution=contribution,
                    description=description,
                    category=category,
                    timeframe=timeframe
                ))

        except Exception as e:
            logger.warning(f"Failed to extract top features: {e}")

        return top_features

    def _calculate_feature_contributions(self,
                                       feature_importance: Dict[str, float],
                                       feature_values: Dict[str, Any],
                                       predictions: Dict[str, Dict[str, float]]) -> Dict[str, float]:
        """Calculate actual feature contributions to prediction."""
        contributions = {}

        try:
            for feature_name, importance in feature_importance.items():
                value = feature_values.get(feature_name, 0)

                # Calculate contribution based on importance and value
                if isinstance(value, (int, float)):
                    contribution = importance * value
                else:
                    contribution = importance  # For categorical features

                contributions[feature_name] = contribution

        except Exception as e:
            logger.warning(f"Failed to calculate feature contributions: {e}")

        return contributions

    def _classify_direction(self, prediction_value: float) -> str:
        """Classify prediction direction."""
        if prediction_value > 0.02:
            return "strong positive"
        elif prediction_value > 0.005:
            return "moderate positive"
        elif prediction_value < -0.02:
            return "strong negative"
        elif prediction_value < -0.005:
            return "moderate negative"
        else:
            return "neutral"

    def _assess_prediction_confidence(self, q05: float, q95: float, median: float) -> str:
        """Assess confidence level based on prediction uncertainty."""
        uncertainty = q95 - q05

        if uncertainty < 0.04:  # Less than 4% range
            return "high"
        elif uncertainty < 0.08:  # Less than 8% range
            return "medium"
        else:
            return "low"

    def _categorize_feature(self, feature_name: str) -> str:
        """Categorize a feature by type."""
        feature_lower = feature_name.lower()

        for category, keywords in self.feature_categories.items():
            if any(keyword in feature_lower for keyword in keywords):
                return category

        return 'other'

    def _extract_timeframe(self, feature_name: str) -> Optional[str]:
        """Extract timeframe from feature name."""
        timeframes = ['daily', 'weekly', 'monthly', 'quarterly', 'hourly']

        for tf in timeframes:
            if tf in feature_name.lower():
                return tf

        return None

    def _describe_feature(self, feature_name: str, feature_value: Any = None) -> str:
        """Generate human-readable description of a feature."""
        descriptions = {
            'ema_': 'Exponential Moving Average',
            'rsi_': 'Relative Strength Index',
            'atr_': 'Average True Range (volatility)',
            'vwap_': 'Volume Weighted Average Price',
            'volume_': 'Trading volume analysis',
            'trend_': 'Trend direction and strength',
            'llm_': 'AI pattern recognition',
            'event_': 'Event-driven factor',
            'market_': 'Market-wide factor',
            'sector_': 'Sector-specific factor'
        }

        for key, desc in descriptions.items():
            if key in feature_name.lower():
                if feature_value is not None and isinstance(feature_value, (int, float)):
                    return f"{desc} (value: {feature_value:.3f})"
                return desc

        return feature_name.replace('_', ' ').title()

    def _get_top_driver_description(self, feature_values: Dict[str, Any]) -> str:
        """Get description of the top driving factor."""
        # Simplified - look for obvious strong signals

        # Check for strong trend signals
        trend_signals = [k for k in feature_values.keys() if 'trend' in k.lower()]
        for signal in trend_signals:
            value = feature_values.get(signal, 0)
            if isinstance(value, (int, float)) and abs(value) > 0.5:
                direction = "upward" if value > 0 else "downward"
                return f"Strong {direction} trend momentum"

        # Check for pattern recognition
        llm_signals = [k for k in feature_values.keys() if 'llm_' in k.lower()]
        for signal in llm_signals:
            value = feature_values.get(signal, 0)
            if isinstance(value, (int, float)) and abs(value) > 0.7:
                if 'bullish' in signal:
                    return "AI-detected bullish pattern"
                elif 'bearish' in signal:
                    return "AI-detected bearish pattern"

        # Check for event proximity
        if feature_values.get('event_proximity_score', 0) > 0.5:
            return "Upcoming significant event"

        return "Multiple technical factors"

    def _determine_market_regime(self,
                               feature_values: Dict[str, Any],
                               mtf_features: Optional[MultiTimeFrameFeatures]) -> str:
        """Determine current market regime."""
        if mtf_features and mtf_features.dominant_trend != "neutral":
            return f"{mtf_features.dominant_trend}_trending"

        # Check volatility
        vol_features = [k for k in feature_values.keys() if 'volatility' in k.lower()]
        if vol_features:
            avg_vol = np.mean([feature_values[k] for k in vol_features if isinstance(feature_values[k], (int, float))])
            if avg_vol > 0.03:
                return "high_volatility"
            elif avg_vol < 0.015:
                return "low_volatility"

        return "mixed"

    def _determine_volatility_environment(self, feature_values: Dict[str, Any]) -> str:
        """Determine volatility environment."""
        atr_features = [k for k in feature_values.keys() if 'atr' in k.lower()]

        if atr_features:
            atr_values = [feature_values[k] for k in atr_features if isinstance(feature_values[k], (int, float))]
            if atr_values:
                avg_atr = np.mean(atr_values)
                if avg_atr > 0.04:
                    return "high"
                elif avg_atr < 0.02:
                    return "low"
                else:
                    return "normal"

        return "unknown"

    def _generate_trading_recommendations(self,
                                        predictions: Dict[str, Dict[str, float]],
                                        technical_explanation: Dict[str, Any],
                                        risk_explanation: Dict[str, Any]) -> List[str]:
        """Generate actionable trading recommendations."""
        recommendations = []

        try:
            # Get 1-day prediction
            if '1d' in predictions:
                day1_pred = predictions['1d']
                median = day1_pred.get('quantile_0.5', 0)
                q05 = day1_pred.get('quantile_0.05', 0)
                q95 = day1_pred.get('quantile_0.95', 0)

                # Direction recommendation
                if median > 0.01:
                    recommendations.append("Consider long position for short-term gain")
                elif median < -0.01:
                    recommendations.append("Consider avoiding or reducing position")

                # Risk management
                uncertainty = q95 - q05
                if uncertainty > 0.1:
                    recommendations.append("High uncertainty - consider smaller position sizes")

                # Stop loss suggestions
                if q05 < -0.05:
                    recommendations.append(f"Potential downside risk of {q05:.1%} - set stop losses accordingly")

            # Multi-day recommendations
            if '5d' in predictions:
                day5_pred = predictions['5d'].get('quantile_0.5', 0)
                if abs(day5_pred) > 0.02:
                    recommendations.append(f"5-day outlook suggests {day5_pred:+.1%} move - suitable for swing trading")

            # Technical recommendations
            key_signals = technical_explanation.get('key_signals', [])
            for signal in key_signals[:2]:  # Top 2 signals
                if 'bullish' in signal.lower():
                    recommendations.append(f"Technical signal: {signal}")
                elif 'bearish' in signal.lower():
                    recommendations.append(f"Caution: {signal}")

        except Exception as e:
            logger.warning(f"Failed to generate trading recommendations: {e}")

        return recommendations[:5]  # Limit to 5 recommendations

    def _generate_warnings(self,
                         predictions: Dict[str, Dict[str, float]],
                         feature_values: Dict[str, Any],
                         model_metadata: Optional[Dict[str, Any]]) -> List[str]:
        """Generate risk warnings."""
        warnings = []

        try:
            # High uncertainty warning
            for horizon, pred in predictions.items():
                if 'quantile_0.05' in pred and 'quantile_0.95' in pred:
                    uncertainty = pred['quantile_0.95'] - pred['quantile_0.05']
                    if uncertainty > 0.15:
                        warnings.append(f"Very high uncertainty in {horizon} prediction ({uncertainty:.1%} range)")

            # Extreme predictions
            for horizon, pred in predictions.items():
                median = pred.get('quantile_0.5', 0)
                if abs(median) > 0.1:
                    warnings.append(f"Extreme {horizon} prediction ({median:+.1%}) - verify with additional analysis")

            # Model confidence warnings
            if model_metadata:
                r_squared = model_metadata.get('r_squared', 1.0)
                if r_squared < 0.3:
                    warnings.append("Low model R-squared - predictions may be unreliable")

            # Data quality warnings
            data_quality = feature_values.get('meta_data_quality_score', 1.0)
            if data_quality < 0.7:
                warnings.append("Data quality concerns detected - use predictions with caution")

            # Event risk warnings
            event_proximity = feature_values.get('event_proximity_score', 0)
            if event_proximity > 0.7:
                warnings.append("Major event approaching - increased volatility expected")

        except Exception as e:
            logger.warning(f"Failed to generate warnings: {e}")

        return warnings


    # Additional helper methods for specific analysis types
    def _analyze_trend_signals(self, trend_features: Dict[str, Any], feature_importance: Dict[str, float]) -> Dict[str, Any]:
        """Analyze trend-related signals."""
        analysis = {}

        for feature, value in trend_features.items():
            importance = feature_importance.get(feature, 0)
            if abs(importance) > self.importance_thresholds['low']:
                if 'slope' in feature:
                    direction = "upward" if value > 0 else "downward"
                    strength = "strong" if abs(value) > 0.001 else "moderate"
                    analysis[feature] = f"{strength} {direction} trend"
                elif 'strength' in feature:
                    strength_desc = "strong" if value > 0.7 else "moderate" if value > 0.4 else "weak"
                    analysis[feature] = f"{strength_desc} trend consistency"

        return analysis

    def _analyze_momentum_signals(self, momentum_features: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze momentum indicators."""
        analysis = {}

        for feature, value in momentum_features.items():
            if 'rsi' in feature.lower() and isinstance(value, (int, float)):
                if value > 70:
                    analysis[feature] = "Overbought condition"
                elif value < 30:
                    analysis[feature] = "Oversold condition"
                else:
                    analysis[feature] = "Neutral momentum"

        return analysis

    def _analyze_volatility_signals(self, vol_features: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze volatility-related signals."""
        analysis = {}

        for feature, value in vol_features.items():
            if isinstance(value, (int, float)):
                if value > 0.04:
                    analysis[feature] = "High volatility environment"
                elif value < 0.015:
                    analysis[feature] = "Low volatility environment"
                else:
                    analysis[feature] = "Normal volatility"

        return analysis

    def _analyze_volume_signals(self, volume_features: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze volume-related signals."""
        analysis = {}

        for feature, value in volume_features.items():
            if 'ratio' in feature and isinstance(value, (int, float)):
                if value > 1.5:
                    analysis[feature] = "High volume (strong conviction)"
                elif value < 0.7:
                    analysis[feature] = "Low volume (weak conviction)"
                else:
                    analysis[feature] = "Normal volume"

        return analysis

    def _analyze_support_resistance(self, sr_features: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze support and resistance levels."""
        analysis = {}

        for feature, value in sr_features.items():
            if isinstance(value, (int, float)):
                if 'support' in feature:
                    if value < 0.02:
                        analysis[feature] = "Near support level"
                    else:
                        analysis[feature] = f"Above support by {value:.1%}"
                elif 'resistance' in feature:
                    if value < 0.02:
                        analysis[feature] = "Near resistance level"
                    else:
                        analysis[feature] = f"Below resistance by {value:.1%}"

        return analysis

    def _generate_technical_signals(self, feature_values: Dict[str, Any], feature_importance: Dict[str, float]) -> List[str]:
        """Generate key technical signals."""
        signals = []

        # Get most important technical features
        tech_features = {k: v for k, v in feature_importance.items()
                        if self._categorize_feature(k) == 'technical' and abs(v) > self.importance_thresholds['medium']}

        for feature, importance in sorted(tech_features.items(), key=lambda x: abs(x[1]), reverse=True)[:3]:
            value = feature_values.get(feature, 0)
            signal_desc = self._describe_technical_signal(feature, value, importance)
            if signal_desc:
                signals.append(signal_desc)

        return signals

    def _describe_technical_signal(self, feature: str, value: Any, importance: float) -> str:
        """Describe a technical signal."""
        if not isinstance(value, (int, float)):
            return ""

        direction = "bullish" if importance > 0 else "bearish"
        strength = "strong" if abs(importance) > 0.1 else "moderate"

        if 'rsi' in feature.lower():
            return f"{strength.title()} RSI signal ({direction})"
        elif 'ema' in feature.lower():
            return f"{strength.title()} moving average signal ({direction})"
        elif 'trend' in feature.lower():
            return f"{strength.title()} trend signal ({direction})"
        elif 'momentum' in feature.lower():
            return f"{strength.title()} momentum signal ({direction})"
        else:
            return f"{strength.title()} {feature.replace('_', ' ')} signal ({direction})"

    def _calculate_overall_confidence(self, predictions: Dict[str, Dict[str, float]],
                                    feature_importance: Dict[str, float],
                                    model_metadata: Optional[Dict[str, Any]]) -> float:
        """Calculate overall confidence score."""
        confidence_factors = []

        # Prediction consistency
        consistency = self._calculate_prediction_consistency(predictions)
        confidence_factors.append(consistency)

        # Feature importance concentration
        importance_concentration = self._calculate_importance_concentration(feature_importance)
        confidence_factors.append(importance_concentration)

        # Model quality
        if model_metadata and 'r_squared' in model_metadata:
            confidence_factors.append(model_metadata['r_squared'])

        return np.mean(confidence_factors) if confidence_factors else 0.5

    def _calculate_prediction_consistency(self, predictions: Dict[str, Dict[str, float]]) -> float:
        """Calculate consistency across prediction horizons."""
        try:
            medians = [pred.get('quantile_0.5', 0) for pred in predictions.values()]
            if len(medians) < 2:
                return 0.5

            # Check if predictions are directionally consistent
            positive_count = sum(1 for m in medians if m > 0)
            negative_count = sum(1 for m in medians if m < 0)

            consistency = max(positive_count, negative_count) / len(medians)
            return consistency
        except Exception:
            return 0.5

    def _calculate_importance_concentration(self, feature_importance: Dict[str, float]) -> float:
        """Calculate how concentrated feature importance is."""
        try:
            importances = list(feature_importance.values())
            if not importances:
                return 0.5

            # Calculate concentration using Herfindahl index
            abs_importances = [abs(imp) for imp in importances]
            total = sum(abs_importances)

            if total == 0:
                return 0.5

            normalized = [imp / total for imp in abs_importances]
            herfindahl = sum(imp ** 2 for imp in normalized)

            # Convert to concentration score (0-1)
            return min(herfindahl * len(normalized), 1.0)
        except Exception:
            return 0.5


def create_prediction_explanation(symbol: str,
                                prediction_date: datetime,
                                predictions: Dict[str, Dict[str, float]],
                                feature_values: Dict[str, Any],
                                feature_importance: Dict[str, float],
                                mtf_features: Optional[MultiTimeFrameFeatures] = None,
                                event_features: Optional[EventFeatures] = None,
                                llm_api_key: Optional[str] = None) -> PredictionExplanation:
    """
    Convenience function to create a prediction explanation.

    Args:
        symbol: Stock symbol
        prediction_date: Date of prediction
        predictions: Model predictions
        feature_values: Input features
        feature_importance: Feature importance scores
        mtf_features: Multi-timeframe analysis
        event_features: Event analysis
        llm_api_key: Optional LLM API key

    Returns:
        Complete prediction explanation
    """
    # Initialize LLM recognizer if API key provided
    llm_recognizer = None
    if llm_api_key:
        llm_recognizer = LLMPatternRecognizer(
            provider=LLMProvider.DEEPSEEK,
            api_key=llm_api_key
        )

    # Create interpreter
    interpreter = ResidualReturnInterpreter(llm_recognizer=llm_recognizer)

    # Generate explanation
    return interpreter.generate_comprehensive_explanation(
        symbol, prediction_date, predictions, feature_values, feature_importance,
        mtf_features, event_features
    )