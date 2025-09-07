"""
Multi-Timeframe Analysis System for Residual Return Prediction.
Analyzes quarterly, monthly, weekly, and daily patterns to provide contextual features.
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple, Any
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import logging

from domains.trading.services.state.universe_state_manager import UniverseStateManager
from signals.enhanced_indicators import calculate_all_technical_indicators, ResidualReturnIndicatorConfig
from modeling.llm_pattern_recognition import LLMPatternRecognizer, PatternAnalysis, LLMProvider

logger = logging.getLogger(__name__)


class TimeFrame(Enum):
    """Supported timeframes for analysis."""
    QUARTERLY = "quarterly"
    MONTHLY = "monthly"
    WEEKLY = "weekly"
    DAILY = "daily"
    HOURLY = "hourly"  # Future improvement


@dataclass
class TimeFrameConfig:
    """Configuration for each timeframe."""
    timeframe: TimeFrame
    lookback_periods: int  # Number of periods to analyze
    aggregation_method: str  # 'last', 'mean', 'ohlc'
    weight: float  # Importance weight for final prediction
    min_data_points: int  # Minimum data required
    pattern_sensitivity: float  # Sensitivity for pattern detection


@dataclass
class MultiTimeFrameFeatures:
    """Features extracted from multiple timeframes."""
    daily_features: Dict[str, Any]
    weekly_features: Dict[str, Any]
    monthly_features: Dict[str, Any]
    quarterly_features: Dict[str, Any]
    cross_timeframe_features: Dict[str, Any]
    timeframe_alignment: Dict[str, float]
    dominant_trend: str
    trend_strength: float


class TimeFrameAggregator:
    """Aggregates daily data into higher timeframes."""
    
    @staticmethod
    def aggregate_to_weekly(daily_data: pd.DataFrame) -> pd.DataFrame:
        """Aggregate daily data to weekly (Monday-Sunday)."""
        if daily_data.empty:
            return pd.DataFrame()
        
        try:
            # Set index to date if not already
            if not isinstance(daily_data.index, pd.DatetimeIndex):
                if 'date' in daily_data.columns:
                    daily_data = daily_data.set_index('date')
                else:
                    return pd.DataFrame()
            
            # Group by week (Monday start)
            weekly_data = daily_data.groupby(pd.Grouper(freq='W-MON')).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum' if 'volume' in daily_data.columns else 'mean'
            }).dropna()
            
            return weekly_data
            
        except Exception as e:
            logger.warning(f"Failed to aggregate to weekly: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def aggregate_to_monthly(daily_data: pd.DataFrame) -> pd.DataFrame:
        """Aggregate daily data to monthly."""
        if daily_data.empty:
            return pd.DataFrame()
        
        try:
            if not isinstance(daily_data.index, pd.DatetimeIndex):
                if 'date' in daily_data.columns:
                    daily_data = daily_data.set_index('date')
                else:
                    return pd.DataFrame()
            
            # Group by month
            monthly_data = daily_data.groupby(pd.Grouper(freq='M')).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum' if 'volume' in daily_data.columns else 'mean'
            }).dropna()
            
            return monthly_data
            
        except Exception as e:
            logger.warning(f"Failed to aggregate to monthly: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def aggregate_to_quarterly(daily_data: pd.DataFrame) -> pd.DataFrame:
        """Aggregate daily data to quarterly."""
        if daily_data.empty:
            return pd.DataFrame()
        
        try:
            if not isinstance(daily_data.index, pd.DatetimeIndex):
                if 'date' in daily_data.columns:
                    daily_data = daily_data.set_index('date')
                else:
                    return pd.DataFrame()
            
            # Group by quarter
            quarterly_data = daily_data.groupby(pd.Grouper(freq='Q')).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum' if 'volume' in daily_data.columns else 'mean'
            }).dropna()
            
            return quarterly_data
            
        except Exception as e:
            logger.warning(f"Failed to aggregate to quarterly: {e}")
            return pd.DataFrame()


class MultiTimeFrameAnalyzer:
    """Comprehensive multi-timeframe analysis system."""
    
    def __init__(self, 
                 universe_state_manager: UniverseStateManager,
                 llm_recognizer: Optional[LLMPatternRecognizer] = None):
        self.universe_state_manager = universe_state_manager
        self.llm_recognizer = llm_recognizer
        self.aggregator = TimeFrameAggregator()
        
        # Default timeframe configurations
        self.timeframe_configs = {
            TimeFrame.QUARTERLY: TimeFrameConfig(
                timeframe=TimeFrame.QUARTERLY,
                lookback_periods=8,  # 2 years of quarters
                aggregation_method='ohlc',
                weight=0.4,  # High weight for long-term context
                min_data_points=4,
                pattern_sensitivity=0.8
            ),
            TimeFrame.MONTHLY: TimeFrameConfig(
                timeframe=TimeFrame.MONTHLY,
                lookback_periods=24,  # 2 years of months
                aggregation_method='ohlc',
                weight=0.3,
                min_data_points=6,
                pattern_sensitivity=0.7
            ),
            TimeFrame.WEEKLY: TimeFrameConfig(
                timeframe=TimeFrame.WEEKLY,
                lookback_periods=52,  # 1 year of weeks
                aggregation_method='ohlc',
                weight=0.2,
                min_data_points=12,
                pattern_sensitivity=0.6
            ),
            TimeFrame.DAILY: TimeFrameConfig(
                timeframe=TimeFrame.DAILY,
                lookback_periods=252,  # 1 year of days
                aggregation_method='raw',
                weight=0.1,  # Lower weight for short-term noise
                min_data_points=20,
                pattern_sensitivity=0.5
            )
        }
        
        # Technical indicator configs for different timeframes
        self.indicator_configs = {
            TimeFrame.QUARTERLY: ResidualReturnIndicatorConfig.minimal_config(),
            TimeFrame.MONTHLY: ResidualReturnIndicatorConfig.minimal_config(),
            TimeFrame.WEEKLY: ResidualReturnIndicatorConfig.comprehensive_config(),
            TimeFrame.DAILY: ResidualReturnIndicatorConfig.comprehensive_config()
        }
    
    async def analyze_multi_timeframe(self, 
                                    instrument_id: int,
                                    current_date: datetime,
                                    symbol: str = "STOCK") -> MultiTimeFrameFeatures:
        """
        Perform comprehensive multi-timeframe analysis.
        
        Args:
            instrument_id: Instrument to analyze
            current_date: Current analysis date
            symbol: Stock symbol for LLM context
            
        Returns:
            MultiTimeFrameFeatures with analysis from all timeframes
        """
        logger.debug(f"Starting multi-timeframe analysis for {instrument_id} on {current_date}")
        
        # Get daily data first (base timeframe)
        max_lookback = max(config.lookback_periods * 5 for config in self.timeframe_configs.values())
        daily_data = self.universe_state_manager.get_lag_prices(
            instrument_id, current_date, max_lookback
        )
        
        if daily_data.empty:
            return self._create_empty_features()
        
        # Ensure datetime index
        if not isinstance(daily_data.index, pd.DatetimeIndex):
            daily_data.index = pd.to_datetime(daily_data.index)
        
        # Aggregate to different timeframes
        timeframe_data = await self._prepare_timeframe_data(daily_data)
        
        # Analyze each timeframe
        timeframe_analyses = {}
        
        for timeframe in [TimeFrame.QUARTERLY, TimeFrame.MONTHLY, TimeFrame.WEEKLY, TimeFrame.DAILY]:
            if timeframe in timeframe_data and not timeframe_data[timeframe].empty:
                analysis = await self._analyze_single_timeframe(
                    timeframe_data[timeframe], timeframe, symbol
                )
                timeframe_analyses[timeframe] = analysis
        
        # Extract features from each timeframe
        daily_features = timeframe_analyses.get(TimeFrame.DAILY, {})
        weekly_features = timeframe_analyses.get(TimeFrame.WEEKLY, {})
        monthly_features = timeframe_analyses.get(TimeFrame.MONTHLY, {})
        quarterly_features = timeframe_analyses.get(TimeFrame.QUARTERLY, {})
        
        # Calculate cross-timeframe features
        cross_timeframe_features = self._calculate_cross_timeframe_features(
            timeframe_data, timeframe_analyses
        )
        
        # Calculate timeframe alignment
        timeframe_alignment = self._calculate_timeframe_alignment(timeframe_analyses)
        
        # Determine dominant trend and strength
        dominant_trend, trend_strength = self._determine_dominant_trend(
            timeframe_analyses, timeframe_alignment
        )
        
        return MultiTimeFrameFeatures(
            daily_features=daily_features,
            weekly_features=weekly_features,
            monthly_features=monthly_features,
            quarterly_features=quarterly_features,
            cross_timeframe_features=cross_timeframe_features,
            timeframe_alignment=timeframe_alignment,
            dominant_trend=dominant_trend,
            trend_strength=trend_strength
        )
    
    async def _prepare_timeframe_data(self, daily_data: pd.DataFrame) -> Dict[TimeFrame, pd.DataFrame]:
        """Prepare data for all timeframes."""
        timeframe_data = {}
        
        # Daily data (already available)
        timeframe_data[TimeFrame.DAILY] = daily_data.copy()
        
        # Weekly aggregation
        weekly_data = self.aggregator.aggregate_to_weekly(daily_data)
        if not weekly_data.empty:
            timeframe_data[TimeFrame.WEEKLY] = weekly_data
        
        # Monthly aggregation
        monthly_data = self.aggregator.aggregate_to_monthly(daily_data)
        if not monthly_data.empty:
            timeframe_data[TimeFrame.MONTHLY] = monthly_data
        
        # Quarterly aggregation
        quarterly_data = self.aggregator.aggregate_to_quarterly(daily_data)
        if not quarterly_data.empty:
            timeframe_data[TimeFrame.QUARTERLY] = quarterly_data
        
        return timeframe_data
    
    async def _analyze_single_timeframe(self, 
                                      data: pd.DataFrame,
                                      timeframe: TimeFrame,
                                      symbol: str) -> Dict[str, Any]:
        """Analyze a single timeframe."""
        try:
            config = self.timeframe_configs[timeframe]
            
            # Limit data to lookback periods
            analysis_data = data.tail(config.lookback_periods)
            
            if len(analysis_data) < config.min_data_points:
                logger.debug(f"Insufficient data for {timeframe.value} analysis")
                return {}
            
            features = {}
            
            # Technical indicators
            tech_features = self._extract_timeframe_technical_features(
                analysis_data, timeframe
            )
            features.update(tech_features)
            
            # Price action features
            price_features = self._extract_price_action_features(
                analysis_data, timeframe
            )
            features.update(price_features)
            
            # Pattern recognition with LLM (if available)
            if self.llm_recognizer:
                pattern_features = await self._extract_llm_pattern_features(
                    analysis_data, timeframe, symbol
                )
                features.update(pattern_features)
            
            # Trend analysis
            trend_features = self._extract_trend_features(analysis_data, timeframe)
            features.update(trend_features)
            
            # Support/resistance levels
            sr_features = self._extract_support_resistance_features(
                analysis_data, timeframe
            )
            features.update(sr_features)
            
            # Add timeframe prefix to all features
            prefixed_features = {
                f"{timeframe.value}_{key}": value 
                for key, value in features.items()
            }
            
            return prefixed_features
            
        except Exception as e:
            logger.warning(f"Failed to analyze {timeframe.value}: {e}")
            return {}
    
    def _extract_timeframe_technical_features(self, 
                                            data: pd.DataFrame,
                                            timeframe: TimeFrame) -> Dict[str, Any]:
        """Extract technical indicators for specific timeframe."""
        try:
            config = self.indicator_configs[timeframe]
            return calculate_all_technical_indicators(data, config)
        except Exception as e:
            logger.warning(f"Failed to extract technical features for {timeframe.value}: {e}")
            return {}
    
    def _extract_price_action_features(self, 
                                     data: pd.DataFrame,
                                     timeframe: TimeFrame) -> Dict[str, Any]:
        """Extract price action features specific to timeframe."""
        features = {}
        
        try:
            if 'close' not in data.columns:
                return features
            
            close_prices = data['close']
            
            # Returns over different periods
            if timeframe == TimeFrame.QUARTERLY:
                periods = [1, 2, 4]  # 1Q, 2Q, 1Y
            elif timeframe == TimeFrame.MONTHLY:
                periods = [1, 3, 6, 12]  # 1M, 3M, 6M, 1Y
            elif timeframe == TimeFrame.WEEKLY:
                periods = [1, 2, 4, 8, 13, 26]  # 1W, 2W, 1M, 2M, 3M, 6M
            else:  # DAILY
                periods = [1, 5, 10, 20, 50]  # 1D, 1W, 2W, 1M, 2M
            
            for period in periods:
                if len(close_prices) > period:
                    ret = (close_prices.iloc[-1] / close_prices.iloc[-period-1]) - 1
                    features[f'return_{period}p'] = ret
            
            # Volatility measures
            returns = close_prices.pct_change().dropna()
            if len(returns) > 0:
                features['volatility'] = returns.std()
                features['skewness'] = returns.skew()
                features['kurtosis'] = returns.kurtosis()
            
            # Range analysis
            if 'high' in data.columns and 'low' in data.columns:
                ranges = (data['high'] - data['low']) / data['close']
                features['avg_range'] = ranges.mean()
                features['range_expansion'] = ranges.iloc[-1] / ranges.mean() - 1
            
            # Price position in range
            if len(close_prices) >= 10:
                recent_high = close_prices.tail(10).max()
                recent_low = close_prices.tail(10).min()
                if recent_high != recent_low:
                    features['price_position'] = (
                        (close_prices.iloc[-1] - recent_low) / (recent_high - recent_low)
                    )
            
        except Exception as e:
            logger.warning(f"Failed to extract price action features: {e}")
        
        return features
    
    async def _extract_llm_pattern_features(self, 
                                          data: pd.DataFrame,
                                          timeframe: TimeFrame,
                                          symbol: str) -> Dict[str, Any]:
        """Extract LLM-based pattern features."""
        try:
            # Get pattern analysis from LLM
            pattern_analysis = await self.llm_recognizer.analyze_price_pattern(
                data, symbol, timeframe.value
            )
            
            # Convert to numerical features
            features = {
                'llm_confidence': pattern_analysis.confidence,
                'llm_bullish': 1.0 if pattern_analysis.predicted_direction == "bullish" else 0.0,
                'llm_bearish': 1.0 if pattern_analysis.predicted_direction == "bearish" else 0.0,
                'llm_pattern_strength': self._calculate_pattern_strength(pattern_analysis)
            }
            
            # Add pattern type as categorical features
            pattern_types = ['head_and_shoulders', 'double_top', 'double_bottom', 
                           'ascending_triangle', 'descending_triangle', 'flag', 'pennant']
            
            for pattern_type in pattern_types:
                features[f'llm_pattern_{pattern_type}'] = (
                    1.0 if pattern_type in pattern_analysis.pattern_type.lower() else 0.0
                )
            
            # Support/resistance distances
            if pattern_analysis.support_resistance:
                current_price = data['close'].iloc[-1] if 'close' in data.columns else data['high'].iloc[-1]
                
                if 'support' in pattern_analysis.support_resistance:
                    support = pattern_analysis.support_resistance['support']
                    features['llm_support_distance'] = (current_price - support) / current_price
                
                if 'resistance' in pattern_analysis.support_resistance:
                    resistance = pattern_analysis.support_resistance['resistance']
                    features['llm_resistance_distance'] = (resistance - current_price) / current_price
            
            return features
            
        except Exception as e:
            logger.warning(f"Failed to extract LLM pattern features: {e}")
            return {}
    
    def _extract_trend_features(self, 
                              data: pd.DataFrame,
                              timeframe: TimeFrame) -> Dict[str, Any]:
        """Extract trend analysis features."""
        features = {}
        
        try:
            if 'close' not in data.columns or len(data) < 10:
                return features
            
            close_prices = data['close']
            
            # Trend strength using linear regression
            x = np.arange(len(close_prices))
            y = close_prices.values
            
            # Calculate slope and R-squared
            slope, intercept = np.polyfit(x, y, 1)
            y_pred = slope * x + intercept
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
            
            features['trend_slope'] = slope / np.mean(y)  # Normalize by price level
            features['trend_strength'] = r_squared
            features['trend_direction'] = 1.0 if slope > 0 else -1.0
            
            # Moving average analysis
            if len(close_prices) >= 20:
                ma_short = close_prices.rolling(5).mean().iloc[-1]
                ma_long = close_prices.rolling(20).mean().iloc[-1]
                
                features['ma_relationship'] = (ma_short / ma_long) - 1
                features['price_vs_ma_short'] = (close_prices.iloc[-1] / ma_short) - 1
                features['price_vs_ma_long'] = (close_prices.iloc[-1] / ma_long) - 1
            
            # Trend consistency (how often price moves in trend direction)
            if len(close_prices) >= 10:
                recent_changes = close_prices.diff().tail(10)
                trend_consistency = (
                    (recent_changes > 0).sum() if slope > 0 else (recent_changes < 0).sum()
                ) / len(recent_changes)
                features['trend_consistency'] = trend_consistency
            
        except Exception as e:
            logger.warning(f"Failed to extract trend features: {e}")
        
        return features
    
    def _extract_support_resistance_features(self, 
                                           data: pd.DataFrame,
                                           timeframe: TimeFrame) -> Dict[str, Any]:
        """Extract support and resistance level features."""
        features = {}
        
        try:
            if 'high' not in data.columns or 'low' not in data.columns:
                return features
            
            high_prices = data['high']
            low_prices = data['low']
            close_prices = data.get('close', high_prices)
            
            # Find recent highs and lows
            window = min(10, len(data) // 2)
            
            if window >= 3:
                # Recent support (lowest low in window)
                recent_support = low_prices.tail(window).min()
                
                # Recent resistance (highest high in window)  
                recent_resistance = high_prices.tail(window).max()
                
                current_price = close_prices.iloc[-1]
                
                # Distance to support/resistance
                features['support_distance'] = (current_price - recent_support) / current_price
                features['resistance_distance'] = (recent_resistance - current_price) / current_price
                
                # Support/resistance strength (how many times tested)
                support_tests = ((low_prices.tail(window) <= recent_support * 1.02) & 
                               (low_prices.tail(window) >= recent_support * 0.98)).sum()
                resistance_tests = ((high_prices.tail(window) >= recent_resistance * 0.98) & 
                                  (high_prices.tail(window) <= recent_resistance * 1.02)).sum()
                
                features['support_strength'] = min(support_tests / window, 1.0)
                features['resistance_strength'] = min(resistance_tests / window, 1.0)
                
                # Price position between support and resistance
                sr_range = recent_resistance - recent_support
                if sr_range > 0:
                    features['sr_position'] = (current_price - recent_support) / sr_range
            
        except Exception as e:
            logger.warning(f"Failed to extract support/resistance features: {e}")
        
        return features
    
    def _calculate_cross_timeframe_features(self, 
                                          timeframe_data: Dict[TimeFrame, pd.DataFrame],
                                          timeframe_analyses: Dict[TimeFrame, Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate features that compare across timeframes."""
        features = {}
        
        try:
            # Compare trends across timeframes
            trend_directions = {}
            trend_strengths = {}
            
            for timeframe, analysis in timeframe_analyses.items():
                direction_key = f"{timeframe.value}_trend_direction"
                strength_key = f"{timeframe.value}_trend_strength"
                
                if direction_key in analysis:
                    trend_directions[timeframe] = analysis[direction_key]
                if strength_key in analysis:
                    trend_strengths[timeframe] = analysis[strength_key]
            
            # Trend alignment score
            if len(trend_directions) >= 2:
                direction_values = list(trend_directions.values())
                # Calculate how aligned the trends are
                alignment = np.mean([
                    1.0 if d1 * d2 > 0 else 0.0  # Same direction
                    for i, d1 in enumerate(direction_values)
                    for j, d2 in enumerate(direction_values)
                    if i < j
                ])
                features['trend_alignment_score'] = alignment
            
            # Multi-timeframe momentum
            if TimeFrame.DAILY in timeframe_analyses and TimeFrame.WEEKLY in timeframe_analyses:
                daily_momentum = timeframe_analyses[TimeFrame.DAILY].get('daily_return_1p', 0)
                weekly_momentum = timeframe_analyses[TimeFrame.WEEKLY].get('weekly_return_1p', 0)
                
                features['momentum_consistency'] = 1.0 if daily_momentum * weekly_momentum > 0 else 0.0
            
            # Volatility regime comparison
            volatilities = {}
            for timeframe, analysis in timeframe_analyses.items():
                vol_key = f"{timeframe.value}_volatility"
                if vol_key in analysis:
                    volatilities[timeframe] = analysis[vol_key]
            
            if len(volatilities) >= 2:
                # Compare short-term vs long-term volatility
                if TimeFrame.DAILY in volatilities and TimeFrame.MONTHLY in volatilities:
                    vol_ratio = volatilities[TimeFrame.DAILY] / volatilities[TimeFrame.MONTHLY]
                    features['volatility_regime'] = vol_ratio
            
            # Price level consistency across timeframes
            price_levels = {}
            for timeframe, data in timeframe_data.items():
                if 'close' in data.columns and not data.empty:
                    price_levels[timeframe] = data['close'].iloc[-1]
            
            # All should be similar (accounting for timing differences)
            if len(price_levels) >= 2:
                price_values = list(price_levels.values())
                price_std = np.std(price_values) / np.mean(price_values)
                features['price_consistency'] = max(0, 1 - price_std * 10)  # Lower is more consistent
            
        except Exception as e:
            logger.warning(f"Failed to calculate cross-timeframe features: {e}")
        
        return features
    
    def _calculate_timeframe_alignment(self, 
                                     timeframe_analyses: Dict[TimeFrame, Dict[str, Any]]) -> Dict[str, float]:
        """Calculate alignment scores between timeframes."""
        alignment = {}
        
        try:
            timeframes = list(timeframe_analyses.keys())
            
            for i, tf1 in enumerate(timeframes):
                for j, tf2 in enumerate(timeframes):
                    if i < j:
                        # Calculate alignment between two timeframes
                        align_score = self._calculate_pairwise_alignment(
                            timeframe_analyses[tf1], timeframe_analyses[tf2]
                        )
                        alignment[f"{tf1.value}_{tf2.value}_alignment"] = align_score
        
        except Exception as e:
            logger.warning(f"Failed to calculate timeframe alignment: {e}")
        
        return alignment
    
    def _calculate_pairwise_alignment(self, 
                                    analysis1: Dict[str, Any],
                                    analysis2: Dict[str, Any]) -> float:
        """Calculate alignment score between two timeframe analyses."""
        try:
            alignment_factors = []
            
            # Trend direction alignment
            trend1 = analysis1.get('trend_direction', 0)
            trend2 = analysis2.get('trend_direction', 0)
            
            if trend1 != 0 and trend2 != 0:
                trend_align = 1.0 if trend1 * trend2 > 0 else 0.0
                alignment_factors.append(trend_align)
            
            # LLM sentiment alignment (if available)
            llm_bull1 = analysis1.get('llm_bullish', 0.5)
            llm_bull2 = analysis2.get('llm_bullish', 0.5)
            
            sentiment_align = 1 - abs(llm_bull1 - llm_bull2)
            alignment_factors.append(sentiment_align)
            
            # Pattern strength consistency
            strength1 = analysis1.get('trend_strength', 0.5)
            strength2 = analysis2.get('trend_strength', 0.5)
            
            strength_align = 1 - abs(strength1 - strength2)
            alignment_factors.append(strength_align)
            
            return np.mean(alignment_factors) if alignment_factors else 0.5
            
        except Exception as e:
            logger.warning(f"Failed to calculate pairwise alignment: {e}")
            return 0.5
    
    def _determine_dominant_trend(self, 
                                timeframe_analyses: Dict[TimeFrame, Dict[str, Any]],
                                timeframe_alignment: Dict[str, float]) -> Tuple[str, float]:
        """Determine the dominant trend across all timeframes."""
        try:
            # Weight timeframes by importance (longer = more important)
            weights = {
                TimeFrame.QUARTERLY: 0.4,
                TimeFrame.MONTHLY: 0.3,
                TimeFrame.WEEKLY: 0.2,
                TimeFrame.DAILY: 0.1
            }
            
            # Collect trend signals
            weighted_signals = []
            
            for timeframe, analysis in timeframe_analyses.items():
                weight = weights.get(timeframe, 0.1)
                
                # Trend direction
                trend_dir = analysis.get(f'{timeframe.value}_trend_direction', 0)
                trend_strength = analysis.get(f'{timeframe.value}_trend_strength', 0)
                
                # LLM sentiment
                llm_bull = analysis.get(f'{timeframe.value}_llm_bullish', 0)
                llm_bear = analysis.get(f'{timeframe.value}_llm_bearish', 0)
                llm_signal = llm_bull - llm_bear
                
                # Combined signal
                combined_signal = (trend_dir + llm_signal) / 2
                weighted_signal = combined_signal * weight * (1 + trend_strength)
                
                weighted_signals.append(weighted_signal)
            
            # Calculate overall trend
            if weighted_signals:
                overall_signal = sum(weighted_signals)
                overall_strength = abs(overall_signal)
                
                if overall_signal > 0.1:
                    dominant_trend = "bullish"
                elif overall_signal < -0.1:
                    dominant_trend = "bearish"
                else:
                    dominant_trend = "neutral"
                
                # Adjust strength by alignment
                avg_alignment = np.mean(list(timeframe_alignment.values())) if timeframe_alignment else 0.5
                adjusted_strength = overall_strength * avg_alignment
                
                return dominant_trend, min(adjusted_strength, 1.0)
            
        except Exception as e:
            logger.warning(f"Failed to determine dominant trend: {e}")
        
        return "neutral", 0.5
    
    def _calculate_pattern_strength(self, pattern_analysis: PatternAnalysis) -> float:
        """Calculate pattern strength from LLM analysis."""
        strength = pattern_analysis.confidence
        
        # Adjust based on technical indicators
        if pattern_analysis.technical_indicators:
            momentum = pattern_analysis.technical_indicators.get("momentum", "neutral")
            if momentum == "strong":
                strength *= 1.2
            elif momentum == "weak":
                strength *= 0.8
        
        return min(strength, 1.0)
    
    def _create_empty_features(self) -> MultiTimeFrameFeatures:
        """Create empty features when no data available."""
        return MultiTimeFrameFeatures(
            daily_features={},
            weekly_features={},
            monthly_features={},
            quarterly_features={},
            cross_timeframe_features={},
            timeframe_alignment={},
            dominant_trend="neutral",
            trend_strength=0.0
        )


def flatten_multi_timeframe_features(mtf_features: MultiTimeFrameFeatures) -> Dict[str, Any]:
    """
    Flatten multi-timeframe features into a single dictionary for model input.
    
    Returns:
        Flattened feature dictionary with prefixed keys
    """
    flattened = {}
    
    # Add individual timeframe features
    flattened.update(mtf_features.daily_features)
    flattened.update(mtf_features.weekly_features)
    flattened.update(mtf_features.monthly_features)
    flattened.update(mtf_features.quarterly_features)
    
    # Add cross-timeframe features
    flattened.update(mtf_features.cross_timeframe_features)
    
    # Add alignment features
    flattened.update(mtf_features.timeframe_alignment)
    
    # Add summary features
    flattened['mtf_dominant_trend_bullish'] = 1.0 if mtf_features.dominant_trend == "bullish" else 0.0
    flattened['mtf_dominant_trend_bearish'] = 1.0 if mtf_features.dominant_trend == "bearish" else 0.0
    flattened['mtf_trend_strength'] = mtf_features.trend_strength
    
    return flattened


# Convenience function for easy integration
async def analyze_multi_timeframe_patterns(
    universe_state_manager: UniverseStateManager,
    instrument_id: int,
    current_date: datetime,
    symbol: str = "STOCK",
    llm_api_key: Optional[str] = None
) -> MultiTimeFrameFeatures:
    """
    Convenience function for multi-timeframe analysis.
    
    Args:
        universe_state_manager: Universe state manager instance
        instrument_id: Instrument to analyze
        current_date: Analysis date
        symbol: Stock symbol
        llm_api_key: Optional LLM API key for pattern recognition
        
    Returns:
        Multi-timeframe features
    """
    # Initialize LLM recognizer if API key provided
    llm_recognizer = None
    if llm_api_key:
        llm_recognizer = LLMPatternRecognizer(
            provider=LLMProvider.DEEPSEEK,
            api_key=llm_api_key
        )
    
    # Create analyzer
    analyzer = MultiTimeFrameAnalyzer(universe_state_manager, llm_recognizer)
    
    # Perform analysis
    return await analyzer.analyze_multi_timeframe(instrument_id, current_date, symbol)