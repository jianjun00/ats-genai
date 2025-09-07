"""
Advanced Volume Profile Variants with Enhanced Features.
Provides specialized volume profile implementations for sophisticated market analysis.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
from typing import Dict, Any, List, Optional, Tuple, Union
from dataclasses import dataclass
import logging

from .indicator import Indicator, InstrumentInterval
from .enhanced_indicators import VolumeProfileIndicator

logger = logging.getLogger(__name__)


@dataclass
class SessionTimeRange:
    """Defines trading session time ranges."""
    name: str
    start_time: time
    end_time: time
    timezone: str = 'America/New_York'


class SessionVolumeProfile(VolumeProfileIndicator):
    """Session-based Volume Profile that resets at session boundaries."""
    
    def __init__(self, 
                 period: int = 20, 
                 bin_count: int = 50, 
                 value_area_pct: float = 70.0,
                 session_start: time = time(9, 30),
                 session_end: time = time(16, 0)):
        super().__init__(period, bin_count, value_area_pct)
        self.session_start = session_start
        self.session_end = session_end
        self.name = f"SessionVolumeProfile_{period}_{bin_count}"
        
    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Calculate session-based volume profile."""
        if len(price_history) < self.period:
            return {'value': None, 'status': 'insufficient_data'}
        
        try:
            # Filter data to current session if timestamp available
            session_data = self._filter_session_data(price_history)
            
            if len(session_data) < min(5, self.period // 2):
                return {'value': None, 'status': 'insufficient_session_data'}
            
            # Calculate volume profile for session data
            result = super().calculate(session_data)
            
            # Add session-specific metadata
            if result['status'] == 'valid':
                result['session_bars'] = len(session_data)
                result['session_start'] = str(self.session_start)
                result['session_end'] = str(self.session_end)
                result['is_session_complete'] = self._is_session_complete(session_data)
            
            return result
            
        except Exception as e:
            return {'value': None, 'status': f'session_calculation_error: {str(e)}'}
    
    def _filter_session_data(self, price_history: pd.DataFrame) -> pd.DataFrame:
        """Filter price history to current trading session."""
        if 'timestamp' not in price_history.columns:
            # If no timestamp, use all data (fallback behavior)
            return price_history.tail(self.period)
        
        # Convert timestamps and filter by session times
        timestamps = pd.to_datetime(price_history['timestamp'])
        
        # Get current session data
        current_date = timestamps.iloc[-1].date()
        session_start_dt = datetime.combine(current_date, self.session_start)
        session_end_dt = datetime.combine(current_date, self.session_end)
        
        # Filter to current session
        session_mask = (timestamps >= session_start_dt) & (timestamps <= session_end_dt)
        session_data = price_history[session_mask].copy()
        
        # If not enough session data, expand to previous sessions
        if len(session_data) < min(10, self.period):
            return price_history.tail(self.period)
        
        return session_data.tail(self.period)
    
    def _is_session_complete(self, session_data: pd.DataFrame) -> bool:
        """Check if we have data covering the complete session."""
        if 'timestamp' not in session_data.columns or len(session_data) == 0:
            return False
        
        timestamps = pd.to_datetime(session_data['timestamp'])
        first_time = timestamps.iloc[0].time()
        last_time = timestamps.iloc[-1].time()
        
        # Consider session complete if we have data near session boundaries
        start_buffer = timedelta(minutes=30)
        end_buffer = timedelta(minutes=30)
        
        session_start_dt = datetime.combine(datetime.today(), self.session_start)
        session_end_dt = datetime.combine(datetime.today(), self.session_end)
        
        has_early_data = first_time <= (session_start_dt + start_buffer).time()
        has_late_data = last_time >= (session_end_dt - end_buffer).time()
        
        return has_early_data and has_late_data


class MultiTimeframeVolumeProfile(Indicator):
    """Multi-timeframe Volume Profile aggregating insights across timeframes."""
    
    def __init__(self, 
                 timeframes: Dict[str, int] = None,
                 bin_count: int = 50,
                 value_area_pct: float = 70.0):
        super().__init__()
        self.timeframes = timeframes or {'5m': 12, '15m': 20, '1h': 24, '4h': 30}
        self.bin_count = bin_count
        self.value_area_pct = value_area_pct
        self.name = f"MultiTimeframeVP_{bin_count}"
        
        # Create individual volume profile indicators
        self.vp_indicators = {}
        for tf_name, period in self.timeframes.items():
            self.vp_indicators[tf_name] = VolumeProfileIndicator(period, bin_count, value_area_pct)
    
    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Calculate multi-timeframe volume profile analysis."""
        if len(price_history) < max(self.timeframes.values()):
            return {'value': None, 'status': 'insufficient_data'}
        
        try:
            # Calculate volume profile for each timeframe
            timeframe_results = {}
            for tf_name, indicator in self.vp_indicators.items():
                result = indicator.calculate(price_history)
                if result['status'] == 'valid':
                    timeframe_results[tf_name] = result
            
            if not timeframe_results:
                return {'value': None, 'status': 'no_valid_timeframes'}
            
            # Aggregate insights across timeframes
            aggregated_result = self._aggregate_timeframe_insights(timeframe_results)
            
            return {
                'value': aggregated_result['consensus_poc'],
                'consensus_poc': aggregated_result['consensus_poc'],
                'consensus_vah': aggregated_result['consensus_vah'],
                'consensus_val': aggregated_result['consensus_val'],
                'timeframe_alignment': aggregated_result['timeframe_alignment'],
                'dominant_timeframe': aggregated_result['dominant_timeframe'],
                'profile_consistency': aggregated_result['profile_consistency'],
                'timeframe_results': timeframe_results,
                'multi_tf_bias': aggregated_result['multi_tf_bias'],
                'confluence_strength': aggregated_result['confluence_strength'],
                'status': 'valid'
            }
            
        except Exception as e:
            return {'value': None, 'status': f'multi_tf_calculation_error: {str(e)}'}
    
    def _aggregate_timeframe_insights(self, timeframe_results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate volume profile insights across multiple timeframes."""
        
        # Extract POC values from all timeframes
        poc_values = [result['poc'] for result in timeframe_results.values()]
        vah_values = [result['vah'] for result in timeframe_results.values()]
        val_values = [result['val'] for result in timeframe_results.values()]
        
        # Calculate consensus levels (median/weighted average)
        consensus_poc = np.median(poc_values)
        consensus_vah = np.median(vah_values)
        consensus_val = np.median(val_values)
        
        # Calculate timeframe alignment (how close POCs are)
        poc_std = np.std(poc_values)
        poc_range = max(poc_values) - min(poc_values)
        alignment_score = 1.0 - min(poc_range / consensus_poc, 0.5) if consensus_poc > 0 else 0
        
        # Determine dominant timeframe (highest volume or strongest signal)
        dominant_tf = max(timeframe_results.keys(), 
                         key=lambda tf: timeframe_results[tf].get('total_volume', 0))
        
        # Calculate profile consistency across timeframes
        shapes = [result['profile_shape'] for result in timeframe_results.values()]
        biases = [result['dominant_side'] for result in timeframe_results.values()]
        
        shape_consistency = len(set(shapes)) / len(shapes) if shapes else 0
        bias_consistency = len(set(biases)) / len(biases) if biases else 0
        profile_consistency = (shape_consistency + bias_consistency) / 2
        
        # Multi-timeframe bias (majority vote)
        bias_counts = {}
        for bias in biases:
            bias_counts[bias] = bias_counts.get(bias, 0) + 1
        multi_tf_bias = max(bias_counts.keys(), key=lambda b: bias_counts[b]) if bias_counts else 'neutral'
        
        # Confluence strength (how many timeframes agree)
        confluence_strength = max(bias_counts.values()) / len(biases) if biases else 0
        
        return {
            'consensus_poc': float(consensus_poc),
            'consensus_vah': float(consensus_vah),
            'consensus_val': float(consensus_val),
            'timeframe_alignment': float(alignment_score),
            'dominant_timeframe': dominant_tf,
            'profile_consistency': float(1.0 - profile_consistency),  # Higher is better
            'multi_tf_bias': multi_tf_bias,
            'confluence_strength': float(confluence_strength)
        }


class AdaptiveVolumeProfile(VolumeProfileIndicator):
    """Adaptive Volume Profile that adjusts parameters based on market conditions."""
    
    def __init__(self, 
                 base_period: int = 20, 
                 base_bin_count: int = 50,
                 value_area_pct: float = 70.0,
                 volatility_adjustment: bool = True,
                 volume_adjustment: bool = True):
        super().__init__(base_period, base_bin_count, value_area_pct)
        self.base_period = base_period
        self.base_bin_count = base_bin_count
        self.volatility_adjustment = volatility_adjustment
        self.volume_adjustment = volume_adjustment
        self.name = f"AdaptiveVolumeProfile_{base_period}_{base_bin_count}"
    
    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Calculate adaptive volume profile with dynamic parameters."""
        if len(price_history) < self.base_period:
            return {'value': None, 'status': 'insufficient_data'}
        
        try:
            # Analyze market conditions
            market_conditions = self._analyze_market_conditions(price_history)
            
            # Adjust parameters based on conditions
            adapted_params = self._adapt_parameters(market_conditions)
            
            # Update instance parameters temporarily
            original_period = self.period
            original_bin_count = self.bin_count
            
            self.period = adapted_params['period']
            self.bin_count = adapted_params['bin_count']
            
            try:
                # Calculate with adapted parameters
                result = super().calculate(price_history)
                
                # Add adaptation metadata
                if result['status'] == 'valid':
                    result['adapted_period'] = adapted_params['period']
                    result['adapted_bin_count'] = adapted_params['bin_count']
                    result['market_volatility'] = market_conditions['volatility_score']
                    result['volume_activity'] = market_conditions['volume_activity']
                    result['adaptation_reason'] = adapted_params['reason']
                
            finally:
                # Restore original parameters
                self.period = original_period
                self.bin_count = original_bin_count
            
            return result
            
        except Exception as e:
            return {'value': None, 'status': f'adaptive_calculation_error: {str(e)}'}
    
    def _analyze_market_conditions(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Analyze current market conditions for parameter adaptation."""
        
        # Calculate volatility (ATR-based)
        if len(price_history) < 14:
            volatility_score = 0.5  # Default medium volatility
        else:
            high_low = price_history['high'] - price_history['low']
            high_close = abs(price_history['high'] - price_history['close'].shift(1))
            low_close = abs(price_history['low'] - price_history['close'].shift(1))
            
            true_ranges = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr = true_ranges.rolling(14).mean().iloc[-1]
            
            # Normalize volatility score (0-1)
            recent_close = price_history['close'].iloc[-1]
            volatility_score = min(atr / (recent_close * 0.02), 2.0) / 2.0  # Cap at 2% daily volatility
        
        # Calculate volume activity
        if 'volume' in price_history.columns and len(price_history) >= 20:
            recent_volume = price_history['volume'].tail(5).mean()
            avg_volume = price_history['volume'].tail(20).mean()
            volume_activity = min(recent_volume / avg_volume, 3.0) / 3.0 if avg_volume > 0 else 0.5
        else:
            volume_activity = 0.5  # Default medium activity
        
        return {
            'volatility_score': volatility_score,
            'volume_activity': volume_activity
        }
    
    def _adapt_parameters(self, market_conditions: Dict[str, Any]) -> Dict[str, Any]:
        """Adapt volume profile parameters based on market conditions."""
        
        volatility = market_conditions['volatility_score']
        volume_activity = market_conditions['volume_activity']
        
        # Adapt period based on volatility
        if self.volatility_adjustment:
            if volatility > 0.7:  # High volatility - use shorter period
                period_multiplier = 0.7
                reason = "high_volatility_short_period"
            elif volatility < 0.3:  # Low volatility - use longer period
                period_multiplier = 1.3
                reason = "low_volatility_long_period"
            else:
                period_multiplier = 1.0
                reason = "normal_volatility"
        else:
            period_multiplier = 1.0
            reason = "no_volatility_adjustment"
        
        # Adapt bin count based on volume activity
        if self.volume_adjustment:
            if volume_activity > 0.7:  # High volume - use more bins for granularity
                bin_multiplier = 1.2
                reason += "_high_volume_more_bins"
            elif volume_activity < 0.3:  # Low volume - use fewer bins to reduce noise
                bin_multiplier = 0.8
                reason += "_low_volume_fewer_bins"
            else:
                bin_multiplier = 1.0
                reason += "_normal_volume"
        else:
            bin_multiplier = 1.0
        
        # Calculate final parameters
        adapted_period = max(10, min(100, int(self.base_period * period_multiplier)))
        adapted_bin_count = max(20, min(100, int(self.base_bin_count * bin_multiplier)))
        
        return {
            'period': adapted_period,
            'bin_count': adapted_bin_count,
            'reason': reason
        }


class VolumeProfileComposite(Indicator):
    """Composite Volume Profile combining multiple variants for comprehensive analysis."""
    
    def __init__(self, 
                 include_basic: bool = True,
                 include_session: bool = True,
                 include_adaptive: bool = True,
                 base_period: int = 20,
                 base_bin_count: int = 50):
        super().__init__()
        self.base_period = base_period
        self.base_bin_count = base_bin_count
        self.name = f"VolumeProfileComposite_{base_period}_{base_bin_count}"
        
        # Initialize component indicators
        self.components = {}
        
        if include_basic:
            self.components['basic'] = VolumeProfileIndicator(base_period, base_bin_count)
        
        if include_session:
            self.components['session'] = SessionVolumeProfile(base_period, base_bin_count)
        
        if include_adaptive:
            self.components['adaptive'] = AdaptiveVolumeProfile(base_period, base_bin_count)
    
    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Calculate composite volume profile analysis."""
        if len(price_history) < self.base_period:
            return {'value': None, 'status': 'insufficient_data'}
        
        try:
            # Calculate each component
            component_results = {}
            valid_components = 0
            
            for name, component in self.components.items():
                result = component.calculate(price_history)
                component_results[name] = result
                if result['status'] == 'valid':
                    valid_components += 1
            
            if valid_components == 0:
                return {'value': None, 'status': 'no_valid_components'}
            
            # Create composite analysis
            composite_analysis = self._create_composite_analysis(component_results)
            
            return {
                'value': composite_analysis['composite_poc'],
                'composite_poc': composite_analysis['composite_poc'],
                'composite_vah': composite_analysis['composite_vah'],
                'composite_val': composite_analysis['composite_val'],
                'component_agreement': composite_analysis['component_agreement'],
                'dominant_component': composite_analysis['dominant_component'],
                'confidence_score': composite_analysis['confidence_score'],
                'component_results': component_results,
                'composite_bias': composite_analysis['composite_bias'],
                'status': 'valid'
            }
            
        except Exception as e:
            return {'value': None, 'status': f'composite_calculation_error: {str(e)}'}
    
    def _create_composite_analysis(self, component_results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Create composite analysis from component results."""
        
        # Extract valid results
        valid_results = {name: result for name, result in component_results.items() 
                        if result['status'] == 'valid'}
        
        if not valid_results:
            raise ValueError("No valid component results")
        
        # Weight components (basic=1.0, session=1.2, adaptive=1.1)
        component_weights = {'basic': 1.0, 'session': 1.2, 'adaptive': 1.1}
        
        # Calculate weighted averages
        weighted_poc = 0
        weighted_vah = 0
        weighted_val = 0
        total_weight = 0
        
        for name, result in valid_results.items():
            weight = component_weights.get(name, 1.0)
            weighted_poc += result['poc'] * weight
            weighted_vah += result['vah'] * weight
            weighted_val += result['val'] * weight
            total_weight += weight
        
        composite_poc = weighted_poc / total_weight
        composite_vah = weighted_vah / total_weight
        composite_val = weighted_val / total_weight
        
        # Calculate component agreement
        poc_values = [result['poc'] for result in valid_results.values()]
        poc_std = np.std(poc_values)
        agreement_score = 1.0 - min(poc_std / composite_poc * 10, 1.0) if composite_poc > 0 else 0
        
        # Determine dominant component (highest confidence or volume)
        dominant_component = max(valid_results.keys(),
                               key=lambda name: valid_results[name].get('total_volume', 0))
        
        # Calculate confidence score based on agreement and component count
        confidence_score = agreement_score * (len(valid_results) / len(self.components))
        
        # Composite bias (majority vote with weights)
        bias_scores = {}
        for name, result in valid_results.items():
            bias = result.get('dominant_side', 'neutral')
            weight = component_weights.get(name, 1.0)
            bias_scores[bias] = bias_scores.get(bias, 0) + weight
        
        composite_bias = max(bias_scores.keys(), key=lambda b: bias_scores[b]) if bias_scores else 'neutral'
        
        return {
            'composite_poc': float(composite_poc),
            'composite_vah': float(composite_vah),
            'composite_val': float(composite_val),
            'component_agreement': float(agreement_score),
            'dominant_component': dominant_component,
            'confidence_score': float(confidence_score),
            'composite_bias': composite_bias
        }


# Configuration factory for advanced Volume Profile indicators
def create_advanced_volume_profile_config():
    """Create configuration with advanced Volume Profile variants."""
    from .indicator_config import IndicatorConfig
    
    config = IndicatorConfig()
    
    # Basic Volume Profile variants
    config.add_indicator('VolumeProfile_basic_20_50', lambda: VolumeProfileIndicator(20, 50))
    
    # Session-based Volume Profiles
    config.add_indicator('SessionVP_intraday', lambda: SessionVolumeProfile(20, 50))
    config.add_indicator('SessionVP_extended', lambda: SessionVolumeProfile(30, 60, 
                                                                          session_start=time(4, 0), 
                                                                          session_end=time(20, 0)))
    
    # Multi-timeframe Volume Profiles
    config.add_indicator('MultiTF_VP_standard', lambda: MultiTimeframeVolumeProfile())
    config.add_indicator('MultiTF_VP_swing', lambda: MultiTimeframeVolumeProfile(
        timeframes={'15m': 20, '1h': 24, '4h': 30, '1d': 20}, bin_count=40))
    
    # Adaptive Volume Profiles
    config.add_indicator('AdaptiveVP_full', lambda: AdaptiveVolumeProfile(20, 50, 
                                                                        volatility_adjustment=True, 
                                                                        volume_adjustment=True))
    config.add_indicator('AdaptiveVP_volatility', lambda: AdaptiveVolumeProfile(20, 50,
                                                                              volatility_adjustment=True,
                                                                              volume_adjustment=False))
    
    # Composite Volume Profiles
    config.add_indicator('CompositeVP_full', lambda: VolumeProfileComposite(
        include_basic=True, include_session=True, include_adaptive=True))
    config.add_indicator('CompositeVP_trading', lambda: VolumeProfileComposite(
        include_basic=True, include_session=True, include_adaptive=False))
    
    return config