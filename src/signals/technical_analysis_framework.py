#!/usr/bin/env python3
"""
Technical Analysis Framework Based on Fundamental Market Principles
技术分析底层要点框架

Core Philosophy from Chinese Market Wisdom:
1. 价格是市场合力 - Price reflects market consensus
2. 趋势是前提 - Trend is fundamental 
3. 市场有记忆 - Markets have memory
4. 时间同样关键 - Time is equally important
5. 均线是定锚 - Moving averages are anchors
6. 形态就是规律 - Patterns reveal rules
7. 尊重周期 - Respect cycles
8. 突破最有力 - Breakouts are powerful
9. 止损！止损！- Stop losses are crucial
10. 技术分析源于人性 - Technical analysis reflects human nature
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union
from enum import Enum
import asyncio
import asyncpg
from dataclasses import dataclass

class TrendDirection(Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    SIDEWAYS = "sideways"
    UNKNOWN = "unknown"

class SignalStrength(Enum):
    VERY_WEAK = 1
    WEAK = 2
    MODERATE = 3
    STRONG = 4
    VERY_STRONG = 5

@dataclass
class TechnicalSignal:
    """Technical analysis signal based on market principles"""
    symbol: str
    timestamp: datetime
    signal_type: str
    direction: TrendDirection
    strength: SignalStrength
    price: float
    stop_loss: Optional[float]
    target: Optional[float]
    confidence: float
    reasoning: str
    timeframe: str

class MarketConsensusAnalyzer:
    """
    原则1: 价格是市场合力 - Price is market consensus
    Analyzes price action to understand market participant sentiment
    """
    
    def __init__(self):
        self.lookback_periods = {
            'short': 5,
            'medium': 20,
            'long': 50
        }
    
    def analyze_price_consensus(self, df: pd.DataFrame) -> Dict:
        """
        Analyze price action to determine market consensus
        Considers volume, price movement, and volatility
        """
        if len(df) < self.lookback_periods['long']:
            return {'consensus': 'insufficient_data', 'strength': 0}
        
        # Recent price momentum
        recent_return = (df['close'].iloc[-1] / df['close'].iloc[-5] - 1) * 100
        
        # Volume-weighted consensus
        volume_weighted_price = (df['close'] * df['volume']).sum() / df['volume'].sum()
        current_price = df['close'].iloc[-1]
        
        # Price vs volume-weighted average
        price_vs_vwap = (current_price / volume_weighted_price - 1) * 100
        
        # Volatility analysis
        volatility = df['close'].pct_change().std() * np.sqrt(252) * 100
        
        consensus_strength = min(abs(recent_return) / 5, 1.0)  # Normalize to 0-1
        
        return {
            'consensus': 'bullish' if recent_return > 0 else 'bearish',
            'strength': consensus_strength,
            'price_vs_vwap': price_vs_vwap,
            'momentum': recent_return,
            'volatility': volatility
        }

class TrendAnalyzer:
    """
    原则2: 趋势是前提 - Trend is fundamental
    Multi-timeframe trend analysis for trend-following strategies
    """
    
    def __init__(self):
        self.ema_periods = {
            'fast': 12,
            'medium': 26,
            'slow': 50,
            'very_slow': 200
        }
    
    def calculate_trend_strength(self, df: pd.DataFrame) -> Dict:
        """
        Calculate trend strength using multiple EMAs
        Respects the principle: 上升趋势中主要做多，下降趋势中主要做空
        """
        if len(df) < self.ema_periods['very_slow']:
            return {'trend': TrendDirection.UNKNOWN, 'strength': 0}
        
        # Calculate EMAs
        emas = {}
        for name, period in self.ema_periods.items():
            emas[name] = df['close'].ewm(span=period).mean()
        
        current_price = df['close'].iloc[-1]
        
        # Trend hierarchy analysis
        trend_signals = []
        
        # Fast trend (12 vs 26 EMA)
        if emas['fast'].iloc[-1] > emas['medium'].iloc[-1]:
            trend_signals.append(1)  # Bullish
        else:
            trend_signals.append(-1)  # Bearish
        
        # Medium trend (26 vs 50 EMA)
        if emas['medium'].iloc[-1] > emas['slow'].iloc[-1]:
            trend_signals.append(1)
        else:
            trend_signals.append(-1)
        
        # Long trend (50 vs 200 EMA)
        if emas['slow'].iloc[-1] > emas['very_slow'].iloc[-1]:
            trend_signals.append(1)
        else:
            trend_signals.append(-1)
        
        # Price vs all EMAs
        price_above_emas = sum([
            1 if current_price > emas[name].iloc[-1] else -1
            for name in self.ema_periods.keys()
        ])
        
        # Overall trend strength
        total_signals = sum(trend_signals) + (price_above_emas / 4)
        trend_strength = abs(total_signals) / 4  # Normalize to 0-1
        
        if total_signals > 2:
            trend_direction = TrendDirection.BULLISH
        elif total_signals < -2:
            trend_direction = TrendDirection.BEARISH
        else:
            trend_direction = TrendDirection.SIDEWAYS
        
        return {
            'trend': trend_direction,
            'strength': trend_strength,
            'emas': emas,
            'signals': trend_signals,
            'price_vs_emas': price_above_emas
        }

class MarketMemoryAnalyzer:
    """
    原则3: 市场有记忆 - Markets have memory
    Detects support and resistance levels based on historical price action
    """
    
    def __init__(self, lookback_days: int = 252):
        self.lookback_days = lookback_days
        self.touch_threshold = 0.02  # 2% threshold for level validation
    
    def find_support_resistance(self, df: pd.DataFrame) -> Dict:
        """
        Find key support and resistance levels using market memory
        支撑位与压力位往往因集体心理而反复起作用
        """
        if len(df) < 50:
            return {'support_levels': [], 'resistance_levels': [], 'current_level': None}
        
        highs = df['high'].values
        lows = df['low'].values
        closes = df['close'].values
        
        # Find local peaks and troughs
        resistance_candidates = self._find_local_extrema(highs, 'max')
        support_candidates = self._find_local_extrema(lows, 'min')
        
        # Validate levels with multiple touches
        resistance_levels = self._validate_levels(resistance_candidates, highs, 'resistance')
        support_levels = self._validate_levels(support_candidates, lows, 'support')
        
        current_price = closes[-1]
        
        # Find nearest levels
        nearest_resistance = min([r for r in resistance_levels if r > current_price], 
                               default=None)
        nearest_support = max([s for s in support_levels if s < current_price], 
                            default=None)
        
        return {
            'support_levels': support_levels,
            'resistance_levels': resistance_levels,
            'nearest_support': nearest_support,
            'nearest_resistance': nearest_resistance,
            'current_price': current_price
        }
    
    def _find_local_extrema(self, data: np.ndarray, extrema_type: str) -> List[float]:
        """Find local maxima or minima in price data"""
        from scipy.signal import argrelextrema
        
        if extrema_type == 'max':
            extrema_indices = argrelextrema(data, np.greater, order=5)[0]
        else:
            extrema_indices = argrelextrema(data, np.less, order=5)[0]
        
        return [data[i] for i in extrema_indices]
    
    def _validate_levels(self, candidates: List[float], price_data: np.ndarray, 
                        level_type: str) -> List[float]:
        """Validate support/resistance levels by counting touches"""
        validated_levels = []
        
        for level in candidates:
            touches = 0
            for price in price_data:
                if abs(price - level) / level < self.touch_threshold:
                    touches += 1
            
            if touches >= 2:  # At least 2 touches to be valid
                validated_levels.append(level)
        
        return sorted(validated_levels)

class TimeAndCycleAnalyzer:
    """
    原则4: 时间同样关键 - Time is equally important
    原则7: 尊重周期，周期也会轮回 - Respect cycles
    Analyzes market cycles and timing
    """
    
    def __init__(self):
        self.cycle_periods = [5, 10, 20, 50, 100, 200]  # Common market cycles
    
    def analyze_market_cycles(self, df: pd.DataFrame) -> Dict:
        """
        Analyze market cycles and timing
        行情的持续时间往往决定利润的大小
        """
        if len(df) < max(self.cycle_periods):
            return {'cycle_phase': 'unknown', 'cycle_strength': 0}
        
        cycles_analysis = {}
        
        for period in self.cycle_periods:
            if len(df) >= period:
                # Calculate cycle using price oscillation
                price_series = df['close'].values
                cycle_high = np.max(price_series[-period:])
                cycle_low = np.min(price_series[-period:])
                current_price = price_series[-1]
                
                # Determine cycle position (0 = bottom, 1 = top)
                if cycle_high != cycle_low:
                    cycle_position = (current_price - cycle_low) / (cycle_high - cycle_low)
                else:
                    cycle_position = 0.5
                
                cycles_analysis[f'cycle_{period}'] = {
                    'position': cycle_position,
                    'range': (cycle_high - cycle_low) / cycle_low * 100,
                    'direction': 'up' if cycle_position > 0.6 else 'down' if cycle_position < 0.4 else 'middle'
                }
        
        # Overall cycle assessment
        avg_position = np.mean([c['position'] for c in cycles_analysis.values()])
        cycle_strength = np.std([c['position'] for c in cycles_analysis.values()])
        
        return {
            'cycles': cycles_analysis,
            'overall_position': avg_position,
            'cycle_strength': cycle_strength,
            'phase': self._determine_cycle_phase(avg_position)
        }
    
    def _determine_cycle_phase(self, position: float) -> str:
        """Determine current cycle phase"""
        if position < 0.2:
            return 'bottom'
        elif position < 0.4:
            return 'early_uptrend'
        elif position < 0.6:
            return 'middle'
        elif position < 0.8:
            return 'late_uptrend'
        else:
            return 'top'

class MovingAverageAnchor:
    """
    原则5: 均线是定锚 - Moving averages are anchors
    Comprehensive moving average analysis for trend anchoring
    """
    
    def __init__(self):
        self.ma_periods = [5, 10, 20, 50, 100, 200]
    
    def analyze_ma_system(self, df: pd.DataFrame) -> Dict:
        """
        Analyze moving average system for trend anchoring
        短中长期均线的方向与交叉，能揭示趋势的健康与反转
        """
        if len(df) < max(self.ma_periods):
            return {'ma_trend': 'unknown', 'crossover_signals': []}
        
        # Calculate all moving averages
        mas = {}
        for period in self.ma_periods:
            mas[f'ma_{period}'] = df['close'].rolling(window=period).mean()
        
        current_price = df['close'].iloc[-1]
        
        # MA alignment analysis
        ma_values = [mas[f'ma_{period}'].iloc[-1] for period in self.ma_periods]
        ma_aligned_bullish = all(ma_values[i] > ma_values[i+1] for i in range(len(ma_values)-1))
        ma_aligned_bearish = all(ma_values[i] < ma_values[i+1] for i in range(len(ma_values)-1))
        
        # Crossover detection
        crossovers = self._detect_crossovers(mas)
        
        # Price vs MA analysis
        price_above_mas = sum(1 for period in self.ma_periods 
                             if current_price > mas[f'ma_{period}'].iloc[-1])
        
        # MA slope analysis (trend strength)
        ma_slopes = {}
        for period in self.ma_periods:
            ma_series = mas[f'ma_{period}']
            slope = (ma_series.iloc[-1] - ma_series.iloc[-5]) / ma_series.iloc[-5] * 100
            ma_slopes[f'ma_{period}_slope'] = slope
        
        return {
            'mas': mas,
            'ma_aligned_bullish': ma_aligned_bullish,
            'ma_aligned_bearish': ma_aligned_bearish,
            'crossovers': crossovers,
            'price_above_mas': price_above_mas,
            'total_mas': len(self.ma_periods),
            'ma_slopes': ma_slopes,
            'anchor_strength': self._calculate_anchor_strength(ma_slopes, ma_aligned_bullish, ma_aligned_bearish)
        }
    
    def _detect_crossovers(self, mas: Dict) -> List[Dict]:
        """Detect recent MA crossovers"""
        crossovers = []
        
        # Check key crossovers (Golden/Death Cross, etc.)
        key_pairs = [
            (50, 200),  # Golden/Death Cross
            (20, 50),   # Intermediate trend
            (5, 20),    # Short-term trend
        ]
        
        for fast, slow in key_pairs:
            if f'ma_{fast}' in mas and f'ma_{slow}' in mas:
                fast_ma = mas[f'ma_{fast}']
                slow_ma = mas[f'ma_{slow}']
                
                # Check for recent crossover (last 3 periods)
                for i in range(1, min(4, len(fast_ma))):
                    if (fast_ma.iloc[-i] > slow_ma.iloc[-i] and 
                        fast_ma.iloc[-i-1] <= slow_ma.iloc[-i-1]):
                        crossovers.append({
                            'type': 'golden_cross',
                            'fast_ma': fast,
                            'slow_ma': slow,
                            'periods_ago': i,
                            'signal': 'bullish'
                        })
                    elif (fast_ma.iloc[-i] < slow_ma.iloc[-i] and 
                          fast_ma.iloc[-i-1] >= slow_ma.iloc[-i-1]):
                        crossovers.append({
                            'type': 'death_cross',
                            'fast_ma': fast,
                            'slow_ma': slow,
                            'periods_ago': i,
                            'signal': 'bearish'
                        })
        
        return crossovers
    
    def _calculate_anchor_strength(self, slopes: Dict, bullish_aligned: bool, 
                                 bearish_aligned: bool) -> float:
        """Calculate the strength of MA anchor"""
        avg_slope = np.mean(list(slopes.values()))
        slope_consistency = 1 - np.std(list(slopes.values())) / (abs(avg_slope) + 0.01)
        
        alignment_bonus = 0.3 if (bullish_aligned or bearish_aligned) else 0
        
        return min(abs(avg_slope) * slope_consistency + alignment_bonus, 1.0)

class PatternRecognizer:
    """
    原则6: 形态就是规律和买卖点要义 - Patterns reveal rules
    Recognizes classic chart patterns for entry/exit points
    """
    
    def __init__(self):
        self.pattern_window = 20
    
    def detect_patterns(self, df: pd.DataFrame) -> Dict:
        """
        Detect classic chart patterns
        双顶双底、头肩形、三角形整理等，都是供需力量在图形里的反映
        """
        if len(df) < self.pattern_window * 2:
            return {'patterns': [], 'pattern_strength': 0}
        
        patterns = []
        
        # Double top/bottom detection
        double_patterns = self._detect_double_top_bottom(df)
        patterns.extend(double_patterns)
        
        # Head and shoulders detection
        hs_patterns = self._detect_head_shoulders(df)
        patterns.extend(hs_patterns)
        
        # Triangle patterns
        triangle_patterns = self._detect_triangles(df)
        patterns.extend(triangle_patterns)
        
        # Flag and pennant patterns
        flag_patterns = self._detect_flags_pennants(df)
        patterns.extend(flag_patterns)
        
        return {
            'patterns': patterns,
            'pattern_count': len(patterns),
            'bullish_patterns': len([p for p in patterns if p['signal'] == 'bullish']),
            'bearish_patterns': len([p for p in patterns if p['signal'] == 'bearish'])
        }
    
    def _detect_double_top_bottom(self, df: pd.DataFrame) -> List[Dict]:
        """Detect double top and double bottom patterns"""
        patterns = []
        highs = df['high'].values
        lows = df['low'].values
        
        # Simple double top detection (last 40 periods)
        if len(df) >= 40:
            recent_highs = highs[-40:]
            max_indices = np.argsort(recent_highs)[-3:]  # Top 3 highs
            
            if len(max_indices) >= 2:
                # Check if two highest points are similar and separated
                peak1_idx, peak2_idx = max_indices[-2:]
                peak1_price, peak2_price = recent_highs[peak1_idx], recent_highs[peak2_idx]
                
                if (abs(peak1_price - peak2_price) / peak1_price < 0.03 and  # Within 3%
                    abs(peak1_idx - peak2_idx) > 5):  # Separated by at least 5 periods
                    patterns.append({
                        'type': 'double_top',
                        'signal': 'bearish',
                        'confidence': 0.7,
                        'target': min(lows[-20:]),  # Neckline target
                        'stop_loss': max(peak1_price, peak2_price) * 1.02
                    })
        
        # Double bottom detection
        if len(df) >= 40:
            recent_lows = lows[-40:]
            min_indices = np.argsort(recent_lows)[:3]  # Bottom 3 lows
            
            if len(min_indices) >= 2:
                trough1_idx, trough2_idx = min_indices[:2]
                trough1_price, trough2_price = recent_lows[trough1_idx], recent_lows[trough2_idx]
                
                if (abs(trough1_price - trough2_price) / trough1_price < 0.03 and
                    abs(trough1_idx - trough2_idx) > 5):
                    patterns.append({
                        'type': 'double_bottom',
                        'signal': 'bullish',
                        'confidence': 0.7,
                        'target': max(highs[-20:]),  # Neckline target
                        'stop_loss': min(trough1_price, trough2_price) * 0.98
                    })
        
        return patterns
    
    def _detect_head_shoulders(self, df: pd.DataFrame) -> List[Dict]:
        """Detect head and shoulders patterns"""
        # Simplified head and shoulders detection
        # This would need more sophisticated implementation for production
        return []
    
    def _detect_triangles(self, df: pd.DataFrame) -> List[Dict]:
        """Detect triangle consolidation patterns"""
        # Simplified triangle detection
        # Would need trend line analysis for production
        return []
    
    def _detect_flags_pennants(self, df: pd.DataFrame) -> List[Dict]:
        """Detect flag and pennant continuation patterns"""
        # Simplified flag/pennant detection
        return []

class BreakoutDetector:
    """
    原则8: 突破最有力 - Breakouts are powerful
    Detects price breakouts with volume confirmation
    """
    
    def __init__(self):
        self.volume_threshold = 1.5  # 1.5x average volume for confirmation
        self.price_threshold = 0.02  # 2% breakout threshold
    
    def detect_breakouts(self, df: pd.DataFrame, support_resistance: Dict) -> List[Dict]:
        """
        Detect breakouts from key levels
        价格突破重要关口并放量时，往往是新一轮行情的起点
        """
        if len(df) < 20:
            return []
        
        breakouts = []
        current_price = df['close'].iloc[-1]
        current_volume = df['volume'].iloc[-1]
        avg_volume = df['volume'].rolling(window=20).mean().iloc[-1]
        
        # Volume confirmation
        volume_confirmed = current_volume > (avg_volume * self.volume_threshold)
        
        # Check resistance breakouts (bullish)
        resistance_levels = support_resistance.get('resistance_levels', [])
        for resistance in resistance_levels:
            if (current_price > resistance * (1 + self.price_threshold) and
                df['close'].iloc[-2] <= resistance):
                breakouts.append({
                    'type': 'resistance_breakout',
                    'signal': 'bullish',
                    'level': resistance,
                    'current_price': current_price,
                    'volume_confirmed': volume_confirmed,
                    'strength': SignalStrength.STRONG if volume_confirmed else SignalStrength.MODERATE,
                    'target': resistance * 1.1,  # 10% target above breakout
                    'stop_loss': resistance * 0.98  # 2% below breakout level
                })
        
        # Check support breakdowns (bearish)
        support_levels = support_resistance.get('support_levels', [])
        for support in support_levels:
            if (current_price < support * (1 - self.price_threshold) and
                df['close'].iloc[-2] >= support):
                breakouts.append({
                    'type': 'support_breakdown',
                    'signal': 'bearish',
                    'level': support,
                    'current_price': current_price,
                    'volume_confirmed': volume_confirmed,
                    'strength': SignalStrength.STRONG if volume_confirmed else SignalStrength.MODERATE,
                    'target': support * 0.9,  # 10% target below breakdown
                    'stop_loss': support * 1.02  # 2% above breakdown level
                })
        
        return breakouts

class RiskManager:
    """
    原则9: 止损！止损！- Stop losses are crucial
    Comprehensive risk management system
    """
    
    def __init__(self):
        self.max_risk_per_trade = 0.02  # 2% max risk per trade
        self.max_portfolio_risk = 0.06  # 6% max portfolio risk
        self.atr_multiplier = 2.0  # ATR-based stop loss multiplier
    
    def calculate_position_size(self, account_balance: float, entry_price: float, 
                              stop_loss: float) -> Dict:
        """
        Calculate optimal position size based on risk management
        技术分析无法保证百分百正确，止损才是长期生存的护身符
        """
        risk_amount = account_balance * self.max_risk_per_trade
        price_risk = abs(entry_price - stop_loss)
        
        if price_risk == 0:
            return {'position_size': 0, 'risk_reward_valid': False}
        
        shares = int(risk_amount / price_risk)
        actual_risk = shares * price_risk
        actual_risk_percentage = actual_risk / account_balance
        
        return {
            'position_size': shares,
            'actual_risk_amount': actual_risk,
            'actual_risk_percentage': actual_risk_percentage,
            'risk_per_share': price_risk,
            'risk_reward_valid': actual_risk_percentage <= self.max_risk_per_trade
        }
    
    def calculate_atr_stop_loss(self, df: pd.DataFrame, direction: str, 
                               entry_price: float) -> float:
        """Calculate ATR-based stop loss"""
        if len(df) < 14:
            # Fallback to percentage stop
            return entry_price * (0.95 if direction == 'long' else 1.05)
        
        # Calculate ATR (Average True Range)
        high = df['high']
        low = df['low']
        close = df['close']
        
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = true_range.rolling(window=14).mean().iloc[-1]
        
        if direction == 'long':
            return entry_price - (atr * self.atr_multiplier)
        else:
            return entry_price + (atr * self.atr_multiplier)

class TechnicalAnalysisFramework:
    """
    原则10: 所有的技术分析的都源于人性 - Technical analysis reflects human nature
    Comprehensive technical analysis framework integrating all principles
    """
    
    def __init__(self):
        self.consensus_analyzer = MarketConsensusAnalyzer()
        self.trend_analyzer = TrendAnalyzer()
        self.memory_analyzer = MarketMemoryAnalyzer()
        self.cycle_analyzer = TimeAndCycleAnalyzer()
        self.ma_anchor = MovingAverageAnchor()
        self.pattern_recognizer = PatternRecognizer()
        self.breakout_detector = BreakoutDetector()
        self.risk_manager = RiskManager()
    
    async def analyze_symbol(self, symbol: str, timeframe: str = '1D') -> TechnicalSignal:
        """
        Comprehensive technical analysis for a symbol
        Integrates all 10 fundamental principles
        """
        # Get market data (placeholder - would connect to your minute data)
        df = await self._get_market_data(symbol, timeframe)
        
        if df is None or len(df) < 50:
            return TechnicalSignal(
                symbol=symbol,
                timestamp=datetime.now(),
                signal_type="insufficient_data",
                direction=TrendDirection.UNKNOWN,
                strength=SignalStrength.VERY_WEAK,
                price=0,
                stop_loss=None,
                target=None,
                confidence=0,
                reasoning="Insufficient historical data",
                timeframe=timeframe
            )
        
        # Apply all analysis principles
        consensus = self.consensus_analyzer.analyze_price_consensus(df)
        trend = self.trend_analyzer.calculate_trend_strength(df)
        memory = self.memory_analyzer.find_support_resistance(df)
        cycles = self.cycle_analyzer.analyze_market_cycles(df)
        ma_analysis = self.ma_anchor.analyze_ma_system(df)
        patterns = self.pattern_recognizer.detect_patterns(df)
        breakouts = self.breakout_detector.detect_breakouts(df, memory)
        
        # Synthesize analysis into trading signal
        signal = self._synthesize_signal(
            symbol, df, consensus, trend, memory, cycles, 
            ma_analysis, patterns, breakouts, timeframe
        )
        
        return signal
    
    def _synthesize_signal(self, symbol: str, df: pd.DataFrame, consensus: Dict, 
                          trend: Dict, memory: Dict, cycles: Dict, ma_analysis: Dict,
                          patterns: Dict, breakouts: List, timeframe: str) -> TechnicalSignal:
        """
        Synthesize all analysis into a coherent trading signal
        Based on human nature and market psychology
        """
        current_price = df['close'].iloc[-1]
        
        # Weight different factors
        trend_weight = 0.3
        breakout_weight = 0.25
        pattern_weight = 0.2
        consensus_weight = 0.15
        cycle_weight = 0.1
        
        # Calculate composite score
        bullish_score = 0
        bearish_score = 0
        
        # Trend analysis (原则2: 趋势是前提)
        if trend['trend'] == TrendDirection.BULLISH:
            bullish_score += trend_weight * trend['strength']
        elif trend['trend'] == TrendDirection.BEARISH:
            bearish_score += trend_weight * trend['strength']
        
        # Breakout analysis (原则8: 突破最有力)
        for breakout in breakouts:
            if breakout['signal'] == 'bullish':
                breakout_strength = 1.0 if breakout['volume_confirmed'] else 0.6
                bullish_score += breakout_weight * breakout_strength
            else:
                breakout_strength = 1.0 if breakout['volume_confirmed'] else 0.6
                bearish_score += breakout_weight * breakout_strength
        
        # Pattern analysis (原则6: 形态就是规律)
        if patterns['bullish_patterns'] > patterns['bearish_patterns']:
            bullish_score += pattern_weight * 0.7
        elif patterns['bearish_patterns'] > patterns['bullish_patterns']:
            bearish_score += pattern_weight * 0.7
        
        # Market consensus (原则1: 价格是市场合力)
        if consensus['consensus'] == 'bullish':
            bullish_score += consensus_weight * consensus['strength']
        else:
            bearish_score += consensus_weight * consensus['strength']
        
        # Cycle analysis (原则7: 尊重周期)
        cycle_phase = cycles.get('phase', 'middle')
        if cycle_phase in ['bottom', 'early_uptrend']:
            bullish_score += cycle_weight * 0.8
        elif cycle_phase in ['top', 'late_uptrend']:
            bearish_score += cycle_weight * 0.8
        
        # Determine final signal
        net_score = bullish_score - bearish_score
        
        if net_score > 0.6:
            direction = TrendDirection.BULLISH
            strength = SignalStrength.VERY_STRONG if net_score > 0.8 else SignalStrength.STRONG
        elif net_score < -0.6:
            direction = TrendDirection.BEARISH  
            strength = SignalStrength.VERY_STRONG if net_score < -0.8 else SignalStrength.STRONG
        elif abs(net_score) > 0.3:
            direction = TrendDirection.BULLISH if net_score > 0 else TrendDirection.BEARISH
            strength = SignalStrength.MODERATE
        else:
            direction = TrendDirection.SIDEWAYS
            strength = SignalStrength.WEAK
        
        # Calculate stop loss and target (原则9: 止损！止损！)
        if direction in [TrendDirection.BULLISH, TrendDirection.BEARISH]:
            signal_direction = 'long' if direction == TrendDirection.BULLISH else 'short'
            stop_loss = self.risk_manager.calculate_atr_stop_loss(df, signal_direction, current_price)
            
            # Target based on nearest support/resistance
            if direction == TrendDirection.BULLISH:
                target = memory.get('nearest_resistance', current_price * 1.1)
            else:
                target = memory.get('nearest_support', current_price * 0.9)
        else:
            stop_loss = None
            target = None
        
        # Generate reasoning
        reasoning = self._generate_reasoning(
            trend, breakouts, patterns, consensus, cycles, ma_analysis
        )
        
        return TechnicalSignal(
            symbol=symbol,
            timestamp=datetime.now(),
            signal_type="comprehensive_technical",
            direction=direction,
            strength=strength,
            price=current_price,
            stop_loss=stop_loss,
            target=target,
            confidence=abs(net_score),
            reasoning=reasoning,
            timeframe=timeframe
        )
    
    def _generate_reasoning(self, trend: Dict, breakouts: List, patterns: Dict,
                          consensus: Dict, cycles: Dict, ma_analysis: Dict) -> str:
        """Generate human-readable reasoning for the signal"""
        reasons = []
        
        # Trend reasoning
        if trend['trend'] == TrendDirection.BULLISH:
            reasons.append(f"Strong bullish trend (strength: {trend['strength']:.2f})")
        elif trend['trend'] == TrendDirection.BEARISH:
            reasons.append(f"Strong bearish trend (strength: {trend['strength']:.2f})")
        
        # Breakout reasoning
        for breakout in breakouts:
            if breakout['volume_confirmed']:
                reasons.append(f"Volume-confirmed {breakout['type']} at {breakout['level']:.2f}")
            else:
                reasons.append(f"Price {breakout['type']} at {breakout['level']:.2f} (low volume)")
        
        # Pattern reasoning
        if patterns['bullish_patterns'] > 0:
            reasons.append(f"{patterns['bullish_patterns']} bullish patterns detected")
        if patterns['bearish_patterns'] > 0:
            reasons.append(f"{patterns['bearish_patterns']} bearish patterns detected")
        
        # MA reasoning
        if ma_analysis.get('ma_aligned_bullish'):
            reasons.append("Moving averages in bullish alignment")
        elif ma_analysis.get('ma_aligned_bearish'):
            reasons.append("Moving averages in bearish alignment")
        
        # Cycle reasoning
        cycle_phase = cycles.get('phase', 'unknown')
        if cycle_phase != 'unknown':
            reasons.append(f"Market cycle in {cycle_phase} phase")
        
        return "; ".join(reasons) if reasons else "Mixed signals - sideways market"
    
    async def _get_market_data(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """
        Get market data from the database
        This would connect to your minute data infrastructure
        """
        # Placeholder - would implement actual database connection
        # using your existing minute data backfill system
        return None

# Example usage and testing
async def main():
    """Example usage of the technical analysis framework"""
    framework = TechnicalAnalysisFramework()
    
    # Analyze multiple symbols
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    
    for symbol in symbols:
        signal = await framework.analyze_symbol(symbol, '1D')
        print(f"\n{symbol} Analysis:")
        print(f"Direction: {signal.direction.value}")
        print(f"Strength: {signal.strength.value}")
        print(f"Confidence: {signal.confidence:.2f}")
        print(f"Reasoning: {signal.reasoning}")
        if signal.stop_loss:
            print(f"Stop Loss: ${signal.stop_loss:.2f}")
        if signal.target:
            print(f"Target: ${signal.target:.2f}")

if __name__ == "__main__":
    asyncio.run(main())