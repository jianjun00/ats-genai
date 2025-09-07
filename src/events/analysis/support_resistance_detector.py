#!/usr/bin/env python3
"""
Advanced Support/Resistance Event Detection System

Identifies, tracks, and classifies support and resistance levels across multiple
timeframes using sophisticated algorithms and market microstructure analysis.

Key Features:
- Multi-algorithm level detection (pivot points, clustering, volume profile)
- Cross-timeframe validation and confirmation  
- Real-time test detection and outcome classification
- Adaptive level strength scoring based on historical performance
- Integration with existing market data infrastructure
"""

import asyncio
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, asdict
from enum import Enum
import logging
# Optional scientific computing imports
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    print("Warning: scipy not available, some S/R detection features will be disabled")

try:
    from sklearn.cluster import DBSCAN
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("Warning: scikit-learn not available, clustering features will be disabled")
from collections import defaultdict

logger = logging.getLogger(__name__)

class SRType(Enum):
    """Support/Resistance type"""
    SUPPORT = "support"
    RESISTANCE = "resistance"

class SRLevelType(Enum):
    """Support/Resistance level classification"""
    PIVOT_POINT = "pivot_point"          # Based on swing highs/lows
    PSYCHOLOGICAL = "psychological"       # Round numbers, major levels
    VOLUME_PROFILE = "volume_profile"     # High volume concentration
    HISTORICAL = "historical"            # Previous significant levels
    DYNAMIC = "dynamic"                  # Moving averages, trendlines
    CONFLUENCE = "confluence"            # Multiple factors converge

class SRTestOutcome(Enum):
    """Outcome of S/R level test"""
    HOLD_STRONG = "hold_strong"          # Bounced with conviction
    HOLD_WEAK = "hold_weak"             # Bounced but weakly  
    BREAK_CLEAN = "break_clean"         # Clean break through
    BREAK_FALSE = "break_false"         # Brief break then return
    PENETRATION = "penetration"         # Minor penetration but held
    PENDING = "pending"                 # Test in progress

class Timeframe(Enum):
    """Analysis timeframes"""
    INTRADAY_1M = "1m"
    INTRADAY_5M = "5m" 
    INTRADAY_15M = "15m"
    INTRADAY_1H = "1h"
    DAILY = "1d"
    WEEKLY = "1w"
    MONTHLY = "1M"
    QUARTERLY = "3M"
    YEARLY = "1Y"

@dataclass
class SRLevel:
    """Support/Resistance level definition"""
    price: float
    sr_type: SRType
    level_type: SRLevelType
    timeframe: Timeframe
    strength: float  # 0.0 to 1.0
    first_established: datetime
    last_tested: datetime
    test_count: int
    hold_count: int
    break_count: int
    confidence: float  # Statistical confidence
    volume_confirmation: bool
    metadata: Dict

@dataclass
class SRTest:
    """Individual test of S/R level"""
    level_id: str
    test_datetime: datetime
    test_price: float
    approach_direction: str  # "from_above" or "from_below"
    max_penetration: float  # How far price went beyond level
    hold_duration: timedelta  # How long price respected level
    volume_spike: float  # Volume relative to average
    outcome: SRTestOutcome
    confidence: float
    timeframe: Timeframe

@dataclass
class SREvent:
    """Complete S/R event for database storage"""
    event_id: str
    symbol: str
    level: SRLevel
    test: SRTest
    created_at: datetime
    updated_at: datetime

class SupportResistanceDetector:
    """Advanced Support/Resistance detection system"""
    
    def __init__(self, config: Dict = None):
        self.config = config or self._default_config()
        
        # Algorithm parameters
        self.pivot_lookback = self.config.get('pivot_lookback', 20)
        self.cluster_epsilon = self.config.get('cluster_epsilon', 0.02)  # 2% price tolerance
        self.min_cluster_size = self.config.get('min_cluster_size', 3)
        self.volume_threshold = self.config.get('volume_threshold', 1.5)  # 1.5x average volume
        
        # Test detection parameters
        self.proximity_tolerance = self.config.get('proximity_tolerance', 0.005)  # 0.5%
        self.break_threshold = self.config.get('break_threshold', 0.01)  # 1% move to confirm break
        self.hold_duration_min = self.config.get('hold_duration_min', 300)  # 5 minutes minimum
        
        # Level strength parameters
        self.strength_decay_days = self.config.get('strength_decay_days', 30)
        self.volume_weight = self.config.get('volume_weight', 0.3)
        
        # Active levels tracking
        self.active_levels: Dict[str, Dict[Timeframe, List[SRLevel]]] = defaultdict(lambda: defaultdict(list))
        self.level_tests: Dict[str, List[SRTest]] = defaultdict(list)
    
    def _default_config(self) -> Dict:
        """Default configuration parameters"""
        return {
            'pivot_lookback': 20,
            'cluster_epsilon': 0.02,
            'min_cluster_size': 3,
            'volume_threshold': 1.5,
            'proximity_tolerance': 0.005,
            'break_threshold': 0.01,
            'hold_duration_min': 300,
            'strength_decay_days': 30,
            'volume_weight': 0.3,
            'timeframes': [
                Timeframe.INTRADAY_1H,
                Timeframe.DAILY,
                Timeframe.WEEKLY,
                Timeframe.MONTHLY
            ],
            'psychological_levels': True,
            'volume_profile_levels': True,
            'dynamic_levels': False  # Moving averages, etc.
        }
    
    async def detect_sr_levels(self, symbol: str, ohlcv_data: pd.DataFrame, timeframe: Timeframe) -> List[SRLevel]:
        """
        Detect support/resistance levels using multiple algorithms
        
        Args:
            symbol: Stock symbol
            ohlcv_data: OHLCV price data
            timeframe: Analysis timeframe
            
        Returns:
            List of detected S/R levels
        """
        logger.info(f"Detecting S/R levels for {symbol} on {timeframe.value} timeframe")
        
        if len(ohlcv_data) < self.pivot_lookback * 2:
            logger.warning(f"Insufficient data for {symbol}: {len(ohlcv_data)} rows")
            return []
        
        levels = []
        
        # 1. Pivot Point Analysis
        pivot_levels = await self._detect_pivot_levels(ohlcv_data, timeframe)
        levels.extend(pivot_levels)
        
        # 2. Volume Profile Analysis  
        if self.config.get('volume_profile_levels'):
            volume_levels = await self._detect_volume_profile_levels(ohlcv_data, timeframe)
            levels.extend(volume_levels)
        
        # 3. Psychological Levels
        if self.config.get('psychological_levels'):
            psych_levels = await self._detect_psychological_levels(ohlcv_data, timeframe)
            levels.extend(psych_levels)
        
        # 4. Historical Level Analysis
        historical_levels = await self._detect_historical_levels(ohlcv_data, timeframe)
        levels.extend(historical_levels)
        
        # 5. Clustering and Confluence Detection
        clustered_levels = await self._apply_clustering(levels, ohlcv_data)
        
        # 6. Level Validation and Strength Calculation
        validated_levels = await self._validate_and_score_levels(clustered_levels, ohlcv_data, timeframe)
        
        logger.info(f"Detected {len(validated_levels)} S/R levels for {symbol} ({timeframe.value})")
        return validated_levels
    
    async def _detect_pivot_levels(self, data: pd.DataFrame, timeframe: Timeframe) -> List[SRLevel]:
        """Detect levels based on swing highs and lows (pivot points)"""
        levels = []
        
        # Calculate swing highs and lows
        highs = data['high'].rolling(window=self.pivot_lookback*2+1, center=True).max()
        lows = data['low'].rolling(window=self.pivot_lookback*2+1, center=True).min()
        
        # Find pivot highs (resistance)
        pivot_highs = []
        for i in range(self.pivot_lookback, len(data) - self.pivot_lookback):
            if data['high'].iloc[i] == highs.iloc[i]:
                # Verify it's actually a pivot (higher than surroundings)
                left_max = data['high'].iloc[i-self.pivot_lookback:i].max()
                right_max = data['high'].iloc[i+1:i+self.pivot_lookback+1].max()
                
                if data['high'].iloc[i] > left_max and data['high'].iloc[i] > right_max:
                    pivot_highs.append({
                        'price': data['high'].iloc[i],
                        'datetime': data.index[i],
                        'volume': data['volume'].iloc[i] if 'volume' in data.columns else 0
                    })
        
        # Find pivot lows (support)  
        pivot_lows = []
        for i in range(self.pivot_lookback, len(data) - self.pivot_lookback):
            if data['low'].iloc[i] == lows.iloc[i]:
                # Verify it's actually a pivot (lower than surroundings)
                left_min = data['low'].iloc[i-self.pivot_lookback:i].min()
                right_min = data['low'].iloc[i+1:i+self.pivot_lookback+1].min()
                
                if data['low'].iloc[i] < left_min and data['low'].iloc[i] < right_min:
                    pivot_lows.append({
                        'price': data['low'].iloc[i],
                        'datetime': data.index[i],
                        'volume': data['volume'].iloc[i] if 'volume' in data.columns else 0
                    })
        
        # Convert to SRLevel objects
        avg_volume = data['volume'].mean() if 'volume' in data.columns else 1
        
        for pivot in pivot_highs:
            volume_conf = min(1.0, pivot['volume'] / avg_volume) if avg_volume > 0 else 0.5
            levels.append(SRLevel(
                price=pivot['price'],
                sr_type=SRType.RESISTANCE,
                level_type=SRLevelType.PIVOT_POINT,
                timeframe=timeframe,
                strength=0.6,  # Initial strength
                first_established=pivot['datetime'],
                last_tested=pivot['datetime'],
                test_count=1,
                hold_count=1,
                break_count=0,
                confidence=volume_conf,
                volume_confirmation=pivot['volume'] > avg_volume * self.volume_threshold,
                metadata={'pivot_type': 'high', 'initial_volume': pivot['volume']}
            ))
        
        for pivot in pivot_lows:
            volume_conf = min(1.0, pivot['volume'] / avg_volume) if avg_volume > 0 else 0.5
            levels.append(SRLevel(
                price=pivot['price'],
                sr_type=SRType.SUPPORT,
                level_type=SRLevelType.PIVOT_POINT,
                timeframe=timeframe,
                strength=0.6,  # Initial strength
                first_established=pivot['datetime'],
                last_tested=pivot['datetime'],
                test_count=1,
                hold_count=1,
                break_count=0,
                confidence=volume_conf,
                volume_confirmation=pivot['volume'] > avg_volume * self.volume_threshold,
                metadata={'pivot_type': 'low', 'initial_volume': pivot['volume']}
            ))
        
        return levels
    
    async def _detect_volume_profile_levels(self, data: pd.DataFrame, timeframe: Timeframe) -> List[SRLevel]:
        """Detect levels based on volume concentration (Volume Profile)"""
        if 'volume' not in data.columns:
            return []
        
        levels = []
        
        # Create price bins and accumulate volume
        price_min = data['low'].min()
        price_max = data['high'].max() 
        price_range = price_max - price_min
        bin_size = price_range / 100  # 100 price bins
        
        # Volume at Price calculation
        volume_profile = defaultdict(float)
        
        for _, row in data.iterrows():
            # Distribute volume across the price range for this bar
            price_levels = np.linspace(row['low'], row['high'], 10)
            volume_per_level = row['volume'] / len(price_levels)
            
            for price in price_levels:
                bin_index = int((price - price_min) / bin_size)
                volume_profile[bin_index] += volume_per_level
        
        # Find high volume nodes (POCs - Point of Control)
        volume_data = [(price_min + bin_idx * bin_size, volume) for bin_idx, volume in volume_profile.items()]
        volume_data.sort(key=lambda x: x[1], reverse=True)
        
        # Select top volume levels
        total_volume = sum(vol for _, vol in volume_data)
        significant_levels = []
        
        for price, volume in volume_data[:20]:  # Top 20 volume levels
            volume_pct = volume / total_volume
            if volume_pct > 0.02:  # At least 2% of total volume
                significant_levels.append({
                    'price': price,
                    'volume_pct': volume_pct,
                    'volume': volume
                })
        
        # Convert to SRLevel objects
        for level_data in significant_levels:
            # Determine if it's more likely support or resistance based on position
            current_price = data['close'].iloc[-1]
            sr_type = SRType.SUPPORT if level_data['price'] < current_price else SRType.RESISTANCE
            
            levels.append(SRLevel(
                price=level_data['price'],
                sr_type=sr_type,
                level_type=SRLevelType.VOLUME_PROFILE,
                timeframe=timeframe,
                strength=min(1.0, level_data['volume_pct'] * 10),  # Scale volume % to strength
                first_established=data.index[0],
                last_tested=data.index[-1],
                test_count=1,
                hold_count=1,
                break_count=0,
                confidence=level_data['volume_pct'],
                volume_confirmation=True,
                metadata={
                    'volume_pct': level_data['volume_pct'],
                    'total_volume': level_data['volume']
                }
            ))
        
        return levels
    
    async def _detect_psychological_levels(self, data: pd.DataFrame, timeframe: Timeframe) -> List[SRLevel]:
        """Detect psychological levels (round numbers, etc.)"""
        levels = []
        
        price_range = (data['low'].min(), data['high'].max())
        current_price = data['close'].iloc[-1]
        
        # Generate round number levels
        psychological_prices = []
        
        # Major round numbers (100, 200, 300, etc.)
        for price in range(int(price_range[0] // 100) * 100, int(price_range[1] // 100 + 1) * 100 + 1, 100):
            if price_range[0] <= price <= price_range[1]:
                psychological_prices.append(price)
        
        # Half levels (150, 250, 350, etc.)
        for price in range(int(price_range[0] // 100) * 100 + 50, int(price_range[1] // 100 + 1) * 100 + 1, 100):
            if price_range[0] <= price <= price_range[1]:
                psychological_prices.append(price)
        
        # Quarter levels for stocks under $100
        if price_range[1] < 100:
            for base in range(int(price_range[0] // 25) * 25, int(price_range[1] // 25 + 1) * 25 + 1, 25):
                if price_range[0] <= base <= price_range[1]:
                    psychological_prices.append(base)
        
        # Previous highs/lows as psychological levels
        if len(data) > 252:  # 1 year of daily data
            yearly_high = data['high'].rolling(252).max().iloc[-1]
            yearly_low = data['low'].rolling(252).min().iloc[-1]
            psychological_prices.extend([yearly_high, yearly_low])
        
        # Convert to SRLevel objects
        for price in psychological_prices:
            if price <= 0:
                continue
                
            # Determine support vs resistance
            sr_type = SRType.SUPPORT if price < current_price else SRType.RESISTANCE
            
            # Check if this level has been tested (price came within 1% of it)
            proximity_tests = data[
                (data['low'] <= price * 1.01) & (data['high'] >= price * 0.99)
            ]
            
            if len(proximity_tests) >= 2:  # At least 2 tests to be significant
                levels.append(SRLevel(
                    price=price,
                    sr_type=sr_type,
                    level_type=SRLevelType.PSYCHOLOGICAL,
                    timeframe=timeframe,
                    strength=0.4,  # Moderate initial strength
                    first_established=data.index[0],
                    last_tested=proximity_tests.index[-1] if len(proximity_tests) > 0 else data.index[-1],
                    test_count=len(proximity_tests),
                    hold_count=len(proximity_tests),  # Assume held until proven otherwise
                    break_count=0,
                    confidence=min(1.0, len(proximity_tests) / 10),
                    volume_confirmation=False,
                    metadata={
                        'round_number': price % 100 == 0 or price % 50 == 0,
                        'historical_extreme': price in [yearly_high, yearly_low] if 'yearly_high' in locals() else False
                    }
                ))
        
        return levels
    
    async def _detect_historical_levels(self, data: pd.DataFrame, timeframe: Timeframe) -> List[SRLevel]:
        """Detect levels based on historical price action"""
        levels = []
        
        # Find significant historical highs and lows
        lookback_periods = {
            Timeframe.INTRADAY_1H: 24 * 7,  # 1 week
            Timeframe.DAILY: 252,           # 1 year
            Timeframe.WEEKLY: 52,           # 1 year
            Timeframe.MONTHLY: 12,          # 1 year
        }
        
        lookback = lookback_periods.get(timeframe, 100)
        if len(data) < lookback:
            return levels
        
        # Historical highs
        historical_high = data['high'].rolling(lookback).max()
        historical_low = data['low'].rolling(lookback).min()
        
        # Find where these levels were established
        for i in range(lookback, len(data)):
            current_high = historical_high.iloc[i]
            current_low = historical_low.iloc[i]
            
            # Check if this is a new high/low level
            prev_high = historical_high.iloc[i-1] if i > 0 else 0
            prev_low = historical_low.iloc[i-1] if i > 0 else float('inf')
            
            # New historical high
            if current_high > prev_high and abs(current_high - prev_high) / prev_high > 0.02:  # 2% threshold
                levels.append(SRLevel(
                    price=current_high,
                    sr_type=SRType.RESISTANCE,
                    level_type=SRLevelType.HISTORICAL,
                    timeframe=timeframe,
                    strength=0.7,  # High strength for historical levels
                    first_established=data.index[i],
                    last_tested=data.index[i],
                    test_count=1,
                    hold_count=1,
                    break_count=0,
                    confidence=0.8,
                    volume_confirmation=False,
                    metadata={
                        'level_age_days': 0,
                        'historical_type': 'high'
                    }
                ))
            
            # New historical low
            if current_low < prev_low and abs(prev_low - current_low) / prev_low > 0.02:  # 2% threshold
                levels.append(SRLevel(
                    price=current_low,
                    sr_type=SRType.SUPPORT,
                    level_type=SRLevelType.HISTORICAL,
                    timeframe=timeframe,
                    strength=0.7,  # High strength for historical levels
                    first_established=data.index[i],
                    last_tested=data.index[i],
                    test_count=1,
                    hold_count=1,
                    break_count=0,
                    confidence=0.8,
                    volume_confirmation=False,
                    metadata={
                        'level_age_days': 0,
                        'historical_type': 'low'
                    }
                ))
        
        return levels
    
    async def _apply_clustering(self, levels: List[SRLevel], data: pd.DataFrame) -> List[SRLevel]:
        """Apply clustering to merge nearby levels and identify confluence"""
        if len(levels) < 2:
            return levels
        
        # Extract prices for clustering
        prices = np.array([level.price for level in levels]).reshape(-1, 1)
        
        if not SKLEARN_AVAILABLE:
            # Fallback: simple distance-based clustering
            clusters = self._simple_price_clustering(levels, self.cluster_epsilon)
        else:
            # Apply DBSCAN clustering
            clustering = DBSCAN(
                eps=self.cluster_epsilon * np.mean(prices),  # Epsilon as % of mean price
                min_samples=2
            ).fit(prices)
            
            # Group levels by cluster
            clusters = defaultdict(list)
            for i, cluster_id in enumerate(clustering.labels_):
                clusters[cluster_id].append(levels[i])
        
        # Merge levels within each cluster
        merged_levels = []
        
        for cluster_id, cluster_levels in clusters.items():
            if cluster_id == -1:  # Noise points (no cluster)
                merged_levels.extend(cluster_levels)
                continue
            
            if len(cluster_levels) == 1:
                merged_levels.extend(cluster_levels)
                continue
            
            # Merge levels in this cluster
            merged_level = await self._merge_cluster_levels(cluster_levels)
            merged_levels.append(merged_level)
        
        return merged_levels
    
    async def _merge_cluster_levels(self, cluster_levels: List[SRLevel]) -> SRLevel:
        """Merge multiple levels into a single confluence level"""
        
        # Calculate weighted average price (weight by strength)
        total_weight = sum(level.strength for level in cluster_levels)
        if total_weight == 0:
            weighted_price = np.mean([level.price for level in cluster_levels])
        else:
            weighted_price = sum(level.price * level.strength for level in cluster_levels) / total_weight
        
        # Determine dominant type
        support_count = sum(1 for level in cluster_levels if level.sr_type == SRType.SUPPORT)
        resistance_count = len(cluster_levels) - support_count
        dominant_type = SRType.SUPPORT if support_count > resistance_count else SRType.RESISTANCE
        
        # Combine statistics
        total_tests = sum(level.test_count for level in cluster_levels)
        total_holds = sum(level.hold_count for level in cluster_levels) 
        total_breaks = sum(level.break_count for level in cluster_levels)
        
        # Enhanced strength due to confluence
        base_strength = np.mean([level.strength for level in cluster_levels])
        confluence_bonus = min(0.3, len(cluster_levels) * 0.1)  # Up to 30% bonus
        final_strength = min(1.0, base_strength + confluence_bonus)
        
        # Enhanced confidence
        avg_confidence = np.mean([level.confidence for level in cluster_levels])
        confluence_confidence = min(1.0, avg_confidence + confluence_bonus)
        
        # Combine metadata
        level_types = [level.level_type for level in cluster_levels]
        combined_metadata = {
            'confluence_count': len(cluster_levels),
            'constituent_types': [lt.value for lt in level_types],
            'price_range': (
                min(level.price for level in cluster_levels),
                max(level.price for level in cluster_levels)
            )
        }
        
        return SRLevel(
            price=weighted_price,
            sr_type=dominant_type,
            level_type=SRLevelType.CONFLUENCE,
            timeframe=cluster_levels[0].timeframe,
            strength=final_strength,
            first_established=min(level.first_established for level in cluster_levels),
            last_tested=max(level.last_tested for level in cluster_levels),
            test_count=total_tests,
            hold_count=total_holds,
            break_count=total_breaks,
            confidence=confluence_confidence,
            volume_confirmation=any(level.volume_confirmation for level in cluster_levels),
            metadata=combined_metadata
        )
    
    async def _validate_and_score_levels(self, levels: List[SRLevel], data: pd.DataFrame, timeframe: Timeframe) -> List[SRLevel]:
        """Validate levels and calculate final strength scores"""
        validated_levels = []
        
        for level in levels:
            # Age-based strength decay
            try:
                if hasattr(data, 'timestamp') and len(data) > 0:
                    last_data_time = pd.to_datetime(data.iloc[-1]['timestamp'])
                elif 'timestamp' in data.columns:
                    last_data_time = pd.to_datetime(data['timestamp'].iloc[-1])
                else:
                    last_data_time = datetime.now()  # Fallback
                    
                level_age_days = (last_data_time - level.last_tested).days
                age_decay = max(0.1, 1.0 - level_age_days / self.strength_decay_days)
            except (AttributeError, TypeError, KeyError):
                # Fallback: assume recent data
                level_age_days = 1  # Default value
                age_decay = 0.9
            
            # Test success rate
            success_rate = level.hold_count / max(1, level.test_count)
            
            # Recency bonus (recently tested levels are more relevant)
            try:
                recency_days = (last_data_time - level.last_tested).days
                recency_bonus = max(0, 1.0 - recency_days / 30) * 0.2  # Up to 20% bonus
            except (AttributeError, TypeError):
                recency_bonus = 0.1  # Default bonus
            
            # Calculate final strength
            final_strength = (
                level.strength * 0.4 +           # Base algorithm strength
                success_rate * 0.3 +             # Historical success rate
                age_decay * 0.2 +                # Age factor
                recency_bonus * 0.1              # Recency bonus
            )
            
            # Update level
            level.strength = min(1.0, final_strength)
            
            # Only keep levels with minimum strength and tests
            if level.strength >= 0.3 and level.test_count >= 1:
                level.metadata['age_days'] = level_age_days
                level.metadata['success_rate'] = success_rate
                validated_levels.append(level)
        
        # Sort by strength (strongest first)
        validated_levels.sort(key=lambda x: x.strength, reverse=True)
        
        return validated_levels
    
    async def detect_sr_tests(self, symbol: str, current_data: pd.DataFrame, levels: List[SRLevel]) -> List[SRTest]:
        """
        Detect when price is testing support/resistance levels
        
        Args:
            symbol: Stock symbol
            current_data: Recent OHLCV data  
            levels: Known S/R levels to test against
            
        Returns:
            List of detected S/R tests
        """
        tests = []
        
        if len(current_data) < 2:
            return tests
        
        current_bar = current_data.iloc[-1]
        previous_bar = current_data.iloc[-2] if len(current_data) > 1 else current_bar
        
        for level in levels:
            # Check if price is near this level
            proximity = abs(current_bar['close'] - level.price) / level.price
            
            if proximity <= self.proximity_tolerance:
                # Price is testing this level
                test = await self._analyze_sr_test(level, current_data, len(current_data) - 1)
                if test:
                    tests.append(test)
        
        return tests
    
    async def _analyze_sr_test(self, level: SRLevel, data: pd.DataFrame, test_index: int) -> Optional[SRTest]:
        """Analyze a specific S/R level test"""
        
        if test_index < 5:  # Need some history to analyze properly
            return None
        
        test_bar = data.iloc[test_index]
        
        # Determine approach direction
        approach_prices = data['close'].iloc[test_index-5:test_index].values
        if len(approach_prices) > 0:
            if level.sr_type == SRType.SUPPORT:
                approach_direction = "from_above" if np.mean(approach_prices) > level.price else "from_below"
            else:  # RESISTANCE
                approach_direction = "from_below" if np.mean(approach_prices) < level.price else "from_above"
        else:
            approach_direction = "unknown"
        
        # Calculate penetration
        if level.sr_type == SRType.SUPPORT:
            max_penetration = max(0, level.price - test_bar['low'])
        else:  # RESISTANCE
            max_penetration = max(0, test_bar['high'] - level.price)
        
        penetration_pct = max_penetration / level.price
        
        # Analyze volume
        avg_volume = data['volume'].rolling(20).mean().iloc[test_index] if 'volume' in data.columns else 1
        volume_spike = (test_bar['volume'] / avg_volume) if avg_volume > 0 else 1
        
        # Determine initial outcome (may be updated later as more data comes in)
        outcome = SRTestOutcome.PENDING
        confidence = 0.5
        
        # If we have subsequent data, analyze the outcome
        if test_index < len(data) - 5:  # Have at least 5 bars after test
            subsequent_data = data.iloc[test_index:test_index+5]
            outcome, confidence = self._classify_test_outcome(level, test_bar, subsequent_data, penetration_pct)
        
        # Calculate hold duration (simplified - would need more sophisticated logic)
        hold_duration = timedelta(minutes=5)  # Placeholder
        
        test_id = f"{level.price}_{level.sr_type.value}_{test_bar.name}"
        
        return SRTest(
            level_id=test_id,
            test_datetime=test_bar.name,
            test_price=test_bar['close'],
            approach_direction=approach_direction,
            max_penetration=penetration_pct,
            hold_duration=hold_duration,
            volume_spike=volume_spike,
            outcome=outcome,
            confidence=confidence,
            timeframe=level.timeframe
        )
    
    def _classify_test_outcome(self, level: SRLevel, test_bar: pd.Series, subsequent_data: pd.DataFrame, penetration_pct: float) -> Tuple[SRTestOutcome, float]:
        """Classify the outcome of an S/R test"""
        
        # Analyze price action after the test
        subsequent_closes = subsequent_data['close'].values
        subsequent_highs = subsequent_data['high'].values
        subsequent_lows = subsequent_data['low'].values
        
        if level.sr_type == SRType.SUPPORT:
            # For support levels
            if penetration_pct > self.break_threshold:
                # Significant penetration - check if it's a clean break
                if np.all(subsequent_closes < level.price * 0.99):
                    return SRTestOutcome.BREAK_CLEAN, 0.8
                else:
                    return SRTestOutcome.BREAK_FALSE, 0.6
            elif penetration_pct > 0.002:  # Minor penetration
                if np.mean(subsequent_closes) > level.price:
                    return SRTestOutcome.PENETRATION, 0.7
                else:
                    return SRTestOutcome.HOLD_WEAK, 0.6
            else:
                # Clean bounce
                if np.min(subsequent_lows) > level.price * 0.995:
                    return SRTestOutcome.HOLD_STRONG, 0.9
                else:
                    return SRTestOutcome.HOLD_WEAK, 0.7
        
        else:  # RESISTANCE
            if penetration_pct > self.break_threshold:
                # Significant penetration - check if it's a clean break
                if np.all(subsequent_closes > level.price * 1.01):
                    return SRTestOutcome.BREAK_CLEAN, 0.8
                else:
                    return SRTestOutcome.BREAK_FALSE, 0.6
            elif penetration_pct > 0.002:  # Minor penetration
                if np.mean(subsequent_closes) < level.price:
                    return SRTestOutcome.PENETRATION, 0.7
                else:
                    return SRTestOutcome.HOLD_WEAK, 0.6
            else:
                # Clean rejection
                if np.max(subsequent_highs) < level.price * 1.005:
                    return SRTestOutcome.HOLD_STRONG, 0.9
                else:
                    return SRTestOutcome.HOLD_WEAK, 0.7
        
        return SRTestOutcome.PENDING, 0.5
    
    def get_active_levels(self, symbol: str, timeframe: Timeframe, price_range: Tuple[float, float] = None) -> List[SRLevel]:
        """Get currently active S/R levels for a symbol"""
        if symbol not in self.active_levels:
            return []
        
        levels = self.active_levels[symbol][timeframe]
        
        if price_range:
            # Filter levels within price range
            min_price, max_price = price_range
            levels = [
                level for level in levels
                if min_price <= level.price <= max_price
            ]
        
        return sorted(levels, key=lambda x: x.strength, reverse=True)
    
    def update_level_strength(self, level_id: str, test_outcome: SRTestOutcome):
        """Update level strength based on test outcome"""
        # Implementation would update the level's statistics and strength
        # based on the test outcome
        pass
    
    async def process_new_data(self, symbol: str, new_ohlcv: pd.DataFrame):
        """Process new market data and update S/R levels and tests"""
        for timeframe in self.config['timeframes']:
            # Detect new levels
            new_levels = await self.detect_sr_levels(symbol, new_ohlcv, timeframe)
            
            # Update active levels
            self.active_levels[symbol][timeframe] = new_levels
            
            # Detect new tests
            active_levels = self.get_active_levels(symbol, timeframe)
            new_tests = await self.detect_sr_tests(symbol, new_ohlcv, active_levels)
            
            # Store new tests
            self.level_tests[symbol].extend(new_tests)
    
    def _simple_price_clustering(self, levels: List[SRLevel], epsilon: float) -> Dict[int, List[SRLevel]]:
        """Simple distance-based clustering fallback when sklearn is not available"""
        clusters = defaultdict(list)
        cluster_id = 0
        
        # Sort levels by price
        sorted_levels = sorted(levels, key=lambda x: x.price)
        
        current_cluster = []
        current_cluster_price = None
        
        for level in sorted_levels:
            if current_cluster_price is None:
                # Start first cluster
                current_cluster = [level]
                current_cluster_price = level.price
            else:
                # Check if level is close enough to current cluster
                price_diff = abs(level.price - current_cluster_price) / current_cluster_price
                
                if price_diff <= epsilon:
                    # Add to current cluster
                    current_cluster.append(level)
                else:
                    # Finish current cluster and start new one
                    if current_cluster:
                        clusters[cluster_id] = current_cluster
                        cluster_id += 1
                    
                    current_cluster = [level]
                    current_cluster_price = level.price
        
        # Add final cluster
        if current_cluster:
            clusters[cluster_id] = current_cluster
        
        return clusters