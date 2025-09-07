"""
Smart Money Zones (SMZ) Methodology Implementation
Based on institutional trading behavior and market structure analysis.

This module implements the complete SMZ trading methodology including:
- Market structure detection (HH, HL, LH, LL)
- Change of Character (CHoCH) and Break of Structure (BOS)
- Smart Money Zones using Fibonacci retracements
- Institutional buy/sell zones identification
- Multi-timeframe confluence analysis
- Entry confirmation and signal generation
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Tuple, Literal
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

from .indicator import Indicator


class MarketStructure(Enum):
    """Market structure types."""
    BULLISH = "bullish"  # HH + HL
    BEARISH = "bearish"  # LH + LL
    COMPRESSION = "compression"  # HL + LH in same range
    UNKNOWN = "unknown"


class StructureChange(Enum):
    """Structure change types."""
    CHOCH_BULLISH = "choch_bullish"  # CHoCH to bullish
    CHOCH_BEARISH = "choch_bearish"  # CHoCH to bearish
    BOS_BULLISH = "bos_bullish"  # BOS to bullish
    BOS_BEARISH = "bos_bearish"  # BOS to bearish
    NONE = "none"


@dataclass
class SwingPoint:
    """Represents a swing high or low point."""
    index: int
    price: float
    timestamp: datetime
    type: Literal["high", "low"]
    significance: float = 0.0  # 0-1 score based on volume/range


@dataclass
class SmartMoneyZone:
    """Represents a Smart Money Zone with institutional levels."""
    swing_high: SwingPoint
    swing_low: SwingPoint
    direction: Literal["bullish", "bearish"]
    
    # Fibonacci levels
    fib_0: float  # BOS level
    fib_618: float  # Golden pocket start
    fib_786: float  # Golden pocket end / SMZ start
    fib_826: float  # SMZ end / Optimal entry
    fib_100: float  # Target level
    
    # Zone definitions
    institutional_zone: Tuple[float, float]  # 0.618 - 0.786
    smart_money_zone: Tuple[float, float]    # 0.786 - 0.826
    
    # Metadata
    created_at: datetime
    timeframe: str
    confidence: float = 0.0


class MarketStructureDetector(Indicator):
    """Detects market structure patterns (HH, HL, LH, LL, CHoCH, BOS)."""
    
    def __init__(self, swing_length: int = 10, min_swing_size: float = 0.001):
        """
        Initialize market structure detector.
        
        Args:
            swing_length: Minimum bars for swing point validation
            min_swing_size: Minimum price movement for valid swing (as percentage)
        """
        super().__init__()
        self.swing_length = swing_length
        self.min_swing_size = min_swing_size
        self.name = f"MarketStructure_{swing_length}"
        
    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Detect market structure and swing points."""
        if len(price_history) < self.swing_length * 3:
            return {'status': 'insufficient_data'}
        
        try:
            # Find swing points
            swing_highs = self._find_swing_highs(price_history)
            swing_lows = self._find_swing_lows(price_history)
            
            # Combine and sort swing points
            all_swings = self._combine_swings(swing_highs, swing_lows, price_history)
            
            if len(all_swings) < 4:
                return {'status': 'insufficient_swings'}
            
            # Analyze market structure
            structure = self._analyze_structure(all_swings)
            structure_change = self._detect_structure_change(all_swings)
            
            # Find recent BOS/CHoCH levels
            bos_level = self._find_bos_level(all_swings, structure_change)
            
            # Calculate current market context
            current_price = price_history['close'].iloc[-1]
            context = self._analyze_market_context(all_swings, current_price)
            
            return {
                'status': 'valid',
                'market_structure': structure.value,
                'structure_change': structure_change.value,
                'swing_points': all_swings,
                'swing_highs': swing_highs,
                'swing_lows': swing_lows,
                'bos_level': bos_level,
                'current_price': current_price,
                'untaken_highs': context['untaken_highs'],
                'untaken_lows': context['untaken_lows'],
                'trend_strength': context['trend_strength'],
                'structure_quality': context['structure_quality']
            }
            
        except Exception as e:
            return {'status': f'calculation_error: {str(e)}'}
    
    def _find_swing_highs(self, df: pd.DataFrame) -> List[SwingPoint]:
        """Find swing high points."""
        highs = df['high'].values
        swing_highs = []
        
        for i in range(self.swing_length, len(highs) - self.swing_length):
            # Check if current point is highest in the window
            left_window = highs[i - self.swing_length:i]
            right_window = highs[i + 1:i + self.swing_length + 1]
            
            if (highs[i] > max(left_window) and 
                highs[i] > max(right_window)):
                
                # Validate swing size
                recent_low = min(df['low'].iloc[i - self.swing_length:i + self.swing_length + 1])
                swing_size = (highs[i] - recent_low) / recent_low
                
                if swing_size >= self.min_swing_size:
                    swing_point = SwingPoint(
                        index=i,
                        price=highs[i],
                        timestamp=df.index[i],
                        type="high",
                        significance=self._calculate_significance(df, i, "high")
                    )
                    swing_highs.append(swing_point)
        
        return swing_highs
    
    def _find_swing_lows(self, df: pd.DataFrame) -> List[SwingPoint]:
        """Find swing low points."""
        lows = df['low'].values
        swing_lows = []
        
        for i in range(self.swing_length, len(lows) - self.swing_length):
            # Check if current point is lowest in the window
            left_window = lows[i - self.swing_length:i]
            right_window = lows[i + 1:i + self.swing_length + 1]
            
            if (lows[i] < min(left_window) and 
                lows[i] < min(right_window)):
                
                # Validate swing size
                recent_high = max(df['high'].iloc[i - self.swing_length:i + self.swing_length + 1])
                swing_size = (recent_high - lows[i]) / lows[i]
                
                if swing_size >= self.min_swing_size:
                    swing_point = SwingPoint(
                        index=i,
                        price=lows[i],
                        timestamp=df.index[i],
                        type="low",
                        significance=self._calculate_significance(df, i, "low")
                    )
                    swing_lows.append(swing_point)
        
        return swing_lows
    
    def _calculate_significance(self, df: pd.DataFrame, index: int, swing_type: str) -> float:
        """Calculate swing point significance based on volume and range."""
        try:
            # Volume significance
            if 'volume' in df.columns:
                avg_volume = df['volume'].rolling(20).mean().iloc[index]
                current_volume = df['volume'].iloc[index]
                volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
                volume_score = min(volume_ratio / 2, 1.0)  # Cap at 1.0
            else:
                volume_score = 0.5
            
            # Range significance
            current_range = df['high'].iloc[index] - df['low'].iloc[index]
            avg_range = (df['high'] - df['low']).rolling(20).mean().iloc[index]
            range_ratio = current_range / avg_range if avg_range > 0 else 1
            range_score = min(range_ratio / 2, 1.0)  # Cap at 1.0
            
            # Combine scores
            significance = (volume_score * 0.6 + range_score * 0.4)
            return min(significance, 1.0)
            
        except:
            return 0.5
    
    def _combine_swings(self, highs: List[SwingPoint], lows: List[SwingPoint], 
                       df: pd.DataFrame) -> List[SwingPoint]:
        """Combine and sort swing points chronologically."""
        all_swings = highs + lows
        all_swings.sort(key=lambda x: x.index)
        return all_swings
    
    def _analyze_structure(self, swings: List[SwingPoint]) -> MarketStructure:
        """Analyze current market structure."""
        if len(swings) < 4:
            return MarketStructure.UNKNOWN
        
        # Get recent highs and lows
        recent_highs = [s for s in swings[-6:] if s.type == "high"]
        recent_lows = [s for s in swings[-6:] if s.type == "low"]
        
        if len(recent_highs) < 2 or len(recent_lows) < 2:
            return MarketStructure.UNKNOWN
        
        # Check for HH + HL (bullish)
        latest_highs = recent_highs[-2:]
        latest_lows = recent_lows[-2:]
        
        hh = latest_highs[-1].price > latest_highs[-2].price
        hl = latest_lows[-1].price > latest_lows[-2].price
        
        lh = latest_highs[-1].price < latest_highs[-2].price
        ll = latest_lows[-1].price < latest_lows[-2].price
        
        if hh and hl:
            return MarketStructure.BULLISH
        elif lh and ll:
            return MarketStructure.BEARISH
        elif (hl and lh) or (hh and ll):
            return MarketStructure.COMPRESSION
        else:
            return MarketStructure.UNKNOWN
    
    def _detect_structure_change(self, swings: List[SwingPoint]) -> StructureChange:
        """Detect CHoCH and BOS patterns."""
        if len(swings) < 4:
            return StructureChange.NONE
        
        # Analyze last 6 swings for pattern detection
        recent_swings = swings[-6:]
        
        # Look for CHoCH patterns
        choch = self._detect_choch(recent_swings)
        if choch != StructureChange.NONE:
            return choch
        
        # Look for BOS patterns
        bos = self._detect_bos(recent_swings)
        return bos
    
    def _detect_choch(self, swings: List[SwingPoint]) -> StructureChange:
        """Detect Change of Character patterns."""
        if len(swings) < 4:
            return StructureChange.NONE
        
        # Bullish CHoCH: After LLs and LHs, we see HH followed by HL
        highs = [s for s in swings if s.type == "high"]
        lows = [s for s in swings if s.type == "low"]
        
        if len(highs) >= 2 and len(lows) >= 2:
            # Check for bullish CHoCH
            recent_highs = highs[-2:]
            recent_lows = lows[-2:]
            
            # Previous structure was bearish (LH + LL)
            prev_lh = len(highs) >= 3 and highs[-3].price > highs[-2].price
            prev_ll = len(lows) >= 3 and lows[-3].price > lows[-2].price
            
            # Current structure shows HH + HL
            current_hh = recent_highs[-1].price > recent_highs[-2].price
            current_hl = recent_lows[-1].price > recent_lows[-2].price
            
            if prev_lh and prev_ll and current_hh and current_hl:
                return StructureChange.CHOCH_BULLISH
            
            # Check for bearish CHoCH
            prev_hh = len(highs) >= 3 and highs[-3].price < highs[-2].price
            prev_hl = len(lows) >= 3 and lows[-3].price < lows[-2].price
            
            current_lh = recent_highs[-1].price < recent_highs[-2].price
            current_ll = recent_lows[-1].price < recent_lows[-2].price
            
            if prev_hh and prev_hl and current_lh and current_ll:
                return StructureChange.CHOCH_BEARISH
        
        return StructureChange.NONE
    
    def _detect_bos(self, swings: List[SwingPoint]) -> StructureChange:
        """Detect Break of Structure patterns."""
        if len(swings) < 3:
            return StructureChange.NONE
        
        # BOS occurs when price closes beyond a prior swing high/low
        latest_swing = swings[-1]
        
        # Look for bullish BOS (break above prior swing high)
        if latest_swing.type == "high":
            prior_highs = [s for s in swings[:-1] if s.type == "high"]
            if prior_highs:
                highest_prior = max(prior_highs, key=lambda x: x.price)
                if latest_swing.price > highest_prior.price:
                    return StructureChange.BOS_BULLISH
        
        # Look for bearish BOS (break below prior swing low)
        if latest_swing.type == "low":
            prior_lows = [s for s in swings[:-1] if s.type == "low"]
            if prior_lows:
                lowest_prior = min(prior_lows, key=lambda x: x.price)
                if latest_swing.price < lowest_prior.price:
                    return StructureChange.BOS_BEARISH
        
        return StructureChange.NONE
    
    def _find_bos_level(self, swings: List[SwingPoint], 
                       structure_change: StructureChange) -> Optional[float]:
        """Find the most recent BOS level."""
        if structure_change == StructureChange.BOS_BULLISH:
            highs = [s for s in swings if s.type == "high"]
            return highs[-2].price if len(highs) >= 2 else None
        elif structure_change == StructureChange.BOS_BEARISH:
            lows = [s for s in swings if s.type == "low"]
            return lows[-2].price if len(lows) >= 2 else None
        return None
    
    def _analyze_market_context(self, swings: List[SwingPoint], 
                               current_price: float) -> Dict[str, Any]:
        """Analyze current market context."""
        if not swings:
            return {
                'untaken_highs': [],
                'untaken_lows': [],
                'trend_strength': 0.0,
                'structure_quality': 0.0
            }
        
        # Find untaken highs/lows (levels not yet reached)
        untaken_highs = [s.price for s in swings if s.type == "high" and s.price > current_price]
        untaken_lows = [s.price for s in swings if s.type == "low" and s.price < current_price]
        
        # Calculate trend strength based on swing progression
        trend_strength = self._calculate_trend_strength(swings)
        
        # Calculate structure quality based on swing significance
        structure_quality = np.mean([s.significance for s in swings[-6:]]) if swings else 0.0
        
        return {
            'untaken_highs': sorted(untaken_highs, reverse=True)[:3],  # Top 3 nearest
            'untaken_lows': sorted(untaken_lows, reverse=True)[:3],    # Top 3 nearest
            'trend_strength': trend_strength,
            'structure_quality': structure_quality
        }
    
    def _calculate_trend_strength(self, swings: List[SwingPoint]) -> float:
        """Calculate trend strength based on swing progression."""
        if len(swings) < 4:
            return 0.0
        
        # Analyze last 6 swings
        recent_swings = swings[-6:]
        highs = [s for s in recent_swings if s.type == "high"]
        lows = [s for s in recent_swings if s.type == "low"]
        
        if len(highs) < 2 or len(lows) < 2:
            return 0.0
        
        # Calculate price progression strength
        high_progression = (highs[-1].price - highs[-2].price) / highs[-2].price
        low_progression = (lows[-1].price - lows[-2].price) / lows[-2].price
        
        # Combine and normalize
        avg_progression = (high_progression + low_progression) / 2
        trend_strength = min(abs(avg_progression) * 10, 1.0)  # Scale and cap at 1.0
        
        return trend_strength


class SmartMoneyZoneDetector(Indicator):
    """Detects Smart Money Zones using Fibonacci retracements after BOS/CHoCH."""
    
    def __init__(self, structure_detector: MarketStructureDetector = None):
        """
        Initialize SMZ detector.
        
        Args:
            structure_detector: Market structure detector instance
        """
        super().__init__()
        self.structure_detector = structure_detector or MarketStructureDetector()
        self.name = "SmartMoneyZones"
        
        # Fibonacci levels for SMZ
        self.fib_levels = {
            'bos': 0.0,          # Break of Structure level
            'golden_start': 0.618,  # Golden pocket start
            'golden_end': 0.786,    # Golden pocket end / SMZ start
            'optimal_entry': 0.826, # SMZ end / Optimal entry
            'target': 1.0           # Target level
        }
    
    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Calculate Smart Money Zones based on market structure."""
        # First get market structure analysis
        structure_result = self.structure_detector.calculate(price_history)
        
        if structure_result['status'] != 'valid':
            return {'status': structure_result['status']}
        
        try:
            # Extract swing points and structure info
            swing_points = structure_result['swing_points']
            structure_change = StructureChange(structure_result['structure_change'])
            
            # Generate SMZ based on recent BOS/CHoCH
            smz_zones = self._generate_smz_zones(swing_points, structure_change, price_history)
            
            # Analyze current price relative to zones
            current_price = price_history['close'].iloc[-1]
            zone_analysis = self._analyze_current_zones(smz_zones, current_price)
            
            # Calculate zone confluence (multiple zones at similar levels)
            confluence_analysis = self._calculate_zone_confluence(smz_zones)
            
            return {
                'status': 'valid',
                'market_structure': structure_result['market_structure'],
                'structure_change': structure_result['structure_change'],
                'swing_points': swing_points,
                'smz_zones': smz_zones,
                'current_price': current_price,
                'active_zones': zone_analysis['active_zones'],
                'nearest_institutional_zone': zone_analysis['nearest_institutional'],
                'nearest_smz': zone_analysis['nearest_smz'],
                'price_in_zone': zone_analysis['price_in_zone'],
                'zone_confluence': confluence_analysis,
                'untaken_levels': structure_result['untaken_highs'] + structure_result['untaken_lows'],
                'trend_strength': structure_result['trend_strength']
            }
            
        except Exception as e:
            return {'status': f'calculation_error: {str(e)}'}
    
    def _generate_smz_zones(self, swing_points: List[SwingPoint], 
                           structure_change: StructureChange,
                           price_history: pd.DataFrame) -> List[SmartMoneyZone]:
        """Generate Smart Money Zones based on swing points and structure changes."""
        zones = []
        
        if structure_change in [StructureChange.BOS_BULLISH, StructureChange.CHOCH_BULLISH]:
            # Bullish zones: Fib from swing low to swing high
            zones.extend(self._create_bullish_zones(swing_points, price_history))
        
        if structure_change in [StructureChange.BOS_BEARISH, StructureChange.CHOCH_BEARISH]:
            # Bearish zones: Fib from swing high to swing low  
            zones.extend(self._create_bearish_zones(swing_points, price_history))
        
        # Also create zones for recent significant swings regardless of BOS/CHoCH
        zones.extend(self._create_additional_zones(swing_points, price_history))
        
        # Sort by creation time and return most recent/relevant
        zones.sort(key=lambda x: x.created_at, reverse=True)
        return zones[:5]  # Keep top 5 most recent zones
    
    def _create_bullish_zones(self, swing_points: List[SwingPoint], 
                             price_history: pd.DataFrame) -> List[SmartMoneyZone]:
        """Create bullish SMZ zones (Fib from low to high)."""
        zones = []
        
        # Find recent swing low -> high patterns
        lows = [s for s in swing_points if s.type == "low"]
        highs = [s for s in swing_points if s.type == "high"]
        
        if len(lows) >= 1 and len(highs) >= 1:
            # Get most recent low before most recent high
            recent_high = highs[-1]
            potential_lows = [low for low in lows if low.index < recent_high.index]
            
            if potential_lows:
                recent_low = max(potential_lows, key=lambda x: x.index)
                zone = self._create_zone(recent_low, recent_high, "bullish", price_history)
                if zone:
                    zones.append(zone)
        
        return zones
    
    def _create_bearish_zones(self, swing_points: List[SwingPoint], 
                             price_history: pd.DataFrame) -> List[SmartMoneyZone]:
        """Create bearish SMZ zones (Fib from high to low)."""
        zones = []
        
        # Find recent swing high -> low patterns
        highs = [s for s in swing_points if s.type == "high"]
        lows = [s for s in swing_points if s.type == "low"]
        
        if len(highs) >= 1 and len(lows) >= 1:
            # Get most recent high before most recent low
            recent_low = lows[-1]
            potential_highs = [high for high in highs if high.index < recent_low.index]
            
            if potential_highs:
                recent_high = max(potential_highs, key=lambda x: x.index)
                zone = self._create_zone(recent_high, recent_low, "bearish", price_history)
                if zone:
                    zones.append(zone)
        
        return zones
    
    def _create_additional_zones(self, swing_points: List[SwingPoint], 
                                price_history: pd.DataFrame) -> List[SmartMoneyZone]:
        """Create additional zones from significant swings."""
        zones = []
        
        # Look for high-significance swing combinations
        significant_swings = [s for s in swing_points if s.significance > 0.7]
        
        for i in range(len(significant_swings) - 1):
            swing1 = significant_swings[i]
            swing2 = significant_swings[i + 1]
            
            # Create zone if swings are different types
            if swing1.type != swing2.type:
                if swing1.type == "low" and swing2.type == "high":
                    zone = self._create_zone(swing1, swing2, "bullish", price_history)
                else:
                    zone = self._create_zone(swing1, swing2, "bearish", price_history)
                
                if zone:
                    zones.append(zone)
        
        return zones
    
    def _create_zone(self, swing1: SwingPoint, swing2: SwingPoint, 
                    direction: str, price_history: pd.DataFrame) -> Optional[SmartMoneyZone]:
        """Create a Smart Money Zone from two swing points."""
        try:
            # Determine high and low points
            if direction == "bullish":
                low_point, high_point = swing1, swing2
            else:
                high_point, low_point = swing1, swing2
            
            # Calculate Fibonacci levels
            price_range = high_point.price - low_point.price
            
            if direction == "bullish":
                # Bullish: Fib from low to high, zones are retracement levels
                fib_0 = low_point.price  # 0% (start)
                fib_618 = high_point.price - (price_range * 0.618)
                fib_786 = high_point.price - (price_range * 0.786)
                fib_826 = high_point.price - (price_range * 0.826)
                fib_100 = high_point.price  # 100% (target)
            else:
                # Bearish: Fib from high to low, zones are extension levels
                fib_0 = high_point.price  # 0% (start)
                fib_618 = low_point.price + (price_range * 0.618)
                fib_786 = low_point.price + (price_range * 0.786)
                fib_826 = low_point.price + (price_range * 0.826)
                fib_100 = low_point.price  # 100% (target)
            
            # Define institutional and SMZ zones
            institutional_zone = (min(fib_618, fib_786), max(fib_618, fib_786))
            smart_money_zone = (min(fib_786, fib_826), max(fib_786, fib_826))
            
            # Calculate confidence based on swing significance and recency
            confidence = self._calculate_zone_confidence(swing1, swing2, price_history)
            
            zone = SmartMoneyZone(
                swing_high=high_point,
                swing_low=low_point,
                direction=direction,
                fib_0=fib_0,
                fib_618=fib_618,
                fib_786=fib_786,
                fib_826=fib_826,
                fib_100=fib_100,
                institutional_zone=institutional_zone,
                smart_money_zone=smart_money_zone,
                created_at=max(swing1.timestamp, swing2.timestamp),
                timeframe="current",
                confidence=confidence
            )
            
            return zone
            
        except Exception:
            return None
    
    def _calculate_zone_confidence(self, swing1: SwingPoint, swing2: SwingPoint, 
                                  price_history: pd.DataFrame) -> float:
        """Calculate confidence score for a zone."""
        # Base confidence from swing significance
        base_confidence = (swing1.significance + swing2.significance) / 2
        
        # Recency bonus (more recent = higher confidence)
        latest_index = max(swing1.index, swing2.index)
        recency_ratio = latest_index / len(price_history)
        recency_bonus = recency_ratio * 0.3
        
        # Price range significance
        price_range = abs(swing1.price - swing2.price)
        avg_range = (price_history['high'] - price_history['low']).mean()
        range_significance = min((price_range / avg_range) / 5, 0.2)  # Cap at 0.2
        
        confidence = base_confidence + recency_bonus + range_significance
        return min(confidence, 1.0)
    
    def _analyze_current_zones(self, zones: List[SmartMoneyZone], 
                              current_price: float) -> Dict[str, Any]:
        """Analyze current price relative to SMZ zones."""
        active_zones = []
        nearest_institutional = None
        nearest_smz = None
        price_in_zone = None
        
        for zone in zones:
            # Check if price is in institutional zone
            inst_min, inst_max = zone.institutional_zone
            if inst_min <= current_price <= inst_max:
                active_zones.append({
                    'type': 'institutional',
                    'zone': zone,
                    'direction': zone.direction
                })
                price_in_zone = 'institutional'
            
            # Check if price is in SMZ
            smz_min, smz_max = zone.smart_money_zone
            if smz_min <= current_price <= smz_max:
                active_zones.append({
                    'type': 'smart_money',
                    'zone': zone,
                    'direction': zone.direction
                })
                price_in_zone = 'smart_money'
            
            # Find nearest zones
            inst_distance = min(abs(current_price - inst_min), abs(current_price - inst_max))
            smz_distance = min(abs(current_price - smz_min), abs(current_price - smz_max))
            
            if nearest_institutional is None or inst_distance < nearest_institutional['distance']:
                nearest_institutional = {
                    'zone': zone,
                    'distance': inst_distance,
                    'direction': zone.direction
                }
            
            if nearest_smz is None or smz_distance < nearest_smz['distance']:
                nearest_smz = {
                    'zone': zone,
                    'distance': smz_distance,
                    'direction': zone.direction
                }
        
        return {
            'active_zones': active_zones,
            'nearest_institutional': nearest_institutional,
            'nearest_smz': nearest_smz,
            'price_in_zone': price_in_zone
        }
    
    def _calculate_zone_confluence(self, zones: List[SmartMoneyZone]) -> Dict[str, Any]:
        """Calculate confluence between multiple zones."""
        if len(zones) < 2:
            return {'confluence_levels': [], 'max_confluence': 0}
        
        confluence_levels = []
        price_tolerance = 0.005  # 0.5% tolerance for confluence
        
        # Check institutional zones confluence
        institutional_levels = []
        for zone in zones:
            institutional_levels.extend([zone.institutional_zone[0], zone.institutional_zone[1]])
        
        # Check SMZ confluence
        smz_levels = []
        for zone in zones:
            smz_levels.extend([zone.smart_money_zone[0], zone.smart_money_zone[1]])
        
        # Find confluent levels
        all_levels = institutional_levels + smz_levels
        confluence_count = {}
        
        for level in all_levels:
            confluent_levels = [l for l in all_levels if abs(l - level) / level <= price_tolerance]
            if len(confluent_levels) > 1:
                confluence_count[level] = len(confluent_levels)
        
        # Sort by confluence strength
        if confluence_count:
            max_confluence = max(confluence_count.values())
            confluence_levels = sorted(confluence_count.items(), key=lambda x: x[1], reverse=True)
        else:
            max_confluence = 0
        
        return {
            'confluence_levels': confluence_levels[:5],  # Top 5
            'max_confluence': max_confluence
        }


class SMZEntryConfirmation(Indicator):
    """Entry confirmation system for Smart Money Zones trades."""
    
    def __init__(self, smz_detector: SmartMoneyZoneDetector = None,
                 confirmation_bars: int = 3, volume_threshold: float = 1.5):
        """
        Initialize SMZ entry confirmation system.
        
        Args:
            smz_detector: Smart Money Zone detector instance
            confirmation_bars: Number of bars for entry confirmation
            volume_threshold: Minimum volume multiplier for confirmation
        """
        super().__init__()
        self.smz_detector = smz_detector or SmartMoneyZoneDetector()
        self.confirmation_bars = confirmation_bars
        self.volume_threshold = volume_threshold
        self.name = "SMZEntryConfirmation"
    
    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Generate entry signals based on SMZ methodology."""
        # Get SMZ analysis
        smz_result = self.smz_detector.calculate(price_history)
        
        if smz_result['status'] != 'valid':
            return {'status': smz_result['status']}
        
        try:
            # Extract current market state
            current_price = smz_result['current_price']
            active_zones = smz_result['active_zones']
            market_structure = MarketStructure(smz_result['market_structure'])
            structure_change = StructureChange(smz_result['structure_change'])
            
            # Generate entry signals
            entry_signals = self._generate_entry_signals(
                price_history, current_price, active_zones, 
                market_structure, structure_change
            )
            
            # Validate signals with confirmation criteria
            validated_signals = self._validate_entry_signals(
                entry_signals, price_history
            )
            
            # Calculate risk management levels
            risk_levels = self._calculate_risk_levels(
                validated_signals, smz_result['smz_zones']
            )
            
            return {
                'status': 'valid',
                'market_structure': smz_result['market_structure'],
                'structure_change': smz_result['structure_change'],
                'active_zones': active_zones,
                'entry_signals': validated_signals,
                'risk_levels': risk_levels,
                'current_price': current_price,
                'zone_confluence': smz_result['zone_confluence'],
                'confirmation_criteria': self._get_confirmation_criteria()
            }
            
        except Exception as e:
            return {'status': f'calculation_error: {str(e)}'}
    
    def _generate_entry_signals(self, price_history: pd.DataFrame, 
                               current_price: float, active_zones: List[Dict],
                               market_structure: MarketStructure,
                               structure_change: StructureChange) -> List[Dict[str, Any]]:
        """Generate potential entry signals based on SMZ methodology."""
        signals = []
        
        # Signal 1: Price in SMZ with structure confirmation
        if active_zones:
            for zone_info in active_zones:
                if zone_info['type'] == 'smart_money':
                    zone = zone_info['zone']
                    direction = zone_info['direction']
                    
                    # Check if structure supports the trade direction
                    structure_alignment = self._check_structure_alignment(
                        direction, market_structure, structure_change
                    )
                    
                    if structure_alignment['aligned']:
                        signals.append({
                            'type': 'smz_entry',
                            'direction': direction,
                            'entry_price': current_price,
                            'zone': zone,
                            'confidence': structure_alignment['confidence'],
                            'reason': f'Price in {direction} SMZ with {structure_alignment["reason"]}'
                        })
        
        # Signal 2: Rejection from institutional zone
        rejection_signals = self._detect_zone_rejections(price_history, active_zones)
        signals.extend(rejection_signals)
        
        # Signal 3: BOS/CHoCH confirmation entries
        structure_signals = self._detect_structure_entries(
            price_history, structure_change
        )
        signals.extend(structure_signals)
        
        return signals
    
    def _check_structure_alignment(self, direction: str, 
                                  market_structure: MarketStructure,
                                  structure_change: StructureChange) -> Dict[str, Any]:
        """Check if market structure aligns with trade direction."""
        confidence = 0.0
        aligned = False
        reason = ""
        
        # Bullish alignment checks
        if direction == "bullish":
            if market_structure == MarketStructure.BULLISH:
                aligned = True
                confidence += 0.4
                reason += "Bullish market structure (HH+HL) "
            
            if structure_change in [StructureChange.BOS_BULLISH, StructureChange.CHOCH_BULLISH]:
                aligned = True
                confidence += 0.6
                reason += f"Recent {structure_change.value} "
        
        # Bearish alignment checks
        elif direction == "bearish":
            if market_structure == MarketStructure.BEARISH:
                aligned = True
                confidence += 0.4
                reason += "Bearish market structure (LH+LL) "
            
            if structure_change in [StructureChange.BOS_BEARISH, StructureChange.CHOCH_BEARISH]:
                aligned = True
                confidence += 0.6
                reason += f"Recent {structure_change.value} "
        
        return {
            'aligned': aligned,
            'confidence': min(confidence, 1.0),
            'reason': reason.strip()
        }
    
    def _detect_zone_rejections(self, price_history: pd.DataFrame,
                               active_zones: List[Dict]) -> List[Dict[str, Any]]:
        """Detect rejections from institutional zones."""
        signals = []
        
        if len(price_history) < self.confirmation_bars:
            return signals
        
        recent_bars = price_history.tail(self.confirmation_bars)
        
        for zone_info in active_zones:
            if zone_info['type'] == 'institutional':
                zone = zone_info['zone']
                direction = zone_info['direction']
                
                # Check for rejection pattern
                rejection = self._analyze_zone_rejection(
                    recent_bars, zone, direction
                )
                
                if rejection['detected']:
                    signals.append({
                        'type': 'zone_rejection',
                        'direction': direction,
                        'entry_price': recent_bars['close'].iloc[-1],
                        'zone': zone,
                        'confidence': rejection['confidence'],
                        'reason': f'Rejection from institutional zone: {rejection["pattern"]}'
                    })
        
        return signals
    
    def _analyze_zone_rejection(self, recent_bars: pd.DataFrame,
                               zone: SmartMoneyZone, direction: str) -> Dict[str, Any]:
        """Analyze price action for zone rejection patterns."""
        inst_min, inst_max = zone.institutional_zone
        
        # Look for wick rejections and volume confirmation
        rejection_detected = False
        confidence = 0.0
        pattern = ""
        
        if direction == "bullish":
            # Look for rejection from lower institutional zone
            for i, (idx, bar) in enumerate(recent_bars.iterrows()):
                if bar['low'] <= inst_min and bar['close'] > inst_min:
                    # Wick rejection pattern
                    wick_size = inst_min - bar['low']
                    body_size = abs(bar['close'] - bar['open'])
                    
                    if wick_size > body_size * 0.5:  # Significant wick
                        rejection_detected = True
                        confidence += 0.4
                        pattern += "Lower wick rejection "
                    
                    # Volume confirmation
                    if 'volume' in recent_bars.columns:
                        avg_volume = recent_bars['volume'].mean()
                        if bar['volume'] > avg_volume * self.volume_threshold:
                            confidence += 0.3
                            pattern += "with volume confirmation"
        
        elif direction == "bearish":
            # Look for rejection from upper institutional zone
            for i, (idx, bar) in enumerate(recent_bars.iterrows()):
                if bar['high'] >= inst_max and bar['close'] < inst_max:
                    # Wick rejection pattern
                    wick_size = bar['high'] - inst_max
                    body_size = abs(bar['close'] - bar['open'])
                    
                    if wick_size > body_size * 0.5:  # Significant wick
                        rejection_detected = True
                        confidence += 0.4
                        pattern += "Upper wick rejection "
                    
                    # Volume confirmation
                    if 'volume' in recent_bars.columns:
                        avg_volume = recent_bars['volume'].mean()
                        if bar['volume'] > avg_volume * self.volume_threshold:
                            confidence += 0.3
                            pattern += "with volume confirmation"
        
        return {
            'detected': rejection_detected,
            'confidence': min(confidence, 1.0),
            'pattern': pattern.strip()
        }
    
    def _detect_structure_entries(self, price_history: pd.DataFrame,
                                 structure_change: StructureChange) -> List[Dict[str, Any]]:
        """Detect entries based on structure changes."""
        signals = []
        
        if structure_change == StructureChange.NONE:
            return signals
        
        current_price = price_history['close'].iloc[-1]
        
        # BOS confirmation entries
        if structure_change in [StructureChange.BOS_BULLISH, StructureChange.BOS_BEARISH]:
            direction = "bullish" if structure_change == StructureChange.BOS_BULLISH else "bearish"
            
            signals.append({
                'type': 'bos_entry',
                'direction': direction,
                'entry_price': current_price,
                'zone': None,
                'confidence': 0.7,
                'reason': f'Break of Structure {direction} confirmation'
            })
        
        # CHoCH confirmation entries
        if structure_change in [StructureChange.CHOCH_BULLISH, StructureChange.CHOCH_BEARISH]:
            direction = "bullish" if structure_change == StructureChange.CHOCH_BULLISH else "bearish"
            
            signals.append({
                'type': 'choch_entry',
                'direction': direction,
                'entry_price': current_price,
                'zone': None,
                'confidence': 0.8,
                'reason': f'Change of Character {direction} confirmation'
            })
        
        return signals
    
    def _validate_entry_signals(self, signals: List[Dict[str, Any]],
                               price_history: pd.DataFrame) -> List[Dict[str, Any]]:
        """Validate entry signals with additional confirmation criteria."""
        validated_signals = []
        
        for signal in signals:
            validation_score = 0.0
            validation_reasons = []
            
            # Volume confirmation
            if 'volume' in price_history.columns and len(price_history) >= 10:
                recent_volume = price_history['volume'].tail(3).mean()
                avg_volume = price_history['volume'].tail(20).mean()
                
                if recent_volume > avg_volume * self.volume_threshold:
                    validation_score += 0.3
                    validation_reasons.append("Volume confirmation")
            
            # Price action confirmation
            if len(price_history) >= self.confirmation_bars:
                recent_bars = price_history.tail(self.confirmation_bars)
                
                # Check for consistent direction
                if signal['direction'] == "bullish":
                    bullish_bars = sum(1 for _, bar in recent_bars.iterrows() 
                                     if bar['close'] > bar['open'])
                    if bullish_bars >= self.confirmation_bars * 0.6:
                        validation_score += 0.2
                        validation_reasons.append("Bullish price action")
                
                elif signal['direction'] == "bearish":
                    bearish_bars = sum(1 for _, bar in recent_bars.iterrows() 
                                     if bar['close'] < bar['open'])
                    if bearish_bars >= self.confirmation_bars * 0.6:
                        validation_score += 0.2
                        validation_reasons.append("Bearish price action")
            
            # Update signal with validation
            signal['validation_score'] = validation_score
            signal['validation_reasons'] = validation_reasons
            signal['total_confidence'] = min(
                (signal['confidence'] + validation_score) / 2, 1.0
            )
            
            # Only include signals with minimum confidence
            if signal['total_confidence'] >= 0.5:
                validated_signals.append(signal)
        
        # Sort by confidence
        validated_signals.sort(key=lambda x: x['total_confidence'], reverse=True)
        
        return validated_signals
    
    def _calculate_risk_levels(self, signals: List[Dict[str, Any]],
                              zones: List[SmartMoneyZone]) -> Dict[str, Any]:
        """Calculate stop loss and take profit levels for signals."""
        risk_levels = {}
        
        for i, signal in enumerate(signals):
            signal_id = f"signal_{i}"
            direction = signal['direction']
            entry_price = signal['entry_price']
            
            # Calculate stop loss
            if signal['zone']:
                zone = signal['zone']
                if direction == "bullish":
                    # Stop below SMZ
                    stop_loss = zone.smart_money_zone[0] * 0.998  # Small buffer
                else:
                    # Stop above SMZ
                    stop_loss = zone.smart_money_zone[1] * 1.002  # Small buffer
            else:
                # Default stop based on ATR or percentage
                stop_distance = entry_price * 0.01  # 1% default
                if direction == "bullish":
                    stop_loss = entry_price - stop_distance
                else:
                    stop_loss = entry_price + stop_distance
            
            # Calculate take profit (1:2 or 1:3 risk-reward)
            risk_amount = abs(entry_price - stop_loss)
            
            if direction == "bullish":
                take_profit_1 = entry_price + (risk_amount * 2)  # 1:2 RR
                take_profit_2 = entry_price + (risk_amount * 3)  # 1:3 RR
            else:
                take_profit_1 = entry_price - (risk_amount * 2)  # 1:2 RR
                take_profit_2 = entry_price - (risk_amount * 3)  # 1:3 RR
            
            risk_levels[signal_id] = {
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'take_profit_1': take_profit_1,
                'take_profit_2': take_profit_2,
                'risk_reward_1': 2.0,
                'risk_reward_2': 3.0,
                'risk_amount': risk_amount
            }
        
        return risk_levels
    
    def _get_confirmation_criteria(self) -> Dict[str, Any]:
        """Get current confirmation criteria settings."""
        return {
            'confirmation_bars': self.confirmation_bars,
            'volume_threshold': self.volume_threshold,
            'minimum_confidence': 0.5,
            'risk_reward_targets': [2.0, 3.0]
        }


class MultiTimeframeAnalysis:
    """Multi-timeframe confluence analysis for Smart Money Zones."""
    
    def __init__(self, timeframes: List[str] = None):
        """
        Initialize multi-timeframe analysis.
        
        Args:
            timeframes: List of timeframe strings (e.g., ['1m', '5m', '15m', '1h'])
        """
        self.timeframes = timeframes or ['5m', '15m', '1h', '4h']
        self.detectors = {tf: SmartMoneyZoneDetector() for tf in self.timeframes}
    
    def analyze_confluence(self, price_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Analyze confluence across multiple timeframes."""
        confluence_results = {}
        
        for timeframe in self.timeframes:
            if timeframe in price_data:
                detector = self.detectors[timeframe]
                result = detector.calculate(price_data[timeframe])
                confluence_results[timeframe] = result
        
        # Calculate overall confluence
        overall_confluence = self._calculate_overall_confluence(confluence_results)
        
        return {
            'timeframe_results': confluence_results,
            'overall_confluence': overall_confluence,
            'dominant_structure': overall_confluence.get('dominant_structure'),
            'confluence_zones': overall_confluence.get('confluence_zones', []),
            'confluence_score': overall_confluence.get('confluence_score', 0.0)
        }
    
    def _calculate_overall_confluence(self, results: Dict[str, Dict]) -> Dict[str, Any]:
        """Calculate overall confluence from multiple timeframes."""
        valid_results = {tf: result for tf, result in results.items() 
                        if result.get('status') == 'valid'}
        
        if not valid_results:
            return {'confluence_score': 0.0}
        
        # Analyze structure alignment
        structures = [result['market_structure'] for result in valid_results.values()]
        structure_counts = {}
        for structure in structures:
            structure_counts[structure] = structure_counts.get(structure, 0) + 1
        
        dominant_structure = max(structure_counts.items(), key=lambda x: x[1])[0]
        structure_alignment = structure_counts[dominant_structure] / len(valid_results)
        
        # Find confluent zones
        confluence_zones = self._find_confluent_zones(valid_results)
        
        # Calculate overall confidence
        avg_trend_strength = np.mean([
            result.get('trend_strength', 0) for result in valid_results.values()
        ])
        
        confluence_score = (
            structure_alignment * 0.4 +
            len(confluence_zones) / 5 * 0.3 +  # Max 5 zones
            avg_trend_strength * 0.3
        )
        
        return {
            'dominant_structure': dominant_structure,
            'structure_alignment': structure_alignment,
            'confluence_zones': confluence_zones,
            'confluence_score': min(confluence_score, 1.0),
            'avg_trend_strength': avg_trend_strength
        }
    
    def _find_confluent_zones(self, results: Dict[str, Dict]) -> List[Dict[str, Any]]:
        """Find zones that appear across multiple timeframes."""
        all_zones = []
        
        for timeframe, result in results.items():
            zones = result.get('smz_zones', [])
            for zone in zones:
                all_zones.append({
                    'timeframe': timeframe,
                    'zone': zone,
                    'institutional_zone': zone.institutional_zone,
                    'smart_money_zone': zone.smart_money_zone
                })
        
        # Find overlapping zones
        confluence_zones = []
        tolerance = 0.01  # 1% price tolerance
        
        for i, zone1 in enumerate(all_zones):
            confluent_timeframes = [zone1['timeframe']]
            
            for j, zone2 in enumerate(all_zones[i+1:], i+1):
                # Check if zones overlap
                if self._zones_overlap(zone1['zone'], zone2['zone'], tolerance):
                    confluent_timeframes.append(zone2['timeframe'])
            
            if len(confluent_timeframes) >= 2:  # At least 2 timeframes
                confluence_zones.append({
                    'zone': zone1['zone'],
                    'timeframes': confluent_timeframes,
                    'confluence_strength': len(confluent_timeframes)
                })
        
        # Remove duplicates and sort by strength
        unique_zones = []
        for cz in confluence_zones:
            is_duplicate = False
            for uz in unique_zones:
                if self._zones_overlap(cz['zone'], uz['zone'], tolerance):
                    is_duplicate = True
                    break
            if not is_duplicate:
                unique_zones.append(cz)
        
        unique_zones.sort(key=lambda x: x['confluence_strength'], reverse=True)
        return unique_zones[:5]  # Top 5 confluent zones
    
    def _zones_overlap(self, zone1: SmartMoneyZone, zone2: SmartMoneyZone, 
                      tolerance: float) -> bool:
        """Check if two zones overlap within tolerance."""
        # Check institutional zone overlap
        inst1_min, inst1_max = zone1.institutional_zone
        inst2_min, inst2_max = zone2.institutional_zone
        
        inst_overlap = not (inst1_max < inst2_min * (1 - tolerance) or 
                           inst2_max < inst1_min * (1 - tolerance))
        
        # Check SMZ overlap
        smz1_min, smz1_max = zone1.smart_money_zone
        smz2_min, smz2_max = zone2.smart_money_zone
        
        smz_overlap = not (smz1_max < smz2_min * (1 - tolerance) or 
                          smz2_max < smz1_min * (1 - tolerance))
        
        return inst_overlap or smz_overlap