"""
Event Sequence Feature Extractor for Residual Return Prediction.
Extracts dynamic price sequences around events as model input features.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
import asyncpg
from state.universe_state_manager import UniverseStateManager

logger = logging.getLogger(__name__)


@dataclass
class EventPattern:
    """Historical event reaction pattern."""
    event_type: str
    importance: str
    avg_reaction: float
    volatility_spike: float
    volume_surge: float
    duration_days: int
    confidence: float
    sample_size: int


@dataclass
class EventFeatures:
    """Event-driven features for a specific date and instrument."""
    instrument_id: int
    date: datetime
    upcoming_events: List[Dict[str, Any]]
    historical_patterns: Dict[str, EventPattern]
    pre_event_sequences: Dict[str, np.ndarray]
    event_proximity_score: float
    event_importance_weighted_score: float


class EventCalendar:
    """Manages economic and company events calendar."""
    
    def __init__(self, connection_pool: asyncpg.Pool, env):
        self.pool = connection_pool
        self.env = env
    
    async def get_upcoming_events(self, current_date: datetime, 
                                instrument_id: Optional[int] = None,
                                days_ahead: int = 5) -> List[Dict[str, Any]]:
        """Get upcoming events within specified days."""
        end_date = current_date + timedelta(days=days_ahead)
        events = []
        
        # Get economic events
        economic_events = await self._get_economic_events(current_date, end_date)
        events.extend(economic_events)
        
        # Get company-specific events (earnings, etc.)
        if instrument_id:
            company_events = await self._get_company_events(
                instrument_id, current_date, end_date
            )
            events.extend(company_events)
        
        # Get options expiration dates
        options_events = await self._get_options_expirations(current_date, end_date)
        events.extend(options_events)
        
        # Get month/quarter end dates
        calendar_events = self._get_calendar_events(current_date, end_date)
        events.extend(calendar_events)
        
        return sorted(events, key=lambda x: x['event_date'])
    
    async def _get_economic_events(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get economic events from database."""
        try:
            # Check if economic events table exists
            events_table = self.env.get_table_name('economic_events')
            
            async with self.pool.acquire() as conn:
                # Check table existence
                table_exists = await conn.fetchval(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = $1
                    )
                """, events_table.split('.')[-1])
                
                if not table_exists:
                    return []
                
                rows = await conn.fetch(f"""
                    SELECT 
                        event_date,
                        event_name,
                        event_type,
                        importance,
                        country,
                        actual_value,
                        forecast_value,
                        previous_value
                    FROM {events_table}
                    WHERE event_date BETWEEN $1 AND $2
                    AND importance IN ('High', 'Medium')
                    ORDER BY event_date, importance DESC
                """, start_date.date(), end_date.date())
                
                events = []
                for row in rows:
                    events.append({
                        'event_date': row['event_date'],
                        'event_name': row['event_name'],
                        'type': 'economic',
                        'subtype': row['event_type'],
                        'importance': row['importance'],
                        'metadata': {
                            'country': row['country'],
                            'actual': row['actual_value'],
                            'forecast': row['forecast_value'],
                            'previous': row['previous_value']
                        }
                    })
                
                return events
                
        except Exception as e:
            logger.warning(f"Failed to get economic events: {e}")
            return []
    
    async def _get_company_events(self, instrument_id: int, start_date: datetime, 
                                end_date: datetime) -> List[Dict[str, Any]]:
        """Get company-specific events (earnings, etc.)."""
        try:
            # Check for company events table
            company_events_table = self.env.get_table_name('company_events')
            
            async with self.pool.acquire() as conn:
                table_exists = await conn.fetchval(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = $1
                    )
                """, company_events_table.split('.')[-1])
                
                if not table_exists:
                    return []
                
                rows = await conn.fetch(f"""
                    SELECT 
                        event_date,
                        event_type,
                        event_description,
                        importance
                    FROM {company_events_table}
                    WHERE instrument_id = $1
                    AND event_date BETWEEN $2 AND $3
                    ORDER BY event_date
                """, instrument_id, start_date.date(), end_date.date())
                
                events = []
                for row in rows:
                    events.append({
                        'event_date': row['event_date'],
                        'event_name': row['event_description'],
                        'type': 'company',
                        'subtype': row['event_type'],
                        'importance': row['importance'],
                        'instrument_id': instrument_id,
                        'metadata': {}
                    })
                
                return events
                
        except Exception as e:
            logger.warning(f"Failed to get company events for {instrument_id}: {e}")
            return []
    
    async def _get_options_expirations(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get options expiration dates."""
        events = []
        
        # Options typically expire on the third Friday of each month
        current = start_date.replace(day=1)
        while current <= end_date + timedelta(days=31):
            # Find third Friday
            first_day = current.replace(day=1)
            first_friday = first_day + timedelta(days=(4 - first_day.weekday()) % 7)
            third_friday = first_friday + timedelta(days=14)
            
            if start_date.date() <= third_friday.date() <= end_date.date():
                events.append({
                    'event_date': third_friday.date(),
                    'event_name': 'Options Expiration',
                    'type': 'options_expiration',
                    'subtype': 'monthly',
                    'importance': 'Medium',
                    'metadata': {'expiration_type': 'monthly'}
                })
            
            # Move to next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        
        # Quarterly expirations (more important)
        quarterly_months = [3, 6, 9, 12]
        for event in events:
            if event['event_date'].month in quarterly_months:
                event['importance'] = 'High'
                event['metadata']['expiration_type'] = 'quarterly'
        
        return events
    
    def _get_calendar_events(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get calendar events (month end, quarter end)."""
        events = []
        
        current = start_date
        while current <= end_date:
            # Month end
            if current.day >= 28:  # Last few days of month
                last_day = (current.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                if current.date() == last_day.date():
                    importance = 'High' if current.month in [3, 6, 9, 12] else 'Medium'
                    event_type = 'quarter_end' if current.month in [3, 6, 9, 12] else 'month_end'
                    
                    events.append({
                        'event_date': current.date(),
                        'event_name': f'{event_type.replace("_", " ").title()}',
                        'type': event_type,
                        'subtype': 'calendar',
                        'importance': importance,
                        'metadata': {'quarter': (current.month - 1) // 3 + 1}
                    })
            
            current += timedelta(days=1)
        
        return events


class EventSequenceExtractor:
    """Extract price sequences around events as model features."""
    
    def __init__(self, universe_state_manager: UniverseStateManager,
                 event_calendar: EventCalendar,
                 lookback_days: int = 5,
                 forward_days: int = 2):
        self.universe_state_manager = universe_state_manager
        self.event_calendar = event_calendar
        self.lookback_days = lookback_days
        self.forward_days = forward_days
        
        # Cache for historical patterns
        self._pattern_cache = {}
    
    async def extract_event_features(self, current_date: datetime, 
                                   instrument_id: int) -> EventFeatures:
        """Extract comprehensive event features for a specific date and instrument."""
        try:
            # Get upcoming events
            upcoming_events = await self.event_calendar.get_upcoming_events(
                current_date, instrument_id, days_ahead=self.lookback_days
            )
            
            # Get historical patterns for each event type
            historical_patterns = {}
            for event in upcoming_events:
                event_type = event['type']
                if event_type not in historical_patterns:
                    pattern = await self._get_historical_event_pattern(
                        event_type, event.get('importance', 'Medium'), instrument_id
                    )
                    historical_patterns[event_type] = pattern
            
            # Extract pre-event price sequences
            pre_event_sequences = self._extract_pre_event_sequences(
                current_date, instrument_id, upcoming_events
            )
            
            # Calculate event scores
            proximity_score = self._calculate_event_proximity_score(upcoming_events, current_date)
            importance_score = self._calculate_importance_weighted_score(upcoming_events, current_date)
            
            return EventFeatures(
                instrument_id=instrument_id,
                date=current_date,
                upcoming_events=upcoming_events,
                historical_patterns=historical_patterns,
                pre_event_sequences=pre_event_sequences,
                event_proximity_score=proximity_score,
                event_importance_weighted_score=importance_score
            )
            
        except Exception as e:
            logger.warning(f"Failed to extract event features for {instrument_id} on {current_date}: {e}")
            return EventFeatures(
                instrument_id=instrument_id,
                date=current_date,
                upcoming_events=[],
                historical_patterns={},
                pre_event_sequences={},
                event_proximity_score=0.0,
                event_importance_weighted_score=0.0
            )
    
    async def _get_historical_event_pattern(self, event_type: str, importance: str,
                                          instrument_id: int) -> EventPattern:
        """Analyze historical price reactions to similar events."""
        cache_key = f"{event_type}_{importance}_{instrument_id}"
        
        if cache_key in self._pattern_cache:
            return self._pattern_cache[cache_key]
        
        try:
            # Get historical events of same type
            historical_events = await self._get_historical_events(
                event_type, importance, lookback_months=24
            )
            
            if not historical_events:
                return self._create_default_pattern(event_type, importance)
            
            # Analyze price reactions
            reactions = []
            for event in historical_events:
                reaction = await self._analyze_event_reaction(
                    instrument_id, event['event_date']
                )
                if reaction:
                    reactions.append(reaction)
            
            if not reactions:
                return self._create_default_pattern(event_type, importance)
            
            # Calculate pattern statistics
            avg_reaction = np.mean([r['price_change'] for r in reactions])
            volatility_spike = np.mean([r['volatility_change'] for r in reactions])
            volume_surge = np.mean([r['volume_change'] for r in reactions])
            duration = int(np.mean([r['duration'] for r in reactions]))
            
            pattern = EventPattern(
                event_type=event_type,
                importance=importance,
                avg_reaction=avg_reaction,
                volatility_spike=volatility_spike,
                volume_surge=volume_surge,
                duration_days=duration,
                confidence=min(len(reactions) / 10.0, 1.0),  # More samples = higher confidence
                sample_size=len(reactions)
            )
            
            # Cache result
            self._pattern_cache[cache_key] = pattern
            return pattern
            
        except Exception as e:
            logger.warning(f"Failed to get historical pattern for {event_type}: {e}")
            return self._create_default_pattern(event_type, importance)
    
    async def _get_historical_events(self, event_type: str, importance: str,
                                   lookback_months: int = 24) -> List[Dict[str, Any]]:
        """Get historical events of the same type."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_months * 30)
        
        # Use event calendar to get historical events
        try:
            all_events = await self.event_calendar.get_upcoming_events(
                start_date, days_ahead=lookback_months * 30
            )
            
            # Filter by type and importance
            filtered_events = [
                event for event in all_events
                if event['type'] == event_type and event.get('importance') == importance
            ]
            
            return filtered_events[:50]  # Limit to most recent 50 events
            
        except Exception as e:
            logger.warning(f"Failed to get historical events: {e}")
            return []
    
    async def _analyze_event_reaction(self, instrument_id: int, 
                                    event_date: datetime) -> Optional[Dict[str, float]]:
        """Analyze price reaction around a specific historical event."""
        try:
            # Get price data around event
            start_date = event_date - timedelta(days=5)
            end_date = event_date + timedelta(days=5)
            
            # Get prices before event
            pre_event_data = self.universe_state_manager.get_lag_prices(
                instrument_id, event_date, 5
            )
            
            # Get prices after event
            post_event_data = self.universe_state_manager.get_lead_prices(
                instrument_id, event_date, 5
            )
            
            if pre_event_data.empty or post_event_data.empty:
                return None
            
            # Calculate reaction metrics
            pre_close = pre_event_data['close'].iloc[-1] if 'close' in pre_event_data.columns else pre_event_data['low'].iloc[-1]
            
            # Price change (max gain/loss in following days)
            if 'high' in post_event_data.columns and 'low' in post_event_data.columns:
                max_gain = (post_event_data['high'].max() / pre_close) - 1
                max_loss = (post_event_data['low'].min() / pre_close) - 1
                price_change = max_gain if abs(max_gain) > abs(max_loss) else max_loss
            else:
                price_change = 0
            
            # Volatility change
            if len(pre_event_data) >= 3 and len(post_event_data) >= 3:
                pre_vol = pre_event_data['close'].pct_change().std() if 'close' in pre_event_data.columns else 0
                post_vol = post_event_data['high'].pct_change().std() if 'high' in post_event_data.columns else 0
                volatility_change = (post_vol / pre_vol) - 1 if pre_vol > 0 else 0
            else:
                volatility_change = 0
            
            # Volume change (if available)
            volume_change = 1.0  # Default if no volume data
            
            return {
                'price_change': price_change,
                'volatility_change': volatility_change,
                'volume_change': volume_change,
                'duration': 3  # Simplified duration
            }
            
        except Exception as e:
            logger.warning(f"Failed to analyze event reaction for {instrument_id}: {e}")
            return None
    
    def _create_default_pattern(self, event_type: str, importance: str) -> EventPattern:
        """Create default pattern when no historical data available."""
        # Default patterns based on event type and importance
        default_reactions = {
            ('economic', 'High'): {'reaction': 0.02, 'vol_spike': 1.5, 'volume': 1.3},
            ('economic', 'Medium'): {'reaction': 0.01, 'vol_spike': 1.2, 'volume': 1.1},
            ('earnings', 'High'): {'reaction': 0.05, 'vol_spike': 2.0, 'volume': 2.0},
            ('options_expiration', 'Medium'): {'reaction': 0.005, 'vol_spike': 1.1, 'volume': 0.9},
            ('quarter_end', 'High'): {'reaction': 0.01, 'vol_spike': 1.3, 'volume': 1.2},
            ('month_end', 'Medium'): {'reaction': 0.005, 'vol_spike': 1.1, 'volume': 1.0}
        }
        
        key = (event_type, importance)
        defaults = default_reactions.get(key, {'reaction': 0.01, 'vol_spike': 1.2, 'volume': 1.1})
        
        return EventPattern(
            event_type=event_type,
            importance=importance,
            avg_reaction=defaults['reaction'],
            volatility_spike=defaults['vol_spike'],
            volume_surge=defaults['volume'],
            duration_days=3,
            confidence=0.3,  # Low confidence for defaults
            sample_size=0
        )
    
    def _extract_pre_event_sequences(self, current_date: datetime, instrument_id: int,
                                   upcoming_events: List[Dict[str, Any]]) -> Dict[str, np.ndarray]:
        """Extract OHLCV sequences leading up to upcoming events."""
        sequences = {}
        
        for event in upcoming_events:
            event_type = event['type']
            days_until = (event['event_date'] - current_date.date()).days
            
            if days_until <= self.lookback_days:
                try:
                    # Get recent price data
                    price_data = self.universe_state_manager.get_lag_prices(
                        instrument_id, current_date, self.lookback_days
                    )
                    
                    if not price_data.empty:
                        # Create sequence array
                        if 'open' in price_data.columns:
                            sequence = price_data[['open', 'high', 'low', 'close']].values
                        else:
                            # If only high/low available, duplicate for OHLC
                            high_low = price_data[['high', 'low']].values
                            sequence = np.column_stack([
                                high_low[:, 1],  # open = low
                                high_low[:, 0],  # high
                                high_low[:, 1],  # low  
                                high_low[:, 0]   # close = high
                            ])
                        
                        # Normalize sequence (percentage changes)
                        if len(sequence) > 1:
                            normalized_sequence = np.diff(sequence, axis=0) / sequence[:-1]
                            sequences[f'{event_type}_sequence'] = normalized_sequence.flatten()
                        
                except Exception as e:
                    logger.warning(f"Failed to extract sequence for {event_type}: {e}")
        
        return sequences
    
    def _calculate_event_proximity_score(self, upcoming_events: List[Dict[str, Any]], 
                                       current_date: datetime) -> float:
        """Calculate proximity score based on upcoming events."""
        if not upcoming_events:
            return 0.0
        
        proximity_scores = []
        for event in upcoming_events:
            days_until = (event['event_date'] - current_date.date()).days
            
            # Exponential decay based on days until event
            proximity = np.exp(-days_until / 3.0)  # Decay with 3-day half-life
            proximity_scores.append(proximity)
        
        return max(proximity_scores) if proximity_scores else 0.0
    
    def _calculate_importance_weighted_score(self, upcoming_events: List[Dict[str, Any]],
                                           current_date: datetime) -> float:
        """Calculate importance-weighted event score."""
        if not upcoming_events:
            return 0.0
        
        importance_weights = {'High': 1.0, 'Medium': 0.6, 'Low': 0.3}
        weighted_scores = []
        
        for event in upcoming_events:
            days_until = (event['event_date'] - current_date.date()).days
            importance = event.get('importance', 'Medium')
            
            # Combine proximity and importance
            proximity = np.exp(-days_until / 3.0)
            weight = importance_weights.get(importance, 0.5)
            
            weighted_score = proximity * weight
            weighted_scores.append(weighted_score)
        
        return sum(weighted_scores)


def flatten_event_features_for_model(event_features: EventFeatures) -> Dict[str, Any]:
    """
    Flatten event features into a dictionary suitable for model input.
    
    Returns:
        Dictionary with keys like:
        - event_proximity_score
        - event_importance_weighted_score  
        - earnings_avg_reaction
        - economic_volatility_spike
        - options_expiration_sequence_0, options_expiration_sequence_1, ...
    """
    features = {
        'event_proximity_score': event_features.event_proximity_score,
        'event_importance_weighted_score': event_features.event_importance_weighted_score
    }
    
    # Add historical pattern features
    for event_type, pattern in event_features.historical_patterns.items():
        prefix = event_type
        features[f'{prefix}_avg_reaction'] = pattern.avg_reaction
        features[f'{prefix}_volatility_spike'] = pattern.volatility_spike
        features[f'{prefix}_volume_surge'] = pattern.volume_surge
        features[f'{prefix}_confidence'] = pattern.confidence
        features[f'{prefix}_sample_size'] = pattern.sample_size
    
    # Add sequence features (flattened)
    for sequence_name, sequence_data in event_features.pre_event_sequences.items():
        if isinstance(sequence_data, np.ndarray):
            for i, value in enumerate(sequence_data[:20]):  # Limit to first 20 values
                features[f'{sequence_name}_{i}'] = value
    
    # Add event counts by type and importance
    event_counts = {}
    for event in event_features.upcoming_events:
        event_type = event['type']
        importance = event.get('importance', 'Medium')
        
        key = f'{event_type}_{importance}_count'
        event_counts[key] = event_counts.get(key, 0) + 1
    
    features.update(event_counts)
    
    return features