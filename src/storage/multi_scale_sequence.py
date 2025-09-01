#!/usr/bin/env python3
"""
Multi-Scale Sequence Data Structure

Provides a unified data structure for handling multi-temporal scale financial data
(minute, hourly, daily, weekly) with efficient access patterns and event integration.

Key Features:
- Unified multi-scale temporal data access
- Event sequence integration
- Efficient time-based indexing
- Memory-optimized caching
- Fast temporal alignment across scales
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, field
from pathlib import Path
import logging
from enum import Enum
import warnings

logger = logging.getLogger(__name__)


class TimeScale(Enum):
    """Temporal scale enumeration."""
    MINUTE = "1min"
    HOURLY = "1H"
    DAILY = "1D"
    WEEKLY = "1W"


@dataclass
class MarketEvent:
    """Market event data structure."""
    event_id: str
    symbol: str
    timestamp: datetime
    event_type: str  # 'news', 'earnings', 'upgrade', 'economic'
    content: str
    sentiment_score: float
    importance_score: float
    embedding: Optional[np.ndarray] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScaleFeatures:
    """Features for a specific time scale."""
    timestamps: pd.DatetimeIndex
    ohlcv: np.ndarray  # Shape: (n_timesteps, 5) - Open, High, Low, Close, Volume
    technical: np.ndarray  # Shape: (n_timesteps, n_technical_indicators)
    volume_profile: Optional[np.ndarray] = None  # Volume-based features
    microstructure: Optional[np.ndarray] = None  # Order book features
    
    def __post_init__(self):
        """Validate data consistency."""
        if len(self.timestamps) != self.ohlcv.shape[0]:
            raise ValueError("Timestamps and OHLCV data length mismatch")
        
        if self.technical is not None and len(self.timestamps) != self.technical.shape[0]:
            raise ValueError("Timestamps and technical data length mismatch")


@dataclass
class EventSequence:
    """Sequence of market events with temporal indexing."""
    events: List[MarketEvent]
    time_range: Tuple[datetime, datetime]
    event_index: Dict[datetime, List[int]] = field(default_factory=dict)
    
    def __post_init__(self):
        """Build temporal index for fast lookups."""
        self._build_temporal_index()
    
    def _build_temporal_index(self):
        """Build temporal index for efficient event queries."""
        self.event_index.clear()
        for i, event in enumerate(self.events):
            # Round to minute for indexing
            rounded_time = event.timestamp.replace(second=0, microsecond=0)
            if rounded_time not in self.event_index:
                self.event_index[rounded_time] = []
            self.event_index[rounded_time].append(i)
    
    def get_events_in_range(self, start: datetime, end: datetime) -> List[MarketEvent]:
        """Get events within specified time range."""
        relevant_events = []
        for timestamp, event_indices in self.event_index.items():
            if start <= timestamp <= end:
                for idx in event_indices:
                    relevant_events.append(self.events[idx])
        return relevant_events
    
    def get_events_at_time(self, timestamp: datetime, tolerance: timedelta = timedelta(minutes=5)) -> List[MarketEvent]:
        """Get events near specific timestamp."""
        rounded_time = timestamp.replace(second=0, microsecond=0)
        relevant_events = []
        
        # Check exact time and tolerance window
        for check_time in pd.date_range(
            rounded_time - tolerance, 
            rounded_time + tolerance, 
            freq='1min'
        ):
            if check_time in self.event_index:
                for idx in self.event_index[check_time]:
                    relevant_events.append(self.events[idx])
        
        return relevant_events


class MultiScaleSequence:
    """
    Multi-scale temporal sequence for financial data.
    
    Handles minute, hourly, daily, and weekly data with automatic
    temporal alignment and efficient access patterns.
    """
    
    def __init__(
        self,
        symbol: str,
        time_range: Tuple[datetime, datetime],
        minute_features: Optional[ScaleFeatures] = None,
        hourly_features: Optional[ScaleFeatures] = None,
        daily_features: Optional[ScaleFeatures] = None,
        weekly_features: Optional[ScaleFeatures] = None,
        event_sequence: Optional[EventSequence] = None
    ):
        self.symbol = symbol
        self.time_range = time_range
        self.scales = {}
        
        # Store scale data
        if minute_features is not None:
            self.scales[TimeScale.MINUTE] = minute_features
        if hourly_features is not None:
            self.scales[TimeScale.HOURLY] = hourly_features
        if daily_features is not None:
            self.scales[TimeScale.DAILY] = daily_features
        if weekly_features is not None:
            self.scales[TimeScale.WEEKLY] = weekly_features
        
        self.event_sequence = event_sequence
        
        # Validate temporal alignment
        self._validate_temporal_alignment()
    
    def _validate_temporal_alignment(self):
        """Validate that all scales are properly aligned temporally."""
        for scale, features in self.scales.items():
            if not self._is_within_time_range(features.timestamps):
                warnings.warn(f"Scale {scale.value} data extends beyond specified time range")
    
    def _is_within_time_range(self, timestamps: pd.DatetimeIndex) -> bool:
        """Check if timestamps are within the specified time range."""
        return (timestamps.min() >= self.time_range[0] and 
                timestamps.max() <= self.time_range[1])
    
    def get_features(self, scale: TimeScale, feature_type: str = 'all') -> Optional[np.ndarray]:
        """
        Get features for specified scale and type.
        
        Args:
            scale: Time scale to retrieve
            feature_type: 'ohlcv', 'technical', 'volume_profile', 'microstructure', or 'all'
        
        Returns:
            Feature array or None if not available
        """
        if scale not in self.scales:
            return None
        
        features = self.scales[scale]
        
        if feature_type == 'ohlcv':
            return features.ohlcv
        elif feature_type == 'technical':
            return features.technical
        elif feature_type == 'volume_profile':
            return features.volume_profile
        elif feature_type == 'microstructure':
            return features.microstructure
        elif feature_type == 'all':
            # Concatenate all available features
            feature_arrays = [features.ohlcv]
            
            if features.technical is not None:
                feature_arrays.append(features.technical)
            if features.volume_profile is not None:
                feature_arrays.append(features.volume_profile)
            if features.microstructure is not None:
                feature_arrays.append(features.microstructure)
            
            return np.concatenate(feature_arrays, axis=1) if feature_arrays else None
        else:
            raise ValueError(f"Unknown feature type: {feature_type}")
    
    def get_timestamps(self, scale: TimeScale) -> Optional[pd.DatetimeIndex]:
        """Get timestamps for specified scale."""
        if scale not in self.scales:
            return None
        return self.scales[scale].timestamps
    
    def get_aligned_features(
        self, 
        primary_scale: TimeScale, 
        context_scales: List[TimeScale],
        alignment_method: str = 'ffill'
    ) -> Dict[str, np.ndarray]:
        """
        Get temporally aligned features across multiple scales.
        
        Args:
            primary_scale: Primary time scale for alignment
            context_scales: Additional scales to align with primary
            alignment_method: 'ffill', 'interpolate', or 'nearest'
        
        Returns:
            Dictionary of aligned feature arrays
        """
        if primary_scale not in self.scales:
            raise ValueError(f"Primary scale {primary_scale.value} not available")
        
        primary_timestamps = self.scales[primary_scale].timestamps
        aligned_features = {}
        
        # Primary scale features (no alignment needed)
        aligned_features[primary_scale.value] = self.get_features(primary_scale, 'all')
        
        # Align context scales to primary timestamps
        for context_scale in context_scales:
            if context_scale not in self.scales:
                continue
            
            context_features = self.get_features(context_scale, 'all')
            context_timestamps = self.scales[context_scale].timestamps
            
            # Create DataFrame for alignment
            context_df = pd.DataFrame(
                context_features,
                index=context_timestamps
            )
            
            # Reindex to primary timestamps with specified method
            if alignment_method == 'ffill':
                aligned_df = context_df.reindex(primary_timestamps, method='ffill')
            elif alignment_method == 'interpolate':
                aligned_df = context_df.reindex(primary_timestamps).interpolate()
            elif alignment_method == 'nearest':
                aligned_df = context_df.reindex(primary_timestamps, method='nearest')
            else:
                raise ValueError(f"Unknown alignment method: {alignment_method}")
            
            aligned_features[context_scale.value] = aligned_df.values
        
        return aligned_features
    
    def get_events_for_sequence(
        self, 
        scale: TimeScale, 
        tolerance: timedelta = timedelta(minutes=5)
    ) -> List[Tuple[int, List[MarketEvent]]]:
        """
        Get events aligned with sequence timestamps.
        
        Args:
            scale: Time scale to align events with
            tolerance: Time tolerance for event matching
        
        Returns:
            List of (timestamp_index, events) tuples
        """
        if scale not in self.scales or self.event_sequence is None:
            return []
        
        timestamps = self.scales[scale].timestamps
        aligned_events = []
        
        for i, timestamp in enumerate(timestamps):
            events = self.event_sequence.get_events_at_time(timestamp, tolerance)
            if events:
                aligned_events.append((i, events))
        
        return aligned_events
    
    def create_sequence_tensor(
        self,
        scale: TimeScale,
        sequence_length: int,
        step_size: int = 1,
        include_events: bool = False
    ) -> Dict[str, np.ndarray]:
        """
        Create sequence tensors for model training/inference.
        
        Args:
            scale: Primary time scale
            sequence_length: Length of each sequence
            step_size: Step size between sequences
            include_events: Whether to include event features
        
        Returns:
            Dictionary containing sequence arrays
        """
        if scale not in self.scales:
            raise ValueError(f"Scale {scale.value} not available")
        
        features = self.get_features(scale, 'all')
        timestamps = self.scales[scale].timestamps
        
        if features is None or len(features) < sequence_length:
            return {}
        
        # Create sequences
        n_sequences = (len(features) - sequence_length) // step_size + 1
        n_features = features.shape[1]
        
        sequences = np.zeros((n_sequences, sequence_length, n_features))
        sequence_timestamps = []
        
        for i in range(n_sequences):
            start_idx = i * step_size
            end_idx = start_idx + sequence_length
            sequences[i] = features[start_idx:end_idx]
            sequence_timestamps.append(timestamps[start_idx:end_idx])
        
        result = {
            'sequences': sequences,
            'timestamps': sequence_timestamps
        }
        
        # Add event features if requested
        if include_events and self.event_sequence is not None:
            event_sequences = []
            for seq_timestamps in sequence_timestamps:
                seq_events = []
                for ts in seq_timestamps:
                    events = self.event_sequence.get_events_at_time(ts)
                    # Convert events to feature vectors (simplified)
                    if events:
                        event_features = np.mean([
                            [e.sentiment_score, e.importance_score] for e in events
                        ], axis=0)
                    else:
                        event_features = np.zeros(2)
                    seq_events.append(event_features)
                event_sequences.append(seq_events)
            
            result['event_features'] = np.array(event_sequences)
        
        return result
    
    def get_context_features(
        self,
        primary_scale: TimeScale,
        context_window: int = 24  # Hours of context
    ) -> Dict[str, np.ndarray]:
        """
        Get context features from longer time scales.
        
        Args:
            primary_scale: Primary time scale
            context_window: Hours of context to include
        
        Returns:
            Dictionary of context features
        """
        context_features = {}
        
        # Define scale hierarchy
        scale_hierarchy = [TimeScale.MINUTE, TimeScale.HOURLY, TimeScale.DAILY, TimeScale.WEEKLY]
        primary_idx = scale_hierarchy.index(primary_scale)
        
        # Get context from higher-level scales
        for scale in scale_hierarchy[primary_idx + 1:]:
            if scale in self.scales:
                features = self.get_features(scale, 'all')
                timestamps = self.scales[scale].timestamps
                
                # Calculate context length for this scale
                if scale == TimeScale.HOURLY:
                    context_length = min(context_window, len(features))
                elif scale == TimeScale.DAILY:
                    context_length = min(context_window // 24, len(features))
                elif scale == TimeScale.WEEKLY:
                    context_length = min(context_window // (24 * 7), len(features))
                else:
                    context_length = len(features)
                
                # Take most recent context
                if context_length > 0:
                    context_features[scale.value] = features[-context_length:]
        
        return context_features
    
    def summary(self) -> Dict[str, Any]:
        """Get summary statistics of the multi-scale sequence."""
        summary = {
            'symbol': self.symbol,
            'time_range': {
                'start': self.time_range[0].isoformat(),
                'end': self.time_range[1].isoformat(),
                'duration_days': (self.time_range[1] - self.time_range[0]).days
            },
            'scales': {},
            'events': {}
        }
        
        # Scale summaries
        for scale, features in self.scales.items():
            summary['scales'][scale.value] = {
                'n_timesteps': len(features.timestamps),
                'n_ohlcv_features': features.ohlcv.shape[1] if features.ohlcv is not None else 0,
                'n_technical_features': features.technical.shape[1] if features.technical is not None else 0,
                'time_range': {
                    'start': features.timestamps.min().isoformat(),
                    'end': features.timestamps.max().isoformat()
                }
            }
        
        # Event summary
        if self.event_sequence is not None:
            summary['events'] = {
                'n_events': len(self.event_sequence.events),
                'event_types': list(set(e.event_type for e in self.event_sequence.events)),
                'time_range': {
                    'start': self.event_sequence.time_range[0].isoformat(),
                    'end': self.event_sequence.time_range[1].isoformat()
                }
            }
        
        return summary
    
    def validate(self) -> Dict[str, List[str]]:
        """Validate the multi-scale sequence for consistency."""
        issues = {
            'errors': [],
            'warnings': []
        }
        
        # Check temporal alignment
        for scale, features in self.scales.items():
            if not self._is_within_time_range(features.timestamps):
                issues['warnings'].append(f"Scale {scale.value} extends beyond time range")
        
        # Check data completeness
        for scale, features in self.scales.items():
            if features.ohlcv is not None and np.any(np.isnan(features.ohlcv)):
                issues['warnings'].append(f"Scale {scale.value} has NaN values in OHLCV")
            
            if features.technical is not None and np.any(np.isnan(features.technical)):
                issues['warnings'].append(f"Scale {scale.value} has NaN values in technical features")
        
        # Check event alignment
        if self.event_sequence is not None:
            event_time_range = self.event_sequence.time_range
            if (event_time_range[0] < self.time_range[0] or 
                event_time_range[1] > self.time_range[1]):
                issues['warnings'].append("Event sequence extends beyond main time range")
        
        return issues


def create_multi_scale_sequence(
    symbol: str,
    time_range: Tuple[datetime, datetime],
    minute_data: Optional[pd.DataFrame] = None,
    hourly_data: Optional[pd.DataFrame] = None,
    daily_data: Optional[pd.DataFrame] = None,
    weekly_data: Optional[pd.DataFrame] = None,
    events: Optional[List[MarketEvent]] = None
) -> MultiScaleSequence:
    """
    Convenience function to create MultiScaleSequence from DataFrames.
    
    Args:
        symbol: Stock symbol
        time_range: (start, end) time range
        minute_data: DataFrame with minute-level data
        hourly_data: DataFrame with hourly data
        daily_data: DataFrame with daily data
        weekly_data: DataFrame with weekly data
        events: List of market events
    
    Returns:
        MultiScaleSequence instance
    """
    
    def create_scale_features(df: pd.DataFrame) -> ScaleFeatures:
        """Convert DataFrame to ScaleFeatures."""
        if df is None:
            return None
        
        # Extract OHLCV columns
        ohlcv_cols = ['open', 'high', 'low', 'close', 'volume']
        ohlcv_data = df[ohlcv_cols].values
        
        # Extract technical indicators (remaining columns)
        technical_cols = [col for col in df.columns if col not in ohlcv_cols + ['timestamp']]
        technical_data = df[technical_cols].values if technical_cols else None
        
        return ScaleFeatures(
            timestamps=pd.to_datetime(df.index if 'timestamp' not in df.columns else df['timestamp']),
            ohlcv=ohlcv_data,
            technical=technical_data
        )
    
    # Convert DataFrames to ScaleFeatures
    minute_features = create_scale_features(minute_data) if minute_data is not None else None
    hourly_features = create_scale_features(hourly_data) if hourly_data is not None else None
    daily_features = create_scale_features(daily_data) if daily_data is not None else None
    weekly_features = create_scale_features(weekly_data) if weekly_data is not None else None
    
    # Create event sequence
    event_sequence = EventSequence(events, time_range) if events else None
    
    return MultiScaleSequence(
        symbol=symbol,
        time_range=time_range,
        minute_features=minute_features,
        hourly_features=hourly_features,
        daily_features=daily_features,
        weekly_features=weekly_features,
        event_sequence=event_sequence
    )


# Example usage
if __name__ == "__main__":
    import random
    from datetime import datetime, timedelta
    
    # Create sample data
    start_time = datetime(2024, 1, 15, 9, 30)
    end_time = start_time + timedelta(days=7)
    
    # Sample minute data
    minute_timestamps = pd.date_range(start_time, end_time, freq='1min')
    minute_df = pd.DataFrame({
        'open': np.random.uniform(150, 160, len(minute_timestamps)),
        'high': np.random.uniform(150, 160, len(minute_timestamps)),
        'low': np.random.uniform(150, 160, len(minute_timestamps)),
        'close': np.random.uniform(150, 160, len(minute_timestamps)),
        'volume': np.random.randint(1000, 10000, len(minute_timestamps)),
        'rsi': np.random.uniform(30, 70, len(minute_timestamps)),
        'macd': np.random.uniform(-1, 1, len(minute_timestamps))
    }, index=minute_timestamps)
    
    # Sample events
    events = [
        MarketEvent(
            event_id='1',
            symbol='AAPL',
            timestamp=start_time + timedelta(hours=2),
            event_type='news',
            content='Positive earnings report',
            sentiment_score=0.8,
            importance_score=0.9
        )
    ]
    
    # Create multi-scale sequence
    sequence = create_multi_scale_sequence(
        symbol='AAPL',
        time_range=(start_time, end_time),
        minute_data=minute_df,
        events=events
    )
    
    print("Multi-scale sequence summary:")
    print(sequence.summary())
    
    # Create sequence tensors
    tensors = sequence.create_sequence_tensor(
        TimeScale.MINUTE,
        sequence_length=60,  # 1 hour sequences
        step_size=30,        # 30-minute steps
        include_events=True
    )
    
    if tensors:
        print(f"Created {tensors['sequences'].shape[0]} sequences")
        print(f"Sequence shape: {tensors['sequences'].shape}")
        if 'event_features' in tensors:
            print(f"Event features shape: {tensors['event_features'].shape}")