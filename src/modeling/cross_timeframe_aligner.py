"""
Cross-Timeframe Alignment System

Aligns indicators and data from different timeframes for multi-timeframe analysis.
Supports alignment of higher timeframe data to lower timeframe intervals.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from enum import Enum

try:
    from .enhanced_feature_types import (
        FeatureSpecification, FeatureType, TimeframeSpec, 
        TechnicalIndicator, EnhancedFeatureRegistry
    )
except ImportError:
    from enhanced_feature_types import (
        FeatureSpecification, FeatureType, TimeframeSpec, 
        TechnicalIndicator, EnhancedFeatureRegistry
    )

logger = logging.getLogger(__name__)


class AlignmentMethod(Enum):
    """Methods for aligning cross-timeframe data."""
    REPEAT = "repeat"                    # Repeat higher TF values
    INTERPOLATE = "interpolate"          # Linear interpolation
    FORWARD_FILL = "forward_fill"        # Forward fill missing values
    BACKWARD_FILL = "backward_fill"      # Backward fill missing values
    NEAREST = "nearest"                  # Nearest neighbor
    STEP_FUNCTION = "step_function"      # Step function (hold until next)


@dataclass
class AlignmentConfig:
    """Configuration for cross-timeframe alignment."""
    source_timeframe: TimeframeSpec
    target_timeframe: TimeframeSpec
    method: AlignmentMethod
    fill_gaps: bool = True
    max_gap_periods: int = 5  # Maximum gap to fill
    edge_behavior: str = "extend"  # 'extend', 'nan', 'drop'


@dataclass  
class AlignmentResult:
    """Result of cross-timeframe alignment."""
    aligned_data: np.ndarray
    source_timestamps: List[datetime]
    target_timestamps: List[datetime]  
    alignment_quality: float  # 0-1 quality score
    gaps_filled: int
    metadata: Dict[str, Any]


class CrossTimeframeAligner:
    """Aligns indicators from different timeframes."""
    
    def __init__(self):
        # Timeframe multipliers relative to 5-minute base
        self.timeframe_multipliers = {
            TimeframeSpec.MINUTE_5: 1,
            TimeframeSpec.MINUTE_15: 3,
            TimeframeSpec.HOUR_1: 12,
            TimeframeSpec.DAILY: 288,
            TimeframeSpec.WEEKLY: 2016,
            TimeframeSpec.MONTHLY: 8640
        }
        
        # Alignment cache for performance
        self._alignment_cache: Dict[str, AlignmentResult] = {}
        
        logger.info("Initialized CrossTimeframeAligner")
    
    async def align_cross_timeframe_features(self, 
                                           base_data: Dict[str, np.ndarray],
                                           cross_specs: List[FeatureSpecification],
                                           symbols: List[str],
                                           start_date: str,
                                           end_date: str) -> Dict[str, np.ndarray]:
        """Align higher timeframe indicators to lower timeframe intervals."""
        
        logger.info(f"Aligning {len(cross_specs)} cross-timeframe features")
        
        aligned_features = {}
        
        for spec in cross_specs:
            if spec.feature_type != FeatureType.CROSS_TIMEFRAME_INDICATORS:
                continue
            
            try:
                aligned_data = await self._align_single_feature(
                    spec, base_data, symbols, start_date, end_date
                )
                
                if aligned_data is not None:
                    aligned_features[spec.name] = aligned_data
                    logger.info(f"Aligned feature {spec.name}: shape {aligned_data.shape}")
                else:
                    logger.warning(f"Failed to align feature: {spec.name}")
                    
            except Exception as e:
                logger.error(f"Error aligning feature {spec.name}: {e}")
                continue
        
        logger.info(f"Successfully aligned {len(aligned_features)} cross-timeframe features")
        return aligned_features
    
    async def _align_single_feature(self,
                                   spec: FeatureSpecification,
                                   base_data: Dict[str, np.ndarray],
                                   symbols: List[str],
                                   start_date: str,
                                   end_date: str) -> Optional[np.ndarray]:
        """Align a single cross-timeframe feature."""
        
        if not spec.source_timeframe or not spec.indicator_type:
            logger.warning(f"Invalid cross-timeframe spec: {spec.name}")
            return None
        
        # Create alignment configuration
        config = AlignmentConfig(
            source_timeframe=spec.source_timeframe,
            target_timeframe=spec.timeframe,
            method=AlignmentMethod.STEP_FUNCTION,  # Most appropriate for indicators
            fill_gaps=True,
            max_gap_periods=5
        )
        
        # Find source feature data
        source_feature_name = self._find_source_feature_name(spec, base_data)
        source_data = base_data.get(source_feature_name)
        
        if source_data is None:
            logger.warning(f"Source data not found for {spec.name}: {source_feature_name}")
            # Create synthetic source data for demonstration
            source_data = await self._generate_synthetic_source_data(spec, symbols, start_date, end_date)
        
        if source_data is None:
            return None
        
        # Perform alignment
        result = self._perform_alignment(source_data, config, spec.intervals)
        
        if result and result.aligned_data.size > 0:
            return result.aligned_data
        
        return None
    
    def _find_source_feature_name(self, spec: FeatureSpecification, 
                                 base_data: Dict[str, np.ndarray]) -> str:
        """Find the corresponding source feature name in base data."""
        
        if not spec.source_timeframe or not spec.indicator_type:
            return ""
        
        # Try different interval sizes for the source timeframe
        for intervals in [8, 16, 32]:
            candidate_name = f"{spec.indicator_type.code}_{spec.source_timeframe.label}_{intervals}"
            if candidate_name in base_data:
                return candidate_name
        
        # If not found, return expected name
        return f"{spec.indicator_type.code}_{spec.source_timeframe.label}_16"
    
    async def _generate_synthetic_source_data(self,
                                            spec: FeatureSpecification,
                                            symbols: List[str],
                                            start_date: str,
                                            end_date: str) -> Optional[np.ndarray]:
        """Generate synthetic source data for demonstration."""
        
        logger.info(f"Generating synthetic source data for {spec.name}")
        
        # Calculate number of samples needed
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        days = (end - start).days
        
        if spec.source_timeframe == TimeframeSpec.HOUR_1:
            periods_per_day = 24
        elif spec.source_timeframe == TimeframeSpec.DAILY:
            periods_per_day = 1
        elif spec.source_timeframe == TimeframeSpec.WEEKLY:
            periods_per_day = 1/7
        else:
            periods_per_day = 288  # 5-minute default
        
        total_periods = int(days * periods_per_day)
        num_symbols = len(symbols)
        
        # Generate realistic indicator values
        np.random.seed(42)  # Reproducible
        base_values = []
        
        for symbol in symbols:
            # Different base levels for different symbols
            if symbol == 'AAPL':
                base_level = 150
            elif symbol == 'TSLA':
                base_level = 200
            else:
                base_level = 100
                
            symbol_values = []
            current_value = base_level
            
            for i in range(total_periods):
                # Simulate indicator movement
                if spec.indicator_type == TechnicalIndicator.ETOP:
                    # ETOP should be above price (resistance)
                    current_value += np.random.normal(0, 0.5)
                    current_value = max(base_level * 0.95, current_value)  # Don't go too low
                elif spec.indicator_type == TechnicalIndicator.EBOT:
                    # EBOT should be below price (support)  
                    current_value += np.random.normal(0, 0.3)
                    current_value = max(base_level * 0.8, current_value)
                else:
                    # General indicator
                    current_value += np.random.normal(0, 0.4)
                
                symbol_values.append(current_value)
                
            base_values.extend(symbol_values)
        
        # Create sequences for the specified intervals
        intervals = 16  # Standard for cross-timeframe
        sequences = []
        
        for symbol_idx in range(num_symbols):
            symbol_start = symbol_idx * total_periods
            symbol_end = symbol_start + total_periods
            symbol_data = base_values[symbol_start:symbol_end]
            
            # Create sliding windows
            for i in range(intervals, len(symbol_data)):
                sequence = np.array(symbol_data[i-intervals:i]).reshape(-1, 1)
                sequences.append(sequence)
        
        if sequences:
            result = np.array(sequences)  # Shape: [samples, intervals, 1]
            logger.info(f"Generated synthetic source data: shape {result.shape}")
            return result
        
        return None
    
    def _perform_alignment(self, 
                          source_data: np.ndarray,
                          config: AlignmentConfig,
                          target_intervals: int) -> Optional[AlignmentResult]:
        """Perform the actual timeframe alignment."""
        
        if source_data.size == 0:
            return None
        
        logger.debug(f"Aligning from {config.source_timeframe.label} to {config.target_timeframe.label}")
        
        # Calculate alignment ratio
        source_multiplier = self.timeframe_multipliers[config.source_timeframe]
        target_multiplier = self.timeframe_multipliers[config.target_timeframe] 
        alignment_ratio = source_multiplier / target_multiplier
        
        if alignment_ratio < 1:
            # Source is higher frequency than target - downsample
            aligned_data = self._downsample_data(source_data, alignment_ratio, config.method)
        elif alignment_ratio > 1:
            # Source is lower frequency than target - upsample  
            aligned_data = self._upsample_data(source_data, alignment_ratio, config.method, target_intervals)
        else:
            # Same frequency - just ensure correct interval count
            aligned_data = self._adjust_intervals(source_data, target_intervals)
        
        if aligned_data is None or aligned_data.size == 0:
            return None
        
        # Calculate quality metrics
        quality_score = self._calculate_alignment_quality(source_data, aligned_data, config)
        
        return AlignmentResult(
            aligned_data=aligned_data,
            source_timestamps=[],  # Would be populated in real implementation
            target_timestamps=[],
            alignment_quality=quality_score,
            gaps_filled=0,  # Would be calculated in real implementation
            metadata={
                "source_timeframe": config.source_timeframe.label,
                "target_timeframe": config.target_timeframe.label,
                "alignment_method": config.method.value,
                "alignment_ratio": alignment_ratio
            }
        )
    
    def _upsample_data(self, 
                      data: np.ndarray,
                      ratio: float,
                      method: AlignmentMethod,
                      target_intervals: int) -> Optional[np.ndarray]:
        """Upsample lower frequency data to higher frequency."""
        
        if data.ndim != 3:
            logger.error(f"Expected 3D data, got {data.ndim}D")
            return None
        
        num_samples, source_intervals, feature_dim = data.shape
        upsample_factor = int(ratio)
        
        upsampled_sequences = []
        
        for sample_idx in range(num_samples):
            sample_data = data[sample_idx]  # Shape: [source_intervals, feature_dim]
            
            if method == AlignmentMethod.REPEAT:
                # Repeat each source value
                upsampled = np.repeat(sample_data, upsample_factor, axis=0)
            
            elif method == AlignmentMethod.STEP_FUNCTION:
                # Step function - hold value until next update
                upsampled = np.repeat(sample_data, upsample_factor, axis=0)
                
            elif method == AlignmentMethod.INTERPOLATE:
                # Linear interpolation between points
                upsampled = self._interpolate_sequence(sample_data, upsample_factor)
                
            else:
                # Default to repeat
                upsampled = np.repeat(sample_data, upsample_factor, axis=0)
            
            # Adjust to target intervals
            if len(upsampled) > target_intervals:
                upsampled = upsampled[-target_intervals:]
            elif len(upsampled) < target_intervals:
                # Pad with last value
                pad_length = target_intervals - len(upsampled)
                last_value = upsampled[-1:] if len(upsampled) > 0 else np.zeros((1, feature_dim))
                padding = np.tile(last_value, (pad_length, 1))
                upsampled = np.vstack([upsampled, padding])
            
            upsampled_sequences.append(upsampled)
        
        result = np.array(upsampled_sequences)
        logger.debug(f"Upsampled data: {data.shape} -> {result.shape}")
        return result
    
    def _downsample_data(self,
                        data: np.ndarray,
                        ratio: float,
                        method: AlignmentMethod) -> Optional[np.ndarray]:
        """Downsample higher frequency data to lower frequency."""
        
        if data.ndim != 3:
            return None
        
        downsample_factor = int(1 / ratio)
        if downsample_factor <= 1:
            return data
        
        num_samples, source_intervals, feature_dim = data.shape
        downsampled_sequences = []
        
        for sample_idx in range(num_samples):
            sample_data = data[sample_idx]
            
            if method in [AlignmentMethod.REPEAT, AlignmentMethod.STEP_FUNCTION]:
                # Take every nth value
                downsampled = sample_data[::downsample_factor]
            else:
                # For other methods, use averaging
                reshaped = sample_data[:len(sample_data)//downsample_factor*downsample_factor]
                reshaped = reshaped.reshape(-1, downsample_factor, feature_dim)
                downsampled = reshaped.mean(axis=1)
            
            downsampled_sequences.append(downsampled)
        
        result = np.array(downsampled_sequences)
        logger.debug(f"Downsampled data: {data.shape} -> {result.shape}")
        return result
    
    def _adjust_intervals(self, 
                         data: np.ndarray,
                         target_intervals: int) -> Optional[np.ndarray]:
        """Adjust data to have target number of intervals."""
        
        if data.ndim != 3:
            return None
        
        num_samples, source_intervals, feature_dim = data.shape
        
        if source_intervals == target_intervals:
            return data
        
        adjusted_sequences = []
        
        for sample_idx in range(num_samples):
            sample_data = data[sample_idx]
            
            if source_intervals > target_intervals:
                # Truncate to target intervals
                adjusted = sample_data[-target_intervals:]
            else:
                # Pad to target intervals
                pad_length = target_intervals - source_intervals
                last_value = sample_data[-1:] if source_intervals > 0 else np.zeros((1, feature_dim))
                padding = np.tile(last_value, (pad_length, 1))
                adjusted = np.vstack([sample_data, padding])
            
            adjusted_sequences.append(adjusted)
        
        result = np.array(adjusted_sequences)
        logger.debug(f"Adjusted intervals: {data.shape} -> {result.shape}")
        return result
    
    def _interpolate_sequence(self, 
                            sequence: np.ndarray,
                            upsample_factor: int) -> np.ndarray:
        """Perform linear interpolation on a sequence."""
        
        if sequence.ndim != 2:
            return sequence
        
        source_length, feature_dim = sequence.shape
        target_length = source_length * upsample_factor
        
        # Create interpolation indices
        source_indices = np.arange(source_length) * upsample_factor
        target_indices = np.arange(target_length)
        
        interpolated = np.zeros((target_length, feature_dim))
        
        for feature_idx in range(feature_dim):
            interpolated[:, feature_idx] = np.interp(
                target_indices, source_indices, sequence[:, feature_idx]
            )
        
        return interpolated
    
    def _calculate_alignment_quality(self,
                                   source_data: np.ndarray,
                                   aligned_data: np.ndarray,
                                   config: AlignmentConfig) -> float:
        """Calculate quality score for the alignment."""
        
        try:
            # Basic quality metrics
            quality_factors = []
            
            # 1. Shape consistency
            if aligned_data.ndim == 3 and source_data.ndim == 3:
                shape_score = 1.0
            else:
                shape_score = 0.5
            quality_factors.append(shape_score)
            
            # 2. Data completeness (no NaN values)
            if np.isfinite(aligned_data).all():
                completeness_score = 1.0
            else:
                completeness_score = np.isfinite(aligned_data).mean()
            quality_factors.append(completeness_score)
            
            # 3. Value range consistency
            if source_data.size > 0 and aligned_data.size > 0:
                source_range = np.ptp(source_data)  # Peak-to-peak
                aligned_range = np.ptp(aligned_data)
                
                if source_range > 0:
                    range_similarity = 1 - abs(aligned_range - source_range) / source_range
                    range_similarity = max(0, min(1, range_similarity))
                else:
                    range_similarity = 1.0
            else:
                range_similarity = 0.0
            quality_factors.append(range_similarity)
            
            # 4. Alignment method appropriateness
            method_scores = {
                AlignmentMethod.STEP_FUNCTION: 0.9,  # Best for indicators
                AlignmentMethod.REPEAT: 0.8,
                AlignmentMethod.INTERPOLATE: 0.7,
                AlignmentMethod.FORWARD_FILL: 0.6,
                AlignmentMethod.BACKWARD_FILL: 0.6,
                AlignmentMethod.NEAREST: 0.5
            }
            method_score = method_scores.get(config.method, 0.5)
            quality_factors.append(method_score)
            
            # Overall quality (weighted average)
            weights = [0.25, 0.3, 0.25, 0.2]  # Emphasis on completeness and range
            quality = sum(factor * weight for factor, weight in zip(quality_factors, weights))
            
            return min(1.0, max(0.0, quality))
            
        except Exception as e:
            logger.error(f"Error calculating alignment quality: {e}")
            return 0.0
    
    def get_alignment_statistics(self) -> Dict[str, Any]:
        """Get statistics about performed alignments."""
        
        if not self._alignment_cache:
            return {"total_alignments": 0}
        
        qualities = [result.alignment_quality for result in self._alignment_cache.values()]
        gaps_filled = [result.gaps_filled for result in self._alignment_cache.values()]
        
        return {
            "total_alignments": len(self._alignment_cache),
            "average_quality": np.mean(qualities) if qualities else 0.0,
            "min_quality": np.min(qualities) if qualities else 0.0,
            "max_quality": np.max(qualities) if qualities else 0.0,
            "total_gaps_filled": sum(gaps_filled),
            "alignment_methods": [result.metadata.get("alignment_method", "unknown") 
                                for result in self._alignment_cache.values()]
        }
    
    def clear_cache(self):
        """Clear the alignment cache."""
        self._alignment_cache.clear()
        logger.info("Alignment cache cleared")


# Utility functions for testing and validation
def validate_cross_timeframe_alignment(source_data: np.ndarray,
                                     aligned_data: np.ndarray,
                                     source_tf: TimeframeSpec,
                                     target_tf: TimeframeSpec) -> Dict[str, Any]:
    """Validate that cross-timeframe alignment is correct."""
    
    validation_results = {
        "is_valid": True,
        "errors": [],
        "warnings": [],
        "metrics": {}
    }
    
    try:
        # 1. Check shapes
        if source_data.ndim != 3 or aligned_data.ndim != 3:
            validation_results["errors"].append(
                f"Invalid dimensions: source {source_data.ndim}D, aligned {aligned_data.ndim}D"
            )
            validation_results["is_valid"] = False
        
        # 2. Check for NaN values
        if not np.isfinite(aligned_data).all():
            nan_count = (~np.isfinite(aligned_data)).sum()
            validation_results["warnings"].append(f"Found {nan_count} NaN values in aligned data")
        
        # 3. Check value ranges
        if source_data.size > 0 and aligned_data.size > 0:
            source_min, source_max = np.min(source_data), np.max(source_data)
            aligned_min, aligned_max = np.min(aligned_data), np.max(aligned_data)
            
            validation_results["metrics"]["source_range"] = (source_min, source_max)
            validation_results["metrics"]["aligned_range"] = (aligned_min, aligned_max)
            
            # Values should be in similar range
            if aligned_max > source_max * 2 or aligned_min < source_min * 0.5:
                validation_results["warnings"].append(
                    "Aligned data range significantly different from source"
                )
        
        # 4. Check timeframe relationship
        multipliers = {
            TimeframeSpec.MINUTE_5: 1,
            TimeframeSpec.MINUTE_15: 3,
            TimeframeSpec.HOUR_1: 12,
            TimeframeSpec.DAILY: 288
        }
        
        source_mult = multipliers.get(source_tf, 1)
        target_mult = multipliers.get(target_tf, 1)
        expected_ratio = source_mult / target_mult
        
        validation_results["metrics"]["expected_ratio"] = expected_ratio
        
        # 5. Sample count validation
        if source_data.shape[0] > 0 and aligned_data.shape[0] > 0:
            sample_ratio = aligned_data.shape[0] / source_data.shape[0]
            validation_results["metrics"]["actual_sample_ratio"] = sample_ratio
            
            # For upsampling, aligned should have more or equal samples
            if expected_ratio > 1 and sample_ratio < 0.9:
                validation_results["warnings"].append(
                    f"Expected more samples after upsampling: {sample_ratio:.2f}"
                )
        
    except Exception as e:
        validation_results["errors"].append(f"Validation error: {str(e)}")
        validation_results["is_valid"] = False
    
    return validation_results


if __name__ == "__main__":
    # Demo and testing
    import asyncio
    
    async def demo():
        """Demonstrate cross-timeframe alignment."""
        
        print("=== Cross-Timeframe Alignment Demo ===")
        
        # Initialize aligner
        aligner = CrossTimeframeAligner()
        
        # Create synthetic source data (hourly ETOP)
        np.random.seed(42)
        num_samples = 100
        source_intervals = 8
        
        # Simulate hourly ETOP values
        hourly_etop = []
        base_value = 150
        
        for sample in range(num_samples):
            sample_values = []
            current_value = base_value + np.random.normal(0, 5)
            
            for interval in range(source_intervals):
                current_value += np.random.normal(0, 1)
                sample_values.append([current_value])  # Shape: [1] for single indicator
                
            hourly_etop.append(sample_values)
        
        source_data = np.array(hourly_etop)  # Shape: [samples, intervals, 1]
        print(f"Source data (hourly ETOP): {source_data.shape}")
        
        # Create alignment configuration
        config = AlignmentConfig(
            source_timeframe=TimeframeSpec.HOUR_1,
            target_timeframe=TimeframeSpec.MINUTE_5,
            method=AlignmentMethod.STEP_FUNCTION,
            fill_gaps=True
        )
        
        # Perform alignment
        result = aligner._perform_alignment(source_data, config, target_intervals=16)
        
        if result:
            print(f"Aligned data (5-min intervals): {result.aligned_data.shape}")
            print(f"Alignment quality: {result.alignment_quality:.3f}")
            print(f"Alignment metadata: {result.metadata}")
            
            # Validate alignment
            validation = validate_cross_timeframe_alignment(
                source_data, result.aligned_data,
                TimeframeSpec.HOUR_1, TimeframeSpec.MINUTE_5
            )
            
            print(f"\nValidation Results:")
            print(f"Is valid: {validation['is_valid']}")
            if validation['errors']:
                print(f"Errors: {validation['errors']}")
            if validation['warnings']:
                print(f"Warnings: {validation['warnings']}")
            print(f"Metrics: {validation['metrics']}")
            
            # Show sample alignment
            print(f"\nSample Alignment (first sample):")
            print(f"Source (hourly): {source_data[0].flatten()[:4]}")  # First 4 values
            print(f"Aligned (5-min): {result.aligned_data[0].flatten()[:12]}")  # First 12 values
            
        else:
            print("Alignment failed")
        
        # Get aligner statistics
        stats = aligner.get_alignment_statistics()
        print(f"\nAligner Statistics: {stats}")
    
    # Run demo
    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo())