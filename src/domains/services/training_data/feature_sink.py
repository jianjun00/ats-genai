"""
Feature Sink for capturing actual features during generation for golden file testing.

This allows capturing the real feature dictionaries that would normally go to ArrayRecord
files, so we can serialize them to JSON for regression testing.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import json
import logging

class FeatureSink:
    """Captures actual features during training data generation for golden file testing."""
    
    def __init__(self):
        self.captured_features: List[Dict[str, Any]] = []
        self.logger = logging.getLogger(__name__)
        
    def capture_training_example(self, 
                                symbol: str, 
                                prediction_timestamp: datetime,
                                base_features: Dict[str, Any],
                                timeframe_features: Dict[str, Dict[str, float]],
                                prediction_targets: Dict[str, Dict[str, float]]) -> None:
        """
        Capture a complete training example with all features.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            prediction_timestamp: Timestamp for prediction
            base_features: Base features dictionary
            timeframe_features: Dict mapping timeframe to feature dict (e.g., {'5m': {'5m_open': 216.83, ...}})
            prediction_targets: Prediction targets dictionary
        """
        captured_example = {
            'symbol': symbol,
            'prediction_timestamp': prediction_timestamp.isoformat(),
            'base_features': base_features,
            'timeframe_features': timeframe_features,
            'prediction_targets': prediction_targets,
            'captured_at': datetime.now().isoformat()
        }
        
        self.captured_features.append(captured_example)
        self.logger.debug(f"Captured features for {symbol} at {prediction_timestamp}: "
                         f"{len(timeframe_features)} timeframes, "
                         f"{sum(len(tf_features) for tf_features in timeframe_features.values())} total features")
    
    def get_captured_features(self) -> List[Dict[str, Any]]:
        """Get all captured training examples."""
        return self.captured_features.copy()
    
    def get_feature_summary(self) -> Dict[str, Any]:
        """Get summary statistics of captured features."""
        if not self.captured_features:
            return {
                'total_examples': 0,
                'symbols': [],
                'timeframes': [],
                'feature_counts': {}
            }
        
        symbols = list(set(example['symbol'] for example in self.captured_features))
        timeframes = set()
        feature_counts = {}
        
        for example in self.captured_features:
            for tf, features in example['timeframe_features'].items():
                timeframes.add(tf)
                if tf not in feature_counts:
                    feature_counts[tf] = len(features)
        
        return {
            'total_examples': len(self.captured_features),
            'symbols': sorted(symbols),
            'timeframes': sorted(list(timeframes)),
            'feature_counts': feature_counts,
            'all_feature_names': {
                tf: list(self.captured_features[0]['timeframe_features'][tf].keys())
                for tf in timeframes
                if tf in self.captured_features[0]['timeframe_features']
            } if self.captured_features else {}
        }
    
    def to_json_serializable(self) -> Dict[str, Any]:
        """Convert captured features to JSON-serializable format for golden files."""
        return {
            'feature_summary': self.get_feature_summary(),
            'captured_examples': self.captured_features
        }
    
    def clear(self) -> None:
        """Clear all captured features."""
        self.captured_features.clear()