#!/usr/bin/env python3
"""
Enhanced Multi-Timeframe Training Data - Manual Verification Demo

This script generates and displays sample training data to manually verify
that our enhanced multi-timeframe system is working correctly.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import numpy as np
import pandas as pd
import json
from datetime import datetime, timedelta
from pathlib import Path

try:
    from modeling.enhanced_feature_types import (
        FeatureSpecification, FeatureType, TimeframeSpec, 
        TechnicalIndicator, EnhancedFeatureRegistry
    )
    from modeling.cross_timeframe_aligner import CrossTimeframeAligner
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Please ensure src/ directory is in PYTHONPATH")
    sys.exit(1)


class ManualVerificationDemo:
    """Generates manual verification data for the enhanced training system."""
    
    def __init__(self):
        print("🔍 Enhanced Multi-Timeframe Training Data - Manual Verification")
        print("=" * 65)
        
        self.registry = EnhancedFeatureRegistry()
        self.aligner = CrossTimeframeAligner()
        
        # Create output directory
        self.output_dir = "manual_verification_output"
        os.makedirs(self.output_dir, exist_ok=True)
    
    def generate_sample_ohlc_data(self, symbol: str, periods: int = 100) -> pd.DataFrame:
        """Generate realistic sample OHLC data."""
        
        # Start with a base price and add random walk
        base_price = {"AAPL": 150.0, "TSLA": 200.0, "GOOGL": 2500.0}.get(symbol, 100.0)
        
        # Generate price movements
        returns = np.random.normal(0.001, 0.02, periods)  # 0.1% drift, 2% volatility
        prices = [base_price]
        
        for i in range(1, periods):
            price = prices[-1] * (1 + returns[i])
            prices.append(max(price, 1.0))  # Ensure positive prices
        
        # Generate OHLC from price series
        data = []
        dates = pd.date_range(start='2024-01-01', periods=periods, freq='D')
        
        for i, (date, close) in enumerate(zip(dates, prices)):
            # Simulate intraday movement
            volatility = close * 0.015  # 1.5% daily volatility
            high = close + abs(np.random.normal(0, volatility * 0.7))
            low = close - abs(np.random.normal(0, volatility * 0.7))
            open_price = low + np.random.uniform(0, 1) * (high - low)
            
            # Ensure OHLC consistency
            high = max(high, open_price, close)
            low = min(low, open_price, close)
            
            volume = int(np.random.lognormal(15, 0.5))  # Realistic volume distribution
            
            data.append({
                'symbol': symbol,
                'date': date.date(),
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': volume
            })
        
        return pd.DataFrame(data)
    
    def generate_technical_indicators(self, ohlc_data: pd.DataFrame) -> dict:
        """Generate technical indicators from OHLC data."""
        
        closes = ohlc_data['close'].values
        highs = ohlc_data['high'].values
        lows = ohlc_data['low'].values
        volumes = ohlc_data['volume'].values
        
        # ETOP (Envelope Top) - Simple moving average + 2%
        sma_20 = pd.Series(closes).rolling(20, min_periods=1).mean()
        etop = sma_20 * 1.02
        
        # EBOT (Envelope Bottom) - Simple moving average - 2%
        ebot = sma_20 * 0.98
        
        # PLDOT (Pivot Line Dots) - Simplified pivot points
        pivot_highs = pd.Series(highs).rolling(5, min_periods=1).max()
        pivot_lows = pd.Series(lows).rolling(5, min_periods=1).min()
        pldot = (pivot_highs + pivot_lows) / 2
        
        # EMA (Exponential Moving Average)
        ema = pd.Series(closes).ewm(span=14).mean()
        
        # RSI (Relative Strength Index)
        delta = pd.Series(closes).diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        # VWAP (Volume Weighted Average Price)
        typical_price = (highs + lows + closes) / 3
        vwap = pd.Series((typical_price * volumes).cumsum() / pd.Series(volumes).cumsum())
        
        return {
            'etop': etop.bfill().values,
            'ebot': ebot.bfill().values,
            'pldot': pldot.bfill().values,
            'ema': ema.bfill().values,
            'rsi': rsi.fillna(50).values,  # RSI defaults to 50
            'vwap': vwap.bfill().values
        }
    
    def create_feature_sequences(self, ohlc_data: pd.DataFrame, indicators: dict, 
                                sequence_length: int = 16) -> dict:
        """Create feature sequences from OHLC and indicators."""
        
        sequences = {}
        total_periods = len(ohlc_data)
        
        if total_periods < sequence_length:
            print(f"⚠️  Not enough data for sequence length {sequence_length}")
            return sequences
        
        num_sequences = total_periods - sequence_length + 1
        
        # Create OHLC sequences
        ohlc_sequence = np.zeros((num_sequences, sequence_length, 4))
        for i in range(num_sequences):
            ohlc_sequence[i] = ohlc_data.iloc[i:i+sequence_length][['open', 'high', 'low', 'close']].values
        sequences['ohlc_daily_16'] = ohlc_sequence
        
        # Create indicator sequences
        for indicator_name, indicator_values in indicators.items():
            indicator_sequence = np.zeros((num_sequences, sequence_length, 1))
            for i in range(num_sequences):
                indicator_sequence[i] = indicator_values[i:i+sequence_length].reshape(-1, 1)
            sequences[f'{indicator_name}_daily_16'] = indicator_sequence
        
        return sequences
    
    def demonstrate_cross_timeframe_alignment(self, base_sequences: dict) -> dict:
        """Demonstrate cross-timeframe feature alignment."""
        
        print("🔄 Demonstrating Cross-Timeframe Alignment...")
        
        # Simulate higher timeframe data (daily to 5-minute alignment)
        cross_features = {}
        
        if 'etop_daily_16' in base_sequences:
            daily_etop = base_sequences['etop_daily_16']
            
            # Simulate alignment from daily to 5-minute (1:288 ratio)
            # For demo purposes, we'll create a simplified version
            num_samples, daily_intervals, feature_dim = daily_etop.shape
            minute_intervals = 32  # Target 5-minute intervals
            
            aligned_etop = np.zeros((num_samples, minute_intervals, feature_dim))
            
            for sample_idx in range(num_samples):
                daily_sample = daily_etop[sample_idx]
                
                # Simple step function alignment (repeat each daily value)
                repeats_per_daily = minute_intervals // daily_intervals
                remainder = minute_intervals % daily_intervals
                
                aligned_sample = []
                for i in range(daily_intervals):
                    repeats = repeats_per_daily + (1 if i < remainder else 0)
                    for _ in range(repeats):
                        aligned_sample.append(daily_sample[i])
                
                aligned_etop[sample_idx] = np.array(aligned_sample[:minute_intervals]).reshape(-1, 1)
            
            cross_features['etop_daily_on_5min'] = aligned_etop
            
        return cross_features
    
    def generate_sample_labels(self, ohlc_data: pd.DataFrame, prediction_horizon: int = 1) -> np.ndarray:
        """Generate sample prediction labels."""
        
        closes = ohlc_data['close'].values
        
        # Generate return-based labels (binary classification)
        labels = []
        for i in range(len(closes) - prediction_horizon):
            current_price = closes[i]
            future_price = closes[i + prediction_horizon]
            return_pct = (future_price / current_price) - 1
            
            # Binary classification: 1 if positive return, 0 otherwise
            label = 1 if return_pct > 0 else 0
            labels.append(label)
        
        return np.array(labels)
    
    def create_comprehensive_demo_dataset(self, symbols: list = ['AAPL', 'TSLA']) -> dict:
        """Create a comprehensive demonstration dataset."""
        
        print(f"📊 Creating demonstration dataset for {symbols}...")
        
        all_sequences = {}
        all_labels = {}
        metadata = {
            'creation_time': datetime.now().isoformat(),
            'symbols': symbols,
            'feature_types': {},
            'feature_shapes': {},
            'sample_counts': {}
        }
        
        for symbol in symbols:
            print(f"  📈 Processing {symbol}...")
            
            # Generate OHLC data
            ohlc_data = self.generate_sample_ohlc_data(symbol, periods=80)
            
            # Generate technical indicators
            indicators = self.generate_technical_indicators(ohlc_data)
            
            # Create feature sequences
            sequences = self.create_feature_sequences(ohlc_data, indicators, sequence_length=16)
            
            # Add cross-timeframe features
            cross_features = self.demonstrate_cross_timeframe_alignment(sequences)
            sequences.update(cross_features)
            
            # Generate labels
            labels = self.generate_sample_labels(ohlc_data, prediction_horizon=1)
            
            # Store results
            symbol_key = symbol.lower()
            all_sequences[symbol_key] = sequences
            all_labels[symbol_key] = labels
            
            # Update metadata
            metadata['sample_counts'][symbol_key] = len(labels)
            for feature_name, feature_data in sequences.items():
                metadata['feature_shapes'][f'{symbol_key}_{feature_name}'] = feature_data.shape
                
                # Determine feature type
                if 'ohlc' in feature_name:
                    feature_type = 'ohlc_intervals'
                elif 'on_' in feature_name:  # Cross-timeframe
                    feature_type = 'cross_timeframe_indicators'
                else:
                    feature_type = 'price_indicator_intervals'
                
                metadata['feature_types'][f'{symbol_key}_{feature_name}'] = feature_type
        
        return {
            'sequences': all_sequences,
            'labels': all_labels,
            'metadata': metadata
        }
    
    def save_verification_data(self, dataset: dict):
        """Save verification data to files."""
        
        print(f"💾 Saving verification data to {self.output_dir}/...")
        
        # Save metadata
        metadata_path = os.path.join(self.output_dir, 'verification_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(dataset['metadata'], f, indent=2)
        
        # Save sequences (as numpy arrays)
        for symbol, sequences in dataset['sequences'].items():
            symbol_dir = os.path.join(self.output_dir, symbol)
            os.makedirs(symbol_dir, exist_ok=True)
            
            for feature_name, feature_data in sequences.items():
                feature_path = os.path.join(symbol_dir, f'{feature_name}.npy')
                np.save(feature_path, feature_data)
        
        # Save labels
        for symbol, labels in dataset['labels'].items():
            labels_path = os.path.join(self.output_dir, f'{symbol}_labels.npy')
            np.save(labels_path, labels)
        
        print(f"  ✅ Data saved to {self.output_dir}/")
    
    def display_verification_summary(self, dataset: dict):
        """Display a summary of the verification dataset."""
        
        print("\n📋 Verification Dataset Summary")
        print("=" * 40)
        
        metadata = dataset['metadata']
        
        print(f"🕐 Created: {metadata['creation_time']}")
        print(f"📊 Symbols: {', '.join(metadata['symbols'])}")
        print(f"📈 Total Features: {len(metadata['feature_shapes'])}")
        
        print(f"\n📏 Sample Counts:")
        for symbol, count in metadata['sample_counts'].items():
            print(f"  • {symbol.upper()}: {count:,} samples")
        
        print(f"\n🎯 Feature Types Distribution:")
        type_counts = {}
        for feature_type in metadata['feature_types'].values():
            type_counts[feature_type] = type_counts.get(feature_type, 0) + 1
        
        for feature_type, count in sorted(type_counts.items()):
            print(f"  • {feature_type}: {count}")
        
        print(f"\n📐 Feature Shapes (Sample):")
        sample_features = list(metadata['feature_shapes'].items())[:6]
        for feature_name, shape in sample_features:
            print(f"  • {feature_name}: {shape}")
        
        if len(metadata['feature_shapes']) > 6:
            print(f"  • ... and {len(metadata['feature_shapes']) - 6} more")
    
    def demonstrate_feature_registry_capabilities(self):
        """Demonstrate feature registry capabilities."""
        
        print(f"\n🔧 Feature Registry Capabilities")
        print("=" * 40)
        
        # Show comprehensive feature coverage
        print(f"📊 Total Registered Features: {len(self.registry.feature_specs)}")
        
        # Group by timeframe
        timeframe_counts = {}
        for spec in self.registry.feature_specs.values():
            timeframe = spec.timeframe.label
            timeframe_counts[timeframe] = timeframe_counts.get(timeframe, 0) + 1
        
        print(f"\n⏱️  Features by Timeframe:")
        for timeframe, count in sorted(timeframe_counts.items()):
            print(f"  • {timeframe}: {count}")
        
        # Show sample feature specifications
        print(f"\n🔍 Sample Feature Specifications:")
        sample_names = ['ohlc_daily_16', 'etop_5min_8', 'etop_1hour_on_5min']
        
        for name in sample_names:
            spec = self.registry.get_feature_spec(name)
            if spec:
                print(f"  • {name}:")
                print(f"    - Type: {spec.feature_type.value}")
                print(f"    - Dimensions: {spec.dimensions}")
                print(f"    - Timeframe: {spec.timeframe.label}")
                if hasattr(spec, 'indicator_type') and spec.indicator_type:
                    print(f"    - Indicator: {spec.indicator_type.display_name}")
    
    def run_complete_verification(self):
        """Run complete verification demonstration."""
        
        # Demonstrate registry capabilities
        self.demonstrate_feature_registry_capabilities()
        
        # Create comprehensive dataset
        dataset = self.create_comprehensive_demo_dataset(['AAPL', 'TSLA'])
        
        # Display summary
        self.display_verification_summary(dataset)
        
        # Save verification data
        self.save_verification_data(dataset)
        
        print(f"\n🎉 Manual Verification Complete!")
        print(f"📁 Verification data saved to: {os.path.abspath(self.output_dir)}")
        print(f"\n💡 Next Steps:")
        print(f"  1. Review generated data files in {self.output_dir}/")
        print(f"  2. Load and inspect numpy arrays with your preferred tools")
        print(f"  3. Validate feature shapes and data quality")
        print(f"  4. Test ML pipeline compatibility")


if __name__ == "__main__":
    demo = ManualVerificationDemo()
    demo.run_complete_verification()