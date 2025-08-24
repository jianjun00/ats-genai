#!/usr/bin/env python3
"""
Training Data Generator for AAPL & TSLA (2020-2025)

Generates comprehensive ML training datasets from minute-level market data
with technical indicators, features, and prediction targets.
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
sys.path.insert(0, '/home/jianjun/ats-genai/src')

class TrainingDataGenerator:
    def __init__(self):
        self.data_base_path = Path("/home/jianjun/ats-data/minute-files")
        self.output_path = Path("/home/jianjun/ats-data/training-data")
        self.output_path.mkdir(exist_ok=True)
        
        self.symbols = ['AAPL', 'TSLA']
        self.vendors = ['eodhd', 'polygon-from-comprehensive', 'tiingo-from-comprehensive', 
                       'comprehensive-sync', 'polygon-sync']
        
        print("🤖 Training Data Generator for AAPL & TSLA")
        print("=" * 50)
        print(f"📁 Data source: {self.data_base_path}")
        print(f"💾 Output: {self.output_path}")
        print(f"📊 Symbols: {self.symbols}")
        print()

    def find_symbol_files(self, symbol):
        """Find all parquet files for a symbol across all vendors."""
        files = []
        
        for vendor_dir in self.data_base_path.iterdir():
            if not vendor_dir.is_dir():
                continue
                
            # Look for symbol directories
            symbol_paths = list(vendor_dir.rglob(f"{symbol}*.parquet"))
            files.extend(symbol_paths)
            
            # Also check if symbol is in subdirectories
            symbol_dirs = list(vendor_dir.rglob(f"{symbol}"))
            for symbol_dir in symbol_dirs:
                if symbol_dir.is_dir():
                    parquet_files = list(symbol_dir.rglob("*.parquet"))
                    files.extend(parquet_files)
        
        return sorted(files)

    def load_minute_data(self, symbol):
        """Load and combine all minute data for a symbol."""
        print(f"📈 Loading minute data for {symbol}...")
        
        files = self.find_symbol_files(symbol)
        print(f"   Found {len(files)} data files")
        
        if not files:
            print(f"   ❌ No data files found for {symbol}")
            return None
        
        dfs = []
        for file_path in files:
            try:
                df = pd.read_parquet(file_path)
                
                # Standardize columns and timezone
                if 'timestamp' in df.columns:
                    df['datetime'] = pd.to_datetime(df['timestamp'], utc=True)
                elif 'datetime' in df.columns:
                    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
                else:
                    continue
                
                # Convert to US Eastern time for market hours
                df['datetime'] = df['datetime'].dt.tz_convert('US/Eastern')
                
                # Ensure required columns exist
                required_cols = ['open', 'high', 'low', 'close', 'volume']
                if not all(col in df.columns for col in required_cols):
                    continue
                
                df['symbol'] = symbol
                df['source_file'] = str(file_path)
                dfs.append(df[['datetime', 'open', 'high', 'low', 'close', 'volume', 'symbol', 'source_file']])
                
            except Exception as e:
                print(f"   ⚠️ Error loading {file_path}: {e}")
                continue
        
        if not dfs:
            print(f"   ❌ No valid data loaded for {symbol}")
            return None
        
        # Combine all data
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Remove duplicates and sort
        combined_df = combined_df.drop_duplicates(subset=['datetime', 'symbol'])
        combined_df = combined_df.sort_values('datetime')
        
        # Filter to 2020-2025 range (timezone aware)
        start_date = pd.Timestamp('2020-01-01', tz='US/Eastern')
        end_date = pd.Timestamp('2025-12-31', tz='US/Eastern')
        combined_df = combined_df[(combined_df['datetime'] >= start_date) & 
                                 (combined_df['datetime'] <= end_date)]
        
        print(f"   ✅ Loaded {len(combined_df):,} minute bars for {symbol}")
        print(f"   📅 Date range: {combined_df['datetime'].min()} to {combined_df['datetime'].max()}")
        
        return combined_df

    def calculate_technical_indicators(self, df):
        """Calculate comprehensive technical indicators."""
        print("   🔧 Calculating technical indicators...")
        
        # Price-based indicators
        df['returns'] = df['close'].pct_change()
        df['log_returns'] = np.log(df['close'] / df['close'].shift(1))
        
        # Moving averages
        for window in [5, 10, 20, 50, 100, 200]:
            df[f'sma_{window}'] = df['close'].rolling(window).mean()
            df[f'ema_{window}'] = df['close'].ewm(span=window).mean()
        
        # Bollinger Bands (20-period)
        sma_20 = df['close'].rolling(20).mean()
        std_20 = df['close'].rolling(20).std()
        df['bb_upper'] = sma_20 + (2 * std_20)
        df['bb_lower'] = sma_20 - (2 * std_20)
        df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / sma_20
        df['bb_position'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'])
        
        # RSI (14-period)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # MACD
        ema_12 = df['close'].ewm(span=12).mean()
        ema_26 = df['close'].ewm(span=26).mean()
        df['macd'] = ema_12 - ema_26
        df['macd_signal'] = df['macd'].ewm(span=9).mean()
        df['macd_histogram'] = df['macd'] - df['macd_signal']
        
        # Volume indicators
        df['volume_sma_20'] = df['volume'].rolling(20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma_20']
        df['vwap'] = (df['close'] * df['volume']).cumsum() / df['volume'].cumsum()
        
        # Price range and volatility
        df['true_range'] = np.maximum(
            df['high'] - df['low'],
            np.maximum(
                abs(df['high'] - df['close'].shift(1)),
                abs(df['low'] - df['close'].shift(1))
            )
        )
        df['atr'] = df['true_range'].rolling(14).mean()
        df['volatility'] = df['returns'].rolling(20).std() * np.sqrt(252 * 390)  # Annualized
        
        # Time-based features
        df['hour'] = df['datetime'].dt.hour
        df['minute'] = df['datetime'].dt.minute
        df['day_of_week'] = df['datetime'].dt.dayofweek
        df['is_market_open'] = ((df['hour'] >= 9) & (df['hour'] < 16)) | \
                              ((df['hour'] == 9) & (df['minute'] >= 30))
        
        return df

    def create_prediction_targets(self, df):
        """Create various prediction targets for ML training."""
        print("   🎯 Creating prediction targets...")
        
        # Price prediction targets (next N minutes)
        for minutes in [1, 5, 15, 30, 60]:
            df[f'price_change_{minutes}m'] = df['close'].shift(-minutes) - df['close']
            df[f'return_{minutes}m'] = df['close'].shift(-minutes) / df['close'] - 1
            df[f'direction_{minutes}m'] = (df[f'return_{minutes}m'] > 0).astype(int)
        
        # Volatility targets
        for minutes in [15, 30, 60]:
            df[f'high_low_range_{minutes}m'] = (
                df['high'].rolling(minutes).max().shift(-minutes) - 
                df['low'].rolling(minutes).min().shift(-minutes)
            ) / df['close']
        
        # Volume spike targets
        df['volume_spike_1h'] = (
            df['volume'].rolling(60).max().shift(-60) > df['volume'].rolling(60).mean() * 2
        ).astype(int)
        
        return df

    def create_feature_matrix(self, df):
        """Create ML-ready feature matrix."""
        print("   🧮 Creating feature matrix...")
        
        # Select features for ML
        feature_cols = [
            # Price features
            'open', 'high', 'low', 'close', 'volume',
            'returns', 'log_returns',
            
            # Moving averages
            'sma_5', 'sma_10', 'sma_20', 'sma_50', 'sma_100',
            'ema_5', 'ema_10', 'ema_20', 'ema_50',
            
            # Technical indicators
            'rsi', 'macd', 'macd_signal', 'macd_histogram',
            'bb_width', 'bb_position',
            'atr', 'volatility', 'volume_ratio',
            
            # Time features
            'hour', 'minute', 'day_of_week', 'is_market_open',
        ]
        
        # Target columns
        target_cols = [
            'direction_1m', 'direction_5m', 'direction_15m', 'direction_30m',
            'return_1m', 'return_5m', 'return_15m', 'return_30m',
            'volume_spike_1h'
        ]
        
        # Create feature matrix
        features = df[['datetime', 'symbol'] + feature_cols + target_cols].copy()
        
        # Remove rows with NaN values
        features = features.dropna()
        
        print(f"   ✅ Created feature matrix: {len(features):,} samples with {len(feature_cols)} features")
        
        return features, feature_cols, target_cols

    def generate_training_data(self):
        """Generate comprehensive training dataset for both symbols."""
        print("🚀 Starting training data generation...")
        print()
        
        all_features = []
        
        for symbol in self.symbols:
            print(f"📊 Processing {symbol}...")
            
            # Load minute data
            df = self.load_minute_data(symbol)
            if df is None:
                continue
            
            # Calculate indicators
            df = self.calculate_technical_indicators(df)
            
            # Create targets
            df = self.create_prediction_targets(df)
            
            # Create feature matrix
            features, feature_cols, target_cols = self.create_feature_matrix(df)
            
            all_features.append(features)
            
            # Save individual symbol data
            symbol_file = self.output_path / f"{symbol}_training_data_2020_2025.parquet"
            features.to_parquet(symbol_file, index=False)
            print(f"   💾 Saved: {symbol_file}")
            print()
        
        if all_features:
            # Combine all symbols
            combined_features = pd.concat(all_features, ignore_index=True)
            
            # Save combined dataset
            combined_file = self.output_path / "AAPL_TSLA_training_data_2020_2025.parquet"
            combined_features.to_parquet(combined_file, index=False)
            
            # Save feature names
            feature_info = {
                'feature_columns': feature_cols,
                'target_columns': target_cols,
                'total_samples': len(combined_features),
                'symbols': self.symbols,
                'date_range': f"{combined_features['datetime'].min()} to {combined_features['datetime'].max()}"
            }
            
            import json
            feature_file = self.output_path / "feature_info.json"
            with open(feature_file, 'w') as f:
                json.dump(feature_info, f, indent=2, default=str)
            
            print("🎉 Training Data Generation Complete!")
            print("=" * 50)
            print(f"📊 Combined dataset: {len(combined_features):,} samples")
            print(f"🎯 Features: {len(feature_cols)} columns")
            print(f"🎯 Targets: {len(target_cols)} prediction targets")
            print(f"💾 Output file: {combined_file}")
            print(f"📋 Feature info: {feature_file}")
            print()
            
            # Show sample statistics
            print("📈 Sample Statistics:")
            for symbol in self.symbols:
                symbol_data = combined_features[combined_features['symbol'] == symbol]
                print(f"   {symbol}: {len(symbol_data):,} samples")
            
            return combined_features
        
        return None

def main():
    generator = TrainingDataGenerator()
    training_data = generator.generate_training_data()
    
    if training_data is not None:
        print("✅ Training data generation successful!")
        print(f"🔗 Access your training data at: {generator.output_path}")
    else:
        print("❌ Training data generation failed!")

if __name__ == "__main__":
    main()