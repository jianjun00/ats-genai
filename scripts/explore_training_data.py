#!/usr/bin/env python3
"""
Quick exploration of the generated training data
"""

import pandas as pd
import numpy as np
from pathlib import Path

def explore_training_data():
    data_path = Path("/home/jianjun/ats-data/training-data")
    
    print("🔍 Training Data Exploration")
    print("=" * 50)
    
    # Load combined dataset
    combined_file = data_path / "AAPL_TSLA_training_data_2020_2025.parquet"
    df = pd.read_parquet(combined_file)
    
    print(f"📊 Dataset Shape: {df.shape}")
    print(f"🗓️  Date Range: {df['datetime'].min()} to {df['datetime'].max()}")
    print(f"📈 Symbols: {df['symbol'].unique()}")
    print()
    
    # Sample breakdown
    print("📊 Sample Distribution:")
    for symbol in df['symbol'].unique():
        count = len(df[df['symbol'] == symbol])
        print(f"   {symbol}: {count:,} samples ({count/len(df)*100:.1f}%)")
    print()
    
    # Feature summary
    feature_cols = [col for col in df.columns if col not in ['datetime', 'symbol'] and not any(target in col for target in ['direction_', 'return_', 'volume_spike'])]
    target_cols = [col for col in df.columns if any(target in col for target in ['direction_', 'return_', 'volume_spike'])]
    
    print(f"🎯 Features: {len(feature_cols)} columns")
    print(f"🎯 Targets: {len(target_cols)} columns")
    print()
    
    # Show sample data
    print("📋 Sample Data (first 5 rows):")
    print(df[['datetime', 'symbol', 'close', 'volume', 'rsi', 'direction_5m', 'return_5m']].head())
    print()
    
    # Target distribution
    print("🎯 Target Distribution (Direction Predictions):")
    for col in ['direction_1m', 'direction_5m', 'direction_15m', 'direction_30m']:
        if col in df.columns:
            up_pct = df[col].mean() * 100
            print(f"   {col}: {up_pct:.1f}% up movements")
    print()
    
    # Basic statistics
    print("📊 Price Statistics:")
    for symbol in df['symbol'].unique():
        symbol_data = df[df['symbol'] == symbol]
        print(f"   {symbol}:")
        print(f"     Close price range: ${symbol_data['close'].min():.2f} - ${symbol_data['close'].max():.2f}")
        print(f"     Average daily volume: {symbol_data['volume'].mean():,.0f}")
        print(f"     Average RSI: {symbol_data['rsi'].mean():.1f}")
    
    print()
    print("✅ Training data ready for ML models!")
    return df

if __name__ == "__main__":
    df = explore_training_data()