#!/usr/bin/env python3
"""
Demo script for Enhanced Training Data Generation with Real Database

This script demonstrates the complete enhanced training data pipeline using
real market data from the ATS database.
"""

import os
import sys
import asyncio
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from modeling.enhanced_training_data_generator import (
    EnhancedTrainingDataGenerator, EnhancedTrainingConfig
)
from modeling.enhanced_feature_types import EnhancedFeatureRegistry


async def main():
    """Main demo function."""
    
    print("🚀 Enhanced Training Data Generation Demo")
    print("=" * 50)
    
    # Database configuration (using the dev database from CLAUDE.md)
    DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/dev_db"
    
    # Initialize generator
    generator = EnhancedTrainingDataGenerator(DATABASE_URL)
    
    try:
        # Initialize database connection
        print("📡 Connecting to database...")
        await generator.initialize()
        print("✅ Database connected successfully")
        
        # Check what symbols are available
        print("\n🔍 Checking available symbols...")
        async with generator.db_pool.acquire() as conn:
            symbols_result = await conn.fetch("""
                SELECT DISTINCT i.symbol, COUNT(dp.date) as record_count
                FROM dev_instruments i 
                JOIN dev_daily_prices dp ON i.id = dp.instrument_id 
                WHERE dp.date >= '2024-01-01' AND dp.date <= '2024-12-31'
                GROUP BY i.symbol
                HAVING COUNT(dp.date) >= 50  -- At least 50 records
                ORDER BY record_count DESC
                LIMIT 10
            """)
            
            if symbols_result:
                print(f"✅ Found {len(symbols_result)} symbols with sufficient data:")
                for row in symbols_result:
                    print(f"   - {row['symbol']}: {row['record_count']} records")
                
                # Use top symbols
                selected_symbols = [row['symbol'] for row in symbols_result[:3]]
            else:
                print("⚠️  No symbols found in database. Using fallback symbols.")
                selected_symbols = ['AAPL', 'TSLA', 'GOOGL']
        
        print(f"\n🎯 Selected symbols for training: {selected_symbols}")
        
        # Get feature registry and select diverse features
        registry = generator.feature_registry
        
        feature_specs = []
        
        # Add OHLC features (different timeframes and intervals)
        ohlc_features = [
            "ohlc_5min_8",      # 8 intervals of 5-minute OHLC
            "ohlc_15min_16",    # 16 intervals of 15-minute OHLC  
            "ohlc_daily_32"     # 32 intervals of daily OHLC
        ]
        
        for feature_name in ohlc_features:
            spec = registry.get_feature_spec(feature_name)
            if spec:
                feature_specs.append(spec)
                print(f"   ✅ {feature_name}: {spec.dimensions}")
            else:
                print(f"   ❌ {feature_name}: not found")
        
        # Add technical indicator features
        indicator_features = [
            "etop_5min_8",      # ETOP (Envelope Top) - resistance levels
            "ebot_5min_8",      # EBOT (Envelope Bottom) - support levels
            "pldot_15min_16",   # PLDOT (Pivot Line Dots) - pivot points
            "ema_daily_16"      # EMA (Exponential Moving Average) - trend
        ]
        
        for feature_name in indicator_features:
            spec = registry.get_feature_spec(feature_name)
            if spec:
                feature_specs.append(spec)
                print(f"   ✅ {feature_name}: {spec.dimensions}")
            else:
                print(f"   ❌ {feature_name}: not found")
        
        # Add cross-timeframe features
        cross_features = [
            "etop_1hour_on_5min",    # 1-hour ETOP aligned to 5-minute intervals
            "pldot_daily_on_15min"   # Daily PLDOT aligned to 15-minute intervals
        ]
        
        for feature_name in cross_features:
            spec = registry.get_feature_spec(feature_name)
            if spec:
                feature_specs.append(spec)
                print(f"   ✅ {feature_name}: {spec.dimensions} (cross-timeframe)")
            else:
                print(f"   ❌ {feature_name}: not found")
        
        if not feature_specs:
            print("❌ No valid feature specs found!")
            return
        
        print(f"\n📊 Using {len(feature_specs)} typed features for training data generation")
        
        # Create output directory
        output_dir = "enhanced_training_data_demo"
        os.makedirs(output_dir, exist_ok=True)
        
        # Configure training data generation
        config = EnhancedTrainingConfig(
            # Data selection
            symbols=selected_symbols,
            start_date="2024-01-01",
            end_date="2024-08-31",  # 8 months of data
            
            # Feature configuration
            feature_specs=feature_specs,
            include_cross_timeframe=True,
            
            # Processing parameters
            sequence_length=64,      # 64 time steps per sample
            prediction_horizon=1,    # Predict 1 day ahead
            min_samples_per_symbol=100,
            
            # Quality control
            max_missing_ratio=0.15,  # Allow up to 15% missing data
            outlier_std_threshold=4.0,
            
            # Output configuration
            output_dir=output_dir,
            dataset_name="enhanced_multiframe_dataset",
            compression_level=6,
            
            # Metadata
            description="Enhanced multi-timeframe training dataset with typed features",
            created_by="demo_enhanced_training_data",
            tags=["demo", "multi-timeframe", "typed-features", "real-data"]
        )
        
        print(f"\n⚙️  Configuration Summary:")
        print(f"   • Symbols: {len(config.symbols)} ({', '.join(config.symbols)})")
        print(f"   • Date range: {config.start_date} to {config.end_date}")
        print(f"   • Features: {len(config.feature_specs)}")
        print(f"   • Sequence length: {config.sequence_length}")
        print(f"   • Prediction horizon: {config.prediction_horizon} days")
        print(f"   • Output directory: {config.output_dir}")
        
        # Generate enhanced training dataset
        print(f"\n🏭 Generating enhanced training dataset...")
        print("   This may take a few minutes depending on data size...")
        
        metadata = await generator.generate_training_dataset(config)
        
        # Display results
        print(f"\n🎉 Training Dataset Generated Successfully!")
        print("=" * 50)
        
        print(f"📈 Dataset Statistics:")
        print(f"   • Total samples: {metadata.total_samples:,}")
        print(f"   • Symbols processed: {metadata.symbols_count}")
        print(f"   • Date range: {metadata.date_range[0]} to {metadata.date_range[1]}")
        print(f"   • Data quality score: {metadata.data_quality_score:.1%}")
        print(f"   • Missing data ratio: {metadata.missing_data_ratio:.1%}")
        print(f"   • Outliers removed: {metadata.outliers_removed:,}")
        
        print(f"\n📊 Feature Information:")
        for name, shape in metadata.feature_shapes.items():
            feature_type = metadata.feature_types.get(name, "unknown")
            print(f"   • {name}: {shape} ({feature_type})")
        
        print(f"\n💾 File Information:")
        total_size = sum(metadata.file_sizes_mb.values())
        for file_type, size_mb in metadata.file_sizes_mb.items():
            path = metadata.file_paths[file_type]
            print(f"   • {file_type}: {size_mb:.1f} MB ({path})")
        print(f"   • Total size: {total_size:.1f} MB")
        
        print(f"\n⏱️  Performance:")
        print(f"   • Generation time: {metadata.generation_duration_seconds:.1f} seconds")
        print(f"   • Processing stages: {len(metadata.processing_stages)}")
        
        for stage in metadata.processing_stages:
            status_icon = "✅" if stage["status"] == "completed" else "❌"
            print(f"     {status_icon} {stage['stage']}: {stage['duration_seconds']:.1f}s")
        
        print(f"\n📄 Metadata saved to: {output_dir}/{config.dataset_name}_metadata.json")
        
        # Verify files exist and are readable
        print(f"\n🔍 Verifying generated files...")
        for file_type, path in metadata.file_paths.items():
            if os.path.exists(path):
                size_mb = os.path.getsize(path) / (1024 * 1024)
                print(f"   ✅ {file_type}: {path} ({size_mb:.1f} MB)")
            else:
                print(f"   ❌ {file_type}: {path} (NOT FOUND)")
        
        print(f"\n🎯 Next Steps:")
        print(f"   1. Load the dataset in your ML training pipeline")
        print(f"   2. Use the typed feature metadata for intelligent preprocessing")
        print(f"   3. Leverage multi-timeframe features for enhanced model performance")
        print(f"   4. Visualize training examples using the feature type information")
        
        return metadata
        
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()
        return None
        
    finally:
        print(f"\n🔌 Closing database connection...")
        await generator.close()
        print("✅ Demo completed")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run the demo
    asyncio.run(main())