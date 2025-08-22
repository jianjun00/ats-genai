#!/usr/bin/env python3
"""
Adaptive Model Demo

This script demonstrates the key concepts of the adaptive model approach:
1. Bootstrap training on historical data
2. Daily model updates during backtesting
3. Performance comparison vs static model
"""

import sys
import asyncio
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ml.dynamic_training.adaptive_sr_model import (
    AdaptiveSupportResistanceModel, 
    AdaptiveModelConfig
)

async def demo_adaptive_model():
    """Demonstrate adaptive model capabilities"""
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    print("="*60)
    print("ADAPTIVE SUPPORT/RESISTANCE MODEL DEMO")
    print("="*60)
    
    # Configuration
    config = AdaptiveModelConfig(
        bootstrap_years=2,
        min_bootstrap_examples=100,  # Lower for demo
        rolling_window_days=180,     # 6 months
        retrain_frequency_days=1,    # Daily updates
        min_retrain_examples=20      # Lower threshold for demo
    )
    
    # Demo symbols
    symbols = ['AAPL', 'MSFT', 'GOOGL']
    
    print(f"Demo Configuration:")
    print(f"- Symbols: {symbols}")
    print(f"- Bootstrap Years: {config.bootstrap_years}")
    print(f"- Rolling Window: {config.rolling_window_days} days")
    print(f"- Retrain Frequency: Daily")
    print()
    
    # Step 1: Create and bootstrap model
    print("Step 1: Bootstrap Model")
    print("-" * 30)
    
    model = AdaptiveSupportResistanceModel(config)
    
    # Bootstrap end date (start of our "backtest")
    bootstrap_end = date(2023, 1, 1)
    
    print(f"Bootstrapping model with data ending {bootstrap_end}...")
    
    try:
        # Note: In this demo, bootstrap will fail due to missing database
        # but it demonstrates the workflow
        success = await model.bootstrap_model(
            symbols=symbols,
            end_date=bootstrap_end
        )
        
        if success:
            print("✅ Bootstrap completed successfully")
            info = model.get_model_info()
            print(f"   Model Version: {info['model_version']}")
            print(f"   Training Examples: {info['total_training_examples']}")
        else:
            print("❌ Bootstrap failed")
            
    except Exception as e:
        print(f"❌ Bootstrap failed (expected in demo): {e}")
        print("   In production, this would succeed with proper database connection")
    
    print()
    
    # Step 2: Simulate daily updates
    print("Step 2: Simulate Daily Updates")
    print("-" * 30)
    
    # Simulate several days of updates
    current_date = bootstrap_end
    for day in range(5):
        current_date += timedelta(days=1)
        print(f"Processing {current_date}...")
        
        try:
            # This will also fail without database, but shows the workflow
            updated = await model.daily_update(
                current_date=current_date,
                symbols=symbols
            )
            
            if updated:
                print(f"   ✅ Model updated to version {model.state.model_version}")
            else:
                print("   ⏭️  No update needed")
                
        except Exception as e:
            print(f"   ❌ Update failed (expected in demo): {str(e)[:50]}...")
    
    print()
    
    # Step 3: Show model state
    print("Step 3: Model State Information")
    print("-" * 30)
    
    info = model.get_model_info()
    print(f"Bootstrap Completed: {info['bootstrap_completed']}")
    print(f"Current Model Version: {info['model_version']}")
    print(f"Last Retrain Date: {info['last_retrain_date']}")
    print(f"Training History Count: {info['training_history_count']}")
    print(f"Cache Size: {info['cache_size']}")
    
    if info['recent_performance']:
        print(f"Recent Performance: {info['recent_performance']}")
    
    print()
    
    # Step 4: Explain the adaptive approach
    print("Step 4: Key Benefits of Adaptive Approach")
    print("-" * 30)
    
    benefits = [
        "🔄 Daily Retraining: Model adapts to changing market conditions",
        "📊 Rolling Window: Uses recent data while maintaining historical context", 
        "⚡ Incremental Updates: Efficient training for daily production use",
        "📈 Performance Monitoring: Tracks accuracy and triggers retraining when needed",
        "🎯 Regime Adaptation: Adjusts to different market volatility periods",
        "💾 State Persistence: Can save/load model state for production deployment"
    ]
    
    for benefit in benefits:
        print(f"   {benefit}")
    
    print()
    
    # Step 5: Production considerations
    print("Step 5: Production Implementation")
    print("-" * 30)
    
    considerations = [
        "Database Connection: Requires connection to market data",
        "Computational Resources: Daily training needs sufficient compute",
        "Data Pipeline: Automated feature generation from price/volume data",
        "Monitoring: Track model performance and retrain triggers",
        "Fallback Strategy: Handle training failures gracefully",
        "Version Control: Track model versions and performance history"
    ]
    
    for i, consideration in enumerate(considerations, 1):
        print(f"   {i}. {consideration}")
    
    print()
    print("="*60)
    print("DEMO COMPLETED")
    print("="*60)
    print()
    print("Next Steps:")
    print("- Set up database connection for real data")
    print("- Run full backtesting experiment with historical data")
    print("- Compare adaptive vs static model performance")
    print("- Deploy to production with monitoring")

if __name__ == "__main__":
    asyncio.run(demo_adaptive_model())