#!/usr/bin/env python3
"""
Flyte workflow for enhanced training data generation with technical indicators.

This workflow generates training data with:
- OHLC sequences for past 21 bars
- Technical indicators: etop, ebot, pldot, oneonedot
- Feature distributions for visualization
- Database integration with metadata storage
"""

from typing import Dict, List, Any
from flytekit import task, workflow
from datetime import date, timedelta
import asyncio

@task
def enhanced_training_data_task(
    symbol: str = "AAPL",
    days_back: int = 90,
    sequence_length: int = 21
) -> Dict[str, Any]:
    """
    Enhanced training data generation task with technical indicators.
    
    Args:
        symbol: Stock symbol to generate data for
        days_back: Number of days of historical data
        sequence_length: Length of sequences (21 bars for past 21 bars)
    
    Returns:
        Dictionary with generation results and metadata
    """
    
    print(f"🚀 Starting Enhanced Training Data Generation for {symbol}")
    print(f"Parameters: days_back={days_back}, sequence_length={sequence_length}")
    
    # Import here to avoid issues with Flyte serialization
    import sys
    import os
    sys.path.append('/scripts')  # Add scripts directory to path
    
    try:
        from src.app.enhanced_training_data_generator import run_enhanced_training_data_job_for_symbol
        
        # Run the enhanced training data generation
        async def run_generation():
            return await run_enhanced_training_data_job_for_symbol(
                symbol=symbol, 
                days_back=days_back
            )
        
        # Execute in async context
        results = asyncio.run(run_generation())
        
        print(f"✅ Enhanced training data generation results:")
        print(f"  Status: {results['status']}")
        
        if results['status'] == 'success':
            print(f"  Run ID: {results['run_id']}")
            print(f"  Dataset IDs: {results['dataset_ids']}")
            print(f"  Features Shape: {results['features_shape']}")
            print(f"  Labels Shape: {results['labels_shape']}")
            
            # Extract metadata for logging
            metadata = results.get('metadata', {})
            if 'feature_names' in metadata:
                print(f"  Feature Names: {metadata['feature_names']}")
            if 'technical_indicators' in metadata:
                indicators = list(metadata['technical_indicators'].keys())
                print(f"  Technical Indicators: {indicators}")
                
            print("\n🎉 Enhanced features generated successfully:")
            print("  • OHLC sequences (21 bars)")
            print("  • Elliott Top (etop) - reversal indicator")
            print("  • Elliott Bottom (ebot) - reversal indicator")
            print("  • Pivot Line Dot (pldot) - momentum indicator") 
            print("  • One-One-Dot (oneonedot) - custom oscillator")
            print("  • Feature distributions for visualization")
            print("  • Database metadata storage")
        else:
            print(f"  Error: {results.get('error', 'Unknown error')}")
        
        return results
        
    except Exception as e:
        error_msg = f"Enhanced training data generation failed: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        traceback.print_exc()
        
        return {
            'status': 'error',
            'error': error_msg,
            'symbol': symbol,
            'days_back': days_back
        }

@workflow
def enhanced_training_data_workflow(
    symbol: str = "AAPL",
    days_back: int = 90,
    sequence_length: int = 21
) -> Dict[str, Any]:
    """
    Enhanced training data generation workflow.
    
    This workflow generates comprehensive training data with:
    1. OHLC price sequences for the past 21 bars
    2. Technical indicators (etop, ebot, pldot, oneonedot)
    3. Feature distributions for web app visualization
    4. Database storage with enhanced metadata
    
    Args:
        symbol: Stock symbol (default: AAPL)
        days_back: Historical data period (default: 90 days)
        sequence_length: Sequence length (default: 21 bars)
    
    Returns:
        Training data generation results with metadata
    """
    
    print(f"📊 Enhanced Training Data Workflow")
    print(f"Symbol: {symbol}")
    print(f"Period: {days_back} days")
    print(f"Sequence Length: {sequence_length} bars")
    print()
    print("Features to be generated:")
    print("  • OHLC (Open, High, Low, Close)")
    print("  • Volume")
    print("  • Elliott Top (etop) - 21 periods")
    print("  • Elliott Bottom (ebot) - 21 periods")
    print("  • Pivot Line Dot (pldot) - 21 periods")
    print("  • One-One-Dot (oneonedot) - 21 periods")
    print()
    
    # Execute enhanced training data generation task
    results = enhanced_training_data_task(
        symbol=symbol,
        days_back=days_back,
        sequence_length=sequence_length
    )
    
    return results

if __name__ == "__main__":
    # Test the workflow locally
    print("🧪 Testing Enhanced Training Data Workflow Locally")
    result = enhanced_training_data_workflow(symbol="AAPL", days_back=90)
    print(f"Final Result: {result}")