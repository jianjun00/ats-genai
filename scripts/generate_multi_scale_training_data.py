#!/usr/bin/env python3
"""
Multi-Scale Training Data Generation Script

Generates training data compatible with the new multi-scale sequence models.
Integrates with the existing training pipeline while supporting advanced features.

Usage:
python scripts/generate_multi_scale_training_data.py [options]
"""

import sys
import asyncio
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add src to Python path
sys.path.insert(0, 'src')

from app.enhanced_training_data_generator import (
    generate_enhanced_training_data, 
    MultiScaleDataGenerator,
    EnhancedTrainingConfig
)
from storage.multi_scale_sequence import TimeScale
from config.feature_flags import is_enabled, feature_manager
from dao.training_dataset_dao import TrainingDatasetDAO, TrainingDatasetRecord
from config.environment import Environment

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


async def generate_and_save_to_database(
    symbols: list,
    output_dir: str,
    days_back: int,
    scales: list,
    save_to_db: bool = True
) -> dict:
    """Generate training data and optionally save to database."""
    
    logger.info(f"Starting multi-scale training data generation")
    logger.info(f"Symbols: {symbols}")
    logger.info(f"Days back: {days_back}")
    logger.info(f"Scales: {[s.value for s in scales]}")
    logger.info(f"Feature flags - Agents: {is_enabled('enable_agent_networks')}, LLM: {is_enabled('enable_llm_events')}")
    
    # Generate data
    result = await generate_enhanced_training_data(
        symbols=symbols,
        output_dir=output_dir,
        days_back=days_back,
        scales=scales
    )
    
    if result['status'] != 'success':
        logger.error(f"Training data generation failed: {result.get('error')}")
        return result
    
    # Save to database if requested
    if save_to_db:
        try:
            env = Environment()
            dao = TrainingDatasetDAO(env)
            
            metadata = result['metadata']
            
            # Create training dataset record
            record = TrainingDatasetRecord(
                dataset_name=f"multi_scale_{'_'.join(symbols)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                feature_metadata={
                    'scales': metadata['scales_used'],
                    'feature_names': metadata['feature_names'],
                    'technical_indicators': metadata['technical_indicators'],
                    'prediction_horizons': metadata['prediction_horizons']
                },
                features_shape=metadata['feature_shape'],
                labels_shape=metadata['label_shape'],
                dataset_size=metadata['total_sequences'],
                creation_timestamp=datetime.now(),
                tfdv_statistics=None,  # Could add TensorFlow Data Validation stats
                tfdv_anomalies=None,
                model_performance_baseline=None,
                model_performance_metadata=metadata
            )
            
            # Save to database
            dataset_id = await dao.create_training_dataset(record)
            
            logger.info(f"Training dataset saved to database with ID: {dataset_id}")
            result['dataset_id'] = dataset_id
            
        except Exception as e:
            logger.error(f"Failed to save to database: {e}")
            # Don't fail the entire operation just because DB save failed
            result['database_error'] = str(e)
    
    return result


def parse_scales(scale_str: str) -> list:
    """Parse scale string into TimeScale objects."""
    scale_map = {
        'minute': TimeScale.MINUTE,
        'hourly': TimeScale.HOURLY, 
        'daily': TimeScale.DAILY,
        'weekly': TimeScale.WEEKLY
    }
    
    scales = []
    for scale_name in scale_str.split(','):
        scale_name = scale_name.strip().lower()
        if scale_name in scale_map:
            scales.append(scale_map[scale_name])
        else:
            raise ValueError(f"Unknown scale: {scale_name}")
    
    return scales


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Generate multi-scale training data')
    
    parser.add_argument('symbols', nargs='+', 
                       help='Stock symbols to generate data for (e.g., AAPL MSFT GOOGL)')
    
    parser.add_argument('--output-dir', default='multi_scale_training_data',
                       help='Output directory for training data (default: multi_scale_training_data)')
    
    parser.add_argument('--days-back', type=int, default=90,
                       help='Number of days of historical data to generate (default: 90)')
    
    parser.add_argument('--scales', default='hourly,daily',
                       help='Comma-separated list of scales: minute,hourly,daily,weekly (default: hourly,daily)')
    
    parser.add_argument('--save-to-db', action='store_true',
                       help='Save training dataset record to database')
    
    parser.add_argument('--enable-agents', action='store_true',
                       help='Enable agent network features (overrides feature flags)')
    
    parser.add_argument('--enable-llm', action='store_true',
                       help='Enable LLM event analysis (overrides feature flags)')
    
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Override feature flags if requested
    if args.enable_agents:
        feature_manager.override_flag("enable_agent_networks", True)
        feature_manager.override_flag("enable_portfolio_agents", True)
        logger.info("Enabled agent network features via command line")
    
    if args.enable_llm:
        feature_manager.override_flag("enable_llm_events", True)
        feature_manager.override_flag("enable_adaptive_selection", True)
        feature_manager.override_flag("enable_event_reflection", True)
        logger.info("Enabled LLM event analysis features via command line")
    
    try:
        # Parse scales
        scales = parse_scales(args.scales)
        
        # Convert symbols to uppercase
        symbols = [s.upper() for s in args.symbols]
        
        # Generate training data
        result = await generate_and_save_to_database(
            symbols=symbols,
            output_dir=args.output_dir,
            days_back=args.days_back,
            scales=scales,
            save_to_db=args.save_to_db
        )
        
        # Print results
        if result['status'] == 'success':
            print(f"\n✅ Multi-scale training data generation completed successfully!")
            print(f"📊 Features shape: {result['features'].shape}")
            print(f"🎯 Labels shape: {result['labels'].shape}")
            print(f"⏱️  Generation time: {result['metadata']['generation_time']:.2f}s")
            print(f"📁 Output directory: {args.output_dir}")
            
            if 'dataset_id' in result:
                print(f"💾 Database ID: {result['dataset_id']}")
            
            # Print feature summary
            metadata = result['metadata']
            print(f"\n📋 Dataset Details:")
            print(f"  Symbols: {', '.join(metadata['symbols'])}")
            print(f"  Scales: {', '.join(metadata['scales_used'])}")
            print(f"  Total sequences: {metadata['total_sequences']:,}")
            print(f"  Features per sequence: {metadata['feature_shape'][2]}")
            print(f"  Prediction horizons: {', '.join(metadata['label_names'])}")
            
            # Advanced features
            print(f"\n🚀 Advanced Features:")
            print(f"  Events: {'✅' if metadata['events_enabled'] else '❌'}")
            print(f"  Agent features: {'✅' if metadata['agent_features_enabled'] else '❌'}")
            print(f"  LLM analysis: {'✅' if metadata['llm_analysis_enabled'] else '❌'}")
            print(f"  Technical indicators: {len(metadata['technical_indicators'])}")
            
            # Per-symbol results
            print(f"\n📈 Per-Symbol Results:")
            for symbol, symbol_result in metadata['symbol_results'].items():
                if 'error' in symbol_result:
                    print(f"  {symbol}: ❌ {symbol_result['error']}")
                else:
                    print(f"  {symbol}: ✅ {symbol_result['sequences_created']} sequences, "
                          f"{len(symbol_result['scales'])} scales, {symbol_result['events']} events")
        
        else:
            print(f"\n❌ Training data generation failed!")
            print(f"Error: {result.get('error', 'Unknown error')}")
            if 'symbol_results' in result:
                for symbol, symbol_result in result['symbol_results'].items():
                    if 'error' in symbol_result:
                        print(f"  {symbol}: {symbol_result['error']}")
    
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())