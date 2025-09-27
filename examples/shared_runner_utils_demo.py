#!/usr/bin/env python3
"""
Demonstration of shared runner utilities.

This example shows how the new shared utilities eliminate code duplication
across different types of runners.
"""

import asyncio
from core.shared.utils.runner_utils import (
    create_training_data_parser,
    create_market_data_parser,
    initialize_runner,
    add_common_arguments,
    parse_dates,
    validate_output_directory,
    generate_dataset_id,
    create_run_metadata
)


def example_training_data_runner():
    """Example training data runner using shared utilities."""
    print("=== TRAINING DATA RUNNER EXAMPLE ===")
    
    # Create parser with all common arguments included
    parser = create_training_data_parser("Example training data extraction")
    
    # Add any custom arguments specific to this runner
    parser.add_argument('--model-type', choices=['lstm', 'transformer'],
                       default='lstm', help='Model type for training data')
    
    # Initialize everything with shared utility
    # This handles: argument parsing, logging setup, gin config loading, environment setup
    config = initialize_runner(parser, "example_training_runner")
    
    # Now you can immediately start your business logic
    logger = config.logger
    environment = config.environment
    
    logger.info("🎯 Starting training data generation...")
    
    # Use shared utilities for common tasks
    start_date, end_date = parse_dates(config.start_date, config.end_date)
    output_path = validate_output_directory(config.output_dir, logger)
    dataset_id = generate_dataset_id("training")
    metadata = create_run_metadata(config, model_type=config.model_type)
    
    logger.info(f"✅ Training data runner ready: {dataset_id}")
    logger.info(f"   Environment: {environment.env_type}")
    logger.info(f"   Date range: {start_date} to {end_date}")
    logger.info(f"   Output: {output_path}")


def example_market_data_runner():
    """Example market data runner using shared utilities."""
    print("\n=== MARKET DATA RUNNER EXAMPLE ===")
    
    # Create parser with market data specific arguments
    parser = create_market_data_parser("Example market data collection")
    
    # Add custom arguments for this specific runner
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Batch size for data processing')
    
    # Initialize everything with shared utility
    config = initialize_runner(parser, "example_market_data_runner")
    
    logger = config.logger
    environment = config.environment
    
    logger.info("📈 Starting market data collection...")
    
    # Use shared utilities
    start_date, end_date = parse_dates(config.start_date, config.end_date)
    metadata = create_run_metadata(config, 
                                 vendor=getattr(config, 'vendor', 'unknown'),
                                 data_type=config.data_type,
                                 batch_size=config.batch_size)
    
    logger.info(f"✅ Market data runner ready")
    logger.info(f"   Vendor: {getattr(config, 'vendor', 'not specified')}")
    logger.info(f"   Data type: {config.data_type}")
    logger.info(f"   Batch size: {config.batch_size}")


def example_custom_runner():
    """Example of creating a completely custom runner."""
    print("\n=== CUSTOM RUNNER EXAMPLE ===")
    
    # Start with base parser and add only what you need
    import argparse
    parser = argparse.ArgumentParser(description="Custom runner example")
    
    # Add only the common arguments you need
    add_common_arguments(parser)
    
    # Add your specific arguments
    parser.add_argument('--operation', required=True,
                       choices=['backup', 'restore', 'migrate'],
                       help='Operation to perform')
    parser.add_argument('--target', required=True,
                       help='Target for the operation')
    
    # Initialize with shared utility
    config = initialize_runner(parser, "custom_runner")
    
    logger = config.logger
    environment = config.environment
    
    logger.info(f"🔧 Starting {config.operation} operation...")
    
    # Create metadata
    metadata = create_run_metadata(config,
                                 operation=config.operation,
                                 target=config.target)
    
    logger.info(f"✅ Custom runner ready")
    logger.info(f"   Operation: {config.operation}")
    logger.info(f"   Target: {config.target}")


if __name__ == "__main__":
    print("🚀 Shared Runner Utilities Demonstration")
    print("=" * 50)
    
    # Override sys.argv for demonstration
    import sys
    original_argv = sys.argv[:]
    
    try:
        # Example 1: Training Data Runner
        sys.argv = [
            'example_training_runner',
            '--symbols', 'AAPL', 'TSLA',
            '--start-date', '2025-07-01',
            '--end-date', '2025-07-02',
            '--environment', 'dev',
            '--model-type', 'lstm',
            '--debug'
        ]
        example_training_data_runner()
        
        # Example 2: Market Data Runner  
        sys.argv = [
            'example_market_data_runner',
            '--start-date', '2025-07-01',
            '--end-date', '2025-07-02',
            '--environment', 'dev',
            '--vendor', 'polygon',
            '--data-type', 'daily',
            '--batch-size', '50'
        ]
        example_market_data_runner()
        
        # Example 3: Custom Runner
        sys.argv = [
            'custom_runner',
            '--environment', 'dev',
            '--operation', 'backup',
            '--target', 'database'
        ]
        example_custom_runner()
        
        print("\n" + "=" * 50)
        print("✅ All examples completed successfully!")
        print("\n📋 Benefits of shared utilities:")
        print("   • Eliminates 80+ lines of boilerplate per runner")
        print("   • Consistent argument patterns across all runners")
        print("   • Centralized logging and environment setup")
        print("   • Standardized error handling and validation")
        print("   • Easy to maintain and extend")
        
    except Exception as e:
        print(f"\n❌ Error in demonstration: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Restore original argv
        sys.argv = original_argv