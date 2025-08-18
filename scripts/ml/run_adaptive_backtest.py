#!/usr/bin/env python3
"""
Adaptive Model Backtesting Script

This script runs a comprehensive backtesting experiment that compares:
1. Adaptive model (daily retraining)
2. Static model (periodic retraining)

The experiment simulates realistic production conditions where models
are retrained during backtesting to adapt to market changes.
"""

import os
import sys
import asyncio
import logging
import argparse
from datetime import date, datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from ml.evaluation.adaptive_backtester import AdaptiveBacktester, AdaptiveBacktestConfig
from ml.dynamic_training.adaptive_sr_model import AdaptiveModelConfig
from universe.historical_universe_creator import HistoricalUniverseCreator

async def create_test_universe(universe_size: int = 20) -> list[str]:
    """Create a test universe for backtesting"""
    logger = logging.getLogger(__name__)
    logger.info(f"Creating test universe with {universe_size} symbols")
    
    # Use historical universe creator to get bias-free universe
    creator = HistoricalUniverseCreator()
    
    try:
        # Get stocks that were available in 2020
        active_stocks_2020 = await creator.get_active_stocks_in_year(
            year=2020,
            min_market_cap_millions=1000,  # $1B+ market cap
            min_avg_volume=200000,         # 200k+ daily volume
            min_trading_days=200           # At least 200 trading days
        )
        
        if len(active_stocks_2020) < universe_size:
            logger.warning(f"Only {len(active_stocks_2020)} stocks available, using all")
            universe_size = len(active_stocks_2020)
        
        # Sample using market cap weighting
        sampled_stocks = creator._sample_stocks_by_market_cap(active_stocks_2020, universe_size)
        symbols = [stock.symbol for stock in sampled_stocks]
        
        logger.info(f"Selected universe: {symbols}")
        return symbols
        
    except Exception as e:
        logger.warning(f"Failed to create historical universe: {e}")
        # Fallback to common large-cap stocks
        fallback_symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 
            'JNJ', 'V', 'PG', 'UNH', 'HD', 'MA', 'DIS', 'PYPL', 'BAC', 'NFLX', 'ADBE', 'CRM'
        ]
        return fallback_symbols[:universe_size]

async def main():
    """Main function to run adaptive backtesting experiment"""
    
    parser = argparse.ArgumentParser(
        description="Run adaptive model backtesting experiment"
    )
    
    parser.add_argument('--universe-size', type=int, default=15,
                       help='Number of stocks in test universe (default: 15)')
    parser.add_argument('--backtest-start', default='2023-01-01',
                       help='Backtest start date (YYYY-MM-DD)')
    parser.add_argument('--backtest-end', default='2024-06-30',
                       help='Backtest end date (YYYY-MM-DD)')
    parser.add_argument('--bootstrap-years', type=int, default=3,
                       help='Years of data for initial model training (default: 3)')
    parser.add_argument('--output-dir', default='adaptive_backtest_results',
                       help='Output directory for results')
    parser.add_argument('--skip-static', action='store_true',
                       help='Skip static model comparison (faster)')
    parser.add_argument('--save-models', action='store_true',
                       help='Save trained models')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--quick-test', action='store_true',
                       help='Run quick test with limited data')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    # Set environment
    os.environ['ENVIRONMENT'] = 'dev'
    
    # Parse dates
    try:
        backtest_start = datetime.strptime(args.backtest_start, '%Y-%m-%d').date()
        backtest_end = datetime.strptime(args.backtest_end, '%Y-%m-%d').date()
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        return 1
    
    # Quick test adjustments
    if args.quick_test:
        args.universe_size = min(args.universe_size, 5)
        # Limit to 1 month for quick test
        from datetime import timedelta
        backtest_end = min(backtest_end, backtest_start + timedelta(days=30))
        logger.info("Quick test mode: limited universe and time period")
    
    try:
        print("="*80)
        print("ADAPTIVE MODEL BACKTESTING EXPERIMENT")
        print("="*80)
        print(f"Backtest Period: {backtest_start} to {backtest_end}")
        print(f"Bootstrap Years: {args.bootstrap_years}")
        print(f"Universe Size: {args.universe_size}")
        print(f"Compare Static Model: {not args.skip_static}")
        print(f"Output Directory: {args.output_dir}")
        print("="*80)
        
        # Step 1: Create test universe
        print("\n🏗️  Step 1: Creating Test Universe...")
        universe = await create_test_universe(args.universe_size)
        print(f"✅ Universe created: {len(universe)} symbols")
        
        # Step 2: Configure experiment
        print("\n⚙️  Step 2: Configuring Experiment...")
        
        # Configure adaptive model
        adaptive_config = AdaptiveModelConfig(
            bootstrap_years=args.bootstrap_years,
            min_bootstrap_examples=1000,
            rolling_window_days=365,
            retrain_frequency_days=1,  # Daily retraining
            min_retrain_examples=50
        )
        
        # Configure backtesting experiment
        backtest_config = AdaptiveBacktestConfig(
            backtest_start_date=backtest_start,
            backtest_end_date=backtest_end,
            bootstrap_years=args.bootstrap_years,
            symbols=universe,
            max_symbols=len(universe),
            compare_static_model=not args.skip_static,
            static_retrain_frequency_days=7,  # Weekly for comparison
            save_predictions=True,
            save_models=args.save_models,
            output_dir=args.output_dir,
            adaptive_config=adaptive_config
        )
        
        print("✅ Experiment configured")
        
        # Step 3: Run backtesting experiment
        print("\n🔬 Step 3: Running Backtesting Experiment...")
        print("This may take a while depending on the period length...")
        
        backtester = AdaptiveBacktester(backtest_config)
        results = await backtester.run_adaptive_backtest()
        
        print("✅ Backtesting completed")
        
        # Step 4: Generate report
        print("\n📊 Step 4: Generating Results Report...")
        
        report = backtester.generate_report(results)
        
        # Save report
        report_path = f"{args.output_dir}/adaptive_backtest_report.md"
        with open(report_path, 'w') as f:
            f.write(report)
        
        print(f"✅ Report generated: {report_path}")
        
        # Step 5: Display summary
        print("\n" + "="*80)
        print("🎉 EXPERIMENT COMPLETED!")
        print("="*80)
        
        # Print key results
        adaptive = results['adaptive_model']
        print(f"📈 Adaptive Model Updates: {adaptive['total_updates']}")
        print(f"📊 Update Frequency: {adaptive['update_frequency']:.1%} of days")
        
        if results.get('static_model'):
            static = results['static_model']
            print(f"📈 Static Model Updates: {static['total_updates']}")
            print(f"📊 Static Update Frequency: {static['update_frequency']:.1%} of days")
        
        if results.get('performance_comparison'):
            comp = results['performance_comparison']
            if 'accuracy' in comp:
                acc = comp['accuracy']
                print(f"🏆 Adaptive Wins: {acc['adaptive_wins']}/{acc['total_comparisons']} days")
        
        proc = results['processing_stats']
        print(f"⏱️  Average Processing Time: {proc['avg_processing_time_seconds']:.2f}s per day")
        
        print(f"📁 All results saved to: {args.output_dir}")
        print("="*80)
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⚠️  Experiment cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"Experiment failed: {e}")
        if args.debug:
            raise
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))