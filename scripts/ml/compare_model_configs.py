#!/usr/bin/env python3
"""
Model Configuration Comparison CLI

This script provides an easy-to-use interface for comparing model configurations
and running backtests to evaluate performance differences.

Usage Examples:
    # Compare two predefined templates
    python scripts/ml/compare_model_configs.py \
        --baseline sr_baseline \
        --test sr_enhanced \
        --start-date 2023-01-01 \
        --end-date 2024-06-30

    # Compare using custom config files
    python scripts/ml/compare_model_configs.py \
        --baseline-config configs/my_baseline.yaml \
        --test-config configs/my_test.yaml \
        --start-date 2023-01-01 \
        --end-date 2024-06-30

    # Compare multiple variants
    python scripts/ml/compare_model_configs.py \
        --baseline sr_baseline \
        --test-variants sr_enhanced,sr_deep,sr_wide \
        --start-date 2023-01-01 \
        --end-date 2024-06-30
"""

import sys
import os
import asyncio
import argparse
import logging
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict, Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from ml.evaluation.model_config_comparison import (
    ModelConfigComparison, 
    ModelConfigManager, 
    ModelConfigDefinition,
    ConfigFactory
)
from dao.universe_dao import UniverseDAO
from config.environment import Environment


class ModelComparisonCLI:
    """CLI interface for model configuration comparisons"""
    
    def __init__(self):
        self.comparison = ModelConfigComparison()
        self.manager = ModelConfigManager()
        self.logger = logging.getLogger(__name__)
        
    async def run_comparison(self, args) -> Dict[str, Any]:
        """Run the model comparison based on CLI arguments"""
        
        # Parse dates
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        
        # Get universe
        universe = await self._get_universe(args.universe, args.universe_size)
        
        # Setup baseline configuration
        if args.baseline_config:
            baseline_config = self.manager.load_config(args.baseline_config)
        else:
            baseline_config = self.manager.get_template(args.baseline)
            if not baseline_config:
                raise ValueError(f"Baseline template '{args.baseline}' not found")
        
        self.comparison.add_baseline_config("baseline", baseline_config)
        
        # Setup test configurations
        test_configs = []
        
        if args.test_config:
            test_config = self.manager.load_config(args.test_config)
            test_configs.append(("test", test_config))
        
        elif args.test:
            test_config = self.manager.get_template(args.test)
            if not test_config:
                raise ValueError(f"Test template '{args.test}' not found")
            test_configs.append(("test", test_config))
        
        elif args.test_variants:
            variants = args.test_variants.split(',')
            for variant_name in variants:
                variant_config = self.manager.get_template(variant_name.strip())
                if variant_config:
                    test_configs.append((variant_name.strip(), variant_config))
                else:
                    self.logger.warning(f"Variant '{variant_name}' not found, skipping")
        
        elif args.create_variants:
            # Create variants automatically
            test_configs = await self._create_automatic_variants(baseline_config, args)
        
        # Add all test configurations
        for name, config in test_configs:
            self.comparison.add_test_config(name, config)
        
        # Run comparisons
        self.logger.info(f"Running comparisons for {len(test_configs)} test configurations")
        
        results = await self.comparison.run_comparative_backtest(
            start_date=start_date,
            end_date=end_date,
            universe=universe,
            initial_capital=args.initial_capital
        )
        
        # Generate summary
        self._print_results_summary(results)
        
        return results
    
    async def _get_universe(self, universe_name: str, universe_size: int) -> List[str]:
        """Get trading universe"""
        if universe_name:
            try:
                universe_dao = UniverseDAO()
                universe_data = await universe_dao.get_universe_by_name(universe_name)
                if universe_data:
                    symbols = [item['symbol'] for item in universe_data]
                    if universe_size and len(symbols) > universe_size:
                        return symbols[:universe_size]
                    return symbols
            except Exception as e:
                self.logger.warning(f"Could not load universe '{universe_name}': {e}")
        
        # Default universe
        default_symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM',
            'JNJ', 'V', 'UNH', 'HD', 'PG', 'DIS', 'MA', 'BAC', 'NFLX', 'CRM'
        ]
        
        if universe_size and len(default_symbols) > universe_size:
            return default_symbols[:universe_size]
        
        return default_symbols
    
    async def _create_automatic_variants(self, baseline_config: ModelConfigDefinition, args) -> List[tuple]:
        """Create automatic variants of the baseline configuration"""
        variants = []
        
        if baseline_config.model_type.value == "support_resistance":
            # Learning rate variants
            if args.lr_variants:
                rates = [float(r) for r in args.lr_variants.split(',')]
                lr_variants = ConfigFactory.create_learning_rate_variants(baseline_config, rates)
                for variant in lr_variants:
                    variants.append((variant.name, variant))
            
            # Architecture variants
            if args.arch_variants:
                architectures = []
                for arch_str in args.arch_variants.split(';'):
                    arch = [int(x) for x in arch_str.split(',')]
                    architectures.append(arch)
                
                arch_variants = ConfigFactory.create_architecture_variants(baseline_config, architectures)
                for variant in arch_variants:
                    variants.append((variant.name, variant))
            
            # Default variants if none specified
            if not args.lr_variants and not args.arch_variants:
                # Create some common variants
                lr_variants = ConfigFactory.create_learning_rate_variants(
                    baseline_config, [0.001, 0.0005, 0.002]
                )
                for variant in lr_variants[:2]:  # Limit to 2 variants
                    variants.append((variant.name, variant))
        
        return variants
    
    def _print_results_summary(self, results: Dict[str, Any]):
        """Print a summary of the comparison results"""
        
        print("\n" + "="*80)
        print("MODEL CONFIGURATION COMPARISON RESULTS")
        print("="*80)
        
        for comparison_name, result in results.items():
            print(f"\n📊 {comparison_name.upper()}")
            print("-" * 60)
            
            # Key metrics comparison
            baseline_metrics = result.baseline_metrics
            test_metrics = result.test_metrics
            
            print(f"BASELINE ({result.baseline_config.name}):")
            self._print_metrics(baseline_metrics)
            
            print(f"\nTEST ({result.test_config.name}):")
            self._print_metrics(test_metrics)
            
            print(f"\nPERFORMANCE DIFFERENCES:")
            for key, value in result.performance_diff.items():
                if '_pct_diff' in key and abs(value) > 1:  # Show significant differences
                    metric = key.replace('_pct_diff', '').replace('_', ' ').title()
                    direction = "↑" if value > 0 else "↓"
                    print(f"  {metric}: {direction} {abs(value):.1f}%")
            
            print(f"\nRECOMMENDations:")
            for rec in result.recommendations:
                print(f"  {rec}")
            
            print("\n" + "-" * 60)
        
        print(f"\n✅ Comparison completed. Detailed results saved to comparison_results/")
    
    def _print_metrics(self, metrics: Dict[str, float]):
        """Print formatted metrics"""
        key_metrics = ['total_return', 'sharpe_ratio', 'max_drawdown', 'win_rate']
        
        for metric in key_metrics:
            if metric in metrics:
                value = metrics[metric]
                if metric == 'total_return':
                    print(f"  Total Return: {value:.1%}")
                elif metric == 'sharpe_ratio':
                    print(f"  Sharpe Ratio: {value:.2f}")
                elif metric == 'max_drawdown':
                    print(f"  Max Drawdown: {value:.1%}")
                elif metric == 'win_rate':
                    print(f"  Win Rate: {value:.1%}")


def main():
    parser = argparse.ArgumentParser(
        description="Compare model configurations using backtesting",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Basic comparison options
    parser.add_argument('--baseline', type=str, help='Baseline config template name')
    parser.add_argument('--baseline-config', type=str, help='Path to baseline config file')
    parser.add_argument('--test', type=str, help='Test config template name')
    parser.add_argument('--test-config', type=str, help='Path to test config file')
    parser.add_argument('--test-variants', type=str, help='Comma-separated list of test variant names')
    
    # Automatic variant creation
    parser.add_argument('--create-variants', action='store_true', help='Create variants automatically')
    parser.add_argument('--lr-variants', type=str, help='Learning rates for variants (e.g., "0.001,0.0005,0.002")')
    parser.add_argument('--arch-variants', type=str, help='Architectures for variants (e.g., "256,128;512,256,128")')
    
    # Backtest parameters
    parser.add_argument('--start-date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--universe', type=str, help='Universe name from database')
    parser.add_argument('--universe-size', type=int, help='Maximum universe size')
    parser.add_argument('--initial-capital', type=float, default=1000000.0, help='Initial capital')
    
    # Output options
    parser.add_argument('--output-dir', type=str, default='comparison_results', help='Output directory')
    parser.add_argument('--save-configs', action='store_true', help='Save generated configs to files')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    # Validation
    if not any([args.baseline, args.baseline_config]):
        parser.error("Must specify either --baseline or --baseline-config")
    
    if not any([args.test, args.test_config, args.test_variants, args.create_variants]):
        parser.error("Must specify test configuration(s)")
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run comparison
    cli = ModelComparisonCLI()
    
    try:
        results = asyncio.run(cli.run_comparison(args))
        
        # Save configs if requested
        if args.save_configs:
            output_dir = Path(args.output_dir)
            for comparison_name, result in results.items():
                baseline_path = cli.manager.save_config(result.baseline_config, "yaml")
                test_path = cli.manager.save_config(result.test_config, "yaml")
                print(f"\n💾 Saved configs:")
                print(f"   Baseline: {baseline_path}")
                print(f"   Test: {test_path}")
        
        print(f"\n🎉 Model configuration comparison completed successfully!")
        return 0
        
    except Exception as e:
        logging.error(f"Comparison failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())