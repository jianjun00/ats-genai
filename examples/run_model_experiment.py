"""
Example: Running Model Configuration Experiments

This script demonstrates how to use the ModelExperimentFramework to:
1. Test whether adding SPY/QQQ signals improves portfolio performance
2. Generate comprehensive performance attribution analysis
3. Create visualizations showing individual stock trade rationale
4. Compare baseline vs experimental configurations with statistical significance

Usage:
    PYTHONPATH=src python examples/run_model_experiment.py
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from ml.evaluation.experiment_framework import (
    ModelExperimentFramework,
    ExperimentConfig,
    create_spy_qqq_experiment_configs
)


async def run_spy_qqq_experiment():
    """
    Run A/B test to see if adding SPY/QQQ signals improves performance
    """
    print("🚀 Starting SPY/QQQ Signal Experiment")
    print("=" * 60)
    
    # Initialize the experiment framework
    framework = ModelExperimentFramework(output_dir="experiment_results/spy_qqq_test")
    
    # Create baseline and experimental configurations
    baseline_config, experimental_config = create_spy_qqq_experiment_configs()
    
    print(f"📊 Baseline: {baseline_config.experiment_name}")
    print(f"📈 Experimental: {experimental_config.experiment_name}")
    print()
    
    # Show configuration differences
    feature_diff = baseline_config.get_feature_diff(experimental_config)
    print("🔍 Configuration Differences:")
    for feature, (baseline_val, experimental_val) in feature_diff.items():
        change = "✅ Enabled" if experimental_val else "❌ Disabled"
        print(f"  - {feature}: {change} in experimental")
    print()
    
    # Run the A/B test
    print("🏃‍♂️ Running A/B test (this may take a few minutes)...")
    comparison_result = await framework.run_ab_test(
        baseline_config=baseline_config,
        experimental_config=experimental_config,
        run_parallel=True  # Run both experiments in parallel for faster results
    )
    
    # Display results
    print("\n" + "=" * 60)
    print("📊 EXPERIMENT RESULTS")
    print("=" * 60)
    
    # Performance summary
    baseline = comparison_result.baseline_result
    experimental = comparison_result.experimental_result
    
    print(f"\n📈 Performance Comparison:")
    print(f"  Baseline Return:     {baseline.total_return:>8.2%}")
    print(f"  Experimental Return: {experimental.total_return:>8.2%}")
    print(f"  Difference:          {comparison_result.return_difference:>8.2%}")
    
    print(f"\n⚡ Risk-Adjusted Performance:")
    print(f"  Baseline Sharpe:     {baseline.sharpe_ratio:>8.2f}")
    print(f"  Experimental Sharpe: {experimental.sharpe_ratio:>8.2f}")
    print(f"  Difference:          {comparison_result.sharpe_difference:>8.2f}")
    
    print(f"\n📉 Risk Metrics:")
    print(f"  Baseline Max DD:     {baseline.max_drawdown:>8.2%}")
    print(f"  Experimental Max DD: {experimental.max_drawdown:>8.2%}")
    print(f"  Difference:          {comparison_result.risk_difference:>8.2%}")
    
    # Statistical significance
    significance = "✅ SIGNIFICANT" if comparison_result.is_significant else "❌ NOT SIGNIFICANT"
    print(f"\n📊 Statistical Analysis:")
    print(f"  T-Statistic:         {comparison_result.return_t_stat:>8.3f}")
    print(f"  P-Value:             {comparison_result.return_p_value:>8.4f}")
    print(f"  Significance:        {significance}")
    
    # Performance attribution
    if comparison_result.performance_attribution:
        print(f"\n🎯 Performance Attribution (Top Contributors):")
        sorted_attribution = sorted(
            comparison_result.performance_attribution.items(),
            key=lambda x: abs(x[1]),
            reverse=True
        )
        
        for feature, contribution in sorted_attribution[:5]:
            direction = "📈" if contribution > 0 else "📉"
            print(f"  {direction} {feature:<20}: {contribution:>8.4f}")
    
    # Trade insights
    experimental_trades = experimental.trade_explanations
    if experimental_trades:
        print(f"\n💡 Trade Insights:")
        print(f"  Total Trades:        {len(experimental_trades):>8}")
        avg_confidence = sum(t.confidence for t in experimental_trades) / len(experimental_trades)
        print(f"  Avg Confidence:      {avg_confidence:>8.2%}")
        
        # Show top trade
        top_trade = max(experimental_trades, key=lambda t: t.confidence)
        print(f"\n🏆 Highest Confidence Trade:")
        print(f"  Symbol:              {top_trade.symbol}")
        print(f"  Action:              {top_trade.action.upper()}")
        print(f"  Date:                {top_trade.date.strftime('%Y-%m-%d')}")
        print(f"  Confidence:          {top_trade.confidence:.2%}")
        print(f"  Position Size:       {top_trade.position_size:.2%}")
    
    # Generate summary report
    print(f"\n📄 Generating comprehensive report...")
    report = comparison_result.generate_comparison_report()
    
    # Show key conclusion
    print("\n" + "=" * 60)
    print("🎯 KEY CONCLUSION")
    print("=" * 60)
    
    if comparison_result.return_difference > 0.01:  # 1% improvement
        if comparison_result.is_significant:
            conclusion = "✅ STRONG POSITIVE RESULT: Adding SPY/QQQ signals significantly improves performance!"
        else:
            conclusion = "📈 POSITIVE TREND: SPY/QQQ signals show promise but need more data for statistical significance"
    elif comparison_result.return_difference > 0:
        conclusion = "🔍 MARGINAL IMPROVEMENT: Minor performance gain from SPY/QQQ signals"
    else:
        conclusion = "❌ NEGATIVE RESULT: SPY/QQQ signals do not improve performance"
    
    print(conclusion)
    
    # File locations
    output_dir = Path("experiment_results/spy_qqq_test")
    print(f"\n📁 Detailed results saved to: {output_dir.absolute()}")
    print(f"📊 Charts and visualizations: {output_dir / '*.png'}")
    print(f"📄 Full report: {output_dir / 'comparison_report_*.md'}")
    print(f"🔍 Individual trade analysis: {output_dir / 'trade_*'}")
    
    return comparison_result


async def run_comprehensive_feature_test():
    """
    Run comprehensive test across multiple feature combinations
    """
    print("\n🔬 Running Comprehensive Feature Analysis")
    print("=" * 60)
    
    framework = ModelExperimentFramework(output_dir="experiment_results/comprehensive_test")
    
    # Define multiple experimental configurations
    experiments = [
        # Test 1: SPY/QQQ signals
        ExperimentConfig(
            experiment_name="with_spy_qqq",
            description="Test SPY/QQQ correlation signals",
            features={"spy_qqq_signals": True, "technical_indicators": True, "momentum_features": True}
        ),
        
        # Test 2: News sentiment
        ExperimentConfig(
            experiment_name="with_news_sentiment",
            description="Test news sentiment analysis",
            features={"news_sentiment": True, "technical_indicators": True, "momentum_features": True}
        ),
        
        # Test 3: Sector rotation
        ExperimentConfig(
            experiment_name="with_sector_rotation",
            description="Test sector rotation signals",
            features={"sector_rotation": True, "technical_indicators": True, "momentum_features": True}
        ),
        
        # Test 4: All advanced features
        ExperimentConfig(
            experiment_name="kitchen_sink",
            description="All features enabled",
            features={
                "spy_qqq_signals": True,
                "technical_indicators": True,
                "news_sentiment": True,
                "sector_rotation": True,
                "volatility_regime": True,
                "momentum_features": True,
                "mean_reversion": True,
                "options_flow": True,
                "alternative_data": True,
                "risk_parity": True
            }
        )
    ]
    
    # Baseline configuration
    baseline = ExperimentConfig(
        experiment_name="baseline_minimal",
        description="Minimal baseline with core features only",
        features={"technical_indicators": True, "momentum_features": True}
    )
    
    # Run all experiments against baseline
    results = []
    for exp_config in experiments:
        print(f"\n🧪 Testing: {exp_config.experiment_name}")
        comparison = await framework.run_ab_test(baseline, exp_config, run_parallel=False)
        results.append(comparison)
        
        # Quick summary
        improvement = "✅" if comparison.return_difference > 0 else "❌"
        print(f"   Return Difference: {comparison.return_difference:+.2%} {improvement}")
    
    # Generate comprehensive summary
    print(f"\n📊 COMPREHENSIVE RESULTS SUMMARY")
    print("=" * 60)
    print(f"{'Experiment':<25} {'Return Diff':<12} {'Sharpe Diff':<12} {'Significant':<12}")
    print("-" * 60)
    
    for comparison in results:
        exp_name = comparison.experimental_result.config.experiment_name
        return_diff = comparison.return_difference
        sharpe_diff = comparison.sharpe_difference
        significant = "✅" if comparison.is_significant else "❌"
        
        print(f"{exp_name:<25} {return_diff:+8.2%}     {sharpe_diff:+8.2f}     {significant}")
    
    # Overall framework summary
    framework_summary = framework.generate_experiment_summary()
    summary_path = Path("experiment_results/comprehensive_test") / "framework_summary.md"
    
    with open(summary_path, 'w') as f:
        f.write(framework_summary)
    
    print(f"\n📄 Complete summary saved to: {summary_path.absolute()}")
    
    return results


async def main():
    """Main execution function"""
    print("🤖 Model Configuration Experiment Framework")
    print("=" * 60)
    print("This script demonstrates configuration-driven model backtesting")
    print("with performance attribution and trade explanation.")
    print()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Run SPY/QQQ experiment
        spy_qqq_result = await run_spy_qqq_experiment()
        
        # Optionally run comprehensive test
        print(f"\n🤔 Would you like to run a comprehensive feature test?")
        print("   This will test multiple feature combinations (takes longer)")
        
        # For demo purposes, we'll skip the comprehensive test
        # In practice, user could choose interactively
        run_comprehensive = False  # Set to True to run comprehensive test
        
        if run_comprehensive:
            comprehensive_results = await run_comprehensive_feature_test()
        
        print(f"\n🎉 Experiment completed successfully!")
        print("   Check the output directories for detailed analysis and visualizations.")
        
    except Exception as e:
        print(f"\n❌ Error running experiment: {e}")
        logging.exception("Experiment failed")
        return 1
    
    return 0


if __name__ == "__main__":
    # Run the experiment
    exit_code = asyncio.run(main())
    sys.exit(exit_code)