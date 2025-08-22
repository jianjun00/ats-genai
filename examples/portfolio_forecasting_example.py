#!/usr/bin/env python3
"""
Portfolio Forecasting System - Complete Example

Demonstrates the complete market-neutral portfolio system that generates
hourly recommendations with risk-adjusted returns for a $200K portfolio.

Features:
- Market-neutral long-short strategy
- Factor hedging (SPY, NASDAQ, oil, interest rates, etc.)
- Smart Money Zones and technical indicator integration
- Advanced performance metrics (Sharpe, Information Ratio, Calmar, Sortino)
- Real-time risk management and position sizing
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from typing import Dict, List

from portfolio.recommendation_engine import (
    HourlyRecommendationEngine, 
    TradingUniverse,
    OptimizationConstraints
)
from portfolio.factor_framework import FactorUniverse
from portfolio.performance_metrics import PerformanceAnalyzer


def create_optimized_universe() -> TradingUniverse:
    """Create an optimized trading universe for the $200K portfolio."""
    # Carefully selected universe with good factor coverage and liquidity
    symbols = [
        # Large Cap Tech (High Beta, Growth)
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'CRM', 'ADBE',
        
        # Large Cap Value/Defensive
        'JPM', 'JNJ', 'PG', 'KO', 'WMT', 'UNH', 'V', 'MA',
        
        # Mid Cap Growth
        'ZM', 'ROKU', 'SQ', 'SNOW', 'CRWD', 'NET', 'OKTA', 'DDOG',
        
        # Cyclical/Energy
        'CAT', 'XOM', 'CVX', 'BA', 'F', 'GM',
        
        # Factor ETFs for hedging
        'SPY',   # S&P 500 (broad market)
        'QQQ',   # NASDAQ (tech exposure)
        'IWM',   # Russell 2000 (small cap)
        'TLT',   # 20+ Year Treasury (interest rate)
        'SHY',   # 1-3 Year Treasury (short rates)
        'GLD',   # Gold (inflation hedge)
        'USO',   # Oil (commodity exposure)
        'XLK',   # Technology sector
        'XLF',   # Financial sector
        'XLE',   # Energy sector
        'VIX'    # Volatility
    ]
    
    return TradingUniverse(symbols)


def create_conservative_constraints() -> OptimizationConstraints:
    """Create conservative constraints suitable for institutional portfolio."""
    return OptimizationConstraints(
        max_position_weight=0.04,        # 4% max per position
        max_sector_exposure=0.15,        # 15% max per sector
        max_leverage=1.5,                # 1.5x gross leverage
        min_position_size=0.005,         # 0.5% minimum position
        transaction_cost_bps=3.0,        # 3 bps transaction cost
        
        # Very tight factor constraints for market neutrality
        max_market_beta=0.03,            # ±3% market beta
        max_sector_beta=0.08,            # ±8% sector beta  
        max_factor_beta=0.05,            # ±5% other factor beta
        
        # Risk constraints
        max_portfolio_volatility=0.12,   # 12% annual volatility
        target_sharpe_ratio=2.0,         # Target 2.0 Sharpe ratio
        
        # Long-short settings
        target_dollar_neutral=True,      # Dollar neutral
        max_net_exposure=0.05,           # ±5% net exposure
        min_gross_exposure=0.60          # Minimum 60% gross exposure
    )


def demonstrate_single_recommendation():
    """Demonstrate a single hourly recommendation."""
    print("="*80)
    print("PORTFOLIO FORECASTING SYSTEM - SINGLE RECOMMENDATION DEMO")
    print("="*80)
    print("Generating market-neutral portfolio recommendation for $200K portfolio")
    
    # Initialize system
    universe = create_optimized_universe()
    constraints = create_conservative_constraints()
    
    engine = HourlyRecommendationEngine(
        portfolio_value=200000,
        universe=universe,
        constraints=constraints
    )
    
    print(f"\n📊 Trading Universe: {len(universe.symbols)} symbols")
    print(f"   Stocks: {len(universe.stocks)}")
    print(f"   ETFs: {len(universe.etfs)}")
    print(f"   Factor Instruments: {len(universe.factor_instruments)}")
    
    print(f"\n⚙️ Portfolio Constraints:")
    print(f"   Max Position Size: {constraints.max_position_weight:.1%}")
    print(f"   Max Market Beta: ±{constraints.max_market_beta:.1%}")
    print(f"   Target Leverage: {constraints.max_leverage:.1f}x")
    print(f"   Transaction Costs: {constraints.transaction_cost_bps:.1f} bps")
    
    # Generate recommendation
    print(f"\n🔄 Generating recommendation...")
    recommendation = engine.generate_hourly_recommendation()
    
    # Display results
    print(f"\n📈 RECOMMENDATION RESULTS ({recommendation.timestamp.strftime('%Y-%m-%d %H:%M:%S')})")
    print("="*60)
    
    print(f"\n💰 Portfolio Metrics:")
    print(f"   Expected Return:      {recommendation.expected_return:.2%}")
    print(f"   Expected Volatility:  {recommendation.expected_volatility:.2%}")
    print(f"   Sharpe Ratio:         {recommendation.sharpe_ratio:.2f}")
    
    # Portfolio composition
    if recommendation.portfolio_weights:
        gross_exposure = sum(abs(w) for w in recommendation.portfolio_weights.values())
        net_exposure = sum(recommendation.portfolio_weights.values())
        long_exposure = sum(max(0, w) for w in recommendation.portfolio_weights.values())
        short_exposure = sum(min(0, w) for w in recommendation.portfolio_weights.values())
        
        print(f"\n📊 Portfolio Composition:")
        print(f"   Gross Exposure:       {gross_exposure:.1%}")
        print(f"   Net Exposure:         {net_exposure:.1%}")
        print(f"   Long Exposure:        {long_exposure:.1%}")
        print(f"   Short Exposure:       {short_exposure:.1%}")
        print(f"   Number of Positions:  {len(recommendation.portfolio_weights)}")
        
        # Top positions
        sorted_positions = sorted(recommendation.portfolio_weights.items(), 
                                key=lambda x: abs(x[1]), reverse=True)
        
        print(f"\n🎯 Top 10 Positions:")
        for i, (symbol, weight) in enumerate(sorted_positions[:10]):
            direction = "LONG" if weight > 0 else "SHORT"
            dollar_amount = weight * 200000
            print(f"   {i+1:2d}. {symbol:6s} {direction:5s} {weight:6.2%} (${dollar_amount:>8,.0f})")
    
    # Factor exposures
    if recommendation.factor_exposures:
        print(f"\n🔍 Factor Exposures:")
        for factor, exposure in recommendation.factor_exposures.items():
            status = "✅" if abs(exposure) < 0.05 else "⚠️"
            print(f"   {status} {factor:6s}: {exposure:>6.2%}")
    
    # Signals summary
    if recommendation.signals_summary:
        signals = recommendation.signals_summary
        print(f"\n📡 Signal Summary:")
        print(f"   Total Signals:        {signals.get('total_signals', 0)}")
        print(f"   Long Signals:         {signals.get('long_signals', 0)}")
        print(f"   Short Signals:        {signals.get('short_signals', 0)}")
        print(f"   Average Confidence:   {signals.get('avg_confidence', 0):.1%}")
        print(f"   High Confidence:      {signals.get('high_confidence_signals', 0)}")
    
    # Risk warnings
    if recommendation.risk_warnings:
        print(f"\n⚠️ Risk Warnings:")
        for warning in recommendation.risk_warnings:
            print(f"   {warning}")
    
    # Execution notes
    if recommendation.execution_notes:
        print(f"\n📝 Execution Notes:")
        for note in recommendation.execution_notes:
            print(f"   {note}")
    
    return recommendation


def demonstrate_continuous_operation():
    """Demonstrate continuous hourly operation."""
    print("\n" + "="*80)
    print("CONTINUOUS HOURLY OPERATION DEMO")
    print("="*80)
    print("Simulating 24 hours of continuous portfolio recommendations")
    
    # Initialize system
    universe = create_optimized_universe()
    constraints = create_conservative_constraints()
    
    engine = HourlyRecommendationEngine(
        portfolio_value=200000,
        universe=universe,
        constraints=constraints
    )
    
    # Run for 24 hours
    print(f"\n🔄 Running continuous recommendations for 24 hours...")
    recommendations = engine.run_continuous_recommendations(hours=24)
    
    # Analyze results
    print(f"\n📊 CONTINUOUS OPERATION RESULTS")
    print("="*50)
    
    if not recommendations:
        print("❌ No recommendations generated")
        return
    
    # Extract time series data
    timestamps = [r.timestamp for r in recommendations]
    returns = [r.expected_return for r in recommendations]
    volatilities = [r.expected_volatility for r in recommendations]
    sharpe_ratios = [r.sharpe_ratio for r in recommendations]
    
    # Calculate statistics
    avg_return = np.mean(returns)
    avg_volatility = np.mean(volatilities)
    avg_sharpe = np.mean(sharpe_ratios)
    
    print(f"\n📈 Performance Statistics:")
    print(f"   Average Expected Return:    {avg_return:.2%}")
    print(f"   Average Expected Vol:       {avg_volatility:.2%}")
    print(f"   Average Sharpe Ratio:       {avg_sharpe:.2f}")
    print(f"   Return Range:               {min(returns):.2%} to {max(returns):.2%}")
    print(f"   Volatility Range:           {min(volatilities):.2%} to {max(volatilities):.2%}")
    
    # Portfolio turnover analysis
    turnovers = []
    for i in range(1, len(recommendations)):
        prev_weights = recommendations[i-1].portfolio_weights
        curr_weights = recommendations[i].portfolio_weights
        
        # Calculate turnover
        all_symbols = set(prev_weights.keys()) | set(curr_weights.keys())
        turnover = sum(abs(curr_weights.get(s, 0) - prev_weights.get(s, 0)) for s in all_symbols)
        turnovers.append(turnover / 2)  # One-way turnover
    
    if turnovers:
        avg_turnover = np.mean(turnovers)
        print(f"\n🔄 Portfolio Turnover:")
        print(f"   Average Hourly Turnover:    {avg_turnover:.1%}")
        print(f"   Estimated Daily Turnover:   {avg_turnover * 24:.1%}")
        print(f"   Turnover Range:             {min(turnovers):.1%} to {max(turnovers):.1%}")
    
    # Factor exposure consistency
    all_factor_exposures = {}
    for rec in recommendations:
        for factor, exposure in rec.factor_exposures.items():
            if factor not in all_factor_exposures:
                all_factor_exposures[factor] = []
            all_factor_exposures[factor].append(exposure)
    
    print(f"\n🎯 Factor Exposure Consistency:")
    for factor, exposures in all_factor_exposures.items():
        avg_exposure = np.mean(exposures)
        max_exposure = max(abs(e) for e in exposures)
        print(f"   {factor:6s}: Avg {avg_exposure:>5.1%}, Max |{max_exposure:.1%}|")
    
    # Risk warnings summary
    all_warnings = []
    for rec in recommendations:
        all_warnings.extend(rec.risk_warnings)
    
    if all_warnings:
        warning_counts = {}
        for warning in all_warnings:
            key = warning.split(':')[0] if ':' in warning else warning
            warning_counts[key] = warning_counts.get(key, 0) + 1
        
        print(f"\n⚠️ Risk Warning Summary:")
        for warning_type, count in sorted(warning_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"   {warning_type}: {count} occurrences")
    
    return recommendations


def demonstrate_performance_analysis():
    """Demonstrate performance analysis capabilities."""
    print("\n" + "="*80)
    print("PERFORMANCE ANALYSIS DEMO")
    print("="*80)
    
    # Simulate historical performance data
    print("🔄 Simulating historical performance data...")
    
    # Generate realistic return series
    np.random.seed(42)
    n_periods = 1000  # About 6 weeks of hourly data
    
    # Market-neutral strategy parameters
    base_return = 0.15 / (252 * 24)  # 15% annual return
    base_volatility = 0.08 / np.sqrt(252 * 24)  # 8% annual volatility
    
    # Generate returns with some autocorrelation
    returns = []
    current_return = base_return
    
    for i in range(n_periods):
        # Mean reversion + noise
        shock = np.random.normal(0, base_volatility)
        current_return = 0.95 * current_return + 0.05 * base_return + shock
        returns.append(current_return)
    
    returns_series = pd.Series(returns)
    returns_series.index = pd.date_range(
        end=datetime.now(), 
        periods=n_periods, 
        freq='1H'
    )
    
    # Generate factor returns for attribution
    factor_universe = FactorUniverse()
    factor_returns = {}
    
    for factor in factor_universe.factor_symbols[:5]:  # Top 5 factors
        if factor == 'SPY':
            # Market factor - higher volatility
            factor_rets = np.random.normal(0.08/(252*24), 0.15/np.sqrt(252*24), n_periods)
        elif factor == 'TLT':
            # Bond factor - negative correlation to stocks
            factor_rets = np.random.normal(0.03/(252*24), 0.12/np.sqrt(252*24), n_periods)
        else:
            # Other factors
            factor_rets = np.random.normal(0.05/(252*24), 0.18/np.sqrt(252*24), n_periods)
        
        factor_returns[factor] = factor_rets
    
    factor_df = pd.DataFrame(factor_returns, index=returns_series.index)
    
    # Benchmark returns (SPY)
    benchmark_returns = factor_df['SPY'] if 'SPY' in factor_df.columns else None
    
    # Analyze performance
    print("📊 Calculating comprehensive performance metrics...")
    analyzer = PerformanceAnalyzer()
    
    performance_metrics = analyzer.calculate_comprehensive_metrics(
        returns_series, 
        factor_df, 
        benchmark_returns
    )
    
    # Generate report
    print("\n" + "="*80)
    print("COMPREHENSIVE PERFORMANCE REPORT")
    print("="*80)
    
    report = analyzer.generate_performance_report(performance_metrics)
    print(report)
    
    # Additional analysis
    print("\n" + "="*60)
    print("RISK-ADJUSTED PERFORMANCE ANALYSIS")
    print("="*60)
    
    print(f"\n🎯 Key Performance Highlights:")
    
    # Performance vs benchmarks
    if performance_metrics.information_ratio > 1.0:
        print("   ✅ Excellent alpha generation vs benchmark")
    elif performance_metrics.information_ratio > 0.5:
        print("   ✅ Good alpha generation vs benchmark")
    else:
        print("   ⚠️ Limited alpha generation vs benchmark")
    
    # Market neutrality assessment
    if abs(performance_metrics.correlation_to_spy) < 0.1:
        print("   ✅ Excellent market neutrality")
    elif abs(performance_metrics.correlation_to_spy) < 0.2:
        print("   ✅ Good market neutrality")
    else:
        print("   ⚠️ Limited market neutrality")
    
    # Risk management
    if performance_metrics.max_drawdown > -0.05:
        print("   ✅ Excellent drawdown control")
    elif performance_metrics.max_drawdown > -0.10:
        print("   ✅ Good drawdown control")
    else:
        print("   ⚠️ High maximum drawdown")
    
    # Factor exposure analysis
    print(f"\n🔍 Factor Exposure Analysis:")
    total_factor_exposure = sum(abs(exp) for exp in performance_metrics.factor_exposures.values())
    if total_factor_exposure < 0.2:
        print("   ✅ Excellent factor neutrality")
    elif total_factor_exposure < 0.5:
        print("   ✅ Good factor neutrality")
    else:
        print("   ⚠️ High factor exposures detected")
    
    return performance_metrics


def demonstrate_factor_analysis():
    """Demonstrate factor framework and hedging analysis."""
    print("\n" + "="*80)
    print("FACTOR ANALYSIS & HEDGING DEMO")
    print("="*80)
    
    from portfolio.factor_framework import FactorRiskModel, FactorNeutralityConstraints
    
    # Initialize factor risk model
    factor_model = FactorRiskModel()
    print(f"📊 Factor Universe: {len(factor_model.factor_universe.factors)} factors")
    
    # Display factor categories
    from portfolio.factor_framework import FactorType
    
    factor_categories = {}
    for factor in factor_model.factor_universe.factors:
        category = factor.factor_type.value
        if category not in factor_categories:
            factor_categories[category] = []
        factor_categories[category].append(factor.symbol)
    
    print(f"\n🏷️ Factor Categories:")
    for category, symbols in factor_categories.items():
        print(f"   {category.title():15s}: {', '.join(symbols)}")
    
    # Demonstrate neutrality constraints
    constraints = FactorNeutralityConstraints(factor_model.factor_universe)
    
    print(f"\n⚖️ Factor Neutrality Constraints:")
    for factor_symbol, (min_limit, max_limit) in constraints.exposure_limits.items():
        print(f"   {factor_symbol:6s}: {min_limit:>6.1%} to {max_limit:>6.1%}")
    
    # Simulate portfolio exposures
    print(f"\n🎯 Example Portfolio Factor Exposure Check:")
    
    # Simulate some factor exposures
    np.random.seed(42)
    sample_exposures = {}
    for factor_symbol in factor_model.factor_universe.factor_symbols:
        # Generate realistic exposures
        if factor_symbol in ['SPY', 'QQQ']:
            exposure = np.random.normal(0, 0.02)  # Very low market exposure
        elif factor_symbol.startswith('XL'):
            exposure = np.random.normal(0, 0.05)  # Low sector exposure
        else:
            exposure = np.random.normal(0, 0.03)  # Low other exposures
        
        sample_exposures[factor_symbol] = exposure
    
    # Check neutrality
    neutrality_status = constraints.check_neutrality(sample_exposures)
    violations = constraints.get_violation_severity(sample_exposures)
    
    # Display results
    compliant_factors = sum(1 for status in neutrality_status.values() if status)
    total_factors = len(neutrality_status)
    
    print(f"   Neutrality Compliance: {compliant_factors}/{total_factors} factors")
    
    # Show violations
    violation_factors = [(factor, severity) for factor, severity in violations.items() if severity > 0]
    if violation_factors:
        print(f"   ⚠️ Constraint Violations:")
        for factor, severity in sorted(violation_factors, key=lambda x: x[1], reverse=True):
            exposure = sample_exposures[factor]
            print(f"     {factor}: {exposure:>6.2%} (violation: {severity:.1%})")
    else:
        print(f"   ✅ All factor constraints satisfied")
    
    # Risk attribution example
    print(f"\n📈 Example Risk Attribution:")
    
    # Create sample covariance matrix
    n_factors = len(factor_model.factor_universe.factor_symbols)
    np.random.seed(42)
    
    # Generate correlation matrix
    correlation_matrix = np.eye(n_factors)
    for i in range(n_factors):
        for j in range(i+1, n_factors):
            corr = np.random.normal(0, 0.1)  # Low correlations
            corr = np.clip(corr, -0.3, 0.3)
            correlation_matrix[i, j] = correlation_matrix[j, i] = corr
    
    # Convert to covariance (use different volatilities)
    volatilities = np.array([0.15, 0.12, 0.18, 0.10, 0.20] + [0.16] * (n_factors - 5))
    cov_matrix = np.outer(volatilities, volatilities) * correlation_matrix
    cov_df = pd.DataFrame(cov_matrix, 
                         index=factor_model.factor_universe.factor_symbols,
                         columns=factor_model.factor_universe.factor_symbols)
    
    # Calculate risk contributions
    risk_contributions = factor_model.exposure_calculator.calculate_factor_risk_contribution(
        sample_exposures, cov_df
    )
    
    # Display top risk contributors
    sorted_contributions = sorted(risk_contributions.items(), 
                                key=lambda x: abs(x[1]), reverse=True)
    
    print(f"   Top Risk Contributors:")
    for factor, contribution in sorted_contributions[:5]:
        print(f"     {factor:6s}: {contribution:>6.1%} of portfolio risk")


def main():
    """Run complete portfolio forecasting demonstration."""
    print("PORTFOLIO FORECASTING SYSTEM - COMPREHENSIVE DEMONSTRATION")
    print("=" * 80)
    print("""
This system generates hourly portfolio recommendations for a $200K market-neutral
strategy with the following key features:

🎯 OBJECTIVES:
   • Generate risk-adjusted returns with Sharpe ratio > 1.5
   • Maintain market neutrality (low correlation to SPY/QQQ)
   • Hedge against major risk factors (oil, rates, sectors)
   • Target 10-15% annual returns with <12% volatility

⚙️ METHODOLOGY:
   • Smart Money Zones for institutional entry/exit timing
   • Session VWAP analysis for flow confirmation
   • Technical indicators for momentum and mean reversion
   • Factor hedging for market-neutral construction
   • Advanced portfolio optimization with transaction costs

📊 PERFORMANCE MEASUREMENT:
   • Sharpe Ratio (return/volatility)
   • Information Ratio (excess return/tracking error)
   • Calmar Ratio (return/max drawdown)
   • Sortino Ratio (return/downside deviation)
   • Factor attribution and neutrality analysis
    """)
    
    try:
        # 1. Single recommendation demo
        recommendation = demonstrate_single_recommendation()
        
        # 2. Continuous operation demo
        recommendations = demonstrate_continuous_operation()
        
        # 3. Performance analysis demo
        performance_metrics = demonstrate_performance_analysis()
        
        # 4. Factor analysis demo
        demonstrate_factor_analysis()
        
        print("\n" + "="*80)
        print("✅ DEMONSTRATION COMPLETED SUCCESSFULLY!")
        print("="*80)
        
        print(f"\n🎉 Key Results Summary:")
        print(f"   • Generated {len(recommendations) if recommendations else 1} hourly recommendations")
        print(f"   • Demonstrated market-neutral portfolio construction")
        print(f"   • Showed factor hedging and risk management")
        print(f"   • Calculated comprehensive performance metrics")
        print(f"   • Validated system for $200K portfolio management")
        
        print(f"\n📈 Next Steps for Live Implementation:")
        print(f"   1. Connect to live market data feeds")
        print(f"   2. Integrate with execution management system")
        print(f"   3. Set up automated hourly scheduling")
        print(f"   4. Implement portfolio monitoring and alerts")
        print(f"   5. Add regulatory compliance and reporting")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error during demonstration: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)