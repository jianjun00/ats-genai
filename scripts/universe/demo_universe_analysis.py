#!/usr/bin/env python3
"""
Demo Universe Analysis

Shows how the universe creation system would work with sample data
when the database contains 5-year historical data.
"""

import sys
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import List, Dict

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from universe.data_complete_universe_creator import DataCompleteness
from universe.data_quality_validator import ValidationResult, ValidationLevel

def create_sample_data_completeness() -> List[DataCompleteness]:
    """Create sample data completeness results showing what we'd expect to find"""
    
    # Sample of high-quality stocks with 5-year data
    high_quality_stocks = [
        # Large cap tech stocks - usually have excellent data quality
        ("AAPL", "Apple Inc", 0.985, 0.892),
        ("MSFT", "Microsoft Corp", 0.983, 0.888),
        ("GOOGL", "Alphabet Inc", 0.981, 0.885),
        ("AMZN", "Amazon.com Inc", 0.979, 0.882),
        ("TSLA", "Tesla Inc", 0.976, 0.879),
        ("META", "Meta Platforms", 0.974, 0.876),
        ("NVDA", "NVIDIA Corp", 0.988, 0.895),
        ("NFLX", "Netflix Inc", 0.972, 0.874),
        
        # Large cap traditional stocks
        ("JPM", "JPMorgan Chase", 0.980, 0.883),
        ("JNJ", "Johnson & Johnson", 0.982, 0.886),
        ("PG", "Procter & Gamble", 0.978, 0.880),
        ("KO", "Coca-Cola Co", 0.977, 0.878),
        ("PFE", "Pfizer Inc", 0.975, 0.875),
        ("XOM", "Exxon Mobil", 0.973, 0.872),
        ("BAC", "Bank of America", 0.971, 0.870),
        
        # ETFs and index funds - typically very reliable
        ("SPY", "SPDR S&P 500 ETF", 0.995, 0.920),
        ("QQQ", "Invesco QQQ ETF", 0.992, 0.915),
        ("IWM", "iShares Russell 2000", 0.990, 0.910),
        ("VTI", "Vanguard Total Stock", 0.989, 0.908),
        
        # Additional high-volume stocks
        ("WMT", "Walmart Inc", 0.976, 0.877),
        ("HD", "Home Depot", 0.974, 0.875),
        ("DIS", "Walt Disney Co", 0.972, 0.873),
        ("ADBE", "Adobe Inc", 0.970, 0.871),
        ("CRM", "Salesforce Inc", 0.968, 0.869),
        ("ORCL", "Oracle Corp", 0.966, 0.867),
        ("IBM", "IBM Corp", 0.964, 0.865),
        ("GE", "General Electric", 0.962, 0.863),
        ("F", "Ford Motor Co", 0.960, 0.861),
        ("GM", "General Motors", 0.958, 0.859),
        
        # Financial sector
        ("GS", "Goldman Sachs", 0.975, 0.876),
        ("MS", "Morgan Stanley", 0.973, 0.874),
        ("C", "Citigroup Inc", 0.971, 0.872),
        ("WFC", "Wells Fargo", 0.969, 0.870),
        ("USB", "US Bancorp", 0.967, 0.868),
        
        # Healthcare
        ("UNH", "UnitedHealth Group", 0.979, 0.881),
        ("ABT", "Abbott Labs", 0.977, 0.879),
        ("TMO", "Thermo Fisher", 0.975, 0.877),
        ("DHR", "Danaher Corp", 0.973, 0.875),
        ("BMY", "Bristol Myers", 0.971, 0.873),
        
        # Energy
        ("CVX", "Chevron Corp", 0.974, 0.875),
        ("COP", "ConocoPhillips", 0.972, 0.873),
        ("SLB", "Schlumberger", 0.970, 0.871),
        ("EOG", "EOG Resources", 0.968, 0.869),
        
        # Consumer goods
        ("MCD", "McDonald's Corp", 0.976, 0.877),
        ("NKE", "Nike Inc", 0.974, 0.875),
        ("SBUX", "Starbucks Corp", 0.972, 0.873),
        ("TGT", "Target Corp", 0.970, 0.871),
        ("LOW", "Lowe's Companies", 0.968, 0.869),
    ]
    
    # Generate sample data completeness objects
    results = []
    base_date = date.today() - timedelta(days=5*365)  # 5 years ago
    
    for i, (symbol, name, daily_completeness, minute_completeness) in enumerate(high_quality_stocks):
        # Calculate sample data counts
        daily_count = int(1260 * daily_completeness)  # ~5 years of trading days
        minute_count = int(daily_count * 390 * minute_completeness)  # ~390 minutes per day
        trading_days = int(daily_count / 1.05)  # Slightly fewer trading days than data points
        
        quality_score = (daily_completeness * 0.3 + minute_completeness * 0.7)
        if daily_count > 1200:
            quality_score += 0.05  # Bonus for substantial data
        if minute_count > 400000:
            quality_score += 0.05  # Bonus for substantial minute data
        
        results.append(DataCompleteness(
            symbol=symbol,
            instrument_id=1000 + i,  # Sample instrument IDs
            daily_start_date=base_date,
            daily_end_date=date.today() - timedelta(days=1),
            daily_count=daily_count,
            minute_start_date=datetime.combine(base_date, datetime.min.time()),
            minute_end_date=datetime.combine(date.today() - timedelta(days=1), datetime.max.time()),
            minute_count=minute_count,
            minute_trading_days=trading_days,
            expected_daily_count=1260,  # Expected ~252 trading days * 5 years
            expected_minute_count=trading_days * 390,
            daily_completeness_ratio=daily_completeness,
            minute_completeness_ratio=minute_completeness,
            overall_quality_score=min(quality_score, 1.0)
        ))
    
    return results

def create_sample_validation_results(symbols: List[str]) -> List[ValidationResult]:
    """Create sample validation results for demonstration"""
    
    results = []
    
    for i, symbol in enumerate(symbols[:10]):  # First 10 symbols
        # Most stocks pass validation
        if i < 8:
            results.extend([
                ValidationResult(
                    symbol=symbol,
                    check_name="daily_data_gaps",
                    level=ValidationLevel.INFO,
                    passed=True,
                    message=f"Daily data 98.{2+i}% complete",
                    details={"completeness_ratio": 0.98 + i*0.001}
                ),
                ValidationResult(
                    symbol=symbol,
                    check_name="minute_data_availability",
                    level=ValidationLevel.INFO,
                    passed=True,
                    message=f"Minute data 89.{5+i}% complete",
                    details={"completeness_ratio": 0.89 + i*0.001}
                ),
                ValidationResult(
                    symbol=symbol,
                    check_name="daily_price_reasonableness",
                    level=ValidationLevel.INFO,
                    passed=True,
                    message=f"Price movements reasonable (max: {2.1+i*0.1:.1f}%)",
                    details={"max_change": 0.021 + i*0.001}
                )
            ])
        else:
            # A couple have warnings
            results.extend([
                ValidationResult(
                    symbol=symbol,
                    check_name="daily_data_gaps",
                    level=ValidationLevel.WARNING,
                    passed=False,
                    message=f"Daily data only 87.{5+i}% complete (missing days detected)",
                    details={"completeness_ratio": 0.87 + i*0.001}
                ),
                ValidationResult(
                    symbol=symbol,
                    check_name="minute_data_availability",
                    level=ValidationLevel.INFO,
                    passed=True,
                    message=f"Minute data 85.{2+i}% complete",
                    details={"completeness_ratio": 0.85 + i*0.001}
                )
            ])
    
    return results

def demonstrate_universe_analysis():
    """Demonstrate what the universe analysis would show with real data"""
    
    print("=" * 80)
    print("DATA COMPLETE UNIVERSE ANALYSIS DEMONSTRATION")
    print("=" * 80)
    print()
    
    # Generate sample data
    completeness_results = create_sample_data_completeness()
    
    # Filter for qualified instruments (same criteria as real system)
    min_daily_completeness = 0.95
    min_minute_completeness = 0.85
    min_overall_quality = 0.80
    
    qualified = []
    for result in completeness_results:
        has_sufficient_history = (
            result.daily_start_date and 
            result.daily_start_date <= date.today() - timedelta(days=5 * 365)
        )
        
        meets_daily_threshold = result.daily_completeness_ratio >= min_daily_completeness
        meets_minute_threshold = result.minute_completeness_ratio >= min_minute_completeness
        meets_quality_threshold = result.overall_quality_score >= min_overall_quality
        
        if (has_sufficient_history and meets_daily_threshold and 
            meets_minute_threshold and meets_quality_threshold):
            qualified.append(result)
    
    # Sort by quality score
    qualified.sort(key=lambda x: x.overall_quality_score, reverse=True)
    
    print(f"📊 ANALYSIS SUMMARY")
    print(f"   Total symbols analyzed: {len(completeness_results)}")
    print(f"   Qualified instruments: {len(qualified)}")
    print(f"   Qualification rate: {len(qualified)/len(completeness_results)*100:.1f}%")
    print()
    
    print(f"🎯 QUALITY CRITERIA")
    print(f"   Minimum history: 5 years")
    print(f"   Daily completeness: {min_daily_completeness*100:.0f}%")
    print(f"   Minute completeness: {min_minute_completeness*100:.0f}%")
    print(f"   Overall quality: {min_overall_quality*100:.0f}%")
    print()
    
    print(f"🏆 TOP QUALIFIED INSTRUMENTS (high_quality_5y universe)")
    print("-" * 80)
    print(f"{'Rank':<4} {'Symbol':<8} {'Daily %':<8} {'Minute %':<9} {'Quality':<8} {'Daily Count':<11} {'Minute Count'}")
    print("-" * 80)
    
    for i, instrument in enumerate(qualified[:20], 1):
        print(f"{i:<4} {instrument.symbol:<8} "
              f"{instrument.daily_completeness_ratio*100:>6.1f}% "
              f"{instrument.minute_completeness_ratio*100:>7.1f}% "
              f"{instrument.overall_quality_score:>6.3f} "
              f"{instrument.daily_count:>9,} "
              f"{instrument.minute_count:>11,}")
    
    if len(qualified) > 20:
        print(f"... and {len(qualified)-20} more instruments")
    
    print()
    
    # Show validation results sample
    validation_results = create_sample_validation_results([r.symbol for r in qualified])
    
    critical_count = len([r for r in validation_results if r.level == ValidationLevel.CRITICAL and not r.passed])
    warning_count = len([r for r in validation_results if r.level == ValidationLevel.WARNING and not r.passed])
    passed_count = len([r for r in validation_results if r.passed])
    
    print(f"🔍 DATA QUALITY VALIDATION (Sample)")
    print("-" * 50)
    print(f"   Total validations: {len(validation_results)}")
    print(f"   ✅ Passed: {passed_count}")
    print(f"   ⚠️  Warnings: {warning_count}")
    print(f"   ❌ Critical issues: {critical_count}")
    print()
    
    print(f"📈 EXAMPLE STATISTICS FOR TOP INSTRUMENTS")
    print("-" * 50)
    
    # Show detailed stats for top 5
    for i, instrument in enumerate(qualified[:5], 1):
        years_of_data = (date.today() - instrument.daily_start_date).days / 365.25
        avg_daily_volume = instrument.minute_count / max(instrument.minute_trading_days, 1) / 390  # Rough estimate
        
        print(f"{i}. {instrument.symbol}")
        print(f"   📅 Data span: {years_of_data:.1f} years")
        print(f"   📊 Daily bars: {instrument.daily_count:,} ({instrument.daily_completeness_ratio*100:.1f}% complete)")
        print(f"   ⏰ Minute bars: {instrument.minute_count:,} ({instrument.minute_completeness_ratio*100:.1f}% complete)")
        print(f"   🎯 Quality score: {instrument.overall_quality_score:.3f}")
        print(f"   📈 Trading days: {instrument.minute_trading_days:,}")
        print()
    
    print(f"💡 USAGE RECOMMENDATIONS")
    print("-" * 50)
    print(f"✓ Use these {len(qualified)} instruments for:")
    print(f"  • Backtesting strategies (5-year historical depth)")
    print(f"  • Machine learning model training (reliable features)")
    print(f"  • Risk modeling (complete risk factor coverage)")
    print(f"  • Performance attribution (consistent benchmarking)")
    print()
    print(f"⚠️  Consider excluding instruments with:")
    print(f"  • Quality scores below 0.85 for critical applications")
    print(f"  • Less than 4 years of minute data for intraday models")
    print(f"  • Validation warnings for real-time trading")
    print()

if __name__ == "__main__":
    demonstrate_universe_analysis()