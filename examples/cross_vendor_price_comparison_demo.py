#!/usr/bin/env python3
"""
Cross-Vendor Price Comparison Demo

Demonstrates the cross-vendor price comparison system that compares price data
across multiple vendors (Polygon, Tiingo, Alpha Vantage, FMP) to identify
discrepancies, calculate consensus prices, and assess vendor reliability.

This demo shows:
1. Price variance detection across vendors
2. Consensus price calculation using majority voting
3. Vendor reliability scoring
4. Comprehensive comparison reports
"""

import asyncio
import sys
import os
from datetime import date, timedelta

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config.environment import Environment
from tests.integration.test_cross_vendor_price_comparison import (
    CrossVendorPriceComparator,
    CrossVendorComparisonResult
)

async def demonstrate_price_variance_detection():
    """Demonstrate detection of price variance scenarios"""
    
    print("🔍 PRICE VARIANCE DETECTION DEMO")
    print("="*60)
    
    env = Environment()
    comparator = CrossVendorPriceComparator(env)
    
    # Scenario 1: High variance (problematic)
    print("\n📊 Scenario 1: High Price Variance")
    high_variance_prices = {
        'polygon': 250.00,
        'tiingo': 280.00,   # 12% higher
        'fmp': 290.00,      # 16% higher  
        'alphavantage': 245.00  # 2% lower
    }
    
    result1 = await comparator._compare_single_date_prices(
        "DEMO", date.today(), high_variance_prices
    )
    
    print(f"  Vendor Prices: {result1.vendor_prices}")
    print(f"  Price Variance: {result1.price_variance:.3f} ({result1.price_variance*100:.1f}%)")
    print(f"  Consensus Price: ${result1.consensus_price:.2f}")
    print(f"  Confidence Score: {result1.confidence_score:.2f}")
    print(f"  Outlier Vendors: {result1.outlier_vendors}")
    print(f"  Test Passed: {'✅' if result1.passed else '❌'}")
    print(f"  Notes: {result1.notes}")
    
    # Scenario 2: Low variance (good quality)
    print("\n📊 Scenario 2: Low Price Variance")
    low_variance_prices = {
        'polygon': 250.00,
        'tiingo': 251.50,   # 0.6% higher
        'fmp': 249.75,      # 0.1% lower
        'alphavantage': 250.25  # 0.1% higher
    }
    
    result2 = await comparator._compare_single_date_prices(
        "DEMO", date.today(), low_variance_prices
    )
    
    print(f"  Vendor Prices: {result2.vendor_prices}")
    print(f"  Price Variance: {result2.price_variance:.3f} ({result2.price_variance*100:.1f}%)")
    print(f"  Consensus Price: ${result2.consensus_price:.2f}")
    print(f"  Confidence Score: {result2.confidence_score:.2f}")
    print(f"  Outlier Vendors: {result2.outlier_vendors}")
    print(f"  Test Passed: {'✅' if result2.passed else '❌'}")
    print(f"  Notes: {result2.notes}")

async def demonstrate_vendor_reliability_scoring():
    """Demonstrate vendor reliability scoring"""
    
    print("\n\n🏢 VENDOR RELIABILITY SCORING DEMO")
    print("="*60)
    
    # Simulate vendor comparison results
    mock_results = [
        # Day 1: Polygon is accurate, others have slight deviations
        {'polygon': 100.00, 'tiingo': 101.00, 'fmp': 99.50, 'consensus': 100.17},
        # Day 2: FMP is way off, others are close  
        {'polygon': 150.00, 'tiingo': 149.75, 'fmp': 165.00, 'consensus': 149.88},
        # Day 3: Alpha Vantage is missing, others agree
        {'polygon': 200.00, 'tiingo': 200.50, 'fmp': 199.75, 'consensus': 200.08},
        # Day 4: All vendors close
        {'polygon': 175.00, 'tiingo': 174.50, 'fmp': 175.25, 'consensus': 174.92},
        # Day 5: Tiingo has issues
        {'polygon': 180.00, 'tiingo': 195.00, 'fmp': 179.50, 'consensus': 179.75}
    ]
    
    # Calculate reliability scores
    vendor_stats = {
        'polygon': {'correct': 0, 'total': 0, 'total_deviation': 0},
        'tiingo': {'correct': 0, 'total': 0, 'total_deviation': 0},
        'fmp': {'correct': 0, 'total': 0, 'total_deviation': 0}
    }
    
    acceptable_variance = 0.05  # 5%
    
    for day_data in mock_results:
        consensus = day_data['consensus']
        
        for vendor in ['polygon', 'tiingo', 'fmp']:
            if vendor in day_data:
                price = day_data[vendor]
                deviation = abs(price - consensus) / consensus
                
                vendor_stats[vendor]['total'] += 1
                vendor_stats[vendor]['total_deviation'] += deviation
                
                if deviation <= acceptable_variance:
                    vendor_stats[vendor]['correct'] += 1
    
    print("\n📈 Vendor Reliability Analysis:")
    print(f"{'Vendor':<12} {'Reliability':<12} {'Avg Deviation':<15} {'Status'}")
    print("-" * 55)
    
    for vendor, stats in vendor_stats.items():
        if stats['total'] > 0:
            reliability = stats['correct'] / stats['total']
            avg_deviation = stats['total_deviation'] / stats['total']
            
            if reliability >= 0.90:
                status = "🟢 Excellent"
            elif reliability >= 0.80:
                status = "🟡 Good"  
            elif reliability >= 0.70:
                status = "🟠 Fair"
            else:
                status = "🔴 Poor"
            
            print(f"{vendor:<12} {reliability*100:>8.1f}%     {avg_deviation*100:>10.2f}%        {status}")

def demonstrate_comparison_thresholds():
    """Demonstrate comparison thresholds and their effects"""
    
    print("\n\n⚙️  COMPARISON THRESHOLDS DEMO")  
    print("="*60)
    
    env = Environment()
    comparator = CrossVendorPriceComparator(env)
    
    print(f"📋 Current Comparison Thresholds:")
    print(f"  • Maximum Acceptable Variance: {comparator.thresholds['max_acceptable_variance']*100:.1f}%")
    print(f"  • High Variance Threshold: {comparator.thresholds['high_variance_threshold']*100:.1f}%")
    print(f"  • Minimum Vendors Required: {comparator.thresholds['min_vendors_for_comparison']}")
    print(f"  • Reliability Threshold: {comparator.thresholds['reliability_threshold']*100:.1f}%")
    
    print(f"\n💡 Threshold Impact Examples:")
    
    # Example variance scenarios
    scenarios = [
        ("Excellent Data Quality", 0.01),  # 1%
        ("Good Data Quality", 0.03),       # 3% 
        ("Acceptable Data Quality", 0.05), # 5%
        ("Marginal Data Quality", 0.08),   # 8%
        ("Poor Data Quality", 0.12),       # 12%
        ("Very Poor Data Quality", 0.20)   # 20%
    ]
    
    for description, variance in scenarios:
        if variance <= comparator.thresholds['max_acceptable_variance']:
            status = "✅ PASS"
        elif variance <= comparator.thresholds['high_variance_threshold']:
            status = "⚠️  WARNING"
        else:
            status = "❌ FAIL"
        
        print(f"  • {description:<25} ({variance*100:>4.1f}% variance): {status}")

async def demonstrate_real_world_scenario():
    """Demonstrate a realistic cross-vendor comparison scenario"""
    
    print("\n\n🌍 REAL-WORLD SCENARIO DEMO")
    print("="*60)
    
    print("Imagine we're validating AAPL prices from different vendors:")
    print("This shows what the system would detect in a real scenario.")
    
    # Realistic price scenario based on actual market data patterns
    scenarios = [
        {
            'date': '2025-08-19',
            'description': 'Normal trading day - vendors agree',
            'prices': {
                'polygon': 230.85,
                'tiingo': 231.12,
                'fmp': 230.95,
                'alphavantage': 230.78
            }
        },
        {
            'date': '2025-08-18', 
            'description': 'Split adjustment discrepancy - one vendor missed it',
            'prices': {
                'polygon': 115.42,    # Post-split
                'tiingo': 115.56,     # Post-split  
                'fmp': 230.84,        # Pre-split (missed adjustment)
                'alphavantage': 115.39 # Post-split
            }
        },
        {
            'date': '2025-08-17',
            'description': 'API delay - one vendor has stale data', 
            'prices': {
                'polygon': 228.95,
                'tiingo': 229.12,
                'fmp': 225.30,        # Stale from previous day
                'alphavantage': 228.88
            }
        }
    ]
    
    env = Environment()
    comparator = CrossVendorPriceComparator(env)
    
    for scenario in scenarios:
        print(f"\n📅 {scenario['date']}: {scenario['description']}")
        
        result = await comparator._compare_single_date_prices(
            "AAPL", date.today(), scenario['prices']
        )
        
        print(f"  Vendor Prices:")
        for vendor, price in scenario['prices'].items():
            print(f"    • {vendor:>12}: ${price:>7.2f}")
        
        print(f"  Analysis:")
        print(f"    • Variance: {result.price_variance*100:>6.2f}%")
        print(f"    • Consensus: ${result.consensus_price:>7.2f}")
        print(f"    • Confidence: {result.confidence_score:>6.2f}")
        
        if result.outlier_vendors:
            print(f"    • Outliers: {', '.join(result.outlier_vendors)}")
        
        print(f"    • Result: {'✅ PASS' if result.passed else '❌ NEEDS ATTENTION'}")
        
        if not result.passed:
            print(f"    • Action: Investigate {', '.join(result.outlier_vendors or ['all vendors'])}")

async def main():
    """Run comprehensive cross-vendor comparison demo"""
    
    print("🚀 CROSS-VENDOR PRICE COMPARISON SYSTEM DEMO")
    print("="*80)
    print("This demo showcases our multi-vendor price validation system that")
    print("compares data across Polygon, Tiingo, Alpha Vantage, and FMP to")  
    print("ensure data quality and identify discrepancies.")
    
    try:
        # Run all demonstration scenarios
        await demonstrate_price_variance_detection()
        await demonstrate_vendor_reliability_scoring()
        demonstrate_comparison_thresholds()
        await demonstrate_real_world_scenario()
        
        print("\n\n🎯 SUMMARY")
        print("="*60)
        print("✅ The cross-vendor comparison system provides:")
        print("  • Real-time price variance detection")
        print("  • Consensus price calculation using majority voting")
        print("  • Vendor reliability scoring and tracking")
        print("  • Automated outlier identification")
        print("  • Comprehensive data quality reporting")
        print("\n💡 This system helps ensure data integrity across our")
        print("   multi-vendor architecture and enables confident")
        print("   decision-making based on validated price data.")
        
    except Exception as e:
        print(f"❌ Demo error: {e}")
        print("Note: Some features require database connectivity")

if __name__ == "__main__":
    asyncio.run(main())