#!/usr/bin/env python3
"""
Cross-Vendor Price Comparison System

Production-ready cross-vendor price comparison system that compares price data
across multiple vendors to ensure data quality, identify discrepancies, and
calculate consensus prices using majority voting logic.
"""

import asyncio
import asyncpg
import numpy as np
from datetime import date, timedelta
from typing import Dict, List
from dataclasses import dataclass

# Add src to path for imports
from shared.utils.environment import Environment
from domains.market_data.services.reconciliation.majority_voting_reconciler import (
    MajorityVotingReconciler,
    VendorPrice
)

@dataclass
class CrossVendorComparisonResult:
    """Result of cross-vendor price comparison"""
    symbol: str
    test_date: date
    vendor_prices: Dict[str, float]
    price_variance: float
    outlier_vendors: List[str]
    consensus_price: float
    confidence_score: float
    passed: bool
    notes: str

@dataclass
class VendorComparisonReport:
    """Comprehensive vendor comparison report"""
    test_period_start: date
    test_period_end: date
    total_symbols_tested: int
    total_comparisons: int
    high_variance_comparisons: int
    vendor_reliability_scores: Dict[str, float]
    vendor_average_deviations: Dict[str, float]
    recommendations: List[str]
    overall_data_quality_score: float

class CrossVendorPriceComparator:
    """
    Comprehensive cross-vendor price comparison system
    """

    def __init__(self, env: Environment):
        self.env = env
        self.db_url = env.get_database_url()

        # Initialize reconciler for majority voting logic
        self.reconciler = MajorityVotingReconciler(env)

        # Vendor tables
        self.vendor_tables = {
            'polygon': env.get_table_name('daily_price_polygon'),
            'tiingo': env.get_table_name('daily_price_tiingo'),
            'alphavantage': env.get_table_name('daily_prices_alphavantage'),
            'fmp': env.get_table_name('daily_prices_fmp')
        }

        # Comparison thresholds
        self.thresholds = {
            'max_acceptable_variance': 0.05,  # 5%
            'high_variance_threshold': 0.10,  # 10%
            'min_vendors_for_comparison': 2,
            'reliability_threshold': 0.80     # 80%
        }

    async def get_multi_vendor_prices(self, symbol: str, start_date: date, end_date: date) -> Dict[str, Dict[date, Dict]]:
        """Get prices from all vendors for comparison"""

        pool = await asyncpg.create_pool(self.db_url, min_size=2, max_size=8)
        vendor_data = {}

        try:
            async with pool.acquire() as conn:
                # Get instrument ID
                instrument_id = await conn.fetchval(
                    "SELECT id FROM dev_instruments WHERE symbol = $1", symbol
                )

                if not instrument_id:
                    return {}

                # Fetch from each vendor
                for vendor, table_name in self.vendor_tables.items():
                    try:
                        rows = await conn.fetch(f"""
                            SELECT date, close, volume, open_price, high_price, low_price, adj_close
                            FROM {table_name}
                            WHERE instrument_id = $1
                              AND date BETWEEN $2 AND $3
                            ORDER BY date
                        """, instrument_id, start_date, end_date)

                        vendor_data[vendor] = {}
                        for row in rows:
                            vendor_data[vendor][row['date']] = {
                                'close': float(row['close']),
                                'volume': int(row['volume']) if row['volume'] else 0,
                                'open': float(row['open_price']) if row['open_price'] else None,
                                'high': float(row['high_price']) if row['high_price'] else None,
                                'low': float(row['low_price']) if row['low_price'] else None,
                                'adj_close': float(row['adj_close']) if row['adj_close'] else None
                            }

                    except Exception as e:
                        print(f"Warning: Could not fetch {vendor} data for {symbol}: {e}")
                        vendor_data[vendor] = {}

        finally:
            await pool.close()

        return vendor_data

    async def compare_prices_for_symbol(self, symbol: str, start_date: date, end_date: date) -> List[CrossVendorComparisonResult]:
        """Compare prices across vendors for a single symbol"""

        # Get data from all vendors
        vendor_data = await self.get_multi_vendor_prices(symbol, start_date, end_date)

        if len(vendor_data) < self.thresholds['min_vendors_for_comparison']:
            return []

        # Find all dates with data from multiple vendors
        all_dates = set()
        for dates_dict in vendor_data.values():
            all_dates.update(dates_dict.keys())

        comparison_results = []

        for test_date in sorted(all_dates):
            # Get prices for this date from all vendors
            date_prices = {}
            for vendor, dates_dict in vendor_data.items():
                if test_date in dates_dict:
                    date_prices[vendor] = dates_dict[test_date]['close']

            if len(date_prices) >= self.thresholds['min_vendors_for_comparison']:
                # Perform comparison
                result = await self._compare_single_date_prices(symbol, test_date, date_prices)
                comparison_results.append(result)

        return comparison_results

    async def _compare_single_date_prices(self, symbol: str, test_date: date, vendor_prices: Dict[str, float]) -> CrossVendorComparisonResult:
        """Compare prices from multiple vendors for a single date"""

        prices = list(vendor_prices.values())
        price_mean = np.mean(prices)
        price_std = np.std(prices)
        price_variance = price_std / price_mean if price_mean > 0 else 0

        # Identify outliers
        outlier_vendors = []
        for vendor, price in vendor_prices.items():
            deviation = abs(price - price_mean) / price_mean if price_mean > 0 else 0
            if deviation > self.thresholds['max_acceptable_variance']:
                outlier_vendors.append(vendor)

        # Calculate consensus using reconciler logic
        vendor_price_objects = [
            VendorPrice(vendor, test_date, symbol, price, 1000000)  # Use non-zero volume
            for vendor, price in vendor_prices.items()
        ]

        consensus = self.reconciler.determine_consensus_price(vendor_price_objects)

        # Determine if comparison passed
        passed = price_variance <= self.thresholds['max_acceptable_variance']

        # Generate notes
        if not passed:
            if price_variance > self.thresholds['high_variance_threshold']:
                notes = f"High price variance ({price_variance:.3f}) detected. Outliers: {outlier_vendors}"
            else:
                notes = f"Moderate price variance ({price_variance:.3f}). May need investigation."
        else:
            notes = f"Acceptable price variance ({price_variance:.3f}). Good data quality."

        return CrossVendorComparisonResult(
            symbol=symbol,
            test_date=test_date,
            vendor_prices=vendor_prices,
            price_variance=price_variance,
            outlier_vendors=outlier_vendors,
            consensus_price=consensus.consensus_price,
            confidence_score=consensus.confidence_score,
            passed=passed,
            notes=notes
        )

    async def run_comprehensive_comparison(self, symbols: List[str], days_back: int = 30) -> VendorComparisonReport:
        """Run comprehensive cross-vendor comparison for multiple symbols"""

        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)

        print(f"🔄 Starting comprehensive cross-vendor comparison")
        print(f"📊 Symbols: {len(symbols)}")
        print(f"📅 Period: {start_date} to {end_date}")

        all_results = []

        # Compare each symbol
        for i, symbol in enumerate(symbols, 1):
            print(f"  [{i}/{len(symbols)}] Comparing {symbol}...")

            try:
                symbol_results = await self.compare_prices_for_symbol(symbol, start_date, end_date)
                all_results.extend(symbol_results)
            except Exception as e:
                print(f"    Error comparing {symbol}: {e}")

        # Generate comprehensive report
        return await self._generate_comparison_report(symbols, all_results, start_date, end_date)

    async def _generate_comparison_report(self, symbols: List[str], results: List[CrossVendorComparisonResult],
                                        start_date: date, end_date: date) -> VendorComparisonReport:
        """Generate comprehensive vendor comparison report"""

        total_comparisons = len(results)
        high_variance_comparisons = sum(1 for r in results if not r.passed)

        # Calculate vendor reliability scores
        vendor_reliability_scores = {}
        vendor_deviation_sums = {}
        vendor_comparison_counts = {}

        # Initialize vendor tracking
        all_vendors = set()
        for result in results:
            all_vendors.update(result.vendor_prices.keys())

        for vendor in all_vendors:
            vendor_reliability_scores[vendor] = 0
            vendor_deviation_sums[vendor] = 0
            vendor_comparison_counts[vendor] = 0

        # Calculate statistics
        for result in results:
            consensus = result.consensus_price

            for vendor, price in result.vendor_prices.items():
                # Calculate deviation from consensus
                deviation = abs(price - consensus) / consensus if consensus > 0 else 0
                vendor_deviation_sums[vendor] += deviation
                vendor_comparison_counts[vendor] += 1

                # Award reliability points for being within acceptable range
                if deviation <= self.thresholds['max_acceptable_variance']:
                    vendor_reliability_scores[vendor] += 1

        # Normalize reliability scores
        for vendor in all_vendors:
            if vendor_comparison_counts[vendor] > 0:
                vendor_reliability_scores[vendor] = vendor_reliability_scores[vendor] / vendor_comparison_counts[vendor]

        # Calculate average deviations
        vendor_average_deviations = {}
        for vendor in all_vendors:
            if vendor_comparison_counts[vendor] > 0:
                vendor_average_deviations[vendor] = vendor_deviation_sums[vendor] / vendor_comparison_counts[vendor]
            else:
                vendor_average_deviations[vendor] = 0

        # Generate recommendations
        recommendations = []

        # Check vendor reliability
        for vendor, reliability in vendor_reliability_scores.items():
            if reliability < self.thresholds['reliability_threshold']:
                recommendations.append(
                    f"⚠️  {vendor.upper()} reliability is low ({reliability:.1%}). "
                    f"Average deviation: {vendor_average_deviations[vendor]:.3f}. Consider data quality review."
                )

        # Check overall data quality
        if high_variance_comparisons > total_comparisons * 0.10:
            recommendations.append(
                f"🔴 High variance detected in {high_variance_comparisons}/{total_comparisons} comparisons "
                f"({high_variance_comparisons/total_comparisons:.1%}). Multi-vendor reconciliation recommended."
            )

        # Overall data quality score
        if total_comparisons > 0:
            passed_comparisons = total_comparisons - high_variance_comparisons
            quality_score = passed_comparisons / total_comparisons * 100
        else:
            quality_score = 100.0

        return VendorComparisonReport(
            test_period_start=start_date,
            test_period_end=end_date,
            total_symbols_tested=len(symbols),
            total_comparisons=total_comparisons,
            high_variance_comparisons=high_variance_comparisons,
            vendor_reliability_scores=vendor_reliability_scores,
            vendor_average_deviations=vendor_average_deviations,
            recommendations=recommendations,
            overall_data_quality_score=quality_score
        )

async def main():
    """Example usage of cross-vendor price comparator"""

    env = Environment()
    comparator = CrossVendorPriceComparator(env)

    # Test with sample symbols
    test_symbols = ["AAPL", "MSFT", "GOOGL"]

    print("🚀 Cross-Vendor Price Comparison Demo")

    try:
        report = await comparator.run_comprehensive_comparison(test_symbols, days_back=7)

        print(f"\n📊 COMPARISON REPORT")
        print(f"Period: {report.test_period_start} to {report.test_period_end}")
        print(f"Symbols Tested: {report.total_symbols_tested}")
        print(f"Total Comparisons: {report.total_comparisons}")
        print(f"High Variance: {report.high_variance_comparisons}")
        print(f"Data Quality Score: {report.overall_data_quality_score:.1f}/100")

        if report.vendor_reliability_scores:
            print(f"\n🏢 Vendor Reliability:")
            for vendor, score in report.vendor_reliability_scores.items():
                print(f"  • {vendor}: {score:.1%}")

        if report.recommendations:
            print(f"\n💡 Recommendations:")
            for rec in report.recommendations:
                print(f"  • {rec}")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("Note: This demo requires database connectivity with price data")

if __name__ == "__main__":
    asyncio.run(main())