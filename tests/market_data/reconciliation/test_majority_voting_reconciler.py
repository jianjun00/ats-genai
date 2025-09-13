#!/usr/bin/env python3
"""
Tests for Majority Voting Price Reconciler

Tests various scenarios of price reconciliation across multiple data vendors:
- Perfect consensus (all vendors agree)
- Majority rule (one outlier)
- High variance scenarios
- Tie-breaking with vendor priority
- Statistical outlier detection
"""

import pytest
import numpy as np
from datetime import date
from unittest.mock import AsyncMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from domains.market_data.services.reconciliation.majority_voting_reconciler import (
    MajorityVotingReconciler,
    VendorPrice,
    PriceConsensus,
    ReconciliationDecision
)
from shared.utils.environment import Environment

@pytest.fixture
def env():
    return Environment()

@pytest.fixture
def reconciler(env):
    return MajorityVotingReconciler(
        env,
        max_price_variance=0.05,  # 5% variance
        min_vendors_for_consensus=2,
        outlier_threshold=2.0  # 2 std devs
    )

class TestVendorPrice:
    """Test VendorPrice data structure"""

    def test_vendor_price_creation(self):
        """Test VendorPrice dataclass creation"""
        price = VendorPrice(
            vendor="polygon",
            date=date(2025, 8, 19),
            symbol="AAPL",
            close_price=250.50,
            volume=1000000,
            adj_close=250.50
        )

        assert price.vendor == "polygon"
        assert price.date == date(2025, 8, 19)
        assert price.symbol == "AAPL"
        assert price.close_price == 250.50
        assert price.volume == 1000000
        assert price.adj_close == 250.50

class TestPriceStatistics:
    """Test price statistical calculations"""

    def test_price_statistics_calculation(self, reconciler):
        """Test statistical measures for price analysis"""
        prices = [
            VendorPrice("polygon", date(2025, 8, 19), "AAPL", 250.00, 1000000),
            VendorPrice("tiingo", date(2025, 8, 19), "AAPL", 251.00, 1100000),
            VendorPrice("fmp", date(2025, 8, 19), "AAPL", 249.50, 950000),
        ]

        stats = reconciler.calculate_price_statistics(prices)

        assert stats['total_vendors'] == 3
        assert abs(stats['mean_price'] - 250.17) < 0.01  # Average of 250, 251, 249.5
        assert abs(stats['median_price'] - 250.00) < 0.01
        assert stats['price_range'] == 1.5  # 251 - 249.5
        assert 'coefficient_of_variation' in stats

    def test_empty_price_statistics(self, reconciler):
        """Test statistics with empty price list"""
        stats = reconciler.calculate_price_statistics([])
        assert stats == {}

class TestOutlierDetection:
    """Test statistical outlier detection"""

    def test_outlier_detection_normal_case(self, reconciler):
        """Test outlier detection with clear outlier"""
        prices = [
            VendorPrice("polygon", date(2025, 8, 19), "AAPL", 250.00, 1000000),
            VendorPrice("tiingo", date(2025, 8, 19), "AAPL", 251.00, 1100000),
            VendorPrice("fmp", date(2025, 8, 19), "AAPL", 249.50, 950000),
            VendorPrice("alphavantage", date(2025, 8, 19), "AAPL", 275.00, 800000),  # Clear outlier
        ]

        stats = reconciler.calculate_price_statistics(prices)
        outliers = reconciler.identify_outliers(prices, stats)

        assert "alphavantage" in outliers
        assert len(outliers) == 1

    def test_no_outliers_detected(self, reconciler):
        """Test case where all prices are within acceptable range"""
        prices = [
            VendorPrice("polygon", date(2025, 8, 19), "AAPL", 250.00, 1000000),
            VendorPrice("tiingo", date(2025, 8, 19), "AAPL", 250.50, 1100000),
            VendorPrice("fmp", date(2025, 8, 19), "AAPL", 249.75, 950000),
        ]

        stats = reconciler.calculate_price_statistics(prices)
        outliers = reconciler.identify_outliers(prices, stats)

        assert len(outliers) == 0

    def test_insufficient_data_for_outliers(self, reconciler):
        """Test outlier detection with insufficient data"""
        prices = [
            VendorPrice("polygon", date(2025, 8, 19), "AAPL", 250.00, 1000000),
            VendorPrice("tiingo", date(2025, 8, 19), "AAPL", 251.00, 1100000),
        ]

        stats = reconciler.calculate_price_statistics(prices)
        outliers = reconciler.identify_outliers(prices, stats)

        assert len(outliers) == 0  # Need at least 3 vendors for outlier detection

class TestConsensusDecisions:
    """Test different consensus decision scenarios"""

    def test_perfect_consensus_scenario(self, reconciler):
        """Test case where all vendors agree within tolerance"""
        prices = [
            VendorPrice("polygon", date(2025, 8, 19), "AAPL", 250.00, 1000000),
            VendorPrice("tiingo", date(2025, 8, 19), "AAPL", 250.20, 1100000),
            VendorPrice("fmp", date(2025, 8, 19), "AAPL", 249.90, 950000),
            VendorPrice("alphavantage", date(2025, 8, 19), "AAPL", 250.10, 800000),
        ]

        consensus = reconciler.determine_consensus_price(prices)

        assert consensus.decision_method == ReconciliationDecision.CONSENSUS
        assert consensus.confidence_score >= 0.8
        assert len(consensus.outlier_vendors) == 0
        # Median of [250.00, 250.20, 249.90, 250.10] = 250.05
        assert abs(consensus.consensus_price - 250.05) < 0.01

    def test_majority_rule_scenario(self, reconciler):
        """Test case where majority of vendors agree, one is outlier"""
        prices = [
            VendorPrice("polygon", date(2025, 8, 19), "AAPL", 250.00, 1000000),
            VendorPrice("tiingo", date(2025, 8, 19), "AAPL", 250.25, 1100000),
            VendorPrice("fmp", date(2025, 8, 19), "AAPL", 249.75, 950000),
            VendorPrice("alphavantage", date(2025, 8, 19), "AAPL", 275.00, 800000),  # Outlier
        ]

        consensus = reconciler.determine_consensus_price(prices)

        assert consensus.decision_method == ReconciliationDecision.CONSENSUS
        assert "alphavantage" in consensus.outlier_vendors
        assert consensus.confidence_score >= 0.8
        # Consensus should be from the 3 non-outlier vendors
        expected_median = np.median([250.00, 250.25, 249.75])
        assert abs(consensus.consensus_price - expected_median) < 0.01

    def test_two_vendor_agreement(self, reconciler):
        """Test case with only two vendors in agreement"""
        prices = [
            VendorPrice("polygon", date(2025, 8, 19), "AAPL", 250.00, 1000000),
            VendorPrice("tiingo", date(2025, 8, 19), "AAPL", 251.00, 1100000),
        ]

        consensus = reconciler.determine_consensus_price(prices)

        assert consensus.decision_method == ReconciliationDecision.MAJORITY_RULE
        assert consensus.confidence_score >= 0.6
        assert consensus.consensus_price == 250.50  # Average of two prices

    def test_tie_breaking_with_priority(self, reconciler):
        """Test tie-breaking using vendor priority"""
        prices = [
            VendorPrice("polygon", date(2025, 8, 19), "AAPL", 250.00, 1000000),  # Priority 1
            VendorPrice("fmp", date(2025, 8, 19), "AAPL", 260.00, 1100000),     # Priority 3
        ]

        # Set up scenario where prices disagree beyond tolerance
        reconciler.max_price_variance = 0.02  # 2% - lower than 4% difference

        consensus = reconciler.determine_consensus_price(prices)

        # Should use polygon (higher priority) for tie-breaking
        assert consensus.consensus_price == 250.00
        assert "polygon" in consensus.notes.lower()

    def test_insufficient_data_scenario(self, reconciler):
        """Test case with insufficient price data"""
        prices = [
            VendorPrice("polygon", date(2025, 8, 19), "AAPL", 250.00, 1000000),
        ]

        consensus = reconciler.determine_consensus_price(prices)

        assert consensus.decision_method == ReconciliationDecision.INSUFFICIENT_DATA
        assert consensus.confidence_score <= 0.5
        assert consensus.consensus_price == 250.00

    def test_empty_prices_scenario(self, reconciler):
        """Test case with no price data"""
        prices = []

        consensus = reconciler.determine_consensus_price(prices)

        assert consensus.decision_method == ReconciliationDecision.INSUFFICIENT_DATA
        assert consensus.confidence_score == 0.0
        assert consensus.consensus_price == 0.0
        assert "No price data available" in consensus.notes

class TestHighVarianceScenarios:
    """Test high variance and edge case scenarios"""

    def test_high_variance_adjustment(self, reconciler):
        """Test confidence score adjustment for high variance"""
        prices = [
            VendorPrice("polygon", date(2025, 8, 19), "AAPL", 220.00, 1000000),
            VendorPrice("tiingo", date(2025, 8, 19), "AAPL", 250.00, 1100000),
            VendorPrice("fmp", date(2025, 8, 19), "AAPL", 280.00, 950000),
        ]

        consensus = reconciler.determine_consensus_price(prices)

        # High variance should reduce confidence
        assert consensus.price_variance > reconciler.max_price_variance
        assert "High variance" in consensus.notes
        # Confidence should be reduced due to high variance
        assert consensus.confidence_score < 0.9

    def test_extreme_outlier_scenario(self, reconciler):
        """Test scenario with extreme price outlier"""
        prices = [
            VendorPrice("polygon", date(2025, 8, 19), "AAPL", 250.00, 1000000),
            VendorPrice("tiingo", date(2025, 8, 19), "AAPL", 251.00, 1100000),
            VendorPrice("fmp", date(2025, 8, 19), "AAPL", 249.50, 950000),
            VendorPrice("alphavantage", date(2025, 8, 19), "AAPL", 500.00, 800000),  # Extreme outlier
        ]

        consensus = reconciler.determine_consensus_price(prices)

        assert "alphavantage" in consensus.outlier_vendors
        assert consensus.decision_method == ReconciliationDecision.CONSENSUS
        # Consensus should ignore the extreme outlier
        expected_median = np.median([250.00, 251.00, 249.50])
        assert abs(consensus.consensus_price - expected_median) < 0.01

class TestDataGrouping:
    """Test price data grouping by date"""

    def test_group_prices_by_date(self, reconciler):
        """Test grouping vendor prices by trading date"""
        prices = [
            VendorPrice("polygon", date(2025, 8, 19), "AAPL", 250.00, 1000000),
            VendorPrice("tiingo", date(2025, 8, 19), "AAPL", 251.00, 1100000),
            VendorPrice("polygon", date(2025, 8, 20), "AAPL", 252.00, 1200000),
            VendorPrice("fmp", date(2025, 8, 20), "AAPL", 251.50, 1150000),
        ]

        grouped = reconciler.group_prices_by_date(prices)

        assert len(grouped) == 2  # Two unique dates
        assert date(2025, 8, 19) in grouped
        assert date(2025, 8, 20) in grouped
        assert len(grouped[date(2025, 8, 19)]) == 2  # Two vendors for Aug 19
        assert len(grouped[date(2025, 8, 20)]) == 2  # Two vendors for Aug 20

class TestVendorPriority:
    """Test vendor priority system for tie-breaking"""

    def test_vendor_priority_order(self, reconciler):
        """Test that vendor priority is correctly defined"""
        assert reconciler.vendor_priority['polygon'] == 1
        assert reconciler.vendor_priority['tiingo'] == 2
        assert reconciler.vendor_priority['fmp'] == 3
        assert reconciler.vendor_priority['alphavantage'] == 4

    def test_priority_tie_breaking(self, reconciler):
        """Test tie-breaking using vendor priority when prices disagree significantly"""
        # Create scenario where Polygon and FMP disagree beyond tolerance
        prices = [
            VendorPrice("fmp", date(2025, 8, 19), "AAPL", 260.00, 1000000),      # Lower priority
            VendorPrice("polygon", date(2025, 8, 19), "AAPL", 240.00, 1100000), # Higher priority
        ]

        # Set tight variance to force tie-breaking
        reconciler.max_price_variance = 0.01  # 1%

        consensus = reconciler.determine_consensus_price(prices)

        # Should choose Polygon due to higher priority
        assert consensus.consensus_price == 240.00

@pytest.mark.integration
@pytest.mark.asyncio
class TestReconcilerIntegration:
    """Integration tests with mocked database calls"""

    @pytest.mark.asyncio

    async def test_reconciliation_workflow(self, reconciler):
        """Test complete reconciliation workflow"""

        # Mock the database calls
        reconciler.get_multi_vendor_prices = AsyncMock(return_value=[
            VendorPrice("polygon", date(2025, 8, 19), "AAPL", 250.00, 1000000),
            VendorPrice("tiingo", date(2025, 8, 19), "AAPL", 251.00, 1100000),
            VendorPrice("fmp", date(2025, 8, 19), "AAPL", 249.50, 950000),
            VendorPrice("alphavantage", date(2025, 8, 19), "AAPL", 275.00, 800000),  # Outlier
        ])

        reconciliations = await reconciler.reconcile_symbol_prices(
            "AAPL",
            date(2025, 8, 19),
            date(2025, 8, 19)
        )

        assert len(reconciliations) == 1
        consensus = reconciliations[0]
        assert consensus.symbol == "AAPL"
        assert consensus.date == date(2025, 8, 19)
        assert "alphavantage" in consensus.outlier_vendors
        assert consensus.confidence_score > 0.8

    @pytest.mark.asyncio

    async def test_report_generation(self, reconciler):
        """Test reconciliation report generation"""

        reconciliations = [
            PriceConsensus(
                date=date(2025, 8, 19),
                symbol="AAPL",
                consensus_price=250.00,
                consensus_volume=1000000,
                decision_method=ReconciliationDecision.CONSENSUS,
                vendor_prices=[
                    VendorPrice("polygon", date(2025, 8, 19), "AAPL", 250.00, 1000000),
                    VendorPrice("tiingo", date(2025, 8, 19), "AAPL", 251.00, 1100000),
                ],
                price_variance=0.002,
                confidence_score=0.9,
                outlier_vendors=[],
                notes="Strong consensus"
            )
        ]

        report = await reconciler.generate_reconciliation_report(reconciliations)

        assert "AAPL" in report
        assert "RECONCILIATION REPORT" in report
        assert "confidence score: 0.90" in report
        assert "High confidence" in report

if __name__ == "__main__":
    # Allow running this test file directly
    pytest.main([__file__, "-v", "-s"])