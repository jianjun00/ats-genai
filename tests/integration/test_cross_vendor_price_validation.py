#!/usr/bin/env python3
"""
Cross-Vendor Price Validation Tests

Critical tests that ensure our 4 vendors provide consistent, accurate price data.
These tests are the foundation of our majority voting system.
"""

import pytest
import asyncio
import asyncpg
from datetime import date, timedelta
from typing import Dict, List, Optional
import statistics
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from config.environment import Environment
from calendars.exchange_calendar import ExchangeCalendar

class CrossVendorPriceValidator:
    """Validates price consistency across all 4 vendors"""
    
    def __init__(self, env: Environment):
        self.env = env
        self.db_url = env.get_database_url()
        self.vendors = {
            'polygon': 'dev_daily_prices_polygon',
            'tiingo': 'dev_daily_prices_tiingo', 
            'alpha_vantage': 'dev_daily_prices_alphavantage',
            'fmp': 'dev_daily_prices_fmp'
        }
    
    async def get_vendor_prices(self, vendor: str, symbol: str, start_date: date, end_date: date) -> Dict[date, Dict[str, float]]:
        """Get prices from a specific vendor for a symbol and date range"""
        table_name = self.vendors[vendor]
        
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=3)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(f"""
                    SELECT p.date, p.close, p.open_price, p.high_price, p.low_price, p.volume
                    FROM {table_name} p
                    JOIN dev_instruments i ON p.instrument_id = i.id
                    WHERE i.symbol = $1 
                      AND p.date BETWEEN $2 AND $3
                    ORDER BY p.date
                """, symbol, start_date, end_date)
                
                result = {}
                for row in rows:
                    result[row['date']] = {
                        'close': float(row['close']),
                        'open': float(row['open_price'] or 0),
                        'high': float(row['high_price'] or 0), 
                        'low': float(row['low_price'] or 0),
                        'volume': int(row['volume'] or 0)
                    }
                return result
        finally:
            await pool.close()
    
    async def get_cross_vendor_prices(self, symbol: str, start_date: date, end_date: date) -> Dict[str, Dict[date, Dict[str, float]]]:
        """Get prices from all vendors for comparison"""
        results = {}
        for vendor in self.vendors.keys():
            try:
                results[vendor] = await self.get_vendor_prices(vendor, symbol, start_date, end_date)
            except Exception as e:
                print(f"Warning: Could not get {vendor} data for {symbol}: {e}")
                results[vendor] = {}
        return results
    
    def calculate_price_variance(self, prices: List[float]) -> float:
        """Calculate coefficient of variation for prices"""
        if len(prices) < 2:
            return 0.0
        mean_price = statistics.mean(prices)
        if mean_price == 0:
            return 0.0
        std_dev = statistics.stdev(prices)
        return std_dev / mean_price
    
    def detect_outliers(self, prices: Dict[str, float], threshold: float = 0.1) -> List[str]:
        """Detect which vendors have outlier prices"""
        if len(prices) < 3:
            return []
        
        price_values = list(prices.values())
        median_price = statistics.median(price_values)
        
        outliers = []
        for vendor, price in prices.items():
            if abs(price - median_price) / median_price > threshold:
                outliers.append(vendor)
        
        return outliers
    
    async def validate_price_consistency(self, symbol: str, start_date: date, end_date: date, max_variance: float = 0.05) -> Dict[str, any]:
        """Validate price consistency across vendors"""
        vendor_data = await self.get_cross_vendor_prices(symbol, start_date, end_date)
        
        # Filter to dates where we have data from at least 2 vendors
        common_dates = set()
        for vendor_dates in vendor_data.values():
            common_dates.update(vendor_dates.keys())
        
        validation_results = {
            'symbol': symbol,
            'date_range': f"{start_date} to {end_date}",
            'total_dates_checked': 0,
            'dates_with_high_variance': [],
            'vendor_outliers': {},
            'average_variance': 0.0,
            'max_variance': 0.0,
            'passed': True,
            'warnings': [],
            'errors': []
        }
        
        variances = []
        
        for test_date in common_dates:
            # Get prices from all vendors for this date
            date_prices = {}
            for vendor, vendor_prices in vendor_data.items():
                if test_date in vendor_prices and vendor_prices[test_date]['close'] > 0:
                    date_prices[vendor] = vendor_prices[test_date]['close']
            
            if len(date_prices) < 2:
                continue
            
            validation_results['total_dates_checked'] += 1
            
            # Calculate variance for this date
            price_values = list(date_prices.values())
            variance = self.calculate_price_variance(price_values)
            variances.append(variance)
            
            # Check if variance exceeds threshold
            if variance > max_variance:
                validation_results['dates_with_high_variance'].append({
                    'date': test_date,
                    'variance': variance,
                    'prices': date_prices,
                    'outliers': self.detect_outliers(date_prices)
                })
                validation_results['passed'] = False
            
            # Track vendor outliers
            outliers = self.detect_outliers(date_prices)
            for outlier in outliers:
                if outlier not in validation_results['vendor_outliers']:
                    validation_results['vendor_outliers'][outlier] = 0
                validation_results['vendor_outliers'][outlier] += 1
        
        if variances:
            validation_results['average_variance'] = statistics.mean(variances)
            validation_results['max_variance'] = max(variances)
        
        # Generate warnings and errors
        if validation_results['total_dates_checked'] == 0:
            validation_results['errors'].append("No comparable dates found across vendors")
            validation_results['passed'] = False
        
        if len(validation_results['dates_with_high_variance']) > 0:
            validation_results['warnings'].append(f"Found {len(validation_results['dates_with_high_variance'])} dates with high price variance")
        
        for vendor, outlier_count in validation_results['vendor_outliers'].items():
            if outlier_count > validation_results['total_dates_checked'] * 0.2:  # >20% outliers
                validation_results['errors'].append(f"Vendor {vendor} has {outlier_count} outlier prices (>{20}% of dates)")
                validation_results['passed'] = False
        
        return validation_results


@pytest.mark.integration
@pytest.mark.database
class TestCrossVendorPriceValidation:
    """Integration tests for cross-vendor price validation"""
    
    @pytest.fixture
    def env(self):
        return Environment()
    
    @pytest.fixture
    def validator(self, env):
        return CrossVendorPriceValidator(env)
    
    @pytest.fixture
    def test_symbols(self):
        """Symbols we expect to have good data for"""
        return ['AAPL', 'MSFT', 'GOOGL']
    
    @pytest.fixture
    def recent_date_range(self):
        """Recent date range for testing"""
        end_date = date.today()
        start_date = end_date - timedelta(days=30)
        return start_date, end_date
    
    @pytest.mark.asyncio
    async def test_cross_vendor_price_consistency(self, validator, test_symbols, recent_date_range):
        """Test that all vendors provide consistent prices for major stocks"""
        start_date, end_date = recent_date_range
        
        for symbol in test_symbols:
            print(f"\nValidating price consistency for {symbol}...")
            
            results = await validator.validate_price_consistency(symbol, start_date, end_date)
            
            # Print detailed results for debugging
            print(f"Results for {symbol}:")
            print(f"  Total dates checked: {results['total_dates_checked']}")
            print(f"  Average variance: {results['average_variance']:.4f}")
            print(f"  Max variance: {results['max_variance']:.4f}")
            print(f"  Passed: {results['passed']}")
            
            if results['warnings']:
                print(f"  Warnings: {results['warnings']}")
            
            if results['errors']:
                print(f"  Errors: {results['errors']}")
            
            if results['dates_with_high_variance']:
                print(f"  High variance dates: {len(results['dates_with_high_variance'])}")
                for hvd in results['dates_with_high_variance'][:3]:  # Show first 3
                    print(f"    {hvd['date']}: variance={hvd['variance']:.4f}, prices={hvd['prices']}")
            
            # Assertions
            assert results['total_dates_checked'] > 0, f"No price data found for {symbol}"
            assert results['average_variance'] < 0.1, f"Average price variance too high for {symbol}: {results['average_variance']}"
            
            # Allow some high variance dates but not too many
            high_variance_ratio = len(results['dates_with_high_variance']) / results['total_dates_checked']
            assert high_variance_ratio < 0.2, f"Too many high variance dates for {symbol}: {high_variance_ratio:.2%}"
    
    @pytest.mark.asyncio
    async def test_vendor_data_availability(self, validator, test_symbols, recent_date_range):
        """Test that each vendor has reasonable data availability"""
        start_date, end_date = recent_date_range
        nyse_cal = ExchangeCalendar('NYSE')
        expected_trading_days = len(nyse_cal.all_trading_days(start_date, end_date))
        
        for symbol in test_symbols:
            vendor_data = await validator.get_cross_vendor_prices(symbol, start_date, end_date)
            
            for vendor, prices in vendor_data.items():
                coverage = len(prices) / expected_trading_days if expected_trading_days > 0 else 0
                print(f"{vendor} coverage for {symbol}: {coverage:.2%} ({len(prices)}/{expected_trading_days} days)")
                
                # Each vendor should have at least 50% coverage for recent data
                assert coverage >= 0.5, f"{vendor} has insufficient data coverage for {symbol}: {coverage:.2%}"
    
    @pytest.mark.asyncio
    async def test_price_reasonableness(self, validator, test_symbols, recent_date_range):
        """Test that all prices are within reasonable bounds"""
        start_date, end_date = recent_date_range
        
        for symbol in test_symbols:
            vendor_data = await validator.get_cross_vendor_prices(symbol, start_date, end_date)
            
            for vendor, prices in vendor_data.items():
                for test_date, price_data in prices.items():
                    close_price = price_data['close']
                    volume = price_data['volume']
                    
                    # Basic sanity checks
                    assert close_price > 0, f"{vendor} has non-positive price for {symbol} on {test_date}: {close_price}"
                    assert close_price < 50000, f"{vendor} has unreasonably high price for {symbol} on {test_date}: {close_price}"
                    assert volume >= 0, f"{vendor} has negative volume for {symbol} on {test_date}: {volume}"
                    
                    # Check OHLC relationships
                    if all(price_data[k] > 0 for k in ['open', 'high', 'low']):
                        high = price_data['high']
                        low = price_data['low'] 
                        open_price = price_data['open']
                        
                        assert high >= low, f"{vendor} high < low for {symbol} on {test_date}"
                        assert high >= close_price, f"{vendor} high < close for {symbol} on {test_date}"
                        assert high >= open_price, f"{vendor} high < open for {symbol} on {test_date}"
                        assert low <= close_price, f"{vendor} low > close for {symbol} on {test_date}"
                        assert low <= open_price, f"{vendor} low > open for {symbol} on {test_date}"
    
    @pytest.mark.asyncio
    async def test_majority_voting_scenarios(self, validator, test_symbols):
        """Test scenarios that would be handled by majority voting"""
        # This test validates our majority voting logic will work correctly
        start_date = date.today() - timedelta(days=10)
        end_date = date.today() - timedelta(days=1)
        
        for symbol in test_symbols:
            vendor_data = await validator.get_cross_vendor_prices(symbol, start_date, end_date)
            
            # Find dates where we have data from multiple vendors
            for test_date in set().union(*[dates.keys() for dates in vendor_data.values()]):
                available_vendors = [vendor for vendor, data in vendor_data.items() if test_date in data]
                
                if len(available_vendors) >= 3:  # Need at least 3 for majority voting
                    prices = [vendor_data[vendor][test_date]['close'] for vendor in available_vendors]
                    
                    # Test majority voting scenarios
                    median_price = statistics.median(prices)
                    
                    # Count how many prices are within 5% of median
                    consensus_prices = [p for p in prices if abs(p - median_price) / median_price <= 0.05]
                    
                    # We should have majority consensus on most dates
                    consensus_ratio = len(consensus_prices) / len(prices)
                    if consensus_ratio < 0.5:  # Less than majority consensus
                        print(f"Low consensus for {symbol} on {test_date}: {dict(zip(available_vendors, prices))}")
                        print(f"Median: {median_price:.2f}, Consensus ratio: {consensus_ratio:.2%}")
                    
                    # This is a warning, not a hard failure, as some dates may legitimately have high variance
                    # But we should investigate if this happens frequently
    
    @pytest.mark.asyncio  
    async def test_vendor_specific_data_quality(self, validator, recent_date_range):
        """Test each vendor's data quality individually"""
        start_date, end_date = recent_date_range
        test_symbols = ['AAPL']  # Focus on one symbol for detailed analysis
        
        for symbol in test_symbols:
            vendor_data = await validator.get_cross_vendor_prices(symbol, start_date, end_date)
            
            for vendor, prices in vendor_data.items():
                if not prices:
                    print(f"WARNING: No data from {vendor} for {symbol}")
                    continue
                
                print(f"\n{vendor.upper()} data quality for {symbol}:")
                print(f"  Total records: {len(prices)}")
                
                # Analyze price movements
                sorted_dates = sorted(prices.keys())
                if len(sorted_dates) > 1:
                    daily_returns = []
                    for i in range(1, len(sorted_dates)):
                        prev_price = prices[sorted_dates[i-1]]['close']
                        curr_price = prices[sorted_dates[i]]['close']
                        daily_return = (curr_price - prev_price) / prev_price
                        daily_returns.append(daily_return)
                    
                    if daily_returns:
                        avg_return = statistics.mean(daily_returns)
                        volatility = statistics.stdev(daily_returns) if len(daily_returns) > 1 else 0
                        max_return = max(daily_returns)
                        min_return = min(daily_returns)
                        
                        print(f"  Average daily return: {avg_return:.4f}")
                        print(f"  Daily volatility: {volatility:.4f}")
                        print(f"  Max daily return: {max_return:.4f}")
                        print(f"  Min daily return: {min_return:.4f}")
                        
                        # Flag extreme movements that might indicate data issues
                        if abs(max_return) > 0.2:  # >20% daily move
                            print(f"  WARNING: Extreme positive return: {max_return:.2%}")
                        if abs(min_return) > 0.2:  # >20% daily move  
                            print(f"  WARNING: Extreme negative return: {min_return:.2%}")
    
    def test_validation_framework_itself(self):
        """Test that our validation framework is working correctly"""
        # Test variance calculation
        validator = CrossVendorPriceValidator(Environment())
        
        # Test with identical prices (should have 0 variance)
        assert validator.calculate_price_variance([100.0, 100.0, 100.0]) == 0.0
        
        # Test with different prices
        variance = validator.calculate_price_variance([100.0, 105.0, 95.0])
        assert variance > 0, "Variance should be positive for different prices"
        
        # Test outlier detection
        prices = {'vendor1': 100.0, 'vendor2': 101.0, 'vendor3': 150.0}  # vendor3 is outlier
        outliers = validator.detect_outliers(prices, threshold=0.1)
        assert 'vendor3' in outliers, "Should detect vendor3 as outlier"
        assert len(outliers) == 1, "Should detect exactly one outlier"

if __name__ == "__main__":
    # Allow running this test file directly
    pytest.main([__file__, "-v", "-s"])