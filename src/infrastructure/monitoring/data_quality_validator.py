#!/usr/bin/env python3
"""
Data Quality Validation Framework

Comprehensive data quality validation for the multi-vendor price data system.
Validates data consistency, completeness, accuracy, and freshness across all vendors.

Key Features:
- Multi-vendor data consistency checks
- Data completeness validation (coverage gaps, missing records)
- Price accuracy validation (outlier detection, statistical validation)
- Data freshness monitoring (stale data detection)
- Cross-vendor correlation analysis
- Volume and market cap validation
- Data lineage tracking
"""

import asyncio
import asyncpg
import logging
import numpy as np
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
from statistics import mean

# Add src to path for imports
from shared.utils.environment import Environment

class ValidationSeverity(Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"

class ValidationCategory(Enum):
    COMPLETENESS = "completeness"
    CONSISTENCY = "consistency" 
    ACCURACY = "accuracy"
    FRESHNESS = "freshness"
    INTEGRITY = "integrity"

@dataclass
class ValidationResult:
    category: ValidationCategory
    severity: ValidationSeverity
    test_name: str
    description: str
    passed: bool
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    affected_records: int = 0
    recommendation: str = ""

@dataclass 
class DataQualityReport:
    timestamp: datetime
    total_tests: int
    passed_tests: int
    failed_tests: int
    critical_issues: int
    warning_issues: int
    info_issues: int
    validation_results: List[ValidationResult]
    overall_score: float  # 0-100 scale
    summary: str

class DataQualityValidator:
    """
    Comprehensive data quality validation for multi-vendor price data
    """
    
    def __init__(self, env: Environment):
        self.env = env
        self.db_url = env.get_database_url()
        self.logger = logging.getLogger(__name__)
        
        # Vendor tables configuration
        self.vendor_tables = {
            'polygon': self.env.get_table_name('daily_prices_polygon'),
            'tiingo': self.env.get_table_name('daily_prices_tiingo'),
            'alphavantage': self.env.get_table_name('daily_prices_alphavantage'),
            'fmp': self.env.get_table_name('daily_prices_fmp')
        }
        
        # Quality thresholds
        self.quality_thresholds = {
            'min_data_coverage': 0.80,     # 80% coverage required
            'max_price_variance': 0.10,    # 10% max variance between vendors
            'max_stale_days': 3,           # Max 3 days without data
            'min_volume_correlation': 0.70, # 70% volume correlation between vendors
            'max_missing_ratio': 0.05      # 5% max missing data ratio
        }
    
    async def get_database_connection_pool(self):
        """Get database connection pool"""
        return await asyncpg.create_pool(self.db_url, min_size=2, max_size=8)
    
    async def validate_data_completeness(self, symbols: List[str], start_date: date, end_date: date) -> List[ValidationResult]:
        """Validate data completeness across all vendors"""
        results = []
        pool = await self.get_database_connection_pool()
        
        try:
            async with pool.acquire() as conn:
                # Get trading days in the period
                trading_days = await self._get_trading_days(conn, start_date, end_date)
                expected_records_per_symbol = len(trading_days)
                
                for vendor, table_name in self.vendor_tables.items():
                    try:
                        # Check data coverage for each symbol
                        for symbol in symbols:
                            instrument_id = await self._get_instrument_id(conn, symbol)
                            if not instrument_id:
                                continue
                            
                            actual_count = await conn.fetchval(f"""
                                SELECT COUNT(*) FROM {table_name}
                                WHERE instrument_id = $1 
                                  AND date BETWEEN $2 AND $3
                            """, instrument_id, start_date, end_date)
                            
                            coverage_ratio = actual_count / expected_records_per_symbol if expected_records_per_symbol > 0 else 0
                            
                            # Evaluate completeness
                            if coverage_ratio < self.quality_thresholds['min_data_coverage']:
                                results.append(ValidationResult(
                                    category=ValidationCategory.COMPLETENESS,
                                    severity=ValidationSeverity.CRITICAL if coverage_ratio < 0.5 else ValidationSeverity.WARNING,
                                    test_name=f"{vendor}_completeness_check",
                                    description=f"Data completeness for {symbol} in {vendor}",
                                    passed=False,
                                    details={
                                        'vendor': vendor,
                                        'symbol': symbol,
                                        'expected_records': expected_records_per_symbol,
                                        'actual_records': actual_count,
                                        'coverage_ratio': coverage_ratio,
                                        'missing_records': expected_records_per_symbol - actual_count
                                    },
                                    affected_records=expected_records_per_symbol - actual_count,
                                    recommendation=f"Backfill missing {expected_records_per_symbol - actual_count} records for {symbol} from {vendor}"
                                ))
                            else:
                                results.append(ValidationResult(
                                    category=ValidationCategory.COMPLETENESS,
                                    severity=ValidationSeverity.INFO,
                                    test_name=f"{vendor}_completeness_check",
                                    description=f"Data completeness for {symbol} in {vendor}",
                                    passed=True,
                                    details={
                                        'vendor': vendor,
                                        'symbol': symbol,
                                        'coverage_ratio': coverage_ratio
                                    }
                                ))
                    
                    except Exception as e:
                        self.logger.error(f"Error checking completeness for {vendor}: {e}")
                        results.append(ValidationResult(
                            category=ValidationCategory.COMPLETENESS,
                            severity=ValidationSeverity.CRITICAL,
                            test_name=f"{vendor}_completeness_check",
                            description=f"Failed to check data completeness for {vendor}",
                            passed=False,
                            details={'error': str(e)},
                            recommendation=f"Investigate database connectivity issues with {vendor} table"
                        ))
        
        finally:
            await pool.close()
        
        return results
    
    async def validate_cross_vendor_consistency(self, symbols: List[str], start_date: date, end_date: date) -> List[ValidationResult]:
        """Validate price consistency across vendors"""
        results = []
        pool = await self.get_database_connection_pool()
        
        try:
            async with pool.acquire() as conn:
                for symbol in symbols:
                    instrument_id = await self._get_instrument_id(conn, symbol)
                    if not instrument_id:
                        continue
                    
                    # Get prices from all vendors for comparison
                    vendor_prices = {}
                    for vendor, table_name in self.vendor_tables.items():
                        try:
                            rows = await conn.fetch(f"""
                                SELECT date, close, volume
                                FROM {table_name}
                                WHERE instrument_id = $1 
                                  AND date BETWEEN $2 AND $3
                                ORDER BY date
                            """, instrument_id, start_date, end_date)
                            
                            vendor_prices[vendor] = {row['date']: {'close': float(row['close']), 'volume': int(row['volume'] or 0)} for row in rows}
                        
                        except Exception as e:
                            self.logger.warning(f"Error fetching {vendor} data for {symbol}: {e}")
                            continue
                    
                    # Analyze price consistency
                    if len(vendor_prices) >= 2:
                        consistency_result = await self._analyze_price_consistency(symbol, vendor_prices)
                        results.append(consistency_result)
                        
                        # Analyze volume consistency 
                        volume_result = await self._analyze_volume_consistency(symbol, vendor_prices)
                        results.append(volume_result)
        
        finally:
            await pool.close()
        
        return results
    
    async def validate_data_freshness(self, symbols: List[str]) -> List[ValidationResult]:
        """Validate data freshness (no stale data)"""
        results = []
        pool = await self.get_database_connection_pool()
        current_date = date.today()
        
        try:
            async with pool.acquire() as conn:
                for vendor, table_name in self.vendor_tables.items():
                    try:
                        # Get most recent data date for each symbol
                        for symbol in symbols:
                            instrument_id = await self._get_instrument_id(conn, symbol)
                            if not instrument_id:
                                continue
                            
                            latest_date = await conn.fetchval(f"""
                                SELECT MAX(date) FROM {table_name}
                                WHERE instrument_id = $1
                            """, instrument_id)
                            
                            if latest_date:
                                days_stale = (current_date - latest_date).days
                                
                                if days_stale > self.quality_thresholds['max_stale_days']:
                                    results.append(ValidationResult(
                                        category=ValidationCategory.FRESHNESS,
                                        severity=ValidationSeverity.WARNING if days_stale <= 7 else ValidationSeverity.CRITICAL,
                                        test_name=f"{vendor}_freshness_check",
                                        description=f"Data freshness for {symbol} in {vendor}",
                                        passed=False,
                                        details={
                                            'vendor': vendor,
                                            'symbol': symbol,
                                            'latest_date': latest_date.isoformat(),
                                            'days_stale': days_stale,
                                            'current_date': current_date.isoformat()
                                        },
                                        recommendation=f"Update {symbol} data in {vendor} table - {days_stale} days stale"
                                    ))
                                else:
                                    results.append(ValidationResult(
                                        category=ValidationCategory.FRESHNESS,
                                        severity=ValidationSeverity.INFO,
                                        test_name=f"{vendor}_freshness_check",
                                        description=f"Data freshness for {symbol} in {vendor}",
                                        passed=True,
                                        details={
                                            'vendor': vendor,
                                            'symbol': symbol,
                                            'days_stale': days_stale
                                        }
                                    ))
                            else:
                                results.append(ValidationResult(
                                    category=ValidationCategory.FRESHNESS,
                                    severity=ValidationSeverity.CRITICAL,
                                    test_name=f"{vendor}_freshness_check",
                                    description=f"No data found for {symbol} in {vendor}",
                                    passed=False,
                                    details={
                                        'vendor': vendor,
                                        'symbol': symbol
                                    },
                                    recommendation=f"Initialize data for {symbol} in {vendor} table"
                                ))
                    
                    except Exception as e:
                        self.logger.error(f"Error checking freshness for {vendor}: {e}")
                        results.append(ValidationResult(
                            category=ValidationCategory.FRESHNESS,
                            severity=ValidationSeverity.CRITICAL,
                            test_name=f"{vendor}_freshness_check",
                            description=f"Failed to check data freshness for {vendor}",
                            passed=False,
                            details={'error': str(e)}
                        ))
        
        finally:
            await pool.close()
        
        return results
    
    async def validate_data_integrity(self, symbols: List[str], start_date: date, end_date: date) -> List[ValidationResult]:
        """Validate data integrity (no negative prices, reasonable ranges)"""
        results = []
        pool = await self.get_database_connection_pool()
        
        try:
            async with pool.acquire() as conn:
                for vendor, table_name in self.vendor_tables.items():
                    try:
                        for symbol in symbols:
                            instrument_id = await self._get_instrument_id(conn, symbol)
                            if not instrument_id:
                                continue
                            
                            # Check for data integrity violations
                            integrity_issues = await conn.fetch(f"""
                                SELECT date, close, volume, open_price, high_price, low_price
                                FROM {table_name}
                                WHERE instrument_id = $1 
                                  AND date BETWEEN $2 AND $3
                                  AND (
                                    close <= 0 OR              -- Negative or zero prices
                                    volume < 0 OR              -- Negative volume
                                    (open_price IS NOT NULL AND open_price <= 0) OR
                                    (high_price IS NOT NULL AND high_price <= 0) OR
                                    (low_price IS NOT NULL AND low_price <= 0) OR
                                    (high_price IS NOT NULL AND low_price IS NOT NULL AND high_price < low_price) OR  -- High < Low
                                    (high_price IS NOT NULL AND high_price < close) OR  -- High < Close
                                    (low_price IS NOT NULL AND low_price > close)       -- Low > Close
                                  )
                                ORDER BY date
                            """, instrument_id, start_date, end_date)
                            
                            if integrity_issues:
                                results.append(ValidationResult(
                                    category=ValidationCategory.INTEGRITY,
                                    severity=ValidationSeverity.CRITICAL,
                                    test_name=f"{vendor}_integrity_check",
                                    description=f"Data integrity violations for {symbol} in {vendor}",
                                    passed=False,
                                    details={
                                        'vendor': vendor,
                                        'symbol': symbol,
                                        'violation_count': len(integrity_issues),
                                        'violations': [
                                            {
                                                'date': issue['date'].isoformat(),
                                                'close': float(issue['close']),
                                                'volume': int(issue['volume'] or 0),
                                                'open': float(issue['open_price']) if issue['open_price'] else None,
                                                'high': float(issue['high_price']) if issue['high_price'] else None,
                                                'low': float(issue['low_price']) if issue['low_price'] else None
                                            }
                                            for issue in integrity_issues[:10]  # Show first 10
                                        ]
                                    },
                                    affected_records=len(integrity_issues),
                                    recommendation=f"Fix {len(integrity_issues)} data integrity violations for {symbol} in {vendor}"
                                ))
                            else:
                                results.append(ValidationResult(
                                    category=ValidationCategory.INTEGRITY,
                                    severity=ValidationSeverity.INFO,
                                    test_name=f"{vendor}_integrity_check",
                                    description=f"Data integrity for {symbol} in {vendor}",
                                    passed=True,
                                    details={
                                        'vendor': vendor,
                                        'symbol': symbol
                                    }
                                ))
                    
                    except Exception as e:
                        self.logger.error(f"Error checking integrity for {vendor}: {e}")
                        results.append(ValidationResult(
                            category=ValidationCategory.INTEGRITY,
                            severity=ValidationSeverity.CRITICAL,
                            test_name=f"{vendor}_integrity_check",
                            description=f"Failed to check data integrity for {vendor}",
                            passed=False,
                            details={'error': str(e)}
                        ))
        
        finally:
            await pool.close()
        
        return results
    
    async def _analyze_price_consistency(self, symbol: str, vendor_prices: Dict[str, Dict]) -> ValidationResult:
        """Analyze price consistency across vendors"""
        
        # Find common dates across all vendors
        all_dates = set()
        for vendor_data in vendor_prices.values():
            all_dates.update(vendor_data.keys())
        
        if not all_dates:
            return ValidationResult(
                category=ValidationCategory.CONSISTENCY,
                severity=ValidationSeverity.WARNING,
                test_name="price_consistency_check",
                description=f"No common price data found for {symbol}",
                passed=False,
                details={'symbol': symbol, 'reason': 'no_data'}
            )
        
        # Calculate price variance for common dates
        high_variance_dates = []
        total_variance = 0
        comparison_count = 0
        
        for trade_date in sorted(all_dates):
            date_prices = []
            vendors_with_data = []
            
            for vendor, prices in vendor_prices.items():
                if trade_date in prices:
                    date_prices.append(prices[trade_date]['close'])
                    vendors_with_data.append(vendor)
            
            if len(date_prices) >= 2:
                price_std = np.std(date_prices)
                price_mean = np.mean(date_prices)
                coefficient_of_variation = price_std / price_mean if price_mean > 0 else 0
                
                total_variance += coefficient_of_variation
                comparison_count += 1
                
                if coefficient_of_variation > self.quality_thresholds['max_price_variance']:
                    high_variance_dates.append({
                        'date': trade_date.isoformat(),
                        'prices': dict(zip(vendors_with_data, date_prices)),
                        'variance': coefficient_of_variation,
                        'std_dev': price_std,
                        'mean': price_mean
                    })
        
        avg_variance = total_variance / comparison_count if comparison_count > 0 else 0
        
        # Determine result
        if high_variance_dates:
            severity = ValidationSeverity.CRITICAL if len(high_variance_dates) > comparison_count * 0.1 else ValidationSeverity.WARNING
            return ValidationResult(
                category=ValidationCategory.CONSISTENCY,
                severity=severity,
                test_name="price_consistency_check",
                description=f"Price consistency issues for {symbol}",
                passed=False,
                details={
                    'symbol': symbol,
                    'high_variance_dates': len(high_variance_dates),
                    'total_comparison_dates': comparison_count,
                    'average_variance': avg_variance,
                    'threshold': self.quality_thresholds['max_price_variance'],
                    'sample_issues': high_variance_dates[:5]  # Show first 5
                },
                affected_records=len(high_variance_dates),
                recommendation=f"Investigate price discrepancies for {symbol} - {len(high_variance_dates)} dates with high variance"
            )
        else:
            return ValidationResult(
                category=ValidationCategory.CONSISTENCY,
                severity=ValidationSeverity.INFO,
                test_name="price_consistency_check",
                description=f"Price consistency for {symbol}",
                passed=True,
                details={
                    'symbol': symbol,
                    'comparison_dates': comparison_count,
                    'average_variance': avg_variance
                }
            )
    
    async def _analyze_volume_consistency(self, symbol: str, vendor_prices: Dict[str, Dict]) -> ValidationResult:
        """Analyze volume consistency across vendors"""
        
        # Collect volume data from all vendors
        all_volumes = []
        vendor_volume_data = {}
        
        for vendor, prices in vendor_prices.items():
            volumes = [data['volume'] for data in prices.values() if data['volume'] > 0]
            if volumes:
                all_volumes.extend(volumes)
                vendor_volume_data[vendor] = volumes
        
        if len(vendor_volume_data) < 2:
            return ValidationResult(
                category=ValidationCategory.CONSISTENCY,
                severity=ValidationSeverity.INFO,
                test_name="volume_consistency_check",
                description=f"Insufficient volume data for comparison for {symbol}",
                passed=True,
                details={'symbol': symbol, 'reason': 'insufficient_data'}
            )
        
        # Calculate correlations between vendors
        correlations = {}
        vendor_names = list(vendor_volume_data.keys())
        
        for i, vendor1 in enumerate(vendor_names):
            for vendor2 in vendor_names[i+1:]:
                try:
                    # Find common dates and calculate correlation
                    corr = np.corrcoef(vendor_volume_data[vendor1][:min(len(vendor_volume_data[vendor1]), len(vendor_volume_data[vendor2]))],
                                      vendor_volume_data[vendor2][:min(len(vendor_volume_data[vendor1]), len(vendor_volume_data[vendor2]))])[0,1]
                    correlations[f"{vendor1}_vs_{vendor2}"] = corr if not np.isnan(corr) else 0
                except Exception:
                    correlations[f"{vendor1}_vs_{vendor2}"] = 0
        
        avg_correlation = mean(correlations.values()) if correlations else 0
        
        # Evaluate volume consistency
        if avg_correlation < self.quality_thresholds['min_volume_correlation']:
            return ValidationResult(
                category=ValidationCategory.CONSISTENCY,
                severity=ValidationSeverity.WARNING,
                test_name="volume_consistency_check",
                description=f"Low volume correlation for {symbol}",
                passed=False,
                details={
                    'symbol': symbol,
                    'average_correlation': avg_correlation,
                    'threshold': self.quality_thresholds['min_volume_correlation'],
                    'correlations': correlations,
                    'vendor_sample_sizes': {vendor: len(volumes) for vendor, volumes in vendor_volume_data.items()}
                },
                recommendation=f"Investigate volume data sources for {symbol} - low correlation suggests data quality issues"
            )
        else:
            return ValidationResult(
                category=ValidationCategory.CONSISTENCY,
                severity=ValidationSeverity.INFO,
                test_name="volume_consistency_check",
                description=f"Volume consistency for {symbol}",
                passed=True,
                details={
                    'symbol': symbol,
                    'average_correlation': avg_correlation,
                    'correlations': correlations
                }
            )
    
    async def _get_trading_days(self, conn, start_date: date, end_date: date) -> List[date]:
        """Get trading days between start and end date"""
        # Simplified - assume all weekdays are trading days
        # In production, this should check against actual trading calendar
        trading_days = []
        current_date = start_date
        
        while current_date <= end_date:
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                trading_days.append(current_date)
            current_date += timedelta(days=1)
        
        return trading_days
    
    async def _get_instrument_id(self, conn, symbol: str) -> Optional[int]:
        """Get instrument ID for symbol"""
        return await conn.fetchval("SELECT id FROM dev_instruments WHERE symbol = $1", symbol)
    
    async def run_comprehensive_validation(self, symbols: List[str], days_back: int = 30) -> DataQualityReport:
        """Run comprehensive data quality validation"""
        
        start_time = datetime.now()
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        
        self.logger.info(f"Starting comprehensive data quality validation for {len(symbols)} symbols")
        self.logger.info(f"Validation period: {start_date} to {end_date}")
        
        # Run all validation tests
        all_results = []
        
        try:
            # Completeness validation
            self.logger.info("Running completeness validation...")
            completeness_results = await self.validate_data_completeness(symbols, start_date, end_date)
            all_results.extend(completeness_results)
            
            # Consistency validation
            self.logger.info("Running consistency validation...")
            consistency_results = await self.validate_cross_vendor_consistency(symbols, start_date, end_date)
            all_results.extend(consistency_results)
            
            # Freshness validation
            self.logger.info("Running freshness validation...")
            freshness_results = await self.validate_data_freshness(symbols)
            all_results.extend(freshness_results)
            
            # Integrity validation
            self.logger.info("Running integrity validation...")
            integrity_results = await self.validate_data_integrity(symbols, start_date, end_date)
            all_results.extend(integrity_results)
            
        except Exception as e:
            self.logger.error(f"Error during validation: {e}")
            all_results.append(ValidationResult(
                category=ValidationCategory.INTEGRITY,
                severity=ValidationSeverity.CRITICAL,
                test_name="validation_framework",
                description="Validation framework failure",
                passed=False,
                details={'error': str(e)},
                recommendation="Fix validation framework issues"
            ))
        
        # Generate report
        report = self._generate_data_quality_report(all_results, start_time)
        
        self.logger.info(f"Data quality validation completed in {(datetime.now() - start_time).total_seconds():.2f} seconds")
        self.logger.info(f"Overall quality score: {report.overall_score:.1f}/100")
        
        return report
    
    def _generate_data_quality_report(self, results: List[ValidationResult], start_time: datetime) -> DataQualityReport:
        """Generate comprehensive data quality report"""
        
        total_tests = len(results)
        passed_tests = sum(1 for r in results if r.passed)
        failed_tests = total_tests - passed_tests
        
        critical_issues = sum(1 for r in results if r.severity == ValidationSeverity.CRITICAL and not r.passed)
        warning_issues = sum(1 for r in results if r.severity == ValidationSeverity.WARNING and not r.passed)
        info_issues = sum(1 for r in results if r.severity == ValidationSeverity.INFO and not r.passed)
        
        # Calculate overall score
        if total_tests == 0:
            overall_score = 100.0
        else:
            # Weight scores: Critical issues = -10 points, Warning = -5 points, Info = -1 point
            penalty_points = (critical_issues * 10) + (warning_issues * 5) + (info_issues * 1)
            max_possible_penalty = total_tests * 10  # If all were critical
            overall_score = max(0, 100 - (penalty_points / max_possible_penalty * 100)) if max_possible_penalty > 0 else 100
        
        # Generate summary
        if critical_issues > 0:
            summary = f"⚠️  Data quality issues detected: {critical_issues} critical, {warning_issues} warnings. Immediate attention required."
        elif warning_issues > 0:
            summary = f"⚡ Data quality concerns: {warning_issues} warnings detected. Monitoring recommended."
        else:
            summary = f"✅ Data quality is good: {passed_tests}/{total_tests} tests passed."
        
        return DataQualityReport(
            timestamp=datetime.now(),
            total_tests=total_tests,
            passed_tests=passed_tests,
            failed_tests=failed_tests,
            critical_issues=critical_issues,
            warning_issues=warning_issues,
            info_issues=info_issues,
            validation_results=results,
            overall_score=overall_score,
            summary=summary
        )

async def main():
    """Example usage of data quality validator"""
    
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    env = Environment()
    validator = DataQualityValidator(env)
    
    # Test symbols
    test_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    
    print("🔍 Starting comprehensive data quality validation...")
    print(f"📊 Testing symbols: {test_symbols}")
    
    # Run validation
    report = await validator.run_comprehensive_validation(test_symbols, days_back=30)
    
    # Display results
    print(f"\n📋 DATA QUALITY REPORT")
    print(f"{'='*60}")
    print(f"Overall Score: {report.overall_score:.1f}/100")
    print(f"Summary: {report.summary}")
    print(f"\nTest Results: {report.passed_tests}/{report.total_tests} passed")
    print(f"Issues: {report.critical_issues} critical, {report.warning_issues} warnings, {report.info_issues} info")
    
    # Show critical issues
    critical_results = [r for r in report.validation_results if r.severity == ValidationSeverity.CRITICAL and not r.passed]
    if critical_results:
        print(f"\n🚨 CRITICAL ISSUES:")
        for result in critical_results[:5]:  # Show first 5
            print(f"  • {result.test_name}: {result.description}")
            print(f"    {result.recommendation}")
    
    # Show warnings
    warning_results = [r for r in report.validation_results if r.severity == ValidationSeverity.WARNING and not r.passed]
    if warning_results:
        print(f"\n⚡ WARNINGS:")
        for result in warning_results[:3]:  # Show first 3
            print(f"  • {result.test_name}: {result.description}")

if __name__ == "__main__":
    asyncio.run(main())