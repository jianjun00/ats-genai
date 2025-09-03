"""
Data Quality Validator for Universe Management

Validates data quality for instruments in universes, ensuring reliable data
for backtesting and model training.
"""

import asyncio
import asyncpg
import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, NamedTuple
from dataclasses import dataclass
from enum import Enum
from shared.utils.environment import Environment
import gin

class ValidationLevel(Enum):
    """Data validation severity levels"""
    CRITICAL = "critical"  # Data unusable
    WARNING = "warning"   # Data has issues but usable
    INFO = "info"        # Informational only

@dataclass
class ValidationResult:
    """Result of a data quality validation check"""
    symbol: str
    check_name: str
    level: ValidationLevel
    passed: bool
    message: str
    details: Dict = None
    
class DataGap(NamedTuple):
    """Represents a gap in time series data"""
    start_date: date
    end_date: date
    expected_count: int
    actual_count: int
    gap_ratio: float

@gin.configurable
class DataQualityValidator:
    """
    Validates data quality for universe instruments across multiple dimensions:
    - Temporal continuity
    - Price reasonableness  
    - Volume consistency
    - Cross-source reconciliation
    """
    
    def __init__(self, env: Environment = None):
        self.env = env or Environment()
        self.logger = logging.getLogger(__name__)
        
        # Validation thresholds
        self.max_daily_price_jump = 0.20      # 20% max daily price change
        self.max_minute_price_jump = 0.05     # 5% max minute price change  
        self.min_volume_percentile = 0.01     # Minimum volume vs historical average
        self.max_zero_volume_ratio = 0.10     # Max 10% zero volume days
        self.min_trading_days_per_year = 220  # Minimum trading days expected
        self.max_gap_days = 5                 # Maximum consecutive missing days
        
    async def validate_universe_quality(self, universe_id: int, 
                                       validation_date: date = None) -> List[ValidationResult]:
        """
        Comprehensive data quality validation for all instruments in a universe.
        
        Args:
            universe_id: Universe to validate
            validation_date: Date for validation (defaults to today)
            
        Returns:
            List of validation results for all checks
        """
        validation_date = validation_date or date.today()
        self.logger.info(f"Starting universe quality validation for universe {universe_id}")
        
        pool = await asyncpg.create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                # Get universe members
                symbols = await self._get_universe_symbols(conn, universe_id, validation_date)
                self.logger.info(f"Validating {len(symbols)} symbols")
                
                all_results = []
                
                # Run validation checks for each symbol
                for symbol in symbols:
                    symbol_results = await self._validate_symbol(conn, symbol, validation_date)
                    all_results.extend(symbol_results)
                
                return all_results
        finally:
            await pool.close()
    
    async def _get_universe_symbols(self, conn, universe_id: int, as_of_date: date) -> List[str]:
        """Get active symbols in universe as of date"""
        
        query = f"""
        SELECT DISTINCT symbol 
        FROM {self.env.get_table_name('universe_membership')}
        WHERE universe_id = $1 
          AND start_at <= $2 
          AND (end_at IS NULL OR end_at > $2)
        ORDER BY symbol
        """
        
        rows = await conn.fetch(query, universe_id, as_of_date)
        return [row['symbol'] for row in rows]
    
    async def _validate_symbol(self, conn, symbol: str, validation_date: date) -> List[ValidationResult]:
        """Run all validation checks for a single symbol"""
        
        results = []
        
        # Daily data validation
        daily_results = await self._validate_daily_data(conn, symbol, validation_date)
        results.extend(daily_results)
        
        # Minute data validation  
        minute_results = await self._validate_minute_data(conn, symbol, validation_date)
        results.extend(minute_results)
        
        # Cross-validation between daily and minute
        cross_results = await self._validate_daily_minute_consistency(conn, symbol, validation_date)
        results.extend(cross_results)
        
        return results
    
    async def _validate_daily_data(self, conn, symbol: str, validation_date: date) -> List[ValidationResult]:
        """Validate daily data quality"""
        
        results = []
        
        # Check data availability and gaps
        gap_result = await self._check_daily_data_gaps(conn, symbol, validation_date)
        if gap_result:
            results.append(gap_result)
        
        # Check price reasonableness
        price_result = await self._check_daily_price_reasonableness(conn, symbol, validation_date)
        if price_result:
            results.append(price_result)
        
        # Check volume consistency
        volume_result = await self._check_daily_volume_consistency(conn, symbol, validation_date)
        if volume_result:
            results.append(volume_result)
        
        return results
    
    async def _check_daily_data_gaps(self, conn, symbol: str, 
                                   validation_date: date) -> Optional[ValidationResult]:
        """Check for gaps in daily data"""
        
        # Look back 2 years for gap analysis
        start_date = validation_date - timedelta(days=730)
        
        query = f"""
        WITH date_series AS (
            SELECT generate_series($2::date, $3::date, '1 day'::interval)::date AS date
        ),
        weekdays AS (
            SELECT date FROM date_series 
            WHERE EXTRACT(DOW FROM date) BETWEEN 1 AND 5  -- Mon-Fri
        ),
        data_dates AS (
            SELECT DISTINCT date FROM (
                SELECT date FROM {self.env.get_table_name('daily_prices_polygon')} WHERE symbol = $1
                UNION
                SELECT date FROM {self.env.get_table_name('daily_prices_tiingo')} WHERE symbol = $1  
                UNION
                SELECT date FROM {self.env.get_table_name('daily_prices')} WHERE symbol = $1
            ) all_daily_data
            WHERE date BETWEEN $2 AND $3
        )
        SELECT 
            COUNT(w.date) as expected_days,
            COUNT(d.date) as actual_days,
            COUNT(w.date) - COUNT(d.date) as missing_days
        FROM weekdays w
        LEFT JOIN data_dates d ON w.date = d.date
        """
        
        try:
            row = await conn.fetchrow(query, symbol, start_date, validation_date)
            
            expected_days = row['expected_days']
            actual_days = row['actual_days']
            missing_days = row['missing_days']
            
            if expected_days == 0:
                return ValidationResult(
                    symbol=symbol,
                    check_name="daily_data_availability",
                    level=ValidationLevel.CRITICAL,
                    passed=False,
                    message="No daily data found",
                    details={"expected_days": expected_days, "actual_days": actual_days}
                )
            
            completeness_ratio = actual_days / expected_days
            
            if completeness_ratio < 0.85:  # Less than 85% complete
                level = ValidationLevel.CRITICAL if completeness_ratio < 0.70 else ValidationLevel.WARNING
                return ValidationResult(
                    symbol=symbol,
                    check_name="daily_data_gaps",
                    level=level,
                    passed=False,
                    message=f"Daily data only {completeness_ratio:.1%} complete ({missing_days} missing days)",
                    details={
                        "expected_days": expected_days,
                        "actual_days": actual_days,
                        "missing_days": missing_days,
                        "completeness_ratio": completeness_ratio
                    }
                )
            
            return ValidationResult(
                symbol=symbol,
                check_name="daily_data_gaps",
                level=ValidationLevel.INFO,
                passed=True,
                message=f"Daily data {completeness_ratio:.1%} complete",
                details={"completeness_ratio": completeness_ratio}
            )
            
        except Exception as e:
            return ValidationResult(
                symbol=symbol,
                check_name="daily_data_gaps",
                level=ValidationLevel.CRITICAL,
                passed=False,
                message=f"Error checking daily data gaps: {e}",
                details={"error": str(e)}
            )
    
    async def _check_daily_price_reasonableness(self, conn, symbol: str,
                                              validation_date: date) -> Optional[ValidationResult]:
        """Check for unreasonable daily price movements"""
        
        # Look back 1 year for price analysis
        start_date = validation_date - timedelta(days=365)
        
        query = f"""
        WITH daily_data AS (
            SELECT date, close, 
                   LAG(close) OVER (ORDER BY date) as prev_close
            FROM (
                SELECT date, close FROM {self.env.get_table_name('daily_prices_polygon')} WHERE symbol = $1
                UNION
                SELECT date, close FROM {self.env.get_table_name('daily_prices_tiingo')} WHERE symbol = $1
                UNION  
                SELECT date, close FROM {self.env.get_table_name('daily_prices')} WHERE symbol = $1
            ) all_data
            WHERE date BETWEEN $2 AND $3 AND close > 0
            ORDER BY date
        ),
        price_changes AS (
            SELECT date, close, prev_close,
                   ABS((close - prev_close) / NULLIF(prev_close, 0)) as abs_change
            FROM daily_data
            WHERE prev_close IS NOT NULL AND prev_close > 0
        )
        SELECT 
            COUNT(*) as total_days,
            COUNT(*) FILTER (WHERE abs_change > $4) as extreme_moves,
            MAX(abs_change) as max_change,
            AVG(abs_change) as avg_change
        FROM price_changes
        """
        
        try:
            row = await conn.fetchrow(query, symbol, start_date, validation_date, 
                                    self.max_daily_price_jump)
            
            if not row or row['total_days'] == 0:
                return ValidationResult(
                    symbol=symbol,
                    check_name="daily_price_reasonableness", 
                    level=ValidationLevel.WARNING,
                    passed=False,
                    message="Insufficient price data for analysis",
                    details={}
                )
            
            total_days = row['total_days']
            extreme_moves = row['extreme_moves'] or 0
            max_change = row['max_change'] or 0
            avg_change = row['avg_change'] or 0
            
            extreme_ratio = extreme_moves / total_days
            
            if extreme_ratio > 0.05:  # More than 5% extreme moves
                return ValidationResult(
                    symbol=symbol,
                    check_name="daily_price_reasonableness",
                    level=ValidationLevel.WARNING,
                    passed=False,
                    message=f"High frequency of extreme price moves ({extreme_ratio:.1%})",
                    details={
                        "extreme_moves": extreme_moves,
                        "total_days": total_days,
                        "extreme_ratio": extreme_ratio,
                        "max_change": max_change,
                        "avg_change": avg_change
                    }
                )
            
            return ValidationResult(
                symbol=symbol,
                check_name="daily_price_reasonableness",
                level=ValidationLevel.INFO,
                passed=True,
                message=f"Price movements reasonable (max: {max_change:.1%})",
                details={"max_change": max_change, "avg_change": avg_change}
            )
            
        except Exception as e:
            return ValidationResult(
                symbol=symbol,
                check_name="daily_price_reasonableness",
                level=ValidationLevel.WARNING,
                passed=False,
                message=f"Error checking price reasonableness: {e}",
                details={"error": str(e)}
            )
    
    async def _check_daily_volume_consistency(self, conn, symbol: str,
                                            validation_date: date) -> Optional[ValidationResult]:
        """Check daily volume consistency"""
        
        start_date = validation_date - timedelta(days=365)
        
        query = f"""
        WITH daily_volumes AS (
            SELECT date, volume
            FROM (
                SELECT date, volume FROM {self.env.get_table_name('daily_prices_polygon')} WHERE symbol = $1
                UNION
                SELECT date, volume FROM {self.env.get_table_name('daily_prices_tiingo')} WHERE symbol = $1
                UNION
                SELECT date, volume FROM {self.env.get_table_name('daily_prices')} WHERE symbol = $1
            ) all_data
            WHERE date BETWEEN $2 AND $3 AND volume IS NOT NULL
        )
        SELECT 
            COUNT(*) as total_days,
            COUNT(*) FILTER (WHERE volume = 0) as zero_volume_days,
            AVG(volume) as avg_volume,
            STDDEV(volume) as volume_stddev,
            MIN(volume) as min_volume,
            MAX(volume) as max_volume
        FROM daily_volumes
        WHERE volume >= 0
        """
        
        try:
            row = await conn.fetchrow(query, symbol, start_date, validation_date)
            
            if not row or row['total_days'] == 0:
                return ValidationResult(
                    symbol=symbol,
                    check_name="daily_volume_consistency",
                    level=ValidationLevel.WARNING,
                    passed=False,
                    message="No volume data available",
                    details={}
                )
            
            total_days = row['total_days']
            zero_volume_days = row['zero_volume_days'] or 0
            avg_volume = row['avg_volume'] or 0
            
            zero_volume_ratio = zero_volume_days / total_days
            
            if zero_volume_ratio > self.max_zero_volume_ratio:
                return ValidationResult(
                    symbol=symbol,
                    check_name="daily_volume_consistency",
                    level=ValidationLevel.WARNING,
                    passed=False,
                    message=f"High proportion of zero volume days ({zero_volume_ratio:.1%})",
                    details={
                        "zero_volume_days": zero_volume_days,
                        "total_days": total_days,
                        "zero_volume_ratio": zero_volume_ratio,
                        "avg_volume": avg_volume
                    }
                )
            
            return ValidationResult(
                symbol=symbol,
                check_name="daily_volume_consistency",
                level=ValidationLevel.INFO,
                passed=True,
                message=f"Volume data consistent (avg: {avg_volume:,.0f})",
                details={"avg_volume": avg_volume, "zero_volume_ratio": zero_volume_ratio}
            )
            
        except Exception as e:
            return ValidationResult(
                symbol=symbol,
                check_name="daily_volume_consistency",
                level=ValidationLevel.WARNING,
                passed=False,
                message=f"Error checking volume consistency: {e}",
                details={"error": str(e)}
            )
    
    async def _validate_minute_data(self, conn, symbol: str, 
                                  validation_date: date) -> List[ValidationResult]:
        """Validate minute data quality"""
        
        results = []
        
        # Check minute data availability
        availability_result = await self._check_minute_data_availability(conn, symbol, validation_date)
        if availability_result:
            results.append(availability_result)
        
        # Check minute price continuity  
        continuity_result = await self._check_minute_price_continuity(conn, symbol, validation_date)
        if continuity_result:
            results.append(continuity_result)
        
        return results
    
    async def _check_minute_data_availability(self, conn, symbol: str,
                                            validation_date: date) -> Optional[ValidationResult]:
        """Check minute data availability and gaps"""
        
        # Check last 30 trading days
        start_date = validation_date - timedelta(days=45)  # Buffer for weekends
        
        query = f"""
        SELECT 
            COUNT(DISTINCT DATE(timestamp)) as trading_days,
            COUNT(*) as total_bars,
            MIN(timestamp) as first_bar,
            MAX(timestamp) as last_bar,
            AVG(EXTRACT(EPOCH FROM (
                LEAD(timestamp) OVER (PARTITION BY DATE(timestamp) ORDER BY timestamp) - timestamp
            ))) / 60 as avg_interval_minutes
        FROM {self.env.get_table_name('minute_bars')}
        WHERE symbol = $1 AND timestamp >= $2
        """
        
        try:
            row = await conn.fetchrow(query, symbol, start_date)
            
            if not row or row['total_bars'] == 0:
                return ValidationResult(
                    symbol=symbol,
                    check_name="minute_data_availability",
                    level=ValidationLevel.CRITICAL,
                    passed=False,
                    message="No minute data found",
                    details={}
                )
            
            trading_days = row['trading_days']
            total_bars = row['total_bars']
            avg_interval = row['avg_interval_minutes']
            
            # Expect ~390 bars per trading day (6.5 hours * 60 minutes)
            expected_bars = trading_days * 390
            completeness_ratio = total_bars / max(expected_bars, 1)
            
            if completeness_ratio < 0.70:
                level = ValidationLevel.CRITICAL if completeness_ratio < 0.50 else ValidationLevel.WARNING
                return ValidationResult(
                    symbol=symbol,
                    check_name="minute_data_availability",
                    level=level,
                    passed=False,
                    message=f"Minute data only {completeness_ratio:.1%} complete",
                    details={
                        "trading_days": trading_days,
                        "total_bars": total_bars,
                        "expected_bars": expected_bars,
                        "completeness_ratio": completeness_ratio,
                        "avg_interval_minutes": avg_interval
                    }
                )
            
            return ValidationResult(
                symbol=symbol,
                check_name="minute_data_availability",
                level=ValidationLevel.INFO,
                passed=True,
                message=f"Minute data {completeness_ratio:.1%} complete",
                details={
                    "trading_days": trading_days,
                    "completeness_ratio": completeness_ratio
                }
            )
            
        except Exception as e:
            return ValidationResult(
                symbol=symbol,
                check_name="minute_data_availability",
                level=ValidationLevel.CRITICAL,
                passed=False,
                message=f"Error checking minute data: {e}",
                details={"error": str(e)}
            )
    
    async def _check_minute_price_continuity(self, conn, symbol: str,
                                           validation_date: date) -> Optional[ValidationResult]:
        """Check minute-to-minute price continuity"""
        
        # Check last 7 days for performance
        start_date = validation_date - timedelta(days=7)
        
        query = f"""
        WITH minute_changes AS (
            SELECT 
                timestamp,
                close,
                LAG(close) OVER (ORDER BY timestamp) as prev_close,
                ABS((close - LAG(close) OVER (ORDER BY timestamp)) / 
                    NULLIF(LAG(close) OVER (ORDER BY timestamp), 0)) as abs_change
            FROM {self.env.get_table_name('minute_bars')}
            WHERE symbol = $1 
              AND timestamp >= $2
              AND close > 0
            ORDER BY timestamp
        )
        SELECT 
            COUNT(*) as total_minutes,
            COUNT(*) FILTER (WHERE abs_change > $3) as extreme_moves,
            MAX(abs_change) as max_change,
            AVG(abs_change) as avg_change
        FROM minute_changes
        WHERE prev_close IS NOT NULL AND prev_close > 0
        """
        
        try:
            row = await conn.fetchrow(query, symbol, start_date, self.max_minute_price_jump)
            
            if not row or row['total_minutes'] == 0:
                return ValidationResult(
                    symbol=symbol,
                    check_name="minute_price_continuity",
                    level=ValidationLevel.WARNING,
                    passed=False,
                    message="Insufficient minute price data",
                    details={}
                )
            
            total_minutes = row['total_minutes']
            extreme_moves = row['extreme_moves'] or 0
            max_change = row['max_change'] or 0
            
            extreme_ratio = extreme_moves / total_minutes
            
            if extreme_ratio > 0.01:  # More than 1% extreme moves
                return ValidationResult(
                    symbol=symbol,
                    check_name="minute_price_continuity",
                    level=ValidationLevel.WARNING,
                    passed=False,
                    message=f"High frequency of extreme minute price moves ({extreme_ratio:.2%})",
                    details={
                        "extreme_moves": extreme_moves,
                        "total_minutes": total_minutes,
                        "extreme_ratio": extreme_ratio,
                        "max_change": max_change
                    }
                )
            
            return ValidationResult(
                symbol=symbol,
                check_name="minute_price_continuity",
                level=ValidationLevel.INFO,
                passed=True,
                message=f"Minute price continuity good (max: {max_change:.2%})",
                details={"max_change": max_change}
            )
            
        except Exception as e:
            return ValidationResult(
                symbol=symbol,
                check_name="minute_price_continuity",
                level=ValidationLevel.WARNING,
                passed=False,
                message=f"Error checking minute price continuity: {e}",
                details={"error": str(e)}
            )
    
    async def _validate_daily_minute_consistency(self, conn, symbol: str,
                                               validation_date: date) -> List[ValidationResult]:
        """Validate consistency between daily and minute data"""
        
        results = []
        
        # Check if daily OHLC matches minute aggregation
        consistency_result = await self._check_ohlc_consistency(conn, symbol, validation_date)
        if consistency_result:
            results.append(consistency_result)
        
        return results
    
    async def _check_ohlc_consistency(self, conn, symbol: str,
                                    validation_date: date) -> Optional[ValidationResult]:
        """Check if daily OHLC matches minute bar aggregation"""
        
        # Check last 5 trading days for performance
        start_date = validation_date - timedelta(days=10)
        
        query = f"""
        WITH daily_from_minute AS (
            SELECT 
                DATE(timestamp) as date,
                (array_agg(open ORDER BY timestamp))[1] as minute_open,
                MAX(high) as minute_high,
                MIN(low) as minute_low,
                (array_agg(close ORDER BY timestamp DESC))[1] as minute_close,
                SUM(volume) as minute_volume
            FROM {self.env.get_table_name('minute_bars')}
            WHERE symbol = $1 AND timestamp >= $2
            GROUP BY DATE(timestamp)
        ),
        daily_actual AS (
            SELECT date, open, high, low, close, volume
            FROM (
                SELECT date, open, high, low, close, volume 
                FROM {self.env.get_table_name('daily_prices_polygon')} WHERE symbol = $1
                UNION
                SELECT date, open, high, low, close, volume
                FROM {self.env.get_table_name('daily_prices_tiingo')} WHERE symbol = $1
            ) combined_daily
            WHERE date >= $2
        )
        SELECT 
            COUNT(*) as comparison_days,
            AVG(ABS((d.close - m.minute_close) / NULLIF(d.close, 0))) as avg_close_diff,
            MAX(ABS((d.close - m.minute_close) / NULLIF(d.close, 0))) as max_close_diff,
            COUNT(*) FILTER (WHERE 
                ABS((d.close - m.minute_close) / NULLIF(d.close, 0)) > 0.01
            ) as significant_differences
        FROM daily_actual d
        JOIN daily_from_minute m ON d.date = m.date
        WHERE d.close > 0 AND m.minute_close > 0
        """
        
        try:
            row = await conn.fetchrow(query, symbol, start_date)
            
            if not row or row['comparison_days'] == 0:
                return ValidationResult(
                    symbol=symbol,
                    check_name="daily_minute_consistency",
                    level=ValidationLevel.INFO,
                    passed=True,
                    message="No overlapping daily/minute data for comparison",
                    details={}
                )
            
            comparison_days = row['comparison_days']
            avg_close_diff = row['avg_close_diff'] or 0
            max_close_diff = row['max_close_diff'] or 0
            significant_differences = row['significant_differences'] or 0
            
            inconsistency_ratio = significant_differences / comparison_days
            
            if inconsistency_ratio > 0.20:  # More than 20% inconsistent
                return ValidationResult(
                    symbol=symbol,
                    check_name="daily_minute_consistency",
                    level=ValidationLevel.WARNING,
                    passed=False,
                    message=f"Daily/minute data inconsistent ({inconsistency_ratio:.1%} of days)",
                    details={
                        "comparison_days": comparison_days,
                        "significant_differences": significant_differences,
                        "inconsistency_ratio": inconsistency_ratio,
                        "max_close_diff": max_close_diff,
                        "avg_close_diff": avg_close_diff
                    }
                )
            
            return ValidationResult(
                symbol=symbol,
                check_name="daily_minute_consistency",
                level=ValidationLevel.INFO,
                passed=True,
                message=f"Daily/minute data consistent (avg diff: {avg_close_diff:.2%})",
                details={
                    "comparison_days": comparison_days,
                    "avg_close_diff": avg_close_diff
                }
            )
            
        except Exception as e:
            return ValidationResult(
                symbol=symbol,
                check_name="daily_minute_consistency",
                level=ValidationLevel.WARNING,
                passed=False,
                message=f"Error checking daily/minute consistency: {e}",
                details={"error": str(e)}
            )
    
    def generate_validation_report(self, results: List[ValidationResult]) -> str:
        """Generate a comprehensive validation report"""
        
        # Aggregate results by level
        critical_count = len([r for r in results if r.level == ValidationLevel.CRITICAL and not r.passed])
        warning_count = len([r for r in results if r.level == ValidationLevel.WARNING and not r.passed])
        passed_count = len([r for r in results if r.passed])
        
        # Group by symbol
        symbol_results = {}
        for result in results:
            if result.symbol not in symbol_results:
                symbol_results[result.symbol] = []
            symbol_results[result.symbol].append(result)
        
        report_lines = [
            "# Data Quality Validation Report",
            f"Generated: {datetime.now().isoformat()}",
            "",
            "## Summary",
            f"- Total validations: {len(results)}",
            f"- Passed: {passed_count}",
            f"- Warnings: {warning_count}",
            f"- Critical issues: {critical_count}",
            f"- Symbols validated: {len(symbol_results)}",
            "",
            "## Issues by Severity",
            ""
        ]
        
        # Critical issues
        if critical_count > 0:
            report_lines.extend([
                "### Critical Issues",
                ""
            ])
            for result in results:
                if result.level == ValidationLevel.CRITICAL and not result.passed:
                    report_lines.append(f"- **{result.symbol}**: {result.check_name} - {result.message}")
            report_lines.append("")
        
        # Warnings
        if warning_count > 0:
            report_lines.extend([
                "### Warnings",
                ""
            ])
            for result in results:
                if result.level == ValidationLevel.WARNING and not result.passed:
                    report_lines.append(f"- **{result.symbol}**: {result.check_name} - {result.message}")
            report_lines.append("")
        
        # Symbol summary
        report_lines.extend([
            "## Symbol Summary",
            "| Symbol | Critical | Warnings | Passed | Status |",
            "|--------|----------|----------|--------|--------|"
        ])
        
        for symbol, symbol_validations in sorted(symbol_results.items()):
            symbol_critical = len([r for r in symbol_validations if r.level == ValidationLevel.CRITICAL and not r.passed])
            symbol_warnings = len([r for r in symbol_validations if r.level == ValidationLevel.WARNING and not r.passed])  
            symbol_passed = len([r for r in symbol_validations if r.passed])
            
            if symbol_critical > 0:
                status = "❌ FAIL"
            elif symbol_warnings > 0:
                status = "⚠️ WARN"
            else:
                status = "✅ PASS"
            
            report_lines.append(f"| {symbol} | {symbol_critical} | {symbol_warnings} | {symbol_passed} | {status} |")
        
        return "\n".join(report_lines)


async def main():
    """Main function to run data quality validation"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate universe data quality")
    parser.add_argument("--universe-id", type=int, required=True,
                       help="Universe ID to validate")
    parser.add_argument("--report-file", help="Output file for validation report")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run validation
    validator = DataQualityValidator()
    results = await validator.validate_universe_quality(args.universe_id)
    
    # Generate report
    report = validator.generate_validation_report(results)
    
    if args.report_file:
        with open(args.report_file, 'w') as f:
            f.write(report)
        print(f"Validation report saved to {args.report_file}")
    else:
        print(report)


if __name__ == "__main__":
    asyncio.run(main())