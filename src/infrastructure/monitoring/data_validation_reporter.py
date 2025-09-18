"""
Data Validation Reporter for ATS Trading System

Generates daily reports on data quality, missing data, and validation issues.
Posts reports to Slack #ats-dev channel for monitoring and alerts.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import timedelta, date
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
import logging
import requests
import json
from pathlib import Path
import exchange_calendars as xcals
from concurrent.futures import ThreadPoolExecutor
import os
import gin

from src.core.config.environment import env

logger = logging.getLogger(__name__)

@gin.configurable
class DataValidationConfig:
    def __init__(self,
                 # Threading settings
                 max_workers: int = 4,

                 # Market hours (24-hour format)
                 market_open_hour: int = 9,
                 market_open_minute: int = 30,
                 market_close_hour: int = 16,
                 market_close_minute: int = 0,

                 # Expected data settings
                 expected_bars_per_day: int = 390,  # 6.5 hours * 60 minutes

                 # Default values for metrics
                 default_expected_bars: int = 0,
                 default_actual_bars: int = 0,
                 default_missing_bars: int = 0,
                 default_quality_score: float = 0.0):

        self.max_workers = max_workers
        self.market_open_hour = market_open_hour
        self.market_open_minute = market_open_minute
        self.market_close_hour = market_close_hour
        self.market_close_minute = market_close_minute
        self.expected_bars_per_day = expected_bars_per_day
        self.default_expected_bars = default_expected_bars
        self.default_actual_bars = default_actual_bars
        self.default_missing_bars = default_missing_bars
        self.default_quality_score = default_quality_score


@dataclass
class ValidationIssue:
    """Represents a data validation issue."""
    symbol: str
    date: date
    issue_type: str
    severity: str  # 'critical', 'warning', 'info'
    description: str
    expected_bars: int = None
    actual_bars: int = None
    missing_bars: int = None
    quality_score: float = None

    def __post_init__(self):
        """Set defaults from config if None"""
        config = DataValidationConfig()  # This will get defaults or gin-configured values
        if self.expected_bars is None:
            self.expected_bars = config.default_expected_bars
        if self.actual_bars is None:
            self.actual_bars = config.default_actual_bars
        if self.missing_bars is None:
            self.missing_bars = config.default_missing_bars
        if self.quality_score is None:
            self.quality_score = config.default_quality_score


@dataclass
class StockInfo:
    """Stock listing information."""
    symbol: str
    name: str
    exchange: str
    listing_date: Optional[date] = None
    delisting_date: Optional[date] = None
    sector: Optional[str] = None
    is_active: bool = True


@dataclass
class ValidationReport:
    """Daily validation report."""
    report_date: date
    total_issues: int
    critical_issues: int
    warning_issues: int
    issues_by_date: Dict[date, int]
    issues_by_stock: Dict[str, int]
    stock_coverage: Dict[str, float]  # Percentage of expected data available
    data_quality_scores: Dict[str, float]
    detailed_issues: List[ValidationIssue]
    summary_stats: Dict[str, Any]


class DataValidationReporter:
    """
    Data validation reporting system for financial market data.

    Features:
    - Validates expected trading days vs actual data availability
    - Detects missing data gaps and quality issues
    - Generates comprehensive daily reports
    - Posts alerts to Slack channels
    - Tracks data coverage and quality metrics
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        slack_webhook_url: Optional[str] = None,
        slack_channel: str = "#ats-dev",
        config: DataValidationConfig = None
    ):
        self.pool = pool
        self.slack_webhook_url = slack_webhook_url or os.getenv("SLACK_WEBHOOK_URL")
        self.slack_channel = slack_channel
        self.config = config or DataValidationConfig()
        self.executor = ThreadPoolExecutor(max_workers=self.config.max_workers)

        # NYSE trading calendar for accurate trading days
        self.trading_calendar = xcals.get_calendar('XNYS')  # NYSE

        # Market hours and settings from configuration
        self.market_open_hour = self.config.market_open_hour
        self.market_open_minute = self.config.market_open_minute
        self.market_close_hour = self.config.market_close_hour
        self.market_close_minute = self.config.market_close_minute
        self.expected_bars_per_day = self.config.expected_bars_per_day

    def is_trading_day(self, check_date: date) -> bool:
        """Check if a date is a valid trading day using NYSE calendar."""
        try:
            # Convert date to pandas Timestamp for exchange-calendars
            pd_date = pd.Timestamp(check_date)
            # Check if it's a valid trading session
            return self.trading_calendar.is_session(pd_date)
        except Exception as e:
            logger.warning(f"Error checking trading day for {check_date}: {e}")
            # Fallback to simple weekend check
            return check_date.weekday() < 5

    def get_expected_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """Get list of expected trading days in date range using NYSE calendar."""
        try:
            # Convert to pandas Timestamps
            start_ts = pd.Timestamp(start_date)
            end_ts = pd.Timestamp(end_date)

            # Get trading sessions from NYSE calendar
            sessions = self.trading_calendar.sessions_in_range(start_ts, end_ts)

            # Convert back to dates
            return [session.date() for session in sessions]
        except Exception as e:
            logger.warning(f"Error getting trading days: {e}, falling back to simple method")
            # Fallback to simple method
            trading_days = []
            current_date = start_date

            while current_date <= end_date:
                if current_date.weekday() < 5:  # Simple weekday check
                    trading_days.append(current_date)
                current_date += timedelta(days=1)

            return trading_days

    async def get_stock_info(self, symbol: str) -> StockInfo:
        """Get stock listing information from database."""
        query = """
        SELECT symbol, name, exchange, listing_date, delisting_date, sector, is_active
        FROM {table_name}
        WHERE symbol = $1
        """.format(table_name=env.get_table_name('instruments'))

        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(query, symbol)

                if row:
                    return StockInfo(
                        symbol=row['symbol'],
                        name=row['name'] or symbol,
                        exchange=row['exchange'] or 'UNKNOWN',
                        listing_date=row['listing_date'],
                        delisting_date=row['delisting_date'],
                        sector=row['sector'],
                        is_active=row['is_active'] if row['is_active'] is not None else True
                    )
                else:
                    # Default stock info if not found in database
                    return StockInfo(
                        symbol=symbol,
                        name=symbol,
                        exchange='UNKNOWN'
                    )
        except Exception as e:
            logger.warning(f"Error getting stock info for {symbol}: {e}")
            return StockInfo(symbol=symbol, name=symbol, exchange='UNKNOWN')

    async def get_data_coverage(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Dict[date, int]:
        """Get actual data coverage for a symbol by date."""
        table_name = env.get_table_name('minute_bars')

        query = """
        SELECT DATE(timestamp) as trade_date, COUNT(*) as bar_count
        FROM {table_name}
        WHERE symbol = $1
          AND DATE(timestamp) >= $2
          AND DATE(timestamp) <= $3
        GROUP BY DATE(timestamp)
        ORDER BY trade_date
        """.format(table_name=table_name)

        coverage = {}

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, start_date, end_date)

                for row in rows:
                    coverage[row['trade_date']] = row['bar_count']

        except Exception as e:
            logger.error(f"Error getting data coverage for {symbol}: {e}")

        return coverage

    async def get_data_quality_scores(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Dict[date, float]:
        """Get data quality scores by date for a symbol."""
        table_name = env.get_table_name('minute_bars')

        query = """
        SELECT DATE(timestamp) as trade_date, AVG(quality_score) as avg_quality
        FROM {table_name}
        WHERE symbol = $1
          AND DATE(timestamp) >= $2
          AND DATE(timestamp) <= $3
          AND quality_score IS NOT NULL
        GROUP BY DATE(timestamp)
        ORDER BY trade_date
        """.format(table_name=table_name)

        quality_scores = {}

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, start_date, end_date)

                for row in rows:
                    quality_scores[row['trade_date']] = float(row['avg_quality'])

        except Exception as e:
            logger.error(f"Error getting quality scores for {symbol}: {e}")

        return quality_scores

    async def get_price_anomalies(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        max_change_percent: float = 10.0
    ) -> List[ValidationIssue]:
        """Detect price anomalies (unrealistic price movements)."""
        table_name = env.get_table_name('minute_bars')

        query = """
        WITH price_changes AS (
            SELECT
                symbol,
                timestamp,
                close,
                LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_close,
                ABS((close - LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp)) /
                    LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp)) * 100 as change_percent
            FROM {table_name}
            WHERE symbol = $1
              AND DATE(timestamp) >= $2
              AND DATE(timestamp) <= $3
              AND close > 0
            ORDER BY timestamp
        )
        SELECT symbol, timestamp, close, prev_close, change_percent
        FROM price_changes
        WHERE change_percent > $4
          AND prev_close IS NOT NULL
        """.format(table_name=table_name)

        issues = []

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, start_date, end_date, max_change_percent)

                for row in rows:
                    change_pct = float(row['change_percent'])
                    severity = "critical" if change_pct > 20.0 else "warning"

                    issues.append(ValidationIssue(
                        symbol=symbol,
                        date=row['timestamp'].date(),
                        issue_type="price_anomaly",
                        severity=severity,
                        description=f"Extreme price change for {symbol} at {row['timestamp']}: "
                                   f"{change_pct:.2f}% (${row['prev_close']:.2f} → ${row['close']:.2f})",
                        quality_score=max(0.0, 1.0 - (change_pct / 100.0))
                    ))

        except Exception as e:
            logger.error(f"Error detecting price anomalies for {symbol}: {e}")

        return issues

    async def get_ohlc_validation_issues(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> List[ValidationIssue]:
        """Validate OHLC relationships (high >= low, close between high/low, etc.)."""
        table_name = env.get_table_name('minute_bars')

        query = """
        SELECT symbol, timestamp, open, high, low, close
        FROM {table_name}
        WHERE symbol = $1
          AND DATE(timestamp) >= $2
          AND DATE(timestamp) <= $3
          AND (
            high < low OR
            close > high OR
            close < low OR
            open > high OR
            open < low
          )
        """.format(table_name=table_name)

        issues = []

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, start_date, end_date)

                for row in rows:
                    o, h, l, c = row['open'], row['high'], row['low'], row['close']
                    violations = []

                    if h < l:
                        violations.append(f"high ({h}) < low ({l})")
                    if c > h:
                        violations.append(f"close ({c}) > high ({h})")
                    if c < l:
                        violations.append(f"close ({c}) < low ({l})")
                    if o > h:
                        violations.append(f"open ({o}) > high ({h})")
                    if o < l:
                        violations.append(f"open ({o}) < low ({l})")

                    issues.append(ValidationIssue(
                        symbol=symbol,
                        date=row['timestamp'].date(),
                        issue_type="ohlc_violation",
                        severity="critical",
                        description=f"OHLC violation for {symbol} at {row['timestamp']}: " + "; ".join(violations),
                        quality_score=0.0
                    ))

        except Exception as e:
            logger.error(f"Error validating OHLC for {symbol}: {e}")

        return issues

    async def validate_symbol_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> List[ValidationIssue]:
        """Validate data for a single symbol."""
        issues = []

        # Get stock information
        stock_info = await self.get_stock_info(symbol)

        # Adjust date range based on listing/delisting dates
        effective_start = start_date
        effective_end = end_date

        if stock_info.listing_date:
            effective_start = max(start_date, stock_info.listing_date)

        if stock_info.delisting_date:
            effective_end = min(end_date, stock_info.delisting_date)

        # Skip if no valid date range
        if effective_start > effective_end:
            return issues

        # Get expected trading days
        expected_trading_days = self.get_expected_trading_days(effective_start, effective_end)

        # Get actual data coverage
        data_coverage = await self.get_data_coverage(symbol, effective_start, effective_end)

        # Get quality scores
        quality_scores = await self.get_data_quality_scores(symbol, effective_start, effective_end)

        # Validate each expected trading day
        for trading_day in expected_trading_days:
            actual_bars = data_coverage.get(trading_day, 0)
            expected_bars = self.expected_bars_per_day
            missing_bars = max(0, expected_bars - actual_bars)

            # Check for missing data
            if actual_bars == 0:
                issues.append(ValidationIssue(
                    symbol=symbol,
                    date=trading_day,
                    issue_type="missing_data",
                    severity="critical",
                    description=f"No data available for {symbol} on {trading_day}",
                    expected_bars=expected_bars,
                    actual_bars=actual_bars,
                    missing_bars=missing_bars
                ))

            # Check for incomplete data
            elif missing_bars > expected_bars * 0.1:  # More than 10% missing
                severity = "critical" if missing_bars > expected_bars * 0.5 else "warning"
                issues.append(ValidationIssue(
                    symbol=symbol,
                    date=trading_day,
                    issue_type="incomplete_data",
                    severity=severity,
                    description=f"Incomplete data for {symbol} on {trading_day}: {actual_bars}/{expected_bars} bars",
                    expected_bars=expected_bars,
                    actual_bars=actual_bars,
                    missing_bars=missing_bars
                ))

            # Check quality scores
            quality_score = quality_scores.get(trading_day, 0.0)
            if quality_score < 0.7:  # Quality threshold
                severity = "critical" if quality_score < 0.5 else "warning"
                issues.append(ValidationIssue(
                    symbol=symbol,
                    date=trading_day,
                    issue_type="low_quality",
                    severity=severity,
                    description=f"Low quality data for {symbol} on {trading_day}: {quality_score:.2f}",
                    expected_bars=expected_bars,
                    actual_bars=actual_bars,
                    quality_score=quality_score
                ))

        # Price anomaly detection
        price_anomalies = await self.get_price_anomalies(symbol, effective_start, effective_end)
        issues.extend(price_anomalies)

        # OHLC validation
        ohlc_issues = await self.get_ohlc_validation_issues(symbol, effective_start, effective_end)
        issues.extend(ohlc_issues)

        return issues

    async def generate_daily_report(
        self,
        report_date: date = None,
        symbols: List[str] = None,
        lookback_days: int = 7
    ) -> ValidationReport:
        """Generate comprehensive daily validation report."""

        if report_date is None:
            report_date = date.today()

        # Date range for validation
        start_date = report_date - timedelta(days=lookback_days)
        end_date = report_date

        # Get symbols to validate
        if symbols is None:
            symbols = await self._get_active_symbols()

        logger.info(f"Generating validation report for {len(symbols)} symbols from {start_date} to {end_date}")

        # Validate all symbols
        all_issues = []
        for symbol in symbols:
            try:
                symbol_issues = await self.validate_symbol_data(symbol, start_date, end_date)
                all_issues.extend(symbol_issues)
            except Exception as e:
                logger.error(f"Error validating {symbol}: {e}")

        # Aggregate statistics
        issues_by_date = {}
        issues_by_stock = {}
        stock_coverage = {}
        data_quality_scores = {}

        critical_issues = sum(1 for issue in all_issues if issue.severity == 'critical')
        warning_issues = sum(1 for issue in all_issues if issue.severity == 'warning')

        # Group issues by date
        for issue in all_issues:
            issues_by_date[issue.date] = issues_by_date.get(issue.date, 0) + 1
            issues_by_stock[issue.symbol] = issues_by_stock.get(issue.symbol, 0) + 1

        # Calculate coverage and quality for each symbol
        for symbol in symbols:
            try:
                coverage = await self.get_data_coverage(symbol, start_date, end_date)
                quality = await self.get_data_quality_scores(symbol, start_date, end_date)

                expected_days = len(self.get_expected_trading_days(start_date, end_date))
                actual_days = len(coverage)
                stock_coverage[symbol] = (actual_days / expected_days) * 100 if expected_days > 0 else 0

                if quality:
                    data_quality_scores[symbol] = sum(quality.values()) / len(quality)
                else:
                    data_quality_scores[symbol] = 0.0

            except Exception as e:
                logger.error(f"Error calculating metrics for {symbol}: {e}")
                stock_coverage[symbol] = 0.0
                data_quality_scores[symbol] = 0.0

        # Summary statistics
        summary_stats = {
            'total_symbols': len(symbols),
            'symbols_with_issues': len(issues_by_stock),
            'avg_coverage': sum(stock_coverage.values()) / len(stock_coverage) if stock_coverage else 0,
            'avg_quality': sum(data_quality_scores.values()) / len(data_quality_scores) if data_quality_scores else 0,
            'trading_days_analyzed': len(self.get_expected_trading_days(start_date, end_date)),
            'dates_with_issues': len(issues_by_date)
        }

        return ValidationReport(
            report_date=report_date,
            total_issues=len(all_issues),
            critical_issues=critical_issues,
            warning_issues=warning_issues,
            issues_by_date=issues_by_date,
            issues_by_stock=issues_by_stock,
            stock_coverage=stock_coverage,
            data_quality_scores=data_quality_scores,
            detailed_issues=all_issues,
            summary_stats=summary_stats
        )

    async def _get_active_symbols(self) -> List[str]:
        """Get list of active symbols from database."""
        query = """
        SELECT DISTINCT symbol
        FROM {table_name}
        WHERE is_active = true OR is_active IS NULL
        ORDER BY symbol
        """.format(table_name=env.get_table_name('instruments'))

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query)
                return [row['symbol'] for row in rows]
        except Exception as e:
            logger.error(f"Error getting active symbols: {e}")
            # Fallback to common symbols
            return ['AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX', 'ADBE']

    def format_slack_report(self, report: ValidationReport) -> Dict[str, Any]:
        """Format validation report for Slack."""

        # Status emoji based on issues
        if report.critical_issues > 0:
            status_emoji = "🔴"
            status_text = "CRITICAL ISSUES DETECTED"
        elif report.warning_issues > 0:
            status_emoji = "🟡"
            status_text = "Warnings Found"
        else:
            status_emoji = "🟢"
            status_text = "All Systems Operational"

        # Create main message
        main_text = f"{status_emoji} *ATS Data Validation Report - {report.report_date}*\n{status_text}"

        # Summary block
        summary_block = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Summary:*\n"
                       f"• Total Issues: {report.total_issues}\n"
                       f"• Critical: {report.critical_issues} | Warnings: {report.warning_issues}\n"
                       f"• Symbols Analyzed: {report.summary_stats['total_symbols']}\n"
                       f"• Average Coverage: {report.summary_stats['avg_coverage']:.1f}%\n"
                       f"• Average Quality: {report.summary_stats['avg_quality']:.3f}"
            }
        }

        blocks = [summary_block]

        # Top issues by stock
        if report.issues_by_stock:
            top_stocks = sorted(report.issues_by_stock.items(), key=lambda x: x[1], reverse=True)[:5]
            stock_text = "*Top Issues by Stock:*\n"
            for symbol, count in top_stocks:
                coverage = report.stock_coverage.get(symbol, 0)
                quality = report.data_quality_scores.get(symbol, 0)
                stock_text += f"• {symbol}: {count} issues (Coverage: {coverage:.1f}%, Quality: {quality:.3f})\n"

            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": stock_text}
            })

        # Issues by date
        if report.issues_by_date:
            date_issues = sorted(report.issues_by_date.items(), key=lambda x: x[1], reverse=True)[:5]
            date_text = "*Issues by Date:*\n"
            for issue_date, count in date_issues:
                date_text += f"• {issue_date}: {count} issues\n"

            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": date_text}
            })

        # Critical issues detail
        if report.critical_issues > 0:
            critical_issues = [issue for issue in report.detailed_issues if issue.severity == 'critical'][:5]
            critical_text = "*Critical Issues (Top 5):*\n"
            for issue in critical_issues:
                critical_text += f"• {issue.symbol} ({issue.date}): {issue.description}\n"

            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": critical_text}
            })

        return {
            "text": main_text,
            "blocks": blocks,
            "channel": self.slack_channel
        }

    async def post_to_slack(self, report: ValidationReport) -> bool:
        """Post validation report to Slack channel."""
        if not self.slack_webhook_url:
            logger.warning("No Slack webhook URL configured - skipping Slack notification")
            return False

        try:
            slack_payload = self.format_slack_report(report)

            response = requests.post(
                self.slack_webhook_url,
                json=slack_payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )

            if response.status_code == 200:
                logger.info(f"Successfully posted validation report to Slack channel {self.slack_channel}")
                return True
            else:
                logger.error(f"Failed to post to Slack: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error posting to Slack: {e}")
            return False

    def save_report(self, report: ValidationReport, output_dir: str = "/tmp") -> str:
        """Save validation report to file."""
        output_path = Path(output_dir) / f"data_validation_report_{report.report_date}.json"

        # Convert report to JSON-serializable format
        report_data = {
            'report_date': report.report_date.isoformat(),
            'total_issues': report.total_issues,
            'critical_issues': report.critical_issues,
            'warning_issues': report.warning_issues,
            'issues_by_date': {date.isoformat(): count for date, count in report.issues_by_date.items()},
            'issues_by_stock': report.issues_by_stock,
            'stock_coverage': report.stock_coverage,
            'data_quality_scores': report.data_quality_scores,
            'summary_stats': report.summary_stats,
            'detailed_issues': [
                {
                    'symbol': issue.symbol,
                    'date': issue.date.isoformat(),
                    'issue_type': issue.issue_type,
                    'severity': issue.severity,
                    'description': issue.description,
                    'expected_bars': issue.expected_bars,
                    'actual_bars': issue.actual_bars,
                    'missing_bars': issue.missing_bars,
                    'quality_score': issue.quality_score
                }
                for issue in report.detailed_issues
            ]
        }

        with open(output_path, 'w') as f:
            json.dump(report_data, f, indent=2)

        logger.info(f"Validation report saved to {output_path}")
        return str(output_path)

    async def close(self):
        """Clean up resources."""
        self.executor.shutdown(wait=True)


# Convenience functions
async def run_daily_validation_report(
    db_url: str,
    symbols: List[str] = None,
    slack_webhook_url: str = None,
    post_to_slack: bool = True,
    save_to_file: bool = True
) -> ValidationReport:
    """Run daily validation report and post to Slack."""

    pool = await asyncpg.create_pool(db_url, min_size=2, max_size=10)

    try:
        reporter = DataValidationReporter(
            pool=pool,
            slack_webhook_url=slack_webhook_url
        )

        # Generate report
        report = await reporter.generate_daily_report(symbols=symbols)

        # Post to Slack
        if post_to_slack:
            await reporter.post_to_slack(report)

        # Save to file
        if save_to_file:
            reporter.save_report(report)

        await reporter.close()
        return report

    finally:
        await pool.close()


if __name__ == "__main__":
    import sys

    # Command line interface
    if len(sys.argv) > 1:
        symbols = sys.argv[1].split(',') if sys.argv[1] != 'all' else None
    else:
        symbols = None

    # Database URL from environment
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/dev_db")

    # Run report
    asyncio.run(run_daily_validation_report(
        db_url=db_url,
        symbols=symbols,
        post_to_slack=True,
        save_to_file=True
    ))