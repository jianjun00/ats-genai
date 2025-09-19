#!/usr/bin/env python3
"""
Daily Prices Data Validation System.
Comprehensive validation for missing, bad, and anomalous daily price data.
"""

import asyncpg
import logging
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import statistics
from decimal import Decimal

from core.platform.config.environment import Environment

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Validation issue severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ValidationRule(Enum):
    """Types of validation rules."""
    MISSING_DATA = "missing_data"
    NULL_VALUES = "null_values"
    NEGATIVE_PRICES = "negative_prices"
    ZERO_PRICES = "zero_prices"
    EXTREME_PRICES = "extreme_prices"
    OHLC_CONSISTENCY = "ohlc_consistency"
    VOLUME_ANOMALY = "volume_anomaly"
    PRICE_GAPS = "price_gaps"
    STALE_DATA = "stale_data"
    CROSS_VENDOR_MISMATCH = "cross_vendor_mismatch"
    TRADING_HALT = "trading_halt"
    SPLIT_ADJUSTMENT = "split_adjustment"


@dataclass
class ValidationIssue:
    """Represents a data validation issue."""
    rule: ValidationRule
    severity: ValidationSeverity
    instrument_symbol: str
    instrument_id: int
    date: date
    vendor: str
    message: str
    details: Dict[str, Any]
    detected_at: datetime
    resolved: bool = False
    resolution_action: Optional[str] = None


@dataclass
class PriceRecord:
    """Standardized price record for validation."""
    instrument_id: int
    symbol: str
    date: date
    open: Optional[Decimal]
    high: Optional[Decimal]
    low: Optional[Decimal]
    close: Optional[Decimal]
    volume: Optional[int]
    vendor: str
    adjclose: Optional[Decimal] = None  # For Tiingo


class DailyPricesValidator:
    """Comprehensive validator for daily prices data."""

    def __init__(self, connection_pool: asyncpg.Pool, env: Environment):
        self.pool = connection_pool
        self.env = env
        self.issues_table = env.get_table_name("price_validation_issues")

        # Validation thresholds
        self.thresholds = {
            "max_price_change_pct": 50.0,  # 50% daily change threshold
            "min_volume": 100,  # Minimum expected volume
            "max_volume_multiplier": 50.0,  # Max volume vs average
            "min_price": 0.01,  # Minimum valid price
            "max_price": 10000.0,  # Maximum reasonable price
            "stale_data_days": 5,  # Days before data considered stale
            "cross_vendor_tolerance_pct": 5.0,  # % tolerance between vendors
            "ohlc_tolerance_pct": 0.1  # OHLC relationship tolerance
        }

    async def initialize(self):
        """Initialize validation system and create required tables."""
        await self._create_validation_tables()
        logger.info("Daily prices validator initialized")

    async def _create_validation_tables(self):
        """Create tables for storing validation issues."""
        async with self.pool.acquire() as conn:
            # Validation issues table
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.issues_table} (
                    id SERIAL PRIMARY KEY,
                    rule VARCHAR(100) NOT NULL,
                    severity VARCHAR(20) NOT NULL,
                    instrument_symbol VARCHAR(50) NOT NULL,
                    instrument_id INTEGER NOT NULL,
                    date DATE NOT NULL,
                    vendor VARCHAR(50) NOT NULL,
                    message TEXT NOT NULL,
                    details JSONB DEFAULT '{{}}',
                    detected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    resolved BOOLEAN DEFAULT FALSE,
                    resolution_action TEXT,
                    resolved_at TIMESTAMP WITH TIME ZONE,

                    INDEX (instrument_symbol, date),
                    INDEX (rule, severity),
                    INDEX (detected_at DESC),
                    INDEX (resolved)
                )
            """)

            # Summary stats table for performance
            summary_table = self.env.get_table_name("price_validation_summary")
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {summary_table} (
                    date DATE PRIMARY KEY,
                    total_instruments INTEGER DEFAULT 0,
                    missing_data_count INTEGER DEFAULT 0,
                    error_count INTEGER DEFAULT 0,
                    warning_count INTEGER DEFAULT 0,
                    critical_count INTEGER DEFAULT 0,
                    cross_vendor_mismatches INTEGER DEFAULT 0,
                    data_quality_score DECIMAL(5,2),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)

    async def validate_daily_price_polygon(self, validation_date: date,
                                  vendors: List[str] = None) -> Dict[str, Any]:
        """
        Run comprehensive validation for daily prices on a specific date.

        Args:
            validation_date: Date to validate
            vendors: List of vendors to validate (None = all)

        Returns:
            Validation summary with issues found
        """
        if vendors is None:
            vendors = ["polygon", "tiingo"]

        logger.info(f"Starting daily prices validation for {validation_date}")

        validation_results = {
            "date": validation_date,
            "vendors": vendors,
            "total_issues": 0,
            "issues_by_severity": {},
            "issues_by_rule": {},
            "instruments_validated": 0,
            "data_quality_score": 0.0,
            "validation_time": datetime.now()
        }

        all_issues = []

        # Get all price records for the date
        price_records = await self._get_price_records(validation_date, vendors)
        validation_results["instruments_validated"] = len(set(r.instrument_id for r in price_records))

        # Run validation rules
        validation_rules = [
            self._validate_missing_data,
            self._validate_null_values,
            self._validate_negative_prices,
            self._validate_ohlc_consistency,
            self._validate_extreme_prices,
            self._validate_volume_anomalies,
            self._validate_price_gaps,
            self._validate_stale_data,
            self._validate_cross_vendor_consistency
        ]

        for rule_func in validation_rules:
            try:
                issues = await rule_func(validation_date, price_records)
                all_issues.extend(issues)
                logger.debug(f"Rule {rule_func.__name__} found {len(issues)} issues")
            except Exception as e:
                logger.error(f"Error in validation rule {rule_func.__name__}: {e}")

        # Store issues in database
        await self._store_validation_issues(all_issues)

        # Calculate summary statistics
        validation_results["total_issues"] = len(all_issues)
        validation_results["issues_by_severity"] = self._group_by_severity(all_issues)
        validation_results["issues_by_rule"] = self._group_by_rule(all_issues)
        validation_results["data_quality_score"] = self._calculate_quality_score(
            validation_results["instruments_validated"], all_issues
        )

        # Store summary
        await self._store_validation_summary(validation_date, validation_results)

        logger.info(f"Validation completed: {len(all_issues)} issues found, "
                   f"quality score: {validation_results['data_quality_score']:.2f}")

        return validation_results

    async def _get_price_records(self, validation_date: date,
                               vendors: List[str]) -> List[PriceRecord]:
        """Get all price records for validation date."""
        all_records = []

        for vendor in vendors:
            table_name = self.env.get_table_name(f"daily_price_polygon_{vendor}")
            instruments_table = self.env.get_table_name("instruments")

            async with self.pool.acquire() as conn:
                if vendor == "tiingo":
                    query = f"""
                        SELECT
                            p.instrument_id, i.symbol, p.date,
                            p.open, p.high, p.low, p.close,
                            p.volume, p.adjclose, '{vendor}' as vendor
                        FROM {table_name} p
                        JOIN {instruments_table} i ON p.instrument_id = i.id
                        WHERE p.date = $1 AND i.is_active = true
                    """
                else:  # polygon
                    query = f"""
                        SELECT
                            p.instrument_id, i.symbol, p.date,
                            p.open, p.high, p.low, p.close,
                            p.volume, null as adjclose, '{vendor}' as vendor
                        FROM {table_name} p
                        JOIN {instruments_table} i ON p.instrument_id = i.id
                        WHERE p.date = $1 AND i.is_active = true
                    """

                rows = await conn.fetch(query, validation_date)

                for row in rows:
                    record = PriceRecord(
                        instrument_id=row["instrument_id"],
                        symbol=row["symbol"],
                        date=row["date"],
                        open=row["open"],
                        high=row["high"],
                        low=row["low"],
                        close=row["close"],
                        volume=row["volume"],
                        vendor=row["vendor"],
                        adjclose=row.get("adjclose")
                    )
                    all_records.append(record)

        return all_records

    async def _validate_missing_data(self, validation_date: date,
                                   price_records: List[PriceRecord]) -> List[ValidationIssue]:
        """Validate for missing data (instruments with no price data)."""
        issues = []

        # Get all active instruments
        instruments_table = self.env.get_table_name("instruments")
        async with self.pool.acquire() as conn:
            all_instruments = await conn.fetch(f"""
                SELECT id, symbol FROM {instruments_table}
                WHERE is_active = true
            """)

        # Group records by vendor
        records_by_vendor = {}
        for record in price_records:
            if record.vendor not in records_by_vendor:
                records_by_vendor[record.vendor] = set()
            records_by_vendor[record.vendor].add(record.instrument_id)

        # Check for missing instruments per vendor
        for vendor in ["polygon", "tiingo"]:
            vendor_instruments = records_by_vendor.get(vendor, set())

            for instrument in all_instruments:
                if instrument["id"] not in vendor_instruments:
                    # Check if it's a trading day (skip weekends)
                    if validation_date.weekday() < 5:  # Monday=0, Friday=4
                        issues.append(ValidationIssue(
                            rule=ValidationRule.MISSING_DATA,
                            severity=ValidationSeverity.WARNING,
                            instrument_symbol=instrument["symbol"],
                            instrument_id=instrument["id"],
                            date=validation_date,
                            vendor=vendor,
                            message=f"Missing price data for {instrument['symbol']} on {validation_date}",
                            details={"expected_trading_day": True},
                            detected_at=datetime.now()
                        ))

        return issues

    async def _validate_null_values(self, validation_date: date,
                                  price_records: List[PriceRecord]) -> List[ValidationIssue]:
        """Validate for null/missing OHLC values."""
        issues = []

        for record in price_records:
            null_fields = []

            if record.open is None:
                null_fields.append("open")
            if record.high is None:
                null_fields.append("high")
            if record.low is None:
                null_fields.append("low")
            if record.close is None:
                null_fields.append("close")

            if null_fields:
                issues.append(ValidationIssue(
                    rule=ValidationRule.NULL_VALUES,
                    severity=ValidationSeverity.ERROR,
                    instrument_symbol=record.symbol,
                    instrument_id=record.instrument_id,
                    date=record.date,
                    vendor=record.vendor,
                    message=f"Null values in fields: {', '.join(null_fields)}",
                    details={"null_fields": null_fields},
                    detected_at=datetime.now()
                ))

        return issues

    async def _validate_negative_prices(self, validation_date: date,
                                      price_records: List[PriceRecord]) -> List[ValidationIssue]:
        """Validate for negative prices."""
        issues = []

        for record in price_records:
            negative_fields = []

            for field_name, value in [
                ("open", record.open), ("high", record.high),
                ("low", record.low), ("close", record.close)
            ]:
                if value is not None and value < 0:
                    negative_fields.append((field_name, float(value)))

            if negative_fields:
                issues.append(ValidationIssue(
                    rule=ValidationRule.NEGATIVE_PRICES,
                    severity=ValidationSeverity.CRITICAL,
                    instrument_symbol=record.symbol,
                    instrument_id=record.instrument_id,
                    date=record.date,
                    vendor=record.vendor,
                    message=f"Negative prices detected: {negative_fields}",
                    details={"negative_prices": negative_fields},
                    detected_at=datetime.now()
                ))

        return issues

    async def _validate_ohlc_consistency(self, validation_date: date,
                                       price_records: List[PriceRecord]) -> List[ValidationIssue]:
        """Validate OHLC relationships (High >= Low, etc.)."""
        issues = []

        for record in price_records:
            # Skip if any OHLC value is missing
            if any(v is None for v in [record.open, record.high, record.low, record.close]):
                continue

            violations = []

            # High should be >= all other prices
            if record.high < record.open:
                violations.append(f"High ({record.high}) < Open ({record.open})")
            if record.high < record.low:
                violations.append(f"High ({record.high}) < Low ({record.low})")
            if record.high < record.close:
                violations.append(f"High ({record.high}) < Close ({record.close})")

            # Low should be <= all other prices
            if record.low > record.open:
                violations.append(f"Low ({record.low}) > Open ({record.open})")
            if record.low > record.close:
                violations.append(f"Low ({record.low}) > Close ({record.close})")

            if violations:
                issues.append(ValidationIssue(
                    rule=ValidationRule.OHLC_CONSISTENCY,
                    severity=ValidationSeverity.ERROR,
                    instrument_symbol=record.symbol,
                    instrument_id=record.instrument_id,
                    date=record.date,
                    vendor=record.vendor,
                    message=f"OHLC consistency violations: {'; '.join(violations)}",
                    details={
                        "violations": violations,
                        "ohlc": {
                            "open": float(record.open),
                            "high": float(record.high),
                            "low": float(record.low),
                            "close": float(record.close)
                        }
                    },
                    detected_at=datetime.now()
                ))

        return issues

    async def _validate_extreme_prices(self, validation_date: date,
                                     price_records: List[PriceRecord]) -> List[ValidationIssue]:
        """Validate for extremely high/low prices and large price changes."""
        issues = []

        # Get previous day's prices for comparison
        prev_date = validation_date - timedelta(days=1)
        while prev_date.weekday() > 4:  # Skip weekends
            prev_date -= timedelta(days=1)

        prev_prices = await self._get_previous_prices(prev_date)

        for record in price_records:
            if record.close is None:
                continue

            close_price = float(record.close)

            # Check absolute price limits
            if close_price < self.thresholds["min_price"]:
                issues.append(ValidationIssue(
                    rule=ValidationRule.EXTREME_PRICES,
                    severity=ValidationSeverity.WARNING,
                    instrument_symbol=record.symbol,
                    instrument_id=record.instrument_id,
                    date=record.date,
                    vendor=record.vendor,
                    message=f"Price too low: {close_price} < {self.thresholds['min_price']}",
                    details={"price": close_price, "threshold": self.thresholds["min_price"]},
                    detected_at=datetime.now()
                ))

            if close_price > self.thresholds["max_price"]:
                issues.append(ValidationIssue(
                    rule=ValidationRule.EXTREME_PRICES,
                    severity=ValidationSeverity.WARNING,
                    instrument_symbol=record.symbol,
                    instrument_id=record.instrument_id,
                    date=record.date,
                    vendor=record.vendor,
                    message=f"Price too high: {close_price} > {self.thresholds['max_price']}",
                    details={"price": close_price, "threshold": self.thresholds["max_price"]},
                    detected_at=datetime.now()
                ))

            # Check price change vs previous day
            prev_key = (record.instrument_id, record.vendor)
            if prev_key in prev_prices:
                prev_close = float(prev_prices[prev_key])
                change_pct = abs((close_price - prev_close) / prev_close * 100)

                if change_pct > self.thresholds["max_price_change_pct"]:
                    issues.append(ValidationIssue(
                        rule=ValidationRule.PRICE_GAPS,
                        severity=ValidationSeverity.WARNING,
                        instrument_symbol=record.symbol,
                        instrument_id=record.instrument_id,
                        date=record.date,
                        vendor=record.vendor,
                        message=f"Large price change: {change_pct:.1f}% (prev: {prev_close}, current: {close_price})",
                        details={
                            "change_pct": change_pct,
                            "prev_close": prev_close,
                            "current_close": close_price,
                            "threshold": self.thresholds["max_price_change_pct"]
                        },
                        detected_at=datetime.now()
                    ))

        return issues

    async def _get_previous_prices(self, prev_date: date) -> Dict[Tuple[int, str], Decimal]:
        """Get previous day's closing prices for comparison."""
        prev_prices = {}

        for vendor in ["polygon", "tiingo"]:
            table_name = self.env.get_table_name(f"daily_price_polygon_{vendor}")

            async with self.pool.acquire() as conn:
                rows = await conn.fetch(f"""
                    SELECT instrument_id, close
                    FROM {table_name}
                    WHERE date = $1 AND close IS NOT NULL
                """, prev_date)

                for row in rows:
                    key = (row["instrument_id"], vendor)
                    prev_prices[key] = row["close"]

        return prev_prices

    async def _validate_volume_anomalies(self, validation_date: date,
                                       price_records: List[PriceRecord]) -> List[ValidationIssue]:
        """Validate for volume anomalies."""
        issues = []

        # Get average volumes for comparison
        avg_volumes = await self._get_average_volumes()

        for record in price_records:
            if record.volume is None or record.volume <= 0:
                issues.append(ValidationIssue(
                    rule=ValidationRule.VOLUME_ANOMALY,
                    severity=ValidationSeverity.WARNING,
                    instrument_symbol=record.symbol,
                    instrument_id=record.instrument_id,
                    date=record.date,
                    vendor=record.vendor,
                    message=f"Zero or null volume: {record.volume}",
                    details={"volume": record.volume},
                    detected_at=datetime.now()
                ))
                continue

            # Check against average volume
            avg_key = (record.instrument_id, record.vendor)
            if avg_key in avg_volumes:
                avg_volume = avg_volumes[avg_key]
                volume_ratio = record.volume / avg_volume

                if volume_ratio > self.thresholds["max_volume_multiplier"]:
                    issues.append(ValidationIssue(
                        rule=ValidationRule.VOLUME_ANOMALY,
                        severity=ValidationSeverity.INFO,
                        instrument_symbol=record.symbol,
                        instrument_id=record.instrument_id,
                        date=record.date,
                        vendor=record.vendor,
                        message=f"Unusually high volume: {record.volume} ({volume_ratio:.1f}x average)",
                        details={
                            "volume": record.volume,
                            "avg_volume": avg_volume,
                            "ratio": volume_ratio
                        },
                        detected_at=datetime.now()
                    ))

        return issues

    async def _get_average_volumes(self) -> Dict[Tuple[int, str], float]:
        """Get 30-day average volumes for comparison."""
        avg_volumes = {}

        cutoff_date = date.today() - timedelta(days=30)

        for vendor in ["polygon", "tiingo"]:
            table_name = self.env.get_table_name(f"daily_price_polygon_{vendor}")

            async with self.pool.acquire() as conn:
                rows = await conn.fetch(f"""
                    SELECT instrument_id, AVG(volume) as avg_volume
                    FROM {table_name}
                    WHERE date >= $1 AND volume > 0
                    GROUP BY instrument_id
                """, cutoff_date)

                for row in rows:
                    key = (row["instrument_id"], vendor)
                    avg_volumes[key] = float(row["avg_volume"])

        return avg_volumes

    async def _validate_price_gaps(self, validation_date: date,
                                 price_records: List[PriceRecord]) -> List[ValidationIssue]:
        """Already handled in extreme_prices validation."""
        return []

    async def _validate_stale_data(self, validation_date: date,
                                 price_records: List[PriceRecord]) -> List[ValidationIssue]:
        """Validate for stale data (missing recent updates)."""
        issues = []

        # Check if today's data exists when it should
        if validation_date == date.today() and validation_date.weekday() < 5:
            # Market hours check (after 4 PM EST, expect data)
            current_time = datetime.now().time()
            market_close = datetime.strptime("16:30", "%H:%M").time()  # 4:30 PM with buffer

            if current_time > market_close:
                # Get instruments that should have data but don't
                instruments_with_data = set((r.instrument_id, r.vendor) for r in price_records)

                instruments_table = self.env.get_table_name("instruments")
                async with self.pool.acquire() as conn:
                    all_instruments = await conn.fetch(f"""
                        SELECT id, symbol FROM {instruments_table}
                        WHERE is_active = true
                        LIMIT 100  -- Check sample for performance
                    """)

                for instrument in all_instruments:
                    for vendor in ["polygon", "tiingo"]:
                        key = (instrument["id"], vendor)
                        if key not in instruments_with_data:
                            issues.append(ValidationIssue(
                                rule=ValidationRule.STALE_DATA,
                                severity=ValidationSeverity.WARNING,
                                instrument_symbol=instrument["symbol"],
                                instrument_id=instrument["id"],
                                date=validation_date,
                                vendor=vendor,
                                message=f"Missing current day data after market close",
                                details={"market_close_time": "16:00", "current_time": current_time.strftime("%H:%M")},
                                detected_at=datetime.now()
                            ))

        return issues

    async def _validate_cross_vendor_consistency(self, validation_date: date,
                                               price_records: List[PriceRecord]) -> List[ValidationIssue]:
        """Validate consistency between vendors."""
        issues = []

        # Group records by instrument
        by_instrument = {}
        for record in price_records:
            if record.instrument_id not in by_instrument:
                by_instrument[record.instrument_id] = {}
            by_instrument[record.instrument_id][record.vendor] = record

        # Compare prices between vendors
        tolerance_pct = self.thresholds["cross_vendor_tolerance_pct"]

        for instrument_id, vendor_records in by_instrument.items():
            if len(vendor_records) < 2:
                continue  # Need at least 2 vendors to compare

            vendors = list(vendor_records.keys())
            for i in range(len(vendors)):
                for j in range(i + 1, len(vendors)):
                    vendor1, vendor2 = vendors[i], vendors[j]
                    record1, record2 = vendor_records[vendor1], vendor_records[vendor2]

                    # Compare closing prices
                    if record1.close is not None and record2.close is not None:
                        price1, price2 = float(record1.close), float(record2.close)
                        diff_pct = abs(price1 - price2) / ((price1 + price2) / 2) * 100

                        if diff_pct > tolerance_pct:
                            issues.append(ValidationIssue(
                                rule=ValidationRule.CROSS_VENDOR_MISMATCH,
                                severity=ValidationSeverity.WARNING,
                                instrument_symbol=record1.symbol,
                                instrument_id=instrument_id,
                                date=validation_date,
                                vendor=f"{vendor1}_vs_{vendor2}",
                                message=f"Price mismatch: {vendor1}=${price1:.2f} vs {vendor2}=${price2:.2f} ({diff_pct:.1f}% diff)",
                                details={
                                    "vendor1": vendor1,
                                    "vendor2": vendor2,
                                    "price1": price1,
                                    "price2": price2,
                                    "diff_pct": diff_pct,
                                    "tolerance": tolerance_pct
                                },
                                detected_at=datetime.now()
                            ))

        return issues

    async def _store_validation_issues(self, issues: List[ValidationIssue]):
        """Store validation issues in database."""
        if not issues:
            return

        async with self.pool.acquire() as conn:
            # Clear existing issues for the same date/rule combinations to avoid duplicates
            for issue in issues:
                await conn.execute(f"""
                    DELETE FROM {self.issues_table}
                    WHERE rule = $1 AND instrument_id = $2 AND date = $3 AND vendor = $4
                """, issue.rule.value, issue.instrument_id, issue.date, issue.vendor)

            # Insert new issues
            issue_records = []
            for issue in issues:
                issue_records.append((
                    issue.rule.value,
                    issue.severity.value,
                    issue.instrument_symbol,
                    issue.instrument_id,
                    issue.date,
                    issue.vendor,
                    issue.message,
                    issue.details,
                    issue.detected_at,
                    issue.resolved
                ))

            await conn.executemany(f"""
                INSERT INTO {self.issues_table}
                (rule, severity, instrument_symbol, instrument_id, date, vendor,
                 message, details, detected_at, resolved)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """, issue_records)

        logger.info(f"Stored {len(issues)} validation issues")

    async def _store_validation_summary(self, validation_date: date, results: Dict[str, Any]):
        """Store validation summary statistics."""
        summary_table = self.env.get_table_name("price_validation_summary")

        async with self.pool.acquire() as conn:
            await conn.execute(f"""
                INSERT INTO {summary_table}
                (date, total_instruments, missing_data_count, error_count, warning_count,
                 critical_count, cross_vendor_mismatches, data_quality_score, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
                ON CONFLICT (date) DO UPDATE SET
                total_instruments = EXCLUDED.total_instruments,
                missing_data_count = EXCLUDED.missing_data_count,
                error_count = EXCLUDED.error_count,
                warning_count = EXCLUDED.warning_count,
                critical_count = EXCLUDED.critical_count,
                cross_vendor_mismatches = EXCLUDED.cross_vendor_mismatches,
                data_quality_score = EXCLUDED.data_quality_score,
                updated_at = CURRENT_TIMESTAMP
            """,
                validation_date,
                results["instruments_validated"],
                results["issues_by_rule"].get("missing_data", 0),
                results["issues_by_severity"].get("error", 0),
                results["issues_by_severity"].get("warning", 0),
                results["issues_by_severity"].get("critical", 0),
                results["issues_by_rule"].get("cross_vendor_mismatch", 0),
                results["data_quality_score"]
            )

    def _group_by_severity(self, issues: List[ValidationIssue]) -> Dict[str, int]:
        """Group issues by severity."""
        by_severity = {}
        for issue in issues:
            severity = issue.severity.value
            by_severity[severity] = by_severity.get(severity, 0) + 1
        return by_severity

    def _group_by_rule(self, issues: List[ValidationIssue]) -> Dict[str, int]:
        """Group issues by rule."""
        by_rule = {}
        for issue in issues:
            rule = issue.rule.value
            by_rule[rule] = by_rule.get(rule, 0) + 1
        return by_rule

    def _calculate_quality_score(self, total_instruments: int, issues: List[ValidationIssue]) -> float:
        """Calculate overall data quality score (0-100)."""
        if total_instruments == 0:
            return 0.0

        # Weight different severity levels
        severity_weights = {
            ValidationSeverity.CRITICAL: 10,
            ValidationSeverity.ERROR: 5,
            ValidationSeverity.WARNING: 2,
            ValidationSeverity.INFO: 1
        }

        total_penalty = sum(severity_weights.get(issue.severity, 1) for issue in issues)
        max_possible_penalty = total_instruments * 10  # Assume worst case

        # Score from 0-100, where 100 is perfect
        score = max(0, 100 - (total_penalty / max_possible_penalty * 100))
        return round(score, 2)

    async def get_validation_report(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Generate comprehensive validation report for date range."""
        async with self.pool.acquire() as conn:
            # Get summary statistics
            summary_table = self.env.get_table_name("price_validation_summary")
            summary_rows = await conn.fetch(f"""
                SELECT * FROM {summary_table}
                WHERE date BETWEEN $1 AND $2
                ORDER BY date DESC
            """, start_date, end_date)

            # Get top issues
            issues_rows = await conn.fetch(f"""
                SELECT rule, severity, COUNT(*) as count,
                       array_agg(DISTINCT instrument_symbol) as affected_symbols
                FROM {self.issues_table}
                WHERE date BETWEEN $1 AND $2 AND NOT resolved
                GROUP BY rule, severity
                ORDER BY count DESC
                LIMIT 20
            """, start_date, end_date)

            # Get instruments with most issues
            problem_instruments = await conn.fetch(f"""
                SELECT instrument_symbol, COUNT(*) as issue_count,
                       array_agg(DISTINCT rule) as issue_types
                FROM {self.issues_table}
                WHERE date BETWEEN $1 AND $2 AND NOT resolved
                GROUP BY instrument_symbol
                ORDER BY issue_count DESC
                LIMIT 10
            """, start_date, end_date)

        return {
            "period": {"start": start_date, "end": end_date},
            "daily_summaries": [dict(row) for row in summary_rows],
            "top_issues": [dict(row) for row in issues_rows],
            "problem_instruments": [dict(row) for row in problem_instruments],
            "overall_quality": statistics.mean([row["data_quality_score"] for row in summary_rows]) if summary_rows else 0
        }