"""
Quality Scan MCP Tool
====================

Comprehensive data quality scanning tool implementing MCP 2025 standards.
Performs completeness, accuracy, consistency, and timeliness checks.
"""

import asyncpg
import logging
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class QualityIssue:
    """Data quality issue detected by scanning"""
    id: str
    issue_type: str  # 'missing_data', 'stale_data', 'duplicate_records', 'extreme_values'
    severity: str    # 'critical', 'high', 'medium', 'low'
    symbol: str
    affected_date: date
    description: str
    field: str
    expected_value: Optional[float]
    actual_value: Optional[float]
    confidence_score: float
    detected_at: datetime
    metadata: Dict[str, Any]

@dataclass
class QualityScanResult:
    """Complete quality scan results"""
    overall_score: float
    issues: List[QualityIssue]
    rule_results: Dict[str, Dict[str, Any]]
    scan_metadata: Dict[str, Any]
    recommendations: List[str]

class QualityScanTool:
    """MCP Tool for comprehensive data quality scanning"""

    def __init__(self):
        self.tool_name = "quality_scan_tool"
        self.version = "1.0.0"
        self.db_config = {
            'host': 'ats-intg-postgres',
            'port': 5432,
            'user': 'postgres',
            'password': 'intg_password',
            'database': 'intg_db'
        }

    def get_tool_definition(self) -> Dict[str, Any]:
        """MCP tool definition following 2025 standard"""
        return {
            "name": self.tool_name,
            "description": "Run comprehensive data quality checks across databases",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Target table for quality scanning"
                    },
                    "date_range": {
                        "type": "object",
                        "properties": {
                            "days_back": {"type": "integer", "default": 7},
                            "start_date": {"type": "string"},
                            "end_date": {"type": "string"}
                        }
                    },
                    "quality_rules": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Quality rules to apply: completeness, timeliness, consistency, accuracy"
                    },
                    "symbol_filter": {
                        "type": "string",
                        "description": "Optional symbol filter"
                    },
                    "severity_threshold": {
                        "type": "string",
                        "default": "medium",
                        "enum": ["critical", "high", "medium", "low"]
                    }
                },
                "required": ["table_name"]
            }
        }

    async def execute(self, arguments: Dict[str, Any]) -> QualityScanResult:
        """Execute comprehensive quality scan"""
        table_name = arguments["table_name"]
        date_range = arguments.get("date_range", {"days_back": 7})
        quality_rules = arguments.get("quality_rules", ["completeness", "timeliness", "consistency"])
        symbol_filter = arguments.get("symbol_filter")
        severity_threshold = arguments.get("severity_threshold", "medium")

        logger.info(f"Starting quality scan for {table_name} with rules: {quality_rules}")

        scan_start_time = datetime.now()
        all_issues = []
        rule_results = {}

        try:
            conn = await asyncpg.connect(**self.db_config)

            # Execute each quality rule
            for rule in quality_rules:
                logger.info(f"Executing quality rule: {rule}")
                rule_result = await self._execute_quality_rule(
                    conn, table_name, rule, date_range, symbol_filter
                )
                rule_results[rule] = rule_result
                all_issues.extend(rule_result.get("issues", []))

            await conn.close()

        except Exception as e:
            logger.error(f"Quality scan failed: {e}")
            # Add system error as critical issue
            all_issues.append(QualityIssue(
                id=f"scan_error_{int(datetime.now().timestamp())}",
                issue_type="scan_error",
                severity="critical",
                symbol="SYSTEM",
                affected_date=date.today(),
                description=f"Quality scan failed: {str(e)}",
                field="system",
                expected_value=None,
                actual_value=None,
                confidence_score=1.0,
                detected_at=datetime.now(),
                metadata={"error_type": type(e).__name__}
            ))

        # Filter by severity threshold
        filtered_issues = self._filter_by_severity(all_issues, severity_threshold)

        # Calculate overall quality score
        overall_score = self._calculate_overall_score(rule_results, filtered_issues)

        # Generate recommendations
        recommendations = self._generate_recommendations(filtered_issues)

        scan_duration = (datetime.now() - scan_start_time).total_seconds()

        return QualityScanResult(
            overall_score=overall_score,
            issues=filtered_issues,
            rule_results=rule_results,
            scan_metadata={
                "scan_duration_seconds": scan_duration,
                "rules_executed": quality_rules,
                "table_scanned": table_name,
                "date_range": date_range,
                "total_issues_found": len(all_issues),
                "filtered_issues": len(filtered_issues)
            },
            recommendations=recommendations
        )

    async def _execute_quality_rule(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        rule: str,
        date_range: Dict[str, Any],
        symbol_filter: Optional[str]
    ) -> Dict[str, Any]:
        """Execute specific quality rule"""

        if rule == "completeness":
            return await self._check_completeness(conn, table_name, date_range, symbol_filter)
        elif rule == "timeliness":
            return await self._check_timeliness(conn, table_name, date_range, symbol_filter)
        elif rule == "consistency":
            return await self._check_consistency(conn, table_name, date_range, symbol_filter)
        elif rule == "accuracy":
            return await self._check_accuracy(conn, table_name, date_range, symbol_filter)
        else:
            logger.warning(f"Unknown quality rule: {rule}")
            return {"score": 1.0, "issues": [], "details": f"Unknown rule: {rule}"}

    async def _check_completeness(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        date_range: Dict[str, Any],
        symbol_filter: Optional[str]
    ) -> Dict[str, Any]:
        """Check data completeness - missing dates and records"""
        issues = []

        # Calculate date range
        if "days_back" in date_range:
            end_date = date.today()
            start_date = end_date - timedelta(days=date_range["days_back"])
        else:
            start_date = datetime.fromisoformat(date_range["start_date"]).date()
            end_date = datetime.fromisoformat(date_range["end_date"]).date()

        # Check for missing trading days
        missing_dates_query = """
        WITH date_series AS (
            SELECT generate_series($1::date, $2::date, '1 day'::interval)::date as expected_date
        ),
        actual_dates AS (
            SELECT DISTINCT date_trunc('day', timestamp)::date as actual_date
            FROM intg_daily_price
            WHERE timestamp >= $1::date AND timestamp <= $2::date
            {symbol_filter}
        )
        SELECT ds.expected_date
        FROM date_series ds
        LEFT JOIN actual_dates ad ON ds.expected_date = ad.actual_date
        WHERE ad.actual_date IS NULL
        AND EXTRACT(dow FROM ds.expected_date) NOT IN (0, 6)  -- Exclude weekends
        ORDER BY ds.expected_date;
        """

        symbol_filter_clause = ""
        query_params = [start_date, end_date]

        if symbol_filter:
            symbol_filter_clause = "AND symbol = $3"
            query_params.append(symbol_filter)

        final_query = missing_dates_query.format(symbol_filter=symbol_filter_clause)

        missing_dates = await conn.fetch(final_query, *query_params)

        for row in missing_dates:
            missing_date = row['expected_date']
            issues.append(QualityIssue(
                id=f"missing_data_{missing_date}",
                issue_type="missing_data",
                severity="high",
                symbol=symbol_filter or "ALL",
                affected_date=missing_date,
                description=f"No daily prices found for {missing_date}",
                field="all_fields",
                expected_value=None,
                actual_value=None,
                confidence_score=0.95,
                detected_at=datetime.now(),
                metadata={"rule": "completeness", "check_type": "missing_dates"}
            ))

        # Calculate completeness score
        total_expected_days = (end_date - start_date).days + 1
        weekdays_expected = sum(1 for i in range(total_expected_days)
                               if (start_date + timedelta(days=i)).weekday() < 5)
        missing_count = len(missing_dates)
        completeness_score = max(0.0, (weekdays_expected - missing_count) / weekdays_expected) if weekdays_expected > 0 else 1.0

        return {
            "score": completeness_score,
            "issues": issues,
            "details": {
                "total_expected_days": weekdays_expected,
                "missing_days": missing_count,
                "completeness_percentage": completeness_score * 100
            }
        }

    async def _check_timeliness(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        date_range: Dict[str, Any],
        symbol_filter: Optional[str]
    ) -> Dict[str, Any]:
        """Check data timeliness - identify stale data"""
        issues = []

        # Find stale data (older than expected)
        stale_data_query = """
        SELECT symbol,
               date_trunc('day', timestamp)::date as price_date,
               timestamp,
               CURRENT_DATE - date_trunc('day', timestamp)::date as days_old
        FROM intg_daily_price
        WHERE CURRENT_DATE - date_trunc('day', timestamp)::date > 30  -- More than 30 days old
        {symbol_filter}
        ORDER BY days_old DESC
        LIMIT 20;
        """

        query_params = []
        symbol_filter_clause = ""

        if symbol_filter:
            symbol_filter_clause = "AND symbol = $1"
            query_params.append(symbol_filter)

        final_query = stale_data_query.format(symbol_filter=symbol_filter_clause)

        stale_records = await conn.fetch(final_query, *query_params)

        for row in stale_records:
            days_old = row['days_old']
            severity = "critical" if days_old > 1000 else "high" if days_old > 100 else "medium"

            issues.append(QualityIssue(
                id=f"stale_data_{row['symbol']}_{row['price_date']}",
                issue_type="stale_data",
                severity=severity,
                symbol=row['symbol'],
                affected_date=row['price_date'],
                description=f"Data is {days_old} days old (last updated: {row['price_date']})",
                field="timestamp",
                expected_value=1.0,  # Expected: recent data
                actual_value=float(days_old),
                confidence_score=0.9,
                detected_at=datetime.now(),
                metadata={"rule": "timeliness", "days_old": days_old}
            ))

        # Calculate timeliness score based on average data age
        if stale_records:
            avg_days_old = sum(row['days_old'] for row in stale_records) / len(stale_records)
            timeliness_score = max(0.0, 1.0 - (avg_days_old / 365))  # Degrade score over a year
        else:
            timeliness_score = 1.0

        return {
            "score": timeliness_score,
            "issues": issues,
            "details": {
                "stale_records_found": len(stale_records),
                "avg_age_days": sum(row['days_old'] for row in stale_records) / len(stale_records) if stale_records else 0
            }
        }

    async def _check_consistency(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        date_range: Dict[str, Any],
        symbol_filter: Optional[str]
    ) -> Dict[str, Any]:
        """Check data consistency - duplicates and integrity violations"""
        issues = []

        # Check for duplicate records
        duplicates_query = """
        SELECT symbol,
               date_trunc('day', timestamp)::date as price_date,
               COUNT(*) as duplicate_count
        FROM intg_daily_price
        WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
        {symbol_filter}
        GROUP BY symbol, date_trunc('day', timestamp)::date
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC;
        """

        query_params = []
        symbol_filter_clause = ""

        if symbol_filter:
            symbol_filter_clause = "AND symbol = $1"
            query_params.append(symbol_filter)

        final_query = duplicates_query.format(symbol_filter=symbol_filter_clause)

        duplicate_records = await conn.fetch(final_query, *query_params)

        for row in duplicate_records:
            duplicate_count = row['duplicate_count']

            issues.append(QualityIssue(
                id=f"duplicate_{row['symbol']}_{row['price_date']}",
                issue_type="duplicate_records",
                severity="critical" if duplicate_count > 5 else "high",
                symbol=row['symbol'],
                affected_date=row['price_date'],
                description=f"Found {duplicate_count} duplicate records for the same date",
                field="primary_key",
                expected_value=1.0,
                actual_value=float(duplicate_count),
                confidence_score=1.0,
                detected_at=datetime.now(),
                metadata={"rule": "consistency", "duplicate_count": duplicate_count}
            ))

        consistency_score = 1.0 if len(duplicate_records) == 0 else max(0.0, 1.0 - (len(duplicate_records) / 10))

        return {
            "score": consistency_score,
            "issues": issues,
            "details": {
                "duplicate_groups_found": len(duplicate_records),
                "total_duplicates": sum(row['duplicate_count'] for row in duplicate_records)
            }
        }

    async def _check_accuracy(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        date_range: Dict[str, Any],
        symbol_filter: Optional[str]
    ) -> Dict[str, Any]:
        """Check data accuracy - extreme values and anomalies"""
        issues = []

        # Check for extreme price movements
        extreme_values_query = """
        SELECT symbol,
               date_trunc('day', timestamp)::date as price_date,
               open_price, high_price, low_price, close_price, volume,
               LAG(close_price) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_close
        FROM intg_daily_price
        WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
        {symbol_filter}
        AND (
            high_price / low_price > 1.5 OR  -- 50% intraday range
            volume > 100000000              -- 100M+ volume
        )
        ORDER BY timestamp DESC
        LIMIT 10;
        """

        query_params = []
        symbol_filter_clause = ""

        if symbol_filter:
            symbol_filter_clause = "AND symbol = $1"
            query_params.append(symbol_filter)

        final_query = extreme_values_query.format(symbol_filter=symbol_filter_clause)

        extreme_records = await conn.fetch(final_query, *query_params)

        for row in extreme_records:
            # Check for extreme intraday movement
            if row['high_price'] and row['low_price'] and row['high_price'] / row['low_price'] > 1.5:
                intraday_range = (row['high_price'] - row['low_price']) / row['close_price']
                issues.append(QualityIssue(
                    id=f"extreme_range_{row['symbol']}_{row['price_date']}",
                    issue_type="extreme_price_range",
                    severity="medium",
                    symbol=row['symbol'],
                    affected_date=row['price_date'],
                    description=f"Extreme intraday price range: {intraday_range:.1%}",
                    field="price_range",
                    expected_value=0.1,  # 10% expected
                    actual_value=intraday_range,
                    confidence_score=0.8,
                    detected_at=datetime.now(),
                    metadata={"rule": "accuracy", "high": row['high_price'], "low": row['low_price']}
                ))

            # Check for extreme volume
            if row['volume'] and row['volume'] > 100000000:
                issues.append(QualityIssue(
                    id=f"extreme_volume_{row['symbol']}_{row['price_date']}",
                    issue_type="extreme_volume",
                    severity="medium",
                    symbol=row['symbol'],
                    affected_date=row['price_date'],
                    description=f"Extremely high volume: {row['volume']:,} shares",
                    field="volume",
                    expected_value=10000000,  # 10M expected
                    actual_value=float(row['volume']),
                    confidence_score=0.7,
                    detected_at=datetime.now(),
                    metadata={"rule": "accuracy", "volume": row['volume']}
                ))

        accuracy_score = max(0.0, 1.0 - (len(extreme_records) / 20))  # Degrade with more anomalies

        return {
            "score": accuracy_score,
            "issues": issues,
            "details": {
                "extreme_records_found": len(extreme_records),
                "accuracy_percentage": accuracy_score * 100
            }
        }

    def _filter_by_severity(self, issues: List[QualityIssue], severity_threshold: str) -> List[QualityIssue]:
        """Filter issues by severity threshold"""
        severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        threshold_level = severity_order.get(severity_threshold, 2)

        return [issue for issue in issues if severity_order.get(issue.severity, 3) <= threshold_level]

    def _calculate_overall_score(self, rule_results: Dict[str, Dict[str, Any]], issues: List[QualityIssue]) -> float:
        """Calculate overall quality score"""
        if not rule_results:
            return 0.0

        # Average of individual rule scores
        rule_scores = [result.get("score", 0.0) for result in rule_results.values()]
        base_score = sum(rule_scores) / len(rule_scores)

        # Penalize based on critical issues
        critical_issues = len([issue for issue in issues if issue.severity == "critical"])
        penalty = min(0.5, critical_issues * 0.1)  # Up to 50% penalty

        return max(0.0, base_score - penalty)

    def _generate_recommendations(self, issues: List[QualityIssue]) -> List[str]:
        """Generate actionable recommendations based on issues found"""
        recommendations = []

        issue_types = {}
        for issue in issues:
            issue_types[issue.issue_type] = issue_types.get(issue.issue_type, 0) + 1

        if "missing_data" in issue_types:
            count = issue_types["missing_data"]
            recommendations.append(f"🔄 Trigger backfill for {count} missing data periods")

        if "stale_data" in issue_types:
            count = issue_types["stale_data"]
            recommendations.append(f"⚡ Refresh {count} stale data records from vendors")

        if "duplicate_records" in issue_types:
            count = issue_types["duplicate_records"]
            recommendations.append(f"🧹 Clean up {count} duplicate record groups")

        if "extreme_volume" in issue_types or "extreme_price_range" in issue_types:
            recommendations.append("📊 Cross-validate extreme values with multiple vendors")

        if not recommendations:
            recommendations.append("✅ No immediate actions required - data quality is good")

        return recommendations

    def to_dict(self) -> Dict[str, Any]:
        """Convert tool to dictionary for serialization"""
        return {
            "tool_name": self.tool_name,
            "version": self.version,
            "description": "Comprehensive data quality scanning with MCP 2025 standards",
            "capabilities": [
                "completeness_checking",
                "timeliness_validation",
                "consistency_verification",
                "accuracy_assessment",
                "anomaly_detection",
                "recommendation_generation"
            ]
        }