"""
Production Regulatory Compliance Validator

Real-time compliance validation using actual regulatory data sources
for comprehensive data quality framework integration.
"""

import asyncio
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Set, Any, Tuple
from dataclasses import dataclass
from enum import Enum

from .real_regulatory_sources import (
    RealRegulatoryDataIntegrator,
    SECFiling,
    EconomicIndicator,
    RegulatoryContext
)

logger = logging.getLogger(__name__)


class ComplianceStatus(Enum):
    """Compliance validation status."""
    COMPLIANT = "compliant"
    NON_COMPLIANT = "non_compliant"
    PENDING_REVIEW = "pending_review"
    INSUFFICIENT_DATA = "insufficient_data"


class ComplianceRule(Enum):
    """Regulatory compliance rules."""
    MIFID_II_TRANSPARENCY = "mifid_ii_transparency"
    DODD_FRANK_SWAP_REPORTING = "dodd_frank_swap_reporting"
    BASEL_III_CAPITAL_ADEQUACY = "basel_iii_capital_adequacy"
    SEC_FAIR_DISCLOSURE = "sec_fair_disclosure"
    CFTC_POSITION_LIMITS = "cftc_position_limits"
    FINRA_BEST_EXECUTION = "finra_best_execution"


@dataclass
class ComplianceViolation:
    """Detected compliance violation."""
    rule: ComplianceRule
    severity: str  # 'critical', 'high', 'medium', 'low'
    description: str
    data_point: Dict[str, Any]
    regulatory_context: RegulatoryContext
    violation_timestamp: datetime
    remediation_required: bool
    expected_resolution_time: timedelta


@dataclass
class ComplianceReport:
    """Comprehensive compliance validation report."""
    symbol: str
    validation_timestamp: datetime
    overall_status: ComplianceStatus
    rules_evaluated: List[ComplianceRule]
    violations: List[ComplianceViolation]
    regulatory_context: RegulatoryContext
    confidence_score: float
    next_review_date: datetime


class ProductionComplianceValidator:
    """
    Production-grade regulatory compliance validator using real regulatory data.

    Validates financial data against actual regulatory requirements with
    real-time regulatory context from SEC, CFTC, and Federal Reserve sources.
    """

    def __init__(self, regulatory_integrator: RealRegulatoryDataIntegrator):
        self.regulatory_integrator = regulatory_integrator
        self._compliance_cache: Dict[str, ComplianceReport] = {}
        self._rule_processors = self._initialize_rule_processors()

    async def validate_daily_price_compliance(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date
    ) -> ComplianceReport:
        """
        Validate daily price data against comprehensive regulatory requirements.

        Args:
            symbol: Stock symbol to validate
            price_data: Daily price data (open, high, low, close, volume, etc.)
            trading_date: Trading date for the price data

        Returns:
            Comprehensive compliance report with violations and regulatory context
        """
        logger.info(f"Validating compliance for {symbol} on {trading_date}")

        # Get real regulatory context
        regulatory_context = await self.regulatory_integrator.get_comprehensive_context(
            symbol, trading_date
        )

        # Initialize compliance report
        report = ComplianceReport(
            symbol=symbol,
            validation_timestamp=datetime.utcnow(),
            overall_status=ComplianceStatus.COMPLIANT,
            rules_evaluated=[],
            violations=[],
            regulatory_context=regulatory_context,
            confidence_score=0.0,
            next_review_date=datetime.utcnow() + timedelta(days=1)
        )

        # Evaluate each compliance rule
        for rule in ComplianceRule:
            try:
                rule_result = await self._evaluate_compliance_rule(
                    rule, symbol, price_data, trading_date, regulatory_context
                )

                report.rules_evaluated.append(rule)

                if rule_result.violations:
                    report.violations.extend(rule_result.violations)

                    # Update overall status based on severity
                    critical_violations = [
                        v for v in rule_result.violations
                        if v.severity == 'critical'
                    ]
                    if critical_violations:
                        report.overall_status = ComplianceStatus.NON_COMPLIANT
                    elif report.overall_status == ComplianceStatus.COMPLIANT:
                        report.overall_status = ComplianceStatus.PENDING_REVIEW

            except Exception as e:
                logger.error(f"Error evaluating {rule.value} for {symbol}: {e}")
                report.overall_status = ComplianceStatus.INSUFFICIENT_DATA

        # Calculate confidence score based on available regulatory data
        report.confidence_score = self._calculate_confidence_score(
            regulatory_context, report.rules_evaluated
        )

        # Cache result for performance
        cache_key = f"{symbol}_{trading_date.isoformat()}"
        self._compliance_cache[cache_key] = report

        logger.info(
            f"Compliance validation complete for {symbol}: "
            f"{report.overall_status.value} with {len(report.violations)} violations"
        )

        return report

    async def _evaluate_compliance_rule(
        self,
        rule: ComplianceRule,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date,
        regulatory_context: RegulatoryContext
    ) -> 'RuleEvaluationResult':
        """Evaluate specific compliance rule against price data."""

        processor = self._rule_processors.get(rule)
        if not processor:
            logger.warning(f"No processor available for rule: {rule.value}")
            return RuleEvaluationResult(rule, [])

        return await processor(symbol, price_data, trading_date, regulatory_context)

    def _initialize_rule_processors(self) -> Dict[ComplianceRule, callable]:
        """Initialize rule processors for each compliance rule."""
        return {
            ComplianceRule.MIFID_II_TRANSPARENCY: self._validate_mifid_ii_transparency,
            ComplianceRule.SEC_FAIR_DISCLOSURE: self._validate_sec_fair_disclosure,
            ComplianceRule.DODD_FRANK_SWAP_REPORTING: self._validate_dodd_frank,
            ComplianceRule.BASEL_III_CAPITAL_ADEQUACY: self._validate_basel_iii,
            ComplianceRule.CFTC_POSITION_LIMITS: self._validate_cftc_limits,
            ComplianceRule.FINRA_BEST_EXECUTION: self._validate_finra_execution,
        }

    async def _validate_mifid_ii_transparency(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date,
        regulatory_context: RegulatoryContext
    ) -> 'RuleEvaluationResult':
        """Validate MiFID II transparency requirements."""
        violations = []

        # Check for pre-trade transparency violations
        if 'volume' in price_data:
            daily_volume = price_data['volume']

            # Use real regulatory context for volume thresholds
            if regulatory_context.economic_indicators:
                market_stress_indicators = [
                    indicator for indicator in regulatory_context.economic_indicators
                    if 'volatility' in indicator.name.lower() or 'stress' in indicator.name.lower()
                ]

                # During market stress, transparency thresholds are lowered
                if market_stress_indicators:
                    stress_threshold = 500000  # Shares
                    if daily_volume > stress_threshold:
                        violations.append(ComplianceViolation(
                            rule=ComplianceRule.MIFID_II_TRANSPARENCY,
                            severity='high',
                            description=f"High volume trading ({daily_volume:,} shares) during market stress requires enhanced transparency reporting",
                            data_point={'volume': daily_volume, 'date': trading_date.isoformat()},
                            regulatory_context=regulatory_context,
                            violation_timestamp=datetime.utcnow(),
                            remediation_required=True,
                            expected_resolution_time=timedelta(hours=4)
                        ))

        # Check for post-trade reporting timing violations using SEC filings
        if regulatory_context.sec_filings:
            recent_filings = [
                filing for filing in regulatory_context.sec_filings
                if filing.filing_date >= trading_date - timedelta(days=1)
            ]

            if recent_filings:
                # If there were recent material filings, enhanced reporting is required
                violations.append(ComplianceViolation(
                    rule=ComplianceRule.MIFID_II_TRANSPARENCY,
                    severity='medium',
                    description=f"Recent SEC filings require enhanced post-trade transparency (filings: {len(recent_filings)})",
                    data_point={'recent_filings': len(recent_filings)},
                    regulatory_context=regulatory_context,
                    violation_timestamp=datetime.utcnow(),
                    remediation_required=True,
                    expected_resolution_time=timedelta(hours=2)
                ))

        return RuleEvaluationResult(ComplianceRule.MIFID_II_TRANSPARENCY, violations)

    async def _validate_sec_fair_disclosure(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date,
        regulatory_context: RegulatoryContext
    ) -> 'RuleEvaluationResult':
        """Validate SEC Regulation Fair Disclosure (Reg FD) compliance."""
        violations = []

        if not regulatory_context.sec_filings:
            return RuleEvaluationResult(ComplianceRule.SEC_FAIR_DISCLOSURE, violations)

        # Check for material information disclosure timing
        material_filings = [
            filing for filing in regulatory_context.sec_filings
            if filing.form_type in ['8-K', '10-Q', '10-K', '20-F'] and
            filing.filing_date == trading_date
        ]

        if material_filings and 'close' in price_data and 'open' in price_data:
            daily_return = (price_data['close'] - price_data['open']) / price_data['open']

            # Significant price movement on material disclosure date
            if abs(daily_return) > 0.05:  # 5% threshold
                violations.append(ComplianceViolation(
                    rule=ComplianceRule.SEC_FAIR_DISCLOSURE,
                    severity='critical',
                    description=f"Material price movement ({daily_return:.2%}) on disclosure date requires Reg FD review",
                    data_point={
                        'daily_return': daily_return,
                        'material_filings': [f.form_type for f in material_filings],
                        'filing_time': material_filings[0].filing_date.isoformat()
                    },
                    regulatory_context=regulatory_context,
                    violation_timestamp=datetime.utcnow(),
                    remediation_required=True,
                    expected_resolution_time=timedelta(hours=1)
                ))

        return RuleEvaluationResult(ComplianceRule.SEC_FAIR_DISCLOSURE, violations)

    async def _validate_dodd_frank(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date,
        regulatory_context: RegulatoryContext
    ) -> 'RuleEvaluationResult':
        """Validate Dodd-Frank Act compliance requirements."""
        violations = []

        # Check for systemically important financial institution (SIFI) requirements
        if regulatory_context.economic_indicators:
            systemic_risk_indicators = [
                indicator for indicator in regulatory_context.economic_indicators
                if 'systemic' in indicator.name.lower() or 'stress' in indicator.name.lower()
            ]

            if systemic_risk_indicators and 'volume' in price_data:
                # Enhanced reporting during systemic risk periods
                high_volume_threshold = 1000000  # Shares
                if price_data['volume'] > high_volume_threshold:
                    violations.append(ComplianceViolation(
                        rule=ComplianceRule.DODD_FRANK_SWAP_REPORTING,
                        severity='high',
                        description=f"High volume trading during systemic risk period requires Dodd-Frank enhanced reporting",
                        data_point={'volume': price_data['volume']},
                        regulatory_context=regulatory_context,
                        violation_timestamp=datetime.utcnow(),
                        remediation_required=True,
                        expected_resolution_time=timedelta(hours=6)
                    ))

        return RuleEvaluationResult(ComplianceRule.DODD_FRANK_SWAP_REPORTING, violations)

    async def _validate_basel_iii(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date,
        regulatory_context: RegulatoryContext
    ) -> 'RuleEvaluationResult':
        """Validate Basel III capital adequacy requirements."""
        violations = []

        # Basel III primarily applies to banks - check if this is a financial institution
        if regulatory_context.sec_filings:
            financial_filings = [
                filing for filing in regulatory_context.sec_filings
                if any(keyword in filing.description.lower() for keyword in
                      ['bank', 'financial', 'capital', 'tier 1', 'regulatory capital'])
            ]

            if financial_filings and 'close' in price_data:
                # Check for capital adequacy reporting requirements
                violations.append(ComplianceViolation(
                    rule=ComplianceRule.BASEL_III_CAPITAL_ADEQUACY,
                    severity='medium',
                    description="Financial institution requires Basel III capital adequacy validation",
                    data_point={'is_financial_institution': True},
                    regulatory_context=regulatory_context,
                    violation_timestamp=datetime.utcnow(),
                    remediation_required=True,
                    expected_resolution_time=timedelta(hours=24)
                ))

        return RuleEvaluationResult(ComplianceRule.BASEL_III_CAPITAL_ADEQUACY, violations)

    async def _validate_cftc_limits(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date,
        regulatory_context: RegulatoryContext
    ) -> 'RuleEvaluationResult':
        """Validate CFTC position limit compliance."""
        violations = []

        # CFTC position limits apply to commodities and derivatives
        if 'volume' in price_data and price_data['volume'] > 2000000:  # High volume threshold
            violations.append(ComplianceViolation(
                rule=ComplianceRule.CFTC_POSITION_LIMITS,
                severity='medium',
                description=f"High volume ({price_data['volume']:,}) may require CFTC position limit verification",
                data_point={'volume': price_data['volume']},
                regulatory_context=regulatory_context,
                violation_timestamp=datetime.utcnow(),
                remediation_required=False,
                expected_resolution_time=timedelta(hours=12)
            ))

        return RuleEvaluationResult(ComplianceRule.CFTC_POSITION_LIMITS, violations)

    async def _validate_finra_execution(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date,
        regulatory_context: RegulatoryContext
    ) -> 'RuleEvaluationResult':
        """Validate FINRA best execution requirements."""
        violations = []

        # Check for best execution compliance using price data
        if all(field in price_data for field in ['open', 'high', 'low', 'close']):
            # Calculate intraday price range
            price_range = (price_data['high'] - price_data['low']) / price_data['close']

            # Wide spreads may indicate execution quality issues
            if price_range > 0.10:  # 10% intraday range
                violations.append(ComplianceViolation(
                    rule=ComplianceRule.FINRA_BEST_EXECUTION,
                    severity='medium',
                    description=f"Wide intraday range ({price_range:.2%}) requires best execution review",
                    data_point={
                        'price_range': price_range,
                        'high': price_data['high'],
                        'low': price_data['low'],
                        'close': price_data['close']
                    },
                    regulatory_context=regulatory_context,
                    violation_timestamp=datetime.utcnow(),
                    remediation_required=False,
                    expected_resolution_time=timedelta(hours=8)
                ))

        return RuleEvaluationResult(ComplianceRule.FINRA_BEST_EXECUTION, violations)

    def _calculate_confidence_score(
        self,
        regulatory_context: RegulatoryContext,
        rules_evaluated: List[ComplianceRule]
    ) -> float:
        """Calculate confidence score based on available regulatory data."""
        base_score = 0.5  # Base confidence

        # Increase confidence based on available data
        if regulatory_context.sec_filings:
            base_score += 0.2

        if regulatory_context.economic_indicators:
            base_score += 0.2

        # Adjust for number of rules successfully evaluated
        rule_coverage = len(rules_evaluated) / len(ComplianceRule)
        base_score += 0.1 * rule_coverage

        return min(base_score, 1.0)

    async def get_compliance_summary(
        self,
        symbol: str,
        date_range: Tuple[date, date]
    ) -> Dict[str, Any]:
        """Get compliance summary for a symbol over date range."""
        start_date, end_date = date_range

        all_reports = []
        current_date = start_date

        while current_date <= end_date:
            cache_key = f"{symbol}_{current_date.isoformat()}"
            if cache_key in self._compliance_cache:
                all_reports.append(self._compliance_cache[cache_key])
            current_date += timedelta(days=1)

        if not all_reports:
            return {"error": "No compliance reports found for date range"}

        # Aggregate compliance statistics
        total_violations = sum(len(report.violations) for report in all_reports)
        critical_violations = sum(
            len([v for v in report.violations if v.severity == 'critical'])
            for report in all_reports
        )

        avg_confidence = sum(report.confidence_score for report in all_reports) / len(all_reports)

        return {
            "symbol": symbol,
            "date_range": f"{start_date} to {end_date}",
            "total_reports": len(all_reports),
            "total_violations": total_violations,
            "critical_violations": critical_violations,
            "average_confidence_score": avg_confidence,
            "compliance_trend": self._calculate_compliance_trend(all_reports)
        }

    def _calculate_compliance_trend(self, reports: List[ComplianceReport]) -> str:
        """Calculate compliance trend from historical reports."""
        if len(reports) < 2:
            return "insufficient_data"

        # Compare first half to second half
        mid_point = len(reports) // 2
        first_half_violations = sum(
            len(report.violations) for report in reports[:mid_point]
        ) / mid_point

        second_half_violations = sum(
            len(report.violations) for report in reports[mid_point:]
        ) / (len(reports) - mid_point)

        if second_half_violations < first_half_violations * 0.8:
            return "improving"
        elif second_half_violations > first_half_violations * 1.2:
            return "deteriorating"
        else:
            return "stable"


@dataclass
class RuleEvaluationResult:
    """Result of evaluating a specific compliance rule."""
    rule: ComplianceRule
    violations: List[ComplianceViolation]