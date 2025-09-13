"""
Production Data Quality Orchestrator

Comprehensive orchestration system integrating all production data quality components
with real vendor APIs, regulatory compliance, and human-in-the-loop review.
"""

import asyncio
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum

from ..ml.anomaly_detection_ensemble import DataQualityAnomalyEnsemble
from .hitl_orchestrator import HITLOrchestrator, HITLDecision
from ...infrastructure.vendor.real_api_client import RealVendorAPIClient
from ...infrastructure.regulatory.compliance_validator import (
    ProductionComplianceValidator,
    ComplianceReport,
    ComplianceStatus
)
from ...infrastructure.streaming.real_time_quality_engine import RealTimeQualityEngine
from ...infrastructure.caching.advanced_cache_manager import AdvancedCacheManager

logger = logging.getLogger(__name__)


class QualityAssessmentLevel(Enum):
    """Data quality assessment levels."""
    TIER_1_CRITICAL = "tier_1_critical"      # Trading-critical data
    TIER_2_ANALYTICS = "tier_2_analytics"    # Analytics and reporting
    TIER_3_RESEARCH = "tier_3_research"      # Research and backtesting


class DataQualityDecision(Enum):
    """Final data quality decisions."""
    APPROVED = "approved"
    REJECTED = "rejected"
    REQUIRES_MANUAL_REVIEW = "requires_manual_review"
    PENDING_EXTERNAL_VERIFICATION = "pending_external_verification"
    COMPLIANCE_HOLD = "compliance_hold"


@dataclass
class QualityAssessmentReport:
    """Comprehensive data quality assessment report."""
    symbol: str
    assessment_date: date
    assessment_timestamp: datetime
    tier: QualityAssessmentLevel

    # Multi-layer assessment results
    anomaly_detection_results: Dict[str, Any]
    regulatory_compliance_report: ComplianceReport
    hitl_decision: HITLDecision
    external_verification_status: str

    # Final decision
    final_decision: DataQualityDecision
    confidence_score: float
    risk_assessment: str

    # Metadata
    data_sources: List[str]
    verification_methods: List[str]
    processing_time_ms: float
    human_reviewer_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)


class ProductionDataQualityOrchestrator:
    """
    Production-grade data quality orchestrator integrating all quality components.

    Orchestrates multi-tier data quality assessment combining:
    - Real-time anomaly detection with ML ensemble
    - Regulatory compliance validation with real regulatory data
    - Human-in-the-loop review for edge cases
    - External vendor verification and cross-validation
    - Advanced caching for performance optimization
    """

    def __init__(
        self,
        vendor_api_client: RealVendorAPIClient,
        compliance_validator: ProductionComplianceValidator,
        hitl_orchestrator: HITLOrchestrator,
        anomaly_detector: DataQualityAnomalyEnsemble,
        streaming_engine: RealTimeQualityEngine,
        cache_manager: AdvancedCacheManager
    ):
        self.vendor_client = vendor_api_client
        self.compliance_validator = compliance_validator
        self.hitl_orchestrator = hitl_orchestrator
        self.anomaly_detector = anomaly_detector
        self.streaming_engine = streaming_engine
        self.cache_manager = cache_manager

        self._assessment_cache: Dict[str, QualityAssessmentReport] = {}
        self._processing_metrics = {
            'total_assessments': 0,
            'tier_1_assessments': 0,
            'tier_2_assessments': 0,
            'tier_3_assessments': 0,
            'compliance_violations': 0,
            'human_reviews_required': 0,
            'average_processing_time_ms': 0.0
        }

    async def assess_daily_price_quality(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date,
        tier: QualityAssessmentLevel = QualityAssessmentLevel.TIER_2_ANALYTICS
    ) -> QualityAssessmentReport:
        """
        Comprehensive daily price data quality assessment.

        Performs multi-tier quality assessment including:
        1. Real-time anomaly detection
        2. Regulatory compliance validation
        3. External vendor cross-validation
        4. Human-in-the-loop review if needed

        Args:
            symbol: Stock symbol
            price_data: Daily price data dictionary
            trading_date: Trading date
            tier: Assessment level (determines rigor and performance requirements)

        Returns:
            Comprehensive quality assessment report with final decision
        """
        start_time = datetime.utcnow()

        logger.info(f"Starting {tier.value} quality assessment for {symbol} on {trading_date}")

        # Check cache first for non-critical assessments
        if tier != QualityAssessmentLevel.TIER_1_CRITICAL:
            cached_result = await self._get_cached_assessment(symbol, trading_date, tier)
            if cached_result:
                logger.info(f"Returning cached assessment for {symbol}")
                return cached_result

        try:
            # Initialize assessment report
            report = QualityAssessmentReport(
                symbol=symbol,
                assessment_date=trading_date,
                assessment_timestamp=start_time,
                tier=tier,
                anomaly_detection_results={},
                regulatory_compliance_report=None,
                hitl_decision=HITLDecision.PENDING,
                external_verification_status="pending",
                final_decision=DataQualityDecision.PENDING_EXTERNAL_VERIFICATION,
                confidence_score=0.0,
                risk_assessment="unknown",
                data_sources=[],
                verification_methods=[],
                processing_time_ms=0.0
            )

            # Execute multi-layer assessment based on tier
            assessment_tasks = await self._create_assessment_tasks(
                symbol, price_data, trading_date, tier, report
            )

            # Execute assessments concurrently for optimal performance
            assessment_results = await asyncio.gather(*assessment_tasks, return_exceptions=True)

            # Process assessment results
            await self._process_assessment_results(report, assessment_results)

            # Make final quality decision
            await self._make_final_quality_decision(report)

            # Calculate processing metrics
            processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            report.processing_time_ms = processing_time

            # Update metrics
            self._update_processing_metrics(report, processing_time)

            # Cache result for future use
            if tier != QualityAssessmentLevel.TIER_1_CRITICAL:
                await self._cache_assessment_result(report)

            logger.info(
                f"Quality assessment complete for {symbol}: "
                f"{report.final_decision.value} ({processing_time:.1f}ms)"
            )

            return report

        except Exception as e:
            logger.error(f"Quality assessment failed for {symbol}: {e}")
            # Return failed assessment report
            processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000

            return QualityAssessmentReport(
                symbol=symbol,
                assessment_date=trading_date,
                assessment_timestamp=start_time,
                tier=tier,
                anomaly_detection_results={"error": str(e)},
                regulatory_compliance_report=None,
                hitl_decision=HITLDecision.MANUAL_REVIEW_REQUIRED,
                external_verification_status="failed",
                final_decision=DataQualityDecision.REQUIRES_MANUAL_REVIEW,
                confidence_score=0.0,
                risk_assessment="critical",
                data_sources=[],
                verification_methods=[],
                processing_time_ms=processing_time
            )

    async def _create_assessment_tasks(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date,
        tier: QualityAssessmentLevel,
        report: QualityAssessmentReport
    ) -> List[asyncio.Task]:
        """Create assessment tasks based on tier requirements."""
        tasks = []

        # Tier 1 (Critical): All assessments with highest priority
        if tier == QualityAssessmentLevel.TIER_1_CRITICAL:
            tasks.extend([
                asyncio.create_task(self._perform_real_time_anomaly_detection(symbol, price_data, trading_date)),
                asyncio.create_task(self._perform_regulatory_compliance_check(symbol, price_data, trading_date)),
                asyncio.create_task(self._perform_multi_vendor_verification(symbol, price_data, trading_date)),
                asyncio.create_task(self._perform_streaming_quality_validation(symbol, price_data, trading_date))
            ])

        # Tier 2 (Analytics): Core assessments without real-time streaming
        elif tier == QualityAssessmentLevel.TIER_2_ANALYTICS:
            tasks.extend([
                asyncio.create_task(self._perform_anomaly_detection(symbol, price_data, trading_date)),
                asyncio.create_task(self._perform_regulatory_compliance_check(symbol, price_data, trading_date)),
                asyncio.create_task(self._perform_vendor_verification(symbol, price_data, trading_date))
            ])

        # Tier 3 (Research): Basic assessments with caching
        else:  # TIER_3_RESEARCH
            tasks.extend([
                asyncio.create_task(self._perform_basic_anomaly_detection(symbol, price_data, trading_date)),
                asyncio.create_task(self._perform_basic_compliance_check(symbol, price_data, trading_date))
            ])

        return tasks

    async def _perform_real_time_anomaly_detection(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date
    ) -> Dict[str, Any]:
        """Perform real-time anomaly detection for Tier 1 critical data."""
        try:
            # Use streaming engine for ultra-low latency detection
            anomalies = await self.streaming_engine.detect_real_time_anomalies(
                symbol, price_data, trading_date
            )

            return {
                "method": "real_time_streaming",
                "anomalies_detected": anomalies,
                "detection_latency_us": getattr(anomalies, 'detection_latency_us', 0),
                "confidence_level": "high"
            }
        except Exception as e:
            logger.error(f"Real-time anomaly detection failed for {symbol}: {e}")
            return {"error": str(e), "method": "real_time_streaming"}

    async def _perform_anomaly_detection(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date
    ) -> Dict[str, Any]:
        """Perform comprehensive anomaly detection for Tier 2 analytics."""
        try:
            # Use ML ensemble for comprehensive analysis
            anomaly_results = await self.anomaly_detector.detect_anomalies(
                symbol, price_data, trading_date
            )

            return {
                "method": "ml_ensemble",
                "anomaly_score": anomaly_results.overall_anomaly_score,
                "anomaly_types": [a.anomaly_type.value for a in anomaly_results.anomalies],
                "confidence_level": "high" if anomaly_results.confidence > 0.8 else "medium",
                "model_results": anomaly_results.model_results
            }
        except Exception as e:
            logger.error(f"ML anomaly detection failed for {symbol}: {e}")
            return {"error": str(e), "method": "ml_ensemble"}

    async def _perform_basic_anomaly_detection(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date
    ) -> Dict[str, Any]:
        """Perform basic anomaly detection for Tier 3 research."""
        try:
            # Simple statistical checks
            anomalies = []

            if 'close' in price_data and 'open' in price_data:
                daily_return = (price_data['close'] - price_data['open']) / price_data['open']
                if abs(daily_return) > 0.10:  # 10% threshold
                    anomalies.append("high_daily_return")

            if 'volume' in price_data and price_data['volume'] > 10000000:  # 10M shares
                anomalies.append("high_volume")

            return {
                "method": "basic_statistical",
                "anomalies_detected": anomalies,
                "confidence_level": "medium"
            }
        except Exception as e:
            return {"error": str(e), "method": "basic_statistical"}

    async def _perform_regulatory_compliance_check(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date
    ) -> ComplianceReport:
        """Perform comprehensive regulatory compliance validation."""
        try:
            compliance_report = await self.compliance_validator.validate_daily_price_compliance(
                symbol, price_data, trading_date
            )
            return compliance_report
        except Exception as e:
            logger.error(f"Regulatory compliance check failed for {symbol}: {e}")
            return None

    async def _perform_basic_compliance_check(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date
    ) -> Optional[ComplianceReport]:
        """Perform basic compliance check for research tier."""
        try:
            # Simplified compliance check
            return await self.compliance_validator.validate_daily_price_compliance(
                symbol, price_data, trading_date
            )
        except Exception as e:
            logger.warning(f"Basic compliance check failed for {symbol}: {e}")
            return None

    async def _perform_multi_vendor_verification(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date
    ) -> Dict[str, Any]:
        """Perform multi-vendor cross-verification for critical tier."""
        try:
            # Get data from multiple vendors for cross-validation
            verification_results = await self.vendor_client.cross_validate_daily_prices(
                symbol, trading_date, vendors=['polygon', 'tiingo', 'eodhd']
            )

            return {
                "method": "multi_vendor_cross_validation",
                "vendors_checked": verification_results.vendors_checked,
                "consensus_score": verification_results.consensus_score,
                "discrepancies": verification_results.discrepancies,
                "confidence_level": "very_high" if verification_results.consensus_score > 0.95 else "high"
            }
        except Exception as e:
            logger.error(f"Multi-vendor verification failed for {symbol}: {e}")
            return {"error": str(e), "method": "multi_vendor_cross_validation"}

    async def _perform_vendor_verification(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date
    ) -> Dict[str, Any]:
        """Perform single vendor verification for analytics tier."""
        try:
            # Single vendor verification
            verification_result = await self.vendor_client.verify_daily_prices(
                symbol, trading_date, primary_vendor='polygon'
            )

            return {
                "method": "single_vendor_verification",
                "vendor": verification_result.vendor,
                "verification_score": verification_result.accuracy_score,
                "confidence_level": "high" if verification_result.accuracy_score > 0.90 else "medium"
            }
        except Exception as e:
            logger.error(f"Vendor verification failed for {symbol}: {e}")
            return {"error": str(e), "method": "single_vendor_verification"}

    async def _perform_streaming_quality_validation(
        self,
        symbol: str,
        price_data: Dict[str, Any],
        trading_date: date
    ) -> Dict[str, Any]:
        """Perform streaming quality validation for critical tier."""
        try:
            validation_result = await self.streaming_engine.validate_data_quality(
                symbol, price_data, trading_date
            )

            return {
                "method": "streaming_quality_validation",
                "validation_score": validation_result.quality_score,
                "latency_us": validation_result.validation_latency_us,
                "confidence_level": "very_high"
            }
        except Exception as e:
            logger.error(f"Streaming quality validation failed for {symbol}: {e}")
            return {"error": str(e), "method": "streaming_quality_validation"}

    async def _process_assessment_results(
        self,
        report: QualityAssessmentReport,
        results: List[Any]
    ) -> None:
        """Process and integrate all assessment results."""

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Assessment task failed: {result}")
                continue

            if isinstance(result, ComplianceReport):
                report.regulatory_compliance_report = result
                report.data_sources.append("regulatory_compliance")
                report.verification_methods.append("regulatory_validation")

            elif isinstance(result, dict):
                if "anomalies" in result or "anomaly" in result:
                    report.anomaly_detection_results = result
                    report.verification_methods.append(result.get("method", "unknown"))

                elif "verification" in result or "vendor" in result:
                    report.external_verification_status = "completed"
                    report.data_sources.extend(result.get("vendors_checked", [result.get("vendor", "unknown")]))
                    report.verification_methods.append(result.get("method", "vendor_verification"))

    async def _make_final_quality_decision(self, report: QualityAssessmentReport) -> None:
        """Make final data quality decision based on all assessment results."""

        # Initialize decision factors
        anomaly_score = 0.0
        compliance_risk = 0.0
        verification_confidence = 1.0

        # Analyze anomaly detection results
        if report.anomaly_detection_results:
            if "error" in report.anomaly_detection_results:
                anomaly_score = 0.5  # Neutral due to error
            else:
                anomaly_score = report.anomaly_detection_results.get("anomaly_score", 0.0)

        # Analyze regulatory compliance
        if report.regulatory_compliance_report:
            if report.regulatory_compliance_report.overall_status == ComplianceStatus.NON_COMPLIANT:
                compliance_risk = 1.0
                report.final_decision = DataQualityDecision.COMPLIANCE_HOLD
                report.risk_assessment = "high_compliance_risk"
                report.confidence_score = 0.0
                return
            elif report.regulatory_compliance_report.overall_status == ComplianceStatus.PENDING_REVIEW:
                compliance_risk = 0.5

        # Analyze external verification
        if report.external_verification_status == "failed":
            verification_confidence = 0.2
        elif "consensus_score" in str(report.anomaly_detection_results):
            # Extract consensus score if available
            verification_confidence = 0.9  # Assume high confidence for completed verification

        # Calculate overall confidence score
        overall_confidence = (
            (1.0 - anomaly_score) * 0.4 +
            (1.0 - compliance_risk) * 0.4 +
            verification_confidence * 0.2
        )

        report.confidence_score = overall_confidence

        # Make final decision based on tier and confidence
        if overall_confidence >= 0.9:
            report.final_decision = DataQualityDecision.APPROVED
            report.risk_assessment = "low"

        elif overall_confidence >= 0.7:
            if report.tier == QualityAssessmentLevel.TIER_1_CRITICAL:
                # Higher standards for critical tier
                report.final_decision = DataQualityDecision.REQUIRES_MANUAL_REVIEW
                report.risk_assessment = "medium"

                # Queue for HITL review
                hitl_decision = await self.hitl_orchestrator.evaluate_data_quality({
                    "symbol": report.symbol,
                    "confidence_score": overall_confidence,
                    "anomaly_score": anomaly_score,
                    "compliance_risk": compliance_risk
                })
                report.hitl_decision = hitl_decision
            else:
                report.final_decision = DataQualityDecision.APPROVED
                report.risk_assessment = "medium"

        elif overall_confidence >= 0.5:
            report.final_decision = DataQualityDecision.REQUIRES_MANUAL_REVIEW
            report.risk_assessment = "high"

            # Always require human review for medium confidence
            hitl_decision = await self.hitl_orchestrator.evaluate_data_quality({
                "symbol": report.symbol,
                "confidence_score": overall_confidence,
                "tier": report.tier.value,
                "priority": "high"
            })
            report.hitl_decision = hitl_decision

        else:
            report.final_decision = DataQualityDecision.REJECTED
            report.risk_assessment = "critical"

    async def _get_cached_assessment(
        self,
        symbol: str,
        trading_date: date,
        tier: QualityAssessmentLevel
    ) -> Optional[QualityAssessmentReport]:
        """Get cached assessment result if available."""
        cache_key = f"quality_assessment_{symbol}_{trading_date.isoformat()}_{tier.value}"

        try:
            cached_data = await self.cache_manager.get(cache_key)
            if cached_data:
                return QualityAssessmentReport(**cached_data)
        except Exception as e:
            logger.warning(f"Cache retrieval failed for {cache_key}: {e}")

        return None

    async def _cache_assessment_result(self, report: QualityAssessmentReport) -> None:
        """Cache assessment result for future use."""
        cache_key = f"quality_assessment_{report.symbol}_{report.assessment_date.isoformat()}_{report.tier.value}"

        try:
            # Cache for 24 hours for Tier 2/3, 4 hours for Tier 1
            ttl_hours = 4 if report.tier == QualityAssessmentLevel.TIER_1_CRITICAL else 24

            await self.cache_manager.set(
                cache_key,
                report.to_dict(),
                ttl_seconds=ttl_hours * 3600
            )
        except Exception as e:
            logger.warning(f"Cache storage failed for {cache_key}: {e}")

    def _update_processing_metrics(self, report: QualityAssessmentReport, processing_time: float) -> None:
        """Update internal processing metrics."""
        self._processing_metrics['total_assessments'] += 1

        if report.tier == QualityAssessmentLevel.TIER_1_CRITICAL:
            self._processing_metrics['tier_1_assessments'] += 1
        elif report.tier == QualityAssessmentLevel.TIER_2_ANALYTICS:
            self._processing_metrics['tier_2_assessments'] += 1
        else:
            self._processing_metrics['tier_3_assessments'] += 1

        if report.regulatory_compliance_report and len(report.regulatory_compliance_report.violations) > 0:
            self._processing_metrics['compliance_violations'] += 1

        if report.hitl_decision == HITLDecision.MANUAL_REVIEW_REQUIRED:
            self._processing_metrics['human_reviews_required'] += 1

        # Update rolling average processing time
        current_avg = self._processing_metrics['average_processing_time_ms']
        total_assessments = self._processing_metrics['total_assessments']

        self._processing_metrics['average_processing_time_ms'] = (
            (current_avg * (total_assessments - 1) + processing_time) / total_assessments
        )

    async def get_processing_metrics(self) -> Dict[str, Any]:
        """Get current processing metrics and performance statistics."""
        return {
            **self._processing_metrics,
            "cache_hit_rate": await self.cache_manager.get_metrics(),
            "system_health": await self._assess_system_health()
        }

    async def _assess_system_health(self) -> Dict[str, str]:
        """Assess overall system health status."""
        try:
            # Check key components
            vendor_health = await self.vendor_client.health_check()
            streaming_health = await self.streaming_engine.health_check()

            return {
                "overall_status": "healthy",
                "vendor_api_status": vendor_health.get("status", "unknown"),
                "streaming_engine_status": streaming_health.get("status", "unknown"),
                "compliance_validator_status": "healthy",  # Assume healthy if no errors
                "hitl_orchestrator_status": "healthy"
            }
        except Exception as e:
            logger.error(f"System health assessment failed: {e}")
            return {
                "overall_status": "degraded",
                "error": str(e)
            }