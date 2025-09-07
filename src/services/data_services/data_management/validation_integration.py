#!/usr/bin/env python3
"""
Validation Integration for Frontfill System.
Integrates daily prices validation into the automated frontfill workflow.
"""

import asyncpg
import logging
from datetime import datetime, date, timedelta
from typing import Dict, Any, List
from dataclasses import dataclass

from core.platform.config.environment import Environment
from validation.daily_prices_validator import DailyPricesValidator
from validation.missing_data_handler import MissingDataHandler
from frontfill.checkpoint_manager import CheckpointManager

logger = logging.getLogger(__name__)


@dataclass
class ValidationConfig:
    """Configuration for validation integration."""
    enable_post_frontfill_validation: bool = True
    enable_missing_data_detection: bool = True
    enable_automatic_backfill: bool = True
    quality_threshold: float = 80.0  # Minimum quality score to pass
    backfill_priority_threshold: int = 3  # Only backfill priority 1-3 gaps
    max_concurrent_backfills: int = 5
    validation_delay_hours: int = 2  # Hours to wait after frontfill before validation


class ValidationIntegration:
    """Integrates validation into frontfill workflow."""

    def __init__(self, connection_pool: asyncpg.Pool, env: Environment,
                 api_keys: Dict[str, str], config: ValidationConfig = None):
        self.pool = connection_pool
        self.env = env
        self.api_keys = api_keys
        self.config = config or ValidationConfig()

        # Initialize components
        self.validator = DailyPricesValidator(connection_pool, env)
        self.missing_data_handler = MissingDataHandler(connection_pool, env, api_keys)
        self.checkpoint_manager = CheckpointManager(connection_pool, env)

    async def initialize(self):
        """Initialize validation integration."""
        await self.validator.initialize()
        logger.info("Validation integration initialized")

    async def run_post_frontfill_validation(self, validation_date: date,
                                          vendors: List[str] = None) -> Dict[str, Any]:
        """
        Run comprehensive validation after frontfill operations.

        Args:
            validation_date: Date to validate
            vendors: Vendors to validate (None = all)

        Returns:
            Validation results with quality score and actions taken
        """
        logger.info(f"Running post-frontfill validation for {validation_date}")

        validation_results = {
            "date": validation_date,
            "validation_passed": False,
            "quality_score": 0.0,
            "actions_taken": [],
            "backfill_results": None,
            "validation_time": datetime.now()
        }

        try:
            # Step 1: Run comprehensive validation
            results = await self.validator.validate_daily_prices(validation_date, vendors)
            validation_results.update(results)

            quality_score = results["data_quality_score"]
            validation_results["quality_score"] = quality_score

            logger.info(f"Validation completed: quality score {quality_score:.2f}")

            # Step 2: Check if quality meets threshold
            if quality_score >= self.config.quality_threshold:
                validation_results["validation_passed"] = True
                validation_results["actions_taken"].append("validation_passed")
                logger.info(f"Quality score {quality_score:.2f} meets threshold {self.config.quality_threshold}")
            else:
                logger.warning(f"Quality score {quality_score:.2f} below threshold {self.config.quality_threshold}")
                validation_results["actions_taken"].append("quality_threshold_failed")

                # Step 3: Detect and potentially backfill missing data
                if self.config.enable_missing_data_detection:
                    await self._handle_quality_issues(validation_date, validation_results)

            # Step 4: Store validation checkpoint
            await self._store_validation_checkpoint(validation_date, validation_results)

        except Exception as e:
            logger.error(f"Error in post-frontfill validation: {e}")
            validation_results["actions_taken"].append(f"validation_error: {e}")

        return validation_results

    async def _handle_quality_issues(self, validation_date: date,
                                   validation_results: Dict[str, Any]):
        """Handle quality issues through missing data detection and backfill."""
        logger.info("Handling quality issues through missing data detection")

        try:
            # Detect missing data gaps for the validation date
            gaps = await self.missing_data_handler.detect_missing_data(
                validation_date, validation_date
            )

            if gaps:
                logger.info(f"Found {len(gaps)} data gaps on {validation_date}")
                validation_results["actions_taken"].append(f"detected_{len(gaps)}_gaps")

                # Filter critical gaps
                critical_gaps = [g for g in gaps if g.backfill_priority <= self.config.backfill_priority_threshold]

                if critical_gaps and self.config.enable_automatic_backfill:
                    logger.info(f"Attempting to backfill {len(critical_gaps)} critical gaps")

                    # Run backfill operation
                    backfill_results = await self.missing_data_handler.backfill_missing_data(
                        critical_gaps,
                        self.config.max_concurrent_backfills,
                        self.config.backfill_priority_threshold
                    )

                    validation_results["backfill_results"] = {
                        "total_gaps": len(gaps),
                        "critical_gaps": len(critical_gaps),
                        "backfill_attempts": len(backfill_results),
                        "successful_backfills": sum(1 for r in backfill_results if r.success),
                        "records_added": sum(r.records_added for r in backfill_results)
                    }

                    validation_results["actions_taken"].append(
                        f"backfilled_{validation_results['backfill_results']['successful_backfills']}_gaps"
                    )

                    # Re-run validation after backfill
                    if validation_results["backfill_results"]["successful_backfills"] > 0:
                        logger.info("Re-running validation after backfill")
                        updated_results = await self.validator.validate_daily_prices(validation_date)
                        validation_results["quality_score"] = updated_results["data_quality_score"]
                        validation_results["actions_taken"].append("re_validated_after_backfill")

                        if updated_results["data_quality_score"] >= self.config.quality_threshold:
                            validation_results["validation_passed"] = True
                            validation_results["actions_taken"].append("quality_improved_after_backfill")
            else:
                validation_results["actions_taken"].append("no_missing_data_gaps_found")

        except Exception as e:
            logger.error(f"Error handling quality issues: {e}")
            validation_results["actions_taken"].append(f"quality_handling_error: {e}")

    async def _store_validation_checkpoint(self, validation_date: date,
                                         validation_results: Dict[str, Any]):
        """Store validation checkpoint for tracking."""
        checkpoint_data = {
            "validation_date": validation_date.isoformat(),
            "quality_score": validation_results["quality_score"],
            "validation_passed": validation_results["validation_passed"],
            "actions_taken": validation_results["actions_taken"],
            "validation_time": validation_results["validation_time"].isoformat()
        }

        if validation_results.get("backfill_results"):
            checkpoint_data["backfill_results"] = validation_results["backfill_results"]

        await self.checkpoint_manager.update_checkpoint(
            "post_frontfill_validation",
            validation_date.isoformat(),
            checkpoint_data
        )

    async def run_missing_data_analysis(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """
        Run comprehensive missing data analysis for a date range.

        Args:
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            Missing data analysis report
        """
        logger.info(f"Running missing data analysis from {start_date} to {end_date}")

        try:
            # Generate comprehensive missing data report
            report = await self.missing_data_handler.generate_missing_data_report(
                start_date, end_date
            )

            # Add recommendations
            report["recommendations"] = self._generate_recommendations(report)

            return report

        except Exception as e:
            logger.error(f"Error in missing data analysis: {e}")
            return {
                "error": str(e),
                "period": {"start": start_date, "end": end_date}
            }

    def _generate_recommendations(self, report: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on missing data analysis."""
        recommendations = []

        summary = report.get("summary", {})
        total_gaps = summary.get("total_gaps", 0)
        total_missing_days = summary.get("total_missing_days", 0)

        if total_gaps == 0:
            recommendations.append("✅ No data gaps detected - data quality is excellent")
        else:
            if total_gaps > 100:
                recommendations.append("🔴 High number of data gaps detected - consider increasing frontfill frequency")
            elif total_gaps > 50:
                recommendations.append("🟡 Moderate number of data gaps - monitor data sources")
            else:
                recommendations.append("🟢 Low number of data gaps - within acceptable range")

            if total_missing_days > 500:
                recommendations.append("📈 High volume of missing data - run bulk backfill operation")

            # Vendor-specific recommendations
            by_vendor = report.get("by_vendor", {})
            for vendor, vendor_stats in by_vendor.items():
                vendor_gaps = vendor_stats.get("gap_count", 0)
                if vendor_gaps > total_gaps * 0.7:  # 70% of gaps from one vendor
                    recommendations.append(f"⚠️ {vendor} has {vendor_gaps} gaps ({vendor_gaps/total_gaps*100:.1f}%) - check API connectivity")

            # Critical gaps
            critical_gaps = report.get("critical_gaps", [])
            if critical_gaps:
                recommendations.append(f"🚨 {len(critical_gaps)} critical gaps requiring immediate attention")

        return recommendations

    async def run_scheduled_validation(self, max_days_back: int = 7) -> Dict[str, Any]:
        """
        Run scheduled validation for recent days.

        Args:
            max_days_back: Maximum days to look back for validation

        Returns:
            Validation summary for all checked dates
        """
        logger.info(f"Running scheduled validation for last {max_days_back} days")

        results = {
            "validation_dates": [],
            "total_validated": 0,
            "passed_validation": 0,
            "failed_validation": 0,
            "avg_quality_score": 0.0,
            "actions_summary": {}
        }

        end_date = date.today() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=max_days_back)

        current_date = start_date
        quality_scores = []

        while current_date <= end_date:
            # Skip weekends
            if current_date.weekday() < 5:
                try:
                    validation_result = await self.run_post_frontfill_validation(current_date)

                    results["validation_dates"].append({
                        "date": current_date.isoformat(),
                        "quality_score": validation_result["quality_score"],
                        "passed": validation_result["validation_passed"],
                        "actions": validation_result["actions_taken"]
                    })

                    results["total_validated"] += 1
                    quality_scores.append(validation_result["quality_score"])

                    if validation_result["validation_passed"]:
                        results["passed_validation"] += 1
                    else:
                        results["failed_validation"] += 1

                    # Aggregate actions
                    for action in validation_result["actions_taken"]:
                        results["actions_summary"][action] = results["actions_summary"].get(action, 0) + 1

                except Exception as e:
                    logger.error(f"Error validating {current_date}: {e}")

            current_date += timedelta(days=1)

        if quality_scores:
            results["avg_quality_score"] = sum(quality_scores) / len(quality_scores)

        logger.info(f"Scheduled validation completed: {results['passed_validation']}/{results['total_validated']} passed")

        return results


# Convenience function for running validation integration
async def run_validation_integration(connection_pool: asyncpg.Pool, env: Environment,
                                   api_keys: Dict[str, str], validation_date: date = None,
                                   config: ValidationConfig = None) -> Dict[str, Any]:
    """
    Convenience function to run validation integration.

    Args:
        connection_pool: Database connection pool
        env: Environment configuration
        api_keys: API keys for data vendors
        validation_date: Date to validate (None = yesterday)
        config: Validation configuration

    Returns:
        Validation results
    """
    if validation_date is None:
        validation_date = date.today() - timedelta(days=1)

    integration = ValidationIntegration(connection_pool, env, api_keys, config)
    await integration.initialize()

    return await integration.run_post_frontfill_validation(validation_date)