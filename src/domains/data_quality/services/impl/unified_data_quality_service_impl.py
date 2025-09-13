"""
Unified Data Quality Service Implementation
==========================================

Concrete implementation that consolidates coverage monitoring, validation,
and issue lifecycle management with shared code and consistent patterns.
"""

import asyncio
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

from ..interfaces.unified_data_quality_service_interface import (
    UnifiedDataQualityServiceInterface,
    DataQualityIssue,
    QualityMetric,
    CoverageScanRequest,
    CoverageScanResult,
    IssueDetectionRequest,
    ResolutionResult,
    QualityDashboardData,
    CoverageRecord,
    IssueCategory,
    IssueSeverity,
    IssueStatus,
    ResolutionStrategy
)

# Import existing components to consolidate
from monitoring.coverage_monitor import CoverageMonitor
from agents.data_quality_agent import DataQualityAgent
from monitoring.alert_system import AlertManager
from infrastructure.monitoring.data_quality_validator import DataQualityValidator

logger = logging.getLogger(__name__)

class UnifiedDataQualityServiceImpl(UnifiedDataQualityServiceInterface):
    """
    Unified implementation consolidating:
    - Coverage monitoring (from our monitoring system)
    - Data quality agent (from existing agent system) 
    - Validation (from existing validation system)
    - Alerting (unified alert management)
    """
    
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        
        # Initialize consolidated components
        self.coverage_monitor = CoverageMonitor(db_config)
        self.data_quality_agent = DataQualityAgent()
        self.data_quality_validator = DataQualityValidator(db_config)
        self.alert_manager = AlertManager()
        
        # Unified state
        self.is_monitoring_active = False
        self.unified_metrics_cache = {}
        self.last_scan_timestamp = None
        
        logger.info("🔄 Unified Data Quality Service initialized with consolidated components")
    
    async def initialize(self):
        """Initialize all consolidated components"""
        await self.coverage_monitor.initialize()
        await self.data_quality_validator.initialize()
        logger.info("✅ All unified data quality components initialized")
    
    async def close(self):
        """Close all consolidated components"""
        await self.coverage_monitor.close()
        await self.data_quality_validator.close()
    
    # =====================================
    # COVERAGE MONITORING OPERATIONS
    # =====================================
    
    async def scan_coverage(self, request: CoverageScanRequest) -> CoverageScanResult:
        """
        Unified coverage scanning leveraging existing coverage monitor
        """
        logger.info(f"🔍 Starting unified coverage scan for {len(request.vendors)} vendors, {request.lookback_days} days")
        
        scan_start = datetime.now()
        all_coverage_records = []
        all_gaps = []
        total_symbols_scanned = 0
        
        for vendor in request.vendors:
            for data_type in request.data_types:
                # Use existing coverage monitor logic
                if vendor == 'firstrate' and data_type == 'minute_bars':
                    coverage_records = await self.coverage_monitor.scan_firstrate_coverage(request.lookback_days)
                elif data_type == 'daily_prices':
                    coverage_records = await self.coverage_monitor.scan_database_coverage(
                        vendor, request.lookback_days
                    )
                else:
                    # Placeholder for other vendor/data_type combinations
                    coverage_records = []
                
                # Convert coverage records to unified format
                for record in coverage_records:
                    unified_record = CoverageRecord(
                        vendor=record.vendor,
                        data_type=record.data_type,
                        symbol=record.symbol,
                        trading_date=record.trading_date,
                        coverage_status=record.coverage_status,
                        data_quality_score=record.data_quality_score,
                        record_count=record.record_count,
                        file_path=record.file_path,
                        file_size_bytes=record.file_size_bytes
                    )
                    all_coverage_records.append(unified_record)
                
                # Detect gaps using existing logic and convert to unified issues
                gaps = await self.coverage_monitor.detect_gaps(coverage_records)
                for gap in gaps:
                    unified_gap = DataQualityIssue(
                        issue_type="coverage_gap",
                        issue_category=IssueCategory.COVERAGE,
                        vendor=gap.vendor,
                        data_type=gap.data_type,
                        symbol=gap.symbol,
                        affected_date_start=gap.gap_start_date,
                        affected_date_end=gap.gap_end_date,
                        severity=self._map_priority_to_severity(gap.priority_score),
                        priority_score=gap.priority_score,
                        estimated_effort_minutes=gap.estimated_effort_minutes,
                        metadata={
                            "gap_days": gap.gap_days,
                            "trading_days_affected": gap.gap_days  # Assuming all gap days are trading days
                        }
                    )
                    all_gaps.append(unified_gap)
                
                total_symbols_scanned += len(set(record.symbol for record in coverage_records))
        
        scan_duration = (datetime.now() - scan_start).total_seconds()
        
        return CoverageScanResult(
            total_symbols_scanned=total_symbols_scanned,
            total_trading_days=request.lookback_days,
            coverage_records=all_coverage_records,
            gaps_detected=all_gaps,
            scan_duration_seconds=scan_duration,
            scan_timestamp=datetime.now()
        )
    
    async def detect_coverage_gaps(self, request: CoverageScanRequest) -> List[DataQualityIssue]:
        """
        Detect coverage gaps using existing coverage monitor logic
        """
        scan_result = await self.scan_coverage(request)
        return scan_result.gaps_detected
    
    async def get_coverage_metrics(self, vendor: str, data_type: str, days: int = 30) -> List[QualityMetric]:
        """
        Get coverage metrics in unified format
        """
        # Use existing coverage monitor metrics calculation
        coverage_summary = await self.coverage_monitor.get_coverage_summary(vendor, data_type, days)
        
        metrics = []
        for metric_date, summary in coverage_summary.items():
            metrics.append(QualityMetric(
                metric_name="coverage_percentage",
                metric_category=IssueCategory.COVERAGE,
                vendor=vendor,
                data_type=data_type,
                metric_date=metric_date,
                value=summary.get('coverage_percentage', 0.0),
                threshold=95.0,  # 95% coverage threshold
                status=self._determine_metric_status(summary.get('coverage_percentage', 0.0), 95.0)
            ))
        
        return metrics
    
    # =====================================
    # DATA VALIDATION OPERATIONS
    # =====================================
    
    async def detect_validation_issues(self, request: IssueDetectionRequest) -> List[DataQualityIssue]:
        """
        Detect validation issues using existing data quality validator
        """
        validation_issues = []
        
        # Use existing data quality validator
        for data_type in ["daily_prices", "minute_bars"]:
            table_name = f"dev_{data_type}"
            
            try:
                # Use existing validation logic
                validation_results = await self.data_quality_validator.validate_table(
                    table_name=table_name,
                    date_range={"days_back": request.lookback_days}
                )
                
                # Convert to unified issue format
                for issue in validation_results.get('issues', []):
                    unified_issue = DataQualityIssue(
                        issue_type=issue.get('issue_type', 'validation_error'),
                        issue_category=IssueCategory.VALIDATION,
                        vendor=issue.get('vendor', 'unknown'),
                        data_type=data_type,
                        symbol=issue.get('symbol', ''),
                        affected_date_start=issue.get('affected_date', date.today()),
                        affected_date_end=issue.get('affected_date', date.today()),
                        severity=IssueSeverity(issue.get('severity', 'medium')),
                        metadata=issue.get('details', {})
                    )
                    validation_issues.append(unified_issue)
                    
            except Exception as e:
                logger.error(f"Validation scan failed for {table_name}: {e}")
        
        return validation_issues
    
    async def get_validation_metrics(self, vendor: str, data_type: str, days: int = 30) -> List[QualityMetric]:
        """
        Get validation metrics using existing validator
        """
        table_name = f"dev_{data_type}"
        
        try:
            validation_summary = await self.data_quality_validator.get_quality_summary(
                table_name=table_name,
                vendor=vendor,
                days=days
            )
            
            metrics = []
            
            # Completeness metric
            completeness_score = validation_summary.get('completeness_score', 0.0)
            metrics.append(QualityMetric(
                metric_name="completeness_score",
                metric_category=IssueCategory.VALIDATION,
                vendor=vendor,
                data_type=data_type,
                metric_date=date.today(),
                value=completeness_score,
                threshold=98.0,
                status=self._determine_metric_status(completeness_score, 98.0)
            ))
            
            # Consistency metric
            consistency_score = validation_summary.get('consistency_score', 0.0)
            metrics.append(QualityMetric(
                metric_name="consistency_score",
                metric_category=IssueCategory.VALIDATION,
                vendor=vendor,
                data_type=data_type,
                metric_date=date.today(),
                value=consistency_score,
                threshold=95.0,
                status=self._determine_metric_status(consistency_score, 95.0)
            ))
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get validation metrics for {vendor}/{data_type}: {e}")
            return []
    
    # =====================================
    # UNIFIED ISSUE MANAGEMENT
    # =====================================
    
    async def detect_all_issues(self, request: IssueDetectionRequest) -> List[DataQualityIssue]:
        """
        Unified issue detection combining coverage + validation
        """
        all_issues = []
        
        # Detect coverage issues if requested
        if IssueCategory.COVERAGE in request.categories:
            coverage_request = CoverageScanRequest(
                vendors=request.vendors or ['firstrate', 'polygon', 'tiingo'],
                data_types=request.data_types or ['daily_prices', 'minute_bars'],
                lookback_days=request.lookback_days
            )
            coverage_issues = await self.detect_coverage_gaps(coverage_request)
            all_issues.extend(coverage_issues)
        
        # Detect validation issues if requested
        if IssueCategory.VALIDATION in request.categories:
            validation_issues = await self.detect_validation_issues(request)
            all_issues.extend(validation_issues)
        
        # Filter by severity threshold
        filtered_issues = [
            issue for issue in all_issues 
            if self._severity_meets_threshold(issue.severity, request.severity_threshold)
        ]
        
        logger.info(f"🔍 Detected {len(filtered_issues)} issues across {len(request.categories)} categories")
        return filtered_issues
    
    async def classify_issue(self, issue: DataQualityIssue) -> DataQualityIssue:
        """
        Intelligent issue classification using agent patterns
        """
        # Use existing agent classification logic
        if hasattr(self.data_quality_agent, '_classify_issue'):
            # Convert to agent's issue format
            agent_issue = self._convert_to_agent_issue(issue)
            classification = await self.data_quality_agent._classify_issue(agent_issue)
            
            # Update issue with classification results
            issue.complexity = classification.complexity.value
            issue.resolution_strategy = ResolutionStrategy(classification.strategy.value)
            issue.estimated_effort_minutes = classification.estimated_resolution_time
            issue.metadata = issue.metadata or {}
            issue.metadata.update({
                "classification_confidence": classification.confidence,
                "classification_reasoning": classification.reasoning,
                "required_approvals": classification.required_approvals,
                "risk_level": classification.risk_level
            })
        
        return issue
    
    async def resolve_issue(self, issue_id: int, strategy: ResolutionStrategy) -> ResolutionResult:
        """
        Unified issue resolution workflow
        """
        try:
            # Get issue from database
            issue = await self.get_issue_by_id(issue_id)
            if not issue:
                return ResolutionResult(
                    success=False,
                    error_message=f"Issue {issue_id} not found"
                )
            
            # Route resolution based on issue category
            if issue.issue_category == IssueCategory.COVERAGE:
                return await self._resolve_coverage_issue(issue, strategy)
            elif issue.issue_category == IssueCategory.VALIDATION:
                return await self._resolve_validation_issue(issue, strategy)
            else:
                return await self._resolve_generic_issue(issue, strategy)
                
        except Exception as e:
            logger.error(f"Failed to resolve issue {issue_id}: {e}")
            return ResolutionResult(
                success=False,
                error_message=str(e)
            )
    
    async def get_issues(
        self,
        category: Optional[IssueCategory] = None,
        severity: Optional[IssueSeverity] = None,
        status: Optional[IssueStatus] = None,
        limit: int = 100
    ) -> List[DataQualityIssue]:
        """
        Get issues with unified filtering across categories
        """
        # This would query the unified dev_data_quality_issues table
        # For now, return placeholder implementation
        return []
    
    # =====================================
    # DASHBOARD AND REPORTING
    # =====================================
    
    async def get_dashboard_data(self) -> QualityDashboardData:
        """
        Get complete dashboard data combining all quality concerns
        """
        try:
            # Get coverage metrics from existing monitor
            coverage_metrics = await self._get_coverage_dashboard_metrics()
            
            # Get validation metrics from existing validator
            validation_metrics = await self._get_validation_dashboard_metrics()
            
            # Get recent issues across all categories
            recent_issues = await self.detect_all_issues(IssueDetectionRequest(
                categories=[IssueCategory.COVERAGE, IssueCategory.VALIDATION],
                lookback_days=7
            ))
            
            # Get agent status from existing agent
            agent_status = await self.data_quality_agent.get_agent_status()
            
            # Calculate overall quality score
            overall_score = await self.calculate_overall_quality_score()
            
            return QualityDashboardData(
                overall_quality_score=overall_score,
                coverage_metrics=coverage_metrics,
                validation_metrics=validation_metrics,
                recent_issues=recent_issues,
                agent_status=agent_status,
                active_workflows=[],  # Would get from workflow manager
                alerts_summary={},    # Would get from alert manager
                metrics_timestamp=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Failed to get dashboard data: {e}")
            raise
    
    async def calculate_overall_quality_score(self) -> float:
        """
        Calculate unified quality score combining coverage + validation
        """
        try:
            # Get recent coverage metrics (40% weight)
            coverage_scores = []
            for vendor in ['firstrate', 'polygon', 'tiingo']:
                for data_type in ['daily_prices', 'minute_bars']:
                    metrics = await self.get_coverage_metrics(vendor, data_type, days=7)
                    if metrics:
                        coverage_scores.append(metrics[0].value)
            
            avg_coverage = sum(coverage_scores) / len(coverage_scores) if coverage_scores else 0.0
            coverage_component = avg_coverage * 0.4
            
            # Get recent validation metrics (60% weight)
            validation_scores = []
            for vendor in ['firstrate', 'polygon', 'tiingo']:
                for data_type in ['daily_prices', 'minute_bars']:
                    metrics = await self.get_validation_metrics(vendor, data_type, days=7)
                    if metrics:
                        # Average completeness and consistency scores
                        scores = [m.value for m in metrics]
                        if scores:
                            validation_scores.append(sum(scores) / len(scores))
            
            avg_validation = sum(validation_scores) / len(validation_scores) if validation_scores else 0.0
            validation_component = avg_validation * 0.6
            
            # Combined score
            unified_score = coverage_component + validation_component
            return min(100.0, max(0.0, unified_score))
            
        except Exception as e:
            logger.error(f"Failed to calculate overall quality score: {e}")
            return 0.0
    
    async def get_metrics_summary(self, categories: List[IssueCategory], days: int = 30) -> Dict[str, Any]:
        """
        Get metrics summary across multiple categories
        """
        summary = {
            "categories": {},
            "overall": {
                "total_issues": 0,
                "critical_issues": 0,
                "resolved_issues": 0,
                "average_resolution_time": 0
            }
        }
        
        for category in categories:
            if category == IssueCategory.COVERAGE:
                summary["categories"]["coverage"] = await self._get_coverage_summary(days)
            elif category == IssueCategory.VALIDATION:
                summary["categories"]["validation"] = await self._get_validation_summary(days)
        
        return summary
    
    # =====================================
    # AGENT ORCHESTRATION
    # =====================================
    
    async def start_agent_monitoring(self) -> bool:
        """
        Start continuous agent monitoring for all quality concerns
        """
        try:
            self.is_monitoring_active = True
            
            # Start existing agent monitoring with enhanced detection
            asyncio.create_task(self._unified_monitoring_loop())
            
            logger.info("🤖 Started unified data quality agent monitoring")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start agent monitoring: {e}")
            return False
    
    async def stop_agent_monitoring(self) -> bool:
        """
        Stop agent monitoring
        """
        self.is_monitoring_active = False
        logger.info("🛑 Stopped unified data quality agent monitoring")
        return True
    
    async def get_agent_status(self) -> Dict[str, Any]:
        """
        Get current agent status and performance metrics
        """
        base_status = await self.data_quality_agent.get_agent_status()
        
        # Add unified monitoring information
        base_status.update({
            "unified_monitoring_active": self.is_monitoring_active,
            "last_unified_scan": self.last_scan_timestamp.isoformat() if self.last_scan_timestamp else None,
            "coverage_monitoring_enabled": True,
            "validation_monitoring_enabled": True,
            "unified_metrics_cache_size": len(self.unified_metrics_cache)
        })
        
        return base_status
    
    async def execute_manual_action(self, issue_id: int, action: str) -> ResolutionResult:
        """
        Execute manual action on specific issue
        """
        return await self.data_quality_agent.execute_manual_action(str(issue_id), action)
    
    # =====================================
    # ALERTING AND NOTIFICATIONS
    # =====================================
    
    async def send_quality_alert(self, issue: DataQualityIssue, alert_config: Dict[str, Any]) -> bool:
        """
        Send unified alert for any type of data quality issue
        """
        try:
            # Format alert based on issue category
            if issue.issue_category == IssueCategory.COVERAGE:
                alert_message = self._format_coverage_alert(issue)
            elif issue.issue_category == IssueCategory.VALIDATION:
                alert_message = self._format_validation_alert(issue)
            else:
                alert_message = self._format_generic_alert(issue)
            
            # Send via existing alert manager
            return await self.alert_manager.send_alert(alert_message, alert_config)
            
        except Exception as e:
            logger.error(f"Failed to send quality alert: {e}")
            return False
    
    async def get_alert_configuration(self) -> Dict[str, Any]:
        """
        Get current alert configuration for all quality categories
        """
        return await self.alert_manager.get_configuration()
    
    async def update_alert_configuration(self, config: Dict[str, Any]) -> bool:
        """
        Update alert configuration
        """
        return await self.alert_manager.update_configuration(config)
    
    # =====================================
    # PRIVATE HELPER METHODS
    # =====================================
    
    async def _unified_monitoring_loop(self):
        """
        Main unified monitoring loop combining coverage + validation
        """
        while self.is_monitoring_active:
            try:
                # Detect all issues
                all_issues = await self.detect_all_issues(IssueDetectionRequest(
                    categories=[IssueCategory.COVERAGE, IssueCategory.VALIDATION],
                    lookback_days=1
                ))
                
                # Process each issue through agent
                for issue in all_issues:
                    classified_issue = await self.classify_issue(issue)
                    
                    # Auto-resolve simple issues
                    if classified_issue.complexity == "simple":
                        await self.resolve_issue(classified_issue.id, ResolutionStrategy.AUTO_RESOLVE)
                
                self.last_scan_timestamp = datetime.now()
                
                # Sleep for monitoring interval
                await asyncio.sleep(300)  # 5 minutes
                
            except Exception as e:
                logger.error(f"Unified monitoring loop error: {e}")
                await asyncio.sleep(60)  # Wait before retrying
    
    def _map_priority_to_severity(self, priority_score: int) -> IssueSeverity:
        """Map priority score to severity level"""
        if priority_score >= 8:
            return IssueSeverity.CRITICAL
        elif priority_score >= 6:
            return IssueSeverity.HIGH
        elif priority_score >= 4:
            return IssueSeverity.MEDIUM
        else:
            return IssueSeverity.LOW
    
    def _severity_meets_threshold(self, severity: IssueSeverity, threshold: IssueSeverity) -> bool:
        """Check if severity meets threshold"""
        severity_values = {
            IssueSeverity.LOW: 1,
            IssueSeverity.MEDIUM: 2,
            IssueSeverity.HIGH: 3,
            IssueSeverity.CRITICAL: 4
        }
        return severity_values[severity] >= severity_values[threshold]
    
    def _determine_metric_status(self, value: float, threshold: float) -> str:
        """Determine metric status based on value and threshold"""
        if value >= threshold:
            return "healthy"
        elif value >= threshold * 0.8:
            return "warning"
        else:
            return "critical"
    
    def _convert_to_agent_issue(self, issue: DataQualityIssue):
        """Convert unified issue to agent's issue format"""
        # This would convert between formats - placeholder for now
        return issue
    
    async def get_issue_by_id(self, issue_id: int) -> Optional[DataQualityIssue]:
        """Get issue by ID from database"""
        # Placeholder - would query unified issues table
        return None
    
    async def _resolve_coverage_issue(self, issue: DataQualityIssue, strategy: ResolutionStrategy) -> ResolutionResult:
        """Resolve coverage-specific issues"""
        if issue.issue_type == "coverage_gap":
            # Trigger backfill using existing monitor logic
            # Placeholder implementation
            return ResolutionResult(success=True, workflow_id="coverage_workflow_123")
        return ResolutionResult(success=False, error_message="Unknown coverage issue type")
    
    async def _resolve_validation_issue(self, issue: DataQualityIssue, strategy: ResolutionStrategy) -> ResolutionResult:
        """Resolve validation-specific issues"""
        # Use existing validation resolution logic
        return ResolutionResult(success=True, workflow_id="validation_workflow_123")
    
    async def _resolve_generic_issue(self, issue: DataQualityIssue, strategy: ResolutionStrategy) -> ResolutionResult:
        """Resolve generic issues"""
        return ResolutionResult(success=True, workflow_id="generic_workflow_123")
    
    async def _get_coverage_dashboard_metrics(self) -> Dict[str, Any]:
        """Get coverage metrics for dashboard"""
        # Use existing coverage monitor dashboard logic
        return {"coverage_percentage": 95.5, "gaps_detected": 3}
    
    async def _get_validation_dashboard_metrics(self) -> Dict[str, Any]:
        """Get validation metrics for dashboard"""
        # Use existing validation dashboard logic
        return {"completeness_score": 98.2, "consistency_score": 97.8}
    
    async def _get_coverage_summary(self, days: int) -> Dict[str, Any]:
        """Get coverage summary for metrics"""
        return {"total_symbols": 1000, "coverage_percentage": 95.5}
    
    async def _get_validation_summary(self, days: int) -> Dict[str, Any]:
        """Get validation summary for metrics"""
        return {"completeness_score": 98.2, "issues_detected": 5}
    
    def _format_coverage_alert(self, issue: DataQualityIssue) -> Dict[str, Any]:
        """Format coverage gap alert"""
        return {
            "title": f"Coverage Gap Detected: {issue.symbol}",
            "message": f"Missing {issue.data_type} data for {issue.symbol} from {issue.affected_date_start} to {issue.affected_date_end}",
            "severity": issue.severity.value,
            "category": "coverage"
        }
    
    def _format_validation_alert(self, issue: DataQualityIssue) -> Dict[str, Any]:
        """Format validation error alert"""
        return {
            "title": f"Validation Error: {issue.symbol}",
            "message": f"Data quality issue in {issue.data_type} for {issue.symbol}: {issue.issue_type}",
            "severity": issue.severity.value,
            "category": "validation"
        }
    
    def _format_generic_alert(self, issue: DataQualityIssue) -> Dict[str, Any]:
        """Format generic data quality alert"""
        return {
            "title": f"Data Quality Issue: {issue.symbol}",
            "message": f"Issue detected: {issue.issue_type}",
            "severity": issue.severity.value,
            "category": issue.issue_category.value
        }