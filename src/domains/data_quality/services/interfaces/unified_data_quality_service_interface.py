"""
Unified Data Quality Service Interface
=====================================

Single interface consolidating coverage monitoring, validation, and issue lifecycle management
under the data quality framework with consistent patterns and shared code.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum

class IssueCategory(Enum):
    COVERAGE = "coverage"
    VALIDATION = "validation"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"

class IssueSeverity(Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

class IssueStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    RESOLVED = "resolved"
    ESCALATED = "escalated"
    CANCELLED = "cancelled"

class ResolutionStrategy(Enum):
    AUTO_RESOLVE = "auto_resolve"
    HUMAN_ASSISTED = "human_assisted"
    ESCALATE = "escalate"
    MONITOR = "monitor"

@dataclass
class DataQualityIssue:
    """Unified issue DTO for coverage gaps + validation errors"""
    id: Optional[int] = None
    issue_type: str = ""              # 'coverage_gap', 'missing_data', 'stale_data', 'extreme_value'
    issue_category: IssueCategory = IssueCategory.VALIDATION
    vendor: str = ""
    data_type: str = ""               # 'daily_prices', 'minute_bars'
    symbol: str = ""
    affected_date_start: date = None
    affected_date_end: date = None
    severity: IssueSeverity = IssueSeverity.MEDIUM
    status: IssueStatus = IssueStatus.PENDING
    
    # Classification and resolution
    complexity: Optional[str] = None   # 'simple', 'medium', 'complex'
    priority_score: int = 5
    estimated_effort_minutes: Optional[int] = None
    resolution_strategy: Optional[ResolutionStrategy] = None
    
    # Workflow tracking
    assigned_agent: Optional[str] = None
    workflow_id: Optional[str] = None
    
    # Flexible metadata
    metadata: Optional[Dict[str, Any]] = None
    resolution_metadata: Optional[Dict[str, Any]] = None
    
    # Timestamps
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None

@dataclass
class QualityMetric:
    """Unified metric DTO for coverage + validation scores"""
    metric_name: str                   # 'coverage_percentage', 'completeness_score', 'timeliness_score'
    metric_category: IssueCategory
    vendor: str
    data_type: str
    metric_date: date
    value: float
    threshold: float = 0.0
    status: str = "unknown"            # 'healthy', 'warning', 'critical'
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class CoverageScanRequest:
    """Request DTO for coverage scanning operations"""
    vendors: List[str]
    data_types: List[str]
    lookback_days: int = 30
    symbols_filter: Optional[List[str]] = None
    include_file_analysis: bool = True

@dataclass
class CoverageScanResult:
    """Result DTO for coverage scanning operations"""
    total_symbols_scanned: int
    total_trading_days: int
    coverage_records: List['CoverageRecord']
    gaps_detected: List[DataQualityIssue]
    scan_duration_seconds: float
    scan_timestamp: datetime

@dataclass
class IssueDetectionRequest:
    """Request DTO for issue detection operations"""
    categories: List[IssueCategory]
    severity_threshold: IssueSeverity = IssueSeverity.MEDIUM
    lookback_days: int = 1
    vendors: Optional[List[str]] = None
    data_types: Optional[List[str]] = None
    symbols: Optional[List[str]] = None

@dataclass
class ResolutionResult:
    """Result DTO for issue resolution operations"""
    success: bool
    workflow_id: Optional[str] = None
    resolution_strategy: Optional[ResolutionStrategy] = None
    estimated_completion_time: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class QualityDashboardData:
    """Complete dashboard data combining coverage + validation + agent status"""
    overall_quality_score: float
    coverage_metrics: Dict[str, Any]
    validation_metrics: Dict[str, Any]
    recent_issues: List[DataQualityIssue]
    agent_status: Dict[str, Any]
    active_workflows: List[Dict[str, Any]]
    alerts_summary: Dict[str, Any]
    metrics_timestamp: datetime

@dataclass
class CoverageRecord:
    """Coverage record from file system analysis"""
    vendor: str
    data_type: str
    symbol: str
    trading_date: date
    coverage_status: str              # 'complete', 'partial', 'missing', 'stale'
    data_quality_score: Optional[float] = None
    record_count: Optional[int] = None
    file_path: Optional[str] = None
    file_size_bytes: Optional[int] = None

class UnifiedDataQualityServiceInterface(ABC):
    """
    Single interface for all data quality operations
    
    Consolidates:
    - Coverage monitoring (gap detection, file analysis)
    - Data validation (completeness, consistency, accuracy)
    - Issue lifecycle management (classification, resolution, tracking)
    - Agent orchestration (automated resolution, learning)
    - Metrics and alerting (unified scoring, notifications)
    """
    
    # =====================================
    # COVERAGE MONITORING OPERATIONS
    # =====================================
    
    @abstractmethod
    async def scan_coverage(self, request: CoverageScanRequest) -> CoverageScanResult:
        """
        Unified coverage scanning across all vendors and data types
        
        Replaces separate coverage monitoring scans with single operation
        that analyzes file systems, databases, and generates coverage records.
        """
        pass
    
    @abstractmethod
    async def detect_coverage_gaps(self, request: CoverageScanRequest) -> List[DataQualityIssue]:
        """
        Detect coverage gaps and convert to unified data quality issues
        
        Replaces gap detection logic from coverage monitor with unified
        issue format that can be processed by the data quality agent.
        """
        pass
    
    @abstractmethod
    async def get_coverage_metrics(self, vendor: str, data_type: str, days: int = 30) -> List[QualityMetric]:
        """
        Get coverage metrics in unified format
        
        Returns coverage percentage, timeliness, and file availability
        metrics using the standard QualityMetric DTO.
        """
        pass
    
    # =====================================
    # DATA VALIDATION OPERATIONS  
    # =====================================
    
    @abstractmethod
    async def detect_validation_issues(self, request: IssueDetectionRequest) -> List[DataQualityIssue]:
        """
        Detect validation issues (completeness, consistency, accuracy)
        
        Scans database tables for missing values, extreme outliers,
        OHLC inconsistencies, and other validation problems.
        """
        pass
    
    @abstractmethod
    async def get_validation_metrics(self, vendor: str, data_type: str, days: int = 30) -> List[QualityMetric]:
        """
        Get validation metrics in unified format
        
        Returns completeness scores, consistency scores, and accuracy
        metrics using the standard QualityMetric DTO.
        """
        pass
    
    # =====================================
    # UNIFIED ISSUE MANAGEMENT
    # =====================================
    
    @abstractmethod
    async def detect_all_issues(self, request: IssueDetectionRequest) -> List[DataQualityIssue]:
        """
        Unified issue detection combining coverage + validation
        
        Single operation that detects coverage gaps, validation errors,
        consistency problems, and timeliness issues across all data.
        """
        pass
    
    @abstractmethod
    async def classify_issue(self, issue: DataQualityIssue) -> DataQualityIssue:
        """
        Intelligent issue classification using agent patterns
        
        Analyzes issue complexity, determines resolution strategy,
        estimates effort, and assigns priority scores.
        """
        pass
    
    @abstractmethod
    async def resolve_issue(self, issue_id: int, strategy: ResolutionStrategy) -> ResolutionResult:
        """
        Unified issue resolution workflow
        
        Orchestrates resolution based on issue type:
        - Coverage gaps → Trigger backfill operations
        - Validation errors → Data cleaning and correction
        - Consistency issues → Cross-validation and reconciliation
        """
        pass
    
    @abstractmethod
    async def get_issues(
        self,
        category: Optional[IssueCategory] = None,
        severity: Optional[IssueSeverity] = None,
        status: Optional[IssueStatus] = None,
        limit: int = 100
    ) -> List[DataQualityIssue]:
        """
        Get issues with unified filtering across categories
        
        Single endpoint for retrieving coverage gaps, validation errors,
        and other data quality issues with consistent filtering.
        """
        pass
    
    # =====================================
    # DASHBOARD AND REPORTING
    # =====================================
    
    @abstractmethod
    async def get_dashboard_data(self) -> QualityDashboardData:
        """
        Get complete dashboard data combining all quality concerns
        
        Single endpoint providing:
        - Overall quality score (coverage + validation)
        - Recent issues across all categories
        - Agent status and active workflows
        - Alert summaries and metrics
        """
        pass
    
    @abstractmethod
    async def calculate_overall_quality_score(self) -> float:
        """
        Calculate unified quality score combining coverage + validation
        
        Weighted score algorithm:
        - Coverage metrics (40%): availability and timeliness
        - Validation metrics (60%): completeness, consistency, accuracy
        """
        pass
    
    @abstractmethod
    async def get_metrics_summary(self, categories: List[IssueCategory], days: int = 30) -> Dict[str, Any]:
        """
        Get metrics summary across multiple categories
        
        Provides aggregated metrics for coverage, validation, and
        other quality categories with trend analysis.
        """
        pass
    
    # =====================================
    # AGENT ORCHESTRATION
    # =====================================
    
    @abstractmethod
    async def start_agent_monitoring(self) -> bool:
        """
        Start continuous agent monitoring for all quality concerns
        
        Initiates agent that monitors coverage, validation, and
        automatically resolves issues using learned patterns.
        """
        pass
    
    @abstractmethod
    async def stop_agent_monitoring(self) -> bool:
        """
        Stop agent monitoring
        """
        pass
    
    @abstractmethod
    async def get_agent_status(self) -> Dict[str, Any]:
        """
        Get current agent status and performance metrics
        
        Returns agent activity, success rates, active workflows,
        and learning patterns across all quality categories.
        """
        pass
    
    @abstractmethod
    async def execute_manual_action(self, issue_id: int, action: str) -> ResolutionResult:
        """
        Execute manual action on specific issue
        
        Allows human operators to trigger specific resolution
        actions for complex issues requiring intervention.
        """
        pass
    
    # =====================================
    # ALERTING AND NOTIFICATIONS
    # =====================================
    
    @abstractmethod
    async def send_quality_alert(self, issue: DataQualityIssue, alert_config: Dict[str, Any]) -> bool:
        """
        Send unified alert for any type of data quality issue
        
        Single alerting interface that formats and sends alerts
        for coverage gaps, validation errors, and other issues.
        """
        pass
    
    @abstractmethod
    async def get_alert_configuration(self) -> Dict[str, Any]:
        """
        Get current alert configuration for all quality categories
        """
        pass
    
    @abstractmethod
    async def update_alert_configuration(self, config: Dict[str, Any]) -> bool:
        """
        Update alert configuration
        """
        pass