"""
Risk Management Service Interface

Defines comprehensive risk management operations for financial trading systems.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum


class RiskLevel(Enum):
    """Risk assessment levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RiskType(Enum):
    """Types of financial risks."""
    MARKET = "market"
    CREDIT = "credit"
    OPERATIONAL = "operational"
    LIQUIDITY = "liquidity"
    CONCENTRATION = "concentration"
    VOLATILITY = "volatility"


class AlertPriority(Enum):
    """Risk alert priority levels."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


@dataclass
class RiskMetric:
    """Individual risk metric measurement."""
    metric_name: str
    current_value: Decimal
    threshold_value: Decimal
    risk_level: RiskLevel
    percentage_of_limit: float
    last_updated: datetime
    trend_direction: str  # "increasing", "decreasing", "stable"


@dataclass
class PositionRisk:
    """Risk assessment for individual position."""
    position_id: str
    symbol: str
    quantity: Decimal
    market_value: Decimal
    unrealized_pnl: Decimal
    value_at_risk_1d: Decimal
    value_at_risk_5d: Decimal
    beta: Optional[Decimal]
    volatility: Decimal
    concentration_risk: Decimal
    liquidity_score: Decimal
    risk_level: RiskLevel
    risk_factors: List[str]
    last_assessed: datetime


@dataclass
class PortfolioRisk:
    """Comprehensive portfolio risk assessment."""
    portfolio_id: str
    total_value: Decimal
    total_var_1d: Decimal
    total_var_5d: Decimal
    max_drawdown: Decimal
    sharpe_ratio: Optional[Decimal]
    beta: Optional[Decimal]
    correlation_risk: Decimal
    concentration_risk: Decimal
    leverage_ratio: Decimal
    cash_ratio: Decimal
    position_risks: List[PositionRisk]
    sector_exposures: Dict[str, Decimal]
    overall_risk_level: RiskLevel
    risk_utilization: float  # Percentage of risk budget used
    assessment_timestamp: datetime


@dataclass
class RiskLimit:
    """Risk limit configuration."""
    limit_id: str
    limit_type: str
    entity_type: str  # "position", "portfolio", "account", "firm"
    entity_id: str
    limit_value: Decimal
    current_usage: Decimal
    utilization_percentage: float
    warning_threshold: float  # 80% for warning
    critical_threshold: float  # 95% for critical
    is_breached: bool
    breach_timestamp: Optional[datetime]
    created_at: datetime
    updated_at: datetime


@dataclass
class RiskAlert:
    """Risk alert notification."""
    alert_id: str
    alert_type: RiskType
    priority: AlertPriority
    entity_type: str
    entity_id: str
    message: str
    details: Dict[str, Any]
    threshold_breached: Optional[str]
    recommended_actions: List[str]
    is_active: bool
    created_at: datetime
    acknowledged_at: Optional[datetime]
    resolved_at: Optional[datetime]


@dataclass
class RiskScenario:
    """Risk scenario analysis."""
    scenario_id: str
    scenario_name: str
    description: str
    market_shocks: Dict[str, Decimal]  # Asset -> shock percentage
    stress_results: Dict[str, Decimal]
    expected_pnl: Decimal
    worst_case_pnl: Decimal
    probability: Optional[float]
    time_horizon: timedelta
    created_at: datetime


@dataclass
class VaRCalculation:
    """Value at Risk calculation result."""
    calculation_id: str
    portfolio_id: str
    confidence_level: float  # 95%, 99%
    time_horizon_days: int
    methodology: str  # "historical", "parametric", "monte_carlo"
    var_amount: Decimal
    expected_shortfall: Decimal
    calculation_timestamp: datetime
    model_parameters: Dict[str, Any]


@dataclass
class RiskReportRequest:
    """Request for risk report generation."""
    report_type: str
    portfolio_ids: Optional[List[str]]
    date_range: Optional[tuple[datetime, datetime]]
    risk_types: Optional[List[RiskType]]
    include_scenarios: bool = False
    include_var_analysis: bool = True
    output_format: str = "json"


@dataclass
class RiskReport:
    """Comprehensive risk report."""
    report_id: str
    report_type: str
    generated_at: datetime
    portfolio_risks: List[PortfolioRisk]
    aggregate_metrics: Dict[str, RiskMetric]
    active_alerts: List[RiskAlert]
    limit_breaches: List[RiskLimit]
    var_analysis: Optional[List[VaRCalculation]]
    scenario_analysis: Optional[List[RiskScenario]]
    executive_summary: str
    recommendations: List[str]


class RiskServiceInterface(ABC):
    """
    Risk Management Service Interface
    
    Provides comprehensive risk management capabilities including:
    - Real-time risk monitoring and alerting
    - VaR calculations and stress testing
    - Risk limit management and enforcement
    - Portfolio and position risk assessment
    - Regulatory compliance monitoring
    """
    
    # Position Risk Management
    
    @abstractmethod
    async def assess_position_risk(
        self,
        position_id: str,
        include_scenarios: bool = False
    ) -> PositionRisk:
        """
        Assess risk for individual position.
        
        Args:
            position_id: Unique position identifier
            include_scenarios: Include scenario analysis
            
        Returns:
            Complete position risk assessment
        """
        pass
    
    @abstractmethod
    async def assess_portfolio_risk(
        self,
        portfolio_id: str,
        include_var: bool = True,
        include_scenarios: bool = False
    ) -> PortfolioRisk:
        """
        Assess comprehensive portfolio risk.
        
        Args:
            portfolio_id: Portfolio identifier
            include_var: Include VaR calculations
            include_scenarios: Include scenario analysis
            
        Returns:
            Complete portfolio risk assessment
        """
        pass
    
    # Risk Limit Management
    
    @abstractmethod
    async def create_risk_limit(
        self,
        entity_type: str,
        entity_id: str,
        limit_type: str,
        limit_value: Decimal,
        warning_threshold: float = 0.8,
        critical_threshold: float = 0.95
    ) -> RiskLimit:
        """
        Create new risk limit.
        
        Args:
            entity_type: Type of entity (position, portfolio, account)
            entity_id: Entity identifier
            limit_type: Type of limit (var, exposure, concentration)
            limit_value: Maximum allowed value
            warning_threshold: Warning threshold percentage
            critical_threshold: Critical threshold percentage
            
        Returns:
            Created risk limit
        """
        pass
    
    @abstractmethod
    async def check_risk_limits(
        self,
        entity_type: str,
        entity_id: str
    ) -> List[RiskLimit]:
        """
        Check all risk limits for entity.
        
        Args:
            entity_type: Type of entity
            entity_id: Entity identifier
            
        Returns:
            List of risk limits with current status
        """
        pass
    
    @abstractmethod
    async def update_risk_limit(
        self,
        limit_id: str,
        limit_value: Optional[Decimal] = None,
        warning_threshold: Optional[float] = None,
        critical_threshold: Optional[float] = None
    ) -> RiskLimit:
        """
        Update existing risk limit.
        
        Args:
            limit_id: Limit identifier
            limit_value: New limit value
            warning_threshold: New warning threshold
            critical_threshold: New critical threshold
            
        Returns:
            Updated risk limit
        """
        pass
    
    # Real-time Monitoring & Alerts
    
    @abstractmethod
    async def start_real_time_monitoring(
        self,
        portfolio_ids: List[str],
        monitoring_frequency_seconds: int = 30
    ) -> str:
        """
        Start real-time risk monitoring for portfolios.
        
        Args:
            portfolio_ids: List of portfolio IDs to monitor
            monitoring_frequency_seconds: How often to check
            
        Returns:
            Monitoring session ID
        """
        pass
    
    @abstractmethod
    async def stop_real_time_monitoring(self, session_id: str) -> bool:
        """
        Stop real-time risk monitoring session.
        
        Args:
            session_id: Monitoring session identifier
            
        Returns:
            Success status
        """
        pass
    
    @abstractmethod
    async def get_active_alerts(
        self,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        priority: Optional[AlertPriority] = None
    ) -> List[RiskAlert]:
        """
        Get active risk alerts.
        
        Args:
            entity_type: Filter by entity type
            entity_id: Filter by entity ID
            priority: Filter by priority level
            
        Returns:
            List of active alerts
        """
        pass
    
    @abstractmethod
    async def acknowledge_alert(
        self,
        alert_id: str,
        acknowledged_by: str,
        notes: Optional[str] = None
    ) -> bool:
        """
        Acknowledge risk alert.
        
        Args:
            alert_id: Alert identifier
            acknowledged_by: User acknowledging
            notes: Optional acknowledgment notes
            
        Returns:
            Success status
        """
        pass
    
    # VaR and Stress Testing
    
    @abstractmethod
    async def calculate_var(
        self,
        portfolio_id: str,
        confidence_level: float = 0.95,
        time_horizon_days: int = 1,
        methodology: str = "historical"
    ) -> VaRCalculation:
        """
        Calculate Value at Risk for portfolio.
        
        Args:
            portfolio_id: Portfolio identifier
            confidence_level: Confidence level (0.95, 0.99)
            time_horizon_days: Time horizon in days
            methodology: Calculation method
            
        Returns:
            VaR calculation results
        """
        pass
    
    @abstractmethod
    async def run_stress_test(
        self,
        portfolio_id: str,
        scenario: RiskScenario
    ) -> Dict[str, Decimal]:
        """
        Run stress test scenario on portfolio.
        
        Args:
            portfolio_id: Portfolio identifier
            scenario: Stress test scenario
            
        Returns:
            Stress test results by position
        """
        pass
    
    @abstractmethod
    async def create_scenario(
        self,
        scenario_name: str,
        description: str,
        market_shocks: Dict[str, Decimal],
        time_horizon: timedelta,
        probability: Optional[float] = None
    ) -> RiskScenario:
        """
        Create stress test scenario.
        
        Args:
            scenario_name: Scenario name
            description: Scenario description
            market_shocks: Market shock percentages by asset
            time_horizon: Scenario time horizon
            probability: Estimated probability
            
        Returns:
            Created scenario
        """
        pass
    
    # Reporting and Analytics
    
    @abstractmethod
    async def generate_risk_report(
        self,
        request: RiskReportRequest
    ) -> RiskReport:
        """
        Generate comprehensive risk report.
        
        Args:
            request: Report generation request
            
        Returns:
            Generated risk report
        """
        pass
    
    @abstractmethod
    async def get_risk_metrics_history(
        self,
        portfolio_id: str,
        metric_names: List[str],
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, List[RiskMetric]]:
        """
        Get historical risk metrics.
        
        Args:
            portfolio_id: Portfolio identifier
            metric_names: List of metric names to retrieve
            start_date: Start date for history
            end_date: End date for history
            
        Returns:
            Historical risk metrics by metric name
        """
        pass
    
    @abstractmethod
    async def get_compliance_status(
        self,
        portfolio_id: str,
        regulation_types: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get regulatory compliance status.
        
        Args:
            portfolio_id: Portfolio identifier
            regulation_types: Specific regulations to check
            
        Returns:
            Compliance status by regulation type
        """
        pass
    
    # Risk Configuration
    
    @abstractmethod
    async def configure_risk_model(
        self,
        model_type: str,
        parameters: Dict[str, Any]
    ) -> str:
        """
        Configure risk calculation model.
        
        Args:
            model_type: Type of risk model
            parameters: Model parameters
            
        Returns:
            Model configuration ID
        """
        pass
    
    @abstractmethod
    async def get_risk_model_performance(
        self,
        model_id: str,
        evaluation_period: timedelta
    ) -> Dict[str, Any]:
        """
        Get risk model performance metrics.
        
        Args:
            model_id: Model identifier
            evaluation_period: Period to evaluate
            
        Returns:
            Model performance metrics
        """
        pass