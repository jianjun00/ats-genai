"""
Portfolio Management Service Interface

Comprehensive portfolio management operations for financial trading systems.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from enum import Enum


class PortfolioType(Enum):
    """Portfolio types."""
    EQUITY = "equity"
    FIXED_INCOME = "fixed_income"
    COMMODITY = "commodity"
    CRYPTO = "crypto"
    MIXED = "mixed"
    HEDGE_FUND = "hedge_fund"
    PENSION = "pension"
    MUTUAL_FUND = "mutual_fund"


class PortfolioStatus(Enum):
    """Portfolio status."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    LIQUIDATING = "liquidating"
    FROZEN = "frozen"
    CLOSED = "closed"


class RebalanceMethod(Enum):
    """Portfolio rebalancing methods."""
    PERCENTAGE = "percentage"
    DOLLAR_VALUE = "dollar_value"
    RISK_PARITY = "risk_parity"
    EQUAL_WEIGHT = "equal_weight"
    MOMENTUM = "momentum"
    MEAN_REVERSION = "mean_reversion"


class PerformanceMetricType(Enum):
    """Performance metric types."""
    TOTAL_RETURN = "total_return"
    ANNUALIZED_RETURN = "annualized_return"
    VOLATILITY = "volatility"
    SHARPE_RATIO = "sharpe_ratio"
    SORTINO_RATIO = "sortino_ratio"
    MAX_DRAWDOWN = "max_drawdown"
    CALMAR_RATIO = "calmar_ratio"
    ALPHA = "alpha"
    BETA = "beta"
    TRACKING_ERROR = "tracking_error"


@dataclass
class Portfolio:
    """Portfolio entity."""
    portfolio_id: str
    portfolio_name: str
    account_id: str
    portfolio_type: PortfolioType
    status: PortfolioStatus
    base_currency: str
    total_value: Decimal
    cash_balance: Decimal
    invested_amount: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    inception_date: datetime
    last_rebalance_date: Optional[datetime]
    benchmark_symbol: Optional[str]
    risk_limit: Optional[Decimal]
    created_at: datetime
    updated_at: datetime
    metadata: Dict[str, Any]


@dataclass
class PortfolioPosition:
    """Portfolio position."""
    position_id: str
    portfolio_id: str
    symbol: str
    quantity: Decimal
    average_cost: Decimal
    current_price: Decimal
    market_value: Decimal
    weight: float  # Percentage of portfolio
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    cost_basis: Decimal
    first_acquired_date: datetime
    last_transaction_date: datetime
    sector: Optional[str]
    industry: Optional[str]
    country: Optional[str]
    created_at: datetime
    updated_at: datetime


@dataclass
class PortfolioTransaction:
    """Portfolio transaction record."""
    transaction_id: str
    portfolio_id: str
    symbol: str
    transaction_type: str  # "buy", "sell", "dividend", "split", "transfer"
    quantity: Decimal
    price: Decimal
    amount: Decimal
    commission: Decimal
    fees: Decimal
    net_amount: Decimal
    transaction_date: datetime
    settlement_date: datetime
    order_id: Optional[str]
    notes: Optional[str]
    created_at: datetime


@dataclass
class PortfolioPerformance:
    """Portfolio performance metrics."""
    portfolio_id: str
    calculation_date: datetime
    start_date: datetime
    end_date: datetime
    total_return: Decimal
    annualized_return: Optional[Decimal]
    volatility: Decimal
    sharpe_ratio: Optional[Decimal]
    sortino_ratio: Optional[Decimal]
    max_drawdown: Decimal
    max_drawdown_duration: Optional[timedelta]
    calmar_ratio: Optional[Decimal]
    alpha: Optional[Decimal]
    beta: Optional[Decimal]
    tracking_error: Optional[Decimal]
    information_ratio: Optional[Decimal]
    win_rate: Optional[float]
    avg_win: Optional[Decimal]
    avg_loss: Optional[Decimal]
    profit_factor: Optional[Decimal]


@dataclass
class RebalanceOrder:
    """Portfolio rebalancing order."""
    rebalance_id: str
    portfolio_id: str
    symbol: str
    action: str  # "buy", "sell"
    target_weight: float
    current_weight: float
    target_quantity: Decimal
    current_quantity: Decimal
    estimated_cost: Decimal
    priority: int
    status: str
    created_at: datetime


@dataclass
class RebalanceResult:
    """Portfolio rebalancing result."""
    rebalance_id: str
    portfolio_id: str
    rebalance_date: datetime
    method: RebalanceMethod
    total_orders: int
    executed_orders: int
    failed_orders: int
    total_cost: Decimal
    execution_time: timedelta
    before_weights: Dict[str, float]
    after_weights: Dict[str, float]
    performance_impact: Optional[Dict[str, Any]]


@dataclass
class AllocationTarget:
    """Portfolio allocation target."""
    target_id: str
    portfolio_id: str
    symbol: str
    target_weight: float
    min_weight: Optional[float]
    max_weight: Optional[float]
    rebalance_threshold: float
    is_active: bool
    created_at: datetime
    updated_at: datetime


@dataclass
class PortfolioOptimization:
    """Portfolio optimization result."""
    optimization_id: str
    portfolio_id: str
    optimization_date: datetime
    objective: str
    constraints: Dict[str, Any]
    recommended_weights: Dict[str, float]
    expected_return: Decimal
    expected_volatility: Decimal
    expected_sharpe: Decimal
    optimization_score: float
    implementation_cost: Decimal
    validity_period: timedelta


@dataclass
class RiskMetrics:
    """Portfolio risk metrics."""
    portfolio_id: str
    calculation_date: datetime
    var_1d_95: Decimal
    var_1d_99: Decimal
    var_5d_95: Decimal
    var_5d_99: Decimal
    expected_shortfall_95: Decimal
    expected_shortfall_99: Decimal
    portfolio_beta: Optional[Decimal]
    portfolio_volatility: Decimal
    concentration_risk: Decimal
    sector_concentration: Dict[str, float]
    correlation_risk: Decimal
    leverage: Decimal


@dataclass
class PortfolioAlert:
    """Portfolio monitoring alert."""
    alert_id: str
    portfolio_id: str
    alert_type: str
    severity: str  # "info", "warning", "critical"
    message: str
    details: Dict[str, Any]
    threshold_breached: Optional[str]
    current_value: Optional[Decimal]
    threshold_value: Optional[Decimal]
    is_active: bool
    created_at: datetime
    acknowledged_at: Optional[datetime]
    resolved_at: Optional[datetime]


@dataclass
class AttributionAnalysis:
    """Portfolio attribution analysis."""
    analysis_id: str
    portfolio_id: str
    analysis_date: datetime
    period: timedelta
    total_return: Decimal
    asset_allocation_effect: Decimal
    security_selection_effect: Decimal
    interaction_effect: Decimal
    currency_effect: Optional[Decimal]
    sector_attribution: Dict[str, Decimal]
    security_attribution: Dict[str, Decimal]
    benchmark_return: Optional[Decimal]


class PortfolioServiceInterface(ABC):
    """
    Portfolio Management Service Interface

    Provides comprehensive portfolio management capabilities including:
    - Portfolio creation and management
    - Position tracking and valuation
    - Performance measurement and attribution
    - Risk monitoring and reporting
    - Rebalancing and optimization
    - Transaction recording and reconciliation
    """

    # Portfolio Management

    @abstractmethod
    async def create_portfolio(
        self,
        portfolio_name: str,
        account_id: str,
        portfolio_type: PortfolioType,
        base_currency: str,
        initial_cash: Decimal,
        benchmark_symbol: Optional[str] = None,
        risk_limit: Optional[Decimal] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Portfolio:
        """
        Create new portfolio.

        Args:
            portfolio_name: Portfolio name
            account_id: Associated account identifier
            portfolio_type: Type of portfolio
            base_currency: Base currency for portfolio
            initial_cash: Initial cash amount
            benchmark_symbol: Benchmark for performance comparison
            risk_limit: Risk limit for portfolio
            metadata: Additional portfolio metadata

        Returns:
            Created portfolio
        """

    @abstractmethod
    async def get_portfolio(self, portfolio_id: str) -> Optional[Portfolio]:
        """
        Get portfolio by ID.

        Args:
            portfolio_id: Portfolio identifier

        Returns:
            Portfolio if found
        """

    @abstractmethod
    async def update_portfolio(
        self,
        portfolio_id: str,
        updates: Dict[str, Any]
    ) -> Portfolio:
        """
        Update portfolio properties.

        Args:
            portfolio_id: Portfolio identifier
            updates: Properties to update

        Returns:
            Updated portfolio
        """

    @abstractmethod
    async def list_portfolios(
        self,
        account_id: Optional[str] = None,
        portfolio_type: Optional[PortfolioType] = None,
        status: Optional[PortfolioStatus] = None
    ) -> List[Portfolio]:
        """
        List portfolios with optional filters.

        Args:
            account_id: Filter by account
            portfolio_type: Filter by portfolio type
            status: Filter by status

        Returns:
            List of portfolios
        """

    @abstractmethod
    async def close_portfolio(
        self,
        portfolio_id: str,
        liquidate_positions: bool = True
    ) -> bool:
        """
        Close portfolio and optionally liquidate positions.

        Args:
            portfolio_id: Portfolio identifier
            liquidate_positions: Whether to liquidate all positions

        Returns:
            Success status
        """

    # Position Management

    @abstractmethod
    async def add_position(
        self,
        portfolio_id: str,
        symbol: str,
        quantity: Decimal,
        price: Decimal,
        transaction_date: datetime,
        transaction_cost: Optional[Decimal] = None
    ) -> PortfolioPosition:
        """
        Add position to portfolio.

        Args:
            portfolio_id: Portfolio identifier
            symbol: Security symbol
            quantity: Position quantity
            price: Acquisition price
            transaction_date: Transaction date
            transaction_cost: Transaction costs

        Returns:
            Created position
        """

    @abstractmethod
    async def update_position(
        self,
        portfolio_id: str,
        symbol: str,
        quantity_change: Decimal,
        price: Decimal,
        transaction_date: datetime,
        transaction_cost: Optional[Decimal] = None
    ) -> PortfolioPosition:
        """
        Update position in portfolio.

        Args:
            portfolio_id: Portfolio identifier
            symbol: Security symbol
            quantity_change: Change in quantity (positive for buy, negative for sell)
            price: Transaction price
            transaction_date: Transaction date
            transaction_cost: Transaction costs

        Returns:
            Updated position
        """

    @abstractmethod
    async def get_positions(
        self,
        portfolio_id: str,
        active_only: bool = True
    ) -> List[PortfolioPosition]:
        """
        Get all positions for portfolio.

        Args:
            portfolio_id: Portfolio identifier
            active_only: Only return positions with non-zero quantity

        Returns:
            List of portfolio positions
        """

    @abstractmethod
    async def get_position(
        self,
        portfolio_id: str,
        symbol: str
    ) -> Optional[PortfolioPosition]:
        """
        Get specific position.

        Args:
            portfolio_id: Portfolio identifier
            symbol: Security symbol

        Returns:
            Position if found
        """

    @abstractmethod
    async def liquidate_position(
        self,
        portfolio_id: str,
        symbol: str,
        execution_price: Optional[Decimal] = None
    ) -> PortfolioTransaction:
        """
        Liquidate position completely.

        Args:
            portfolio_id: Portfolio identifier
            symbol: Security symbol
            execution_price: Override market price for execution

        Returns:
            Liquidation transaction
        """

    # Transaction Management

    @abstractmethod
    async def record_transaction(
        self,
        portfolio_id: str,
        symbol: str,
        transaction_type: str,
        quantity: Decimal,
        price: Decimal,
        commission: Decimal,
        fees: Decimal,
        transaction_date: datetime,
        settlement_date: Optional[datetime] = None,
        order_id: Optional[str] = None,
        notes: Optional[str] = None
    ) -> PortfolioTransaction:
        """
        Record portfolio transaction.

        Args:
            portfolio_id: Portfolio identifier
            symbol: Security symbol
            transaction_type: Type of transaction
            quantity: Transaction quantity
            price: Transaction price
            commission: Commission costs
            fees: Additional fees
            transaction_date: Transaction date
            settlement_date: Settlement date
            order_id: Associated order ID
            notes: Transaction notes

        Returns:
            Recorded transaction
        """

    @abstractmethod
    async def get_transactions(
        self,
        portfolio_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        symbol: Optional[str] = None,
        transaction_type: Optional[str] = None
    ) -> List[PortfolioTransaction]:
        """
        Get portfolio transactions.

        Args:
            portfolio_id: Portfolio identifier
            start_date: Filter from date
            end_date: Filter to date
            symbol: Filter by symbol
            transaction_type: Filter by transaction type

        Returns:
            List of transactions
        """

    @abstractmethod
    async def calculate_realized_pnl(
        self,
        portfolio_id: str,
        symbol: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Decimal:
        """
        Calculate realized P&L.

        Args:
            portfolio_id: Portfolio identifier
            symbol: Calculate for specific symbol
            start_date: Calculate from date
            end_date: Calculate to date

        Returns:
            Realized P&L amount
        """

    # Valuation & Performance

    @abstractmethod
    async def update_portfolio_valuation(
        self,
        portfolio_id: str,
        market_prices: Optional[Dict[str, Decimal]] = None
    ) -> Portfolio:
        """
        Update portfolio valuation with current market prices.

        Args:
            portfolio_id: Portfolio identifier
            market_prices: Override market prices

        Returns:
            Updated portfolio with current valuation
        """

    @abstractmethod
    async def calculate_performance_metrics(
        self,
        portfolio_id: str,
        start_date: datetime,
        end_date: datetime,
        benchmark_symbol: Optional[str] = None
    ) -> PortfolioPerformance:
        """
        Calculate comprehensive performance metrics.

        Args:
            portfolio_id: Portfolio identifier
            start_date: Performance calculation start date
            end_date: Performance calculation end date
            benchmark_symbol: Benchmark for relative metrics

        Returns:
            Portfolio performance metrics
        """

    @abstractmethod
    async def get_performance_history(
        self,
        portfolio_id: str,
        metric_type: PerformanceMetricType,
        start_date: datetime,
        end_date: datetime,
        frequency: str = "daily"
    ) -> List[Dict[str, Any]]:
        """
        Get historical performance data.

        Args:
            portfolio_id: Portfolio identifier
            metric_type: Type of performance metric
            start_date: History start date
            end_date: History end date
            frequency: Data frequency (daily, weekly, monthly)

        Returns:
            Historical performance data
        """

    @abstractmethod
    async def calculate_attribution_analysis(
        self,
        portfolio_id: str,
        start_date: datetime,
        end_date: datetime,
        benchmark_symbol: str
    ) -> AttributionAnalysis:
        """
        Calculate performance attribution analysis.

        Args:
            portfolio_id: Portfolio identifier
            start_date: Analysis start date
            end_date: Analysis end date
            benchmark_symbol: Benchmark for attribution

        Returns:
            Attribution analysis results
        """

    # Risk Management

    @abstractmethod
    async def calculate_portfolio_risk_metrics(
        self,
        portfolio_id: str,
        confidence_levels: List[float] = [0.95, 0.99],
        time_horizons: List[int] = [1, 5]
    ) -> RiskMetrics:
        """
        Calculate portfolio risk metrics.

        Args:
            portfolio_id: Portfolio identifier
            confidence_levels: VaR confidence levels
            time_horizons: VaR time horizons in days

        Returns:
            Portfolio risk metrics
        """

    @abstractmethod
    async def monitor_risk_limits(
        self,
        portfolio_id: str,
        risk_limits: Dict[str, Decimal]
    ) -> List[PortfolioAlert]:
        """
        Monitor portfolio against risk limits.

        Args:
            portfolio_id: Portfolio identifier
            risk_limits: Risk limits to monitor

        Returns:
            List of risk alerts if any limits breached
        """

    @abstractmethod
    async def stress_test_portfolio(
        self,
        portfolio_id: str,
        stress_scenarios: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Decimal]]:
        """
        Run stress tests on portfolio.

        Args:
            portfolio_id: Portfolio identifier
            stress_scenarios: Stress test scenarios

        Returns:
            Stress test results by scenario
        """

    # Rebalancing & Optimization

    @abstractmethod
    async def set_allocation_targets(
        self,
        portfolio_id: str,
        targets: List[AllocationTarget]
    ) -> bool:
        """
        Set portfolio allocation targets.

        Args:
            portfolio_id: Portfolio identifier
            targets: List of allocation targets

        Returns:
            Success status
        """

    @abstractmethod
    async def check_rebalancing_needed(
        self,
        portfolio_id: str,
        threshold: float = 0.05
    ) -> Dict[str, float]:
        """
        Check if portfolio needs rebalancing.

        Args:
            portfolio_id: Portfolio identifier
            threshold: Rebalancing threshold percentage

        Returns:
            Deviations from target allocations
        """

    @abstractmethod
    async def generate_rebalance_orders(
        self,
        portfolio_id: str,
        method: RebalanceMethod,
        constraints: Optional[Dict[str, Any]] = None
    ) -> List[RebalanceOrder]:
        """
        Generate rebalancing orders.

        Args:
            portfolio_id: Portfolio identifier
            method: Rebalancing method
            constraints: Rebalancing constraints

        Returns:
            List of rebalancing orders
        """

    @abstractmethod
    async def execute_rebalancing(
        self,
        portfolio_id: str,
        rebalance_orders: List[RebalanceOrder]
    ) -> RebalanceResult:
        """
        Execute portfolio rebalancing.

        Args:
            portfolio_id: Portfolio identifier
            rebalance_orders: Orders to execute

        Returns:
            Rebalancing execution result
        """

    @abstractmethod
    async def optimize_portfolio(
        self,
        portfolio_id: str,
        objective: str,
        constraints: Dict[str, Any],
        universe: Optional[List[str]] = None
    ) -> PortfolioOptimization:
        """
        Optimize portfolio allocation.

        Args:
            portfolio_id: Portfolio identifier
            objective: Optimization objective
            constraints: Optimization constraints
            universe: Investment universe symbols

        Returns:
            Portfolio optimization result
        """

    # Monitoring & Alerts

    @abstractmethod
    async def start_real_time_monitoring(
        self,
        portfolio_id: str,
        monitoring_rules: List[Dict[str, Any]],
        callback: Callable[[PortfolioAlert], None]
    ) -> str:
        """
        Start real-time portfolio monitoring.

        Args:
            portfolio_id: Portfolio identifier
            monitoring_rules: Rules for monitoring
            callback: Function to call when alerts triggered

        Returns:
            Monitoring session ID
        """

    @abstractmethod
    async def stop_real_time_monitoring(self, session_id: str) -> bool:
        """
        Stop real-time portfolio monitoring.

        Args:
            session_id: Monitoring session identifier

        Returns:
            Success status
        """

    @abstractmethod
    async def get_portfolio_alerts(
        self,
        portfolio_id: str,
        active_only: bool = True,
        severity: Optional[str] = None
    ) -> List[PortfolioAlert]:
        """
        Get portfolio alerts.

        Args:
            portfolio_id: Portfolio identifier
            active_only: Only return active alerts
            severity: Filter by severity level

        Returns:
            List of portfolio alerts
        """

    @abstractmethod
    async def acknowledge_alert(
        self,
        alert_id: str,
        acknowledged_by: str,
        notes: Optional[str] = None
    ) -> bool:
        """
        Acknowledge portfolio alert.

        Args:
            alert_id: Alert identifier
            acknowledged_by: User acknowledging alert
            notes: Acknowledgment notes

        Returns:
            Success status
        """

    # Reporting

    @abstractmethod
    async def generate_portfolio_report(
        self,
        portfolio_id: str,
        report_type: str,
        start_date: datetime,
        end_date: datetime,
        include_positions: bool = True,
        include_transactions: bool = True,
        include_performance: bool = True,
        include_risk_metrics: bool = True
    ) -> Dict[str, Any]:
        """
        Generate comprehensive portfolio report.

        Args:
            portfolio_id: Portfolio identifier
            report_type: Type of report to generate
            start_date: Report start date
            end_date: Report end date
            include_positions: Include position details
            include_transactions: Include transaction history
            include_performance: Include performance metrics
            include_risk_metrics: Include risk analysis

        Returns:
            Portfolio report data
        """

    @abstractmethod
    async def export_portfolio_data(
        self,
        portfolio_id: str,
        data_types: List[str],
        format: str = "json",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> bytes:
        """
        Export portfolio data.

        Args:
            portfolio_id: Portfolio identifier
            data_types: Types of data to export
            format: Export format (json, csv, excel)
            start_date: Export from date
            end_date: Export to date

        Returns:
            Exported data as bytes
        """
