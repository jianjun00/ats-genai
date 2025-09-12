"""
Order Management and Execution Service Interface

Comprehensive order lifecycle management and execution for financial trading systems.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any, Union, Callable, AsyncIterator
from dataclasses import dataclass
from enum import Enum


class OrderType(Enum):
    """Order types."""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"
    ICEBERG = "iceberg"
    TWAP = "twap"
    VWAP = "vwap"
    IMPLEMENTATION_SHORTFALL = "implementation_shortfall"
    ARRIVAL_PRICE = "arrival_price"


class OrderSide(Enum):
    """Order side."""
    BUY = "buy"
    SELL = "sell"
    BUY_TO_COVER = "buy_to_cover"
    SELL_SHORT = "sell_short"


class OrderStatus(Enum):
    """Order status."""
    PENDING = "pending"
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    SUSPENDED = "suspended"


class TimeInForce(Enum):
    """Time in force types."""
    DAY = "day"
    GTC = "gtc"  # Good Till Cancelled
    IOC = "ioc"  # Immediate or Cancel
    FOK = "fok"  # Fill or Kill
    GTD = "gtd"  # Good Till Date
    ATO = "ato"  # At The Opening
    ATC = "atc"  # At The Close


class ExecutionAlgorithm(Enum):
    """Execution algorithm types."""
    TWAP = "twap"
    VWAP = "vwap"
    IMPLEMENTATION_SHORTFALL = "implementation_shortfall"
    ARRIVAL_PRICE = "arrival_price"
    ICEBERG = "iceberg"
    SNIPER = "sniper"
    STEALTH = "stealth"
    LIQUIDITY_SEEKING = "liquidity_seeking"
    OPPORTUNISTIC = "opportunistic"


class OrderRejectReason(Enum):
    """Order rejection reasons."""
    INSUFFICIENT_FUNDS = "insufficient_funds"
    INVALID_SYMBOL = "invalid_symbol"
    INVALID_QUANTITY = "invalid_quantity"
    INVALID_PRICE = "invalid_price"
    MARKET_CLOSED = "market_closed"
    RISK_LIMIT_EXCEEDED = "risk_limit_exceeded"
    DUPLICATE_ORDER = "duplicate_order"
    SYSTEM_ERROR = "system_error"
    REGULATORY_REJECTION = "regulatory_rejection"


@dataclass
class Order:
    """Order entity."""
    order_id: str
    client_order_id: str
    portfolio_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    price: Optional[Decimal]
    stop_price: Optional[Decimal]
    time_in_force: TimeInForce
    status: OrderStatus
    filled_quantity: Decimal
    remaining_quantity: Decimal
    avg_fill_price: Optional[Decimal]
    commission: Decimal
    fees: Decimal
    execution_algorithm: Optional[ExecutionAlgorithm]
    algorithm_parameters: Optional[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime
    expires_at: Optional[datetime]
    parent_order_id: Optional[str]
    routing_destination: Optional[str]
    metadata: Dict[str, Any]


@dataclass
class Execution:
    """Order execution/fill."""
    execution_id: str
    order_id: str
    symbol: str
    side: OrderSide
    quantity: Decimal
    price: Decimal
    execution_time: datetime
    counterparty: Optional[str]
    execution_venue: str
    commission: Decimal
    fees: Decimal
    settlement_date: datetime
    trade_id: str
    is_final: bool
    metadata: Dict[str, Any]


@dataclass
class OrderBook:
    """Order book snapshot."""
    symbol: str
    timestamp: datetime
    bids: List[Dict[str, Decimal]]  # [{"price": price, "size": size, "orders": count}]
    asks: List[Dict[str, Decimal]]
    bid_price: Decimal
    ask_price: Decimal
    spread: Decimal
    mid_price: Decimal
    last_trade_price: Optional[Decimal]
    last_trade_size: Optional[Decimal]
    total_bid_size: Decimal
    total_ask_size: Decimal


@dataclass
class ExecutionReport:
    """Execution report for order status updates."""
    report_id: str
    order_id: str
    client_order_id: str
    execution_type: str  # "new", "partial_fill", "fill", "cancelled", "rejected"
    order_status: OrderStatus
    symbol: str
    side: OrderSide
    quantity: Decimal
    price: Optional[Decimal]
    filled_quantity: Decimal
    remaining_quantity: Decimal
    avg_fill_price: Optional[Decimal]
    last_fill_quantity: Optional[Decimal]
    last_fill_price: Optional[Decimal]
    commission: Decimal
    fees: Decimal
    timestamp: datetime
    text: Optional[str]
    reject_reason: Optional[OrderRejectReason]


@dataclass
class AlgorithmicOrderConfig:
    """Algorithmic order configuration."""
    config_id: str
    algorithm: ExecutionAlgorithm
    symbol: str
    quantity: Decimal
    side: OrderSide
    start_time: datetime
    end_time: datetime
    participation_rate: Optional[float]
    price_limit: Optional[Decimal]
    urgency: float  # 0.0 (patient) to 1.0 (aggressive)
    risk_aversion: float  # 0.0 (risk-seeking) to 1.0 (risk-averse)
    dark_pool_preference: Optional[float]
    minimum_fill_size: Optional[Decimal]
    maximum_slice_size: Optional[Decimal]
    interval_duration: Optional[timedelta]
    custom_parameters: Dict[str, Any]


@dataclass
class ExecutionAnalytics:
    """Execution performance analytics."""
    analysis_id: str
    order_id: str
    symbol: str
    benchmark_price: Decimal
    arrival_price: Decimal
    vwap: Decimal
    twap: Decimal
    implementation_shortfall: Decimal
    market_impact: Decimal
    timing_cost: Decimal
    opportunity_cost: Decimal
    slippage: Decimal
    execution_cost_bps: Decimal
    fill_rate: float
    market_participation: float
    duration: timedelta
    venue_breakdown: Dict[str, float]
    analysis_timestamp: datetime


@dataclass
class RiskCheck:
    """Pre-trade risk check result."""
    check_id: str
    order_id: str
    check_type: str
    passed: bool
    risk_score: float
    details: Dict[str, Any]
    warnings: List[str]
    rejections: List[str]
    timestamp: datetime


@dataclass
class OrderRouting:
    """Order routing configuration."""
    routing_id: str
    symbol: str
    venue: str
    priority: int
    conditions: Dict[str, Any]
    is_active: bool
    latency_ms: Optional[float]
    fill_rate: Optional[float]
    cost_per_share: Optional[Decimal]
    market_share: Optional[float]


@dataclass
class ExecutionVenue:
    """Execution venue information."""
    venue_id: str
    venue_name: str
    venue_type: str  # "exchange", "dark_pool", "ecn", "market_maker"
    is_active: bool
    supported_symbols: List[str]
    supported_order_types: List[OrderType]
    latency_ms: float
    fees: Dict[str, Decimal]
    market_hours: Dict[str, Any]
    connection_status: str
    last_heartbeat: datetime


class OrderExecutionServiceInterface(ABC):
    """
    Order Management and Execution Service Interface
    
    Provides comprehensive order lifecycle management and execution capabilities:
    - Order creation, modification, and cancellation
    - Real-time order status tracking and execution reports
    - Algorithmic execution strategies (TWAP, VWAP, IS, etc.)
    - Smart order routing and venue optimization
    - Pre-trade and post-trade risk management
    - Execution analytics and performance measurement
    - Order book management and market data integration
    """
    
    # Order Lifecycle Management
    
    @abstractmethod
    async def create_order(
        self,
        portfolio_id: str,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        order_type: OrderType,
        price: Optional[Decimal] = None,
        stop_price: Optional[Decimal] = None,
        time_in_force: TimeInForce = TimeInForce.DAY,
        client_order_id: Optional[str] = None,
        execution_algorithm: Optional[ExecutionAlgorithm] = None,
        algorithm_parameters: Optional[Dict[str, Any]] = None,
        routing_destination: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Order:
        """
        Create new order.
        
        Args:
            portfolio_id: Portfolio identifier
            symbol: Security symbol
            side: Order side (buy/sell)
            quantity: Order quantity
            order_type: Type of order
            price: Limit price for limit orders
            stop_price: Stop price for stop orders
            time_in_force: Order time in force
            client_order_id: Client-specified order ID
            execution_algorithm: Algorithmic execution strategy
            algorithm_parameters: Algorithm configuration
            routing_destination: Preferred execution venue
            metadata: Additional order metadata
            
        Returns:
            Created order
        """
        pass
    
    @abstractmethod
    async def modify_order(
        self,
        order_id: str,
        quantity: Optional[Decimal] = None,
        price: Optional[Decimal] = None,
        stop_price: Optional[Decimal] = None,
        time_in_force: Optional[TimeInForce] = None
    ) -> Order:
        """
        Modify existing order.
        
        Args:
            order_id: Order identifier
            quantity: New order quantity
            price: New limit price
            stop_price: New stop price
            time_in_force: New time in force
            
        Returns:
            Modified order
        """
        pass
    
    @abstractmethod
    async def cancel_order(self, order_id: str, reason: Optional[str] = None) -> bool:
        """
        Cancel order.
        
        Args:
            order_id: Order identifier
            reason: Cancellation reason
            
        Returns:
            Success status
        """
        pass
    
    @abstractmethod
    async def cancel_all_orders(
        self,
        portfolio_id: Optional[str] = None,
        symbol: Optional[str] = None,
        side: Optional[OrderSide] = None
    ) -> List[str]:
        """
        Cancel multiple orders.
        
        Args:
            portfolio_id: Cancel orders for specific portfolio
            symbol: Cancel orders for specific symbol
            side: Cancel orders for specific side
            
        Returns:
            List of cancelled order IDs
        """
        pass
    
    @abstractmethod
    async def get_order(self, order_id: str) -> Optional[Order]:
        """
        Get order by ID.
        
        Args:
            order_id: Order identifier
            
        Returns:
            Order if found
        """
        pass
    
    @abstractmethod
    async def list_orders(
        self,
        portfolio_id: Optional[str] = None,
        symbol: Optional[str] = None,
        status: Optional[OrderStatus] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[Order]:
        """
        List orders with optional filters.
        
        Args:
            portfolio_id: Filter by portfolio
            symbol: Filter by symbol
            status: Filter by order status
            start_date: Filter from date
            end_date: Filter to date
            limit: Maximum number of orders
            
        Returns:
            List of orders
        """
        pass
    
    # Order Execution
    
    @abstractmethod
    async def submit_for_execution(self, order_id: str) -> bool:
        """
        Submit order for execution.
        
        Args:
            order_id: Order identifier
            
        Returns:
            Submission success status
        """
        pass
    
    @abstractmethod
    async def execute_algorithmic_order(
        self,
        config: AlgorithmicOrderConfig
    ) -> str:
        """
        Execute order using algorithmic strategy.
        
        Args:
            config: Algorithmic execution configuration
            
        Returns:
            Execution session ID
        """
        pass
    
    @abstractmethod
    async def get_execution_progress(
        self,
        order_id: str
    ) -> Dict[str, Any]:
        """
        Get algorithmic execution progress.
        
        Args:
            order_id: Order identifier
            
        Returns:
            Execution progress information
        """
        pass
    
    @abstractmethod
    async def pause_algorithmic_execution(
        self,
        order_id: str
    ) -> bool:
        """
        Pause algorithmic order execution.
        
        Args:
            order_id: Order identifier
            
        Returns:
            Success status
        """
        pass
    
    @abstractmethod
    async def resume_algorithmic_execution(
        self,
        order_id: str
    ) -> bool:
        """
        Resume algorithmic order execution.
        
        Args:
            order_id: Order identifier
            
        Returns:
            Success status
        """
        pass
    
    # Execution Reporting
    
    @abstractmethod
    async def get_executions(
        self,
        order_id: Optional[str] = None,
        portfolio_id: Optional[str] = None,
        symbol: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Execution]:
        """
        Get execution records.
        
        Args:
            order_id: Filter by order ID
            portfolio_id: Filter by portfolio
            symbol: Filter by symbol
            start_date: Filter from date
            end_date: Filter to date
            
        Returns:
            List of executions
        """
        pass
    
    @abstractmethod
    async def subscribe_execution_reports(
        self,
        callback: Callable[[ExecutionReport], None],
        portfolio_id: Optional[str] = None,
        symbol: Optional[str] = None
    ) -> str:
        """
        Subscribe to real-time execution reports.
        
        Args:
            callback: Function to call with execution reports
            portfolio_id: Filter by portfolio
            symbol: Filter by symbol
            
        Returns:
            Subscription ID
        """
        pass
    
    @abstractmethod
    async def unsubscribe_execution_reports(self, subscription_id: str) -> bool:
        """
        Unsubscribe from execution reports.
        
        Args:
            subscription_id: Subscription identifier
            
        Returns:
            Success status
        """
        pass
    
    # Risk Management
    
    @abstractmethod
    async def pre_trade_risk_check(
        self,
        order: Order,
        risk_rules: Optional[List[str]] = None
    ) -> RiskCheck:
        """
        Perform pre-trade risk check.
        
        Args:
            order: Order to check
            risk_rules: Specific risk rules to apply
            
        Returns:
            Risk check result
        """
        pass
    
    @abstractmethod
    async def real_time_risk_monitoring(
        self,
        portfolio_id: str,
        risk_limits: Dict[str, Any],
        callback: Callable[[Dict[str, Any]], None]
    ) -> str:
        """
        Start real-time risk monitoring.
        
        Args:
            portfolio_id: Portfolio to monitor
            risk_limits: Risk limits configuration
            callback: Function to call when limits breached
            
        Returns:
            Monitoring session ID
        """
        pass
    
    @abstractmethod
    async def calculate_position_risk(
        self,
        portfolio_id: str,
        symbol: str,
        additional_quantity: Decimal,
        price: Decimal
    ) -> Dict[str, Any]:
        """
        Calculate risk impact of additional position.
        
        Args:
            portfolio_id: Portfolio identifier
            symbol: Security symbol
            additional_quantity: Additional quantity to analyze
            price: Expected execution price
            
        Returns:
            Risk impact analysis
        """
        pass
    
    # Smart Order Routing
    
    @abstractmethod
    async def configure_routing_rules(
        self,
        symbol: str,
        routing_rules: List[OrderRouting]
    ) -> bool:
        """
        Configure smart order routing rules.
        
        Args:
            symbol: Symbol to configure routing for
            routing_rules: List of routing configurations
            
        Returns:
            Success status
        """
        pass
    
    @abstractmethod
    async def get_best_execution_venue(
        self,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        order_type: OrderType
    ) -> str:
        """
        Determine best execution venue for order.
        
        Args:
            symbol: Security symbol
            side: Order side
            quantity: Order quantity
            order_type: Order type
            
        Returns:
            Recommended venue ID
        """
        pass
    
    @abstractmethod
    async def get_venue_analytics(
        self,
        venue_id: str,
        symbol: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get execution venue analytics.
        
        Args:
            venue_id: Venue identifier
            symbol: Filter by symbol
            start_date: Analytics from date
            end_date: Analytics to date
            
        Returns:
            Venue performance analytics
        """
        pass
    
    @abstractmethod
    async def list_execution_venues(
        self,
        active_only: bool = True,
        symbol: Optional[str] = None
    ) -> List[ExecutionVenue]:
        """
        List available execution venues.
        
        Args:
            active_only: Only return active venues
            symbol: Filter by supported symbol
            
        Returns:
            List of execution venues
        """
        pass
    
    # Market Data Integration
    
    @abstractmethod
    async def get_order_book(
        self,
        symbol: str,
        depth: int = 10,
        venue: Optional[str] = None
    ) -> OrderBook:
        """
        Get current order book.
        
        Args:
            symbol: Security symbol
            depth: Order book depth
            venue: Specific venue (if None, consolidated book)
            
        Returns:
            Order book snapshot
        """
        pass
    
    @abstractmethod
    async def subscribe_order_book_updates(
        self,
        symbol: str,
        callback: Callable[[OrderBook], None],
        venue: Optional[str] = None
    ) -> str:
        """
        Subscribe to order book updates.
        
        Args:
            symbol: Security symbol
            callback: Function to call with updates
            venue: Specific venue to subscribe to
            
        Returns:
            Subscription ID
        """
        pass
    
    @abstractmethod
    async def get_market_impact_estimate(
        self,
        symbol: str,
        side: OrderSide,
        quantity: Decimal
    ) -> Dict[str, Decimal]:
        """
        Estimate market impact of order.
        
        Args:
            symbol: Security symbol
            side: Order side
            quantity: Order quantity
            
        Returns:
            Market impact estimates
        """
        pass
    
    # Execution Analytics
    
    @abstractmethod
    async def calculate_execution_analytics(
        self,
        order_id: str,
        benchmark_method: str = "arrival_price"
    ) -> ExecutionAnalytics:
        """
        Calculate execution performance analytics.
        
        Args:
            order_id: Order identifier
            benchmark_method: Benchmark calculation method
            
        Returns:
            Execution analytics
        """
        pass
    
    @abstractmethod
    async def generate_execution_report(
        self,
        portfolio_id: str,
        start_date: datetime,
        end_date: datetime,
        include_analytics: bool = True
    ) -> Dict[str, Any]:
        """
        Generate comprehensive execution report.
        
        Args:
            portfolio_id: Portfolio identifier
            start_date: Report start date
            end_date: Report end date
            include_analytics: Include execution analytics
            
        Returns:
            Execution report data
        """
        pass
    
    @abstractmethod
    async def benchmark_execution_performance(
        self,
        orders: List[str],
        benchmark_type: str = "vwap",
        time_window: Optional[timedelta] = None
    ) -> Dict[str, Any]:
        """
        Benchmark execution performance.
        
        Args:
            orders: List of order IDs to benchmark
            benchmark_type: Benchmark type (vwap, twap, arrival_price)
            time_window: Time window for benchmark calculation
            
        Returns:
            Benchmark analysis results
        """
        pass
    
    # Configuration & Administration
    
    @abstractmethod
    async def configure_execution_algorithm(
        self,
        algorithm: ExecutionAlgorithm,
        default_parameters: Dict[str, Any]
    ) -> bool:
        """
        Configure execution algorithm parameters.
        
        Args:
            algorithm: Execution algorithm
            default_parameters: Default algorithm parameters
            
        Returns:
            Success status
        """
        pass
    
    @abstractmethod
    async def get_algorithm_performance(
        self,
        algorithm: ExecutionAlgorithm,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Get algorithm performance metrics.
        
        Args:
            algorithm: Execution algorithm
            start_date: Analysis start date
            end_date: Analysis end date
            
        Returns:
            Algorithm performance metrics
        """
        pass
    
    @abstractmethod
    async def get_system_status(self) -> Dict[str, Any]:
        """
        Get order management system status.
        
        Returns:
            System status information
        """
        pass
    
    @abstractmethod
    async def get_execution_statistics(
        self,
        start_date: datetime,
        end_date: datetime,
        portfolio_id: Optional[str] = None,
        symbol: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get execution statistics.
        
        Args:
            start_date: Statistics start date
            end_date: Statistics end date
            portfolio_id: Filter by portfolio
            symbol: Filter by symbol
            
        Returns:
            Execution statistics
        """
        pass
    
    # Streaming Interfaces
    
    @abstractmethod
    async def get_order_stream(
        self,
        portfolio_id: Optional[str] = None,
        symbol: Optional[str] = None
    ) -> AsyncIterator[Order]:
        """
        Get streaming order updates.
        
        Args:
            portfolio_id: Filter by portfolio
            symbol: Filter by symbol
            
        Yields:
            Order updates
        """
        pass
    
    @abstractmethod
    async def get_execution_stream(
        self,
        portfolio_id: Optional[str] = None,
        symbol: Optional[str] = None
    ) -> AsyncIterator[Execution]:
        """
        Get streaming execution updates.
        
        Args:
            portfolio_id: Filter by portfolio
            symbol: Filter by symbol
            
        Yields:
            Execution updates
        """
        pass