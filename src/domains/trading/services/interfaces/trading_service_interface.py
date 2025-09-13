"""
Trading Service Interface

Defines the business logic interface for trading operations including
universe management, factor intervals, and portfolio optimization.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, date
from dataclasses import dataclass
from decimal import Decimal


@dataclass
class UniverseDTO:
    """Universe data transfer object"""
    id: Optional[int] = None
    name: Optional[str] = None
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class UniverseMembershipDTO:
    """Universe membership data transfer object"""
    id: Optional[int] = None
    universe_id: Optional[int] = None
    instrument_id: Optional[int] = None
    symbol: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    created_at: Optional[datetime] = None


@dataclass
class FactorIntervalDTO:
    """Factor interval data transfer object"""
    id: Optional[int] = None
    universe_state_interval_id: Optional[int] = None
    factor_name: Optional[str] = None
    factor_value: Optional[Decimal] = None
    created_at: Optional[datetime] = None


@dataclass
class UniverseStateIntervalDTO:
    """Universe state interval data transfer object"""
    id: Optional[int] = None
    universe_id: Optional[int] = None
    interval_start: Optional[datetime] = None
    interval_end: Optional[datetime] = None
    state_data: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None


@dataclass
class UniverseSearchCriteria:
    """Search criteria for universes"""
    name_pattern: Optional[str] = None
    active_only: Optional[bool] = True
    limit: Optional[int] = 100
    offset: Optional[int] = None
    order_by: Optional[str] = "name"
    order_direction: Optional[str] = "ASC"


@dataclass
class FactorSearchCriteria:
    """Search criteria for factor intervals"""
    universe_state_interval_id: Optional[int] = None
    factor_names: Optional[List[str]] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: Optional[int] = 1000
    offset: Optional[int] = None


@dataclass
class PortfolioOptimizationRequest:
    """Portfolio optimization request"""
    universe_id: int
    objective: str  # 'max_return', 'min_risk', 'max_sharpe'
    constraints: Optional[Dict[str, Any]] = None
    target_date: Optional[date] = None
    lookback_days: Optional[int] = 252
    factors: Optional[List[str]] = None


@dataclass
class PortfolioOptimizationResult:
    """Portfolio optimization result"""
    universe_id: int
    weights: Dict[str, Decimal]  # symbol -> weight
    expected_return: Optional[Decimal] = None
    expected_risk: Optional[Decimal] = None
    sharpe_ratio: Optional[Decimal] = None
    optimization_metrics: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None


@dataclass
class TradingOperationResult:
    """Result of trading operations"""
    success: bool
    record_id: Optional[int] = None
    created_count: int = 0
    updated_count: int = 0
    deleted_count: int = 0
    error_message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


class TradingServiceInterface(ABC):
    """
    Interface for trading business operations.
    
    This service handles:
    1. Universe creation and management
    2. Universe membership tracking
    3. Factor interval management
    4. Portfolio optimization
    5. Trading analytics and metrics
    """

    # Universe Operations
    
    @abstractmethod
    async def create_universe(self, universe: UniverseDTO) -> TradingOperationResult:
        """Create a new trading universe"""
    
    @abstractmethod
    async def get_universe_by_id(self, universe_id: int) -> Optional[UniverseDTO]:
        """Retrieve universe by ID"""
    
    @abstractmethod
    async def get_universe_by_name(self, name: str) -> Optional[UniverseDTO]:
        """Retrieve universe by name"""
    
    @abstractmethod
    async def list_universes(self, criteria: Optional[UniverseSearchCriteria] = None) -> List[UniverseDTO]:
        """List universes based on search criteria"""
    
    @abstractmethod
    async def update_universe(self, universe: UniverseDTO) -> TradingOperationResult:
        """Update universe information"""
    
    @abstractmethod
    async def delete_universe(self, universe_id: int) -> TradingOperationResult:
        """Delete universe (soft delete recommended)"""
    
    # Universe Membership Operations
    
    @abstractmethod
    async def add_universe_member(self, membership: UniverseMembershipDTO) -> TradingOperationResult:
        """Add instrument to universe"""
    
    @abstractmethod
    async def remove_universe_member(self, universe_id: int, 
                                   instrument_id: Optional[int] = None,
                                   symbol: Optional[str] = None,
                                   end_date: Optional[datetime] = None) -> TradingOperationResult:
        """Remove instrument from universe"""
    
    @abstractmethod
    async def get_universe_members(self, universe_id: int, 
                                 as_of_date: Optional[datetime] = None) -> List[UniverseMembershipDTO]:
        """Get current or historical universe members"""
    
    @abstractmethod
    async def get_active_memberships(self, universe_id: int, 
                                   as_of_date: datetime) -> List[UniverseMembershipDTO]:
        """Get active universe memberships as of specific date"""
    
    @abstractmethod
    async def update_membership_batch(self, memberships: List[UniverseMembershipDTO]) -> TradingOperationResult:
        """Update multiple universe memberships in batch"""
    
    # Factor Interval Operations
    
    @abstractmethod
    async def create_factor_interval(self, factor: FactorIntervalDTO) -> TradingOperationResult:
        """Create a new factor interval record"""
    
    @abstractmethod
    async def get_factor_interval_by_id(self, factor_id: int) -> Optional[FactorIntervalDTO]:
        """Retrieve factor interval by ID"""
    
    @abstractmethod
    async def list_factor_intervals(self, criteria: FactorSearchCriteria) -> List[FactorIntervalDTO]:
        """List factor intervals based on search criteria"""
    
    @abstractmethod
    async def create_factor_intervals_batch(self, factors: List[FactorIntervalDTO]) -> TradingOperationResult:
        """Create multiple factor intervals in batch"""
    
    @abstractmethod
    async def delete_factor_interval(self, factor_id: int) -> TradingOperationResult:
        """Delete factor interval"""
    
    @abstractmethod
    async def get_factors_by_universe_state(self, universe_state_interval_id: int) -> List[FactorIntervalDTO]:
        """Get all factors for a specific universe state interval"""
    
    # Universe State Operations
    
    @abstractmethod
    async def create_universe_state_interval(self, state: UniverseStateIntervalDTO) -> TradingOperationResult:
        """Create a new universe state interval"""
    
    @abstractmethod
    async def get_universe_state_interval(self, state_id: int) -> Optional[UniverseStateIntervalDTO]:
        """Retrieve universe state interval by ID"""
    
    @abstractmethod
    async def get_universe_states_by_period(self, universe_id: int,
                                          start_time: datetime,
                                          end_time: datetime) -> List[UniverseStateIntervalDTO]:
        """Get universe states for a specific time period"""
    
    # Portfolio Operations
    
    @abstractmethod
    async def optimize_portfolio(self, request: PortfolioOptimizationRequest) -> PortfolioOptimizationResult:
        """Optimize portfolio weights for given universe and constraints"""
    
    @abstractmethod
    async def calculate_portfolio_metrics(self, universe_id: int,
                                        weights: Dict[str, Decimal],
                                        start_date: date,
                                        end_date: date) -> Dict[str, Any]:
        """Calculate portfolio performance metrics"""
    
    @abstractmethod
    async def get_universe_correlation_matrix(self, universe_id: int,
                                            start_date: date,
                                            end_date: date) -> Dict[str, Dict[str, float]]:
        """Calculate correlation matrix for universe members"""
    
    @abstractmethod
    async def calculate_factor_exposures(self, universe_id: int,
                                       weights: Dict[str, Decimal],
                                       as_of_date: date) -> Dict[str, Decimal]:
        """Calculate factor exposures for portfolio weights"""
    
    # Analytics Operations
    
    @abstractmethod
    async def get_universe_analytics(self, universe_id: int,
                                   start_date: Optional[date] = None,
                                   end_date: Optional[date] = None) -> Dict[str, Any]:
        """Get comprehensive analytics for a universe"""
    
    @abstractmethod
    async def calculate_universe_returns(self, universe_id: int,
                                       start_date: date,
                                       end_date: date,
                                       weighting_scheme: str = "equal") -> Dict[str, Any]:
        """Calculate universe-level returns"""
    
    @abstractmethod
    async def get_factor_performance(self, factor_names: List[str],
                                   start_date: date,
                                   end_date: date) -> Dict[str, Dict[str, Any]]:
        """Get factor performance analytics"""
    
    @abstractmethod
    async def detect_universe_anomalies(self, universe_id: int,
                                      start_date: date,
                                      end_date: date) -> List[Dict[str, Any]]:
        """Detect anomalies in universe composition or performance"""
    
    # Risk Management Operations
    
    @abstractmethod
    async def calculate_var(self, universe_id: int,
                          weights: Dict[str, Decimal],
                          confidence_level: float = 0.95,
                          lookback_days: int = 252) -> Dict[str, Any]:
        """Calculate Value at Risk for portfolio"""
    
    @abstractmethod
    async def stress_test_portfolio(self, universe_id: int,
                                  weights: Dict[str, Decimal],
                                  stress_scenarios: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run stress tests on portfolio"""
    
    @abstractmethod
    async def calculate_portfolio_beta(self, universe_id: int,
                                     weights: Dict[str, Decimal],
                                     benchmark_symbol: str,
                                     lookback_days: int = 252) -> Optional[Decimal]:
        """Calculate portfolio beta relative to benchmark"""
    
    # Data Quality Operations
    
    @abstractmethod
    async def validate_universe_data(self, universe_id: int,
                                   as_of_date: date) -> Dict[str, Any]:
        """Validate data quality for universe"""
    
    @abstractmethod
    async def get_universe_coverage_report(self, universe_id: int,
                                         start_date: date,
                                         end_date: date) -> Dict[str, Any]:
        """Get data coverage report for universe"""
    
    @abstractmethod
    async def reconcile_universe_memberships(self, universe_id: int,
                                           target_date: date) -> TradingOperationResult:
        """Reconcile universe memberships against reference data"""
    
    # Utility Operations
    
    @abstractmethod
    async def export_universe_data(self, universe_id: int,
                                 start_date: Optional[date] = None,
                                 end_date: Optional[date] = None,
                                 format: str = "csv") -> Union[str, Dict[str, Any]]:
        """Export universe data in specified format"""
    
    @abstractmethod
    async def clone_universe(self, source_universe_id: int,
                           new_name: str,
                           clone_memberships: bool = True) -> TradingOperationResult:
        """Clone an existing universe"""
    
    @abstractmethod
    async def merge_universes(self, universe_ids: List[int],
                            target_name: str,
                            merge_strategy: str = "union") -> TradingOperationResult:
        """Merge multiple universes into a new universe"""
