"""
Trading Service Implementation

Implements the TradingServiceInterface providing comprehensive
trading operations including universe management, factor intervals,
and portfolio optimization.
"""

import logging
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any, Union
from decimal import Decimal


from ..interfaces.trading_service_interface import (
    TradingServiceInterface,
    UniverseDTO,
    UniverseMembershipDTO,
    FactorIntervalDTO,
    UniverseStateIntervalDTO,
    UniverseSearchCriteria,
    FactorSearchCriteria,
    PortfolioOptimizationRequest,
    PortfolioOptimizationResult,
    TradingOperationResult
)
from ...repositories.universe_dao import UniverseDAO
from ...repositories.universe_membership_dao import UniverseMembershipDAO
from ...repositories.factor_interval_dao import FactorIntervalDAO
# Optional universe state interval DAO import with fallback
try:
    from ...repositories.universe_state_interval_dao import UniverseStateIntervalDAO
except ImportError:
    UniverseStateIntervalDAO = None


class TradingServiceImpl(TradingServiceInterface):
    """
    Comprehensive trading service implementation.
    
    This service coordinates trading operations across multiple
    data sources and provides portfolio optimization capabilities.
    """
    
    def __init__(self, 
                 universe_dao: UniverseDAO,
                 universe_membership_dao: UniverseMembershipDAO,
                 factor_interval_dao: FactorIntervalDAO,
                 universe_state_interval_dao: Optional[UniverseStateIntervalDAO] = None,
                 market_data_service: Optional[Any] = None):
        self.universe_dao = universe_dao
        self.universe_membership_dao = universe_membership_dao
        self.factor_interval_dao = factor_interval_dao
        self.universe_state_interval_dao = universe_state_interval_dao
        self.market_data_service = market_data_service
        self.logger = logging.getLogger(__name__)
    
    # Universe Operations
    
    async def create_universe(self, universe: UniverseDTO) -> TradingOperationResult:
        """Create a new trading universe"""
        try:
            # Validate universe data
            if not universe.name:
                return TradingOperationResult(
                    success=False,
                    error_message="Universe name is required"
                )
            
            # Check if universe with same name already exists
            existing = await self.universe_dao.get_universe_by_name(universe.name)
            if existing:
                return TradingOperationResult(
                    success=False,
                    error_message=f"Universe with name '{universe.name}' already exists"
                )
            
            # Create universe
            universe_id = await self.universe_dao.create_universe(
                name=universe.name,
                description=universe.description
            )
            
            return TradingOperationResult(
                success=True,
                record_id=universe_id,
                created_count=1,
                details={'name': universe.name}
            )
            
        except Exception as e:
            self.logger.error(f"Error creating universe: {e}")
            return TradingOperationResult(
                success=False,
                error_message=f"Database error: {str(e)}"
            )
    
    async def get_universe_by_id(self, universe_id: int) -> Optional[UniverseDTO]:
        """Retrieve universe by ID"""
        try:
            record = await self.universe_dao.get_universe(universe_id)
            return self._dao_to_universe_dto(record) if record else None
        except Exception as e:
            self.logger.error(f"Error retrieving universe {universe_id}: {e}")
            return None
    
    async def get_universe_by_name(self, name: str) -> Optional[UniverseDTO]:
        """Retrieve universe by name"""
        try:
            record = await self.universe_dao.get_universe_by_name(name)
            return self._dao_to_universe_dto(record) if record else None
        except Exception as e:
            self.logger.error(f"Error retrieving universe '{name}': {e}")
            return None
    
    async def list_universes(self, criteria: Optional[UniverseSearchCriteria] = None) -> List[UniverseDTO]:
        """List universes based on search criteria"""
        try:
            records = await self.universe_dao.list_universes()
            universes = [self._dao_to_universe_dto(record) for record in records]
            
            # Apply filtering if criteria provided
            if criteria:
                if criteria.name_pattern:
                    pattern = criteria.name_pattern.lower()
                    universes = [u for u in universes if pattern in (u.name or '').lower()]
                
                # Apply limit
                if criteria.limit:
                    universes = universes[:criteria.limit]
            
            return universes
            
        except Exception as e:
            self.logger.error(f"Error listing universes: {e}")
            return []
    
    async def update_universe(self, universe: UniverseDTO) -> TradingOperationResult:
        """Update universe information"""
        try:
            if not universe.id:
                return TradingOperationResult(
                    success=False,
                    error_message="Universe ID is required for update"
                )
            
            success = await self.universe_dao.update_universe(
                universe_id=universe.id,
                name=universe.name,
                description=universe.description
            )
            
            if success:
                return TradingOperationResult(
                    success=True,
                    updated_count=1,
                    details={'universe_id': universe.id}
                )
            else:
                return TradingOperationResult(
                    success=False,
                    error_message="Universe not found or no changes made"
                )
                
        except Exception as e:
            self.logger.error(f"Error updating universe: {e}")
            return TradingOperationResult(
                success=False,
                error_message=f"Database error: {str(e)}"
            )
    
    async def delete_universe(self, universe_id: int) -> TradingOperationResult:
        """Delete universe (soft delete recommended)"""
        # Current DAO doesn't support delete - would need implementation
        return TradingOperationResult(
            success=False,
            error_message="Delete operation not implemented in current DAO"
        )
    
    # Universe Membership Operations
    
    async def add_universe_member(self, membership: UniverseMembershipDTO) -> TradingOperationResult:
        """Add instrument to universe"""
        try:
            if not membership.universe_id:
                return TradingOperationResult(
                    success=False,
                    error_message="Universe ID is required"
                )
            
            success = await self.universe_membership_dao.add_membership(
                universe_id=membership.universe_id,
                symbol=membership.symbol,
                instrument_id=membership.instrument_id,
                start_at=membership.start_date,
                end_at=membership.end_date
            )
            
            return TradingOperationResult(
                success=success,
                created_count=1 if success else 0,
                error_message=None if success else "Failed to add membership"
            )
            
        except Exception as e:
            self.logger.error(f"Error adding universe member: {e}")
            return TradingOperationResult(
                success=False,
                error_message=f"Database error: {str(e)}"
            )
    
    async def remove_universe_member(self, universe_id: int, 
                                   instrument_id: Optional[int] = None,
                                   symbol: Optional[str] = None,
                                   end_date: Optional[datetime] = None) -> TradingOperationResult:
        """Remove instrument from universe"""
        try:
            if instrument_id:
                # End membership by updating end_date
                await self.universe_membership_dao.update_membership_end(
                    universe_id=universe_id,
                    instrument_id=instrument_id,
                    end_at=end_date or datetime.utcnow()
                )
                return TradingOperationResult(
                    success=True,
                    updated_count=1
                )
            elif symbol:
                # Remove membership by symbol (requires start_at - simplified)
                success = await self.universe_membership_dao.remove_membership(
                    universe_id=universe_id,
                    symbol=symbol,
                    start_at=datetime.utcnow()  # This is a limitation of current DAO
                )
                return TradingOperationResult(
                    success=success,
                    deleted_count=1 if success else 0
                )
            else:
                return TradingOperationResult(
                    success=False,
                    error_message="Either instrument_id or symbol is required"
                )
                
        except Exception as e:
            self.logger.error(f"Error removing universe member: {e}")
            return TradingOperationResult(
                success=False,
                error_message=f"Database error: {str(e)}"
            )
    
    async def get_universe_members(self, universe_id: int, 
                                 as_of_date: Optional[datetime] = None) -> List[UniverseMembershipDTO]:
        """Get current or historical universe members"""
        try:
            if as_of_date:
                records = await self.universe_membership_dao.get_active_memberships(
                    universe_id=universe_id,
                    as_of=as_of_date
                )
            else:
                records = await self.universe_membership_dao.get_memberships_by_universe(
                    universe_id=universe_id
                )
            
            return [self._dao_to_membership_dto(record) for record in records]
            
        except Exception as e:
            self.logger.error(f"Error retrieving universe members: {e}")
            return []
    
    async def get_active_memberships(self, universe_id: int, 
                                   as_of_date: datetime) -> List[UniverseMembershipDTO]:
        """Get active universe memberships as of specific date"""
        try:
            records = await self.universe_membership_dao.get_active_memberships(
                universe_id=universe_id,
                as_of=as_of_date
            )
            return [self._dao_to_membership_dto(record) for record in records]
            
        except Exception as e:
            self.logger.error(f"Error retrieving active memberships: {e}")
            return []
    
    async def update_membership_batch(self, memberships: List[UniverseMembershipDTO]) -> TradingOperationResult:
        """Update multiple universe memberships in batch"""
        try:
            updated_count = 0
            created_count = 0
            errors = []
            
            for membership in memberships:
                if membership.id:
                    # Update existing - would need DAO enhancement
                    # For now, treat as creation
                    result = await self.add_universe_member(membership)
                    if result.success:
                        created_count += 1
                    else:
                        errors.append(f"Membership update failed: {result.error_message}")
                else:
                    # Create new
                    result = await self.add_universe_member(membership)
                    if result.success:
                        created_count += 1
                    else:
                        errors.append(f"Membership creation failed: {result.error_message}")
            
            return TradingOperationResult(
                success=True,
                created_count=created_count,
                updated_count=updated_count,
                details={'errors': errors[:5]}  # Limit error details
            )
            
        except Exception as e:
            self.logger.error(f"Error in batch membership update: {e}")
            return TradingOperationResult(
                success=False,
                error_message=f"Batch operation failed: {str(e)}"
            )
    
    # Factor Interval Operations
    
    async def create_factor_interval(self, factor: FactorIntervalDTO) -> TradingOperationResult:
        """Create a new factor interval record"""
        try:
            if not all([factor.universe_state_interval_id, factor.factor_name, factor.factor_value is not None]):
                return TradingOperationResult(
                    success=False,
                    error_message="Universe state interval ID, factor name, and value are required"
                )
            
            factor_id = await self.factor_interval_dao.create(
                universe_state_interval_id=factor.universe_state_interval_id,
                factor_name=factor.factor_name,
                factor_value=float(factor.factor_value)
            )
            
            return TradingOperationResult(
                success=True,
                record_id=factor_id,
                created_count=1
            )
            
        except Exception as e:
            self.logger.error(f"Error creating factor interval: {e}")
            return TradingOperationResult(
                success=False,
                error_message=f"Database error: {str(e)}"
            )
    
    async def get_factor_interval_by_id(self, factor_id: int) -> Optional[FactorIntervalDTO]:
        """Retrieve factor interval by ID"""
        try:
            record = await self.factor_interval_dao.get(factor_id)
            return self._dao_to_factor_dto(record) if record else None
        except Exception as e:
            self.logger.error(f"Error retrieving factor interval {factor_id}: {e}")
            return None
    
    async def list_factor_intervals(self, criteria: FactorSearchCriteria) -> List[FactorIntervalDTO]:
        """List factor intervals based on search criteria"""
        try:
            records = await self.factor_interval_dao.list(
                universe_state_interval_id=criteria.universe_state_interval_id
            )
            factors = [self._dao_to_factor_dto(record) for record in records]
            
            # Apply additional filtering
            if criteria.factor_names:
                factors = [f for f in factors if f.factor_name in criteria.factor_names]
            
            # Apply limit
            if criteria.limit:
                factors = factors[:criteria.limit]
            
            return factors
            
        except Exception as e:
            self.logger.error(f"Error listing factor intervals: {e}")
            return []
    
    async def create_factor_intervals_batch(self, factors: List[FactorIntervalDTO]) -> TradingOperationResult:
        """Create multiple factor intervals in batch"""
        try:
            created_count = 0
            skipped_count = 0
            errors = []
            
            for factor in factors:
                result = await self.create_factor_interval(factor)
                if result.success:
                    created_count += 1
                else:
                    skipped_count += 1
                    if result.error_message:
                        errors.append(result.error_message)
            
            return TradingOperationResult(
                success=True,
                created_count=created_count,
                details={
                    'total_processed': len(factors),
                    'skipped_count': skipped_count,
                    'errors': errors[:10]
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error in batch factor creation: {e}")
            return TradingOperationResult(
                success=False,
                error_message=f"Batch operation failed: {str(e)}"
            )
    
    async def delete_factor_interval(self, factor_id: int) -> TradingOperationResult:
        """Delete factor interval"""
        try:
            success = await self.factor_interval_dao.delete(factor_id)
            
            return TradingOperationResult(
                success=success,
                deleted_count=1 if success else 0,
                error_message=None if success else "Factor not found"
            )
            
        except Exception as e:
            self.logger.error(f"Error deleting factor interval: {e}")
            return TradingOperationResult(
                success=False,
                error_message=f"Database error: {str(e)}"
            )
    
    async def get_factors_by_universe_state(self, universe_state_interval_id: int) -> List[FactorIntervalDTO]:
        """Get all factors for a specific universe state interval"""
        try:
            records = await self.factor_interval_dao.list(
                universe_state_interval_id=universe_state_interval_id
            )
            return [self._dao_to_factor_dto(record) for record in records]
            
        except Exception as e:
            self.logger.error(f"Error retrieving factors for universe state {universe_state_interval_id}: {e}")
            return []
    
    # Universe State Operations (Simplified implementations)
    
    async def create_universe_state_interval(self, state: UniverseStateIntervalDTO) -> TradingOperationResult:
        """Create a new universe state interval"""
        # Would need universe_state_interval_dao implementation
        return TradingOperationResult(
            success=False,
            error_message="Universe state operations require enhanced DAO implementation"
        )
    
    async def get_universe_state_interval(self, state_id: int) -> Optional[UniverseStateIntervalDTO]:
        """Retrieve universe state interval by ID"""
        # Would need universe_state_interval_dao implementation
        return None
    
    async def get_universe_states_by_period(self, universe_id: int,
                                          start_time: datetime,
                                          end_time: datetime) -> List[UniverseStateIntervalDTO]:
        """Get universe states for a specific time period"""
        # Would need universe_state_interval_dao implementation
        return []
    
    # Portfolio Operations (Simplified implementations)
    
    async def optimize_portfolio(self, request: PortfolioOptimizationRequest) -> PortfolioOptimizationResult:
        """Optimize portfolio weights for given universe and constraints"""
        try:
            # Get universe members
            members = await self.get_active_memberships(
                universe_id=request.universe_id,
                as_of_date=request.target_date or datetime.utcnow()
            )
            
            if not members:
                # Return empty result
                return PortfolioOptimizationResult(
                    universe_id=request.universe_id,
                    weights={},
                    optimization_metrics={'error': 'No active members found'}
                )
            
            # Simplified equal-weight optimization
            weight = Decimal('1.0') / len(members)
            weights = {}
            
            for member in members:
                if member.symbol:
                    weights[member.symbol] = weight
            
            return PortfolioOptimizationResult(
                universe_id=request.universe_id,
                weights=weights,
                expected_return=None,  # Would need market data integration
                expected_risk=None,
                optimization_metrics={
                    'method': 'equal_weight',
                    'member_count': len(members)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error optimizing portfolio: {e}")
            return PortfolioOptimizationResult(
                universe_id=request.universe_id,
                weights={},
                optimization_metrics={'error': str(e)}
            )
    
    async def calculate_portfolio_metrics(self, universe_id: int,
                                        weights: Dict[str, Decimal],
                                        start_date: date,
                                        end_date: date) -> Dict[str, Any]:
        """Calculate portfolio performance metrics"""
        # Would require market data service integration
        return {
            'status': 'not_implemented',
            'message': 'Portfolio metrics calculation requires market data service integration'
        }
    
    async def get_universe_correlation_matrix(self, universe_id: int,
                                            start_date: date,
                                            end_date: date) -> Dict[str, Dict[str, float]]:
        """Calculate correlation matrix for universe members"""
        # Would require market data service integration
        return {}
    
    async def calculate_factor_exposures(self, universe_id: int,
                                       weights: Dict[str, Decimal],
                                       as_of_date: date) -> Dict[str, Decimal]:
        """Calculate factor exposures for portfolio weights"""
        # Would require factor model integration
        return {}
    
    # Analytics Operations (Simplified implementations)
    
    async def get_universe_analytics(self, universe_id: int,
                                   start_date: Optional[date] = None,
                                   end_date: Optional[date] = None) -> Dict[str, Any]:
        """Get comprehensive analytics for a universe"""
        try:
            universe = await self.get_universe_by_id(universe_id)
            if not universe:
                return {'error': 'Universe not found'}
            
            # Get current members
            members = await self.get_universe_members(universe_id)
            active_members = [m for m in members if m.end_date is None]
            
            return {
                'universe_id': universe_id,
                'universe_name': universe.name,
                'total_members': len(members),
                'active_members': len(active_members),
                'creation_date': universe.created_at.isoformat() if universe.created_at else None,
                'analysis_period': {
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting universe analytics: {e}")
            return {'error': str(e)}
    
    async def calculate_universe_returns(self, universe_id: int,
                                       start_date: date,
                                       end_date: date,
                                       weighting_scheme: str = "equal") -> Dict[str, Any]:
        """Calculate universe-level returns"""
        # Would require market data service integration
        return {
            'status': 'not_implemented',
            'message': 'Universe returns calculation requires market data service integration'
        }
    
    async def get_factor_performance(self, factor_names: List[str],
                                   start_date: date,
                                   end_date: date) -> Dict[str, Dict[str, Any]]:
        """Get factor performance analytics"""
        # Would require time series analysis of factor values
        return {}
    
    async def detect_universe_anomalies(self, universe_id: int,
                                      start_date: date,
                                      end_date: date) -> List[Dict[str, Any]]:
        """Detect anomalies in universe composition or performance"""
        anomalies = []
        
        try:
            # Check for membership anomalies
            members = await self.get_universe_members(universe_id)
            
            # Detect memberships without end dates that are very old
            cutoff_date = datetime.utcnow() - timedelta(days=365 * 2)  # 2 years
            
            for member in members:
                if member.end_date is None and member.start_date and member.start_date < cutoff_date:
                    anomalies.append({
                        'type': 'stale_membership',
                        'symbol': member.symbol,
                        'instrument_id': member.instrument_id,
                        'details': f'Membership active since {member.start_date} without end date',
                        'severity': 'medium'
                    })
            
            # Detect duplicate memberships
            symbol_counts = {}
            for member in members:
                if member.symbol and member.end_date is None:
                    symbol_counts[member.symbol] = symbol_counts.get(member.symbol, 0) + 1
            
            for symbol, count in symbol_counts.items():
                if count > 1:
                    anomalies.append({
                        'type': 'duplicate_membership',
                        'symbol': symbol,
                        'details': f'Symbol has {count} active memberships',
                        'severity': 'high'
                    })
        
        except Exception as e:
            self.logger.error(f"Error detecting anomalies: {e}")
            anomalies.append({
                'type': 'detection_error',
                'details': str(e),
                'severity': 'low'
            })
        
        return anomalies
    
    # Risk Management Operations (Simplified implementations)
    
    async def calculate_var(self, universe_id: int,
                          weights: Dict[str, Decimal],
                          confidence_level: float = 0.95,
                          lookback_days: int = 252) -> Dict[str, Any]:
        """Calculate Value at Risk for portfolio"""
        return {
            'status': 'not_implemented',
            'message': 'VaR calculation requires market data service integration'
        }
    
    async def stress_test_portfolio(self, universe_id: int,
                                  weights: Dict[str, Decimal],
                                  stress_scenarios: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run stress tests on portfolio"""
        return {
            'status': 'not_implemented',
            'message': 'Stress testing requires market data service integration'
        }
    
    async def calculate_portfolio_beta(self, universe_id: int,
                                     weights: Dict[str, Decimal],
                                     benchmark_symbol: str,
                                     lookback_days: int = 252) -> Optional[Decimal]:
        """Calculate portfolio beta relative to benchmark"""
        return None
    
    # Data Quality Operations
    
    async def validate_universe_data(self, universe_id: int,
                                   as_of_date: date) -> Dict[str, Any]:
        """Validate data quality for universe"""
        try:
            universe = await self.get_universe_by_id(universe_id)
            if not universe:
                return {
                    'valid': False,
                    'issues': ['Universe not found'],
                    'universe_id': universe_id
                }
            
            members = await self.get_active_memberships(universe_id, datetime.combine(as_of_date, datetime.min.time()))
            issues = []
            
            # Check for empty universe
            if not members:
                issues.append('Universe has no active members')
            
            # Check for members without symbols
            missing_symbol_count = sum(1 for m in members if not m.symbol)
            if missing_symbol_count > 0:
                issues.append(f'{missing_symbol_count} members missing symbol')
            
            # Check for members without instrument_id
            missing_instrument_count = sum(1 for m in members if not m.instrument_id)
            if missing_instrument_count > 0:
                issues.append(f'{missing_instrument_count} members missing instrument_id')
            
            return {
                'valid': len(issues) == 0,
                'issues': issues,
                'universe_id': universe_id,
                'member_count': len(members),
                'validation_date': as_of_date.isoformat(),
                'data_quality_score': max(0.0, 1.0 - (len(issues) * 0.2))
            }
            
        except Exception as e:
            self.logger.error(f"Error validating universe data: {e}")
            return {
                'valid': False,
                'issues': [f'Validation error: {str(e)}'],
                'universe_id': universe_id
            }
    
    async def get_universe_coverage_report(self, universe_id: int,
                                         start_date: date,
                                         end_date: date) -> Dict[str, Any]:
        """Get data coverage report for universe"""
        # Would require detailed data availability analysis
        return {
            'status': 'not_implemented',
            'message': 'Coverage reporting requires enhanced data tracking'
        }
    
    async def reconcile_universe_memberships(self, universe_id: int,
                                           target_date: date) -> TradingOperationResult:
        """Reconcile universe memberships against reference data"""
        # Would require external reference data source
        return TradingOperationResult(
            success=False,
            error_message="Reconciliation requires external reference data source"
        )
    
    # Utility Operations
    
    async def export_universe_data(self, universe_id: int,
                                 start_date: Optional[date] = None,
                                 end_date: Optional[date] = None,
                                 format: str = "csv") -> Union[str, Dict[str, Any]]:
        """Export universe data in specified format"""
        try:
            universe = await self.get_universe_by_id(universe_id)
            if not universe:
                return "Universe not found" if format == "csv" else {'error': 'Universe not found'}
            
            members = await self.get_universe_members(universe_id)
            
            if format.lower() == "json":
                return {
                    'universe': {
                        'id': universe.id,
                        'name': universe.name,
                        'description': universe.description
                    },
                    'members': [
                        {
                            'symbol': m.symbol,
                            'instrument_id': m.instrument_id,
                            'start_date': m.start_date.isoformat() if m.start_date else None,
                            'end_date': m.end_date.isoformat() if m.end_date else None
                        }
                        for m in members
                    ]
                }
            else:
                # CSV format
                lines = ['symbol,instrument_id,start_date,end_date']
                for m in members:
                    lines.append(f"{m.symbol or ''},{m.instrument_id or ''},"
                               f"{m.start_date.isoformat() if m.start_date else ''},"
                               f"{m.end_date.isoformat() if m.end_date else ''}")
                return '\n'.join(lines)
                
        except Exception as e:
            self.logger.error(f"Error exporting universe data: {e}")
            return f"Export error: {str(e)}" if format == "csv" else {'error': str(e)}
    
    async def clone_universe(self, source_universe_id: int,
                           new_name: str,
                           clone_memberships: bool = True) -> TradingOperationResult:
        """Clone an existing universe"""
        try:
            # Get source universe
            source_universe = await self.get_universe_by_id(source_universe_id)
            if not source_universe:
                return TradingOperationResult(
                    success=False,
                    error_message="Source universe not found"
                )
            
            # Create new universe
            new_universe = UniverseDTO(
                name=new_name,
                description=f"Clone of {source_universe.name}: {source_universe.description or ''}"
            )
            
            create_result = await self.create_universe(new_universe)
            if not create_result.success:
                return create_result
            
            new_universe_id = create_result.record_id
            
            # Clone memberships if requested
            if clone_memberships:
                source_members = await self.get_universe_members(source_universe_id)
                
                for member in source_members:
                    if member.end_date is None:  # Only clone active memberships
                        new_membership = UniverseMembershipDTO(
                            universe_id=new_universe_id,
                            symbol=member.symbol,
                            instrument_id=member.instrument_id,
                            start_date=datetime.utcnow()
                        )
                        await self.add_universe_member(new_membership)
            
            return TradingOperationResult(
                success=True,
                record_id=new_universe_id,
                created_count=1,
                details={
                    'source_universe_id': source_universe_id,
                    'new_universe_name': new_name
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error cloning universe: {e}")
            return TradingOperationResult(
                success=False,
                error_message=f"Clone operation failed: {str(e)}"
            )
    
    async def merge_universes(self, universe_ids: List[int],
                            target_name: str,
                            merge_strategy: str = "union") -> TradingOperationResult:
        """Merge multiple universes into a new universe"""
        try:
            if len(universe_ids) < 2:
                return TradingOperationResult(
                    success=False,
                    error_message="At least 2 universes required for merge"
                )
            
            # Create target universe
            target_universe = UniverseDTO(
                name=target_name,
                description=f"Merged universe from {len(universe_ids)} source universes"
            )
            
            create_result = await self.create_universe(target_universe)
            if not create_result.success:
                return create_result
            
            target_universe_id = create_result.record_id
            
            # Collect all members from source universes
            all_symbols = set()
            symbol_to_member = {}
            
            for universe_id in universe_ids:
                members = await self.get_universe_members(universe_id)
                for member in members:
                    if member.symbol and member.end_date is None:  # Active members only
                        if merge_strategy == "union" or member.symbol not in all_symbols:
                            all_symbols.add(member.symbol)
                            symbol_to_member[member.symbol] = member
            
            # Add merged members to target universe
            added_count = 0
            for symbol, member in symbol_to_member.items():
                new_membership = UniverseMembershipDTO(
                    universe_id=target_universe_id,
                    symbol=member.symbol,
                    instrument_id=member.instrument_id,
                    start_date=datetime.utcnow()
                )
                result = await self.add_universe_member(new_membership)
                if result.success:
                    added_count += 1
            
            return TradingOperationResult(
                success=True,
                record_id=target_universe_id,
                created_count=added_count + 1,  # +1 for the universe itself
                details={
                    'source_universe_count': len(universe_ids),
                    'merged_member_count': added_count,
                    'merge_strategy': merge_strategy
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error merging universes: {e}")
            return TradingOperationResult(
                success=False,
                error_message=f"Merge operation failed: {str(e)}"
            )
    
    # Helper Methods
    
    def _dao_to_universe_dto(self, dao_record: Dict[str, Any]) -> UniverseDTO:
        """Convert DAO record to UniverseDTO"""
        return UniverseDTO(
            id=dao_record.get('id'),
            name=dao_record.get('name'),
            description=dao_record.get('description'),
            created_at=dao_record.get('created_at'),
            updated_at=dao_record.get('updated_at')
        )
    
    def _dao_to_membership_dto(self, dao_record: Dict[str, Any]) -> UniverseMembershipDTO:
        """Convert DAO record to UniverseMembershipDTO"""
        return UniverseMembershipDTO(
            id=dao_record.get('id'),
            universe_id=dao_record.get('universe_id'),
            instrument_id=dao_record.get('instrument_id'),
            symbol=dao_record.get('symbol'),
            start_date=dao_record.get('start_date') or dao_record.get('start_at'),
            end_date=dao_record.get('end_date') or dao_record.get('end_at'),
            created_at=dao_record.get('created_at')
        )
    
    def _dao_to_factor_dto(self, dao_record: Dict[str, Any]) -> FactorIntervalDTO:
        """Convert DAO record to FactorIntervalDTO"""
        return FactorIntervalDTO(
            id=dao_record.get('id'),
            universe_state_interval_id=dao_record.get('universe_state_interval_id'),
            factor_name=dao_record.get('factor_name'),
            factor_value=Decimal(str(dao_record.get('factor_value'))) if dao_record.get('factor_value') is not None else None,
            created_at=dao_record.get('created_at')
        )