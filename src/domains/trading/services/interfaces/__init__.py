"""
Trading Service Interfaces Package

This package contains the interface definitions for trading services.
"""

from .trading_service_interface import (
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

__all__ = [
    'TradingServiceInterface',
    'UniverseDTO',
    'UniverseMembershipDTO',
    'FactorIntervalDTO',
    'UniverseStateIntervalDTO',
    'UniverseSearchCriteria',
    'FactorSearchCriteria',
    'PortfolioOptimizationRequest',
    'PortfolioOptimizationResult',
    'TradingOperationResult'
]