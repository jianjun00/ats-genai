"""
Market Data Service Interface

Defines the business logic interface for market data operations including
daily prices, fundamentals, and market data processing.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, date
from dataclasses import dataclass
from decimal import Decimal
import pandas as pd


@dataclass
class DailyPriceDTO:
    """Daily price data transfer object"""
    id: Optional[int] = None
    symbol: Optional[str] = None
    date: Optional[date] = None
    open: Optional[Decimal] = None
    high: Optional[Decimal] = None
    low: Optional[Decimal] = None
    close: Optional[Decimal] = None
    volume: Optional[int] = None
    adjusted_close: Optional[Decimal] = None
    market_cap: Optional[int] = None
    vendor: Optional[str] = None
    instrument_id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class FundamentalDTO:
    """Fundamental data transfer object"""
    id: Optional[int] = None
    instrument_id: Optional[int] = None
    date: Optional[date] = None
    market_cap: Optional[int] = None
    pe_ratio: Optional[Decimal] = None
    eps: Optional[Decimal] = None
    revenue: Optional[int] = None
    profit: Optional[int] = None
    debt: Optional[int] = None
    cash: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class MarketDataSearchCriteria:
    """Search criteria for market data"""
    symbols: Optional[List[str]] = None
    instrument_ids: Optional[List[int]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    vendors: Optional[List[str]] = None
    limit: Optional[int] = 1000
    offset: Optional[int] = None
    order_by: Optional[str] = "date"
    order_direction: Optional[str] = "DESC"


@dataclass
class PriceAnalysisResult:
    """Result of price analysis operations"""
    symbol: str
    start_date: date
    end_date: date
    total_return: Optional[Decimal] = None
    volatility: Optional[Decimal] = None
    max_drawdown: Optional[Decimal] = None
    sharpe_ratio: Optional[Decimal] = None
    beta: Optional[Decimal] = None
    correlation: Optional[Decimal] = None
    metrics: Optional[Dict[str, Any]] = None


@dataclass
class MarketDataOperationResult:
    """Result of market data operations"""
    success: bool
    record_id: Optional[int] = None
    created_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    error_message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


class MarketDataServiceInterface(ABC):
    """
    Interface for market data business operations.
    
    This service handles:
    1. Daily price data management and analysis
    2. Fundamental data operations
    3. Market data quality validation
    4. Cross-vendor data consolidation
    5. Price analytics and calculations
    """

    # Daily Price Operations
    
    @abstractmethod
    async def create_daily_price(self, price_data: DailyPriceDTO) -> MarketDataOperationResult:
        """Create a new daily price record with business validation"""
    
    @abstractmethod
    async def get_daily_price_by_id(self, price_id: int) -> Optional[DailyPriceDTO]:
        """Retrieve daily price by ID"""
    
    @abstractmethod
    async def get_daily_price(self, symbol: str, date: date, vendor: Optional[str] = None) -> Optional[DailyPriceDTO]:
        """Retrieve daily price for symbol and date"""
    
    @abstractmethod
    async def list_daily_prices(self, criteria: MarketDataSearchCriteria) -> List[DailyPriceDTO]:
        """List daily prices based on search criteria"""
    
    @abstractmethod
    async def update_daily_price(self, price_data: DailyPriceDTO) -> MarketDataOperationResult:
        """Update daily price record"""
    
    @abstractmethod
    async def create_daily_prices_batch(self, prices: List[DailyPriceDTO]) -> MarketDataOperationResult:
        """Create multiple daily price records in batch"""
    
    @abstractmethod
    async def get_price_history(self, 
                              symbol: str, 
                              start_date: date, 
                              end_date: date,
                              vendor: Optional[str] = None) -> List[DailyPriceDTO]:
        """Get price history for symbol and date range"""
    
    # Fundamental Data Operations
    
    @abstractmethod
    async def create_fundamental(self, fundamental: FundamentalDTO) -> MarketDataOperationResult:
        """Create a new fundamental data record"""
    
    @abstractmethod
    async def get_fundamental_by_id(self, fundamental_id: int) -> Optional[FundamentalDTO]:
        """Retrieve fundamental data by ID"""
    
    @abstractmethod
    async def get_fundamental(self, instrument_id: int, date: date) -> Optional[FundamentalDTO]:
        """Retrieve fundamental data for instrument and date"""
    
    @abstractmethod
    async def list_fundamentals(self, instrument_id: int) -> List[FundamentalDTO]:
        """List fundamental data for instrument"""
    
    @abstractmethod
    async def create_fundamentals_batch(self, fundamentals: List[FundamentalDTO]) -> MarketDataOperationResult:
        """Create multiple fundamental records in batch"""
    
    # Price Analytics Operations
    
    @abstractmethod
    async def calculate_returns(self, 
                              symbol: str, 
                              start_date: date, 
                              end_date: date,
                              return_type: str = "simple") -> Optional[Decimal]:
        """Calculate returns for symbol over date range"""
    
    @abstractmethod
    async def calculate_volatility(self, 
                                 symbol: str, 
                                 start_date: date, 
                                 end_date: date,
                                 window: int = 30) -> Optional[Decimal]:
        """Calculate volatility for symbol over date range"""
    
    @abstractmethod
    async def analyze_price_performance(self, 
                                      symbol: str, 
                                      start_date: date, 
                                      end_date: date,
                                      benchmark: Optional[str] = None) -> PriceAnalysisResult:
        """Comprehensive price performance analysis"""
    
    @abstractmethod
    async def get_correlation_matrix(self, 
                                   symbols: List[str], 
                                   start_date: date, 
                                   end_date: date) -> Dict[str, Dict[str, float]]:
        """Calculate correlation matrix for multiple symbols"""
    
    # Market Data Quality Operations
    
    @abstractmethod
    async def validate_price_data(self, price_data: DailyPriceDTO) -> Dict[str, Any]:
        """Validate price data quality and consistency"""
    
    @abstractmethod
    async def detect_price_anomalies(self, 
                                   symbol: str, 
                                   start_date: date, 
                                   end_date: date) -> List[Dict[str, Any]]:
        """Detect anomalies in price data"""
    
    @abstractmethod
    async def get_data_coverage_report(self, 
                                     symbols: Optional[List[str]] = None,
                                     start_date: Optional[date] = None,
                                     end_date: Optional[date] = None) -> Dict[str, Any]:
        """Get data coverage report for symbols and date range"""
    
    # Vendor Data Operations
    
    @abstractmethod
    async def consolidate_vendor_data(self, 
                                    symbol: str, 
                                    date: date) -> Optional[DailyPriceDTO]:
        """Consolidate data from multiple vendors for symbol and date"""
    
    @abstractmethod
    async def get_vendor_comparison(self, 
                                  symbol: str, 
                                  date: date) -> Dict[str, DailyPriceDTO]:
        """Compare data from different vendors for symbol and date"""
    
    @abstractmethod
    async def sync_vendor_data(self, 
                             vendor: str, 
                             symbols: Optional[List[str]] = None,
                             start_date: Optional[date] = None,
                             end_date: Optional[date] = None) -> MarketDataOperationResult:
        """Synchronize data from specific vendor"""
    
    # Market Statistics Operations
    
    @abstractmethod
    async def get_market_summary(self, date: Optional[date] = None) -> Dict[str, Any]:
        """Get market summary statistics for date"""
    
    @abstractmethod
    async def get_top_performers(self, 
                               date: date, 
                               metric: str = "return",
                               limit: int = 10) -> List[Dict[str, Any]]:
        """Get top performing securities by metric"""
    
    @abstractmethod
    async def get_market_breadth(self, date: date) -> Dict[str, Any]:
        """Get market breadth statistics"""
    
    # Data Export Operations
    
    @abstractmethod
    async def export_price_data(self, 
                              criteria: MarketDataSearchCriteria,
                              format: str = "csv") -> Union[str, pd.DataFrame]:
        """Export price data in specified format"""
    
    @abstractmethod
    async def get_ohlc_data(self, 
                          symbol: str, 
                          start_date: date, 
                          end_date: date) -> pd.DataFrame:
        """Get OHLC data as pandas DataFrame"""
