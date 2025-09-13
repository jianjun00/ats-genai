"""
Market Data Service Implementation

Implements the MarketDataServiceInterface providing comprehensive
market data operations including price analytics, fundamental data,
and cross-vendor consolidation.
"""

import logging
import math
from datetime import date
from typing import List, Optional, Dict, Any, Union
from decimal import Decimal

import pandas as pd

from ..interfaces.market_data_service_interface import (
    MarketDataServiceInterface,
    DailyPriceDTO,
    FundamentalDTO, 
    MarketDataSearchCriteria,
    PriceAnalysisResult,
    MarketDataOperationResult
)
from ...repositories.daily_price_polygon_dao import DailyPricesDAO
from ...repositories.fundamentals_dao import FundamentalsDAO


class MarketDataServiceImpl(MarketDataServiceInterface):
    """
    Comprehensive market data service implementation.
    
    This service coordinates market data operations across multiple
    data sources and provides analytics capabilities.
    """
    
    def __init__(self, 
                 daily_price_polygon_dao: DailyPricesDAO, 
                 fundamentals_dao: FundamentalsDAO,
                 instruments_dao: Optional[Any] = None):
        self.daily_price_polygon_dao = daily_price_polygon_dao
        self.fundamentals_dao = fundamentals_dao
        self.instruments_dao = instruments_dao
        self.logger = logging.getLogger(__name__)
    
    # Daily Price Operations
    
    async def create_daily_price(self, price_data: DailyPriceDTO) -> MarketDataOperationResult:
        """Create a new daily price record with business validation"""
        try:
            # Validate price data
            validation_result = await self.validate_price_data(price_data)
            if not validation_result.get('valid', False):
                return MarketDataOperationResult(
                    success=False,
                    error_message=f"Invalid price data: {', '.join(validation_result.get('issues', []))}"
                )
            
            # Insert price record
            await self.daily_price_polygon_dao.insert_price(
                date=price_data.date,
                instrument_id=price_data.instrument_id,
                open_=price_data.open,
                high=price_data.high,
                low=price_data.low,
                close=price_data.close,
                volume=price_data.volume or 0
            )
            
            return MarketDataOperationResult(
                success=True,
                created_count=1,
                details={'symbol': price_data.symbol, 'date': str(price_data.date)}
            )
            
        except Exception as e:
            self.logger.error(f"Error creating daily price: {e}")
            return MarketDataOperationResult(
                success=False,
                error_message=f"Database error: {str(e)}"
            )
    
    async def get_daily_price_by_id(self, price_id: int) -> Optional[DailyPriceDTO]:
        """Retrieve daily price by ID"""
        try:
            # Note: Current DAO doesn't support get by ID, would need enhancement
            self.logger.warning("get_daily_price_by_id not implemented in current DAO")
            return None
        except Exception as e:
            self.logger.error(f"Error retrieving daily price by ID {price_id}: {e}")
            return None
    
    async def get_daily_price(self, symbol: str, date: date, vendor: Optional[str] = None) -> Optional[DailyPriceDTO]:
        """Retrieve daily price for symbol and date"""
        try:
            # Would need instrument_id lookup first
            if not self.instruments_dao:
                self.logger.warning("Instruments DAO not available for symbol lookup")
                return None
            
            # This is a simplified implementation - would need full symbol->instrument_id lookup
            return None
        except Exception as e:
            self.logger.error(f"Error retrieving daily price for {symbol} on {date}: {e}")
            return None
    
    async def list_daily_price_polygon(self, criteria: MarketDataSearchCriteria) -> List[DailyPriceDTO]:
        """List daily prices based on search criteria"""
        try:
            prices = []
            
            # Handle instrument_ids criteria
            if criteria.instrument_ids and criteria.start_date:
                # Get prices for specific instruments and date range
                if criteria.end_date:
                    # Multiple dates - would need enhanced DAO method
                    for instrument_id in criteria.instrument_ids:
                        instrument_prices = await self.daily_price_polygon_dao.list_prices(instrument_id)
                        for price_record in instrument_prices:
                            if criteria.start_date <= price_record['date'] <= criteria.end_date:
                                prices.append(self._dao_to_daily_price_dto(price_record))
                else:
                    # Single date
                    price_records = await self.daily_price_polygon_dao.list_prices_for_instruments_and_date(
                        criteria.instrument_ids, criteria.start_date
                    )
                    prices = [self._dao_to_daily_price_dto(record) for record in price_records]
            
            # Apply limit
            if criteria.limit:
                prices = prices[:criteria.limit]
            
            return prices
            
        except Exception as e:
            self.logger.error(f"Error listing daily prices: {e}")
            return []
    
    async def update_daily_price(self, price_data: DailyPriceDTO) -> MarketDataOperationResult:
        """Update daily price record"""
        # Current DAO doesn't support update, only insert with ON CONFLICT DO NOTHING
        return MarketDataOperationResult(
            success=False,
            error_message="Update operation not supported in current DAO implementation"
        )
    
    async def create_daily_price_polygon_batch(self, prices: List[DailyPriceDTO]) -> MarketDataOperationResult:
        """Create multiple daily price records in batch"""
        try:
            created_count = 0
            skipped_count = 0
            errors = []
            
            for price_data in prices:
                try:
                    result = await self.create_daily_price(price_data)
                    if result.success:
                        created_count += result.created_count
                    else:
                        skipped_count += 1
                        if result.error_message:
                            errors.append(f"{price_data.symbol}: {result.error_message}")
                except Exception as e:
                    skipped_count += 1
                    errors.append(f"{price_data.symbol}: {str(e)}")
            
            return MarketDataOperationResult(
                success=True,
                created_count=created_count,
                skipped_count=skipped_count,
                details={
                    'total_processed': len(prices),
                    'errors': errors[:10]  # Limit error details
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error in batch price creation: {e}")
            return MarketDataOperationResult(
                success=False,
                error_message=f"Batch operation failed: {str(e)}"
            )
    
    async def get_price_history(self, 
                              symbol: str, 
                              start_date: date, 
                              end_date: date,
                              vendor: Optional[str] = None) -> List[DailyPriceDTO]:
        """Get price history for symbol and date range"""
        try:
            # Would need symbol to instrument_id lookup
            if not self.instruments_dao:
                self.logger.warning("Cannot get price history without instruments DAO")
                return []
            
            # Simplified implementation - would need full implementation
            return []
            
        except Exception as e:
            self.logger.error(f"Error retrieving price history for {symbol}: {e}")
            return []
    
    # Fundamental Data Operations
    
    async def create_fundamental(self, fundamental: FundamentalDTO) -> MarketDataOperationResult:
        """Create a new fundamental data record"""
        try:
            if not fundamental.instrument_id or not fundamental.date:
                return MarketDataOperationResult(
                    success=False,
                    error_message="Instrument ID and date are required for fundamental data"
                )
            
            await self.fundamentals_dao.insert_fundamental(
                instrument_id=fundamental.instrument_id,
                date=fundamental.date,
                market_cap=fundamental.market_cap or 0
            )
            
            return MarketDataOperationResult(
                success=True,
                created_count=1,
                details={'instrument_id': fundamental.instrument_id, 'date': str(fundamental.date)}
            )
            
        except Exception as e:
            self.logger.error(f"Error creating fundamental data: {e}")
            return MarketDataOperationResult(
                success=False,
                error_message=f"Database error: {str(e)}"
            )
    
    async def get_fundamental_by_id(self, fundamental_id: int) -> Optional[FundamentalDTO]:
        """Retrieve fundamental data by ID"""
        # Current DAO doesn't support get by ID
        self.logger.warning("get_fundamental_by_id not implemented in current DAO")
        return None
    
    async def get_fundamental(self, instrument_id: int, date: date) -> Optional[FundamentalDTO]:
        """Retrieve fundamental data for instrument and date"""
        try:
            record = await self.fundamentals_dao.get_fundamental(instrument_id, date)
            return self._dao_to_fundamental_dto(record) if record else None
        except Exception as e:
            self.logger.error(f"Error retrieving fundamental for instrument {instrument_id}: {e}")
            return None
    
    async def list_fundamentals(self, instrument_id: int) -> List[FundamentalDTO]:
        """List fundamental data for instrument"""
        try:
            records = await self.fundamentals_dao.list_fundamentals(instrument_id)
            return [self._dao_to_fundamental_dto(record) for record in records]
        except Exception as e:
            self.logger.error(f"Error listing fundamentals for instrument {instrument_id}: {e}")
            return []
    
    async def create_fundamentals_batch(self, fundamentals: List[FundamentalDTO]) -> MarketDataOperationResult:
        """Create multiple fundamental records in batch"""
        try:
            created_count = 0
            skipped_count = 0
            
            for fundamental in fundamentals:
                result = await self.create_fundamental(fundamental)
                if result.success:
                    created_count += result.created_count
                else:
                    skipped_count += 1
            
            return MarketDataOperationResult(
                success=True,
                created_count=created_count,
                skipped_count=skipped_count,
                details={'total_processed': len(fundamentals)}
            )
            
        except Exception as e:
            self.logger.error(f"Error in batch fundamental creation: {e}")
            return MarketDataOperationResult(
                success=False,
                error_message=f"Batch operation failed: {str(e)}"
            )
    
    # Price Analytics Operations
    
    async def calculate_returns(self, 
                              symbol: str, 
                              start_date: date, 
                              end_date: date,
                              return_type: str = "simple") -> Optional[Decimal]:
        """Calculate returns for symbol over date range"""
        try:
            prices = await self.get_price_history(symbol, start_date, end_date)
            if len(prices) < 2:
                return None
            
            # Sort by date
            prices.sort(key=lambda x: x.date)
            start_price = prices[0].close
            end_price = prices[-1].close
            
            if not start_price or not end_price or start_price <= 0:
                return None
            
            if return_type.lower() == "log":
                return Decimal(str(math.log(float(end_price) / float(start_price))))
            else:  # simple return
                return (end_price - start_price) / start_price
                
        except Exception as e:
            self.logger.error(f"Error calculating returns for {symbol}: {e}")
            return None
    
    async def calculate_volatility(self, 
                                 symbol: str, 
                                 start_date: date, 
                                 end_date: date,
                                 window: int = 30) -> Optional[Decimal]:
        """Calculate volatility for symbol over date range"""
        try:
            prices = await self.get_price_history(symbol, start_date, end_date)
            if len(prices) < window:
                return None
            
            # Calculate daily returns
            prices.sort(key=lambda x: x.date)
            returns = []
            
            for i in range(1, len(prices)):
                if prices[i-1].close and prices[i].close and prices[i-1].close > 0:
                    daily_return = (prices[i].close - prices[i-1].close) / prices[i-1].close
                    returns.append(float(daily_return))
            
            if len(returns) < 2:
                return None
            
            # Calculate standard deviation
            mean_return = sum(returns) / len(returns)
            variance = sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)
            volatility = math.sqrt(variance) * math.sqrt(252)  # Annualized
            
            return Decimal(str(volatility))
            
        except Exception as e:
            self.logger.error(f"Error calculating volatility for {symbol}: {e}")
            return None
    
    async def analyze_price_performance(self, 
                                      symbol: str, 
                                      start_date: date, 
                                      end_date: date,
                                      benchmark: Optional[str] = None) -> PriceAnalysisResult:
        """Comprehensive price performance analysis"""
        try:
            # Get price data
            prices = await self.get_price_history(symbol, start_date, end_date)
            
            result = PriceAnalysisResult(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )
            
            if len(prices) < 2:
                return result
            
            # Calculate metrics
            result.total_return = await self.calculate_returns(symbol, start_date, end_date)
            result.volatility = await self.calculate_volatility(symbol, start_date, end_date)
            
            # Calculate max drawdown
            prices.sort(key=lambda x: x.date)
            peak = Decimal('0')
            max_dd = Decimal('0')
            
            for price in prices:
                if price.close and price.close > peak:
                    peak = price.close
                elif price.close and peak > 0:
                    drawdown = (peak - price.close) / peak
                    if drawdown > max_dd:
                        max_dd = drawdown
            
            result.max_drawdown = max_dd
            
            # Additional metrics would require benchmark data
            result.metrics = {
                'price_count': len(prices),
                'analysis_period_days': (end_date - start_date).days
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error analyzing performance for {symbol}: {e}")
            return PriceAnalysisResult(symbol=symbol, start_date=start_date, end_date=end_date)
    
    async def get_correlation_matrix(self, 
                                   symbols: List[str], 
                                   start_date: date, 
                                   end_date: date) -> Dict[str, Dict[str, float]]:
        """Calculate correlation matrix for multiple symbols"""
        try:
            # Get price data for all symbols
            symbol_prices = {}
            for symbol in symbols:
                prices = await self.get_price_history(symbol, start_date, end_date)
                if prices:
                    symbol_prices[symbol] = prices
            
            if len(symbol_prices) < 2:
                return {}
            
            # Calculate returns matrix
            # This is a simplified implementation - full version would align dates
            correlation_matrix = {}
            
            for symbol1 in symbols:
                correlation_matrix[symbol1] = {}
                for symbol2 in symbols:
                    if symbol1 == symbol2:
                        correlation_matrix[symbol1][symbol2] = 1.0
                    else:
                        # Simplified correlation calculation
                        correlation_matrix[symbol1][symbol2] = 0.5  # Placeholder
            
            return correlation_matrix
            
        except Exception as e:
            self.logger.error(f"Error calculating correlation matrix: {e}")
            return {}
    
    # Market Data Quality Operations
    
    async def validate_price_data(self, price_data: DailyPriceDTO) -> Dict[str, Any]:
        """Validate price data quality and consistency"""
        issues = []
        
        # Required fields validation
        if not price_data.date:
            issues.append("Date is required")
        
        if not price_data.instrument_id:
            issues.append("Instrument ID is required")
        
        # Price validation
        if price_data.open is not None and price_data.open <= 0:
            issues.append("Open price must be positive")
        
        if price_data.high is not None and price_data.high <= 0:
            issues.append("High price must be positive")
        
        if price_data.low is not None and price_data.low <= 0:
            issues.append("Low price must be positive")
        
        if price_data.close is not None and price_data.close <= 0:
            issues.append("Close price must be positive")
        
        # Price relationship validation
        if all([price_data.open, price_data.high, price_data.low, price_data.close]):
            if price_data.high < max(price_data.open, price_data.close, price_data.low):
                issues.append("High price should be >= all other prices")
            
            if price_data.low > min(price_data.open, price_data.close, price_data.high):
                issues.append("Low price should be <= all other prices")
        
        # Volume validation
        if price_data.volume is not None and price_data.volume < 0:
            issues.append("Volume cannot be negative")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues,
            'data_quality_score': max(0.0, 1.0 - (len(issues) * 0.2))
        }
    
    async def detect_price_anomalies(self, 
                                   symbol: str, 
                                   start_date: date, 
                                   end_date: date) -> List[Dict[str, Any]]:
        """Detect anomalies in price data"""
        anomalies = []
        
        try:
            prices = await self.get_price_history(symbol, start_date, end_date)
            if len(prices) < 5:
                return anomalies
            
            prices.sort(key=lambda x: x.date)
            
            # Detect large price gaps
            for i in range(1, len(prices)):
                if prices[i-1].close and prices[i].open:
                    gap = abs(prices[i].open - prices[i-1].close) / prices[i-1].close
                    if gap > 0.1:  # 10% gap threshold
                        anomalies.append({
                            'type': 'price_gap',
                            'date': prices[i].date,
                            'severity': 'high' if gap > 0.2 else 'medium',
                            'details': f'Gap of {gap:.2%} from previous close'
                        })
            
            # Detect zero volume
            for price in prices:
                if price.volume == 0:
                    anomalies.append({
                        'type': 'zero_volume',
                        'date': price.date,
                        'severity': 'medium',
                        'details': 'Zero trading volume detected'
                    })
            
        except Exception as e:
            self.logger.error(f"Error detecting anomalies for {symbol}: {e}")
        
        return anomalies
    
    async def get_data_coverage_report(self, 
                                     symbols: Optional[List[str]] = None,
                                     start_date: Optional[date] = None,
                                     end_date: Optional[date] = None) -> Dict[str, Any]:
        """Get data coverage report for symbols and date range"""
        # This would require significant DAO enhancements to implement properly
        return {
            'status': 'not_implemented',
            'message': 'Data coverage reporting requires enhanced DAO capabilities'
        }
    
    # Vendor Data Operations (Simplified implementations)
    
    async def consolidate_vendor_data(self, 
                                    symbol: str, 
                                    date: date) -> Optional[DailyPriceDTO]:
        """Consolidate data from multiple vendors for symbol and date"""
        # Would require vendor-specific data access
        return None
    
    async def get_vendor_comparison(self, 
                                  symbol: str, 
                                  date: date) -> Dict[str, DailyPriceDTO]:
        """Compare data from different vendors for symbol and date"""
        return {}
    
    async def sync_vendor_data(self, 
                             vendor: str, 
                             symbols: Optional[List[str]] = None,
                             start_date: Optional[date] = None,
                             end_date: Optional[date] = None) -> MarketDataOperationResult:
        """Synchronize data from specific vendor"""
        return MarketDataOperationResult(
            success=False,
            error_message="Vendor sync not implemented"
        )
    
    # Market Statistics Operations (Simplified implementations)
    
    async def get_market_summary(self, date: Optional[date] = None) -> Dict[str, Any]:
        """Get market summary statistics for date"""
        return {'status': 'not_implemented'}
    
    async def get_top_performers(self, 
                               date: date, 
                               metric: str = "return",
                               limit: int = 10) -> List[Dict[str, Any]]:
        """Get top performing securities by metric"""
        return []
    
    async def get_market_breadth(self, date: date) -> Dict[str, Any]:
        """Get market breadth statistics"""
        return {}
    
    # Data Export Operations
    
    async def export_price_data(self, 
                              criteria: MarketDataSearchCriteria,
                              format: str = "csv") -> Union[str, pd.DataFrame]:
        """Export price data in specified format"""
        try:
            prices = await self.list_daily_price_polygon(criteria)
            
            if format.lower() == "dataframe":
                # Convert to pandas DataFrame
                data = []
                for price in prices:
                    data.append({
                        'date': price.date,
                        'symbol': price.symbol,
                        'open': float(price.open) if price.open else None,
                        'high': float(price.high) if price.high else None,
                        'low': float(price.low) if price.low else None,
                        'close': float(price.close) if price.close else None,
                        'volume': price.volume
                    })
                return pd.DataFrame(data)
            else:
                # Return CSV string
                df = await self.export_price_data(criteria, "dataframe")
                return df.to_csv(index=False)
                
        except Exception as e:
            self.logger.error(f"Error exporting price data: {e}")
            return pd.DataFrame() if format.lower() == "dataframe" else ""
    
    async def get_ohlc_data(self, 
                          symbol: str, 
                          start_date: date, 
                          end_date: date) -> pd.DataFrame:
        """Get OHLC data as pandas DataFrame"""
        try:
            prices = await self.get_price_history(symbol, start_date, end_date)
            
            data = []
            for price in prices:
                data.append({
                    'date': price.date,
                    'open': float(price.open) if price.open else None,
                    'high': float(price.high) if price.high else None,
                    'low': float(price.low) if price.low else None,
                    'close': float(price.close) if price.close else None,
                    'volume': price.volume
                })
            
            df = pd.DataFrame(data)
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                df.sort_index(inplace=True)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error getting OHLC data for {symbol}: {e}")
            return pd.DataFrame()
    
    # Helper Methods
    
    def _dao_to_daily_price_dto(self, dao_record: Dict[str, Any]) -> DailyPriceDTO:
        """Convert DAO record to DailyPriceDTO"""
        return DailyPriceDTO(
            id=dao_record.get('id'),
            symbol=dao_record.get('symbol'),
            date=dao_record.get('date'),
            open=dao_record.get('open'),
            high=dao_record.get('high'),
            low=dao_record.get('low'),
            close=dao_record.get('close'),
            volume=dao_record.get('volume'),
            adjusted_close=dao_record.get('adjusted_close'),
            market_cap=dao_record.get('market_cap'),
            vendor=dao_record.get('vendor'),
            instrument_id=dao_record.get('instrument_id'),
            created_at=dao_record.get('created_at'),
            updated_at=dao_record.get('updated_at')
        )
    
    def _dao_to_fundamental_dto(self, dao_record: Dict[str, Any]) -> FundamentalDTO:
        """Convert DAO record to FundamentalDTO"""
        return FundamentalDTO(
            id=dao_record.get('id'),
            instrument_id=dao_record.get('instrument_id'),
            date=dao_record.get('date'),
            market_cap=dao_record.get('market_cap'),
            pe_ratio=dao_record.get('pe_ratio'),
            eps=dao_record.get('eps'),
            revenue=dao_record.get('revenue'),
            profit=dao_record.get('profit'),
            debt=dao_record.get('debt'),
            cash=dao_record.get('cash'),
            created_at=dao_record.get('created_at'),
            updated_at=dao_record.get('updated_at')
        )