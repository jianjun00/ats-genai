"""
Unified Daily Price Validator

This module implements statistical validation and price reconciliation across
multiple data vendors (Polygon, Tiingo, FMP, AlphaVantage, yfinance) to produce
a single unified daily price with confidence scoring and validation metadata.
"""

import asyncio
import asyncpg
import logging
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, NamedTuple
from dataclasses import dataclass
from enum import Enum
import statistics
import yfinance as yf
import aiohttp
import os

from config.environment import Environment


class ValidationStatus(Enum):
    VALID = "valid"
    OUTLIER_STATISTICAL = "outlier_statistical"
    OUTLIER_VENDOR_DISAGREEMENT = "outlier_vendor_disagreement"
    MISSING_VENDOR_DATA = "missing_vendor_data"
    MANUAL_REVIEW = "manual_review"
    HOLIDAY_EXCLUDED = "holiday_excluded"
    CORPORATE_ACTION = "corporate_action"
    DATA_QUALITY_ISSUE = "data_quality_issue"


@dataclass
class VendorPrice:
    """Container for price data from a specific vendor"""
    vendor: str
    symbol: str
    date: date
    open_price: Optional[float]
    high_price: Optional[float]
    low_price: Optional[float]
    close: float
    adj_close: Optional[float]
    volume: int
    confidence: float = 1.0
    
    
@dataclass
class ValidationResult:
    """Result of price validation process"""
    is_valid: bool
    status: ValidationStatus
    confidence_score: float
    statistical_score: Optional[float]
    price_variance: Optional[float]
    validation_notes: str
    rejection_reason: Optional[str] = None


@dataclass
class UnifiedPrice:
    """Final unified price after validation and reconciliation"""
    instrument_id: int
    date: date
    open_price: Optional[float]
    high_price: Optional[float]
    low_price: Optional[float]
    close: float
    adj_close: Optional[float]
    volume: int
    primary_vendor: str
    secondary_vendors: List[str]
    vendor_count: int
    validation_result: ValidationResult
    vendor_prices: Dict[str, float]  # For audit trail


class UnifiedDailyPriceValidator:
    """
    Main class for unified daily price validation and reconciliation
    """
    
    def __init__(self, environment: Environment):
        self.env = environment
        self.logger = logging.getLogger(__name__)
        self.conn: Optional[asyncpg.Connection] = None
        
        # Statistical validation parameters
        self.outlier_threshold_sigma = 4.0  # 4-sigma outlier detection
        self.extreme_outlier_threshold_sigma = 6.0  # 6-sigma extreme outlier
        self.vendor_disagreement_threshold = 0.05  # 5% disagreement threshold
        self.min_vendors_for_validation = 2
        self.lookback_days_for_stats = 52  # ~1 trading quarter for statistical baseline
        
    async def connect(self):
        """Establish database connection"""
        if self.conn is None:
            self.conn = await asyncpg.connect(self.env.get_database_url())
            self.logger.info("✅ Connected to database for price validation")
    
    async def disconnect(self):
        """Close database connection"""
        if self.conn:
            await self.conn.close()
            self.conn = None
    
    async def fetch_vendor_prices(self, symbol: str, target_date: date) -> List[VendorPrice]:
        """
        Fetch prices from all available vendors for a given symbol and date
        """
        vendor_prices = []
        
        # Fetch from Polygon
        polygon_price = await self._fetch_polygon_price(symbol, target_date)
        if polygon_price:
            vendor_prices.append(polygon_price)
            
        # Fetch from Tiingo
        tiingo_price = await self._fetch_tiingo_price(symbol, target_date)
        if tiingo_price:
            vendor_prices.append(tiingo_price)
            
        # Fetch from FMP
        fmp_price = await self._fetch_fmp_price(symbol, target_date)
        if fmp_price:
            vendor_prices.append(fmp_price)
            
        # Fetch from AlphaVantage
        av_price = await self._fetch_alphavantage_price(symbol, target_date)
        if av_price:
            vendor_prices.append(av_price)
            
        return vendor_prices
    
    async def _fetch_polygon_price(self, symbol: str, target_date: date) -> Optional[VendorPrice]:
        """Fetch price from Polygon vendor table"""
        try:
            # First get instrument_id from symbol
            instrument_id = await self._get_instrument_id(symbol)
            if not instrument_id:
                return None
                
            query = """
                SELECT open_price, high_price, low_price, close, volume, vwap
                FROM dev_daily_prices_polygon 
                WHERE instrument_id = $1 AND date = $2
            """
            row = await self.conn.fetchrow(query, instrument_id, target_date)
            
            if row:
                return VendorPrice(
                    vendor="polygon",
                    symbol=symbol,
                    date=target_date,
                    open_price=float(row['open_price']) if row['open_price'] else None,
                    high_price=float(row['high_price']) if row['high_price'] else None,
                    low_price=float(row['low_price']) if row['low_price'] else None,
                    close=float(row['close']),
                    adj_close=None,  # Polygon doesn't store adj_close in current schema
                    volume=int(row['volume'])
                )
        except Exception as e:
            self.logger.warning(f"Error fetching Polygon price for {symbol} on {target_date}: {e}")
        return None
    
    async def _fetch_tiingo_price(self, symbol: str, target_date: date) -> Optional[VendorPrice]:
        """Fetch price from Tiingo vendor table"""
        try:
            instrument_id = await self._get_instrument_id(symbol)
            if not instrument_id:
                return None
                
            query = """
                SELECT open_price, high_price, low_price, close, adj_close, volume
                FROM dev_daily_prices_tiingo 
                WHERE instrument_id = $1 AND date = $2
            """
            row = await self.conn.fetchrow(query, instrument_id, target_date)
            
            if row:
                return VendorPrice(
                    vendor="tiingo",
                    symbol=symbol,
                    date=target_date,
                    open_price=float(row['open_price']) if row['open_price'] else None,
                    high_price=float(row['high_price']) if row['high_price'] else None,
                    low_price=float(row['low_price']) if row['low_price'] else None,
                    close=float(row['close']),
                    adj_close=float(row['adj_close']) if row['adj_close'] else None,
                    volume=int(row['volume'])
                )
        except Exception as e:
            self.logger.warning(f"Error fetching Tiingo price for {symbol} on {target_date}: {e}")
        return None
    
    async def _fetch_fmp_price(self, symbol: str, target_date: date) -> Optional[VendorPrice]:
        """Fetch price from FMP vendor table"""
        try:
            instrument_id = await self._get_instrument_id(symbol)
            if not instrument_id:
                return None
                
            query = """
                SELECT open_price, high_price, low_price, close, adj_close, volume
                FROM dev_daily_prices_fmp 
                WHERE instrument_id = $1 AND date = $2
            """
            row = await self.conn.fetchrow(query, instrument_id, target_date)
            
            if row:
                return VendorPrice(
                    vendor="fmp",
                    symbol=symbol,
                    date=target_date,
                    open_price=float(row['open_price']) if row['open_price'] else None,
                    high_price=float(row['high_price']) if row['high_price'] else None,
                    low_price=float(row['low_price']) if row['low_price'] else None,
                    close=float(row['close']),
                    adj_close=float(row['adj_close']) if row['adj_close'] else None,
                    volume=int(row['volume'])
                )
        except Exception as e:
            self.logger.warning(f"Error fetching FMP price for {symbol} on {target_date}: {e}")
        return None
    
    async def _fetch_alphavantage_price(self, symbol: str, target_date: date) -> Optional[VendorPrice]:
        """Fetch price from AlphaVantage vendor table"""
        try:
            instrument_id = await self._get_instrument_id(symbol)
            if not instrument_id:
                return None
                
            query = """
                SELECT open_price, high_price, low_price, close, adj_close, volume
                FROM dev_daily_prices_alphavantage 
                WHERE instrument_id = $1 AND date = $2
            """
            row = await self.conn.fetchrow(query, instrument_id, target_date)
            
            if row:
                return VendorPrice(
                    vendor="alphavantage",
                    symbol=symbol,
                    date=target_date,
                    open_price=float(row['open_price']) if row['open_price'] else None,
                    high_price=float(row['high_price']) if row['high_price'] else None,
                    low_price=float(row['low_price']) if row['low_price'] else None,
                    close=float(row['close']),
                    adj_close=float(row['adj_close']) if row['adj_close'] else None,
                    volume=int(row['volume'])
                )
        except Exception as e:
            self.logger.warning(f"Error fetching AlphaVantage price for {symbol} on {target_date}: {e}")
        return None
    
    async def fetch_yfinance_validation_price(self, symbol: str, target_date: date) -> Optional[VendorPrice]:
        """
        Fetch price from yfinance as validation source
        This is used as a secondary opinion when vendor prices disagree
        """
        try:
            # Get data for a small window around target date
            start_date = target_date - timedelta(days=5)
            end_date = target_date + timedelta(days=1)
            
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=start_date, end=end_date, auto_adjust=False)
            
            if not hist.empty and target_date.strftime('%Y-%m-%d') in hist.index.strftime('%Y-%m-%d'):
                row = hist.loc[hist.index.strftime('%Y-%m-%d') == target_date.strftime('%Y-%m-%d')].iloc[0]
                
                return VendorPrice(
                    vendor="yfinance",
                    symbol=symbol,
                    date=target_date,
                    open_price=float(row['Open']) if pd.notna(row['Open']) else None,
                    high_price=float(row['High']) if pd.notna(row['High']) else None,
                    low_price=float(row['Low']) if pd.notna(row['Low']) else None,
                    close=float(row['Close']),
                    adj_close=float(row['Adj Close']) if pd.notna(row['Adj Close']) else None,
                    volume=int(row['Volume'])
                )
        except Exception as e:
            self.logger.warning(f"Error fetching yfinance validation for {symbol} on {target_date}: {e}")
        return None
    
    async def _get_instrument_id(self, symbol: str) -> Optional[int]:
        """Get instrument_id for symbol"""
        try:
            query = "SELECT id FROM dev_instruments WHERE symbol = $1"
            row = await self.conn.fetchrow(query, symbol)
            return row['id'] if row else None
        except Exception as e:
            self.logger.error(f"Error getting instrument_id for {symbol}: {e}")
            return None
    
    async def calculate_historical_statistics(self, symbol: str, target_date: date) -> Dict:
        """
        Calculate historical price statistics for statistical validation
        """
        try:
            instrument_id = await self._get_instrument_id(symbol)
            if not instrument_id:
                return {}
            
            # Look back N days for statistical baseline (from all vendor tables)
            start_date = target_date - timedelta(days=self.lookback_days_for_stats)
            
            # Collect historical closes from unified table if available, otherwise from vendor tables
            query = """
                WITH vendor_prices AS (
                    SELECT date, close FROM dev_daily_prices_polygon 
                    WHERE instrument_id = $1 AND date BETWEEN $2 AND $3
                    UNION ALL
                    SELECT date, close FROM dev_daily_prices_tiingo 
                    WHERE instrument_id = $1 AND date BETWEEN $2 AND $3
                    UNION ALL
                    SELECT date, close FROM dev_daily_prices_fmp 
                    WHERE instrument_id = $1 AND date BETWEEN $2 AND $3
                )
                SELECT date, AVG(close) as avg_close
                FROM vendor_prices
                GROUP BY date
                ORDER BY date
            """
            
            rows = await self.conn.fetch(query, instrument_id, start_date, target_date)
            
            if len(rows) < 10:  # Need minimum history for statistical validation
                return {}
            
            prices = [float(row['avg_close']) for row in rows]
            log_returns = np.diff(np.log(prices))
            
            return {
                'mean_price': np.mean(prices),
                'std_price': np.std(prices),
                'mean_log_return': np.mean(log_returns),
                'std_log_return': np.std(log_returns),
                'price_history': prices[-10:],  # Last 10 days for context
                'sample_size': len(prices)
            }
        except Exception as e:
            self.logger.error(f"Error calculating historical statistics for {symbol}: {e}")
            return {}
    
    def validate_price_statistically(self, price: float, historical_stats: Dict, symbol: str) -> ValidationResult:
        """
        Perform statistical validation of a price against historical patterns
        """
        if not historical_stats or 'std_price' not in historical_stats:
            return ValidationResult(
                is_valid=True,
                status=ValidationStatus.VALID,
                confidence_score=0.7,  # Lower confidence without historical context
                statistical_score=None,
                price_variance=None,
                validation_notes=f"No historical statistics available for {symbol}",
            )
        
        mean_price = historical_stats['mean_price']
        std_price = historical_stats['std_price']
        
        # Calculate z-score
        z_score = abs((price - mean_price) / std_price) if std_price > 0 else 0
        
        # Determine validation result based on z-score
        if z_score > self.extreme_outlier_threshold_sigma:
            return ValidationResult(
                is_valid=False,
                status=ValidationStatus.OUTLIER_STATISTICAL,
                confidence_score=0.0,
                statistical_score=z_score,
                price_variance=None,
                validation_notes=f"Price ${price:.2f} is {z_score:.2f}-sigma from mean ${mean_price:.2f}",
                rejection_reason=f"Extreme statistical outlier: {z_score:.2f}-sigma deviation"
            )
        elif z_score > self.outlier_threshold_sigma:
            return ValidationResult(
                is_valid=False,
                status=ValidationStatus.MANUAL_REVIEW,
                confidence_score=0.3,
                statistical_score=z_score,
                price_variance=None,
                validation_notes=f"Price ${price:.2f} is {z_score:.2f}-sigma from mean ${mean_price:.2f}, flagged for review",
                rejection_reason=f"Statistical outlier requiring manual review: {z_score:.2f}-sigma"
            )
        else:
            # Price is within acceptable statistical range
            confidence = max(0.5, 1.0 - (z_score / self.outlier_threshold_sigma) * 0.3)
            return ValidationResult(
                is_valid=True,
                status=ValidationStatus.VALID,
                confidence_score=confidence,
                statistical_score=z_score,
                price_variance=None,
                validation_notes=f"Price ${price:.2f} is {z_score:.2f}-sigma from mean ${mean_price:.2f}",
            )
    
    def reconcile_vendor_prices(self, vendor_prices: List[VendorPrice], symbol: str) -> Tuple[ValidationResult, Optional[VendorPrice]]:
        """
        Reconcile prices from multiple vendors and determine consensus
        """
        if not vendor_prices:
            return ValidationResult(
                is_valid=False,
                status=ValidationStatus.MISSING_VENDOR_DATA,
                confidence_score=0.0,
                statistical_score=None,
                price_variance=None,
                validation_notes="No vendor data available",
                rejection_reason="No price data from any vendor"
            ), None
        
        if len(vendor_prices) < self.min_vendors_for_validation:
            # Single vendor - use with lower confidence
            vendor_price = vendor_prices[0]
            return ValidationResult(
                is_valid=True,
                status=ValidationStatus.VALID,
                confidence_score=0.6,  # Lower confidence for single vendor
                statistical_score=None,
                price_variance=None,
                validation_notes=f"Single vendor data from {vendor_price.vendor}",
            ), vendor_price
        
        # Multiple vendors - perform reconciliation
        close_prices = [vp.close for vp in vendor_prices]
        volumes = [vp.volume for vp in vendor_prices]
        
        # Calculate price statistics
        mean_price = statistics.mean(close_prices)
        price_variance = statistics.variance(close_prices) if len(close_prices) > 1 else 0.0
        max_deviation = max(abs(p - mean_price) for p in close_prices)
        max_deviation_pct = (max_deviation / mean_price) if mean_price > 0 else 0
        
        # Check for vendor disagreement
        if max_deviation_pct > self.vendor_disagreement_threshold:
            return ValidationResult(
                is_valid=False,
                status=ValidationStatus.OUTLIER_VENDOR_DISAGREEMENT,
                confidence_score=0.2,
                statistical_score=None,
                price_variance=price_variance,
                validation_notes=f"Vendor disagreement: max deviation {max_deviation_pct*100:.2f}%",
                rejection_reason=f"Vendor prices disagree by {max_deviation_pct*100:.2f}%"
            ), None
        
        # Create consensus price - use volume-weighted average if volumes available
        if all(v > 0 for v in volumes):
            total_volume = sum(volumes)
            consensus_price = sum(p * v for p, v in zip(close_prices, volumes)) / total_volume
            consensus_volume = int(statistics.mean(volumes))
        else:
            consensus_price = mean_price
            consensus_volume = int(statistics.mean(volumes)) if volumes else 0
        
        # Select primary vendor (highest volume or first if volumes equal)
        primary_vendor = max(vendor_prices, key=lambda vp: vp.volume).vendor
        
        # Create consensus vendor price
        consensus_vp = VendorPrice(
            vendor=primary_vendor,
            symbol=symbol,
            date=vendor_prices[0].date,
            open_price=statistics.mean([vp.open_price for vp in vendor_prices if vp.open_price]),
            high_price=max([vp.high_price for vp in vendor_prices if vp.high_price]),
            low_price=min([vp.low_price for vp in vendor_prices if vp.low_price]),
            close=consensus_price,
            adj_close=statistics.mean([vp.adj_close for vp in vendor_prices if vp.adj_close]),
            volume=consensus_volume
        )
        
        # High confidence for consensus
        confidence = min(1.0, 0.8 + (len(vendor_prices) * 0.05))  # Higher confidence with more vendors
        
        return ValidationResult(
            is_valid=True,
            status=ValidationStatus.VALID,
            confidence_score=confidence,
            statistical_score=None,
            price_variance=price_variance,
            validation_notes=f"Consensus from {len(vendor_prices)} vendors, max dev {max_deviation_pct*100:.2f}%",
        ), consensus_vp
    
    async def validate_and_unify_price(self, symbol: str, target_date: date) -> Optional[UnifiedPrice]:
        """
        Main method to validate and unify price data for a symbol on a specific date
        """
        try:
            self.logger.info(f"Validating price for {symbol} on {target_date}")
            
            # Fetch instrument_id
            instrument_id = await self._get_instrument_id(symbol)
            if not instrument_id:
                self.logger.warning(f"No instrument found for symbol {symbol}")
                return None
            
            # Step 1: Fetch prices from all vendors
            vendor_prices = await self.fetch_vendor_prices(symbol, target_date)
            
            # Step 2: Reconcile vendor prices
            reconcile_result, consensus_price = self.reconcile_vendor_prices(vendor_prices, symbol)
            
            if not reconcile_result.is_valid or not consensus_price:
                # If vendors disagree significantly, try external validation
                if reconcile_result.status == ValidationStatus.OUTLIER_VENDOR_DISAGREEMENT:
                    yf_price = await self.fetch_yfinance_validation_price(symbol, target_date)
                    if yf_price:
                        vendor_prices.append(yf_price)
                        # Re-reconcile with external validation
                        reconcile_result, consensus_price = self.reconcile_vendor_prices(vendor_prices, symbol)
                
                if not reconcile_result.is_valid or not consensus_price:
                    self.logger.warning(f"Failed to reconcile prices for {symbol} on {target_date}: {reconcile_result.validation_notes}")
                    return None
            
            # Step 3: Statistical validation against historical patterns
            historical_stats = await self.calculate_historical_statistics(symbol, target_date)
            stat_validation = self.validate_price_statistically(consensus_price.close, historical_stats, symbol)
            
            # Step 4: Combine validations - use most restrictive result
            if not stat_validation.is_valid:
                final_validation = stat_validation
            else:
                final_validation = ValidationResult(
                    is_valid=reconcile_result.is_valid and stat_validation.is_valid,
                    status=reconcile_result.status,
                    confidence_score=min(reconcile_result.confidence_score, stat_validation.confidence_score),
                    statistical_score=stat_validation.statistical_score,
                    price_variance=reconcile_result.price_variance,
                    validation_notes=f"{reconcile_result.validation_notes}; {stat_validation.validation_notes}",
                )
            
            # Step 5: Create unified price record
            vendor_names = [vp.vendor for vp in vendor_prices]
            primary_vendor = consensus_price.vendor
            secondary_vendors = [v for v in vendor_names if v != primary_vendor]
            
            unified_price = UnifiedPrice(
                instrument_id=instrument_id,
                date=target_date,
                open_price=consensus_price.open_price,
                high_price=consensus_price.high_price,
                low_price=consensus_price.low_price,
                close=consensus_price.close,
                adj_close=consensus_price.adj_close,
                volume=consensus_price.volume,
                primary_vendor=primary_vendor,
                secondary_vendors=secondary_vendors,
                vendor_count=len(vendor_prices),
                validation_result=final_validation,
                vendor_prices={vp.vendor: vp.close for vp in vendor_prices}
            )
            
            self.logger.info(f"✅ Unified price for {symbol} on {target_date}: ${consensus_price.close:.2f} "
                           f"({final_validation.status.value}, confidence: {final_validation.confidence_score:.2f})")
            
            return unified_price
            
        except Exception as e:
            self.logger.error(f"Error validating price for {symbol} on {target_date}: {e}")
            return None