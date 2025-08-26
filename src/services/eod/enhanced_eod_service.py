#!/usr/bin/env python3
"""
ATS Enhanced End-of-Day Price Service
Consolidates and enhances existing daily price collection from Tiingo, Polygon, FMP
"""

import gin
import asyncio
import logging
import os
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Set
from fastapi import FastAPI, BackgroundTasks, HTTPException
from dataclasses import dataclass
from enum import Enum
import aiohttp
import pandas as pd

# Reuse existing ATS framework
from config.environment import Environment, EnvironmentType
from dao.base.base_dao import BaseDAO
from dao.market_data.daily_prices_dao import DailyPricesDAO
from dao.instrument_xrefs_dao import InstrumentXrefsDAO
from core.logging.logger_config import get_logger
from calendars.exchange_calendar import ExchangeCalendar

# Reuse existing vendor configurations
from config.polygon import POLYGON_API_KEY
from market_data.eod.daily_price_tiingo import TIINGO_API_KEY, tiingo_url
from market_data.eod.daily_price_polygon import download_prices_polygon

# Configure Gin
try:
    gin.parse_config_file('config/app_dev.gin')
except Exception as e:
    print(f'[WARN] Could not parse gin config: {e}')

logger = get_logger(__name__)

class DataQuality(Enum):
    EXCELLENT = 1.0
    GOOD = 0.8
    ACCEPTABLE = 0.6
    POOR = 0.4
    MISSING = 0.0

@dataclass
class EODPrice:
    """Standardized EOD price data structure"""
    symbol: str
    date: date
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    adjusted_close: float
    volume: int
    market_cap: Optional[int]
    vendor: str
    quality: DataQuality = DataQuality.GOOD
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for DAO insertion"""
        return {
            'symbol': self.symbol,
            'date': self.date,
            'open': self.open_price,
            'high': self.high_price,
            'low': self.low_price,
            'close': self.close_price,
            'adjusted_close': self.adjusted_close,
            'volume': self.volume,
            'market_cap': self.market_cap,
            'vendor': self.vendor,
            'instrument_id': None  # Will be resolved by DAO
        }

class EnhancedEODDAO(BaseDAO):
    """Enhanced DAO for EOD data - extends existing patterns with improvements"""
    
    def __init__(self, env: Environment):
        super().__init__("daily_prices")
        self.env = env
        self.daily_prices_dao = DailyPricesDAO()
        self.instrument_dao = InstrumentXrefsDAO(env)
        self.logger = get_logger(f"{__name__}.EOD")
    
    def get_schema(self) -> Dict[str, any]:
        """Reuse existing daily prices schema"""
        return self.daily_prices_dao.get_schema()
    
    async def insert_eod_prices(self, prices: List[EODPrice]) -> Dict[str, int]:
        """Enhanced insertion with quality tracking and conflict resolution"""
        if not prices:
            return {"inserted": 0, "updated": 0, "errors": 0}
        
        results = {"inserted": 0, "updated": 0, "errors": 0}
        
        for price in prices:
            try:
                # Resolve instrument_id using existing pattern
                instrument_id = await self.instrument_dao.resolve_instrument_id(price.symbol)
                if not instrument_id:
                    self.logger.warning(f"No instrument found for symbol {price.symbol}")
                    results["errors"] += 1
                    continue
                
                price_data = price.to_dict()
                price_data['instrument_id'] = instrument_id
                
                # Check for existing data to determine insert vs update
                existing = self.daily_prices_dao.get_price_by_symbol_date(
                    price.symbol, price.date, price.vendor
                )
                
                if existing:
                    # Update with quality-based resolution
                    updated = await self._smart_update(existing, price_data)
                    if updated:
                        results["updated"] += 1
                else:
                    # Insert new record
                    record_id = await self.daily_prices_dao.create_async(price_data)
                    if record_id:
                        results["inserted"] += 1
                    else:
                        results["errors"] += 1
                        
            except Exception as e:
                self.logger.error(f"Error processing price for {price.symbol}: {e}")
                results["errors"] += 1
        
        return results
    
    async def _smart_update(self, existing: Dict, new_data: Dict) -> bool:
        """Smart update with quality-based conflict resolution"""
        # Implement quality-based update logic
        # For now, always update - could be enhanced with vendor quality ranking
        try:
            updated = await self.daily_prices_dao.update_async(existing['id'], new_data)
            return updated
        except Exception as e:
            self.logger.error(f"Update failed: {e}")
            return False
    
    async def get_data_gaps(
        self, 
        symbol: str, 
        start_date: date, 
        end_date: date,
        vendor: Optional[str] = None
    ) -> List[date]:
        """Find missing data gaps using existing exchange calendar"""
        nyse_cal = ExchangeCalendar('NYSE')
        trading_days = set(nyse_cal.all_trading_days(start_date, end_date))
        
        # Get existing data
        price_history = self.daily_prices_dao.get_price_history(
            symbol, start_date, end_date, vendor
        )
        existing_dates = {datetime.fromisoformat(str(row['date'])).date() for row in price_history}
        
        return sorted(trading_days - existing_dates)

class BaseVendorCollector:
    """Enhanced base collector - extends existing patterns"""
    
    def __init__(self, vendor: str, env: Environment):
        self.vendor = vendor
        self.env = env
        self.dao = EnhancedEODDAO(env)
        self.logger = get_logger(f"{__name__}.{vendor}")
        self.rate_limit_delay = 1.0  # Default rate limiting
        
    async def collect_eod_data(
        self, 
        symbols: List[str], 
        start_date: date, 
        end_date: date
    ) -> List[EODPrice]:
        """Abstract method for EOD collection"""
        raise NotImplementedError
    
    async def backfill_missing_data(self, symbol: str, days_back: int = 30) -> int:
        """Backfill missing data for a symbol"""
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        
        gaps = await self.dao.get_data_gaps(symbol, start_date, end_date, self.vendor)
        if not gaps:
            self.logger.info(f"No gaps found for {symbol}")
            return 0
        
        # Collect data for gap periods
        prices = await self.collect_eod_data([symbol], min(gaps), max(gaps))
        results = await self.dao.insert_eod_prices(prices)
        
        self.logger.info(f"Backfilled {symbol}: {results}")
        return results["inserted"] + results["updated"]

class TiingoEODCollector(BaseVendorCollector):
    """Enhanced Tiingo collector - reuses existing patterns"""
    
    def __init__(self, env: Environment):
        super().__init__('tiingo', env)
        self.api_key = TIINGO_API_KEY
        self.rate_limit_delay = 0.2  # Tiingo-specific rate limiting
        
    async def collect_eod_data(
        self, 
        symbols: List[str], 
        start_date: date, 
        end_date: date
    ) -> List[EODPrice]:
        """Enhanced Tiingo collection using existing patterns"""
        prices = []
        
        if not self.api_key:
            self.logger.warning("Tiingo API key not configured")
            return prices
        
        async with aiohttp.ClientSession() as session:
            for symbol in symbols:
                try:
                    # Use existing tiingo_url pattern
                    url = tiingo_url(symbol, start_date, end_date)
                    
                    async with session.get(url, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            for item in data:
                                try:
                                    price_date = pd.to_datetime(item['date']).date()
                                    price = EODPrice(
                                        symbol=symbol,
                                        date=price_date,
                                        open_price=float(item.get('open', 0)),
                                        high_price=float(item.get('high', 0)),
                                        low_price=float(item.get('low', 0)),
                                        close_price=float(item.get('close', 0)),
                                        adjusted_close=float(item.get('adjClose', item.get('close', 0))),
                                        volume=int(item.get('volume', 0)),
                                        market_cap=None,  # Not provided by Tiingo
                                        vendor='tiingo',
                                        quality=self._assess_quality(item)
                                    )
                                    prices.append(price)
                                except (ValueError, KeyError) as e:
                                    self.logger.warning(f"Invalid data for {symbol}: {e}")
                        else:
                            self.logger.warning(f"Tiingo API error for {symbol}: {response.status}")
                    
                    # Rate limiting
                    await asyncio.sleep(self.rate_limit_delay)
                    
                except Exception as e:
                    self.logger.error(f"Error collecting Tiingo data for {symbol}: {e}")
        
        self.logger.info(f"Collected {len(prices)} EOD prices from Tiingo")
        return prices
    
    def _assess_quality(self, item: Dict) -> DataQuality:
        """Assess data quality based on completeness"""
        required_fields = ['open', 'high', 'low', 'close', 'volume']
        valid_fields = sum(1 for field in required_fields if item.get(field) is not None)
        
        if valid_fields == len(required_fields):
            return DataQuality.EXCELLENT
        elif valid_fields >= 4:
            return DataQuality.GOOD
        elif valid_fields >= 3:
            return DataQuality.ACCEPTABLE
        else:
            return DataQuality.POOR

class PolygonEODCollector(BaseVendorCollector):
    """Enhanced Polygon collector - reuses existing patterns"""
    
    def __init__(self, env: Environment):
        super().__init__('polygon', env)
        self.api_key = POLYGON_API_KEY or env.get_polygon_api_key()
        self.rate_limit_delay = 12  # 5 requests per minute
        
    async def collect_eod_data(
        self, 
        symbols: List[str], 
        start_date: date, 
        end_date: date
    ) -> List[EODPrice]:
        """Enhanced Polygon collection using existing patterns"""
        prices = []
        
        if not self.api_key:
            self.logger.warning("Polygon API key not configured")
            return prices
        
        for symbol in symbols:
            try:
                # Reuse existing download_prices_polygon function
                raw_prices = download_prices_polygon(
                    symbol, 
                    start_date.strftime('%Y-%m-%d'),
                    end_date.strftime('%Y-%m-%d'),
                    self.api_key
                )
                
                # Get shares outstanding for market cap
                shares_outstanding = await self._get_shares_outstanding(symbol)
                
                for item in raw_prices:
                    try:
                        price_date = datetime.utcfromtimestamp(item['t']/1000).date()
                        close_price = float(item['c'])
                        
                        price = EODPrice(
                            symbol=symbol,
                            date=price_date,
                            open_price=float(item['o']),
                            high_price=float(item['h']),
                            low_price=float(item['l']),
                            close_price=close_price,
                            adjusted_close=close_price,  # Polygon provides adjusted data
                            volume=int(item['v']),
                            market_cap=int(close_price * shares_outstanding) if shares_outstanding else None,
                            vendor='polygon',
                            quality=DataQuality.EXCELLENT  # Polygon generally high quality
                        )
                        prices.append(price)
                    except (ValueError, KeyError) as e:
                        self.logger.warning(f"Invalid Polygon data for {symbol}: {e}")
                
                # Rate limiting
                await asyncio.sleep(self.rate_limit_delay)
                
            except Exception as e:
                self.logger.error(f"Error collecting Polygon data for {symbol}: {e}")
        
        self.logger.info(f"Collected {len(prices)} EOD prices from Polygon")
        return prices
    
    async def _get_shares_outstanding(self, symbol: str) -> Optional[int]:
        """Get shares outstanding for market cap calculation"""
        if not self.api_key:
            return None
        
        try:
            url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apikey={self.api_key}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('results', {}).get('share_class_shares_outstanding')
        except Exception as e:
            self.logger.error(f"Error fetching shares outstanding for {symbol}: {e}")
        
        return None

class FMPEODCollector(BaseVendorCollector):
    """FMP EOD collector - implements missing vendor"""
    
    def __init__(self, env: Environment):
        super().__init__('fmp', env)
        self.api_key = os.getenv('FMP_API_KEY', '')
        self.rate_limit_delay = 1.0  # FMP rate limiting
        
    async def collect_eod_data(
        self, 
        symbols: List[str], 
        start_date: date, 
        end_date: date
    ) -> List[EODPrice]:
        """FMP EOD collection"""
        prices = []
        
        if not self.api_key:
            self.logger.warning("FMP API key not configured")
            return prices
        
        async with aiohttp.ClientSession() as session:
            for symbol in symbols:
                try:
                    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
                    params = {
                        'apikey': self.api_key,
                        'from': start_date.strftime('%Y-%m-%d'),
                        'to': end_date.strftime('%Y-%m-%d')
                    }
                    
                    async with session.get(url, params=params, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            historical = data.get('historical', [])
                            
                            for item in historical:
                                try:
                                    price_date = datetime.strptime(item['date'], '%Y-%m-%d').date()
                                    
                                    price = EODPrice(
                                        symbol=symbol,
                                        date=price_date,
                                        open_price=float(item.get('open', 0)),
                                        high_price=float(item.get('high', 0)),
                                        low_price=float(item.get('low', 0)),
                                        close_price=float(item.get('close', 0)),
                                        adjusted_close=float(item.get('adjClose', item.get('close', 0))),
                                        volume=int(item.get('volume', 0)),
                                        market_cap=None,  # Would need separate API call
                                        vendor='fmp',
                                        quality=self._assess_quality(item)
                                    )
                                    prices.append(price)
                                except (ValueError, KeyError) as e:
                                    self.logger.warning(f"Invalid FMP data for {symbol}: {e}")
                        else:
                            self.logger.warning(f"FMP API error for {symbol}: {response.status}")
                    
                    await asyncio.sleep(self.rate_limit_delay)
                    
                except Exception as e:
                    self.logger.error(f"Error collecting FMP data for {symbol}: {e}")
        
        self.logger.info(f"Collected {len(prices)} EOD prices from FMP")
        return prices
    
    def _assess_quality(self, item: Dict) -> DataQuality:
        """Assess FMP data quality"""
        if all(item.get(field) is not None for field in ['open', 'high', 'low', 'close']):
            return DataQuality.EXCELLENT
        else:
            return DataQuality.GOOD

# FastAPI Application - Enhanced EOD Service
app = FastAPI(
    title="ATS Enhanced EOD Service",
    description="Enhanced end-of-day price collection using existing ATS framework",
    version="2.0.0"
)

class EnhancedEODService:
    """Enhanced EOD service orchestrator using existing patterns"""
    
    def __init__(self):
        self.env = Environment(gin_config_path='config/app_dev.gin')
        self.collectors = {
            'tiingo': TiingoEODCollector(self.env),
            'polygon': PolygonEODCollector(self.env),
            'fmp': FMPEODCollector(self.env)
        }
        self.symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA']
        self.metrics = {
            'total_collected': 0,
            'total_inserted': 0,
            'total_updated': 0,
            'total_errors': 0,
            'last_run': None,
            'vendor_stats': {}
        }
        self.logger = get_logger(__name__)
    
    async def initialize(self):
        """Initialize all collectors"""
        self.logger.info("Enhanced EOD service initialized")
    
    async def run_collection_cycle(
        self, 
        symbols: Optional[List[str]] = None,
        days_back: int = 5,
        vendors: Optional[List[str]] = None
    ):
        """Enhanced collection cycle with configurable parameters"""
        symbols = symbols or self.symbols
        vendors = vendors or list(self.collectors.keys())
        
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        
        self.logger.info(f"Starting EOD collection: {len(symbols)} symbols, {len(vendors)} vendors")
        
        total_stats = {"collected": 0, "inserted": 0, "updated": 0, "errors": 0}
        
        for vendor in vendors:
            if vendor not in self.collectors:
                self.logger.warning(f"Unknown vendor: {vendor}")
                continue
            
            collector = self.collectors[vendor]
            try:
                # Collect data
                prices = await collector.collect_eod_data(symbols, start_date, end_date)
                
                # Insert/update data
                results = await collector.dao.insert_eod_prices(prices)
                
                # Update metrics
                vendor_stats = {
                    "collected": len(prices),
                    "inserted": results["inserted"],
                    "updated": results["updated"],
                    "errors": results["errors"]
                }
                
                self.metrics['vendor_stats'][vendor] = vendor_stats
                
                for key in ["inserted", "updated", "errors"]:
                    total_stats[key] += results[key]
                total_stats["collected"] += len(prices)
                
                self.logger.info(f"{vendor}: {vendor_stats}")
                
            except Exception as e:
                self.logger.error(f"Collection failed for {vendor}: {e}")
                total_stats["errors"] += 1
        
        # Update global metrics
        for key, value in total_stats.items():
            self.metrics[f'total_{key}'] += value
        
        self.metrics['last_run'] = datetime.now().isoformat()
        self.logger.info(f"Collection cycle complete: {total_stats}")
        
        return total_stats
    
    async def backfill_data(self, symbol: str, days_back: int = 30):
        """Backfill missing data across all vendors"""
        results = {}
        for vendor, collector in self.collectors.items():
            try:
                filled = await collector.backfill_missing_data(symbol, days_back)
                results[vendor] = filled
            except Exception as e:
                self.logger.error(f"Backfill failed for {vendor}: {e}")
                results[vendor] = 0
        
        return results

# Global service instance
service = EnhancedEODService()

@app.on_event("startup")
async def startup():
    """Service startup using existing patterns"""
    logger.info("Starting ATS Enhanced EOD Service")
    await service.initialize()
    logger.info("Enhanced EOD service started successfully")

@app.get("/health")
async def health():
    """Enhanced health check endpoint"""
    return {
        "status": "healthy",
        "environment": service.env.env_type.value,
        "vendors": list(service.collectors.keys()),
        "symbols": service.symbols,
        "metrics": service.metrics,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/metrics")
async def get_metrics():
    """Detailed metrics endpoint"""
    return service.metrics

@app.post("/collect")
async def trigger_collection(
    background_tasks: BackgroundTasks,
    symbols: Optional[str] = None,
    vendors: Optional[str] = None,
    days_back: int = 5
):
    """Manual collection trigger with parameters"""
    symbol_list = symbols.split(',') if symbols else None
    vendor_list = vendors.split(',') if vendors else None
    
    background_tasks.add_task(
        service.run_collection_cycle, 
        symbol_list, 
        days_back, 
        vendor_list
    )
    
    return {
        "message": "Enhanced EOD collection triggered",
        "symbols": symbol_list or service.symbols,
        "vendors": vendor_list or list(service.collectors.keys()),
        "days_back": days_back,
        "timestamp": datetime.now().isoformat()
    }

@app.post("/backfill/{symbol}")
async def backfill_symbol(
    symbol: str, 
    background_tasks: BackgroundTasks,
    days_back: int = 30
):
    """Backfill missing data for a specific symbol"""
    background_tasks.add_task(service.backfill_data, symbol.upper(), days_back)
    
    return {
        "message": f"Backfill triggered for {symbol.upper()}",
        "days_back": days_back,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/gaps/{symbol}")
async def get_data_gaps(symbol: str, days_back: int = 30):
    """Get data gaps for a symbol across all vendors"""
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)
    
    gaps = {}
    for vendor, collector in service.collectors.items():
        try:
            vendor_gaps = await collector.dao.get_data_gaps(
                symbol.upper(), start_date, end_date, vendor
            )
            gaps[vendor] = [gap.isoformat() for gap in vendor_gaps]
        except Exception as e:
            logger.error(f"Error getting gaps for {vendor}: {e}")
            gaps[vendor] = []
    
    return {
        "symbol": symbol.upper(),
        "date_range": {"start": start_date.isoformat(), "end": end_date.isoformat()},
        "gaps": gaps
    }

@app.get("/")
async def root():
    """Root endpoint with service info"""
    return {
        "service": "ATS Enhanced EOD Service",
        "version": "2.0.0",
        "framework": "Reuses and enhances existing ATS patterns",
        "vendors": list(service.collectors.keys()),
        "features": [
            "Multi-vendor EOD collection",
            "Quality assessment",
            "Gap detection and backfill",
            "Enhanced metrics",
            "Conflict resolution"
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8082)