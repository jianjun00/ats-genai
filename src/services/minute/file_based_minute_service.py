#!/usr/bin/env python3
"""
File-Based Minute Price Service

Updated minute price service that writes to monthly Parquet files instead of database.
Provides the same API interface but uses file-based storage with overlap detection.
"""

import gin
import asyncio
from datetime import datetime
from typing import List, Dict, Optional
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from storage.file_based_minute_manager import FileBasedMinuteManager, MinuteBar
from core.config.environment import Environment
from core.logging.logger_config import get_logger

# Configure Gin
try:
    gin.parse_config_file('config/app_dev.gin')
except Exception as e:
    print(f'[WARN] Could not parse gin config: {e}')

logger = get_logger(__name__)

# Pydantic models for API
class MinutePriceRequest(BaseModel):
    symbol: str
    bars: List[Dict]
    overlap_strategy: str = 'merge'

class QueryRequest(BaseModel):
    symbol: str
    start_date: str  # ISO format
    end_date: str    # ISO format
    columns: Optional[List[str]] = None

class VendorCollector:
    """Base class for vendor data collection with file-based storage"""
    
    def __init__(self, vendor: str, file_manager: FileBasedMinuteManager):
        self.vendor = vendor
        self.file_manager = file_manager
        self.logger = get_logger(f"{__name__}.{vendor}")
        
    async def collect_minute_data(self, symbols: List[str]) -> List[MinuteBar]:
        """Abstract method for data collection"""
        raise NotImplementedError

class PolygonMinuteCollector(VendorCollector):
    """Polygon minute data collector with file-based storage"""
    
    def __init__(self, file_manager: FileBasedMinuteManager):
        super().__init__('polygon', file_manager)
        self.api_key = os.getenv('POLYGON_API_KEY', '')
    
    async def collect_minute_data(self, symbols: List[str]) -> List[MinuteBar]:
        """Collect minute data from Polygon API"""
        bars = []
        
        if not self.api_key:
            self.logger.warning("Polygon API key not configured")
            return bars
        
        import aiohttp
        async with aiohttp.ClientSession() as session:
            for symbol in symbols:
                try:
                    # Get today's minute data
                    today = datetime.now().strftime('%Y-%m-%d')
                    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{today}/{today}"
                    params = {
                        'apikey': self.api_key,
                        'adjusted': 'true',
                        'sort': 'desc',
                        'limit': 60  # Last 60 minutes
                    }
                    
                    async with session.get(url, params=params, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            if 'results' in data and data['results']:
                                for item in data['results']:
                                    bar = MinuteBar(
                                        symbol=symbol,
                                        timestamp=datetime.fromtimestamp(item['t'] / 1000),
                                        open=float(item['o']),
                                        high=float(item['h']),
                                        low=float(item['l']),
                                        close=float(item['c']),
                                        volume=int(item['v']),
                                        vwap=item.get('vw'),
                                        trade_count=item.get('n'),
                                        vendor='polygon',
                                        quality_score=0.9
                                    )
                                    bars.append(bar)
                        else:
                            self.logger.warning(f"Polygon API error for {symbol}: {response.status}")
                    
                    # Rate limiting
                    await asyncio.sleep(12)  # 5 requests per minute limit
                    
                except Exception as e:
                    self.logger.error(f"Error collecting Polygon data for {symbol}: {e}")
        
        self.logger.info(f"Collected {len(bars)} minute bars from Polygon")
        return bars

class TiingoMinuteCollector(VendorCollector):
    """Tiingo minute data collector with file-based storage"""
    
    def __init__(self, file_manager: FileBasedMinuteManager):
        super().__init__('tiingo', file_manager)
        self.api_key = os.getenv('TIINGO_API_KEY', '')
    
    async def collect_minute_data(self, symbols: List[str]) -> List[MinuteBar]:
        """Collect minute data from Tiingo API"""
        bars = []
        
        if not self.api_key:
            self.logger.warning("Tiingo API key not configured")
            return bars
        
        import aiohttp
        async with aiohttp.ClientSession() as session:
            for symbol in symbols:
                try:
                    # Get today's minute data from Tiingo
                    url = f"https://api.tiingo.com/iex/{symbol}/prices"
                    params = {
                        'token': self.api_key,
                        'startDate': datetime.now().strftime('%Y-%m-%d'),
                        'endDate': datetime.now().strftime('%Y-%m-%d'),
                        'resampleFreq': '1min'
                    }
                    
                    async with session.get(url, params=params, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            for item in data[-60:]:  # Last 60 minutes
                                try:
                                    bar = MinuteBar(
                                        symbol=symbol,
                                        timestamp=datetime.fromisoformat(item['date'].replace('Z', '+00:00')),
                                        open=float(item['open']),
                                        high=float(item['high']),
                                        low=float(item['low']),
                                        close=float(item['close']),
                                        volume=int(item['volume']),
                                        vendor='tiingo',
                                        quality_score=0.8
                                    )
                                    bars.append(bar)
                                except (KeyError, ValueError) as e:
                                    self.logger.warning(f"Error parsing Tiingo data item for {symbol}: {e}")
                                    continue
                        else:
                            self.logger.warning(f"Tiingo API error for {symbol}: {response.status}")
                    
                    await asyncio.sleep(0.2)  # Tiingo rate limiting
                    
                except Exception as e:
                    self.logger.error(f"Error collecting Tiingo data for {symbol}: {e}")
        
        self.logger.info(f"Collected {len(bars)} minute bars from Tiingo")
        return bars

class FMPMinuteCollector(VendorCollector):
    """FMP minute data collector with file-based storage"""
    
    def __init__(self, file_manager: FileBasedMinuteManager):
        super().__init__('fmp', file_manager)
        self.api_key = os.getenv('FMP_API_KEY', '')
    
    async def collect_minute_data(self, symbols: List[str]) -> List[MinuteBar]:
        """Collect minute data from FMP API"""
        bars = []
        
        if not self.api_key:
            self.logger.warning("FMP API key not configured")
            return bars
        
        import aiohttp
        async with aiohttp.ClientSession() as session:
            for symbol in symbols:
                try:
                    url = f"https://financialmodelingprep.com/api/v3/historical-chart/1min/{symbol}"
                    params = {'apikey': self.api_key}
                    
                    async with session.get(url, params=params, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            for item in data[:60]:  # Latest 60 minutes
                                try:
                                    bar = MinuteBar(
                                        symbol=symbol,
                                        timestamp=datetime.fromisoformat(item['date']),
                                        open=float(item['open']),
                                        high=float(item['high']),
                                        low=float(item['low']),
                                        close=float(item['close']),
                                        volume=int(item['volume']),
                                        vendor='fmp',
                                        quality_score=0.7
                                    )
                                    bars.append(bar)
                                except (KeyError, ValueError) as e:
                                    self.logger.warning(f"Error parsing FMP data item for {symbol}: {e}")
                                    continue
                        else:
                            self.logger.warning(f"FMP API error for {symbol}: {response.status}")
                    
                    await asyncio.sleep(1)  # FMP rate limiting
                    
                except Exception as e:
                    self.logger.error(f"Error collecting FMP data for {symbol}: {e}")
        
        self.logger.info(f"Collected {len(bars)} minute bars from FMP")
        return bars

# FastAPI Application
app = FastAPI(
    title="File-Based ATS Minute Price Service",
    description="1-minute intraday price collection with file-based storage",
    version="2.0.0"
)

class FileBasedMinutePriceService:
    """Main service orchestrator with file-based storage"""
    
    def __init__(self):
        self.env = Environment()
        
        # Initialize file-based storage manager
        storage_path = os.getenv('MINUTE_DATA_PATH', '/home/jianjun/ats-data/minute-files')
        self.file_manager = FileBasedMinuteManager(
            base_path=storage_path,
            max_concurrent_operations=4,
            backup_enabled=True,
            compression='snappy'
        )
        
        # Initialize collectors
        self.collectors = {
            'polygon': PolygonMinuteCollector(self.file_manager),
            'tiingo': TiingoMinuteCollector(self.file_manager),
            'fmp': FMPMinuteCollector(self.file_manager)
        }
        
        # Default symbols to collect
        self.symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA',
            'JPM', 'JNJ', 'V', 'PG', 'UNH', 'DIS', 'NFLX', 'CRM', 'ADBE'
        ]
        
        self.is_running = False
        self.metrics = {
            'total_collected': 0,
            'successful_stores': 0,
            'errors': 0,
            'last_run': None,
            'files_created': 0,
            'files_updated': 0
        }
        self.logger = get_logger(__name__)
    
    async def initialize(self):
        """Initialize the service"""
        self.logger.info("Initializing file-based minute price service")
        # File manager doesn't need explicit initialization
        await self.cleanup_old_data()
        self.logger.info("Service initialization complete")
    
    async def cleanup_old_data(self):
        """Clean up old backup files"""
        try:
            cleaned = await self.file_manager.cleanup_old_backups(days_old=7)
            self.logger.info(f"Cleaned {cleaned} old backup files")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    async def run_collection_cycle(self, overlap_strategy: str = 'merge'):
        """Run data collection cycle for all vendors"""
        self.logger.info("Starting file-based collection cycle")
        
        # Collect from all vendors concurrently
        tasks = [
            collector.collect_minute_data(self.symbols)
            for collector in self.collectors.values()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        total_collected = 0
        total_stored = 0
        
        for i, result in enumerate(results):
            vendor = list(self.collectors.keys())[i]
            
            if isinstance(result, Exception):
                self.logger.error(f"Collection failed for {vendor}: {result}")
                self.metrics['errors'] += 1
                continue
            
            if not result:
                self.logger.info(f"{vendor}: no data collected")
                continue
            
            # Group bars by symbol and store
            symbol_groups = {}
            for bar in result:
                if bar.symbol not in symbol_groups:
                    symbol_groups[bar.symbol] = []
                symbol_groups[bar.symbol].append(bar)
            
            vendor_stored = 0
            for symbol, symbol_bars in symbol_groups.items():
                try:
                    store_result = await self.file_manager.store_minute_data(
                        symbol, symbol_bars, overlap_strategy
                    )
                    
                    vendor_stored += store_result['stored']
                    self.metrics['files_created'] += store_result.get('files_created', 0)
                    self.metrics['files_updated'] += store_result.get('files_updated', 0)
                    
                    if store_result.get('errors'):
                        self.logger.warning(f"Storage errors for {symbol}: {store_result['errors']}")
                        
                except Exception as e:
                    self.logger.error(f"Error storing {symbol} data from {vendor}: {e}")
                    self.metrics['errors'] += 1
            
            total_collected += len(result)
            total_stored += vendor_stored
            
            self.logger.info(f"{vendor}: collected {len(result)} bars, stored {vendor_stored}")
        
        self.metrics['total_collected'] += total_collected
        self.metrics['successful_stores'] += total_stored
        self.metrics['last_run'] = datetime.now().isoformat()
        
        self.logger.info(f"Collection cycle complete: {total_collected} bars collected, {total_stored} stored")
    
    async def start_continuous_collection(self):
        """Start continuous collection loop"""
        self.is_running = True
        self.logger.info("Starting continuous collection with file-based storage")
        
        while self.is_running:
            try:
                await self.run_collection_cycle()
                
                # Run integrity check every 10 cycles
                if self.metrics.get('cycles', 0) % 10 == 0:
                    await self._run_integrity_check()
                
                self.metrics['cycles'] = self.metrics.get('cycles', 0) + 1
                await asyncio.sleep(60)  # Collect every minute
                
            except Exception as e:
                self.logger.error(f"Collection loop error: {e}")
                self.metrics['errors'] += 1
                await asyncio.sleep(30)  # Shorter sleep on error
    
    async def _run_integrity_check(self):
        """Run periodic integrity check"""
        try:
            self.logger.info("Running periodic integrity check...")
            integrity_result = await self.file_manager.verify_data_integrity()
            
            if integrity_result['corrupt_files'] > 0 or integrity_result['checksum_mismatches'] > 0:
                self.logger.warning(f"Integrity issues found: {integrity_result}")
            else:
                self.logger.debug("Integrity check passed")
                
        except Exception as e:
            self.logger.error(f"Error during integrity check: {e}")
    
    async def query_data(self, symbol: str, start_date: datetime, end_date: datetime, columns: Optional[List[str]] = None):
        """Query minute data from files"""
        try:
            result = await self.file_manager.query_minute_data(symbol, start_date, end_date, columns)
            return result
        except Exception as e:
            self.logger.error(f"Error querying data for {symbol}: {e}")
            raise HTTPException(status_code=500, detail=f"Query error: {e}")
    
    async def get_storage_stats(self):
        """Get comprehensive storage statistics"""
        try:
            stats = await self.file_manager.get_storage_stats()
            stats['service_metrics'] = self.metrics
            return stats
        except Exception as e:
            self.logger.error(f"Error getting storage stats: {e}")
            raise HTTPException(status_code=500, detail=f"Stats error: {e}")
    
    async def store_external_data(self, symbol: str, bars_data: List[Dict], overlap_strategy: str = 'merge'):
        """Store externally provided minute data"""
        try:
            # Convert dict data to MinuteBar objects
            bars = []
            for bar_dict in bars_data:
                bar = MinuteBar(
                    symbol=symbol,
                    timestamp=datetime.fromisoformat(bar_dict['timestamp']) if isinstance(bar_dict['timestamp'], str) else bar_dict['timestamp'],
                    open=float(bar_dict['open']),
                    high=float(bar_dict['high']),
                    low=float(bar_dict['low']),
                    close=float(bar_dict['close']),
                    volume=int(bar_dict['volume']),
                    vwap=bar_dict.get('vwap'),
                    trade_count=bar_dict.get('trade_count'),
                    vendor=bar_dict.get('vendor', 'external'),
                    quality_score=float(bar_dict.get('quality_score', 1.0))
                )
                bars.append(bar)
            
            # Store the data
            result = await self.file_manager.store_minute_data(symbol, bars, overlap_strategy)
            return result
            
        except Exception as e:
            self.logger.error(f"Error storing external data for {symbol}: {e}")
            raise HTTPException(status_code=500, detail=f"Storage error: {e}")
    
    def stop(self):
        """Stop the continuous collection"""
        self.is_running = False
        self.logger.info("Service stop requested")

# Global service instance
service = FileBasedMinutePriceService()

@app.on_event("startup")
async def startup():
    """Service startup"""
    logger.info("Starting File-Based ATS Minute Price Service")
    await service.initialize()
    
    # Start collection in background
    asyncio.create_task(service.start_continuous_collection())
    logger.info("File-based service started successfully")

@app.on_event("shutdown")
async def shutdown():
    """Service shutdown"""
    logger.info("Shutting down file-based service")
    service.stop()
    await service.file_manager.close()

# API Endpoints
@app.get("/health")
async def health():
    """Enhanced health check endpoint"""
    storage_stats = await service.get_storage_stats()
    
    return {
        "status": "healthy" if service.is_running else "stopped",
        "storage_type": "file_based",
        "environment": service.env.env_type.value,
        "vendors": list(service.collectors.keys()),
        "symbols": len(service.symbols),
        "metrics": service.metrics,
        "storage_summary": {
            "total_files": storage_stats.get('files', 0),
            "total_symbols": storage_stats.get('symbols', 0),
            "total_size_mb": round(storage_stats.get('total_size_mb', 0), 2)
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get("/metrics")
async def get_metrics():
    """Get comprehensive metrics"""
    return await service.get_storage_stats()

@app.post("/store")
async def store_data(request: MinutePriceRequest):
    """Store minute data from external source"""
    result = await service.store_external_data(
        request.symbol, 
        request.bars, 
        request.overlap_strategy
    )
    return {
        "message": "Data stored successfully",
        "result": result,
        "timestamp": datetime.now().isoformat()
    }

@app.post("/query")
async def query_data(request: QueryRequest):
    """Query minute data from files"""
    start_date = datetime.fromisoformat(request.start_date)
    end_date = datetime.fromisoformat(request.end_date)
    
    result_df = await service.query_data(
        request.symbol, 
        start_date, 
        end_date, 
        request.columns
    )
    
    # Convert DataFrame to dict for JSON response
    if result_df.empty:
        return {
            "symbol": request.symbol,
            "data": [],
            "count": 0,
            "message": "No data found for specified range"
        }
    
    return {
        "symbol": request.symbol,
        "data": result_df.to_dict('records'),
        "count": len(result_df),
        "date_range": {
            "start": result_df['timestamp'].min().isoformat(),
            "end": result_df['timestamp'].max().isoformat()
        }
    }

@app.post("/collect")
async def trigger_collection(background_tasks: BackgroundTasks):
    """Manual collection trigger"""
    background_tasks.add_task(service.run_collection_cycle)
    return {
        "message": "Collection triggered", 
        "timestamp": datetime.now().isoformat(),
        "storage_type": "file_based"
    }

@app.get("/integrity")
async def check_integrity():
    """Run data integrity check"""
    result = await service.file_manager.verify_data_integrity()
    return {
        "integrity_check": result,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "File-Based ATS Minute Price Service",
        "version": "2.0.0",
        "storage_type": "monthly_parquet_files",
        "framework": "FastAPI with file-based storage",
        "status": "running" if service.is_running else "stopped",
        "features": [
            "Monthly file organization",
            "Overlap detection and resolution", 
            "Missing file handling",
            "Data integrity verification",
            "Atomic file operations",
            "Backup and restore"
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8082)  # Different port from DB version