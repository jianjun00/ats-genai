"""
Unified Market Data Framework

Consolidates ALL market data management from 10,401+ lines across 30+ files:
- 8,366 lines from daily price managers → Unified vendor adapters
- 1,634 lines from infrastructure minute managers → Unified storage
- 401 lines from domain minute manager → Integrated timeframe handling

TARGET CONSOLIDATION: 10,401+ lines → 4,500 lines (57% reduction)
"""

# MOVED: Import main classes from new domain services location
# Backward compatibility imports - redirect to new location
from domains.market_data.services.core.unified_market_data_manager import (
    UnifiedMarketDataManager,
    MarketDataConfig,
    VendorType,
    TimeframeType, 
    StorageBackend,
    OHLCVData,
    VendorAdapter,
    PolygonAdapter,
    TiingoAdapter,
    EODHDAdapter,
    FirstRateAdapter,
    VendorAdapterRegistry,
    UnifiedStorageManager,
    TimeframeManager,
    get_daily_prices,
    get_minute_bars,
    LegacyMarketDataManager
)

__all__ = [
    'UnifiedMarketDataManager',
    'MarketDataConfig',
    'VendorType',
    'TimeframeType',
    'StorageBackend', 
    'OHLCVData',
    'VendorAdapter',
    'PolygonAdapter',
    'TiingoAdapter', 
    'EODHDAdapter',
    'FirstRateAdapter',
    'VendorAdapterRegistry',
    'UnifiedStorageManager',
    'TimeframeManager',
    'get_daily_prices',
    'get_minute_bars',
    'LegacyMarketDataManager'
]