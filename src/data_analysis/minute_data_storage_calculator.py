"""
Data Storage Requirements Calculator for 1-Minute Financial Data

Calculate storage requirements for 1-minute OHLCV data from various vendors
and design hybrid storage architecture for efficient data management.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List
import os

class MinuteDataStorageCalculator:
    """Calculate storage requirements for 1-minute financial data."""
    
    def __init__(self):
        # Market hours: 9:30 AM - 4:00 PM ET = 6.5 hours = 390 minutes per day
        self.market_minutes_per_day = 390
        self.trading_days_per_year = 252
        
        # Data structure sizes (bytes)
        self.minute_bar_sizes = {
            'basic_ohlcv': {
                'symbol': 10,          # VARCHAR(10)
                'timestamp': 8,        # TIMESTAMPTZ
                'open': 8,            # NUMERIC(12,4) 
                'high': 8,            # NUMERIC(12,4)
                'low': 8,             # NUMERIC(12,4)
                'close': 8,           # NUMERIC(12,4)
                'volume': 8,          # BIGINT
                'total': 58
            },
            'enhanced_with_indicators': {
                'basic_ohlcv': 58,
                'vwap': 8,            # NUMERIC(12,4)
                'trade_count': 4,     # INTEGER
                'returns': 4,         # NUMERIC(8,6)
                'sma_5': 8,          # NUMERIC(12,4)
                'sma_20': 8,         # NUMERIC(12,4)
                'ema_12': 8,         # NUMERIC(12,4)
                'ema_26': 8,         # NUMERIC(12,4)
                'macd': 4,           # NUMERIC(8,6)
                'macd_signal': 4,    # NUMERIC(8,6)
                'rsi': 4,            # NUMERIC(5,2)
                'bb_upper': 8,       # NUMERIC(12,4)
                'bb_middle': 8,      # NUMERIC(12,4)
                'bb_lower': 8,       # NUMERIC(12,4)
                'volume_sma': 8,     # NUMERIC(15,2)
                'volume_ratio': 4,   # NUMERIC(6,3)
                'volatility': 4,     # NUMERIC(8,6)
                'quality_score': 4,  # NUMERIC(3,2)
                'metadata': 50,      # JSONB + other fields
                'total': 158
            }
        }
        
        # Compression ratios for different storage formats
        self.compression_ratios = {
            'raw_postgresql': 1.0,
            'timescaledb_compressed': 0.3,    # ~70% compression
            'parquet_snappy': 0.4,            # ~60% compression
            'parquet_lz4': 0.35,              # ~65% compression
            'csv_gzip': 0.2,                  # ~80% compression
            'hdf5_blosc': 0.25                # ~75% compression
        }
    
    def calculate_symbol_storage(
        self, 
        symbol: str, 
        years: int = 1,
        data_type: str = 'enhanced_with_indicators',
        storage_format: str = 'timescaledb_compressed'
    ) -> Dict[str, Any]:
        """Calculate storage requirements for a single symbol."""
        
        # Base calculations
        minutes_per_year = self.market_minutes_per_day * self.trading_days_per_year
        total_minutes = minutes_per_year * years
        
        # Bytes per minute bar
        bytes_per_bar = self.minute_bar_sizes[data_type]['total']
        
        # Raw storage requirement
        raw_bytes = total_minutes * bytes_per_bar
        
        # Compressed storage requirement
        compression_ratio = self.compression_ratios[storage_format]
        compressed_bytes = raw_bytes * compression_ratio
        
        return {
            'symbol': symbol,
            'years': years,
            'total_minutes': total_minutes,
            'data_type': data_type,
            'storage_format': storage_format,
            'bytes_per_bar': bytes_per_bar,
            'raw_bytes': raw_bytes,
            'compressed_bytes': compressed_bytes,
            'raw_mb': raw_bytes / (1024 * 1024),
            'compressed_mb': compressed_bytes / (1024 * 1024),
            'raw_gb': raw_bytes / (1024 * 1024 * 1024),
            'compressed_gb': compressed_bytes / (1024 * 1024 * 1024)
        }
    
    def calculate_portfolio_storage(
        self,
        symbols: List[str],
        years: int = 1,
        data_type: str = 'enhanced_with_indicators',
        storage_format: str = 'timescaledb_compressed'
    ) -> Dict[str, Any]:
        """Calculate storage requirements for a portfolio of symbols."""
        
        symbol_results = []
        total_raw_bytes = 0
        total_compressed_bytes = 0
        
        for symbol in symbols:
            result = self.calculate_symbol_storage(symbol, years, data_type, storage_format)
            symbol_results.append(result)
            total_raw_bytes += result['raw_bytes']
            total_compressed_bytes += result['compressed_bytes']
        
        return {
            'symbol_count': len(symbols),
            'years': years,
            'data_type': data_type,
            'storage_format': storage_format,
            'symbols': symbol_results,
            'total_raw_bytes': total_raw_bytes,
            'total_compressed_bytes': total_compressed_bytes,
            'total_raw_mb': total_raw_bytes / (1024 * 1024),
            'total_compressed_mb': total_compressed_bytes / (1024 * 1024),
            'total_raw_gb': total_raw_bytes / (1024 * 1024 * 1024),
            'total_compressed_gb': total_compressed_bytes / (1024 * 1024 * 1024),
            'avg_mb_per_symbol': (total_compressed_bytes / len(symbols)) / (1024 * 1024)
        }
    
    def vendor_comparison(self) -> Dict[str, Any]:
        """Compare 1-minute data availability across vendors."""
        
        vendors = {
            'polygon': {
                'supports_1min': True,
                'api_limit_free': 5,      # calls per minute
                'api_limit_premium': 100,  # calls per minute  
                'cost_free': 0,
                'cost_premium': 99,       # USD per month
                'data_quality': 'high',
                'coverage': 'us_stocks_crypto',
                'real_time': True,
                'historical_years': 20
            },
            'tiingo': {
                'supports_1min': False,   # Limited intraday support
                'api_limit_free': 500,    # calls per day
                'api_limit_premium': 50000, # calls per day
                'cost_free': 0,
                'cost_premium': 30,       # USD per month
                'data_quality': 'medium',
                'coverage': 'us_stocks_eod',
                'real_time': False,
                'historical_years': 30
            },
            'interactive_brokers': {
                'supports_1min': True,
                'api_limit_free': 'unlimited_with_account',
                'api_limit_premium': 'unlimited_with_account',
                'cost_free': 0,           # With trading account
                'cost_premium': 0,        # With trading account
                'data_quality': 'high',
                'coverage': 'global_multi_asset',
                'real_time': True,
                'historical_years': 5     # Limited historical
            },
            'alpha_vantage': {
                'supports_1min': True,
                'api_limit_free': 5,      # calls per minute
                'api_limit_premium': 1200, # calls per minute
                'cost_free': 0,
                'cost_premium': 49.99,    # USD per month
                'data_quality': 'medium',
                'coverage': 'us_stocks_forex_crypto',
                'real_time': True,
                'historical_years': 20
            }
        }
        
        return vendors
    
    def storage_architecture_recommendation(
        self,
        symbol_count: int,
        years: int = 2
    ) -> Dict[str, Any]:
        """Recommend hybrid storage architecture based on data volume."""
        
        # Calculate storage requirements
        sample_symbols = [f"SYMBOL_{i}" for i in range(symbol_count)]
        portfolio_storage = self.calculate_portfolio_storage(sample_symbols, years)
        
        total_gb = portfolio_storage['total_compressed_gb']
        
        if total_gb < 10:
            architecture = "database_only"
            recommendation = {
                'primary_storage': 'PostgreSQL + TimescaleDB',
                'secondary_storage': None,
                'rationale': 'Small dataset fits entirely in database with good performance'
            }
        elif total_gb < 100:
            architecture = "hybrid_database_disk"
            recommendation = {
                'primary_storage': 'PostgreSQL + TimescaleDB (recent 3-6 months)',
                'secondary_storage': 'Parquet files on disk (older data)',
                'rationale': 'Balance between query performance and storage efficiency'
            }
        else:
            architecture = "disk_primary_database_cache"
            recommendation = {
                'primary_storage': 'Parquet files on disk (main storage)',
                'secondary_storage': 'PostgreSQL + TimescaleDB (hot cache, recent 1 month)',
                'rationale': 'Large dataset requires disk storage with database cache for performance'
            }
        
        return {
            'architecture': architecture,
            'total_storage_gb': total_gb,
            'recommendation': recommendation,
            'estimated_cost_analysis': self._estimate_costs(total_gb),
            'performance_considerations': self._performance_recommendations(architecture)
        }
    
    def _estimate_costs(self, total_gb: float) -> Dict[str, Any]:
        """Estimate storage and compute costs."""
        
        # Cloud storage costs (approximate)
        cloud_storage_costs = {
            'aws_s3_standard': total_gb * 0.023,  # $0.023 per GB/month
            'aws_s3_ia': total_gb * 0.0125,       # $0.0125 per GB/month
            'gcp_standard': total_gb * 0.020,     # $0.020 per GB/month
            'local_disk': total_gb * 0.002        # ~$0.002 per GB (hardware amortized)
        }
        
        # Database costs (RDS/managed)
        database_costs = {
            'aws_rds_postgres': 200,              # ~$200/month for suitable instance
            'gcp_cloud_sql': 180,                 # ~$180/month for suitable instance
            'local_postgres': 0                   # Self-managed
        }
        
        return {
            'storage_costs_monthly': cloud_storage_costs,
            'database_costs_monthly': database_costs,
            'total_monthly_cost_cloud': min(cloud_storage_costs.values()) + min(database_costs.values()),
            'total_monthly_cost_local': cloud_storage_costs['local_disk']
        }
    
    def _performance_recommendations(self, architecture: str) -> List[str]:
        """Provide performance recommendations for given architecture."""
        
        recommendations = {
            'database_only': [
                'Use TimescaleDB hypertables for time-series optimization',
                'Implement proper indexing on (symbol, timestamp)',
                'Enable TimescaleDB compression for data older than 7 days',
                'Use connection pooling for concurrent access'
            ],
            'hybrid_database_disk': [
                'Keep recent 3-6 months in database for fast queries',
                'Use Parquet with columnar compression for historical data',
                'Implement data lifecycle management with automated archival',
                'Create unified query interface across database and files'
            ],
            'disk_primary_database_cache': [
                'Use Parquet partitioned by date for efficient scanning',
                'Implement LRU cache in database for frequently accessed data',
                'Pre-aggregate common queries (daily/weekly summaries)',
                'Use columnar database like DuckDB for file-based analytics'
            ]
        }
        
        return recommendations.get(architecture, [])

def main():
    """Run storage analysis and provide recommendations."""
    
    calculator = MinuteDataStorageCalculator()
    
    print("=" * 80)
    print("1-MINUTE DATA STORAGE ANALYSIS")
    print("=" * 80)
    
    # Single symbol analysis
    print("\n1. SINGLE SYMBOL STORAGE (AAPL, 1 year)")
    print("-" * 50)
    aapl_storage = calculator.calculate_symbol_storage('AAPL', years=1)
    print(f"Raw storage: {aapl_storage['raw_mb']:.1f} MB ({aapl_storage['raw_gb']:.3f} GB)")
    print(f"Compressed: {aapl_storage['compressed_mb']:.1f} MB ({aapl_storage['compressed_gb']:.3f} GB)")
    print(f"Total minute bars: {aapl_storage['total_minutes']:,}")
    
    # Portfolio analysis scenarios
    scenarios = [
        ('Small Portfolio (10 symbols, 1 year)', 10, 1),
        ('Medium Portfolio (100 symbols, 2 years)', 100, 2),
        ('Large Portfolio (500 symbols, 2 years)', 500, 2),
        ('Enterprise (2000 symbols, 5 years)', 2000, 5)
    ]
    
    print("\n2. PORTFOLIO STORAGE SCENARIOS")
    print("-" * 50)
    
    for name, symbol_count, years in scenarios:
        sample_symbols = [f"SYM_{i:04d}" for i in range(symbol_count)]
        result = calculator.calculate_portfolio_storage(sample_symbols, years)
        
        print(f"\n{name}:")
        print(f"  Total compressed: {result['total_compressed_gb']:.1f} GB")
        print(f"  Avg per symbol: {result['avg_mb_per_symbol']:.1f} MB")
        print(f"  Total minute bars: {result['symbol_count'] * result['symbols'][0]['total_minutes']:,}")
    
    # Vendor comparison
    print("\n3. VENDOR COMPARISON FOR 1-MINUTE DATA")
    print("-" * 50)
    vendors = calculator.vendor_comparison()
    
    for vendor, details in vendors.items():
        print(f"\n{vendor.upper()}:")
        print(f"  1-min support: {details['supports_1min']}")
        print(f"  API limits: {details['api_limit_free']} (free) / {details['api_limit_premium']} (premium)")
        print(f"  Cost: ${details['cost_free']} (free) / ${details['cost_premium']} (premium)")
        print(f"  Data quality: {details['data_quality']}")
    
    # Architecture recommendations
    print("\n4. STORAGE ARCHITECTURE RECOMMENDATIONS")
    print("-" * 50)
    
    for name, symbol_count, years in scenarios:
        arch = calculator.storage_architecture_recommendation(symbol_count, years)
        print(f"\n{name} ({arch['total_storage_gb']:.1f} GB):")
        print(f"  Architecture: {arch['architecture']}")
        print(f"  Primary: {arch['recommendation']['primary_storage']}")
        if arch['recommendation']['secondary_storage']:
            print(f"  Secondary: {arch['recommendation']['secondary_storage']}")
        print(f"  Rationale: {arch['recommendation']['rationale']}")
        print(f"  Est. monthly cost: ${arch['estimated_cost_analysis']['total_monthly_cost_local']:.2f} (local)")
    
    # Current /home/jianjun/ats analysis
    print("\n5. CURRENT /home/jianjun/ats DATA ANALYSIS")
    print("-" * 50)
    print("Current data structure:")
    print("  - 30-minute futures data (129 symbols)")
    print("  - Parquet format with compression")
    print("  - Total size: ~2.3GB")
    print("  - Coverage: 2008-2023 (15 years)")
    
    # Estimate current data in 1-minute equivalent
    current_30min_bars = 2.3 * 1024 * 1024 * 1024 / 158  # Approximate bars
    equivalent_1min_bars = current_30min_bars * 30
    equivalent_1min_gb = (equivalent_1min_bars * 158) / (1024 * 1024 * 1024) * 0.3  # With compression
    
    print(f"\nIf converted to 1-minute data:")
    print(f"  - Estimated size: ~{equivalent_1min_gb:.1f} GB (compressed)")
    print(f"  - Storage recommendation: disk_primary_database_cache")
    
    print("\n6. RECOMMENDATION FOR /home/jianjun/ats")
    print("-" * 50)
    print("VENDOR CHOICE: Polygon.io")
    print("  - Best 1-minute data support")
    print("  - High quality US stocks + crypto")
    print("  - 20 years of historical data")
    print("  - Real-time updates")
    
    print("\nSTORAGE ARCHITECTURE:")
    print("  - Primary: Parquet files in /home/jianjun/ats/data/STK/1min/")
    print("  - Cache: PostgreSQL/TimescaleDB (recent 1 month)")
    print("  - Format: Parquet with Snappy compression")
    print("  - Organization: /symbol/year/month/ structure")
    
    print("\nIMPLEMENTATION STEPS:")
    print("  1. Create 1-minute data directory structure")
    print("  2. Implement Polygon adapter (already done)")
    print("  3. Create data lifecycle management")
    print("  4. Set up database cache for hot data")
    print("  5. Implement unified query interface")

if __name__ == "__main__":
    main()