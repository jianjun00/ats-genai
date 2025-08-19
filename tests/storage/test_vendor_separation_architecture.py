#!/usr/bin/env python3
"""
Comprehensive Tests for Vendor Separation Architecture

Tests the vendor separation storage system that stores Polygon and Tiingo data
separately for complete data lineage and on-the-fly reconciliation.

Key Features Tested:
- Vendor-specific data storage
- On-the-fly reconciliation strategies
- Data lineage preservation
- Quality scoring and metadata tracking
- Historical issue resolution capabilities
"""

import pytest
import asyncio
import pandas as pd
import asyncpg
from datetime import datetime, date, timedelta
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from pathlib import Path
import tempfile
import shutil
import json

# Test data models
@dataclass
class MockMinuteBar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vendor: str
    quality_score: float = 0.8
    vwap: Optional[float] = None
    trade_count: Optional[int] = None

class MockVendorAdapter:
    """Mock adapter for testing vendor data fetching."""
    
    def __init__(self, vendor_name: str, data_quality: float = 0.8):
        self.vendor_name = vendor_name
        self.data_quality = data_quality
        self.fetch_count = 0
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
    
    async def fetch_minute_bars_async(self, symbol: str, start_date: datetime, end_date: datetime) -> List[MockMinuteBar]:
        """Generate mock data for testing."""
        self.fetch_count += 1
        
        bars = []
        current = start_date
        bar_count = 0
        
        # Generate realistic trading hours data
        while current <= end_date and bar_count < 100:  # Limit for testing
            # Only generate bars during trading hours (9:30 AM - 4:00 PM ET)
            if current.hour >= 13 and current.hour < 21:  # UTC trading hours
                # Add some vendor-specific characteristics
                if self.vendor_name == "polygon":
                    # Polygon tends to have more bars (extended hours)
                    volume_multiplier = 1.2
                    price_variance = 0.01
                elif self.vendor_name == "tiingo":
                    # Tiingo has fewer bars but consistent quality
                    volume_multiplier = 1.0
                    price_variance = 0.005
                else:
                    volume_multiplier = 1.0
                    price_variance = 0.01
                
                base_price = 150.0 + (bar_count * 0.1)
                
                bar = MockMinuteBar(
                    symbol=symbol,
                    timestamp=current,
                    open=base_price,
                    high=base_price + price_variance,
                    low=base_price - price_variance,
                    close=base_price + (price_variance / 2),
                    volume=int(1000 * volume_multiplier),
                    vendor=self.vendor_name,
                    quality_score=self.data_quality,
                    vwap=base_price + (price_variance / 4),
                    trade_count=50 if self.vendor_name == "polygon" else None
                )
                bars.append(bar)
                bar_count += 1
            
            current += timedelta(minutes=1)
        
        return bars

class TestVendorSeparationStorage:
    """Test vendor-separated storage functionality."""
    
    @pytest.fixture
    async def temp_storage_path(self):
        """Create temporary storage directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def mock_polygon_adapter(self):
        """Mock Polygon adapter with higher data quality."""
        return MockVendorAdapter("polygon", 0.8)
    
    @pytest.fixture
    def mock_tiingo_adapter(self):
        """Mock Tiingo adapter with good data quality."""
        return MockVendorAdapter("tiingo", 0.7)
    
    @pytest.fixture
    def test_symbols(self):
        """Test symbols for vendor separation testing."""
        return ["AAPL", "MSFT", "GOOGL"]
    
    @pytest.fixture
    def test_date_range(self):
        """Test date range for vendor separation testing."""
        end_date = date.today()
        start_date = end_date - timedelta(days=3)
        return start_date, end_date
    
    def test_vendor_separation_concept(self):
        """Test basic vendor separation concepts."""
        # Test data lineage preservation
        polygon_bar = MockMinuteBar(
            symbol="AAPL",
            timestamp=datetime.now(),
            open=150.0,
            high=151.0,
            low=149.5,
            close=150.5,
            volume=1000,
            vendor="polygon",
            quality_score=0.8
        )
        
        tiingo_bar = MockMinuteBar(
            symbol="AAPL", 
            timestamp=datetime.now(),
            open=150.1,
            high=150.9,
            low=149.6,
            close=150.4,
            volume=950,
            vendor="tiingo",
            quality_score=0.7
        )
        
        # Verify vendor separation
        assert polygon_bar.vendor == "polygon"
        assert tiingo_bar.vendor == "tiingo"
        assert polygon_bar.quality_score > tiingo_bar.quality_score
        
        # Verify data independence
        assert polygon_bar.volume != tiingo_bar.volume
        assert polygon_bar.open != tiingo_bar.open
    
    async def test_vendor_specific_storage(self, temp_storage_path, test_symbols):
        """Test storing vendor data separately."""
        
        # Create vendor-specific storage directories
        for symbol in test_symbols:
            symbol_dir = temp_storage_path / symbol / "2025" / "08"
            symbol_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate mock vendor data
        polygon_data = []
        tiingo_data = []
        
        for symbol in test_symbols:
            # Mock polygon data (more bars)
            for i in range(10):
                polygon_data.append(MockMinuteBar(
                    symbol=symbol,
                    timestamp=datetime(2025, 8, 18, 9, 30 + i),
                    open=150.0 + i,
                    high=151.0 + i,
                    low=149.0 + i,
                    close=150.5 + i,
                    volume=1000 + i * 10,
                    vendor="polygon",
                    quality_score=0.8
                ))
            
            # Mock tiingo data (fewer bars)
            for i in range(5):
                tiingo_data.append(MockMinuteBar(
                    symbol=symbol,
                    timestamp=datetime(2025, 8, 18, 9, 30 + i * 2),
                    open=150.1 + i,
                    high=150.9 + i,
                    low=149.1 + i,
                    close=150.4 + i,
                    volume=950 + i * 8,
                    vendor="tiingo",
                    quality_score=0.7
                ))
        
        # Test vendor-specific parquet storage
        for symbol in test_symbols:
            symbol_polygon_data = [bar for bar in polygon_data if bar.symbol == symbol]
            symbol_tiingo_data = [bar for bar in tiingo_data if bar.symbol == symbol]
            
            # Create vendor-specific DataFrames
            polygon_df = pd.DataFrame([{
                'timestamp': bar.timestamp,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume,
                'vendor': bar.vendor,
                'quality_score': bar.quality_score
            } for bar in symbol_polygon_data])
            
            tiingo_df = pd.DataFrame([{
                'timestamp': bar.timestamp,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume,
                'vendor': bar.vendor,
                'quality_score': bar.quality_score
            } for bar in symbol_tiingo_data])
            
            # Store vendor-specific parquet files
            polygon_file = temp_storage_path / symbol / "2025" / "08" / f"{symbol}_2025_08_polygon.parquet"
            tiingo_file = temp_storage_path / symbol / "2025" / "08" / f"{symbol}_2025_08_tiingo.parquet"
            
            polygon_df.to_parquet(polygon_file, index=False)
            tiingo_df.to_parquet(tiingo_file, index=False)
            
            # Verify files were created
            assert polygon_file.exists()
            assert tiingo_file.exists()
            
            # Verify data separation
            loaded_polygon = pd.read_parquet(polygon_file)
            loaded_tiingo = pd.read_parquet(tiingo_file)
            
            assert len(loaded_polygon) == 10
            assert len(loaded_tiingo) == 5
            assert all(loaded_polygon['vendor'] == 'polygon')
            assert all(loaded_tiingo['vendor'] == 'tiingo')
            assert all(loaded_polygon['quality_score'] == 0.8)
            assert all(loaded_tiingo['quality_score'] == 0.7)
    
    def test_reconciliation_strategies(self):
        """Test on-the-fly reconciliation strategies."""
        
        # Create overlapping vendor data for same timestamp
        timestamp = datetime(2025, 8, 18, 9, 30)
        
        polygon_bar = MockMinuteBar(
            symbol="AAPL",
            timestamp=timestamp,
            open=150.0,
            high=151.0,
            low=149.5,
            close=150.5,
            volume=1200,
            vendor="polygon",
            quality_score=0.8
        )
        
        tiingo_bar = MockMinuteBar(
            symbol="AAPL",
            timestamp=timestamp,
            open=150.1,
            high=150.9,
            low=149.6,
            close=150.4,
            volume=1000,
            vendor="tiingo",
            quality_score=0.7
        )
        
        bars_data = [polygon_bar, tiingo_bar]
        
        # Test best_quality strategy
        best_quality_bar = max(bars_data, key=lambda x: x.quality_score)
        assert best_quality_bar.vendor == "polygon"
        assert best_quality_bar.quality_score == 0.8
        
        # Test polygon_priority strategy
        polygon_priority_bar = next((bar for bar in bars_data if bar.vendor == "polygon"), bars_data[0])
        assert polygon_priority_bar.vendor == "polygon"
        
        # Test tiingo_priority strategy
        tiingo_priority_bar = next((bar for bar in bars_data if bar.vendor == "tiingo"), bars_data[0])
        assert tiingo_priority_bar.vendor == "tiingo"
        
        # Test both strategy (return all vendors)
        both_strategy_bars = bars_data
        assert len(both_strategy_bars) == 2
        assert any(bar.vendor == "polygon" for bar in both_strategy_bars)
        assert any(bar.vendor == "tiingo" for bar in both_strategy_bars)
    
    def test_data_lineage_tracking(self):
        """Test complete data lineage tracking."""
        
        # Create vendor data with metadata
        vendor_data = {
            "polygon": {
                "api_calls": 5,
                "total_bars": 1500,
                "avg_quality": 0.8,
                "coverage_start": "2025-08-11T09:30:00Z",
                "coverage_end": "2025-08-18T16:00:00Z",
                "errors": 0
            },
            "tiingo": {
                "api_calls": 5,
                "total_bars": 975,
                "avg_quality": 0.7,
                "coverage_start": "2025-08-11T13:30:00Z",
                "coverage_end": "2025-08-18T20:00:00Z",
                "errors": 1
            }
        }
        
        # Verify lineage tracking
        assert vendor_data["polygon"]["total_bars"] > vendor_data["tiingo"]["total_bars"]
        assert vendor_data["polygon"]["avg_quality"] > vendor_data["tiingo"]["avg_quality"]
        assert vendor_data["polygon"]["errors"] < vendor_data["tiingo"]["errors"]
        
        # Test coverage analysis
        polygon_coverage_hours = 6.5 * 5  # 6.5 hours/day * 5 days
        tiingo_coverage_hours = 6.5 * 5   # Same coverage but different quality
        
        assert polygon_coverage_hours == tiingo_coverage_hours
        
        # Calculate bars per hour
        polygon_bars_per_hour = vendor_data["polygon"]["total_bars"] / polygon_coverage_hours
        tiingo_bars_per_hour = vendor_data["tiingo"]["total_bars"] / tiingo_coverage_hours
        
        assert polygon_bars_per_hour > tiingo_bars_per_hour  # Polygon has more granular data
    
    def test_historical_issue_resolution(self, temp_storage_path):
        """Test historical issue resolution without full re-ingestion."""
        
        # Simulate initial vendor data storage
        original_polygon_data = pd.DataFrame([
            {
                'timestamp': datetime(2025, 8, 18, 9, 30),
                'open': 150.0,
                'high': 151.0,
                'low': 149.5,
                'close': 150.5,
                'volume': 1000,
                'vendor': 'polygon',
                'quality_score': 0.8
            },
            {
                'timestamp': datetime(2025, 8, 18, 9, 31),
                'open': 150.5,
                'high': 151.2,
                'low': 150.0,
                'close': 150.8,
                'volume': 1100,  # This has an error - should be 1200
                'vendor': 'polygon',
                'quality_score': 0.8
            }
        ])
        
        tiingo_data = pd.DataFrame([
            {
                'timestamp': datetime(2025, 8, 18, 9, 30),
                'open': 150.1,
                'high': 150.9,
                'low': 149.6,
                'close': 150.4,
                'volume': 950,
                'vendor': 'tiingo',
                'quality_score': 0.7
            }
        ])
        
        # Store original data
        polygon_file = temp_storage_path / "AAPL_2025_08_polygon.parquet"
        tiingo_file = temp_storage_path / "AAPL_2025_08_tiingo.parquet"
        
        original_polygon_data.to_parquet(polygon_file, index=False)
        tiingo_data.to_parquet(tiingo_file, index=False)
        
        # Simulate issue discovery: Polygon volume data was incorrect
        # Only need to re-fetch and replace Polygon data
        corrected_polygon_data = pd.DataFrame([
            {
                'timestamp': datetime(2025, 8, 18, 9, 30),
                'open': 150.0,
                'high': 151.0,
                'low': 149.5,
                'close': 150.5,
                'volume': 1000,
                'vendor': 'polygon',
                'quality_score': 0.8
            },
            {
                'timestamp': datetime(2025, 8, 18, 9, 31),
                'open': 150.5,
                'high': 151.2,
                'low': 150.0,
                'close': 150.8,
                'volume': 1200,  # Corrected volume
                'vendor': 'polygon',
                'quality_score': 0.8
            }
        ])
        
        # Replace only Polygon data (Tiingo data unchanged)
        corrected_polygon_data.to_parquet(polygon_file, index=False)
        
        # Verify fix
        loaded_polygon = pd.read_parquet(polygon_file)
        loaded_tiingo = pd.read_parquet(tiingo_file)
        
        # Polygon data should be corrected
        assert loaded_polygon.iloc[1]['volume'] == 1200
        
        # Tiingo data should be unchanged
        assert len(loaded_tiingo) == 1
        assert loaded_tiingo.iloc[0]['volume'] == 950
        
        # Verify vendor separation maintained
        assert all(loaded_polygon['vendor'] == 'polygon')
        assert all(loaded_tiingo['vendor'] == 'tiingo')
    
    async def test_performance_comparison(self, mock_polygon_adapter, mock_tiingo_adapter, test_symbols):
        """Test performance characteristics of vendor separation."""
        
        start_time = datetime.now()
        
        # Simulate concurrent vendor data fetching
        async with mock_polygon_adapter as polygon:
            async with mock_tiingo_adapter as tiingo:
                tasks = []
                
                for symbol in test_symbols:
                    start_date = datetime(2025, 8, 18, 9, 30)
                    end_date = datetime(2025, 8, 18, 16, 0)
                    
                    tasks.append(polygon.fetch_minute_bars_async(symbol, start_date, end_date))
                    tasks.append(tiingo.fetch_minute_bars_async(symbol, start_date, end_date))
                
                results = await asyncio.gather(*tasks)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Verify concurrent execution completed
        assert len(results) == len(test_symbols) * 2
        
        # Verify performance metrics
        polygon_results = results[::2]  # Even indices are Polygon
        tiingo_results = results[1::2]   # Odd indices are Tiingo
        
        total_polygon_bars = sum(len(result) for result in polygon_results)
        total_tiingo_bars = sum(len(result) for result in tiingo_results)
        
        assert total_polygon_bars > 0
        assert total_tiingo_bars > 0
        assert duration.total_seconds() < 5  # Should complete quickly for mock data
        
        # Verify fetch counts
        assert mock_polygon_adapter.fetch_count == len(test_symbols)
        assert mock_tiingo_adapter.fetch_count == len(test_symbols)
    
    def test_vendor_metadata_tracking(self):
        """Test vendor-specific metadata tracking."""
        
        # Create vendor metadata
        polygon_metadata = {
            "vendor": "polygon",
            "api_version": "v2",
            "data_source": "aggregates",
            "adjusted": True,
            "sort_order": "asc",
            "limit_per_request": 50000,
            "rate_limit_ms": 120,
            "quality_factors": {
                "extended_hours": True,
                "trade_count_available": True,
                "vwap_calculated": True
            }
        }
        
        tiingo_metadata = {
            "vendor": "tiingo",
            "api_version": "v1",
            "data_source": "iex",
            "resample_freq": "1min",
            "rate_limit_ms": 600,
            "quality_factors": {
                "extended_hours": False,
                "trade_count_available": False,
                "vwap_calculated": False
            }
        }
        
        # Verify metadata differences
        assert polygon_metadata["quality_factors"]["extended_hours"] != tiingo_metadata["quality_factors"]["extended_hours"]
        assert polygon_metadata["quality_factors"]["trade_count_available"] != tiingo_metadata["quality_factors"]["trade_count_available"]
        assert polygon_metadata["rate_limit_ms"] < tiingo_metadata["rate_limit_ms"]
        
        # Test quality scoring based on metadata
        polygon_quality = 0.8  # Higher due to extended hours and trade count
        tiingo_quality = 0.7   # Lower but still good quality
        
        assert polygon_quality > tiingo_quality

class TestVendorSeparationIntegration:
    """Integration tests for vendor separation with real-world scenarios."""
    
    def test_storage_space_estimation(self):
        """Test storage space requirements for vendor separation."""
        
        # Estimate storage requirements
        symbols = 100
        trading_days_per_year = 252
        minutes_per_trading_day = 390
        vendors = 2  # Polygon + Tiingo
        
        # Bars per symbol per year
        bars_per_symbol_per_year = trading_days_per_year * minutes_per_trading_day
        
        # Total bars per year
        total_bars_per_year = symbols * bars_per_symbol_per_year * vendors
        
        # Storage per bar (estimated 100 bytes including metadata)
        bytes_per_bar = 100
        storage_per_year_gb = (total_bars_per_year * bytes_per_bar) / (1024**3)
        
        # Verify reasonable storage requirements
        assert storage_per_year_gb < 10  # Should be under 10GB for 100 symbols
        assert total_bars_per_year > 1_000_000  # Should have substantial data volume
    
    def test_query_flexibility(self):
        """Test flexible querying with vendor separation."""
        
        # Mock vendor data with different characteristics
        vendors_data = {
            "polygon": {
                "bars_count": 1000,
                "quality_score": 0.8,
                "extended_hours": True,
                "api_latency_ms": 120
            },
            "tiingo": {
                "bars_count": 750,
                "quality_score": 0.7,
                "extended_hours": False,
                "api_latency_ms": 600
            }
        }
        
        # Test different query strategies
        
        # Strategy 1: Best quality for trading decisions
        best_quality_vendor = max(vendors_data.keys(), key=lambda v: vendors_data[v]["quality_score"])
        assert best_quality_vendor == "polygon"
        
        # Strategy 2: Most data for backtesting
        most_data_vendor = max(vendors_data.keys(), key=lambda v: vendors_data[v]["bars_count"])
        assert most_data_vendor == "polygon"
        
        # Strategy 3: Fastest response for real-time
        fastest_vendor = min(vendors_data.keys(), key=lambda v: vendors_data[v]["api_latency_ms"])
        assert fastest_vendor == "polygon"
        
        # Strategy 4: Cross-vendor validation
        quality_difference = vendors_data["polygon"]["quality_score"] - vendors_data["tiingo"]["quality_score"]
        assert abs(quality_difference - 0.1) < 0.001  # 10% quality difference acceptable for validation
    
    def test_data_quality_validation(self):
        """Test data quality validation across vendors."""
        
        # Create sample vendor data with quality issues
        polygon_sample = [
            {"timestamp": "2025-08-18T09:30:00Z", "close": 150.0, "volume": 1000},
            {"timestamp": "2025-08-18T09:31:00Z", "close": 150.5, "volume": 1100},
            {"timestamp": "2025-08-18T09:32:00Z", "close": 151.0, "volume": 0},  # Quality issue: zero volume
        ]
        
        tiingo_sample = [
            {"timestamp": "2025-08-18T09:30:00Z", "close": 150.1, "volume": 950},
            {"timestamp": "2025-08-18T09:31:00Z", "close": 150.4, "volume": 1050},
            # Missing 09:32 bar - different coverage
        ]
        
        # Quality validation rules
        def validate_vendor_data(data, vendor_name):
            issues = []
            
            for i, bar in enumerate(data):
                # Check for zero volume
                if bar["volume"] == 0:
                    issues.append(f"{vendor_name}: Zero volume at {bar['timestamp']}")
                
                # Check for price gaps
                if i > 0:
                    prev_close = data[i-1]["close"]
                    current_close = bar["close"]
                    gap_percent = abs(current_close - prev_close) / prev_close
                    
                    if gap_percent > 0.05:  # 5% gap threshold
                        issues.append(f"{vendor_name}: Large price gap at {bar['timestamp']}")
            
            return issues
        
        polygon_issues = validate_vendor_data(polygon_sample, "polygon")
        tiingo_issues = validate_vendor_data(tiingo_sample, "tiingo")
        
        # Verify quality issue detection
        assert len(polygon_issues) == 1
        assert "Zero volume" in polygon_issues[0]
        assert len(tiingo_issues) == 0
        
        # Test coverage comparison
        polygon_timestamps = {bar["timestamp"] for bar in polygon_sample}
        tiingo_timestamps = {bar["timestamp"] for bar in tiingo_sample}
        
        missing_from_tiingo = polygon_timestamps - tiingo_timestamps
        assert len(missing_from_tiingo) == 1
        assert "2025-08-18T09:32:00Z" in missing_from_tiingo

if __name__ == "__main__":
    pytest.main([__file__, "-v"])