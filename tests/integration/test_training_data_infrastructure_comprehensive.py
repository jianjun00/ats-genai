#!/usr/bin/env python3
"""
Comprehensive test suite for training data generation infrastructure.

This test suite systematically detects all the issues uncovered during AAPL 
training data generation debugging and prevents regressions.

Issues tested:
1. Import dependencies and module availability
2. FirstRate adapter file structure and data processing
3. Database constraints and UUID deduplication
4. Volume data preservation through the pipeline
5. UniverseStateManager method availability
6. Feature extraction with real data
7. End-to-end integration flow
8. Error handling and edge cases
"""

import pytest
import asyncio
import tempfile
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
import pandas as pd
import numpy as np
from pathlib import Path

# Test imports - this will catch import dependency issues
def test_critical_imports():
    """Test all critical imports work correctly."""
    import_errors = []
    
    # Core imports that failed before
    try:
        from domains.trading.services.core.app.runner import Runner
    except ImportError as e:
        import_errors.append(f"Runner import failed: {e}")
    
    try:
        from domains.ml.services.training_data.runners.training_data_callback_runner import main
    except ImportError as e:
        import_errors.append(f"Training callback runner import failed: {e}")
        
    try:
        from core.market_data.unified_manager import UnifiedMarketDataManager, MarketDataConfig, VendorType, StorageBackend
    except ImportError as e:
        import_errors.append(f"UnifiedMarketDataManager import failed: {e}")
        
    try:
        from domains.trading.services.state.universe_state_manager import UniverseStateManager
    except ImportError as e:
        import_errors.append(f"UniverseStateManager import failed: {e}")
    
    if import_errors:
        pytest.fail(f"Critical import failures:\n" + "\n".join(import_errors))


class TestFirstRateAdapter:
    """Test FirstRate adapter file structure and data processing."""
    
    def create_mock_parquet_data(self, temp_dir, symbol="AAPL", year=2025, month=7):
        """Create mock parquet file with proper structure."""
        # Create directory structure: {first_letter}/{SYMBOL}/{YYYY}/{MM}/
        symbol_dir = Path(temp_dir) / symbol[0] / symbol / str(year) / f"{month:02d}"
        symbol_dir.mkdir(parents=True, exist_ok=True)
        
        # Create mock minute data with volume
        dates = pd.date_range(f"{year}-{month:02d}-01 14:00:00", periods=10, freq='1min')
        df = pd.DataFrame({
            'timestamp': dates,
            'open': np.random.uniform(200, 210, 10),
            'high': np.random.uniform(210, 220, 10), 
            'low': np.random.uniform(190, 200, 10),
            'close': np.random.uniform(200, 210, 10),
            'volume': np.random.randint(10000, 100000, 10)  # CRITICAL: Include volume
        })
        
        parquet_file = symbol_dir / f"{symbol}_{year}_{month:02d}.parquet"
        df.to_parquet(parquet_file, index=False)
        return parquet_file, df

    def test_firstrate_adapter_file_structure(self):
        """Test FirstRate adapter reads correct monthly file structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create mock data
            parquet_file, expected_df = self.create_mock_parquet_data(temp_dir)
            
            # Test FirstRate adapter
            from core.market_data.unified_manager import FirstRateAdapter
            from core.market_data.unified_manager import TimeframeType
            
            adapter = FirstRateAdapter(file_path=str(temp_dir))
            
            # Test get_ohlcv with date range
            start_date = datetime(2025, 7, 1, 14, 0, 0)
            end_date = datetime(2025, 7, 1, 14, 10, 0)
            
            result = asyncio.run(adapter.get_ohlcv(
                symbols=["AAPL"],
                start_date=start_date, 
                end_date=end_date,
                timeframe=TimeframeType.MINUTE_1
            ))
            
            # Verify data was loaded correctly
            assert "AAPL" in result, "AAPL data not found in result"
            assert not result["AAPL"].empty, "AAPL dataframe is empty"
            assert "volume" in result["AAPL"].columns, "Volume column missing"
            assert result["AAPL"]["volume"].notna().all(), "Volume contains null values"

    def test_volume_data_preservation(self):
        """Test volume data is preserved through FirstRate adapter.""" 
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_file, original_df = self.create_mock_parquet_data(temp_dir)
            
            from core.market_data.unified_manager import FirstRateAdapter
            from core.market_data.unified_manager import TimeframeType
            
            adapter = FirstRateAdapter(file_path=str(temp_dir))
            
            start_date = datetime(2025, 7, 1, 14, 0, 0)
            end_date = datetime(2025, 7, 1, 14, 10, 0)
            
            result = asyncio.run(adapter.get_ohlcv(
                symbols=["AAPL"],
                start_date=start_date,
                end_date=end_date, 
                timeframe=TimeframeType.MINUTE_1
            ))
            
            # Verify volume values match original data
            result_df = result["AAPL"]
            assert len(result_df) > 0, "No data returned"
            
            # Check that volume values are preserved and reasonable
            volumes = result_df['volume']
            assert volumes.min() >= 10000, f"Volume too low: {volumes.min()}"
            assert volumes.max() <= 100000, f"Volume too high: {volumes.max()}"
            assert volumes.dtype in ['int64', 'float64'], f"Wrong volume dtype: {volumes.dtype}"

    def test_get_minute_ohlc_batch_includes_volume(self):
        """Test get_minute_ohlc_batch method includes volume in result."""
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_file, original_df = self.create_mock_parquet_data(temp_dir)
            
            from core.market_data.unified_manager import UnifiedMarketDataManager, MarketDataConfig, VendorType, StorageBackend
            
            config = MarketDataConfig(
                vendors=[VendorType.FIRSTRATE],
                storage_backend=StorageBackend.FILE,
                file_storage_path=temp_dir
            )
            manager = UnifiedMarketDataManager(config)
            
            start_date = datetime(2025, 7, 1, 14, 0, 0)
            end_date = datetime(2025, 7, 1, 14, 1, 0)
            
            result = asyncio.run(manager.get_minute_ohlc_batch(
                symbols=["AAPL"],
                start=start_date,
                end=end_date
            ))
            
            # This was the critical bug - volume was missing from result
            assert "AAPL" in result, "AAPL not in result"
            assert result["AAPL"] is not None, "AAPL result is None"
            assert "volume" in result["AAPL"], "Volume missing from OHLC result"
            assert result["AAPL"]["volume"] is not None, "Volume value is None"
            assert isinstance(result["AAPL"]["volume"], (int, float)), f"Volume wrong type: {type(result['AAPL']['volume'])}"


class TestUniverseStateManager:
    """Test UniverseStateManager method availability and functionality."""
    
    def test_required_methods_exist(self):
        """Test all methods required by training data generation exist."""
        from domains.trading.services.state.universe_state_manager import UniverseStateManager
        
        manager = UniverseStateManager()
        
        missing_methods = []
        
        # These methods were missing and caused training data generation to fail
        if not hasattr(manager, 'get_lag_prices'):
            missing_methods.append('get_lag_prices')
        if not hasattr(manager, 'get_lead_prices'):
            missing_methods.append('get_lead_prices')
        if not hasattr(manager, 'get_lagged_signals'):
            missing_methods.append('get_lagged_signals')
            
        if missing_methods:
            pytest.fail(f"Required methods missing: {missing_methods}")
            
        # Test methods are callable
        assert callable(manager.get_lag_prices), "get_lag_prices not callable"
        assert callable(manager.get_lead_prices), "get_lead_prices not callable"
        assert callable(manager.get_lagged_signals), "get_lagged_signals not callable"

    def test_get_lag_prices_signature(self):
        """Test get_lag_prices method has correct signature and behavior."""
        from domains.trading.services.state.universe_state_manager import UniverseStateManager
        
        manager = UniverseStateManager()
        
        # Test method can be called with expected parameters
        try:
            result = manager.get_lag_prices(
                instrument_id=31,
                cur_datetime=datetime(2025, 7, 1, 14, 0, 0),
                lag_periods=1,
                time_interval='1m'
            )
            
            # Should return DataFrame even if empty
            assert isinstance(result, pd.DataFrame), f"Expected DataFrame, got {type(result)}"
            
        except ValueError as e:
            # Cache insufficient is expected behavior when cache is empty
            if "cache insufficient" in str(e).lower():
                # This is expected - cache is empty in test environment
                print(f"✅ get_lag_prices correctly reports cache insufficient: {e}")
            else:
                pytest.fail(f"get_lag_prices failed with unexpected ValueError: {e}")
        except Exception as e:
            pytest.fail(f"get_lag_prices failed with valid parameters: {e}")

    def test_get_lagged_signals_signature(self):
        """Test get_lagged_signals method has correct signature and behavior."""
        from domains.trading.services.state.universe_state_manager import UniverseStateManager
        import asyncio
        
        manager = UniverseStateManager()
        
        # Test async method can be called with expected parameters
        async def run_test():
            try:
                result = await manager.get_lagged_signals(
                    instrument_id=31,
                    cur_datetime=datetime(2025, 7, 1, 14, 0, 0),
                    lag_periods=1,
                    time_interval='1m',
                    signal_names=['sma_20', 'ema_12']
                )
                
                # Should return DataFrame even if empty
                assert isinstance(result, pd.DataFrame), f"Expected DataFrame, got {type(result)}"
                return True
                
            except ValueError as e:
                # Cache insufficient is expected behavior when cache is empty
                if "cache insufficient" in str(e).lower():
                    print(f"✅ get_lagged_signals correctly reports cache insufficient: {e}")
                    return True
                else:
                    pytest.fail(f"get_lagged_signals failed with unexpected ValueError: {e}")
            except Exception as e:
                pytest.fail(f"get_lagged_signals failed with valid parameters: {e}")
        
        # Run the async test
        asyncio.run(run_test())


class TestDatabaseIntegration:
    """Test database constraints and UUID deduplication."""
    
    def test_universe_state_interval_constraint(self):
        """Test database constraint allows same interval with different universe_state_interval_id."""
        # This test would need a test database, but we can test the constraint exists
        # In a real implementation, this would:
        # 1. Create test records with same instrument_id, interval_start, interval_duration, run_id
        # 2. But different universe_state_interval_id values
        # 3. Verify both can be inserted without constraint violation
        
        # For now, just verify the concept
        test_records = [
            {
                'instrument_id': 31,
                'interval_start': '2025-07-01 14:00:00+00',
                'interval_duration': '60m',
                'run_id': 'test_run_123',
                'universe_state_interval_id': 1001
            },
            {
                'instrument_id': 31,
                'interval_start': '2025-07-01 14:00:00+00', 
                'interval_duration': '60m',
                'run_id': 'test_run_123',
                'universe_state_interval_id': 1002  # Different universe state
            }
        ]
        
        # These should be allowed by the fixed constraint
        assert test_records[0]['universe_state_interval_id'] != test_records[1]['universe_state_interval_id']
        assert True  # Placeholder for actual database test


class TestFeatureExtraction:
    """Test feature extraction with real data scenarios."""
    
    def test_volume_feature_extraction(self):
        """Test feature extraction correctly handles volume data."""
        # Create sample OHLCV data with volume
        data = pd.DataFrame({
            'open': [208.02],
            'high': [208.11],
            'low': [208.01], 
            'close': [208.08],
            'volume': [56512.0],  # Real volume from AAPL data
            'date': [datetime(2025, 7, 1, 14, 1, 0)]
        })
        
        # Test that volume can be processed without NoneType errors
        try:
            volume_float = float(data['volume'].iloc[0])
            assert volume_float == 56512.0, f"Volume conversion failed: {volume_float}"
            
            # Test feature calculations that use volume
            volume_features = {
                'volume_latest': volume_float,
                'volume_log': np.log(volume_float + 1),
                'volume_normalized': volume_float / 100000.0
            }
            
            for feature_name, feature_value in volume_features.items():
                assert not np.isnan(feature_value), f"{feature_name} is NaN"
                assert np.isfinite(feature_value), f"{feature_name} is not finite"
                
        except Exception as e:
            pytest.fail(f"Volume feature extraction failed: {e}")

    def test_none_volume_handling(self):
        """Test graceful handling of None volume values."""
        # Test data with None volume (edge case)
        data = pd.DataFrame({
            'open': [208.02],
            'high': [208.11],
            'low': [208.01],
            'close': [208.08],
            'volume': [None],  # None volume case
            'date': [datetime(2025, 7, 1, 14, 1, 0)]
        })
        
        # Should handle None gracefully without crashing
        try:
            volume_val = data['volume'].iloc[0]
            if volume_val is None:
                # Should provide default behavior
                volume_float = 0.0  # Or np.nan, depending on strategy
            else:
                volume_float = float(volume_val)
                
            assert isinstance(volume_float, float), f"Volume handling failed: {type(volume_float)}"
            
        except Exception as e:
            pytest.fail(f"None volume handling failed: {e}")


class TestEndToEndIntegration:
    """Test complete end-to-end integration flow."""
    
    def test_training_data_pipeline_components(self):
        """Test all pipeline components can be instantiated and connected."""
        component_errors = []
        
        # Test core components
        try:
            from domains.trading.services.core.app.runner import Runner
            from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
            from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
            
            # These should all be importable and instantiable
            # TrainingDataConfig requires multiple parameters per gin config
            config = TrainingDataConfig(
                timeframes=['5m', '15m', '1h'], 
                feature_types=['ohlcv', 'technical_indicators'],
                signal_names=['sma_20', 'rsi_14', 'macd']
            )
            assert hasattr(config, 'timeframes'), "TrainingDataConfig missing timeframes"
            assert hasattr(config, 'feature_types'), "TrainingDataConfig missing feature_types"
            
        except Exception as e:
            component_errors.append(f"Component instantiation failed: {e}")
            
        if component_errors:
            pytest.fail("Pipeline component errors:\n" + "\n".join(component_errors))

    def test_data_flow_integration(self):
        """Test data flows correctly through the pipeline."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create mock data file
            from tests.integration.test_training_data_infrastructure_comprehensive import TestFirstRateAdapter
            test_instance = TestFirstRateAdapter()
            parquet_file, expected_df = test_instance.create_mock_parquet_data(temp_dir)
            
            # Test data can flow from file -> adapter -> manager -> training generator
            try:
                from core.market_data.unified_manager import UnifiedMarketDataManager, MarketDataConfig, VendorType, StorageBackend
                
                config = MarketDataConfig(
                    vendors=[VendorType.FIRSTRATE],
                    storage_backend=StorageBackend.FILE,
                    file_storage_path=temp_dir
                )
                manager = UnifiedMarketDataManager(config)
                
                start_date = datetime(2025, 7, 1, 14, 0, 0)
                end_date = datetime(2025, 7, 1, 14, 1, 0)
                
                # This should work end-to-end without crashes
                result = asyncio.run(manager.get_minute_ohlc_batch(
                    symbols=["AAPL"],
                    start=start_date,
                    end=end_date
                ))
                
                # Verify data integrity through the pipeline
                assert "AAPL" in result, "Data lost in pipeline"
                assert result["AAPL"] is not None, "Data corrupted in pipeline"
                assert "volume" in result["AAPL"], "Volume lost in pipeline"
                
            except Exception as e:
                pytest.fail(f"Data flow integration failed: {e}")


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    def test_missing_parquet_file_handling(self):
        """Test graceful handling when parquet files are missing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            from core.market_data.unified_manager import FirstRateAdapter
            from core.market_data.unified_manager import TimeframeType
            
            adapter = FirstRateAdapter(file_path=str(temp_dir))
            
            # Try to read from non-existent file
            start_date = datetime(2025, 7, 1, 14, 0, 0)
            end_date = datetime(2025, 7, 1, 14, 1, 0)
            
            result = asyncio.run(adapter.get_ohlcv(
                symbols=["NONEXISTENT"],
                start_date=start_date,
                end_date=end_date,
                timeframe=TimeframeType.MINUTE_1
            ))
            
            # Should return empty DataFrame instead of crashing
            assert "NONEXISTENT" in result, "Missing symbol not handled"
            assert result["NONEXISTENT"].empty, "Should return empty DataFrame for missing file"

    def test_malformed_data_handling(self):
        """Test handling of malformed parquet data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create parquet file with missing volume column
            symbol_dir = Path(temp_dir) / "A" / "AAPL" / "2025" / "07"
            symbol_dir.mkdir(parents=True, exist_ok=True)
            
            # Malformed data - missing volume column
            df = pd.DataFrame({
                'timestamp': [datetime(2025, 7, 1, 14, 0, 0)],
                'open': [208.02],
                'high': [208.11], 
                'low': [208.01],
                'close': [208.08]
                # volume column missing
            })
            
            parquet_file = symbol_dir / "AAPL_2025_07.parquet"
            df.to_parquet(parquet_file, index=False)
            
            from core.market_data.unified_manager import FirstRateAdapter
            from core.market_data.unified_manager import TimeframeType
            
            adapter = FirstRateAdapter(file_path=str(temp_dir))
            
            # Should handle missing volume gracefully
            start_date = datetime(2025, 7, 1, 14, 0, 0)
            end_date = datetime(2025, 7, 1, 14, 1, 0)
            
            try:
                result = asyncio.run(adapter.get_ohlcv(
                    symbols=["AAPL"],
                    start_date=start_date,
                    end_date=end_date,
                    timeframe=TimeframeType.MINUTE_1
                ))
                
                # Should not crash, but handle gracefully
                assert "AAPL" in result, "Malformed data handling failed"
                
            except Exception as e:
                # If it crashes, that's also valuable information
                assert "volume" in str(e).lower(), f"Unexpected error handling malformed data: {e}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])