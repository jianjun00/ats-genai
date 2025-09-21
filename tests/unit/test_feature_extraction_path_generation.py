#!/usr/bin/env python3
"""
Test cases for feature extraction path generation logic.

Tests the new path convention with feature groups:
/data/training_data/{dataset_id}/{feature_group}/{symbol_date_range}/{timeframe}/{symbol_date_range}_{feature_group}.arrayrecord
"""

import pytest
import tempfile
from datetime import date
from pathlib import Path
from typing import List, Dict

from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback


class TestFeatureExtractionPathGeneration:
    """Test feature extraction file path generation with feature groups."""

    def test_generate_ohlcv_basic_path(self):
        """Test path generation for OHLCV basic feature group."""
        # Expected: /data/training_data/dataset_123/ohlcv_basic/AAPL_2025_07/5m/AAPL_2025_07_ohlcv_basic.arrayrecord
        
        dataset_id = "dataset_20250921_072901"
        feature_group = "ohlcv_basic"
        symbol = "AAPL"
        year_month = "2025_07"
        timeframe = "5m"
        
        expected_path = f"/data/training_data/{dataset_id}/{feature_group}/{symbol}_{year_month}/{timeframe}/{symbol}_{year_month}_{feature_group}.arrayrecord"
        
        generated_path = generate_feature_extraction_path(
            output_dir="/data/training_data",
            dataset_id=dataset_id,
            feature_group=feature_group,
            symbol=symbol,
            year_month=year_month,
            timeframe=timeframe
        )
        
        assert generated_path == expected_path

    def test_generate_technical_momentum_path(self):
        """Test path generation for technical momentum feature group."""
        # Expected: /data/training_data/dataset_123/technical_momentum/TSLA_2025_08/15m/TSLA_2025_08_technical_momentum.arrayrecord
        
        dataset_id = "dataset_20250921_073045"
        feature_group = "technical_momentum"
        symbol = "TSLA"
        year_month = "2025_08"
        timeframe = "15m"
        
        expected_path = f"/data/training_data/{dataset_id}/{feature_group}/{symbol}_{year_month}/{timeframe}/{symbol}_{year_month}_{feature_group}.arrayrecord"
        
        generated_path = generate_feature_extraction_path(
            output_dir="/data/training_data",
            dataset_id=dataset_id,
            feature_group=feature_group,
            symbol=symbol,
            year_month=year_month,
            timeframe=timeframe
        )
        
        assert generated_path == expected_path

    def test_generate_multiple_feature_groups_same_symbol(self):
        """Test that same symbol generates different paths for different feature groups."""
        
        dataset_id = "dataset_20250921_072901"
        symbol = "AAPL"
        year_month = "2025_07"
        timeframe = "5m"
        
        ohlcv_path = generate_feature_extraction_path(
            output_dir="/data/training_data",
            dataset_id=dataset_id,
            feature_group="ohlcv_basic",
            symbol=symbol,
            year_month=year_month,
            timeframe=timeframe
        )
        
        technical_path = generate_feature_extraction_path(
            output_dir="/data/training_data",
            dataset_id=dataset_id,
            feature_group="technical_momentum",
            symbol=symbol,
            year_month=year_month,
            timeframe=timeframe
        )
        
        # Paths should be different
        assert ohlcv_path != technical_path
        
        # Should contain correct feature group in path
        assert "ohlcv_basic" in ohlcv_path
        assert "technical_momentum" in technical_path
        
        # Should have feature group in filename
        assert "AAPL_2025_07_ohlcv_basic.arrayrecord" in ohlcv_path
        assert "AAPL_2025_07_technical_momentum.arrayrecord" in technical_path

    def test_generate_multiple_timeframes_same_feature_group(self):
        """Test that same feature group generates different paths for different timeframes."""
        
        dataset_id = "dataset_20250921_072901"
        feature_group = "ohlcv_basic"
        symbol = "AAPL"
        year_month = "2025_07"
        
        timeframes = ["5m", "15m", "1h", "1d"]
        paths = []
        
        for timeframe in timeframes:
            path = generate_feature_extraction_path(
                output_dir="/data/training_data",
                dataset_id=dataset_id,
                feature_group=feature_group,
                symbol=symbol,
                year_month=year_month,
                timeframe=timeframe
            )
            paths.append(path)
        
        # All paths should be unique
        assert len(set(paths)) == len(timeframes)
        
        # Each path should contain the correct timeframe
        for i, timeframe in enumerate(timeframes):
            assert f"/{timeframe}/" in paths[i]
            assert "AAPL_2025_07_ohlcv_basic.arrayrecord" in paths[i]

    def test_path_structure_validation(self):
        """Test that generated paths follow the exact specified structure."""
        
        dataset_id = "dataset_20250921_072901"
        feature_group = "technical_volatility"
        symbol = "MSFT"
        year_month = "2025_09"
        timeframe = "1h"
        
        path = generate_feature_extraction_path(
            output_dir="/data/training_data",
            dataset_id=dataset_id,
            feature_group=feature_group,
            symbol=symbol,
            year_month=year_month,
            timeframe=timeframe
        )
        
        # Parse path components
        path_obj = Path(path)
        parts = path_obj.parts
        
        # Validate structure: /data/training_data/{dataset_id}/{feature_group}/{symbol_date}/{timeframe}/{filename}
        assert parts[-5] == "training_data"  # Base directory
        assert parts[-4] == dataset_id       # Dataset ID
        assert parts[-3] == feature_group    # Feature group
        assert parts[-2] == f"{symbol}_{year_month}"  # Symbol and date
        assert parts[-1] == timeframe        # Timeframe directory
        
        # Validate filename
        filename = path_obj.name
        expected_filename = f"{symbol}_{year_month}_{feature_group}.arrayrecord"
        assert filename == expected_filename

    def test_directory_creation_with_feature_groups(self):
        """Test that directory structure is created correctly with feature groups."""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_id = "dataset_test_123"
            feature_group = "ohlcv_basic"
            symbol = "TEST"
            year_month = "2025_01"
            timeframe = "5m"
            
            path = generate_feature_extraction_path(
                output_dir=temp_dir,
                dataset_id=dataset_id,
                feature_group=feature_group,
                symbol=symbol,
                year_month=year_month,
                timeframe=timeframe
            )
            
            # Create the directory structure
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            
            # Verify structure exists
            expected_dirs = [
                temp_dir,
                f"{temp_dir}/{dataset_id}",
                f"{temp_dir}/{dataset_id}/{feature_group}",
                f"{temp_dir}/{dataset_id}/{feature_group}/{symbol}_{year_month}",
                f"{temp_dir}/{dataset_id}/{feature_group}/{symbol}_{year_month}/{timeframe}"
            ]
            
            for expected_dir in expected_dirs:
                assert Path(expected_dir).exists()
                assert Path(expected_dir).is_dir()

    def test_feature_group_isolation(self):
        """Test that feature groups are properly isolated in different directories."""
        
        dataset_id = "dataset_20250921_072901"
        symbol = "AAPL"
        year_month = "2025_07"
        timeframe = "5m"
        
        feature_groups = ["ohlcv_basic", "technical_momentum", "technical_volatility", "fundamental_quarterly"]
        paths = []
        
        for feature_group in feature_groups:
            path = generate_feature_extraction_path(
                output_dir="/data/training_data",
                dataset_id=dataset_id,
                feature_group=feature_group,
                symbol=symbol,
                year_month=year_month,
                timeframe=timeframe
            )
            paths.append(path)
        
        # Each feature group should have its own directory
        for i, feature_group in enumerate(feature_groups):
            assert f"/{feature_group}/" in paths[i]
            assert f"_{feature_group}.arrayrecord" in paths[i]
        
        # All paths should be unique
        assert len(set(paths)) == len(feature_groups)

    def test_legacy_path_vs_new_path_difference(self):
        """Test that new feature group paths are different from legacy paths."""
        
        dataset_id = "dataset_20250921_072901"
        symbol = "AAPL"
        year_month = "2025_07"
        timeframe = "5m"
        
        # Legacy path (current implementation)
        legacy_path = f"/data/training_data/{dataset_id}/{symbol}_{year_month}/{timeframe}/{symbol}_{year_month}.arrayrecord"
        
        # New path with feature group
        new_path = generate_feature_extraction_path(
            output_dir="/data/training_data",
            dataset_id=dataset_id,
            feature_group="ohlcv_basic",
            symbol=symbol,
            year_month=year_month,
            timeframe=timeframe
        )
        
        # Paths should be different
        assert legacy_path != new_path
        
        # New path should include feature group
        assert "ohlcv_basic" in new_path
        assert "ohlcv_basic" not in legacy_path


def generate_feature_extraction_path(output_dir: str, dataset_id: str, feature_group: str, 
                                   symbol: str, year_month: str, timeframe: str) -> str:
    """
    Generate feature extraction file path with feature group.
    
    Path format: {output_dir}/{dataset_id}/{feature_group}/{symbol_date_range}/{timeframe}/{symbol_date_range}_{feature_group}.arrayrecord
    
    Args:
        output_dir: Base output directory
        dataset_id: Unique dataset identifier
        feature_group: Feature group name (e.g., 'ohlcv_basic', 'technical_momentum')
        symbol: Instrument symbol
        year_month: Year and month in YYYY_MM format
        timeframe: Timeframe (e.g., '5m', '15m', '1h', '1d')
    
    Returns:
        Complete file path string
    """
    symbol_date = f"{symbol}_{year_month}"
    filename = f"{symbol_date}_{feature_group}.arrayrecord"
    
    path = Path(output_dir) / dataset_id / feature_group / symbol_date / timeframe / filename
    return str(path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])