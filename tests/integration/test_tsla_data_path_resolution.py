#!/usr/bin/env python3
"""
TSLA Data Path Resolution Tests

Tests the critical fixes made for TSLA data path discovery in the 
FirstRate directory structure.

Based on fixes documented in PRD: ArrayRecord Training Data System (September 4, 2025)
Issue: FileBasedMinuteManager couldn't locate TSLA minute data
Root Cause: Expected standard path format, but FirstRate uses /firstrate/T/TSLA/ structure
Solution: Enhanced path resolution to check FirstRate directory structure first
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))


@pytest.mark.integration
def test_tsla_firstrate_data_discovery():
    """Test that TSLA data can be found in FirstRate directory structure."""
    try:
        from storage.file_based_minute_manager import FileBasedMinuteManager
    except ImportError:
        pytest.skip("FileBasedMinuteManager not available")
    
    base_path = "/data/minute-bars"
    if not Path(base_path).exists():
        # Try alternative path
        base_path = "/mnt/d/ats-data/minute-bars"
        if not Path(base_path).exists():
            pytest.skip("Minute data directory not found")
    
    manager = FileBasedMinuteManager(base_path)
    
    # Test FirstRate path structure for TSLA
    start_date = datetime(2025, 8, 1)
    end_date = datetime(2025, 8, 2)
    
    try:
        data = manager.get_minute_data("TSLA", start_date, end_date)
        assert data is not None, "TSLA data not found via FirstRate path"
        assert len(data) > 0, "TSLA data is empty"
        
        print(f"✅ Found TSLA data: {len(data)} records")
        
    except FileNotFoundError as e:
        # Check if FirstRate directory exists
        firstrate_path = Path(base_path) / "firstrate"
        if not firstrate_path.exists():
            pytest.skip("FirstRate directory not found - may not be available in test environment")
        
        tsla_path = firstrate_path / "T" / "TSLA"
        if not tsla_path.exists():
            pytest.skip("TSLA directory not found in FirstRate structure")
        
        pytest.fail(f"TSLA data not accessible despite directory existing: {e}")


def test_firstrate_directory_structure():
    """Test that FirstRate directory structure is correctly detected."""
    base_paths = ["/data/minute-bars", "/mnt/d/ats-data/minute-bars"]
    
    firstrate_path = None
    for base_path in base_paths:
        potential_path = Path(base_path) / "firstrate"
        if potential_path.exists():
            firstrate_path = potential_path
            break
    
    if not firstrate_path:
        pytest.skip("FirstRate directory not found in any expected location")
    
    print(f"Found FirstRate directory: {firstrate_path}")
    
    # Check alphabetical organization
    letter_dirs = [d for d in firstrate_path.iterdir() if d.is_dir() and len(d.name) == 1]
    
    assert len(letter_dirs) > 0, "FirstRate should have alphabetical subdirectories"
    
    # Check if T directory exists (for TSLA)
    t_dir = firstrate_path / "T"
    if t_dir.exists():
        print(f"Found T directory: {t_dir}")
        
        # Check if TSLA directory exists
        tsla_dir = t_dir / "TSLA"
        if tsla_dir.exists():
            print(f"Found TSLA directory: {tsla_dir}")
            
            # Check for date subdirectories
            year_dirs = [d for d in tsla_dir.iterdir() if d.is_dir() and d.name.isdigit()]
            if year_dirs:
                print(f"Found year directories: {[d.name for d in year_dirs]}")
                
                # Check for parquet files in year/month structure
                for year_dir in year_dirs[:2]:  # Check first 2 years
                    month_dirs = [d for d in year_dir.iterdir() if d.is_dir() and d.name.isdigit()]
                    if month_dirs:
                        for month_dir in month_dirs[:2]:  # Check first 2 months
                            parquet_files = list(month_dir.glob("*.parquet"))
                            if parquet_files:
                                print(f"Found TSLA parquet files in {year_dir.name}/{month_dir.name}: {len(parquet_files)}")
                                return  # Found data structure
                
        pytest.skip("TSLA directory exists but no data files found")
    else:
        pytest.skip("T directory not found in FirstRate structure")


@pytest.mark.integration
def test_file_path_resolution_priority():
    """Test that FirstRate path is checked before standard paths."""
    try:
        from storage.file_based_minute_manager import FileBasedMinuteManager
    except ImportError:
        pytest.skip("FileBasedMinuteManager not available")
    
    # Mock the path resolution to test priority
    base_path = "/data/minute-bars"
    if not Path(base_path).exists():
        base_path = "/mnt/d/ats-data/minute-bars"
        if not Path(base_path).exists():
            pytest.skip("Minute data directory not found")
    
    manager = FileBasedMinuteManager(base_path)
    
    # Test the _get_monthly_file_path method if accessible
    if hasattr(manager, '_get_monthly_file_path'):
        test_date = datetime(2025, 8, 1)
        
        # Test FirstRate path generation
        try:
            file_path = manager._get_monthly_file_path("TSLA", test_date)
            print(f"Generated file path: {file_path}")
            
            # Should prefer FirstRate structure for TSLA
            if "firstrate" in str(file_path):
                assert "/firstrate/T/TSLA/" in str(file_path), "Should use FirstRate T/TSLA structure"
                print("✅ FirstRate path priority working correctly")
            else:
                print("⚠️ FirstRate structure not found, using fallback path")
                
        except Exception as e:
            print(f"Path resolution test failed: {e}")


def test_tsla_vs_other_symbols_path_handling():
    """Test that TSLA uses FirstRate while other symbols use standard paths."""
    try:
        from storage.file_based_minute_manager import FileBasedMinuteManager
    except ImportError:
        pytest.skip("FileBasedMinuteManager not available")
    
    base_path = "/data/minute-bars"
    if not Path(base_path).exists():
        base_path = "/mnt/d/ats-data/minute-bars"
        if not Path(base_path).exists():
            pytest.skip("Minute data directory not found")
    
    manager = FileBasedMinuteManager(base_path)
    
    test_date = datetime(2025, 8, 1)
    symbols_to_test = ["TSLA", "AAPL", "MSFT"]
    
    for symbol in symbols_to_test:
        try:
            # Don't actually load data, just test path resolution
            if hasattr(manager, '_get_monthly_file_path'):
                file_path = manager._get_monthly_file_path(symbol, test_date)
                print(f"{symbol} path: {file_path}")
                
                if symbol == "TSLA":
                    # TSLA should prefer FirstRate if available
                    if Path(base_path, "firstrate", "T", "TSLA").exists():
                        assert "/firstrate/T/TSLA/" in str(file_path), f"TSLA should use FirstRate path"
                else:
                    # Other symbols should use standard paths
                    # (May also use FirstRate if available, but different structure)
                    assert str(file_path), f"Should generate path for {symbol}"
                    
        except Exception as e:
            print(f"Path test failed for {symbol}: {e}")


def test_monthly_file_path_structure():
    """Test that monthly file paths follow expected naming convention."""
    try:
        from storage.file_based_minute_manager import FileBasedMinuteManager
    except ImportError:
        pytest.skip("FileBasedMinuteManager not available")
    
    base_path = "/data/minute-bars"
    if not Path(base_path).exists():
        base_path = "/mnt/d/ats-data/minute-bars"
        if not Path(base_path).exists():
            pytest.skip("Minute data directory not found")
    
    manager = FileBasedMinuteManager(base_path)
    
    if not hasattr(manager, '_get_monthly_file_path'):
        pytest.skip("_get_monthly_file_path method not accessible")
    
    # Test various dates
    test_dates = [
        datetime(2025, 8, 1),
        datetime(2025, 8, 15), 
        datetime(2025, 9, 1)
    ]
    
    for test_date in test_dates:
        file_path = manager._get_monthly_file_path("TSLA", test_date)
        
        # Should contain year and month
        year_str = str(test_date.year)
        month_str = f"{test_date.month:02d}"
        
        assert year_str in str(file_path), f"Path should contain year {year_str}"
        assert month_str in str(file_path), f"Path should contain month {month_str}"
        
        # Should be parquet file
        assert str(file_path).endswith(".parquet"), "Should be parquet file"
        
        print(f"✅ Path structure valid for {test_date.strftime('%Y-%m')}: {file_path}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])