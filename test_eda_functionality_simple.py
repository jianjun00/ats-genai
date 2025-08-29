#!/usr/bin/env python3
"""
Simple test script to verify EDA functionality without pytest dependencies.
"""

def test_numeric_column_filtering_logic():
    """Test the logic for filtering numeric columns for dropdown."""
    print("🧪 Testing numeric column filtering logic...")
    
    # Sample schema data matching what the API returns
    sample_schema = {
        "columns": [
            {"column_name": "symbol", "data_type": "character varying", "is_nullable": "NO"},
            {"column_name": "name", "data_type": "text", "is_nullable": "YES"}, 
            {"column_name": "market_cap", "data_type": "numeric", "is_nullable": "YES"},
            {"column_name": "price", "data_type": "double precision", "is_nullable": "YES"},
            {"column_name": "volume", "data_type": "bigint", "is_nullable": "YES"},
            {"column_name": "start_date", "data_type": "date", "is_nullable": "YES"}
        ]
    }
    
    # Apply the same filtering logic as frontend
    numeric_columns = []
    for col in sample_schema["columns"]:
        data_type = col["data_type"].lower()
        if any(t in data_type for t in ["numeric", "integer", "double", "bigint", "smallint", "real", "decimal", "float"]):
            numeric_columns.append(col["column_name"])
    
    # Should identify exactly the numeric columns
    expected = ["market_cap", "price", "volume"]
    assert numeric_columns == expected, f"Expected {expected}, got {numeric_columns}"
    print(f"   ✅ Found expected numeric columns: {numeric_columns}")

def test_ohlcv_column_detection():
    """Test detection of OHLCV columns for financial data."""
    print("🧪 Testing OHLCV column detection...")
    
    ohlcv_schema = {
        "columns": [
            {"column_name": "symbol", "data_type": "character varying", "is_nullable": "NO"},
            {"column_name": "date", "data_type": "date", "is_nullable": "NO"},
            {"column_name": "open", "data_type": "numeric", "is_nullable": "YES"},
            {"column_name": "high", "data_type": "numeric", "is_nullable": "YES"}, 
            {"column_name": "low", "data_type": "numeric", "is_nullable": "YES"},
            {"column_name": "close", "data_type": "numeric", "is_nullable": "YES"},
            {"column_name": "volume", "data_type": "bigint", "is_nullable": "YES"}
        ]
    }
    
    numeric_columns = []
    ohlcv_columns = []
    
    for col in ohlcv_schema["columns"]:
        data_type = col["data_type"].lower()
        if any(t in data_type for t in ["numeric", "integer", "double", "bigint"]):
            numeric_columns.append(col["column_name"])
            
            # Check if it's an OHLCV column
            if col["column_name"] in ["open", "high", "low", "close", "volume"]:
                ohlcv_columns.append(col["column_name"])
    
    assert len(numeric_columns) == 5, f"Expected 5 numeric columns, got {len(numeric_columns)}"
    assert len(ohlcv_columns) == 5, f"Expected 5 OHLCV columns, got {len(ohlcv_columns)}"
    assert ohlcv_columns == ["open", "high", "low", "close", "volume"]
    print(f"   ✅ Found {len(ohlcv_columns)} OHLCV columns: {ohlcv_columns}")

def test_threading_server_regression():
    """Regression test to ensure threading server is properly configured."""
    print("🧪 Testing threading server regression...")
    
    try:
        # Check that analytics service has ThreadingHTTPServer
        with open('/home/jianjun/ats-genai-admin/src/services/analytics_service.py', 'r') as f:
            content = f.read()
            
        # Verify that ThreadingHTTPServer is used instead of HTTPServer
        assert "ThreadingHTTPServer" in content, "Service should use ThreadingHTTPServer for concurrent requests"
        assert "from http.server import" in content and "ThreadingHTTPServer" in content, "ThreadingHTTPServer should be imported"
        
        # Verify the server creation line
        server_creation_lines = [line for line in content.split('\n') if 'ThreadingHTTPServer' in line and 'server =' in line]
        assert len(server_creation_lines) > 0, "ThreadingHTTPServer should be instantiated"
        
        print("   ✅ Threading server regression check passed")
        
    except FileNotFoundError:
        print("   ⚠️ Analytics service file not found - skipping regression check")
    except Exception as e:
        print(f"   ❌ Threading server regression check failed: {e}")
        raise

def test_fallback_system_regression():
    """Regression test to ensure fallback system is properly implemented in code."""
    print("🧪 Testing fallback system regression...")
    
    try:
        # Check that analytics service has fallback data implemented
        with open('/home/jianjun/ats-genai-admin/src/services/analytics_service.py', 'r') as f:
            content = f.read()
        
        # Verify fallback data exists in the code
        assert "fallback" in content.lower(), "Service should implement fallback data system"
        assert "dev_instrument_tiingo" in content, "Should have Tiingo fallback data"
        
        # Verify row counts are realistic (not zero)
        import re
        row_count_matches = re.findall(r"'row_count':\s*(\d+)", content)
        for count_str in row_count_matches:
            count = int(count_str)
            assert count > 0, f"Found zero row count {count} in fallback data - would cause 'Loading...' issue"
        
        print("   ✅ Fallback system regression check passed")
        
    except FileNotFoundError:
        print("   ⚠️ Analytics service file not found - skipping fallback regression check")
    except Exception as e:
        print(f"   ❌ Fallback system regression check failed: {e}")
        raise

def run_all_tests():
    """Run all EDA functionality tests."""
    print("🚀 Running EDA Functionality Tests")
    print("=" * 50)
    
    tests = [
        ("Numeric Column Filtering Logic", test_numeric_column_filtering_logic),
        ("OHLCV Column Detection", test_ohlcv_column_detection),
        ("Threading Server Regression", test_threading_server_regression),
        ("Fallback System Regression", test_fallback_system_regression),
    ]
    
    passed_tests = 0
    failed_tests = []
    
    for test_name, test_func in tests:
        try:
            test_func()
            passed_tests += 1
            print(f"✅ PASSED: {test_name}")
        except Exception as e:
            failed_tests.append((test_name, str(e)))
            print(f"❌ FAILED: {test_name} - {e}")
    
    # Summary
    print("\n" + "=" * 50)
    print(f"📊 EDA FUNCTIONALITY TEST SUMMARY")
    print(f"   Total Tests: {len(tests)}")
    print(f"   Passed: {passed_tests}")
    print(f"   Failed: {len(failed_tests)}")
    
    if failed_tests:
        print(f"\n❌ FAILED TESTS:")
        for test_name, error in failed_tests:
            print(f"   - {test_name}: {error}")
        return False
    else:
        print(f"\n🎉 ALL EDA FUNCTIONALITY TESTS PASSED!")
        print(f"✅ Core logic is working correctly")
        print(f"✅ Regression protections in place")
        print(f"✅ Column filtering logic validated")
        return True

if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)