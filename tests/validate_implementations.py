#!/usr/bin/env python3
"""
Validate implementations for fundamentals and news collection

Simple validation script that can run without full pytest framework to verify:
- Scripts can be imported without errors
- Key classes and methods exist
- Basic functionality works
"""

import sys
import os
import importlib.util
from datetime import datetime, date

def validate_script_import(script_path, module_name):
    """Validate that a script can be imported successfully."""
    try:
        spec = importlib.util.spec_from_file_location(module_name, script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        print(f"✅ {module_name}: Script imports successfully")
        return module, True
    except Exception as e:
        print(f"❌ {module_name}: Import failed - {e}")
        return None, False

def validate_tiingo_fundamentals():
    """Validate Tiingo fundamentals implementation."""
    print("\n🔍 Validating Tiingo Fundamentals Implementation...")

    script_path = "/home/jianjun/ats-genai-admin/scripts/tiingo_30_year_fundamentals_backfill.py"
    module, success = validate_script_import(script_path, "tiingo_fundamentals")

    if not success:
        return False

    try:
        # Test class exists and can be instantiated
        collector_class = getattr(module, 'TiingoFundamentalsCollector')
        collector = collector_class("test_api_key")

        print(f"✅ TiingoFundamentalsCollector class exists and instantiates")

        # Test key attributes
        assert collector.api_key == "test_api_key"
        assert collector.base_url == "https://api.tiingo.com/tiingo/fundamentals"
        assert hasattr(collector, 'stats')
        print(f"✅ Key attributes configured correctly")

        # Test DOW 30 symbol restriction
        assert hasattr(collector, 'get_instruments_for_backfill')
        print(f"✅ DOW 30 restriction method exists")

        # Test API methods exist
        required_methods = [
            'fetch_daily_fundamentals',
            'fetch_statements',
            'standardize_tiingo_article',
            'insert_daily_fundamentals',
            'insert_statements',
            'ensure_fundamentals_tables'
        ]

        for method_name in required_methods:
            if hasattr(collector, method_name):
                print(f"✅ Method {method_name} exists")
            else:
                print(f"❌ Method {method_name} missing")
                return False

        return True

    except Exception as e:
        print(f"❌ TiingoFundamentalsCollector validation failed: {e}")
        return False

def validate_tiingo_news():
    """Validate Tiingo news implementation."""
    print("\n🔍 Validating Tiingo News Implementation...")

    script_path = "/home/jianjun/ats-genai-admin/scripts/tiingo_30_year_news_backfill.py"
    module, success = validate_script_import(script_path, "tiingo_news")

    if not success:
        return False

    try:
        # Test class exists and can be instantiated
        collector_class = getattr(module, 'TiingoNewsCollector')
        collector = collector_class("test_api_key")

        print(f"✅ TiingoNewsCollector class exists and instantiates")

        # Test key attributes
        assert collector.api_key == "test_api_key"
        assert hasattr(collector, 'start_time')
        assert hasattr(collector, 'total_articles_collected')
        print(f"✅ Key attributes configured correctly")

        # Test critical ID conversion fix
        test_article = {
            'id': 83408655,  # Integer ID
            'publishedDate': '2024-08-27T12:00:00Z',
            'title': 'Test Article',
            'url': 'https://example.com'
        }

        standardized = collector.standardize_tiingo_article(test_article)
        assert standardized['tiingo_id'] == '83408655'  # Should be string
        assert isinstance(standardized['tiingo_id'], str)
        print(f"✅ Critical ID conversion fix working (int -> string)")

        # Test API methods exist
        required_methods = [
            'fetch_news_for_symbol_year',
            'standardize_tiingo_article',
            'insert_tiingo_news_articles',
            'ensure_tiingo_news_table',
            'process_symbol_year_batch'
        ]

        for method_name in required_methods:
            if hasattr(collector, method_name):
                print(f"✅ Method {method_name} exists")
            else:
                print(f"❌ Method {method_name} missing")
                return False

        return True

    except Exception as e:
        print(f"❌ TiingoNewsCollector validation failed: {e}")
        return False

def validate_news_analysis():
    """Validate news data analysis implementation."""
    print("\n🔍 Validating News Data Analysis Implementation...")

    script_path = "/home/jianjun/ats-genai-admin/scripts/check_news_data_status.py"
    module, success = validate_script_import(script_path, "news_analysis")

    if not success:
        return False

    try:
        # Test class exists and can be instantiated
        analyzer_class = getattr(module, 'NewsDataAnalyzer')
        analyzer = analyzer_class()

        print(f"✅ NewsDataAnalyzer class exists and instantiates")

        # Test vendor configuration
        assert analyzer.vendors == ['polygon', 'tiingo', 'eodhd']
        print(f"✅ Multi-vendor configuration correct")

        # Test analysis methods exist
        required_methods = [
            'analyze_polygon_news',
            'analyze_tiingo_news',
            'analyze_eodhd_news',
            'analyze_news_coverage',
            'log_news_analysis_results'
        ]

        for method_name in required_methods:
            if hasattr(analyzer, method_name):
                print(f"✅ Method {method_name} exists")
            else:
                print(f"❌ Method {method_name} missing")
                return False

        return True

    except Exception as e:
        print(f"❌ NewsDataAnalyzer validation failed: {e}")
        return False

def validate_api_testing():
    """Validate API testing implementations."""
    print("\n🔍 Validating API Testing Implementations...")

    # Test Tiingo fundamentals API testing
    debug_script = "/home/jianjun/ats-genai-admin/scripts/debug_tiingo_fundamentals.py"
    _, success1 = validate_script_import(debug_script, "tiingo_fundamentals_debug")

    # Test Tiingo news API testing
    news_test_script = "/home/jianjun/ats-genai-admin/scripts/test_tiingo_news_api.py"
    _, success2 = validate_script_import(news_test_script, "tiingo_news_test")

    # Test news table schema checking
    schema_script = "/home/jianjun/ats-genai-admin/scripts/check_news_table_schemas.py"
    _, success3 = validate_script_import(schema_script, "news_schema_check")

    if success1 and success2 and success3:
        print(f"✅ All API testing scripts import successfully")
        return True
    else:
        print(f"❌ Some API testing scripts failed to import")
        return False

def main():
    """Run all validations."""
    print("🚀 Starting Implementation Validation...")
    print("=" * 60)

    results = []

    # Validate each component
    results.append(("Tiingo Fundamentals", validate_tiingo_fundamentals()))
    results.append(("Tiingo News", validate_tiingo_news()))
    results.append(("News Analysis", validate_news_analysis()))
    results.append(("API Testing", validate_api_testing()))

    # Summary
    print("\n" + "=" * 60)
    print("📊 VALIDATION SUMMARY")
    print("=" * 60)

    passed = 0
    total = len(results)

    for component, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{component:20} {status}")
        if success:
            passed += 1

    print(f"\nOverall: {passed}/{total} components validated successfully")

    if passed == total:
        print("🎉 All implementations validated successfully!")
        return True
    else:
        print("⚠️  Some implementations need attention")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)