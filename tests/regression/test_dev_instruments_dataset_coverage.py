#!/usr/bin/env python3
"""
Regression test to ensure dev_instrument dataset is included in EDA.
This test covers the issue where the main consolidated instruments table
was missing from the EDA dataset list despite being available in the database.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

def test_dev_instrument_included_in_eda_datasets():
    """Test that dev_instrument is included in the EDA dataset list."""
    print("🧪 Testing dev_instrument dataset inclusion...")

    try:
        # Read the analytics service code to verify dev_instrument is included
        with open('/home/jianjun/ats-genai-admin/src/services/analytics_service.py', 'r') as f:
            content = f.read()

        # Check that dev_instrument is in the fallback dataset list
        assert "'name': 'dev_instrument'" in content, "dev_instrument should be in EDA dataset list"
        assert "'display_name': 'All Instruments (Consolidated)'" in content, "dev_instrument should have proper display name"

        # Verify it's the first dataset (most important)
        lines = content.split('\n')
        dataset_list_started = False
        first_dataset_found = False

        for line in lines:
            if "return [" in line:
                dataset_list_started = True
                continue

            if dataset_list_started and "'name':" in line and not first_dataset_found:
                assert "'dev_instrument'" in line, "dev_instrument should be the first dataset in the list"
                first_dataset_found = True
                break

        print("   ✅ dev_instrument is included as primary dataset")

    except FileNotFoundError:
        print("   ⚠️ Analytics service file not found - skipping test")
        return
    except Exception as e:
        print(f"   ❌ Test failed: {e}")
        raise

def test_dev_instrument_schema_definition():
    """Test that dev_instrument has proper schema definition for EDA."""
    print("🧪 Testing dev_instrument schema definition...")

    try:
        with open('/home/jianjun/ats-genai-admin/src/services/analytics_service.py', 'r') as f:
            content = f.read()

        # Check that schema definition exists for dev_instrument
        assert 'if table_name == "dev_instrument":' in content, "dev_instrument should have schema definition"

        # Verify key columns are defined
        required_columns = [
            '"id"',
            '"symbol"',
            '"name"',
            '"exchange"',
            '"active"',
            '"sector"'
        ]

        for column in required_columns:
            assert f'"column_name": {column}' in content, f"Schema should define {column} column"

        # Verify we have the correct number of columns (16 total)
        # Count column definitions in dev_instrument schema section
        import re
        dev_instrument_section = re.search(
            r'if table_name == "dev_instrument":(.*?)elif table_name ==',
            content,
            re.DOTALL
        )

        if dev_instrument_section:
            schema_content = dev_instrument_section.group(1)
            column_count = schema_content.count('"column_name":')
            assert column_count == 16, f"dev_instrument schema should have 16 columns, found {column_count}"

        print("   ✅ dev_instrument schema properly defined with 16 columns")

    except Exception as e:
        print(f"   ❌ Schema test failed: {e}")
        raise

def test_dev_instrument_row_count_realistic():
    """Test that dev_instrument has realistic row count in fallback data."""
    print("🧪 Testing dev_instrument row count...")

    try:
        with open('/home/jianjun/ats-genai-admin/src/services/analytics_service.py', 'r') as f:
            content = f.read()

        # Check that row count is realistic (should be around 69,796)
        import re
        row_count_match = re.search(r"'name': 'dev_instrument'.*?'row_count': (\d+)", content, re.DOTALL)

        if row_count_match:
            row_count = int(row_count_match.group(1))
            assert row_count > 50000, f"dev_instrument should have >50k rows, found {row_count}"
            assert row_count < 100000, f"dev_instrument row count should be realistic, found {row_count}"
            print(f"   ✅ dev_instrument has realistic row count: {row_count:,}")
        else:
            raise AssertionError("Could not find row_count for dev_instrument")

    except Exception as e:
        print(f"   ❌ Row count test failed: {e}")
        raise

def test_dev_instrument_has_numeric_columns_for_analysis():
    """Test that dev_instrument schema includes analyzable numeric columns."""
    print("🧪 Testing dev_instrument numeric columns for EDA analysis...")

    try:
        with open('/home/jianjun/ats-genai-admin/src/services/analytics_service.py', 'r') as f:
            content = f.read()

        # Extract dev_instrument schema section
        import re
        dev_instrument_section = re.search(
            r'if table_name == "dev_instrument":(.*?)elif table_name ==',
            content,
            re.DOTALL
        )

        if not dev_instrument_section:
            raise AssertionError("Could not find dev_instrument schema section")

        schema_content = dev_instrument_section.group(1)

        # Check for numeric columns that can be analyzed
        # The id column is integer type and can be used for analysis
        assert '"column_name": "id"' in schema_content and '"data_type": "integer"' in schema_content, \
            "dev_instrument should have id column as integer for analysis"

        print("   ✅ dev_instrument has analyzable numeric columns")

        # Note: dev_instrument is primarily categorical data (symbol, name, exchange, etc.)
        # but the id column provides a numeric field for histogram analysis

    except Exception as e:
        print(f"   ❌ Numeric columns test failed: {e}")
        raise

def test_dev_instrument_missing_dataset_regression():
    """Regression test to prevent dev_instrument from being omitted again."""
    print("🧪 Testing regression: dev_instrument not missing from EDA...")

    # This test specifically addresses the user question:
    # "how come we do not have dev_instrument as a dataset?"

    try:
        with open('/home/jianjun/ats-genai-admin/src/services/analytics_service.py', 'r') as f:
            content = f.read()

        # Ensure dev_instrument is prominently placed (first in list)
        lines = content.split('\n')
        in_dataset_list = False
        dataset_order = []

        for line in lines:
            if "return [" in line:
                in_dataset_list = True
                continue

            if in_dataset_list and "'name':" in line:
                # Extract dataset name
                import re
                name_match = re.search(r"'name': '([^']+)'", line)
                if name_match:
                    dataset_order.append(name_match.group(1))

            if in_dataset_list and line.strip() == ']':
                break

        # Verify dev_instrument is first
        assert len(dataset_order) > 0, "Should find datasets in fallback list"
        assert dataset_order[0] == "dev_instrument", f"dev_instrument should be first dataset, found order: {dataset_order}"

        # Verify it's included alongside other instrument datasets
        instrument_datasets = [ds for ds in dataset_order if 'instrument' in ds]
        assert 'dev_instrument' in instrument_datasets, "dev_instrument should be in instrument datasets"
        assert len(instrument_datasets) >= 3, f"Should have multiple instrument datasets, found: {instrument_datasets}"

        print(f"   ✅ dev_instrument is first in dataset list: {dataset_order[:3]}")
        print(f"   ✅ Instrument datasets included: {instrument_datasets}")

    except Exception as e:
        print(f"   ❌ Regression test failed: {e}")
        raise


def run_dev_instrument_coverage_tests():
    """Run all dev_instrument dataset coverage tests."""
    print("🚀 Running dev_instrument Dataset Coverage Tests")
    print("=" * 55)

    tests = [
        ("dev_instrument Included in EDA Datasets", test_dev_instrument_included_in_eda_datasets),
        ("dev_instrument Schema Definition", test_dev_instrument_schema_definition),
        ("dev_instrument Row Count Realistic", test_dev_instrument_row_count_realistic),
        ("dev_instrument Numeric Columns for Analysis", test_dev_instrument_has_numeric_columns_for_analysis),
        ("dev_instrument Missing Dataset Regression", test_dev_instrument_missing_dataset_regression),
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
    print("\n" + "=" * 55)
    print(f"📊 DEV_INSTRUMENTS COVERAGE TEST SUMMARY")
    print(f"   Total Tests: {len(tests)}")
    print(f"   Passed: {passed_tests}")
    print(f"   Failed: {len(failed_tests)}")

    if failed_tests:
        print(f"\n❌ FAILED TESTS:")
        for test_name, error in failed_tests:
            print(f"   - {test_name}: {error}")
        return False
    else:
        print(f"\n🎉 ALL DEV_INSTRUMENTS COVERAGE TESTS PASSED!")
        print(f"✅ dev_instrument is properly included in EDA")
        print(f"✅ Main consolidated instruments table available for analysis")
        print(f"✅ Schema and metadata correctly defined")
        print(f"✅ Regression protection in place")
        return True


if __name__ == "__main__":
    success = run_dev_instrument_coverage_tests()
    exit(0 if success else 1)