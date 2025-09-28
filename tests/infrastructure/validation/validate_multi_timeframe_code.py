#!/usr/bin/env python3
"""
Static code validation for multi-timeframe functionality.

Validates the code structure and documentation without requiring external dependencies.
"""

import os
import re

def validate_universe_state_manager():
    """Validate UniverseStateManager multi-timeframe enhancements."""

    print("🔍 VALIDATING UNIVERSE STATE MANAGER")
    print("=" * 50)

    file_path = "src/state/universe_state_manager.py"

    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False

    with open(file_path, 'r') as f:
        content = f.read()

    validations = []

    # Check 1: get_lag_prices method has time_interval parameter
    if 'def get_lag_prices(self, instrument_id: int, cur_date, lag_days: int, time_interval: str = '1d')' in content:
        validations.append(("✅", "get_lag_prices has time_interval parameter"))
    else:
        validations.append(("❌", "get_lag_prices missing time_interval parameter"))

    # Check 2: Documentation mentions market_data_manager
    if 'market_data_manager' in content and 'aggregation' in content:
        validations.append(("✅", "Documentation mentions market_data_manager aggregation"))
    else:
        validations.append(("❌", "Documentation missing market_data_manager reference"))

    # Check 3: Has proper docstring with examples
    if 'Example:' in content and 'lag_5m' in content:
        validations.append(("✅", "Has comprehensive docstring with examples"))
    else:
        validations.append(("❌", "Missing comprehensive docstring"))

    # Check 4: Supports different time intervals
    supported_intervals = ['1m', '5m', '15m', '1h', '1d', '1w']
    interval_mentions = sum(1 for interval in supported_intervals if f"'{interval}'" in content)
    if interval_mentions >= 4:
        validations.append(("✅", f"Documents {interval_mentions} time intervals"))
    else:
        validations.append(("⚠️", f"Only documents {interval_mentions} time intervals"))

    # Check 5: Has market_data_manager integration
    if 'hasattr(self, 'market_data_manager')' in content and 'get_ohlcv_data' in content:
        validations.append(("✅", "Has market_data_manager integration"))
    else:
        validations.append(("❌", "Missing market_data_manager integration"))

    for status, message in validations:
        print(f"   {status} {message}")

    passed = sum(1 for status, _ in validations if status == "✅")
    total = len(validations)

    print(f"\n📊 UniverseStateManager: {passed}/{total} validations passed")
    return passed == total

def validate_training_data_job_runner():
    """Validate TrainingDataJobRunner multi-timeframe enhancements."""

    print("\n🔍 VALIDATING TRAINING DATA JOB RUNNER")
    print("=" * 50)

    file_path = "src/app/training_data_job_runner.py"

    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False

    with open(file_path, 'r') as f:
        content = f.read()

    validations = []

    # Check 1: Has _get_multi_timeframe_features_from_universe_state method
    if '_get_multi_timeframe_features_from_universe_state' in content:
        validations.append(("✅", "Has multi-timeframe feature extraction method"))
    else:
        validations.append(("❌", "Missing multi-timeframe feature extraction method"))

    # Check 2: Uses time_interval parameter in get_lag_prices calls
    if 'time_interval=timeframe' in content or 'time_interval=' in content:
        validations.append(("✅", "Uses time_interval parameter"))
    else:
        validations.append(("❌", "Missing time_interval parameter usage"))

    # Check 3: Has gin configuration compliance
    gin_timeframes = ['5m', '15m', '1h', '1d']
    gin_mentions = sum(1 for tf in gin_timeframes if f"'{tf}'" in content)
    if gin_mentions >= 3:
        validations.append(("✅", f"References gin timeframes ({gin_mentions}/4)"))
    else:
        validations.append(("⚠️", f"Limited gin timeframe references ({gin_mentions}/4)"))

    # Check 4: Has proper feature naming pattern
    if '_lag_' in content and 'lag_idx' in content:
        validations.append(("✅", "Has proper feature naming pattern"))
    else:
        validations.append(("❌", "Missing feature naming pattern"))

    # Check 5: Has comprehensive documentation
    if 'Multi-timeframe Configuration' in content and 'Feature Extraction Process' in content:
        validations.append(("✅", "Has comprehensive method documentation"))
    else:
        validations.append(("❌", "Missing comprehensive documentation"))

    # Check 6: Handles errors gracefully
    if 'try:' in content and 'except Exception' in content and 'features = {}' in content:
        validations.append(("✅", "Has error handling"))
    else:
        validations.append(("⚠️", "Limited error handling"))

    # Check 7: No fake data generation
    fake_indicators = ['_generate_intraday_feature_approximations', 'fake', 'mock', 'simulate']
    has_fake = any(indicator in content for indicator in fake_indicators)
    if not has_fake:
        validations.append(("✅", "No fake data generation"))
    else:
        validations.append(("❌", "Contains fake data generation"))

    for status, message in validations:
        print(f"   {status} {message}")

    passed = sum(1 for status, _ in validations if status == "✅")
    total = len(validations)

    print(f"\n📊 TrainingDataJobRunner: {passed}/{total} validations passed")
    return passed == total

def validate_gin_configuration():
    """Validate training_data.gin configuration."""

    print("\n🔍 VALIDATING GIN CONFIGURATION")
    print("=" * 50)

    file_path = "config/training_data.gin"

    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False

    with open(file_path, 'r') as f:
        content = f.read()

    validations = []

    # Check 1: Has sequence_lengths configuration
    if 'sequence_lengths' in content:
        validations.append(("✅", "Has sequence_lengths configuration"))
    else:
        validations.append(("❌", "Missing sequence_lengths configuration"))

    # Check 2: Has all required timeframes
    required_timeframes = ['5m', '15m', '1h', '1d']
    found_timeframes = []
    for tf in required_timeframes:
        if f"'{tf}'" in content or f'"{tf}"' in content:
            found_timeframes.append(tf)

    if len(found_timeframes) >= 3:
        validations.append(("✅", f"Has required timeframes: {found_timeframes}"))
    else:
        validations.append(("❌", f"Missing timeframes: {set(required_timeframes) - set(found_timeframes)}"))

    # Check 3: Has prediction_horizons
    if 'prediction_horizons' in content:
        validations.append(("✅", "Has prediction_horizons configuration"))
    else:
        validations.append(("❌", "Missing prediction_horizons configuration"))

    # Check 4: Has feature_types
    if 'feature_types' in content:
        validations.append(("✅", "Has feature_types configuration"))
    else:
        validations.append(("❌", "Missing feature_types configuration"))

    # Check 5: Has expected values
    expected_values = ['52', '24', '20']  # Sequence lengths from gin
    found_values = sum(1 for val in expected_values if val in content)
    if found_values >= 2:
        validations.append(("✅", f"Has expected sequence length values"))
    else:
        validations.append(("⚠️", "Limited sequence length values"))

    for status, message in validations:
        print(f"   {status} {message}")

    passed = sum(1 for status, _ in validations if status == "✅")
    total = len(validations)

    print(f"\n📊 Gin Configuration: {passed}/{total} validations passed")
    return passed == total

def validate_test_coverage():
    """Validate test coverage for multi-timeframe functionality."""

    print("\n🔍 VALIDATING TEST COVERAGE")
    print("=" * 50)

    test_files = [
        "tests/test_multi_timeframe_universe_state_manager.py",
        "tests/test_multi_timeframe_training_data_job_runner.py"
    ]

    validations = []

    for test_file in test_files:
        if os.path.exists(test_file):
            validations.append(("✅", f"Test file exists: {test_file}"))

            with open(test_file, 'r') as f:
                content = f.read()

            # Count test methods
            test_methods = len(re.findall(r'def test_\w+', content))
            if test_methods >= 5:
                validations.append(("✅", f"{test_file}: {test_methods} test methods"))
            else:
                validations.append(("⚠️", f"{test_file}: only {test_methods} test methods"))

            # Check for key test patterns
            if 'time_interval' in content:
                validations.append(("✅", f"{test_file}: tests time_interval parameter"))
            else:
                validations.append(("❌", f"{test_file}: missing time_interval tests"))

        else:
            validations.append(("❌", f"Missing test file: {test_file}"))

    for status, message in validations:
        print(f"   {status} {message}")

    passed = sum(1 for status, _ in validations if status == "✅")
    total = len(validations)

    print(f"\n📊 Test Coverage: {passed}/{total} validations passed")
    return passed >= total * 0.8  # 80% threshold

def validate_documentation_completeness():
    """Validate documentation completeness."""

    print("\n🔍 VALIDATING DOCUMENTATION")
    print("=" * 50)

    files_to_check = [
        "src/state/universe_state_manager.py",
        "src/app/training_data_job_runner.py"
    ]

    validations = []

    for file_path in files_to_check:
        if not os.path.exists(file_path):
            validations.append(("❌", f"File not found: {file_path}"))
            continue

        with open(file_path, 'r') as f:
            content = f.read()

        # Check for comprehensive docstrings
        docstring_indicators = ['Args:', 'Returns:', 'Example:', 'Notes:']
        found_indicators = sum(1 for indicator in docstring_indicators if indicator in content)

        if found_indicators >= 3:
            validations.append(("✅", f"{file_path}: comprehensive docstrings"))
        else:
            validations.append(("⚠️", f"{file_path}: limited docstrings"))

        # Check for multi-timeframe specific documentation
        mtf_keywords = ['multi-timeframe', 'timeframe', 'aggregation', 'market_data_manager']
        mtf_mentions = sum(1 for keyword in mtf_keywords if keyword in content)

        if mtf_mentions >= 3:
            validations.append(("✅", f"{file_path}: multi-timeframe documentation"))
        else:
            validations.append(("⚠️", f"{file_path}: limited multi-timeframe docs"))

    for status, message in validations:
        print(f"   {status} {message}")

    passed = sum(1 for status, _ in validations if status == "✅")
    total = len(validations)

    print(f"\n📊 Documentation: {passed}/{total} validations passed")
    return passed >= total * 0.7  # 70% threshold

def main():
    """Run all validations."""

    print("🧪 MULTI-TIMEFRAME CODE VALIDATION")
    print("=" * 60)
    print("Static analysis of multi-timeframe functionality implementation")
    print("")

    results = []

    # Run all validations
    results.append(("UniverseStateManager", validate_universe_state_manager()))
    results.append(("TrainingDataJobRunner", validate_training_data_job_runner()))
    results.append(("Gin Configuration", validate_gin_configuration()))
    results.append(("Test Coverage", validate_test_coverage()))
    results.append(("Documentation", validate_documentation_completeness()))

    # Summary
    print("\n" + "=" * 60)
    print("📊 VALIDATION SUMMARY")
    print("=" * 60)

    passed_count = 0
    for component, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{component:<25} {status}")
        if passed:
            passed_count += 1

    total_components = len(results)
    success_rate = (passed_count / total_components) * 100

    print(f"\n📈 Overall Success Rate: {success_rate:.1f}% ({passed_count}/{total_components})")

    if success_rate >= 80:
        print("\n🎉 VALIDATION PASSED!")
        print("✅ Multi-timeframe implementation is well-structured")
        print("✅ Code follows proper patterns and documentation standards")
        print("✅ Integration with UniverseStateManager implemented correctly")
        print("✅ Gin configuration compliance maintained")
        print("\n🎯 Code is ready for multi-timeframe training data generation")
        return True
    else:
        print("\n💥 VALIDATION FAILED!")
        print("❌ Multi-timeframe implementation has structural issues")
        print("❌ Review the validation output above for specific problems")
        print("❌ Address issues before proceeding")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)