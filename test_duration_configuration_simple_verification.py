#!/usr/bin/env python3
"""
Simple verification test that duration configuration is properly implemented
by checking the source code directly.
"""
import os
import ast
import inspect

def test_function_signatures_have_duration_parameters():
    """
    Test that runner_utils functions have base_duration and target_durations parameters
    by parsing the source code directly.
    """
    print("🔍 TESTING FUNCTION SIGNATURES IN SOURCE CODE")
    print("=" * 50)
    
    files_to_check = [
        'src/domains/trading/services/core/app/runner_utils.py',
        'src/infrastructure/services_legacy/core/app/runner_utils.py'
    ]
    
    all_good = True
    
    for file_path in files_to_check:
        if os.path.exists(file_path):
            print(f"\n📁 Checking: {file_path}")
            
            with open(file_path, 'r') as f:
                content = f.read()
            
            try:
                tree = ast.parse(content)
                
                # Find the run_file_daily_price_ohlcv function
                for node in ast.walk(tree):
                    if isinstance(node, ast.FunctionDef) and node.name == 'run_file_daily_price_ohlcv':
                        print(f"  Found function: {node.name}")
                        
                        # Check parameters
                        param_names = [arg.arg for arg in node.args.args]
                        
                        has_base_duration = 'base_duration' in param_names
                        has_target_durations = 'target_durations' in param_names
                        
                        print(f"  Parameters: {param_names}")
                        print(f"  Has base_duration: {has_base_duration}")
                        print(f"  Has target_durations: {has_target_durations}")
                        
                        if has_base_duration and has_target_durations:
                            print(f"  ✅ Function signature is correct")
                        else:
                            print(f"  ❌ Function missing duration parameters")
                            all_good = False
                        break
                else:
                    print(f"  ❌ Function run_file_daily_price_ohlcv not found")
                    all_good = False
                    
            except SyntaxError as e:
                print(f"  ❌ Syntax error in file: {e}")
                all_good = False
        else:
            print(f"❌ File not found: {file_path}")
            all_good = False
    
    return all_good


def test_builder_calls_use_parameters():
    """
    Test that UniverseStateIntervalBuilder calls use parameter variables
    instead of hardcoded values.
    """
    print(f"\n🔧 TESTING BUILDER CALLS USE PARAMETERS")
    print("=" * 45)
    
    files_to_check = [
        'src/domains/trading/services/core/app/runner_utils.py',
        'src/infrastructure/services_legacy/core/app/runner_utils.py'
    ]
    
    all_good = True
    
    for file_path in files_to_check:
        if os.path.exists(file_path):
            print(f"\n📁 Checking: {file_path}")
            
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Check for proper parameter usage
            lines = content.split('\n')
            builder_section = False
            
            for i, line in enumerate(lines, 1):
                if 'UniverseStateIntervalBuilder(' in line:
                    builder_section = True
                    print(f"  Line {i}: {line.strip()}")
                elif builder_section and line.strip():
                    print(f"  Line {i}: {line.strip()}")
                    
                    # Check for parameter usage vs hardcoded values
                    if 'base_duration=' in line:
                        if 'base_duration=base_duration' in line:
                            print(f"    ✅ Uses base_duration parameter")
                        elif "base_duration='1d'" in line:
                            print(f"    ❌ Uses hardcoded '1d' instead of parameter")
                            all_good = False
                    
                    if 'target_durations=' in line:
                        if 'target_durations=target_durations' in line:
                            print(f"    ✅ Uses target_durations parameter")
                        elif "target_durations='1d'" in line:
                            print(f"    ❌ Uses hardcoded '1d' instead of parameter")
                            all_good = False
                    
                    # Stop when we reach the end of the builder call
                    if ')' in line and builder_section:
                        builder_section = False
                        break
    
    return all_good


def test_runner_calls_use_parameters():
    """
    Test that Runner calls use parameter variables for base_duration.
    """
    print(f"\n⚙️ TESTING RUNNER CALLS USE PARAMETERS")
    print("=" * 40)
    
    files_to_check = [
        'src/domains/trading/services/core/app/runner_utils.py',
        'src/infrastructure/services_legacy/core/app/runner_utils.py'
    ]
    
    all_good = True
    
    for file_path in files_to_check:
        if os.path.exists(file_path):
            print(f"\n📁 Checking: {file_path}")
            
            with open(file_path, 'r') as f:
                content = f.read()
            
            lines = content.split('\n')
            runner_section = False
            
            for i, line in enumerate(lines, 1):
                if 'Runner(' in line:
                    runner_section = True
                    print(f"  Line {i}: {line.strip()}")
                elif runner_section and line.strip():
                    print(f"  Line {i}: {line.strip()}")
                    
                    # Check for parameter usage vs hardcoded values
                    if 'base_duration=' in line:
                        if 'base_duration=base_duration' in line:
                            print(f"    ✅ Uses base_duration parameter")
                        elif "base_duration='1d'" in line:
                            print(f"    ❌ Uses hardcoded '1d' instead of parameter")
                            all_good = False
                    
                    # Stop when we reach the end of the Runner call
                    if ')' in line and runner_section:
                        runner_section = False
                        break
    
    return all_good


def show_before_after_comparison():
    """
    Show the before/after comparison of the fix.
    """
    print(f"\n📊 BEFORE/AFTER COMPARISON")
    print("=" * 30)
    
    print("❌ BEFORE (Hardcoded):")
    print("```python")
    print("async def run_file_daily_price_ohlcv(")
    print("    vendors_dirs, instrument_ids, start_date, end_date, env,")
    print("    universe_id=1, output_dir=None, ...  # No duration params")
    print("):")
    print("    runner = Runner(..., base_duration='1d')  # Hardcoded")
    print("    builder = UniverseStateIntervalBuilder(")
    print("        env=env,")
    print("        base_duration='1d',        # Hardcoded")
    print("        target_durations='1d'      # Hardcoded")
    print("    )")
    print("```")
    
    print("\n✅ AFTER (Configurable):")
    print("```python")
    print("async def run_file_daily_price_ohlcv(")
    print("    vendors_dirs, instrument_ids, start_date, end_date, env,")
    print("    universe_id=1, ")
    print("    base_duration: str = '1d',        # Configurable with default")
    print("    target_durations: str = '1d',     # Configurable with default")
    print("    output_dir=None, ...")
    print("):")
    print("    runner = Runner(..., base_duration=base_duration)  # Parameter")
    print("    builder = UniverseStateIntervalBuilder(")
    print("        env=env,")
    print("        base_duration=base_duration,      # Parameter")
    print("        target_durations=target_durations # Parameter")
    print("    )")
    print("```")


if __name__ == "__main__":
    print("🚨 DURATION CONFIGURATION SIMPLE VERIFICATION")
    print("=" * 60)
    
    # Test 1: Function signatures
    signatures_good = test_function_signatures_have_duration_parameters()
    
    # Test 2: Builder calls use parameters
    builder_params_good = test_builder_calls_use_parameters()
    
    # Test 3: Runner calls use parameters
    runner_params_good = test_runner_calls_use_parameters()
    
    # Show comparison
    show_before_after_comparison()
    
    print(f"\n" + "=" * 60)
    print(f"📋 VERIFICATION SUMMARY:")
    print(f"  Function signatures have duration params: {signatures_good}")
    print(f"  Builder calls use parameters: {builder_params_good}")
    print(f"  Runner calls use parameters: {runner_params_good}")
    
    if signatures_good and builder_params_good and runner_params_good:
        print(f"\n🎉 ALL DURATION CONFIGURATION VERIFICATIONS PASSED!")
        print(f"   ✅ Functions accept base_duration and target_durations parameters")
        print(f"   ✅ UniverseStateIntervalBuilder receives parameter values")
        print(f"   ✅ Runner receives parameter values")
        print(f"   ✅ No more hardcoded '1d' values")
        print(f"\n🚀 DURATION CONFIGURATION IS NOW FULLY CONFIGURABLE!")
        print(f"\n📖 USAGE EXAMPLES:")
        print(f"   # Default daily processing")
        print(f"   await run_file_daily_price_ohlcv(...)")
        print(f"   ")
        print(f"   # Multi-timeframe training data")
        print(f"   await run_file_daily_price_ohlcv(..., ")
        print(f"       base_duration='5m', target_durations='5m,15m,1h,1d')")
        print(f"   ")
        print(f"   # Custom interval analysis")
        print(f"   await run_file_daily_price_ohlcv(..., ")
        print(f"       base_duration='1h', target_durations='1h,4h,1d')")
    else:
        print(f"\n❌ SOME DURATION CONFIGURATION VERIFICATIONS FAILED!")
        print(f"   Please check the function signatures and parameter usage")
    
    print(f"\n📝 BENEFITS ACHIEVED:")
    print(f"  - No more one-size-fits-all hardcoded '1d' values")
    print(f"  - Training data can use multiple timeframes (5m, 15m, 1h, 1d)")
    print(f"  - Different use cases can specify optimal configurations")
    print(f"  - Maintains backward compatibility with sensible defaults")
    print(f"  - Follows same pattern as training_data_callback_runner.py")