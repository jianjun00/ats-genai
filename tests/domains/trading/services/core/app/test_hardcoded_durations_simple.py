#!/usr/bin/env python3
"""
Simple test to identify hardcoded duration values in runner_utils.py files.
"""
import os

def test_identify_hardcoded_duration_locations():
    """
    Test to identify and document locations where durations are hardcoded.
    """
    print("🔍 IDENTIFYING HARDCODED DURATION LOCATIONS")
    print("=" * 50)
    
    hardcoded_locations = []
    
    # Files to check for hardcoded durations
    files_to_check = [
        'src/domains/trading/services/core/app/runner_utils.py',
    ]
    
    for file_path in files_to_check:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read()
                
            print(f"\n📁 Checking: {file_path}")
            
            # Look for hardcoded '1d' in UniverseStateIntervalBuilder calls
            hardcoded_found = False
            if "base_duration='1d'" in content:
                hardcoded_found = True
                print(f"❌ Found hardcoded base_duration='1d'")
                
            if "target_durations='1d'" in content:
                hardcoded_found = True
                print(f"❌ Found hardcoded target_durations='1d'")
                
            if hardcoded_found:
                hardcoded_locations.append(file_path)
                
                # Extract relevant lines
                lines = content.split('\n')
                for i, line in enumerate(lines, 1):
                    if 'UniverseStateIntervalBuilder(' in line:
                        print(f"   Line {i}: {line.strip()}")
                    elif ('base_duration=' in line and "'1d'" in line) or ('target_durations=' in line and "'1d'" in line):
                        print(f"   Line {i}: {line.strip()}")
            else:
                print(f"✅ No hardcoded durations found")
    
    return hardcoded_locations


def show_correct_pattern():
    """
    Show the correct configurable pattern that should be used.
    """
    print(f"\n📋 CORRECT CONFIGURABLE PATTERN")
    print("=" * 40)
    
    print("❌ WRONG (Hardcoded):")
    print("```python")
    print("builder = UniverseStateIntervalBuilder(")
    print("    env=env,")
    print("    base_duration='1d',        # ← HARDCODED")
    print("    target_durations='1d'      # ← HARDCODED")
    print(")")
    print("```")
    
    print("\n✅ CORRECT (Configurable):")
    print("```python")
    print("async def run_file_daily_price_ohlcv(")
    print("    ...,")
    print("    base_duration: str = '1d',        # ← PARAMETER WITH DEFAULT")
    print("    target_durations: str = '1d',     # ← PARAMETER WITH DEFAULT")
    print("):")
    print("    builder = UniverseStateIntervalBuilder(")
    print("        env=env,")
    print("        base_duration=base_duration,      # ← USE PARAMETER")
    print("        target_durations=target_durations # ← USE PARAMETER")
    print("    )")
    print("```")


def show_training_callback_example():
    """
    Show how training_data_callback_runner.py correctly handles this.
    """
    print(f"\n✅ GOOD EXAMPLE FROM TRAINING_DATA_CALLBACK_RUNNER.PY")
    print("=" * 60)
    
    training_file = 'src/domains/ml/services/training_data/runners/training_data_callback_runner.py'
    if os.path.exists(training_file):
        with open(training_file, 'r') as f:
            content = f.read()
            
        lines = content.split('\n')
        builder_found = False
        
        for i, line in enumerate(lines, 1):
            if 'UniverseStateIntervalBuilder(' in line:
                builder_found = True
                print(f"Line {i}: {line.strip()}")
                # Show next few lines for context
                for j in range(1, 5):
                    if i + j - 1 < len(lines):
                        next_line = lines[i + j - 1].strip()
                        if next_line and not next_line.startswith('#'):
                            print(f"Line {i + j}: {next_line}")
                        if ')' in next_line:
                            break
                break
        
        if builder_found:
            print(f"\n✅ TRAINING CALLBACK CORRECTLY USES:")
            print(f"   - base_duration=args.base_duration (configurable)")
            print(f"   - target_durations from gin config (configurable)")
        else:
            print(f"❌ Could not find UniverseStateIntervalBuilder in training callback")


if __name__ == "__main__":
    print("🚨 HARDCODED DURATION DETECTION TEST")
    print("=" * 60)
    
    # Test: Identify hardcoded locations
    hardcoded_locations = test_identify_hardcoded_duration_locations()
    
    # Show correct pattern
    show_correct_pattern()
    
    # Show good example
    show_training_callback_example()
    
    print(f"\n" + "=" * 60)
    print(f"📋 SUMMARY:")
    print(f"  Files with hardcoded durations: {len(hardcoded_locations)}")
    for loc in hardcoded_locations:
        print(f"  - {loc}")
    
    if hardcoded_locations:
        print(f"\n❌ HARDCODED DURATION ISSUE CONFIRMED!")
        print(f"   {len(hardcoded_locations)} files need to be fixed")
        print(f"\n🔧 REQUIRED FIXES:")
        print(f"   1. Add base_duration and target_durations parameters to function signatures")
        print(f"   2. Pass parameters to UniverseStateIntervalBuilder instead of hardcoded '1d'")
        print(f"   3. Follow pattern from training_data_callback_runner.py")
    else:
        print(f"\n✅ NO HARDCODED DURATIONS FOUND!")
        print(f"   All files use proper configuration")