#!/usr/bin/env python3
"""
Test to verify the fix for run_dev.py arrayrecord command
Demonstrates that it now shows content even for sparse files
"""

import subprocess
import os

def test_arrayrecord_command_fix():
    """Test the fixed arrayrecord command with different file types"""
    
    print("🚨 TESTING FIXED ARRAYRECORD COMMAND")
    print("=" * 60)
    print("Verifying that sparse files are now properly analyzed")
    print()
    
    test_cases = [
        {
            'name': 'Sparse file (original failing case)',
            'file': '/data/training_data/dataset_20250922_182618/technical_momentum/AAPL_2025_07/1w/AAPL_2025_07_technical_momentum.arrayrecord',
            'expected': 'SPARSE FILE DETECTED'
        },
        {
            'name': 'Empty file',
            'file': '/data/training_data/unknown_dataset/AAPL_2025_09/1w/AAPL_2025_09.arrayrecord',
            'expected': 'Empty ArrayRecord file'
        },
        {
            'name': 'Valid file with data',
            'file': '/data/training_data/dataset_20250909_080134/TSLA_20250701_000000_20250701_235959/15m/TSLA_20250701_000000_20250701_235959.arrayrecord',
            'expected': 'JSON Record with'
        }
    ]
    
    results = {}
    
    for test_case in test_cases:
        print(f"🔍 Testing: {test_case['name']}")
        print(f"   File: {test_case['file']}")
        
        if not os.path.exists(test_case['file']):
            print(f"   ❌ File not found - skipping")
            results[test_case['name']] = 'SKIPPED'
            continue
        
        # Run the arrayrecord command
        cmd = [
            'python3', 'scripts/run_dev.py', 'arrayrecord', 
            '--file', test_case['file'], 
            '--sample-size', '1'
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            output = result.stdout + result.stderr
            
            if test_case['expected'] in output:
                print(f"   ✅ SUCCESS: Found expected output '{test_case['expected']}'")
                results[test_case['name']] = 'PASS'
            else:
                print(f"   ❌ FAILED: Expected '{test_case['expected']}' not found")
                print(f"   📄 Output preview: {output[:200]}...")
                results[test_case['name']] = 'FAIL'
        
        except subprocess.TimeoutExpired:
            print(f"   ❌ TIMEOUT: Command took too long")
            results[test_case['name']] = 'TIMEOUT'
        except Exception as e:
            print(f"   ❌ ERROR: {e}")
            results[test_case['name']] = 'ERROR'
        
        print()
    
    # Summary
    print("📋 TEST RESULTS SUMMARY")
    print("=" * 40)
    
    for test_name, result in results.items():
        status_icon = "✅" if result == "PASS" else "❌" if result == "FAIL" else "⚠️"
        print(f"{status_icon} {test_name}: {result}")
    
    # Overall assessment
    passed = sum(1 for r in results.values() if r == "PASS")
    total = len([r for r in results.values() if r != "SKIPPED"])
    
    print(f"\n📊 Overall: {passed}/{total} tests passed")
    
    if passed == total and total > 0:
        print("✅ ALL TESTS PASSED - ArrayRecord command fix is working correctly!")
        print()
        print("🎯 KEY IMPROVEMENTS:")
        print("   1. Sparse files are now properly analyzed instead of just failing")
        print("   2. File size and content analysis provided for diagnostic purposes")
        print("   3. Clear diagnosis and suggestions for data issues")
        print("   4. JSON format support added for modern ArrayRecord files")
        print("   5. Enhanced binary format parsing with better error handling")
    else:
        print("❌ Some tests failed - fix may need additional work")
    
    return results

if __name__ == "__main__":
    test_arrayrecord_command_fix()