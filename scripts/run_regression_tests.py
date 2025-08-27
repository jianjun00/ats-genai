#!/usr/bin/env python3
"""
Regression Test Runner

Runs comprehensive regression tests to ensure critical issues don't resurface.
This script should be run before deployments and after major changes.
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path

def run_command(cmd, description):
    """Run a command and handle the result"""
    print(f"\n🔧 {description}")
    print(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print("✅ SUCCESS")
            if result.stdout:
                print(result.stdout)
            return True
        else:
            print("❌ FAILED")
            print(f"Exit code: {result.returncode}")
            if result.stderr:
                print(f"Error: {result.stderr}")
            if result.stdout:
                print(f"Output: {result.stdout}")
            return False
            
    except subprocess.TimeoutExpired:
        print("⏰ TIMEOUT - Test execution took too long")
        return False
    except Exception as e:
        print(f"💥 EXCEPTION - {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Run regression tests for critical issues")
    parser.add_argument('--category', choices=['all', 'tiingo', 'security', 'schema'], 
                       default='all', help='Test category to run')
    parser.add_argument('--integration', action='store_true', 
                       help='Include integration tests (requires database)')
    parser.add_argument('--fast', action='store_true',
                       help='Run only fast tests (skip integration/slow tests)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    # Determine test directory
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    test_dir = project_root / 'tests' / 'regression'
    
    if not test_dir.exists():
        print(f"❌ Regression test directory not found: {test_dir}")
        sys.exit(1)
    
    print("🚀 Running Regression Tests for Critical Issues")
    print(f"📁 Test directory: {test_dir}")
    print(f"🎯 Category: {args.category}")
    
    # Build pytest command
    base_cmd = [sys.executable, '-m', 'pytest', str(test_dir)]
    
    if args.verbose:
        base_cmd.extend(['-v', '--tb=short'])
    
    # Determine which tests to run
    test_results = []
    
    if args.category == 'all' or args.category == 'tiingo':
        print("\n" + "="*60)
        print("🔍 TIINGO END DATE INTERPRETATION TESTS")
        print("="*60)
        
        tiingo_cmd = base_cmd + ['-k', 'tiingo', '--tb=short']
        if not args.integration:
            tiingo_cmd.extend(['-m', 'not integration'])
        
        success = run_command(tiingo_cmd, "Testing Tiingo end date interpretation fixes")
        test_results.append(('Tiingo Tests', success))
    
    if args.category == 'all' or args.category == 'security':
        print("\n" + "="*60)
        print("🔒 HARDCODED API KEYS SECURITY TESTS") 
        print("="*60)
        
        security_cmd = base_cmd + ['-k', 'api_key', '--tb=short']
        if not args.integration:
            security_cmd.extend(['-m', 'not integration'])
        
        success = run_command(security_cmd, "Testing hardcoded API key prevention")
        test_results.append(('Security Tests', success))
    
    if args.category == 'all' or args.category == 'schema':
        print("\n" + "="*60)
        print("🗃️  DATABASE SCHEMA COMPATIBILITY TESTS")
        print("="*60)
        
        schema_cmd = base_cmd + ['-k', 'schema', '--tb=short']
        if not args.integration:
            schema_cmd.extend(['-m', 'not integration'])
        
        success = run_command(schema_cmd, "Testing database schema compatibility")
        test_results.append(('Schema Tests', success))
    
    # Run all tests if no specific category
    if args.category == 'all':
        print("\n" + "="*60)
        print("🎯 COMPREHENSIVE REGRESSION TEST SUITE")
        print("="*60)
        
        all_cmd = base_cmd.copy()
        if args.fast:
            all_cmd.extend(['-m', 'not slow and not integration'])
        elif not args.integration:
            all_cmd.extend(['-m', 'not integration'])
        
        success = run_command(all_cmd, "Running complete regression test suite")
        test_results.append(('All Regression Tests', success))
    
    # Summary
    print("\n" + "="*60)
    print("📊 REGRESSION TEST SUMMARY")
    print("="*60)
    
    all_passed = True
    for test_name, passed in test_results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{test_name:30} {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 ALL REGRESSION TESTS PASSED!")
        print("✅ No critical issues detected")
        print("✅ Safe to proceed with deployment")
    else:
        print("\n💥 REGRESSION TESTS FAILED!")
        print("❌ Critical issues detected - DO NOT DEPLOY")
        print("❌ Review failures and fix issues before proceeding")
    
    print(f"\n📋 Test Results: {sum(1 for _, passed in test_results if passed)}/{len(test_results)} passed")
    
    # Exit with appropriate code
    sys.exit(0 if all_passed else 1)

if __name__ == '__main__':
    main()