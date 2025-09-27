#!/usr/bin/env python3
"""
Test to verify that runner_utils.py has been consolidated into a single authoritative file
and prevent future duplication.
"""
import os

def test_single_runner_utils_file_exists():
    """
    Test that only one runner_utils.py file exists in the codebase.
    """
    print("🔍 TESTING RUNNER_UTILS CONSOLIDATION")
    print("=" * 45)
    
    # Find all runner_utils.py files
    runner_utils_files = []
    for root, dirs, files in os.walk('.'):
        # Skip hidden directories and common ignore patterns
        dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['__pycache__', 'node_modules']]
        
        for file in files:
            if file == 'runner_utils.py':
                file_path = os.path.join(root, file)
                runner_utils_files.append(file_path)
    
    print(f"Found runner_utils.py files: {len(runner_utils_files)}")
    for file_path in runner_utils_files:
        print(f"  - {file_path}")
    
    # Verify only one file exists
    if len(runner_utils_files) == 1:
        print(f"\n✅ CONSOLIDATION SUCCESSFUL!")
        print(f"   Single authoritative runner_utils.py file exists")
        return True
    elif len(runner_utils_files) == 0:
        print(f"\n❌ NO RUNNER_UTILS FILES FOUND!")
        print(f"   Expected: src/domains/trading/services/core/app/runner_utils.py")
        return False
    else:
        print(f"\n❌ MULTIPLE RUNNER_UTILS FILES FOUND!")
        print(f"   This violates the single-source-of-truth principle")
        print(f"   Expected: Only src/domains/trading/services/core/app/runner_utils.py")
        return False


def test_authoritative_file_location():
    """
    Test that the authoritative runner_utils.py is in the correct domain-based location.
    """
    print(f"\n📁 TESTING AUTHORITATIVE FILE LOCATION")
    print("=" * 40)
    
    expected_path = 'src/domains/trading/services/core/app/runner_utils.py'
    
    if os.path.exists(expected_path):
        print(f"✅ Authoritative file exists at: {expected_path}")
        
        # Check file size to ensure it's not empty
        file_size = os.path.getsize(expected_path)
        print(f"   File size: {file_size} bytes")
        
        if file_size > 0:
            print(f"   ✅ File contains content")
            return True
        else:
            print(f"   ❌ File is empty")
            return False
    else:
        print(f"❌ Authoritative file missing: {expected_path}")
        return False


def test_no_legacy_files_remain():
    """
    Test that no legacy runner_utils.py files remain in deprecated locations.
    """
    print(f"\n🗑️ TESTING NO LEGACY FILES REMAIN")
    print("=" * 35)
    
    deprecated_locations = [
        'src/infrastructure/services_legacy/core/app/runner_utils.py',
        'src/app/runner_utils.py',
        'src/services/runner_utils.py',
        'src/infrastructure/core/app/runner_utils.py'
    ]
    
    legacy_files_found = []
    
    for deprecated_path in deprecated_locations:
        if os.path.exists(deprecated_path):
            legacy_files_found.append(deprecated_path)
            print(f"❌ Legacy file found: {deprecated_path}")
    
    if not legacy_files_found:
        print(f"✅ No legacy runner_utils.py files found")
        print(f"   All deprecated locations are clean")
        return True
    else:
        print(f"\n❌ LEGACY FILES STILL EXIST!")
        print(f"   Found {len(legacy_files_found)} legacy files")
        for legacy_file in legacy_files_found:
            print(f"   - {legacy_file}")
        print(f"\n🔧 REQUIRED ACTION:")
        print(f"   Remove legacy files and update imports to use:")
        print(f"   src/domains/trading/services/core/app/runner_utils.py")
        return False


def test_imports_use_correct_path():
    """
    Test that imports use the correct domain-based path.
    """
    print(f"\n📥 TESTING IMPORT PATHS")
    print("=" * 25)
    
    # Search for runner_utils imports in Python files
    incorrect_imports = []
    correct_imports = []
    
    for root, dirs, files in os.walk('.'):
        # Skip hidden directories and common ignore patterns
        dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['__pycache__', 'node_modules']]
        
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                # Check for various import patterns (skip test files with examples)
                if 'from app.runner_utils import' in content and 'test_runner_utils_consolidation_verification.py' not in file_path:
                    incorrect_imports.append((file_path, 'app.runner_utils'))
                elif 'from domains.core.app.runner_utils import' in content:
                    incorrect_imports.append((file_path, 'infrastructure.services_legacy.core.app.runner_utils'))
                elif 'from domains.trading.services.core.app.runner_utils import' in content:
                    correct_imports.append((file_path, 'domains.trading.services.core.app.runner_utils'))
                    
    print(f"Correct imports found: {len(correct_imports)}")
    for file_path, import_path in correct_imports:
        print(f"  ✅ {file_path} → {import_path}")
    
    print(f"\nIncorrect imports found: {len(incorrect_imports)}")
    for file_path, import_path in incorrect_imports:
        print(f"  ❌ {file_path} → {import_path}")
    
    if len(incorrect_imports) == 0:
        print(f"\n✅ ALL IMPORTS USE CORRECT DOMAIN-BASED PATH!")
        return True
    else:
        print(f"\n❌ INCORRECT IMPORTS FOUND!")
        print(f"   Update these imports to use:")
        print(f"   from domains.trading.services.core.app.runner_utils import ...")
        return False


def show_consolidation_summary():
    """
    Show summary of the consolidation benefits.
    """
    print(f"\n📋 CONSOLIDATION BENEFITS")
    print("=" * 30)
    
    print("✅ SINGLE SOURCE OF TRUTH:")
    print("   - Only one runner_utils.py file exists")
    print("   - Located in domain-based architecture: domains/trading/services/core/app/")
    print("   - No duplicate maintenance burden")
    
    print("\n✅ CONSISTENCY GUARANTEED:")
    print("   - All imports reference the same file")
    print("   - No version mismatches between duplicates")
    print("   - Clear ownership in trading domain")
    
    print("\n✅ DOMAIN-BASED ARCHITECTURE:")
    print("   - Follows established domain structure")
    print("   - Trading-related utilities in trading domain")
    print("   - No legacy infrastructure dependencies")
    
    print("\n🔧 IMPORT PATTERN:")
    print("   from domains.trading.services.core.app.runner_utils import run_file_daily_price_ohlcv")


if __name__ == "__main__":
    print("🚨 RUNNER_UTILS CONSOLIDATION VERIFICATION")
    print("=" * 60)
    
    # Test 1: Single file exists
    single_file = test_single_runner_utils_file_exists()
    
    # Test 2: Authoritative location correct
    correct_location = test_authoritative_file_location()
    
    # Test 3: No legacy files
    no_legacy = test_no_legacy_files_remain()
    
    # Test 4: Imports use correct path
    correct_imports = test_imports_use_correct_path()
    
    # Show summary
    show_consolidation_summary()
    
    print(f"\n" + "=" * 60)
    print(f"📋 CONSOLIDATION VERIFICATION SUMMARY:")
    print(f"  Single runner_utils.py file: {single_file}")
    print(f"  Correct authoritative location: {correct_location}")
    print(f"  No legacy files remain: {no_legacy}")
    print(f"  All imports use correct path: {correct_imports}")
    
    if single_file and correct_location and no_legacy and correct_imports:
        print(f"\n🎉 RUNNER_UTILS CONSOLIDATION SUCCESSFUL!")
        print(f"   ✅ Single authoritative file: src/domains/trading/services/core/app/runner_utils.py")
        print(f"   ✅ All imports use domain-based path")
        print(f"   ✅ No duplicate maintenance required")
        print(f"   ✅ Domain-based architecture enforced")
    else:
        print(f"\n❌ CONSOLIDATION ISSUES DETECTED!")
        print(f"   Please address failing tests above")
    
    print(f"\n📝 MAINTENANCE NOTE:")
    print(f"  - Keep only ONE runner_utils.py file")
    print(f"  - Use domain-based import path in all new code")
    print(f"  - Report any duplicate files found as bugs")
    print(f"  - Consolidation prevents maintenance overhead and version mismatches")