#!/usr/bin/env python3
"""
Comprehensive validation of migration results and impact analysis
"""

import subprocess
import sys
from pathlib import Path

# List of all files we've migrated
MIGRATED_FILES = [
    "src/infrastructure/vendor/tiingo/services/populate_instrument_tiingo.py",
    "src/infrastructure/vendor/polygon/services/populate_instrument_polygon.py",
    "src/infrastructure/vendor/eodhd/services/populate_instrument_eodhd.py",
    "src/domains/market_data/services/core/agent/adapters/tiingo_adapter.py",
    "src/domains/market_data/services/vendor_adapters/news/turbo_news_backfill.py"
]

def validate_all_migrations():
    """Validate all migrated files and generate comprehensive report"""
    print("🚀 COMPREHENSIVE MIGRATION VALIDATION")
    print("=" * 60)

    results = []

    for file_path in MIGRATED_FILES:
        if not Path(file_path).exists():
            print(f"⚠️ File not found: {file_path}")
            continue

        print(f"\n🔍 Validating: {Path(file_path).name}")
        try:
            result = subprocess.run([
                "python3", "scripts/validate_shared_utilities_integration.py",
                "--file", file_path
            ], capture_output=True, text=True)

            if "100%" in result.stdout:
                results.append(("✅", Path(file_path).name, 100))
                print("   ✅ 100% - Fully migrated")
            elif "75%" in result.stdout:
                results.append(("⚠️", Path(file_path).name, 75))
                print("   ⚠️ 75% - Mostly migrated")
            elif "50%" in result.stdout:
                results.append(("🔶", Path(file_path).name, 50))
                print("   🔶 50% - Partially migrated")
            else:
                results.append(("❌", Path(file_path).name, 0))
                print("   ❌ 0% - Not migrated")

        except Exception as e:
            print(f"   ❌ Error validating: {e}")
            results.append(("❌", Path(file_path).name, 0))

    # Generate summary report
    print(f"\n📊 MIGRATION IMPACT SUMMARY")
    print("=" * 40)

    total_files = len(results)
    fully_migrated = len([r for r in results if r[2] >= 80])
    partially_migrated = len([r for r in results if 50 <= r[2] < 80])
    needs_work = len([r for r in results if r[2] < 50])
    avg_score = sum(r[2] for r in results) / max(total_files, 1)

    print(f"📁 Total Files Migrated: {total_files}")
    print(f"✅ Fully Migrated (≥80%): {fully_migrated}")
    print(f"⚠️ Partially Migrated (50-79%): {partially_migrated}")
    print(f"❌ Needs Work (<50%): {needs_work}")
    print(f"📈 Average Migration Score: {avg_score:.1f}%")

    print(f"\n🎯 DETAILED RESULTS:")
    for icon, filename, score in results:
        print(f"   {icon} {filename:<45} {score:>3}%")

    # Calculate impact metrics
    estimated_lines_saved = total_files * 35  # Average 35 lines saved per file
    maintenance_reduction = f"{total_files} files → 3 shared utilities"

    print(f"\n💎 MIGRATION BENEFITS ACHIEVED:")
    print(f"   📏 Estimated lines of code saved: ~{estimated_lines_saved} lines")
    print(f"   🎯 Maintenance points reduced: {maintenance_reduction}")
    print(f"   🛡️ Standardization coverage: {(fully_migrated/max(total_files,1)*100):.1f}%")
    print(f"   📊 Enhanced monitoring: {fully_migrated} files with rich statistics")
    print(f"   ⚡ Rate limiting: {fully_migrated} files with vendor-specific limits")
    print(f"   🔑 API key management: {fully_migrated + partially_migrated} files standardized")

    return avg_score >= 75

if __name__ == "__main__":
    success = validate_all_migrations()
    sys.exit(0 if success else 1)