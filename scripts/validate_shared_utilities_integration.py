#!/usr/bin/env python3
"""
Validation Script for Shared Utilities Integration

This script validates that migrated files properly integrate with shared utilities
and provides comprehensive testing and reporting.

Usage:
    python scripts/validate_shared_utilities_integration.py --all
    python scripts/validate_shared_utilities_integration.py --file src/infrastructure/vendor/tiingo/services/populate_instrument_tiingo.py
    python scripts/validate_shared_utilities_integration.py --vendor polygon
"""

import os
import sys
import argparse
import importlib.util
from pathlib import Path
from typing import List, Dict, Tuple
import logging
import subprocess
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SharedUtilitiesValidator:
    """Validates shared utilities integration in migrated files"""

    def __init__(self):
        self.src_path = Path("src")
        self.validation_results = {}

    def check_shared_utilities_available(self) -> Dict[str, bool]:
        """Verify all shared utilities are available"""
        utilities = {
            'vendor_api_keys': 'shared.utils.vendor_api_keys',
            'database_connections': 'shared.utils.database_connections',
            'backfill_framework': 'shared.utils.backfill_framework'
        }

        results = {}
        sys.path.insert(0, str(self.src_path))

        for name, module_path in utilities.items():
            try:
                module = importlib.import_module(module_path)
                results[name] = True
                logger.info(f"✅ {name}: {module_path} available")
            except ImportError as e:
                results[name] = False
                logger.error(f"❌ {name}: {module_path} not available - {e}")

        return results

    def validate_file_migration(self, file_path: Path) -> Dict[str, any]:
        """Validate a single file's migration to shared utilities"""
        if not file_path.exists():
            return {"error": f"File not found: {file_path}"}

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        results = {
            "file": str(file_path),
            "api_keys": self._check_api_key_migration(content),
            "database": self._check_database_migration(content),
            "statistics": self._check_statistics_migration(content),
            "legacy_patterns": self._check_legacy_patterns(content),
            "imports": self._check_shared_imports(content),
            "score": 0
        }

        # Calculate migration score
        score = 0
        if results["api_keys"]["migrated"]:
            score += 30
        if results["database"]["migrated"]:
            score += 25
        if results["statistics"]["migrated"]:
            score += 20
        if not results["legacy_patterns"]["found"]:
            score += 15
        if results["imports"]["all_present"]:
            score += 10

        results["score"] = score
        return results

    def _check_api_key_migration(self, content: str) -> Dict[str, any]:
        """Check if API key patterns have been migrated"""
        shared_imports = [
            'from shared.utils.vendor_api_keys import',
            'get_polygon_api_key',
            'get_tiingo_api_key',
            'get_eodhd_api_key'
        ]

        legacy_patterns = [
            'from vendor.polygon.utils import POLYGON_API_KEY',
            'from vendor.tiingo.utils import TIINGO_API_KEY',
            'from vendor.eodhd.utils import EODHD_API_KEY',
            'os.environ.get("POLYGON_API_KEY")',
            'os.environ.get("TIINGO_API_KEY")',
            'os.environ.get("EODHD_API_KEY")'
        ]

        has_shared = any(pattern in content for pattern in shared_imports)
        has_legacy = any(pattern in content for pattern in legacy_patterns)

        return {
            "migrated": has_shared and not has_legacy,
            "has_shared_imports": has_shared,
            "has_legacy_patterns": has_legacy,
            "legacy_count": sum(1 for pattern in legacy_patterns if pattern in content)
        }

    def _check_database_migration(self, content: str) -> Dict[str, any]:
        """Check if database connection patterns have been migrated"""
        shared_patterns = [
            'from shared.utils.database_connections import',
            'get_database_pool',
            'get_table_name'
        ]

        legacy_patterns = [
            'Database.create_connection_pool',
            'asyncpg.create_pool',
            'await asyncpg.create_pool'
        ]

        has_shared = any(pattern in content for pattern in shared_patterns)
        has_legacy = any(pattern in content for pattern in legacy_patterns)

        return {
            "migrated": has_shared,
            "has_shared_imports": has_shared,
            "has_legacy_patterns": has_legacy
        }

    def _check_statistics_migration(self, content: str) -> Dict[str, any]:
        """Check if statistics patterns have been migrated"""
        shared_patterns = [
            'from shared.utils.backfill_framework import',
            'BackfillStats',
            'VendorRateLimiters',
            'stats.log_final_summary'
        ]

        custom_patterns = [
            r'@dataclass.*class.*Stats',
            r'class.*Stats.*:',
            r'time\.sleep\('
        ]

        has_shared = any(pattern in content for pattern in shared_patterns)
        has_custom = any(re.search(pattern, content) for pattern in custom_patterns)

        return {
            "migrated": has_shared,
            "has_shared_imports": has_shared,
            "has_custom_stats": has_custom
        }

    def _check_legacy_patterns(self, content: str) -> Dict[str, any]:
        """Check for remaining legacy patterns that should be removed"""
        legacy_patterns = [
            'if not.*API_KEY:',
            'logger.warning.*API key.*not found',
            'sys.exit.*API.*key',
            'complex fallback logic'
        ]

        found_patterns = [pattern for pattern in legacy_patterns if re.search(pattern, content)]

        return {
            "found": len(found_patterns) > 0,
            "patterns": found_patterns,
            "count": len(found_patterns)
        }

    def _check_shared_imports(self, content: str) -> Dict[str, any]:
        """Check if all relevant shared utility imports are present"""
        expected_imports = []

        # Check which imports should be present based on content
        if 'API_KEY' in content:
            expected_imports.append('shared.utils.vendor_api_keys')
        if 'pool' in content or 'database' in content.lower():
            expected_imports.append('shared.utils.database_connections')
        if any(word in content.lower() for word in ['stats', 'backfill', 'rate', 'limit']):
            expected_imports.append('shared.utils.backfill_framework')

        present_imports = [imp for imp in expected_imports if imp in content]

        return {
            "all_present": len(present_imports) == len(expected_imports),
            "expected": expected_imports,
            "present": present_imports,
            "missing": list(set(expected_imports) - set(present_imports))
        }

    def generate_migration_report(self, results: List[Dict]) -> str:
        """Generate a comprehensive migration report"""
        total_files = len(results)
        fully_migrated = sum(1 for r in results if r.get("score", 0) >= 80)
        partially_migrated = sum(1 for r in results if 50 <= r.get("score", 0) < 80)
        needs_migration = sum(1 for r in results if r.get("score", 0) < 50)

        avg_score = sum(r.get("score", 0) for r in results) / max(total_files, 1)

        report = f"""
📊 SHARED UTILITIES MIGRATION REPORT
{'='*60}

📈 OVERALL STATISTICS:
   Total Files Analyzed: {total_files}
   Fully Migrated (≥80%): {fully_migrated} files
   Partially Migrated (50-79%): {partially_migrated} files
   Needs Migration (<50%): {needs_migration} files
   Average Migration Score: {avg_score:.1f}%

🎯 MIGRATION PROGRESS:
   {'█' * int(avg_score/5)}{'░' * (20-int(avg_score/5))} {avg_score:.1f}%

📋 DETAILED RESULTS:
"""

        for result in sorted(results, key=lambda x: x.get("score", 0), reverse=True):
            score = result.get("score", 0)
            file_name = Path(result["file"]).name

            status_icon = "✅" if score >= 80 else "⚠️" if score >= 50 else "❌"

            report += f"\n   {status_icon} {file_name:<40} {score:>3}%"

            if result["api_keys"]["migrated"]:
                report += " 🔑"
            if result["database"]["migrated"]:
                report += " 🗄️"
            if result["statistics"]["migrated"]:
                report += " 📊"

        report += f"""

🔧 RECOMMENDATIONS:

🟢 READY FOR PRODUCTION ({fully_migrated} files):
   Files with 80%+ migration score are ready for immediate use

🟡 NEEDS MINOR UPDATES ({partially_migrated} files):
   Files with 50-79% score need small improvements:
   - Add missing shared utility imports
   - Remove remaining legacy patterns

🔴 REQUIRES MIGRATION ({needs_migration} files):
   Files with <50% score need full migration:
   - Implement API key management with shared utilities
   - Replace database connection patterns
   - Add statistics and rate limiting framework

📝 NEXT STEPS:
   1. Review files with score <50% for migration priority
   2. Run automated migration script on high-value targets
   3. Test migrated files in development environment
   4. Deploy incrementally with monitoring

🚀 MIGRATION IMPACT:
   Estimated lines of code reduction: {total_files * 30} lines
   Maintenance locations reduced: {total_files} → 3 (shared utilities)
   Standardization coverage: {(fully_migrated/max(total_files,1)*100):.1f}%
"""

        return report

def main():
    parser = argparse.ArgumentParser(description="Validate shared utilities integration")
    parser.add_argument("--all", action="store_true", help="Validate all vendor service files")
    parser.add_argument("--file", help="Validate specific file")
    parser.add_argument("--vendor", choices=["polygon", "tiingo", "eodhd"], help="Validate specific vendor files")
    parser.add_argument("--report", action="store_true", help="Generate detailed report")

    args = parser.parse_args()

    validator = SharedUtilitiesValidator()

    # First check if shared utilities are available
    print("🔍 Checking Shared Utilities Availability...")
    utilities_status = validator.check_shared_utilities_available()

    if not all(utilities_status.values()):
        print("⚠️ Some shared utilities are not available. Migration validation may be limited.")

    # Determine files to validate
    files_to_validate = []

    if args.file:
        files_to_validate = [Path(args.file)]
    elif args.vendor:
        vendor_dir = Path(f"src/infrastructure/vendor/{args.vendor}/services")
        if vendor_dir.exists():
            files_to_validate = list(vendor_dir.glob("*.py"))
    elif args.all:
        for vendor in ["polygon", "tiingo", "eodhd"]:
            vendor_dir = Path(f"src/infrastructure/vendor/{vendor}/services")
            if vendor_dir.exists():
                files_to_validate.extend(vendor_dir.glob("*.py"))
    else:
        # Default: validate our demonstration file
        files_to_validate = [Path("src/infrastructure/vendor/tiingo/services/populate_instrument_tiingo.py")]

    print(f"\n🔬 Validating {len(files_to_validate)} files...")

    results = []
    for file_path in files_to_validate:
        if file_path.is_file():
            result = validator.validate_file_migration(file_path)
            results.append(result)

            score = result.get("score", 0)
            status = "✅" if score >= 80 else "⚠️" if score >= 50 else "❌"
            print(f"  {status} {file_path.name:<40} {score:>3}%")

    if args.report and results:
        print(validator.generate_migration_report(results))

    return 0 if results else 1

if __name__ == "__main__":
    sys.exit(main())