#!/usr/bin/env python3
"""
DAO Import Migration Script

This script helps migrate from old scattered DAO imports to the new unified DAO structure.
It provides both automated replacement and analysis tools.
"""

import os
import re
import argparse
from pathlib import Path
from typing import Dict, List, Tuple


class DAOMigrationTool:
    """Tool to migrate DAO imports from old structure to new unified structure."""
    
    def __init__(self, repo_root: str):
        self.repo_root = Path(repo_root)
        
        # Define import mappings from old to new structure
        self.import_mappings = {
            # Daily Prices DAOs
            'from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO': 
                'from dao.vendors.polygon_dao import PolygonDAO',
            'from dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO': 
                'from dao.vendors.tiingo_dao import TiingoDAO',
                
            # Dividend DAOs
            'from dao.dividend_polygon_dao import DividendPolygonDAO': 
                'from dao.vendors.polygon_dao import PolygonDAO',
            'from dao.dividend_tiingo_dao import DividendTiingoDAO': 
                'from dao.vendors.tiingo_dao import TiingoDAO',
                
            # Stock Split DAOs  
            'from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO': 
                'from dao.vendors.polygon_dao import PolygonDAO',
            'from dao.stock_splits_tiingo_dao import StockSplitsTiingoDAO': 
                'from dao.vendors.tiingo_dao import TiingoDAO',
                
            # Instrument DAOs
            'from dao.instrument_polygon_dao import InstrumentPolygonDAO': 
                'from dao.vendors.polygon_dao import PolygonDAO',
        }
        
        # Class name mappings
        self.class_mappings = {
            'DailyPricesPolygonDAO': 'PolygonDAO',
            'DailyPricesTiingoDAO': 'TiingoDAO',
            'DividendPolygonDAO': 'PolygonDAO',
            'DividendTiingoDAO': 'TiingoDAO',
            'StockSplitsPolygonDAO': 'PolygonDAO',
            'StockSplitsTiingoDAO': 'TiingoDAO',
            'InstrumentPolygonDAO': 'PolygonDAO',
        }
        
        # Method name mappings for vendor-specific DAOs
        self.method_mappings = {
            # Polygon DAO method mappings
            'PolygonDAO': {
                'insert_price': 'insert_daily_price',
                'get_price': 'get_daily_price',
                'list_prices': 'list_daily_prices',
                'batch_insert_prices': 'batch_insert_daily_prices',
                'insert_dividend': 'insert_dividend',
                'get_dividends_by_symbol': 'get_dividends_by_symbol',
                'insert_split': 'insert_stock_split',
                'get_splits_by_symbol': 'get_splits_by_symbol',
                'insert_instrument': 'insert_instrument',
                'get_instrument': 'get_instrument',
            },
            # Tiingo DAO method mappings
            'TiingoDAO': {
                'insert_price': 'insert_daily_price',
                'get_price': 'get_daily_price', 
                'list_prices': 'list_daily_prices',
                'batch_insert_prices': 'batch_insert_daily_prices',
                'insert_dividend': 'insert_dividend',
                'get_dividends_by_symbol': 'get_dividends_by_symbol',
                'insert_split': 'insert_stock_split',
                'get_splits_by_symbol': 'get_splits_by_symbol',
            }
        }
    
    def find_files_with_old_imports(self) -> List[Path]:
        """Find all Python files that contain old DAO imports."""
        old_dao_patterns = [
            'daily_prices_polygon_dao',
            'daily_prices_tiingo_dao', 
            'dividend_polygon_dao',
            'dividend_tiingo_dao',
            'stock_splits_polygon_dao',
            'stock_splits_tiingo_dao',
            'instrument_polygon_dao',
        ]
        
        files_with_old_imports = []
        
        for pattern in old_dao_patterns:
            for py_file in self.repo_root.rglob('*.py'):
                if py_file.is_file():
                    try:
                        content = py_file.read_text(encoding='utf-8')
                        if pattern in content:
                            files_with_old_imports.append(py_file)
                    except (UnicodeDecodeError, FileNotFoundError):
                        continue
        
        return list(set(files_with_old_imports))  # Remove duplicates
    
    def analyze_file(self, file_path: Path) -> Dict[str, List[str]]:
        """Analyze a file to identify old DAO usage patterns."""
        try:
            content = file_path.read_text(encoding='utf-8')
        except (UnicodeDecodeError, FileNotFoundError):
            return {}
        
        analysis = {
            'old_imports': [],
            'class_usage': [],
            'method_calls': [],
            'suggested_imports': [],
        }
        
        lines = content.split('\n')
        
        for line_num, line in enumerate(lines, 1):
            # Check for old imports
            for old_import, new_import in self.import_mappings.items():
                if old_import in line:
                    analysis['old_imports'].append(f"Line {line_num}: {line.strip()}")
                    analysis['suggested_imports'].append(new_import)
            
            # Check for old class usage
            for old_class, new_class in self.class_mappings.items():
                if old_class in line and 'import' not in line:
                    analysis['class_usage'].append(f"Line {line_num}: {line.strip()}")
        
        return analysis
    
    def generate_migration_report(self, output_file: str = None) -> str:
        """Generate a comprehensive migration report."""
        files_to_migrate = self.find_files_with_old_imports()
        
        report_lines = [
            "# DAO Migration Analysis Report",
            f"Generated on: {os.popen('date').read().strip()}",
            "",
            f"## Summary",
            f"- Files requiring migration: {len(files_to_migrate)}",
            "",
            "## Files Requiring Migration",
            ""
        ]
        
        for file_path in sorted(files_to_migrate):
            rel_path = file_path.relative_to(self.repo_root)
            analysis = self.analyze_file(file_path)
            
            report_lines.extend([
                f"### {rel_path}",
                ""
            ])
            
            if analysis.get('old_imports'):
                report_lines.extend([
                    "**Old Imports Found:**",
                    "```python"
                ])
                for old_import in analysis['old_imports']:
                    report_lines.append(old_import)
                report_lines.append("```")
                report_lines.append("")
            
            if analysis.get('suggested_imports'):
                report_lines.extend([
                    "**Suggested New Imports:**",
                    "```python"
                ])
                for new_import in set(analysis['suggested_imports']):
                    report_lines.append(new_import)
                report_lines.append("```")
                report_lines.append("")
            
            if analysis.get('class_usage'):
                report_lines.extend([
                    "**Class Usage to Update:**",
                    "```python"
                ])
                for usage in analysis['class_usage'][:5]:  # Limit to first 5 examples
                    report_lines.append(usage)
                if len(analysis['class_usage']) > 5:
                    report_lines.append(f"... and {len(analysis['class_usage']) - 5} more")
                report_lines.append("```")
                report_lines.append("")
        
        report_lines.extend([
            "",
            "## Migration Instructions",
            "",
            "1. Review each file listed above",
            "2. Update imports according to suggestions",
            "3. Update class instantiations and method calls",
            "4. Test thoroughly after each file migration",
            "5. Update corresponding test files",
            "",
            "## Next Steps",
            "",
            "- Use `migrate_dao_imports.py --migrate <file>` to auto-migrate individual files",
            "- Run tests after each migration: `PYTHONPATH=src python -m pytest`",
            "- Review DAO_MIGRATION_GUIDE.md for detailed patterns",
            ""
        ])
        
        report_content = '\n'.join(report_lines)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report_content)
            print(f"Migration report written to: {output_file}")
        
        return report_content
    
    def migrate_file(self, file_path: Path, dry_run: bool = True) -> Tuple[bool, str]:
        """Migrate a single file from old DAO imports to new structure."""
        try:
            content = file_path.read_text(encoding='utf-8')
        except (UnicodeDecodeError, FileNotFoundError):
            return False, f"Could not read file: {file_path}"
        
        original_content = content
        changes_made = []
        
        # Replace imports
        for old_import, new_import in self.import_mappings.items():
            if old_import in content:
                content = content.replace(old_import, new_import)
                changes_made.append(f"Updated import: {old_import} -> {new_import}")
        
        # Replace class names (more careful replacement)
        for old_class, new_class in self.class_mappings.items():
            # Replace class instantiation
            pattern = rf'\b{old_class}\('
            if re.search(pattern, content):
                content = re.sub(pattern, f'{new_class}(', content)
                changes_made.append(f"Updated class usage: {old_class} -> {new_class}")
        
        if content != original_content:
            if not dry_run:
                # Create backup
                backup_path = file_path.with_suffix(f"{file_path.suffix}.backup")
                backup_path.write_text(original_content, encoding='utf-8')
                
                # Write updated content
                file_path.write_text(content, encoding='utf-8')
                
                return True, f"File migrated successfully. Backup created: {backup_path}"
            else:
                return True, f"Dry run - would make {len(changes_made)} changes: {changes_made}"
        else:
            return False, "No changes needed"
    
    def validate_migration(self, file_path: Path) -> List[str]:
        """Validate that a migrated file doesn't have obvious issues."""
        try:
            content = file_path.read_text(encoding='utf-8')
        except (UnicodeDecodeError, FileNotFoundError):
            return ["Could not read file for validation"]
        
        issues = []
        
        # Check for remaining old imports
        for old_import in self.import_mappings.keys():
            if old_import in content:
                issues.append(f"Still contains old import: {old_import}")
        
        # Check for import-class mismatches
        lines = content.split('\n')
        imported_classes = set()
        used_classes = set()
        
        for line in lines:
            # Track imported classes
            import_match = re.search(r'from .+ import (\w+)', line)
            if import_match:
                imported_classes.add(import_match.group(1))
            
            # Track used classes (rough heuristic)
            for old_class in self.class_mappings.keys():
                if old_class in line and 'import' not in line:
                    used_classes.add(old_class)
        
        # Check for unused imports (basic check)
        for imported_class in imported_classes:
            if imported_class not in content.replace(f"import {imported_class}", ""):
                issues.append(f"Imported but possibly unused: {imported_class}")
        
        return issues


def main():
    parser = argparse.ArgumentParser(description='DAO Migration Tool')
    parser.add_argument('--repo-root', default='.', help='Repository root directory')
    parser.add_argument('--analyze', action='store_true', help='Analyze files needing migration')
    parser.add_argument('--report', help='Generate migration report to file')
    parser.add_argument('--migrate', help='Migrate specific file')
    parser.add_argument('--dry-run', action='store_true', help='Dry run migration (default)')
    parser.add_argument('--apply', action='store_true', help='Actually apply changes')
    parser.add_argument('--validate', help='Validate migrated file')
    
    args = parser.parse_args()
    
    migration_tool = DAOMigrationTool(args.repo_root)
    
    if args.analyze or args.report:
        report = migration_tool.generate_migration_report(args.report)
        if not args.report:
            print(report)
    
    elif args.migrate:
        file_path = Path(args.migrate)
        if not file_path.exists():
            print(f"File not found: {file_path}")
            return 1
        
        dry_run = not args.apply
        success, message = migration_tool.migrate_file(file_path, dry_run=dry_run)
        print(f"Migration {'simulation' if dry_run else 'result'}: {message}")
        
        if success and not dry_run:
            # Validate migration
            issues = migration_tool.validate_migration(file_path)
            if issues:
                print("Validation issues found:")
                for issue in issues:
                    print(f"  - {issue}")
            else:
                print("Migration validation passed")
    
    elif args.validate:
        file_path = Path(args.validate)
        if not file_path.exists():
            print(f"File not found: {file_path}")
            return 1
        
        issues = migration_tool.validate_migration(file_path)
        if issues:
            print("Validation issues:")
            for issue in issues:
                print(f"  - {issue}")
            return 1
        else:
            print("Validation passed")
    
    else:
        parser.print_help()
    
    return 0


if __name__ == '__main__':
    exit(main())