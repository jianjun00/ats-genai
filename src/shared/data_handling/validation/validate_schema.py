#!/usr/bin/env python3
"""
Database Schema Validation Script

This script validates that all database code matches the actual database schema.
Run this before committing any database-related changes.

Usage:
    python scripts/validate_schema.py --check-all
    python scripts/validate_schema.py --strict
    python scripts/validate_schema.py --file src/module.py
"""

import os
import sys
import asyncio
import asyncpg
import argparse
import re
from pathlib import Path
from typing import List, Tuple

# Use proper relative imports
try:
    from ..config.environment import Environment
except ImportError:
    # Fallback environment configuration
    class Environment:
        def get_database_url(self):
            host = os.getenv("DB_HOST", "localhost")
            port = os.getenv("DB_PORT", "5433")
            user = os.getenv("DB_USER", "postgres")
            password = os.getenv("DB_PASSWORD", "postgres")
            database = os.getenv("DB_NAME", "dev_db")
            return f"postgresql://{user}:{password}@{host}:{port}/{database}"


class SchemaValidator:
    """Validates database schema compatibility"""

    def __init__(self):
        self.env = Environment()
        self.errors = []
        self.warnings = []
        self.db_schema = {}

    async def connect_database(self):
        """Connect to database and get schema"""
        try:
            self.conn = await asyncpg.connect(self.env.get_database_url())
            await self.load_schema()
            print("✅ Connected to database successfully")
        except Exception as e:
            self.errors.append(f"❌ Database connection failed: {e}")
            return False
        return True

    async def load_schema(self):
        """Load current database schema"""
        query = """
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name LIKE 'dev_%'
            ORDER BY table_name, ordinal_position
        """

        rows = await self.conn.fetch(query)

        for row in rows:
            table = row['table_name']
            if table not in self.db_schema:
                self.db_schema[table] = {}

            self.db_schema[table][row['column_name']] = {
                'type': row['data_type'],
                'nullable': row['is_nullable'] == 'YES'
            }

        print(f"✅ Loaded schema for {len(self.db_schema)} tables")

        # Validate that all dev tables have required audit columns
        self.validate_required_columns()

    def validate_required_columns(self):
        """Validate that all dev tables have required audit columns like created_at"""
        required_columns = ['created_at']  # Could add 'updated_at' in future if needed

        # Exception list for tables that might not need created_at
        exceptions = {
            'dev_db_version',  # Migration management table, managed separately
        }

        for table_name, columns in self.db_schema.items():
            if not table_name.startswith('dev_'):
                continue

            if table_name in exceptions:
                continue

            for required_col in required_columns:
                if required_col not in columns:
                    self.errors.append(
                        f"❌ Table '{table_name}' missing required column '{required_col}' - all data tables must have audit timestamps"
                    )
                else:
                    # Validate column type for created_at
                    col_type = columns[required_col]['type']
                    if required_col == 'created_at' and 'timestamp' not in col_type.lower():
                        self.warnings.append(
                            f"⚠️  Table '{table_name}' column '{required_col}' has type '{col_type}' - should be TIMESTAMPTZ"
                        )

    def scan_python_files(self, directory: str = "src") -> List[Path]:
        """Find all Python files that might contain database code"""
        python_files = []
        for path in Path(directory).rglob("*.py"):
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
                # Look for database-related patterns
                if any(pattern in content.lower() for pattern in [
                    'select ', 'insert ', 'update ', 'delete ', 'from ', 'asyncpg', 'fetchrow', 'execute'
                ]):
                    python_files.append(path)

        return python_files

    def extract_sql_queries(self, file_path: Path) -> List[Tuple[str, int]]:
        """Extract SQL queries from Python file"""
        queries = []

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.split('\n')

        # Look for SQL patterns
        sql_patterns = [
            r'"""[\s]*SELECT.*?"""',
            r"'''[\s]*SELECT.*?'''",
            r'"SELECT.*?"',
            r"'SELECT.*?'",
            r'f"""[\s]*SELECT.*?"""',
            r"f'''[\s]*SELECT.*?'''"
        ]

        for pattern in sql_patterns:
            matches = re.finditer(pattern, content, re.DOTALL | re.IGNORECASE)
            for match in matches:
                query = match.group(0)
                # Find line number
                line_num = content[:match.start()].count('\n') + 1
                queries.append((query, line_num))

        return queries

    def validate_table_references(self, query: str, file_path: Path, line_num: int):
        """Validate that tables referenced in query exist"""
        # Extract table names from FROM clauses
        from_pattern = r'FROM\s+(\w+)'
        matches = re.findall(from_pattern, query, re.IGNORECASE)

        for table_name in matches:
            if table_name.startswith('dev_') and table_name not in self.db_schema:
                self.errors.append(
                    f"❌ {file_path}:{line_num} - Table '{table_name}' does not exist"
                )

    def validate_column_references(self, query: str, file_path: Path, line_num: int):
        """Validate that columns referenced in query exist"""
        # Extract table from FROM clause
        from_match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
        if not from_match:
            return

        table_name = from_match.group(1)
        if table_name not in self.db_schema:
            return

        table_columns = set(self.db_schema[table_name].keys())

        # Extract column names from SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return

        select_clause = select_match.group(1)

        # Simple column extraction (doesn't handle all SQL complexities)
        columns = re.findall(r'\b(\w+)\b', select_clause)

        for column in columns:
            if column.lower() not in ['select', 'from', 'where', 'and', 'or', 'as'] and \
               column not in table_columns and \
               not column.startswith('$'):  # Skip parameters
                self.errors.append(
                    f"❌ {file_path}:{line_num} - Column '{column}' does not exist in table '{table_name}'"
                )

    async def validate_query_syntax(self, query: str, file_path: Path, line_num: int):
        """Validate SQL query syntax by preparing it"""
        try:
            # Clean up the query (remove quotes, f-string markers, etc.)
            clean_query = query.strip('"\'')
            clean_query = re.sub(r'^f["\']', '', clean_query)
            clean_query = re.sub(r'["\']$', '', clean_query)
            clean_query = clean_query.strip()

            if clean_query.upper().startswith('SELECT'):
                await self.conn.prepare(clean_query)
        except Exception as e:
            self.errors.append(
                f"❌ {file_path}:{line_num} - SQL syntax error: {e}"
            )

    def check_anti_patterns(self, file_path: Path):
        """Check for known anti-patterns"""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.split('\n')

        anti_patterns = {
            'dev_training_dataset': 'Should be "dev_training_dataset" (singular)',
            'created_at': 'Should be "creation_timestamp"',
            'file_size_bytes': 'Should be "file_size_mb"',
        }

        for line_num, line in enumerate(lines, 1):
            for pattern, message in anti_patterns.items():
                if pattern in line:
                    self.errors.append(
                        f"❌ {file_path}:{line_num} - Anti-pattern detected: {pattern}. {message}"
                    )

    async def validate_file(self, file_path: Path):
        """Validate a single Python file"""
        print(f"🔍 Validating {file_path}")

        # Check for anti-patterns
        self.check_anti_patterns(file_path)

        # Extract and validate SQL queries
        queries = self.extract_sql_queries(file_path)

        for query, line_num in queries:
            self.validate_table_references(query, file_path, line_num)
            self.validate_column_references(query, file_path, line_num)
            await self.validate_query_syntax(query, file_path, line_num)

    async def validate_all(self, directory: str = "src"):
        """Validate all Python files in directory"""
        if not await self.connect_database():
            return False

        files = self.scan_python_files(directory)
        print(f"🔍 Found {len(files)} Python files with potential database code")

        for file_path in files:
            await self.validate_file(file_path)

        await self.conn.close()
        return len(self.errors) == 0

    def print_results(self):
        """Print validation results"""
        print("\n" + "="*60)
        print("SCHEMA VALIDATION RESULTS")
        print("="*60)

        if self.errors:
            print(f"\n❌ {len(self.errors)} ERRORS FOUND:")
            for error in self.errors:
                print(f"  {error}")

        if self.warnings:
            print(f"\n⚠️  {len(self.warnings)} WARNINGS:")
            for warning in self.warnings:
                print(f"  {warning}")

        if not self.errors and not self.warnings:
            print("\n✅ ALL SCHEMA VALIDATIONS PASSED!")
            print("✅ No schema errors detected")
            print("✅ Code is ready for deployment")

        print("\n" + "="*60)

        return len(self.errors) == 0


async def main():
    parser = argparse.ArgumentParser(description="Database Schema Validation")
    parser.add_argument("--check-all", action="store_true",
                       help="Check all Python files in src/")
    parser.add_argument("--strict", action="store_true",
                       help="Strict mode - fail on warnings too")
    parser.add_argument("--file", type=str,
                       help="Check specific file")
    parser.add_argument("--directory", type=str, default="src",
                       help="Directory to scan (default: src)")

    args = parser.parse_args()

    validator = SchemaValidator()

    success = False
    if args.file:
        if await validator.connect_database():
            await validator.validate_file(Path(args.file))
            await validator.conn.close()
        success = validator.print_results()
    elif args.check_all:
        success = await validator.validate_all(args.directory)
        success = validator.print_results()
    else:
        print("❌ Please specify --check-all or --file <filename>")
        return 1

    if not success:
        print("\n❌ SCHEMA VALIDATION FAILED")
        print("❌ Fix these issues before committing or deploying")
        return 1

    if args.strict and validator.warnings:
        print("\n❌ STRICT MODE: Warnings treated as errors")
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)