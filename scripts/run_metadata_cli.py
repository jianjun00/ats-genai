#!/usr/bin/env python3
"""
CLI tool for querying and managing training run metadata.

This tool provides comprehensive access to training run history, metadata,
and reproducibility information stored in the runs table.

USAGE:
    # List recent runs
    python scripts/run_metadata_cli.py list --limit 10

    # Get detailed metadata for specific run
    python scripts/run_metadata_cli.py show --run-id 42

    # Search runs by type
    python scripts/run_metadata_cli.py list --run-type "training_dataset_generation"

    # Show git commit history
    python scripts/run_metadata_cli.py commits --limit 5

    # Export run metadata
    python scripts/run_metadata_cli.py export --run-id 42 --output metadata.json

    # Validate run reproducibility
    python scripts/run_metadata_cli.py validate --run-id 42

EXAMPLES:
    # Show all unified dataset generation runs
    python scripts/run_metadata_cli.py list --run-type "unified_training" --environment dev

    # Compare two runs for reproducibility
    python scripts/run_metadata_cli.py compare --run-ids 41,42

    # Show runs from last 24 hours
    python scripts/run_metadata_cli.py list --since "1 day ago"
"""

import asyncio
import sys
import json
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path
import asyncpg
# from tabulate import tabulate  # Optional, will use simple formatting if not available

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from src.ml.training_data.utils.run_metadata_tracker import RunMetadataTracker

class RunMetadataCLI:
    """CLI interface for run metadata management."""

    def __init__(self, environment: str = 'dev'):
        """Initialize CLI with target environment."""
        self.environment = environment
        self.table_name = f"{environment}_runs"
        self._connection = None

    async def _get_connection(self) -> asyncpg.Connection:
        """Get database connection for specified environment."""
        if self._connection is None:
            if self.environment == 'dev':
                self._connection = await asyncpg.connect(
                    host='localhost', port=3432, user='postgres',
                    password='dev_password', database='dev_db'
                )
            elif self.environment == 'intg':
                self._connection = await asyncpg.connect(
                    host='localhost', port=4432, user='postgres',
                    password='intg_password', database='intg_db'
                )
            else:
                raise ValueError(f"Unknown environment: {self.environment}")

        return self._connection

    async def list_runs(self, limit: int = 20, run_type: str = None,
                       status: str = None, since: str = None) -> List[Dict[str, Any]]:
        """List training runs with optional filters."""
        conn = await self._get_connection()

        # Build query with filters
        where_clauses = []
        params = []
        param_count = 0

        if run_type:
            param_count += 1
            where_clauses.append(f"run_type ILIKE ${param_count}")
            params.append(f"%{run_type}%")

        if status:
            param_count += 1
            where_clauses.append(f"status = ${param_count}")
            params.append(status)

        if since:
            param_count += 1
            where_clauses.append(f"created_at >= NOW() - INTERVAL ${param_count}")
            params.append(since)

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        param_count += 1

        query = f"""
            SELECT id, run_type, status, created_by, start_time, end_time,
                   git_commit_hash, git_branch, command_line,
                   (EXTRACT(EPOCH FROM (COALESCE(end_time, NOW()) - start_time)) / 60)::INTEGER as duration_minutes
            FROM {self.table_name}
            {where_sql}
            ORDER BY created_at DESC
            LIMIT ${param_count}
        """

        params.append(limit)
        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]

    async def get_run_details(self, run_id: int) -> Optional[Dict[str, Any]]:
        """Get comprehensive details for a specific run."""
        conn = await self._get_connection()

        query = f"""
            SELECT * FROM {self.table_name} WHERE id = $1
        """

        row = await conn.fetchrow(query, run_id)
        return dict(row) if row else None

    async def get_commit_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get unique git commits used in runs."""
        conn = await self._get_connection()

        query = f"""
            SELECT DISTINCT git_commit_hash, git_branch,
                   MIN(start_time) as first_used,
                   MAX(start_time) as last_used,
                   COUNT(*) as run_count
            FROM {self.table_name}
            WHERE git_commit_hash != '' AND git_commit_hash != 'unknown'
            GROUP BY git_commit_hash, git_branch
            ORDER BY first_used DESC
            LIMIT $1
        """

        rows = await conn.fetch(query, limit)
        return [dict(row) for row in rows]

    async def validate_reproducibility(self, run_id: int) -> Dict[str, Any]:
        """Validate if a run can be reproduced based on metadata."""
        run_data = await self.get_run_details(run_id)
        if not run_data:
            return {"error": f"Run {run_id} not found"}

        issues = []
        warnings = []

        # Check git commit
        if not run_data.get('git_commit_hash') or run_data['git_commit_hash'] == 'unknown':
            issues.append("Missing git commit hash - cannot verify code version")

        # Check for uncommitted changes
        host_info = json.loads(run_data.get('host_info', '{}'))
        if host_info.get('has_uncommitted_changes'):
            warnings.append("Run executed with uncommitted changes")

        # Check command line
        if not run_data.get('command_line'):
            issues.append("Missing command line arguments")

        # Check dependencies
        if not run_data.get('dependencies_hash') or run_data['dependencies_hash'] == 'unknown':
            warnings.append("Missing dependencies hash - package versions not tracked")

        # Check environment
        if not run_data.get('environment'):
            warnings.append("Environment not specified")

        return {
            "run_id": run_id,
            "reproducible": len(issues) == 0,
            "issues": issues,
            "warnings": warnings,
            "metadata_completeness": self._calculate_completeness(run_data)
        }

    def _calculate_completeness(self, run_data: Dict[str, Any]) -> float:
        """Calculate metadata completeness percentage."""
        required_fields = [
            'git_commit_hash', 'git_branch', 'command_line', 'environment',
            'host_info', 'python_version', 'dependencies_hash'
        ]

        present = 0
        for field in required_fields:
            value = run_data.get(field)
            if value and value != 'unknown' and value != '':
                present += 1

        return (present / len(required_fields)) * 100

    async def compare_runs(self, run_ids: List[int]) -> Dict[str, Any]:
        """Compare metadata between multiple runs."""
        runs = []
        for run_id in run_ids:
            run_data = await self.get_run_details(run_id)
            if run_data:
                runs.append(run_data)

        if len(runs) < 2:
            return {"error": "Need at least 2 valid runs to compare"}

        comparison = {
            "runs": [{"id": r["id"], "run_type": r["run_type"]} for r in runs],
            "differences": {},
            "similarities": {}
        }

        # Compare key fields
        compare_fields = ['git_commit_hash', 'git_branch', 'environment', 'python_version']

        for field in compare_fields:
            values = [r.get(field) for r in runs]
            unique_values = list(set(values))

            if len(unique_values) == 1:
                comparison["similarities"][field] = unique_values[0]
            else:
                comparison["differences"][field] = dict(zip(run_ids, values))

        return comparison

    async def export_metadata(self, run_id: int, output_file: str) -> bool:
        """Export complete run metadata to JSON file."""
        run_data = await self.get_run_details(run_id)
        if not run_data:
            print(f"Run {run_id} not found")
            return False

        # Convert datetime objects to strings for JSON serialization
        for key, value in run_data.items():
            if isinstance(value, datetime):
                run_data[key] = value.isoformat()

        with open(output_file, 'w') as f:
            json.dump(run_data, f, indent=2, default=str)

        print(f"Metadata exported to {output_file}")
        return True

    async def close(self):
        """Close database connection."""
        if self._connection:
            await self._connection.close()

def print_runs_table(runs: List[Dict[str, Any]]) -> None:
    """Print runs in formatted table."""
    if not runs:
        print("No runs found.")
        return

    # Simple table formatting without tabulate
    print(f"{'ID':<5} {'Type':<30} {'Status':<10} {'Created By':<20} {'Duration':<10} {'Branch':<15} {'Commit':<10}")
    print("-" * 110)

    for run in runs:
        duration = f"{run.get('duration_minutes', 0)}m" if run.get('duration_minutes') else "N/A"
        commit = run.get('git_commit_hash', 'unknown')[:8] if run.get('git_commit_hash') else 'N/A'

        print(f"{run['id']:<5} {run['run_type'][:29]:<30} {run['status']:<10} {(run.get('created_by') or 'N/A')[:19]:<20} {duration:<10} {(run.get('git_branch') or 'N/A')[:14]:<15} {commit:<10}")

def print_run_details(run_data: Dict[str, Any]) -> None:
    """Print detailed run information."""
    print(f"\n{'='*60}")
    print(f"RUN METADATA - ID {run_data['id']}")
    print(f"{'='*60}")

    # Basic info
    print(f"Type: {run_data.get('run_type', 'N/A')}")
    print(f"Status: {run_data.get('status', 'N/A')}")
    print(f"Created by: {run_data.get('created_by', 'N/A')}")
    print(f"Environment: {run_data.get('environment', 'N/A')}")

    # Timing
    if run_data.get('start_time'):
        print(f"Started: {run_data['start_time']}")
    if run_data.get('end_time'):
        print(f"Ended: {run_data['end_time']}")

    # Git info
    print(f"\nGIT INFORMATION:")
    print(f"  Commit: {run_data.get('git_commit_hash', 'unknown')}")
    print(f"  Branch: {run_data.get('git_branch', 'unknown')}")

    # Command line
    if run_data.get('command_line'):
        print(f"\nCOMMAND LINE:")
        print(f"  {run_data['command_line']}")

    # Host info
    if run_data.get('host_info'):
        host_info = json.loads(run_data['host_info'])
        print(f"\nHOST INFORMATION:")
        print(f"  Hostname: {host_info.get('hostname', 'N/A')}")
        print(f"  Platform: {host_info.get('platform', 'N/A')}")
        print(f"  Python: {host_info.get('python_version', 'N/A')}")
        if host_info.get('container_id'):
            print(f"  Container: {host_info['container_id']}")

    # Parameters
    if run_data.get('parameters'):
        parameters = json.loads(run_data['parameters'])
        if parameters:
            print(f"\nPARAMETERS:")
            for key, value in parameters.items():
                print(f"  {key}: {value}")

    # Results
    if run_data.get('results'):
        results = json.loads(run_data['results'])
        if results:
            print(f"\nRESULTS:")
            for key, value in results.items():
                print(f"  {key}: {value}")

async def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Training run metadata CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument('--environment', '-e', default='dev',
                       choices=['dev', 'intg', 'prod'],
                       help='Environment to query (default: dev)')

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # List command
    list_parser = subparsers.add_parser('list', help='List training runs')
    list_parser.add_argument('--limit', '-l', type=int, default=20,
                           help='Maximum number of runs to show')
    list_parser.add_argument('--run-type', '-t', help='Filter by run type')
    list_parser.add_argument('--status', '-s', help='Filter by status')
    list_parser.add_argument('--since', help='Show runs since (e.g., "1 day", "2 hours")')

    # Show command
    show_parser = subparsers.add_parser('show', help='Show detailed run information')
    show_parser.add_argument('--run-id', '-r', type=int, required=True,
                           help='Run ID to show details for')

    # Commits command
    commits_parser = subparsers.add_parser('commits', help='Show git commit history')
    commits_parser.add_argument('--limit', '-l', type=int, default=10,
                              help='Maximum number of commits to show')

    # Export command
    export_parser = subparsers.add_parser('export', help='Export run metadata to JSON')
    export_parser.add_argument('--run-id', '-r', type=int, required=True,
                             help='Run ID to export')
    export_parser.add_argument('--output', '-o', required=True,
                             help='Output JSON file path')

    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate run reproducibility')
    validate_parser.add_argument('--run-id', '-r', type=int, required=True,
                               help='Run ID to validate')

    # Compare command
    compare_parser = subparsers.add_parser('compare', help='Compare multiple runs')
    compare_parser.add_argument('--run-ids', '-r', required=True,
                              help='Comma-separated list of run IDs to compare')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    cli = RunMetadataCLI(environment=args.environment)

    try:
        if args.command == 'list':
            runs = await cli.list_runs(
                limit=args.limit,
                run_type=args.run_type,
                status=args.status,
                since=args.since
            )
            print_runs_table(runs)

        elif args.command == 'show':
            run_data = await cli.get_run_details(args.run_id)
            if run_data:
                print_run_details(run_data)
            else:
                print(f"Run {args.run_id} not found in {args.environment} environment")

        elif args.command == 'commits':
            commits = await cli.get_commit_history(args.limit)
            if commits:
                print(f"{'Commit':<14} {'Branch':<20} {'First Used':<17} {'Last Used':<17} {'Runs':<5}")
                print("-" * 75)
                for commit in commits:
                    print(f"{commit['git_commit_hash'][:12]:<14} {commit['git_branch'][:19]:<20} {commit['first_used'].strftime('%Y-%m-%d %H:%M'):<17} {commit['last_used'].strftime('%Y-%m-%d %H:%M'):<17} {commit['run_count']:<5}")
            else:
                print("No git commit history found")

        elif args.command == 'export':
            success = await cli.export_metadata(args.run_id, args.output)

        elif args.command == 'validate':
            validation = await cli.validate_reproducibility(args.run_id)
            if 'error' in validation:
                print(f"Error: {validation['error']}")
            else:
                print(f"\nREPRODUCIBILITY VALIDATION - Run {args.run_id}")
                print("=" * 50)
                print(f"Reproducible: {'✅ YES' if validation['reproducible'] else '❌ NO'}")
                print(f"Metadata Completeness: {validation['metadata_completeness']:.1f}%")

                if validation['issues']:
                    print("\nISSUES (Critical):")
                    for issue in validation['issues']:
                        print(f"  ❌ {issue}")

                if validation['warnings']:
                    print("\nWARNINGS:")
                    for warning in validation['warnings']:
                        print(f"  ⚠️ {warning}")

        elif args.command == 'compare':
            run_ids = [int(x.strip()) for x in args.run_ids.split(',')]
            comparison = await cli.compare_runs(run_ids)

            if 'error' in comparison:
                print(f"Error: {comparison['error']}")
            else:
                print(f"\nRUN COMPARISON")
                print("=" * 40)
                print(f"Runs: {', '.join([str(r['id']) for r in comparison['runs']])}")

                if comparison['similarities']:
                    print("\nSIMILARITIES:")
                    for key, value in comparison['similarities'].items():
                        print(f"  {key}: {value}")

                if comparison['differences']:
                    print("\nDIFFERENCES:")
                    for key, values in comparison['differences'].items():
                        print(f"  {key}:")
                        for run_id, value in values.items():
                            print(f"    Run {run_id}: {value}")

    finally:
        await cli.close()

if __name__ == '__main__':
    asyncio.run(main())