#!/usr/bin/env python3
"""
Service Architecture Migration CLI

Command-line interface for migrating from DAO-based to service-based architecture.
Provides comprehensive migration orchestration with planning, execution, validation, and rollback capabilities.
"""

import asyncio
import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.infrastructure.migration.migration_orchestrator import (
    MigrationOrchestrator,
    MigrationPlan,
    ComprehensiveMigrationReport
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('migration.log')
    ]
)

logger = logging.getLogger(__name__)


class ServiceMigrationCLI:
    """CLI for service architecture migration."""

    def __init__(self):
        self.orchestrator: Optional[MigrationOrchestrator] = None

    async def initialize_orchestrator(
        self,
        database_url: str,
        source_directory: str = "src",
        config_directory: str = "config",
        test_directory: str = "tests"
    ):
        """Initialize migration orchestrator."""
        self.orchestrator = MigrationOrchestrator(
            source_directory=source_directory,
            database_url=database_url,
            config_directory=config_directory,
            test_directory=test_directory,
            migration_workspace="migrations/workspace",
            enable_rollback=True
        )

        await self.orchestrator.initialize()
        logger.info("Migration orchestrator initialized successfully")

    async def plan_migration(
        self,
        services: Optional[List[str]] = None,
        output_file: Optional[str] = None
    ) -> MigrationPlan:
        """Create comprehensive migration plan."""
        if not self.orchestrator:
            raise RuntimeError("Orchestrator not initialized")

        print("🔍 Analyzing codebase and creating migration plan...")

        migration_plan = await self.orchestrator.create_migration_plan(
            target_services=services
        )

        # Display plan summary
        self._display_migration_plan(migration_plan)

        # Save plan to file if requested
        if output_file:
            plan_data = {
                'migration_id': migration_plan.migration_id,
                'target_services': migration_plan.target_services,
                'phases': migration_plan.phases,
                'estimated_duration_hours': migration_plan.estimated_duration_hours,
                'prerequisites': migration_plan.prerequisites,
                'created_at': migration_plan.created_at.isoformat()
            }

            with open(output_file, 'w') as f:
                json.dump(plan_data, f, indent=2)

            print(f"📄 Migration plan saved to: {output_file}")

        return migration_plan

    async def execute_migration(
        self,
        migration_plan: MigrationPlan,
        dry_run: bool = False,
        continue_on_failure: bool = False,
        confirm: bool = False
    ) -> ComprehensiveMigrationReport:
        """Execute migration plan."""
        if not self.orchestrator:
            raise RuntimeError("Orchestrator not initialized")

        # Confirm execution if not already confirmed
        if not confirm and not dry_run:
            print("\n⚠️  WARNING: This will modify your codebase, database, and configuration files.")
            print("   Make sure you have backups and are in a development environment.")

            response = input("\nProceed with migration? [y/N]: ").lower().strip()
            if response != 'y':
                print("❌ Migration cancelled by user")
                return None

        print(f"\n🚀 {'Executing migration (DRY RUN)' if dry_run else 'Executing migration'}...")
        print(f"   Migration ID: {migration_plan.migration_id}")
        print(f"   Target Services: {', '.join(migration_plan.target_services)}")
        print(f"   Estimated Duration: {migration_plan.estimated_duration_hours:.1f} hours")

        # Execute migration
        migration_report = await self.orchestrator.execute_migration(
            migration_plan=migration_plan,
            dry_run=dry_run,
            continue_on_failure=continue_on_failure
        )

        # Display results
        self._display_migration_results(migration_report)

        # Save report
        report_file = f"migration_report_{migration_plan.migration_id}.json"
        await self._save_migration_report(migration_report, report_file)
        print(f"📊 Migration report saved to: {report_file}")

        return migration_report

    async def validate_migration(
        self,
        migration_id: str
    ) -> Dict[str, Any]:
        """Validate migration results."""
        if not self.orchestrator:
            raise RuntimeError("Orchestrator not initialized")

        print(f"🔍 Validating migration: {migration_id}")

        validation_results = await self.orchestrator.validate_migration(migration_id)

        # Display validation results
        self._display_validation_results(validation_results)

        return validation_results

    async def rollback_migration(
        self,
        migration_id: str,
        target_phase: Optional[str] = None,
        confirm: bool = False
    ) -> Dict[str, Any]:
        """Rollback migration."""
        if not self.orchestrator:
            raise RuntimeError("Orchestrator not initialized")

        # Confirm rollback
        if not confirm:
            print(f"\n⚠️  WARNING: This will rollback migration {migration_id}")
            if target_phase:
                print(f"   Rolling back to phase: {target_phase}")
            else:
                print("   Rolling back completely")

            response = input("\nProceed with rollback? [y/N]: ").lower().strip()
            if response != 'y':
                print("❌ Rollback cancelled by user")
                return None

        print(f"🔄 Rolling back migration: {migration_id}")

        rollback_results = await self.orchestrator.rollback_migration(
            migration_id=migration_id,
            target_phase=target_phase
        )

        # Display rollback results
        self._display_rollback_results(rollback_results)

        return rollback_results

    def get_migration_status(self, migration_id: Optional[str] = None) -> Dict[str, Any]:
        """Get migration status."""
        if not self.orchestrator:
            raise RuntimeError("Orchestrator not initialized")

        status = self.orchestrator.get_migration_status(migration_id)
        self._display_migration_status(status)
        return status

    # Display methods

    def _display_migration_plan(self, migration_plan: MigrationPlan):
        """Display migration plan in a formatted way."""
        print("\n📋 MIGRATION PLAN")
        print("=" * 50)
        print(f"Migration ID: {migration_plan.migration_id}")
        print(f"Target Services: {', '.join(migration_plan.target_services)}")
        print(f"Estimated Duration: {migration_plan.estimated_duration_hours:.1f} hours")
        print(f"Total Phases: {len(migration_plan.phases)}")

        print("\n🔧 Migration Phases:")
        for i, phase in enumerate(migration_plan.phases, 1):
            print(f"  {i:2d}. {phase.replace('_', ' ').title()}")

        print("\n✅ Prerequisites:")
        for prereq in migration_plan.prerequisites:
            print(f"  • {prereq}")

        print("\n📝 Post-Migration Validation:")
        for validation in migration_plan.post_migration_validation:
            print(f"  • {validation}")

    def _display_migration_results(self, migration_report: ComprehensiveMigrationReport):
        """Display migration results."""
        print("\n📊 MIGRATION RESULTS")
        print("=" * 50)

        status_emoji = {
            'completed': '✅',
            'partial': '⚠️',
            'failed': '❌',
            'running': '🔄'
        }

        emoji = status_emoji.get(migration_report.overall_status, '❓')
        print(f"Overall Status: {emoji} {migration_report.overall_status.upper()}")

        if migration_report.duration_minutes:
            print(f"Duration: {migration_report.duration_minutes:.1f} minutes")

        print(f"Phases Completed: {len(migration_report.phases_completed)}")
        print(f"Phases Failed: {len(migration_report.phases_failed)}")

        if migration_report.phases_completed:
            print("\n✅ Completed Phases:")
            for phase in migration_report.phases_completed:
                print(f"  • {phase.replace('_', ' ').title()}")

        if migration_report.phases_failed:
            print("\n❌ Failed Phases:")
            for phase in migration_report.phases_failed:
                print(f"  • {phase.replace('_', ' ').title()}")

        # Migration statistics
        print("\n📈 Migration Statistics:")
        print(f"  Code Migrations: {len(migration_report.code_migration_results)}")
        print(f"  Database Migrations: {len(migration_report.database_migration_results)}")
        print(f"  Config Migrations: {len(migration_report.config_migration_results)}")
        print(f"  Test Migrations: {len(migration_report.test_migration_results)}")

        if migration_report.next_steps:
            print("\n🎯 Next Steps:")
            for step in migration_report.next_steps:
                print(f"  • {step}")

    def _display_validation_results(self, validation_results: Dict[str, Any]):
        """Display validation results."""
        print("\n🔍 VALIDATION RESULTS")
        print("=" * 50)

        status_emoji = {
            'valid': '✅',
            'warning': '⚠️',
            'invalid': '❌',
            'error': '💥'
        }

        overall_status = validation_results.get('overall_status', 'unknown')
        emoji = status_emoji.get(overall_status, '❓')
        print(f"Overall Status: {emoji} {overall_status.upper()}")

        # Component validation status
        components = [
            'code_validation',
            'database_validation',
            'config_validation',
            'test_validation',
            'performance_validation',
            'integration_validation'
        ]

        print("\n📋 Component Validation:")
        for component in components:
            if component in validation_results:
                comp_status = validation_results[component].get('overall_status',
                                                               validation_results[component].get('status', 'unknown'))
                comp_emoji = status_emoji.get(comp_status, '❓')
                comp_name = component.replace('_', ' ').title().replace('Validation', '')
                print(f"  {comp_emoji} {comp_name}: {comp_status}")

        # Issues found
        issues = validation_results.get('issues_found', [])
        if issues:
            print(f"\n⚠️ Issues Found ({len(issues)}):")
            for issue in issues[:10]:  # Show first 10 issues
                print(f"  • {issue}")
            if len(issues) > 10:
                print(f"  ... and {len(issues) - 10} more issues")

        # Recommendations
        recommendations = validation_results.get('recommendations', [])
        if recommendations:
            print("\n💡 Recommendations:")
            for rec in recommendations:
                print(f"  • {rec}")

    def _display_rollback_results(self, rollback_results: Dict[str, Any]):
        """Display rollback results."""
        print("\n🔄 ROLLBACK RESULTS")
        print("=" * 50)

        status = rollback_results.get('rollback_status', 'unknown')
        status_emoji = {
            'completed': '✅',
            'partial': '⚠️',
            'failed': '❌',
            'running': '🔄'
        }

        emoji = status_emoji.get(status, '❓')
        print(f"Rollback Status: {emoji} {status.upper()}")

        phases_rolled_back = rollback_results.get('phases_rolled_back', [])
        if phases_rolled_back:
            print(f"\n✅ Rolled Back Phases ({len(phases_rolled_back)}):")
            for phase in phases_rolled_back:
                print(f"  • {phase.replace('_', ' ').title()}")

        errors = rollback_results.get('errors', [])
        if errors:
            print(f"\n❌ Rollback Errors ({len(errors)}):")
            for error in errors:
                print(f"  • {error}")

    def _display_migration_status(self, status: Dict[str, Any]):
        """Display migration status."""
        print("\n📊 MIGRATION STATUS")
        print("=" * 50)

        migration_status = status.get('status', 'unknown')

        if migration_status == 'no_active_migration':
            print("No active migration")
            return
        elif migration_status == 'migration_not_found':
            print(f"Migration not found: {status.get('migration_id')}")
            return

        status_emoji = {
            'completed': '✅',
            'failed': '❌',
            'running': '🔄'
        }

        emoji = status_emoji.get(migration_status, '❓')
        print(f"Status: {emoji} {migration_status.upper()}")
        print(f"Migration ID: {status.get('migration_id')}")

        if 'total_phases' in status:
            print(f"Progress: {status.get('completed_phases', 0)}/{status.get('total_phases', 0)} phases")

        if 'current_phase' in status and status['current_phase']:
            print(f"Current Phase: {status['current_phase'].replace('_', ' ').title()}")

        if 'failed_phases' in status and status['failed_phases'] > 0:
            print(f"Failed Phases: {status['failed_phases']}")

    async def _save_migration_report(
        self,
        migration_report: ComprehensiveMigrationReport,
        filename: str
    ):
        """Save migration report to file."""
        # Convert to serializable format
        report_data = {
            'migration_id': migration_report.migration_id,
            'overall_status': migration_report.overall_status,
            'start_time': migration_report.start_time.isoformat(),
            'end_time': migration_report.end_time.isoformat() if migration_report.end_time else None,
            'duration_minutes': migration_report.duration_minutes,
            'phases_completed': migration_report.phases_completed,
            'phases_failed': migration_report.phases_failed,
            'rollback_available': migration_report.rollback_available,
            'next_steps': migration_report.next_steps,
            'summary': {
                'code_migrations': len(migration_report.code_migration_results),
                'database_migrations': len(migration_report.database_migration_results),
                'config_migrations': len(migration_report.config_migration_results),
                'test_migrations': len(migration_report.test_migration_results)
            }
        }

        with open(filename, 'w') as f:
            json.dump(report_data, f, indent=2)


async def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Service Architecture Migration CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Plan migration for specific services
  python scripts/migrate_to_services.py plan --services instruments market_data

  # Execute migration (dry run)
  python scripts/migrate_to_services.py execute --dry-run --plan-file migration_plan.json

  # Execute real migration
  python scripts/migrate_to_services.py execute --plan-file migration_plan.json --confirm

  # Validate migration results
  python scripts/migrate_to_services.py validate --migration-id migration_20241201_120000

  # Rollback migration
  python scripts/migrate_to_services.py rollback --migration-id migration_20241201_120000

  # Check migration status
  python scripts/migrate_to_services.py status --migration-id migration_20241201_120000
        """)

    parser.add_argument(
        '--database-url',
        default='postgresql://postgres:dev_password@localhost:5432/dev_db',
        help='Database URL for migration'
    )

    parser.add_argument(
        '--source-dir',
        default='src',
        help='Source code directory'
    )

    parser.add_argument(
        '--config-dir',
        default='config',
        help='Configuration directory'
    )

    parser.add_argument(
        '--test-dir',
        default='tests',
        help='Test directory'
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Plan command
    plan_parser = subparsers.add_parser('plan', help='Create migration plan')
    plan_parser.add_argument(
        '--services',
        nargs='*',
        help='Target services for migration'
    )
    plan_parser.add_argument(
        '--output-file',
        help='Save migration plan to file'
    )

    # Execute command
    execute_parser = subparsers.add_parser('execute', help='Execute migration')
    execute_parser.add_argument(
        '--plan-file',
        help='Load migration plan from file'
    )
    execute_parser.add_argument(
        '--services',
        nargs='*',
        help='Target services (if no plan file)'
    )
    execute_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform dry run without making changes'
    )
    execute_parser.add_argument(
        '--continue-on-failure',
        action='store_true',
        help='Continue migration even if phases fail'
    )
    execute_parser.add_argument(
        '--confirm',
        action='store_true',
        help='Skip confirmation prompt'
    )

    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate migration')
    validate_parser.add_argument(
        '--migration-id',
        required=True,
        help='Migration ID to validate'
    )

    # Rollback command
    rollback_parser = subparsers.add_parser('rollback', help='Rollback migration')
    rollback_parser.add_argument(
        '--migration-id',
        required=True,
        help='Migration ID to rollback'
    )
    rollback_parser.add_argument(
        '--target-phase',
        help='Rollback to specific phase'
    )
    rollback_parser.add_argument(
        '--confirm',
        action='store_true',
        help='Skip confirmation prompt'
    )

    # Status command
    status_parser = subparsers.add_parser('status', help='Get migration status')
    status_parser.add_argument(
        '--migration-id',
        help='Specific migration ID (optional)'
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Initialize CLI
    cli = ServiceMigrationCLI()

    # Initialize orchestrator
    await cli.initialize_orchestrator(
        database_url=args.database_url,
        source_directory=args.source_dir,
        config_directory=args.config_dir,
        test_directory=args.test_dir
    )

    # Execute command
    if args.command == 'plan':
        await cli.plan_migration(
            services=args.services,
            output_file=args.output_file
        )

    elif args.command == 'execute':
        # Load or create migration plan
        if args.plan_file:
            with open(args.plan_file, 'r') as f:
                plan_data = json.load(f)

            migration_plan = MigrationPlan(
                migration_id=plan_data['migration_id'],
                target_services=plan_data['target_services'],
                phases=plan_data['phases'],
                estimated_duration_hours=plan_data['estimated_duration_hours'],
                rollback_plan=plan_data.get('rollback_plan', {}),
                prerequisites=plan_data.get('prerequisites', []),
                post_migration_validation=plan_data.get('post_migration_validation', []),
                created_at=datetime.fromisoformat(plan_data['created_at'])
            )
        else:
            migration_plan = await cli.plan_migration(services=args.services)

        await cli.execute_migration(
            migration_plan=migration_plan,
            dry_run=args.dry_run,
            continue_on_failure=args.continue_on_failure,
            confirm=args.confirm
        )

    elif args.command == 'validate':
        await cli.validate_migration(args.migration_id)

    elif args.command == 'rollback':
        await cli.rollback_migration(
            migration_id=args.migration_id,
            target_phase=args.target_phase,
            confirm=args.confirm
        )

    elif args.command == 'status':
        cli.get_migration_status(args.migration_id)

if __name__ == "__main__":
    asyncio.run(main())