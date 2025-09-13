"""
Migration Infrastructure Module

Comprehensive migration tools for transforming DAO-based architecture to service-based architecture.
"""

from .migration_orchestrator import (
    MigrationOrchestrator,
    MigrationPlan,
    ComprehensiveMigrationReport
)

from .code_migrator import (
    CodeMigrator,
    MigrationResult
)

from .database_migrator import (
    DatabaseMigrator,
    SchemaMigration,
    MigrationStatus
)

from .config_migrator import (
    ConfigMigrator,
    ConfigMigrationResult
)

from .test_migrator import (
    TestMigrator,
    TestMigrationResult
)

__all__ = [
    # Orchestration
    'MigrationOrchestrator',
    'MigrationPlan',
    'ComprehensiveMigrationReport',

    # Code migration
    'CodeMigrator',
    'MigrationResult',

    # Database migration
    'DatabaseMigrator',
    'SchemaMigration',
    'MigrationStatus',

    # Configuration migration
    'ConfigMigrator',
    'ConfigMigrationResult',

    # Test migration
    'TestMigrator',
    'TestMigrationResult'
]