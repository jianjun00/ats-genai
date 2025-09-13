"""
Database Schema Migration Manager for Service Architecture

Handles database schema changes required for service-based architecture transformation.
Manages table creation, index optimization, and service-specific schema updates.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import json

import asyncpg
from asyncpg import Connection, Pool

logger = logging.getLogger(__name__)


@dataclass
class SchemaMigration:
    """Database schema migration definition."""
    migration_id: str
    name: str
    description: str
    sql_statements: List[str]
    rollback_statements: List[str]
    dependencies: List[str]
    service_domain: str
    created_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            'migration_id': self.migration_id,
            'name': self.name,
            'description': self.description,
            'sql_statements': self.sql_statements,
            'rollback_statements': self.rollback_statements,
            'dependencies': self.dependencies,
            'service_domain': self.service_domain,
            'created_at': self.created_at.isoformat()
        }


@dataclass
class MigrationStatus:
    """Status of a migration execution."""
    migration_id: str
    status: str  # pending, running, completed, failed, rolled_back
    executed_at: Optional[datetime]
    execution_time_ms: Optional[float]
    error_message: Optional[str]
    applied_statements: List[str]


class DatabaseMigrator:
    """Manages database schema migrations for service architecture."""

    def __init__(
        self,
        database_url: str,
        migration_directory: str = "migrations/service_architecture",
        schema_name: str = "public"
    ):
        self.database_url = database_url
        self.migration_directory = Path(migration_directory)
        self.schema_name = schema_name
        self.pool: Optional[Pool] = None

        # Service-specific migration templates
        self.service_migration_templates = {
            'instruments': self._create_instruments_service_migrations(),
            'market_data': self._create_market_data_service_migrations(),
            'analytics': self._create_analytics_service_migrations(),
            'user_management': self._create_user_service_migrations()
        }

    async def initialize(self):
        """Initialize database connection pool and migration infrastructure."""
        logger.info("Initializing database migrator")

        # Create connection pool
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=2,
            max_size=10,
            command_timeout=60
        )

        # Create migration tracking table
        await self._create_migration_tracking_table()

        # Ensure migration directory exists
        self.migration_directory.mkdir(parents=True, exist_ok=True)

        logger.info("Database migrator initialized successfully")

    async def close(self):
        """Close database connections."""
        if self.pool:
            await self.pool.close()

    async def generate_service_migrations(
        self,
        service_domain: str,
        custom_requirements: Optional[Dict[str, Any]] = None
    ) -> List[SchemaMigration]:
        """Generate database migrations for a specific service domain."""
        logger.info(f"Generating migrations for service domain: {service_domain}")

        migrations = []

        # Get base template for service domain
        if service_domain in self.service_migration_templates:
            template_migrations = self.service_migration_templates[service_domain]
            migrations.extend(template_migrations)

        # Add custom requirements
        if custom_requirements:
            custom_migration = self._create_custom_migration(
                service_domain,
                custom_requirements
            )
            migrations.append(custom_migration)

        # Add service infrastructure migrations
        infrastructure_migration = self._create_service_infrastructure_migration(
            service_domain
        )
        migrations.append(infrastructure_migration)

        logger.info(f"Generated {len(migrations)} migrations for {service_domain}")
        return migrations

    async def apply_migrations(
        self,
        migrations: List[SchemaMigration],
        dry_run: bool = False
    ) -> List[MigrationStatus]:
        """Apply database migrations with comprehensive tracking."""
        logger.info(f"Applying {len(migrations)} migrations (dry_run={dry_run})")

        results = []

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for migration in migrations:
                    status = await self._apply_single_migration(
                        conn, migration, dry_run
                    )
                    results.append(status)

                    # Stop on failure unless it's a dry run
                    if status.status == 'failed' and not dry_run:
                        logger.error(f"Migration {migration.migration_id} failed, stopping")
                        break

        return results

    async def rollback_migration(
        self,
        migration_id: str,
        force: bool = False
    ) -> MigrationStatus:
        """Rollback a specific migration."""
        logger.info(f"Rolling back migration: {migration_id}")

        # Get migration details
        migration = await self._get_migration_by_id(migration_id)
        if not migration:
            raise ValueError(f"Migration {migration_id} not found")

        # Check dependencies
        if not force:
            dependent_migrations = await self._get_dependent_migrations(migration_id)
            if dependent_migrations:
                raise ValueError(
                    f"Cannot rollback {migration_id}. "
                    f"Dependent migrations: {dependent_migrations}"
                )

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                start_time = datetime.now()

                try:
                    # Execute rollback statements
                    for stmt in migration.rollback_statements:
                        logger.debug(f"Executing rollback: {stmt[:100]}...")
                        await conn.execute(stmt)

                    # Update migration status
                    execution_time = (datetime.now() - start_time).total_seconds() * 1000

                    await conn.execute("""
                        UPDATE service_migrations
                        SET status = 'rolled_back',
                            rolled_back_at = $1,
                            execution_time_ms = $2
                        WHERE migration_id = $3
                    """, datetime.now(), execution_time, migration_id)

                    logger.info(f"Successfully rolled back migration: {migration_id}")

                    return MigrationStatus(
                        migration_id=migration_id,
                        status='rolled_back',
                        executed_at=datetime.now(),
                        execution_time_ms=execution_time,
                        error_message=None,
                        applied_statements=migration.rollback_statements
                    )

                except Exception as e:
                    logger.error(f"Failed to rollback migration {migration_id}: {e}")

                    return MigrationStatus(
                        migration_id=migration_id,
                        status='rollback_failed',
                        executed_at=datetime.now(),
                        execution_time_ms=None,
                        error_message=str(e),
                        applied_statements=[]
                    )

    async def get_migration_status(self) -> Dict[str, Any]:
        """Get comprehensive migration status report."""
        async with self.pool.acquire() as conn:
            # Get migration counts by status
            status_counts = await conn.fetch("""
                SELECT status, COUNT(*) as count
                FROM service_migrations
                GROUP BY status
            """)

            # Get recent migrations
            recent_migrations = await conn.fetch("""
                SELECT migration_id, name, status, executed_at, execution_time_ms
                FROM service_migrations
                ORDER BY executed_at DESC
                LIMIT 10
            """)

            # Get service domain statistics
            domain_stats = await conn.fetch("""
                SELECT service_domain, COUNT(*) as migration_count,
                       AVG(execution_time_ms) as avg_execution_time
                FROM service_migrations
                WHERE status = 'completed'
                GROUP BY service_domain
            """)

            return {
                'status_summary': {dict(row) for row in status_counts},
                'recent_migrations': [dict(row) for row in recent_migrations],
                'domain_statistics': [dict(row) for row in domain_stats],
                'total_migrations': sum(row['count'] for row in status_counts)
            }

    async def validate_schema_integrity(self) -> Dict[str, Any]:
        """Validate database schema integrity for service architecture."""
        logger.info("Validating database schema integrity")

        validation_results = {
            'table_checks': [],
            'index_checks': [],
            'constraint_checks': [],
            'service_schema_checks': [],
            'overall_status': 'unknown'
        }

        async with self.pool.acquire() as conn:
            # Check required service tables
            service_tables = [
                'service_migrations', 'service_instances', 'service_health',
                'service_cache_keys', 'service_metrics'
            ]

            for table in service_tables:
                exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = $1 AND table_name = $2
                    )
                """, self.schema_name, table)

                validation_results['table_checks'].append({
                    'table': table,
                    'exists': exists,
                    'status': 'ok' if exists else 'missing'
                })

            # Check service-specific schema requirements
            for domain in self.service_migration_templates.keys():
                domain_status = await self._validate_service_domain_schema(conn, domain)
                validation_results['service_schema_checks'].append(domain_status)

        # Determine overall status
        all_checks = (
            validation_results['table_checks'] +
            validation_results['service_schema_checks']
        )

        if all(check.get('status') == 'ok' for check in all_checks):
            validation_results['overall_status'] = 'healthy'
        elif any(check.get('status') == 'missing' for check in all_checks):
            validation_results['overall_status'] = 'degraded'
        else:
            validation_results['overall_status'] = 'healthy'

        logger.info(f"Schema validation completed: {validation_results['overall_status']}")
        return validation_results

    # Private helper methods

    async def _create_migration_tracking_table(self):
        """Create the migration tracking table."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS service_migrations (
                    id SERIAL PRIMARY KEY,
                    migration_id VARCHAR(100) UNIQUE NOT NULL,
                    name VARCHAR(200) NOT NULL,
                    description TEXT,
                    service_domain VARCHAR(50) NOT NULL,
                    sql_statements JSONB NOT NULL,
                    rollback_statements JSONB NOT NULL,
                    dependencies JSONB DEFAULT '[]',
                    status VARCHAR(20) DEFAULT 'pending',
                    executed_at TIMESTAMP,
                    rolled_back_at TIMESTAMP,
                    execution_time_ms FLOAT,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_service_migrations_status
                ON service_migrations(status)
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_service_migrations_domain
                ON service_migrations(service_domain)
            """)

    async def _apply_single_migration(
        self,
        conn: Connection,
        migration: SchemaMigration,
        dry_run: bool
    ) -> MigrationStatus:
        """Apply a single migration with comprehensive error handling."""
        logger.info(f"Applying migration: {migration.migration_id}")

        start_time = datetime.now()
        applied_statements = []

        try:
            # Check if migration already applied
            existing = await conn.fetchval("""
                SELECT status FROM service_migrations WHERE migration_id = $1
            """, migration.migration_id)

            if existing == 'completed':
                logger.info(f"Migration {migration.migration_id} already applied")
                return MigrationStatus(
                    migration_id=migration.migration_id,
                    status='completed',
                    executed_at=None,
                    execution_time_ms=0,
                    error_message=None,
                    applied_statements=[]
                )

            if not dry_run:
                # Record migration start
                await conn.execute("""
                    INSERT INTO service_migrations
                    (migration_id, name, description, service_domain,
                     sql_statements, rollback_statements, dependencies, status)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, 'running')
                    ON CONFLICT (migration_id) DO UPDATE SET
                        status = 'running',
                        executed_at = CURRENT_TIMESTAMP
                """,
                migration.migration_id, migration.name, migration.description,
                migration.service_domain, json.dumps(migration.sql_statements),
                json.dumps(migration.rollback_statements),
                json.dumps(migration.dependencies))

            # Execute migration statements
            for stmt in migration.sql_statements:
                logger.debug(f"Executing: {stmt[:100]}...")

                if not dry_run:
                    await conn.execute(stmt)

                applied_statements.append(stmt)

            execution_time = (datetime.now() - start_time).total_seconds() * 1000

            if not dry_run:
                # Mark migration as completed
                await conn.execute("""
                    UPDATE service_migrations
                    SET status = 'completed',
                        executed_at = $1,
                        execution_time_ms = $2
                    WHERE migration_id = $3
                """, datetime.now(), execution_time, migration.migration_id)

            logger.info(f"Migration {migration.migration_id} completed successfully")

            return MigrationStatus(
                migration_id=migration.migration_id,
                status='completed' if not dry_run else 'dry_run_success',
                executed_at=datetime.now(),
                execution_time_ms=execution_time,
                error_message=None,
                applied_statements=applied_statements
            )

        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            error_msg = str(e)

            logger.error(f"Migration {migration.migration_id} failed: {error_msg}")

            if not dry_run:
                # Mark migration as failed
                await conn.execute("""
                    UPDATE service_migrations
                    SET status = 'failed',
                        executed_at = $1,
                        execution_time_ms = $2,
                        error_message = $3
                    WHERE migration_id = $4
                """, datetime.now(), execution_time, error_msg, migration.migration_id)

            return MigrationStatus(
                migration_id=migration.migration_id,
                status='failed',
                executed_at=datetime.now(),
                execution_time_ms=execution_time,
                error_message=error_msg,
                applied_statements=applied_statements
            )

    def _create_instruments_service_migrations(self) -> List[SchemaMigration]:
        """Create migrations for instruments service schema."""
        return [
            SchemaMigration(
                migration_id="instruments_001_service_tables",
                name="Create Instruments Service Tables",
                description="Create service-specific tables for instruments domain",
                sql_statements=[
                    """
                    CREATE TABLE IF NOT EXISTS instrument_service_cache (
                        cache_key VARCHAR(255) PRIMARY KEY,
                        cache_value JSONB NOT NULL,
                        expires_at TIMESTAMP NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """,
                    """
                    CREATE INDEX idx_instrument_cache_expires
                    ON instrument_service_cache(expires_at)
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS instrument_service_metrics (
                        id SERIAL PRIMARY KEY,
                        operation_name VARCHAR(100) NOT NULL,
                        execution_time_ms FLOAT NOT NULL,
                        cache_hit BOOLEAN DEFAULT FALSE,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                ],
                rollback_statements=[
                    "DROP TABLE IF EXISTS instrument_service_metrics",
                    "DROP TABLE IF EXISTS instrument_service_cache"
                ],
                dependencies=[],
                service_domain="instruments",
                created_at=datetime.now()
            )
        ]

    def _create_market_data_service_migrations(self) -> List[SchemaMigration]:
        """Create migrations for market data service schema."""
        return [
            SchemaMigration(
                migration_id="market_data_001_service_tables",
                name="Create Market Data Service Tables",
                description="Create service-specific tables for market data domain",
                sql_statements=[
                    """
                    CREATE TABLE IF NOT EXISTS market_data_service_cache (
                        cache_key VARCHAR(255) PRIMARY KEY,
                        cache_value JSONB NOT NULL,
                        expires_at TIMESTAMP NOT NULL,
                        data_type VARCHAR(50) NOT NULL,
                        symbol VARCHAR(20),
                        timeframe VARCHAR(10),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """,
                    """
                    CREATE INDEX idx_market_data_cache_expires
                    ON market_data_service_cache(expires_at)
                    """,
                    """
                    CREATE INDEX idx_market_data_cache_symbol
                    ON market_data_service_cache(symbol, timeframe)
                    """
                ],
                rollback_statements=[
                    "DROP TABLE IF EXISTS market_data_service_cache"
                ],
                dependencies=[],
                service_domain="market_data",
                created_at=datetime.now()
            )
        ]

    def _create_analytics_service_migrations(self) -> List[SchemaMigration]:
        """Create migrations for analytics service schema."""
        return [
            SchemaMigration(
                migration_id="analytics_001_service_tables",
                name="Create Analytics Service Tables",
                description="Create service-specific tables for analytics domain",
                sql_statements=[
                    """
                    CREATE TABLE IF NOT EXISTS analytics_service_jobs (
                        job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        job_type VARCHAR(50) NOT NULL,
                        parameters JSONB NOT NULL,
                        status VARCHAR(20) DEFAULT 'pending',
                        result JSONB,
                        error_message TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        started_at TIMESTAMP,
                        completed_at TIMESTAMP
                    )
                    """,
                    """
                    CREATE INDEX idx_analytics_jobs_status
                    ON analytics_service_jobs(status, created_at)
                    """
                ],
                rollback_statements=[
                    "DROP TABLE IF EXISTS analytics_service_jobs"
                ],
                dependencies=[],
                service_domain="analytics",
                created_at=datetime.now()
            )
        ]

    def _create_user_service_migrations(self) -> List[SchemaMigration]:
        """Create migrations for user management service schema."""
        return [
            SchemaMigration(
                migration_id="users_001_service_tables",
                name="Create User Service Tables",
                description="Create service-specific tables for user management domain",
                sql_statements=[
                    """
                    CREATE TABLE IF NOT EXISTS user_service_sessions (
                        session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        user_id INTEGER NOT NULL,
                        session_data JSONB NOT NULL,
                        expires_at TIMESTAMP NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """,
                    """
                    CREATE INDEX idx_user_sessions_expires
                    ON user_service_sessions(expires_at)
                    """,
                    """
                    CREATE INDEX idx_user_sessions_user
                    ON user_service_sessions(user_id)
                    """
                ],
                rollback_statements=[
                    "DROP TABLE IF EXISTS user_service_sessions"
                ],
                dependencies=[],
                service_domain="user_management",
                created_at=datetime.now()
            )
        ]

    def _create_service_infrastructure_migration(
        self,
        service_domain: str
    ) -> SchemaMigration:
        """Create infrastructure migration for service domain."""
        return SchemaMigration(
            migration_id=f"{service_domain}_infrastructure",
            name=f"Create {service_domain.title()} Service Infrastructure",
            description=f"Create infrastructure tables for {service_domain} service",
            sql_statements=[
                f"""
                CREATE TABLE IF NOT EXISTS {service_domain}_service_health (
                    check_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    service_instance VARCHAR(100) NOT NULL,
                    status VARCHAR(20) NOT NULL,
                    response_time_ms FLOAT,
                    details JSONB,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                f"""
                CREATE INDEX idx_{service_domain}_health_instance
                ON {service_domain}_service_health(service_instance, checked_at)
                """
            ],
            rollback_statements=[
                f"DROP TABLE IF EXISTS {service_domain}_service_health"
            ],
            dependencies=[],
            service_domain=service_domain,
            created_at=datetime.now()
        )

    def _create_custom_migration(
        self,
        service_domain: str,
        requirements: Dict[str, Any]
    ) -> SchemaMigration:
        """Create custom migration based on specific requirements."""
        migration_id = f"{service_domain}_custom_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        sql_statements = []
        rollback_statements = []

        # Generate SQL based on requirements
        if 'tables' in requirements:
            for table_name, table_def in requirements['tables'].items():
                create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
                columns = []

                for col_name, col_def in table_def.get('columns', {}).items():
                    columns.append(f"{col_name} {col_def}")

                create_sql += ", ".join(columns) + ")"
                sql_statements.append(create_sql)
                rollback_statements.append(f"DROP TABLE IF EXISTS {table_name}")

                # Add indexes
                for index in table_def.get('indexes', []):
                    index_sql = f"CREATE INDEX IF NOT EXISTS {index['name']} ON {table_name}({index['columns']})"
                    sql_statements.append(index_sql)

        return SchemaMigration(
            migration_id=migration_id,
            name=f"Custom {service_domain.title()} Migration",
            description=f"Custom migration for {service_domain} based on specific requirements",
            sql_statements=sql_statements,
            rollback_statements=list(reversed(rollback_statements)),
            dependencies=[],
            service_domain=service_domain,
            created_at=datetime.now()
        )

    async def _get_migration_by_id(self, migration_id: str) -> Optional[SchemaMigration]:
        """Get migration details by ID."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM service_migrations WHERE migration_id = $1
            """, migration_id)

            if not row:
                return None

            return SchemaMigration(
                migration_id=row['migration_id'],
                name=row['name'],
                description=row['description'],
                sql_statements=json.loads(row['sql_statements']),
                rollback_statements=json.loads(row['rollback_statements']),
                dependencies=json.loads(row['dependencies']),
                service_domain=row['service_domain'],
                created_at=row['created_at']
            )

    async def _get_dependent_migrations(self, migration_id: str) -> List[str]:
        """Get migrations that depend on the given migration."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT migration_id FROM service_migrations
                WHERE dependencies ? $1 AND status = 'completed'
            """, migration_id)

            return [row['migration_id'] for row in rows]

    async def _validate_service_domain_schema(
        self,
        conn: Connection,
        domain: str
    ) -> Dict[str, Any]:
        """Validate schema for specific service domain."""
        expected_tables = {
            'instruments': ['instrument_service_cache', 'instrument_service_metrics'],
            'market_data': ['market_data_service_cache'],
            'analytics': ['analytics_service_jobs'],
            'user_management': ['user_service_sessions']
        }

        tables_to_check = expected_tables.get(domain, [])
        table_statuses = []

        for table in tables_to_check:
            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )
            """, self.schema_name, table)

            table_statuses.append({
                'table': table,
                'exists': exists,
                'status': 'ok' if exists else 'missing'
            })

        overall_status = 'ok' if all(t['exists'] for t in table_statuses) else 'degraded'

        return {
            'domain': domain,
            'tables': table_statuses,
            'status': overall_status
        }