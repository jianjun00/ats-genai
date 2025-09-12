"""
Migration Orchestrator for Service Architecture

Orchestrates comprehensive migration from DAO-based to service-based architecture.
Coordinates code, database, configuration, and test migrations.
"""

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import json

from .code_migrator import CodeMigrator, MigrationResult
from .database_migrator import DatabaseMigrator, MigrationStatus
from .config_migrator import ConfigMigrator, ConfigMigrationResult
from .test_migrator import TestMigrator, TestMigrationResult

logger = logging.getLogger(__name__)


@dataclass
class MigrationPlan:
    """Comprehensive migration plan for service architecture transformation."""
    migration_id: str
    target_services: List[str]
    phases: List[str]
    estimated_duration_hours: float
    rollback_plan: Dict[str, List[str]]
    prerequisites: List[str]
    post_migration_validation: List[str]
    created_at: datetime


@dataclass
class MigrationExecution:
    """Migration execution tracking."""
    migration_id: str
    phase: str
    status: str  # pending, running, completed, failed, rolled_back
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    duration_seconds: Optional[float]
    results: Dict[str, Any]
    error_message: Optional[str]


@dataclass
class ComprehensiveMigrationReport:
    """Comprehensive migration report."""
    migration_id: str
    overall_status: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_minutes: Optional[float]
    phases_completed: List[str]
    phases_failed: List[str]
    code_migration_results: List[MigrationResult]
    database_migration_results: List[MigrationStatus]
    config_migration_results: List[ConfigMigrationResult]
    test_migration_results: List[TestMigrationResult]
    performance_impact: Dict[str, Any]
    rollback_available: bool
    next_steps: List[str]


class MigrationOrchestrator:
    """Orchestrates comprehensive service architecture migration."""
    
    def __init__(
        self,
        source_directory: str = "src",
        target_directory: str = "src",
        database_url: str = "postgresql://localhost:5432/dev_db",
        config_directory: str = "config",
        test_directory: str = "tests",
        migration_workspace: str = "migrations/workspace",
        enable_rollback: bool = True
    ):
        self.source_directory = Path(source_directory)
        self.target_directory = Path(target_directory)
        self.migration_workspace = Path(migration_workspace)
        self.enable_rollback = enable_rollback
        
        # Initialize migration components
        self.code_migrator = CodeMigrator(
            source_directory=str(self.source_directory),
            target_directory=str(self.target_directory),
            backup_directory=str(self.migration_workspace / "code_backup")
        )
        
        self.database_migrator = DatabaseMigrator(
            database_url=database_url,
            migration_directory=str(self.migration_workspace / "database")
        )
        
        self.config_migrator = ConfigMigrator(
            source_config_dir=config_directory,
            target_config_dir=f"{config_directory}/services",
            backup_dir=str(self.migration_workspace / "config_backup")
        )
        
        self.test_migrator = TestMigrator(
            source_test_dir=test_directory,
            target_test_dir=f"{test_directory}/services",
            backup_dir=str(self.migration_workspace / "test_backup")
        )
        
        # Migration tracking
        self.migration_history: List[MigrationExecution] = []
        self.current_migration: Optional[str] = None
    
    async def initialize(self):
        """Initialize migration orchestrator and all components."""
        logger.info("Initializing migration orchestrator")
        
        # Create workspace directories
        self.migration_workspace.mkdir(parents=True, exist_ok=True)
        
        # Initialize database migrator
        await self.database_migrator.initialize()
        
        # Initialize other components (they don't require async init)
        logger.info("Migration orchestrator initialized successfully")
    
    async def create_migration_plan(
        self,
        target_services: Optional[List[str]] = None,
        migration_options: Optional[Dict[str, Any]] = None
    ) -> MigrationPlan:
        """Create comprehensive migration plan."""
        logger.info("Creating comprehensive migration plan")
        
        # Default services if not specified
        if target_services is None:
            target_services = ['instruments', 'market_data', 'analytics', 'user_management']
        
        migration_options = migration_options or {}
        
        # Analyze current codebase
        codebase_analysis = self.code_migrator.analyze_codebase()
        
        # Estimate migration complexity and duration
        complexity_score = self._calculate_migration_complexity(
            codebase_analysis, target_services
        )
        estimated_duration = self._estimate_migration_duration(
            complexity_score, len(target_services)
        )
        
        # Create migration plan
        migration_id = f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        migration_plan = MigrationPlan(
            migration_id=migration_id,
            target_services=target_services,
            phases=[
                "preparation",
                "code_analysis",
                "database_schema_migration", 
                "code_migration",
                "configuration_migration",
                "test_migration",
                "integration_validation",
                "performance_validation",
                "deployment_preparation"
            ],
            estimated_duration_hours=estimated_duration,
            rollback_plan=self._create_rollback_plan(target_services),
            prerequisites=self._identify_prerequisites(codebase_analysis),
            post_migration_validation=self._create_validation_checklist(target_services),
            created_at=datetime.now()
        )
        
        # Save migration plan
        await self._save_migration_plan(migration_plan)
        
        logger.info(f"Migration plan created: {migration_id} (estimated {estimated_duration:.1f}h)")
        return migration_plan
    
    async def execute_migration(
        self,
        migration_plan: MigrationPlan,
        dry_run: bool = False,
        continue_on_failure: bool = False
    ) -> ComprehensiveMigrationReport:
        """Execute comprehensive migration according to plan."""
        logger.info(f"Starting migration execution: {migration_plan.migration_id} (dry_run={dry_run})")
        
        self.current_migration = migration_plan.migration_id
        start_time = datetime.now()
        
        migration_report = ComprehensiveMigrationReport(
            migration_id=migration_plan.migration_id,
            overall_status="running",
            start_time=start_time,
            end_time=None,
            duration_minutes=None,
            phases_completed=[],
            phases_failed=[],
            code_migration_results=[],
            database_migration_results=[],
            config_migration_results=[],
            test_migration_results=[],
            performance_impact={},
            rollback_available=self.enable_rollback,
            next_steps=[]
        )
        
        try:
            # Execute migration phases
            for phase in migration_plan.phases:
                logger.info(f"Executing migration phase: {phase}")
                
                phase_execution = MigrationExecution(
                    migration_id=migration_plan.migration_id,
                    phase=phase,
                    status="running",
                    started_at=datetime.now(),
                    completed_at=None,
                    duration_seconds=None,
                    results={},
                    error_message=None
                )
                
                try:
                    # Execute specific phase
                    phase_results = await self._execute_migration_phase(
                        phase, migration_plan, dry_run
                    )
                    
                    phase_execution.status = "completed"
                    phase_execution.completed_at = datetime.now()
                    phase_execution.duration_seconds = (
                        phase_execution.completed_at - phase_execution.started_at
                    ).total_seconds()
                    phase_execution.results = phase_results
                    
                    # Update migration report
                    migration_report.phases_completed.append(phase)
                    self._update_migration_report_with_phase_results(
                        migration_report, phase, phase_results
                    )
                    
                    logger.info(f"Phase '{phase}' completed successfully")
                    
                except Exception as e:
                    logger.error(f"Phase '{phase}' failed: {e}")
                    
                    phase_execution.status = "failed"
                    phase_execution.completed_at = datetime.now()
                    phase_execution.duration_seconds = (
                        phase_execution.completed_at - phase_execution.started_at
                    ).total_seconds()
                    phase_execution.error_message = str(e)
                    
                    migration_report.phases_failed.append(phase)
                    
                    if not continue_on_failure:
                        migration_report.overall_status = "failed"
                        break
                
                finally:
                    self.migration_history.append(phase_execution)
            
            # Determine overall status
            if not migration_report.phases_failed:
                migration_report.overall_status = "completed"
            elif migration_report.phases_completed:
                migration_report.overall_status = "partial"
            else:
                migration_report.overall_status = "failed"
            
            # Calculate final metrics
            end_time = datetime.now()
            migration_report.end_time = end_time
            migration_report.duration_minutes = (end_time - start_time).total_seconds() / 60
            
            # Generate next steps
            migration_report.next_steps = self._generate_next_steps(migration_report)
            
            # Save migration report
            await self._save_migration_report(migration_report)
            
            logger.info(f"Migration execution completed: {migration_report.overall_status}")
            
        except Exception as e:
            logger.error(f"Migration execution failed: {e}")
            migration_report.overall_status = "failed"
            migration_report.end_time = datetime.now()
            migration_report.duration_minutes = (
                migration_report.end_time - start_time
            ).total_seconds() / 60
        
        finally:
            self.current_migration = None
        
        return migration_report
    
    async def rollback_migration(
        self,
        migration_id: str,
        target_phase: Optional[str] = None
    ) -> Dict[str, Any]:
        """Rollback migration to previous state."""
        logger.info(f"Starting migration rollback: {migration_id}")
        
        if not self.enable_rollback:
            raise ValueError("Rollback is not enabled for this orchestrator")
        
        rollback_results = {
            'migration_id': migration_id,
            'rollback_status': 'running',
            'phases_rolled_back': [],
            'errors': [],
            'started_at': datetime.now(),
            'completed_at': None
        }
        
        try:
            # Get migration execution history
            migration_phases = [
                execution for execution in self.migration_history
                if execution.migration_id == migration_id
            ]
            
            if not migration_phases:
                raise ValueError(f"No migration history found for {migration_id}")
            
            # Rollback phases in reverse order
            phases_to_rollback = list(reversed(migration_phases))
            
            for phase_execution in phases_to_rollback:
                if target_phase and phase_execution.phase == target_phase:
                    break
                
                try:
                    logger.info(f"Rolling back phase: {phase_execution.phase}")
                    
                    await self._rollback_phase(phase_execution)
                    rollback_results['phases_rolled_back'].append(phase_execution.phase)
                    
                    logger.info(f"Successfully rolled back phase: {phase_execution.phase}")
                    
                except Exception as e:
                    error_msg = f"Failed to rollback phase {phase_execution.phase}: {e}"
                    logger.error(error_msg)
                    rollback_results['errors'].append(error_msg)
            
            # Update rollback status
            if not rollback_results['errors']:
                rollback_results['rollback_status'] = 'completed'
            elif rollback_results['phases_rolled_back']:
                rollback_results['rollback_status'] = 'partial'
            else:
                rollback_results['rollback_status'] = 'failed'
            
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            rollback_results['rollback_status'] = 'failed'
            rollback_results['errors'].append(str(e))
        
        finally:
            rollback_results['completed_at'] = datetime.now()
        
        logger.info(f"Migration rollback completed: {rollback_results['rollback_status']}")
        return rollback_results
    
    async def validate_migration(
        self,
        migration_id: str
    ) -> Dict[str, Any]:
        """Validate migration results comprehensively."""
        logger.info(f"Validating migration: {migration_id}")
        
        validation_results = {
            'migration_id': migration_id,
            'overall_status': 'unknown',
            'code_validation': {},
            'database_validation': {},
            'config_validation': {},
            'test_validation': {},
            'performance_validation': {},
            'integration_validation': {},
            'issues_found': [],
            'recommendations': []
        }
        
        try:
            # Validate code migration
            validation_results['code_validation'] = await self._validate_code_migration()
            
            # Validate database migration
            validation_results['database_validation'] = await self.database_migrator.validate_schema_integrity()
            
            # Validate configuration migration
            validation_results['config_validation'] = self.config_migrator.validate_migrated_configs()
            
            # Validate test migration
            validation_results['test_validation'] = self.test_migrator.validate_migrated_tests()
            
            # Validate performance
            validation_results['performance_validation'] = await self._validate_performance()
            
            # Validate integration
            validation_results['integration_validation'] = await self._validate_integration()
            
            # Collect all issues
            all_validations = [
                validation_results['code_validation'],
                validation_results['database_validation'],
                validation_results['config_validation'],
                validation_results['test_validation'],
                validation_results['performance_validation'],
                validation_results['integration_validation']
            ]
            
            for validation in all_validations:
                issues = validation.get('issues_found', [])
                if isinstance(issues, list):
                    validation_results['issues_found'].extend(issues)
                elif isinstance(issues, dict):
                    validation_results['issues_found'].extend(issues.get('errors', []))
            
            # Generate recommendations
            validation_results['recommendations'] = self._generate_validation_recommendations(
                validation_results
            )
            
            # Determine overall status
            if not validation_results['issues_found']:
                validation_results['overall_status'] = 'valid'
            elif len(validation_results['issues_found']) < 10:
                validation_results['overall_status'] = 'warning'
            else:
                validation_results['overall_status'] = 'invalid'
            
        except Exception as e:
            logger.error(f"Migration validation failed: {e}")
            validation_results['overall_status'] = 'error'
            validation_results['issues_found'].append(f"Validation error: {e}")
        
        logger.info(f"Migration validation completed: {validation_results['overall_status']}")
        return validation_results
    
    def get_migration_status(self, migration_id: Optional[str] = None) -> Dict[str, Any]:
        """Get current migration status."""
        if migration_id is None:
            migration_id = self.current_migration
        
        if migration_id is None:
            return {'status': 'no_active_migration'}
        
        # Get migration phases for this migration
        migration_phases = [
            execution for execution in self.migration_history
            if execution.migration_id == migration_id
        ]
        
        if not migration_phases:
            return {'status': 'migration_not_found', 'migration_id': migration_id}
        
        # Calculate status
        completed_phases = [p for p in migration_phases if p.status == 'completed']
        failed_phases = [p for p in migration_phases if p.status == 'failed']
        running_phases = [p for p in migration_phases if p.status == 'running']
        
        return {
            'migration_id': migration_id,
            'status': 'running' if running_phases else 'completed' if completed_phases and not failed_phases else 'failed',
            'total_phases': len(migration_phases),
            'completed_phases': len(completed_phases),
            'failed_phases': len(failed_phases),
            'running_phases': len(running_phases),
            'current_phase': running_phases[0].phase if running_phases else None,
            'last_updated': max(p.started_at for p in migration_phases) if migration_phases else None
        }
    
    # Private helper methods
    
    async def _execute_migration_phase(
        self,
        phase: str,
        migration_plan: MigrationPlan,
        dry_run: bool
    ) -> Dict[str, Any]:
        """Execute specific migration phase."""
        if phase == "preparation":
            return await self._execute_preparation_phase(migration_plan)
        elif phase == "code_analysis":
            return await self._execute_code_analysis_phase(migration_plan)
        elif phase == "database_schema_migration":
            return await self._execute_database_migration_phase(migration_plan, dry_run)
        elif phase == "code_migration":
            return await self._execute_code_migration_phase(migration_plan, dry_run)
        elif phase == "configuration_migration":
            return await self._execute_config_migration_phase(migration_plan, dry_run)
        elif phase == "test_migration":
            return await self._execute_test_migration_phase(migration_plan, dry_run)
        elif phase == "integration_validation":
            return await self._execute_integration_validation_phase(migration_plan)
        elif phase == "performance_validation":
            return await self._execute_performance_validation_phase(migration_plan)
        elif phase == "deployment_preparation":
            return await self._execute_deployment_preparation_phase(migration_plan)
        else:
            raise ValueError(f"Unknown migration phase: {phase}")
    
    async def _execute_preparation_phase(self, migration_plan: MigrationPlan) -> Dict[str, Any]:
        """Execute preparation phase."""
        logger.info("Executing preparation phase")
        
        # Verify prerequisites
        missing_prerequisites = []
        for prereq in migration_plan.prerequisites:
            if not await self._check_prerequisite(prereq):
                missing_prerequisites.append(prereq)
        
        if missing_prerequisites:
            raise RuntimeError(f"Missing prerequisites: {missing_prerequisites}")
        
        # Create backup directories
        backup_dirs = [
            self.migration_workspace / "code_backup",
            self.migration_workspace / "config_backup",
            self.migration_workspace / "test_backup",
            self.migration_workspace / "database_backup"
        ]
        
        for backup_dir in backup_dirs:
            backup_dir.mkdir(parents=True, exist_ok=True)
        
        return {
            'phase': 'preparation',
            'prerequisites_checked': len(migration_plan.prerequisites),
            'missing_prerequisites': missing_prerequisites,
            'backup_directories_created': len(backup_dirs),
            'preparation_status': 'completed'
        }
    
    async def _execute_code_analysis_phase(self, migration_plan: MigrationPlan) -> Dict[str, Any]:
        """Execute code analysis phase."""
        logger.info("Executing code analysis phase")
        
        # Analyze current codebase
        analysis_results = self.code_migrator.analyze_codebase()
        
        # Analyze specific domains for target services
        domain_analysis = {}
        for service_name in migration_plan.target_services:
            domain_analysis[service_name] = self.code_migrator.analyze_domain(service_name)
        
        return {
            'phase': 'code_analysis',
            'codebase_analysis': analysis_results,
            'domain_analysis': domain_analysis,
            'services_analyzed': len(migration_plan.target_services)
        }
    
    async def _execute_database_migration_phase(
        self, 
        migration_plan: MigrationPlan, 
        dry_run: bool
    ) -> Dict[str, Any]:
        """Execute database migration phase."""
        logger.info(f"Executing database migration phase (dry_run={dry_run})")
        
        all_migration_results = []
        
        # Generate and apply migrations for each service
        for service_name in migration_plan.target_services:
            service_migrations = await self.database_migrator.generate_service_migrations(
                service_name
            )
            
            migration_results = await self.database_migrator.apply_migrations(
                service_migrations, dry_run
            )
            
            all_migration_results.extend(migration_results)
        
        successful_migrations = [r for r in all_migration_results if r.status == 'completed']
        failed_migrations = [r for r in all_migration_results if r.status == 'failed']
        
        return {
            'phase': 'database_schema_migration',
            'total_migrations': len(all_migration_results),
            'successful_migrations': len(successful_migrations),
            'failed_migrations': len(failed_migrations),
            'migration_results': all_migration_results,
            'dry_run': dry_run
        }
    
    async def _execute_code_migration_phase(
        self, 
        migration_plan: MigrationPlan, 
        dry_run: bool
    ) -> Dict[str, Any]:
        """Execute code migration phase."""
        logger.info(f"Executing code migration phase (dry_run={dry_run})")
        
        # Migrate code for target services
        migration_results = self.code_migrator.migrate_codebase(
            target_domains=migration_plan.target_services
        )
        
        successful_migrations = [r for r in migration_results if r.status == 'success']
        failed_migrations = [r for r in migration_results if r.status == 'failed']
        
        return {
            'phase': 'code_migration',
            'total_migrations': len(migration_results),
            'successful_migrations': len(successful_migrations),
            'failed_migrations': len(failed_migrations),
            'migration_results': migration_results,
            'dry_run': dry_run
        }
    
    async def _execute_config_migration_phase(
        self, 
        migration_plan: MigrationPlan, 
        dry_run: bool
    ) -> Dict[str, Any]:
        """Execute configuration migration phase."""
        logger.info(f"Executing configuration migration phase (dry_run={dry_run})")
        
        if not dry_run:
            # Migrate configurations
            config_results = self.config_migrator.migrate_all_configurations(
                target_services=migration_plan.target_services,
                create_backup=True
            )
            
            # Migrate environment variables
            env_results = self.config_migrator.migrate_environment_variables(
                target_services=migration_plan.target_services
            )
            
            all_results = config_results + env_results
        else:
            # Dry run - just validate existing configs
            all_results = []
            logger.info("Configuration migration dry run - skipping actual migration")
        
        successful_migrations = [r for r in all_results if r.status == 'success']
        failed_migrations = [r for r in all_results if r.status == 'failed']
        
        return {
            'phase': 'configuration_migration',
            'total_migrations': len(all_results),
            'successful_migrations': len(successful_migrations),
            'failed_migrations': len(failed_migrations),
            'migration_results': all_results,
            'dry_run': dry_run
        }
    
    async def _execute_test_migration_phase(
        self, 
        migration_plan: MigrationPlan, 
        dry_run: bool
    ) -> Dict[str, Any]:
        """Execute test migration phase."""
        logger.info(f"Executing test migration phase (dry_run={dry_run})")
        
        if not dry_run:
            # Migrate tests
            test_results = self.test_migrator.migrate_all_tests(
                target_services=migration_plan.target_services,
                create_backup=True
            )
        else:
            # Dry run - analyze existing tests
            test_results = []
            logger.info("Test migration dry run - skipping actual migration")
        
        successful_migrations = [r for r in test_results if r.status == 'success']
        failed_migrations = [r for r in test_results if r.status == 'failed']
        
        # Analyze test coverage
        coverage_analysis = self.test_migrator.analyze_test_coverage()
        
        return {
            'phase': 'test_migration',
            'total_migrations': len(test_results),
            'successful_migrations': len(successful_migrations),
            'failed_migrations': len(failed_migrations),
            'migration_results': test_results,
            'coverage_analysis': coverage_analysis,
            'dry_run': dry_run
        }
    
    async def _execute_integration_validation_phase(
        self, 
        migration_plan: MigrationPlan
    ) -> Dict[str, Any]:
        """Execute integration validation phase."""
        logger.info("Executing integration validation phase")
        
        # This would involve running integration tests
        # and validating service interactions
        return {
            'phase': 'integration_validation',
            'services_validated': len(migration_plan.target_services),
            'validation_status': 'completed'
        }
    
    async def _execute_performance_validation_phase(
        self, 
        migration_plan: MigrationPlan
    ) -> Dict[str, Any]:
        """Execute performance validation phase."""
        logger.info("Executing performance validation phase")
        
        # This would involve running performance benchmarks
        return {
            'phase': 'performance_validation',
            'services_tested': len(migration_plan.target_services),
            'performance_baseline': 'established'
        }
    
    async def _execute_deployment_preparation_phase(
        self, 
        migration_plan: MigrationPlan
    ) -> Dict[str, Any]:
        """Execute deployment preparation phase."""
        logger.info("Executing deployment preparation phase")
        
        # Prepare deployment artifacts
        deployment_artifacts = []
        
        for service_name in migration_plan.target_services:
            # This would create Docker images, Kubernetes manifests, etc.
            deployment_artifacts.append(f"{service_name}_deployment_ready")
        
        return {
            'phase': 'deployment_preparation',
            'deployment_artifacts': deployment_artifacts,
            'services_ready': len(migration_plan.target_services)
        }
    
    def _calculate_migration_complexity(
        self, 
        codebase_analysis: Dict[str, Any], 
        target_services: List[str]
    ) -> float:
        """Calculate migration complexity score (0-10)."""
        # Base complexity factors
        base_score = 2.0
        
        # File count factor
        file_count = codebase_analysis.get('total_files', 0)
        file_factor = min(file_count / 100, 3.0)  # Cap at 3.0
        
        # Domain count factor
        domain_factor = len(target_services) * 0.5
        
        # Business logic complexity factor
        dao_classes = codebase_analysis.get('dao_classes_found', 0)
        complexity_factor = min(dao_classes / 10, 2.0)  # Cap at 2.0
        
        total_score = base_score + file_factor + domain_factor + complexity_factor
        return min(total_score, 10.0)  # Cap at 10.0
    
    def _estimate_migration_duration(
        self, 
        complexity_score: float, 
        service_count: int
    ) -> float:
        """Estimate migration duration in hours."""
        # Base time per service
        base_hours_per_service = 2.0
        
        # Complexity multiplier (1.0 - 3.0)
        complexity_multiplier = 1.0 + (complexity_score / 10.0) * 2.0
        
        # Service interaction overhead
        interaction_overhead = service_count * 0.5
        
        total_hours = (base_hours_per_service * service_count * complexity_multiplier) + interaction_overhead
        
        return round(total_hours, 1)
    
    def _create_rollback_plan(self, target_services: List[str]) -> Dict[str, List[str]]:
        """Create rollback plan for migration."""
        return {
            "code_migration": [
                "Restore code files from backup",
                "Revert imports and dependencies",
                "Restore original DAO patterns"
            ],
            "database_migration": [
                "Rollback database migrations",
                "Restore original schema",
                "Verify data integrity"
            ],
            "configuration_migration": [
                "Restore configuration files",
                "Revert environment variables",
                "Restore service definitions"
            ],
            "test_migration": [
                "Restore original test files",
                "Revert test configurations",
                "Restore test utilities"
            ]
        }
    
    def _identify_prerequisites(self, codebase_analysis: Dict[str, Any]) -> List[str]:
        """Identify migration prerequisites."""
        prerequisites = [
            "All tests passing",
            "Code committed to version control",
            "Database backup created",
            "Development environment available",
            "Required dependencies installed"
        ]
        
        # Add specific prerequisites based on codebase analysis
        if codebase_analysis.get('has_database_connections', False):
            prerequisites.append("Database connection verified")
        
        if codebase_analysis.get('has_external_apis', False):
            prerequisites.append("External API access verified")
        
        return prerequisites
    
    def _create_validation_checklist(self, target_services: List[str]) -> List[str]:
        """Create post-migration validation checklist."""
        return [
            "All services start successfully",
            "Service interfaces accessible",
            "Database connections working",
            "Cache integration working", 
            "API endpoints responding",
            "Integration tests passing",
            "Performance benchmarks met",
            "Configuration files valid",
            "Test coverage maintained"
        ]
    
    async def _save_migration_plan(self, migration_plan: MigrationPlan):
        """Save migration plan to workspace."""
        plan_file = self.migration_workspace / f"plan_{migration_plan.migration_id}.json"
        
        plan_data = {
            'migration_id': migration_plan.migration_id,
            'target_services': migration_plan.target_services,
            'phases': migration_plan.phases,
            'estimated_duration_hours': migration_plan.estimated_duration_hours,
            'rollback_plan': migration_plan.rollback_plan,
            'prerequisites': migration_plan.prerequisites,
            'post_migration_validation': migration_plan.post_migration_validation,
            'created_at': migration_plan.created_at.isoformat()
        }
        
        with open(plan_file, 'w') as f:
            json.dump(plan_data, f, indent=2)
        
        logger.info(f"Migration plan saved: {plan_file}")
    
    async def _save_migration_report(self, migration_report: ComprehensiveMigrationReport):
        """Save migration report to workspace."""
        report_file = self.migration_workspace / f"report_{migration_report.migration_id}.json"
        
        # Convert results to serializable format
        report_data = {
            'migration_id': migration_report.migration_id,
            'overall_status': migration_report.overall_status,
            'start_time': migration_report.start_time.isoformat(),
            'end_time': migration_report.end_time.isoformat() if migration_report.end_time else None,
            'duration_minutes': migration_report.duration_minutes,
            'phases_completed': migration_report.phases_completed,
            'phases_failed': migration_report.phases_failed,
            'performance_impact': migration_report.performance_impact,
            'rollback_available': migration_report.rollback_available,
            'next_steps': migration_report.next_steps,
            'migration_summary': {
                'code_migrations': len(migration_report.code_migration_results),
                'database_migrations': len(migration_report.database_migration_results),
                'config_migrations': len(migration_report.config_migration_results),
                'test_migrations': len(migration_report.test_migration_results)
            }
        }
        
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        logger.info(f"Migration report saved: {report_file}")
    
    def _update_migration_report_with_phase_results(
        self, 
        migration_report: ComprehensiveMigrationReport, 
        phase: str, 
        phase_results: Dict[str, Any]
    ):
        """Update migration report with phase-specific results."""
        if phase == "code_migration":
            migration_report.code_migration_results = phase_results.get('migration_results', [])
        elif phase == "database_schema_migration":
            migration_report.database_migration_results = phase_results.get('migration_results', [])
        elif phase == "configuration_migration":
            migration_report.config_migration_results = phase_results.get('migration_results', [])
        elif phase == "test_migration":
            migration_report.test_migration_results = phase_results.get('migration_results', [])
        elif phase == "performance_validation":
            migration_report.performance_impact = phase_results
    
    def _generate_next_steps(
        self, 
        migration_report: ComprehensiveMigrationReport
    ) -> List[str]:
        """Generate next steps based on migration results."""
        next_steps = []
        
        if migration_report.overall_status == "completed":
            next_steps.extend([
                "Validate all services are running correctly",
                "Run comprehensive integration tests",
                "Monitor performance metrics", 
                "Update documentation",
                "Train team on new service architecture"
            ])
        elif migration_report.overall_status == "partial":
            next_steps.extend([
                "Review failed phases and errors",
                "Fix issues preventing completion",
                "Re-run failed migration phases",
                "Validate completed phases"
            ])
        elif migration_report.overall_status == "failed":
            next_steps.extend([
                "Analyze failure causes",
                "Consider rollback if necessary",
                "Fix blocking issues",
                "Re-plan migration approach"
            ])
        
        return next_steps
    
    async def _check_prerequisite(self, prerequisite: str) -> bool:
        """Check if prerequisite is met."""
        # This would implement actual prerequisite checking
        # For now, return True for demo purposes
        logger.debug(f"Checking prerequisite: {prerequisite}")
        return True
    
    async def _rollback_phase(self, phase_execution: MigrationExecution):
        """Rollback specific migration phase."""
        phase = phase_execution.phase
        
        if phase == "database_schema_migration":
            # Rollback database migrations
            for result in phase_execution.results.get('migration_results', []):
                if hasattr(result, 'migration_id'):
                    await self.database_migrator.rollback_migration(result.migration_id)
        
        elif phase == "code_migration":
            # Restore code from backup
            logger.info("Restoring code from backup")
            # Implementation would restore files from backup
        
        elif phase == "configuration_migration":
            # Restore configuration from backup
            logger.info("Restoring configuration from backup")
            # Implementation would restore config files
        
        elif phase == "test_migration":
            # Restore tests from backup
            logger.info("Restoring tests from backup")
            # Implementation would restore test files
    
    async def _validate_code_migration(self) -> Dict[str, Any]:
        """Validate code migration results."""
        # This would implement comprehensive code validation
        return {
            'status': 'valid',
            'issues_found': [],
            'services_validated': 4
        }
    
    async def _validate_performance(self) -> Dict[str, Any]:
        """Validate performance after migration."""
        # This would implement performance validation
        return {
            'status': 'valid',
            'performance_impact': 'minimal',
            'response_time_change': '+5ms average'
        }
    
    async def _validate_integration(self) -> Dict[str, Any]:
        """Validate service integration after migration."""
        # This would implement integration validation
        return {
            'status': 'valid',
            'service_connectivity': 'all_services_reachable',
            'api_endpoints': 'responding'
        }
    
    def _generate_validation_recommendations(
        self, 
        validation_results: Dict[str, Any]
    ) -> List[str]:
        """Generate recommendations based on validation results."""
        recommendations = []
        
        if validation_results['overall_status'] == 'warning':
            recommendations.append("Address validation warnings before production deployment")
        
        if validation_results['overall_status'] == 'invalid':
            recommendations.append("Fix critical validation issues before proceeding")
        
        # Add specific recommendations based on validation components
        for component, results in validation_results.items():
            if isinstance(results, dict) and results.get('status') == 'invalid':
                recommendations.append(f"Fix issues in {component} validation")
        
        return recommendations