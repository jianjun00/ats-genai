"""
Code Migration Tool

Automated migration from existing DAO patterns to service-based architecture.
Analyzes existing code and generates service implementations with proper patterns.
"""

import ast
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Set, Optional, Any, Tuple
import logging
import shutil
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class MigrationConfig:
    """Configuration for code migration."""
    source_directory: str
    target_directory: str
    backup_directory: Optional[str] = None
    dry_run: bool = True
    preserve_original: bool = True
    generate_tests: bool = True
    add_caching: bool = True
    add_monitoring: bool = True
    target_patterns: List[str] = field(default_factory=lambda: ['service', 'cached_service'])


@dataclass
class CodeAnalysisResult:
    """Results from code analysis."""
    file_path: str
    class_definitions: List[Dict[str, Any]]
    method_definitions: List[Dict[str, Any]]
    import_statements: List[str]
    dao_usage: List[Dict[str, Any]]
    business_logic_complexity: int
    migration_recommendations: List[str]


@dataclass
class MigrationResult:
    """Results from migration operation."""
    source_file: str
    target_files: List[str]
    success: bool
    warnings: List[str]
    errors: List[str]
    lines_migrated: int
    new_lines_generated: int
    migration_type: str


class ASTAnalyzer:
    """AST-based code analyzer for migration planning."""
    
    def __init__(self):
        self.dao_patterns = [
            'DAO', 'dao', 'Repository', 'repository',
            'DataAccess', 'data_access', 'Mapper', 'mapper'
        ]
        self.service_indicators = [
            'Service', 'service', 'Manager', 'manager',
            'Handler', 'handler', 'Controller', 'controller'
        ]
    
    def analyze_file(self, file_path: str) -> CodeAnalysisResult:
        """Analyze a Python file for migration opportunities."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            # Analyze different aspects
            class_definitions = self._analyze_classes(tree)
            method_definitions = self._analyze_methods(tree)
            import_statements = self._analyze_imports(tree)
            dao_usage = self._analyze_dao_usage(tree, content)
            
            # Calculate complexity
            complexity = self._calculate_complexity(tree)
            
            # Generate recommendations
            recommendations = self._generate_recommendations(
                class_definitions, method_definitions, dao_usage, complexity
            )
            
            return CodeAnalysisResult(
                file_path=file_path,
                class_definitions=class_definitions,
                method_definitions=method_definitions,
                import_statements=import_statements,
                dao_usage=dao_usage,
                business_logic_complexity=complexity,
                migration_recommendations=recommendations
            )
            
        except Exception as e:
            logger.error(f"Error analyzing file {file_path}: {e}")
            return CodeAnalysisResult(
                file_path=file_path,
                class_definitions=[],
                method_definitions=[],
                import_statements=[],
                dao_usage=[],
                business_logic_complexity=0,
                migration_recommendations=[f"Analysis failed: {str(e)}"]
            )
    
    def _analyze_classes(self, tree: ast.AST) -> List[Dict[str, Any]]:
        """Analyze class definitions."""
        classes = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_info = {
                    'name': node.name,
                    'line_number': node.lineno,
                    'base_classes': [self._get_name(base) for base in node.bases],
                    'methods': [n.name for n in node.body if isinstance(n, ast.FunctionDef)],
                    'decorators': [self._get_name(d) for d in node.decorator_list],
                    'is_dao': any(pattern in node.name for pattern in self.dao_patterns),
                    'is_service': any(pattern in node.name for pattern in self.service_indicators)
                }
                classes.append(class_info)
        
        return classes
    
    def _analyze_methods(self, tree: ast.AST) -> List[Dict[str, Any]]:
        """Analyze method definitions."""
        methods = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                method_info = {
                    'name': node.name,
                    'line_number': node.lineno,
                    'args': [arg.arg for arg in node.args.args],
                    'decorators': [self._get_name(d) for d in node.decorator_list],
                    'is_async': isinstance(node, ast.AsyncFunctionDef),
                    'returns_annotation': self._get_name(node.returns) if node.returns else None,
                    'complexity': self._calculate_method_complexity(node)
                }
                methods.append(method_info)
        
        return methods
    
    def _analyze_imports(self, tree: ast.AST) -> List[str]:
        """Analyze import statements."""
        imports = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ''
                for alias in node.names:
                    imports.append(f"{module}.{alias.name}")
        
        return imports
    
    def _analyze_dao_usage(self, tree: ast.AST, content: str) -> List[Dict[str, Any]]:
        """Analyze DAO usage patterns."""
        dao_usage = []
        
        # Pattern-based analysis for DAO calls
        dao_call_patterns = [
            r'\.get\(',
            r'\.create\(',
            r'\.update\(',
            r'\.delete\(',
            r'\.find\(',
            r'\.save\(',
            r'\.fetch\(',
            r'\.execute\('
        ]
        
        lines = content.split('\n')
        for i, line in enumerate(lines):
            for pattern in dao_call_patterns:
                if re.search(pattern, line):
                    dao_usage.append({
                        'line_number': i + 1,
                        'line_content': line.strip(),
                        'pattern': pattern,
                        'method_type': self._infer_method_type(pattern)
                    })
        
        return dao_usage
    
    def _calculate_complexity(self, tree: ast.AST) -> int:
        """Calculate cyclomatic complexity."""
        complexity = 1  # Base complexity
        
        for node in ast.walk(tree):
            if isinstance(node, (ast.If, ast.While, ast.For, ast.AsyncFor)):
                complexity += 1
            elif isinstance(node, ast.Try):
                complexity += len(node.handlers)
            elif isinstance(node, ast.BoolOp):
                complexity += len(node.values) - 1
        
        return complexity
    
    def _calculate_method_complexity(self, method_node: ast.FunctionDef) -> int:
        """Calculate complexity for a specific method."""
        complexity = 1
        
        for node in ast.walk(method_node):
            if isinstance(node, (ast.If, ast.While, ast.For, ast.AsyncFor)):
                complexity += 1
            elif isinstance(node, ast.Try):
                complexity += len(node.handlers)
        
        return complexity
    
    def _get_name(self, node) -> str:
        """Extract name from AST node."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return f"{self._get_name(node.value)}.{node.attr}"
        elif isinstance(node, ast.Constant):
            return str(node.value)
        else:
            return str(node)
    
    def _infer_method_type(self, pattern: str) -> str:
        """Infer CRUD operation type from pattern."""
        if 'get' in pattern or 'find' in pattern or 'fetch' in pattern:
            return 'READ'
        elif 'create' in pattern or 'save' in pattern:
            return 'CREATE'
        elif 'update' in pattern:
            return 'UPDATE'
        elif 'delete' in pattern:
            return 'DELETE'
        else:
            return 'UNKNOWN'
    
    def _generate_recommendations(
        self,
        classes: List[Dict[str, Any]],
        methods: List[Dict[str, Any]],
        dao_usage: List[Dict[str, Any]],
        complexity: int
    ) -> List[str]:
        """Generate migration recommendations."""
        recommendations = []
        
        # Check for DAO classes
        dao_classes = [c for c in classes if c['is_dao']]
        if dao_classes:
            recommendations.append(
                f"Found {len(dao_classes)} DAO class(es). Consider migrating to service pattern."
            )
        
        # Check for complex methods
        complex_methods = [m for m in methods if m['complexity'] > 10]
        if complex_methods:
            recommendations.append(
                f"Found {len(complex_methods)} complex method(s). Consider breaking down into smaller methods."
            )
        
        # Check for async patterns
        async_methods = [m for m in methods if m['is_async']]
        sync_methods = [m for m in methods if not m['is_async']]
        if len(sync_methods) > len(async_methods):
            recommendations.append(
                "Consider converting synchronous methods to async for better performance."
            )
        
        # Check for DAO usage
        if dao_usage:
            crud_operations = {}
            for usage in dao_usage:
                op_type = usage['method_type']
                crud_operations[op_type] = crud_operations.get(op_type, 0) + 1
            
            recommendations.append(
                f"Found CRUD operations: {crud_operations}. Consider service abstraction."
            )
        
        # Overall complexity recommendation
        if complexity > 20:
            recommendations.append(
                f"High complexity ({complexity}). Consider refactoring for maintainability."
            )
        
        return recommendations


class ServiceCodeGenerator:
    """Generates service-based code from analysis results."""
    
    def __init__(self, config: MigrationConfig):
        self.config = config
    
    def generate_service_interface(
        self,
        analysis: CodeAnalysisResult,
        domain_name: str
    ) -> str:
        """Generate service interface from analysis."""
        
        class_name = f"{domain_name.title()}ServiceInterface"
        
        # Extract CRUD operations from DAO usage
        crud_ops = self._extract_crud_operations(analysis)
        
        interface_code = f'''"""
{domain_name.title()} Service Interface

Auto-generated service interface from existing DAO patterns.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from datetime import datetime

from src.domains.{domain_name.lower()}.dtos.{domain_name.lower()}_dtos import (
    {domain_name.title()}DTO,
    SearchCriteria,
    OperationResult,
    BulkOperationResult
)


class {class_name}(ABC):
    """Service interface for {domain_name.lower()} operations."""
    
    # === CRUD Operations ===
    
    @abstractmethod
    async def create_{domain_name.lower()}(self, dto: {domain_name.title()}DTO) -> OperationResult:
        """Create new {domain_name.lower()}."""
        pass
    
    @abstractmethod
    async def get_{domain_name.lower()}_by_id(self, {domain_name.lower()}_id: int) -> Optional[{domain_name.title()}DTO]:
        """Get {domain_name.lower()} by ID."""
        pass
    
    @abstractmethod
    async def update_{domain_name.lower()}(self, dto: {domain_name.title()}DTO) -> OperationResult:
        """Update existing {domain_name.lower()}."""
        pass
    
    @abstractmethod
    async def delete_{domain_name.lower()}(self, {domain_name.lower()}_id: int) -> OperationResult:
        """Delete {domain_name.lower()}."""
        pass
    
    # === Search Operations ===
    
    @abstractmethod
    async def list_{domain_name.lower()}s(self, criteria: SearchCriteria) -> List[{domain_name.title()}DTO]:
        """List {domain_name.lower()}s based on criteria."""
        pass
    
    @abstractmethod
    async def search_{domain_name.lower()}s(self, query: str, criteria: SearchCriteria) -> List[{domain_name.title()}DTO]:
        """Search {domain_name.lower()}s by query."""
        pass
    
    @abstractmethod
    async def count_{domain_name.lower()}s(self, criteria: SearchCriteria) -> int:
        """Count {domain_name.lower()}s matching criteria."""
        pass
    
    # === Batch Operations ===
    
    @abstractmethod
    async def create_{domain_name.lower()}s_batch(self, dtos: List[{domain_name.title()}DTO]) -> BulkOperationResult:
        """Create multiple {domain_name.lower()}s."""
        pass
    
    @abstractmethod
    async def update_{domain_name.lower()}s_batch(self, dtos: List[{domain_name.title()}DTO]) -> BulkOperationResult:
        """Update multiple {domain_name.lower()}s."""
        pass
'''
        
        # Add domain-specific methods from analysis
        if crud_ops:
            interface_code += "\n    # === Domain-Specific Operations ===\n\n"
            for op in crud_ops:
                interface_code += self._generate_domain_method(op, domain_name)
        
        interface_code += '''
    # === Utility Operations ===
    
    @abstractmethod
    async def validate_{domain_name.lower()}_data(self, dto: {domain_name.title()}DTO) -> OperationResult:
        """Validate {domain_name.lower()} data."""
        pass
    
    @abstractmethod
    async def get_{domain_name.lower()}_metadata(self) -> Dict[str, Any]:
        """Get {domain_name.lower()} metadata."""
        pass
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check."""
        pass
'''.format(domain_name=domain_name.lower())
        
        return interface_code
    
    def generate_service_implementation(
        self,
        analysis: CodeAnalysisResult,
        domain_name: str
    ) -> str:
        """Generate service implementation from analysis."""
        
        class_name = f"{domain_name.title()}ServiceImpl"
        interface_name = f"{domain_name.title()}ServiceInterface"
        
        # Determine DAO dependencies from analysis
        dao_dependencies = self._extract_dao_dependencies(analysis)
        
        impl_code = f'''"""
{domain_name.title()} Service Implementation

Auto-generated service implementation with business logic and DAO coordination.
Migrated from existing DAO patterns.
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any

from .interfaces.{domain_name.lower()}_service_interface import {interface_name}
from src.domains.{domain_name.lower()}.dtos.{domain_name.lower()}_dtos import (
    {domain_name.title()}DTO,
    SearchCriteria,
    OperationResult,
    BulkOperationResult
)

logger = logging.getLogger(__name__)


class {class_name}({interface_name}):
    """Service implementation for {domain_name.lower()} operations."""
    
    def __init__(self{self._generate_dao_params(dao_dependencies)}):
        """Initialize service with DAO dependencies."""
{self._generate_dao_assignments(dao_dependencies)}
    
    # === CRUD Operations ===
    
    async def create_{domain_name.lower()}(self, dto: {domain_name.title()}DTO) -> OperationResult:
        """Create new {domain_name.lower()}."""
        try:
            logger.info(f"Creating {domain_name.lower()}: {{dto.name if hasattr(dto, 'name') else 'N/A'}}")
            
            # Business validation
            validation_result = await self.validate_{domain_name.lower()}_data(dto)
            if not validation_result.success:
                return validation_result
            
            # Convert DTO to entity
            entity = self._dto_to_entity(dto)
            
            # Create via DAO
            created_entity = await self.{domain_name.lower()}_dao.create(entity)
            
            # Convert back to DTO
            result_dto = self._entity_to_dto(created_entity)
            
            return OperationResult(
                success=True,
                message=f"{domain_name.title()} created successfully",
                data=result_dto
            )
            
        except Exception as e:
            logger.error(f"Error creating {domain_name.lower()}: {{e}}")
            return OperationResult(
                success=False,
                error_message=f"Failed to create {domain_name.lower()}: {{str(e)}}"
            )
    
    async def get_{domain_name.lower()}_by_id(self, {domain_name.lower()}_id: int) -> Optional[{domain_name.title()}DTO]:
        """Get {domain_name.lower()} by ID."""
        try:
            logger.debug(f"Retrieving {domain_name.lower()} with ID: {{{domain_name.lower()}_id}}")
            
            entity = await self.{domain_name.lower()}_dao.get_by_id({domain_name.lower()}_id)
            if not entity:
                return None
            
            return self._entity_to_dto(entity)
            
        except Exception as e:
            logger.error(f"Error retrieving {domain_name.lower()} {{{domain_name.lower()}_id}}: {{e}}")
            return None
    
    async def update_{domain_name.lower()}(self, dto: {domain_name.title()}DTO) -> OperationResult:
        """Update existing {domain_name.lower()}."""
        try:
            logger.info(f"Updating {domain_name.lower()}: {{dto.id}}")
            
            # Validate DTO
            validation_result = await self.validate_{domain_name.lower()}_data(dto)
            if not validation_result.success:
                return validation_result
            
            # Check if exists
            existing = await self.get_{domain_name.lower()}_by_id(dto.id)
            if not existing:
                return OperationResult(
                    success=False,
                    error_message=f"{domain_name.title()} not found: {{dto.id}}"
                )
            
            # Convert and update
            entity = self._dto_to_entity(dto)
            updated_entity = await self.{domain_name.lower()}_dao.update(entity)
            
            return OperationResult(
                success=True,
                message=f"{domain_name.title()} updated successfully",
                data=self._entity_to_dto(updated_entity)
            )
            
        except Exception as e:
            logger.error(f"Error updating {domain_name.lower()}: {{e}}")
            return OperationResult(
                success=False,
                error_message=f"Failed to update {domain_name.lower()}: {{str(e)}}"
            )
    
    async def delete_{domain_name.lower()}(self, {domain_name.lower()}_id: int) -> OperationResult:
        """Delete {domain_name.lower()}."""
        try:
            logger.info(f"Deleting {domain_name.lower()}: {{{domain_name.lower()}_id}}")
            
            # Check if exists
            existing = await self.get_{domain_name.lower()}_by_id({domain_name.lower()}_id)
            if not existing:
                return OperationResult(
                    success=False,
                    error_message=f"{domain_name.title()} not found: {{{domain_name.lower()}_id}}"
                )
            
            # Delete via DAO
            success = await self.{domain_name.lower()}_dao.delete({domain_name.lower()}_id)
            
            if success:
                return OperationResult(
                    success=True,
                    message=f"{domain_name.title()} deleted successfully"
                )
            else:
                return OperationResult(
                    success=False,
                    error_message=f"Failed to delete {domain_name.lower()}: {{{domain_name.lower()}_id}}"
                )
                
        except Exception as e:
            logger.error(f"Error deleting {domain_name.lower()} {{{domain_name.lower()}_id}}: {{e}}")
            return OperationResult(
                success=False,
                error_message=f"Failed to delete {domain_name.lower()}: {{str(e)}}"
            )
'''
        
        # Add remaining methods (truncated for brevity)
        impl_code += self._generate_remaining_methods(domain_name, dao_dependencies)
        
        return impl_code
    
    def generate_cached_service(
        self,
        analysis: CodeAnalysisResult,
        domain_name: str
    ) -> str:
        """Generate cached service wrapper."""
        
        if not self.config.add_caching:
            return ""
        
        cached_class_name = f"Cached{domain_name.title()}Service"
        interface_name = f"{domain_name.title()}ServiceInterface"
        
        cached_code = f'''"""
Cached {domain_name.title()} Service

Auto-generated cached service wrapper with intelligent caching strategies.
"""

from typing import List, Optional, Dict, Any
import logging

from src.infrastructure.caching import cached, MultiLayerCache, CacheInvalidationManager
from .interfaces.{domain_name.lower()}_service_interface import {interface_name}
from .impl.{domain_name.lower()}_service_impl import {domain_name.title()}ServiceImpl

logger = logging.getLogger(__name__)


class {cached_class_name}({interface_name}):
    """Cached wrapper for {domain_name.lower()} service operations."""
    
    def __init__(
        self,
        base_service: {domain_name.title()}ServiceImpl,
        cache: MultiLayerCache
    ):
        self.base_service = base_service
        self.cache = cache
        self.invalidation_manager = CacheInvalidationManager(cache)
    
    @cached(ttl=3600, cache_name="{domain_name.lower()}")
    async def get_{domain_name.lower()}_by_id(self, {domain_name.lower()}_id: int):
        """Get {domain_name.lower()} by ID with caching."""
        return await self.base_service.get_{domain_name.lower()}_by_id({domain_name.lower()}_id)
    
    async def create_{domain_name.lower()}(self, dto):
        """Create {domain_name.lower()} and invalidate caches."""
        result = await self.base_service.create_{domain_name.lower()}(dto)
        
        if result.success:
            # Invalidate relevant caches
            await self._invalidate_{domain_name.lower()}_caches()
        
        return result
    
    async def _invalidate_{domain_name.lower()}_caches(self):
        """Invalidate {domain_name.lower()}-related caches."""
        await self.invalidation_manager.invalidate_by_tag("{domain_name.lower()}s")
    
    # Delegate other methods to base service
    async def update_{domain_name.lower()}(self, dto):
        result = await self.base_service.update_{domain_name.lower()}(dto)
        if result.success:
            await self._invalidate_{domain_name.lower()}_caches()
        return result
    
    async def delete_{domain_name.lower()}(self, {domain_name.lower()}_id):
        result = await self.base_service.delete_{domain_name.lower()}({domain_name.lower()}_id)
        if result.success:
            await self._invalidate_{domain_name.lower()}_caches()
        return result
'''
        
        return cached_code
    
    def _extract_crud_operations(self, analysis: CodeAnalysisResult) -> List[Dict[str, Any]]:
        """Extract CRUD operations from analysis."""
        operations = []
        
        for usage in analysis.dao_usage:
            if usage['method_type'] in ['CREATE', 'READ', 'UPDATE', 'DELETE']:
                operations.append({
                    'type': usage['method_type'],
                    'pattern': usage['pattern'],
                    'line': usage['line_content']
                })
        
        return operations
    
    def _extract_dao_dependencies(self, analysis: CodeAnalysisResult) -> List[str]:
        """Extract DAO dependencies from analysis."""
        dependencies = set()
        
        for class_def in analysis.class_definitions:
            if class_def['is_dao']:
                dependencies.add(class_def['name'].lower())
        
        # Add default DAO if none found
        if not dependencies:
            dependencies.add('main_dao')
        
        return list(dependencies)
    
    def _generate_dao_params(self, dependencies: List[str]) -> str:
        """Generate DAO constructor parameters."""
        if not dependencies:
            return ""
        
        params = []
        for dep in dependencies:
            params.append(f"{dep}: Any")
        
        return ", " + ", ".join(params)
    
    def _generate_dao_assignments(self, dependencies: List[str]) -> str:
        """Generate DAO instance assignments."""
        assignments = []
        for dep in dependencies:
            assignments.append(f"        self.{dep} = {dep}")
        
        return "\n".join(assignments)
    
    def _generate_domain_method(self, operation: Dict[str, Any], domain_name: str) -> str:
        """Generate domain-specific method from operation."""
        method_name = f"perform_{operation['type'].lower()}_operation"
        
        return f'''    @abstractmethod
    async def {method_name}(self, params: Dict[str, Any]) -> OperationResult:
        """Perform {operation['type'].lower()} operation."""
        pass

'''
    
    def _generate_remaining_methods(self, domain_name: str, dao_dependencies: List[str]) -> str:
        """Generate remaining service methods."""
        
        return f'''
    # === Search Operations ===
    
    async def list_{domain_name.lower()}s(self, criteria: SearchCriteria) -> List[{domain_name.title()}DTO]:
        """List {domain_name.lower()}s based on criteria."""
        try:
            entities = await self.{domain_name.lower()}_dao.find_by_criteria(criteria)
            return [self._entity_to_dto(entity) for entity in entities]
        except Exception as e:
            logger.error(f"Error listing {domain_name.lower()}s: {{e}}")
            return []
    
    async def search_{domain_name.lower()}s(self, query: str, criteria: SearchCriteria) -> List[{domain_name.title()}DTO]:
        """Search {domain_name.lower()}s by query."""
        try:
            entities = await self.{domain_name.lower()}_dao.search(query, criteria)
            return [self._entity_to_dto(entity) for entity in entities]
        except Exception as e:
            logger.error(f"Error searching {domain_name.lower()}s: {{e}}")
            return []
    
    # === Utility Methods ===
    
    async def validate_{domain_name.lower()}_data(self, dto: {domain_name.title()}DTO) -> OperationResult:
        """Validate {domain_name.lower()} data."""
        # Add validation logic here
        return OperationResult(success=True, message="Validation passed")
    
    def _dto_to_entity(self, dto: {domain_name.title()}DTO) -> Any:
        """Convert DTO to entity."""
        # Implement conversion logic
        return dto
    
    def _entity_to_dto(self, entity: Any) -> {domain_name.title()}DTO:
        """Convert entity to DTO."""
        # Implement conversion logic
        return entity
    
    async def get_{domain_name.lower()}_metadata(self) -> Dict[str, Any]:
        """Get {domain_name.lower()} metadata."""
        return {{
            "service": "{domain_name.title()}ServiceImpl",
            "version": "1.0.0",
            "generated": "{datetime.utcnow().isoformat()}",
            "dao_dependencies": {dao_dependencies}
        }}
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check."""
        return {{
            "status": "healthy",
            "service": "{domain_name.title()}ServiceImpl",
            "timestamp": "{datetime.utcnow().isoformat()}"
        }}
'''


class CodeMigrator:
    """Main migration orchestrator."""
    
    def __init__(self, config: MigrationConfig):
        self.config = config
        self.analyzer = ASTAnalyzer()
        self.generator = ServiceCodeGenerator(config)
        self.migration_results: List[MigrationResult] = []
    
    def migrate_codebase(self, target_domains: Optional[List[str]] = None) -> List[MigrationResult]:
        """Migrate entire codebase to service-based architecture."""
        logger.info(f"Starting codebase migration from {self.config.source_directory}")
        
        # Create backup if requested
        if self.config.preserve_original and self.config.backup_directory:
            self._create_backup()
        
        # Discover files to migrate
        python_files = self._discover_python_files()
        logger.info(f"Found {len(python_files)} Python files to analyze")
        
        # Analyze and migrate each file
        for file_path in python_files:
            try:
                result = self._migrate_file(file_path)
                self.migration_results.append(result)
                
                if result.success:
                    logger.info(f"Successfully migrated {file_path}")
                else:
                    logger.warning(f"Migration issues for {file_path}: {result.errors}")
                    
            except Exception as e:
                logger.error(f"Failed to migrate {file_path}: {e}")
                self.migration_results.append(MigrationResult(
                    source_file=file_path,
                    target_files=[],
                    success=False,
                    warnings=[],
                    errors=[str(e)],
                    lines_migrated=0,
                    new_lines_generated=0,
                    migration_type="ERROR"
                ))
        
        # Generate migration report
        self._generate_migration_report()
        
        return self.migration_results
    
    def _discover_python_files(self) -> List[str]:
        """Discover Python files in source directory."""
        python_files = []
        source_path = Path(self.config.source_directory)
        
        if source_path.is_file() and source_path.suffix == '.py':
            python_files.append(str(source_path))
        else:
            for file_path in source_path.rglob('*.py'):
                if not self._should_skip_file(str(file_path)):
                    python_files.append(str(file_path))
        
        return python_files
    
    def _should_skip_file(self, file_path: str) -> bool:
        """Determine if file should be skipped."""
        skip_patterns = [
            '__pycache__',
            '.git',
            '.pytest_cache',
            'test_',
            '_test.py',
            'conftest.py',
            '__init__.py'
        ]
        
        return any(pattern in file_path for pattern in skip_patterns)
    
    def _migrate_file(self, file_path: str) -> MigrationResult:
        """Migrate a single file."""
        logger.debug(f"Analyzing file: {file_path}")
        
        # Analyze the file
        analysis = self.analyzer.analyze_file(file_path)
        
        if not analysis.class_definitions:
            return MigrationResult(
                source_file=file_path,
                target_files=[],
                success=True,
                warnings=["No classes found to migrate"],
                errors=[],
                lines_migrated=0,
                new_lines_generated=0,
                migration_type="SKIP"
            )
        
        # Determine domain name from file path or class name
        domain_name = self._extract_domain_name(file_path, analysis)
        
        target_files = []
        warnings = []
        errors = []
        total_new_lines = 0
        
        try:
            # Generate service interface
            if 'service' in self.config.target_patterns:
                interface_code = self.generator.generate_service_interface(analysis, domain_name)
                interface_file = self._write_generated_code(
                    interface_code,
                    f"{domain_name.lower()}_service_interface.py",
                    "interfaces"
                )
                target_files.append(interface_file)
                total_new_lines += len(interface_code.split('\n'))
            
            # Generate service implementation
            if 'service' in self.config.target_patterns:
                impl_code = self.generator.generate_service_implementation(analysis, domain_name)
                impl_file = self._write_generated_code(
                    impl_code,
                    f"{domain_name.lower()}_service_impl.py",
                    "impl"
                )
                target_files.append(impl_file)
                total_new_lines += len(impl_code.split('\n'))
            
            # Generate cached service
            if 'cached_service' in self.config.target_patterns and self.config.add_caching:
                cached_code = self.generator.generate_cached_service(analysis, domain_name)
                if cached_code:
                    cached_file = self._write_generated_code(
                        cached_code,
                        f"{domain_name.lower()}_service_cached.py",
                        "impl"
                    )
                    target_files.append(cached_file)
                    total_new_lines += len(cached_code.split('\n'))
            
            # Add warnings from analysis
            warnings.extend(analysis.migration_recommendations)
            
            return MigrationResult(
                source_file=file_path,
                target_files=target_files,
                success=True,
                warnings=warnings,
                errors=errors,
                lines_migrated=self._count_file_lines(file_path),
                new_lines_generated=total_new_lines,
                migration_type="SERVICE_GENERATION"
            )
            
        except Exception as e:
            errors.append(str(e))
            return MigrationResult(
                source_file=file_path,
                target_files=target_files,
                success=False,
                warnings=warnings,
                errors=errors,
                lines_migrated=0,
                new_lines_generated=total_new_lines,
                migration_type="ERROR"
            )
    
    def _extract_domain_name(self, file_path: str, analysis: CodeAnalysisResult) -> str:
        """Extract domain name from file path or analysis."""
        # Try to extract from file path
        path_parts = Path(file_path).parts
        
        # Look for domain indicators in path
        domain_indicators = ['domains', 'models', 'entities', 'dao', 'repositories']
        for i, part in enumerate(path_parts):
            if part in domain_indicators and i + 1 < len(path_parts):
                return path_parts[i + 1]
        
        # Try to extract from class names
        for class_def in analysis.class_definitions:
            class_name = class_def['name']
            if any(pattern in class_name.lower() for pattern in ['dao', 'repository', 'model', 'entity']):
                # Remove suffix to get domain name
                domain_name = re.sub(r'(DAO|Repository|Model|Entity)$', '', class_name)
                if domain_name:
                    return domain_name.lower()
        
        # Default to file name
        return Path(file_path).stem
    
    def _write_generated_code(self, code: str, filename: str, subdirectory: str) -> str:
        """Write generated code to file."""
        if self.config.dry_run:
            logger.info(f"[DRY RUN] Would write: {filename}")
            return f"[DRY_RUN]/{subdirectory}/{filename}"
        
        # Create target directory structure
        target_dir = Path(self.config.target_directory) / subdirectory
        target_dir.mkdir(parents=True, exist_ok=True)
        
        target_file = target_dir / filename
        
        with open(target_file, 'w', encoding='utf-8') as f:
            f.write(code)
        
        logger.debug(f"Generated: {target_file}")
        return str(target_file)
    
    def _count_file_lines(self, file_path: str) -> int:
        """Count lines in a file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return len(f.readlines())
        except Exception:
            return 0
    
    def _create_backup(self):
        """Create backup of source directory."""
        if not self.config.backup_directory:
            return
        
        backup_path = Path(self.config.backup_directory)
        source_path = Path(self.config.source_directory)
        
        logger.info(f"Creating backup: {backup_path}")
        
        if backup_path.exists():
            shutil.rmtree(backup_path)
        
        shutil.copytree(source_path, backup_path)
        logger.info("Backup created successfully")
    
    def _generate_migration_report(self):
        """Generate comprehensive migration report."""
        report_file = Path(self.config.target_directory) / "migration_report.md"
        
        total_files = len(self.migration_results)
        successful = len([r for r in self.migration_results if r.success])
        failed = total_files - successful
        
        total_lines_migrated = sum(r.lines_migrated for r in self.migration_results)
        total_new_lines = sum(r.new_lines_generated for r in self.migration_results)
        
        report_content = f"""# Code Migration Report

Generated on: {datetime.utcnow().isoformat()}

## Summary

- **Total Files Processed**: {total_files}
- **Successfully Migrated**: {successful}
- **Failed Migrations**: {failed}
- **Success Rate**: {(successful/total_files*100):.1f}%
- **Lines Migrated**: {total_lines_migrated:,}
- **New Lines Generated**: {total_new_lines:,}

## Migration Results

"""
        
        for result in self.migration_results:
            status_icon = "✅" if result.success else "❌"
            report_content += f"### {status_icon} {result.source_file}\n\n"
            report_content += f"- **Type**: {result.migration_type}\n"
            report_content += f"- **Lines Migrated**: {result.lines_migrated}\n"
            report_content += f"- **New Lines**: {result.new_lines_generated}\n"
            
            if result.target_files:
                report_content += f"- **Generated Files**:\n"
                for target_file in result.target_files:
                    report_content += f"  - {target_file}\n"
            
            if result.warnings:
                report_content += f"- **Warnings**:\n"
                for warning in result.warnings:
                    report_content += f"  - {warning}\n"
            
            if result.errors:
                report_content += f"- **Errors**:\n"
                for error in result.errors:
                    report_content += f"  - {error}\n"
            
            report_content += "\n"
        
        # Write report
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"Migration report generated: {report_file}")


# Convenience functions
def migrate_to_services(
    source_dir: str,
    target_dir: str,
    dry_run: bool = True,
    add_caching: bool = True,
    add_monitoring: bool = True
) -> List[MigrationResult]:
    """Convenience function for migrating to service architecture."""
    
    config = MigrationConfig(
        source_directory=source_dir,
        target_directory=target_dir,
        dry_run=dry_run,
        add_caching=add_caching,
        add_monitoring=add_monitoring,
        backup_directory=f"{source_dir}_backup" if not dry_run else None
    )
    
    migrator = CodeMigrator(config)
    return migrator.migrate_codebase()