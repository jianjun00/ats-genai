#!/usr/bin/env python3
"""
Comprehensive metadata tracking utility for ML training runs.

This module provides standardized metadata collection and storage for all training runs,
ensuring full reproducibility and traceability of experiments.

METADATA CAPTURED:
- Git commit hash and branch information
- Complete command line arguments
- Environment variables and system info
- Python version and key dependencies
- Host/container information
- Working directory and file paths
- Execution timestamps and performance metrics

USAGE:
    from src.ml.training_data.utils.run_metadata_tracker import RunMetadataTracker
    
    # Initialize tracker
    tracker = RunMetadataTracker(
        run_type="unified_training_dataset_generation",
        created_by="generate_unified_training_dataset.py"
    )
    
    # Start run tracking
    run_id = await tracker.start_run(
        parameters={"symbols": ["AAPL", "TSLA"], "sequence_length": 60}
    )
    
    # Update during execution
    await tracker.update_progress(run_id, {"sequences_processed": 1000})
    
    # Complete run
    await tracker.complete_run(run_id, {
        "total_sequences": 169,
        "files_created": ["AAPL.riegeli", "TSLA.riegeli"],
        "quality_score": 1.0
    })
"""

import asyncio
import sys
import os
import subprocess
import json
import hashlib
import platform
import socket
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
import asyncpg
import logging

logger = logging.getLogger(__name__)

class RunMetadataTracker:
    """Comprehensive metadata tracker for ML training runs."""
    
    def __init__(self, run_type: str, created_by: str, environment: Optional[str] = None):
        """
        Initialize metadata tracker.
        
        Args:
            run_type: Type of run (e.g., "training_dataset_generation", "model_training")
            created_by: Script or user that initiated the run
            environment: Environment name (dev, intg, prod) - auto-detected if None
        """
        self.run_type = run_type
        self.created_by = created_by
        self.environment = environment or self._detect_environment()
        self.run_id = None
        self._connection = None
        
    def _detect_environment(self) -> str:
        """Auto-detect environment based on database connection or container info."""
        # Check for environment variable first
        env = os.getenv('ENVIRONMENT')
        if env:
            return env
            
        # Check database port to infer environment
        db_port = os.getenv('DB_PORT', '3432')
        if db_port == '3432':
            return 'dev'
        elif db_port == '4432':
            return 'intg'
        elif db_port == '5432':
            return 'prod'
        
        return 'unknown'
    
    def _get_git_info(self) -> Dict[str, str]:
        """Get current git commit and branch information."""
        try:
            # Get current commit hash
            commit_hash = subprocess.check_output(
                ['git', 'rev-parse', 'HEAD'], 
                cwd=Path(__file__).parent.parent.parent.parent,
                stderr=subprocess.DEVNULL
            ).decode().strip()
            
            # Get current branch
            branch = subprocess.check_output(
                ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
                cwd=Path(__file__).parent.parent.parent.parent,
                stderr=subprocess.DEVNULL
            ).decode().strip()
            
            # Check for uncommitted changes
            try:
                subprocess.check_output(
                    ['git', 'diff-index', '--quiet', 'HEAD', '--'],
                    cwd=Path(__file__).parent.parent.parent.parent,
                    stderr=subprocess.DEVNULL
                )
                has_changes = False
            except subprocess.CalledProcessError:
                has_changes = True
            
            return {
                'commit_hash': commit_hash,
                'branch': branch,
                'has_uncommitted_changes': has_changes
            }
        except (subprocess.CalledProcessError, FileNotFoundError):
            return {
                'commit_hash': 'unknown',
                'branch': 'unknown',
                'has_uncommitted_changes': False
            }
    
    def _get_host_info(self) -> Dict[str, Any]:
        """Get host system and container information."""
        host_info = {
            'hostname': socket.gethostname(),
            'platform': platform.platform(),
            'python_version': platform.python_version(),
            'architecture': platform.architecture()[0],
            'processor': platform.processor() or 'unknown',
            'working_directory': str(Path.cwd()),
            'user': os.getenv('USER', 'unknown'),
            'container_id': None,
            'docker_image': None
        }
        
        # Check if running in Docker
        try:
            with open('/proc/1/cgroup', 'r') as f:
                content = f.read()
                if 'docker' in content or 'containerd' in content:
                    # Extract container ID
                    for line in content.split('\n'):
                        if 'docker' in line and '/' in line:
                            host_info['container_id'] = line.split('/')[-1][:12]
                            break
        except FileNotFoundError:
            pass
        
        # Check for Docker image info
        docker_image = os.getenv('DOCKER_IMAGE')
        if docker_image:
            host_info['docker_image'] = docker_image
            
        return host_info
    
    def _get_dependencies_hash(self) -> str:
        """Get hash of key dependencies for reproducibility."""
        try:
            # Look for requirements files
            requirements_files = []
            project_root = Path(__file__).parent.parent.parent.parent
            
            for req_file in ['requirements.txt', 'pyproject.toml', 'Pipfile']:
                req_path = project_root / req_file
                if req_path.exists():
                    requirements_files.append(str(req_path))
            
            if requirements_files:
                # Hash the content of requirements files
                hasher = hashlib.sha256()
                for req_file in requirements_files:
                    with open(req_file, 'rb') as f:
                        hasher.update(f.read())
                return hasher.hexdigest()[:16]
            
            # Fallback: hash key package versions
            key_packages = ['numpy', 'pandas', 'tensorflow', 'torch', 'asyncpg']
            version_info = []
            
            for package in key_packages:
                try:
                    import importlib.metadata
                    version = importlib.metadata.version(package)
                    version_info.append(f"{package}=={version}")
                except importlib.metadata.PackageNotFoundError:
                    pass
            
            if version_info:
                hasher = hashlib.sha256()
                hasher.update('\n'.join(version_info).encode())
                return hasher.hexdigest()[:16]
                
        except Exception as e:
            logger.warning(f"Could not compute dependencies hash: {e}")
        
        return 'unknown'
    
    async def _get_connection(self) -> asyncpg.Connection:
        """Get database connection based on environment."""
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
    
    async def start_run(self, parameters: Dict[str, Any] = None) -> int:
        """
        Start a new run and record initial metadata.
        
        Args:
            parameters: Run-specific parameters and configuration
            
        Returns:
            Run ID for tracking this execution
        """
        conn = await self._get_connection()
        
        # Collect comprehensive metadata
        git_info = self._get_git_info()
        host_info = self._get_host_info()
        command_line = ' '.join(sys.argv)
        dependencies_hash = self._get_dependencies_hash()
        
        # Store in database
        table_name = f"{self.environment}_runs"
        
        query = f"""
            INSERT INTO {table_name} (
                run_type, status, command_line, git_commit_hash, git_branch,
                environment, parameters, created_by, host_info, working_directory,
                python_version, dependencies_hash, start_time
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            RETURNING id
        """
        
        self.run_id = await conn.fetchval(
            query,
            self.run_type,           # run_type
            'running',               # status
            command_line,            # command_line
            git_info['commit_hash'], # git_commit_hash
            git_info['branch'],      # git_branch
            self.environment,        # environment
            json.dumps(parameters or {}),  # parameters
            self.created_by,         # created_by
            json.dumps(host_info),   # host_info
            host_info['working_directory'],  # working_directory
            host_info['python_version'],     # python_version
            dependencies_hash,       # dependencies_hash
            datetime.utcnow()        # start_time
        )
        
        logger.info(f"Started run {self.run_id} - Type: {self.run_type}, Environment: {self.environment}")
        logger.info(f"Git: {git_info['branch']}@{git_info['commit_hash'][:8]}")
        logger.info(f"Host: {host_info['hostname']} ({host_info['platform']})")
        
        if git_info['has_uncommitted_changes']:
            logger.warning("⚠️ Running with uncommitted changes - run may not be fully reproducible")
        
        return self.run_id
    
    async def update_progress(self, run_id: int, progress_data: Dict[str, Any]) -> None:
        """Update run with progress information during execution."""
        conn = await self._get_connection()
        table_name = f"{self.environment}_runs"
        
        # Merge with existing parameters
        existing_params = await conn.fetchval(
            f"SELECT parameters FROM {table_name} WHERE id = $1", run_id
        )
        
        if existing_params:
            merged_params = json.loads(existing_params)
            merged_params.update(progress_data)
        else:
            merged_params = progress_data
        
        await conn.execute(
            f"UPDATE {table_name} SET parameters = $1 WHERE id = $2",
            json.dumps(merged_params), run_id
        )
        
        logger.debug(f"Updated run {run_id} progress: {progress_data}")
    
    async def complete_run(self, run_id: int, results: Dict[str, Any], 
                          status: str = 'completed', error_message: str = None) -> None:
        """Mark run as complete with final results."""
        conn = await self._get_connection()
        table_name = f"{self.environment}_runs"
        
        await conn.execute(f"""
            UPDATE {table_name} 
            SET status = $1, end_time = $2, results = $3, error_message = $4
            WHERE id = $5
        """, status, datetime.utcnow(), json.dumps(results), error_message, run_id)
        
        if status == 'completed':
            logger.info(f"✅ Completed run {run_id} successfully")
        else:
            logger.error(f"❌ Run {run_id} failed: {error_message}")
        
        # Log key results
        if results:
            for key, value in results.items():
                if isinstance(value, (int, float, str)):
                    logger.info(f"  {key}: {value}")
    
    async def get_run_metadata(self, run_id: int) -> Optional[Dict[str, Any]]:
        """Get complete metadata for a specific run."""
        conn = await self._get_connection()
        table_name = f"{self.environment}_runs"
        
        query = f"""
            SELECT * FROM {table_name} WHERE id = $1
        """
        
        row = await conn.fetchrow(query, run_id)
        if row:
            return dict(row)
        return None
    
    async def close(self):
        """Close database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None

# Convenience functions for common use cases

async def track_training_run(run_type: str, created_by: str, parameters: Dict[str, Any] = None) -> int:
    """Quick start for training run tracking."""
    tracker = RunMetadataTracker(run_type=run_type, created_by=created_by)
    return await tracker.start_run(parameters)

async def complete_training_run(run_id: int, results: Dict[str, Any], environment: str = 'dev'):
    """Quick completion for training run tracking."""
    tracker = RunMetadataTracker("", "", environment)
    await tracker.complete_run(run_id, results)
    await tracker.close()

# Context manager for automatic run lifecycle management
class RunTracker:
    """Context manager for automatic run tracking lifecycle."""
    
    def __init__(self, run_type: str, created_by: str, parameters: Dict[str, Any] = None):
        self.tracker = RunMetadataTracker(run_type, created_by)
        self.parameters = parameters
        self.run_id = None
    
    async def __aenter__(self):
        self.run_id = await self.tracker.start_run(self.parameters)
        return self.tracker, self.run_id
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            # Success
            await self.tracker.complete_run(self.run_id, {}, 'completed')
        else:
            # Error occurred
            await self.tracker.complete_run(
                self.run_id, {}, 'failed', 
                f"{exc_type.__name__}: {exc_val}"
            )
        await self.tracker.close()