#!/usr/bin/env python3
"""
Run Context Manager - Provides unique run IDs and artifact organization.

This module provides:
- Unique run_id generation from timestamps
- Run-specific directory management
- Context passing for state isolation
"""

import os
import uuid
import shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass
import json
import logging

logger = logging.getLogger(__name__)


@dataclass
class RunContext:
    """Context information for a single run."""
    run_id: str
    start_time: datetime
    base_dir: Path
    artifacts_dir: Path
    universe_state_dir: Path
    logs_dir: Path
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'run_id': self.run_id,
            'start_time': self.start_time.isoformat(),
            'base_dir': str(self.base_dir),
            'artifacts_dir': str(self.artifacts_dir),
            'universe_state_dir': str(self.universe_state_dir),
            'logs_dir': str(self.logs_dir),
            'metadata': self.metadata
        }

    def save_metadata(self):
        """Save run metadata to file."""
        metadata_file = self.artifacts_dir / "run_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info(f"Saved run metadata to {metadata_file}")


class RunIdGenerator:
    """Generates unique run IDs based on timestamps."""

    @staticmethod
    def generate() -> str:
        """
        Generate a unique run ID using timestamp and UUID.

        Format: run_YYYYMMDD_HHMMSS_<short_uuid>
        Example: run_20241218_143521_a1b2c3d4
        """
        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        short_uuid = str(uuid.uuid4())[:8]
        return f"run_{timestamp}_{short_uuid}"

    @staticmethod
    def extract_timestamp(run_id: str) -> Optional[datetime]:
        """Extract timestamp from run_id."""
        try:
            # Parse: run_YYYYMMDD_HHMMSS_<uuid>
            parts = run_id.split('_')
            if len(parts) >= 3 and parts[0] == 'run':
                date_part = parts[1]  # YYYYMMDD
                time_part = parts[2]  # HHMMSS
                timestamp_str = f"{date_part}_{time_part}"
                return datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
        except (ValueError, IndexError):
            pass
        return None


class RunContextManager:
    """Manages run contexts and directory structures."""

    def __init__(self, base_artifacts_dir: str = "data/runs"):
        """
        Initialize the run context manager.

        Args:
            base_artifacts_dir: Base directory for all run artifacts
        """
        self.base_artifacts_dir = Path(base_artifacts_dir)
        self.base_artifacts_dir.mkdir(parents=True, exist_ok=True)
        self._current_context: Optional[RunContext] = None

    def create_run_context(self,
                          run_id: Optional[str] = None,
                          metadata: Optional[Dict[str, Any]] = None) -> RunContext:
        """
        Create a new run context with unique directories.

        Args:
            run_id: Optional custom run ID (generates one if None)
            metadata: Optional metadata to store with the run

        Returns:
            RunContext instance
        """
        if run_id is None:
            run_id = RunIdGenerator.generate()

        start_time = datetime.now(timezone.utc)

        # Create run-specific directories
        run_base_dir = self.base_artifacts_dir / run_id
        artifacts_dir = run_base_dir / "artifacts"
        universe_state_dir = run_base_dir / "universe_state"
        logs_dir = run_base_dir / "logs"

        # Create all directories
        for dir_path in [run_base_dir, artifacts_dir, universe_state_dir, logs_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

        # Create universe state subdirectories for compatibility
        universe_state_subdirs = ["states", "metadata", "cache"]
        for subdir in universe_state_subdirs:
            (universe_state_dir / subdir).mkdir(parents=True, exist_ok=True)

        # Default metadata
        if metadata is None:
            metadata = {}

        try:
            working_dir = str(Path.cwd())
        except (FileNotFoundError, OSError):
            working_dir = 'unknown'

        metadata.update({
            'created_at': start_time.isoformat(),
            'python_version': f"{os.sys.version_info.major}.{os.sys.version_info.minor}",
            'working_directory': working_dir,
            'environment': os.environ.get('ENVIRONMENT', 'unknown')
        })

        context = RunContext(
            run_id=run_id,
            start_time=start_time,
            base_dir=run_base_dir,
            artifacts_dir=artifacts_dir,
            universe_state_dir=universe_state_dir,
            logs_dir=logs_dir,
            metadata=metadata
        )

        # Save metadata immediately
        context.save_metadata()

        logger.info(f"Created run context: {run_id}")
        logger.info(f"Artifacts directory: {artifacts_dir}")
        logger.info(f"Universe state directory: {universe_state_dir}")

        return context

    def set_current_context(self, context: RunContext):
        """Set the current active run context."""
        self._current_context = context
        logger.info(f"Set current run context to: {context.run_id}")

    def get_current_context(self) -> Optional[RunContext]:
        """Get the current active run context."""
        return self._current_context

    def list_runs(self, limit: int = 50) -> list[Dict[str, Any]]:
        """
        List recent runs with their metadata.

        Args:
            limit: Maximum number of runs to return

        Returns:
            List of run metadata dictionaries
        """
        runs = []

        # Find all run directories
        for run_dir in sorted(self.base_artifacts_dir.iterdir(), reverse=True):
            if run_dir.is_dir() and run_dir.name.startswith('run_'):
                # Look for metadata file in artifacts subdirectory
                metadata_file = run_dir / "artifacts" / "run_metadata.json"
                if metadata_file.exists():
                    try:
                        with open(metadata_file, 'r') as f:
                            run_data = json.load(f)
                        runs.append(run_data)
                        if len(runs) >= limit:
                            break
                    except Exception as e:
                        logger.warning(f"Failed to read metadata for {run_dir}: {e}")
                else:
                    # Fallback: create basic metadata if file doesn't exist
                    try:
                        timestamp = RunIdGenerator.extract_timestamp(run_dir.name)
                        basic_metadata = {
                            'run_id': run_dir.name,
                            'start_time': timestamp.isoformat() if timestamp else 'unknown',
                            'base_dir': str(run_dir),
                            'artifacts_dir': str(run_dir / "artifacts"),
                            'universe_state_dir': str(run_dir / "universe_state"),
                            'logs_dir': str(run_dir / "logs"),
                            'metadata': {'created_at': 'unknown', 'source': 'fallback'}
                        }
                        runs.append(basic_metadata)
                        if len(runs) >= limit:
                            break
                    except Exception as e:
                        logger.warning(f"Failed to create fallback metadata for {run_dir}: {e}")

        return runs

    def cleanup_old_runs(self, keep_days: int = 30):
        """
        Clean up runs older than specified days.

        Args:
            keep_days: Number of days to keep runs
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=keep_days)
        removed_count = 0

        for run_dir in self.base_artifacts_dir.iterdir():
            if run_dir.is_dir() and run_dir.name.startswith('run_'):
                run_id = run_dir.name
                run_time = RunIdGenerator.extract_timestamp(run_id)

                if run_time:
                    # Make run_time timezone-aware for comparison
                    if run_time.tzinfo is None:
                        run_time = run_time.replace(tzinfo=timezone.utc)

                    if run_time < cutoff_time:
                        try:
                            shutil.rmtree(run_dir)
                            removed_count += 1
                            logger.info(f"Removed old run: {run_id}")
                        except Exception as e:
                            logger.warning(f"Failed to remove {run_dir}: {e}")

        logger.info(f"Cleanup complete: removed {removed_count} old runs")


# Global instance for convenience
_global_run_manager = RunContextManager()

def get_run_manager() -> RunContextManager:
    """Get the global run context manager."""
    return _global_run_manager

def create_run_context(run_id: Optional[str] = None,
                      metadata: Optional[Dict[str, Any]] = None) -> RunContext:
    """Convenience function to create a run context."""
    return _global_run_manager.create_run_context(run_id, metadata)

def get_current_run_context() -> Optional[RunContext]:
    """Convenience function to get current run context."""
    return _global_run_manager.get_current_context()